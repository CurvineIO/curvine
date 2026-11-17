// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::object_lock_pool::ObjectLockPool;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use smallvec::SmallVec;

#[cfg(debug_assertions)]
use std::collections::BTreeSet;

const DEFAULT_INODE_LOCK_SHARDS: usize = 4096;

const INLINE_INODE_LOCKS: usize = 4;

type InodeLocks<T> = SmallVec<[T; INLINE_INODE_LOCKS]>;

#[cfg(debug_assertions)]
thread_local! {
    static HELD_INODE_LOCKS: std::cell::RefCell<BTreeSet<(usize, usize)>> =
        const { std::cell::RefCell::new(BTreeSet::new()) };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeLockMode {
    Read,
    Write,
}

impl InodeLockMode {
    fn merge(self, other: Self) -> Self {
        if matches!(self, Self::Write) || matches!(other, Self::Write) {
            Self::Write
        } else {
            Self::Read
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InodeLockRequest {
    pub depth: usize,
    pub inode_id: i64,
    pub mode: InodeLockMode,
}

impl InodeLockRequest {
    pub fn read(depth: usize, inode_id: i64) -> Self {
        Self {
            depth,
            inode_id,
            mode: InodeLockMode::Read,
        }
    }

    pub fn write(depth: usize, inode_id: i64) -> Self {
        Self {
            depth,
            inode_id,
            mode: InodeLockMode::Write,
        }
    }
}

pub struct InodeLockManager {
    locks: ObjectLockPool,
}

impl Default for InodeLockManager {
    fn default() -> Self {
        Self::new(DEFAULT_INODE_LOCK_SHARDS)
    }
}

impl InodeLockManager {
    /// Record how long the caller waited for this lock acquisition, in
    /// microseconds, so hot directories and stalled holders stay observable
    /// (design doc §11.2). Best-effort: metric registration failure is
    /// ignored on the hot path.
    ///
    /// Samples accumulate in a thread-local `LocalHistogram` and reach the
    /// shared histogram after `FLUSH_EVERY` samples, on the first sample at
    /// least `FLUSH_AFTER` past the previous flush, or at thread exit:
    /// `Histogram::observe` does four shared atomic RMWs (one a CAS loop on
    /// the f64 sum) that every lock_many on every core contended on. Metric
    /// name, labels, buckets and per-sample values are unchanged.
    ///
    /// Visibility is no longer immediate: a scrape may miss up to
    /// `FLUSH_EVERY - 1` samples per thread and label. A thread that goes
    /// idle keeps its unflushed tail until its next sample or its exit, and
    /// an abort loses it; the time bound only holds while the thread keeps
    /// taking locks.
    fn record_acquire_us(mode: InodeLockMode, started: std::time::Instant) {
        use curvine_metrics::LocalHistogram;
        use std::cell::{Cell, OnceCell};
        use std::time::{Duration, Instant};
        const FLUSH_EVERY: u32 = 64;
        const FLUSH_AFTER: Duration = Duration::from_secs(1);
        struct Local {
            histogram: OnceCell<LocalHistogram>,
            pending: Cell<u32>,
            flushed_at: Cell<Option<Instant>>,
        }
        thread_local! {
            static READ: Local = const { Local::new() };
            static WRITE: Local = const { Local::new() };
        }
        impl Local {
            const fn new() -> Self {
                Self {
                    histogram: OnceCell::new(),
                    pending: Cell::new(0),
                    flushed_at: Cell::new(None),
                }
            }
        }
        let (slot, label) = match mode {
            InodeLockMode::Read => (&READ, "read"),
            InodeLockMode::Write => (&WRITE, "write"),
        };
        let now = Instant::now();
        let micros = now.saturating_duration_since(started).as_micros() as f64;
        // `try_with` skips the sample instead of panicking when a lock is
        // taken from another thread-local destructor after this slot is gone.
        let _ = slot.try_with(|local| {
            // Not cached on failure: metrics registered later are still picked up.
            let histogram = match local.histogram.get() {
                Some(histogram) => histogram,
                None => match crate::master::Master::get_metrics() {
                    Ok(metrics) => local.histogram.get_or_init(|| {
                        metrics
                            .inode_lock_acquire_us
                            .with_label_values(&[label])
                            .local()
                    }),
                    Err(_) => return,
                },
            };
            histogram.observe(micros);
            let pending = local.pending.get() + 1;
            let flushed_at = *local.flushed_at.get().get_or_insert(now);
            if pending >= FLUSH_EVERY || now.saturating_duration_since(flushed_at) >= FLUSH_AFTER {
                histogram.flush();
                local.pending.set(0);
                local.flushed_at.set(Some(now));
            } else {
                local.pending.set(pending);
                local.flushed_at.set(Some(flushed_at));
            }
        });
    }

    pub fn new(shard_count: usize) -> Self {
        Self {
            locks: ObjectLockPool::new(shard_count),
        }
    }

    /// Non-blocking shared acquire: the target inode stripe's read lock only if
    /// currently free — no waiting, no queue registration. `None` means a writer
    /// holds the stripe. Optimistic readers use it because they hold the
    /// namespace view guard, and a cold window waits for every such guard, so
    /// they must never block on an object lock while holding one.
    pub fn try_read_inode(&self, inode_id: i64) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
        let stripe = self.locks.index(inode_id as u64);
        self.locks.try_read_by_stripe(stripe)
    }

    /// Test-only non-blocking exclusive observation (file_status Busy retry):
    /// a write lock on the same stripe succeeds only when the parked reader's
    /// read lock is gone — read/read coexistence would make a read probe
    /// pass regardless.
    #[cfg(test)]
    pub fn try_write_inode(&self, inode_id: i64) -> Option<parking_lot::RwLockWriteGuard<'_, ()>> {
        let stripe = self.locks.index(inode_id as u64);
        self.locks.try_write_by_stripe(stripe)
    }

    pub fn lock_many<'a>(&'a self, requests: &[InodeLockRequest]) -> InodeLockSet<'a> {
        let started = std::time::Instant::now();
        let mut stripes: SmallVec<[(usize, InodeLockMode); 8]> = requests
            .iter()
            .map(|request| (self.locks.index(request.inode_id as u64), request.mode))
            .collect();
        stripes.sort_unstable_by_key(|entry| entry.0);
        let mut unique = 0;
        for i in 0..stripes.len() {
            if unique > 0 && stripes[unique - 1].0 == stripes[i].0 {
                stripes[unique - 1].1 = stripes[unique - 1].1.merge(stripes[i].1);
            } else {
                stripes[unique] = stripes[i];
                unique += 1;
            }
        }
        stripes.truncate(unique);

        #[cfg(debug_assertions)]
        HELD_INODE_LOCKS.with(|held| {
            let held = held.borrow();
            for &(stripe, _) in &stripes {
                assert!(
                    !held.contains(&lock_key(self, stripe)),
                    "reentrant inode lock on stripe {}",
                    stripe
                );
            }
        });
        let mut guards = InodeLocks::with_capacity(stripes.len());
        for &(stripe, mode) in &stripes {
            guards.push(match mode {
                InodeLockMode::Read => InodeGuard::Read {
                    _guard: self.locks.lock_shared_at(stripe),
                },
                InodeLockMode::Write => InodeGuard::Write {
                    _guard: self.locks.lock_exclusive_at(stripe),
                },
            });
        }
        #[cfg(debug_assertions)]
        HELD_INODE_LOCKS.with(|held| {
            let mut held = held.borrow_mut();
            for &(stripe, _) in &stripes {
                held.insert(lock_key(self, stripe));
            }
        });
        let mode = if stripes.iter().any(|entry| entry.1 == InodeLockMode::Write) {
            InodeLockMode::Write
        } else {
            InodeLockMode::Read
        };
        Self::record_acquire_us(mode, started);
        InodeLockSet {
            guards,
            #[cfg(debug_assertions)]
            manager: self,
            #[cfg(debug_assertions)]
            stripes: stripes.into_iter().map(|entry| entry.0).collect(),
        }
    }

    pub fn read_inode(&self, inode_id: i64) -> InodeLockSet<'_> {
        self.lock_many(&[InodeLockRequest::read(0, inode_id)])
    }

    pub fn write_inode(&self, inode_id: i64) -> InodeLockSet<'_> {
        self.lock_many(&[InodeLockRequest::write(0, inode_id)])
    }
}

pub struct InodeLockSet<'a> {
    guards: InodeLocks<InodeGuard<'a>>,
    #[cfg(debug_assertions)]
    manager: &'a InodeLockManager,
    #[cfg(debug_assertions)]
    stripes: InodeLocks<usize>,
}

enum InodeGuard<'a> {
    Read { _guard: RwLockReadGuard<'a, ()> },
    Write { _guard: RwLockWriteGuard<'a, ()> },
}

impl Drop for InodeLockSet<'_> {
    fn drop(&mut self) {
        self.guards.clear();
        #[cfg(debug_assertions)]
        HELD_INODE_LOCKS.with(|held| {
            let mut held = held.borrow_mut();
            for &stripe in &self.stripes {
                held.remove(&lock_key(self.manager, stripe));
            }
        });
    }
}

#[cfg(debug_assertions)]
fn lock_key(manager: &InodeLockManager, stripe: usize) -> (usize, usize) {
    (manager as *const InodeLockManager as usize, stripe)
}
