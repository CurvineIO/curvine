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
    fn record_acquire_us(mode: InodeLockMode, started: std::time::Instant) {
        use std::sync::OnceLock;
        static READ: OnceLock<curvine_metrics::Histogram> = OnceLock::new();
        static WRITE: OnceLock<curvine_metrics::Histogram> = OnceLock::new();
        let (cache, label) = match mode {
            InodeLockMode::Read => (&READ, "read"),
            InodeLockMode::Write => (&WRITE, "write"),
        };
        let histogram = match cache.get() {
            Some(histogram) => histogram,
            None => match crate::master::Master::get_metrics() {
                Ok(metrics) => {
                    cache.get_or_init(|| metrics.inode_lock_acquire_us.with_label_values(&[label]))
                }
                Err(_) => return,
            },
        };
        histogram.observe(started.elapsed().as_micros() as f64);
    }

    pub fn new(shard_count: usize) -> Self {
        Self {
            locks: ObjectLockPool::new(shard_count),
        }
    }

    /// Test-only non-blocking shared observation (R8): acquires the target
    /// inode stripe's read lock only if currently free — no waiting, no
    /// queue registration. `None` proves the stripe is still held by the
    /// terminal request; `Some` would prove it was already released.
    #[cfg(test)]
    pub fn try_read_inode(&self, inode_id: i64) -> Option<parking_lot::RwLockReadGuard<'_, ()>> {
        let stripe = self.locks.index(inode_id as u64);
        self.locks.try_read_by_stripe(stripe)
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
