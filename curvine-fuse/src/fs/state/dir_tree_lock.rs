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

use crate::fs::dcache::DirTree;
use crate::fuse_metrics::{DCACHE_LOCK_MODE_READ, DCACHE_LOCK_MODE_WRITE};
use crate::FuseMetrics;
use curvine_metrics::Histogram;
use std::ops::{Deref, DerefMut};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

struct ModeHistograms {
    wait: Histogram,
    hold: Histogram,
}

struct LockHistograms {
    read: ModeHistograms,
    write: ModeHistograms,
}

impl LockHistograms {
    fn resolve() -> Option<Self> {
        let mut out = None;
        FuseMetrics::with(|m| {
            let mode = |label| ModeHistograms {
                wait: m.dcache_lock_wait_us.with_label_values(&[label]),
                hold: m.dcache_lock_hold_us.with_label_values(&[label]),
            };
            out = Some(Self {
                read: mode(DCACHE_LOCK_MODE_READ),
                write: mode(DCACHE_LOCK_MODE_WRITE),
            });
        });
        out
    }
}

/// Process-wide dcache lock; records wait/hold time per mode when metrics are enabled.
pub struct DirTreeLock {
    inner: RwLock<DirTree>,
    metrics: Option<LockHistograms>,
}

impl DirTreeLock {
    pub fn new(tree: DirTree, metrics_enabled: bool) -> Self {
        let metrics = if metrics_enabled {
            LockHistograms::resolve()
        } else {
            None
        };
        Self {
            inner: RwLock::new(tree),
            metrics,
        }
    }

    pub fn read(&self) -> DirTreeReadGuard<'_> {
        let Some(m) = &self.metrics else {
            return DirTreeReadGuard {
                guard: self.inner.read().unwrap(),
                _hold: None,
            };
        };
        let start = Instant::now();
        let guard = self.inner.read().unwrap();
        DirTreeReadGuard {
            guard,
            _hold: Some(HoldTimer::start(start, &m.read)),
        }
    }

    pub fn write(&self) -> DirTreeWriteGuard<'_> {
        let Some(m) = &self.metrics else {
            return DirTreeWriteGuard {
                guard: self.inner.write().unwrap(),
                _hold: None,
            };
        };
        let start = Instant::now();
        let guard = self.inner.write().unwrap();
        DirTreeWriteGuard {
            guard,
            _hold: Some(HoldTimer::start(start, &m.write)),
        }
    }
}

struct HoldTimer<'a> {
    acquired: Instant,
    hist: &'a Histogram,
}

impl<'a> HoldTimer<'a> {
    fn start(wait_start: Instant, hists: &'a ModeHistograms) -> Self {
        let acquired = Instant::now();
        hists.wait.observe(micros(acquired - wait_start));
        Self {
            acquired,
            hist: &hists.hold,
        }
    }
}

impl Drop for HoldTimer<'_> {
    fn drop(&mut self) {
        self.hist.observe(micros(self.acquired.elapsed()));
    }
}

fn micros(d: std::time::Duration) -> f64 {
    d.as_nanos() as f64 / 1000.0
}

// Field order matters: the lock guard drops (unlocks) before the hold timer observes.
pub struct DirTreeReadGuard<'a> {
    guard: RwLockReadGuard<'a, DirTree>,
    _hold: Option<HoldTimer<'a>>,
}

impl Deref for DirTreeReadGuard<'_> {
    type Target = DirTree;

    fn deref(&self) -> &DirTree {
        &self.guard
    }
}

pub struct DirTreeWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, DirTree>,
    _hold: Option<HoldTimer<'a>>,
}

impl Deref for DirTreeWriteGuard<'_> {
    type Target = DirTree;

    fn deref(&self) -> &DirTree {
        &self.guard
    }
}

impl DerefMut for DirTreeWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut DirTree {
        &mut self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(lock: &DirTreeLock) -> [u64; 4] {
        let m = lock.metrics.as_ref().unwrap();
        [
            m.read.wait.get_sample_count(),
            m.read.hold.get_sample_count(),
            m.write.wait.get_sample_count(),
            m.write.hold.get_sample_count(),
        ]
    }

    #[test]
    fn records_wait_and_hold_per_mode_when_enabled() {
        FuseMetrics::ensure_init().unwrap();
        let lock = DirTreeLock::new(DirTree::default(), true);

        let before = counts(&lock);
        {
            let r = lock.read();
            assert_eq!(r.inode_lens(), 1);
            assert!(counts(&lock)[0] > before[0]);
        }
        let after_read = counts(&lock);
        assert!(after_read[1] > before[1]);

        drop(lock.write());
        let after_write = counts(&lock);
        assert!(after_write[2] > after_read[2]);
        assert!(after_write[3] > after_read[3]);
    }

    #[test]
    fn disabled_metrics_skip_histograms() {
        FuseMetrics::ensure_init().unwrap();
        let lock = DirTreeLock::new(DirTree::default(), false);
        assert!(lock.metrics.is_none());
        assert_eq!(lock.write().inode_lens(), 1);
    }
}
