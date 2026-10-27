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
                hold: None,
            };
        };
        let start = Instant::now();
        let guard = self.inner.read().unwrap();
        DirTreeReadGuard {
            guard,
            hold: Some(HoldTimer::start(start, &m.read)),
        }
    }

    pub fn write(&self) -> DirTreeWriteGuard<'_> {
        let Some(m) = &self.metrics else {
            return DirTreeWriteGuard {
                guard: self.inner.write().unwrap(),
                hold: None,
            };
        };
        let start = Instant::now();
        let guard = self.inner.write().unwrap();
        DirTreeWriteGuard {
            guard,
            hold: Some(HoldTimer::start(start, &m.write)),
        }
    }
}

struct HoldTimer<'a> {
    acquired: Instant,
    /// End of the critical section, captured by the owning guard's `Drop::drop` while the lock
    /// is still held. `None` means the guard was leaked, so no sample is recorded.
    released: Option<Instant>,
    hist: &'a Histogram,
}

impl<'a> HoldTimer<'a> {
    fn start(wait_start: Instant, hists: &'a ModeHistograms) -> Self {
        let acquired = Instant::now();
        hists.wait.observe(micros(acquired - wait_start));
        Self {
            acquired,
            released: None,
            hist: &hists.hold,
        }
    }

    /// Closes the critical section. The owning guard calls this from `Drop::drop`, which a
    /// struct runs before its fields are dropped, so the lock is still held here and any
    /// scheduling delay after the unlock cannot be charged to the hold time.
    fn finish(&mut self) {
        self.released = Some(Instant::now());
    }
}

impl Drop for HoldTimer<'_> {
    fn drop(&mut self) {
        // The owning guard's `RwLock` guard is dropped before this timer (see field order on the
        // guard types), so recording the sample is not itself counted as hold time.
        if let Some(released) = self.released {
            self.hist.observe(micros(released - self.acquired));
        }
    }
}

fn micros(d: std::time::Duration) -> f64 {
    d.as_nanos() as f64 / 1000.0
}

// Field order matters: the lock guard drops (unlocks) before the hold timer observes, so the
// sample is recorded outside the critical section. `Drop::drop` below runs before the fields are
// dropped and therefore still sees the lock held.
pub struct DirTreeReadGuard<'a> {
    guard: RwLockReadGuard<'a, DirTree>,
    hold: Option<HoldTimer<'a>>,
}

impl Drop for DirTreeReadGuard<'_> {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.as_mut() {
            hold.finish();
        }
    }
}

impl Deref for DirTreeReadGuard<'_> {
    type Target = DirTree;

    fn deref(&self) -> &DirTree {
        &self.guard
    }
}

// Field order matters, see `DirTreeReadGuard`.
pub struct DirTreeWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, DirTree>,
    hold: Option<HoldTimer<'a>>,
}

impl Drop for DirTreeWriteGuard<'_> {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.as_mut() {
            hold.finish();
        }
    }
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
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    /// Held inside the critical section; long enough to be unambiguous, short enough to keep the
    /// suite fast.
    const CRITICAL_SECTION: Duration = Duration::from_millis(5);
    const CRITICAL_SECTION_US: f64 = 5_000.0;

    /// How long a writer starves a reader in the contention test.
    const CONTENTION: Duration = Duration::from_millis(50);
    const CONTENTION_US: f64 = 50_000.0;
    /// A blocked reader must report at least this much of `CONTENTION`; the slack absorbs thread
    /// start-up between the handshake below and the blocked `read()` call.
    const MIN_WAIT_US: f64 = 45_000.0;

    /// `[read.wait, read.hold, write.wait, write.hold]` sample counts.
    fn counts(lock: &DirTreeLock) -> [u64; 4] {
        let m = lock.metrics.as_ref().unwrap();
        [
            m.read.wait.get_sample_count(),
            m.read.hold.get_sample_count(),
            m.write.wait.get_sample_count(),
            m.write.hold.get_sample_count(),
        ]
    }

    /// `[read.wait, read.hold, write.wait, write.hold]` sample sums, in microseconds.
    fn sums(lock: &DirTreeLock) -> [f64; 4] {
        let m = lock.metrics.as_ref().unwrap();
        [
            m.read.wait.get_sample_sum(),
            m.read.hold.get_sample_sum(),
            m.write.wait.get_sample_sum(),
            m.write.hold.get_sample_sum(),
        ]
    }

    fn mode_histograms(label: &str) -> ModeHistograms {
        let m = FuseMetrics::get();
        ModeHistograms {
            wait: m.dcache_lock_wait_us.with_label_values(&[label]),
            hold: m.dcache_lock_hold_us.with_label_values(&[label]),
        }
    }

    fn isolated_mode_histograms(test: &str) -> ModeHistograms {
        FuseMetrics::ensure_init().unwrap();
        mode_histograms(&format!("test_{test}"))
    }

    /// Sum deltas are only meaningful when no other test observes the same series, and the
    /// histograms are global (`node_state` tests also take `dir_read`/`dir_write`). Each accuracy
    /// test therefore uses label values unique to it.
    fn isolated_lock(test: &str) -> DirTreeLock {
        FuseMetrics::ensure_init().unwrap();
        DirTreeLock {
            inner: RwLock::new(DirTree::default()),
            metrics: Some(LockHistograms {
                read: mode_histograms(&format!("test_{test}_read")),
                write: mode_histograms(&format!("test_{test}_write")),
            }),
        }
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
    fn hold_time_covers_the_critical_section() {
        let lock = isolated_lock("hold_time_covers_the_critical_section");
        let before = sums(&lock);

        {
            let guard = lock.write();
            std::thread::sleep(CRITICAL_SECTION);
            assert_eq!(guard.inode_lens(), 1);
        }

        let after = sums(&lock);
        let hold = after[3] - before[3];
        assert!(
            hold >= CRITICAL_SECTION_US,
            "hold sample {hold}us is shorter than the {CRITICAL_SECTION_US}us critical section"
        );
    }

    #[test]
    fn wait_time_covers_blocking_while_hold_time_stays_short() {
        let lock = Arc::new(isolated_lock(
            "wait_time_covers_blocking_while_hold_time_stays_short",
        ));
        let before = sums(&lock);

        // The writer's own samples land in the write series, so the read series holds exactly one
        // wait/hold pair below: the blocked reader's.
        let writer = lock.write();
        let (started, blocked) = mpsc::channel();
        let reader = {
            let lock = Arc::clone(&lock);
            std::thread::spawn(move || {
                started.send(()).unwrap();
                let guard = lock.read();
                assert_eq!(guard.inode_lens(), 1);
            })
        };
        blocked.recv().unwrap();
        std::thread::sleep(CONTENTION);
        drop(writer);
        reader.join().unwrap();

        let after = sums(&lock);
        let wait = after[0] - before[0];
        let hold = after[1] - before[1];

        assert!(
            wait >= MIN_WAIT_US,
            "wait sample {wait}us should cover the {CONTENTION:?} block"
        );
        // The reader's critical section is a single deref. Sampling the hold time after the unlock
        // instead lets the post-unlock scheduling gap grow towards the wait measured above.
        assert!(
            hold < CRITICAL_SECTION_US,
            "hold sample {hold}us should stay below {CRITICAL_SECTION_US}us while the reader \
             waited {wait}us"
        );
    }

    #[test]
    fn hold_sample_ends_at_finish_not_at_drop() {
        let hists = isolated_mode_histograms("hold_sample_ends_at_finish_not_at_drop");
        let before = hists.hold.get_sample_sum();

        let mut timer = HoldTimer::start(Instant::now(), &hists);
        std::thread::sleep(CRITICAL_SECTION);
        timer.finish();
        // Work done after the unlock must not be charged to the hold time. This is the regression
        // guard for sampling in `Drop` instead of at `finish()`, which would add this sleep.
        std::thread::sleep(CONTENTION);
        drop(timer);

        let hold = hists.hold.get_sample_sum() - before;
        let upper = CRITICAL_SECTION_US + CONTENTION_US / 2.0;
        assert!(
            (CRITICAL_SECTION_US..upper).contains(&hold),
            "hold sample {hold}us should cover the {CRITICAL_SECTION_US}us before finish, not the \
             {CONTENTION:?} after it"
        );
    }

    #[test]
    fn disabled_metrics_skip_histograms() {
        FuseMetrics::ensure_init().unwrap();
        let lock = DirTreeLock::new(DirTree::default(), false);
        assert!(lock.metrics.is_none());
        assert_eq!(lock.write().inode_lens(), 1);
    }
}
