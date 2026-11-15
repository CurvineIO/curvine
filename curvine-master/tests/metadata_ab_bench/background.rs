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

//! Background-load scenario: a hot parent directory under continuous
//! create/delete while foreground threads share one deep child path.
//!
//! Kept apart from `harness.rs` so each module stays small and the main
//! matrix runner does not grow a second, differently-shaped thread topology.
//!
//! Shutdown rule: the foreground does a fixed number of operations and the
//! background keeps offering load until the last foreground thread finishes.
//! If both sides simply ran a fixed count, whichever finished first would
//! leave the other running without contention, and that tail would drag its
//! p99 down - differently on the two builds. All foreground samples are
//! therefore taken under the same background pressure.
//!
//! Every path is formatted before the barrier; the timed region holds one
//! `MasterFilesystem` call only.

use crate::harness::{merge_into, Layout, Report, ThreadResult};
use crate::hot;
use curvine_master::master::fs::MasterFilesystem;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// Hot-parent background scenario constants. `target_rate` is per background
/// thread and is an *offered* rate: the throttle never pushes the background
/// faster than this, but it cannot make a saturated store keep up. When the
/// achieved rate is below target, `rate_ratio` reports it; two builds are only
/// comparable when their achieved background rates are close.
pub const HOT_PARENT_TARGET_OPS_PER_SEC: u64 = 20_000;
pub const HOT_PARENT_DEPTH: usize = 4;

/// Hard ceiling on background cycles, so a foreground panic cannot leave a
/// background thread spinning forever.
const BG_MAX_CYCLES: u64 = 100_000_000;

/// Background: hot parent directory under continuous create/delete.
/// Foreground: a shared deep child path, concurrently read and written.
///
/// The foreground deliberately shares one ancestor chain so its ancestor read
/// locks contend with the background parent write lock - that interference is
/// the thing this scenario exists to expose.
pub struct HotParent {
    pub target_rate: u64,
    pub bg: Report,
    pub fg_status: Report,
    pub fg_create: Report,
}

pub fn run_hot_parent(
    fs: &Arc<MasterFilesystem>,
    threads: usize,
    ops: u64,
    target_rate: u64,
) -> HotParent {
    let barrier = Arc::new(Barrier::new(threads * 2 + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let fg_remaining = Arc::new(AtomicUsize::new(threads));
    let parent = hot::hot_parent_dir();
    let deep = hot::hot_deep_dir();
    let probe = format!("{deep}/probe");

    let mut bg_handles = Vec::with_capacity(threads);
    let mut fg_handles = Vec::with_capacity(threads);

    for thread in 0..threads {
        let fs = fs.clone();
        let barrier = barrier.clone();
        let stop = stop.clone();
        let parent = parent.clone();
        bg_handles.push(std::thread::spawn(move || {
            // Paths are recycled across cycles: create+delete on the same name
            // is a valid cycle and keeps the offered load steady.
            let paths: Vec<String> = (0..ops)
                .map(|seq| format!("{parent}/{thread}-{seq}"))
                .collect();
            let interval_ns = 1_000_000_000u64 / target_rate.max(1);
            let mut out = ThreadResult {
                ok: 0,
                err: 0,
                lat: Vec::with_capacity(ops as usize),
                first_err: None,
                elapsed_ns: 0,
            };
            barrier.wait();
            let started = Instant::now();
            let mut cycle = 0u64;
            while !stop.load(Ordering::Relaxed) && cycle < BG_MAX_CYCLES {
                // Throttle outside the timed region.
                if cycle.is_multiple_of(64) {
                    let expected = interval_ns.saturating_mul(cycle);
                    let actual = started.elapsed().as_nanos() as u64;
                    if expected > actual {
                        std::thread::sleep(Duration::from_nanos(expected - actual));
                    }
                }
                let path = &paths[(cycle as usize) % paths.len()];
                let t0 = Instant::now();
                let result = hot::hot_parent_cycle(fs.as_ref(), path);
                let ns = t0.elapsed().as_nanos() as u64;
                match result {
                    Ok(()) => {
                        out.ok += 1;
                        out.lat.push(ns);
                    }
                    Err(e) => {
                        out.err += 1;
                        if out.first_err.is_none() {
                            out.first_err = Some(e.to_string());
                        }
                    }
                }
                cycle += 1;
            }
            out.elapsed_ns = started.elapsed().as_nanos() as u64;
            out
        }));
    }

    for thread in 0..threads {
        let fs = fs.clone();
        let barrier = barrier.clone();
        let stop = stop.clone();
        let fg_remaining = fg_remaining.clone();
        let deep = deep.clone();
        let probe = probe.clone();
        fg_handles.push(std::thread::spawn(move || {
            let create_paths: Vec<String> = (0..ops)
                .map(|seq| format!("{deep}/c{thread}-{seq}"))
                .collect();
            let mut status = ThreadResult {
                ok: 0,
                err: 0,
                lat: Vec::with_capacity(ops as usize),
                first_err: None,
                elapsed_ns: 0,
            };
            let mut create = ThreadResult {
                ok: 0,
                err: 0,
                lat: Vec::with_capacity(ops as usize),
                first_err: None,
                elapsed_ns: 0,
            };
            barrier.wait();
            let fg_started = Instant::now();
            for seq in 0..ops {
                let t0 = Instant::now();
                let r = hot::hot_parent_status(fs.as_ref(), &probe);
                let ns = t0.elapsed().as_nanos() as u64;
                match r {
                    Ok(()) => {
                        status.ok += 1;
                        status.lat.push(ns);
                    }
                    Err(e) => {
                        status.err += 1;
                        if status.first_err.is_none() {
                            status.first_err = Some(e.to_string());
                        }
                    }
                }

                let t1 = Instant::now();
                let r = hot::hot_parent_create(fs.as_ref(), &create_paths[seq as usize]);
                let ns = t1.elapsed().as_nanos() as u64;
                match r {
                    Ok(()) => {
                        create.ok += 1;
                        create.lat.push(ns);
                    }
                    Err(e) => {
                        create.err += 1;
                        if create.first_err.is_none() {
                            create.first_err = Some(e.to_string());
                        }
                    }
                }
            }
            let fg_ns = fg_started.elapsed().as_nanos() as u64;
            status.elapsed_ns = fg_ns;
            create.elapsed_ns = fg_ns;
            // Last foreground thread out releases the background.
            if fg_remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                stop.store(true, Ordering::Release);
            }
            (status, create)
        }));
    }

    barrier.wait();
    let mut bg = ThreadResult {
        ok: 0,
        err: 0,
        lat: Vec::new(),
        first_err: None,
        elapsed_ns: 0,
    };
    let mut fg_status = ThreadResult {
        ok: 0,
        err: 0,
        lat: Vec::new(),
        first_err: None,
        elapsed_ns: 0,
    };
    let mut fg_create = ThreadResult {
        ok: 0,
        err: 0,
        lat: Vec::new(),
        first_err: None,
        elapsed_ns: 0,
    };
    for handle in bg_handles {
        let part = handle.join().expect("background worker panicked");
        merge_into(&mut bg, part);
    }
    for handle in fg_handles {
        let (status, create) = handle.join().expect("foreground worker panicked");
        merge_into(&mut fg_status, status);
        merge_into(&mut fg_create, create);
    }
    HotParent {
        target_rate,
        bg: finish(bg),
        fg_status: finish(fg_status),
        fg_create: finish(fg_create),
    }
}

fn finish(mut acc: ThreadResult) -> Report {
    acc.lat.sort_unstable();
    Report {
        ok: acc.ok,
        err: acc.err,
        lat: acc.lat,
        elapsed_s: acc.elapsed_ns as f64 / 1e9,
        first_err: acc.first_err,
    }
}

impl HotParent {
    /// Three machine-parsable lines: background, foreground read, foreground
    /// write. `rate_ratio` is achieved/target, so a background that fell short
    /// of its offered rate is visible next to the foreground numbers.
    pub fn lines(&self, ver: &str, threads: usize) -> [String; 3] {
        let total = self.bg.ok + self.bg.err;
        // `bg` is the merge of every background thread: `qps()` is the *total*
        // across all of them, while `target_rate` is per thread. Dividing by the
        // per-thread rate would inflate the ratio by `threads` (8x/32x), so the
        // denominator must be `threads * target_rate` (saturating: both factors
        // are caller-provided and a zero means "no background target").
        let ratio = rate_ratio(self.bg.qps(), threads, self.target_rate);
        [
            format!(
                "{} target_rate_per_thread={} rate_ratio={:.6} rate_ratio_raw={} ok_rate={:.6}",
                self.bg
                    .line(ver, "create_delete", Layout::Same, threads)
                    .replace("RESULT ", "RESULT bg=hot_parent "),
                self.target_rate,
                ratio,
                ratio,
                if total == 0 {
                    0.0
                } else {
                    self.bg.ok as f64 / total as f64
                }
            ),
            self.fg_status
                .line(ver, "file_status_deep", Layout::Same, threads)
                .replace("RESULT ", "RESULT bg=hot_parent "),
            self.fg_create
                .line(ver, "create_deep", Layout::Same, threads)
                .replace("RESULT ", "RESULT bg=hot_parent "),
        ]
    }
}

/// Achieved background rate relative to the *configured* rate.
///
/// `qps` is the merge of every background thread, so the denominator is
/// `threads * target_rate`; using the per-thread rate would scale the ratio by
/// `threads`. Saturating arithmetic keeps the degenerate `threads == 0` /
/// `target_rate == 0` inputs from dividing by zero or overflowing.
pub fn rate_ratio(qps: f64, threads: usize, target_rate: u64) -> f64 {
    let denom = (threads as u64).saturating_mul(target_rate);
    if denom == 0 || !qps.is_finite() {
        return 0.0;
    }
    qps / denom as f64
}

#[cfg(test)]
mod rate_ratio_tests {
    use super::rate_ratio;

    #[test]
    fn total_qps_is_split_across_threads() {
        // 1 thread: total == per-thread.
        assert!((rate_ratio(20_000.0, 1, 20_000) - 1.0).abs() < 1e-9);
        // 8 threads: a correct run reaches 8x the per-thread rate.
        assert!((rate_ratio(160_000.0, 8, 20_000) - 1.0).abs() < 1e-9);
        // 32 threads likewise; the old denominator would report 32.0 here.
        assert!((rate_ratio(640_000.0, 32, 20_000) - 1.0).abs() < 1e-9);
        // Half the target.
        assert!((rate_ratio(320_000.0, 32, 20_000) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn degenerate_inputs_do_not_panic() {
        assert_eq!(rate_ratio(1_000.0, 0, 20_000), 0.0);
        assert_eq!(rate_ratio(1_000.0, 8, 0), 0.0);
        assert_eq!(rate_ratio(f64::NAN, 8, 20_000), 0.0);
        assert_eq!(rate_ratio(f64::INFINITY, 8, 20_000), 0.0);
        // Absurd inputs must saturate rather than wrap: the denominator becomes
        // u64::MAX, so the result is a tiny positive number inside [0, 1] — the
        // point is "no panic, no overflow", not an exact value.
        let extreme = rate_ratio(1.0, usize::MAX, u64::MAX);
        assert!((0.0..=1.0).contains(&extreme), "got {extreme}");
    }
}
