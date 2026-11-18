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

//! A/B metadata benchmark at the `MasterFilesystem` API level.
//!
//! Purpose: give the baseline (`bff4e7dc5`) and the sharded-lock candidate
//! branch one tool that compiles unchanged on both, times every operation
//! independently, and prints machine-parsable results for a strict per-cell
//! comparison (QPS / p50 / p99 per operation x thread count x parent layout).
//!
//! Scope and known limits (state these whenever numbers are quoted):
//!
//! * Journal is disabled (`fs_only_for_test`), so this measures the metadata
//!   layer only: path resolution, locks, planning and the store write path.
//!   It is not a Raft end-to-end number.
//! * WAL is a separate switch hardcoded in `RocksInodeStore::new`
//!   (`set_disable_wal(true)` on the baseline, `false` on the candidate).
//!   A fair A/B therefore needs a baseline build with only that line flipped;
//!   see `WAL-ON-CONTROL.patch` next to this directory.
//! * `close` has no `MasterFilesystem` entry point on the baseline. The real
//!   baseline close path is `complete_file(only_flush = false)`, which is what
//!   the `complete_file` case measures; the candidate's separate `close_file`
//!   entry point is deliberately not in this matrix because the baseline has
//!   no counterpart to compare against.
//!
//! Isolation: every cell gets its own `MasterFilesystem` on its own meta
//! directory, which is deleted afterwards. Sharing one store across cells
//! would let earlier cells accumulate tombstones and compaction debt, and the
//! two builds write-amplify differently, so that debt would bias them
//! asymmetrically.
//!
//! Run (release is mandatory, debug numbers are meaningless):
//!
//! ```text
//! BENCH_VER=baseline cargo test --release -p curvine-master \
//!     --test metadata_ab_bench -- --ignored --nocapture
//! ```
//!
//! `BENCH_OPS` shrinks the workload for smoke runs only. A real A/B must not
//! set it: both builds have to run the identical, default workload.

mod background;
mod fixtures;
mod harness;
mod hot;
mod mixed;
mod ops;

#[cfg(test)]
#[path = "tests/rate_ratio_test.rs"]
mod rate_ratio_test;

#[cfg(test)]
#[path = "tests/close_id_arg_test.rs"]
mod close_id_arg_test;

#[cfg(test)]
#[path = "tests/prep_failure_test.rs"]
mod prep_failure_test;

use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use harness::{Env, Layout};
use std::sync::Arc;
use std::time::Duration;

/// Thread counts required by the acceptance matrix.
/// Thread counts exercised. 1/8/16 are the formal cells; 32 is recorded as a
/// reference oversubscription row and is excluded from the pass/fail judgement.
const THREADS: [usize; 4] = [1, 8, 16, 32];

/// `BENCH_THREADS` replaces the matrix with an explicit list, so a cell between
/// the fixed ones (4, say) can be measured without editing the matrix.
fn thread_counts() -> Vec<usize> {
    match std::env::var("BENCH_THREADS") {
        Ok(v) if !v.trim().is_empty() => v
            .split(',')
            .map(|x| {
                x.trim()
                    .parse()
                    .expect("BENCH_THREADS must be comma-separated counts")
            })
            .collect(),
        _ => THREADS.to_vec(),
    }
}

/// Per-cell watchdog. Some cells can block in the kernel (block-layer IO
/// throttle / writeback back-pressure on the trash purge path) instead of
/// returning, and the whole matrix then never finishes. A cell that does not
/// finish within this budget is reported as `flag=TIMEOUT` and the run moves
/// on, so one environment-bound cell cannot hide the other 131.
///
/// The abandoned worker thread is deliberately not joined: it may still be
/// parked in the kernel, and there is no safe cancellation for that. Its
/// filesystem lives under its own directory, so the next cell is unaffected.
const CELL_WATCHDOG: Duration = Duration::from_secs(60);

/// Narrows a diagnostic run to the cells named by env vars, so checking one
/// cell never costs a full 132-cell sweep. Unset (or empty) means "all", which
/// is what a formal A/A or A/B uses.
fn selected(var: &str, val: &str) -> bool {
    match std::env::var(var) {
        Ok(v) if !v.trim().is_empty() => v.split(',').any(|x| x.trim() == val),
        _ => true,
    }
}

/// Fresh filesystem on its own meta directory. Returns the directories too, so
/// the caller can delete them once the store is closed.
fn new_fs() -> (MasterFilesystem, Vec<String>) {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    let meta_dir = Utils::test_sub_dir(format!("metadata-ab-bench/meta-{}", Utils::rand_str(6)));
    let journal_dir =
        Utils::test_sub_dir(format!("metadata-ab-bench/journal-{}", Utils::rand_str(6)));
    conf.master.meta_dir = meta_dir.clone();
    conf.journal.journal_dir = journal_dir.clone();
    let fs =
        JournalSystem::fs_only_for_test(&conf).expect("failed to build the metadata filesystem");
    (fs, vec![meta_dir, journal_dir])
}

/// Runs `body` against a private filesystem, then closes the store and deletes
/// its directories. Store setup and teardown are outside the timed region.
fn isolated<R>(body: impl FnOnce(&Arc<MasterFilesystem>) -> R) -> R {
    let (fs, dirs) = new_fs();
    let fs = Arc::new(fs);
    ops::register_worker(&fs);
    let out = body(&fs);
    drop(fs);
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
    out
}

#[test]
#[ignore = "benchmark: run explicitly with --ignored"]
fn metadata_ab_bench() {
    let ops = harness::ops_per_thread();
    let ver = std::env::var("BENCH_VER").unwrap_or_else(|_| "unknown".to_string());
    println!("# metadata A/B bench ver={ver} ops_per_thread={ops} journal=disabled");

    let hot_only = std::env::var("BENCH_ONLY_HOT").is_ok_and(|v| !v.trim().is_empty() && v != "0");
    // Opt-in mixed read/write scenario (see `mixed.rs`): when set it replaces the
    // ordinary cells and the hot-parent scenario, so existing runs are unchanged.
    let mixed_only = std::env::var("BENCH_MIXED").is_ok_and(|v| !v.trim().is_empty() && v != "0");
    if mixed_only {
        run_mixed(&ver, ops);
        return;
    }
    for case in ops::CASES {
        if hot_only || !selected("BENCH_ONLY_CASES", case.name) {
            continue;
        }
        for layout in Layout::ALL {
            if !selected("BENCH_ONLY_LAYOUTS", layout.tag()) {
                continue;
            }
            for threads in thread_counts() {
                if !selected("BENCH_ONLY_THREADS", &threads.to_string()) {
                    continue;
                }
                let tag = ver.clone();
                let (tx, rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let line = isolated(move |fs| {
                        let env = Arc::new(Env::new(case.name, layout, threads, ops));
                        (case.prepare)(fs, &env).expect("fixture preparation failed");
                        let report = harness::run(fs, &env, case.args, case.body, case.recycle);
                        report.line(&tag, case.name, layout, threads)
                    });
                    let _ = tx.send(line);
                });
                match rx.recv_timeout(CELL_WATCHDOG) {
                    Ok(line) => println!("{line}"),
                    Err(_) => {
                        println!(
                            "RESULT ver={ver} op={} layout={} threads={threads} flag=TIMEOUT",
                            case.name,
                            layout.tag()
                        );
                        eprintln!(
                            "FATAL: {}/{}/{threads} exceeded {}s; ending the run so no \
                             worker thread outlives it. Re-run from this cell with BENCH_ONLY_*.",
                            case.name,
                            layout.tag(),
                            CELL_WATCHDOG.as_secs()
                        );
                        std::process::exit(3);
                    }
                }
            }
        }
    }

    // Background-load scenario: hot parent under continuous create/delete
    // while foreground threads share one deep child path.
    let hot_filtered =
        !hot_only && std::env::var("BENCH_ONLY_CASES").is_ok_and(|v| !v.trim().is_empty());
    for &threads in if hot_filtered {
        &THREADS[..0]
    } else {
        &THREADS[..]
    } {
        // Honour the thread filter for the hot segment too: a diagnostic run
        // pins one thread count and runs it under its own taskset, so letting
        // all four hot cells through would mix differently-pinned numbers into
        // the same (op, layout, threads) key.
        if !selected("BENCH_ONLY_THREADS", &threads.to_string()) {
            continue;
        }
        let tag = ver.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        // Shared stop flag: on watchdog expiry we signal the workers to stop
        // themselves instead of leaking threads that keep issuing create/delete
        // (that leak is what drove load to five digits once).
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_worker = std::sync::Arc::clone(&stop);
        std::thread::spawn(move || {
            let lines = isolated(move |fs| {
                hot::prepare_hot_parent(fs).expect("hot-parent fixture preparation failed");
                let hot = background::run_hot_parent(
                    fs,
                    threads,
                    ops,
                    background::HOT_PARENT_TARGET_OPS_PER_SEC,
                    stop_worker,
                );
                hot.lines(&tag, threads)
            });
            let _ = tx.send(lines);
        });
        match rx.recv_timeout(CELL_WATCHDOG) {
            Ok(lines) => {
                for line in lines {
                    println!("{line}");
                }
            }
            Err(_) => {
                stop.store(true, std::sync::atomic::Ordering::Release);
                println!(
                    "RESULT ver={ver} op=hot_parent layout=same threads={threads} flag=TIMEOUT"
                );
                eprintln!(
                    "FATAL: hot_parent/{threads} exceeded {}s; stop signalled. \
                     Re-run this cell alone with BENCH_ONLY_*.",
                    CELL_WATCHDOG.as_secs()
                );
                std::process::exit(3);
            }
        }
    }
}

/// One mixed cell per (layout, readers/writers split). Each runs on a private
/// filesystem under the same watchdog as the ordinary cells.
fn run_mixed(ver: &str, ops: u64) {
    for layout in Layout::ALL {
        if !selected("BENCH_ONLY_LAYOUTS", layout.tag()) {
            continue;
        }
        for (readers, writers) in mixed::SPLITS {
            let split = format!("r{readers}w{writers}");
            if !selected("BENCH_ONLY_SPLITS", &split) {
                continue;
            }
            let tag = ver.to_string();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let lines = isolated(move |fs| {
                    mixed::prepare(fs, layout, readers + writers)
                        .expect("mixed fixture preparation failed");
                    mixed::run(fs, layout, readers, writers, ops).lines(&tag, layout)
                });
                let _ = tx.send(lines);
            });
            match rx.recv_timeout(CELL_WATCHDOG) {
                Ok(lines) => {
                    for line in lines {
                        println!("{line}");
                    }
                }
                Err(_) => {
                    println!(
                        "RESULT ver={ver} op=mixed_{split} layout={} flag=TIMEOUT",
                        layout.tag()
                    );
                    eprintln!(
                        "FATAL: mixed/{split}/{} exceeded {}s; ending the run.",
                        layout.tag(),
                        CELL_WATCHDOG.as_secs()
                    );
                    std::process::exit(3);
                }
            }
        }
    }
}
