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

/// Thread counts required by the acceptance matrix.
const THREADS: [usize; 3] = [1, 8, 32];

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

    for case in ops::CASES {
        for layout in Layout::ALL {
            for threads in THREADS {
                let line = isolated(|fs| {
                    let env = Arc::new(Env::new(case.name, layout, threads, ops));
                    (case.prepare)(fs, &env).expect("fixture preparation failed");
                    let report = harness::run(fs, &env, case.args, case.body);
                    report.line(&ver, case.name, layout, threads)
                });
                println!("{line}");
            }
        }
    }

    // Background-load scenario: hot parent under continuous create/delete
    // while foreground threads share one deep child path.
    for threads in THREADS {
        let lines = isolated(|fs| {
            hot::prepare_hot_parent(fs).expect("hot-parent fixture preparation failed");
            let hot = background::run_hot_parent(
                fs,
                threads,
                ops,
                background::HOT_PARENT_TARGET_OPS_PER_SEC,
            );
            hot.lines(&ver, threads)
        });
        for line in lines {
            println!("{line}");
        }
    }
}
