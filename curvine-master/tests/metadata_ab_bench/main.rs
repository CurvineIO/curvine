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
mod harness;
mod ops;

use curvine_config::ClusterConf;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use harness::{Env, Layout};
use std::sync::Arc;

/// Thread counts required by the acceptance matrix.
const THREADS: [usize; 3] = [1, 8, 32];

fn new_fs() -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "metadata-ab-bench/meta-{}",
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "metadata-ab-bench/journal-{}",
        Utils::rand_str(6)
    ));
    JournalSystem::fs_only_for_test(&conf).expect("failed to build the metadata filesystem")
}

#[test]
#[ignore = "benchmark: run explicitly with --ignored"]
fn metadata_ab_bench() {
    let ops = harness::ops_per_thread();
    let ver = std::env::var("BENCH_VER").unwrap_or_else(|_| "unknown".to_string());
    println!("# metadata A/B bench ver={ver} ops_per_thread={ops} journal=disabled");

    let fs = Arc::new(new_fs());
    ops::register_worker(&fs);

    for case in ops::CASES {
        for layout in Layout::ALL {
            for threads in THREADS {
                let env = Arc::new(Env::new(case.name, layout, threads, ops));
                (case.prepare)(&fs, &env).expect("fixture preparation failed");
                let report = harness::run(&fs, &env, case.body);
                println!("{}", report.line(&ver, case.name, layout, threads));
                ops::cleanup(&fs, &env);
            }
        }
    }

    // Background-load scenario: hot parent under continuous create/delete
    // while foreground threads share one deep child path.
    for threads in THREADS {
        ops::prepare_hot_parent(&fs).expect("hot-parent fixture preparation failed");
        let hot = background::run_hot_parent(
            &fs,
            threads,
            ops,
            background::HOT_PARENT_TARGET_OPS_PER_SEC,
        );
        for line in hot.lines(&ver, threads) {
            println!("{line}");
        }
        ops::cleanup_hot_parent(&fs);
    }
}
