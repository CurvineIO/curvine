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

//! Metadata edge/read throughput benchmark at the `MasterFilesystem` API
//! level, following the design document's §10 scenario shape (same-parent /
//! multi-parent edge writes, exists, list_status) across thread counts.
//!
//! Journal is disabled (fs_only_for_test), so numbers measure the metadata
//! layer itself: path resolution, child-index locking, shard promotion and
//! the commit-gate protocol — not Raft. Run with:
//!
//! ```text
//! cargo test --release -p curvine-master --test metadata_edge_bench -- --nocapture
//! ```
//!
//! Every scenario prints one `RESULT` line per thread count so runs can be
//! diffed mechanically (the design doc's A/B protocol alternates two builds
//! and compares the printed medians).

use curvine_config::ClusterConf;
use curvine_model::RenameFlags;
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WINDOW_MS: u64 = 3_000;
const WARMUP_MS: u64 = 500;

struct Bench {
    fs: Arc<MasterFilesystem>,
}

impl Bench {
    fn new(name: &str) -> Self {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "metadata-edge-bench/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "metadata-edge-bench/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        Self {
            fs: Arc::new(JournalSystem::fs_only_for_test(&conf).unwrap()),
        }
    }

    fn run<F>(&self, label: &str, threads: usize, worker: F)
    where
        F: Fn(&MasterFilesystem, usize) + Send + Sync + 'static + Copy,
    {
        // Warmup on one thread to populate the namespace this scenario needs.
        let warm_start = Instant::now();
        while warm_start.elapsed() < Duration::from_millis(WARMUP_MS) {
            worker(&self.fs, 0);
        }

        let ops = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::with_capacity(threads);
        for thread in 0..threads {
            let fs = self.fs.clone();
            let ops = ops.clone();
            let stop = stop.clone();
            handles.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    worker(&fs, thread);
                    ops.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        let start = Instant::now();
        std::thread::sleep(Duration::from_millis(WINDOW_MS));
        stop.store(true, Ordering::Relaxed);
        let elapsed = start.elapsed();
        for handle in handles {
            handle.join().unwrap();
        }

        let total = ops.load(Ordering::Relaxed);
        let qps = total as f64 / elapsed.as_secs_f64();
        println!(
            "RESULT scenario={} threads={} ops={} qps={:.0}",
            label,
            threads,
            total,
            qps
        );
    }
}

use curvine_master::master::journal::JournalSystem;

fn child_name(prefix: &str, thread: usize, i: u64) -> String {
    format!("{prefix}-{thread}-{i}")
}

fn same_parent_create_delete(fs: &MasterFilesystem, thread: usize) {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let i = SEQ.fetch_add(1, Ordering::Relaxed);
    let name = child_name("bench", thread, i);
    let path = format!("/bench-same/{name}");
    fs.create(&path, false).unwrap();
    fs.delete(&path, false).unwrap();
}

fn multi_parent_create_delete(fs: &MasterFilesystem, thread: usize) {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let i = SEQ.fetch_add(1, Ordering::Relaxed);
    let name = child_name("bench", thread, i);
    let path = format!("/bench-multi/t{thread}/{name}");
    fs.create(&path, true).unwrap();
    fs.delete(&path, false).unwrap();
}

fn same_parent_rename(fs: &MasterFilesystem, thread: usize) {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let i = SEQ.fetch_add(1, Ordering::Relaxed);
    let src = format!("/bench-rename/{}-{}", thread, i);
    fs.create(&src, false).unwrap();
    let dst = format!("/bench-rename/{}-{}.moved", thread, i);
    fs.rename(&src, &dst, RenameFlags::empty()).unwrap();
    fs.delete(&dst, false).unwrap();
}

fn exists_read(fs: &MasterFilesystem, _thread: usize) {
    fs.exists("/bench-read/exists-target").unwrap();
}

fn list_status_2048(fs: &MasterFilesystem, _thread: usize) {
    let statuses = fs.list_status("/bench-list").unwrap();
    assert_eq!(statuses.len(), 2048);
}

#[test]
fn metadata_edge_bench() {
    let bench = Bench::new("edge");

    // Seed the directories every scenario touches.
    bench.fs.mkdir("/bench-same", true).unwrap();
    bench.fs.mkdir("/bench-multi", true).unwrap();
    bench.fs.mkdir("/bench-rename", true).unwrap();
    bench.fs.mkdir("/bench-read", true).unwrap();
    bench.fs.create("/bench-read/exists-target", false).unwrap();
    bench.fs.mkdir("/bench-list", true).unwrap();
    for i in 0..2048 {
        bench
            .fs
            .create(&format!("/bench-list/f{i}"), false)
            .unwrap();
    }

    let scenarios: &[(&str, usize, &(dyn Fn(&MasterFilesystem, usize) + Send + Sync))] = &[
        (
            "same_parent_create_delete",
            1,
            &same_parent_create_delete,
        ),
        (
            "same_parent_create_delete",
            8,
            &same_parent_create_delete,
        ),
        (
            "same_parent_create_delete",
            32,
            &same_parent_create_delete,
        ),
        (
            "multi_parent_create_delete",
            1,
            &multi_parent_create_delete,
        ),
        (
            "multi_parent_create_delete",
            8,
            &multi_parent_create_delete,
        ),
        (
            "multi_parent_create_delete",
            32,
            &multi_parent_create_delete,
        ),
        ("same_parent_rename", 1, &same_parent_rename),
        ("same_parent_rename", 8, &same_parent_rename),
        ("same_parent_rename", 32, &same_parent_rename),
        ("exists", 1, &exists_read),
        ("exists", 8, &exists_read),
        ("exists", 32, &exists_read),
        ("list_status_2048", 1, &list_status_2048),
        ("list_status_2048", 8, &list_status_2048),
    ];

    for (label, threads, worker) in scenarios {
        bench.run(label, *threads, *worker);
    }
}
