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
//! multi-parent edge writes, exists, file_status and list_status) across
//! thread counts. The final status experiment uses a separate database with
//! persisted counts enabled explicitly; production defaults remain unchanged.
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
use curvine_master::master::fs::MasterFilesystem;
use curvine_master::Master;
use curvine_model::{ListOptions, RenameFlags};
use curvine_runtime::common::Utils;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const WINDOW_MS: u64 = 3_000;
const WARMUP_MS: u64 = 500;
// Keep the most recent samples per thread; capped high-throughput threads
// have equal weight, so these percentiles are diagnostic rather than exact.
const SAMPLE_EVERY: u64 = 16;
const SAMPLES_PER_THREAD: usize = 4096;

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

        let start_barrier = Arc::new(Barrier::new(threads + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(threads);
        for thread in 0..threads {
            let fs = self.fs.clone();
            let start_barrier = start_barrier.clone();
            let stop = stop.clone();
            handles.push(std::thread::spawn(move || {
                let mut ops = 0_u64;
                let mut samples = Vec::with_capacity(SAMPLES_PER_THREAD);
                start_barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    if ops.is_multiple_of(SAMPLE_EVERY) {
                        let started = Instant::now();
                        worker(&fs, thread);
                        let latency = started.elapsed().as_nanos() as u64;
                        let slot = ((ops / SAMPLE_EVERY) as usize) % SAMPLES_PER_THREAD;
                        if samples.len() < SAMPLES_PER_THREAD {
                            samples.push(latency);
                        } else {
                            samples[slot] = latency;
                        }
                    } else {
                        worker(&fs, thread);
                    }
                    ops += 1;
                }
                (ops, samples)
            }));
        }

        let start = Instant::now();
        start_barrier.wait();
        std::thread::sleep(Duration::from_millis(WINDOW_MS));
        stop.store(true, Ordering::Relaxed);
        let mut total = 0_u64;
        let mut samples = Vec::new();
        for handle in handles {
            let (ops, latencies) = handle.join().unwrap();
            total += ops;
            samples.extend(latencies);
        }
        let elapsed = start.elapsed();
        let qps = total as f64 / elapsed.as_secs_f64();
        samples.sort_unstable();
        let percentile = |percent: usize| {
            samples
                .get((samples.len() * percent).div_ceil(100).saturating_sub(1))
                .copied()
                .unwrap_or(0)
        };
        println!(
            "RESULT scenario={} threads={} ops={} qps={:.0} latency_samples={} p50_ns={} p99_ns={}",
            label,
            threads,
            total,
            qps,
            samples.len(),
            percentile(50),
            percentile(99)
        );
    }
}

use curvine_master::master::journal::JournalSystem;

fn child_name(prefix: &str, thread: usize, i: u64) -> String {
    format!("{prefix}-{thread}-{i}")
}

thread_local! {
    static SEQ: Cell<u64> = const { Cell::new(0) };
}

fn next_id() -> u64 {
    SEQ.with(|seq| {
        let id = seq.get();
        seq.set(id + 1);
        id
    })
}

fn same_parent_create_delete(fs: &MasterFilesystem, thread: usize) {
    let i = next_id();
    let name = child_name("bench", thread, i);
    let path = format!("/bench-same/{name}");
    fs.create(&path, false).unwrap();
    fs.delete(&path, false).unwrap();
}

fn multi_parent_create_delete(fs: &MasterFilesystem, thread: usize) {
    let i = next_id();
    let name = child_name("bench", thread, i);
    let path = format!("/bench-multi/t{thread}/{name}");
    fs.create(&path, true).unwrap();
    fs.delete(&path, false).unwrap();
}

fn same_parent_rename(fs: &MasterFilesystem, thread: usize) {
    let i = next_id();
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

fn list_page_2048(fs: &MasterFilesystem, _thread: usize) {
    let statuses = fs
        .list_options(
            "/bench-list",
            ListOptions {
                limit: Some(64),
                start_after: Some("f1800".to_string()),
            },
        )
        .unwrap();
    assert_eq!(statuses.len(), 64);
}

fn file_status_2048(fs: &MasterFilesystem, _thread: usize) {
    let status = fs.file_status("/bench-list").unwrap();
    assert_eq!(status.children_num, 2048);
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
        bench.fs.create(format!("/bench-list/f{i}"), false).unwrap();
    }

    type Scenario = (
        &'static str,
        usize,
        &'static (dyn Fn(&MasterFilesystem, usize) + Send + Sync),
    );
    let scenarios: &[Scenario] = &[
        ("same_parent_create_delete", 1, &same_parent_create_delete),
        ("same_parent_create_delete", 8, &same_parent_create_delete),
        ("same_parent_create_delete", 32, &same_parent_create_delete),
        ("multi_parent_create_delete", 1, &multi_parent_create_delete),
        ("multi_parent_create_delete", 8, &multi_parent_create_delete),
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
        ("file_status_2048", 1, &file_status_2048),
        ("file_status_2048", 8, &file_status_2048),
        ("list_status_2048", 1, &list_status_2048),
        ("list_status_2048", 8, &list_status_2048),
        ("list_page_2048", 1, &list_page_2048),
        ("list_page_2048", 8, &list_page_2048),
    ];

    for (label, threads, worker) in scenarios {
        bench.run(label, *threads, *worker);
    }

    // This is a controlled format experiment, not production enablement.
    // The test binary has a single test; reset the process-wide switch even
    // if a setup or assertion fails.
    struct ResetFormat;
    impl Drop for ResetFormat {
        fn drop(&mut self) {
            curvine_model::persist_children_count(false);
        }
    }
    let _reset = ResetFormat;
    curvine_model::persist_children_count(true);
    let persisted = Bench::new("persisted-count");
    persisted.fs.mkdir("/bench-list", true).unwrap();
    for i in 0..2048 {
        persisted
            .fs
            .create(format!("/bench-list/f{i}"), false)
            .unwrap();
    }
    let id = persisted.fs.file_status("/bench-list").unwrap().id;
    assert_eq!(
        persisted
            .fs
            .fs_dir
            .view()
            .unwrap()
            .store_ref()
            .store()
            .get_directory_attributes(id)
            .unwrap()
            .unwrap()
            .children(),
        Some(2048)
    );
    persisted.run("file_status_2048_persisted", 1, file_status_2048);
    persisted.run("file_status_2048_persisted", 8, file_status_2048);
}
