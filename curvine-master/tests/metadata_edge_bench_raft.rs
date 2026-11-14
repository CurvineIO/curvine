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

//! Metadata write throughput with Journal/Raft enabled: a single-member Raft
//! group runs in-process, so every mutation pays one propose + commit + apply
//! round trip. This is the end-to-end counterpart to `metadata_edge_bench`
//! (which disables Journal/Raft to isolate the metadata layer).
//!
//! ```text
//! cargo test --release -p curvine-master --test metadata_edge_bench_raft -- --nocapture
//! ```

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_raft::raft::RaftPeer;
use curvine_runtime::common::Utils;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const WINDOW_MS: u64 = 3_000;
const WARMUP_MS: u64 = 500;

struct Bench {
    fs: Arc<curvine_master::master::fs::MasterFilesystem>,
}

impl Bench {
    fn new(name: &str) -> Self {
        Master::init_test_metrics();
        let mut conf = ClusterConf::default();
        // Keep the configured default batching: the sender coalesces entries
        // up to writer_flush_batch_size or the flush interval.
        conf.journal.raft_tick_interval_ms = 100;
        conf.journal.raft_check_quorum = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "metadata-edge-bench-raft/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "metadata-edge-bench-raft/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));

        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        conf.journal.hostname = "127.0.0.1".into();
        conf.journal.rpc_port = port;
        conf.journal.journal_addrs = vec![RaftPeer::new(1, "127.0.0.1", port)];

        let js = JournalSystem::from_conf(&conf).unwrap();
        let fs = Arc::new(js.fs());
        let monitor = js.master_monitor();
        js.start_blocking().unwrap();
        // Wait for leader election so writes do not race the election.
        let mut wait = 30 * 1000;
        while wait > 0 {
            if monitor.is_active() {
                break;
            }
            let spend = curvine_runtime::common::TimeSpent::new();
            std::thread::sleep(Duration::from_millis(100));
            wait -= spend.used_ms();
        }
        assert!(monitor.is_active(), "leader must emerge");
        Self { fs }
    }

    fn run<F>(&self, label: &str, threads: usize, worker: F)
    where
        F: Fn(&curvine_master::master::fs::MasterFilesystem, usize) + Send + Sync + Copy + 'static,
    {
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
                start_barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    worker(&fs, thread);
                    ops += 1;
                }
                ops
            }));
        }

        let start = Instant::now();
        start_barrier.wait();
        std::thread::sleep(Duration::from_millis(WINDOW_MS));
        stop.store(true, Ordering::Relaxed);
        let mut total = 0_u64;
        for handle in handles {
            total += handle.join().unwrap();
        }
        let elapsed = start.elapsed();
        let qps = total as f64 / elapsed.as_secs_f64();
        println!(
            "RESULT scenario={} threads={} ops={} qps={:.0}",
            label, threads, total, qps
        );
    }
}

fn same_parent_create_delete(fs: &curvine_master::master::fs::MasterFilesystem, thread: usize) {
    let name = format!("/bench-raft-same/f{thread}");
    fs.mkdir(&name, true).unwrap();
    fs.delete(&name, false).unwrap();
}

fn multi_parent_create_delete(fs: &curvine_master::master::fs::MasterFilesystem, thread: usize) {
    let name = format!("/bench-raft-multi/d{}/f{}", thread % 8, thread);
    fs.mkdir(&name, true).unwrap();
    fs.delete(&name, false).unwrap();
}

#[test]
fn metadata_edge_bench_raft() {
    let bench = Bench::new("raft");
    bench.fs.mkdir("/bench-raft-same", true).unwrap();
    bench.fs.mkdir("/bench-raft-multi", true).unwrap();
    for d in 0..8 {
        bench
            .fs
            .mkdir(format!("/bench-raft-multi/d{d}"), false)
            .unwrap();
    }

    type Scenario<'a> = (
        &'a str,
        usize,
        fn(&curvine_master::master::fs::MasterFilesystem, usize),
    );
    let scenarios: Vec<Scenario> = vec![
        ("same_parent_create_delete", 1, same_parent_create_delete),
        ("same_parent_create_delete", 8, same_parent_create_delete),
        ("multi_parent_create_delete", 1, multi_parent_create_delete),
        ("multi_parent_create_delete", 8, multi_parent_create_delete),
    ];
    for (label, threads, worker) in scenarios {
        bench.run(label, threads, worker);
    }
}
