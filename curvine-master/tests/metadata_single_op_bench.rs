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

//! Single-operation latency with a bounded, stable directory size.
//! Run with `cargo test --release -p curvine-master --test metadata_single_op_bench -- --ignored --nocapture`.

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use std::time::{Duration, Instant};

#[test]
#[ignore = "performance benchmark"]
fn single_operation_latency() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("single-op-meta-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("single-op-journal-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.mkdir("/bench", true).unwrap();
    let paths: Vec<_> = (0..256).map(|i| format!("/bench/file-{i}")).collect();
    for path in &paths {
        fs.create(path, false).unwrap();
    }

    assert_eq!(fs.file_status("/bench").unwrap().children_num, 256);
    let mut create_ns = Vec::new();
    let mut delete_ns = Vec::new();
    let start = Instant::now();
    let warmup = Duration::from_millis(500);
    let duration = warmup + Duration::from_secs(3);
    let mut count = 0;
    while start.elapsed() < duration {
        let path = &paths[count % paths.len()];
        // 17 is coprime to the 256 paths, so sampling covers every name.
        let sampling = start.elapsed() >= warmup && count % 17 == 0;
        let started = sampling.then(Instant::now);
        fs.delete(path, false).unwrap();
        if let Some(started) = started {
            delete_ns.push(started.elapsed().as_nanos() as u64);
        }
        let started = sampling.then(Instant::now);
        fs.create(path, false).unwrap();
        if let Some(started) = started {
            create_ns.push(started.elapsed().as_nanos() as u64);
        }
        count += 1;
    }
    assert_eq!(fs.file_status("/bench").unwrap().children_num, 256);
    for (name, samples) in [("create", &mut create_ns), ("delete", &mut delete_ns)] {
        samples.sort_unstable();
        assert!(!samples.is_empty());
        let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100) - 1];
        println!(
            "RESULT operation={name} samples={} p50_ns={} p99_ns={}",
            samples.len(),
            percentile(50),
            percentile(99)
        );
    }
}
