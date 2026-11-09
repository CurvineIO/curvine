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

//! `create_delete` measures a mkdir/delete pair, not a single operation.
//! Samples are capped per thread, so percentiles can overrepresent early work.

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

#[test]
#[ignore = "performance benchmark"]
fn mkdir_compare() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("mkdir-compare-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("mkdir-compare-j-{}", Utils::rand_str(6)));
    let fs = Arc::new(JournalSystem::fs_only_for_test(&conf).unwrap());
    fs.mkdir("/parent/existing", true).unwrap();
    for mode in ["existing", "create_delete"] {
        for threads in [1, 8, 32] {
            let barrier = Arc::new(Barrier::new(threads + 1));
            let stop = Arc::new(AtomicBool::new(false));
            let mut workers = Vec::new();
            for id in 0..threads {
                let fs = fs.clone();
                let barrier = barrier.clone();
                let stop = stop.clone();
                workers.push(std::thread::spawn(move || {
                    let path = format!("/parent/entry{id}");
                    let mut ops = 0u64;
                    let mut samples = Vec::new();
                    barrier.wait();
                    while !stop.load(Ordering::Relaxed) {
                        let started = (ops % 64 == 0).then(Instant::now);
                        if mode == "existing" {
                            fs.mkdir("/parent/existing", true).unwrap();
                        } else {
                            fs.mkdir(&path, false).unwrap();
                            fs.delete(&path, false).unwrap();
                        }
                        if let Some(started) = started {
                            if samples.len() < 10_000 {
                                samples.push(started.elapsed().as_nanos() as u64);
                            }
                        }
                        ops += 1;
                    }
                    (ops, samples)
                }));
            }
            barrier.wait();
            let start = Instant::now();
            std::thread::sleep(Duration::from_millis(2000));
            stop.store(true, Ordering::Relaxed);
            let mut ops = 0;
            let mut samples = Vec::new();
            for worker in workers {
                let (count, latencies) = worker.join().unwrap();
                ops += count;
                samples.extend(latencies);
            }
            let secs = start.elapsed().as_secs_f64();
            samples.sort_unstable();
            println!(
                "RESULT mode={mode} threads={threads} qps={:.0} p50_ns={} p99_ns={} samples={}",
                ops as f64 / secs,
                samples[samples.len() / 2],
                samples[samples.len() * 99 / 100],
                samples.len()
            );
        }
    }
}
