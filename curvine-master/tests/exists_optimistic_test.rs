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

use curvine_master::master::fs::MasterFilesystem;
use curvine_master::Master;
use curvine_runtime::common::Utils;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn test_fs(name: &str) -> Arc<MasterFilesystem> {
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("exists-optimistic-{}-{}", name, Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "exists-optimistic-j-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    Arc::new(curvine_master::master::journal::JournalSystem::fs_only_for_test(&conf).unwrap())
}

#[test]
fn exists_never_sees_partial_namespace_state_under_concurrent_writes() {
    let fs = test_fs("linearizable");
    fs.mkdir("/dir", true).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for thread in 0..4 {
        let fs = fs.clone();
        let stop = stop.clone();
        writers.push(std::thread::spawn(move || {
            let mut i: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let path = format!("/dir/w{thread}-{i}");
                fs.create(&path, false).unwrap();
                assert!(fs.exists(&path).unwrap(), "{path} must exist after create");
                fs.delete(&path, false).unwrap();
                assert!(
                    !fs.exists(&path).unwrap(),
                    "{path} must be gone after delete"
                );
                i += 1;
            }
        }));
    }

    let mut reads: u64 = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        // A path is either fully present (inode resolvable) or absent; the
        // optimistic walk must never return a torn intermediate answer.
        for thread in 0..4 {
            for i in 0..4 {
                let path = format!("/dir/w{thread}-{i}");
                let _ = fs.exists(&path).unwrap();
            }
        }
        reads += 16;
    }
    stop.store(true, Ordering::Relaxed);
    for writer in writers {
        writer.join().unwrap();
    }
    assert!(reads > 0);
}
