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

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::InodeId;
use curvine_master::Master;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};

#[test]
fn concurrent_watermark_advances_do_not_reuse_ids() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "watermarks-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let ids = Arc::new(InodeId::new());
    ids.advance_to(2000);
    ids.advance_to(1);
    assert_eq!(ids.current(), 2000);
    {
        let dir = fs.fs_dir.read();
        dir.update_op_id(25);
        dir.update_op_id(1);
        assert_eq!(dir.next_op_id(), 26);
        dir.update_last_inode_id(2000).unwrap();
        dir.update_last_inode_id(1).unwrap();
        assert_eq!(dir.last_inode_id(), 2000);
    }
    let start = Arc::new(Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let dir = fs.fs_dir.clone();
            let ids = ids.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                let mut op_ids = Vec::new();
                let mut inode_ids = Vec::new();
                for i in 0..1000 {
                    let value = (worker * 1000 + i + 10_000) as u64;
                    ids.advance_to(value as i64);
                    inode_ids.push(ids.next().unwrap());
                    let guard = dir.read();
                    guard.update_op_id(value);
                    guard.update_last_inode_id(value as i64).unwrap();
                    op_ids.push(guard.next_op_id());
                }
                (op_ids, inode_ids)
            })
        })
        .collect();
    let mut ops = HashSet::new();
    let mut inodes = HashSet::new();
    for worker in workers {
        let (op_ids, inode_ids) = worker.join().unwrap();
        for id in op_ids {
            assert!(ops.insert(id), "duplicate op id {id}");
        }
        for id in inode_ids {
            assert!(inodes.insert(id), "duplicate inode id {id}");
        }
    }
    let dir = fs.fs_dir.read();
    assert!(dir.next_op_id() > 17_999);
    assert!(dir.last_inode_id() >= 17_999);
    drop(dir);
    assert!(ids.current() >= 17_999);
    journal.shutdown();
}
