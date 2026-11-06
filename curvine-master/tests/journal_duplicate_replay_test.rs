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
use curvine_master::master::Master;
use curvine_runtime::common::Utils;

// A timed-out proposal can be committed before the RPC is retried. This test
// isolates reordered follower replay; it does not simulate the RPC timeout.
#[test]
#[ignore = "duplicate proposal replay lacks a durable idempotency boundary"]
fn delayed_duplicate_delete_does_not_remove_recreated_path() {
    Master::init_test_metrics();
    let mut source_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    source_conf.change_test_meta_dir(format!("duplicate-source-{}", Utils::rand_str(8)));
    let mut target_conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    target_conf.change_test_meta_dir(format!("duplicate-target-{}", Utils::rand_str(8)));
    let source = JournalSystem::from_conf(&source_conf).unwrap();
    let target = JournalSystem::from_conf(&target_conf).unwrap();
    let fs = source.fs();
    fs.mkdir("/duplicate", false).unwrap();
    let mkdir = fs.fs_dir.read().take_entries();
    fs.delete("/duplicate", false).unwrap();
    let delete = fs.fs_dir.read().take_entries();
    fs.mkdir("/duplicate", false).unwrap();
    let recreate = fs.fs_dir.read().take_entries();
    assert_eq!((mkdir.len(), delete.len(), recreate.len()), (1, 1, 1));
    let loader = target.journal_loader();
    loader.apply_entry(mkdir[0].clone()).unwrap();
    loader.apply_entry(delete[0].clone()).unwrap();
    loader.apply_entry(recreate[0].clone()).unwrap();
    loader.apply_entry(delete[0].clone()).unwrap();
    assert!(target.fs().exists("/duplicate").unwrap());
    source.shutdown();
    target.shutdown();
}
