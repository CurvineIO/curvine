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
use curvine_master::master::space::{DirSpaceManager, DirSpaceQueryError, ScanOutcome};
use curvine_master::master::{Master, MountManager};
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;
use std::sync::Arc;

#[test]
fn dangling_tracked_directory_is_a_failed_scan() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("dir-space-corrupt-{}", Utils::rand_str(8)));
    conf.master.dir_space.enabled = true;
    conf.master.dir_space.tracked_paths = vec!["/point".into()];
    conf.master.init().unwrap();
    let journal = JournalSystem::from_conf(&conf).unwrap();
    journal
        .master_monitor()
        .set_role_for_test(RoleState::Leader);
    let fs = journal.fs();
    fs.mkdir("/point", false).unwrap();
    let id = fs.file_status("/point").unwrap().id;
    let manager = DirSpaceManager::new(
        fs.clone(),
        Arc::new(MountManager::new(fs.clone())),
        Arc::new(conf.master.clone()),
    );
    assert_eq!(manager.try_scan_tracked_points(0).0, ScanOutcome::Success);
    {
        let dir = fs.fs_dir.view().unwrap();
        let mut batch = dir.store_ref().store().new_batch();
        batch.delete_inode(id).unwrap();
        batch.commit().unwrap();
    }
    assert!(matches!(
        manager.query("/point"),
        Err(DirSpaceQueryError::Storage(_))
    ));
    let (outcome, _, complete) = manager.try_scan_tracked_points(0);
    assert_eq!(outcome, ScanOutcome::Failed);
    assert!(!complete);
    journal.shutdown();
}
