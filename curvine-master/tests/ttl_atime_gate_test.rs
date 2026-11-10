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
use curvine_master::master::meta::inode::InodePath;
use curvine_master::Master;
use curvine_model::{
    CreateFileOpts, OpenFlags, SetAttrOpts, StoragePolicy, TtlAction, TtlType,
    INTERNAL_TTL_TYPE_XATTR,
};

#[test]
fn atime_refresh_does_not_touch_a_replacement_inode() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "atime-replacement-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let original = fs.create("/file", false).unwrap();
    fs.delete("/file", false).unwrap();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1000,
        ttl_action: TtlAction::Free,
        ..Default::default()
    };
    opts.x_attr.insert(
        INTERNAL_TTL_TYPE_XATTR.to_string(),
        TtlType::Atime.to_xattr_value(),
    );
    let replacement = fs
        .create_with_opts("/file", opts, OpenFlags::new_create())
        .unwrap();
    assert_ne!(original.id, replacement.id);
    fs.set_attr(
        "/file",
        SetAttrOpts {
            atime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    {
        let dir = fs.fs_dir.write();
        let path = InodePath::resolve(dir.root_node(), "/file", dir.store_ref()).unwrap();
        let epoch = dir.store_ref().scan_control().cancel_epoch();
        assert!(!dir.touch_atime_for_ttl(&path, original.id, epoch).unwrap());
        let restore = dir
            .store_ref()
            .begin_restore(std::time::Duration::from_secs(1))
            .unwrap();
        drop(restore);
        assert!(!dir
            .touch_atime_for_ttl(&path, replacement.id, epoch)
            .unwrap());
        assert_eq!(
            dir.store_ref()
                .get_inode(replacement.id, None)
                .unwrap()
                .unwrap()
                .atime(),
            1
        );
        let current_epoch = dir.store_ref().scan_control().cancel_epoch();
        assert!(dir
            .touch_atime_for_ttl(&path, replacement.id, current_epoch)
            .unwrap());
    }
    assert!(fs.file_status("/file").unwrap().atime > 1);
    journal.shutdown();
}

#[test]
fn atime_refresh_respects_snapshot_fence() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "atime-gate-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1000,
        ttl_action: TtlAction::Free,
        ..Default::default()
    };
    opts.x_attr.insert(
        INTERNAL_TTL_TYPE_XATTR.to_string(),
        TtlType::Atime.to_xattr_value(),
    );
    fs.create_with_opts("/file", opts, OpenFlags::new_create())
        .unwrap();
    fs.set_attr(
        "/file",
        SetAttrOpts {
            atime: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    let gate = fs.metadata_commit_gate();
    let fence = gate.close_owned();
    fs.get_block_locations("/file").unwrap();
    assert_eq!(fs.file_status("/file").unwrap().atime, 1);
    drop(fence);
    fs.get_block_locations("/file").unwrap();
    assert!(fs.file_status("/file").unwrap().atime > 1);
    journal.shutdown();
}
