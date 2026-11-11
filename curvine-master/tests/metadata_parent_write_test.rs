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
use curvine_master::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use curvine_master::master::meta::inode::InodePath;
use curvine_master::Master;
use curvine_model::SetAttrOpts;
use curvine_runtime::common::Utils;

#[test]
fn unchanged_parent_preserves_persisted_child_count() {
    const CHILD_ENV: &str = "CURVINE_PARENT_COUNT_TEST_CHILD";
    if std::env::var_os(CHILD_ENV).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .env(CHILD_ENV, "1")
            .args([
                "--exact",
                "unchanged_parent_preserves_persisted_child_count",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    curvine_model::persist_children_count(true);
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("parent-v4-{}", Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!("parent-v4-j-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let parent = fs.mkdir("/parent", false).unwrap();
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(4_000_000_000_000),
            ..Default::default()
        },
    )
    .unwrap();
    fs.create("/parent/a", false).unwrap();
    fs.create("/parent/b", false).unwrap();
    fs.delete("/parent/a", false).unwrap();
    let attributes = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .store()
        .get_directory_attributes(parent.id)
        .unwrap()
        .unwrap();
    assert_eq!(attributes.children(), Some(1));
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/parent").unwrap().children_num, 1);
    assert!(fs.exists("/parent/b").unwrap());
}

#[test]
fn unchanged_legacy_parent_initializes_attributes_from_inode() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("parent-legacy-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("parent-legacy-j-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    let parent = fs.mkdir("/parent", false).unwrap();
    let future = 4_000_000_000_000;
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(future),
            ..Default::default()
        },
    )
    .unwrap();
    {
        let shared = fs.fs_dir.clone();
        let guard = shared.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        // Simulate a legacy directory: the dir-attrs row is gone, and the
        // live node's persisted flag must be invalidated so the next delta
        // re-carries its base and rebuilds the record.
        let path = InodePath::resolve(guard.root_node(), "/parent", guard.store_ref()).unwrap();
        let node = path.get_last_dir_node().expect("live /parent node");
        node.invalidate_persisted();
        let mut batch = guard.store_ref().new_batch();
        batch.delete_directory_attributes(parent.id).unwrap();
        batch.commit().unwrap();
    }
    fs.restore_from_rocksdb().unwrap();
    fs.create("/parent/file", false).unwrap();
    let snapshot = fs.file_status_by_id("/parent", Some(parent.id)).unwrap();
    assert_eq!(
        (snapshot.mtime, snapshot.nlink, snapshot.children_num),
        (future, parent.nlink, 1)
    );
    fs.restore_from_rocksdb().unwrap();
    let restored = fs.file_status("/parent").unwrap();
    assert_eq!(
        (restored.mtime, restored.nlink, restored.children_num),
        (future, parent.nlink, 1)
    );
    fs.delete("/parent/file", false).unwrap();
    fs.restore_from_rocksdb().unwrap();
    let restored = fs.file_status("/parent").unwrap();
    assert_eq!(
        (restored.mtime, restored.nlink, restored.children_num),
        (future, parent.nlink, 0)
    );
}

#[test]
fn unchanged_parent_metadata_survives_tree_restore() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!("parent-write-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("parent-write-journal-{}", Utils::rand_str(6)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();

    fs.mkdir("/parent", false).unwrap();
    let mtime = 4_000_000_000_000;
    fs.set_attr(
        "/parent",
        SetAttrOpts {
            mtime: Some(mtime),
            ..Default::default()
        },
    )
    .unwrap();
    let parent = fs.file_status("/parent").unwrap();
    assert_eq!(parent.mtime, mtime);
    #[cfg(feature = "fault-injection")]
    {
        use curvine_fault::{FaultRuleBuilder, FaultRuntime};
        let runtime = FaultRuntime::process();
        let rule = FaultRuleBuilder::named("master.metadata.add.before_commit")
            .times(1)
            .unwrap()
            .return_error("injected add failure")
            .unwrap();
        runtime.configure("parent-write-add", rule).unwrap();
        let result = fs.create("/parent/file", false);
        runtime.remove("parent-write-add").unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected add failure"));
        assert!(!fs.exists("/parent/file").unwrap());
        assert_eq!(fs.file_status("/parent").unwrap().mtime, mtime);
    }
    fs.create("/parent/file", false).unwrap();
    let after = fs.file_status("/parent").unwrap();
    assert_eq!(after.mtime, mtime);
    assert_eq!(after.children_num, 1);
    let by_id = fs.file_status_by_id("/parent", Some(parent.id)).unwrap();
    assert_eq!(by_id.mtime, mtime);
    assert_eq!(by_id.children_num, 1);
    let inode = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .store()
        .get_inode(parent.id)
        .unwrap()
        .unwrap();
    assert_eq!(inode.mtime(), mtime);

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/parent").unwrap().mtime, mtime);
    assert_eq!(fs.file_status("/parent").unwrap().children_num, 1);
    assert!(fs.file_status("/parent/file").is_ok());

    #[cfg(feature = "fault-injection")]
    {
        use curvine_fault::{FaultRuleBuilder, FaultRuntime};
        let runtime = FaultRuntime::process();
        let rule = FaultRuleBuilder::named("master.metadata.delete.before_commit")
            .times(1)
            .unwrap()
            .return_error("injected delete failure")
            .unwrap();
        runtime.configure("parent-write-delete", rule).unwrap();
        let result = fs.delete("/parent/file", false);
        runtime.remove("parent-write-delete").unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected delete failure"));
        assert!(fs.file_status("/parent/file").is_ok());
        assert_eq!(fs.file_status("/parent").unwrap().mtime, mtime);
    }
    fs.delete("/parent/file", false).unwrap();
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/parent").unwrap().mtime, mtime);
    assert_eq!(fs.file_status("/parent").unwrap().children_num, 0);
    let by_id = fs.file_status_by_id("/parent", Some(parent.id)).unwrap();
    assert_eq!(by_id.mtime, mtime);
    assert_eq!(by_id.children_num, 0);
    assert!(!fs.exists("/parent/file").unwrap());

    fs.mkdir("/parent/dir", false).unwrap();
    let nlink_with_dir = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .store()
        .get_inode(parent.id)
        .unwrap()
        .unwrap()
        .nlink()
        .unwrap();
    fs.delete("/parent/dir", false).unwrap();
    let nlink_after = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .store()
        .get_inode(parent.id)
        .unwrap()
        .unwrap()
        .nlink()
        .unwrap();
    assert_eq!(nlink_with_dir, nlink_after + 1);
    fs.restore_from_rocksdb().unwrap();
    assert!(!fs.exists("/parent/dir").unwrap());

    fs.create("/parent/reopened-file", false).unwrap();
    drop(fs);
    conf.format_master = false;
    let reopened = JournalSystem::fs_only_for_test(&conf).unwrap();
    assert_eq!(reopened.file_status("/parent").unwrap().mtime, mtime);
    assert_eq!(reopened.file_status("/parent").unwrap().children_num, 1);
    assert!(reopened.file_status("/parent/reopened-file").is_ok());
}
