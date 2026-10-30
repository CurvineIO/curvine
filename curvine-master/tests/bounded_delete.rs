// Copyright 2026 OPPO.
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

//! Client recursive deletes above `master.delete_max_sync_inodes` must not walk
//! the subtree under the namespace lock (#1864).

use curvine_config::ClusterConf;
use curvine_master::master::fs::{DeleteMode, MasterFilesystem};
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;

// FileSystemStats uses global metrics; serialize this test binary's fixtures.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn filesystem(trash_enabled: bool) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.trash_enabled = trash_enabled;
    conf.master.meta_dir = Utils::test_sub_dir(format!("bounded-delete-{}", Utils::rand_str(12)));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs
}

/// Creates `/victim` with `dirs` subdirectories of `per_dir` files and returns
/// the subtree size in inodes, `/victim` included.
fn create_tree(fs: &MasterFilesystem, dirs: usize, per_dir: usize) -> usize {
    for dir in 0..dirs {
        for file in 0..per_dir {
            fs.create(format!("/victim/d{dir}/f{file}"), true).unwrap();
        }
    }
    1 + dirs + dirs * per_dir
}

#[test]
fn delete_within_budget_stays_synchronous() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem(false);
    let size = create_tree(&fs, 4, 25);

    let res = fs
        .delete_bounded("/victim", true, DeleteMode::Default, size)
        .unwrap();

    assert!(!res.purge_pending);
    assert!(!res.trashed);
    assert_eq!(res.inodes, 100);
    assert!(!fs.exists("/victim").unwrap());
    assert_eq!(fs.get_file_counts(), (0, 0));
}

#[test]
fn delete_over_budget_is_staged_and_purged_with_the_same_result() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem(false);
    let keep = fs.create("/keep", false).unwrap();
    let size = create_tree(&fs, 4, 25);

    for mode in [DeleteMode::Default, DeleteMode::SkipTrash] {
        if !fs.exists("/victim").unwrap() {
            create_tree(&fs, 4, 25);
        }
        let res = fs.delete_bounded("/victim", true, mode, size - 1).unwrap();

        // The namespace effect is immediate, but no inode was walked yet.
        assert!(res.purge_pending, "{mode:?}");
        assert_eq!(res.inodes, 0, "{mode:?}");
        assert!(res.blocks.is_empty(), "{mode:?}");
        assert!(!fs.exists("/victim").unwrap());
        assert!(
            fs.list_trash().unwrap().is_empty(),
            "staged entries are not restorable"
        );

        let purged = fs.drain_pending_trash().unwrap().unwrap();
        assert_eq!(purged.inodes, 100, "{mode:?}");
    }

    // Only `/keep` plus the two staging roots remain, and replay agrees.
    let (dirs, files) = fs.get_file_counts();
    assert_eq!(files, 1);
    assert!(
        dirs <= 2,
        "only /.Trash and /.Trash/.pending may remain, got {dirs}"
    );
    let counts = fs.get_file_counts();
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts(), counts);
    assert!(!fs.exists("/victim").unwrap());
    assert_eq!(fs.file_status("/keep").unwrap().id, keep.id);
}

#[test]
fn wide_directory_is_detected_without_walking_every_child() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem(false);
    for file in 0..64 {
        fs.create(format!("/victim/f{file}"), true).unwrap();
    }

    let res = fs
        .delete_bounded("/victim", true, DeleteMode::Default, 1)
        .unwrap();
    assert!(res.purge_pending);
    assert_eq!(fs.drain_pending_trash().unwrap().unwrap().inodes, 64);
}

#[test]
fn zero_budget_and_non_recursive_deletes_keep_current_behavior() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem(false);
    create_tree(&fs, 2, 10);

    // A non-recursive delete of a non-empty directory still fails.
    assert!(fs
        .delete_bounded("/victim", false, DeleteMode::Default, 1)
        .is_err());
    // A single file never exceeds a positive budget.
    let res = fs
        .delete_bounded("/victim/d0/f0", true, DeleteMode::Default, 1)
        .unwrap();
    assert!(!res.purge_pending);
    assert_eq!(res.inodes, 1);

    let res = fs
        .delete_bounded("/victim", true, DeleteMode::Default, 0)
        .unwrap();
    assert!(!res.purge_pending);
    assert_eq!(res.inodes, 19);
}

#[test]
fn soft_delete_is_not_bounded() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem(true);
    create_tree(&fs, 2, 10);

    // Moving into the recycle bin is already an O(1) rename.
    let res = fs
        .delete_bounded("/victim", true, DeleteMode::Default, 1)
        .unwrap();
    assert!(res.trashed);
    assert!(!res.purge_pending);
    assert_eq!(fs.list_trash().unwrap().len(), 1);
}

#[test]
fn oversized_trash_entry_is_claimed_and_nested_paths_are_rejected() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = filesystem(true);
    create_tree(&fs, 2, 10);
    fs.delete_with_mode("/victim", true, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0).path;

    // Deleting a subtree inside a trash entry has no O(1) staging target.
    let nested = format!("{entry}/d0");
    let err = fs
        .delete_bounded(&nested, true, DeleteMode::Default, 1)
        .unwrap_err();
    assert!(
        err.to_string().contains("cannot be deleted synchronously"),
        "{err}"
    );
    assert!(fs.exists(&nested).unwrap());

    // The whole entry is handed to the purger with one rename.
    let res = fs
        .delete_bounded(&entry, true, DeleteMode::Default, 1)
        .unwrap();
    assert!(res.purge_pending);
    assert!(fs.list_trash().unwrap().is_empty());
    assert_eq!(fs.drain_pending_trash().unwrap().unwrap().inodes, 20);
}
