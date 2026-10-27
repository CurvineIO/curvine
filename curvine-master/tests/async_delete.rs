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

use curvine_config::ClusterConf;
use curvine_master::master::fs::{DeleteMode, MasterFilesystem, PendingTrashPurger};
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::trash;
use curvine_master::Master;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;

// FileSystemStats uses global metrics; serialize this test binary's fixtures.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn fs_with(name: &str, trash_enabled: bool) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.trash_enabled = trash_enabled;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("async-delete/meta-{}-{}", name, Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "async-delete/journal-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs
}

fn create_tree(fs: &MasterFilesystem, path: &str, dirs: usize, per_dir: usize) {
    for dir in 0..dirs {
        for file in 0..per_dir {
            fs.create(format!("{path}/d{dir}/f{file}"), true).unwrap();
        }
    }
}

fn pending_entries(fs: &MasterFilesystem) -> Vec<String> {
    // `.pending` is hidden from client-facing listings; inspect it through the
    // purger's own scan instead.
    fs.scan_pending_trash(usize::MAX, None)
        .map(|page| {
            page.paths
                .into_iter()
                .map(|path| path.rsplit('/').next().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn async_delete_returns_after_staging_instead_of_walking_the_subtree() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = fs_with("staged", false);
    create_tree(&fs, "/victim", 4, 50);
    let counts_before = fs.get_file_counts();

    let res = fs
        .delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
        .unwrap();

    // No inode was walked, so nothing is reported and the counts are unchanged.
    assert!(res.purge_pending, "the response must mark the entry staged");
    assert!(res.trashed, "staging is a soft delete of the same entry");
    assert_eq!(res.inodes, 0);
    assert_eq!(res.bytes, 0);
    assert!(res.blocks.is_empty());

    // No file inode was walked. The directory count may grow by the two staging
    // roots (`/.Trash` and `/.Trash/.pending`) created on first use.
    let (dirs_before, files_before) = counts_before;
    let (dirs_after, files_after) = fs.get_file_counts();
    assert_eq!(
        files_after, files_before,
        "async delete must not walk inodes"
    );
    assert!(
        dirs_after <= dirs_before + 2,
        "staging must not add more than its two roots (was {dirs_before}, now {dirs_after})"
    );

    // The namespace effect is immediate and the path is immediately reusable.
    assert!(!fs.exists("/victim").unwrap());
    assert_eq!(pending_entries(&fs).len(), 1);
    fs.mkdir("/victim", false).unwrap();
    fs.create("/victim/again", true).unwrap();
}

#[test]
fn async_delete_behaves_the_same_with_and_without_the_recycle_bin() {
    let _serial = TEST_LOCK.lock().unwrap();

    for trash_enabled in [false, true] {
        let fs = fs_with(&format!("enabled-{trash_enabled}"), trash_enabled);
        create_tree(&fs, "/victim", 2, 10);

        let res = fs
            .delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
            .unwrap();

        assert!(res.purge_pending, "trash_enabled={trash_enabled}");
        assert!(!fs.exists("/victim").unwrap());
        // Staged entries never show up as restorable recycle-bin entries.
        assert!(fs.list_trash().unwrap().is_empty());
        let pending = pending_entries(&fs);
        assert_eq!(pending.len(), 1);
        assert!(trash::is_pending_entry_path(&format!(
            "{}/{}",
            trash::TRASH_PENDING_ROOT,
            pending[0]
        )));
    }
}

#[test]
fn staged_entries_are_not_restorable() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = fs_with("not-restorable", true);
    create_tree(&fs, "/victim", 1, 5);
    fs.delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
        .unwrap();

    let entry = pending_entries(&fs).remove(0);
    let entry_path = format!("{}/{}", trash::TRASH_PENDING_ROOT, entry);
    assert!(fs.restore_trash(&entry_path, None, false).is_err());
    assert!(fs.list_trash().unwrap().is_empty());
}

#[test]
fn pending_purger_reclaims_staged_entries() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = fs_with("drain", false);
    create_tree(&fs, "/victim", 3, 40);
    fs.delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
        .unwrap();
    assert_eq!(pending_entries(&fs).len(), 1);

    // A one-entry page walks the staging directory over more than one page.
    let (entries, inodes, _bytes) = PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone())
        .with_scan_batch(1)
        .drain_pending()
        .unwrap()
        .expect("the purger must stay active");

    assert_eq!(entries, 1);
    assert_eq!(inodes, 120);
    assert!(
        pending_entries(&fs).is_empty(),
        "the staging dir is drained"
    );
    assert_eq!(
        fs.list_options("/.Trash", Default::default())
            .unwrap()
            .into_iter()
            .map(|child| child.name)
            .collect::<Vec<_>>(),
        Vec::<String>::new(),
        "the staging directory is drained and never listed to clients"
    );
    assert_eq!(fs.get_file_counts().1, 0, "every file inode is reclaimed");

    // Rechecked through rocksdb: the staged tree is gone from disk too.
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts().1, 0);
    assert!(pending_entries(&fs).is_empty());

    // A second cycle has nothing left to do.
    let idle = PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone())
        .drain_pending()
        .unwrap()
        .expect("the purger must stay active");
    assert_eq!(idle, (0, 0, 0));
}

#[test]
fn demoted_master_does_not_drain_pending_entries() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = fs_with("demoted", false);
    create_tree(&fs, "/victim", 1, 10);
    fs.delete_with_mode("/victim", true, DeleteMode::AsyncPurge)
        .unwrap();

    fs.master_monitor.set_role_for_test(RoleState::Follower);
    assert!(
        PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone())
            .drain_pending()
            .unwrap()
            .is_none()
    );
    assert_eq!(pending_entries(&fs).len(), 1);

    fs.master_monitor.set_role_for_test(RoleState::Leader);
    let (entries, inodes, _) = PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone())
        .drain_pending()
        .unwrap()
        .unwrap();
    assert_eq!((entries, inodes), (1, 10));
    assert!(pending_entries(&fs).is_empty());
}

#[test]
fn sync_delete_keeps_its_exact_counts_and_trash_semantics() {
    let _serial = TEST_LOCK.lock().unwrap();

    // A permanent delete still walks the subtree and reports real counts.
    let fs = fs_with("sync-permanent", false);
    create_tree(&fs, "/victim", 2, 7);
    let res = fs
        .delete_with_mode("/victim", true, DeleteMode::SkipTrash)
        .unwrap();
    assert!(!res.trashed);
    assert!(!res.purge_pending);
    assert_eq!(res.inodes, 14, "permanent deletes report the real counts");
    assert!(fs.list_trash().unwrap().is_empty());

    // With the recycle bin on, the default mode moves the entry and reports no
    // counts, and the entry stays restorable rather than staged.
    let fs = fs_with("sync-trash", true);
    create_tree(&fs, "/victim", 2, 7);
    let res = fs
        .delete_with_mode("/victim", true, DeleteMode::Default)
        .unwrap();
    assert!(res.trashed, "the recycle bin is enabled");
    assert!(!res.purge_pending);
    assert_eq!(res.inodes, 0, "a moved entry deletes no inode");
    assert!(pending_entries(&fs).is_empty());
    assert_eq!(fs.list_trash().unwrap().len(), 1, "the entry is restorable");
}

#[test]
fn async_delete_of_a_hardlinked_file_stays_synchronous() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = fs_with("hardlink", true);
    fs.create("/a.log", true).unwrap();
    fs.link("/a.log", "/b.log").unwrap();

    // A shared inode cannot be moved to staging, so the name is unlinked instead
    // of being staged; the purge promise would be wrong, so the flag stays false.
    let res = fs
        .delete_with_mode("/a.log", false, DeleteMode::AsyncPurge)
        .unwrap();
    assert!(!res.purge_pending);
    assert!(pending_entries(&fs).is_empty());
    assert!(!fs.exists("/a.log").unwrap());
    assert!(fs.exists("/b.log").unwrap());
}
