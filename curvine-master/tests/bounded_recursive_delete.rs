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

//! A large permanent recursive delete is staged under `/.Trash/.purging` and
//! purged in bounded batches before the call returns (#1864).

use curvine_config::ClusterConf;
use curvine_master::master::fs::{DeleteMode, MasterFilesystem, PendingTrashPurger};
use curvine_master::master::journal::{JournalEntry, JournalSystem};
use curvine_master::Master;
use curvine_model::{
    ClientAddress, CommitBlock, CreateFileOpts, OpenFlags, SetAttrOptsBuilder, TtlAction,
    WorkerInfo,
};
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;

// FileSystemStats uses global metrics; serialize this test binary's fixtures.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

const BLOCK: i64 = 1048576;

fn conf(name: &str, stage_min_children: usize, journal: bool) -> ClusterConf {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = journal;
    conf.master.trash_purge_batch_inodes = 8;
    conf.master.trash_purge_yield_ms = 0;
    conf.master.delete_stage_min_children = stage_min_children;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "bounded-delete/meta-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "bounded-delete/journal-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    conf
}

fn filesystem(name: &str, stage_min_children: usize, journal: bool) -> MasterFilesystem {
    let fs = JournalSystem::fs_only_for_test(&conf(name, stage_min_children, journal)).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    let mut worker = WorkerInfo {
        capacity: 1 << 40,
        available: 1 << 40,
        ..Default::default()
    };
    worker.address.worker_id = 11;
    fs.add_test_worker(worker);
    fs
}

/// Writes one completed file with `blocks` blocks, so deletes have bytes and
/// block locations to release.
fn write_file(fs: &MasterFilesystem, path: &str, blocks: usize) {
    let mut opts = CreateFileOpts::with_create(true);
    opts.block_size = BLOCK;
    fs.create_with_opts(path, opts, OpenFlags::new_create())
        .unwrap();
    let mut commits: Vec<CommitBlock> = Vec::new();
    let mut previous = None;
    for index in 0..blocks {
        let mut located = fs
            .add_block(
                path,
                None,
                ClientAddress::default(),
                commits.last().cloned().into_iter().collect(),
                vec![],
                index as i64 * BLOCK,
                previous,
            )
            .unwrap();
        located.block.len = BLOCK;
        previous = Some(located.block.clone());
        commits.push(CommitBlock::from(&located));
    }
    fs.complete_file(path, None, blocks as i64 * BLOCK, commits, "", false, None)
        .unwrap();
}

fn wide_tree(fs: &MasterFilesystem, root: &str) {
    for dir in 0..12 {
        for file in 0..10 {
            write_file(fs, &format!("{root}/d{dir}/f{file}"), 1);
        }
    }
    fs.mkdir(format!("{root}/empty/inner"), true).unwrap();
}

fn purging_entries(fs: &MasterFilesystem) -> Vec<String> {
    fs.scan_purging_trash(1024, None).unwrap().paths
}

#[test]
fn staged_delete_reports_the_same_counts_as_a_direct_delete() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let direct = filesystem("direct", 0, false);
    wide_tree(&direct, "/victim");
    let direct_res = direct.delete("/victim", true).unwrap();
    // File counts live in process-global metrics, so read them before the
    // second filesystem starts creating inodes.
    assert_eq!(direct.get_file_counts().1, 0);

    let staged = filesystem("staged", 4, false);
    let keep = staged.create("/keep", false).unwrap();
    wide_tree(&staged, "/victim");
    let staged_res = staged.delete("/victim", true).unwrap();

    assert_eq!(direct_res.inodes, 120);
    assert_eq!(staged_res.inodes, direct_res.inodes);
    assert_eq!(staged_res.bytes, direct_res.bytes);
    assert!(!staged_res.trashed);
    assert!(!staged_res.purge_pending);
    assert!(!staged.exists("/victim").unwrap());
    assert!(purging_entries(&staged).is_empty());
    assert!(staged.list_trash().unwrap().is_empty());
    // (dirs, files): only /keep is left besides the /.Trash staging dirs.
    assert_eq!(staged.get_file_counts().1, 1);

    // Durable state agrees with memory after a restart.
    let counts = staged.get_file_counts();
    staged.restore_from_rocksdb().unwrap();
    assert_eq!(staged.get_file_counts(), counts);
    assert_eq!(staged.file_status("/keep").unwrap().id, keep.id);
    assert!(!staged.exists("/victim").unwrap());
}

#[test]
fn staged_delete_journals_a_rename_and_bounded_purge_batches() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("journal", 4, true);
    wide_tree(&fs, "/victim");
    let dir = fs.fs_dir();
    dir.read().take_entries();

    fs.delete("/victim", true).unwrap();

    let entries = dir.read().take_entries();
    let renames = entries
        .iter()
        .filter(|e| matches!(e, JournalEntry::Rename(_)))
        .count();
    assert_eq!(renames, 1, "the subtree is staged by one rename");
    assert!(
        !entries.iter().any(|e| matches!(e, JournalEntry::Delete(_))),
        "the subtree must not be removed by one Delete record"
    );
    let batches = entries
        .iter()
        .filter(|e| matches!(e, JournalEntry::TrashPurge(_)))
        .count();
    // 120 files + 13 directories + the entry itself, at most 8 paths each.
    assert!(batches >= 134 / 8, "got {} batches", batches);
}

#[test]
fn follower_replay_reaches_the_leader_namespace() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let leader = filesystem("replay-leader", 4, true);
    let follower_js = JournalSystem::from_conf(&conf("replay-follower", 4, true)).unwrap();
    let follower = follower_js.fs();
    let loader = follower_js.journal_loader();

    leader.create("/keep/file", true).unwrap();
    wide_tree(&leader, "/victim");
    leader.delete("/victim", true).unwrap();

    let dir = leader.fs_dir();
    for entry in dir.read().take_entries() {
        loader.apply_entry(entry).unwrap();
    }

    // File counts are process-global, so compare the namespaces themselves.
    let names = |fs: &MasterFilesystem, path: &str| -> Vec<String> {
        let mut names: Vec<_> = fs
            .list_status(path)
            .unwrap()
            .into_iter()
            .map(|s| s.name)
            .collect();
        names.sort();
        names
    };
    assert_eq!(names(&follower, "/"), names(&leader, "/"));
    assert_eq!(names(&follower, "/keep"), vec!["file".to_string()]);
    assert!(!follower.exists("/victim").unwrap());
    assert!(purging_entries(&follower).is_empty());
    // The durable follower store holds no remnant of the subtree either.
    follower.restore_from_rocksdb().unwrap();
    assert_eq!(names(&follower, "/"), names(&leader, "/"));
    assert!(purging_entries(&follower).is_empty());

    drop(loader);
    drop(follower);
    follower_js.shutdown();
}

#[test]
fn staged_delete_keeps_an_expired_parent_mtime_like_a_direct_delete() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let leader = filesystem("expired-leader", 4, true);
    let follower_js = JournalSystem::from_conf(&conf("expired-follower", 4, true)).unwrap();
    let follower = follower_js.fs();
    let loader = follower_js.journal_loader();

    // Below the cutoff: deleted directly. Above it: staged by a rename.
    for dir in 0..3 {
        leader.create(format!("/ttl/small/d{dir}/f"), true).unwrap();
    }
    wide_tree(&leader, "/ttl/large");
    leader
        .set_attr(
            "/ttl",
            SetAttrOptsBuilder::new()
                .ttl_ms(1)
                .ttl_action(TtlAction::Delete)
                .build(),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let expired_mtime = leader.file_status("/ttl").unwrap().mtime;

    let dir = leader.fs_dir();
    for entry in dir.read().take_entries() {
        loader.apply_entry(entry).unwrap();
    }

    leader.delete("/ttl/small", true).unwrap();
    assert_eq!(leader.file_status("/ttl").unwrap().mtime, expired_mtime);

    leader.delete("/ttl/large", true).unwrap();
    let entries = dir.read().take_entries();
    assert!(
        entries.iter().any(|e| matches!(e, JournalEntry::Rename(_))),
        "/ttl/large must be staged"
    );
    assert_eq!(leader.file_status("/ttl").unwrap().mtime, expired_mtime);

    // Replay evaluates expiry at the journaled timestamp and agrees.
    for entry in entries {
        loader.apply_entry(entry).unwrap();
    }
    assert_eq!(follower.file_status("/ttl").unwrap().mtime, expired_mtime);
    assert!(follower.list_status("/ttl").unwrap().is_empty());
    follower.restore_from_rocksdb().unwrap();
    assert_eq!(follower.file_status("/ttl").unwrap().mtime, expired_mtime);

    drop(loader);
    drop(follower);
    follower_js.shutdown();
}

#[test]
fn staged_delete_preserves_an_external_hard_link() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("hardlink", 1, false);
    let mut path = String::from("/victim");
    for depth in 0..32 {
        path.push_str(&format!("/level-{depth}"));
        fs.create(format!("{path}/file"), true).unwrap();
    }
    let original = fs.file_status(format!("{path}/file")).unwrap();
    fs.link(format!("{path}/file"), "/survivor".to_owned())
        .unwrap();

    let res = fs.delete("/victim", true).unwrap();

    assert_eq!(res.inodes, 31);
    assert!(!fs.exists("/victim").unwrap());
    let survivor = fs.file_status("/survivor").unwrap();
    assert_eq!(survivor.id, original.id);
    assert_eq!(survivor.nlink, 1);
    assert_eq!(fs.get_file_counts().1, 1);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.get_file_counts().1, 1);
    assert_eq!(fs.file_status("/survivor").unwrap().nlink, 1);
}

#[test]
fn small_and_mounted_deletes_stay_direct() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("direct-paths", 4, true);
    let dir = fs.fs_dir();

    // Below the cutoff: three direct children.
    for dir in 0..3 {
        fs.create(format!("/small/d{dir}/f"), true).unwrap();
    }
    // Above the cutoff, but mounted.
    wide_tree(&fs, "/mnt/data");
    dir.read().take_entries();

    assert_eq!(fs.delete("/small", true).unwrap().inodes, 3);
    let res = fs
        .delete_with_mode("/mnt/data", true, DeleteMode::Mounted)
        .unwrap();
    assert_eq!(res.inodes, 120);

    let entries = dir.read().take_entries();
    assert!(!entries
        .iter()
        .any(|e| matches!(e, JournalEntry::Rename(_) | JournalEntry::TrashPurge(_))));
    assert!(!fs.exists("/.Trash").unwrap());
}

#[test]
fn lost_leadership_leaves_the_entry_for_the_purger() {
    let _serial = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let fs = filesystem("demoted", 4, false);
    wide_tree(&fs, "/victim");

    // Staging needs no leadership check of its own (the RPC layer gates the
    // delete); the purge stops at its first fenced batch.
    fs.master_monitor.set_role_for_test(RoleState::Follower);
    let res = fs.delete("/victim", true).unwrap();
    assert!(res.purge_pending);
    assert_eq!(res.inodes, 0);
    assert!(!fs.exists("/victim").unwrap());
    assert_eq!(purging_entries(&fs).len(), 1);

    // Clients cannot reach the staged entry.
    let staged = purging_entries(&fs).remove(0);
    assert!(!fs.exists(&staged).unwrap());
    assert!(fs.list_status(&staged).is_err());
    assert!(fs.delete(&staged, true).is_err());
    assert!(fs
        .list_status("/.Trash")
        .unwrap()
        .iter()
        .all(|s| s.name != ".purging"));

    // The next active master adopts the leftover entry.
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    let drained = PendingTrashPurger::new(fs.clone(), fs.master_monitor.clone())
        .drain_pending()
        .unwrap()
        .unwrap();
    assert_eq!(drained.0, 1);
    assert_eq!(drained.1, 120);
    assert!(purging_entries(&fs).is_empty());
    assert_eq!(fs.get_file_counts().1, 0);
}
