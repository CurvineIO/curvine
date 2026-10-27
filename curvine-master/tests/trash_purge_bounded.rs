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
use curvine_master::master::fs::{DeleteMode, MasterFilesystem, TrashPurger};
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_raft::raft::RoleState;
use curvine_runtime::common::Utils;
use std::sync::mpsc;
use std::time::{Duration, Instant};

// FileSystemStats uses global metrics; serialize this test binary's fixtures.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn trash_fs(name: &str, batch_inodes: usize, yield_ms: u64) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.master.trash_enabled = true;
    conf.master.trash_purge_batch_inodes = batch_inodes;
    conf.master.trash_purge_yield_ms = yield_ms;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("trash-purger/meta-{}-{}", name, Utils::rand_str(6)));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "trash-purger/journal-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs
}

/// Creates `dirs * per_dir` files, trashes the tree and returns the trash entry
/// path.
fn trash_tree(fs: &MasterFilesystem, path: &str, dirs: usize, per_dir: usize) -> String {
    for dir in 0..dirs {
        for file in 0..per_dir {
            fs.create(format!("{path}/d{dir}/f{file}"), true).unwrap();
        }
    }
    fs.delete_with_mode(path, true, DeleteMode::Default)
        .unwrap();
    let entries = fs.list_trash().unwrap();
    assert_eq!(entries.len(), 1, "expected a single trash entry");
    entries[0].path.clone()
}

fn purger(fs: &MasterFilesystem) -> TrashPurger {
    TrashPurger::new(fs.clone(), fs.master_monitor.clone())
}

#[test]
fn purges_a_large_entry_in_bounded_batches() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("bounded", 8, 0);
    let entry = trash_tree(&fs, "/victim", 4, 100);

    let outcome = purger(&fs).purge_entry(&entry).unwrap().unwrap();

    // 400 files + 4 directories + the entry itself, at 8 inodes per batch.
    assert!(
        outcome.batches >= 400 / 8,
        "expected the purge to be split into many batches, got {}",
        outcome.batches
    );
    assert_eq!(outcome.inodes, 400);
    assert!(!fs.exists(&entry).unwrap());
    assert!(fs.exists("/.Trash").unwrap());
    assert!(fs.list_trash().unwrap().is_empty());
    assert!(fs
        .list_options("/.Trash", Default::default())
        .unwrap()
        .is_empty());
    // Only the empty trash root directory is left; every file inode is gone.
    assert_eq!(fs.get_file_counts(), (1, 0));
}

#[test]
fn releases_the_namespace_lock_between_batches() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("lock-release", 4, 10);
    let entry = trash_tree(&fs, "/victim", 4, 150);

    let (tx, rx) = mpsc::channel();
    let purge_fs = fs.clone();
    let purge_entry = entry.clone();
    let handle = std::thread::spawn(move || {
        let started = Instant::now();
        let outcome = purger(&purge_fs).purge_entry(&purge_entry);
        tx.send(outcome).unwrap();
        started.elapsed()
    });

    // With 600 files at 4 inodes per batch and a 10 ms yield between batches the
    // sweep takes well over a second; a foreground metadata operation must still
    // acquire the namespace lock promptly.
    std::thread::sleep(Duration::from_millis(20));
    let started = Instant::now();
    {
        let fs_dir = fs.fs_dir();
        let _guard = fs_dir.write();
    }
    let waited = started.elapsed();

    let outcome = rx
        .recv_timeout(Duration::from_secs(60))
        .unwrap()
        .expect("the purger must finish")
        .expect("the purger must stay active");
    let purged_for = handle.join().unwrap();

    assert!(outcome.batches > 1, "purge must span several batches");
    assert!(
        purged_for > waited,
        "the lock must be released while the purge is still running \
         (purged for {purged_for:?}, lock waited {waited:?})"
    );
    assert!(
        waited < Duration::from_millis(900),
        "foreground metadata must not wait for the whole purge, waited {waited:?}"
    );
    assert!(!fs.exists(&entry).unwrap());
}

#[test]
fn demoted_master_does_not_purge_and_resumes_after_promotion() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("demotion", 8, 0);
    let entry = trash_tree(&fs, "/victim", 2, 10);
    let counts = fs.get_file_counts();

    fs.master_monitor.set_role_for_test(RoleState::Follower);
    assert!(purger(&fs).purge_entry(&entry).unwrap().is_none());
    assert!(fs.exists(&entry).unwrap());
    assert_eq!(fs.get_file_counts(), counts);

    fs.master_monitor.set_role_for_test(RoleState::Leader);
    let outcome = purger(&fs).purge_entry(&entry).unwrap().unwrap();
    assert_eq!(outcome.inodes, 20);
    assert!(!fs.exists(&entry).unwrap());
    assert!(fs.list_trash().unwrap().is_empty());
}

#[test]
fn purge_survives_a_restart_and_replaying_a_finished_entry_is_a_no_op() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("restart", 8, 0);
    let entry = trash_tree(&fs, "/victim", 3, 20);

    // Purge after a rocksdb reload: the entry and its xattrs come back from disk
    // exactly like they would after a master restart.
    fs.restore_from_rocksdb().unwrap();
    let outcome = purger(&fs).purge_entry(&entry).unwrap().unwrap();
    assert_eq!(outcome.inodes, 60);
    fs.restore_from_rocksdb().unwrap();
    assert!(!fs.exists(&entry).unwrap());
    assert!(fs.list_trash().unwrap().is_empty());

    // A second cycle for the same (now missing) entry must not fail.
    let replay = purger(&fs).purge_entry(&entry).unwrap().unwrap();
    assert_eq!(replay.batches, 0);
    assert_eq!(replay.inodes, 0);
}

#[test]
fn purges_a_trashed_single_file_entry() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("single-file", 8, 0);
    fs.create("/data/a.log", true).unwrap();
    fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0).path;

    // A trashed entry can be a plain file: the walk must emit the root itself
    // instead of descending into it.
    let outcome = purger(&fs).purge_entry(&entry).unwrap().unwrap();
    assert_eq!(outcome.batches, 1);
    assert_eq!(outcome.inodes, 1);
    assert!(!fs.exists(&entry).unwrap());
    assert!(fs.list_trash().unwrap().is_empty());
    // (dirs, files): the surviving /data parent and the trash root, no files.
    assert_eq!(fs.get_file_counts(), (2, 0));
}

// Regression: a purge batch is one journal record and one Raft proposal, so it
// is bounded by path bytes as well as inode count. With 4096 paths near
// `max_path_len` (8000) an inode-only bound produced ~32 MiB batches, over the
// 16 MiB RPC frame. A small byte bound must split a batch the inode bound
// alone would allow.
#[test]
fn bounds_batches_by_path_bytes_as_well_as_inodes() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fs = trash_fs("byte-bound", 4096, 0);
    let long = "n".repeat(200);
    for file in 0..40 {
        fs.create(format!("/victim/{long}-{file}"), true).unwrap();
    }
    fs.delete_with_mode("/victim", true, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0).path;

    // Each path is ~240 bytes; 1 KiB per batch holds at most 5 of them.
    let outcome = purger(&fs)
        .with_batch_bytes(1024)
        .purge_entry(&entry)
        .unwrap()
        .unwrap();

    assert!(
        outcome.batches >= 40 / 5,
        "expected the byte bound to split the purge, got {} batches",
        outcome.batches
    );
    assert_eq!(outcome.inodes, 40);
    assert!(!fs.exists(&entry).unwrap());

    // The default bound keeps the same tree in a single inode-bounded batch.
    let fs = trash_fs("byte-bound-default", 4096, 0);
    for file in 0..40 {
        fs.create(format!("/victim/{long}-{file}"), true).unwrap();
    }
    fs.delete_with_mode("/victim", true, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0).path;
    let outcome = purger(&fs).purge_entry(&entry).unwrap().unwrap();
    assert_eq!(outcome.batches, 1);
}
