// Copyright 2026 OPPO
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

//! Same-snapshot lock read regression.
//!
//! `InodeStoreSnapshot::get_locks` exists so a lock read can pair a path
//! resolution and its lock row at one point in time. The live
//! `InodeStore::get_locks` reads the current DB, so an old snapshot that
//! resolved the old inode could otherwise be paired with a lock row written
//! after a rename.
//!
//! The test holds one snapshot across a rename and a lock write, then checks
//! that the old snapshot still reports the old path identity with the old
//! (empty) lock row while the live store and a fresh snapshot see the new
//! state. This distinguishes the two points in time instead of only checking
//! that some call returned Ok.

use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalSystem;
use crate::master::meta::store::InodeStoreSnapshot;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_model::{FileLock, LockFlags, LockType, RenameFlags};
use std::path::PathBuf;

const ORIGINAL: &str = "/original";
const MOVED: &str = "/moved";

/// Removes the test tree when the test ends, including on early panic.
struct TestTree(PathBuf);

impl Drop for TestTree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn test_fs(name: &str) -> (MasterFilesystem, TestTree) {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.journal.enable = false;
    conf.change_test_meta_dir(name);
    let base = PathBuf::from(&conf.master.meta_dir)
        .parent()
        .expect("meta dir has a parent")
        .to_path_buf();
    let fs = JournalSystem::fs_only_for_test(&conf).expect("metadata filesystem");
    (fs, TestTree(base))
}

fn resolved_id(snapshot: &InodeStoreSnapshot<'_>, path: &str) -> Option<i64> {
    snapshot
        .resolve_path(path)
        .expect("snapshot path resolution")
        .target()
        .map(|inode| inode.id())
}

fn fixture_lock() -> FileLock {
    FileLock {
        client_id: "snapshot-lock-fixture".to_string(),
        owner_id: 7,
        pid: 4_242,
        acquire_time: 1_000,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 1,
    }
}

#[test]
fn snapshot_keeps_old_path_identity_paired_with_old_lock_row() {
    let (fs, _tree) = test_fs("snapshot-locks-same-point-in-time");

    fs.create(ORIGINAL, true).expect("create /original");
    let old_id = fs.file_status(ORIGINAL).expect("status /original").id;

    // One namespace view and one store snapshot, both from the pre-rename
    // point in time. The snapshot borrows the view's store, so the view is
    // held for as long as the snapshot lives.
    let handle = fs.fs_dir();
    let view = handle.view().expect("namespace view");
    let old_snapshot =
        InodeStoreSnapshot::new(view.store.checked_read_snapshot().expect("read snapshot"));

    assert_eq!(resolved_id(&old_snapshot, ORIGINAL), Some(old_id));
    assert!(
        old_snapshot
            .get_locks(old_id)
            .expect("snapshot locks")
            .is_empty(),
        "the recorded inode starts with no lock row"
    );

    // Move the recorded inode away and put a different inode at the old path.
    // Both operations keep the parent (root) in place, so they take the
    // sharded path and do not need an exclusive cold window.
    fs.rename(ORIGINAL, MOVED, RenameFlags::empty())
        .expect("rename /original -> /moved");
    fs.create(ORIGINAL, true).expect("recreate /original");
    let new_id = fs.file_status(ORIGINAL).expect("status /original").id;
    assert_ne!(new_id, old_id, "replacement must be a different inode");

    // Write a lock row for the recorded inode after the snapshot was taken.
    let lock = fixture_lock();
    view.store
        .apply_set_locks(old_id, std::slice::from_ref(&lock))
        .expect("write lock row fixture");

    // Old snapshot: still the old identity, still the old (empty) lock row.
    assert_eq!(resolved_id(&old_snapshot, ORIGINAL), Some(old_id));
    assert!(
        old_snapshot
            .get_locks(old_id)
            .expect("snapshot locks")
            .is_empty(),
        "an old snapshot must not read a lock row written after it was taken"
    );

    // Live store: the new lock row is visible for the recorded inode.
    let live = view.store.get_locks(old_id).expect("live locks");
    assert!(
        live.to_vec().contains(&lock),
        "the live store must see the lock row written after the snapshot: {:?}",
        live.to_vec()
    );

    // Fresh snapshot: the new layout. /original is the replacement and
    // /moved is the recorded inode, both with the new lock row.
    let new_snapshot =
        InodeStoreSnapshot::new(view.store.checked_read_snapshot().expect("read snapshot"));
    assert_eq!(resolved_id(&new_snapshot, ORIGINAL), Some(new_id));
    assert_eq!(resolved_id(&new_snapshot, MOVED), Some(old_id));
    assert!(
        new_snapshot
            .get_locks(old_id)
            .expect("snapshot locks")
            .to_vec()
            .contains(&lock),
        "a snapshot taken after the write must see the lock row"
    );

    // Release the borrowed handles before the filesystem.
    drop(new_snapshot);
    drop(old_snapshot);
    drop(view);
    drop(handle);
    drop(fs);
}
