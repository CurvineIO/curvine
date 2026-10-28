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

//! Client ownership index (#1880).
//!
//! The index answers "which inodes does this client own lock/writer state on?"
//! so the session reaper (#1884) never has to scan the whole namespace. It is
//! derived state, maintained inside the apply paths so the leader, followers and
//! journal replay stay identical.

use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalEntry;
use crate::master::meta::inode::InodePath;
use crate::master::meta::session_owned_clients;
use crate::master::meta::store::OwnerKind;
use curvine_config::ClusterConf;
use curvine_model::{CreateFileOpts, FileLock, LockFlags, LockType};
use curvine_runtime::common::Utils;

fn test_fs(name: &str) -> MasterFilesystem {
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "client-owner-index/meta-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "client-owner-index/journal-{}-{}",
        name,
        Utils::rand_str(6)
    ));
    crate::master::journal::JournalSystem::fs_only_for_test(&conf).unwrap()
}

fn lock(client_id: &str, session_owned: bool) -> FileLock {
    FileLock {
        client_id: client_id.to_string(),
        owner_id: 1,
        pid: 1,
        acquire_time: 0,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 100,
        session_owned,
    }
}

fn file_id(fs: &MasterFilesystem, path: &str) -> i64 {
    let fs_dir = fs.fs_dir.read();
    let inp = InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store).unwrap();
    inp.get_last_inode().unwrap().id()
}

fn owners(fs: &MasterFilesystem, client_id: &str) -> Vec<(OwnerKind, i64)> {
    fs.fs_dir.read().store.get_client_owners(client_id).unwrap()
}

fn create_file(fs: &MasterFilesystem, path: &str) {
    create_file_named(fs, path, "");
}

fn create_file_named(fs: &MasterFilesystem, path: &str, client_name: &str) {
    let mut fs_dir = fs.fs_dir.write();
    let inp = InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store).unwrap();
    let mut opts = CreateFileOpts::with_create(false);
    opts.client_name = client_name.to_string();
    fs_dir.create_file(inp, opts).unwrap();
}

#[test]
fn session_owned_locks_are_indexed_and_legacy_locks_are_not() {
    let fs = test_fs("locks");
    create_file(&fs, "/indexed.log");
    let ino = file_id(&fs, "/indexed.log");

    fs.set_lock("/indexed.log", lock("session-client", true))
        .unwrap();
    fs.set_lock("/indexed.log", lock("legacy-client", false))
        .unwrap();

    assert_eq!(
        owners(&fs, "session-client"),
        vec![(OwnerKind::Lock, ino)],
        "a session-owned lock must be reachable through the index"
    );
    assert!(
        owners(&fs, "legacy-client").is_empty(),
        "legacy locks keep the TTL path and are never reclaimed per client, so \
         they must not grow the index"
    );
}

#[test]
fn releasing_a_lock_removes_its_index_entry() {
    let fs = test_fs("unlock");
    create_file(&fs, "/unlock.log");
    let ino = file_id(&fs, "/unlock.log");

    fs.set_lock("/unlock.log", lock("session-client", true))
        .unwrap();
    assert_eq!(owners(&fs, "session-client"), vec![(OwnerKind::Lock, ino)]);

    let mut unlock = lock("session-client", true);
    unlock.lock_type = LockType::UnLock;
    fs.set_lock("/unlock.log", unlock).unwrap();

    assert!(
        owners(&fs, "session-client").is_empty(),
        "an unlock must drop the hint, otherwise the reaper keeps revisiting it"
    );
}

#[test]
fn writer_markers_are_indexed_on_create_and_dropped_on_complete() {
    let fs = test_fs("writer");
    let mut fs_dir = fs.fs_dir.write();
    let inp = InodePath::resolve(fs_dir.root_ptr(), "/writer.log", &fs_dir.store).unwrap();
    let mut opts = CreateFileOpts::with_create(false);
    opts.client_name = "writing-client".to_string();
    let inp = fs_dir.create_file(inp, opts).unwrap();
    let ino = inp.get_last_inode().unwrap().id();
    drop(fs_dir);

    assert_eq!(
        owners(&fs, "writing-client"),
        vec![(OwnerKind::Writer, ino)],
        "opening a file for write marks the client as an owner"
    );

    // Complete the file the way a client does: the writer marker disappears and so
    // must its index entry.
    let fs_dir = fs.fs_dir.write();
    let mut inode = fs_dir.store.get_inode(ino, None).unwrap().unwrap();
    let file = inode.as_file_mut().unwrap();
    file.complete(0, &[], "writing-client", false).unwrap();
    fs_dir.store.apply_complete_file(&inode, &[]).unwrap();
    drop(fs_dir);

    assert!(
        owners(&fs, "writing-client").is_empty(),
        "a completed write must drop the hint"
    );
}

#[test]
fn release_writer_removes_only_the_named_client() {
    let fs = test_fs("release-writer");
    let mut fs_dir = fs.fs_dir.write();
    let inp = InodePath::resolve(fs_dir.root_ptr(), "/shared.log", &fs_dir.store).unwrap();
    let mut opts = CreateFileOpts::with_create(false);
    opts.client_name = "crashed-client".to_string();
    let inp = fs_dir.create_file(inp, opts).unwrap();
    let ino = inp.get_last_inode().unwrap().id();

    // A second client starts writing the same file before the first one is reaped.
    let mut inode = fs_dir.store.get_inode(ino, None).unwrap().unwrap();
    inode.as_file_mut().unwrap().reopen("healthy-client");
    fs_dir.store.apply_reopen_file(&inode).unwrap();
    drop(fs_dir);

    assert_eq!(
        owners(&fs, "crashed-client"),
        vec![(OwnerKind::Writer, ino)]
    );
    assert_eq!(
        owners(&fs, "healthy-client"),
        vec![(OwnerKind::Writer, ino)]
    );

    let mut fs_dir = fs.fs_dir.write();
    assert!(fs_dir.release_writer(ino, "crashed-client").unwrap());
    // Idempotent: a replayed or duplicated entry must not fail.
    assert!(!fs_dir.release_writer(ino, "crashed-client").unwrap());
    let inode = fs_dir.store.get_inode(ino, None).unwrap().unwrap();
    drop(fs_dir);

    let writers = inode.writer_clients();
    assert_eq!(
        writers,
        vec!["healthy-client".to_string()],
        "only the named client's marker may be removed"
    );
    assert!(
        owners(&fs, "crashed-client").is_empty(),
        "the reaped client's hint must go with the marker"
    );
    assert_eq!(
        owners(&fs, "healthy-client"),
        vec![(OwnerKind::Writer, ino)]
    );
}

#[test]
fn deleting_a_file_drops_its_writer_hints() {
    let fs = test_fs("delete");
    let mut fs_dir = fs.fs_dir.write();
    let inp = InodePath::resolve(fs_dir.root_ptr(), "/deleted.log", &fs_dir.store).unwrap();
    let mut opts = CreateFileOpts::with_create(false);
    opts.client_name = "writing-client".to_string();
    let inp = fs_dir.create_file(inp, opts).unwrap();
    let ino = inp.get_last_inode().unwrap().id();
    drop(fs_dir);

    assert_eq!(
        owners(&fs, "writing-client"),
        vec![(OwnerKind::Writer, ino)]
    );

    fs.delete("/deleted.log", false).unwrap();

    assert!(
        owners(&fs, "writing-client").is_empty(),
        "a deleted inode cannot own anything: the hint must not outlive it"
    );
}

#[test]
fn replaying_release_writer_reproduces_the_reaped_state() {
    // Build "the journaled fact": a file whose writer marker belongs to a client.
    let leader = test_fs("replay");
    let mut fs_dir = leader.fs_dir.write();
    let inp = InodePath::resolve(fs_dir.root_ptr(), "/replayed.log", &fs_dir.store).unwrap();
    let mut opts = CreateFileOpts::with_create(false);
    opts.client_name = "replayed-client".to_string();
    let inp = fs_dir.create_file(inp, opts).unwrap();
    let ino = inp.get_last_inode().unwrap().id();
    drop(fs_dir);

    let entry = JournalEntry::ReleaseWriter(crate::master::journal::ReleaseWriterEntry {
        op_id: 1,
        rpc_id: 0,
        ino,
        client: "replayed-client".to_string(),
    });
    let JournalEntry::ReleaseWriter(release) = entry else {
        unreachable!()
    };

    // Replay it on a fresh store, the way a follower applies the entry after it has
    // replayed the create that produced the marker.
    let follower = test_fs("replay-target");
    create_file_named(&follower, "/replayed.log", "replayed-client");
    let follower_ino = file_id(&follower, "/replayed.log");
    assert_eq!(
        owners(&follower, "replayed-client"),
        vec![(OwnerKind::Writer, follower_ino)]
    );

    let mut follower_dir = follower.fs_dir.write();
    assert!(follower_dir
        .release_writer(follower_ino, &release.client)
        .unwrap());
    let inode = follower_dir
        .store
        .get_inode(follower_ino, None)
        .unwrap()
        .unwrap();
    drop(follower_dir);

    assert!(
        inode.writer_clients().is_empty(),
        "replay must reproduce the reaped state: the file is complete again"
    );
    assert!(!inode.as_file_ref().unwrap().is_writing());
    assert!(owners(&follower, "replayed-client").is_empty());
}

/// The index must be derivable from persisted lock records alone, so a lost index
/// (upgrade, corruption) can be rebuilt without a full inode scan.
#[test]
fn lock_index_is_rebuildable_from_the_lock_table() {
    let fs = test_fs("rebuild");
    create_file(&fs, "/rebuild.log");
    let ino = file_id(&fs, "/rebuild.log");
    fs.set_lock("/rebuild.log", lock("session-client", true))
        .unwrap();
    fs.set_lock("/rebuild.log", lock("legacy-client", false))
        .unwrap();

    {
        let fs_dir = fs.fs_dir.read();
        fs_dir.store.clear_client_owners("session-client").unwrap();
    }
    assert!(owners(&fs, "session-client").is_empty());

    // Rebuild: the lock table is the source of truth.
    let fs_dir = fs.fs_dir.read();
    let locks = fs_dir.store.get_locks(ino).unwrap();
    for client in session_owned_clients(&locks.to_vec()) {
        fs_dir
            .store
            .add_client_owner(&client, OwnerKind::Lock, ino)
            .unwrap();
    }
    drop(fs_dir);

    assert_eq!(owners(&fs, "session-client"), vec![(OwnerKind::Lock, ino)]);
    assert!(owners(&fs, "legacy-client").is_empty());
}
