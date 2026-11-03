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

use super::*;
use curvine_model::{FileLock, LockFlags, LockType};
use curvine_runtime::common::SerdeUtils;
use serde::Serialize;

/// `FileLock` as it was written before session ownership (#1878).
#[derive(Serialize)]
struct LegacyFileLock {
    client_id: String,
    owner_id: u64,
    pid: u32,
    acquire_time: u64,
    lock_type: LockType,
    lock_flags: LockFlags,
    start: u64,
    end: u64,
}

#[derive(Serialize)]
struct LegacySetLocksEntry {
    op_id: u64,
    rpc_id: i64,
    ino: i64,
    locks: Vec<LegacyFileLock>,
}

#[derive(Serialize)]
struct LegacyRenameEntry {
    op_id: u64,
    rpc_id: i64,
    src: String,
    dst: String,
    mtime: i64,
    flags: u32,
}

#[derive(Serialize)]
struct LegacyFreeEntry {
    op_id: u64,
    rpc_id: i64,
    path: String,
    mtime: i64,
}

#[derive(Serialize)]
#[allow(dead_code)]
enum LegacyJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(LegacyRenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(LegacySetLocksEntry),
    Free(LegacyFreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
}

#[derive(Serialize)]
struct LegacyJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyJournalEntry>,
}

#[test]
fn legacy_rename_batch_uses_zero_exchange_inode_ids() {
    let legacy = LegacyJournalBatch {
        seq_id: 42,
        batch: vec![
            LegacyJournalEntry::Rename(LegacyRenameEntry {
                op_id: 7,
                rpc_id: 9,
                src: "/src".to_string(),
                dst: "/dst".to_string(),
                mtime: 11,
                flags: 0,
            }),
            LegacyJournalEntry::Delete(DeleteEntry {
                op_id: 8,
                rpc_id: 10,
                path: "/deleted".to_string(),
                mtime: 12,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    assert_eq!(batch.seq_id, 42);
    let JournalEntry::Rename(entry) = &batch.batch[0] else {
        panic!("expected rename journal entry");
    };
    assert_eq!(entry.op_id, 7);
    assert_eq!(entry.src_inode_id, 0);
    assert_eq!(entry.dst_inode_id, 0);
    let JournalEntry::Delete(entry) = &batch.batch[1] else {
        panic!("expected delete journal entry");
    };
    assert_eq!(entry.op_id, 8);
    assert_eq!(entry.path, "/deleted");
}

#[test]
fn current_rename_batch_keeps_exchange_inode_ids() {
    let current = JournalBatch {
        seq_id: 43,
        batch: vec![JournalEntry::Rename(RenameEntry {
            op_id: 8,
            rpc_id: 10,
            src: "/src".to_string(),
            dst: "/dst".to_string(),
            mtime: 12,
            flags: 2,
            src_inode_id: 100,
            dst_inode_id: 200,
        })],
    };
    let bytes = SerdeUtils::serialize(&current).unwrap();

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Rename(entry) = &batch.batch[0] else {
        panic!("expected rename journal entry");
    };
    assert_eq!(entry.src_inode_id, 100);
    assert_eq!(entry.dst_inode_id, 200);
}

#[test]
fn current_free_batch_keeps_recursive_flag() {
    let current = JournalBatch {
        seq_id: 44,
        batch: vec![JournalEntry::Free(FreeEntry {
            op_id: 9,
            rpc_id: 11,
            path: "/free".to_string(),
            mtime: 13,
            recursive: true,
        })],
    };
    let bytes = SerdeUtils::serialize(&current).unwrap();

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Free(entry) = &batch.batch[0] else {
        panic!("expected free journal entry");
    };
    assert!(entry.recursive);
}

#[test]
fn legacy_free_batch_defaults_recursive_flag() {
    let legacy = LegacyJournalBatch {
        seq_id: 45,
        batch: vec![
            LegacyJournalEntry::Free(LegacyFreeEntry {
                op_id: 10,
                rpc_id: 12,
                path: "/free".to_string(),
                mtime: 14,
            }),
            LegacyJournalEntry::UfsApplied(UfsAppliedEntry {
                op_id: 11,
                rpc_id: 13,
                term: 2,
                index: 15,
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::Free(entry) = &batch.batch[0] else {
        panic!("expected free journal entry");
    };
    assert_eq!(entry.op_id, 10);
    assert_eq!(entry.path, "/free");
    assert!(!entry.recursive);
    let JournalEntry::UfsApplied(entry) = &batch.batch[1] else {
        panic!("expected ufs applied journal entry");
    };
    assert_eq!(entry.index, 15);
}

#[test]
fn legacy_rename_batch_rejects_trailing_bytes() {
    let legacy = LegacyJournalBatch {
        seq_id: 44,
        batch: vec![LegacyJournalEntry::Rename(LegacyRenameEntry {
            op_id: 9,
            rpc_id: 11,
            src: "/src".to_string(),
            dst: "/dst".to_string(),
            mtime: 13,
            flags: 0,
        })],
    };
    let mut bytes = SerdeUtils::serialize(&legacy).unwrap();
    bytes.extend_from_slice(&[0, 0, 0, 0]);

    assert!(JournalBatch::deserialize_compat(&bytes).is_err());
}

#[test]
fn trash_purge_variant_is_appended_after_existing_discriminants() {
    // bincode encodes an enum variant as its positional index, so appending
    // TrashPurge must not move Delete or CacheInvalidation. Pin both ends of
    // the enum: an accidental reorder breaks the older side first.
    let delete = SerdeUtils::serialize(&JournalEntry::Delete(DeleteEntry {
        op_id: 1,
        rpc_id: 2,
        path: "/deleted".to_string(),
        mtime: 3,
    }))
    .unwrap();
    assert_eq!(
        delete[..4],
        7u32.to_le_bytes(),
        "Delete must stay variant 7"
    );

    let invalidation =
        SerdeUtils::serialize(&JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 4,
            rpc_id: 5,
            inodes: vec![],
        }))
        .unwrap();
    assert_eq!(
        invalidation[..4],
        17u32.to_le_bytes(),
        "CacheInvalidation must stay variant 17"
    );

    let purge = SerdeUtils::serialize(&JournalEntry::TrashPurge(TrashPurgeEntry {
        op_id: 6,
        rpc_id: 0,
        entry: "/.Trash/1-2".to_string(),
        mtime: 7,
        paths: vec!["/.Trash/1-2/a".to_string(), "/.Trash/1-2/b".to_string()],
    }))
    .unwrap();
    assert_eq!(
        purge[..4],
        18u32.to_le_bytes(),
        "TrashPurge must be appended as variant 18"
    );
}

#[test]
fn trash_purge_batch_round_trips_its_paths() {
    let current = JournalBatch {
        seq_id: 45,
        batch: vec![JournalEntry::TrashPurge(TrashPurgeEntry {
            op_id: 12,
            rpc_id: 0,
            entry: "/.Trash/9-42".to_string(),
            mtime: 15,
            paths: vec![
                "/.Trash/9-42/dir/file-a".to_string(),
                "/.Trash/9-42/dir/file-b".to_string(),
                "/.Trash/9-42/dir".to_string(),
            ],
        })],
    };
    let bytes = SerdeUtils::serialize(&current).unwrap();

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::TrashPurge(entry) = &batch.batch[0] else {
        panic!("expected trash purge journal entry");
    };
    assert_eq!(entry.op_id, 12);
    assert_eq!(entry.entry, "/.Trash/9-42");
    assert_eq!(entry.mtime, 15);
    assert_eq!(
        entry.paths,
        vec![
            "/.Trash/9-42/dir/file-a".to_string(),
            "/.Trash/9-42/dir/file-b".to_string(),
            "/.Trash/9-42/dir".to_string(),
        ]
    );
}

/// Locks persisted before #1878 have no trailing `session_owned` flag. Replaying
/// such a batch must still work: a master restart with an old journal cannot be
/// allowed to fail, and the replayed locks belong to the legacy TTL path.
#[test]
fn legacy_set_locks_batch_replays_as_not_session_owned() {
    let legacy = LegacyJournalBatch {
        seq_id: 7,
        batch: vec![LegacyJournalEntry::SetLocks(LegacySetLocksEntry {
            op_id: 21,
            rpc_id: 22,
            ino: 33,
            locks: vec![LegacyFileLock {
                client_id: "legacy-client".to_string(),
                owner_id: 44,
                pid: 55,
                acquire_time: 66,
                lock_type: LockType::WriteLock,
                lock_flags: LockFlags::Plock,
                start: 0,
                end: 100,
            }],
        })],
    };
    let bytes = SerdeUtils::serialize(&legacy).unwrap();

    assert!(
        SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err(),
        "the current schema must not silently misread the legacy layout"
    );

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    let JournalEntry::SetLocks(entry) = &batch.batch[0] else {
        panic!("expected set-locks journal entry");
    };
    assert_eq!(entry.ino, 33);
    assert_eq!(entry.locks.len(), 1);
    let lock: &FileLock = &entry.locks[0];
    assert_eq!(lock.client_id, "legacy-client");
    assert_eq!(lock.owner_id, 44);
    assert_eq!(lock.acquire_time, 66);
    assert!(
        !lock.session_owned,
        "a lock from before #1878 must not claim a session lease"
    );
}

/// The layout right before #1878: every entry as today, except `SetLocks`.
#[derive(Serialize)]
#[allow(dead_code)]
enum PreSessionJournalEntry {
    Mkdir(MkdirEntry),
    CreateFile(CreateFileEntry),
    ReopenFile(ReopenFileEntry),
    OverWriteFile(OverWriteFileEntry),
    AddBlock(AddBlockEntry),
    CompleteFile(CompleteFileEntry),
    Rename(RenameEntry),
    Delete(DeleteEntry),
    Mount(MountEntry),
    UnMount(UnMountEntry),
    SetAttr(SetAttrEntry),
    Symlink(SymlinkEntry),
    Link(LinkEntry),
    SetLocks(LegacySetLocksEntry),
    Free(FreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
    CacheInvalidation(CacheInvalidationEntry),
    TrashPurge(TrashPurgeEntry),
}

#[derive(Serialize)]
struct PreSessionJournalBatch {
    seq_id: u64,
    batch: Vec<PreSessionJournalEntry>,
}

fn legacy_lock(client_id: &str, owner_id: u64) -> LegacyFileLock {
    LegacyFileLock {
        client_id: client_id.to_string(),
        owner_id,
        pid: 55,
        acquire_time: 66,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 100,
    }
}

/// A batch written by the release right before #1878 mixes an old-layout
/// `SetLocks` with entries in their current layouts (`Free { recursive }`,
/// `Rename` with exchange inode ids) and variants appended after the oldest
/// legacy mirror (`CacheInvalidation`, `TrashPurge`). Neither the current nor
/// the oldest legacy schema can decode it; replay after an upgrade must not fail.
#[test]
fn pre_session_mixed_batch_replays_every_entry() {
    let pre_session = PreSessionJournalBatch {
        seq_id: 8,
        batch: vec![
            PreSessionJournalEntry::SetLocks(LegacySetLocksEntry {
                op_id: 1,
                rpc_id: 2,
                ino: 33,
                locks: vec![legacy_lock("client-a", 1), legacy_lock("client-b", 2)],
            }),
            PreSessionJournalEntry::Free(FreeEntry {
                op_id: 3,
                rpc_id: 4,
                path: "/free".to_string(),
                mtime: 5,
                recursive: true,
            }),
            PreSessionJournalEntry::Rename(RenameEntry {
                op_id: 6,
                rpc_id: 7,
                src: "/src".to_string(),
                dst: "/dst".to_string(),
                mtime: 8,
                flags: 2,
                src_inode_id: 100,
                dst_inode_id: 200,
            }),
            PreSessionJournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 9,
                rpc_id: 10,
                inodes: vec![],
            }),
            PreSessionJournalEntry::TrashPurge(TrashPurgeEntry {
                op_id: 11,
                rpc_id: 0,
                entry: "/.Trash/1-2".to_string(),
                mtime: 12,
                paths: vec!["/.Trash/1-2/a".to_string()],
            }),
        ],
    };
    let bytes = SerdeUtils::serialize(&pre_session).unwrap();

    assert!(SerdeUtils::deserialize::<JournalBatch>(&bytes).is_err());

    let batch = JournalBatch::deserialize_compat(&bytes).unwrap();
    assert_eq!(batch.seq_id, 8);
    assert_eq!(batch.batch.len(), 5);

    let JournalEntry::SetLocks(entry) = &batch.batch[0] else {
        panic!("expected set-locks journal entry");
    };
    assert_eq!(entry.ino, 33);
    let owners: Vec<_> = entry.locks.iter().map(|l| l.client_id.as_str()).collect();
    assert_eq!(owners, ["client-a", "client-b"]);
    assert!(entry.locks.iter().all(|l| !l.session_owned));

    let JournalEntry::Free(entry) = &batch.batch[1] else {
        panic!("expected free journal entry");
    };
    assert!(
        entry.recursive,
        "the current Free layout must not be downgraded"
    );

    let JournalEntry::Rename(entry) = &batch.batch[2] else {
        panic!("expected rename journal entry");
    };
    assert_eq!((entry.src_inode_id, entry.dst_inode_id), (100, 200));

    assert!(matches!(batch.batch[3], JournalEntry::CacheInvalidation(_)));
    let JournalEntry::TrashPurge(entry) = &batch.batch[4] else {
        panic!("expected trash purge journal entry");
    };
    assert_eq!(entry.paths, vec!["/.Trash/1-2/a".to_string()]);
}
