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
use curvine_runtime::common::SerdeUtils;
use serde::Serialize;

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
    SetLocks(SetLocksEntry),
    Free(FreeEntry),
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
