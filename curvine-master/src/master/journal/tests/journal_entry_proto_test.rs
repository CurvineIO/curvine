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
use crate::master::meta::inode::{InodeDir, InodeFile, InodeView};
use crate::master::meta::BlockMeta;
use curvine_model::{
    AccessMode, BlockLocation, CommitBlock, FileLock, LockFlags, LockType, MountInfo, Provider,
    SetAttrOpts, StorageType, TtlAction, WriteType,
};
use std::collections::HashMap;

fn mount_info() -> MountInfo {
    MountInfo {
        cv_path: "/mnt".to_string(),
        ufs_path: "oss://bucket/data".to_string(),
        mount_id: 5,
        properties: HashMap::from([(
            "fs.oss.endpoint".to_string(),
            "oss-cn-hangzhou.aliyuncs.com".to_string(),
        )]),
        ttl_ms: 60_000,
        ttl_action: TtlAction::Delete,
        read_verify_ufs: true,
        storage_type: Some(StorageType::Ssd),
        block_size: Some(4 << 20),
        replicas: Some(3),
        write_type: WriteType::FsMode,
        provider: Some(Provider::Opendal),
        auto_cache: false,
        access_mode: AccessMode::ReadWrite,
        write_cache: true,
    }
}

fn set_attr_opts() -> SetAttrOpts {
    SetAttrOpts {
        recursive: true,
        replicas: Some(2),
        owner: Some("alice".to_string()),
        group: Some("staff".to_string()),
        mode: Some(0o640),
        atime: Some(1_100),
        mtime: Some(1_200),
        ttl_ms: Some(5_000),
        ttl_action: Some(TtlAction::Delete),
        add_x_attr: HashMap::from([("user.tag".to_string(), b"v".to_vec())]),
        remove_x_attr: vec!["user.old".to_string()],
        ufs_mtime: Some(1_300),
    }
}

/// Builds a batch carrying every `JournalEntry` variant with values that
/// exercise every protobuf branch and every codec-backed `bytes` field.
fn all_variant_batch() -> JournalBatch {
    let commit = |block_id: i64, block_len: i64| CommitBlock {
        block_id,
        block_len,
        locations: vec![BlockLocation::with_id(7)],
        expected_revision: None,
    };

    JournalBatch {
        seq_id: 900,
        batch: vec![
            JournalEntry::Mkdir(MkdirEntry {
                op_id: 1,
                rpc_id: -1,
                path: "/dir".to_string(),
                dir: InodeDir::new(100, 1_000),
            }),
            JournalEntry::CreateFile(CreateFileEntry {
                op_id: 2,
                rpc_id: 2,
                path: "/dir/file".to_string(),
                file: InodeFile::new(101, 1_001),
            }),
            JournalEntry::ReopenFile(ReopenFileEntry {
                op_id: 3,
                rpc_id: 3,
                path: "/dir/file".to_string(),
                file: InodeFile::new(102, 1_002),
            }),
            JournalEntry::OverWriteFile(OverWriteFileEntry {
                op_id: 4,
                rpc_id: 4,
                path: "/dir/file".to_string(),
                file: InodeFile::new(103, 1_003),
            }),
            JournalEntry::AddBlock(AddBlockEntry {
                op_id: 5,
                rpc_id: 5,
                path: "/dir/file".to_string(),
                blocks: vec![BlockMeta::new(2_001, 4_096)],
                commit_block: vec![commit(2_001, 4_096)],
            }),
            JournalEntry::CompleteFile(CompleteFileEntry {
                op_id: 6,
                rpc_id: 6,
                path: "/dir/file".to_string(),
                file: InodeFile::new(104, 1_004),
                commit_blocks: vec![commit(2_001, 8_192)],
            }),
            JournalEntry::Rename(RenameEntry {
                op_id: 7,
                rpc_id: 7,
                src: "/src".to_string(),
                dst: "/dst".to_string(),
                mtime: 11,
                flags: 2,
                src_inode_id: 100,
                dst_inode_id: 200,
            }),
            JournalEntry::Delete(DeleteEntry {
                op_id: 8,
                rpc_id: 8,
                path: "/deleted".to_string(),
                mtime: 12,
            }),
            JournalEntry::Mount(MountEntry {
                op_id: 9,
                rpc_id: 9,
                info: mount_info(),
            }),
            JournalEntry::UnMount(UnMountEntry {
                op_id: 10,
                rpc_id: 10,
                id: 5,
            }),
            JournalEntry::SetAttr(SetAttrEntry {
                op_id: 11,
                rpc_id: 11,
                path: "/dir/file".to_string(),
                opts: set_attr_opts(),
            }),
            JournalEntry::Symlink(SymlinkEntry {
                op_id: 12,
                rpc_id: 12,
                link: "/target".to_string(),
                new_inode: InodeFile::new(105, 1_005),
                force: true,
            }),
            JournalEntry::Link(LinkEntry {
                op_id: 13,
                rpc_id: 13,
                mtime: 13,
                src_path: "/src".to_string(),
                dst_path: "/dst".to_string(),
            }),
            JournalEntry::SetLocks(SetLocksEntry {
                op_id: 14,
                rpc_id: 14,
                ino: 106,
                locks: vec![FileLock {
                    client_id: "client-1".to_string(),
                    owner_id: 7,
                    pid: 42,
                    acquire_time: 9,
                    lock_type: LockType::WriteLock,
                    lock_flags: LockFlags::Flock,
                    start: 0,
                    end: 100,
                    session_owned: false,
                }],
            }),
            JournalEntry::Free(FreeEntry {
                op_id: 15,
                rpc_id: 15,
                path: "/free".to_string(),
                mtime: 14,
                recursive: true,
            }),
            JournalEntry::UfsApplied(UfsAppliedEntry {
                op_id: 16,
                rpc_id: 16,
                term: 3,
                index: 77,
            }),
            JournalEntry::Snapshot(SnapshotEntry {
                op_id: 17,
                rpc_id: 17,
                node_id: 1,
                dir: "/snapshot".to_string(),
            }),
            JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 18,
                rpc_id: 18,
                inodes: vec![
                    InodeView::new_file("cached".to_string(), InodeFile::new(107, 1_007)),
                    InodeView::new_dir("cached-dir".to_string(), InodeDir::new(108, 1_008)),
                    InodeView::new_entry("cached-entry".to_string(), 109),
                ],
            }),
            JournalEntry::TrashPurge(TrashPurgeEntry {
                op_id: 19,
                rpc_id: 19,
                entry: "trash-1".to_string(),
                mtime: 19,
                paths: vec!["/trash/a".to_string(), "/trash/b".to_string()],
            }),
            JournalEntry::ReleaseWriter(ReleaseWriterEntry {
                op_id: 20,
                rpc_id: 20,
                ino: 110,
                client: "client-x".to_string(),
            }),
            JournalEntry::FencedAddBlock(AddBlockEntry {
                op_id: 21,
                rpc_id: 21,
                path: "/dir/file".to_string(),
                blocks: vec![BlockMeta::new(2_002, 4_096)],
                commit_block: vec![CommitBlock {
                    expected_revision: Some(3),
                    ..commit(2_002, 4_096)
                }],
            }),
            JournalEntry::FencedCompleteFile(CompleteFileEntry {
                op_id: 22,
                rpc_id: 22,
                path: "/dir/file".to_string(),
                file: InodeFile::new(105, 1_005),
                commit_blocks: vec![CommitBlock {
                    expected_revision: Some(4),
                    ..commit(2_002, 8_192)
                }],
            }),
            JournalEntry::ReplicaLocation(ReplicaLocationEntry {
                op_id: 23,
                rpc_id: 23,
                block_id: 2_002,
                location: BlockLocation::with_id(9),
                commit_revision: 4,
            }),
        ],
    }
}

#[test]
fn protobuf_batch_round_trips_all_entry_variants() {
    let batch = all_variant_batch();
    let bytes = batch.encode().unwrap();

    let decoded = JournalBatch::decode(&bytes).unwrap();
    assert_eq!(decoded.seq_id, 900);
    assert_eq!(decoded.len(), 23);
    let op_ids: Vec<u64> = decoded.batch.iter().map(JournalEntry::op_id).collect();
    assert_eq!(op_ids, (1..=23).collect::<Vec<_>>());

    // Spot-check the shared protobuf models and the codec-backed inode list:
    // a field dropped symmetrically by encode and decode would not change the
    // batch structure, so assert the values themselves. The rest of the
    // hand-mapped variants get the same treatment below, because a mapping
    // that swaps two same-typed fields on both sides keeps the bytes stable.
    match &decoded.batch[0] {
        JournalEntry::Mkdir(e) => {
            assert_eq!((e.op_id, e.rpc_id), (1, -1));
            assert_eq!(e.path, "/dir");
            assert_eq!((e.dir.id, e.dir.mtime), (100, 1_000));
        }
        other => panic!("unexpected mkdir entry: {:?}", other),
    }
    match &decoded.batch[1] {
        JournalEntry::CreateFile(e) => {
            assert_eq!((e.op_id, e.rpc_id), (2, 2));
            assert_eq!(e.path, "/dir/file");
            assert_eq!((e.file.id, e.file.mtime), (101, 1_001));
        }
        other => panic!("unexpected create file entry: {:?}", other),
    }
    match &decoded.batch[2] {
        JournalEntry::ReopenFile(e) => {
            assert_eq!((e.op_id, e.rpc_id), (3, 3));
            assert_eq!((e.file.id, e.file.mtime), (102, 1_002));
        }
        other => panic!("unexpected reopen file entry: {:?}", other),
    }
    match &decoded.batch[3] {
        JournalEntry::OverWriteFile(e) => {
            assert_eq!((e.op_id, e.rpc_id), (4, 4));
            assert_eq!((e.file.id, e.file.mtime), (103, 1_003));
        }
        other => panic!("unexpected overwrite file entry: {:?}", other),
    }
    match &decoded.batch[4] {
        JournalEntry::AddBlock(e) => {
            assert_eq!((e.op_id, e.rpc_id), (5, 5));
            assert_eq!(e.path, "/dir/file");
            assert_eq!((e.blocks[0].id, e.blocks[0].len), (2_001, 4_096));
            assert_eq!(e.commit_block[0].block_id, 2_001);
            assert_eq!(e.commit_block[0].block_len, 4_096);
            assert_eq!(e.commit_block[0].locations[0].worker_id, 7);
        }
        other => panic!("unexpected add block entry: {:?}", other),
    }
    match &decoded.batch[5] {
        JournalEntry::CompleteFile(e) => {
            assert_eq!((e.op_id, e.rpc_id), (6, 6));
            assert_eq!((e.file.id, e.file.mtime), (104, 1_004));
            assert_eq!(e.commit_blocks[0].block_id, 2_001);
            assert_eq!(e.commit_blocks[0].block_len, 8_192);
        }
        other => panic!("unexpected complete file entry: {:?}", other),
    }
    match &decoded.batch[6] {
        JournalEntry::Rename(e) => {
            assert_eq!((e.op_id, e.rpc_id), (7, 7));
            assert_eq!((e.src.as_str(), e.dst.as_str()), ("/src", "/dst"));
            assert_eq!((e.mtime, e.flags), (11, 2));
            assert_eq!((e.src_inode_id, e.dst_inode_id), (100, 200));
        }
        other => panic!("unexpected rename entry: {:?}", other),
    }
    match &decoded.batch[7] {
        JournalEntry::Delete(e) => {
            assert_eq!((e.op_id, e.rpc_id, e.mtime), (8, 8, 12));
            assert_eq!(e.path, "/deleted");
        }
        other => panic!("unexpected delete entry: {:?}", other),
    }
    match &decoded.batch[8] {
        JournalEntry::Mount(m) => assert_eq!(m.info, mount_info()),
        other => panic!("unexpected mount entry: {:?}", other),
    }
    match &decoded.batch[9] {
        JournalEntry::UnMount(e) => assert_eq!((e.op_id, e.rpc_id, e.id), (10, 10, 5)),
        other => panic!("unexpected unmount entry: {:?}", other),
    }
    match &decoded.batch[10] {
        JournalEntry::SetAttr(e) => {
            assert_eq!((e.op_id, e.rpc_id), (11, 11));
            assert_eq!(e.path, "/dir/file");
            assert_eq!(e.opts.replicas, Some(2));
            assert_eq!(e.opts.owner.as_deref(), Some("alice"));
            assert_eq!(e.opts.group.as_deref(), Some("staff"));
            assert_eq!(e.opts.mode, Some(0o640));
            assert_eq!(e.opts.atime, Some(1_100));
            assert_eq!(e.opts.mtime, Some(1_200));
            assert_eq!(e.opts.ttl_ms, Some(5_000));
            assert_eq!(e.opts.ttl_action, Some(TtlAction::Delete));
            assert_eq!(
                e.opts.add_x_attr.get("user.tag").map(Vec::as_slice),
                Some(&b"v"[..])
            );
            assert_eq!(e.opts.remove_x_attr, vec!["user.old".to_string()]);
            assert_eq!(e.opts.ufs_mtime, Some(1_300));
        }
        other => panic!("unexpected setattr entry: {:?}", other),
    }
    match &decoded.batch[11] {
        JournalEntry::Symlink(e) => {
            assert_eq!((e.op_id, e.rpc_id), (12, 12));
            assert_eq!(e.link, "/target");
            assert_eq!((e.new_inode.id, e.new_inode.mtime), (105, 1_005));
            assert!(e.force);
        }
        other => panic!("unexpected symlink entry: {:?}", other),
    }
    match &decoded.batch[12] {
        JournalEntry::Link(e) => {
            assert_eq!((e.op_id, e.rpc_id, e.mtime), (13, 13, 13));
            assert_eq!((e.src_path.as_str(), e.dst_path.as_str()), ("/src", "/dst"));
        }
        other => panic!("unexpected link entry: {:?}", other),
    }
    match &decoded.batch[13] {
        JournalEntry::SetLocks(e) => {
            assert_eq!((e.op_id, e.rpc_id, e.ino), (14, 14, 106));
            assert_eq!(e.locks[0].client_id, "client-1");
            assert_eq!(e.locks[0].owner_id, 7);
            assert_eq!((e.locks[0].pid, e.locks[0].acquire_time), (42, 9));
            assert_eq!(e.locks[0].lock_flags, LockFlags::Flock);
            assert_eq!((e.locks[0].start, e.locks[0].end), (0, 100));
            assert!(!e.locks[0].session_owned);
        }
        other => panic!("unexpected set locks entry: {:?}", other),
    }
    match &decoded.batch[14] {
        JournalEntry::Free(e) => {
            assert_eq!((e.op_id, e.rpc_id, e.mtime), (15, 15, 14));
            assert_eq!(e.path, "/free");
            assert!(e.recursive);
        }
        other => panic!("unexpected free entry: {:?}", other),
    }
    match &decoded.batch[15] {
        JournalEntry::UfsApplied(e) => {
            assert_eq!((e.op_id, e.rpc_id, e.term, e.index), (16, 16, 3, 77));
        }
        other => panic!("unexpected ufs applied entry: {:?}", other),
    }
    match &decoded.batch[16] {
        JournalEntry::Snapshot(e) => {
            assert_eq!((e.op_id, e.rpc_id, e.node_id), (17, 17, 1));
            assert_eq!(e.dir, "/snapshot");
        }
        other => panic!("unexpected snapshot entry: {:?}", other),
    }
    match &decoded.batch[17] {
        JournalEntry::CacheInvalidation(e) => {
            assert_eq!(e.inodes.len(), 3);
            assert!(matches!(e.inodes[0], InodeView::File(_)));
            assert!(matches!(e.inodes[1], InodeView::Dir(_)));
            assert!(matches!(e.inodes[2], InodeView::FileEntry(_)));
        }
        other => panic!("unexpected cache invalidation entry: {:?}", other),
    }

    match &decoded.batch[18] {
        JournalEntry::TrashPurge(e) => {
            assert_eq!(e.entry, "trash-1");
            assert_eq!(e.mtime, 19);
            assert_eq!(
                e.paths,
                vec!["/trash/a".to_string(), "/trash/b".to_string()]
            );
        }
        other => panic!("unexpected trash purge entry: {:?}", other),
    }
    match &decoded.batch[19] {
        JournalEntry::ReleaseWriter(e) => {
            assert_eq!(e.ino, 110);
            assert_eq!(e.client, "client-x");
        }
        other => panic!("unexpected release writer entry: {:?}", other),
    }
    match &decoded.batch[20] {
        JournalEntry::FencedAddBlock(e) => {
            assert_eq!((e.blocks[0].id, e.blocks[0].len), (2_002, 4_096));
            assert_eq!(e.commit_block[0].expected_revision, Some(3));
        }
        other => panic!("unexpected fenced add block entry: {:?}", other),
    }
    match &decoded.batch[21] {
        JournalEntry::FencedCompleteFile(e) => {
            assert_eq!((e.file.id, e.file.mtime), (105, 1_005));
            assert_eq!(e.commit_blocks[0].expected_revision, Some(4));
        }
        other => panic!("unexpected fenced complete file entry: {:?}", other),
    }
    match &decoded.batch[22] {
        JournalEntry::ReplicaLocation(e) => {
            assert_eq!((e.block_id, e.commit_revision), (2_002, 4));
            assert_eq!(e.location.worker_id, 9);
        }
        other => panic!("unexpected replica location entry: {:?}", other),
    }

    // Re-encoding must reproduce the same bytes, so every field (including
    // the codec-backed inode/block payloads) survives the round trip.
    assert_eq!(bytes, decoded.encode().unwrap());
}

#[test]
fn batch_next_clears_entries_and_advances_seq() {
    let mut batch = all_variant_batch();
    let bytes = batch.encode().unwrap();

    // Encoding is non-destructive: a failed propose retries the same entries.
    assert_eq!(bytes, batch.encode().unwrap());

    batch.next();
    assert_eq!(batch.seq_id, 901);
    assert!(batch.is_empty(), "flushed entries must not be re-proposed");
}

#[test]
fn protobuf_record_with_corrupt_payload_is_rejected() {
    // Invalid wire type must fail loudly.
    assert!(JournalBatch::decode(&[0xFF, 0xFF, 0xFF]).is_err());

    // Empty payload is rejected even though prost would decode it into a
    // default batch: `encode` always writes at least the `seq_id` field.
    assert!(JournalBatch::decode(&[]).is_err());
}

#[test]
fn entries_with_multi_entry_maps_round_trip() {
    // Entry payloads keep native protobuf `map` fields (`HashMap`), whose entry
    // order is not preserved across encode/decode; the values must still match.
    let mut info = mount_info();
    for i in 0..8 {
        info.properties
            .insert(format!("fs.oss.key-{i}"), format!("value-{i}"));
    }
    let mut opts = set_attr_opts();
    for i in 0..8 {
        opts.add_x_attr
            .insert(format!("user.tag-{i}"), vec![i as u8]);
    }

    let batch = JournalBatch {
        seq_id: 901,
        batch: vec![
            JournalEntry::Mount(MountEntry {
                op_id: 1,
                rpc_id: 1,
                info: info.clone(),
            }),
            JournalEntry::SetAttr(SetAttrEntry {
                op_id: 2,
                rpc_id: 2,
                path: "/dir/file".to_string(),
                opts: opts.clone(),
            }),
        ],
    };

    let decoded = JournalBatch::decode(&batch.encode().unwrap()).unwrap();
    match &decoded.batch[0] {
        JournalEntry::Mount(m) => assert_eq!(m.info, info),
        other => panic!("unexpected mount entry: {:?}", other),
    }
    match &decoded.batch[1] {
        JournalEntry::SetAttr(e) => {
            assert_eq!(e.opts.add_x_attr, opts.add_x_attr);
            assert_eq!(e.opts.owner, opts.owner);
        }
        other => panic!("unexpected setattr entry: {:?}", other),
    }
}

#[test]
fn bincode_record_is_rejected() {
    // Legacy bincode `JournalBatch { seq_id: 1, batch: vec![] }`: u64 LE seq
    // id followed by a zero-length Vec. The first byte decodes to protobuf
    // field number 0, which is never a valid tag.
    let legacy = [1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    assert!(JournalBatch::decode(&legacy).is_err());
}
