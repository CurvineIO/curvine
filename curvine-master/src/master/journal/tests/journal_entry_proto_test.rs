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
    AccessMode, BlockLocation, CommitBlock, FileGen, FileLock, LockFlags, LockType, MountInfo,
    ProtoUtils, Provider, SetAttrOpts, SetAttrOptsBuilder, StorageType, TtlAction, TtlType,
    WriteType,
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
        ttl_type: TtlType::Atime,
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
        expect_gen: None,
    }
}

/// Builds a batch carrying every `JournalEntry` variant with values that
/// exercise every protobuf branch and every codec-backed `bytes` field.
fn all_variant_batch() -> JournalBatch {
    let commit = |block_id: i64, block_len: i64| CommitBlock {
        block_id,
        block_len,
        locations: vec![BlockLocation::with_id(7)],
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
        ],
    }
}

#[test]
fn protobuf_batch_round_trips_all_entry_variants() {
    let mut batch = all_variant_batch();
    let bytes = batch.encode().unwrap();

    let mut decoded = JournalBatch::decode(&bytes).unwrap();
    assert_eq!(decoded.seq_id, 900);
    assert_eq!(decoded.len(), 18);
    let op_ids: Vec<u64> = decoded.batch.iter().map(JournalEntry::op_id).collect();
    assert_eq!(op_ids, (1..=18).collect::<Vec<_>>());

    // Spot-check the shared protobuf models and the codec-backed inode list:
    // a field dropped symmetrically by encode and decode would not change the
    // batch structure, so assert the values themselves.
    match &decoded.batch[8] {
        JournalEntry::Mount(m) => assert_eq!(m.info, mount_info()),
        other => panic!("unexpected mount entry: {:?}", other),
    }
    match &decoded.batch[10] {
        JournalEntry::SetAttr(e) => {
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
    match &decoded.batch[17] {
        JournalEntry::CacheInvalidation(e) => {
            assert_eq!(e.inodes.len(), 3);
            assert!(matches!(e.inodes[0], InodeView::File(_)));
            assert!(matches!(e.inodes[1], InodeView::Dir(_)));
            assert!(matches!(e.inodes[2], InodeView::FileEntry(_)));
        }
        other => panic!("unexpected cache invalidation entry: {:?}", other),
    }

    // Re-encoding must reproduce the same bytes, so every field (including
    // the codec-backed inode/block payloads) survives the round trip.
    assert_eq!(bytes, decoded.encode().unwrap());
}

#[test]
fn set_attr_proto_round_trips_the_generation_guard() {
    // The guard travels on the RPC but is stripped by the leader before the
    // journal entry is built (`fs_dir::set_attr` calls `take()`), so replay
    // stays unconditional. This pins the field mapping itself.
    let gen = FileGen::new(9_001, 2_147_483_648, 1_000, 77);
    let opts = SetAttrOptsBuilder::new()
        .ufs_mtime(1_300)
        .expect_gen(gen)
        .build();

    let back = ProtoUtils::set_attr_opts_from_pb(ProtoUtils::set_attr_opts_to_pb(opts));

    assert_eq!(back.ufs_mtime, Some(1_300));
    assert_eq!(back.expect_gen, Some(gen));
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
fn bincode_record_is_rejected() {
    // Legacy bincode `JournalBatch { seq_id: 1, batch: vec![] }`: u64 LE seq
    // id followed by a zero-length Vec. The first byte decodes to protobuf
    // field number 0, which is never a valid tag.
    let legacy = [1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    assert!(JournalBatch::decode(&legacy).is_err());
}
