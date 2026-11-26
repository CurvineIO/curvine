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

use crate::master::meta::inode::{InodeDir, InodeFile, InodeView};
use crate::master::meta::BlockMeta;
use crate::master::MetadataCodec;
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_model::{
    BlockLocation, CommitBlock, FileLock, MountInfo, ProtoUtils, SetAttrOpts, StorageType,
};
use curvine_proto::*;
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MkdirEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) dir: InodeDir,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ReopenFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OverWriteFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
}

// Apply for a new block
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AddBlockEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) blocks: Vec<BlockMeta>,
    pub(crate) commit_block: Vec<CommitBlock>,
}

// File writing is completed.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CompleteFileEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) file: InodeFile,
    pub(crate) commit_blocks: Vec<CommitBlock>,
}

// Rename
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RenameEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) src: String,
    pub(crate) dst: String,
    pub(crate) mtime: i64,
    pub(crate) flags: u32,
    /// Pre-exchange inode ids for idempotent EXCHANGE replay (0 when absent / legacy).
    pub(crate) src_inode_id: i64,
    pub(crate) dst_inode_id: i64,
}

// delete
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeleteEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
}

// One bounded batch of a trash purge. `Delete` replays as a whole-subtree
// removal, so a purge that releases the namespace lock between batches needs
// its own entry: it names the trash entry it belongs to (for logging and
// completion) and the exact paths removed by this batch. Paths are removed
// leaf-first by the producer, which keeps every batch's work bounded and lets
// replay skip paths that are already gone.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TrashPurgeEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) entry: String,
    pub(crate) mtime: i64,
    pub(crate) paths: Vec<String>,
}

// mount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) info: MountInfo,
}

// umount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnMountEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) id: u32,
}

// set attr
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetAttrEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) opts: SetAttrOpts,
}

// symlink
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SymlinkEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) link: String,
    pub(crate) new_inode: InodeFile,
    pub(crate) force: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LinkEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    /// Link creation time, reused during replay for parent mtime and inode ctime.
    pub(crate) mtime: i64,
    pub(crate) src_path: String,
    pub(crate) dst_path: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SetLocksEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) ino: i64,
    pub(crate) locks: Vec<FileLock>,
}

/// Removes exactly one client's writer marker from one inode (#1880).
///
/// `client` is required: another client may be writing the same file, and clearing
/// the whole `WriteFeature` would mark that file complete while it is still being
/// written.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ReleaseWriterEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) ino: i64,
    pub(crate) client: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FreeEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
    #[serde(default)]
    pub(crate) recursive: bool,
}

/// Clears an unusable Curvine cache copy while preserving the file's UFS
/// metadata. The full inode metadata is recorded so followers can apply the
/// same state transition without consulting their potentially stale locations.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheInvalidationEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) inodes: Vec<InodeView>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UfsAppliedEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) term: u64,
    pub(crate) index: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SnapshotEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) node_id: u64,
    pub(crate) dir: String,
}

/// Adds an authenticated repair to the authority of one committed version.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ReplicaLocationEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) block_id: i64,
    pub(crate) location: BlockLocation,
    pub(crate) commit_revision: u64,
}

/// One durable metadata mutation.
///
/// Entries travel as the `JournalEntryProto` oneof in
/// `curvine-proto/journal.proto`, wrapped in a `JournalBatchProto` envelope;
/// heavy inode metadata rides inside it as opaque `curvine-codec` records.
/// Variants are append-only: a new variant takes a fresh oneof field number
/// and stays last in this enum, and the field numbers and codec layouts of
/// released variants never change. A journal written by a newer master cannot
/// be replayed by an older one once it contains a variant the binary does not
/// know. Checkpointing entries away alone does not make a rollback safe:
/// older binaries ignore the replica authority stored in the checkpoint by
/// the fenced variants and can resurrect rejected replicas.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum JournalEntry {
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
    SetLocks(SetLocksEntry),
    Free(FreeEntry),
    UfsApplied(UfsAppliedEntry),
    Snapshot(SnapshotEntry),
    // Protobuf field numbers live in journal.proto: append new variants there
    // with a fresh field number and keep them last in this enum.
    CacheInvalidation(CacheInvalidationEntry),
    TrashPurge(TrashPurgeEntry),
    /// Drop one client's writer marker (#1880). Emitted by the session reaper when
    /// a client that was writing a file is gone; the journal entry is what makes
    /// "the file is complete again" durable and replayable.
    ReleaseWriter(ReleaseWriterEntry),
    ReplicaLocation(ReplicaLocationEntry),
    /// New writers emit these variants; old AddBlock/CompleteFile remain legacy discovery.
    FencedAddBlock(AddBlockEntry),
    FencedCompleteFile(CompleteFileEntry),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct JournalBatch {
    pub(crate) seq_id: u64,
    pub(crate) batch: Vec<JournalEntry>,
}

impl JournalBatch {
    /// Encodes the batch as a `JournalBatchProto` payload. Heavy inode
    /// metadata travels as `MetadataCodec` bytes inside the protobuf.
    ///
    /// The batch is left intact, so a failed propose can be retried with the
    /// same entries and origin.
    pub fn encode(&self) -> CommonResult<Vec<u8>> {
        let mut entries = Vec::with_capacity(self.batch.len());
        for entry in &self.batch {
            entries.push(entry.to_proto()?);
        }

        Ok(JournalBatchProto {
            seq_id: self.seq_id,
            batch: entries,
        }
        .encode_to_vec())
    }

    /// Decodes one record written by [`Self::encode`].
    pub(crate) fn decode(bytes: &[u8]) -> CommonResult<Self> {
        // `encode` always writes at least the `seq_id` field, so an empty
        // payload is structurally invalid; prost would otherwise fill the
        // required field with a default and accept it.
        if bytes.is_empty() {
            return err_box!("journal record has no protobuf payload");
        }
        Self::from_proto(JournalBatchProto::decode(bytes)?)
    }

    fn from_proto(proto: JournalBatchProto) -> CommonResult<Self> {
        let mut batch = Vec::with_capacity(proto.batch.len());
        for entry in proto.batch {
            batch.push(JournalEntry::from_proto(entry)?);
        }

        Ok(Self {
            seq_id: proto.seq_id,
            batch,
        })
    }

    pub fn new(seq_id: u64) -> Self {
        Self {
            seq_id,
            batch: Vec::new(),
        }
    }

    pub fn push(&mut self, entry: JournalEntry) {
        self.batch.push(entry)
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn next(&mut self) {
        self.seq_id += 1;
        self.batch.clear();
    }
}

impl JournalEntry {
    /// Serialized byte size of this entry (the payload it contributes to a
    /// `JournalBatch`). Used by the sender to bound one Propose frame by
    /// bytes, not just op count.
    pub fn encode_size(&self) -> CommonResult<usize> {
        Ok(self.to_proto()?.encoded_len())
    }

    pub fn op_id(&self) -> u64 {
        match self {
            JournalEntry::Mkdir(e) => e.op_id,
            JournalEntry::CreateFile(e) => e.op_id,
            JournalEntry::ReopenFile(e) => e.op_id,
            JournalEntry::OverWriteFile(e) => e.op_id,
            JournalEntry::AddBlock(e) | JournalEntry::FencedAddBlock(e) => e.op_id,
            JournalEntry::CompleteFile(e) | JournalEntry::FencedCompleteFile(e) => e.op_id,
            JournalEntry::Rename(e) => e.op_id,
            JournalEntry::Delete(e) => e.op_id,
            JournalEntry::Mount(e) => e.op_id,
            JournalEntry::UnMount(e) => e.op_id,
            JournalEntry::SetAttr(e) => e.op_id,
            JournalEntry::Symlink(e) => e.op_id,
            JournalEntry::Link(e) => e.op_id,
            JournalEntry::SetLocks(e) => e.op_id,
            JournalEntry::Free(e) => e.op_id,
            JournalEntry::CacheInvalidation(e) => e.op_id,
            JournalEntry::UfsApplied(e) => e.op_id,
            JournalEntry::Snapshot(e) => e.op_id,
            JournalEntry::TrashPurge(e) => e.op_id,
            JournalEntry::ReleaseWriter(e) => e.op_id,
            JournalEntry::ReplicaLocation(e) => e.op_id,
        }
    }

    pub fn rpc_id(&self) -> i64 {
        match self {
            JournalEntry::Mkdir(e) => e.rpc_id,
            JournalEntry::CreateFile(e) => e.rpc_id,
            JournalEntry::ReopenFile(e) => e.rpc_id,
            JournalEntry::OverWriteFile(e) => e.rpc_id,
            JournalEntry::AddBlock(e) | JournalEntry::FencedAddBlock(e) => e.rpc_id,
            JournalEntry::CompleteFile(e) | JournalEntry::FencedCompleteFile(e) => e.rpc_id,
            JournalEntry::Rename(e) => e.rpc_id,
            JournalEntry::Delete(e) => e.rpc_id,
            JournalEntry::Mount(e) => e.rpc_id,
            JournalEntry::UnMount(e) => e.rpc_id,
            JournalEntry::SetAttr(e) => e.rpc_id,
            JournalEntry::Symlink(e) => e.rpc_id,
            JournalEntry::Link(e) => e.rpc_id,
            JournalEntry::SetLocks(e) => e.rpc_id,
            JournalEntry::Free(e) => e.rpc_id,
            JournalEntry::CacheInvalidation(e) => e.rpc_id,
            JournalEntry::UfsApplied(e) => e.rpc_id,
            JournalEntry::Snapshot(e) => e.rpc_id,
            JournalEntry::TrashPurge(e) => e.rpc_id,
            JournalEntry::ReleaseWriter(e) => e.rpc_id,
            JournalEntry::ReplicaLocation(e) => e.rpc_id,
        }
    }

    pub fn inode_id(&self) -> Option<i64> {
        match self {
            JournalEntry::Mkdir(e) => Some(e.dir.id),
            JournalEntry::CreateFile(e) => Some(e.file.id),
            JournalEntry::ReopenFile(e) => Some(e.file.id),
            JournalEntry::OverWriteFile(e) => Some(e.file.id),
            JournalEntry::CompleteFile(e) | JournalEntry::FencedCompleteFile(e) => Some(e.file.id),
            JournalEntry::Symlink(e) => Some(e.new_inode.id),
            JournalEntry::SetLocks(e) => Some(e.ino),
            JournalEntry::ReleaseWriter(e) => Some(e.ino),
            _ => None,
        }
    }

    pub fn allocated_inode_id(&self) -> Option<i64> {
        match self {
            JournalEntry::Mkdir(e) => Some(e.dir.id),
            JournalEntry::CreateFile(e) => Some(e.file.id),
            JournalEntry::Symlink(e) => Some(e.new_inode.id),
            _ => None,
        }
    }

    fn to_proto(&self) -> CommonResult<JournalEntryProto> {
        use journal_entry_proto::Entry as PbEntry;

        let entry = match self {
            JournalEntry::Mkdir(e) => PbEntry::Mkdir(MkdirEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                dir: MetadataCodec::encode(&e.dir)?,
            }),

            JournalEntry::CreateFile(e) => PbEntry::CreateFile(CreateFileEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                file: MetadataCodec::encode(&e.file)?,
            }),

            JournalEntry::ReopenFile(e) => PbEntry::ReopenFile(ReopenFileEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                file: MetadataCodec::encode(&e.file)?,
            }),

            JournalEntry::OverWriteFile(e) => PbEntry::OverwriteFile(OverWriteFileEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                file: MetadataCodec::encode(&e.file)?,
            }),

            JournalEntry::AddBlock(e) => PbEntry::AddBlock(AddBlockEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                blocks: MetadataCodec::encode(&e.blocks)?,
                commit_block: e
                    .commit_block
                    .iter()
                    .cloned()
                    .map(ProtoUtils::commit_block_to_pb)
                    .collect(),
            }),

            JournalEntry::CompleteFile(e) => PbEntry::CompleteFile(CompleteFileEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                file: MetadataCodec::encode(&e.file)?,
                commit_blocks: e
                    .commit_blocks
                    .iter()
                    .cloned()
                    .map(ProtoUtils::commit_block_to_pb)
                    .collect(),
            }),

            JournalEntry::Rename(e) => PbEntry::Rename(RenameEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                src: e.src.clone(),
                dst: e.dst.clone(),
                mtime: e.mtime,
                flags: e.flags,
                src_inode_id: Some(e.src_inode_id),
                dst_inode_id: Some(e.dst_inode_id),
            }),

            JournalEntry::Delete(e) => PbEntry::Delete(DeleteEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                mtime: e.mtime,
            }),

            JournalEntry::Mount(e) => PbEntry::Mount(MountEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                info: ProtoUtils::mount_info_to_pb(e.info.clone()),
            }),

            JournalEntry::UnMount(e) => PbEntry::Unmount(UnMountEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                id: e.id,
            }),

            JournalEntry::SetAttr(e) => PbEntry::SetAttr(SetAttrEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                opts: ProtoUtils::set_attr_opts_to_pb(e.opts.clone()),
            }),

            JournalEntry::Symlink(e) => PbEntry::Symlink(SymlinkEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                link: e.link.clone(),
                new_inode: MetadataCodec::encode(&e.new_inode)?,
                force: e.force,
            }),

            JournalEntry::Link(e) => PbEntry::Link(LinkEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                mtime: e.mtime,
                src_path: e.src_path.clone(),
                dst_path: e.dst_path.clone(),
            }),

            JournalEntry::SetLocks(e) => PbEntry::SetLocks(SetLocksEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                ino: e.ino,
                locks: e
                    .locks
                    .iter()
                    .cloned()
                    .map(ProtoUtils::file_lock_to_pb)
                    .collect(),
            }),

            JournalEntry::Free(e) => PbEntry::Free(FreeEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                mtime: e.mtime,
                recursive: Some(e.recursive),
            }),

            JournalEntry::UfsApplied(e) => PbEntry::UfsApplied(UfsAppliedEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                term: e.term,
                index: e.index,
            }),

            JournalEntry::Snapshot(e) => PbEntry::Snapshot(SnapshotEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                node_id: e.node_id,
                dir: e.dir.clone(),
            }),

            JournalEntry::CacheInvalidation(e) => {
                PbEntry::CacheInvalidation(CacheInvalidationEntryProto {
                    op_id: e.op_id,
                    rpc_id: e.rpc_id,
                    inodes: MetadataCodec::encode(&e.inodes)?,
                })
            }

            JournalEntry::TrashPurge(e) => PbEntry::TrashPurge(TrashPurgeEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                entry: e.entry.clone(),
                mtime: e.mtime,
                paths: e.paths.clone(),
            }),

            JournalEntry::ReleaseWriter(e) => PbEntry::ReleaseWriter(ReleaseWriterEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                ino: e.ino,
                client: e.client.clone(),
            }),

            JournalEntry::FencedAddBlock(e) => PbEntry::FencedAddBlock(AddBlockEntryProto {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path.clone(),
                blocks: MetadataCodec::encode(&e.blocks)?,
                commit_block: e
                    .commit_block
                    .iter()
                    .cloned()
                    .map(ProtoUtils::commit_block_to_pb)
                    .collect(),
            }),

            JournalEntry::FencedCompleteFile(e) => {
                PbEntry::FencedCompleteFile(CompleteFileEntryProto {
                    op_id: e.op_id,
                    rpc_id: e.rpc_id,
                    path: e.path.clone(),
                    file: MetadataCodec::encode(&e.file)?,
                    commit_blocks: e
                        .commit_blocks
                        .iter()
                        .cloned()
                        .map(ProtoUtils::commit_block_to_pb)
                        .collect(),
                })
            }

            JournalEntry::ReplicaLocation(e) => {
                PbEntry::ReplicaLocation(ReplicaLocationEntryProto {
                    op_id: e.op_id,
                    rpc_id: e.rpc_id,
                    block_id: e.block_id,
                    location: ProtoUtils::block_location_to_pb(e.location.clone()),
                    commit_revision: e.commit_revision,
                })
            }
        };

        Ok(JournalEntryProto { entry: Some(entry) })
    }

    fn from_proto(proto: JournalEntryProto) -> CommonResult<Self> {
        use journal_entry_proto::Entry as PbEntry;

        let Some(entry) = proto.entry else {
            return err_box!("journal entry protobuf payload is missing");
        };

        Ok(match entry {
            PbEntry::Mkdir(e) => JournalEntry::Mkdir(MkdirEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                dir: MetadataCodec::decode(&e.dir)?,
            }),

            PbEntry::CreateFile(e) => JournalEntry::CreateFile(CreateFileEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                file: MetadataCodec::decode(&e.file)?,
            }),

            PbEntry::ReopenFile(e) => JournalEntry::ReopenFile(ReopenFileEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                file: MetadataCodec::decode(&e.file)?,
            }),

            PbEntry::OverwriteFile(e) => JournalEntry::OverWriteFile(OverWriteFileEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                file: MetadataCodec::decode(&e.file)?,
            }),

            PbEntry::AddBlock(e) => JournalEntry::AddBlock(AddBlockEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                blocks: MetadataCodec::decode(&e.blocks)?,
                commit_block: e
                    .commit_block
                    .into_iter()
                    .map(ProtoUtils::commit_block_from_pb)
                    .collect(),
            }),

            PbEntry::CompleteFile(e) => JournalEntry::CompleteFile(CompleteFileEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                file: MetadataCodec::decode(&e.file)?,
                commit_blocks: e
                    .commit_blocks
                    .into_iter()
                    .map(ProtoUtils::commit_block_from_pb)
                    .collect(),
            }),

            PbEntry::Rename(e) => JournalEntry::Rename(RenameEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                src: e.src,
                dst: e.dst,
                mtime: e.mtime,
                flags: e.flags,
                src_inode_id: e.src_inode_id.unwrap_or(0),
                dst_inode_id: e.dst_inode_id.unwrap_or(0),
            }),

            PbEntry::Delete(e) => JournalEntry::Delete(DeleteEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                mtime: e.mtime,
            }),

            PbEntry::Mount(e) => JournalEntry::Mount(MountEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                info: ProtoUtils::mount_info_from_pb(e.info),
            }),

            PbEntry::Unmount(e) => JournalEntry::UnMount(UnMountEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                id: e.id,
            }),

            PbEntry::SetAttr(e) => JournalEntry::SetAttr(SetAttrEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                opts: ProtoUtils::set_attr_opts_from_pb(e.opts),
            }),

            PbEntry::Symlink(e) => JournalEntry::Symlink(SymlinkEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                link: e.link,
                new_inode: MetadataCodec::decode(&e.new_inode)?,
                force: e.force,
            }),

            PbEntry::Link(e) => JournalEntry::Link(LinkEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                mtime: e.mtime,
                src_path: e.src_path,
                dst_path: e.dst_path,
            }),

            PbEntry::SetLocks(e) => JournalEntry::SetLocks(SetLocksEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                ino: e.ino,
                locks: e
                    .locks
                    .into_iter()
                    .map(ProtoUtils::file_lock_from_pb)
                    .collect(),
            }),

            PbEntry::Free(e) => JournalEntry::Free(FreeEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                mtime: e.mtime,
                recursive: e.recursive.unwrap_or(false),
            }),

            PbEntry::UfsApplied(e) => JournalEntry::UfsApplied(UfsAppliedEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                term: e.term,
                index: e.index,
            }),

            PbEntry::Snapshot(e) => JournalEntry::Snapshot(SnapshotEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                node_id: e.node_id,
                dir: e.dir,
            }),

            PbEntry::CacheInvalidation(e) => {
                JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                    op_id: e.op_id,
                    rpc_id: e.rpc_id,
                    inodes: MetadataCodec::decode(&e.inodes)?,
                })
            }

            PbEntry::TrashPurge(e) => JournalEntry::TrashPurge(TrashPurgeEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                entry: e.entry,
                mtime: e.mtime,
                paths: e.paths,
            }),

            PbEntry::ReleaseWriter(e) => JournalEntry::ReleaseWriter(ReleaseWriterEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                ino: e.ino,
                client: e.client,
            }),

            PbEntry::FencedAddBlock(e) => JournalEntry::FencedAddBlock(AddBlockEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                blocks: MetadataCodec::decode(&e.blocks)?,
                commit_block: e
                    .commit_block
                    .into_iter()
                    .map(ProtoUtils::commit_block_from_pb)
                    .collect(),
            }),

            PbEntry::FencedCompleteFile(e) => JournalEntry::FencedCompleteFile(CompleteFileEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                path: e.path,
                file: MetadataCodec::decode(&e.file)?,
                commit_blocks: e
                    .commit_blocks
                    .into_iter()
                    .map(ProtoUtils::commit_block_from_pb)
                    .collect(),
            }),

            PbEntry::ReplicaLocation(e) => JournalEntry::ReplicaLocation(ReplicaLocationEntry {
                op_id: e.op_id,
                rpc_id: e.rpc_id,
                block_id: e.block_id,
                location: ProtoUtils::block_location_from_pb(e.location),
                commit_revision: e.commit_revision,
            }),
        })
    }
}

impl JournalBatch {
    /// Decode one `bincode` batch written before the codec switch.
    ///
    /// The journal store is the last place that still holds `bincode` payloads:
    /// the offline upgrade tool reads them to rewrite the log as protobuf.
    /// `bincode` is positional and not self-describing, so a payload is only
    /// readable with the exact schema that wrote it, and a journal directory can
    /// hold batches from several builds at once. Four payloads changed over
    /// time:
    ///
    /// * `MountInfo` gained `auto_cache`/`access_mode`, then `write_cache`;
    /// * `FileLock` gained `session_owned` (#1878);
    /// * `RenameEntry` gained the pre-exchange inode ids;
    /// * `FreeEntry` gained `recursive`.
    ///
    /// A layout is tried as a whole batch, newest first. A per-entry fallback
    /// cannot work: nothing marks where an entry ends, so a mismatch would only
    /// be noticed at the wrong offset.
    pub fn decode_legacy_bincode(bytes: &[u8]) -> CommonResult<Self> {
        macro_rules! try_layout {
            ($mount:ty, $rename:ty, $free:ty, $locks:ty) => {
                if let Ok(batch) = legacy::Batch::<$mount, $rename, $free, $locks>::read(bytes) {
                    return batch.into_current();
                }
            };
        }

        // The current catalog first: a record this build wrote must never be
        // read with an older layout, which would drop its trailing fields.
        try_layout!(MountEntry, RenameEntry, FreeEntry, SetLocksEntry);
        // Pre-#1878: locks without the trailing `session_owned` flag.
        try_layout!(MountEntry, RenameEntry, FreeEntry, legacy::SetLocks);
        // ... and before the exchange inode ids and `recursive` as well.
        try_layout!(
            MountEntry,
            legacy::RenameBeforeExchange,
            legacy::FreeBeforeRecursive,
            legacy::SetLocks
        );
        // Before `write_cache`.
        try_layout!(
            legacy::MountBeforeWriteCache,
            RenameEntry,
            FreeEntry,
            legacy::SetLocks
        );
        try_layout!(
            legacy::MountBeforeWriteCache,
            legacy::RenameBeforeExchange,
            legacy::FreeBeforeRecursive,
            legacy::SetLocks
        );
        // Oldest: before `auto_cache`/`access_mode`.
        try_layout!(
            legacy::MountBeforeAutoCache,
            RenameEntry,
            FreeEntry,
            legacy::SetLocks
        );
        try_layout!(
            legacy::MountBeforeAutoCache,
            legacy::RenameBeforeExchange,
            legacy::FreeBeforeRecursive,
            legacy::SetLocks
        );

        err_box!("journal batch matches no persisted bincode layout")
    }
}

/// Older `bincode` layouts of the payloads that changed since the codec
/// switch.
///
/// Only those payloads are mirrored; every other variant keeps its current
/// type, so one generic enum serves every generation.
mod legacy {
    use super::*;
    use curvine_model::{AccessMode, LockFlags, LockType, Provider, TtlAction, WriteType};
    use curvine_runtime::common::SerdeUtils;
    use std::collections::HashMap;

    /// Fixed-int encoding with trailing bytes rejected, matching what the old
    /// writers produced (`bincode::serialize`) except for the trailing check: a
    /// payload in another layout leaves bytes over instead of being accepted.
    fn strict<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> CommonResult<T> {
        let value: T = try_err!(SerdeUtils::deserialize_strict::<T>(bytes));
        Ok(value)
    }

    /// `FileLock` before #1878: no trailing `session_owned` flag.
    #[derive(Deserialize)]
    pub(super) struct Lock {
        client_id: String,
        owner_id: u64,
        pid: u32,
        acquire_time: u64,
        lock_type: LockType,
        lock_flags: LockFlags,
        start: u64,
        end: u64,
    }

    impl From<Lock> for FileLock {
        fn from(lock: Lock) -> Self {
            Self {
                client_id: lock.client_id,
                owner_id: lock.owner_id,
                pid: lock.pid,
                acquire_time: lock.acquire_time,
                lock_type: lock.lock_type,
                lock_flags: lock.lock_flags,
                start: lock.start,
                end: lock.end,
                // Acquired by a client that never sent a heartbeat, so it stays
                // on the legacy TTL path.
                session_owned: false,
            }
        }
    }

    #[derive(Deserialize)]
    pub(super) struct SetLocks {
        op_id: u64,
        rpc_id: i64,
        ino: i64,
        locks: Vec<Lock>,
    }

    impl From<SetLocks> for SetLocksEntry {
        fn from(entry: SetLocks) -> Self {
            Self {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                ino: entry.ino,
                locks: entry.locks.into_iter().map(FileLock::from).collect(),
            }
        }
    }

    /// `MountInfo` before `write_cache` was appended.
    #[derive(Deserialize)]
    struct InfoBeforeWriteCache {
        cv_path: String,
        ufs_path: String,
        mount_id: u32,
        properties: HashMap<String, String>,
        ttl_ms: i64,
        ttl_action: TtlAction,
        read_verify_ufs: bool,
        storage_type: Option<StorageType>,
        block_size: Option<i64>,
        replicas: Option<i32>,
        write_type: WriteType,
        provider: Option<Provider>,
        auto_cache: bool,
        access_mode: AccessMode,
    }

    impl InfoBeforeWriteCache {
        fn into_current(self) -> MountInfo {
            MountInfo {
                cv_path: self.cv_path,
                ufs_path: self.ufs_path,
                mount_id: self.mount_id,
                properties: self.properties,
                ttl_ms: self.ttl_ms,
                ttl_action: self.ttl_action,
                read_verify_ufs: self.read_verify_ufs,
                storage_type: self.storage_type,
                block_size: self.block_size,
                replicas: self.replicas,
                write_type: self.write_type,
                provider: self.provider,
                auto_cache: self.auto_cache,
                access_mode: self.access_mode,
                write_cache: false,
            }
        }
    }

    /// The oldest layout: it ends at `provider`, before caching was
    /// configurable.
    #[derive(Deserialize)]
    struct InfoBeforeAutoCache {
        cv_path: String,
        ufs_path: String,
        mount_id: u32,
        properties: HashMap<String, String>,
        ttl_ms: i64,
        ttl_action: TtlAction,
        read_verify_ufs: bool,
        storage_type: Option<StorageType>,
        block_size: Option<i64>,
        replicas: Option<i32>,
        write_type: WriteType,
        provider: Option<Provider>,
    }

    impl InfoBeforeAutoCache {
        fn into_current(self) -> MountInfo {
            MountInfo {
                cv_path: self.cv_path,
                ufs_path: self.ufs_path,
                mount_id: self.mount_id,
                properties: self.properties,
                ttl_ms: self.ttl_ms,
                ttl_action: self.ttl_action,
                read_verify_ufs: self.read_verify_ufs,
                storage_type: self.storage_type,
                block_size: self.block_size,
                replicas: self.replicas,
                write_type: self.write_type,
                provider: self.provider,
                auto_cache: true,
                access_mode: AccessMode::ReadOnly,
                write_cache: false,
            }
        }
    }

    macro_rules! mount_entry {
        ($name:ident, $info:ty) => {
            #[derive(Deserialize)]
            pub(super) struct $name {
                op_id: u64,
                rpc_id: i64,
                info: $info,
            }

            impl From<$name> for MountEntry {
                fn from(entry: $name) -> Self {
                    Self {
                        op_id: entry.op_id,
                        rpc_id: entry.rpc_id,
                        info: entry.info.into_current(),
                    }
                }
            }
        };
    }

    mount_entry!(MountBeforeWriteCache, InfoBeforeWriteCache);
    mount_entry!(MountBeforeAutoCache, InfoBeforeAutoCache);

    /// `RenameEntry` before the pre-exchange inode ids.
    #[derive(Deserialize)]
    pub(super) struct RenameBeforeExchange {
        op_id: u64,
        rpc_id: i64,
        src: String,
        dst: String,
        mtime: i64,
        flags: u32,
    }

    impl From<RenameBeforeExchange> for RenameEntry {
        fn from(entry: RenameBeforeExchange) -> Self {
            Self {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                src: entry.src,
                dst: entry.dst,
                mtime: entry.mtime,
                flags: entry.flags,
                // No EXCHANGE ids were recorded; replay falls back to a plain
                // rename, which is what that build did.
                src_inode_id: 0,
                dst_inode_id: 0,
            }
        }
    }

    /// `FreeEntry` before `recursive` was appended.
    #[derive(Deserialize)]
    pub(super) struct FreeBeforeRecursive {
        op_id: u64,
        rpc_id: i64,
        path: String,
        mtime: i64,
    }

    impl From<FreeBeforeRecursive> for FreeEntry {
        fn from(entry: FreeBeforeRecursive) -> Self {
            Self {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                path: entry.path,
                mtime: entry.mtime,
                recursive: false,
            }
        }
    }

    /// [`JournalEntry`] with the four changed payloads left generic. Variant
    /// order must match: `bincode` encodes the variant as its positional index.
    #[derive(Deserialize)]
    #[serde(bound(
        deserialize = "Mount: Deserialize<'de>, Rename: Deserialize<'de>, Free: Deserialize<'de>, Locks: Deserialize<'de>"
    ))]
    pub(super) enum Entry<Mount, Rename, Free, Locks> {
        Mkdir(MkdirEntry),
        CreateFile(CreateFileEntry),
        ReopenFile(ReopenFileEntry),
        OverWriteFile(OverWriteFileEntry),
        AddBlock(AddBlockEntry),
        CompleteFile(CompleteFileEntry),
        Rename(Rename),
        Delete(DeleteEntry),
        Mount(Mount),
        UnMount(UnMountEntry),
        SetAttr(SetAttrEntry),
        Symlink(SymlinkEntry),
        Link(LinkEntry),
        SetLocks(Locks),
        Free(Free),
        UfsApplied(UfsAppliedEntry),
        Snapshot(SnapshotEntry),
        CacheInvalidation(CacheInvalidationEntry),
        TrashPurge(TrashPurgeEntry),
        ReleaseWriter(ReleaseWriterEntry),
    }

    impl<Mount, Rename, Free, Locks> Entry<Mount, Rename, Free, Locks>
    where
        Mount: Into<MountEntry>,
        Rename: Into<RenameEntry>,
        Free: Into<FreeEntry>,
        Locks: Into<SetLocksEntry>,
    {
        fn into_current(self) -> JournalEntry {
            match self {
                Entry::Mkdir(e) => JournalEntry::Mkdir(e),
                Entry::CreateFile(e) => JournalEntry::CreateFile(e),
                Entry::ReopenFile(e) => JournalEntry::ReopenFile(e),
                Entry::OverWriteFile(e) => JournalEntry::OverWriteFile(e),
                Entry::AddBlock(e) => JournalEntry::AddBlock(e),
                Entry::CompleteFile(e) => JournalEntry::CompleteFile(e),
                Entry::Rename(e) => JournalEntry::Rename(e.into()),
                Entry::Delete(e) => JournalEntry::Delete(e),
                Entry::Mount(e) => JournalEntry::Mount(e.into()),
                Entry::UnMount(e) => JournalEntry::UnMount(e),
                Entry::SetAttr(e) => JournalEntry::SetAttr(e),
                Entry::Symlink(e) => JournalEntry::Symlink(e),
                Entry::Link(e) => JournalEntry::Link(e),
                Entry::SetLocks(e) => JournalEntry::SetLocks(e.into()),
                Entry::Free(e) => JournalEntry::Free(e.into()),
                Entry::UfsApplied(e) => JournalEntry::UfsApplied(e),
                Entry::Snapshot(e) => JournalEntry::Snapshot(e),
                Entry::CacheInvalidation(e) => JournalEntry::CacheInvalidation(e),
                Entry::TrashPurge(e) => JournalEntry::TrashPurge(e),
                Entry::ReleaseWriter(e) => JournalEntry::ReleaseWriter(e),
            }
        }
    }

    #[derive(Deserialize)]
    #[serde(bound(
        deserialize = "Mount: Deserialize<'de>, Rename: Deserialize<'de>, Free: Deserialize<'de>, Locks: Deserialize<'de>"
    ))]
    pub(super) struct Batch<Mount, Rename, Free, Locks> {
        seq_id: u64,
        batch: Vec<Entry<Mount, Rename, Free, Locks>>,
    }

    impl<Mount, Rename, Free, Locks> Batch<Mount, Rename, Free, Locks>
    where
        Mount: serde::de::DeserializeOwned,
        Rename: serde::de::DeserializeOwned,
        Free: serde::de::DeserializeOwned,
        Locks: serde::de::DeserializeOwned,
    {
        pub(super) fn read(bytes: &[u8]) -> CommonResult<Self> {
            strict(bytes)
        }
    }

    impl<Mount, Rename, Free, Locks> Batch<Mount, Rename, Free, Locks>
    where
        Mount: Into<MountEntry>,
        Rename: Into<RenameEntry>,
        Free: Into<FreeEntry>,
        Locks: Into<SetLocksEntry>,
    {
        pub(super) fn into_current(self) -> CommonResult<JournalBatch> {
            Ok(JournalBatch {
                seq_id: self.seq_id,
                batch: self.batch.into_iter().map(Entry::into_current).collect(),
            })
        }
    }
}
