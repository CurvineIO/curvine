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
use crate::master::meta::{BlockMeta, LegacyFileLock};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{CommitBlock, FileLock, MountInfo, SetAttrOpts};
use curvine_runtime::common::SerdeUtils;
use log::debug;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

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
    // Keep new variants appended so existing journal discriminants remain stable.
    CacheInvalidation(CacheInvalidationEntry),
    TrashPurge(TrashPurgeEntry),
}

impl JournalEntry {
    pub fn op_id(&self) -> u64 {
        match self {
            JournalEntry::Mkdir(e) => e.op_id,
            JournalEntry::CreateFile(e) => e.op_id,
            JournalEntry::ReopenFile(e) => e.op_id,
            JournalEntry::OverWriteFile(e) => e.op_id,
            JournalEntry::AddBlock(e) => e.op_id,
            JournalEntry::CompleteFile(e) => e.op_id,
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
        }
    }

    pub fn rpc_id(&self) -> i64 {
        match self {
            JournalEntry::Mkdir(e) => e.rpc_id,
            JournalEntry::CreateFile(e) => e.rpc_id,
            JournalEntry::ReopenFile(e) => e.rpc_id,
            JournalEntry::OverWriteFile(e) => e.rpc_id,
            JournalEntry::AddBlock(e) => e.rpc_id,
            JournalEntry::CompleteFile(e) => e.rpc_id,
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
        }
    }

    pub fn inode_id(&self) -> Option<i64> {
        match self {
            JournalEntry::Mkdir(e) => Some(e.dir.id),
            JournalEntry::CreateFile(e) => Some(e.file.id),
            JournalEntry::ReopenFile(e) => Some(e.file.id),
            JournalEntry::OverWriteFile(e) => Some(e.file.id),
            JournalEntry::CompleteFile(e) => Some(e.file.id),
            JournalEntry::Symlink(e) => Some(e.new_inode.id),
            JournalEntry::SetLocks(e) => Some(e.ino),
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
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct JournalBatch {
    pub(crate) seq_id: u64,
    pub(crate) batch: Vec<JournalEntry>,
}

impl JournalBatch {
    pub(crate) fn deserialize_compat(bytes: &[u8]) -> CommonResult<Self> {
        match SerdeUtils::deserialize(bytes) {
            Ok(batch) => Ok(batch),
            Err(current_err) => {
                // The immediately preceding layout: every entry as today except a
                // `SetLocks` without `session_owned` (#1878).
                let pre_session_err = match deserialize_pre_session_batch(bytes) {
                    Ok(batch) => {
                        debug!(
                            "replaying journal batch with pre-session SetLocks schema, seq_id={}",
                            batch.seq_id
                        );
                        return Ok(batch.into());
                    }
                    Err(e) => e,
                };
                match deserialize_legacy_batch(bytes) {
                    Ok(batch) => {
                        debug!(
                            "replaying legacy journal batch with pre-extension entry schemas, seq_id={}",
                            batch.seq_id
                        );
                        Ok(batch.into())
                    }
                    Err(legacy_err) => err_box!(
                        "failed to deserialize journal batch with current, pre-session or legacy schemas: current={}, pre_session={}, legacy={}",
                        current_err,
                        pre_session_err,
                        legacy_err
                    ),
                }
            }
        }
    }

    pub fn new(seq_id: u64) -> Self {
        Self {
            seq_id,
            batch: vec![],
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

// bincode encodes struct fields positionally, so appending fields to RenameEntry
// cannot be made backward compatible with serde(default). Keep this schema only
// for replaying entries written before exchange inode ids were introduced.
#[derive(Deserialize)]
struct LegacyRenameEntry {
    op_id: u64,
    rpc_id: i64,
    src: String,
    dst: String,
    mtime: i64,
    flags: u32,
}

// FreeEntry::recursive was appended in #721. Old bincode entries do not have
// this field, so decoding them with the current type consumes the next entry's
// enum tag as a bool.
#[derive(Deserialize)]
struct LegacyFreeEntry {
    op_id: u64,
    rpc_id: i64,
    path: String,
    mtime: i64,
}

#[derive(Deserialize)]
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

/// `SetLocks` as written before session ownership (#1878): the lock payload has
/// no trailing `session_owned` flag. Batches carrying it decode through this
/// mirror, through [`PreSessionJournalEntry`] or [`LegacyJournalEntry`].
#[derive(Deserialize)]
struct LegacySetLocksEntry {
    op_id: u64,
    rpc_id: i64,
    ino: i64,
    locks: Vec<LegacyFileLock>,
}

impl From<LegacySetLocksEntry> for SetLocksEntry {
    fn from(entry: LegacySetLocksEntry) -> Self {
        Self {
            op_id: entry.op_id,
            rpc_id: entry.rpc_id,
            ino: entry.ino,
            locks: entry.locks.into_iter().map(Into::into).collect(),
        }
    }
}

/// Journal entries as written right before session ownership (#1878): identical
/// to [`JournalEntry`] in variant order and in every payload layout, except
/// that `SetLocks` carries locks without the trailing `session_owned` flag.
///
/// [`LegacyJournalEntry`] cannot stand in for it: that mirror also keeps the
/// pre-#721 `Free`/`Rename` layouts and lacks the variants appended since, so a
/// pre-#1878 batch with an old `SetLocks` next to `Free { recursive }`,
/// `CacheInvalidation` or `TrashPurge` would fail every decoder.
#[derive(Deserialize)]
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

#[derive(Deserialize)]
struct PreSessionJournalBatch {
    seq_id: u64,
    batch: Vec<PreSessionJournalEntry>,
}

fn deserialize_pre_session_batch(bytes: &[u8]) -> CommonResult<PreSessionJournalBatch> {
    let mut reader = Cursor::new(bytes);
    let batch = SerdeUtils::deserialize_from(&mut reader)?;
    if reader.position() != bytes.len() as u64 {
        return err_box!("pre-session journal batch has trailing bytes");
    }
    Ok(batch)
}

impl From<PreSessionJournalBatch> for JournalBatch {
    fn from(batch: PreSessionJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch.batch.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<PreSessionJournalEntry> for JournalEntry {
    fn from(entry: PreSessionJournalEntry) -> Self {
        match entry {
            PreSessionJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            PreSessionJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            PreSessionJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            PreSessionJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            PreSessionJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            PreSessionJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            PreSessionJournalEntry::Rename(entry) => Self::Rename(entry),
            PreSessionJournalEntry::Delete(entry) => Self::Delete(entry),
            PreSessionJournalEntry::Mount(entry) => Self::Mount(entry),
            PreSessionJournalEntry::UnMount(entry) => Self::UnMount(entry),
            PreSessionJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            PreSessionJournalEntry::Symlink(entry) => Self::Symlink(entry),
            PreSessionJournalEntry::Link(entry) => Self::Link(entry),
            PreSessionJournalEntry::SetLocks(entry) => Self::SetLocks(entry.into()),
            PreSessionJournalEntry::Free(entry) => Self::Free(entry),
            PreSessionJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            PreSessionJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
            PreSessionJournalEntry::CacheInvalidation(entry) => Self::CacheInvalidation(entry),
            PreSessionJournalEntry::TrashPurge(entry) => Self::TrashPurge(entry),
        }
    }
}

#[derive(Deserialize)]
struct LegacyJournalBatch {
    seq_id: u64,
    batch: Vec<LegacyJournalEntry>,
}

fn deserialize_legacy_batch(bytes: &[u8]) -> CommonResult<LegacyJournalBatch> {
    let mut reader = Cursor::new(bytes);
    let batch = SerdeUtils::deserialize_from(&mut reader)?;
    if reader.position() != bytes.len() as u64 {
        return err_box!("legacy journal batch has trailing bytes");
    }
    Ok(batch)
}

impl From<LegacyJournalBatch> for JournalBatch {
    fn from(batch: LegacyJournalBatch) -> Self {
        Self {
            seq_id: batch.seq_id,
            batch: batch.batch.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<LegacyJournalEntry> for JournalEntry {
    fn from(entry: LegacyJournalEntry) -> Self {
        match entry {
            LegacyJournalEntry::Mkdir(entry) => Self::Mkdir(entry),
            LegacyJournalEntry::CreateFile(entry) => Self::CreateFile(entry),
            LegacyJournalEntry::ReopenFile(entry) => Self::ReopenFile(entry),
            LegacyJournalEntry::OverWriteFile(entry) => Self::OverWriteFile(entry),
            LegacyJournalEntry::AddBlock(entry) => Self::AddBlock(entry),
            LegacyJournalEntry::CompleteFile(entry) => Self::CompleteFile(entry),
            LegacyJournalEntry::Rename(entry) => Self::Rename(RenameEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                src: entry.src,
                dst: entry.dst,
                mtime: entry.mtime,
                flags: entry.flags,
                src_inode_id: 0,
                dst_inode_id: 0,
            }),
            LegacyJournalEntry::Delete(entry) => Self::Delete(entry),
            LegacyJournalEntry::Mount(entry) => Self::Mount(entry),
            LegacyJournalEntry::UnMount(entry) => Self::UnMount(entry),
            LegacyJournalEntry::SetAttr(entry) => Self::SetAttr(entry),
            LegacyJournalEntry::Symlink(entry) => Self::Symlink(entry),
            LegacyJournalEntry::Link(entry) => Self::Link(entry),
            LegacyJournalEntry::SetLocks(entry) => Self::SetLocks(entry.into()),
            LegacyJournalEntry::Free(entry) => Self::Free(FreeEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                path: entry.path,
                mtime: entry.mtime,
                recursive: false,
            }),
            LegacyJournalEntry::UfsApplied(entry) => Self::UfsApplied(entry),
            LegacyJournalEntry::Snapshot(entry) => Self::Snapshot(entry),
        }
    }
}
