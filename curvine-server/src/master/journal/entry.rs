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

use crate::master::meta::inode::{InodeDir, InodeFile};
use crate::master::meta::BlockMeta;
use curvine_common::state::{CommitBlock, FileLock, MountInfo, SetAttrOpts};
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
}

// delete
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeleteEntry {
    pub(crate) op_id: u64,
    pub(crate) rpc_id: i64,
    pub(crate) path: String,
    pub(crate) mtime: i64,
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
    /// Link creation time, used during replay to set inode mtime.
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
            JournalEntry::UfsApplied(e) => e.op_id,
            JournalEntry::Snapshot(e) => e.op_id,
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
            JournalEntry::UfsApplied(e) => e.rpc_id,
            JournalEntry::Snapshot(e) => e.rpc_id,
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
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct JournalBatch {
    pub(crate) seq_id: u64,
    pub(crate) batch: Vec<JournalEntry>,
}

impl JournalBatch {
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
