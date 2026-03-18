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

#![allow(clippy::result_large_err)]

use crate::master::journal::*;
use crate::master::meta::inode::{InodeDir, InodeFile, InodePath};
use crate::master::meta::FsDir;
use crate::master::{Master, MasterMetrics};
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::{CommitBlock, FileLock, MountInfo, RenameFlags, SetAttrOpts};
use curvine_common::FsResult;
use log::{debug, info};
use orpc::common::LocalTime;
use orpc::err_box;
use orpc::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use orpc::sync::AtomicCounter;
use std::sync::Mutex;

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    node_id: u64,
    sender: BlockingSender<JournalEntry>,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<BlockingReceiver<JournalEntry>>>,

    snapshot_entries: u64,
    entries_since_snapshot: AtomicCounter,
}

impl JournalWriter {
    pub fn new(testing: bool, client: RaftClient, conf: &JournalConf) -> Self {
        let (sender, receiver) = BlockingChannel::new(conf.writer_channel_size).split();

        let receiver = if !testing {
            // Start the send log thread.
            let task = SenderTask::new(client, conf, 0);
            task.spawn(receiver).unwrap();
            None
        } else {
            Some(Mutex::new(receiver))
        };

        Self {
            enable: conf.enable,
            node_id: conf.node_id().unwrap(),
            sender,
            metrics: Master::get_metrics(),
            receiver,
            snapshot_entries: conf.snapshot_entries,
            entries_since_snapshot: AtomicCounter::new(0),
        }
    }

    fn send_inner(&self, entry: JournalEntry) -> FsResult<()> {
        debug!("send_entry {:?}", entry);
        self.sender.send(entry)?;
        self.metrics.journal_queue_len.inc();
        Ok(())
    }

    fn send(&self, fs_dir: &FsDir, entry: JournalEntry) -> FsResult<()> {
        if self.enable {
            self.send_inner(entry)?;
            self.maybe_emit_snapshot(fs_dir)?;
        }
        Ok(())
    }

    fn maybe_emit_snapshot(&self, fs_dir: &FsDir) -> FsResult<()> {
        if self.snapshot_entries == 0 {
            return Ok(());
        }

        let entries = self.entries_since_snapshot.add_and_get(1);
        if entries < self.snapshot_entries {
            return Ok(());
        }

        let now = LocalTime::mills();
        self.entries_since_snapshot.set(0);
        let dir = match fs_dir.store.create_checkpoint(now) {
            Ok(d) => d,
            Err(e) => {
                return err_box!("leaderSnapshot: create_checkpoint failed: {}", e);
            }
        };

        info!(
            "create leader snapshot, dir {}, entries {}, cost {} ms, inode_id {}",
            dir,
            entries,
            LocalTime::mills() - now,
            fs_dir.inode_id.current()
        );

        self.send_inner(JournalEntry::Snapshot(SnapshotEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            node_id: self.node_id,
            dir,
        }))?;
        Ok(())
    }

    pub fn log_mkdir(&self, fs_dir: &FsDir, path: impl AsRef<str>, dir: &InodeDir) -> FsResult<()> {
        let entry = MkdirEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            dir: dir.clone(),
        };
        self.send(fs_dir, JournalEntry::Mkdir(entry))
    }

    pub fn log_create_file(&self, fs_dir: &FsDir, inp: &InodePath) -> FsResult<()> {
        let entry = CreateFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(fs_dir, JournalEntry::CreateFile(entry))
    }

    pub fn log_reopen_file<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
    ) -> FsResult<()> {
        let entry = ReopenFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            file: file.clone(),
        };
        self.send(fs_dir, JournalEntry::ReopenFile(entry))
    }

    pub fn log_add_block<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
        commit_block: Vec<CommitBlock>,
    ) -> FsResult<()> {
        let entry = AddBlockEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            blocks: file.blocks.clone(),
            commit_block,
        };
        self.send(fs_dir, JournalEntry::AddBlock(entry))
    }

    pub fn log_complete_file<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        file: &InodeFile,
        commit_blocks: Vec<CommitBlock>,
    ) -> FsResult<()> {
        let entry = CompleteFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            file: file.clone(),
            commit_blocks,
        };
        self.send(fs_dir, JournalEntry::CompleteFile(entry))
    }

    pub fn log_overwrite_file(&self, fs_dir: &FsDir, inp: &InodePath) -> FsResult<()> {
        let entry = OverWriteFileEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(fs_dir, JournalEntry::OverWriteFile(entry))
    }

    pub fn log_rename<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src: P,
        dst: P,
        mtime: i64,
        flags: RenameFlags,
    ) -> FsResult<()> {
        let entry = RenameEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            src: src.as_ref().to_string(),
            dst: dst.as_ref().to_string(),
            mtime,
            flags: flags.value(),
        };
        self.send(fs_dir, JournalEntry::Rename(entry))
    }

    pub fn log_delete<P: AsRef<str>>(&self, fs_dir: &FsDir, path: P, mtime: i64) -> FsResult<()> {
        let entry = DeleteEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            mtime,
        };
        self.send(fs_dir, JournalEntry::Delete(entry))
    }

    pub fn log_free<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        path: P,
        mtime: i64,
        recursive: bool,
    ) -> FsResult<()> {
        let entry = FreeEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: path.as_ref().to_string(),
            mtime,
            recursive,
        };
        self.send(fs_dir, JournalEntry::Free(entry))
    }

    pub fn log_mount(&self, fs_dir: &FsDir, info: MountInfo) -> FsResult<()> {
        let entry = MountEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            info,
        };
        self.send(fs_dir, JournalEntry::Mount(entry))
    }

    pub fn log_unmount(&self, fs_dir: &FsDir, id: u32) -> FsResult<()> {
        let entry = UnMountEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            id,
        };
        self.send(fs_dir, JournalEntry::UnMount(entry))
    }

    pub fn log_set_attr(&self, fs_dir: &FsDir, inp: &InodePath, opts: SetAttrOpts) -> FsResult<()> {
        let entry = SetAttrEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            path: inp.path().to_string(),
            opts,
        };
        self.send(fs_dir, JournalEntry::SetAttr(entry))
    }

    pub fn log_symlink<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        link: P,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<()> {
        let entry = SymlinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            link: link.as_ref().to_string(),
            new_inode,
            force,
        };
        self.send(fs_dir, JournalEntry::Symlink(entry))
    }

    pub fn log_link<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        src_path: P,
        dst_path: P,
    ) -> FsResult<()> {
        let entry = LinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            mtime: LocalTime::mills() as i64,
            src_path: src_path.as_ref().to_string(),
            dst_path: dst_path.as_ref().to_string(),
        };
        self.send(fs_dir, JournalEntry::Link(entry))
    }

    pub fn log_ufs_applied(&self, op_id: u64, term: u64, index: u64) -> FsResult<()> {
        if !self.enable {
            return Ok(());
        }

        let entry = UfsAppliedEntry {
            op_id,
            rpc_id: 0,
            term,
            index,
        };
        self.metrics.journal_queue_len.inc();
        self.sender.send(JournalEntry::UfsApplied(entry))?;

        Ok(())
    }

    pub fn log_set_locks(&self, fs_dir: &FsDir, ino: i64, locks: Vec<FileLock>) -> FsResult<()> {
        let entry = SetLocksEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            ino,
            locks,
        };
        self.send(fs_dir, JournalEntry::SetLocks(entry))
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        let mut entries = vec![];
        let receiver = self.receiver.as_ref().unwrap().lock().unwrap();
        while let Ok(v) = receiver.recv_check() {
            entries.push(v)
        }
        entries
    }
}
