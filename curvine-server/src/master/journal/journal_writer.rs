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
use crate::master::meta::inode::{InodeFile, InodePath};
use crate::master::{Master, MasterMetrics};
use curvine_common::conf::JournalConf;
use curvine_common::raft::RaftClient;
use curvine_common::state::{CommitBlock, MountInfo, SetAttrOpts};
use curvine_common::FsResult;
use log::info;
use std::sync::mpsc::{Receiver, SendError, Sender, SyncSender};
use std::sync::{mpsc, Mutex};

enum SenderAdapter {
    Bounded(SyncSender<JournalEntry>),
    UnBounded(Sender<JournalEntry>),
}

impl SenderAdapter {
    fn send(&self, entry: JournalEntry) -> Result<(), SendError<JournalEntry>> {
        match self {
            SenderAdapter::Bounded(s) => s.send(entry),
            SenderAdapter::UnBounded(s) => s.send(entry),
        }
    }
}

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    debug: bool,
    sender: SenderAdapter,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<Receiver<JournalEntry>>>,
}

impl JournalWriter {
    pub fn new(testing: bool, client: RaftClient, conf: &JournalConf) -> Self {
        let (sender, receiver) = if conf.writer_channel_size == 0 {
            let (sender, receiver) = mpsc::channel();
            (SenderAdapter::UnBounded(sender), receiver)
        } else {
            let (sender, receiver) = mpsc::sync_channel(conf.writer_channel_size);
            (SenderAdapter::Bounded(sender), receiver)
        };

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
            debug: conf.writer_debug,
            sender,
            metrics: Master::get_metrics(),
            receiver,
        }
    }

    fn send(&self, entry: JournalEntry) -> FsResult<()> {
        if self.debug {
            info!("send {:?}", entry);
        }

        if self.enable {
            self.sender.send(entry)?;
            self.metrics.journal_queue_len.inc();
        }
        Ok(())
    }

    pub fn log_mkdir(&self, op_ms: u64, inp: &InodePath) -> FsResult<()> {
        let entry = MkdirEntry {
            op_ms,
            path: inp.path().to_string(),
            dir: inp.clone_last_dir()?,
        };
        self.send(JournalEntry::Mkdir(entry))
    }

    pub fn log_create_file(&self, op_ms: u64, inp: &InodePath) -> FsResult<()> {
        let entry = CreateFileEntry {
            op_ms,
            path: inp.path().to_string(),
            file: inp.clone_last_file()?,
        };
        self.send(JournalEntry::CreateFile(entry))
    }

    pub fn log_append_file<P: AsRef<str>>(
        &self,
        op_ms: u64,
        path: P,
        file: &InodeFile,
    ) -> FsResult<()> {
        let entry = AppendFileEntry {
            op_ms,
            path: path.as_ref().to_string(),
            file: file.clone(),
        };
        self.send(JournalEntry::AppendFile(entry))
    }

    pub fn log_add_block<P: AsRef<str>>(
        &self,
        op_ms: u64,
        path: P,
        file: &InodeFile,
        commit_block: Option<CommitBlock>,
    ) -> FsResult<()> {
        let entry = AddBlockEntry {
            op_ms,
            path: path.as_ref().to_string(),
            blocks: file.blocks.clone(),
            commit_block,
        };

        self.send(JournalEntry::AddBlock(entry))
    }

    pub fn log_complete_file<P: AsRef<str>>(
        &self,
        op_ms: u64,
        path: P,
        file: &InodeFile,
        commit_block: Option<CommitBlock>,
    ) -> FsResult<()> {
        let entry = CompleteFileEntry {
            op_ms,
            path: path.as_ref().to_string(),
            file: file.clone(),
            commit_block,
        };

        self.send(JournalEntry::CompleteFile(entry))
    }

    pub fn log_rename<P: AsRef<str>>(
        &self,
        op_ms: u64,
        src: P,
        dst: P,
        mtime: i64,
    ) -> FsResult<()> {
        let entry = RenameEntry {
            op_ms,
            src: src.as_ref().to_string(),
            dst: dst.as_ref().to_string(),
            mtime,
        };

        self.send(JournalEntry::Rename(entry))
    }

    pub fn log_delete<P: AsRef<str>>(&self, op_ms: u64, path: P, mtime: i64) -> FsResult<()> {
        let entry = DeleteEntry {
            op_ms,
            path: path.as_ref().to_string(),
            mtime,
        };
        self.send(JournalEntry::Delete(entry))
    }

    pub fn log_mount(&self, op_ms: u64, info: MountInfo) -> FsResult<()> {
        let entry = MountEntry { op_ms, info };

        self.send(JournalEntry::Mount(entry))
    }

    pub fn log_unmount(&self, op_ms: u64, id: u32) -> FsResult<()> {
        let entry = UnMountEntry { op_ms, id };

        self.send(JournalEntry::UnMount(entry))
    }

    pub fn log_set_attr(&self, op_ms: u64, inp: &InodePath, opts: SetAttrOpts) -> FsResult<()> {
        let entry = SetAttrEntry {
            op_ms,
            path: inp.path().to_string(),
            opts,
        };
        self.send(JournalEntry::SetAttr(entry))
    }

    pub fn log_symlink<P: AsRef<str>>(
        &self,
        op_ms: u64,
        link: P,
        new_inode: InodeFile,
        force: bool,
    ) -> FsResult<()> {
        let entry = SymlinkEntry {
            op_ms,
            link: link.as_ref().to_string(),
            new_inode,
            force,
        };
        self.send(JournalEntry::Symlink(entry))
    }

    pub fn log_hardlink<P: AsRef<str>>(
        &self,
        op_ms: u64,
        old_path: P,
        new_path: P,
    ) -> FsResult<()> {
        let entry = HardlinkEntry {
            op_ms,
            old_path: old_path.as_ref().to_string(),
            new_path: new_path.as_ref().to_string(),
        };
        self.send(JournalEntry::Hardlink(entry))
    }

    // for testing
    pub fn take_entries(&self) -> Vec<JournalEntry> {
        let mut entries = vec![];

        while let Ok(v) = self.receiver.as_ref().unwrap().lock().unwrap().try_recv() {
            entries.push(v)
        }
        entries
    }
}
