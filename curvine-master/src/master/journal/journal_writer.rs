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
use curvine_config::JournalConf;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_model::{CommitBlock, FileLock, MountInfo, RenameFlags, SetAttrOpts};
use curvine_raft::conf::JournalConfExt;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::{FileUtils, LocalTime, TimeSpent};
use curvine_runtime::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, info, warn};
use std::collections::VecDeque;
use std::sync::{mpsc, Mutex};

pub(crate) enum JournalWriteRequest {
    Legacy(JournalEntry),
    Metadata(Vec<MetadataCommand>, mpsc::SyncSender<FsResult<()>>),
}

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    node_id: u64,
    client: RaftClient,
    sender: BlockingSender<JournalWriteRequest>,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<BlockingReceiver<JournalWriteRequest>>>,
    metadata_delta_log: Mutex<MetadataDeltaLog>,

    snapshot_entries: u64,
    entries_since_snapshot: AtomicCounter,
}

#[derive(Default)]
struct MetadataDeltaLog {
    capacity: usize,
    low_watermark: u64,
    changes: VecDeque<CvMetadataChange>,
}

impl JournalWriter {
    pub fn new(testing: bool, client: RaftClient, conf: &JournalConf) -> FsResult<Self> {
        let node_id = conf.node_id()?;
        let metrics = Master::get_metrics()?;
        let (sender, receiver) = BlockingChannel::new(conf.writer_channel_size).split();

        let receiver = if !testing {
            // Start the send log thread.
            let task = SenderTask::new(client.clone(), conf, 0)?;
            task.spawn(receiver)?;
            None
        } else {
            Some(Mutex::new(receiver))
        };

        Ok(Self {
            enable: conf.enable,
            node_id,
            client,
            sender,
            metrics,
            receiver,
            metadata_delta_log: Mutex::new(MetadataDeltaLog::new(conf.metadata_delta_log_capacity)),
            snapshot_entries: conf.snapshot_entries,
            entries_since_snapshot: AtomicCounter::new(0),
        })
    }

    fn send_inner(&self, entry: JournalEntry) -> FsResult<()> {
        debug!("send_entry {:?}", entry);
        self.sender.send(JournalWriteRequest::Legacy(entry))?;
        self.metrics.journal_queue_len.inc();
        Ok(())
    }

    pub fn commit_metadata_commands(&self, commands: Vec<MetadataCommand>) -> FsResult<()> {
        if !self.enable {
            return Ok(());
        }
        if commands.is_empty() {
            return Ok(());
        }

        if let Some(receiver) = self.receiver.as_ref() {
            return self.commit_metadata_commands_for_test(receiver, commands);
        }

        let (tx, rx) = mpsc::sync_channel(1);
        self.sender
            .send(JournalWriteRequest::Metadata(commands, tx))?;
        self.metrics.journal_queue_len.inc();
        match rx.recv() {
            Ok(res) => res,
            Err(e) => err_box!("metadata journal sender dropped response: {}", e),
        }
    }

    fn commit_metadata_commands_for_test(
        &self,
        receiver: &Mutex<BlockingReceiver<JournalWriteRequest>>,
        commands: Vec<MetadataCommand>,
    ) -> FsResult<()> {
        let spend = TimeSpent::new();
        let seq_id = commands[0].op_id();
        let mut batch = JournalCommandBatch::new(seq_id);
        let receiver = match receiver.lock() {
            Ok(receiver) => receiver,
            Err(e) => return err_box!("failed to lock journal test receiver: {}", e),
        };
        while let Ok(req) = receiver.try_recv() {
            match req {
                JournalWriteRequest::Legacy(entry) => {
                    let force = matches!(entry, JournalEntry::Snapshot(_));
                    self.metrics.journal_queue_len.dec();
                    batch.push_legacy(entry);
                    if force {
                        self.propose_command_batch(batch)?;
                        self.metrics.journal_flush_count.inc();
                        self.metrics
                            .journal_flush_time
                            .inc_by(spend.used_us() as i64);
                        batch = JournalCommandBatch::new(seq_id);
                    }
                }
                JournalWriteRequest::Metadata(_, _) => {
                    return err_box!("unexpected queued metadata command in test journal writer");
                }
            }
        }
        for command in commands {
            batch.push_metadata(command);
        }
        self.propose_command_batch(batch)?;

        self.metrics.journal_flush_count.inc();
        self.metrics
            .journal_flush_time
            .inc_by(spend.used_us() as i64);
        Ok(())
    }

    pub(crate) fn propose_command_batch(&self, batch: JournalCommandBatch) -> FsResult<()> {
        let bytes = JournalEnvelope::encode(batch)?;
        self.client.block_on_send_propose(bytes)?;
        Ok(())
    }

    fn send(&self, fs_dir: &FsDir, entry: JournalEntry) -> FsResult<()> {
        if self.enable {
            self.record_metadata_delta(&entry);
            self.send_inner(entry)?;
            self.maybe_emit_snapshot(fs_dir)?;
        }
        Ok(())
    }

    fn record_metadata_delta(&self, entry: &JournalEntry) {
        self.record_metadata_changes(entry.cv_metadata_changes());
    }

    fn record_metadata_changes(&self, changes: Vec<CvMetadataChange>) {
        if changes.is_empty() {
            return;
        }
        self.metadata_delta_log.lock().unwrap().push(changes);
    }

    pub(crate) fn on_metadata_commands_applied(
        &self,
        fs_dir: &FsDir,
        commands: &[MetadataCommand],
    ) {
        if !self.enable || commands.is_empty() {
            return;
        }

        for command in commands {
            self.record_metadata_changes(command.cv_metadata_changes());
        }
        if let Err(e) = self.maybe_emit_snapshot_after(fs_dir, commands.len() as u64) {
            warn!(
                "failed to emit snapshot after committed metadata batch: {}",
                e
            );
        }
    }

    fn maybe_emit_snapshot(&self, fs_dir: &FsDir) -> FsResult<()> {
        self.maybe_emit_snapshot_after(fs_dir, 1)
    }

    fn maybe_emit_snapshot_after(&self, fs_dir: &FsDir, applied_entries: u64) -> FsResult<()> {
        if self.snapshot_entries == 0 {
            return Ok(());
        }

        let mut previous = self.entries_since_snapshot.get();
        let entries = loop {
            let current = previous.saturating_add(applied_entries);
            let next = if current < self.snapshot_entries {
                current
            } else {
                current % self.snapshot_entries
            };
            if self.entries_since_snapshot.compare_and_set(previous, next) {
                break current;
            }
            previous = self.entries_since_snapshot.get();
        };
        if entries < self.snapshot_entries {
            return Ok(());
        }

        let now = LocalTime::mills();
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

        let snapshot_entry = JournalEntry::Snapshot(SnapshotEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            node_id: self.node_id,
            dir: dir.clone(),
        });

        if let Err(send_error) = self.send_inner(snapshot_entry) {
            if let Err(cleanup_error) = FileUtils::delete_path(&dir, true) {
                warn!(
                    "failed to remove checkpoint {} after snapshot entry send failure: {}",
                    dir, cleanup_error
                );
            }

            return Err(send_error);
        }
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
        exchange_pre_swap_ids: Option<(i64, i64)>,
    ) -> FsResult<()> {
        let (src_inode_id, dst_inode_id) = exchange_pre_swap_ids.unwrap_or((0, 0));
        let entry = RenameEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            src: src.as_ref().to_string(),
            dst: dst.as_ref().to_string(),
            mtime,
            flags: flags.value(),
            src_inode_id,
            dst_inode_id,
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

    pub fn log_cache_invalidations(
        &self,
        fs_dir: &FsDir,
        inodes: Vec<crate::master::meta::inode::InodeView>,
    ) -> FsResult<()> {
        if inodes.is_empty() {
            return Ok(());
        }

        let entry = CacheInvalidationEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            inodes,
        };
        self.send(fs_dir, JournalEntry::CacheInvalidation(entry))
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
        mtime: i64,
    ) -> FsResult<()> {
        let entry = LinkEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            mtime,
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
        self.sender
            .send(JournalWriteRequest::Legacy(JournalEntry::UfsApplied(entry)))?;

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
        let Some(receiver) = self.receiver.as_ref() else {
            return entries;
        };
        let receiver = match receiver.lock() {
            Ok(receiver) => receiver,
            Err(e) => {
                log::error!("failed to take journal entries: {}", e);
                return entries;
            }
        };
        while let Ok(v) = receiver.try_recv() {
            match v {
                JournalWriteRequest::Legacy(entry) => entries.push(entry),
                JournalWriteRequest::Metadata(_, ack) => {
                    let _ = ack.send(err_box!(
                        "unexpected queued metadata command while taking journal entries"
                    ));
                }
            }
        }
        entries
    }

    pub(crate) fn cv_metadata_changes_since(
        &self,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Option<Vec<CvMetadataChange>> {
        let log = self.metadata_delta_log.lock().unwrap();
        if from_epoch < log.low_watermark {
            return None;
        }
        Some(
            log.changes
                .iter()
                .filter(|change| change.op_id > from_epoch && change.op_id <= to_epoch)
                .cloned()
                .collect(),
        )
    }
}

impl MetadataDeltaLog {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            low_watermark: 0,
            changes: VecDeque::with_capacity(capacity.min(1024)),
        }
    }

    fn push(&mut self, changes: Vec<CvMetadataChange>) {
        if self.capacity == 0 {
            if let Some(max_op_id) = changes.iter().map(|change| change.op_id).max() {
                self.low_watermark = self.low_watermark.max(max_op_id);
            }
            return;
        }
        for change in changes {
            self.changes.push_back(change);
            while self.changes.len() > self.capacity {
                if let Some(evicted) = self.changes.pop_front() {
                    self.low_watermark = self.low_watermark.max(evicted.op_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
    use crate::master::quota::eviction::EvictionConf;
    use curvine_config::ClusterConf;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    fn test_conf(name: &str) -> ClusterConf {
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            journal: JournalConf {
                enable: true,
                snapshot_entries: 1,
                writer_channel_size: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        conf.change_test_meta_dir(name);
        conf
    }

    fn build_writer(conf: &ClusterConf) -> FsResult<JournalWriter> {
        Master::init_test_metrics();

        let rt = conf.journal.create_runtime();
        let client = RaftClient::from_conf(rt, &conf.journal);
        JournalWriter::new(true, client, &conf.journal)
    }

    fn build_fs_dir(conf: &ClusterConf, writer: Arc<JournalWriter>) -> FsResult<FsDir> {
        let ttl_bucket_list = Arc::new(TtlBucketList::new(
            conf.master.ttl_bucket_interval_ms() as i64
        )?);

        let eviction_conf = EvictionConf::from_conf(conf);
        let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(eviction_conf));

        FsDir::new(conf, writer, ttl_bucket_list, evictor)
    }

    fn checkpoint_dirs(conf: &ClusterConf) -> Vec<PathBuf> {
        let db_conf = conf.db_conf();
        let checkpoint_dir = Path::new(&db_conf.checkpoint_dir);

        if !checkpoint_dir.exists() {
            return Vec::new();
        }

        fs::read_dir(checkpoint_dir)
            .expect("failed to read checkpoint directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect()
    }

    #[test]
    fn removes_checkpoint_when_snapshot_enqueue_fails() -> FsResult<()> {
        let conf = test_conf("snapshot_enqueue_failure");
        let mut writer = build_writer(&conf)?;

        // The testing writer retains the only receiver. Dropping it makes
        // std::sync::mpsc::Sender::send return an error before handoff.
        let receiver = writer
            .receiver
            .take()
            .expect("testing writer should retain its receiver");
        drop(receiver);

        let writer = Arc::new(writer);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        let result = writer.maybe_emit_snapshot(&fs_dir);
        assert!(result.is_err(), "snapshot enqueue should fail");

        let checkpoints = checkpoint_dirs(&conf);
        assert!(
            checkpoints.is_empty(),
            "checkpoint should be removed after enqueue failure: {:?}",
            checkpoints
        );

        Ok(())
    }

    #[test]
    fn keeps_checkpoint_when_snapshot_enqueue_succeeds() -> FsResult<()> {
        let conf = test_conf("snapshot_enqueue_success");
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.maybe_emit_snapshot(&fs_dir)?;

        let entry = writer
            .receiver
            .as_ref()
            .expect("testing writer should retain its receiver")
            .lock()
            .expect("testing receiver lock should not be poisoned")
            .recv_timeout(Duration::from_secs(1))
            .expect("snapshot entry should be enqueued");

        match entry {
            JournalEntry::Snapshot(entry) => {
                assert!(
                    Path::new(&entry.dir).exists(),
                    "checkpoint should remain after successful handoff: {}",
                    entry.dir
                );
            }
            other => panic!("expected snapshot entry, got {:?}", other),
        }

        Ok(())
    }
}
