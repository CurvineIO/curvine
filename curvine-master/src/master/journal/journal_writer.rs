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
use curvine_error::{FsError, FsResult};
use curvine_model::{
    CommitBlock, FileLock, LeaderSnapshotInfo, MountInfo, RenameFlags, SetAttrOpts,
};
use curvine_raft::conf::JournalConfExt;
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::{FileUtils, LocalTime, TimeSpent};
use curvine_runtime::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use curvine_runtime::sync::{AtomicBool, AtomicCounter};
use log::{debug, info, warn};
use std::sync::Mutex;

// Write metadata operation logs.
pub struct JournalWriter {
    enable: bool,
    node_id: u64,
    sender: BlockingSender<JournalEntry>,
    metrics: &'static MasterMetrics,
    receiver: Option<Mutex<BlockingReceiver<JournalEntry>>>,

    snapshot_in_flight: AtomicBool,

    leader_create_snapshot: bool,
    snapshot_entries: u64,
    snapshot_min_interval_ms: u64,
    last_snapshot_ms: AtomicCounter,
    entries_since_snapshot: AtomicCounter,
}

impl JournalWriter {
    pub fn new(testing: bool, client: RaftClient, conf: &JournalConf) -> FsResult<Self> {
        let node_id = conf.node_id()?;
        let metrics = Master::get_metrics()?;
        let (sender, receiver) = BlockingChannel::new(conf.writer_channel_size).split();

        let receiver = if !testing {
            // Start the send log thread.
            let task = SenderTask::new(client, conf, 0)?;
            task.spawn(receiver)?;
            None
        } else {
            Some(Mutex::new(receiver))
        };

        Ok(Self {
            enable: conf.enable,
            node_id,
            sender,
            metrics,
            receiver,
            snapshot_in_flight: AtomicBool::new(false),
            leader_create_snapshot: conf.leader_creates_snapshot(),
            snapshot_entries: conf.snapshot_entries,
            snapshot_min_interval_ms: conf.snapshot_min_interval.as_millis(),
            last_snapshot_ms: AtomicCounter::new(0),
            entries_since_snapshot: AtomicCounter::new(0),
        })
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
        if !self.leader_create_snapshot {
            return Ok(());
        }
        if self.snapshot_entries == 0 {
            return Ok(());
        }

        let entries = self.entries_since_snapshot.add_and_get(1);
        if entries < self.snapshot_entries {
            return Ok(());
        }

        let now = LocalTime::mills();
        let last = self.last_snapshot_ms.get();
        if last > 0 && now.saturating_sub(last) < self.snapshot_min_interval_ms {
            return Ok(());
        }

        self.emit_snapshot(fs_dir, "periodic", entries)?;

        Ok(())
    }

    fn emit_snapshot(
        &self,
        fs_dir: &FsDir,
        source: &str,
        entries: u64,
    ) -> FsResult<LeaderSnapshotInfo> {
        let now = LocalTime::mills();
        let checkpoint_id = self.next_checkpoint_id(fs_dir);
        let spend = TimeSpent::new();

        let dir = fs_dir.create_checkpoint(checkpoint_id).map_err(|e| {
            FsError::common(format!(
                "leaderSnapshot[{}]: create_checkpoint failed: {}",
                source, e
            ))
        })?;

        let cost_ms = spend.used_ms();

        info!(
            "create leader snapshot, source {}, dir {}, entries {}, cost {} ms",
            source, dir, entries, cost_ms
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

        self.entries_since_snapshot.set(0);
        self.last_snapshot_ms.set(now);

        Ok(LeaderSnapshotInfo { dir, cost_ms })
    }

    fn next_checkpoint_id(&self, fs_dir: &FsDir) -> u64 {
        let mut id = LocalTime::mills();
        while FileUtils::exists(fs_dir.get_checkpoint_path(id)) {
            id += 1;
        }
        id
    }

    pub fn create_snapshot(&self, fs_dir: &FsDir, force: bool) -> FsResult<LeaderSnapshotInfo> {
        if !self.enable {
            return err_box!("leader snapshot requires journal.enable = true");
        }

        if !force && self.inside_snapshot_interval() {
            return err_box!(
                "the last leader snapshot was {} ms ago, snapshot_min_interval is {} ms; retry later or pass --force",
                self.last_snapshot_age_ms(),
                self.snapshot_min_interval_ms
            );
        }

        if !self.snapshot_in_flight.compare_and_set(false, true) {
            return err_box!("a leader snapshot is already in progress");
        }

        let info = self.emit_snapshot(fs_dir, "operator", self.entries_since_snapshot.get());
        self.snapshot_in_flight.set(false);
        info
    }

    fn inside_snapshot_interval(&self) -> bool {
        let last = self.last_snapshot_ms.get();
        last > 0 && self.last_snapshot_age_ms() < self.snapshot_min_interval_ms
    }

    fn last_snapshot_age_ms(&self) -> u64 {
        LocalTime::mills().saturating_sub(self.last_snapshot_ms.get())
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

    /// Journals one bounded purge batch of `entry`. The batch names the paths it
    /// removed, so replay applies exactly the same slice the leader applied.
    pub fn log_trash_purge<P: AsRef<str>>(
        &self,
        fs_dir: &FsDir,
        entry: P,
        mtime: i64,
        paths: Vec<String>,
    ) -> FsResult<()> {
        if paths.is_empty() {
            return Ok(());
        }

        let entry = TrashPurgeEntry {
            op_id: fs_dir.next_op_id(),
            rpc_id: 0,
            entry: entry.as_ref().to_string(),
            mtime,
            paths,
        };
        self.send(fs_dir, JournalEntry::TrashPurge(entry))
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
            entries.push(v);
        }
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::ttl::TtlBucketList;
    use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
    use crate::master::quota::eviction::EvictionConf;
    use curvine_config::{ClusterConf, RaftPeer};
    use curvine_runtime::common::DurationUnit;
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
        assert_eq!(
            writer.last_snapshot_ms.get(),
            0,
            "failed snapshot must not start the interval"
        );
        assert_eq!(
            writer.entries_since_snapshot.get(),
            1,
            "failed snapshot must not consume the entry count"
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

    #[test]
    fn retries_snapshot_after_failure_without_waiting_for_entries() -> FsResult<()> {
        let mut conf = test_conf("snapshot_failure_retry");
        conf.journal.snapshot_entries = 2;
        let mut writer = build_writer(&conf)?;

        let receiver = writer
            .receiver
            .take()
            .expect("testing writer should retain its receiver");
        drop(receiver);

        let writer = Arc::new(writer);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        // The first check only reaches one of the two required entries.
        writer.maybe_emit_snapshot(&fs_dir)?;
        assert_eq!(writer.entries_since_snapshot.get(), 1);

        // The second check creates a checkpoint, but handing the entry off
        // fails; the consumed entry count must be restored.
        assert!(writer.maybe_emit_snapshot(&fs_dir).is_err());
        assert_eq!(
            writer.entries_since_snapshot.get(),
            2,
            "failed snapshot must restore the consumed entry count"
        );

        // With the count restored, the next check retries immediately instead
        // of waiting for another full snapshot_entries batch.
        assert!(
            writer.maybe_emit_snapshot(&fs_dir).is_err(),
            "a restored entry count must allow an immediate retry"
        );

        Ok(())
    }

    fn raft_group(conf: &ClusterConf, nodes: u64) -> Vec<RaftPeer> {
        (1..=nodes)
            .map(|id| {
                let hostname = if id == 1 {
                    conf.journal.hostname.clone()
                } else {
                    format!("10.0.0.{id}")
                };
                RaftPeer::new(id, hostname, conf.journal.rpc_port)
            })
            .collect()
    }

    #[test]
    fn skips_checkpoint_when_raft_group_has_multiple_nodes() -> FsResult<()> {
        let mut conf = test_conf("snapshot_multi_node");
        conf.journal.journal_addrs = raft_group(&conf, 3);
        // Default is None, so a multi-node raft group must fall back to
        // single-node-only creation and skip the leader checkpoint.
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.maybe_emit_snapshot(&fs_dir)?;

        assert!(
            checkpoint_dirs(&conf).is_empty(),
            "leader checkpoint is disabled when the raft group has more than one node"
        );
        Ok(())
    }

    #[test]
    fn creates_checkpoint_when_leader_override_is_enabled() -> FsResult<()> {
        let mut conf = test_conf("snapshot_leader_override");
        conf.journal.journal_addrs = raft_group(&conf, 3);
        conf.journal.leader_create_snapshot = Some(true);
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.maybe_emit_snapshot(&fs_dir)?;

        assert_eq!(checkpoint_dirs(&conf).len(), 1);
        Ok(())
    }

    #[test]
    fn skips_checkpoint_when_leader_override_is_disabled() -> FsResult<()> {
        let mut conf = test_conf("snapshot_leader_disabled");
        conf.journal.leader_create_snapshot = Some(false);
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.maybe_emit_snapshot(&fs_dir)?;

        assert!(checkpoint_dirs(&conf).is_empty());
        Ok(())
    }

    #[test]
    fn creates_at_most_one_checkpoint_per_interval() -> FsResult<()> {
        let mut conf = test_conf("snapshot_min_interval");
        conf.journal.writer_channel_size = 8;
        conf.journal.snapshot_min_interval = DurationUnit::new(10 * DurationUnit::MINUTE);
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.maybe_emit_snapshot(&fs_dir)?;
        assert_eq!(checkpoint_dirs(&conf).len(), 1);

        writer.maybe_emit_snapshot(&fs_dir)?;
        assert_eq!(
            checkpoint_dirs(&conf).len(),
            1,
            "a second checkpoint inside the interval must be skipped"
        );

        writer
            .last_snapshot_ms
            .set(LocalTime::mills().saturating_sub(writer.snapshot_min_interval_ms + 1));
        std::thread::sleep(Duration::from_millis(2));
        writer.maybe_emit_snapshot(&fs_dir)?;
        assert_eq!(checkpoint_dirs(&conf).len(), 2);

        Ok(())
    }

    #[test]
    fn create_snapshot_creates_checkpoint_and_enqueues_snapshot_entry() -> FsResult<()> {
        let conf = test_conf("snapshot_operator_create");
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        let info = writer.create_snapshot(&fs_dir, false)?;

        assert!(Path::new(&info.dir).exists(), "checkpoint should be kept");
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
                assert_eq!(entry.dir, info.dir);
                assert_eq!(entry.node_id, conf.journal.node_id()?);
            }
            other => panic!("expected snapshot entry, got {:?}", other),
        }

        assert_eq!(writer.entries_since_snapshot.get(), 0);
        assert!(writer.last_snapshot_ms.get() > 0);
        Ok(())
    }

    #[test]
    fn create_snapshot_rejects_concurrent_requests() -> FsResult<()> {
        let conf = test_conf("snapshot_operator_busy");
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.snapshot_in_flight.set(true);

        let err = writer
            .create_snapshot(&fs_dir, false)
            .expect_err("a concurrent snapshot must be rejected");
        assert!(err.to_string().contains("already in progress"));
        Ok(())
    }

    #[test]
    fn create_snapshot_respects_the_min_interval_unless_forced() -> FsResult<()> {
        let mut conf = test_conf("snapshot_operator_min_interval");
        conf.journal.writer_channel_size = 8;
        conf.journal.snapshot_min_interval = DurationUnit::new(10 * DurationUnit::MINUTE);
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        writer.create_snapshot(&fs_dir, false)?;

        let err = writer
            .create_snapshot(&fs_dir, false)
            .expect_err("a second call inside the interval must be rejected");
        assert!(
            err.to_string().contains("snapshot_min_interval"),
            "unexpected error: {}",
            err
        );
        assert_eq!(checkpoint_dirs(&conf).len(), 1);

        writer.create_snapshot(&fs_dir, true)?;
        assert_eq!(checkpoint_dirs(&conf).len(), 2);
        Ok(())
    }

    #[test]
    fn create_snapshot_never_reuses_a_checkpoint_id() -> FsResult<()> {
        let mut conf = test_conf("snapshot_operator_unique_id");
        conf.journal.writer_channel_size = 8;
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        let first = writer.create_snapshot(&fs_dir, true)?;
        let second = writer.create_snapshot(&fs_dir, true)?;

        assert_ne!(first.dir, second.dir);
        assert!(
            Path::new(&first.dir).exists(),
            "a second snapshot must not delete the registered checkpoint"
        );
        Ok(())
    }

    #[test]
    fn create_snapshot_requires_journal() -> FsResult<()> {
        let mut conf = test_conf("snapshot_operator_no_journal");
        conf.journal.enable = false;
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        let err = writer
            .create_snapshot(&fs_dir, false)
            .expect_err("journal disabled must reject operator snapshots");
        assert!(err.to_string().contains("journal.enable"));
        Ok(())
    }

    #[test]
    fn next_checkpoint_id_steps_over_existing_directories() -> FsResult<()> {
        let conf = test_conf("snapshot_operator_existing_id");
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        let first = writer.next_checkpoint_id(&fs_dir);
        fs::create_dir_all(fs_dir.get_checkpoint_path(first))?;

        let second = writer.next_checkpoint_id(&fs_dir);

        assert!(second > first, "an existing checkpoint id must be skipped");
        assert!(
            !Path::new(&fs_dir.get_checkpoint_path(second)).exists(),
            "the picked id must be free on disk"
        );
        Ok(())
    }

    #[test]
    fn create_snapshot_keeps_the_throttle_clock_in_real_time() -> FsResult<()> {
        let conf = test_conf("snapshot_operator_throttle_clock");
        let writer = Arc::new(build_writer(&conf)?);
        let fs_dir = build_fs_dir(&conf, writer.clone())?;

        let before = LocalTime::mills();
        writer.create_snapshot(&fs_dir, false)?;

        assert!(writer.last_snapshot_ms.get() >= before);
        assert!(
            writer.last_snapshot_ms.get() <= LocalTime::mills(),
            "a snapshot must never push the throttle clock into the future"
        );
        Ok(())
    }
}
