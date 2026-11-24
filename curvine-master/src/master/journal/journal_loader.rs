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

#![allow(clippy::needless_range_loop)]

use crate::common::UfsFactory;
use crate::master::fs::MasterFilesystem;
use crate::master::journal::ufs_loader::export_transfer_client;
use crate::master::journal::*;
use crate::master::meta::inode::InodeView::File;
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::meta::InodeId;
use crate::master::{Master, MasterMetrics, MountManager, SyncFsDir};
use curvine_config::{ClusterConf, JournalConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsError;
use curvine_model::RenameFlags;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_raft::raft::{FollowerReadinessMonitor, RaftClient, RaftResult, RaftUtils};
use curvine_runtime::common::{FileUtils, TimeSpent};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, error, info, warn};
use raft::eraftpb::{Entry, EntryType};
use raft::StateRole;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
#[cfg(test)]
use std::time::Duration;
use std::{fs, mem};

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    node_id: u64,
    fs_dir: SyncFsDir,
    mnt_mgr: Arc<MountManager>,
    ufs_loader: UfsLoader,
    log_store: RocksLogStorage,
    sender: AsyncSender<ApplyMsg>,
    fsm_state: Arc<Mutex<FsmState>>,
    retain_checkpoint_num: usize,
    ignore_reply_error: bool,
    batch_size: u64,
    metrics: &'static MasterMetrics,
    has_apply_worker: bool,
    follower_readiness: FollowerReadinessMonitor,
    /// Shared raft-term handle, so the session lease epoch follows election
    /// instead of process start (#1883 review).
    term: Arc<AtomicU64>,
    applied_term: Arc<AtomicCounter>,
}

impl JournalLoader {
    pub fn new_replay_loader(
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        cluster_conf: &ClusterConf,
    ) -> CommonResult<Self> {
        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), conf);
        let journal_writer = Arc::new(JournalWriter::new(true, client, conf)?);
        let log_store = RocksLogStorage::from_conf(conf, false);
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            cluster_conf,
            log_store,
            journal_writer,
            FollowerReadinessMonitor::default(),
            true,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicCounter::new(0)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        cluster_conf: &ClusterConf,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        follower_readiness: FollowerReadinessMonitor,
        term: Arc<AtomicU64>,
        applied_term: Arc<AtomicCounter>,
    ) -> CommonResult<Self> {
        Self::build(
            rt,
            fs_dir,
            mnt_mgr,
            conf,
            cluster_conf,
            log_store,
            journal_writer,
            follower_readiness,
            false,
            term,
            applied_term,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        rt: Arc<Runtime>,
        fs_dir: SyncFsDir,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        cluster_conf: &ClusterConf,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        follower_readiness: FollowerReadinessMonitor,
        testing: bool,
        term: Arc<AtomicU64>,
        applied_term: Arc<AtomicCounter>,
    ) -> CommonResult<Self> {
        // The replayer owns the UFS side effects, so it needs the same log,
        // writer and watermarks the apply loop uses. The write-back itself is
        // dispatched to the transfer service, looped back to this master's own
        // RPC endpoint when transfer runs embedded.
        let fsm_state = Arc::new(Mutex::new(FsmState::default()));
        let ufs_factory = Arc::new(UfsFactory::with_rt(&cluster_conf.client, rt.clone()));
        let transfer = export_transfer_client(cluster_conf, rt.clone())?;
        let ufs_loader = UfsLoader::new(
            mnt_mgr.clone(),
            ufs_factory,
            transfer,
            fs_dir.clone(),
            conf,
            log_store.clone(),
            journal_writer.clone(),
            fsm_state.clone(),
        )?;
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let loader = Self {
            node_id: conf.node_id()?,
            fs_dir,
            mnt_mgr,
            ufs_loader,
            log_store,
            sender,
            fsm_state,
            retain_checkpoint_num: conf.retain_checkpoint_num,
            ignore_reply_error: conf.ignore_reply_error,
            batch_size: conf.scan_batch_size,
            metrics: Master::get_metrics()?,
            has_apply_worker: !testing,
            follower_readiness,
            term,
            applied_term,
        };

        loader.refresh_metrics()?;
        if !testing {
            let loader1 = loader.clone();
            rt.spawn(async move {
                Self::run_apply(loader1, receiver).await;
            });

            let ufs_replayer = loader.ufs_loader.clone();
            rt.spawn(async move {
                ufs_replayer.run().await;
            });
        }

        Ok(loader)
    }

    fn fsm_state(&self) -> CommonResult<MutexGuard<'_, FsmState>> {
        match self.fsm_state.lock() {
            Ok(state) => Ok(state),
            Err(e) => err_box!("fsm_state lock poisoned: {}", e),
        }
    }

    fn fsm_state_snapshot(&self) -> CommonResult<FsmState> {
        Ok(self.fsm_state()?.clone())
    }

    fn abort_on_fatal_apply_error(message: impl AsRef<str>) -> ! {
        error!(
            "fatal journal apply error: {}; aborting master to avoid serving inconsistent metadata",
            message.as_ref()
        );
        std::process::abort();
    }

    /// Advance the metadata watermark only. UFS progress belongs to the
    /// replayer inside `UfsLoader`, which walks `(ufs_applied, applied]` on its
    /// own schedule so a slow object store can never stall raft apply. It polls
    /// the watermarks, so there is no wake-up to send here.
    fn set_applied(&self, applied: AppliedIndex) -> CommonResult<()> {
        let mut state = self.fsm_state()?;
        let applied_index = applied.index;
        state.applied = applied;
        self.refresh_fsm_metrics(&state);
        drop(state);
        self.follower_readiness.observe_applied(applied_index);
        self.hard_state_changed(&self.log_store.hard_state());
        Ok(())
    }

    fn refresh_replay_readiness(&self) -> CommonResult<()> {
        let applied = self.fsm_state_snapshot()?.applied;
        let committed_index = self.log_store.hard_state().commit;
        self.follower_readiness.observe_applied(applied.index);
        self.follower_readiness
            .set_replay_ready(applied.index >= committed_index);
        Ok(())
    }

    fn refresh_fsm_metrics(&self, state: &FsmState) {
        self.metrics.journal_applied.set(state.applied.index as i64);
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
        let previous = self.applied_term.get();
        self.applied_term.set(state.applied.term);
        let raft_term = self.term.load(Ordering::Acquire);
        if state.applied.term != previous && state.applied.term == raft_term && raft_term > 0 {
            info!(
                "term replay caught up, applied_term={} raft_term={}",
                state.applied.term, raft_term
            );
        }
    }

    /// Refresh gauges only: never advance metadata/UFS replay watermarks just
    /// to make monitoring appear caught up.
    fn refresh_metrics(&self) -> CommonResult<()> {
        self.refresh_fsm_metrics(&*self.fsm_state()?);
        self.hard_state_changed(&self.log_store.hard_state());
        Ok(())
    }

    fn build_applied(entry: &Entry) -> AppliedIndex {
        AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        }
    }

    /// Advance the committed apply high-water mark without replaying any
    /// metadata op: the no-op/config branch and batches deliberately skipped
    /// under `ignore_reply_error` both need the watermark to move so that
    /// replay can continue past them.
    fn advance_watermark(&self, cur: FsmState, entry: &Entry) -> CommonResult<()> {
        let mut applied = cur.applied;
        applied.term = entry.term;
        applied.index = entry.index;
        self.set_applied(applied)
    }

    /// Replay one metadata operation from the log.
    ///
    /// `already_applied` marks entries this node proposed while leading whose
    /// pre-applied mutation is still in the FSM (`is_preapplied_current`): the
    /// RPC handler mutated the namespace before proposing, so running the
    /// operation again would apply it twice. Every other entry — replicated
    /// ones, forwarded ones, and ones whose mutation a restore discarded —
    /// must be replayed, otherwise a failover silently loses whichever
    /// mutations were not committed yet when the election happened.
    fn replay_entry(&self, already_applied: bool, entry: &JournalEntry) -> CommonResult<()> {
        if already_applied {
            return Ok(());
        }

        crate::fault_point! {
            sync,
            name: "master.replay.before_entry",
            description: "Before one journal operation is replayed",
            context: {"op_id" => entry.op_id() as i64},
        }
        {
            let fs_dir = self.fs_dir.read();
            fs_dir.update_op_id(entry.op_id());
            if let Some(inode_id) = entry.inode_id() {
                fs_dir.update_last_inode_id(inode_id)?;
            }
        }

        let result = self.apply_entry(entry.clone());
        crate::fault_point! {
            sync,
            name: "master.replay.after_entry",
            description: "After one journal operation is replayed",
            context: {"op_id" => entry.op_id() as i64},
        }
        result
    }

    /// Whether the FSM still carries the mutation a parked propose's handler
    /// applied for it. `proposer_epoch` is the restore epoch the proposer
    /// sampled while mutating; every snapshot restore bumps the local epoch
    /// and discards mutations applied before it, so a stale epoch means the
    /// entry must be replayed (#2487 review).
    fn is_preapplied_current(&self, proposer_epoch: Option<u64>) -> bool {
        proposer_epoch.is_some_and(|epoch| epoch == self.fs_dir.read().preapply_epoch())
    }

    fn apply0(&self, is_leader: bool, already_applied: bool, entry: &Entry) -> CommonResult<()> {
        let cur = self.fsm_state_snapshot()?;
        if entry.index <= cur.applied.index {
            info!(
                "skip entry index {}, term {}, fsm_state {:?}",
                entry.index, entry.term, cur
            );
            return Ok(());
        }

        // Empty leader no-ops and configuration entries have no metadata mutation,
        // but still advance the committed apply high-water mark.
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            return self.advance_watermark(cur, entry);
        }

        let batch = match JournalBatch::deserialize_compat(&entry.data) {
            Ok(batch) => batch,
            Err(e) if self.ignore_reply_error => {
                error!(
                    "skip failed journal batch deserialization, entry index={}, term={}, error={}",
                    entry.index, entry.term, e
                );
                return self.advance_watermark(cur, entry);
            }
            Err(e) => return Err(e),
        };

        let batch_len = batch.len();
        crate::fault_point! {
            sync,
            name: "master.apply.before_batch",
            description: "Before a committed journal batch is applied",
            context: {
                "index" => entry.index as i64,
                "term" => entry.term as i64,
                "entries" => batch_len as i64,
            },
        }
        let mut snapshot = None;
        let mut applied = Self::build_applied(entry);

        for (seq, op_entry) in batch.batch.into_iter().enumerate() {
            applied.op_id = op_entry.op_id();
            applied.rpc_id = op_entry.rpc_id();

            match op_entry {
                JournalEntry::Snapshot(e) if is_leader && e.node_id == self.node_id => {
                    if seq + 1 != batch_len {
                        return err_box!("snapshot should be the last entry");
                    }
                    snapshot.replace(e);
                    continue;
                }

                _ => {}
            }

            if let Err(e) = self.replay_entry(already_applied, &op_entry) {
                if self.ignore_reply_error {
                    error!(
                        "skip failed journal replay, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                return err_box!("failed to apply journal: {:?}: {}", op_entry, e);
            }
        }

        crate::fault_point! {
            sync,
            name: "master.apply.before_watermark",
            description: "Before the applied watermark advances past a batch",
            context: {
                "index" => entry.index as i64,
                "term" => entry.term as i64,
                "entries" => batch_len as i64,
            },
        }
        self.set_applied(applied)?;
        crate::fault_point! {
            sync,
            name: "master.apply.after_batch",
            description: "After a committed journal batch is applied",
            context: {
                "index" => entry.index as i64,
                "term" => entry.term as i64,
                "entries" => batch_len as i64,
            },
        }

        if let Some(e) = snapshot {
            let spent = TimeSpent::new();
            let snap_data = self.create_snapshot0(Some(e.dir.to_string()))?;

            self.log_store.create_snapshot(snap_data.clone())?;
            self.log_store.compact(snap_data.fsm_state.compact())?;

            info!(
                "create leader snapshot, dir={}, cost={}ms, fsm_state={:?}",
                e.dir,
                spent.used_ms(),
                snap_data.fsm_state
            );
        }

        Ok(())
    }

    async fn apply_msg(&self, is_leader: bool, msg: &ApplyMsg) -> CommonResult<()> {
        match msg {
            // A replicated entry no handler here ever ran: the sender's RPC
            // path belongs to another incarnation, so the state machine has to
            // apply it. This covers followers replaying the leader's log and a
            // new leader replaying what the previous one left uncommitted.
            ApplyMsg::Entry(entry) => self.apply0(is_leader, false, entry),

            // A propose parked on the acknowledgement. `proposer_epoch` is set
            // only when this node's handler already mutated the namespace, so
            // replaying that entry here would apply it twice. A batch forwarded
            // from another master's handler carries `None` and must be
            // replayed before its sender is acknowledged.
            ApplyMsg::EntryWithAck((entry, _, proposer_epoch)) => self.apply0(
                is_leader,
                self.is_preapplied_current(*proposer_epoch),
                entry,
            ),

            ApplyMsg::Scan(applied_index) => {
                self.follower_readiness.set_replay_ready(false);
                let mut last_applied = applied_index.index;

                let commit_index = self.log_store.hard_state().commit;
                crate::fault_point! {
                    async,
                    name: "master.replay.before_scan",
                    description: "Before committed entries are replayed from the journal",
                    context: {
                        "applied" => last_applied as i64,
                        "commit" => commit_index as i64,
                    },
                }
                loop {
                    if last_applied >= commit_index {
                        let applied = self.fsm_state_snapshot()?.applied;
                        self.follower_readiness.observe_applied(applied.index);
                        self.follower_readiness
                            .set_replay_ready(applied.index >= commit_index);
                        crate::fault_point! {
                            async,
                            name: "master.replay.after_scan",
                            description: "After committed entries were replayed from the journal",
                            context: {"applied" => applied.index as i64},
                        }
                        return Ok(());
                    }

                    let high = (last_applied + self.batch_size).min(commit_index + 1);
                    let list = self.log_store.scan_entries(last_applied + 1, high)?;

                    // `scan_entries` reads the raw range and silently returns
                    // nothing when the entries were compacted away. Below commit
                    // that can only mean the owed UFS range is gone; continuing
                    // would let the next entry advance `ufs_applied` past the gap
                    // and lose those UFS side effects for good.
                    if list.is_empty() {
                        self.follower_readiness.set_replay_ready(false);
                        return err_box!(
                            "metadata invariant broken: journal replay stopped at index {}, commit index {}, but no entries are on disk",
                            last_applied,
                            commit_index
                        );
                    };

                    self.ensure_contiguous_replay(last_applied + 1, &list)?;

                    info!(
                        "replay-scan, start_index: {}, entries: {}, commit_index: {}",
                        last_applied + 1,
                        list.len(),
                        commit_index
                    );

                    crate::fault_point! {
                        async,
                        name: "master.replay.before_batch",
                        description: "Before one scanned journal batch is replayed",
                        context: {
                            "start" => (last_applied + 1) as i64,
                            "entries" => list.len() as i64,
                        },
                    }
                    for entry in list {
                        self.apply0(is_leader, false, &entry)?;
                        last_applied = entry.index;
                    }
                }
            }

            _ => err_box!("unsupported apply message in journal loader apply_msg"),
        }
    }

    fn complete_entry_ack(msg: ApplyMsg, result: RaftResult<()>) {
        if let ApplyMsg::EntryWithAck((_, tx, _)) = msg {
            crate::fault_point! {
                sync,
                name: "master.apply.before_ack",
                description: "Before the apply result is acknowledged to the raft driver",
                context: {"success" => result.is_ok()},
            }
            if let Err(e) = tx.send(result) {
                warn!("send journal entry apply acknowledgement failed: {}", e);
            }
            crate::fault_point! {
                sync,
                name: "master.apply.after_ack",
                description: "After the apply result is acknowledged to the raft driver",
                context: {},
            }
        }
    }

    async fn catch_up_committed_metadata(&self) -> RaftResult<()> {
        let committed = self.log_store.hard_state().commit;
        let applied = self.fsm_state_snapshot()?.applied;
        self.apply_msg(false, &ApplyMsg::new_scan(applied)).await?;

        let metadata_applied = self.fsm_state_snapshot()?.applied.index;
        if metadata_applied < committed {
            return err_box!(
                "metadata catch-up stopped before committed raft index: applied={}, commit={}",
                metadata_applied,
                committed
            );
        }
        Ok(())
    }
    async fn run_apply(self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut is_leader = false;

        loop {
            let apply_msg = match receiver.recv().await {
                Some(v) => v,
                None => break,
            };

            match apply_msg {
                ApplyMsg::CreateSnapshot(tx) => {
                    if let Err(e) = tx.send(self.create_snapshot0(None)) {
                        warn!("send create snapshot result failed: {}", e);
                    }
                }

                ApplyMsg::ApplySnapshot((tx, snapshot)) => {
                    if let Err(e) = tx.send(self.apply_snapshot0(snapshot)) {
                        warn!("send apply snapshot result failed: {}", e);
                    }
                }

                ApplyMsg::Reset(tx) => {
                    if let Err(e) = tx.send(self.reset0()) {
                        warn!("send metadata reset result failed: {}", e);
                    }
                }

                ApplyMsg::RoleChange((role, tx)) => {
                    let result = async {
                        if role == StateRole::Leader {
                            // Publish leadership only after committed namespace
                            // mutations have advanced local metadata high-water
                            // marks. The UFS replayer then resumes on its own,
                            // from `ufs_applied`.
                            self.catch_up_committed_metadata().await?;
                            let raft_term = self.term.load(Ordering::Acquire);
                            let applied_term = self.applied_term.get();
                            if applied_term != raft_term {
                                info!(
                                    "leader term {} elected, applied_term={}: write serving waits for a term-{} entry",
                                    raft_term, applied_term, raft_term
                                );
                            }
                            is_leader = true;
                            self.ufs_loader.set_leader(true);
                        } else {
                            is_leader = false;
                            self.ufs_loader.set_leader(false);
                        }
                        Ok(())
                    }
                    .await;

                    if tx.send(result).is_err() {
                        warn!("leader role-change acknowledgement receiver dropped");
                    }
                }

                ApplyMsg::Shutdown(tx) => {
                    let _ = tx.send(());
                    break;
                }

                msg => match self.apply_msg(is_leader, &msg).await {
                    Ok(()) => Self::complete_entry_ack(msg, Ok(())),
                    Err(error) => {
                        Self::abort_on_fatal_apply_error(format!("apply entry failed: {}", error))
                    }
                },
            }
        }
    }

    fn create_snapshot0(&self, dir_option: Option<String>) -> RaftResult<SnapshotData> {
        crate::fault_point! {
            sync,
            name: "master.snapshot.create.before",
            description: "Before the metadata store is checkpointed for a snapshot",
            context: {"checkpoint_dir" => dir_option.is_some()},
        }
        let fsm_state = self.fsm_state_snapshot()?;
        let dir = match dir_option {
            Some(dir) => dir,
            None => {
                let fs_dir = self.fs_dir.read();
                fs_dir.create_checkpoint(fsm_state.applied.index)?
            }
        };

        let data = RaftUtils::create_file_snapshot(&dir, self.node_id, fsm_state)?;

        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }

        crate::fault_point! {
            sync,
            name: "master.snapshot.create.after",
            description: "After the metadata store was checkpointed for a snapshot",
            context: {
                "snapshot_id" => data.snapshot_id as i64,
                "applied" => data.fsm_state.applied.index as i64,
            },
        }
        Ok(data)
    }

    /// Discard the local metadata store so the journal can rebuild it.
    ///
    /// The metadata DB is a derived cache, not a source of truth: with no
    /// snapshot there is no trusted base, and the DB can contain leader-first
    /// writes that never reached the log. The caller replays the journal from
    /// its first index afterwards.
    fn reset0(&self) -> RaftResult<()> {
        crate::fault_point! {
            sync,
            name: "master.rebuild.before_reset",
            description: "Before the derived metadata store is discarded",
            context: {"node" => self.node_id as i64},
        }
        self.follower_readiness.set_replay_ready(false);
        {
            let mut fs_dir = self.fs_dir.write();
            fs_dir.reset_storage()?;
        }
        *self.fsm_state()? = FsmState::default();
        // The mount table is derived from the metadata store; rebuild it too.
        self.mnt_mgr.restore_best_effort();
        self.refresh_metrics()?;
        crate::fault_point! {
            sync,
            name: "master.rebuild.after_reset",
            description: "After the derived metadata store was discarded",
            context: {"node" => self.node_id as i64},
        }
        Ok(())
    }

    fn apply_snapshot0(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let mut spend = TimeSpent::new();

        crate::fault_point! {
            sync,
            name: "master.snapshot.apply.before",
            description: "Before the metadata store applies a snapshot",
            context: {
                "node" => self.node_id as i64,
                "snapshot_id" => snapshot.snapshot_id as i64,
            },
        }
        // Raft uses the default value as a no-snapshot placeholder. It carries no
        // filesystem state and must not create a restore directory or replace metadata.
        let is_empty_placeholder_snapshot = snapshot.snapshot_id == 0
            && snapshot.files_data.is_none()
            && snapshot.bytes_data.is_none();
        if is_empty_placeholder_snapshot {
            warn!("skip zero-index empty placeholder snapshot");
            return Ok(());
        }

        let restore_path = match &snapshot.files_data {
            Some(data) => data.dir.clone(),
            None => {
                return err_box!(
                    "refusing to apply empty snapshot {} without checkpoint files",
                    snapshot.snapshot_id
                )
            }
        };
        let actual_size = FileUtils::dir_size(&restore_path).unwrap_or_else(|e| {
            warn!(
                "failed to compute checkpoint size for {}: {}",
                restore_path, e
            );
            0
        });
        let checkpoint_size = if log::log_enabled!(log::Level::Info) {
            actual_size
        } else {
            0
        };

        // Never wipe populated metadata with an empty checkpoint. Harness
        // daily failures showed FileNotFound after restore from checkpoint_size=0
        // following raft quorum loss (CurvineIO/curvine#1207).
        // get_file_counts() returns (dir_count, file_count).
        {
            let fs_dir = self.fs_dir.read();
            let (dir_count, file_count) = fs_dir.get_file_counts();
            if actual_size == 0 && (dir_count > 0 || file_count > 0) {
                return err_box!(
                    "refusing to apply empty snapshot {} at {} ({} bytes) over filesystem with {} files and {} dirs",
                    snapshot.snapshot_id,
                    restore_path,
                    actual_size,
                    file_count,
                    dir_count
                );
            }
        }

        let mut fs_dir = self.fs_dir.write();
        fs_dir.restore(&restore_path, checkpoint_size)?;
        fs_dir.update_op_id(snapshot.fsm_state.op_id());
        drop(fs_dir);
        let restore_ms = spend.used_ms();
        spend.reset();

        self.mnt_mgr.restore_best_effort();
        let mount_ms = spend.used_ms();

        // Restore both watermarks as the snapshot recorded them. `ufs_applied`
        // may lag `applied`, and that owed range is replayed from the entries
        // the log store keeps, so it must not be clamped to the snapshot index.
        *self.fsm_state()? = snapshot.fsm_state;
        self.refresh_metrics()?;
        self.refresh_replay_readiness()?;

        info!(
            "apply_snapshot: fs_dir_restore={} ms, mount_restore={} ms, total={} ms",
            restore_ms,
            mount_ms,
            restore_ms + mount_ms
        );
        crate::fault_point! {
            sync,
            name: "master.snapshot.apply.after",
            description: "After the metadata store applied a snapshot",
            context: {
                "node" => self.node_id as i64,
                "applied" => self.fsm_state_snapshot()?.applied.index as i64,
            },
        }

        Ok(())
    }

    pub fn apply_entry(&self, entry: JournalEntry) -> CommonResult<()> {
        debug!("replay entry: {:?}", entry);

        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e),

            JournalEntry::CreateFile(e) => self.create_file(e),

            JournalEntry::OverWriteFile(e) => self.overwrite_file(e),

            JournalEntry::AddBlock(e) => self.add_block(e),

            JournalEntry::CompleteFile(e) => self.complete_file(e),

            JournalEntry::Rename(e) => self.rename(e),

            JournalEntry::Delete(e) => self.delete(e),

            JournalEntry::Free(e) => self.free(e),

            JournalEntry::CacheInvalidation(e) => self.cache_invalidation(e),

            JournalEntry::ReopenFile(e) => self.reopen_file(e),

            JournalEntry::Mount(e) => self.mount(e),

            JournalEntry::UnMount(e) => self.unmount(e),

            JournalEntry::SetAttr(e) => self.set_attr(e),

            JournalEntry::Symlink(e) => self.symlink(e),

            JournalEntry::Link(e) => self.link(e),

            JournalEntry::SetLocks(e) => self.set_locks(e),
            JournalEntry::ReleaseWriter(e) => self.release_writer(e),

            JournalEntry::UfsApplied(e) => self.ufs_applied(e),

            JournalEntry::TrashPurge(e) => self.trash_purge(e),

            _ => Ok(()),
        }
    }

    fn trash_purge(&self, entry: TrashPurgeEntry) -> CommonResult<()> {
        debug!(
            "TrashPurge: entry={} paths={}",
            entry.entry,
            entry.paths.len()
        );

        let mut fs_dir = self.fs_dir.write();
        let res = fs_dir.purge_paths(&entry.paths, entry.mtime)?;
        debug!(
            "TrashPurge: entry={} removed inodes={} bytes={}",
            entry.entry, res.inodes, res.bytes
        );
        Ok(())
    }

    fn cache_invalidation(&self, entry: CacheInvalidationEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        fs_dir.store.apply_cache_invalidations(entry.inodes)
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), entry.path, &fs_dir.store)?;
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_dir(name, entry.dir))?;
        Ok(())
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        if inp.is_full() {
            warn!("create_file: file already exists: {:?}", entry);
            return Ok(());
        }
        let name = inp.name().to_string();
        let _ = fs_dir.add_last_inode(inp, InodeView::new_file(name, entry.file))?;
        Ok(())
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("reopen_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let writers_before = file.writer_clients();
        let _ = mem::replace(file, entry.file);

        fs_dir
            .store
            .apply_reopen_file(inode.as_ref(), &writers_before)?;

        Ok(())
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;

        let mut inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("overwrite_file: file not found: {:?}", entry);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let writers_before = file.writer_clients();
        let _ = mem::replace(file, entry.file);

        fs_dir
            .store
            .apply_overwrite_file(inode.as_ref(), &writers_before)?;

        Ok(())
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();

        let inode_id = entry.blocks.first().map(|v| InodeId::get_id(v.id));

        let mut inode = match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, inode_id) {
            Ok(v) => v,
            Err(e) => {
                warn!("add_block: file not found: {:?} {}", entry, e);
                return Ok(());
            }
        };
        let file = inode.as_file_mut()?;
        let _ = mem::replace(&mut file.blocks, entry.blocks);
        fs_dir
            .store
            .apply_new_block(inode.as_ref(), &entry.commit_block)?;

        Ok(())
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();

        let mut inode =
            match MasterFilesystem::resolve_file_inode(&fs_dir, &entry.path, Some(entry.file.id)) {
                Ok(v) => v,
                Err(e) => {
                    warn!("complete_file: file not found: {:?} {}", entry, e);
                    return Ok(());
                }
            };
        let file = inode.as_file_mut()?;
        let writers_before = file.writer_clients();

        let _ = mem::replace(file, entry.file);
        // Update block location
        fs_dir
            .store
            .apply_complete_file(inode.as_ref(), &writers_before, &entry.commit_blocks)?;

        Ok(())
    }
    pub fn rename(&self, entry: RenameEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_src = entry.src;
        let src_inp = InodePath::resolve(fs_dir.root_ptr(), &entry_src, &fs_dir.store)?;
        let dst_inp = InodePath::resolve(fs_dir.root_ptr(), entry.dst, &fs_dir.store)?;
        if src_inp.get_last_inode().is_none() {
            warn!("Rename: source path not found: {}", entry_src);
            return Ok(());
        }
        fs_dir.unprotected_rename(
            &src_inp,
            &dst_inp,
            entry.mtime,
            RenameFlags::new(entry.flags),
            if RenameFlags::new(entry.flags).exchange_mode()
                && entry.src_inode_id != 0
                && entry.dst_inode_id != 0
            {
                Some((entry.src_inode_id, entry.dst_inode_id))
            } else {
                None
            },
        )?;

        Ok(())
    }

    pub fn delete(&self, entry: DeleteEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let entry_path = entry.path;
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry_path, &fs_dir.store)?;
        if inp.get_last_inode().is_none() {
            warn!("Delete: path not found: {}", entry_path);
            return Ok(());
        }
        fs_dir.unprotected_delete(&inp, entry.mtime)?;
        Ok(())
    }

    pub fn free(&self, entry: FreeEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let Some(inode) = inp.get_last_inode() else {
            warn!("Free: path not found: {:?}", entry);
            return Ok(());
        };
        fs_dir.unprotected_free(inode, entry.mtime, entry.recursive)?;
        Ok(())
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        self.mnt_mgr.unprotected_add_mount(entry.info.clone())?;

        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_store_mount(entry.info)?;
        Ok(())
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        if !self.mnt_mgr.has_mounted(entry.id)? {
            warn!("Unmount: id already unmounted: {:?}", entry);
            return Ok(());
        }
        self.mnt_mgr.unprotected_umount_by_id(entry.id)?;
        let mut fs_dir = self.fs_dir.write();
        fs_dir.unprotected_unmount(entry.id)?;
        Ok(())
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &entry.path, &fs_dir.store)?;
        let last_inode = match inp.get_last_inode() {
            Some(v) => v,
            None => {
                warn!("SetAttr: path not found: {:?}", entry);
                return Ok(());
            }
        };

        fs_dir.unprotected_set_attr(last_inode, entry.opts)?;
        Ok(())
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let link_path = entry.link;
        let mut fs_dir = self.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), &link_path, &fs_dir.store)?;
        match fs_dir.unprotected_symlink(inp, entry.new_inode, entry.force) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Symlink: file already exists: {:?}", link_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        let old_path = InodePath::resolve(fs_dir.root_ptr(), &entry.src_path, &fs_dir.store)?;
        let new_path = InodePath::resolve(fs_dir.root_ptr(), &entry.dst_path, &fs_dir.store)?;

        // Get the original inode ID
        let original_inode_id = match old_path.get_last_inode() {
            Some(inode) => inode.id(),
            None => {
                warn!("Link: source path not found: {:?}", entry);
                return Ok(());
            }
        };

        if let Some(mut inode_ptr) = old_path.get_last_inode() {
            if let File(_) = inode_ptr.as_mut() {
                inode_ptr.incr_nlink(entry.mtime);
            }
        }

        match fs_dir.unprotected_link(new_path, original_inode_id, entry.mtime as u64) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {:?}", entry);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        let fs_dir = self.fs_dir.write();
        let before = fs_dir.store.get_locks(entry.ino)?.session_owned_clients();
        fs_dir
            .store
            .apply_set_locks(entry.ino, &before, &entry.locks)
    }

    pub fn release_writer(&self, entry: ReleaseWriterEntry) -> CommonResult<()> {
        let mut fs_dir = self.fs_dir.write();
        fs_dir.release_writer(entry.ino, &entry.client)?;
        Ok(())
    }

    pub fn ufs_applied(&self, entry: UfsAppliedEntry) -> CommonResult<()> {
        let mut lock = self.fsm_state()?;
        if entry.index <= lock.ufs_applied.index {
            return Ok(());
        }
        lock.ufs_applied = AppliedIndex {
            op_id: entry.op_id,
            rpc_id: entry.rpc_id,
            term: entry.term,
            index: entry.index,
        };
        self.refresh_fsm_metrics(&lock);
        Ok(())
    }

    /// The UFS side-effect loader. It carries the replay wiring, so tests replay
    /// single entries through this view instead of rebuilding the state by hand.
    #[doc(hidden)]
    pub fn ufs_loader(&self) -> &UfsLoader {
        &self.ufs_loader
    }

    // Clean up expired checkpoints.
    pub fn purge_checkpoint(&self, current_ck: impl AsRef<str>) -> CommonResult<()> {
        let current_ck = current_ck.as_ref();
        let ck_dir = match Path::new(current_ck).parent() {
            None => return Ok(()),
            Some(v) => v,
        };

        let current_mtime = match Path::new(current_ck).metadata() {
            Ok(meta) => FileUtils::mtime(&meta)?,
            Err(_) => return Ok(()),
        };

        let mut vec = vec![];
        for entry in fs::read_dir(ck_dir)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let mtime = FileUtils::mtime(&meta)?;
            if mtime < current_mtime {
                vec.push((mtime, entry.path()));
            }
        }

        // Sort oldest-first and keep at most (retain_checkpoint_num - 1) older
        // checkpoints so that together with current_ck the total is retain_checkpoint_num.
        vec.sort_by_key(|x| x.0);
        let keep = self.retain_checkpoint_num.saturating_sub(1);
        let del_num = vec.len().saturating_sub(keep);

        for (_, path) in vec.iter().take(del_num) {
            let path = path.as_path();
            FileUtils::delete_path(path, true)?;
            info!("delete expired checkpoint: {}", path.to_string_lossy());
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> RaftResult<()> {
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::Shutdown(tx)).await?;
        rx.receive().await?;
        self.ufs_loader.stop();
        Ok(())
    }

    async fn apply_direct(&self, msg: ApplyMsg) -> RaftResult<()> {
        let result = self.apply_msg(false, &msg).await.map_err(Into::into);

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            Self::complete_entry_ack(msg, result);
            Ok(())
        } else {
            result
        }
    }

    /// The journal is the source of truth, so replay must be a contiguous fold.
    /// A hole would silently skip committed mutations and leave the rebuilt state
    /// inconsistent.
    fn ensure_contiguous_replay(&self, expected: u64, list: &[Entry]) -> CommonResult<()> {
        for (offset, entry) in list.iter().enumerate() {
            let expected_index = expected + offset as u64;
            if entry.index != expected_index {
                self.follower_readiness.set_replay_ready(false);
                return err_box!(
                    "journal replay gap: expected entry {}, found {}",
                    expected_index,
                    entry.index
                );
            }
        }
        Ok(())
    }
}

impl AppStorage for JournalLoader {
    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_direct(msg).await;
        }

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            self.sender.send(msg).await?;
            return Ok(());
        }

        if wait {
            return self.apply_direct(msg).await;
        }

        self.sender.send(msg).await?;
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        match self.fsm_state.lock() {
            Ok(state) => state.clone(),
            Err(e) => {
                error!("fatal fsm_state lock poisoned: {}", e);
                std::process::abort();
            }
        }
    }

    fn hard_state_changed(&self, state: &raft::eraftpb::HardState) {
        self.metrics.journal_committed.set(state.commit as i64);
        self.metrics.journal_term.set(state.term as i64);
        // Every persisted term moves the lease epoch forward; a promotion always
        // carries a higher term than the leadership it replaces.
        self.term.store(state.term, Ordering::Release);
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        if !self.has_apply_worker {
            if role == StateRole::Leader {
                self.catch_up_committed_metadata().await?;
            }
            return Ok(());
        }
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::RoleChange((role, tx))).await?;
        rx.receive().await?
    }

    async fn reset(&self) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.reset0();
        }
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::Reset(tx)).await?;
        rx.receive().await?
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        if !self.has_apply_worker {
            return self.create_snapshot0(None);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::CreateSnapshot(tx);

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_snapshot0(snapshot);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::ApplySnapshot((tx, snapshot));

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self.fs_dir.read();
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::meta::inode::InodeDir;
    use curvine_config::{ClusterConf, MasterConf};
    use curvine_error::FsResult;
    use curvine_model::{AccessMode, MountOptions, WriteType};
    use curvine_runtime::common::{SerdeUtils, Utils};

    // Regression tests for the batch-deserialization skip branch in `apply0`:
    // with `ignore_reply_error` enabled an undecodable batch (typically written
    // by a newer-schema leader during a rolling upgrade) is skipped and the
    // apply watermark advances past it; with the flag disabled the replay error
    // is propagated and the watermark must not move.
    fn ignore_reply_error_conf(
        name: &str,
        ignore_reply_error: bool,
    ) -> (ClusterConf, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "curvine-journal-loader-{}-{}",
            name,
            Utils::rand_id()
        ));
        let conf = ClusterConf {
            testing: true,
            format_master: true,
            master: MasterConf {
                meta_dir: root.join("meta").to_str().unwrap().into(),
                ..Default::default()
            },
            journal: JournalConf {
                journal_dir: root.join("journal").to_str().unwrap().into(),
                io_threads: 1,
                worker_threads: 2,
                ignore_reply_error,
                ..JournalConf::with_test()
            },
            ..Default::default()
        };
        (conf, root)
    }

    // Shorter than the smallest serialized batch (u64 seq_id + u64 batch
    // length), so both the current and the legacy schema reject it.
    fn corrupt_entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: b"corrupt".to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn trash_purge_entry_removes_its_paths_idempotently() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("trash-purge", true);
        let js = JournalSystem::from_conf(&conf)?;
        let fs = js.fs();
        let loader = js.journal_loader();

        // `from_conf` starts without an election; this case only exercises the
        // loader, so promote the node explicitly to reach the write paths.
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        assert!(fs.master_monitor.is_serving());
        fs.create("/victim/a/file-a", true)?;
        fs.create("/victim/a/file-b", true)?;
        fs.create("/victim/b/file-c", true)?;

        let batch = JournalEntry::TrashPurge(TrashPurgeEntry {
            op_id: 77,
            rpc_id: 0,
            entry: "/victim".to_string(),
            mtime: 5,
            paths: vec![
                "/victim/a/file-a".to_string(),
                "/victim/a/file-b".to_string(),
            ],
        });

        loader.apply_entry(batch.clone())?;
        assert!(!fs.exists("/victim/a/file-a")?);
        assert!(!fs.exists("/victim/a/file-b")?);
        // Unlisted siblings and the entry root survive the batch.
        assert!(fs.exists("/victim/b/file-c")?);
        assert!(fs.exists("/victim")?);

        // A replayed batch is a no-op instead of failing on removed paths.
        loader.apply_entry(batch)?;
        assert!(fs.exists("/victim/b/file-c")?);

        drop(loader);
        drop(fs);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn replays_previous_term_entries_and_gate_opens_on_the_new_term_noop() -> FsResult<()> {
        use crate::master::meta::inode::InodeFile;
        use curvine_model::MkdirOptsBuilder;
        use raft::eraftpb::HardState;
        use std::sync::atomic::Ordering;

        let (conf, root) = ignore_reply_error_conf("previous-term-replay", false);
        let js = JournalSystem::from_conf(&conf)?;
        let fs = js.fs();
        let loader = js.journal_loader();

        let dir_id = 100;
        let file_id = 101;

        let mkdir = {
            let mut batch = JournalBatch::new(1);
            batch.push(JournalEntry::Mkdir(MkdirEntry {
                op_id: 1,
                rpc_id: 0,
                path: "/prev".to_string(),
                dir: InodeDir::with_opts(dir_id, 0, MkdirOptsBuilder::new().build()),
            }));
            SerdeUtils::serialize(&batch).unwrap()
        };
        let create = {
            let mut batch = JournalBatch::new(2);
            batch.push(JournalEntry::CreateFile(CreateFileEntry {
                op_id: 2,
                rpc_id: 0,
                path: "/prev/file".to_string(),
                file: InodeFile::new(file_id, 0),
            }));
            SerdeUtils::serialize(&batch).unwrap()
        };

        loader.log_store.append(&[
            Entry {
                index: 1,
                term: 1,
                data: mkdir,
                ..Default::default()
            },
            Entry {
                index: 2,
                term: 1,
                data: create,
                ..Default::default()
            },
        ])?;
        loader.log_store.set_hard_state(&HardState {
            term: 1,
            commit: 2,
            vote: 1,
        })?;

        let rt = conf.journal.create_runtime();
        rt.block_on(async {
            loader
                .apply_msg(false, &ApplyMsg::new_scan(AppliedIndex::default()))
                .await
        })
        .map_err(|e| FsError::common(e.to_string()))?;
        assert_eq!(loader.fsm_state_snapshot()?.applied.index, 2);
        assert_eq!(loader.fsm_state_snapshot()?.applied.term, 1);
        assert!(fs.exists("/prev/file")?);
        assert!(fs.last_inode_id() >= file_id);

        loader.term.store(2, Ordering::Release);
        assert_eq!(loader.applied_term.get(), 1);
        assert_ne!(
            loader.applied_term.get(),
            loader.term.load(Ordering::Acquire)
        );

        loader.log_store.append(&[Entry {
            index: 3,
            term: 2,
            data: vec![],
            ..Default::default()
        }])?;
        loader.log_store.set_hard_state(&HardState {
            term: 2,
            commit: 3,
            vote: 1,
        })?;
        let applied = loader.fsm_state_snapshot()?.applied;
        rt.block_on(async { loader.apply_msg(false, &ApplyMsg::new_scan(applied)).await })
            .map_err(|e| FsError::common(e.to_string()))?;

        assert_eq!(loader.fsm_state_snapshot()?.applied.index, 3);
        assert_eq!(loader.applied_term.get(), 2);
        assert_eq!(loader.term.load(Ordering::Acquire), 2);

        drop(loader);
        drop(fs);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    fn valid_entry(index: u64, term: u64) -> Entry {
        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 41,
            rpc_id: 9,
            inodes: vec![],
        }));
        let data = SerdeUtils::serialize(&batch).unwrap();

        Entry {
            index,
            term,
            data,
            ..Default::default()
        }
    }

    #[test]
    fn follower_skips_corrupt_batch_and_keeps_replaying() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("follower", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        rt.block_on(loader.apply(true, ApplyMsg::new_entry(corrupt_entry(1, 5))))?;
        let state = loader.get_fsm_state();
        assert_eq!(
            state.applied.index, 1,
            "skipped batch must advance the follower watermark"
        );
        assert_eq!(state.applied.term, 5);
        assert_eq!(
            state.ufs_applied.index, 0,
            "follower must not advance the UFS watermark"
        );

        rt.block_on(loader.apply(true, ApplyMsg::new_entry(valid_entry(2, 5))))?;
        let state = loader.get_fsm_state();
        assert_eq!(state.applied.index, 2, "next valid entry must still apply");
        assert_eq!(state.applied.op_id, 41);
        assert_eq!(state.applied.rpc_id, 9);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn leader_advances_only_the_metadata_watermark() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("leader", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();

        for index in 1..=2 {
            loader.apply0(true, true, &corrupt_entry(index, 7))?;
        }
        let state = loader.get_fsm_state();
        assert_eq!(
            state.applied.index, 2,
            "leader advances the metadata watermark"
        );
        assert_eq!(state.applied.term, 7);
        assert_eq!(loader.follower_readiness.snapshot().applied_index, Some(2));
        assert_eq!(
            state.ufs_applied.index, 0,
            "UFS progress belongs to the background replayer, never to apply"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn ufs_replayer_advances_its_own_watermark() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-replay", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        // Metadata apply never moves the UFS watermark; it only publishes work.
        loader.apply0(true, true, &valid_entry(1, 5))?;
        let state = loader.get_fsm_state();
        assert_eq!(state.applied.index, 1);
        assert_eq!(state.ufs_applied.index, 0);
        assert_eq!(
            loader.ufs_loader.replay_range()?,
            (0, 1),
            "the replayer owes (ufs_applied, applied]"
        );

        // The replayer pulls the owed entry straight from the log.
        rt.block_on(loader.ufs_loader.replay_entry(&valid_entry(1, 5)))?;
        let state = loader.get_fsm_state();
        assert_eq!(
            state.ufs_applied.index, 1,
            "the replayer advances ufs_applied on its own"
        );
        assert_eq!(
            loader.ufs_loader.replay_range()?,
            (1, 1),
            "nothing is owed once the replayer caught up"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    // The integration tests in `curvine-server/tests/journal_test.rs` cannot
    // reach a caught-up UFS watermark on their own: the replayer task is not
    // spawned in testing mode. They drive `drain_replay_for_test`, so the drain
    // itself is covered here.
    #[test]
    fn drain_replay_for_test_catches_ufs_applied_up() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-drain", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        js.append_committed_entry_for_test(valid_entry(1, 5))?;
        js.append_committed_entry_for_test(Entry {
            index: 2,
            term: 5,
            ..Default::default()
        })?;
        rt.block_on(async { loader.role_change(StateRole::Leader).await })?;

        let state = loader.get_fsm_state();
        assert_eq!(state.applied.index, 2);
        assert_eq!(
            state.ufs_applied.index, 0,
            "metadata catch-up must leave the UFS watermark to the replayer"
        );

        rt.block_on(loader.ufs_loader.drain_replay_for_test())?;
        let state = loader.get_fsm_state();
        assert_eq!(
            state.ufs_applied.index, state.applied.index,
            "draining the owed range catches the UFS watermark up"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn ufs_replayer_owes_a_release_writer_export() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-release-writer", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::ReleaseWriter(ReleaseWriterEntry {
            op_id: 42,
            rpc_id: 9,
            ino: 4_000_000,
            client: "client-a".to_string(),
        }));
        let entry = Entry {
            index: 1,
            term: 5,
            data: SerdeUtils::serialize(&batch).unwrap(),
            ..Default::default()
        };

        let owed = rt.block_on(loader.ufs_loader.replay_entry(&entry))?;
        assert!(
            owed.is_some(),
            "a replayed release_writer carries a UFS effect and must be reported as owed"
        );
        assert_eq!(loader.get_fsm_state().ufs_applied.index, 1);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    // The replayer applies a whole batch before advancing its watermark, so a
    // failure midway replays the successful prefix. `mkdir a/child; rename a -> b`
    // is the shape that breaks: recreating `a/child` on the retry leaves the
    // rename pointing at a non-empty destination and replay never converges.
    #[test]
    fn retried_mkdir_skips_a_directory_the_rename_moved() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-mkdir-retry", true);
        let ufs_root = root.join("ufs");
        fs::create_dir_all(&ufs_root)?;
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        let ufs_uri = format!("file://{}", ufs_root.display()).replace('\\', "/");
        let info = MountOptions::builder()
            .write_type(WriteType::FsMode)
            .access_mode(AccessMode::ReadWrite)
            .build()
            .to_info(1, "/mnt", &ufs_uri);
        loader.apply_entry(JournalEntry::Mount(MountEntry {
            op_id: 1,
            rpc_id: 0,
            info,
        }))?;

        let mkdir = |op_id: u64, path: &str, id: i64| {
            JournalEntry::Mkdir(MkdirEntry {
                op_id,
                rpc_id: 0,
                path: path.to_string(),
                dir: InodeDir::new(id, 1),
            })
        };
        loader.apply_entry(mkdir(2, "/mnt", 9_000_001))?;
        loader.apply_entry(mkdir(3, "/mnt/a", 9_000_002))?;
        let mkdir_child = mkdir(4, "/mnt/a/child", 9_000_003);
        loader.apply_entry(mkdir_child.clone())?;

        // First replay of the prefix creates the child directory on UFS.
        rt.block_on(loader.ufs_loader.apply_entry(&mkdir_child))?;
        assert!(
            ufs_root.join("a").join("child").is_dir(),
            "the first replay must create the child directory on UFS"
        );

        // The rename lands in metadata and on UFS, then the batch is retried.
        let rename = JournalEntry::Rename(RenameEntry {
            op_id: 5,
            rpc_id: 0,
            src: "/mnt/a".to_string(),
            dst: "/mnt/b".to_string(),
            mtime: 2,
            flags: 0,
            src_inode_id: 0,
            dst_inode_id: 0,
        });
        loader.apply_entry(rename.clone())?;
        rt.block_on(loader.ufs_loader.apply_entry(&rename))?;
        assert!(
            ufs_root.join("b").join("child").is_dir(),
            "the rename must move the created directory on UFS"
        );
        assert!(!ufs_root.join("a").exists());

        rt.block_on(loader.ufs_loader.apply_entry(&mkdir_child))?;
        assert!(
            !ufs_root.join("a").exists(),
            "a retried mkdir must not recreate the directory at its stale path"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn follower_aligns_ufs_watermark_from_the_log_marker() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-marker", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();

        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::UfsApplied(UfsAppliedEntry {
            op_id: 7,
            rpc_id: 8,
            term: 3,
            index: 3,
        }));
        let entry = Entry {
            index: 3,
            term: 3,
            data: SerdeUtils::serialize(&batch).unwrap(),
            ..Default::default()
        };

        loader.apply0(false, false, &entry)?;
        let state = loader.get_fsm_state();
        assert_eq!(
            state.ufs_applied.index, 3,
            "a follower takes the UFS watermark from the leader marker"
        );
        assert_eq!(state.applied.index, 3);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    fn trash_purge_entry(index: u64, paths: Vec<&str>) -> Entry {
        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::TrashPurge(TrashPurgeEntry {
            op_id: 77 + index,
            rpc_id: 0,
            entry: "/victim".to_string(),
            mtime: 5,
            paths: paths.into_iter().map(|p| p.to_string()).collect(),
        }));

        Entry {
            index,
            term: 3,
            data: SerdeUtils::serialize(&batch).unwrap(),
            ..Default::default()
        }
    }

    // The apply path must key off the message shape, not the current role: a
    // new leader replays what the previous leader left committed (`Entry`,
    // no caller parked here), while its own propose arrives as `EntryWithAck`
    // carrying the pre-apply epoch its handler sampled, which the local FSM
    // still matches, so the replay is skipped.
    #[test]
    fn leader_replays_a_replicated_entry_and_skips_its_own_acknowledged_one() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("leader-replay", true);
        let js = JournalSystem::from_conf(&conf)?;
        let fs = js.fs();
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        // `from_conf` starts without an election; this case only exercises the
        // loader, so promote the node explicitly to reach the write paths.
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        assert!(fs.master_monitor.is_serving());
        fs.create("/victim/a/file-a", true)?;
        fs.create("/victim/b/file-b", true)?;

        // Written by the previous leader and committed only after this node won
        // the election: no handler here ever ran, so the entry must apply.
        rt.block_on(loader.apply(
            true,
            ApplyMsg::new_entry(trash_purge_entry(1, vec!["/victim/a/file-a"])),
        ))?;
        assert!(
            !fs.exists("/victim/a/file-a")?,
            "a leader must replay entries carried over from the previous leader"
        );

        // This leader's own propose: the handler already removed the file and
        // is parked on the acknowledgement, so replaying it would be a no-op.
        let (tx, _rx) = CallChannel::channel();
        let epoch = loader.fs_dir.read().preapply_epoch();
        rt.block_on(loader.apply(
            true,
            ApplyMsg::new_entry_with_ack(
                trash_purge_entry(2, vec!["/victim/b/file-b"]),
                tx,
                Some(epoch),
            ),
        ))?;
        assert!(
            fs.exists("/victim/b/file-b")?,
            "an acknowledged entry already ran in the handler and must not be replayed"
        );
        // `EntryWithAck` is handed to the apply worker, so the watermark moves on
        // that task. Wait for it instead of assuming the queued entry already ran.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while loader.get_fsm_state().applied.index < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            loader.get_fsm_state().applied.index,
            2,
            "both entries advance the metadata watermark"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    fn mkdir_entry(index: u64, path: &str, inode_id: i64) -> Entry {
        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::Mkdir(MkdirEntry {
            op_id: 500 + index,
            rpc_id: 0,
            path: path.to_string(),
            dir: InodeDir::new(inode_id, 0),
        }));

        Entry {
            index,
            term: 3,
            data: SerdeUtils::serialize(&batch).unwrap(),
            ..Default::default()
        }
    }

    // Regression test for the #2488 review: a parked `EntryWithAck` may only
    // skip its replay while the local FSM still carries the mutation its
    // handler applied. A batch forwarded by another master (`None`) never ran
    // a handler here, and a batch proposed before a snapshot restore carries a
    // stale epoch whose mutation the restore discarded; both must replay.
    #[test]
    fn entry_with_ack_replays_forwarded_and_stale_epoch_batches() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ack-epoch", true);
        let js = JournalSystem::from_conf(&conf)?;
        let fs = js.fs();
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        let current_epoch = loader.fs_dir.read().preapply_epoch();

        // The acknowledgement is published after the entry applied, so it is
        // the sync point for the apply worker.
        let apply_and_ack = |entry: Entry, epoch: Option<u64>| {
            let (tx, rx) = CallChannel::channel();
            rt.block_on(loader.apply(true, ApplyMsg::new_entry_with_ack(entry, tx, epoch)))
                .expect("queue the entry");
            rt.block_on(rx.receive())
                .expect("apply worker must answer the parked sender")
                .expect("the batch must apply cleanly");
        };

        // Forwarded by another master: no handler ran here, so the entry must
        // be replayed before the parked sender is acknowledged.
        let forwarded_inode = 9_000_001;
        apply_and_ack(mkdir_entry(1, "/forwarded", forwarded_inode), None);
        assert!(
            fs.exists("/forwarded")?,
            "a forwarded batch must be replayed, not skipped"
        );
        assert!(
            loader.fs_dir.read().last_inode_id() >= forwarded_inode,
            "the replayed entry must advance the inode allocator watermark"
        );

        // Proposed before a snapshot restore: the restore wiped the mutation
        // the handler applied, so the sampled epoch no longer matches and the
        // entry must be replayed.
        let stale_inode = 9_000_002;
        apply_and_ack(
            mkdir_entry(2, "/stale", stale_inode),
            Some(current_epoch + 1),
        );
        assert!(
            fs.exists("/stale")?,
            "a batch proposed before a restore must be replayed"
        );
        assert!(loader.fs_dir.read().last_inode_id() >= stale_inode);

        // Proposed at the current epoch: the handler's mutation is still in
        // place, so the replay is skipped and the watermark does not move.
        apply_and_ack(mkdir_entry(3, "/skipped", 9_000_003), Some(current_epoch));
        assert!(
            !fs.exists("/skipped")?,
            "a batch pre-applied at the current epoch must not be replayed"
        );
        assert_eq!(loader.fs_dir.read().last_inode_id(), stale_inode);
        assert_eq!(
            loader.get_fsm_state().applied.index,
            3,
            "every entry advances the metadata watermark"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn corrupt_batch_fails_replay_without_ignore_reply_error() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("strict", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        let error = rt
            .block_on(loader.apply(true, ApplyMsg::new_entry(corrupt_entry(1, 5))))
            .expect_err("corrupt batch must fail replay when ignore_reply_error is false");
        assert!(
            error
                .to_string()
                .contains("failed to deserialize journal batch"),
            "unexpected error: {}",
            error
        );
        assert_eq!(
            loader.get_fsm_state().applied.index,
            0,
            "failed replay must not move the watermark"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    // `set_entries` writes the entries it is given without enforcing raft log
    // continuity, so it can build the exact journal state a corrupt or
    // partially compacted log presents to replay.
    fn gap_entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn scan_fails_closed_on_a_gap_inside_a_batch() -> FsResult<()> {
        use raft::eraftpb::HardState;

        let (conf, root) = ignore_reply_error_conf("scan-gap-batch", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        loader
            .log_store
            .set_entries(&[gap_entry(1, 1), gap_entry(3, 1)])?;
        loader.log_store.set_hard_state(&HardState {
            term: 1,
            commit: 3,
            vote: 1,
        })?;

        let error = rt
            .block_on(loader.apply_msg(false, &ApplyMsg::new_scan(AppliedIndex::default())))
            .expect_err("a batch with a missing index must not be folded");
        assert!(
            error
                .to_string()
                .contains("journal replay gap: expected entry 2, found 3"),
            "unexpected error: {}",
            error
        );
        assert_eq!(
            loader.get_fsm_state().applied.index,
            0,
            "a rejected batch must not move the watermark"
        );
        assert!(
            !loader.follower_readiness.snapshot().replay_ready,
            "replay readiness must stay closed after a gap"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_folds_a_contiguous_batch() -> FsResult<()> {
        use raft::eraftpb::HardState;

        let (conf, root) = ignore_reply_error_conf("scan-contiguous", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        loader
            .log_store
            .set_entries(&[gap_entry(1, 1), gap_entry(2, 1), gap_entry(3, 1)])?;
        loader.log_store.set_hard_state(&HardState {
            term: 1,
            commit: 3,
            vote: 1,
        })?;

        rt.block_on(loader.apply_msg(false, &ApplyMsg::new_scan(AppliedIndex::default())))
            .map_err(|e| FsError::common(e.to_string()))?;
        assert_eq!(
            loader.get_fsm_state().applied.index,
            3,
            "a contiguous batch must be folded in full"
        );
        assert!(
            loader.follower_readiness.snapshot().replay_ready,
            "replay catches up to the commit index"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn scan_fails_closed_on_a_gap_across_batches() -> FsResult<()> {
        use raft::eraftpb::HardState;

        let (mut conf, root) = ignore_reply_error_conf("scan-gap-across-batches", false);
        // Three entries per scan: [1, 2] is folded, then [4] is read with 3
        // missing, so the gap is only visible across two batches.
        conf.journal.scan_batch_size = 3;
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        loader
            .log_store
            .set_entries(&[gap_entry(1, 1), gap_entry(2, 1), gap_entry(4, 1)])?;
        loader.log_store.set_hard_state(&HardState {
            term: 1,
            commit: 4,
            vote: 1,
        })?;

        let error = rt
            .block_on(loader.apply_msg(false, &ApplyMsg::new_scan(AppliedIndex::default())))
            .expect_err("a batch after a hole must not be folded");
        assert!(
            error
                .to_string()
                .contains("journal replay gap: expected entry 3, found 4"),
            "unexpected error: {}",
            error
        );
        assert_eq!(
            loader.get_fsm_state().applied.index,
            2,
            "only the contiguous batch before the hole is applied"
        );
        assert!(
            !loader.follower_readiness.snapshot().replay_ready,
            "replay readiness must stay closed after a gap"
        );

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }
}
