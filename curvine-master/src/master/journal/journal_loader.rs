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

use crate::master::fs::MasterFilesystem;
use crate::master::journal::recovery::{
    classify_apply_error, should_defer_ufs, ApplyDisposition, RecoveryGate,
};
use crate::master::journal::*;
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::inode::InodeView::File;
use crate::master::meta::inode::{InodePath, InodeView};
use crate::master::meta::store::{JournalCursorStore, RocksInodeStore};
use crate::master::meta::{
    BlockLocationLockManager, ColdError, ColdFsDirGuard, CommitGate, FsDir, InodeId, InodeLockMode,
    InodeLockRequest, NamespaceGuard,
};
use crate::master::meta::{FsDirHandle, InodeLockManager};

use crate::master::space::MetadataScanControl;
use crate::master::{JobManager, Master, MasterMetrics, MountManager, SyncWorkerManager};
use curvine_config::DBConf;
use curvine_config::JournalConf;
use curvine_core_error::{err_box, ternary, CommonResult, ErrorExt};
use curvine_error::{FsError, FsResult};
use curvine_model::RenameFlags;
use curvine_raft::conf::JournalConfExt;
use curvine_raft::proto::raft::{AppliedIndex, FsmState, SnapshotData};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg, LogStorage, RocksLogStorage};
use curvine_raft::raft::{RaftClient, RaftError, RaftResult, RaftUtils};
use curvine_raft::utils::SerdeUtils;
use curvine_runtime::common::{FileUtils, TimeSpent};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel};
use curvine_runtime::sync::AtomicCounter;
use log::{debug, error, info, warn};
use prost::Message as _;
use raft::eraftpb::{Entry, EntryType};
use raft::{StateRole, Storage};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::{fs, mem};
use tokio::sync::Notify;

#[cfg(test)]
#[path = "tests/role_queue_fence_test.rs"]
mod role_queue_fence_test;

#[cfg(test)]
#[path = "tests/scan_worker_test.rs"]
mod scan_worker_test;

/// Test-only gate used by the snapshot-cancellation regression. The
/// implementation lives with the tests; the core file only wires it in.
#[cfg(test)]
#[path = "tests/snapshot_apply_gate.rs"]
pub(crate) mod snapshot_apply_gate;

#[cfg(test)]
#[path = "tests/snapshot_cancel_gate_test.rs"]
mod snapshot_cancel_gate_test;

#[cfg(test)]
#[path = "tests/exclusive_progress_test.rs"]
mod exclusive_progress_test;

enum UfsDrive {
    CaughtUp,
    Advanced,
    WaitWorkers,
    Retry(String),
    Stopped,
}

// Replay the master metadata operation log.
#[derive(Clone)]
pub struct JournalLoader {
    node_id: u64,
    client: RaftClient,
    group_size: usize,
    fs_dir: Arc<FsDirHandle>,
    inode_locks: Arc<InodeLockManager>,
    meta_data_dir: String,
    scan_control: Arc<MetadataScanControl>,
    inode_id: Arc<InodeId>,
    op_id: Arc<AtomicCounter>,
    mnt_mgr: Arc<MountManager>,
    journal_writer: Arc<JournalWriter>,
    block_location_locks: Arc<BlockLocationLockManager>,
    ufs_loader: UfsLoader,
    worker_manager: SyncWorkerManager,
    worker_notify: Arc<Notify>,
    recovery: RecoveryGate,
    is_leader: Arc<AtomicBool>,
    ufs_wake: Arc<Notify>,
    log_store: RocksLogStorage,
    sender: AsyncSender<ApplyMsg>,
    fsm_state: Arc<Mutex<FsmState>>,
    retain_checkpoint_num: usize,
    ignore_reply_error: bool,
    applied_req_window: u64,
    max_retry_num: u64,
    skip_failed_ufs_replay_after_retry: bool,
    batch_size: u64,
    retry_interval: Duration,
    metrics: &'static MasterMetrics,
    has_apply_worker: bool,
    metadata_commit_gate: Arc<CommitGate>,
    ufs_shutdown: Arc<AtomicBool>,
    ufs_stopped: tokio::sync::watch::Sender<bool>,
    /// Replaceable handle for the cursor row; the applier must persist the
    /// applied boundary without waiting on the namespace guard, and restore
    /// republishes the new store into the same handle.
    cursor_store: JournalCursorStore,
}

impl JournalLoader {
    /// Reset namespace rows for journal-only recovery when the local rows may
    /// contain uncommitted mutations.
    ///
    /// The write path materializes RocksDB rows before the Raft commit
    /// receipt, so a crash can leave durable rows that no committed entry
    /// covers. When no registered snapshot exists and the log still retains
    /// every entry from index 1 (nothing was compacted — compaction only
    /// follows a registered snapshot), the committed log alone can rebuild the
    /// namespace: wipe the rows and reset the cursor, then the normal startup
    /// scan replays `[1..commit]` with the same convergence guarantee the
    /// follower path already has. Member recovery (recover_from_peers or a
    /// recovery marker) bypasses this: it replaces state from a peer instead.
    ///
    /// A crash mid-replay re-runs the whole wipe+replay on restart, so the
    /// protocol converges without a separate staging marker.
    pub fn maybe_reset_namespace_for_replay(
        log_store: &RocksLogStorage,
        meta_db_conf: DBConf,
        conf: &JournalConf,
        recovering: bool,
        format_master: bool,
    ) -> CommonResult<bool> {
        if !conf.enable || recovering || format_master {
            return Ok(false);
        }
        if log_store.latest_snapshot()?.is_some() {
            return Ok(false);
        }
        if log_store.first_index()? != 1 {
            return err_box!(
                "journal log starts at {} without a registered snapshot; \
                 namespace rows cannot be rebuilt from the committed log",
                log_store.first_index()?
            );
        }
        let committed = log_store.hard_state().commit;
        info!(
            "no registered snapshot: resetting namespace rows for committed-log \
             replay, commit {}",
            committed
        );
        let store = RocksInodeStore::new(meta_db_conf, false)?;
        store.reset_namespace_for_replay()?;
        info!(
            "namespace rows reset complete; replaying committed log [1..{}]",
            committed
        );
        Ok(true)
    }

    pub fn new_replay_loader(
        fs_dir: Arc<FsDirHandle>,
        inode_locks: Arc<InodeLockManager>,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
    ) -> CommonResult<Self> {
        let rt = conf.create_runtime();
        let client = RaftClient::from_conf(rt.clone(), conf);
        let recovery = RecoveryGate::new();
        let journal_writer = Arc::new(JournalWriter::new(true, client, conf, recovery.clone())?);
        let log_store = RocksLogStorage::from_conf(conf, false);
        let worker_manager = SyncWorkerManager::new(crate::master::fs::WorkerManager::new(
            &curvine_config::ClusterConf::default(),
        )?);
        Self::build(
            rt,
            fs_dir,
            inode_locks,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            worker_manager,
            recovery,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rt: Arc<Runtime>,
        fs_dir: Arc<FsDirHandle>,
        inode_locks: Arc<InodeLockManager>,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        worker_manager: SyncWorkerManager,
        recovery: RecoveryGate,
    ) -> CommonResult<Self> {
        Self::build(
            rt,
            fs_dir,
            inode_locks,
            mnt_mgr,
            conf,
            job_manager,
            log_store,
            journal_writer,
            worker_manager,
            recovery,
            false,
        )
    }

    pub(crate) fn validate_conf(conf: &JournalConf) -> CommonResult<()> {
        if conf.scan_batch_size == 0 {
            return err_box!("journal scan_batch_size must be greater than zero");
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        rt: Arc<Runtime>,
        fs_dir: Arc<FsDirHandle>,
        inode_locks: Arc<InodeLockManager>,
        mnt_mgr: Arc<MountManager>,
        conf: &JournalConf,
        job_manager: Arc<JobManager>,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        worker_manager: SyncWorkerManager,
        recovery: RecoveryGate,
        testing: bool,
    ) -> CommonResult<Self> {
        let client = RaftClient::from_conf(rt.clone(), conf);
        Self::validate_conf(conf)?;
        // Install the fs_mode release gate before anything can serve a release:
        // with transfer enabled the replayer no longer waits for exports, so an
        // unverified Free would drop Curvine data UFS never received.
        if job_manager.transfer_enabled() {
            let releaser = crate::master::journal::FreeReconciler::start(job_manager.clone());
            job_manager.master_fs().set_fs_mode_releaser(releaser);
        }

        let metadata_commit_gate = job_manager.master_fs().metadata_commit_gate();
        let block_location_locks = job_manager.master_fs().block_location_locks.clone();
        let ufs_loader = UfsLoader::new(job_manager, fs_dir.clone(), conf);
        let (sender, receiver) = AsyncChannel::new(conf.writer_channel_size).split();
        let worker_notify = worker_manager.read().registered_notify();
        // The cursor and metadata rows are persisted independently. An invalid
        // cursor cannot safely be replaced by zero: replay may mutate already
        // materialized metadata a second time.
        let persisted_applied = fs_dir.view()?.load_journal_applied()?;
        let mut initial_state = FsmState::default();
        if let Some(ref bytes) = persisted_applied {
            let applied = match AppliedIndex::decode(bytes.as_slice()) {
                Ok(applied) => applied,
                Err(error) => return err_box!("invalid journal applied cursor: {}", error),
            };
            if applied.index > 0 {
                // A local applied cursor must never claim entries beyond the
                // durable Raft commit. Offline replay uses a separate loader.
                if conf.enable && !testing {
                    let committed = log_store.hard_state().commit;
                    if applied.index > committed {
                        return err_box!(
                            "journal applied cursor {} exceeds raft commit {}",
                            applied.index,
                            committed
                        );
                    }
                    match log_store.term(applied.index) {
                        Ok(term) if term != applied.term => {
                            return err_box!(
                                "journal applied cursor term mismatch at index {}: cursor {}, raft {}",
                                applied.index, applied.term, term
                            );
                        }
                        Ok(_) => {}
                        // An already compacted entry cannot be checked here;
                        // snapshot recovery must establish the baseline separately.
                        Err(raft::Error::Store(raft::StorageError::Compacted)) => {}
                        Err(error) => {
                            return err_box!(
                                "failed to validate journal applied cursor at index {}: {}",
                                applied.index,
                                error
                            );
                        }
                    }
                }
                info!(
                    "recover journal applied cursor from meta store: {:?}",
                    applied
                );
                initial_state.applied = applied;
            }
        } else {
            info!(
                "no persisted journal applied cursor; recovery requires a snapshot or log replay"
            );
        }
        let (meta_data_dir, scan_control, inode_id, op_id, cursor_store) = {
            let dir = fs_dir.view()?;
            (
                dir.store.store().db.conf().data_dir.clone(),
                dir.store.scan_control().clone(),
                dir.inode_id.clone(),
                dir.op_id.clone(),
                dir.store.cursor_store(),
            )
        };
        let loader = Self {
            node_id: conf.node_id()?,
            client,
            group_size: conf.journal_addrs.len().max(1),
            block_location_locks,
            fs_dir,
            meta_data_dir,
            scan_control,
            inode_id,
            op_id,
            mnt_mgr,
            journal_writer,
            ufs_loader,
            worker_manager,
            worker_notify,
            recovery,
            inode_locks,
            is_leader: Arc::new(AtomicBool::new(false)),
            ufs_wake: Arc::new(Notify::new()),
            log_store,
            sender,
            fsm_state: Arc::new(Mutex::new(initial_state)),
            retain_checkpoint_num: conf.retain_checkpoint_num,
            ignore_reply_error: conf.ignore_reply_error,
            applied_req_window: conf.raft_retry_cache_size,
            max_retry_num: conf.max_retry_num,
            skip_failed_ufs_replay_after_retry: conf.skip_failed_ufs_replay_after_retry,
            batch_size: conf.scan_batch_size,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
            metrics: Master::get_metrics()?,
            has_apply_worker: !testing,
            metadata_commit_gate,
            ufs_shutdown: Arc::new(AtomicBool::new(false)),
            ufs_stopped: tokio::sync::watch::channel(testing).0,
            cursor_store,
        };

        loader.refresh_metrics()?;
        if !testing {
            let apply_loader = loader.clone();
            rt.spawn(async move {
                Self::run_apply(apply_loader, receiver).await;
            });
            let replay_loader = loader.clone();
            let stopped = loader.ufs_stopped.clone();
            rt.spawn(async move {
                replay_loader.run_ufs_replay().await;
                stopped.send_replace(true);
            });
        }

        Ok(loader)
    }

    /// The cursor and metadata are separate writes, so crash consistency is
    /// not guaranteed. A cursor write failure must fence and fail the apply.
    fn persist_applied(&self, applied: &AppliedIndex) -> CommonResult<()> {
        if let Err(error) = self.cursor_store.put(
            RocksInodeStore::JOURNAL_APPLIED_KEY,
            &applied.encode_to_vec(),
        ) {
            let message = format!(
                "failed to persist journal applied cursor at index {}: {}",
                applied.index, error
            );
            self.recovery.enter_safe_mode(&message);
            return err_box!("{}", message);
        }
        Ok(())
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

    fn set_applied(
        &self,
        is_leader: bool,
        applied: AppliedIndex,
        _has_ufs_affecting: bool,
    ) -> CommonResult<()> {
        // Metadata and UFS watermarks move independently. UFS success is
        // recorded by the replay coordinator, not by this apply path.
        let mut state = self.fsm_state()?;
        if applied.index < state.applied.index {
            let message = format!(
                "metadata applied cursor regressed from {} to {}",
                state.applied.index, applied.index
            );
            self.recovery.enter_safe_mode(&message);
            return err_box!("{}", message);
        }
        if applied.index > 0
            && applied.index == state.applied.index
            && applied.term != state.applied.term
        {
            let message = format!(
                "metadata applied term changed at index {}: {} to {}",
                applied.index, state.applied.term, applied.term
            );
            self.recovery.enter_safe_mode(&message);
            return err_box!("{}", message);
        }
        self.persist_applied(&applied)?;
        let applied_index = applied.index;
        state.applied = applied;
        self.refresh_fsm_metrics(&state);
        drop(state);
        if is_leader {
            self.journal_writer.advance_metadata_applied(applied_index);
        } else {
            self.journal_writer.advance_metadata_catch_up(applied_index);
        }
        self.hard_state_changed(&self.log_store.hard_state());
        if is_leader {
            self.ufs_wake.notify_one();
        }
        Ok(())
    }

    fn commit_ufs_progress(&self, applied: AppliedIndex, log_marker: bool) -> CommonResult<()> {
        if log_marker {
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)?;
        }
        let mut state = self.fsm_state()?;
        if applied.index >= state.ufs_applied.index {
            state.ufs_applied = applied;
        }
        self.refresh_fsm_metrics(&state);
        Ok(())
    }

    fn refresh_fsm_metrics(&self, state: &FsmState) {
        self.metrics.journal_applied.set(state.applied.index as i64);
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
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

    /// The proposing request's ID, recorded by the Raft layer in the entry
    /// context so a duplicate proposal can be recognized after a restart or
    /// failover. Empty for no-op/config entries.
    fn entry_req_id(entry: &Entry) -> Option<i64> {
        let context = entry.get_context();
        if context.is_empty() {
            return None;
        }
        SerdeUtils::deserialize(context).ok()
    }

    /// Advance the committed apply high-water mark without replaying any
    /// metadata op: the no-op/config branch and batches deliberately skipped
    /// under `ignore_reply_error` both need the watermark to move so that
    /// replay can continue past them.
    fn advance_watermark(&self, is_leader: bool, cur: FsmState, entry: &Entry) -> CommonResult<()> {
        let mut applied = cur.applied;
        applied.term = entry.term;
        applied.index = entry.index;
        self.set_applied(is_leader, applied, false)
    }

    async fn replay_entry(&self, is_leader: bool, entry: &JournalEntry) -> CommonResult<()> {
        // The counters survive restore and advance monotonically. Do not take
        // the namespace lock here: a writer can hold it while the bounded
        // journal queue waits for this apply to finish.
        FsDir::advance_op_id(&self.op_id, entry.op_id());
        if let Some(inode_id) = entry.inode_id() {
            self.inode_id.advance_to(inode_id);
        }

        if is_leader {
            // The leader already applied this metadata in the RPC handler, or
            // catch-up did. UFS side effects run on the coordinator.
            Ok(())
        } else {
            self.apply_entry(entry.clone())
        }
    }

    async fn apply0(
        &self,
        is_leader: bool,
        entry: &Entry,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        let cur = self.fsm_state_snapshot()?;
        if entry.index > 0 && entry.index == cur.applied.index && entry.term != cur.applied.term {
            let message = format!(
                "metadata applied term changed at index {}: {} to {}",
                entry.index, cur.applied.term, entry.term
            );
            self.recovery.enter_safe_mode(&message);
            return err_box!("{}", message);
        }
        let role_applied = ternary!(is_leader, cur.ufs_applied.index, cur.applied.index);
        // A leader can retry its current entry while UFS is behind (notably
        // to finish snapshot publication), but an older entry must never
        // rewind the metadata cursor or reprocess an obsolete snapshot.
        if entry.index < cur.applied.index || entry.index <= role_applied {
            info!(
                "skip entry index {}, term {}, fsm_state {:?}",
                entry.index, entry.term, cur
            );
            return Ok(());
        }

        // Empty leader no-ops and configuration entries have no metadata mutation,
        // but still advance the committed apply high-water mark.
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            return self.advance_watermark(is_leader, cur, entry);
        }

        let batch = match JournalBatch::decode(&entry.data) {
            Ok(batch) => batch,
            Err(e) if self.ignore_reply_error => {
                error!(
                    "skip failed journal batch deserialization, entry index={}, term={}, error={}",
                    entry.index, entry.term, e
                );
                return self.advance_watermark(is_leader, cur, entry);
            }
            Err(e) => return err_box!("failed to decode journal batch: {}", e),
        };

        // A duplicate proposal can reach a different leader after a failover
        // or reach this node again after a restart; the in-process retry cache
        // cannot see across either boundary. The persisted applied-request
        // record is the durable dedup boundary: the same proposing request
        // must never replay its metadata a second time.
        let request_id = Self::entry_req_id(entry);
        if let Some(req_id) = request_id {
            match self.cursor_store.get_applied_req(req_id) {
                Ok(Some(applied_req)) => {
                    warn!(
                        "skip duplicate journal batch: request {} already applied at index {}, duplicate entry index {}, term {}",
                        req_id, applied_req.index, entry.index, entry.term
                    );
                    return self.advance_watermark(is_leader, cur, entry);
                }
                Ok(None) => {}
                Err(error) => {
                    return err_box!(
                        "failed to read applied-request record for request {}: {}",
                        req_id,
                        error
                    )
                }
            }
        }

        let batch_len = batch.len();
        let mut snapshot = None;
        let mut applied = Self::build_applied(entry);
        // Operation IDs can be allocated before independent writers enqueue
        // their entries, and UFS markers can carry older IDs. Preserve the
        // maximum committed ID for snapshot recovery and future allocation.
        applied.op_id = cur.applied.op_id;
        let mut has_ufs_affecting = false;

        for (seq, op_entry) in batch.batch.into_iter().enumerate() {
            applied.op_id = applied.op_id.max(op_entry.op_id());
            applied.rpc_id = op_entry.rpc_id();

            match op_entry {
                JournalEntry::Snapshot(e) if is_leader && e.node_id == self.node_id => {
                    if seq + 1 != batch_len {
                        return err_box!("snapshot should be the last entry");
                    }
                    snapshot.replace(e);
                    continue;
                }

                JournalEntry::CacheInvalidation(_)
                | JournalEntry::UfsApplied(_)
                | JournalEntry::UfsDeferred(_) => (),

                // A purge only touches the local trash namespace, so it owes no
                // UFS acknowledgement record.
                JournalEntry::TrashPurge(_) => (),

                _ => has_ufs_affecting = true,
            }

            if let Err(e) = self.replay_entry(is_leader, &op_entry).await {
                if self.ignore_reply_error {
                    error!(
                        "skip failed journal replay, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                if is_leader && skip_ufs_error {
                    error!(
                        "skip failed UFS replay after retries, entry index={}, term={}, journal={:?}, error={}",
                        entry.index, entry.term, op_entry, e
                    );
                    continue;
                }

                return err_box!("failed to apply journal: {:?}: {}", op_entry, e);
            }
        }

        // Record the proposing request before the cursor advance so every
        // crash window between rows and cursor keeps the request marked as
        // applied: with a snapshot the duplicate is skipped and the cursor
        // catches up; without one the rebuild wipes rows and records together
        // and replays everything from scratch.
        if let Some(req_id) = request_id {
            self.cursor_store
                .put_applied_req(req_id, entry.index, entry.term)?;
        }
        self.set_applied(is_leader, applied, has_ufs_affecting)?;
        if request_id.is_some() {
            let entry_index = entry.index;
            let window = self.applied_req_window;
            if entry_index % 8192 == 0 {
                self.cursor_store
                    .prune_applied_req_below(entry_index.saturating_sub(window))?;
            }
        }

        if let Some(e) = snapshot {
            let spent = TimeSpent::new();
            // A leader snapshot claims every entry up to `applied` and lets the
            // log drop them. If the UFS replay is behind, those entries are
            // still owed a copy: publishing this snapshot would delete them and
            // strand the replay forever. Skip it and keep the log.
            let state = {
                let mut state = self.fsm_state()?;
                // A snapshot-only entry has no UFS side effect. If the UFS
                // cursor reached the preceding entry, advance it here rather
                // than racing the asynchronous replay after set_applied().
                if batch_len == 1
                    && state.applied.index == entry.index
                    && state.ufs_applied.index.checked_add(1) == Some(entry.index)
                {
                    let mut ufs_applied = state.applied.clone();
                    ufs_applied.op_id = ufs_applied.op_id.max(state.ufs_applied.op_id);
                    state.ufs_applied = ufs_applied;
                    self.refresh_fsm_metrics(&state);
                }
                state.clone()
            };
            if !state.ufs_replay_caught_up() {
                info!(
                    "skip leader snapshot {}: UFS replay at i{} is behind applied i{}",
                    e.dir, state.ufs_applied.index, state.applied.index
                );
                if let Err(err) = self.purge_checkpoint(&e.dir) {
                    warn!("purge skipped leader snapshot {}: {}", e.dir, err);
                }
            } else {
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
        }

        Ok(())
    }

    async fn apply_msg(
        &self,
        is_leader: bool,
        msg: &ApplyMsg,
        skip_ufs_error: bool,
    ) -> CommonResult<()> {
        match msg {
            ApplyMsg::Entry(entry) | ApplyMsg::EntryWithAck((entry, _)) => {
                self.apply0(is_leader, entry, skip_ufs_error).await?;
                Ok(())
            }

            ApplyMsg::Scan(applied_index) => {
                let mut last_applied = applied_index.index;
                if is_leader && skip_ufs_error {
                    last_applied = last_applied.max(self.fsm_state_snapshot()?.ufs_applied.index);
                }

                let commit_index = self.log_store.hard_state().commit;
                loop {
                    if last_applied >= commit_index {
                        return Ok(());
                    }

                    let low = last_applied + 1;
                    let end = commit_index.checked_add(1).ok_or_else(|| {
                        "cannot scan a committed journal index at u64::MAX".to_string()
                    })?;
                    let high = low.saturating_add(self.batch_size).min(end);
                    let list = self.log_store.scan_entries(low, high)?;

                    if list.is_empty() {
                        return err_box!(
                            "corrupt journal: missing committed journal entry at index {} (commit {})",
                            low,
                            commit_index
                        );
                    };

                    info!(
                        "replay-scan, start_index: {}, entries: {}, commit_index: {}",
                        last_applied + 1,
                        list.len(),
                        commit_index
                    );

                    for entry in list {
                        if entry.index != last_applied + 1 {
                            return err_box!(
                                "corrupt journal: missing committed journal entry at index {} (found {}, commit {})",
                                last_applied + 1,
                                entry.index,
                                commit_index
                            );
                        }
                        self.apply0(is_leader, &entry, skip_ufs_error).await?;
                        last_applied = entry.index;
                        if skip_ufs_error {
                            return Ok(());
                        }
                    }
                }
            }

            _ => err_box!("unsupported apply message in journal loader apply_msg"),
        }
    }

    fn complete_entry_ack(msg: ApplyMsg, result: RaftResult<()>) {
        if let ApplyMsg::EntryWithAck((_, tx)) = msg {
            if let Err(e) = tx.send(result) {
                warn!("send journal entry apply acknowledgement failed: {}", e);
            }
        }
    }

    /// Enable persisted directory child counts only when every peer in the
    /// Raft group reports it can read the newer record format.
    ///
    /// The probe rides the existing Ping RPC. Re-evaluated on every leader
    /// promotion (covering membership changes, restarts, and term changes)
    /// and reset whenever this node leaves the leader role, so a downgrade or
    /// an unreachable peer immediately returns new records to the legacy
    /// format. Readers always accept both formats, so flipping the flag is
    /// safe; it only decides what NEW records encode.
    async fn negotiate_record_format(&self) {
        curvine_model::persist_children_count(false);
        // Ping this node first: the reply carries the group membership, so no
        // extra configuration plumbing is needed here. A configured
        // single-node group skips the probe entirely.
        if self.group_size <= 1 {
            curvine_model::persist_children_count(true);
            info!("single-node raft group: persisted child counts enabled");
            return;
        }
        let peers = match self.client.ping(self.node_id).await {
            Ok(response) => response.group,
            Err(error) => {
                warn!(
                    "record-format negotiation skipped: cannot list raft group: {}",
                    error
                );
                return;
            }
        };
        let others: Vec<_> = peers
            .iter()
            .filter(|peer| peer.node_id != self.node_id)
            .collect();
        if others.is_empty() {
            curvine_model::persist_children_count(true);
            info!("single-node raft group: persisted child counts enabled");
            return;
        }
        for peer in others {
            let peer_id = peer.node_id;
            let readable = match self.client.ping(peer_id).await {
                Ok(response) => {
                    response.reader_format.unwrap_or(0)
                        >= curvine_raft::raft::RECORD_FORMAT_READABLE
                }
                Err(error) => {
                    warn!(
                        "record-format negotiation: ping node {} failed: {}",
                        peer_id, error
                    );
                    false
                }
            };
            if !readable {
                info!(
                    "record format negotiation: node {} cannot read the newer format yet; child counts stay unpersisted",
                    peer_id
                );
                return;
            }
        }
        curvine_model::persist_children_count(true);
        info!("record format negotiation: all replicas can read the newer format; persisted child counts enabled");
    }

    async fn catch_up_committed_metadata(&self) -> RaftResult<()> {
        let committed = self.log_store.hard_state().commit;
        let applied = self.fsm_state_snapshot()?.applied;
        self.journal_writer
            .begin_metadata_catch_up(applied.index, committed);
        self.apply_msg(false, &ApplyMsg::new_scan(applied), false)
            .await?;

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
    fn publish_leader_if_current(&self, epoch: u64) -> bool {
        let caught_up = self
            .fsm_state_snapshot()
            .map(|state| {
                state.ufs_applied.index >= state.applied.index
                    && self.deferred_ufs_count().unwrap_or(1) == 0
            })
            .unwrap_or(false);
        let published = self.journal_writer.unfence_metadata_if_epoch(epoch, || {
            self.recovery.bump_generation();
            self.recovery.set_ufs_caught_up(caught_up);
            if !caught_up {
                self.recovery.set_pending(1);
            }
            self.is_leader.store(true, Ordering::Release);
        });
        if published {
            self.ufs_wake.notify_one();
        }
        published
    }

    async fn run_apply(self, mut receiver: AsyncReceiver<ApplyMsg>) {
        let mut is_leader = false;
        let mut retry_msg: Option<ApplyMsg> = None;
        let mut retry_deadline: Option<tokio::time::Instant> = None;
        let mut buffered: VecDeque<ApplyMsg> = VecDeque::new();
        let mut logged_skip_ignored = false;

        loop {
            let apply_msg = if retry_msg.is_some() {
                let deadline = match retry_deadline {
                    Some(deadline) => deadline,
                    None => {
                        let deadline = tokio::time::Instant::now() + self.retry_interval;
                        retry_deadline = Some(deadline);
                        deadline
                    }
                };
                if tokio::time::Instant::now() >= deadline {
                    retry_deadline = None;
                    match retry_msg.take() {
                        Some(msg) => msg,
                        None => continue,
                    }
                } else {
                    tokio::select! {
                        biased;
                        _ = tokio::time::sleep_until(deadline) => {
                            retry_deadline = None;
                            match retry_msg.take() {
                                Some(msg) => msg,
                                None => continue,
                            }
                        }
                        incoming = receiver.recv() => {
                            match incoming {
                                Some(ApplyMsg::RoleChange(change)) => ApplyMsg::RoleChange(change),
                                Some(ApplyMsg::Shutdown(tx)) => ApplyMsg::Shutdown(tx),
                                Some(ApplyMsg::CreateSnapshot(tx)) => ApplyMsg::CreateSnapshot(tx),
                                Some(msg @ ApplyMsg::ScanWithAck(_)) => msg,
                                Some(other) => {
                                    buffered.push_back(other);
                                    continue;
                                }
                                None => break,
                            }
                        }
                    }
                }
            } else if let Some(msg) = buffered.pop_front() {
                msg
            } else {
                match receiver.recv().await {
                    Some(msg) => msg,
                    None => break,
                }
            };

            match apply_msg {
                ApplyMsg::CreateSnapshot(tx) => {
                    // A retrying batch may already have applied some operations.
                    // Its watermark has not advanced, so that state cannot be snapshotted.
                    let result = if retry_msg.is_some() {
                        Err(RaftError::raft(raft::Error::Store(
                            raft::StorageError::SnapshotTemporarilyUnavailable,
                        )))
                    } else {
                        // Test-only interleaving point: keyed by this
                        // loader's meta dir so a concurrent loader cannot
                        // consume another test's gate.
                        #[cfg(test)]
                        snapshot_apply_gate::park(&self.meta_data_dir);
                        self.create_snapshot0(None)
                    };
                    if let Err(e) = tx.send(result) {
                        warn!("send create snapshot result failed: {}", e);
                    }
                }

                ApplyMsg::ScanWithAck((applied, tx)) => {
                    // Preserve the direct recovery scan's non-leader semantics.
                    let result = self
                        .apply_msg(false, &ApplyMsg::new_scan(applied), false)
                        .await
                        .map_err(Into::into);
                    if let Err(e) = tx.send(result) {
                        warn!("send scan result failed: {}", e);
                    }
                }

                ApplyMsg::ApplySnapshot((tx, snapshot)) => {
                    if let Err(e) = tx.send(self.apply_snapshot0(snapshot)) {
                        warn!("send apply snapshot result failed: {}", e);
                    }
                }

                ApplyMsg::RoleChange((role, epoch, tx)) => {
                    let result = async {
                        if role == StateRole::Leader {
                            self.catch_up_committed_metadata().await?;
                            // The probe can wait on unresponsive peers for
                            // seconds; it must not delay the role publish that
                            // unblocks reads. It runs in the background and
                            // flips the flag when it completes (default false,
                            // the safe side).
                            if self.group_size > 1 {
                                let negotiator = self.clone();
                                tokio::spawn(async move {
                                    negotiator.negotiate_record_format().await;
                                });
                            } else {
                                self.negotiate_record_format().await;
                            }
                            if !self.publish_leader_if_current(epoch) {
                                return Err("leader promotion superseded by a newer metadata fence".to_string().into());
                            }
                            is_leader = true;
                            info!("metadata caught up for leader promotion; UFS replay continues in the background");
                        } else {
                            curvine_model::persist_children_count(false);
                            if !self.journal_writer.is_metadata_role_epoch(epoch) {
                                return Err("role change superseded by a newer metadata fence".to_string().into());
                            }
                            is_leader = false;
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

                msg => {
                    if self.recovery.is_safe_mode() {
                        Self::complete_entry_ack(msg, err_box!("master is in safe mode"));
                        continue;
                    }
                    match self.apply_msg(is_leader, &msg, false).await {
                        Ok(()) => Self::complete_entry_ack(msg, Ok(())),
                        Err(error) => {
                            let message = error.to_string();
                            match classify_apply_error(&message) {
                                ApplyDisposition::EnterSafeMode => {
                                    self.recovery.enter_safe_mode(&message);
                                    Self::complete_entry_ack(msg, Err(error.into()));
                                }
                                ApplyDisposition::Retry => {
                                    if !logged_skip_ignored
                                        && self.skip_failed_ufs_replay_after_retry
                                    {
                                        error!(
                                            "skip_failed_ufs_replay_after_retry is ignored; transient apply errors keep retrying (max_retry_num={})",
                                            self.max_retry_num
                                        );
                                        logged_skip_ignored = true;
                                    }
                                    warn!("apply entry failed, retrying without blocking the apply channel: {message}");
                                    retry_deadline =
                                        Some(tokio::time::Instant::now() + self.retry_interval);
                                    retry_msg = Some(msg);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn record_ufs_progress(&self, drive: &UfsDrive) {
        match drive {
            UfsDrive::CaughtUp => {
                if self.deferred_ufs_count().unwrap_or(1) == 0 {
                    self.recovery.set_pending(0);
                    self.recovery.set_ufs_caught_up(true);
                } else {
                    self.recovery
                        .set_pending(self.deferred_ufs_count().unwrap_or(1));
                    self.recovery.set_ufs_caught_up(false);
                }
            }
            UfsDrive::Advanced | UfsDrive::WaitWorkers | UfsDrive::Retry(_) => {
                self.recovery.set_ufs_caught_up(false);
            }
            UfsDrive::Stopped => {}
        }
    }

    fn live_worker_count(&self) -> usize {
        self.worker_manager.read().live_worker_count()
    }

    async fn run_ufs_replay(self) {
        let mut failures: u64 = 0;
        let mut logged_skip_ignored = false;
        loop {
            if self.ufs_shutdown.load(Ordering::Acquire) {
                break;
            }
            if self.recovery.is_safe_mode() {
                tokio::select! {
                    _ = self.ufs_wake.notified() => {}
                    _ = tokio::time::sleep(self.retry_interval) => {}
                }
                continue;
            }
            if !self.is_leader.load(Ordering::Acquire) {
                self.recovery.set_ufs_caught_up(false);
                self.ufs_wake.notified().await;
                continue;
            }
            let generation = self.recovery.generation();
            let drive = self.drive_ufs_once(generation).await;
            self.record_ufs_progress(&drive);
            match drive {
                UfsDrive::CaughtUp => {
                    failures = 0;
                    tokio::select! {
                        _ = self.ufs_wake.notified() => {}
                        _ = self.worker_notify.notified() => {}
                        _ = tokio::time::sleep(self.retry_interval) => {}
                    }
                }
                UfsDrive::Advanced => {
                    failures = 0;
                }
                UfsDrive::WaitWorkers => {
                    self.recovery.note_error("No workers available");
                    tokio::select! {
                        _ = self.worker_notify.notified() => {}
                        _ = self.ufs_wake.notified() => {}
                        _ = tokio::time::sleep(self.retry_interval) => {}
                    }
                }
                UfsDrive::Retry(message) => {
                    failures = failures.saturating_add(1);
                    self.recovery.note_error(&message);
                    if should_defer_ufs(&message, failures, self.max_retry_num) {
                        match self.defer_current_ufs(generation, &message).await {
                            Ok(true) => {
                                failures = 0;
                                self.recovery.set_ufs_caught_up(false);
                                warn!(
                                    "UFS op recorded in metadata and removed from the replay cursor: {message}"
                                );
                                continue;
                            }
                            Ok(false) => {}
                            Err(e) => self.recovery.note_error(&e.to_string()),
                        }
                    }
                    if !logged_skip_ignored
                        && self.skip_failed_ufs_replay_after_retry
                        && failures >= self.max_retry_num
                    {
                        error!(
                            "UFS replay still failing after {} attempts; continuing to retry without abort or skip: {}",
                            self.max_retry_num, message
                        );
                        logged_skip_ignored = true;
                    } else {
                        warn!("UFS replay retry: {message}");
                    }
                    tokio::select! {
                        _ = self.ufs_wake.notified() => {}
                        _ = self.worker_notify.notified() => {}
                        _ = tokio::time::sleep(self.retry_interval) => {}
                    }
                }
                UfsDrive::Stopped => {}
            }
        }
    }

    async fn drive_ufs_once(&self, generation: u64) -> UfsDrive {
        if generation != self.recovery.generation() || !self.is_leader.load(Ordering::Acquire) {
            return UfsDrive::Stopped;
        }
        let state = match self.fsm_state_snapshot() {
            Ok(state) => state,
            Err(e) => return UfsDrive::Retry(e.to_string()),
        };
        let target = state.applied.index;
        let cursor = state.ufs_applied.index;
        if cursor >= target {
            return self.drive_deferred(generation).await;
        }
        let list = match self.log_store.scan_entries(cursor + 1, cursor + 2) {
            Ok(list) => list,
            Err(e) => {
                let message = e.to_string();
                if classify_apply_error(&message) == ApplyDisposition::EnterSafeMode {
                    self.recovery.enter_safe_mode(&message);
                    return UfsDrive::Stopped;
                }
                return UfsDrive::Retry(message);
            }
        };
        let Some(entry) = list.into_iter().next() else {
            // A snapshot can advance past this cursor, deleting the entries the
            // replay still needs. Waiting forever blocks every later checkpoint
            // and grows the journal without bound, so the cluster never recovers
            // on its own. Skip the range that is already gone and record the new
            // watermark so followers apply the same position.
            let first = match raft::Storage::first_index(&self.log_store) {
                Ok(first) => first,
                Err(e) => return UfsDrive::Retry(e.to_string()),
            };
            if cursor + 1 < first {
                error!(
                    "UFS replay skips entries {}..{} already discarded by a snapshot",
                    cursor + 1,
                    first
                );
                let mut applied = state.ufs_applied.clone();
                applied.index = first - 1;
                return self.finish_ufs_entry(&Entry::default(), generation, applied, true);
            }
            return UfsDrive::Retry(format!("ufs replay waiting for log entry {}", cursor + 1));
        };
        if entry.index > target {
            return UfsDrive::CaughtUp;
        }
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            return self.finish_ufs_entry(
                &entry,
                generation,
                AppliedIndex {
                    term: entry.term,
                    index: entry.index,
                    op_id: state.ufs_applied.op_id,
                    rpc_id: state.ufs_applied.rpc_id,
                },
                false,
            );
        }
        let batch = match JournalBatch::decode(&entry.data) {
            Ok(batch) => batch,
            Err(e) if self.ignore_reply_error => {
                warn!(
                    "skip failed journal batch during UFS replay, entry index={}, error={}",
                    entry.index, e
                );
                return self.finish_ufs_entry(
                    &entry,
                    generation,
                    AppliedIndex {
                        term: entry.term,
                        index: entry.index,
                        op_id: state.ufs_applied.op_id,
                        rpc_id: state.ufs_applied.rpc_id,
                    },
                    false,
                );
            }
            Err(e) => {
                self.recovery.enter_safe_mode(&e.to_string());
                return UfsDrive::Stopped;
            }
        };

        let mut applied = AppliedIndex {
            term: entry.term,
            index: entry.index,
            op_id: state.ufs_applied.op_id,
            rpc_id: state.ufs_applied.rpc_id,
        };
        let mut ufs_ops = Vec::new();
        let mut needs_worker = false;
        for op in batch.batch {
            applied.op_id = applied.op_id.max(op.op_id());
            applied.rpc_id = op.rpc_id();
            if self.ufs_loader.needs_worker(&op) {
                needs_worker = true;
            }
            if op.affects_ufs() {
                ufs_ops.push(op);
            }
        }
        if needs_worker && self.live_worker_count() == 0 {
            self.recovery.set_pending(target.saturating_sub(cursor));
            return UfsDrive::WaitWorkers;
        }
        // Transfer mode merges the entry's consecutive CompleteFile exports into
        // one submit, flushed before the next namespace op so copies are
        // enqueued in journal order.
        let mut exports = Vec::new();
        for op in &ufs_ops {
            if generation != self.recovery.generation() || !self.is_leader.load(Ordering::Acquire) {
                return UfsDrive::Stopped;
            }
            let result = match op {
                JournalEntry::CompleteFile(e) if self.ufs_loader.transfer_enabled() => {
                    match self.ufs_loader.fs_mode_export(e) {
                        Ok(export) => {
                            exports.extend(export);
                            Ok(())
                        }
                        Err(err) => Err(err.into()),
                    }
                }
                _ => {
                    self.ufs_loader
                        .submit_fs_mode_exports(mem::take(&mut exports))
                        .await;
                    self.ufs_loader.apply_entry(op).await
                }
            };
            if let Err(e) = result {
                let message = e.to_string();
                if classify_apply_error(&message) == ApplyDisposition::EnterSafeMode {
                    self.recovery.enter_safe_mode(&message);
                    return UfsDrive::Stopped;
                }
                self.recovery.set_pending(target.saturating_sub(cursor));
                return UfsDrive::Retry(message);
            }
        }
        self.ufs_loader.submit_fs_mode_exports(exports).await;
        self.finish_ufs_entry(&entry, generation, applied, !ufs_ops.is_empty())
    }

    fn finish_ufs_entry(
        &self,
        _entry: &Entry,
        generation: u64,
        applied: AppliedIndex,
        log_marker: bool,
    ) -> UfsDrive {
        if generation != self.recovery.generation() || !self.is_leader.load(Ordering::Acquire) {
            return UfsDrive::Stopped;
        }
        match self.commit_ufs_progress(applied, log_marker) {
            Ok(()) => UfsDrive::Advanced,
            Err(e) => UfsDrive::Retry(e.to_string()),
        }
    }

    fn encode_deferred_ops(ops: Vec<JournalEntry>) -> CommonResult<Vec<u8>> {
        let mut batch = JournalBatch::new(0);
        for op in ops {
            batch.push(op);
        }
        batch.encode()
    }

    fn decode_deferred_ops(payload: &[u8]) -> CommonResult<Vec<JournalEntry>> {
        Ok(JournalBatch::decode(payload)?.batch)
    }

    fn deferred_ufs_count(&self) -> CommonResult<u64> {
        Ok(self.deferred_ufs_records()?.len() as u64)
    }

    fn deferred_ufs_records(&self) -> CommonResult<Vec<(u64, Vec<u8>)>> {
        let view = self
            .fs_dir
            .view()
            .map_err(|e| RaftError::from(e.to_string()))?;
        view.store.ensure_available()?;
        view.store.store().ufs_deferred_records()
    }

    fn put_deferred_ufs(&self, index: u64, payload: &[u8]) -> CommonResult<()> {
        let view = self
            .fs_dir
            .view()
            .map_err(|e| RaftError::from(e.to_string()))?;
        view.store.ensure_available()?;
        view.store.store().put_ufs_deferred(index, payload)
    }

    fn delete_deferred_ufs(&self, index: u64) -> CommonResult<()> {
        let view = self
            .fs_dir
            .view()
            .map_err(|e| RaftError::from(e.to_string()))?;
        view.store.ensure_available()?;
        view.store.store().delete_ufs_deferred(index)
    }

    /// Copy the current log entry's UFS ops into the metadata store and move
    /// the replay cursor past them. Catch-up stays false while the record exists.
    async fn defer_current_ufs(&self, generation: u64, error: &str) -> CommonResult<bool> {
        if generation != self.recovery.generation() || !self.is_leader.load(Ordering::Acquire) {
            return Ok(false);
        }
        let state = self.fsm_state_snapshot()?;
        if state.ufs_applied.index >= state.applied.index {
            return Ok(false);
        }
        let list = self
            .log_store
            .scan_entries(state.ufs_applied.index + 1, state.ufs_applied.index + 2)?;
        let Some(entry) = list.into_iter().next() else {
            return Ok(false);
        };
        let batch = JournalBatch::decode(&entry.data)?;
        let ufs_ops: Vec<JournalEntry> = batch
            .batch
            .into_iter()
            .filter(|op| op.affects_ufs())
            .collect();
        if ufs_ops.is_empty() {
            self.commit_ufs_progress(
                AppliedIndex {
                    term: entry.term,
                    index: entry.index,
                    ..Default::default()
                },
                false,
            )?;
            return Ok(true);
        }
        let op_id = ufs_ops.last().expect("ufs ops is non-empty").op_id();
        let rpc_id = ufs_ops.last().expect("ufs ops is non-empty").rpc_id();
        let payload = Self::encode_deferred_ops(ufs_ops)?;
        self.put_deferred_ufs(entry.index, &payload)?;
        self.journal_writer.log_ufs_deferred(UfsDeferredEntry {
            op_id,
            rpc_id,
            term: entry.term,
            index: entry.index,
            error: error.to_string(),
            payload,
        })?;
        self.commit_ufs_progress(
            AppliedIndex {
                op_id,
                rpc_id,
                term: entry.term,
                index: entry.index,
            },
            false,
        )?;
        self.recovery
            .set_pending(self.deferred_ufs_count().unwrap_or(1));
        self.recovery.set_ufs_caught_up(false);
        Ok(true)
    }

    async fn drive_deferred(&self, generation: u64) -> UfsDrive {
        if generation != self.recovery.generation() || !self.is_leader.load(Ordering::Acquire) {
            return UfsDrive::Stopped;
        }
        let records = match self.deferred_ufs_records() {
            Ok(records) => records,
            Err(e) => return UfsDrive::Retry(e.to_string()),
        };
        let Some((index, payload)) = records.into_iter().next() else {
            return UfsDrive::CaughtUp;
        };
        let ops: Vec<JournalEntry> = match Self::decode_deferred_ops(&payload) {
            Ok(ops) => ops,
            Err(e) => {
                self.recovery.enter_safe_mode(&e.to_string());
                return UfsDrive::Stopped;
            }
        };
        if ops.iter().any(|op| self.ufs_loader.needs_worker(op)) && self.live_worker_count() == 0 {
            self.recovery.set_pending(1);
            return UfsDrive::WaitWorkers;
        }
        for op in &ops {
            if generation != self.recovery.generation() || !self.is_leader.load(Ordering::Acquire) {
                return UfsDrive::Stopped;
            }
            if let Err(e) = self.ufs_loader.apply_entry(op).await {
                let message = e.to_string();
                if classify_apply_error(&message) == ApplyDisposition::EnterSafeMode {
                    self.recovery.enter_safe_mode(&message);
                    return UfsDrive::Stopped;
                }
                return UfsDrive::Retry(message);
            }
        }
        if let Err(e) = self.delete_deferred_ufs(index) {
            return UfsDrive::Retry(e.to_string());
        }
        UfsDrive::Advanced
    }

    fn create_snapshot0(&self, dir_option: Option<String>) -> RaftResult<SnapshotData> {
        if self.recovery.is_safe_mode() {
            return Err(RaftError::raft(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            )));
        }
        self.check_meta_data_dir("create_snapshot")?;
        let fsm_state = self.fsm_state_snapshot()?;
        let dir = match dir_option {
            Some(dir) => dir,
            None => {
                let fs_dir = self
                    .fs_dir
                    .view()
                    .map_err(|e| RaftError::from(e.to_string()))?;
                fs_dir
                    .create_checkpoint(fsm_state.applied.index)
                    .map_err(|e| {
                        // create_checkpoint returns a boxed CommonError; convert
                        // through the string form so ctx() is available and the
                        // failure stays a typed RaftError::Storage candidate.
                        let msg = format!(
                            "failed to create metadata checkpoint (id {}) under meta dir {}: {}",
                            fsm_state.applied.index,
                            fs_dir.store.store().db.conf().data_dir,
                            e
                        );
                        if Self::meta_dir_missing(e.as_ref()) {
                            RaftError::storage(msg)
                        } else {
                            RaftError::from(msg)
                        }
                    })?
            }
        };

        let data = RaftUtils::create_file_snapshot(&dir, self.node_id, fsm_state.clone()).map_err(
            |e| {
                e.ctx(format!(
                    "failed to build file snapshot from checkpoint dir {}",
                    dir
                ))
            },
        )?;
        if let Err(e) = self.purge_checkpoint(&dir) {
            warn!("purge checkpoint: {}", e);
        }

        Ok(data)
    }

    /// Fail fast when the master meta data directory is missing or no longer a
    /// valid RocksDB store (e.g. it was deleted while the process was running).
    /// Without this check the failure surfaces later as a bare
    /// "No such file or directory" deep inside checkpoint/restore, which gives
    /// no clue about which directory or what remediation is required.
    fn check_meta_data_dir(&self, op: &str) -> RaftResult<()> {
        // Restore temporarily opens a different database, then reopens this
        // same configured directory; its path is stable for this loader.
        self.scan_control.wait_until_restored();
        let data_dir = &self.meta_data_dir;
        let path = Path::new(data_dir);
        let valid = path.is_dir() && path.join("CURRENT").is_file();
        if !valid {
            return Err(RaftError::storage(format!(
                "master meta data_dir is missing or is not a valid RocksDB store: {} (operation: {}). \
                 The local metadata was deleted, moved, or only partially restored. \
                 Restore a consistent meta+journal pair from a healthy voter (member recovery, journal.recover_from_peers) before continuing; \
                 restarting this node without restoring cannot recover the data.",
                data_dir, op
            )));
        }
        Ok(())
    }

    /// Heuristic: the checkpoint/restore layer reports a vanished underlying
    /// directory as a bare boxed io error with no path context. Match it so we
    /// can upgrade it to the typed storage-fatal error.
    fn meta_dir_missing(e: &dyn std::error::Error) -> bool {
        let msg = e.to_string();
        msg.contains("No such file or directory") || msg.contains("os error 2")
    }

    fn apply_snapshot0(&self, snapshot: SnapshotData) -> RaftResult<()> {
        let mut spend = TimeSpent::new();

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
        if !Path::new(&restore_path).join("CURRENT").is_file() {
            return err_box!(
                "refusing snapshot {}: checkpoint {} is not a RocksDB directory",
                snapshot.snapshot_id,
                restore_path
            );
        }
        let actual_size = FileUtils::dir_size(&restore_path).map_err(|error| {
            RaftError::storage(format!(
                "failed to inspect checkpoint {}: {}",
                restore_path, error
            ))
        })?;
        if actual_size == 0 {
            return err_box!(
                "refusing to apply empty snapshot {} at {}",
                snapshot.snapshot_id,
                restore_path
            );
        }
        let checkpoint_size = if log::log_enabled!(log::Level::Info) {
            actual_size
        } else {
            0
        };

        let mut fs_dir = self
            .fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .map_err(|e| RaftError::from(e.to_string()))?;
        if let Err(e) = fs_dir.restore(&restore_path, checkpoint_size) {
            // FsDir::restore owns the failure semantics: a pre-replacement
            // failure leaves the old store live (normal backfill via replay),
            // but a post-replacement failure must fence the view too —
            // fail_metadata_restore only fences the journal writer, so an
            // unavailable cursor store marks the handle FAILED here.
            if !fs_dir.store.cursor_store_available() {
                fs_dir.fail_closed();
            }
            let msg = format!(
                "failed to restore metadata from checkpoint {} (checkpoint_size {} bytes); \
                 local meta data_dir is {}: {}",
                restore_path, checkpoint_size, self.meta_data_dir, e
            );
            return Err(if Self::meta_dir_missing(e.as_ref()) {
                RaftError::storage(msg)
            } else {
                RaftError::from(msg)
            });
        }
        // The checkpoint can predate its snapshot journal entry. Persist the
        // snapshot's actual applied index before reporting a successful restore.
        // This does not make the replacement itself crash-atomic.
        if let Err(e) = self.persist_applied(&snapshot.fsm_state.applied) {
            // The restored store's cursor row is invalid: the namespace cannot
            // safely reopen. FsDir::restore already recorded the lifecycle
            // failure for post-replacement paths; this cursor failure is
            // post-replacement too.
            fs_dir.store.invalidate_cursor_store();
            fs_dir.fail_closed();
            return Err(RaftError::storage(e.to_string()));
        }
        if let Err(error) = fs_dir.store.store().db.flush(true) {
            fs_dir.store.invalidate_cursor_store();
            fs_dir.fail_closed();
            let message = format!("failed to flush restored journal cursor: {}", error);
            self.recovery.enter_safe_mode(&message);
            return Err(RaftError::storage(message));
        }
        fs_dir.update_op_id(snapshot.fsm_state.op_id());
        drop(fs_dir);
        let restore_ms = spend.used_ms();
        spend.reset();

        self.mnt_mgr.restore_best_effort();
        let mount_ms = spend.used_ms();

        *self.fsm_state()? = snapshot.fsm_state;
        self.refresh_metrics()?;

        info!(
            "apply_snapshot: fs_dir_restore={} ms, mount_restore={} ms, total={} ms",
            restore_ms,
            mount_ms,
            restore_ms + mount_ms
        );

        Ok(())
    }

    /// Plans the stripe lock requests for a path's ancestor chain and target.
    /// Uses the shared InodeLockManager to serialize with RPC-side writers.
    fn plan_replay_locks(
        &self,
        view: &crate::master::meta::FsDir,
        path: &str,
    ) -> FsResult<smallvec::SmallVec<[InodeLockRequest; 8]>> {
        Self::plan_replay_locks_mode(view, path, false)
    }

    /// Lock plan for a replay path. Mutations that replace an existing child
    /// edge (delete/rename/link/force symlink) also write the parent edge, so
    /// both the target and its parent are locked Write; ancestors stay Read.
    fn plan_replay_locks_mode(
        view: &crate::master::meta::FsDir,
        path: &str,
        parent_write: bool,
    ) -> FsResult<smallvec::SmallVec<[InodeLockRequest; 8]>> {
        let inp = InodePath::resolve(view.root_node(), path, &view.store)?;
        let full = inp.is_full();
        let ids: Vec<i64> = inp.inodes.iter().map(|e| e.id()).collect();
        let last = ids.len().saturating_sub(1);
        // The modified parent depends on whether the target itself exists:
        // an existing target replaces its edge in the parent (parent is the
        // second-to-last entry); a missing target only creates a new edge in
        // the existing parent (parent is the last entry). Root (depth 0)
        // always takes Write — it has no ancestor lock to serialize with.
        let parent = if full { last.saturating_sub(1) } else { last };
        Ok(ids
            .iter()
            .enumerate()
            .map(|(depth, id)| {
                let write = depth == last || (parent_write && depth == parent);
                if write {
                    InodeLockRequest::write(depth, *id)
                } else {
                    InodeLockRequest::read(depth, *id)
                }
            })
            .collect())
    }

    /// Plans locks, acquires them, then re-resolves the path under the locks
    /// and validates that the locked ID chain matches the re-resolved chain.
    /// Returns the resolved InodePath for unprotected mutation.
    /// Locks a replay path with full guard coverage: plan from one view, drop
    /// it, acquire the stripe set (guards live for the caller's scope), then
    /// re-open the view and revalidate every locked identity. Returns the
    /// view guard and the revalidated path; the caller keeps both alive until
    /// the mutation and its store publication complete.
    /// Plans the stripe set from one view, acquires the locks, re-opens the
    /// view, revalidates every locked identity, then runs `op` with the locks
    /// and the view guard held across the whole mutation and its store
    /// publication. The guards never outlive this helper; the operation never
    /// runs without them.
    fn with_locked_replay_view<T>(
        &self,
        path: &str,
        parent_write: bool,
        op: impl FnOnce(&crate::master::meta::NamespaceGuard<'_>, InodePath) -> CommonResult<T>,
    ) -> CommonResult<T> {
        // Capture the scan epoch while planning: a restore between planning
        // and locking invalidates the same-ID chain (IDs can be reused), so
        // the epoch is re-checked under the locks alongside the identities.
        let plan_epoch = {
            let view = self.fs_dir.view().map_err(|e| e.to_string())?;
            let control = view.store.scan_control();
            if control.is_restoring() {
                return err_box!("replay planning rejected: snapshot restore pending");
            }
            let epoch = control.cancel_epoch();
            let requests = Self::plan_replay_locks_mode(&view, path, parent_write)?;
            (epoch, requests)
        };
        let (plan_epoch, requests) = plan_epoch;
        let locks = self.inode_locks.lock_many(&requests);
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        let control = view.store.scan_control();
        if control.is_restoring() {
            return err_box!("replay lock validation failed: snapshot restore pending");
        }
        let live_epoch = control.cancel_epoch();
        if live_epoch != plan_epoch {
            return err_box!(
                "replay lock validation failed for {}: epoch moved ({} -> {})",
                path,
                plan_epoch,
                live_epoch
            );
        }
        let inp = InodePath::resolve(view.root_node(), path, &view.store)?;
        let ids: Vec<i64> = inp.inodes.iter().map(|e| e.id()).collect();
        let locked: Vec<i64> = requests.iter().map(|r| r.inode_id).collect();
        if ids != locked {
            return err_box!(
                "replay lock validation failed for {}: locked={:?} resolved={:?}",
                path,
                locked,
                ids
            );
        }
        let result = op(&view, inp);
        drop(locks);
        result
    }

    fn plan_replay_locks_by_inode(
        &self,
        view: &crate::master::meta::FsDir,
        inode_id: i64,
    ) -> FsResult<smallvec::SmallVec<[InodeLockRequest; 8]>> {
        Ok(smallvec::smallvec![InodeLockRequest::write(0, inode_id)])
    }

    /// Plans stripe locks for a set of inode IDs (for CacheInvalidation entries).
    fn plan_replay_locks_by_inodes(
        &self,
        view: &crate::master::meta::FsDir,
        inodes: &[InodeView],
    ) -> FsResult<smallvec::SmallVec<[InodeLockRequest; 8]>> {
        Ok(inodes
            .iter()
            .map(|inode| InodeLockRequest::write(0, inode.id()))
            .collect())
    }

    /// Replay mutation addressed by an inode ID. When the ID is valid the
    /// actual object's Write lock is planned and re-validated under the locks
    /// (epoch + ID presence); only an invalid/absent ID falls back to the
    /// path-chain protocol. The path stays as request context — it is never
    /// used to pick the write target when an ID is available.
    /// Runs one replay cold-bridge body against the exclusive owner with the
    /// agreed failure boundary: a body error is failed-closed only when the
    /// journal recorded a publication failure during it (failure_seq moved);
    /// a pure validation/store error leaves the handle OPEN for normal
    /// backfill via replay.
    /// C2 recursive replay coordination (design §8.2): the same planning
    /// and lock pool as the RPC entry, but no RPC admission, no commit
    /// scope, no journal writes and no self-ack. The confirmation-style
    /// guard is declared after the inode/view bindings; an in-lock
    /// Err+failure_seq move (or a body unwind) terminalizes the holder
    /// before the locks drop. Budget overflow keeps the original cold
    /// bridge; the apply worker never drains the ordinary gate.
    fn with_replay_locks<T>(
        &self,
        path: &str,
        parent_mode: crate::master::meta::InodeLockMode,
        need_locations: bool,
        body: impl FnOnce(&FsDir) -> FsResult<T>,
    ) -> CommonResult<T> {
        // R1/R2: the body must be available BOTH inside the locked window
        // (with the guard) and in the caller's cold fallback after churn.
        // `Option::take` hands it over exactly once per branch.
        self.with_replay_locks_impl(path, parent_mode, need_locations, &mut Some(body))
    }

    fn with_replay_locks_impl<T>(
        &self,
        path: &str,
        parent_mode: crate::master::meta::InodeLockMode,
        need_locations: bool,
        body_slot: &mut Option<impl FnOnce(&FsDir) -> FsResult<T>>,
    ) -> CommonResult<T> {
        // R5: side-effect-free churn (epoch/identity/token mismatch or
        // location coverage shortfall) retries the whole capture/sample/
        // lock/revalidate sequence up to 3 times; only then does the cold
        // bridge own it. A real body error is never retried.
        const MAX_RETRIES: usize = 3;
        for attempt in 0..MAX_RETRIES {
            let _ = attempt;
            use crate::master::fs::master_filesystem::MetadataConfirmation;
            use crate::master::meta::subtree_write_plan::SubtreeWritePlan;

            // R1: the fallback decision is made while the planning view is
            // alive, but `with_replay_cold` must only run AFTER this thread has
            // released its own view (begin_cold waits for every view strong
            // reference — calling it inside the view scope would self-wait until
            // the DrainTimeout).
            enum PlanOutcome<T> {
                Planned(T),
                ColdFallback,
            }
            let plan = {
                let fs_dir = self.fs_dir.view().map_err(|e| e.to_string())?;
                match SubtreeWritePlan::capture(&fs_dir, path, parent_mode) {
                    Ok(Some(plan)) => PlanOutcome::Planned(plan),
                    // Budget overflow / unresolvable: the cold bridge keeps the
                    // original behavior (§7) — entered after the view drops.
                    Ok(None) => PlanOutcome::ColdFallback,
                    Err(e) => return Err(e.to_string().into()),
                }
            };
            let plan = match plan {
                PlanOutcome::Planned(plan) => plan,
                PlanOutcome::ColdFallback => {
                    let Some(body) = body_slot.take() else {
                        return Err("replay body already consumed".to_string().into());
                    };
                    return self.with_replay_cold(body);
                }
            };
            let inode_locks = self.inode_locks.lock_many(plan.inode_requests());

            // Location locks for a recursive free replay (the same protection
            // as the RPC path, minus admission): sample with a short view,
            // drop the view, then take worker → block stripes. R1: the
            // over-budget decision is recorded inside the view scope; the cold
            // bridge runs only after both the view and the inode locks are
            // released (begin_cold self-waits otherwise).
            let location_locks = if need_locations {
                let mut block_ids: Vec<i64> = Vec::new();
                let mut worker_ids: Vec<u32> = Vec::new();
                let mut over_budget = false;
                {
                    let fs_dir = self.fs_dir.view().map_err(|e| e.to_string())?;
                    for request in plan.inode_requests() {
                        if !matches!(request.mode, crate::master::meta::InodeLockMode::Write) {
                            continue;
                        }
                        let Some(inode) = fs_dir
                            .store
                            .get_inode(request.inode_id, None)
                            .map_err(|e| e.to_string())?
                        else {
                            continue;
                        };
                        let Ok(file) = inode.as_file_ref() else {
                            continue;
                        };
                        // R5: independent location budget — every block entry
                        // and worker-location entry is charged against
                        // SUBTREE_LOCATION_LIMIT (design §6); the plan budget
                        // does not cover locations.
                        let mut location_entries = 0usize;
                        for block_id in file.block_ids() {
                            location_entries += 1;
                            block_ids.push(block_id);
                            if location_entries
                                > crate::master::meta::subtree_write_plan::SUBTREE_LOCATION_LIMIT
                            {
                                over_budget = true;
                                break;
                            }
                            for location in fs_dir
                                .get_block_locations(block_id)
                                .map_err(|e| e.to_string())?
                            {
                                location_entries += 1;
                                worker_ids.push(location.worker_id);
                                if location_entries
                                > crate::master::meta::subtree_write_plan::SUBTREE_LOCATION_LIMIT
                            {
                                over_budget = true;
                                break;
                            }
                            }
                            if over_budget {
                                break;
                            }
                        }
                        if over_budget {
                            break;
                        }
                    }
                }
                if over_budget {
                    drop(inode_locks);
                    let Some(body) = body_slot.take() else {
                        return Err("replay body already consumed".to_string().into());
                    };
                    return self.with_replay_cold(body);
                }
                Some(
                    self.block_location_locks
                        .write_workers_blocks(&worker_ids, &block_ids),
                )
            } else {
                None
            };

            // R2: the final revalidation and the body share ONE view window.
            // A cold/restore lifecycle could otherwise take the unique owner
            // between two separate views, replacing root/store while the plan's
            // locks are still held. Churn (side-effect-free mismatch) is
            // reported as a flag; the caller releases the locks and falls back
            // to the cold bridge. The guard is declared after the view binding
            // (§8.2) and lives only inside this view window.
            enum ReplayStep<T> {
                Body(T),
                Churn,
            }
            let result = (|| -> CommonResult<ReplayStep<T>> {
                let fs_dir = self.fs_dir.view().map_err(|e| e.to_string())?;
                if plan
                    .revalidate(&fs_dir, path)
                    .map_err(|e| e.to_string())?
                    .is_none()
                {
                    // Side-effect-free churn: the caller releases view/locks
                    // and takes the cold bridge. The body was never moved here
                    // — it stays available for the caller's cold fallback.
                    return Ok(ReplayStep::Churn);
                }
                let guard = MetadataConfirmation::arm(&self.fs_dir, &self.journal_writer);
                let mut guard = guard;
                let failure_seq_before = self.journal_writer.failure_seq();
                let Some(body) = body_slot.take() else {
                    return Err("replay body already consumed".to_string().into());
                };
                let result = body(&fs_dir);
                let seq_moved = self.journal_writer.failure_seq() != failure_seq_before;
                if result.is_err() && seq_moved {
                    // A real in-lock fatal: terminalize while the locks are
                    // still held (handle.take() empties the guard).
                    guard.fail_terminal();
                } else {
                    // Ok, or a plain business error that did not move the seq:
                    // the holder stays usable — the replay error goes to the
                    // original apply error/retry rules without retiring the
                    // namespace (design §8.2).
                    guard.disarm();
                }
                drop(fs_dir);
                drop(guard);
                let step = result.map(ReplayStep::Body).map_err(|e| e.to_string())?;
                Ok(step)
            })();

            drop(location_locks);
            drop(inode_locks);
            match result? {
                ReplayStep::Body(value) => return Ok(value),
                ReplayStep::Churn => continue,
            }
        }
        // Retries exhausted on side-effect-free churn: the cold bridge owns
        // the replay (design §8.2). The body was never consumed by a churn
        // pass.
        let Some(body) = body_slot.take() else {
            return Err("replay body already consumed".to_string().into());
        };
        self.with_replay_cold(body)
    }

    fn with_replay_cold<T>(&self, body: impl FnOnce(&FsDir) -> FsResult<T>) -> CommonResult<T> {
        let mut cold = self
            .fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .map_err(|e| e.to_string())?;
        let failure_seq_before = cold.journal_writer.failure_seq();
        let result = body(&cold);
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if cold.journal_writer.failure_seq() != failure_seq_before {
                    // A publication failure was recorded during the body: the
                    // holder must be fenced, not just the writer.
                    cold.fail_closed();
                }
                Err(error.into())
            }
        }
    }

    fn with_locked_replay_inode<T>(
        &self,
        path: &str,
        inode_id: Option<i64>,
        op: impl FnOnce(&crate::master::meta::NamespaceGuard<'_>) -> CommonResult<T>,
    ) -> CommonResult<T> {
        match inode_id.filter(|id| *id > 0) {
            // No usable ID: the path-chain protocol owns this request, with
            // its own view/lock/epoch/chain validation.
            None => self.with_locked_replay_view(path, false, |view, _| op(view)),
            Some(id) => {
                // Short planning view: capture the epoch only, release before
                // taking the object lock.
                let plan_epoch = {
                    let view = self.fs_dir.view().map_err(|e| e.to_string())?;
                    let control = view.store.scan_control();
                    if control.is_restoring() {
                        return err_box!("replay planning rejected: snapshot restore pending");
                    }
                    control.cancel_epoch()
                };
                let locks = self.inode_locks.write_inode(id);
                let view = self.fs_dir.view().map_err(|e| e.to_string())?;
                let control = view.store.scan_control();
                if control.is_restoring() {
                    return err_box!("replay lock validation failed: snapshot restore pending");
                }
                if control.cancel_epoch() != plan_epoch {
                    return err_box!(
                        "replay lock validation failed for {} (inode {}): epoch moved ({} -> {})",
                        path,
                        id,
                        plan_epoch,
                        control.cancel_epoch()
                    );
                }
                // No extra existence check here: op resolves the ID itself and
                // its FileNotFound handling keeps the replay skip contract.
                let result = op(&view);
                drop(locks);
                drop(view);
                result
            }
        }
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

            JournalEntry::UfsApplied(e) => self.ufs_applied(e),

            JournalEntry::UfsDeferred(e) => self.apply_ufs_deferred(e),
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

        let res = self.with_replay_cold(|fs_dir| fs_dir.purge_paths(&entry.paths, entry.mtime))?;
        debug!(
            "TrashPurge: entry={} removed inodes={} bytes={}",
            entry.entry, res.inodes, res.bytes
        );
        Ok(())
    }

    fn cache_invalidation(&self, entry: CacheInvalidationEntry) -> CommonResult<()> {
        let requests = {
            let view = self.fs_dir.view().map_err(|e| e.to_string())?;
            self.plan_replay_locks_by_inodes(&view, &entry.inodes)?
        };
        let _locks = self.inode_locks.lock_many(&requests);
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        view.store.apply_cache_invalidations(entry.inodes)
    }

    fn mkdir(&self, entry: MkdirEntry) -> CommonResult<()> {
        let entry_path = entry.path.clone();
        self.with_locked_replay_view(&entry_path, false, |view, inp| {
            let name = inp.name().to_string();
            let _ = view.add_last_inode(inp, InodeView::new_dir(name, entry.dir))?;
            Ok(())
        })
    }

    fn create_file(&self, entry: CreateFileEntry) -> CommonResult<()> {
        let entry_path = entry.path.clone();
        self.with_locked_replay_view(&entry_path, false, |view, inp| {
            if inp.is_full() {
                warn!("create_file: file already exists: {:?}", entry);
                return Ok(());
            }
            let name = inp.name().to_string();
            let _ = view.add_last_inode(inp, InodeView::new_file(name, entry.file))?;
            Ok(())
        })
    }

    fn reopen_file(&self, entry: ReopenFileEntry) -> CommonResult<()> {
        let entry_path = entry.path.clone();
        self.with_locked_replay_view(&entry_path, false, |view, inp| {
            let mut inode = match inp.get_last_inode() {
                Some(v) => v,
                None => {
                    warn!("reopen_file: file not found: {:?}", entry);
                    return Ok(());
                }
            };
            let file = inode.as_file_mut()?;
            let _ = mem::replace(file, entry.file);

            view.store.apply_reopen_file(inode.as_ref())?;

            Ok(())
        })
    }

    fn overwrite_file(&self, entry: OverWriteFileEntry) -> CommonResult<()> {
        let entry_path = entry.path.clone();
        self.with_locked_replay_view(&entry_path, false, |view, inp| {
            let mut inode = match inp.get_last_inode() {
                Some(v) => v,
                None => {
                    warn!("overwrite_file: file not found: {:?}", entry);
                    return Ok(());
                }
            };
            let file = inode.as_file_mut()?;
            let _ = mem::replace(file, entry.file);

            view.store.apply_overwrite_file(inode.as_ref())?;

            Ok(())
        })
    }

    fn add_block(&self, entry: AddBlockEntry) -> CommonResult<()> {
        let inode_id = entry.blocks.first().map(|v| InodeId::get_id(v.id));
        let entry_path = entry.path.clone();
        self.with_locked_replay_inode(&entry_path, inode_id, |view| {
            let mut inode = match MasterFilesystem::resolve_file_inode(view, &entry.path, inode_id)
            {
                Ok(v) => v,
                Err(e) if matches!(e.kind(), curvine_error::ErrorKind::FileNotFound) => {
                    // A path the entry cannot resolve any more (e.g. it was
                    // deleted by a later entry replayed first) is an idempotent
                    // skip; store/lifecycle errors must surface.
                    warn!("add_block: file not found: {:?} {}", entry, e);
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };
            let file = inode.as_file_mut()?;
            let _ = mem::replace(&mut file.blocks, entry.blocks);
            view.store
                .apply_new_block(inode.as_ref(), &entry.commit_block)?;

            Ok(())
        })
    }

    fn complete_file(&self, entry: CompleteFileEntry) -> CommonResult<()> {
        let entry_path = entry.path.clone();
        self.with_locked_replay_inode(&entry_path, Some(entry.file.id), |view| {
            let mut inode = match MasterFilesystem::resolve_file_inode(
                view,
                &entry.path,
                Some(entry.file.id),
            ) {
                Ok(v) => v,
                Err(e) if matches!(e.kind(), curvine_error::ErrorKind::FileNotFound) => {
                    // Same idempotent-skip contract as add_block: only a real
                    // "not found" is replayable-absent.
                    warn!("complete_file: file not found: {:?} {}", entry, e);
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            };
            let file = inode.as_file_mut()?;

            let _ = mem::replace(file, entry.file);
            // Update block location
            view.store
                .apply_complete_file(inode.as_ref(), &entry.commit_blocks)?;

            Ok(())
        })
    }
    pub fn rename(&self, entry: RenameEntry) -> CommonResult<()> {
        // Both chains must stay in the locked set and still match their
        // planned identities (plus epoch) once the locks are held: a path
        // replaced between planning and locking would otherwise be mutated
        // under the old object's locks.
        let (plan_epoch, (src_requests, dst_requests)) = {
            let view = self.fs_dir.view().map_err(|e| e.to_string())?;
            let control = view.store.scan_control();
            if control.is_restoring() {
                return err_box!("rename replay rejected: snapshot restore pending");
            }
            let epoch = control.cancel_epoch();
            let src_reqs = Self::plan_replay_locks_mode(&view, &entry.src, true)?;
            let dst_reqs = Self::plan_replay_locks_mode(&view, &entry.dst, true)?;
            (epoch, (src_reqs, dst_reqs))
        };
        // Keep the two chains' boundaries: each is compared in order against
        // its own locked segment, so per-path identity survives (a merged,
        // deduped set could not tell which path owns which inode).
        let mut requests = src_requests.clone();
        requests.extend(dst_requests.iter().cloned());
        let _locks = self.inode_locks.lock_many(&requests);
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        let control = view.store.scan_control();
        if control.is_restoring() {
            return err_box!("rename replay rejected: snapshot restore pending");
        }
        if control.cancel_epoch() != plan_epoch {
            return err_box!(
                "rename replay validation failed: epoch moved ({} -> {})",
                plan_epoch,
                control.cancel_epoch()
            );
        }
        let entry_src = entry.src;
        let src_inp = InodePath::resolve(view.root_node(), &entry_src, &view.store)?;
        let dst_inp = InodePath::resolve(view.root_node(), entry.dst, &view.store)?;
        let src_ids: Vec<i64> = src_inp.inodes.iter().map(|e| e.id()).collect();
        let dst_ids: Vec<i64> = dst_inp.inodes.iter().map(|e| e.id()).collect();
        let (src_locked, dst_locked) = {
            let src: Vec<i64> = requests
                .iter()
                .take(src_requests.len())
                .map(|r| r.inode_id)
                .collect();
            let dst: Vec<i64> = requests
                .iter()
                .skip(src_requests.len())
                .map(|r| r.inode_id)
                .collect();
            (src, dst)
        };
        if src_locked != src_ids || dst_locked != dst_ids {
            return err_box!(
                "rename replay validation failed: locked src={:?} dst={:?} live src={:?} dst={:?}",
                src_locked,
                dst_locked,
                src_ids,
                dst_ids
            );
        }
        if src_inp.get_last_inode().is_none() {
            warn!("Rename: source path not found: {}", entry_src);
            return Ok(());
        }
        view.unprotected_rename(
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
        let entry_path = entry.path;
        // A directory delete reaches outside the target chain: the store's
        // DFS walks every subtree file and adjusts the nlink of its tree-wide
        // hardlinks, which the directory-chain locks do not exclude. Route
        // that case through the same cold bridge as recursive free/set_attr;
        // a plain file keeps the ordered sharded locks.
        let is_dir = {
            let view = self.fs_dir.view().map_err(|e| e.to_string())?;
            let inp = InodePath::resolve(view.root_node(), &entry_path, &view.store)?;
            inp.get_last_inode()
                .map(|inode| inode.is_dir())
                .unwrap_or(false)
        };
        if is_dir {
            // C2: a recursive directory delete replays under the bounded
            // subtree-lock path (parent Write; no location locks — the
            // Delete entry carries no block list).
            // R3: a recursive permanent delete reads stage_decrement_nlink
            // location rows inside the body — the full inode → worker →
            // block order protects it against concurrent report/replication
            // writes, regardless of what the journal entry format carries.
            return self.with_replay_locks(&entry_path, InodeLockMode::Write, true, |fs_dir| {
                let inp = InodePath::resolve(fs_dir.root_node(), &entry_path, &fs_dir.store)?;
                if inp.get_last_inode().is_none() {
                    warn!("Delete: path not found: {}", entry_path);
                    return Ok(());
                }
                fs_dir.unprotected_delete(&inp, entry.mtime)?;
                Ok(())
            });
        }
        self.with_locked_replay_view(&entry_path, true, |view, inp| {
            if inp.get_last_inode().is_none() {
                warn!("Delete: path not found: {}", entry_path);
                return Ok(());
            }
            view.unprotected_delete(&inp, entry.mtime)?;
            Ok(())
        })
    }

    pub fn free(&self, entry: FreeEntry) -> CommonResult<()> {
        if entry.recursive {
            // C2: a recursive free replays under the bounded subtree-lock
            // path with the full inode → worker → block order (a free
            // always drops data blocks).
            return self.with_replay_locks(&entry.path, InodeLockMode::Write, true, |fs_dir| {
                let inp = InodePath::resolve(fs_dir.root_node(), &entry.path, &fs_dir.store)?;
                let Some(inode) = inp.get_last_inode() else {
                    warn!("Free: path not found: {:?}", entry);
                    return Ok(());
                };
                fs_dir.unprotected_free(inode, entry.mtime, entry.recursive)?;
                Ok(())
            });
        }
        let entry_path = entry.path.clone();
        self.with_locked_replay_view(&entry_path, true, |view, inp| {
            let Some(inode) = inp.get_last_inode() else {
                warn!("Free: path not found: {:?}", entry);
                return Ok(());
            };
            view.unprotected_free(inode, entry.mtime, entry.recursive)?;
            Ok(())
        })
    }

    pub fn mount(&self, entry: MountEntry) -> CommonResult<()> {
        // Namespace first, then the mount table transaction: the same order
        // as the online RPC path. Replay shares the view + mount business lock
        // (same protocol as the online path after S6-b).
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        Ok(self.mnt_mgr.replay_mount(&view, entry.info)?)
    }

    pub fn unmount(&self, entry: UnMountEntry) -> CommonResult<()> {
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        Ok(self.mnt_mgr.replay_unmount(&view, entry.id)?)
    }

    pub fn set_attr(&self, entry: SetAttrEntry) -> CommonResult<()> {
        if entry.opts.recursive {
            // C2: a recursive set_attr replays under the bounded subtree-
            // lock path (parent Read; no location locks — attributes do not
            // touch blocks).
            let entry_path = entry.path.clone();
            let opts = entry.opts.clone();
            return self.with_replay_locks(&entry_path, InodeLockMode::Read, false, |fs_dir| {
                let inp = InodePath::resolve(fs_dir.root_node(), &entry_path, &fs_dir.store)?;
                let last_inode = match inp.get_last_inode() {
                    Some(v) => v,
                    None => {
                        warn!("SetAttr: path not found: {:?}", entry_path);
                        return Ok(());
                    }
                };
                fs_dir.unprotected_set_attr(last_inode, opts.clone())?;
                Ok(())
            });
        }
        let entry_path = entry.path.clone();
        self.with_locked_replay_view(&entry_path, false, |view, inp| {
            let last_inode = match inp.get_last_inode() {
                Some(v) => v,
                None => {
                    warn!("SetAttr: path not found: {:?}", entry);
                    return Ok(());
                }
            };

            view.unprotected_set_attr(last_inode, entry.opts)?;
            Ok(())
        })
    }

    pub fn symlink(&self, entry: SymlinkEntry) -> CommonResult<()> {
        let link_path = entry.link;
        self.with_locked_replay_view(&link_path, true, |view, inp| {
            match view.unprotected_symlink(inp, entry.new_inode, entry.force) {
                Ok(_) => Ok(()),
                Err(FsError::FileAlreadyExists(_)) => {
                    warn!("Symlink: file already exists: {:?}", link_path);
                    Ok(())
                }
                Err(e) => Err(e.into()),
            }
        })
    }

    pub fn link(&self, entry: LinkEntry) -> CommonResult<()> {
        // Link replaces the destination edge (and touches the source's
        // nlink): both chains planned with parent-write, and the planning
        // epoch + both ID chains are revalidated under the locks.
        let (plan_epoch, (src_requests, dst_requests)) = {
            let view = self.fs_dir.view().map_err(|e| e.to_string())?;
            let control = view.store.scan_control();
            if control.is_restoring() {
                return err_box!("link replay rejected: snapshot restore pending");
            }
            let epoch = control.cancel_epoch();
            let src_reqs = Self::plan_replay_locks_mode(&view, &entry.src_path, true)?;
            let dst_reqs = Self::plan_replay_locks_mode(&view, &entry.dst_path, true)?;
            (epoch, (src_reqs, dst_reqs))
        };
        // Same per-chain validation as rename: the two locked segments are
        // compared in order, keeping per-path identity.
        let mut requests = src_requests.clone();
        requests.extend(dst_requests.iter().cloned());
        let _locks = self.inode_locks.lock_many(&requests);
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        let control = view.store.scan_control();
        if control.is_restoring() {
            return err_box!("link replay rejected: snapshot restore pending");
        }
        if control.cancel_epoch() != plan_epoch {
            return err_box!(
                "link replay validation failed: epoch moved ({} -> {})",
                plan_epoch,
                control.cancel_epoch()
            );
        }
        let old_path = InodePath::resolve(view.root_node(), &entry.src_path, &view.store)?;
        let new_path = InodePath::resolve(view.root_node(), &entry.dst_path, &view.store)?;
        let src_ids: Vec<i64> = old_path.inodes.iter().map(|e| e.id()).collect();
        let dst_ids: Vec<i64> = new_path.inodes.iter().map(|e| e.id()).collect();
        let src_locked: Vec<i64> = requests
            .iter()
            .take(src_requests.len())
            .map(|r| r.inode_id)
            .collect();
        let dst_locked: Vec<i64> = requests
            .iter()
            .skip(src_requests.len())
            .map(|r| r.inode_id)
            .collect();
        if src_locked != src_ids || dst_locked != dst_ids {
            return err_box!(
                "link replay validation failed: locked src={:?} dst={:?} live src={:?} dst={:?}",
                src_locked,
                dst_locked,
                src_ids,
                dst_ids
            );
        }

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

        match view.unprotected_link(new_path, original_inode_id, entry.mtime as u64) {
            Ok(_) => Ok(()),
            Err(FsError::FileAlreadyExists(_)) => {
                warn!("Link: dst_path already exists: {:?}", entry);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_locks(&self, entry: SetLocksEntry) -> CommonResult<()> {
        let requests = {
            let view = self.fs_dir.view().map_err(|e| e.to_string())?;
            self.plan_replay_locks_by_inode(&view, entry.ino)?
        };
        let _locks = self.inode_locks.lock_many(&requests);
        let view = self.fs_dir.view().map_err(|e| e.to_string())?;
        view.store.apply_set_locks(entry.ino, &entry.locks)
    }

    pub fn ufs_applied(&self, entry: UfsAppliedEntry) -> CommonResult<()> {
        let mut state = self.fsm_state()?;
        if entry.index > state.ufs_applied.index {
            state.ufs_applied = AppliedIndex {
                op_id: state.ufs_applied.op_id.max(entry.op_id),
                rpc_id: entry.rpc_id,
                term: entry.term,
                index: entry.index,
            };
            self.refresh_fsm_metrics(&state);
        }
        Ok(())
    }

    fn apply_ufs_deferred(&self, entry: UfsDeferredEntry) -> CommonResult<()> {
        self.put_deferred_ufs(entry.index, &entry.payload)?;
        let current = self.fsm_state_snapshot()?.ufs_applied.index;
        if entry.index > current {
            self.ufs_applied(UfsAppliedEntry {
                op_id: entry.op_id,
                rpc_id: entry.rpc_id,
                term: entry.term,
                index: entry.index,
            })?;
        }
        Ok(())
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
        // Let any in-flight UFS operation finish while the apply worker is
        // still available, then release the replay task's log-store handle.
        self.ufs_shutdown.store(true, Ordering::Release);
        self.ufs_wake.notify_one();
        let mut stopped = self.ufs_stopped.subscribe();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !*stopped.borrow_and_update() {
                stopped
                    .changed()
                    .await
                    .map_err(|error| RaftError::from(error.to_string()))?;
            }
            Ok::<(), RaftError>(())
        })
        .await
        .map_err(|_| RaftError::from("timed out stopping UFS journal replay".to_owned()))??;
        let (tx, rx) = CallChannel::channel();
        self.sender.send(ApplyMsg::Shutdown(tx)).await?;
        rx.receive().await?;
        Ok(())
    }

    async fn apply_direct(&self, msg: ApplyMsg) -> RaftResult<()> {
        let result = self.apply_msg(false, &msg, false).await.map_err(Into::into);

        if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
            Self::complete_entry_ack(msg, result);
            Ok(())
        } else {
            result
        }
    }
}

impl AppStorage for JournalLoader {
    async fn apply(&self, wait: bool, msg: ApplyMsg) -> RaftResult<()> {
        if !self.has_apply_worker {
            return self.apply_direct(msg).await;
        }

        if matches!(&msg, ApplyMsg::EntryWithAck(_) | ApplyMsg::ScanWithAck(_)) {
            self.sender.send(msg).await?;
            return Ok(());
        }

        if wait {
            return match msg {
                ApplyMsg::Scan(applied) => {
                    let (tx, rx) = CallChannel::channel();
                    self.sender
                        .send(ApplyMsg::ScanWithAck((applied, tx)))
                        .await?;
                    rx.receive().await?
                }
                other => self.apply_direct(other).await,
            };
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
    }

    async fn role_change(&self, role: StateRole) -> RaftResult<()> {
        // Fence before waiting for the apply queue: a delayed role-change
        // message must not leave the previous role accepting new writes.
        let epoch = self.journal_writer.fence_metadata_with(|| {
            self.is_leader.store(false, Ordering::Release);
            self.recovery.bump_generation();
            self.recovery.set_ufs_caught_up(false);
        });
        self.ufs_wake.notify_one();
        if !self.has_apply_worker {
            if role == StateRole::Leader {
                self.catch_up_committed_metadata().await?;
                if self.group_size > 1 {
                    let negotiator = self.clone();
                    tokio::spawn(async move {
                        negotiator.negotiate_record_format().await;
                    });
                } else {
                    self.negotiate_record_format().await;
                }
                if !self.journal_writer.unfence_metadata_if_epoch(epoch, || {}) {
                    return Err("leader promotion superseded by a newer metadata fence"
                        .to_string()
                        .into());
                }
            } else {
                curvine_model::persist_children_count(false);
            }
            return Ok(());
        }
        let (tx, rx) = CallChannel::channel();
        self.sender
            .send(ApplyMsg::RoleChange((role, epoch, tx)))
            .await?;
        rx.receive().await?
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        // The task retains the fence until the queued snapshot completes even
        // if the caller drops its future after sending the apply message.
        let loader = self.clone();
        tokio::spawn(async move {
            // In-flight commits may need the apply worker, so drain elsewhere.
            let gate = loader.metadata_commit_gate.clone();
            let _barrier = tokio::task::spawn_blocking(move || gate.close_and_wait_owned())
                .await
                .map_err(|error| RaftError::from(error.to_string()))?;
            if !loader.has_apply_worker {
                return loader.create_snapshot0(None);
            }
            let (tx, rx) = CallChannel::channel();
            loader.sender.send(ApplyMsg::CreateSnapshot(tx)).await?;
            rx.receive().await?
        })
        .await
        .map_err(|error| RaftError::from(error.to_string()))?
    }

    async fn apply_snapshot(&self, snapshot: SnapshotData) -> RaftResult<()> {
        // Admitted writers finish only after their Raft commit, and a commit
        // response waits on the apply worker. Draining them here — in the Raft
        // job executor — would therefore deadlock if the barrier ran inside
        // the apply worker. Close the gate in the caller, keep it closed while
        // the worker replaces metadata, and release it after completion.
        let gate = self.metadata_commit_gate.clone();
        let _barrier = tokio::task::spawn_blocking(move || gate.close_and_wait_owned())
            .await
            .map_err(|error| RaftError::from(error.to_string()))?;
        if !self.has_apply_worker {
            return self.apply_snapshot0(snapshot);
        }
        let (tx, rx) = CallChannel::channel();
        let msg = ApplyMsg::ApplySnapshot((tx, snapshot));

        self.sender.send(msg).await?;
        rx.receive().await?
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        let fs_dir = self
            .fs_dir
            .view()
            .map_err(|e| RaftError::from(e.to_string()))?;
        Ok(fs_dir.get_checkpoint_path(snapshot_id))
    }
}

#[cfg(test)]
#[path = "tests/journal_watermark_test.rs"]
mod journal_watermark_test;

#[cfg(test)]
#[path = "tests/recursive_replay_lock_test.rs"]
mod recursive_replay_lock_test;

#[cfg(test)]
#[path = "tests/recursive_rpc_progress_test.rs"]
mod recursive_rpc_progress_test;

#[cfg(test)]
#[path = "tests/replay_lock_test.rs"]
mod replay_lock_test;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::{DeleteEntry, MkdirEntry};
    use crate::master::meta::inode::InodeDir;
    use curvine_config::{ClusterConf, MasterConf};
    use curvine_error::FsResult;
    use curvine_runtime::common::Utils;

    // Regression tests for the batch-deserialization skip branch in `apply0`:
    // with `ignore_reply_error` enabled an undecodable batch (typically written
    // by a newer-schema leader during a rolling upgrade) is skipped and the
    // apply watermark advances past it; with the flag disabled the replay error
    // is propagated and the watermark must not move.
    pub(super) fn ignore_reply_error_conf(
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

    // Not a valid JournalBatchProto payload.
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

    fn valid_entry(index: u64, term: u64) -> Entry {
        let mut batch = JournalBatch::new(1);
        batch.push(JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 41,
            rpc_id: 9,
            inodes: vec![],
        }));
        let data = batch.encode().unwrap();

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
    fn leader_metadata_apply_does_not_advance_ufs_watermark() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("leader", true);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        for index in 1..=2 {
            rt.block_on(loader.apply0(true, &corrupt_entry(index, 7), false))?;
        }
        let state = loader.get_fsm_state();
        assert_eq!(
            state.ufs_applied.index, 0,
            "leader metadata apply must not advance the UFS watermark"
        );
        assert_eq!(
            state.applied.index, 2,
            "leader metadata watermark still advances"
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
            error.to_string().contains("failed to decode journal batch"),
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

    fn raft_batch(index: u64, op: JournalEntry) -> Entry {
        let mut batch = JournalBatch::new(index);
        batch.push(op);
        Entry {
            index,
            term: 3,
            entry_type: EntryType::EntryNormal as i32,
            data: batch.encode().unwrap(),
            ..Default::default()
        }
    }

    fn set_metadata_applied(loader: &JournalLoader, index: u64) {
        let mut state = loader.fsm_state.lock().expect("fsm lock");
        state.applied.index = index;
        state.applied.term = 3;
    }

    #[test]
    fn ufs_replay_waits_for_a_live_worker_then_catches_up() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-gate", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        let mkdir = JournalEntry::Mkdir(MkdirEntry {
            op_id: 1,
            rpc_id: 1,
            path: "/ufs".to_string(),
            dir: InodeDir::new(1, 0),
        });
        let delete = JournalEntry::Delete(DeleteEntry {
            op_id: 2,
            rpc_id: 2,
            path: "/ufs/file".to_string(),
            mtime: 0,
        });
        loader
            .log_store
            .append(&[raft_batch(1, mkdir), raft_batch(2, delete)])
            .expect("log append");
        set_metadata_applied(&loader, 2);
        loader.is_leader.store(true, Ordering::Release);

        let waiting = rt.block_on(loader.drive_ufs_once(loader.recovery.generation()));
        assert!(
            matches!(waiting, UfsDrive::Advanced),
            "mkdir does not need a worker"
        );
        loader.record_ufs_progress(&waiting);
        assert!(!loader.recovery.ufs_caught_up());
        assert_eq!(loader.get_fsm_state().ufs_applied.index, 1);

        let blocked = rt.block_on(loader.drive_ufs_once(loader.recovery.generation()));
        assert!(
            matches!(blocked, UfsDrive::WaitWorkers),
            "export must not run while worker_map is empty"
        );
        loader.record_ufs_progress(&blocked);
        assert!(!loader.recovery.ufs_caught_up());
        assert_eq!(loader.get_fsm_state().ufs_applied.index, 1);
        assert_eq!(loader.get_fsm_state().applied.index, 2);

        let cluster_id = loader.worker_manager.read().cluster_id.clone();
        loader
            .worker_manager
            .write()
            .heartbeat(
                &cluster_id,
                curvine_model::HeartbeatStatus::Running,
                curvine_model::WorkerAddress {
                    worker_id: 4,
                    hostname: "worker-4".to_string(),
                    ip_addr: "127.0.0.1".to_string(),
                    rpc_port: 8997,
                    web_port: 9000,
                },
                1,
                String::new(),
                curvine_model::TransferWorkerCapabilities::default(),
                0,
                0,
                String::new(),
                0,
                vec![],
                None,
                false,
            )
            .expect("running heartbeat");
        assert_eq!(loader.live_worker_count(), 1);

        let resumed = rt.block_on(loader.drive_ufs_once(loader.recovery.generation()));
        assert!(
            matches!(resumed, UfsDrive::Advanced),
            "delete replay continues from the previous ufs watermark"
        );
        assert_eq!(loader.get_fsm_state().ufs_applied.index, 2);

        let caught_up = rt.block_on(loader.drive_ufs_once(loader.recovery.generation()));
        assert!(matches!(caught_up, UfsDrive::CaughtUp));
        loader.record_ufs_progress(&caught_up);
        assert!(loader.recovery.ufs_caught_up());
        assert_eq!(loader.recovery.pending(), 0);

        let stale = rt.block_on(loader.drive_ufs_once(loader.recovery.generation() + 1));
        assert!(matches!(stale, UfsDrive::Stopped));
        assert_eq!(loader.get_fsm_state().ufs_applied.index, 2);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn corrupt_ufs_replay_enters_safe_mode_without_advancing() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-damage", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        loader
            .log_store
            .append(&[corrupt_entry(1, 3)])
            .expect("log append");
        set_metadata_applied(&loader, 1);
        loader.is_leader.store(true, Ordering::Release);

        let drive = rt.block_on(loader.drive_ufs_once(loader.recovery.generation()));
        assert!(matches!(drive, UfsDrive::Stopped));
        assert!(loader.recovery.is_safe_mode());
        assert_eq!(loader.get_fsm_state().ufs_applied.index, 0);
        assert_eq!(loader.get_fsm_state().applied.index, 1);

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }

    #[test]
    fn permanent_ufs_failure_is_stored_without_reporting_catch_up() -> FsResult<()> {
        let (conf, root) = ignore_reply_error_conf("ufs-defer", false);
        let js = JournalSystem::from_conf(&conf)?;
        let loader = js.journal_loader();
        let rt = conf.journal.create_runtime();

        let mkdir = JournalEntry::Mkdir(MkdirEntry {
            op_id: 1,
            rpc_id: 1,
            path: "/ufs".to_string(),
            dir: InodeDir::new(1, 0),
        });
        let delete = JournalEntry::Delete(DeleteEntry {
            op_id: 2,
            rpc_id: 2,
            path: "/ufs/file".to_string(),
            mtime: 0,
        });
        loader
            .log_store
            .append(&[raft_batch(1, mkdir), raft_batch(2, delete)])
            .expect("log append");
        set_metadata_applied(&loader, 2);
        loader.is_leader.store(true, Ordering::Release);
        let generation = loader.recovery.generation();

        let mkdir_drive = rt.block_on(loader.drive_ufs_once(generation));
        assert!(matches!(mkdir_drive, UfsDrive::Advanced));
        loader.record_ufs_progress(&mkdir_drive);
        let blocked = rt.block_on(loader.drive_ufs_once(generation));
        assert!(matches!(blocked, UfsDrive::WaitWorkers));
        assert_eq!(loader.deferred_ufs_count().expect("count"), 0);
        assert_eq!(loader.get_fsm_state().compact(), 0);

        assert!(rt
            .block_on(loader.defer_current_ufs(generation, "UFS permission denied"))
            .expect("defer"));
        let state = loader.get_fsm_state();
        assert_eq!(state.ufs_applied.index, 2);
        assert_eq!(state.compact(), 1, "log GC can pass the copied UFS op");
        assert!(!loader.recovery.ufs_caught_up());
        assert_eq!(loader.deferred_ufs_count().expect("count"), 1);

        let still_blocked = rt.block_on(loader.drive_ufs_once(generation));
        assert!(matches!(still_blocked, UfsDrive::WaitWorkers));
        assert_eq!(loader.deferred_ufs_count().expect("count"), 1);
        assert!(!loader.recovery.ufs_caught_up());

        let cluster_id = loader.worker_manager.read().cluster_id.clone();
        loader
            .worker_manager
            .write()
            .heartbeat(
                &cluster_id,
                curvine_model::HeartbeatStatus::Running,
                curvine_model::WorkerAddress {
                    worker_id: 4,
                    hostname: "worker-4".to_string(),
                    ip_addr: "127.0.0.1".to_string(),
                    rpc_port: 8997,
                    web_port: 9000,
                },
                1,
                String::new(),
                curvine_model::TransferWorkerCapabilities::default(),
                0,
                0,
                String::new(),
                0,
                vec![],
                None,
                false,
            )
            .expect("running heartbeat");

        let retried = rt.block_on(loader.drive_ufs_once(generation));
        assert!(matches!(retried, UfsDrive::Advanced));
        assert_eq!(loader.deferred_ufs_count().expect("count"), 0);
        let caught_up = rt.block_on(loader.drive_ufs_once(generation));
        assert!(matches!(caught_up, UfsDrive::CaughtUp));
        loader.record_ufs_progress(&caught_up);
        assert!(loader.recovery.ufs_caught_up());

        drop(loader);
        js.shutdown();
        let _ = fs::remove_dir_all(&root);
        Ok(())
    }
}
