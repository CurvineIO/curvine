//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::common::UfsFactory;
use crate::master::journal::{
    CompleteFileEntry, DeleteEntry, JournalBatch, JournalEntry, JournalWriter, MkdirEntry,
    ReleaseWriterEntry, RenameEntry,
};
use crate::master::meta::inode::InodeDir;
use crate::master::{Master, MasterMetrics, MountManager, SyncFsDir};
use curvine_config::{ClusterConf, JournalConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{TransferCommand, TransferKind, TransferState, SOURCE_NOT_FOUND_MARKER};
use curvine_raft::proto::raft::{AppliedIndex, FsmState};
use curvine_raft::raft::storage::{LogStorage, RocksLogStorage};
use curvine_runtime::common::DurationUnit;
use curvine_runtime::runtime::Runtime;
use curvine_transfer_client::TransferClient;
use curvine_unified_fs::MountValue;
use log::{debug, error, info, warn};
use raft::eraftpb::{Entry, EntryType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Result of replaying one raft entry in the background worker.
enum Step {
    /// Replayed; carries the watermark when the entry owed a UFS effect.
    Applied(Option<AppliedIndex>),
    /// Leadership was lost while retrying: abandon the entry and re-evaluate
    /// instead of writing a shared UFS the new leader now owns.
    Paused,
    /// The worker must exit because shutdown was requested while backing off.
    Stop,
}

/// How often the worker re-reads the watermarks while it believes it has
/// nothing to do. A UFS copy takes minutes, so polling once a second costs
/// nothing measurable and keeps every wake-up signal out of the design: there
/// is no notification to lose, because there is no notification.
const POLL_INTERVAL: Duration = Duration::from_secs(1);

const STALL_AFTER: Duration = Duration::from_secs(60);

const UFS_PENDING: &str = "pending";
const UFS_STALLED: &str = "stalled";
const UFS_FAILED: &str = "failed";

/// How often an export in flight is polled. The replayer runs one entry at a
/// time, so this is a single status RPC per interval, and a UFS copy takes
/// minutes: a coarse interval costs nothing and keeps the transfer service out
/// of the replay loop.
const EXPORT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Who submits the journal write-back transfers. The transfer store dedups on
/// `(submitter, client_request_id)`, so this has to stay stable across restarts
/// for replay retries to keep converging on one job.
const EXPORT_SUBMITTER: &str = "curvine-master-journal";

/// One export transfer the replayer is waiting on.
struct ExportJob {
    job_id: String,
    run_id: u64,
    state: TransferState,
}

#[derive(Clone)]
pub struct UfsLoader {
    /// Mount lookup for the replay path. Only fs_mode mounts owe a UFS effect.
    mount_manager: Arc<MountManager>,
    ufs_factory: Arc<UfsFactory>,
    /// Loopback to the transfer service that performs the write-back.
    transfer: TransferClient,
    fs_dir: SyncFsDir,
    copy_timeout: Duration,

    // ---- asynchronous UFS replay ----
    //
    // The metadata apply loop never touches UFS: a slow or failing object store
    // cannot stall raft apply, and every UFS op below is idempotent, so a role
    // change may abandon an entry and the next leader redoes it.
    /// Local leadership. UFS is a single writer shared by the whole cluster, so
    /// followers must never repeat these effects.
    is_leader: Arc<AtomicBool>,
    /// Set at shutdown. The worker polls it like every other input, so there is
    /// no signal that can be missed.
    stopped: Arc<AtomicBool>,
    /// The log is the work queue: `(ufs_applied, applied]` needs its UFS side
    /// effects, and this worker reads it directly instead of buffering entries.
    log_store: RocksLogStorage,
    journal_writer: Arc<JournalWriter>,
    fsm_state: Arc<Mutex<FsmState>>,
    retry_interval: Duration,
    metrics: &'static MasterMetrics,
}

/// Builds the transfer client the journal write-back dispatches through.
///
/// A configured standalone `transfer.endpoints` is used as-is. Otherwise transfer
/// runs inside this master, and the loopback is pinned to this master's own RPC
/// endpoint rather than the whole master set: only the leader replays UFS effects
/// and only the leader's embedded transfer scheduler runs, so a submit that
/// landed on a follower would sit in that follower's in-memory store forever.
pub(crate) fn export_transfer_client(
    conf: &ClusterConf,
    rt: Arc<Runtime>,
) -> FsResult<TransferClient> {
    if !conf.transfer.endpoints.is_empty() {
        return TransferClient::with_rt(conf, rt);
    }
    let mut loopback = conf.clone();
    loopback.transfer.endpoints = vec![conf.master_addr().to_string()];
    TransferClient::with_rt(&loopback, rt)
}

impl UfsLoader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        mount_manager: Arc<MountManager>,
        ufs_factory: Arc<UfsFactory>,
        transfer: TransferClient,
        fs_dir: SyncFsDir,
        conf: &JournalConf,
        log_store: RocksLogStorage,
        journal_writer: Arc<JournalWriter>,
        fsm_state: Arc<Mutex<FsmState>>,
    ) -> CommonResult<Self> {
        let copy_timeout = match DurationUnit::from_str(&conf.ufs_copy_timeout) {
            Ok(unit) => unit.as_duration(),
            Err(e) => {
                warn!(
                    "Invalid ufs_copy_timeout value '{}': {}. Falling back to default 20min",
                    conf.ufs_copy_timeout, e
                );
                Duration::from_secs(20 * 60)
            }
        };

        Ok(Self {
            mount_manager,
            ufs_factory,
            transfer,
            fs_dir,
            copy_timeout,
            is_leader: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            log_store,
            journal_writer,
            fsm_state,
            retry_interval: Duration::from_secs(conf.retry_interval_secs),
            metrics: Master::get_metrics()?,
        })
    }

    /// Publish local leadership. UFS is a single writer shared by the whole
    /// cluster, so the worker only replays while this node leads.
    pub fn set_leader(&self, is_leader: bool) {
        self.is_leader.store(is_leader, Ordering::Release);
    }

    /// Ask the replayer to exit. It notices within one poll interval.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    fn fsm_state_snapshot(&self) -> CommonResult<FsmState> {
        match self.fsm_state.lock() {
            Ok(state) => Ok(state.clone()),
            Err(e) => err_box!("fsm_state lock poisoned: {}", e),
        }
    }

    fn publish_ufs(&self, kind: &str, value: i64) {
        self.metrics
            .journal_ufs
            .with_label_values(&[kind])
            .set(value);
    }

    fn note_replay_failure(&self) {
        self.metrics
            .journal_ufs
            .with_label_values(&[UFS_FAILED])
            .inc();
    }

    /// Replay every UFS side effect the watermarks still owe.
    ///
    /// Progress lives entirely in `ufs_applied`: there is no in-memory queue to
    /// lose on restart, and a replayed entry that was already applied is a no-op
    /// because every UFS op is idempotent.
    pub async fn run(&self) {
        while self.cycle().await {
            // One entry per cycle is mostly synchronous work (decode, watermark,
            // lock), so a long backlog would never hit an await point. Yield
            // after every entry instead of monopolising the runtime thread.
            tokio::task::yield_now().await;
        }
        info!("ufs replayer stopped");
    }

    /// One poll cycle: replay the single entry the watermarks owe next.
    async fn cycle(&self) -> bool {
        if !self.is_leader.load(Ordering::Acquire) {
            self.clear_progress();
            return self.idle().await;
        }

        let Some((from, to)) = self.owed_range() else {
            return self.idle().await;
        };

        if from >= to {
            return self.idle().await;
        }

        // Exactly one entry per cycle. `scan_entries` silently skips a missing
        // key, so a wider window could advance `ufs_applied` past side effects
        // that never ran; a single index makes the hole impossible to miss, and
        // losing leadership then abandons at most one entry.
        let next = from + 1;
        let mut entries = match self.log_store.scan_entries(next, next + 1) {
            Ok(entries) => entries,
            Err(e) => {
                error!(
                    "ufs replay: scanning journal entry {} failed, retrying: {}",
                    next, e
                );
                return self.sleep_or_stop().await;
            }
        };
        if entries.len() != 1 || entries[0].index != next {
            self.report_gap(from, next);
            return self.idle().await;
        }

        let entry = entries.remove(0);
        match self.replay_one(&entry).await {
            Step::Applied(reached) => {
                self.record_watermark(reached);
                true
            }
            Step::Paused => true,
            Step::Stop => false,
        }
    }

    /// Drain the owed range one entry per cycle, the way the replayer task does
    /// it. Test-only: the replayer is not spawned when the journal system runs
    /// in testing mode, so a test that has to observe a caught-up UFS watermark
    /// drives the same loop here instead.
    #[doc(hidden)]
    pub async fn drain_replay_for_test(&self) -> CommonResult<()> {
        let was_leader = self.is_leader.swap(true, Ordering::Release);
        let result = async {
            loop {
                let (from, to) = self.replay_range()?;
                if from >= to {
                    return Ok(());
                }
                if !self.cycle().await {
                    return err_box!("ufs replay stopped before draining ({}, {}]", from, to);
                }
                if self.replay_range()?.0 == from {
                    return err_box!(
                        "ufs replay made no progress at index {}, owes ({}, {}]",
                        from,
                        from,
                        to
                    );
                }
            }
        }
        .await;
        self.is_leader.store(was_leader, Ordering::Release);
        result
    }

    /// `(ufs_applied, applied]`, or `None` when the range cannot be read.
    fn owed_range(&self) -> Option<(u64, u64)> {
        match self.replay_range() {
            Ok((from, to)) => {
                self.publish_ufs(UFS_PENDING, to.saturating_sub(from) as i64);
                Some((from, to))
            }
            Err(e) => {
                if self.enter_stall() {
                    error!(
                        "ufs replay: cannot read the watermarks, the UFS backlog cannot drain: {}",
                        e
                    );
                }
                None
            }
        }
    }

    fn clear_progress(&self) {
        self.publish_ufs(UFS_PENDING, 0);
        self.publish_ufs(UFS_STALLED, 0);
    }

    fn report_gap(&self, from: u64, next: u64) {
        if self.enter_stall() {
            error!(
                "ufs replay: metadata invariant broken, ufs replay is stuck at index {}, \
                 but journal entry {} is not on disk; the UFS backlog cannot drain while \
                 compaction stays pinned at ufs_applied",
                from, next
            );
        }
    }

    fn enter_stall(&self) -> bool {
        let stalled = self.metrics.journal_ufs.with_label_values(&[UFS_STALLED]);
        if stalled.get() == 1 {
            return false;
        }
        stalled.set(1);
        true
    }

    fn clear_stall(&self) {
        self.publish_ufs(UFS_STALLED, 0);
    }

    /// One marker per replayed entry: a follower only needs the latest
    /// watermark to resume from. Losing a marker is harmless because a follower
    /// that promotes replays the unmarked suffix, which is idempotent.
    fn record_watermark(&self, reached: Option<AppliedIndex>) {
        if !self.is_leader.load(Ordering::Acquire) {
            return;
        }
        let Some(applied) = reached else {
            return;
        };
        if let Err(e) =
            self.journal_writer
                .log_ufs_applied(applied.op_id, applied.term, applied.index)
        {
            warn!(
                "ufs replay: recording watermark {} failed, continuing: {}",
                applied.index, e
            );
        }
    }

    /// Sleep one poll interval. Returns false when shutdown was requested.
    async fn idle(&self) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        !self.stopped.load(Ordering::Acquire)
    }

    /// Back off before retrying a failed entry, in poll sized steps so shutdown
    /// is still noticed promptly.
    async fn sleep_or_stop(&self) -> bool {
        let mut left = self.retry_interval;
        while !left.is_zero() {
            let step = left.min(POLL_INTERVAL);
            tokio::time::sleep(step).await;
            if self.stopped.load(Ordering::Acquire) {
                return false;
            }
            left = left.saturating_sub(step);
        }
        true
    }

    /// `(ufs_applied, applied]`: what metadata apply already committed but the
    /// UFS side effect has not caught up with yet.
    pub(super) fn replay_range(&self) -> CommonResult<(u64, u64)> {
        let state = self.fsm_state_snapshot()?;
        Ok((state.ufs_applied.index, state.applied.index))
    }

    /// Apply one raft entry's UFS effects.
    async fn replay_one(&self, entry: &Entry) -> Step {
        let mut failing_since: Option<Instant> = None;
        let mut stall_logged = false;

        loop {
            if !self.is_leader.load(Ordering::Acquire) {
                return Step::Paused;
            }
            if self.stopped.load(Ordering::Acquire) {
                return Step::Stop;
            }

            match self.replay_entry(entry).await {
                Ok(reached) => {
                    self.clear_stall();
                    return Step::Applied(reached);
                }
                Err(e) => {
                    // A demotion (or shutdown) interrupts an in-flight UFS
                    // effect, for instance the export wait below. The entry is
                    // not failing, so do not count it, do not sleep, and do not
                    // touch the watermark: the next leader redoes the entry
                    // and its own submit dedups onto the same transfer job.
                    if !self.is_leader.load(Ordering::Acquire) {
                        return Step::Paused;
                    }
                    if self.stopped.load(Ordering::Acquire) {
                        return Step::Stop;
                    }

                    self.note_replay_failure();
                    let _ = self.owed_range();

                    let since = *failing_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= STALL_AFTER {
                        self.publish_ufs(UFS_STALLED, 1);
                        if !stall_logged {
                            stall_logged = true;
                            error!(
                                "ufs replay: entry {} has been failing for {:?}, the UFS \
                                 backlog cannot drain and journal compaction stays pinned at \
                                 ufs_applied: {}",
                                entry.index,
                                since.elapsed(),
                                e
                            );
                        }
                    }
                    warn!(
                        "ufs replay failed at index {}, retrying in {:?}: {}",
                        entry.index, self.retry_interval, e
                    );
                    if !self.sleep_or_stop().await {
                        return Step::Stop;
                    }
                }
            }
        }
    }

    /// Replay one raft entry's UFS effects and advance the watermark. Returns
    /// the watermark when the entry owed a UFS effect.
    pub(super) async fn replay_entry(&self, entry: &Entry) -> CommonResult<Option<AppliedIndex>> {
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            let applied = Self::build_applied(entry);
            self.advance(applied)?;
            return Ok(None);
        }

        let batch = JournalBatch::deserialize_compat(&entry.data)?;
        let mut applied = Self::build_applied(entry);
        let mut has_ufs_affecting = false;

        for op_entry in batch.batch {
            applied.op_id = op_entry.op_id();
            applied.rpc_id = op_entry.rpc_id();

            match &op_entry {
                JournalEntry::Mkdir(_)
                | JournalEntry::CompleteFile(_)
                | JournalEntry::Rename(_)
                | JournalEntry::Delete(_)
                | JournalEntry::ReleaseWriter(_) => {}
                _ => continue,
            }

            has_ufs_affecting = true;
            self.apply_entry(&op_entry).await?;
        }

        self.advance(applied.clone())?;
        Ok(if has_ufs_affecting {
            Some(applied)
        } else {
            None
        })
    }

    fn build_applied(entry: &Entry) -> AppliedIndex {
        AppliedIndex {
            term: entry.term,
            index: entry.index,
            ..Default::default()
        }
    }

    /// Advance `ufs_applied` for one replayed entry.
    fn advance(&self, applied: AppliedIndex) -> CommonResult<()> {
        let mut state = match self.fsm_state.lock() {
            Ok(state) => state,
            Err(e) => return err_box!("fsm_state lock poisoned: {}", e),
        };
        if applied.index >= state.ufs_applied.index {
            state.ufs_applied = applied;
        }
        self.metrics
            .journal_ufs_applied
            .set(state.ufs_applied.index as i64);
        Ok(())
    }

    pub fn get_real_path(&self, inode_id: i64) -> FsResult<Path> {
        let path = self.fs_dir.read().get_inode_path(inode_id)?;
        Ok(Path::from_str(path)?)
    }

    /// Resolves the mount of `path` and returns its UFS side.
    ///
    /// Same resolution the load-job manager used to provide: mount table ->
    /// `MountValue` -> toggled path, restricted to fs_mode mounts because only
    /// those owe a UFS side effect.
    fn get_mnt(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        match self.mount_manager.get_mount_info(path)? {
            Some(info) => {
                let mnt = self.ufs_factory.get_mnt(&info)?;
                if !mnt.info.is_fs_mode() {
                    return Ok(None);
                }
                Ok(Some((mnt.toggle_path(path)?, mnt)))
            }
            None => Ok(None),
        }
    }

    async fn mkdir_parent(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        if let Some(parent) = mnt.info.get_ufs_path(path)?.parent()? {
            mnt.ufs()?.mkdir(&parent, true).await?;
        }
        Ok(())
    }

    async fn mkdir_with_parent_retry(&self, path: &Path, mnt: &MountValue) -> FsResult<bool> {
        let ufs_path = mnt.info.get_ufs_path(path)?;
        match mnt.ufs()?.mkdir(&ufs_path, false).await {
            Ok(created) => Ok(created),
            Err(FsError::FileNotFound(_)) => {
                self.mkdir_parent(path, mnt).await?;
                mnt.ufs()?.mkdir(&ufs_path, false).await
            }
            Err(e) => Err(e),
        }
    }

    /// Copies one file back to its UFS, bounded by a single `ufs_copy_timeout`
    /// deadline.
    ///
    /// The deadline covers **submit and completion together**, exactly as it did
    /// for the load-job runner: submitting has to snapshot the cluster and reach
    /// the transfer service, and a copy that then outlives its budget keeps the
    /// journal watermark pinned. A timeout cancels the transfer and fails the
    /// entry, so the watermark does not advance and the replay retries it.
    pub async fn submit_export_task(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        let deadline = tokio::time::Instant::now() + self.copy_timeout;
        let job = match tokio::time::timeout_at(deadline, self.dispatch_export(path, mnt)).await {
            Ok(result) => result?,
            Err(_) => {
                return err_box!(
                    "export of {} timed out after {:?} before it was submitted",
                    path.full_path(),
                    self.copy_timeout
                )
            }
        };

        match tokio::time::timeout_at(deadline, self.wait_export(&job)).await {
            Ok(result) => result,
            Err(_) => {
                // A transfer left running past its deadline keeps copying in the
                // background; stop it so the failure is final.
                if let Err(err) = self.transfer.cancel(&job.job_id, Some(job.run_id)).await {
                    warn!(
                        "cancel expired export transfer {} failed: {}",
                        job.job_id, err
                    );
                }
                err_box!(
                    "export of {} timed out after {:?} (transfer {})",
                    path.full_path(),
                    self.copy_timeout,
                    job.job_id
                )
            }
        }
    }

    /// Submits the export of `path` and returns the transfer to wait on.
    ///
    /// The request id is derived from `(source, target)` alone, so a journal
    /// replay retry lands on the job of the attempt it is retrying. That dedup is
    /// permanent, so a *terminal* job is what a later export of the same path
    /// finds, and `retry` is the transfer equivalent of what the load-job runner
    /// did with a terminal job context: forget it and dispatch again.
    ///
    /// `Completed` is retried too, unlike a load. A path rewritten in Curvine has
    /// a new `CompleteFile` entry but the same `(source, target)`, so adopting a
    /// completed job would drop the new content instead of writing it back.
    async fn dispatch_export(&self, path: &Path, mnt: &MountValue) -> FsResult<ExportJob> {
        let submitted = self
            .transfer
            .submit(self.export_command(path, mnt)?)
            .await?;
        let job = ExportJob {
            job_id: submitted.job_id,
            run_id: submitted.run_id,
            state: TransferState::from(submitted.state),
        };

        if !job.state.is_terminal() {
            return Ok(job);
        }

        debug!(
            "export transfer {} for {} is {:?}, dispatching a fresh attempt",
            job.job_id,
            path.full_path(),
            job.state
        );
        let retried = self.transfer.retry(&job.job_id).await?;
        Ok(ExportJob {
            job_id: retried.job_id,
            run_id: retried.run_id,
            state: TransferState::from(retried.state),
        })
    }

    fn export_command(&self, path: &Path, mnt: &MountValue) -> FsResult<TransferCommand> {
        let source = path.clone_uri();
        let target = mnt.info.get_ufs_path(path)?.clone_uri();
        let mut command = TransferCommand {
            kind: TransferKind::Export,
            client_request_id: export_request_id(&source, &target),
            source_path: source,
            target_path: target,
            submitter: EXPORT_SUBMITTER.to_string(),
            tenant: String::new(),
            options: Default::default(),
        };
        // The legacy write-back always wrote the UFS target: `check_already_loaded`
        // never skipped a CV source, and the export task created the UFS object
        // with overwrite=true. `overwrite=false` is *not* a "skip when already
        // done" here -- the transfer worker rejects any existing target before
        // copying -- and would fail the common case of re-exporting an updated
        // file, pinning the replay watermark for good.
        command.set_overwrite(true);
        Ok(command)
    }

    /// Polls the export until it is terminal. Returns `Ok` on `Completed` and on
    /// a source that disappeared between apply and replay.
    async fn wait_export(&self, job: &ExportJob) -> FsResult<()> {
        let mut state = job.state;
        let mut message = String::new();

        // `submit`/`retry` report the state but not the progress message, so a
        // terminal state can arrive here with no detail at all. Fetch the
        // status once so the first iteration classifies it exactly like a later
        // poll would: a vanished source still skips, and a failure carries its
        // reason instead of an empty suffix.
        if state.is_terminal() {
            let status = self.transfer.status(&job.job_id).await?;
            state = TransferState::from(status.state);
            message = status.progress.message;
        }

        loop {
            // A demoted leader must not keep waiting on (or driving) an export:
            // the new leader replays the same entry and submits its own
            // idempotent copy. Return an error the caller maps to `Step::Paused`
            // so the watermark stays put. The transfer itself is deliberately
            // left running -- cancelling it could race the new leader's submit.
            if !self.is_leader.load(Ordering::Acquire) {
                return err_box!(
                    "leadership lost while waiting for export transfer {}; leaving the entry to the next leader",
                    job.job_id
                );
            }

            match state {
                TransferState::Completed => return Ok(()),
                TransferState::Failed => {
                    // A vanished source is the replay's expected no-op: the file
                    // was deleted (or renamed) after the entry was applied, so
                    // there is nothing left to copy back. The load-job runner
                    // surfaced this as `FsError::FileNotFound` from dispatch.
                    if message.contains(SOURCE_NOT_FOUND_MARKER) {
                        info!(
                            "source of export transfer {} is gone, skipping: {}",
                            job.job_id, message
                        );
                        return Ok(());
                    }
                    return err_box!("export transfer {} failed: {}", job.job_id, message);
                }
                TransferState::Canceled => {
                    return err_box!("export transfer {} was canceled: {}", job.job_id, message)
                }
                TransferState::PartialSuccess => {
                    return err_box!(
                        "export transfer {} partially succeeded: {}",
                        job.job_id,
                        message
                    )
                }
                TransferState::Pending
                | TransferState::Planning
                | TransferState::Dispatching
                | TransferState::Running
                | TransferState::Canceling => {}
            }

            tokio::time::sleep(EXPORT_POLL_INTERVAL).await;
            let status = self.transfer.status(&job.job_id).await?;
            state = TransferState::from(status.state);
            message = status.progress.message;
        }
    }

    pub async fn apply_entry(&self, entry: &JournalEntry) -> CommonResult<()> {
        match entry {
            JournalEntry::Mkdir(e) => self.mkdir(e).await,
            JournalEntry::CompleteFile(e) => self.complete_file(e).await,
            JournalEntry::Rename(e) => self.rename(e).await,
            JournalEntry::Delete(e) => self.delete(e).await,
            JournalEntry::ReleaseWriter(e) => self.release_writer(e).await,
            _ => Ok(()),
        }
    }

    fn dir_still_at(&self, dir: &InodeDir, path: &Path) -> CommonResult<bool> {
        match self.fs_dir.read().get_inode_path(dir.id) {
            Ok(current) => Ok(current == path.full_path()),
            Err(FsError::FileNotFound(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn mkdir(&self, e: &MkdirEntry) -> CommonResult<()> {
        let path = Path::from_str(&e.path)?;
        let Some((_, mnt)) = self.get_mnt(&path)? else {
            return Ok(());
        };

        // A retried batch replays this entry after the rename that moved the
        // inode, so the stale path must not be recreated on UFS.
        if !self.dir_still_at(&e.dir, &path)? {
            debug!(
                "mkdir: inode {} is no longer at {}, skipping the UFS create",
                e.dir.id, e.path
            );
            return Ok(());
        }

        self.mkdir_with_parent_retry(&path, &mnt).await?;
        Ok(())
    }

    pub async fn complete_file(&self, e: &CompleteFileEntry) -> CommonResult<()> {
        if !e.file.is_complete() || e.file.ufs_only() {
            return Ok(());
        }

        let entry_path = Path::from_str(&e.path)?;
        let Some((_, mnt)) = self.get_mnt(&entry_path)? else {
            return Ok(());
        };

        // Rebuild via inode id so rename-after-open still exports to the live path.
        // Only fall back to e.path on typed not-found; store / corruption errors propagate.
        let path = match self.get_real_path(e.file.id) {
            Ok(path) => path,
            Err(FsError::FileNotFound(_)) => {
                warn!(
                    "complete_file: inode {} not found, fallback to entry.path={}",
                    e.file.id, e.path
                );
                entry_path
            }
            Err(err) => return Err(err.into()),
        };
        self.submit_export_task(&path, &mnt).await?;
        Ok(())
    }

    pub async fn rename(&self, e: &RenameEntry) -> CommonResult<()> {
        let src = Path::from_str(&e.src)?;
        let dst = Path::from_str(&e.dst)?;
        if let Some((src_ufs_path, mnt)) = self.get_mnt(&src)? {
            let ufs = mnt.ufs()?;
            if !ufs.exists(&src_ufs_path).await? {
                warn!(
                    "rename: src file not exists: {}, exporting dst {}",
                    src_ufs_path,
                    dst.full_path()
                );
                if let Some((_, dst_mnt)) = self.get_mnt(&dst)? {
                    // Keep journal apply aligned with fs_mode's durable export contract.
                    self.submit_export_task(&dst, &dst_mnt).await?;
                }
                return Ok(());
            }

            let src_dst_path = mnt.info.get_ufs_path(&dst)?;
            if ufs.fs_kind().support_rename() {
                match ufs.rename(&src_ufs_path, &src_dst_path).await {
                    Ok(_) => {}
                    Err(e @ FsError::FileNotFound(_)) => {
                        if !ufs.exists(&src_ufs_path).await? {
                            return Err(e.into());
                        }
                        self.mkdir_parent(&dst, &mnt).await?;
                        ufs.rename(&src_ufs_path, &src_dst_path).await?;
                    }
                    Err(e) => return Err(e.into()),
                }
                Ok(())
            } else {
                ufs.delete(&src_ufs_path, true).await?;
                self.submit_export_task(&dst, &mnt).await?;
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    /// Export a file that became complete because its last writer was released.
    ///
    /// The metadata side of the entry is already applied on the leader, so the
    /// stored inode tells us whether any writer remains. Reading it under the
    /// guard and exporting outside it keeps the export from stalling metadata
    /// writes.
    pub async fn release_writer(&self, e: &ReleaseWriterEntry) -> CommonResult<()> {
        let exportable = {
            let fs_dir = self.fs_dir.read();
            match fs_dir.store.get_inode(e.ino, None)? {
                Some(inode) => match inode.as_file_ref() {
                    Ok(file) => file.is_complete() && !file.ufs_only(),
                    Err(_) => false,
                },
                None => false,
            }
        };
        if !exportable {
            return Ok(());
        }

        let path = match self.get_real_path(e.ino) {
            Ok(path) => path,
            Err(FsError::FileNotFound(_)) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let Some((_, mnt)) = self.get_mnt(&path)? else {
            return Ok(());
        };
        self.submit_export_task(&path, &mnt).await?;
        Ok(())
    }

    pub async fn delete(&self, e: &DeleteEntry) -> CommonResult<()> {
        let path = Path::from_str(&e.path)?;
        let Some((ufs_path, mnt)) = self.get_mnt(&path)? else {
            return Ok(());
        };
        // `delete` stats the path itself, so an `exists` pre-check only adds
        // round trips and turns a racing delete into a retried entry. A missing
        // object is the expected steady state here, not an error.
        match mnt.ufs()?.delete(&ufs_path, true).await {
            Ok(_) => Ok(()),
            Err(FsError::FileNotFound(_)) => {
                debug!("delete: {} not on UFS, nothing to remove", ufs_path);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Request id of the write-back transfer for `(source, target)`.
///
/// Derived from the paths alone -- no entry index, time or attempt counter -- so
/// every replay of the same entry converges on one transfer job instead of
/// stacking copies of the same file. The kind is part of the id through
/// `TransferCommand::default_client_request_id`, which the unified filesystem
/// uses for its own exports, so the two never share a job.
fn export_request_id(source: &str, target: &str) -> String {
    TransferCommand::default_client_request_id(TransferKind::Export, source, target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_request_id_is_deterministic_for_the_same_paths() {
        let first = export_request_id("/mnt/db/t1/log", "s3://bucket/db/t1/log");
        let second = export_request_id("/mnt/db/t1/log", "s3://bucket/db/t1/log");

        assert_eq!(first, second);
        assert_eq!(
            first,
            TransferCommand::default_client_request_id(
                TransferKind::Export,
                "/mnt/db/t1/log",
                "s3://bucket/db/t1/log"
            )
        );
        assert!(first.starts_with("job_"));
    }

    #[test]
    fn export_request_id_changes_with_either_path_or_the_kind() {
        let base = export_request_id("/mnt/db/t1/log", "s3://bucket/db/t1/log");

        assert_ne!(
            base,
            export_request_id("/mnt/db/t1/other", "s3://bucket/db/t1/log")
        );
        assert_ne!(
            base,
            export_request_id("/mnt/db/t1/log", "s3://bucket/db/t1/other")
        );
        assert_ne!(
            base,
            TransferCommand::default_client_request_id(
                TransferKind::Load,
                "/mnt/db/t1/log",
                "s3://bucket/db/t1/log"
            )
        );
    }
}
