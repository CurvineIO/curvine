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

use crate::master::journal::{
    CompleteFileEntry, DeleteEntry, JournalBatch, JournalEntry, JournalWriter, MkdirEntry,
    ReleaseWriterEntry, RenameEntry,
};
use crate::master::{JobManager, Master, MasterMetrics, SyncFsDir};
use curvine_config::JournalConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{JobTaskState, LoadJobCommand};
use curvine_raft::proto::raft::{AppliedIndex, FsmState};
use curvine_raft::raft::storage::{LogStorage, RocksLogStorage};
use curvine_runtime::common::DurationUnit;
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

#[derive(Clone)]
pub struct UfsLoader {
    job_manager: Arc<JobManager>,
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

impl UfsLoader {
    pub(crate) fn new(
        job_manager: Arc<JobManager>,
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
            job_manager,
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
            match self.replay_entry(entry).await {
                Ok(reached) => {
                    self.clear_stall();
                    return Step::Applied(reached);
                }
                Err(e) => {
                    // A demoted node must not keep retrying a shared UFS write:
                    // the new leader owns that side effect now.
                    if !self.is_leader.load(Ordering::Acquire) {
                        return Step::Paused;
                    }
                    self.note_replay_failure();

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

    fn get_mnt(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        match self.job_manager.get_mnt(path)? {
            Some((path, mnt)) if mnt.info.is_fs_mode() => Ok(Some((path, mnt))),

            _ => Ok(None),
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

    pub async fn submit_export_task(&self, path: &Path, mnt: &MountValue) -> FsResult<()> {
        let command = LoadJobCommand::builder(path.clone_uri()).build();
        let runner = self.job_manager.create_runner();
        let res = match runner.submit_export_task(command, mnt.info.clone()).await {
            Ok(res) => res,
            Err(e) => {
                return if matches!(e, FsError::FileNotFound(_)) {
                    info!("file {} not found, skipping export job", path.full_path());
                    Ok(())
                } else {
                    err_box!("export job failed: {}", e)
                };
            }
        };

        if matches!(res.state, JobTaskState::Completed) {
            return Ok(());
        }

        let status = self
            .job_manager
            .wait_job_complete(res.job_id, self.copy_timeout)
            .await?;
        if matches!(status.state, JobTaskState::Completed) {
            Ok(())
        } else {
            err_box!(
                "export job failed: {} {}",
                status.state,
                status.progress.message
            )
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

    pub async fn mkdir(&self, e: &MkdirEntry) -> CommonResult<()> {
        let path = Path::from_str(&e.path)?;
        if let Some((_, mnt)) = self.get_mnt(&path)? {
            self.mkdir_with_parent_retry(&path, &mnt).await?;
            Ok(())
        } else {
            Ok(())
        }
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
