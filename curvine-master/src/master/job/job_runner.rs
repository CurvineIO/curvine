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

use crate::common::UfsFactory;
use crate::master::fs::MasterFilesystem;
use crate::master::{JobContext, JobStore, TaskDetail};
use curvine_config::ClientConf;
use curvine_config::LoadTransferDefaults;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{
    FileStatus, JobTaskState, JobTaskType, LoadJobCommand, LoadJobInfo, LoadJobResult,
    LoadTaskInfo, MountInfo, WorkerAddress,
};
use curvine_runtime::common::CommonUtils;
use curvine_runtime::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use curvine_runtime::sync::AtomicCounter;
use curvine_unified_fs::MountValue;
use dashmap::mapref::entry::Entry;
use futures::future;
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

/// Directory LISTs a load plan keeps in flight while walking the source tree.
/// The walk is latency bound (one network round trip per directory), so a
/// handful of parallel listings is what keeps planning from becoming the limit
/// of a multi-terabyte load; it is not a throughput knob for the copy itself.
const PLAN_WALK_CONCURRENCY: usize = 32;
use std::sync::Arc;
use std::time::Duration;

/// Maximum number of load-task submissions kept in flight per worker.
///
/// The worker rejects a task once its own queue is full, so the work in flight
/// per worker is what bounds how many submissions can sit on backpressure at
/// once. Keeping the window per worker stops one slow (or full) worker from
/// holding the slots other workers could use.
const SUBMIT_CONCURRENCY_PER_WORKER: usize = 64;

/// Backoff applied to a task the worker rejected because its queue is full.
///
/// It doubles per attempt up to the cap, so a worker that stays full is polled
/// a bounded number of times instead of once per queued task. The refill rate a
/// full worker sees is `window / backoff`, which with the caps below stays well
/// above the per-worker task completion rate even for small objects.
const QUEUE_FULL_BACKOFF: Backoff = Backoff {
    initial: Duration::from_millis(100),
    max: Duration::from_secs(1),
};

/// Retry schedule for retryable submissions.
#[derive(Clone, Copy)]
struct Backoff {
    initial: Duration,
    max: Duration,
}

/// What a worker answered for one submission.
enum SubmitOutcome {
    /// The worker holds the task.
    Accepted,
    /// The worker's queue is full; the same task is welcome once it drains.
    QueueFull,
    /// The worker refused the task; retrying cannot help.
    Rejected(String),
    /// The job stopped running while dispatch was waiting; drop the task.
    Abandoned,
}

/// Submit until the worker accepts the task, pacing on its backpressure signal.
///
/// Returns how many times dispatch had to wait, so the caller can report pacing
/// per worker instead of logging one line per queued task. A queue-full answer
/// is not an error, but it is not a promise either: a worker whose tasks never
/// finish keeps answering it forever, so the wait is capped twice over -- by
/// `timeout` for this submission, and by `hard_deadline` when the caller has an
/// overall budget (an export that must finish inside `ufs_copy_timeout`).
async fn submit_until_accepted<F, Fut>(
    backoff: Backoff,
    timeout: Duration,
    hard_deadline: Option<tokio::time::Instant>,
    mut submit: F,
) -> FsResult<u64>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = FsResult<SubmitOutcome>>,
{
    let mut wait = backoff.initial;
    let mut waits = 0;
    let started = std::time::Instant::now();

    loop {
        // Checked before every attempt, not only after a queue-full answer, so a
        // caller with a budget does not spend one more RPC past its deadline.
        if let Some(hard) = hard_deadline {
            if tokio::time::Instant::now() >= hard {
                return Err(FsError::common(
                    "deadline exceeded while waiting for worker admission",
                ));
            }
        }

        match submit().await? {
            SubmitOutcome::Accepted | SubmitOutcome::Abandoned => return Ok(waits),
            SubmitOutcome::Rejected(reason) => return Err(FsError::common(reason)),
            SubmitOutcome::QueueFull => {
                if started.elapsed() >= timeout {
                    return Err(FsError::common(format!(
                        "worker queue stayed full for {}s, giving up on this submission",
                        started.elapsed().as_secs()
                    )));
                }

                // A caller-supplied deadline (an export with a copy budget) must
                // not be outlived by the queue-full wait.
                let mut sleep_for = wait;
                if let Some(hard) = hard_deadline {
                    let now = tokio::time::Instant::now();
                    if now >= hard {
                        return Err(FsError::common(
                            "deadline exceeded while waiting for worker admission",
                        ));
                    }
                    sleep_for = sleep_for.min(hard - now);
                }

                waits += 1;
                tokio::time::sleep(sleep_for).await;
                wait = (wait * 2).min(backoff.max);
            }
        }
    }
}

/// Run `submit` over `items` with at most `limit` futures in flight.
///
/// A plain `try_join_all` over one future per item keeps the whole set live and
/// makes every poll O(n); bounding the window keeps memory flat and the poll
/// cost proportional to `limit` instead of the number of tasks.
async fn submit_with_bound<I, T, F, Fut>(items: I, limit: usize, submit: F) -> FsResult<()>
where
    I: IntoIterator<Item = T>,
    F: FnMut(T) -> Fut,
    Fut: std::future::Future<Output = FsResult<()>>,
{
    stream::iter(items)
        .map(submit)
        .buffer_unordered(limit)
        .try_collect::<()>()
        .await
}

pub struct LoadJobRunner {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
    transfer_defaults: LoadTransferDefaults,
    /// Ceiling on how long one submission waits for a worker queue to drain.
    queue_full_timeout: Duration,
}

#[derive(Clone)]
pub(crate) struct QueuedLoadJob {
    pub(crate) job_id: String,
    pub(crate) run_id: u64,
}

struct PlannedLoadJob {
    tasks: FastHashMap<String, TaskDetail>,
    total_size: i64,
}

#[derive(Clone, Copy)]
enum CvSourceMode {
    LoadUfsOnlyFromUfs,
    ExportCurvineToUfs,
}

/// What a walk cost, so the shape of the traversal is observable instead of
/// only measurable: a linear walk pops every entry off the queue exactly once,
/// so `entries_popped == listings_started + files`. A traversal that re-scans its
/// queue (the O(dirs^2) version this replaced) pops the same entry once per
/// pass and shows up here immediately.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalkStats {
    pub listings_started: usize,
    pub entries_popped: usize,
}

/// Walks a source tree and hands every file entry to `on_file`.
///
/// Listing one directory is a network round trip, and a wide tree has hundreds of
/// thousands of them, so at most `concurrency` listings are in flight and a slot
/// is refilled as soon as one finishes: a batch barrier would idle the other slots
/// behind the slowest directory. A listing failure ends the walk immediately
/// instead of waiting for its slower siblings; the dropped futures cancel their
/// requests.
///
/// `on_file` runs between awaits, so it can plan the file (and enforce a file
/// limit) without buffering the tree.
async fn walk_files<L, Fut, F>(
    roots: Vec<FileStatus>,
    concurrency: usize,
    list: L,
    mut on_file: F,
) -> FsResult<WalkStats>
where
    L: Fn(Path) -> Fut,
    Fut: Future<Output = FsResult<Vec<FileStatus>>>,
    F: FnMut(FileStatus) -> FsResult<()>,
{
    let concurrency = concurrency.max(1);
    let mut queue: VecDeque<FileStatus> = roots.into();
    let mut in_flight = FuturesUnordered::new();
    let mut stats = WalkStats::default();

    loop {
        // Take entries from the front only while a listing slot exists: a
        // directory we cannot start yet stays at the head of the queue (nothing
        // is ever put back), so a wide tree costs one pass, not one pass per
        // completed listing. Files are planned immediately, which also keeps a
        // listing result from piling up in memory.
        while in_flight.len() < concurrency {
            let Some(is_dir) = queue.front().map(|status| status.is_dir) else {
                break;
            };
            let Some(status) = queue.pop_front() else {
                break;
            };
            stats.entries_popped += 1;
            if !is_dir {
                on_file(status)?;
                continue;
            }
            let dir_path = Path::from_str(status.path)?;
            stats.listings_started += 1;
            in_flight.push(list(dir_path));
        }

        if in_flight.is_empty() {
            return Ok(stats);
        }

        if let Some(children) = in_flight.next().await {
            queue.extend(children?);
        }
    }
}

impl LoadJobRunner {
    const RUN_ID_SEQ_MOD: u64 = 1_000_000;

    pub fn new(
        jobs: JobStore,
        master_fs: MasterFilesystem,
        factory: Arc<UfsFactory>,
        job_max_files: usize,
        run_seq: Arc<AtomicCounter>,
        transfer_defaults: LoadTransferDefaults,
        queue_full_timeout: Duration,
    ) -> Self {
        Self {
            jobs,
            master_fs,
            factory,
            job_max_files,
            run_seq,
            transfer_defaults,
            queue_full_timeout,
        }
    }

    pub fn choose_worker(&self, _block_size: i64) -> FsResult<WorkerAddress> {
        let worker_mgr = self.master_fs.worker_manager.read();
        let workers = worker_mgr.choose_workers(1, vec![])?;
        if let Some(worker) = workers.first() {
            Ok(worker.clone())
        } else {
            err_box!("No available worker found")
        }
    }

    fn build_job_context(
        &self,
        command: &LoadJobCommand,
        mnt: &MountInfo,
        cv_source_mode: CvSourceMode,
    ) -> FsResult<(JobContext, Path, Path)> {
        let command_source = Path::from_str(&command.source_path)?;
        let command_target = match command.target_path.as_ref() {
            Some(target_path) => Some(Path::from_str(target_path)?),
            None => None,
        };

        let (source_path, target_path) =
            match (cv_source_mode, command_source.is_cv(), command_target) {
                (CvSourceMode::LoadUfsOnlyFromUfs, false, Some(target_path)) => {
                    if !target_path.is_cv() {
                        return err_box!(
                            "load job target path {} must be a CV path",
                            target_path.full_path()
                        );
                    }

                    let mounted_target = mnt.get_cv_path(&command_source)?;
                    if mounted_target != target_path {
                        let requested = target_path.full_path();
                        let mounted = mounted_target.full_path();
                        let source = command_source.full_path();
                        return err_box!(
                        "load job target path {} does not match mounted CV path {} for source {}",
                        requested,
                        mounted,
                        source
                    );
                    }

                    (command_source, target_path)
                }
                (CvSourceMode::LoadUfsOnlyFromUfs, false, None) => {
                    let target_path = mnt.get_cv_path(&command_source)?;
                    (command_source, target_path)
                }
                (CvSourceMode::LoadUfsOnlyFromUfs, true, Some(_)) => {
                    let source = command_source.full_path();
                    return err_box!(
                    "load job with explicit target requires a UFS source path, got CV source {}",
                    source
                );
                }
                (CvSourceMode::LoadUfsOnlyFromUfs, true, None) => {
                    let status = self.master_fs.file_status(command_source.path())?;
                    if !status.is_dir && !status.storage_policy.ufs_exists() {
                        return err_box!(
                            "load job for CV path {} requires UFS-backed metadata",
                            command_source.full_path()
                        );
                    }
                    (mnt.get_ufs_path(&command_source)?, command_source)
                }
                (CvSourceMode::ExportCurvineToUfs, true, Some(_)) => {
                    return err_box!(
                        "export job source path {} does not accept an explicit target path",
                        command_source.full_path()
                    );
                }
                (CvSourceMode::ExportCurvineToUfs, true, None) => {
                    let target_path = mnt.get_ufs_path(&command_source)?;
                    (command_source, target_path)
                }
                (CvSourceMode::ExportCurvineToUfs, false, _) => {
                    return err_box!(
                        "export job source path {} must be a CV path",
                        command_source.full_path()
                    );
                }
            };
        let job_id = CommonUtils::create_job_id(source_path.full_path());
        let run_id = self.next_run_id();
        let job_context = JobContext::with_conf(
            command,
            job_id,
            source_path.clone_uri(),
            target_path.clone_uri(),
            mnt,
            &ClientConf::default(),
            run_id,
            match cv_source_mode {
                CvSourceMode::LoadUfsOnlyFromUfs => JobTaskType::Load,
                CvSourceMode::ExportCurvineToUfs => JobTaskType::Export,
            },
        );

        Ok((job_context, source_path, target_path))
    }

    fn next_run_id(&self) -> u64 {
        LocalTime::mills()
            .saturating_mul(Self::RUN_ID_SEQ_MOD)
            .saturating_add(self.run_seq.next() % Self::RUN_ID_SEQ_MOD)
    }

    fn running_job_result(&self, job_id: &str) -> Option<LoadJobResult> {
        self.jobs.get(job_id).and_then(|exist_job| {
            let state: JobTaskState = exist_job.state.state();
            if state.is_running() {
                Some(LoadJobResult::with_state(&exist_job.info, state))
            } else {
                None
            }
        })
    }

    async fn check_already_loaded(
        &self,
        mnt: &MountValue,
        source_path: &Path,
        target_path: &Path,
    ) -> FsResult<bool> {
        // Data-state fast-path. Applies whether the slot was vacant or held
        // a terminal ctx — e.g. after JobCleanupTask, after master restart +
        // journal replay, or after an earlier run completed the sync.
        //
        // Only UFS→CV imports can be fast-skipped here; CV sources always
        // need an explicit export task.
        if source_path.is_cv() {
            return Ok(false);
        }

        // Target not present in Curvine yet — must load.
        let cv_status = match self.master_fs.file_status(target_path.path()) {
            Ok(cv_status) => cv_status,
            Err(FsError::FileNotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };

        // Cached target exists but its own metadata says it isn't usable — must reload.
        if !cv_status.cv_valid(None) {
            return Ok(false);
        }

        // Target looks valid locally; confirm against UFS source before skipping.
        let source_status = mnt.ufs()?.get_status(source_path).await?;
        Ok(cv_status.cv_valid(Some(&source_status)))
    }

    pub(crate) fn enqueue_load_job(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
    ) -> FsResult<(LoadJobResult, Option<QueuedLoadJob>)> {
        let (job_context, source_path, target_path) =
            self.build_job_context(&command, &mnt, CvSourceMode::LoadUfsOnlyFromUfs)?;
        let job_id = job_context.info.job_id.clone();
        let run_id = job_context.run_id;

        match self.jobs.entry(job_id.clone()) {
            Entry::Occupied(mut e) => {
                let state: JobTaskState = e.get().state.state();
                if state.is_running() {
                    let existing = e.get();
                    debug!(
                        "job {} already running during enqueue (state={:?})",
                        job_id, state
                    );
                    return Ok((LoadJobResult::with_state(&existing.info, state), None));
                }
                info!(
                    "job {} previous run in terminal state {:?}, replacing",
                    job_id, state
                );
                e.insert(job_context);
            }

            Entry::Vacant(e) => {
                e.insert(job_context);
            }
        }

        debug!(
            "queued load job {}: {} -> {}",
            job_id,
            source_path.full_path(),
            target_path.full_path()
        );

        let job = match self.jobs.get(&job_id) {
            Some(job) => job,
            None => return err_box!("queued job {} missing after insert", job_id),
        };
        Ok((
            LoadJobResult::with_job(&job.info),
            Some(QueuedLoadJob { job_id, run_id }),
        ))
    }

    /// Submits a load job for the given source path (and mount).
    ///
    /// **Concurrency:** The job id is derived from the source path. If two clients
    /// submit for the same path while a job is already **running**, the call
    /// returns success with the **in-flight** job’s state and `LoadJobResult` built
    /// from that job’s `LoadJobInfo`; the later request’s `LoadJobCommand` options
    /// (replicas, overwrite, etc.) are **not** applied. **First submitter wins** for
    /// that path. Use `JobManager::get_job_status` to inspect the job that is
    /// actually running (including its resolved options).
    pub async fn submit_load_task(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
    ) -> FsResult<LoadJobResult> {
        self.submit_direct_task(command, mnt, CvSourceMode::LoadUfsOnlyFromUfs, "load", None)
            .await
    }

    /// Submits a durable export job used by fs_mode journal replay.
    ///
    /// CV source paths stay CV sources here. This preserves the journal contract:
    /// committed Curvine data is copied back to the mounted UFS.
    pub async fn submit_export_task(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
    ) -> FsResult<LoadJobResult> {
        self.submit_direct_task(
            command,
            mnt,
            CvSourceMode::ExportCurvineToUfs,
            "export",
            None,
        )
        .await
    }

    /// Export bounded by one absolute deadline.
    ///
    /// The deadline covers dispatch as well as completion: a caller that must not
    /// park longer than `deadline` cannot rely on the completion wait alone,
    /// because dispatching to a busy cluster waits for worker admission.
    pub async fn submit_export_task_within(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
        deadline: tokio::time::Instant,
    ) -> FsResult<LoadJobResult> {
        self.submit_direct_task(
            command,
            mnt,
            CvSourceMode::ExportCurvineToUfs,
            "export",
            Some(deadline),
        )
        .await
    }

    async fn submit_direct_task(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
        cv_source_mode: CvSourceMode,
        job_kind: &str,
        deadline: Option<tokio::time::Instant>,
    ) -> FsResult<LoadJobResult> {
        let (mut job_context, source_path, target_path) =
            self.build_job_context(&command, &mnt, cv_source_mode)?;
        let job_id = job_context.info.job_id.clone();
        let run_id = job_context.run_id;

        if let Some(res) = self.running_job_result(&job_id) {
            return Ok(res);
        }

        let mnt_value = self.factory.get_mnt(&mnt)?;
        if self
            .check_already_loaded(&mnt_value, &source_path, &target_path)
            .await?
        {
            info!(
                "skip {} job {}: source_path {} already loaded or in progress",
                job_kind,
                job_id,
                source_path.full_path()
            );
            return Ok(LoadJobResult::with_state(
                &job_context.info,
                JobTaskState::Completed,
            ));
        }

        debug!(
            "submitting {} job {}: {} -> {}",
            job_kind,
            job_id,
            source_path.full_path(),
            target_path.full_path()
        );

        let planned = self
            .create_all_tasks(&job_context.info, &source_path, &mnt, run_id)
            .await?;
        let total_size = planned.total_size;
        if planned.tasks.is_empty() {
            info!(
                "{} job {} has no tasks: {} -> {}",
                job_kind,
                job_id,
                source_path.full_path(),
                target_path.full_path()
            );
            return Ok(LoadJobResult::with_state(
                &job_context.info,
                JobTaskState::Completed,
            ));
        }
        let tasks = planned.tasks.clone();
        for (task_id, detail) in planned.tasks.take() {
            job_context.add_task_detail(task_id, detail);
        }

        info!(
            "{} job {} submitted: {} -> {}, tasks {}, total_size {}",
            job_kind,
            job_id,
            source_path.full_path(),
            target_path.full_path(),
            job_context.tasks.len(),
            ByteUnit::byte_to_string(total_size as u64)
        );

        let res = LoadJobResult::with_job(&job_context.info);

        // Install / replace the ctx into the store atomically. We branch into:
        //   - Vacant: first submitter, install and dispatch.
        //   - Occupied + running: another submitter won the race; return that job’s
        //     state (this request's command options are not applied).
        //   - Occupied + terminal: previous run finished/failed/canceled and
        //     hasn't been cleaned up yet. Replace with the new ctx and dispatch.
        match self.jobs.entry(job_id.clone()) {
            Entry::Occupied(mut e) => {
                let state: JobTaskState = e.get().state.state();
                if state.is_running() {
                    let existing = e.get();
                    debug!(
                        "job {} race-lost on entry: another submitter is dispatching (state={:?})",
                        job_id, state
                    );
                    return Ok(LoadJobResult::with_state(&existing.info, state));
                }
                info!(
                    "job {} previous run in terminal state {:?}, replacing",
                    job_id, state
                );
                e.insert(job_context);
            }

            Entry::Vacant(e) => {
                e.insert(job_context);
            }
        }

        if let Err(err) = self
            .submit_all_task(tasks, Some((&job_id, None)), deadline)
            .await
        {
            warn!("dispatch {} job {} failed: {}", job_kind, job_id, err);
            // @todo Cancel sub-tasks that may have already been dispatched.
            if let Err(update_err) = self.jobs.update_state(
                &job_id,
                JobTaskState::Failed,
                format!("dispatch failed: {}", err),
            ) {
                error!(
                    "failed to mark {} job {} as failed after dispatch error: {}",
                    job_kind, job_id, update_err
                );
            }
            return Err(err);
        }

        Ok(res)
    }

    pub(crate) async fn run_queued_load_job(&self, queued: QueuedLoadJob) -> FsResult<()> {
        let job_info = match self.current_job_info(&queued) {
            Some(job_info) => job_info,
            None => return Ok(()),
        };

        let source_path = Path::from_str(&job_info.source_path)?;
        let target_path = Path::from_str(&job_info.target_path)?;
        let mnt = job_info.mount_info.clone();
        let mnt_value = match self.factory.get_mnt(&mnt) {
            Ok(mnt_value) => mnt_value,
            Err(err) => {
                self.jobs.update_state_if_run(
                    &queued.job_id,
                    queued.run_id,
                    JobTaskState::Failed,
                    format!("prepare UFS failed: {}", err),
                );
                return Err(err);
            }
        };

        let already_loaded = match self
            .check_already_loaded(&mnt_value, &source_path, &target_path)
            .await
        {
            Ok(already_loaded) => already_loaded,
            Err(err) => {
                self.jobs.update_state_if_run(
                    &queued.job_id,
                    queued.run_id,
                    JobTaskState::Failed,
                    format!("check cached file failed: {}", err),
                );
                return Err(err);
            }
        };
        if already_loaded {
            info!(
                "queued load job {} already loaded: {} -> {}",
                queued.job_id,
                source_path.full_path(),
                target_path.full_path()
            );
            self.jobs.update_state_if_run(
                &queued.job_id,
                queued.run_id,
                JobTaskState::Completed,
                "Already loaded",
            );
            return Ok(());
        }

        let planned = match self
            .create_all_tasks(&job_info, &source_path, &mnt, queued.run_id)
            .await
        {
            Ok(planned) => planned,
            Err(err) => {
                self.jobs.update_state_if_run(
                    &queued.job_id,
                    queued.run_id,
                    JobTaskState::Failed,
                    format!("plan load job failed: {}", err),
                );
                return Err(err);
            }
        };
        let total_size = planned.total_size;
        let task_count = planned.tasks.len();
        if task_count == 0 {
            info!(
                "queued load job {} has no tasks: {} -> {}",
                queued.job_id,
                source_path.full_path(),
                target_path.full_path()
            );
            self.jobs.update_state_if_run(
                &queued.job_id,
                queued.run_id,
                JobTaskState::Completed,
                "No load tasks to dispatch",
            );
            return Ok(());
        }
        let tasks = planned.tasks.clone();
        let assigned_workers = Self::assigned_workers(&tasks);

        if !self.install_plan_if_current(&queued, planned) {
            debug!(
                "skip dispatch for stale queued load job {} run {}",
                queued.job_id, queued.run_id
            );
            return Ok(());
        }

        info!(
            "queued load job {} planned: {} -> {}, tasks {}, total_size {}",
            queued.job_id,
            source_path.full_path(),
            target_path.full_path(),
            task_count,
            ByteUnit::byte_to_string(total_size as u64)
        );

        if !self.is_current_running(&queued) {
            return Ok(());
        }

        if let Err(err) = self
            .submit_all_task(tasks, Some((&queued.job_id, Some(queued.run_id))), None)
            .await
        {
            warn!("dispatch load job {} failed: {}", queued.job_id, err);
            let failed_current_run = self.jobs.update_state_if_run(
                &queued.job_id,
                queued.run_id,
                JobTaskState::Failed,
                format!("dispatch failed: {}", err),
            );
            if failed_current_run {
                self.cancel_workers_best_effort(&queued.job_id, &assigned_workers)
                    .await;
            }
            return Err(err);
        }

        Ok(())
    }

    fn assigned_workers(tasks: &FastHashMap<String, TaskDetail>) -> FastHashSet<WorkerAddress> {
        let mut workers = FastHashSet::default();
        for detail in tasks.values() {
            workers.insert(detail.task.worker.clone());
        }
        workers
    }

    fn current_job_info(&self, queued: &QueuedLoadJob) -> Option<LoadJobInfo> {
        self.jobs.get(&queued.job_id).and_then(|job| {
            let state: JobTaskState = job.state.state();
            if job.run_id == queued.run_id && state.is_running() {
                Some(job.info.clone())
            } else {
                None
            }
        })
    }

    fn is_current_running(&self, queued: &QueuedLoadJob) -> bool {
        self.current_job_info(queued).is_some()
    }

    fn install_plan_if_current(&self, queued: &QueuedLoadJob, planned: PlannedLoadJob) -> bool {
        let mut job = match self.jobs.get_mut(&queued.job_id) {
            Some(job) => job,
            None => return false,
        };

        let state: JobTaskState = job.state.state();
        if job.run_id != queued.run_id || !state.is_running() {
            return false;
        }

        for (task_id, detail) in planned.tasks.take() {
            job.add_task_detail(task_id, detail);
        }

        true
    }

    /// `active` is the `(job_id, run_id)` this submission belongs to; once the job
    /// leaves the running state (canceled / replaced / removed) no new task is
    /// admitted, so a cancellation cannot keep spawning copies.
    async fn submit_all_task(
        &self,
        tasks: FastHashMap<String, TaskDetail>,
        active: Option<(&str, Option<u64>)>,
        deadline: Option<tokio::time::Instant>,
    ) -> FsResult<()> {
        // Dispatch per worker. A worker accepts a bounded number of tasks and
        // then answers "queue full" until it works through them, so grouping by
        // worker lets one full worker pace itself without holding the slots the
        // other workers could use.
        let mut by_worker: FastHashMap<WorkerAddress, Vec<(String, TaskDetail)>> =
            FastHashMap::new();
        for (id, detail) in tasks.take() {
            by_worker
                .entry(detail.task.worker.clone())
                .or_default()
                .push((id, detail));
        }

        let workers = by_worker.len().max(1);
        info!(
            "dispatching load job tasks to {} workers with per-worker admission control",
            workers
        );

        stream::iter(by_worker.take())
            .map(|(worker, batch)| async move {
                self.dispatch_to_worker(worker, batch, active, deadline)
                    .await
            })
            .buffer_unordered(workers)
            .try_collect::<()>()
            .await
    }

    /// Hands `batch` to one worker, waiting out its queue-full backpressure.
    ///
    /// Building one future per file and joining them all at once makes every
    /// poll O(n): for a job with millions of files that pins a whole core in
    /// `FuturesUnordered::poll_next`, so submissions are windowed instead.
    async fn dispatch_to_worker(
        &self,
        worker: WorkerAddress,
        batch: Vec<(String, TaskDetail)>,
        active: Option<(&str, Option<u64>)>,
        deadline: Option<tokio::time::Instant>,
    ) -> FsResult<()> {
        let client = self.factory.get_worker_client(&worker).await?;
        let total = batch.len();
        let paced = Arc::new(AtomicU64::new(0));
        let window = SUBMIT_CONCURRENCY_PER_WORKER.min(total.max(1));

        submit_with_bound(batch, window, |(id, task): (String, TaskDetail)| {
            let client = client.clone();
            let worker = worker.clone();
            let paced = paced.clone();
            async move {
                let task_info = task.task;
                let id: &str = id.as_str();
                let waits = submit_until_accepted(
                    QUEUE_FULL_BACKOFF,
                    self.queue_full_timeout,
                    deadline,
                    || {
                        let client = client.clone();
                        let task_info = task_info.clone();
                        let worker = worker.clone();
                        async move {
                            if let Some((job_id, run_id)) = active {
                                if !self.run_is_active(job_id, run_id) {
                                    debug!(
                                        "job {} is no longer running, skip submitting sub-task {}",
                                        job_id, id
                                    );
                                    return Ok(SubmitOutcome::Abandoned);
                                }
                            }

                            let response = client.submit_load_task_response(task_info).await?;
                            if response.accepted.unwrap_or(true) {
                                debug!("dispatched sub-task {} to worker {}", id, worker);
                                return Ok(SubmitOutcome::Accepted);
                            }

                            let reason = response.reject_reason.unwrap_or_default();
                            if response.retryable.unwrap_or(false) {
                                debug!(
                                    "worker {} has no queue room for sub-task {}: {}",
                                    worker, id, reason
                                );
                                return Ok(SubmitOutcome::QueueFull);
                            }

                            Ok(SubmitOutcome::Rejected(format!(
                                "Worker rejected load task {}: {}",
                                response.task_id,
                                if reason.is_empty() {
                                    "no reason provided".to_string()
                                } else {
                                    reason
                                }
                            )))
                        }
                    },
                )
                .await?;

                if waits > 0 {
                    paced.fetch_add(waits, Ordering::Relaxed);
                }

                Ok::<(), FsError>(())
            }
        })
        .await?;

        let paced = paced.load(Ordering::Relaxed);
        if paced > 0 {
            info!(
                "worker {}: dispatched {} sub-tasks after {} queue-full waits",
                worker, total, paced
            );
        } else {
            debug!("worker {}: dispatched {} sub-tasks", worker, total);
        }

        Ok(())
    }

    /// A run is still allowed to submit when the store holds it, it is running,
    /// and (when known) it is the run that asked for the submission.
    fn run_is_active(&self, job_id: &str, run_id: Option<u64>) -> bool {
        match self.jobs.get(job_id) {
            Some(job) => {
                let state: JobTaskState = job.state.state();
                state.is_running() && run_id.is_none_or(|run| job.run_id == run)
            }
            None => false,
        }
    }

    async fn create_all_tasks(
        &self,
        job: &LoadJobInfo,
        source_path: &Path,
        mnt: &MountInfo,
        run_id: u64,
    ) -> FsResult<PlannedLoadJob> {
        // Tasks carry a copy of the job info, and the worker builds its UFS from
        // that copy's mount properties, so inject the transfer defaults here.
        let mut job = job.clone();
        self.transfer_defaults.apply(&mut job.mount_info.properties);
        let job = &job;
        let source_status = if source_path.is_cv() {
            self.master_fs.file_status(source_path.path())?
        } else {
            let ufs = self.factory.get_ufs(mnt)?;
            ufs.get_status(source_path).await?
        };

        let block_size = job.block_size;

        let mut total_size = 0;
        let mut tasks = FastHashMap::new();
        let mut task_index = 0;

        // Get target base path for direction detection
        let target_base = Path::from_str(&job.target_path)?;

        // One listing is a network round trip and a wide tree has hundreds of
        // thousands of them, so the walk keeps `PLAN_WALK_CONCURRENCY` of them in
        // flight and refills a slot as soon as one finishes (a batch barrier would
        // idle the other slots behind the slowest directory).
        let walk = walk_files(
            vec![source_status],
            PLAN_WALK_CONCURRENCY,
            |dir_path| async move {
                if dir_path.is_cv() {
                    // Traverse Curvine directory
                    self.master_fs.list_status(dir_path.path())
                } else {
                    // Traverse UFS directory
                    let ufs = self.factory.get_ufs(mnt)?;
                    ufs.list_status(&dir_path).await
                }
            },
            |status| {
                let worker = self.choose_worker(block_size)?;

                let source_path = Path::from_str(status.path)?;

                // Calculate target_path based on source and target types
                let target_path = if source_path.is_cv() && !target_base.is_cv() {
                    // Export: Curvine → UFS
                    mnt.get_ufs_path(&source_path)?
                } else if !source_path.is_cv() && target_base.is_cv() {
                    // Import: UFS → Curvine
                    mnt.get_cv_path(&source_path)?
                } else {
                    // Same type (Curvine→Curvine or UFS→UFS), not supported yet
                    return err_box!(
                        "Unsupported path combination: source={}, target={}",
                        source_path.full_path(),
                        target_base.full_path()
                    );
                };

                let task_id = format!("{}_run_{}_task_{}", job.job_id, run_id, task_index);
                task_index += 1;
                total_size += status.len;

                let task = LoadTaskInfo {
                    job: job.clone(),
                    task_id: task_id.clone(),
                    worker: worker.clone(),
                    source_path: source_path.clone_uri(),
                    target_path: target_path.clone_uri(),
                    create_time: LocalTime::mills() as i64,
                    source_read_plan_json: String::new(),
                    transfer_report: None,
                };
                tasks.insert(task_id.clone(), TaskDetail::new(task));

                if tasks.len() > self.job_max_files {
                    return err_box!("Job {} files exceeds {}", job.job_id, self.job_max_files);
                }
                debug!(
                    "created sub-task {} ({} -> {})",
                    task_id,
                    source_path.full_path(),
                    target_path.full_path()
                );

                Ok(())
            },
        )
        .await?;

        debug!(
            "planned load job {}: {} directories listed, {} queue entries, {} files",
            job.job_id,
            walk.listings_started,
            walk.entries_popped,
            tasks.len()
        );
        Ok(PlannedLoadJob { tasks, total_size })
    }

    /// Send cancel to every assigned worker. The caller has already marked the
    /// job Canceled, and a repeated cancel is a no-op on a terminal job, so an
    /// unreachable worker must not stop the remaining workers from being told.
    pub async fn cancel_job(
        &self,
        job_id: impl AsRef<str>,
        assigned_workers: FastHashSet<WorkerAddress>,
    ) -> FsResult<()> {
        self.cancel_workers_best_effort(job_id.as_ref(), &assigned_workers)
            .await;
        Ok(())
    }

    async fn cancel_workers_best_effort(
        &self,
        job_id: &str,
        assigned_workers: &FastHashSet<WorkerAddress>,
    ) {
        // Dial workers concurrently: each unreachable worker can hold a full
        // connect-retry window, and serial dialing would stack those windows
        // past the caller's RPC timeout.
        let cancels = assigned_workers.iter().map(|worker| async move {
            let client = match self.factory.get_worker_client(worker).await {
                Ok(client) => client,
                Err(err) => {
                    error!(
                        "failed to create cancel client for worker {}: {}",
                        worker, err
                    );
                    return;
                }
            };

            if let Err(err) = client.cancel_job(job_id).await {
                error!(
                    "failed to send cancel request to worker {}: {}",
                    worker, err
                );
            }
        });
        future::join_all(cancels).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use crate::Master;
    use curvine_config::ClusterConf;
    use curvine_core_error::{CommonErrorExt, CommonResultExt};
    use curvine_fs_api::RpcCode;
    use curvine_net::net::{ConnState, NetUtils};
    use curvine_proto::{CancelJobRequest, CancelJobResponse};
    use curvine_rpc::handler::{HandlerService, MessageHandler};
    use curvine_rpc::message::{Builder, Message};
    use curvine_rpc::server::{RpcServer, ServerConf};
    use curvine_runtime::common::Utils;
    use curvine_runtime::runtime::{RpcRuntime, Runtime};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    fn worker(id: u32, port: u16) -> WorkerAddress {
        WorkerAddress {
            worker_id: id,
            hostname: "localhost".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: port as u32,
            web_port: 0,
        }
    }

    /// Worker stand-in that records the job id of every CancelJob it receives.
    struct CancelProbe {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl MessageHandler for CancelProbe {
        type Error = CommonErrorExt;

        fn handle(&self, msg: &Message) -> CommonResultExt<Message> {
            if RpcCode::from(msg.code()) == RpcCode::CancelJob {
                let req: CancelJobRequest = msg.parse_header()?;
                self.seen.lock().unwrap().push(req.job_id);
            }
            Ok(Builder::success_with_header(msg, CancelJobResponse {}).build())
        }
    }

    struct CancelProbeService {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl HandlerService for CancelProbeService {
        type Item = CancelProbe;

        fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
            CancelProbe {
                seen: self.seen.clone(),
            }
        }
    }

    /// Runner for cancel tests. `rpc_timeout_ms` bounds how long a worker that
    /// accepts but never answers can hold a cancel.
    fn test_runner(rpc_timeout_ms: u64) -> (LoadJobRunner, Arc<Runtime>) {
        Master::init_test_metrics();
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            ..Default::default()
        };
        conf.journal.enable = false;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("job-runner/meta-{}", Utils::rand_str(6)));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("job-runner/journal-{}", Utils::rand_str(6)));
        // Generous connect budget: a refused port fails at once anyway, and a
        // freshly started probe server can take well over 100ms to accept.
        conf.client.conn_retry_max_duration_ms = 2000;
        conf.client.conn_timeout_ms = 2000;
        conf.client.rpc_retry_max_duration_ms = 100;
        conf.client.rpc_timeout_ms = rpc_timeout_ms;
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();

        let rt = conf.journal.create_runtime();
        let runner = LoadJobRunner::new(
            JobStore::new(),
            fs,
            Arc::new(UfsFactory::with_rt(&conf.client, rt.clone())),
            1,
            Arc::new(AtomicCounter::new(0)),
            LoadTransferDefaults::from_job(&conf.job),
            Duration::from_secs(60),
        );
        (runner, rt)
    }

    /// One unreachable worker must not stop cancel from reaching the others,
    /// and must not fail a cancel the caller already recorded.
    #[test]
    fn cancel_job_reaches_live_workers_when_one_is_unreachable() {
        // Bind then drop: nothing listens on `dead_port`, so connect is refused.
        let dead_port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        // Real RPC server that answers CancelJob and records the job id.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let live_port = NetUtils::hold_available_port();
        let server = RpcServer::new(
            ServerConf::with_hostname("127.0.0.1", live_port),
            CancelProbeService { seen: seen.clone() },
        );
        let server_rt = server.clone_rt();
        let mut listener = server.start();
        server_rt
            .block_on(listener.wait_startup())
            .expect("cancel probe server must start");

        let (runner, rt) = test_runner(1000);

        let mut workers = FastHashSet::default();
        workers.insert(worker(1, dead_port));
        workers.insert(worker(2, live_port));

        let res = rt.block_on(runner.cancel_job("job-1", workers));
        assert!(res.is_ok(), "unreachable worker failed cancel: {:?}", res);

        assert_eq!(
            *seen.lock().unwrap(),
            vec!["job-1".to_string()],
            "live worker must receive exactly one CancelJob for job-1"
        );
    }

    /// Stalled workers are waited on together, so cancel latency stays near
    /// one RPC timeout instead of growing with the number of stalled workers.
    #[test]
    fn cancel_job_waits_on_stalled_workers_concurrently() {
        const RPC_TIMEOUT_MS: u64 = 1000;
        const STALLED: u32 = 4;
        let (runner, rt) = test_runner(RPC_TIMEOUT_MS);

        // Bound but never accepted: the kernel completes the handshake, the
        // request is written, and no response ever comes back.
        let stalled: Vec<TcpListener> = (0..STALLED)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let mut workers = FastHashSet::default();
        for (i, l) in stalled.iter().enumerate() {
            workers.insert(worker(i as u32 + 1, l.local_addr().unwrap().port()));
        }

        let start = Instant::now();
        let res = rt.block_on(runner.cancel_job("job-1", workers));
        let elapsed = start.elapsed();
        assert!(res.is_ok(), "stalled workers failed cancel: {:?}", res);

        // Serial dialing needs at least STALLED x RPC_TIMEOUT_MS.
        let serial_floor = Duration::from_millis(RPC_TIMEOUT_MS * STALLED as u64);
        assert!(
            elapsed < serial_floor / 2,
            "cancel took {:?}; stalled workers were waited on serially",
            elapsed
        );
    }

    fn fs_entry(path: &str, is_dir: bool) -> FileStatus {
        FileStatus {
            path: path.to_string(),
            is_dir,
            ..Default::default()
        }
    }

    fn fs_dir(path: &str) -> FileStatus {
        fs_entry(path, true)
    }

    fn fs_file(path: &str) -> FileStatus {
        fs_entry(path, false)
    }

    /// The walk must overlap its listings, never exceed the bound, survive
    /// out-of-order completions, and still return every file exactly once.
    #[test]
    fn walk_files_overlaps_listings_within_the_bound() {
        const DIRS: usize = 40;
        const CONCURRENCY: usize = 3;
        let rt = Runtime::new("walk-overlap-test", 4, 16);
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listed = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));

        let mut files = Vec::new();
        let res = rt.block_on(walk_files(
            vec![fs_dir("s3://b/root")],
            CONCURRENCY,
            |dir: Path| {
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                let listed = listed.clone();
                async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(current, Ordering::SeqCst);
                    listed.lock().push(dir.full_path().to_string());
                    // Later directories answer faster, so completions interleave
                    // in a different order than the walk started them.
                    let index: usize = dir
                        .full_path()
                        .rsplit('d')
                        .next()
                        .and_then(|n| n.parse().ok())
                        .unwrap_or(0);
                    let delay = (DIRS.saturating_sub(index)).min(6) as u64;
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);

                    let mut children = Vec::new();
                    if dir.full_path().ends_with("/root") {
                        for d in 0..DIRS {
                            children.push(fs_dir(&format!("s3://b/root/d{d}")));
                        }
                        children.push(fs_file("s3://b/root/top.bin"));
                    } else {
                        children.push(fs_file(&format!("{}/a.bin", dir.full_path())));
                        children.push(fs_file(&format!("{}/b.bin", dir.full_path())));
                    }
                    Ok::<_, FsError>(children)
                }
            },
            |status| {
                files.push(status.path);
                Ok(())
            },
        ));

        assert!(res.is_ok(), "walk failed: {res:?}");
        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= CONCURRENCY,
            "{peak} listings were in flight, over the bound of {CONCURRENCY}"
        );
        assert!(peak > 1, "the walk never overlapped its listings");
        assert_eq!(listed.lock().len(), DIRS + 1, "every directory was listed");

        files.sort();
        let mut expected: Vec<String> = vec!["s3://b/root/top.bin".to_string()];
        for d in 0..DIRS {
            expected.push(format!("s3://b/root/d{d}/a.bin"));
            expected.push(format!("s3://b/root/d{d}/b.bin"));
        }
        expected.sort();
        assert_eq!(
            files, expected,
            "the walk must visit every file exactly once"
        );
    }

    /// A wide tree must cost one pass: the queue is only drained when a listing
    /// slot is free, so the walk neither re-drains nor re-queues unstarted
    /// directories (that version was O(dirs^2) and stalled on big fan-outs).
    #[test]
    fn walk_files_handles_a_wide_fan_out() {
        const DIRS: usize = 5_000;
        let rt = Runtime::new("walk-wide-test", 4, 16);
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut files = 0usize;

        let res = rt.block_on(walk_files(
            vec![fs_dir("s3://b/root")],
            3,
            |dir: Path| {
                let started = started.clone();
                let done = done.clone();
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    done.fetch_add(1, Ordering::SeqCst);
                    let children = if dir.full_path().ends_with("/root") {
                        (0..DIRS)
                            .map(|d| fs_dir(&format!("s3://b/root/d{d}")))
                            .collect()
                    } else {
                        vec![fs_file(&format!("{}/f.bin", dir.full_path()))]
                    };
                    Ok::<_, FsError>(children)
                }
            },
            |_status| {
                files += 1;
                Ok(())
            },
        ));

        let stats = res.expect("walk failed");
        assert_eq!(
            started.load(Ordering::SeqCst),
            DIRS + 1,
            "every directory must be listed exactly once"
        );
        // The invariant a re-scanning queue breaks: every entry taken off the
        // queue exactly once. The O(dirs^2) version popped each pending
        // directory again on every completion, so this is what pins the shape of
        // the traversal (a count-only test passes either way).
        assert_eq!(
            stats.entries_popped,
            stats.listings_started + files,
            "the walk popped an entry more than once: {} listings, {} pops, {} files",
            stats.listings_started,
            stats.entries_popped,
            files
        );
        assert_eq!(done.load(Ordering::SeqCst), DIRS + 1);
        assert_eq!(files, DIRS, "every file must be planned exactly once");
    }

    /// A listing failure stops the walk instead of waiting for slower siblings.
    #[test]
    fn walk_files_reports_a_listing_failure_promptly() {
        const DIRS: usize = 10;
        let rt = Runtime::new("walk-error-test", 4, 16);
        let listed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let res = rt.block_on(walk_files(
            vec![fs_dir("s3://b/root")],
            2,
            |dir: Path| {
                let listed = listed.clone();
                async move {
                    listed.fetch_add(1, Ordering::SeqCst);
                    if dir.full_path().ends_with("d2") {
                        return Err(FsError::common("listing failed"));
                    }
                    if dir.full_path().ends_with("/root") {
                        let children = (0..DIRS)
                            .map(|d| fs_dir(&format!("s3://b/root/d{d}")))
                            .collect();
                        return Ok(children);
                    }
                    // Everything else is slow, so a barrier implementation would
                    // wait for all of them before reporting the failure.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Ok(Vec::new())
                }
            },
            |_status| Ok(()),
        ));

        assert!(res.is_err(), "the listing failure must fail the walk");
        assert!(
            listed.load(Ordering::SeqCst) < DIRS + 1,
            "the walk kept listing after a failure"
        );
    }

    /// The submission window must stay bounded. With more items than
    /// `SUBMIT_CONCURRENCY_PER_WORKER`, at most that many submissions may be in
    /// flight at once, and every item must still be submitted exactly once. This
    /// fails if the bounded stream is replaced by an unbounded `try_join_all`.
    #[test]
    fn submit_with_bound_caps_in_flight_submissions() {
        let rt = Runtime::new("submit-bound-test", 2, 8);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let submitted = Arc::new(AtomicUsize::new(0));

        let total = SUBMIT_CONCURRENCY_PER_WORKER * 3;
        let items: Vec<usize> = (0..total).collect();
        let res = rt.block_on(submit_with_bound(
            items,
            SUBMIT_CONCURRENCY_PER_WORKER,
            |_item| {
                let in_flight = in_flight.clone();
                let max_in_flight = max_in_flight.clone();
                let submitted = submitted.clone();
                async move {
                    let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    max_in_flight.fetch_max(current, Ordering::SeqCst);
                    // Hold the slot so a second submission can overlap.
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    submitted.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), FsError>(())
                }
            },
        ));

        assert!(res.is_ok(), "bounded submission failed: {:?}", res);
        assert_eq!(
            submitted.load(Ordering::SeqCst),
            total,
            "every task must be submitted exactly once"
        );
        let peak = max_in_flight.load(Ordering::SeqCst);
        assert!(
            peak <= SUBMIT_CONCURRENCY_PER_WORKER,
            "in-flight submissions {} exceeded the bound {}",
            peak,
            SUBMIT_CONCURRENCY_PER_WORKER
        );
        assert!(peak > 1, "the test did not exercise any concurrency");
    }
    /// A full worker queue must delay the submission, not fail the task: the
    /// retry loop has to keep trying until the worker accepts it.
    #[test]
    fn submit_until_accepted_retries_a_full_queue() {
        let rt = Runtime::new("submit-backpressure-test", 2, 8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let rejections = 3;

        let attempts_this = attempts.clone();
        let res = rt.block_on(submit_until_accepted(
            Backoff {
                initial: Duration::from_millis(1),
                max: Duration::from_millis(2),
            },
            Duration::from_secs(30),
            None,
            || {
                let attempts = attempts_this.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt <= rejections {
                        Ok(SubmitOutcome::QueueFull)
                    } else {
                        Ok(SubmitOutcome::Accepted)
                    }
                }
            },
        ));

        assert_eq!(res.unwrap(), rejections as u64);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            rejections + 1,
            "the task must be submitted again after every queue-full answer"
        );
    }

    /// A worker that refuses a task for good must fail the dispatch instead of
    /// retrying it forever.
    #[test]
    fn submit_until_accepted_stops_on_a_verdict() {
        let rt = Runtime::new("submit-verdict-test", 2, 8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_this = attempts.clone();

        let res = rt.block_on(submit_until_accepted(
            QUEUE_FULL_BACKOFF,
            Duration::from_secs(30),
            None,
            || {
                let attempts = attempts_this.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FsError>(SubmitOutcome::Rejected("stale task".to_string()))
                }
            },
        ));

        let err = res.expect_err("a verdict rejection must not succeed");
        assert!(
            err.to_string().contains("stale task"),
            "rejection reason must reach the caller: {}",
            err
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "no retry on a verdict");
    }

    /// A worker that answers queue-full forever must not park the dispatch: the
    /// wait is capped, and the failure says why.
    #[test]
    fn submit_until_accepted_gives_up_when_the_queue_never_drains() {
        let rt = Runtime::new("submit-queue-stuck-test", 2, 8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_this = attempts.clone();

        let res = rt.block_on(submit_until_accepted(
            Backoff {
                initial: Duration::from_millis(1),
                max: Duration::from_millis(2),
            },
            Duration::from_millis(20),
            None,
            || {
                let attempts = attempts_this.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FsError>(SubmitOutcome::QueueFull)
                }
            },
        ));

        let err = res.expect_err("a queue that never drains must not be waited on forever");
        assert!(
            err.to_string().contains("queue stayed full"),
            "the failure must name the cause: {}",
            err
        );
        assert!(
            attempts.load(Ordering::SeqCst) > 1,
            "the loop must actually have retried before giving up"
        );
    }

    /// An export's copy budget must bound the queue-full wait too: without the
    /// hard deadline the wait is capped only by `worker_queue_full_timeout`
    /// (30m), which can outlive the 20m export deadline and then fail a job that
    /// had just been admitted.
    #[test]
    fn submit_until_accepted_respects_a_hard_deadline() {
        let rt = Runtime::new("submit-hard-deadline-test", 2, 8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_this = attempts.clone();

        let started = std::time::Instant::now();
        let res = rt.block_on(submit_until_accepted(
            Backoff {
                initial: Duration::from_millis(50),
                max: Duration::from_millis(50),
            },
            Duration::from_secs(600),
            Some(tokio::time::Instant::now() + Duration::from_millis(120)),
            || {
                let attempts = attempts_this.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FsError>(SubmitOutcome::QueueFull)
                }
            },
        ));

        let err = res.expect_err("the hard deadline must end the wait");
        assert!(
            err.to_string().contains("deadline exceeded"),
            "the failure must name the deadline: {}",
            err
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the wait outlived the 120ms hard deadline: {:?}",
            started.elapsed()
        );
    }

    /// A canceled job stops dispatching even while it waits on backpressure.
    #[test]
    fn submit_until_accepted_stops_when_the_job_is_abandoned() {
        let rt = Runtime::new("submit-abandoned-test", 2, 8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_this = attempts.clone();

        let res = rt.block_on(submit_until_accepted(
            QUEUE_FULL_BACKOFF,
            Duration::from_secs(30),
            None,
            || {
                let attempts = attempts_this.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, FsError>(SubmitOutcome::Abandoned)
                }
            },
        ));

        assert!(res.is_ok(), "abandoning a canceled job is not an error");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod transfer_defaults_tests {
    use curvine_config::LoadTransferDefaults;
    use curvine_model::MountInfo;
    use std::collections::HashMap;

    fn empty_mount() -> MountInfo {
        MountInfo {
            cv_path: "/cv".to_string(),
            ufs_path: "s3://bucket".to_string(),
            mount_id: 1,
            properties: HashMap::new(),
            ttl_ms: 0,
            ttl_action: Default::default(),
            read_verify_ufs: false,
            storage_type: None,
            block_size: None,
            replicas: None,
            write_type: Default::default(),
            provider: None,
            auto_cache: true,
            access_mode: Default::default(),
            write_cache: false,
        }
    }

    /// The shipped `[job]` defaults, which is what a planner injects.
    fn defaults() -> LoadTransferDefaults {
        LoadTransferDefaults::from_job(&curvine_config::JobConf::default())
    }

    #[test]
    fn fills_the_worker_visible_mount_properties() {
        let mut mnt = empty_mount();
        defaults().apply(&mut mnt.properties);

        // These are the keys the worker reads when it plans a transfer.
        assert_eq!(mnt.properties["load_task.parallel_streams"], "8");
        assert_eq!(mnt.properties["load_task.min_bytes_per_stream"], "33554432");
        // …and the keys the opendal reader reads for its range requests.
        assert_eq!(mnt.properties["opendal.read_chunk_size"], "2097152");
        assert_eq!(mnt.properties["opendal.read_concurrent"], "4");
    }

    #[test]
    fn keeps_values_the_mount_already_sets() {
        let mut mnt = empty_mount();
        mnt.properties
            .insert("load_task.parallel_streams".to_string(), "2".to_string());
        mnt.properties
            .insert("opendal.read_chunk_size".to_string(), "1048576".to_string());

        defaults().apply(&mut mnt.properties);

        assert_eq!(mnt.properties["load_task.parallel_streams"], "2");
        assert_eq!(mnt.properties["opendal.read_chunk_size"], "1048576");
        // Unset keys still get the configured default.
        assert_eq!(mnt.properties["opendal.read_concurrent"], "4");
    }
}
