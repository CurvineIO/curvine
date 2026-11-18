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
    load_target_is_current, needs_source_status_refresh, FileStatus, JobTaskState, JobTaskType,
    LoadJobCommand, LoadJobInfo, LoadJobResult, LoadRefresh, LoadTaskInfo, MountInfo,
    WorkerAddress,
};
use curvine_runtime::common::CommonUtils;
use curvine_runtime::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use curvine_runtime::sync::AtomicCounter;
use curvine_unified_fs::MountValue;
use dashmap::mapref::entry::Entry;
use futures::future;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::collections::LinkedList;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Files the cache policy left alone because Curvine already holds them.
    skipped_files: usize,
    skipped_size: i64,
}

#[derive(Clone, Copy)]
enum CvSourceMode {
    LoadUfsOnlyFromUfs,
    ExportCurvineToUfs,
}

/// Fresh source stats the planner issues at once when a listing carried no mtime.
const STAT_SETTLE_CONCURRENCY: usize = 32;

/// Turn one source file into a plan task.
#[allow(clippy::too_many_arguments)]
fn plan_load_task(
    tasks: &mut FastHashMap<String, TaskDetail>,
    task_index: &mut u64,
    total_size: &mut i64,
    job: &LoadJobInfo,
    run_id: u64,
    worker: &WorkerAddress,
    source_path: &Path,
    target_path: &Path,
    len: i64,
    job_max_files: usize,
) -> FsResult<()> {
    let task_id = format!("{}_run_{}_task_{}", job.job_id, run_id, task_index);
    *task_index += 1;
    *total_size += len;

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

    if tasks.len() > job_max_files {
        return err_box!("Job {} files exceeds {}", job.job_id, job_max_files);
    }
    debug!(
        "created sub-task {} ({} -> {})",
        task_id,
        source_path.full_path(),
        target_path.full_path()
    );
    Ok(())
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
        refresh: LoadRefresh,
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

        // `always` means re-copy whatever is cached, so the whole-source fast
        // path must not short-circuit the job.
        if refresh == LoadRefresh::Always {
            return Ok(false);
        }

        // Target not present in Curvine yet — must load.
        let cv_status = match self.master_fs.file_status(target_path.path()) {
            Ok(cv_status) => cv_status,
            Err(FsError::FileNotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };

        // `skip-cached` trusts the cache without looking at the source.
        if refresh == LoadRefresh::SkipCached {
            return Ok(!cv_status.is_dir && cv_status.is_complete && cv_status.cv_exists());
        }

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
            .check_already_loaded(
                &mnt_value,
                &source_path,
                &target_path,
                job_context.info.refresh,
            )
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
        job_context.skipped_files = planned.skipped_files;
        job_context.skipped_size = planned.skipped_size;
        if planned.tasks.is_empty() {
            info!(
                "{} job {} has no tasks: {} -> {}, {} file(s) already current",
                job_kind,
                job_id,
                source_path.full_path(),
                target_path.full_path(),
                planned.skipped_files
            );
            // Nothing to copy, but the run still has a result worth querying: the
            // cache policy skipped these files because Curvine already holds
            // them, and `load-status` must say so instead of "job not found".
            job_context.progress.skipped_files = planned.skipped_files as i64;
            job_context.progress.skipped_size = planned.skipped_size;
            job_context.progress.loaded_size = planned.skipped_size;
            job_context.progress.total_size = planned.skipped_size;
            job_context.update_state(
                JobTaskState::Completed,
                format!("{} file(s) already cached", planned.skipped_files),
            );
            let result = LoadJobResult::with_state(&job_context.info, JobTaskState::Completed);
            match self.jobs.entry(job_id.clone()) {
                Entry::Occupied(mut e) => {
                    // Never replace a run another submitter is still executing.
                    let running: JobTaskState = e.get().state.state();
                    if !running.is_running() {
                        e.insert(job_context);
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(job_context);
                }
            }
            return Ok(result);
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
            .check_already_loaded(&mnt_value, &source_path, &target_path, job_info.refresh)
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
        // The plan's skips belong to the job this task is dispatching: record them
        // before any report arrives, so `load-status` counts them.
        self.jobs
            .set_skipped(&queued.job_id, planned.skipped_files, planned.skipped_size);
        if task_count == 0 {
            info!(
                "queued load job {} has no tasks: {} -> {}, {} file(s) already current",
                queued.job_id,
                source_path.full_path(),
                target_path.full_path(),
                planned.skipped_files
            );
            self.jobs.record_skipped_only_run(
                &queued.job_id,
                planned.skipped_files,
                planned.skipped_size,
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
        let mut stack = LinkedList::new();
        let mut task_index = 0;
        let mut skipped_files = 0usize;
        let mut skipped_size = 0i64;
        let refresh = job.refresh;
        // Files whose cached copy has the same size but whose listing carried no
        // mtime: one fresh source stat settles them. They are collected here and
        // resolved after the walk, several at a time, because a backend that
        // omits the mtime would otherwise cost one serialized round trip per
        // cached file inside the walk.
        let mut deferred: Vec<(Path, Path, FileStatus, Option<FileStatus>)> = Vec::new();
        stack.push_back(source_status);

        // Get target base path for direction detection
        let target_base = Path::from_str(&job.target_path)?;

        while let Some(status) = stack.pop_front() {
            if status.is_dir {
                // List directory based on path type
                let dir_path = Path::from_str(status.path)?;
                let childs = if dir_path.is_cv() {
                    // Traverse Curvine directory
                    self.master_fs.list_status(dir_path.path())?
                } else {
                    // Traverse UFS directory
                    let ufs = self.factory.get_ufs(mnt)?;
                    ufs.list_status(&dir_path).await?
                };

                for child in childs {
                    stack.push_back(child);
                }
            } else {
                let worker = self.choose_worker(block_size)?;

                let source_path = Path::from_str(&status.path)?;

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

                // Honour the cache policy before the file becomes a task: an
                // already-current copy must not be planned at all, or the
                // dispatch would re-copy it.
                if refresh != LoadRefresh::Always && target_path.is_cv() {
                    let existing = match self.master_fs.file_status(target_path.path()) {
                        Ok(status) => Some(status),
                        Err(FsError::FileNotFound(_)) => None,
                        Err(err) => return Err(err),
                    };
                    let current = load_target_is_current(refresh, &status, existing.as_ref());
                    // A listing may omit the last-modified time, which makes an
                    // unchanged file look stale. Collect those and settle them
                    // after the walk (bounded concurrency), instead of paying one
                    // serialized stat here for every cached file of such a
                    // backend. Files whose size already differs are copied as-is.
                    if refresh == LoadRefresh::IfNewer
                        && !current
                        && needs_source_status_refresh(&status, existing.as_ref())
                    {
                        deferred.push((
                            source_path.clone(),
                            target_path.clone(),
                            status.clone(),
                            existing.clone(),
                        ));
                        continue;
                    }
                    if current {
                        skipped_files += 1;
                        skipped_size += status.len;
                        debug!(
                            "skip {} ({}) under refresh={}",
                            source_path.full_path(),
                            target_path.full_path(),
                            refresh
                        );
                        continue;
                    }
                }

                plan_load_task(
                    &mut tasks,
                    &mut task_index,
                    &mut total_size,
                    job,
                    run_id,
                    &worker,
                    &source_path,
                    &target_path,
                    status.len,
                    self.job_max_files,
                )?;
            }
        }

        // Second phase: settle the files whose listing carried no mtime, several
        // round trips at a time. A file that is current after all is a skip; the
        // rest keep the normal planning path.
        if !deferred.is_empty() {
            let ufs = self.factory.get_ufs(mnt)?;
            let resolved = stream::iter(deferred)
                .map(|(source_path, target_path, source_status, existing)| {
                    let ufs = ufs.clone();
                    async move {
                        let fresh = ufs.get_status(&source_path).await;
                        (source_path, target_path, source_status, existing, fresh)
                    }
                })
                .buffer_unordered(STAT_SETTLE_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;

            for (source_path, target_path, source_status, existing, fresh) in resolved {
                let fresh = fresh?;
                if load_target_is_current(refresh, &fresh, existing.as_ref()) {
                    skipped_files += 1;
                    skipped_size += fresh.len;
                    debug!(
                        "skip {} ({}) under refresh={} (same size, stale listing)",
                        source_path.full_path(),
                        target_path.full_path(),
                        refresh
                    );
                    continue;
                }
                let worker = self.choose_worker(block_size)?;
                let len = if fresh.len > 0 {
                    fresh.len
                } else {
                    source_status.len
                };
                plan_load_task(
                    &mut tasks,
                    &mut task_index,
                    &mut total_size,
                    job,
                    run_id,
                    &worker,
                    &source_path,
                    &target_path,
                    len,
                    self.job_max_files,
                )?;
            }
        }

        if skipped_files > 0 {
            info!(
                "job {} planned {} task(s), {} file(s) already current under refresh={}",
                job.job_id,
                tasks.len(),
                skipped_files,
                refresh
            );
        }

        Ok(PlannedLoadJob {
            tasks,
            total_size,
            skipped_files,
            skipped_size,
        })
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
