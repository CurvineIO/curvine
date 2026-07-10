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
use crate::master::fs::policy::ChooseContext;
use crate::master::fs::MasterFilesystem;
use crate::master::{JobContext, JobStore, TaskDetail};
use curvine_client::unified::MountValue;
use curvine_common::conf::ClientConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    JobTaskState, LoadJobCommand, LoadJobInfo, LoadJobResult, LoadTaskInfo, MountInfo,
    WorkerAddress,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use dashmap::mapref::entry::Entry;
use futures::future;
use log::{debug, error, info, warn};
use orpc::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use orpc::err_box;
use orpc::sync::AtomicCounter;
use std::collections::LinkedList;
use std::sync::Arc;

pub struct LoadJobRunner {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
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

impl LoadJobRunner {
    const RUN_ID_SEQ_MOD: u64 = 1_000_000;

    pub fn new(
        jobs: JobStore,
        master_fs: MasterFilesystem,
        factory: Arc<UfsFactory>,
        job_max_files: usize,
        run_seq: Arc<AtomicCounter>,
    ) -> Self {
        Self {
            jobs,
            master_fs,
            factory,
            job_max_files,
            run_seq,
        }
    }

    pub fn choose_worker(&self, block_size: i64) -> FsResult<WorkerAddress> {
        let ctx = ChooseContext::with_num(1, block_size, vec![]);
        let worker_mgr = self.master_fs.worker_manager.read();
        let workers = worker_mgr.choose_worker(ctx)?;
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
        let (source_path, target_path) = match (cv_source_mode, command_source.is_cv()) {
            (CvSourceMode::LoadUfsOnlyFromUfs, false) => {
                let target_path = mnt.get_cv_path(&command_source)?;
                (command_source, target_path)
            }
            (CvSourceMode::LoadUfsOnlyFromUfs, true) => {
                let status = self.master_fs.file_status(command_source.path())?;
                if !status.storage_policy.ufs_only() {
                    return err_box!(
                        "load job for CV path {} requires UFS-only metadata",
                        command_source.full_path()
                    );
                }
                (mnt.get_ufs_path(&command_source)?, command_source)
            }
            (CvSourceMode::ExportCurvineToUfs, true) => {
                let target_path = mnt.get_ufs_path(&command_source)?;
                (command_source, target_path)
            }
            (CvSourceMode::ExportCurvineToUfs, false) => {
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
        let source_status = mnt.ufs.get_status(source_path).await?;
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
        self.submit_direct_task(command, mnt, CvSourceMode::LoadUfsOnlyFromUfs, "load")
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
        self.submit_direct_task(command, mnt, CvSourceMode::ExportCurvineToUfs, "export")
            .await
    }

    async fn submit_direct_task(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
        cv_source_mode: CvSourceMode,
        job_kind: &str,
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

        if let Err(err) = self.submit_all_task(tasks).await {
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

        if let Err(err) = self.submit_all_task(tasks).await {
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

    async fn submit_all_task(&self, tasks: FastHashMap<String, TaskDetail>) -> FsResult<()> {
        let submit_futures: Vec<_> = tasks
            .take()
            .into_iter()
            .map(|(id, task)| async move {
                let worker = task.task.worker.clone();
                let client = self.factory.get_worker_client(&worker).await?;
                client.submit_load_task(task.task).await?;
                debug!("dispatched sub-task {} to worker {}", id, worker);
                Ok::<(), FsError>(())
            })
            .collect();

        future::try_join_all(submit_futures).await?;
        Ok(())
    }

    async fn create_all_tasks(
        &self,
        job: &LoadJobInfo,
        source_path: &Path,
        mnt: &MountInfo,
        run_id: u64,
    ) -> FsResult<PlannedLoadJob> {
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
            }
        }

        Ok(PlannedLoadJob { tasks, total_size })
    }

    pub async fn cancel_job(
        &self,
        job_id: impl AsRef<str>,
        assigned_workers: FastHashSet<WorkerAddress>,
    ) -> FsResult<()> {
        let job_id = job_id.as_ref();
        for worker in assigned_workers.iter() {
            let client = self.factory.get_worker_client(worker).await?;
            let res = client.cancel_job(job_id).await;

            if let Err(e) = res {
                error!("failed to send cancel request to worker {}: {}", worker, e);
                if let Err(update_err) = self.jobs.update_state(
                    job_id,
                    JobTaskState::Canceled,
                    format!("failed to send cancel request to worker {}: {}", worker, e),
                ) {
                    error!(
                        "failed to mark load job {} as canceled after worker cancel error: {}",
                        job_id, update_err
                    );
                }
            }
        }

        Ok(())
    }

    async fn cancel_workers_best_effort(
        &self,
        job_id: &str,
        assigned_workers: &FastHashSet<WorkerAddress>,
    ) {
        for worker in assigned_workers.iter() {
            let client = match self.factory.get_worker_client(worker).await {
                Ok(client) => client,
                Err(err) => {
                    error!(
                        "failed to create cancel client for worker {}: {}",
                        worker, err
                    );
                    continue;
                }
            };

            if let Err(err) = client.cancel_job(job_id).await {
                error!(
                    "failed to send cancel request to worker {}: {}",
                    worker, err
                );
            }
        }
    }
}
