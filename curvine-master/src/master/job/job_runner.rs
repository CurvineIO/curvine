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
use curvine_config::ClientConf;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{
    JobTaskState, LoadJobCommand, LoadJobInfo, LoadJobResult, LoadTaskInfo, MountInfo,
    WorkerAddress,
};
use curvine_runtime::common::CommonUtils;
use curvine_runtime::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use curvine_runtime::sync::AtomicCounter;
use curvine_unified_fs::MountValue;
use dashmap::mapref::entry::Entry;
use futures::future;
use log::{debug, error, info, warn};
use std::collections::LinkedList;
use std::sync::Arc;

/// Scheme + authority prefix of a path, without the path component.
/// CV paths (no scheme) return an empty string.
fn path_scheme_authority_prefix(path: &Path) -> String {
    match path.scheme() {
        None => String::new(),
        Some(scheme) => {
            let authority = path.authority().unwrap_or("");
            if authority.is_empty() {
                format!("{}://", scheme)
            } else {
                format!("{}://{}", scheme, authority)
            }
        }
    }
}

/// Path component with trailing slash removed, except when the component is exactly "/".
fn path_component_of(path: &Path) -> String {
    let p = path.path();
    if p == "/" {
        p.to_string()
    } else {
        p.trim_end_matches('/').to_string()
    }
}

/// Prefix used when joining a relative child path onto a job target base.
fn build_target_base_for_join(target: &Path) -> String {
    format!(
        "{}{}",
        path_scheme_authority_prefix(target),
        path_component_of(target)
    )
}

fn join_target_rel(target_base_for_join: &str, rel: &str) -> FsResult<Path> {
    let joined = if rel.is_empty() || target_base_for_join.ends_with('/') {
        format!("{}{}", target_base_for_join, rel)
    } else {
        format!("{}/{}", target_base_for_join, rel)
    };
    Ok(Path::from_str(&joined)?)
}

/// Map a task source path onto the job's target base using the job source root.
fn map_task_target_path(
    source_root_component: &str,
    task_source_path: &Path,
    target_base: &Path,
    target_base_for_join: &str,
) -> FsResult<Path> {
    let task_component = path_component_of(task_source_path);
    if task_component == source_root_component {
        Ok(Path::from_str(target_base.full_path())?)
    } else if source_root_component == "/" {
        join_target_rel(target_base_for_join, task_component.trim_start_matches('/'))
    } else if let Some(rel) = task_component
        .strip_prefix(source_root_component)
        .and_then(|suffix| suffix.strip_prefix('/'))
    {
        join_target_rel(target_base_for_join, rel)
    } else {
        err_box!(
            "Cannot map task source {} onto job target base {} (source root={})",
            task_source_path.full_path(),
            target_base.full_path(),
            source_root_component
        )
    }
}

pub struct LoadJobRunner {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
}

/// A load job that has been accepted into the store and awaits background
/// planning + dispatch (#998). Carries the run-scoped `run_id` so stale
/// planning tasks cannot revive a replaced run.
#[derive(Clone)]
pub(crate) struct QueuedLoadJob {
    pub(crate) job_id: String,
    pub(crate) run_id: u64,
}

/// Result of planning a load job: the per-file tasks and their total size.
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

    /// Build a fresh `JobContext` (with a new run-scoped `run_id`) plus the
    /// resolved source/target paths. Preserves next's explicit `target_path`
    /// override support (falling back to the mount toggle when unset).
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

    /// Data-state fast-path: returns true when the target is already present and
    /// valid in Curvine, so no load tasks need to be dispatched. Applies whether
    /// the slot was vacant or held a terminal ctx — e.g. after JobCleanupTask,
    /// after master restart + journal replay, or after an earlier run completed
    /// the sync. Only UFS→CV imports can be fast-skipped; CV sources always need
    /// an explicit export task.
    async fn check_already_loaded(
        &self,
        mnt: &MountValue,
        source_path: &Path,
        target_path: &Path,
    ) -> FsResult<bool> {
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

    /// Accept a load job into the store without doing any UFS planning (#998).
    /// Returns the immediate result plus, when the job was freshly accepted, a
    /// `QueuedLoadJob` that the caller schedules for bounded background planning.
    /// Keeps the submit RPC critical path off the recursive UFS walk.
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

    /// Plan and dispatch a previously enqueued load job in the background (#998).
    /// All state transitions are run-scoped via `update_state_if_run` so a stale
    /// planning task (whose run has been replaced) cannot revive or corrupt the
    /// current run.
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
        let mut tasks: FastHashMap<String, TaskDetail> = FastHashMap::new();
        let mut stack = LinkedList::new();
        let mut task_index = 0;
        stack.push_back(source_status);

        // Capture job-level source root and target base before the loop.
        // `source_path` (the function parameter) is shadowed by a local inside the loop.
        //
        // We use `Path::path()` — the path *component* only, without scheme/authority —
        // for prefix matching. This avoids URI-scheme mangling from `trim_end_matches('/')`:
        //   • "/"        would be stripped to ""
        //   • "file:///" would be stripped to "file:"
        // With `path()` the scheme+authority is kept separately and recombined below.
        let target_base = Path::from_str(&job.target_path)?;

        // source_root_component: path component of the job source root, trailing slash
        // removed unless the path is exactly "/".
        let source_root_component = path_component_of(source_path);

        // Rebuild join prefix from Path fields (scheme/authority/path), not raw string
        // slicing, so unnormalized stored URIs cannot corrupt scheme boundaries.
        let target_base_for_join = build_target_base_for_join(&target_base);

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

                let target_path = map_task_target_path(
                    &source_root_component,
                    &source_path,
                    &target_base,
                    &target_base_for_join,
                )?;

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
                    estimated_size: status.len,
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
            }
        }

        Ok(PlannedLoadJob { tasks, total_size })
    }

    pub async fn cancel_job(
        &self,
        job_id: impl AsRef<str>,
        assigned_workers: FastHashSet<WorkerAddress>,
    ) -> FsResult<()> {
        // Send cancel to all assigned workers concurrently. The original sequential
        // loop multiplied per-worker latency, causing the caller's RPC timeout to
        // fire when several workers were involved. Errors from individual workers
        // are logged but do not abort the overall cancel.
        let job_id = job_id.as_ref().to_string();
        let cancel_futures = assigned_workers.iter().map(|worker| {
            let worker = worker.clone();
            let job_id = job_id.clone();
            let factory = self.factory.clone();
            let jobs = self.jobs.clone();
            async move {
                let client = match factory.get_worker_client(&worker).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("failed to get worker client for {}: {}", worker, e);
                        if let Err(update_err) = jobs.update_state(
                            &job_id,
                            JobTaskState::Canceled,
                            format!("failed to get worker client for {}: {}", worker, e),
                        ) {
                            error!(
                                "failed to mark load job {} as canceled after worker client error: {}",
                                job_id, update_err
                            );
                        }
                        return;
                    }
                };
                if let Err(e) = client.cancel_job(&job_id).await {
                    error!("failed to send cancel request to worker {}: {}", worker, e);
                    if let Err(update_err) = jobs.update_state(
                        &job_id,
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
        });
        future::join_all(cancel_futures).await;
        Ok(())
    }

    /// Best-effort cancel of already-dispatched sub-tasks after a dispatch
    /// failure in a background run (#998). Errors are logged, never propagated.
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

#[cfg(test)]
mod target_path_mapping_tests {
    use super::*;

    #[test]
    fn single_file_maps_to_target_base() {
        let source_root =
            path_component_of(&Path::from_str("file:///data/ufs-a/file.txt").expect("source root"));
        let task = Path::from_str("file:///data/ufs-a/file.txt").expect("task source");
        let target = Path::from_str("/multi-fs-a/file.txt").expect("target base");
        let join_base = build_target_base_for_join(&target);
        let mapped =
            map_task_target_path(&source_root, &task, &target, &join_base).expect("mapped");
        assert_eq!(mapped.full_path(), "/multi-fs-a/file.txt");
    }

    #[test]
    fn directory_child_maps_relative_path() {
        let source_root =
            path_component_of(&Path::from_str("file:///data/ufs-a/dir").expect("source root"));
        let task = Path::from_str("file:///data/ufs-a/dir/sub/x.txt").expect("task source");
        let target = Path::from_str("/multi-fs-b/export").expect("target base");
        let join_base = build_target_base_for_join(&target);
        let mapped =
            map_task_target_path(&source_root, &task, &target, &join_base).expect("mapped");
        assert_eq!(mapped.full_path(), "/multi-fs-b/export/sub/x.txt");
    }

    #[test]
    fn ufs_authority_preserved_in_target_join() {
        let source_root =
            path_component_of(&Path::from_str("file:///tmp/src").expect("source root"));
        let task = Path::from_str("file:///tmp/src/a.txt").expect("task source");
        let target = Path::from_str("file:///mnt/ufs/out").expect("target base");
        let join_base = build_target_base_for_join(&target);
        let mapped =
            map_task_target_path(&source_root, &task, &target, &join_base).expect("mapped");
        assert_eq!(mapped.full_path(), "file:///mnt/ufs/out/a.txt");
    }

    #[test]
    fn mismatched_source_root_returns_error() {
        let source_root =
            path_component_of(&Path::from_str("file:///data/other").expect("source root"));
        let task = Path::from_str("file:///data/ufs-a/dir/file.txt").expect("task source");
        let target = Path::from_str("/cv/target").expect("target base");
        let join_base = build_target_base_for_join(&target);
        assert!(map_task_target_path(&source_root, &task, &target, &join_base).is_err());
    }
}
