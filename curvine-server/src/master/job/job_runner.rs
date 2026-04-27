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
    JobTaskState, LoadJobCommand, LoadJobResult, LoadTaskInfo, MountInfo, WorkerAddress,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use dashmap::mapref::entry::Entry;
use futures::future;
use log::{debug, error, info, warn};
use orpc::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use orpc::err_box;
use std::collections::LinkedList;
use std::sync::Arc;

pub struct LoadJobRunner {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    job_max_files: usize,
}

impl LoadJobRunner {
    pub fn new(
        jobs: JobStore,
        master_fs: MasterFilesystem,
        factory: Arc<UfsFactory>,
        job_max_files: usize,
    ) -> Self {
        Self {
            jobs,
            master_fs,
            factory,
            job_max_files,
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

    async fn check_job_exists(
        &self,
        job: &JobContext,
        mnt: &MountValue,
        source_path: &Path,
        target_path: &Path,
    ) -> FsResult<Option<LoadJobResult>> {
        if let Some(exist_job) = self.jobs.get(&job.info.job_id) {
            let state: JobTaskState = exist_job.state.state();
            if state.is_running() {
                return Ok(Some(LoadJobResult::with_state(&exist_job.info, state)));
            }
        }

        // Data-state fast-path. Applies whether the slot was vacant or held
        // a terminal ctx — e.g. after JobCleanupTask, after master restart +
        // journal replay, or after an earlier run completed the sync.
        //
        // Only UFS→CV imports can be fast-skipped here; CV sources always
        // need an explicit export task.
        if source_path.is_cv() {
            return Ok(None);
        }

        // Target not present in Curvine yet — must load.
        let cv_status = match self.master_fs.file_status(target_path.path()) {
            Ok(cv_status) => cv_status,
            Err(FsError::FileNotFound(_)) => return Ok(None),
            Err(err) => return Err(err),
        };

        // Cached target exists but its own metadata says it isn't usable — must reload.
        if !cv_status.cv_valid(None) {
            return Ok(None);
        }

        // Target looks valid locally; confirm against UFS source before skipping.
        let source_status = mnt.ufs.get_status(source_path).await?;
        if cv_status.cv_valid(Some(&source_status)) {
            Ok(Some(LoadJobResult::with_state(
                &job.info,
                JobTaskState::Completed,
            )))
        } else {
            Ok(None)
        }
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
        let source_path = Path::from_str(&command.source_path)?;
        let target_path = mnt.toggle_path(&source_path)?;

        let job_id = CommonUtils::create_job_id(source_path.full_path());
        let mut job_context = JobContext::with_conf(
            &command,
            job_id.clone(),
            source_path.clone_uri(),
            target_path.clone_uri(),
            &mnt,
            &ClientConf::default(),
        );

        let mnt_value = self.factory.get_mnt(&mnt)?;
        if let Some(res) = self
            .check_job_exists(&job_context, &mnt_value, &source_path, &target_path)
            .await?
        {
            info!(
                "skip load job {}: source_path {} already loaded or in progress",
                job_id,
                source_path.full_path()
            );
            return Ok(res);
        }

        debug!(
            "submitting load job {}: {} -> {}",
            job_id,
            source_path.full_path(),
            target_path.full_path()
        );

        let total_size = self
            .create_all_tasks(&mut job_context, &source_path, &mnt)
            .await?;

        info!(
            "load job {} submitted: {} -> {}, tasks {}, total_size {}",
            job_id,
            source_path.full_path(),
            target_path.full_path(),
            job_context.tasks.len(),
            ByteUnit::byte_to_string(total_size as u64)
        );

        let tasks = job_context.tasks.clone();
        let res = LoadJobResult::with_job(&job_context.info);

        // Install / replace the ctx into the store atomically. We branch into:
        //   - Vacant: first submitter, install and dispatch.
        //   - Occupied + running: another submitter won the race; return that job’s
        //     state (this request’s command is not applied—see `submit_load_task` doc).
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
            warn!("dispatch load job {} failed: {}", job_id, err);
            // @todo Cancel sub-tasks that may have already been dispatched.
            self.jobs.update_state(
                &job_id,
                JobTaskState::Failed,
                format!("dispatch failed: {}", err),
            );
            return Err(err);
        }

        Ok(res)
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
        job: &mut JobContext,
        source_path: &Path,
        mnt: &MountInfo,
    ) -> FsResult<i64> {
        let source_status = if source_path.is_cv() {
            self.master_fs.file_status(source_path.path())?
        } else {
            let ufs = self.factory.get_ufs(mnt)?;
            ufs.get_status(source_path).await?
        };

        job.update_state(JobTaskState::Pending, "Assigning workers");
        let block_size = job.info.block_size;

        let mut total_size = 0;
        let mut stack = LinkedList::new();
        let mut task_index = 0;
        stack.push_back(source_status);

        // Get target base path for direction detection
        let target_base = Path::from_str(&job.info.target_path)?;

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

                let task_id = format!("{}_task_{}", job.info.job_id, task_index);
                task_index += 1;
                total_size += status.len;

                let task = LoadTaskInfo {
                    job: job.info.clone(),
                    task_id: task_id.clone(),
                    worker: worker.clone(),
                    source_path: source_path.clone_uri(),
                    target_path: target_path.clone_uri(),
                    create_time: LocalTime::mills() as i64,
                };
                job.add_task(task.clone());

                if job.tasks.len() > self.job_max_files {
                    return err_box!(
                        "Job {} files exceeds {}",
                        job.info.job_id,
                        self.job_max_files
                    );
                }
                debug!(
                    "created sub-task {} ({} -> {})",
                    task_id,
                    source_path.full_path(),
                    target_path.full_path()
                );
            }
        }

        Ok(total_size)
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
                self.jobs.update_state(
                    job_id,
                    JobTaskState::Canceled,
                    format!("failed to send cancel request to worker {}: {}", worker, e),
                );
            }
        }

        Ok(())
    }
}
