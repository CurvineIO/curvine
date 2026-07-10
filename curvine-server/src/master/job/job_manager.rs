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
use crate::master::{JobStore, LoadJobRunner, MountManager};
use core::time::Duration;
use curvine_client::unified::MountValue;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::executor::ScheduledExecutor;
use curvine_common::fs::Path;
use curvine_common::state::{
    JobStatus, JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobResult,
};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::LocalTime;
use orpc::runtime::{LoopTask, RpcRuntime, Runtime};
use orpc::sync::AtomicCounter;
use orpc::{err_box, err_ext, CommonResult};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::timeout;

/// Load the Task Manager
pub struct JobManager {
    rt: Arc<Runtime>,
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    mount_manager: Arc<MountManager>,
    job_life_ttl: Duration,
    job_cleanup_ttl: Duration,
    job_max_files: usize,
    run_seq: Arc<AtomicCounter>,
    load_job_semaphore: Arc<Semaphore>,
}

impl JobManager {
    pub fn from_cluster_conf(
        master_fs: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        rt: Arc<Runtime>,
        conf: &ClusterConf,
    ) -> Self {
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));

        Self {
            rt,
            jobs: JobStore::new(),
            master_fs,
            factory,
            mount_manager,
            job_life_ttl: conf.job.job_life_ttl,
            job_cleanup_ttl: conf.job.job_cleanup_ttl,
            job_max_files: conf.job.job_max_files,
            run_seq: Arc::new(AtomicCounter::new(0)),
            load_job_semaphore: Arc::new(Semaphore::new(conf.job.master_max_concurrent_load_jobs)),
        }
    }

    /// Start the job manager
    pub fn start(&self) -> CommonResult<()> {
        let cleanup_interval = self.job_cleanup_ttl.as_millis() as u64;
        let ttl_ms = self.job_life_ttl.as_millis() as i64;

        let executor = ScheduledExecutor::new("job_cleanup", cleanup_interval);
        executor.start(JobCleanupTask {
            jobs: self.jobs.clone(),
            ttl_ms,
        })?;

        info!("JobManager started");
        Ok(())
    }

    fn update_state(
        &self,
        job_id: &str,
        state: JobTaskState,
        message: impl Into<String>,
    ) -> FsResult<()> {
        self.jobs.update_state(job_id, state, message)
    }

    pub async fn wait_job_complete(
        &self,
        job_id: impl AsRef<str>,
        duration: Duration,
    ) -> FsResult<JobStatus> {
        timeout(duration, self.wait_job_complete0(job_id)).await?
    }

    async fn wait_job_complete0(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        let job_id = job_id.as_ref();

        let mut listener = match self.jobs.get(job_id) {
            Some(job) => job.new_listener(),
            None => return err_ext!(FsError::job_not_found(job_id)),
        };

        let status = self.get_job_status(job_id)?;
        if status.state.is_finish() {
            return Ok(status);
        }

        loop {
            let next_state = JobTaskState::from(listener.next_state().await?);
            if next_state.is_finish() {
                return self.get_job_status(job_id);
            }
        }
    }

    pub fn get_job_status(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        let job_id = job_id.as_ref();
        if let Some(job) = self.jobs.get(job_id) {
            Ok(JobStatus {
                job_id: job.info.job_id.clone(),
                state: job.state.state(),
                source_path: job.info.source_path.clone(),
                target_path: job.info.target_path.clone(),
                progress: job.progress.clone(),
            })
        } else {
            err_ext!(FsError::job_not_found(job_id))
        }
    }

    pub fn create_runner(&self) -> LoadJobRunner {
        LoadJobRunner::new(
            self.jobs.clone(),
            self.master_fs.clone(),
            self.factory.clone(),
            self.job_max_files,
            self.run_seq.clone(),
        )
    }

    pub fn get_mnt(&self, path: &Path) -> FsResult<Option<(Path, Arc<MountValue>)>> {
        if let Some(mnt) = self.mount_manager.get_mount_info(path)? {
            let mnt_value = self.factory.get_mnt(&mnt)?;
            let target_path = mnt_value.toggle_path(path)?;

            Ok(Some((target_path, mnt_value)))
        } else {
            Ok(None)
        }
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    /// See `LoadJobRunner::submit_load_task` for the concurrency contract: concurrent
    /// submits for the same path while a load is running return the **existing** run’s
    /// result; the new command’s options are not applied (first submitter wins).
    pub async fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        let source_path = Path::from_str(&command.source_path)?;

        // Check mount info for both UFS and CV paths. Public load jobs import
        // from UFS into Curvine; CV paths are accepted only when existing
        // metadata marks them as UFS-only.
        let mnt = if let Some(mnt) = self.mount_manager.get_mount_info(&source_path)? {
            mnt
        } else {
            return err_box!("Not found mount info for path: {}", source_path);
        };

        let job_runner = self.create_runner();
        let (res, queued) = job_runner.enqueue_load_job(command, mnt)?;

        if let Some(queued) = queued {
            let runner = self.create_runner();
            let job_id = res.job_id.clone();
            let semaphore = self.load_job_semaphore.clone();
            let jobs = self.jobs.clone();
            self.rt.spawn(async move {
                let _permit = match semaphore.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(err) => {
                        warn!(
                            "async load job {} failed to acquire permit: {}",
                            job_id, err
                        );
                        jobs.update_state_if_run(
                            &queued.job_id,
                            queued.run_id,
                            JobTaskState::Failed,
                            format!("async load job failed to acquire permit: {}", err),
                        );
                        return;
                    }
                };

                if let Err(err) = runner.run_queued_load_job(queued).await {
                    warn!("async load job {} failed: {}", job_id, err);
                }
            });
        }

        Ok(res)
    }

    /// Handle cancellation of tasks
    pub async fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let assigned_workers = {
            if let Some(job) = self.jobs.get(job_id) {
                let state: JobTaskState = job.state.state();
                // Check whether it can be canceled
                if state == JobTaskState::Completed
                    || state == JobTaskState::Failed
                    || state == JobTaskState::Canceled
                {
                    info!(
                        "job {} is already in final state {:?}, source_path: {}, target_path: {}",
                        job_id, state, job.info.source_path, job.info.target_path
                    );
                    return Ok(());
                }

                job.assigned_workers.clone()
            } else {
                return err_ext!(FsError::job_not_found(job_id));
            }
        };

        self.update_state(job_id, JobTaskState::Canceled, "Canceling job by user")?;

        let job_runner = self.create_runner();
        job_runner.cancel_job(&job_id, assigned_workers).await?;

        Ok(())
    }

    pub fn update_progress(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        self.jobs.update_progress(job_id, task_id, progress)
    }

    pub fn jobs(&self) -> &JobStore {
        &self.jobs
    }

    pub fn factory(&self) -> &Arc<UfsFactory> {
        &self.factory
    }
}

struct JobCleanupTask {
    jobs: JobStore,
    ttl_ms: i64,
}

impl LoopTask for JobCleanupTask {
    type Error = FsError;

    fn run(&self) -> Result<(), Self::Error> {
        // Collect tasks that need to be removed first
        let mut jobs_to_remove = vec![];
        let now = LocalTime::mills() as i64;
        for entry in self.jobs.iter() {
            let job = entry.value();
            if now > self.ttl_ms + job.info.create_time {
                jobs_to_remove.push(job.info.job_id.clone());
            }
        }

        for job_id in jobs_to_remove {
            if let Some(v) = self.jobs.remove(&job_id) {
                debug!("Removing expired job: {:?}", v.1.info);
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}
