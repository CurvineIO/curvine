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
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::executor::ScheduledExecutor;
use curvine_common::fs::Path;
use curvine_common::state::{
    JobStatus, JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobResult,
};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::LocalTime;
use orpc::err_box;
use orpc::runtime::{LoopTask, RpcRuntime, Runtime};
use orpc::sync::channel::BlockingChannel;
use std::sync::Arc;

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
        }
    }

    /// Start the job manager
    pub fn start(&self) {
        let cleanup_interval = self.job_cleanup_ttl.as_millis() as u64;
        let ttl_ms = self.job_life_ttl.as_millis() as i64;

        let executor = ScheduledExecutor::new("job_cleanup", cleanup_interval);
        executor
            .start(JobCleanupTask {
                jobs: self.jobs.clone(),
                ttl_ms,
            })
            .unwrap();

        info!("JobManager started");
    }

    fn update_state(&self, job_id: &str, state: JobTaskState, message: impl Into<String>) {
        self.jobs.update_state(job_id, state, message)
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
            err_box!("Not fond job {}", job_id)
        }
    }

    pub fn create_runner(&self) -> LoadJobRunner {
        LoadJobRunner::new(
            self.jobs.clone(),
            self.master_fs.clone(),
            self.factory.clone(),
            self.job_max_files,
        )
    }

    pub fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        let source_path = Path::from_str(&command.source_path)?;
        if source_path.is_cv() {
            return err_box!("No need to load cv path");
        }

        // check mount
        let mnt = if let Some(mnt) = self.mount_manager.get_mount_info(&source_path)? {
            mnt
        } else {
            return err_box!("Not found mount info for path: {}", source_path);
        };

        let job_runner = self.create_runner();

        let (tx, mut rx) = BlockingChannel::new(1).split();
        self.rt.spawn(async move {
            let res = job_runner.submit_load_task(command, mnt).await;
            if let Err(e) = tx.send(res) {
                warn!("send submit_load_job result: {}", e);
            }
        });

        rx.recv_check()?
    }

    /// Handle cancellation of tasks
    pub fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let assigned_workers = {
            if let Some(job) = self.jobs.get(job_id) {
                let state: JobTaskState = job.state.state();
                // Check whether it can be canceled
                if state == JobTaskState::Completed
                    || state == JobTaskState::Failed
                    || state == JobTaskState::Canceled
                {
                    return err_box!("Cannot cancel job in state {:?}", state);
                }

                job.assigned_workers.clone()
            } else {
                return err_box!("Job {} not found", job_id);
            }
        };

        self.update_state(job_id, JobTaskState::Canceled, "Canceling job by user");

        let job_runner = self.create_runner();
        let job_id = job_id.to_string();
        self.rt.spawn(async move {
            if let Err(e) = job_runner.cancel_job(&job_id, assigned_workers).await {
                warn!("Cancel job {} error: {}", job_id, e);
            }
        });

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
                info!("Removing expired job: {:?}", v.1.info);
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}
