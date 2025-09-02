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

use crate::master::fs::policy::ChooseContext;
use crate::master::fs::MasterFilesystem;
use crate::master::{JobContext, JobStore, JobWorkerClient, MountManager};
use core::time::Duration;
use curvine_client::unified::UfsFileSystem;
use curvine_common::conf::{ClientConf, ClusterConf};
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    FileStatus, JobStatus, JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobResult,
    LoadTaskInfo, MountInfo, WorkerAddress,
};
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::client::ClientFactory;
use orpc::common::{ByteUnit, LocalTime, Utils};
use orpc::err_box;
use orpc::io::net::InetAddr;
use orpc::runtime::{RpcRuntime, Runtime};
use std::collections::linked_list::LinkedList;
use std::sync::Arc;

/// Load the Task Manager
pub struct JobManager {
    rt: Arc<Runtime>,
    jobs: JobStore,
    master_fs: MasterFilesystem,
    client_factory: Arc<ClientFactory>,
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
        let client_factory = Arc::new(ClientFactory::with_rt(
            conf.client.client_rpc_conf(),
            rt.clone(),
        ));

        Self {
            rt,
            jobs: JobStore::new(),
            master_fs,
            client_factory,
            mount_manager,
            job_life_ttl: conf.job.job_life_ttl,
            job_cleanup_ttl: conf.job.job_cleanup_ttl,
            job_max_files: conf.job.job_max_files,
        }
    }

    /// Start the job manager
    pub fn start(&self) {
        let cleanup_interval = self.job_cleanup_ttl;
        let ttl_ms = self.job_life_ttl.as_millis() as i64;

        let jobs = self.jobs.clone();
        self.rt.spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                Self::cleanup_expired_jobs(jobs.clone(), ttl_ms);
            }
        });
        info!("JobManager started");
    }

    fn create_job_id(source: impl AsRef<str>) -> String {
        format!("job_{}", Utils::murmur3(source.as_ref().as_bytes()))
    }

    fn update_state(&self, job_id: &str, state: JobTaskState, message: impl Into<String>) {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            job.update_state(state, message);
        }
    }

    async fn get_worker_client(&self, worker: &WorkerAddress) -> FsResult<JobWorkerClient> {
        let worker_addr = InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);

        let client = self.client_factory.get(&worker_addr).await?;
        let timeout = Duration::from_millis(self.client_factory.conf().rpc_timeout_ms);
        let client = JobWorkerClient::new(client, timeout);
        Ok(client)
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

    fn check_job_exists(
        &self,
        job_id: &str,
        source_status: &FileStatus,
        target_path: &Path,
    ) -> bool {
        if source_status.is_dir {
            // For directories, if a task already exists and is in loading state, duplicate submission is not allowed.
            if let Some(job) = self.jobs.get(job_id) {
                let state: JobTaskState = job.state.state();
                state == JobTaskState::Pending || state == JobTaskState::Loading
            } else {
                false
            }
        } else {
            // Files are generally auto-loaded and executed in parallel. Validate ufs_mtime to prevent distributing a large number of duplicate tasks.
            if let Ok(cv_status) = self.master_fs.file_status(target_path.path()) {
                if cv_status.storage_policy.ufs_mtime == 0 {
                    false
                } else {
                    cv_status.storage_policy.ufs_mtime == source_status.mtime
                }
            } else {
                false
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
            err_box!("Not fond job {}", job_id)
        }
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

        // Send a cancel request to all assigned Workers
        for worker in assigned_workers {
            let res = self.rt.block_on(async {
                let client = self.get_worker_client(&worker).await?;
                client.cancel_job(&job_id).await
            });

            // Just print logs.
            if let Err(e) = res {
                error!(
                    "Failed to send cancel load request to worker{}: {}",
                    worker, e
                );
                self.update_state(
                    job_id,
                    JobTaskState::Canceled,
                    format!(
                        "Failed to send cancel load request to worker {}: {}",
                        worker, e
                    ),
                );
            }
        }

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

        let target_path = mnt.get_cv_path(&source_path)?;
        let job_id = Self::create_job_id(source_path.full_path());
        let result = LoadJobResult {
            job_id: job_id.clone(),
            target_path: target_path.clone_path(),
        };

        let ufs = UfsFileSystem::with_mount(&mnt)?;
        let source_status = self.rt.block_on(ufs.get_status(&source_path))?;

        // check job status
        if self.check_job_exists(&job_id, &source_status, &target_path) {
            info!("job {} already exists", job_id);
            return Ok(result);
        }

        info!("Submitting load job {}", job_id);
        let mut job_context = JobContext::with_conf(
            &command,
            job_id.clone(),
            source_path.clone_uri(),
            target_path.clone_path(),
            &mnt,
            &ClientConf::default(),
        );

        let res =
            self.rt
                .block_on(self.create_all_tasks(&mut job_context, source_status, &ufs, &mnt));

        match res {
            Err(e) => {
                warn!("Submit load job {} failed: {}", job_id, e);
                // @todo Whether to cancel some tasks that may have been dispatched.
                Err(e)
            }

            Ok(size) => {
                info!(
                    "Submit load job {} success, tasks {}, total_size {}",
                    job_id,
                    job_context.tasks.len(),
                    ByteUnit::byte_to_string(size as u64)
                );
                self.jobs.insert(job_id, job_context);
                Ok(result)
            }
        }
    }

    async fn create_all_tasks(
        &self,
        job: &mut JobContext,
        source_status: FileStatus,
        ufs: &UfsFileSystem,
        mnt: &MountInfo,
    ) -> FsResult<i64> {
        job.update_state(JobTaskState::Pending, "Assigning workers");
        let block_size = job.info.block_size;

        let mut total_size = 0;
        let mut stack = LinkedList::new();
        let mut task_index = 0;
        stack.push_back(source_status);
        while let Some(status) = stack.pop_front() {
            if status.is_dir {
                let dir_path = Path::from_str(status.path)?;
                let childs = ufs.list_status(&dir_path).await?;
                for child in childs {
                    stack.push_back(child);
                }
            } else {
                let worker = self.choose_worker(block_size)?;

                let source_path = Path::from_str(status.path)?;
                let target_path = mnt.get_cv_path(&source_path)?;

                let task_id = format!("{}_task_{}", job.info.job_id, task_index);
                task_index += 1;
                total_size += status.len;

                let task = LoadTaskInfo {
                    job: job.info.clone(),
                    task_id: task_id.clone(),
                    worker: worker.clone(),
                    source_path: source_path.clone_uri(),
                    target_path: target_path.clone_path(),
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

                let client = self.get_worker_client(&worker).await?;
                client.submit_load_task(task).await?;
                info!("Added sub-task {}", task_id);
            }
        }

        Ok(total_size)
    }

    fn cleanup_expired_jobs(jobs: JobStore, ttl_ms: i64) {
        // Collect tasks that need to be removed first
        let mut jobs_to_remove = vec![];
        let now = LocalTime::mills() as i64;

        for entry in jobs.iter() {
            let job = entry.value();
            if ttl_ms + job.info.create_time > now {
                jobs_to_remove.push(job.info.job_id.clone());
            }
        }

        for job_id in jobs_to_remove {
            if jobs.remove(&job_id).is_some() {
                info!("Removing expired job: {}", job_id);
            }
        }
    }
}
