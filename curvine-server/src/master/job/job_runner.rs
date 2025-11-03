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
use curvine_common::conf::ClientConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    FileStatus, JobTaskState, LoadJobCommand, LoadJobResult, LoadTaskInfo, MountInfo, WorkerAddress,
};
use curvine_common::FsResult;
use futures::future;
use log::{error, info, warn};
use orpc::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime, Utils};
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

    fn create_job_id(source: impl AsRef<str>) -> String {
        format!("job_{}", Utils::md5(source))
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
        let job = if let Some(job) = self.jobs.get(job_id) {
            job
        } else {
            return false;
        };

        let state: JobTaskState = job.state.state();
        if state == JobTaskState::Pending || state == JobTaskState::Loading {
            return true;
        }

        if !source_status.is_dir {
            // Files are generally auto-loaded and executed in parallel.
            // Validate ufs_mtime to prevent distributing a large number of duplicate tasks.
            if let Ok(cv_status) = self.master_fs.file_status(target_path.path()) {
                if cv_status.is_expired() || !cv_status.is_complete {
                    false
                } else {
                    source_status.len == cv_status.len
                        && cv_status.storage_policy.ufs_mtime != 0
                        && cv_status.storage_policy.ufs_mtime == source_status.mtime
                }
            } else {
                false
            }
        } else {
            true
        }
    }

    pub async fn submit_load_task(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
    ) -> FsResult<LoadJobResult> {
        let source_path = Path::from_str(&command.source_path)?;

        let target_path = if let Some(ref target) = command.target_path {
            Path::from_str(target)?
        } else if source_path.is_cv() {
            mnt.get_ufs_path(&source_path)?
        } else {
            mnt.get_cv_path(&source_path)?
        };

        let job_id = Self::create_job_id(source_path.full_path());
        let result = LoadJobResult {
            job_id: job_id.clone(),
            target_path: target_path.clone_path(),
        };

        let source_status = if source_path.is_cv() {
            self.master_fs.file_status(source_path.path())?
        } else {
            let ufs = self.factory.get_ufs(&mnt)?;
            ufs.get_status(&source_path).await?
        };

        if self.check_job_exists(&job_id, &source_status, &target_path) {
            info!(
                "job {}, source_path {} already exists",
                job_id,
                source_path.full_path()
            );
            return Ok(result);
        }

        info!("Submitting load job {}", job_id);
        let mut job_context = JobContext::with_conf(
            &command,
            job_id.clone(),
            source_path.clone_uri(),
            target_path.clone_uri(),
            &mnt,
            &ClientConf::default(),
        );

        let res = self
            .create_all_tasks(&mut job_context, source_status, &mnt)
            .await;

        match res {
            Err(e) => {
                warn!("Create load job {} failed: {}", job_id, e);
                Err(e)
            }

            Ok(size) => {
                info!(
                    "Submit load job {} success, tasks {}, total_size {}",
                    job_id,
                    job_context.tasks.len(),
                    ByteUnit::byte_to_string(size as u64)
                );

                let tasks = job_context.tasks.clone();
                self.jobs.insert(job_id, job_context);
                // @todo Whether to cancel some tasks that may have been dispatched.
                self.submit_all_task(tasks).await?;

                Ok(result)
            }
        }
    }

    async fn submit_all_task(&self, tasks: FastHashMap<String, TaskDetail>) -> FsResult<()> {
        let submit_futures: Vec<_> = tasks
            .take()
            .into_iter()
            .map(|(id, task)| async move {
                let client = self.factory.get_worker_client(&task.task.worker).await?;
                client.submit_load_task(task.task).await?;
                info!("Submit sub-task {}", id);
                Ok::<(), FsError>(())
            })
            .collect();

        future::try_join_all(submit_futures).await?;
        Ok(())
    }

    async fn create_all_tasks(
        &self,
        job: &mut JobContext,
        source_status: FileStatus,
        mnt: &MountInfo,
    ) -> FsResult<i64> {
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
                info!("Added sub-task {}", task_id);
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
                error!(
                    "Failed to send cancel load request to worker{}: {}",
                    worker, e
                );
                self.jobs.update_state(
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
}
