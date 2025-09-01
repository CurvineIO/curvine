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

use curvine_common::conf::ClientConf;
use curvine_common::state::{
    JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobInfo, LoadTaskInfo, MountInfo,
    WorkerAddress,
};
use curvine_common::FsResult;
use orpc::common::{FastHashMap, FastHashSet, LocalTime};
use orpc::err_box;
use orpc::sync::StateCtl;

pub struct TaskDetail {
    pub task: LoadTaskInfo,
    pub progress: JobTaskProgress,
}

impl TaskDetail {
    pub fn new(task: LoadTaskInfo) -> Self {
        Self {
            task,
            progress: JobTaskProgress::default(),
        }
    }
}

pub struct JobContext {
    pub info: LoadJobInfo,
    pub state: StateCtl,
    pub progress: JobTaskProgress,
    pub assigned_workers: FastHashSet<WorkerAddress>,
    pub tasks: FastHashMap<String, TaskDetail>,
}

impl JobContext {
    pub fn with_conf(
        job_conf: &LoadJobCommand,
        job_id: String,
        source_path: String,
        target_path: String,
        mnt: &MountInfo,
        client_conf: &ClientConf,
    ) -> Self {
        let replicas = job_conf
            .replicas
            .unwrap_or(mnt.replicas.unwrap_or(client_conf.replicas));

        let block_size = job_conf
            .block_size
            .unwrap_or(mnt.block_size.unwrap_or(client_conf.block_size));

        let storage_type = job_conf
            .storage_type
            .unwrap_or(mnt.storage_type.unwrap_or(client_conf.storage_type));

        let ttl_ms = job_conf.ttl_ms.unwrap_or(mnt.ttl_ms);

        let ttl_action = job_conf.ttl_action.unwrap_or(mnt.ttl_action);

        let job = LoadJobInfo {
            job_id,
            source_path,
            target_path,
            replicas,
            block_size,
            storage_type,
            ttl_ms,
            ttl_action,
            ufs_conf: mnt.properties.clone(),
            create_time: LocalTime::mills() as i64,
        };

        JobContext {
            info: job,
            state: StateCtl::new(JobTaskState::Pending.into()),
            progress: Default::default(),
            assigned_workers: Default::default(),
            tasks: Default::default(),
        }
    }

    pub fn add_task(&mut self, task: LoadTaskInfo) {
        self.update_state(
            JobTaskState::Loading,
            format!("Assigned to worker {}", task.worker),
        );
        self.assigned_workers.insert(task.worker.clone());
        self.tasks
            .insert(task.task_id.clone(), TaskDetail::new(task));
    }

    pub fn update_state(&mut self, state: JobTaskState, message: impl Into<String>) {
        self.state.set_state(state);
        self.progress.update_time = LocalTime::mills() as i64;
        self.progress.message = message.into();
    }

    pub fn update_progress(
        &mut self,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        let task_id = task_id.as_ref();
        let detail = if let Some(v) = self.tasks.get_mut(task_id) {
            v
        } else {
            return err_box!("Not fond task {}", task_id);
        };
        // set task progress
        detail.progress = progress;

        // check job status
        let mut total_size: i64 = 0;
        let mut loaded_size: i64 = 0;
        let mut complete: usize = 0;
        let mut job_state: JobTaskState = self.state.state();
        let mut message = "".to_string();

        for detail in self.tasks.values() {
            total_size += detail.progress.total_size;
            loaded_size += detail.progress.loaded_size;
            match detail.progress.state {
                JobTaskState::Completed => complete += 1,
                JobTaskState::Failed => {
                    job_state = JobTaskState::Failed;
                    message = format!(
                        "task {} failed: {}",
                        detail.task.task_id, detail.progress.message
                    )
                }
                _ => (),
            }
        }

        if complete == self.tasks.len() {
            job_state = JobTaskState::Completed;
            message = "All subtasks completed".into();
        }

        self.update_state(job_state, message);
        self.progress.loaded_size = loaded_size;
        self.progress.total_size = total_size;

        Ok(())
    }
}
