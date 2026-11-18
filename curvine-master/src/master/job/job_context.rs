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

use curvine_config::ClientConf;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_model::{
    JobTaskProgress, JobTaskState, JobTaskType, LoadJobCommand, LoadJobInfo, LoadTaskInfo,
    MountInfo, WorkerAddress,
};
use curvine_runtime::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use curvine_runtime::sync::{StateListener, StateMonitor};
use log::{info, warn};

#[derive(Debug, Clone)]
pub struct TaskDetail {
    pub task: LoadTaskInfo,
    pub progress: JobTaskProgress,
    /// Worker session the task was accepted by. A restarted worker comes back on
    /// the same address with an empty task table, so the watchdog compares this
    /// against the worker's current session to notice that the task is gone.
    pub dispatched_session: String,
    /// When the submission to `task.worker` was accepted, `0` until then.
    ///
    /// The watchdog must not treat a task that is still waiting for its turn in
    /// the dispatch loop as stuck: a million-file plan is dispatched over hours,
    /// and only a dispatched task can be lost with its worker.
    pub dispatched_at: i64,
    /// Reports applied to this task so far. The watchdog judges a task from a
    /// snapshot, and a report can land before the failure is written: comparing
    /// this counter (instead of a millisecond timestamp) tells them apart.
    pub reports: u64,
}

impl TaskDetail {
    pub fn new(task: LoadTaskInfo) -> Self {
        Self {
            task,
            progress: JobTaskProgress::default(),
            dispatched_session: String::new(),
            dispatched_at: 0,
            reports: 0,
        }
    }
}

#[derive(Clone)]
pub struct JobContext {
    pub info: LoadJobInfo,
    /// `Export` when the job copies a Curvine source back to UFS (fs_mode),
    /// `Load` when it imports a UFS source into Curvine.
    pub kind: JobTaskType,
    pub run_id: u64,
    pub state: StateMonitor,
    pub progress: JobTaskProgress,
    pub assigned_workers: FastHashSet<WorkerAddress>,
    pub tasks: FastHashMap<String, TaskDetail>,
}

impl JobContext {
    #[allow(clippy::too_many_arguments)]
    pub fn with_conf(
        job_conf: &LoadJobCommand,
        job_id: String,
        source_path: String,
        target_path: String,
        mnt: &MountInfo,
        client_conf: &ClientConf,
        run_id: u64,
        kind: JobTaskType,
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
            mount_info: mnt.clone(),
            create_time: LocalTime::mills() as i64,
            overwrite: job_conf.overwrite,
        };

        JobContext {
            info: job,
            kind,
            run_id,
            state: StateMonitor::new(JobTaskState::Pending.into()),
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

    pub fn add_task_detail(&mut self, task_id: String, detail: TaskDetail) {
        self.update_state(
            JobTaskState::Loading,
            format!("Assigned to worker {}", detail.task.worker),
        );
        self.assigned_workers.insert(detail.task.worker.clone());
        self.tasks.insert(task_id, detail);
    }

    pub fn update_state(&mut self, state: JobTaskState, message: impl Into<String>) {
        self.state.advance_state(state, true);
        self.progress.update_time = LocalTime::mills() as i64;
        self.progress.message = message.into();
    }

    pub fn new_listener(&self) -> StateListener {
        self.state.new_listener()
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
            return err_box!("Task not found: {}", task_id);
        };
        // The report time comes from the worker's clock, which cannot be compared
        // with the master's dispatch time: a lagging worker clock looks like
        // silence and a fast one hides it. The watchdog measures silence from the
        // moment the master accepted the report, so stamp that here.
        detail.progress = JobTaskProgress {
            update_time: LocalTime::mills() as i64,
            ..progress
        };
        detail.reports = detail.reports.saturating_add(1);

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
                JobTaskState::UNKNOWN
                | JobTaskState::Pending
                | JobTaskState::Loading
                | JobTaskState::Canceled
                // Master update_progress still collapses mixed outcomes to Failed today;
                // PartialSuccess is emitted only on the transfer path for now.
                | JobTaskState::PartialSuccess => (),
            }
        }

        if complete == self.tasks.len() {
            job_state = JobTaskState::Completed;
            message = "All subtasks completed".into();
            info!(
                "job {} all subtasks completed, tasks {}, len = {}, cost {} ms",
                self.info.job_id,
                self.tasks.len(),
                ByteUnit::byte_to_string(loaded_size as u64),
                LocalTime::mills() as i64 - self.info.create_time
            )
        } else if job_state == JobTaskState::Failed {
            warn!(
                "job {} execute failed, tasks {}, len = {}, cost {} ms, error {}",
                self.info.job_id,
                self.tasks.len(),
                ByteUnit::byte_to_string(loaded_size as u64),
                LocalTime::mills() as i64 - self.info.create_time,
                message
            )
        }

        self.update_state(job_state, message);
        self.progress.loaded_size = loaded_size;
        self.progress.total_size = total_size;

        Ok(())
    }
}
