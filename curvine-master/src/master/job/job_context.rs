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
}

impl TaskDetail {
    pub fn new(task: LoadTaskInfo) -> Self {
        Self {
            task,
            progress: JobTaskProgress::default(),
        }
    }
}

/// Running totals over `tasks`.
///
/// Kept incrementally so applying one task report is O(1): a large load job has
/// millions of tasks, and re-scanning them for every report (tens per second)
/// pins a master core and backlogs reports, which in turn makes every task's
/// `update_time` stale.
#[derive(Debug, Clone, Default)]
pub struct ProgressTotals {
    pub total_size: i64,
    pub loaded_size: i64,
    pub completed: usize,
    pub failed: usize,
}

impl ProgressTotals {
    /// `sign` is `-1` to withdraw an old progress value and `1` to add a new one.
    fn apply(&mut self, progress: &JobTaskProgress, sign: i64) {
        self.total_size += sign * progress.total_size;
        self.loaded_size += sign * progress.loaded_size;
        let state: JobTaskState = progress.state;
        let delta: isize = sign as isize;
        match state {
            JobTaskState::Completed => {
                self.completed = self.completed.saturating_add_signed(delta);
            }
            JobTaskState::Failed => {
                self.failed = self.failed.saturating_add_signed(delta);
            }
            _ => (),
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
    pub totals: ProgressTotals,
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
            totals: Default::default(),
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
        debug_assert!(
            !self.tasks.contains_key(&task_id),
            "task {} added twice to job {}",
            task_id,
            self.info.job_id
        );
        self.update_state(
            JobTaskState::Loading,
            format!("Assigned to worker {}", detail.task.worker),
        );
        self.assigned_workers.insert(detail.task.worker.clone());
        // A fresh task reports nothing yet, but account it anyway so the totals
        // stay a sum over every task in `tasks`.
        self.totals.apply(&detail.progress, 1);
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

        // Withdraw the previous contribution, apply the new one: O(1) per report
        // instead of a scan of every task in the job.
        let previous = detail.progress.clone();
        self.totals.apply(&previous, -1);
        detail.progress = progress.clone();
        self.totals.apply(&progress, 1);

        let total_size = self.totals.total_size;
        let loaded_size = self.totals.loaded_size;
        let complete = self.totals.completed;

        // check job status
        let mut job_state: JobTaskState = self.state.state();
        let mut message = "".to_string();

        if self.totals.failed > 0 {
            job_state = JobTaskState::Failed;
            message = if progress.state == JobTaskState::Failed {
                format!("task {} failed: {}", detail.task.task_id, progress.message)
            } else {
                // Another task failed earlier; keep the message that named it.
                self.progress.message.clone()
            };
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

#[cfg(test)]
mod totals_tests {
    use super::{JobContext, TaskDetail};
    use curvine_config::ClientConf;
    use curvine_model::{
        JobTaskProgress, JobTaskState, JobTaskType, LoadJobCommand, LoadTaskInfo, MountInfo,
        WorkerAddress,
    };
    use std::sync::Arc;

    fn job_context(tasks: usize) -> JobContext {
        let mut job = JobContext::with_conf(
            &LoadJobCommand::builder("s3://bucket".to_string()).build(),
            "job-1".to_string(),
            "s3://bucket".to_string(),
            "/cv".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
            1,
            JobTaskType::Load,
        );
        for i in 0..tasks {
            let task_id = format!("task-{i}");
            let task = LoadTaskInfo {
                job: Arc::new(job.info.clone()).as_ref().clone(),
                task_id: task_id.clone(),
                worker: WorkerAddress {
                    worker_id: 1,
                    hostname: "localhost".to_string(),
                    ip_addr: "127.0.0.1".to_string(),
                    rpc_port: 8997,
                    web_port: 0,
                },
                source_path: "s3://bucket/file".to_string(),
                target_path: "/cv/file".to_string(),
                create_time: 0,
                source_read_plan_json: String::new(),
                transfer_report: None,
            };
            job.add_task_detail(task_id, TaskDetail::new(task));
        }
        job
    }

    fn progress(state: JobTaskState, loaded: i64, total: i64) -> JobTaskProgress {
        JobTaskProgress {
            state,
            loaded_size: loaded,
            total_size: total,
            update_time: 1,
            message: String::new(),
        }
    }

    #[test]
    fn totals_track_reports_without_a_scan() {
        let mut job = job_context(2);
        assert_eq!(job.totals.total_size, 0);

        job.update_progress("task-0", progress(JobTaskState::Loading, 10, 100))
            .unwrap();
        assert_eq!(job.totals.loaded_size, 10);
        assert_eq!(job.totals.total_size, 100);
        assert_eq!(job.progress.loaded_size, 10);

        // A second report for the same task replaces its contribution instead of
        // adding to it.
        job.update_progress("task-0", progress(JobTaskState::Completed, 100, 100))
            .unwrap();
        assert_eq!(job.totals.loaded_size, 100);
        assert_eq!(job.totals.completed, 1);
        assert_eq!(job.totals.total_size, 100);
    }

    #[test]
    fn the_job_completes_when_every_task_does() {
        let mut job = job_context(2);
        job.update_progress("task-0", progress(JobTaskState::Completed, 100, 100))
            .unwrap();
        let state: JobTaskState = job.state.state();
        assert_ne!(state, JobTaskState::Completed);

        job.update_progress("task-1", progress(JobTaskState::Completed, 100, 100))
            .unwrap();
        let state: JobTaskState = job.state.state();
        assert_eq!(state, JobTaskState::Completed);
        assert_eq!(job.totals.completed, 2);
    }

    #[test]
    fn one_failure_fails_the_job_and_keeps_the_reason() {
        let mut job = job_context(2);
        job.update_progress(
            "task-0",
            JobTaskProgress {
                message: "boom".to_string(),
                ..progress(JobTaskState::Failed, 0, 100)
            },
        )
        .unwrap();
        let state: JobTaskState = job.state.state();
        assert_eq!(state, JobTaskState::Failed);
        assert!(job.progress.message.contains("boom"));
        assert_eq!(job.totals.failed, 1);

        // A later healthy report must not clear the failure message.
        job.update_progress("task-1", progress(JobTaskState::Loading, 50, 100))
            .unwrap();
        let state: JobTaskState = job.state.state();
        assert_eq!(state, JobTaskState::Failed);
        assert!(
            job.progress.message.contains("boom"),
            "the failure reason was lost: {}",
            job.progress.message
        );
    }
}
