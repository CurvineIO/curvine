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
    ///
    /// Negating the value first would overflow for the `i64::MIN` a report may
    /// carry, so the two directions use their own saturating operation.
    fn apply(&mut self, progress: &JobTaskProgress, sign: i64) {
        if sign >= 0 {
            self.total_size = self.total_size.saturating_add(progress.total_size);
            self.loaded_size = self.loaded_size.saturating_add(progress.loaded_size);
        } else {
            self.total_size = self.total_size.saturating_sub(progress.total_size);
            self.loaded_size = self.loaded_size.saturating_sub(progress.loaded_size);
        }
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
        let task_id = task.task_id.clone();
        self.insert_task(task_id, TaskDetail::new(task));
    }

    pub fn add_task_detail(&mut self, task_id: String, detail: TaskDetail) {
        self.insert_task(task_id, detail);
    }

    /// Both insertion paths meet here, so the totals stay a sum over `tasks`
    /// even when a task id is replaced: the displaced task's contribution is
    /// withdrawn first, or a job with one replaced task could report Completed
    /// while the replacement is still pending.
    fn insert_task(&mut self, task_id: String, detail: TaskDetail) {
        self.update_state(
            JobTaskState::Loading,
            format!("Assigned to worker {}", detail.task.worker),
        );
        self.assigned_workers.insert(detail.task.worker.clone());
        if let Some(previous) = self.tasks.get(&task_id) {
            let previous = previous.progress.clone();
            self.totals.apply(&previous, -1);
        }
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
    fn a_replaced_task_withdraws_its_old_contribution() {
        let mut job = job_context(2);
        job.update_progress("task-0", progress(JobTaskState::Completed, 100, 100))
            .unwrap();

        // Re-insert task-0 (a retry hands it out again): its completed report
        // must stop counting, or the job looks finished while the replacement
        // has not run yet.
        let replacement = LoadTaskInfo {
            job: job.info.clone(),
            task_id: "task-0".to_string(),
            worker: WorkerAddress {
                worker_id: 2,
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
        job.add_task_detail("task-0".to_string(), TaskDetail::new(replacement));

        assert_eq!(
            job.totals.completed, 0,
            "the replaced task is not completed"
        );
        assert_eq!(job.totals.total_size, 0, "the replaced plan is withdrawn");
        assert_eq!(job.tasks.len(), 2, "the replacement reused the task slot");

        job.update_progress("task-1", progress(JobTaskState::Completed, 100, 100))
            .unwrap();
        let state: JobTaskState = job.state.state();
        assert_ne!(
            state,
            JobTaskState::Completed,
            "one task is still pending, so the job is not done"
        );

        job.update_progress("task-0", progress(JobTaskState::Completed, 100, 100))
            .unwrap();
        let state: JobTaskState = job.state.state();
        assert_eq!(state, JobTaskState::Completed);
        assert_eq!(job.totals.completed, 2);
        assert_eq!(job.totals.loaded_size, 200, "no contribution counted twice");
    }

    #[test]
    fn an_extreme_byte_count_does_not_overflow_the_withdrawal() {
        let mut job = job_context(1);
        // A report may carry `i64::MIN`; withdrawing it must not negate it.
        job.update_progress(
            "task-0",
            progress(JobTaskState::Loading, i64::MIN, i64::MIN),
        )
        .unwrap();
        job.update_progress("task-0", progress(JobTaskState::Completed, 10, 10))
            .unwrap();
        assert_eq!(job.totals.loaded_size, 10);
        assert_eq!(job.totals.total_size, 10);
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

/// Micro-benchmark for the report path: `cargo test --release -p curvine-master
/// --lib update_progress_cost -- --ignored --nocapture`.
#[cfg(test)]
mod update_progress_bench {
    use super::{JobContext, TaskDetail};
    use curvine_config::ClientConf;
    use curvine_model::{
        JobTaskProgress, JobTaskState, JobTaskType, LoadJobCommand, LoadTaskInfo, MountInfo,
        WorkerAddress,
    };
    use std::time::Instant;

    #[test]
    #[ignore = "micro-benchmark, run explicitly"]
    fn update_progress_cost() {
        const TASKS: usize = 300_000;
        const REPORTS: usize = 200;

        let mut job = JobContext::with_conf(
            &LoadJobCommand::builder("s3://bucket".to_string()).build(),
            "job-bench".to_string(),
            "s3://bucket".to_string(),
            "/cv".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
            1,
            JobTaskType::Load,
        );
        let worker = WorkerAddress {
            worker_id: 1,
            hostname: "localhost".to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 8997,
            web_port: 0,
        };
        for i in 0..TASKS {
            let task_id = format!("task-{i}");
            let task = LoadTaskInfo {
                job: job.info.clone(),
                task_id: task_id.clone(),
                worker: worker.clone(),
                source_path: "s3://bucket/file".to_string(),
                target_path: "/cv/file".to_string(),
                create_time: 0,
                source_read_plan_json: String::new(),
                transfer_report: None,
            };
            job.add_task_detail(task_id, TaskDetail::new(task));
        }

        let ids: Vec<String> = job.tasks.keys().take(REPORTS).cloned().collect();
        let started = Instant::now();
        for (i, id) in ids.iter().enumerate() {
            let progress = JobTaskProgress {
                state: JobTaskState::Loading,
                loaded_size: i as i64,
                total_size: 1024,
                update_time: 1,
                message: String::new(),
            };
            job.update_progress(id, progress).unwrap();
        }
        let elapsed = started.elapsed();
        println!(
            "{} reports over {} tasks: {:?} total, {:.1} us/report",
            REPORTS,
            TASKS,
            elapsed,
            elapsed.as_secs_f64() * 1e6 / REPORTS as f64
        );
    }
}
