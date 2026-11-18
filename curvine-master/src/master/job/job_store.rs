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

use crate::master::{JobContext, Master};
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_model::{JobTaskProgress, JobTaskState};
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::FastDashMap;
use log::{debug, error, warn};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

pub type JobStateCallback =
    Arc<dyn Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync>;

pub struct JobCallback {
    callback: JobStateCallback,
    filter_states: Option<Vec<JobTaskState>>,
}

impl JobCallback {
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(callback),
            filter_states: None,
        }
    }

    pub fn with_filter(mut self, states: Vec<JobTaskState>) -> Self {
        self.filter_states = Some(states);
        self
    }

    pub fn should_trigger(&self, new_state: JobTaskState) -> bool {
        match &self.filter_states {
            None => true,
            Some(states) => states.contains(&new_state),
        }
    }
}

#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<FastDashMap<String, JobContext>>,
    callbacks: Arc<RwLock<HashMap<String, Vec<JobCallback>>>>,
}

impl Default for JobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl JobStore {
    pub fn new() -> Self {
        JobStore {
            jobs: Arc::new(FastDashMap::default()),
            callbacks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_callback(&self, job_id: String, callback: JobCallback) -> FsResult<()> {
        let mut callbacks = match self.callbacks.write() {
            Ok(callbacks) => callbacks,
            Err(e) => return err_box!("failed to register job callback for {}: {}", job_id, e),
        };
        callbacks.entry(job_id).or_default().push(callback);
        Ok(())
    }

    pub fn register_completion_callback<F>(&self, job_id: String, callback: F) -> FsResult<()>
    where
        F: Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync + 'static,
    {
        let cb = JobCallback::new(callback)
            .with_filter(vec![JobTaskState::Completed, JobTaskState::Failed]);
        self.register_callback(job_id, cb)
    }

    fn trigger_callbacks(
        &self,
        job_id: &str,
        old_state: JobTaskState,
        new_state: JobTaskState,
        job: &JobContext,
    ) -> FsResult<()> {
        let callbacks_guard = match self.callbacks.read() {
            Ok(callbacks) => callbacks,
            Err(e) => return err_box!("failed to trigger job callbacks for {}: {}", job_id, e),
        };
        if let Some(callbacks) = callbacks_guard.get(job_id) {
            for cb in callbacks {
                if cb.should_trigger(new_state) {
                    (cb.callback)(job_id, old_state, new_state, job);
                }
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
        self.apply_report(job_id.as_ref(), task_id.as_ref(), progress, None)
            .map(|_| ())
    }

    /// Apply one task report, optionally only if the task's report counter is
    /// still the one the caller observed.
    ///
    /// The check and the write happen under the same job guard, so a report that
    /// lands while the watchdog is judging cannot be overwritten by its verdict.
    fn apply_report(
        &self,
        job_id: &str,
        task_id: &str,
        progress: JobTaskProgress,
        expected_reports: Option<u64>,
    ) -> FsResult<bool> {
        let mut job = if let Some(job) = self.jobs.get_mut(job_id) {
            job
        } else {
            return err_box!("Job not found: {}", job_id);
        };

        let old_state: JobTaskState = job.state.state();
        if old_state.is_finish() {
            debug!(
                "ignore task report for terminal job {}, task {}, state {:?}",
                job_id, task_id, old_state
            );
            return Ok(false);
        }

        let previous = match job.tasks.get(task_id) {
            Some(detail) => detail.progress.clone(),
            None => {
                debug!(
                    "ignore stale task report for job {}, unknown task {}",
                    job_id, task_id
                );
                return Ok(false);
            }
        };

        // A finished task is final. A delayed report must not reopen it, or the
        // task would be counted as finished twice.
        if previous.state.is_finish() {
            debug!(
                "ignore task report for finished task {}, job {}, state {:?}",
                task_id, job_id, previous.state
            );
            return Ok(false);
        }

        // The verdict was formed from a snapshot: a report that arrived since
        // means the task is alive, whatever the sweep decided.
        if let Some(expected) = expected_reports {
            let detail = job.tasks.get(task_id);
            if detail.is_none_or(|detail| detail.reports != expected) {
                return Ok(false);
            }
        }

        job.update_progress(task_id, progress)?;
        Self::record_task_report(&job, task_id, &previous);

        let new_state: JobTaskState = job.state.state();

        if old_state != new_state {
            Self::record_transition(old_state, new_state, &job);
            let job_id_owned = job_id.to_string();
            let job_clone = (*job).clone();
            drop(job);

            self.trigger_callbacks(&job_id_owned, old_state, new_state, &job_clone)?;
        }

        Ok(true)
    }

    /// Record that a task was accepted by `worker_session`.
    pub fn mark_dispatched(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        worker_session: &str,
    ) -> bool {
        let Some(mut job) = self.jobs.get_mut(job_id.as_ref()) else {
            return false;
        };
        let now = LocalTime::mills() as i64;
        match job.tasks.get_mut(task_id.as_ref()) {
            Some(detail) => {
                detail.dispatched_at = now;
                detail.dispatched_session = worker_session.to_string();
                true
            }
            None => false,
        }
    }

    pub fn update_state(
        &self,
        job_id: &str,
        state: JobTaskState,
        message: impl Into<String>,
    ) -> FsResult<()> {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            let old_state: JobTaskState = job.state.state();
            job.update_state(state, message);
            let new_state = state;

            if old_state != new_state {
                Self::record_transition(old_state, new_state, &job);
                let job_clone = (*job).clone();
                drop(job);

                self.trigger_callbacks(job_id, old_state, new_state, &job_clone)?;
            }
        }
        Ok(())
    }

    pub fn update_state_if_run(
        &self,
        job_id: &str,
        run_id: u64,
        state: JobTaskState,
        message: impl Into<String>,
    ) -> bool {
        let mut job = match self.jobs.get_mut(job_id) {
            Some(job) => job,
            None => return false,
        };

        let old_state: JobTaskState = job.state.state();
        if job.run_id != run_id || !old_state.is_running() {
            return false;
        }

        job.update_state(state, message);
        let new_state = state;

        if old_state != new_state {
            Self::record_transition(old_state, new_state, &job);
            let job_clone = (*job).clone();
            drop(job);

            if let Err(err) = self.trigger_callbacks(job_id, old_state, new_state, &job_clone) {
                error!(
                    "failed to trigger job callbacks for {} after run-scoped state update: {}",
                    job_id, err
                );
            }
        }

        true
    }

    /// Counts a run that just reached a terminal state. Leaving one terminal
    /// state for another (e.g. a dispatch failure after a cancel) is not a new
    /// finish.
    fn record_transition(old_state: JobTaskState, new_state: JobTaskState, job: &JobContext) {
        if old_state.is_finish() || !new_state.is_finish() {
            return;
        }
        if let Ok(metrics) = Master::get_metrics() {
            let elapsed_ms = (LocalTime::mills() as i64).saturating_sub(job.info.create_time);
            metrics.record_load_job_finished(job.kind, new_state, elapsed_ms.max(0) as u64);
        }
    }

    fn record_task_report(job: &JobContext, task_id: &str, previous: &JobTaskProgress) {
        let Some(detail) = job.tasks.get(task_id) else {
            return;
        };
        if let Ok(metrics) = Master::get_metrics() {
            metrics.record_load_task_report(job.kind, previous, &detail.progress);
        }
    }

    /// Fail a task only if it is still the silent one the sweep saw.
    ///
    /// The sweep collects candidates, then fails them one by one; a fresh report
    /// can arrive in between. The check and the write happen under the same job
    /// guard, so a late report wins instead of being overwritten with `Failed`.
    pub fn fail_task_if_stale(
        &self,
        job_id: &str,
        task_id: &str,
        observed_reports: u64,
        progress: JobTaskProgress,
    ) -> FsResult<bool> {
        self.apply_report(job_id, task_id, progress, Some(observed_reports))
    }

    pub fn remove_callbacks(&self, job_id: &str) -> FsResult<()> {
        let mut callbacks = match self.callbacks.write() {
            Ok(callbacks) => callbacks,
            Err(e) => {
                warn!(
                    "job callback registry was poisoned while removing callbacks for {}, recovering",
                    job_id
                );
                self.callbacks.clear_poison();
                e.into_inner()
            }
        };
        callbacks.remove(job_id);
        Ok(())
    }

    pub fn remove_job_if<F>(
        &self,
        job_id: &str,
        should_remove: F,
    ) -> FsResult<Option<(String, JobContext)>>
    where
        F: FnOnce(&JobContext) -> bool,
    {
        let removed = self.jobs.remove_if(job_id, |_, job| should_remove(job));
        if removed.is_some() {
            self.remove_callbacks(job_id)?;
        }
        Ok(removed)
    }
}

impl Deref for JobStore {
    type Target = FastDashMap<String, JobContext>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::job::TaskDetail;
    use curvine_config::ClientConf;
    use curvine_model::{JobTaskType, LoadJobCommand, LoadTaskInfo, MountInfo, WorkerAddress};

    #[test]
    fn a_late_report_wins_over_the_watchdog() {
        let source = "s3://bucket/a";
        let mut job = JobContext::with_conf(
            &LoadJobCommand::builder(source).build(),
            "job-1".to_string(),
            source.to_string(),
            "/target".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
            1,
            JobTaskType::Load,
        );
        for task_id in ["task-1", "task-2"] {
            job.add_task_detail(
                task_id.to_string(),
                TaskDetail::new(LoadTaskInfo {
                    job: job.info.clone(),
                    task_id: task_id.to_string(),
                    worker: WorkerAddress::default(),
                    source_path: source.to_string(),
                    target_path: "/target".to_string(),
                    create_time: 0,
                    source_read_plan_json: String::new(),
                    transfer_report: None,
                }),
            );
        }
        let jobs = JobStore::new();
        jobs.insert("job-1".to_string(), job);

        jobs.update_progress("job-1", "task-1", progress(JobTaskState::Loading, 10))
            .unwrap();
        let observed = jobs
            .get("job-1")
            .unwrap()
            .tasks
            .get("task-1")
            .unwrap()
            .reports;

        // A report arrives after the sweep collected the task: its report counter
        // moved on, so the failure must not be applied.
        jobs.update_progress("job-1", "task-1", progress(JobTaskState::Loading, 20))
            .unwrap();
        let applied = jobs
            .fail_task_if_stale(
                "job-1",
                "task-1",
                observed,
                JobTaskProgress {
                    state: JobTaskState::Failed,
                    message: "no progress".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(!applied, "a task that reported after the sweep is alive");
        let state: JobTaskState = jobs.get("job-1").unwrap().state.state();
        assert_eq!(state, JobTaskState::Loading, "the job must not fail");

        // With the task still silent, the failure lands.
        let observed = jobs
            .get("job-1")
            .unwrap()
            .tasks
            .get("task-1")
            .unwrap()
            .reports;
        let applied = jobs
            .fail_task_if_stale(
                "job-1",
                "task-1",
                observed,
                JobTaskProgress {
                    state: JobTaskState::Failed,
                    message: "no progress".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(applied);
        let state: JobTaskState = jobs.get("job-1").unwrap().state.state();
        assert_eq!(state, JobTaskState::Failed);
    }

    fn progress(state: JobTaskState, loaded_size: i64) -> JobTaskProgress {
        JobTaskProgress {
            state,
            loaded_size,
            total_size: 200,
            ..Default::default()
        }
    }

    #[test]
    fn delayed_report_does_not_reopen_a_finished_task() {
        let source = "s3://bucket/a";
        let mut job = JobContext::with_conf(
            &LoadJobCommand::builder(source).build(),
            "job-1".to_string(),
            source.to_string(),
            "/target".to_string(),
            &MountInfo::default(),
            &ClientConf::default(),
            1,
            JobTaskType::Load,
        );
        for task_id in ["task-1", "task-2"] {
            job.add_task_detail(
                task_id.to_string(),
                TaskDetail::new(LoadTaskInfo {
                    job: job.info.clone(),
                    task_id: task_id.to_string(),
                    worker: WorkerAddress::default(),
                    source_path: source.to_string(),
                    target_path: "/target".to_string(),
                    create_time: 0,
                    source_read_plan_json: String::new(),
                    transfer_report: None,
                }),
            );
        }
        let jobs = JobStore::new();
        jobs.insert("job-1".to_string(), job);

        jobs.update_progress("job-1", "task-1", progress(JobTaskState::Completed, 200))
            .unwrap();
        // The delayed report of the same task arrives after its completion.
        jobs.update_progress("job-1", "task-1", progress(JobTaskState::Loading, 100))
            .unwrap();

        let job = jobs.get("job-1").unwrap();
        let task = &job.tasks["task-1"].progress;
        assert_eq!(task.state, JobTaskState::Completed);
        assert_eq!(task.loaded_size, 200);
        assert_eq!(job.progress.loaded_size, 200);
        let state: JobTaskState = job.state.state();
        assert_eq!(state, JobTaskState::Loading);
    }
}
