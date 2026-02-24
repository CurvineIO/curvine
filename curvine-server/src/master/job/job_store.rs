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

use crate::master::JobContext;
use curvine_common::state::{JobTaskProgress, JobTaskState, LoadJobCommand, MountInfo};
use curvine_common::FsResult;
use orpc::err_box;
use orpc::sync::FastDashMap;
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
pub struct DeferredPublishIntent {
    pub job_id: String,
    pub command: LoadJobCommand,
    pub mount: MountInfo,
}

#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<FastDashMap<String, JobContext>>,
    callbacks: Arc<RwLock<HashMap<String, Vec<JobCallback>>>>,
    aliases: Arc<FastDashMap<String, String>>,
    deferred_publish: Arc<FastDashMap<String, DeferredPublishIntent>>,
    active_path_jobs: Arc<FastDashMap<String, usize>>,
}

pub struct JobProgressUpdate {
    pub task_state_changed: bool,
    pub job_state_changed: bool,
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
            aliases: Arc::new(FastDashMap::default()),
            deferred_publish: Arc::new(FastDashMap::default()),
            active_path_jobs: Arc::new(FastDashMap::default()),
        }
    }

    fn is_active_state(state: JobTaskState) -> bool {
        matches!(state, JobTaskState::Pending | JobTaskState::Loading)
    }

    fn for_each_paths<F>(source_path: &str, target_path: &str, mut f: F)
    where
        F: FnMut(&str),
    {
        f(source_path);
        if source_path != target_path {
            f(target_path);
        }
    }

    fn increment_active_path_count(&self, path: &str) {
        if let Some(mut count) = self.active_path_jobs.get_mut(path) {
            *count += 1;
            return;
        }
        self.active_path_jobs.insert(path.to_string(), 1);
    }

    fn decrement_active_path_count(&self, path: &str) {
        if let Some(mut count) = self.active_path_jobs.get_mut(path) {
            if *count <= 1 {
                drop(count);
                let _ = self.active_path_jobs.remove(path);
            } else {
                *count -= 1;
            }
        }
    }

    fn apply_active_index_transition(
        &self,
        source_path: &str,
        target_path: &str,
        old_state: JobTaskState,
        new_state: JobTaskState,
    ) {
        let old_active = Self::is_active_state(old_state);
        let new_active = Self::is_active_state(new_state);
        if old_active == new_active {
            return;
        }
        if new_active {
            Self::for_each_paths(source_path, target_path, |path| {
                self.increment_active_path_count(path)
            });
        } else {
            Self::for_each_paths(source_path, target_path, |path| {
                self.decrement_active_path_count(path)
            });
        }
    }

    pub fn insert_job(&self, job_id: impl Into<String>, job: JobContext) -> Option<JobContext> {
        let source_path = job.info.source_path.clone();
        let target_path = job.info.target_path.clone();
        let new_state: JobTaskState = job.state.state();
        let old = self.jobs.insert(job_id.into(), job);

        if let Some(old_job) = old.as_ref() {
            self.apply_active_index_transition(
                &old_job.info.source_path,
                &old_job.info.target_path,
                old_job.state.state(),
                JobTaskState::UNKNOWN,
            );
        }

        self.apply_active_index_transition(
            &source_path,
            &target_path,
            JobTaskState::UNKNOWN,
            new_state,
        );

        old
    }

    pub fn has_active_job_touching_path(&self, path: impl AsRef<str>) -> bool {
        self.active_path_jobs
            .get(path.as_ref())
            .map(|count| *count > 0)
            .unwrap_or(false)
    }

    pub fn register_callback(&self, job_id: String, callback: JobCallback) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks.entry(job_id).or_default().push(callback);
    }

    pub fn register_completion_callback<F>(&self, job_id: String, callback: F)
    where
        F: Fn(&str, JobTaskState, JobTaskState, &JobContext) + Send + Sync + 'static,
    {
        let cb = JobCallback::new(callback)
            .with_filter(vec![JobTaskState::Completed, JobTaskState::Failed]);
        self.register_callback(job_id, cb);
    }

    fn trigger_callbacks(
        &self,
        job_id: &str,
        old_state: JobTaskState,
        new_state: JobTaskState,
        job: &JobContext,
    ) {
        let callbacks_guard = self.callbacks.read().unwrap();
        if let Some(callbacks) = callbacks_guard.get(job_id) {
            for cb in callbacks {
                if cb.should_trigger(new_state) {
                    (cb.callback)(job_id, old_state, new_state, job);
                }
            }
        }
    }

    pub fn update_progress(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<JobProgressUpdate> {
        let job_id = job_id.as_ref();
        let task_id = task_id.as_ref();

        let mut job = if let Some(job) = self.jobs.get_mut(job_id) {
            job
        } else {
            return err_box!("Not fond job {}", job_id);
        };

        let old_state: JobTaskState = job.state.state();

        let task_state_changed = job.update_progress(task_id, progress)?;

        let new_state: JobTaskState = job.state.state();
        let job_state_changed = old_state != new_state;
        if job_state_changed {
            self.apply_active_index_transition(
                &job.info.source_path,
                &job.info.target_path,
                old_state,
                new_state,
            );
        }

        if job_state_changed {
            let job_id_owned = job_id.to_string();
            let job_clone = (*job).clone();
            drop(job);

            self.trigger_callbacks(&job_id_owned, old_state, new_state, &job_clone);
        }

        Ok(JobProgressUpdate {
            task_state_changed,
            job_state_changed,
        })
    }

    pub fn update_state(&self, job_id: &str, state: JobTaskState, message: impl Into<String>) {
        if let Some(mut job) = self.jobs.get_mut(job_id) {
            let old_state: JobTaskState = job.state.state();
            job.update_state(state, message);
            let new_state = state;
            if old_state != new_state {
                self.apply_active_index_transition(
                    &job.info.source_path,
                    &job.info.target_path,
                    old_state,
                    new_state,
                );
            }

            if old_state != new_state {
                let job_clone = (*job).clone();
                drop(job);

                self.trigger_callbacks(job_id, old_state, new_state, &job_clone);
            }
        }
    }

    pub fn remove_callbacks(&self, job_id: &str) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks.remove(job_id);
    }

    pub fn add_alias(&self, alias: impl Into<String>, canonical_job_id: impl Into<String>) {
        self.aliases.insert(alias.into(), canonical_job_id.into());
    }

    pub fn resolve_job_id(&self, job_id: impl AsRef<str>) -> Option<String> {
        let job_id = job_id.as_ref();
        if self.jobs.contains_key(job_id) {
            return Some(job_id.to_string());
        }

        let resolved = self.aliases.get(job_id).map(|v| v.value().clone())?;
        if self.jobs.contains_key(&resolved) {
            Some(resolved)
        } else {
            self.aliases.remove(job_id);
            None
        }
    }

    pub fn remove_job(&self, job_id: &str) -> Option<(String, JobContext)> {
        let removed = self.jobs.remove(job_id)?;
        self.apply_active_index_transition(
            &removed.1.info.source_path,
            &removed.1.info.target_path,
            removed.1.state.state(),
            JobTaskState::UNKNOWN,
        );
        let aliases_to_remove: Vec<String> = self
            .aliases
            .iter()
            .filter_map(|entry| {
                if entry.value() == job_id {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();
        for alias in aliases_to_remove {
            self.aliases.remove(&alias);
        }
        self.remove_callbacks(job_id);
        Some(removed)
    }

    pub fn queue_deferred_publish(
        &self,
        source_path: impl Into<String>,
        intent: DeferredPublishIntent,
    ) -> Option<DeferredPublishIntent> {
        self.deferred_publish.insert(source_path.into(), intent)
    }

    pub fn take_deferred_publish(
        &self,
        source_path: impl AsRef<str>,
    ) -> Option<DeferredPublishIntent> {
        self.deferred_publish
            .remove(source_path.as_ref())
            .map(|(_, intent)| intent)
    }

    pub fn clear_deferred_publish(&self, source_path: impl AsRef<str>) {
        self.deferred_publish.remove(source_path.as_ref());
    }

    pub fn clear_deferred_publish_by_job(
        &self,
        job_id: impl AsRef<str>,
    ) -> Option<(String, DeferredPublishIntent)> {
        let job_id = job_id.as_ref();
        let source_path = self
            .deferred_publish
            .iter()
            .find_map(|entry| (entry.value().job_id == job_id).then(|| entry.key().clone()))?;
        self.deferred_publish.remove(&source_path)
    }

    pub fn list_deferred_publish(&self) -> Vec<(String, DeferredPublishIntent)> {
        self.deferred_publish
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
}

impl Deref for JobStore {
    type Target = FastDashMap<String, JobContext>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}
