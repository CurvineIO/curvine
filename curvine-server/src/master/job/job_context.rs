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
    PersistedLoadJobSnapshot, PersistedLoadTaskProgress, WorkerAddress,
};
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use orpc::sync::StateCtl;

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

#[derive(Clone)]
pub struct JobContext {
    pub info: LoadJobInfo,
    pub state: StateCtl,
    pub progress: JobTaskProgress,
    pub assigned_workers: FastHashSet<WorkerAddress>,
    pub tasks: FastHashMap<String, TaskDetail>,
}

impl JobContext {
    fn is_expected_competition_failure(message: &str) -> bool {
        message.contains("owned by another lease")
            || message.contains("source") && message.contains("changed during commit")
            || message.contains("lost ownership before commit finalize")
    }

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
            mount_info: mnt.clone(),
            create_time: LocalTime::mills() as i64,
            overwrite: job_conf.overwrite,
            source_generation: job_conf.source_generation.clone(),
            expected_source_id: job_conf.expected_source_id,
            expected_source_len: job_conf.expected_source_len,
            expected_source_mtime: job_conf.expected_source_mtime,
            expected_target_mtime: job_conf.expected_target_mtime,
            expected_target_missing: job_conf.expected_target_missing,
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
    ) -> FsResult<bool> {
        let current_state: JobTaskState = self.state.state();
        if current_state == JobTaskState::Canceled {
            // User cancellation is terminal; ignore late task reports.
            return Ok(false);
        }

        let task_id = task_id.as_ref();
        let detail = if let Some(v) = self.tasks.get_mut(task_id) {
            v
        } else if self.allow_legacy_task_alias() {
            if let Some(v) = self.resolve_legacy_task_alias(task_id) {
                v
            } else {
                warn!(
                    "Ignore stale task report for job {}, unknown task_id {}",
                    self.info.job_id, task_id
                );
                return Ok(false);
            }
        } else {
            warn!(
                "Ignore stale task report for generation job {}, unknown task_id {}",
                self.info.job_id, task_id
            );
            return Ok(false);
        };
        let task_state_changed = detail.progress.state != progress.state;
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
            info!(
                "job {} all subtasks completed, tasks {}, len = {}, cost {} ms",
                self.info.job_id,
                self.tasks.len(),
                ByteUnit::byte_to_string(loaded_size as u64),
                LocalTime::mills() as i64 - self.info.create_time
            )
        } else if job_state == JobTaskState::Failed {
            let log_msg = format!(
                "job {} execute failed, tasks {}, len = {}, cost {} ms, error {}",
                self.info.job_id,
                self.tasks.len(),
                ByteUnit::byte_to_string(loaded_size as u64),
                LocalTime::mills() as i64 - self.info.create_time,
                message
            );
            if Self::is_expected_competition_failure(&message) {
                debug!("{}", log_msg);
            } else {
                warn!("{}", log_msg);
            }
        }

        self.update_state(job_state, message);
        self.progress.loaded_size = loaded_size;
        self.progress.total_size = total_size;

        Ok(task_state_changed)
    }

    fn allow_legacy_task_alias(&self) -> bool {
        self.info.source_generation.is_none()
    }

    fn resolve_legacy_task_alias(&mut self, task_id: &str) -> Option<&mut TaskDetail> {
        // Backward compatibility: allow manual reports using legacy "<old_job_id>_task_<idx>"
        // while current task ids may include generation tags and a different canonical job id.
        let (_prefix, idx) = task_id.rsplit_once("_task_")?;
        if idx.is_empty() || !idx.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }

        let suffix = format!("_task_{}", idx);
        let mut matched_key: Option<String> = None;
        for key in self.tasks.keys() {
            if key.ends_with(&suffix) {
                if matched_key.is_some() {
                    return None;
                }
                matched_key = Some(key.clone());
            }
        }

        matched_key.and_then(|key| self.tasks.get_mut(&key))
    }

    pub fn to_snapshot(&self) -> PersistedLoadJobSnapshot {
        let tasks = self
            .tasks
            .values()
            .map(|detail| PersistedLoadTaskProgress {
                task_id: detail.task.task_id.clone(),
                worker: detail.task.worker.clone(),
                source_path: detail.task.source_path.clone(),
                target_path: detail.task.target_path.clone(),
                create_time: detail.task.create_time,
                progress: detail.progress.clone(),
            })
            .collect();

        PersistedLoadJobSnapshot {
            info: self.info.clone(),
            state: self.state.state(),
            progress: self.progress.clone(),
            tasks,
        }
    }

    pub fn from_snapshot(snapshot: PersistedLoadJobSnapshot) -> Self {
        let mut tasks = FastHashMap::default();
        let mut assigned_workers = FastHashSet::default();
        for task in snapshot.tasks {
            assigned_workers.insert(task.worker.clone());
            let mut detail = TaskDetail::new(LoadTaskInfo {
                job: snapshot.info.clone(),
                task_id: task.task_id.clone(),
                worker: task.worker,
                source_path: task.source_path,
                target_path: task.target_path,
                create_time: task.create_time,
            });
            detail.progress = task.progress;
            tasks.insert(detail.task.task_id.clone(), detail);
        }

        Self {
            info: snapshot.info,
            state: StateCtl::new(snapshot.state.into()),
            progress: snapshot.progress,
            assigned_workers,
            tasks,
        }
    }
}
