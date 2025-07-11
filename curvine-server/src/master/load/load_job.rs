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

use chrono::{DateTime, Utc};
use curvine_common::conf::ClusterConf;
use curvine_common::proto::LoadState;
use curvine_common::state::WorkerAddress;
use log::info;
use std::collections::HashMap;
use uuid::Uuid;

/// Load the Task Manager configuration
#[derive(Clone)]
pub struct LoadManagerConfig {
    /// Default TTL (seconds)
    pub default_ttl_seconds: u64,

    /// Clean up expired task interval (seconds)
    pub cleanup_interval_seconds: u64,
}

impl LoadManagerConfig {
    /// Create a load task manager configuration from cluster configuration
    pub fn from_cluster_conf(conf: &ClusterConf) -> Self {
        // Try to access conf.master.load, if it does not exist, use the default configuration
        let load_conf = &conf.master.load;

        Self {
            default_ttl_seconds: load_conf.job_ttl_seconds,
            cleanup_interval_seconds: load_conf.job_cleanup_interval_seconds,
        }
    }
}

/// Load task information
#[derive(Debug, Clone)]
pub struct LoadJob {
    /// Task ID
    pub job_id: String,
    /// Source path
    pub source_path: String,
    /// Target path (Curvine file path)
    pub target_path: String,
    /// Current status
    pub state: LoadState,
    /// Status message, such as error message
    pub message: Option<String>,
    /// Total size (bytes)
    pub total_size: u64,
    /// Loaded size (bytes)
    pub loaded_size: u64,
    /// Whether to process folders recursively
    pub recursive: bool,
    /// The Worker node assigned to this task
    pub assigned_workers: Vec<WorkerAddress>,
    /// Subtask details, used to track the subtask status of folder recursive processing
    pub task_details: HashMap<String, TaskDetail>,
    /// Creation time
    pub create_time: DateTime<Utc>,
    /// Update time
    pub update_time: DateTime<Utc>,
    /// Expiry time
    pub expire_time: Option<DateTime<Utc>>,
}

/// Subtask details
#[derive(Debug, Clone)]
pub struct TaskDetail {
    /// Task ID
    pub task_id: String,
    /// Source path
    pub path: String,
    /// Target path
    pub target_path: String,
    /// Current status
    pub state: LoadState,
    /// Status message
    pub message: Option<String>,
    /// Total size (bytes)
    pub total_size: Option<u64>,
    /// Loaded size (bytes)
    pub loaded_size: Option<u64>,
    /// Worker ID
    pub worker_id: u32,
    /// Creation time
    pub create_time: DateTime<Utc>,
    /// Update time
    pub update_time: DateTime<Utc>,
}

impl LoadJob {
    /// Create a new loading task
    pub fn new(source_path: String, target_path: String, recursive: bool) -> Self {
        let now = Utc::now();
        Self {
            job_id: Uuid::new_v4().to_string(),
            source_path,
            target_path,
            state: LoadState::Pending,
            message: None,
            total_size: 0,
            loaded_size: 0,
            recursive,
            assigned_workers: Vec::new(),
            task_details: HashMap::new(),
            create_time: now,
            update_time: now,
            expire_time: None,
        }
    }

    /// Update task status
    pub fn update_state(&mut self, state: LoadState, message: Option<String>) {
        self.state = state;
        if let Some(msg) = message {
            self.message = Some(msg);
        }
        self.update_time = Utc::now();

        info!(
            "Updated job status: id={}, state={:?}, message={}, progress={}/{} ({:.2}%)",
            self.job_id,
            state,
            self.message.as_ref().unwrap_or(&"".to_string()),
            self.loaded_size,
            self.total_size,
            self.progress_percentage(),
        );
    }

    /// Set the total size
    pub fn set_total_size(&mut self, size: u64) {
        self.total_size = size;
        self.update_time = Utc::now();
    }

    /// Update loading progress
    pub fn update_progress(&mut self, loaded_size: u64) {
        self.loaded_size = loaded_size;
        self.update_time = Utc::now();
    }

    /// Assign Worker nodes
    pub fn assign_worker(&mut self, worker: WorkerAddress) {
        if !self.assigned_workers.contains(&worker) {
            self.assigned_workers.push(worker);
            self.update_time = Utc::now();
        }
    }

    /// Add subtasks
    pub fn add_sub_task(
        &mut self,
        task_id: String,
        path: String,
        target_path: String,
        worker_id: u32,
    ) {
        let now = Utc::now();
        let task_detail = TaskDetail {
            task_id: task_id.clone(),
            path,
            target_path,
            state: LoadState::Pending,
            message: None,
            total_size: None,
            loaded_size: None,
            worker_id,
            create_time: now,
            update_time: now,
        };

        self.task_details.insert(task_id, task_detail);
        self.update_time = now;
    }

    /// Update subtask status
    pub fn update_sub_task(
        &mut self,
        task_id: &str,
        state: LoadState,
        loaded_size: Option<u64>,
        total_size: Option<u64>,
        message: Option<String>,
    ) -> bool {
        if let Some(task) = self.task_details.get_mut(task_id) {
            task.state = state;

            if let Some(size) = total_size {
                task.total_size = Some(size);
            }

            if let Some(size) = loaded_size {
                task.loaded_size = Some(size);
            }

            if let Some(msg) = message {
                task.message = Some(msg);
            }

            task.update_time = Utc::now();

            // Update the total task status
            self.update_job_from_sub_tasks();

            true
        } else {
            false
        }
    }

    /// Update the overall task status according to the subtask status
    pub fn update_job_from_sub_tasks(&mut self) {
        if self.task_details.is_empty() {
            return;
        }

        let mut all_completed = true;
        let mut any_failed = false;
        let mut any_canceled = false;
        let mut any_loading = false;
        let mut any_pending = false;

        let mut total_size: u64 = 0;
        let mut loaded_size: u64 = 0;

        // Statistics of subtasks
        for task in self.task_details.values() {
            match task.state {
                LoadState::Completed => {}
                LoadState::Failed => {
                    any_failed = true;
                    all_completed = false;
                }
                LoadState::Canceled => {
                    any_canceled = true;
                    all_completed = false;
                }
                LoadState::Loading => {
                    any_loading = true;
                    all_completed = false;
                }
                LoadState::Pending => {
                    any_pending = true;
                    all_completed = false;
                }
            }

            // Calculate the total size and loaded size
            if let Some(size) = task.total_size {
                total_size += size;
            }

            if let Some(size) = task.loaded_size {
                loaded_size += size;
            }
        }

        // Update the total and loaded tasks
        self.total_size = total_size;
        self.loaded_size = loaded_size;

        // Update task status
        if all_completed {
            self.update_state(
                LoadState::Completed,
                Some("All subtasks completed".to_string()),
            );
        } else if any_failed {
            self.update_state(LoadState::Failed, Some("Some subtasks failed".to_string()));
        } else if any_canceled {
            self.update_state(
                LoadState::Canceled,
                Some("Some subtasks canceled".to_string()),
            );
        } else if any_loading {
            self.update_state(LoadState::Loading, Some("Tasks in progress".to_string()));
        } else if any_pending {
            self.update_state(LoadState::Pending, Some("Tasks pending".to_string()));
        }
    }

    /// Calculate progress percentage
    pub fn progress_percentage(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.loaded_size as f64 / self.total_size as f64) * 100.0
    }
}
