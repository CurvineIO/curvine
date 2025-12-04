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

use crate::state::{MountInfo, StorageType, TtlAction, WorkerAddress};
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::common::ByteUnit;
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i8)]
pub enum JobTaskState {
    #[num_enum(default)]
    UNKNOWN = 0,
    Pending = 1,
    Loading = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
}

pub struct LoadJobResult {
    pub job_id: String,
    pub target_path: String,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum JobTaskType {
    #[num_enum(default)]
    Load = 1,
}

pub struct JobStatus {
    pub job_id: String,
    pub state: JobTaskState,
    pub source_path: String,
    pub target_path: String,
    pub progress: JobTaskProgress,
}

impl JobStatus {
    /// Returns a formatted progress string with percentage and byte counts
    pub fn progress_string(&self, show_bar: bool) -> String {
        self.progress.progress_string(show_bar)
    }
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct LoadJobCommand {
    pub source_path: String,
    pub target_path: Option<String>,
    pub replicas: Option<i32>,
    pub block_size: Option<i64>,
    pub storage_type: Option<StorageType>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub overwrite: Option<bool>,
}

impl LoadJobCommand {
    pub fn builder(source_path: impl Into<String>) -> LoadJobCommandBuilder {
        LoadJobCommandBuilder::new(source_path).overwrite(true)
    }
}

#[derive(Default)]
pub struct LoadJobCommandBuilder {
    source_path: String,
    target_path: Option<String>,
    replicas: Option<i32>,
    block_size: Option<i64>,
    storage_type: Option<StorageType>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    overwrite: Option<bool>,
}

impl LoadJobCommandBuilder {
    pub fn new(source_path: impl Into<String>) -> Self {
        Self {
            source_path: source_path.into(),
            ..Default::default()
        }
    }

    pub fn target_path(mut self, target_path: impl Into<String>) -> Self {
        let _ = self.target_path.insert(target_path.into());
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        let _ = self.replicas.insert(replicas);
        self
    }

    pub fn block_size(mut self, block_size: i64) -> Self {
        let _ = self.block_size.insert(block_size);
        self
    }

    pub fn storage_type(mut self, storage_type: StorageType) -> Self {
        let _ = self.storage_type.insert(storage_type);
        self
    }

    pub fn ttl_ms(mut self, ttl_ms: i64) -> Self {
        let _ = self.ttl_ms.insert(ttl_ms);
        self
    }

    pub fn ttl_action(mut self, ttl_action: TtlAction) -> Self {
        let _ = self.ttl_action.insert(ttl_action);
        self
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        let _ = self.overwrite.insert(overwrite);
        self
    }

    pub fn build(self) -> LoadJobCommand {
        LoadJobCommand {
            source_path: self.source_path,
            target_path: self.target_path,
            replicas: self.replicas,
            block_size: self.block_size,
            storage_type: self.storage_type,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            overwrite: self.overwrite,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadJobInfo {
    pub job_id: String,
    pub source_path: String,
    pub target_path: String,
    pub block_size: i64,
    pub replicas: i32,
    pub storage_type: StorageType,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,
    pub mount_info: MountInfo,
    pub create_time: i64,
    pub overwrite: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTaskInfo {
    pub job: LoadJobInfo,
    pub task_id: String,
    pub worker: WorkerAddress,
    pub source_path: String,
    pub target_path: String,
    pub create_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTaskProgress {
    pub state: JobTaskState,
    pub loaded_size: i64,
    pub total_size: i64,
    pub update_time: i64,
    pub message: String,
}

impl Default for JobTaskProgress {
    fn default() -> Self {
        Self {
            state: JobTaskState::Pending,
            total_size: 0,
            loaded_size: 0,
            update_time: 0,
            message: String::new(),
        }
    }
}

impl JobTaskProgress {
    /// Returns a formatted progress string with percentage and byte counts
    /// Format: "[████████░░░░░░░░░░] 45.2% (123.4 MB / 273.0 MB)"
    /// If show_bar is false, format: "45.2% (123.4 MB / 273.0 MB)"
    pub fn progress_string(&self, show_bar: bool) -> String {
        let loaded = self.loaded_size.max(0) as u64;
        let total = self.total_size.max(0) as u64;

        let percentage = if total == 0 {
            0.0
        } else {
            (loaded as f64 / total as f64 * 100.0).min(100.0)
        };

        if show_bar {
            if total == 0 {
                return format!(
                    "[{}] 0.0% ({} / {})",
                    "░".repeat(20),
                    ByteUnit::byte_to_string(loaded),
                    ByteUnit::byte_to_string(total)
                );
            }

            let filled = (percentage / 100.0 * 20.0) as usize;
            let empty = 20 - filled.min(20);

            let progress_bar = format!("{}{}", "█".repeat(filled.min(20)), "░".repeat(empty));

            format!(
                "[{}] {:.1}% ({} / {})",
                progress_bar,
                percentage,
                ByteUnit::byte_to_string(loaded),
                ByteUnit::byte_to_string(total)
            )
        } else {
            format!(
                "{:.1}% ({} / {})",
                percentage,
                ByteUnit::byte_to_string(loaded),
                ByteUnit::byte_to_string(total)
            )
        }
    }
}
