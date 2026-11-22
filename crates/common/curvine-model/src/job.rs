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
use curvine_runtime::common::ByteUnit;
use num_enum::{FromPrimitive, IntoPrimitive};
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
    Default,
)]
#[repr(i8)]
pub enum WorkerTaskState {
    #[default]
    UNKNOWN = 0,
    Pending = 1,
    Loading = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
    /// Some sub-tasks succeeded and some failed (transfer load jobs).
    PartialSuccess = 6,
}

impl WorkerTaskState {
    pub fn is_finish(&self) -> bool {
        matches!(
            self,
            WorkerTaskState::Completed
                | WorkerTaskState::Failed
                | WorkerTaskState::Canceled
                | WorkerTaskState::PartialSuccess
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(self, WorkerTaskState::Pending | WorkerTaskState::Loading)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferJobInfo {
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
pub struct TransferTaskInfo {
    pub job: TransferJobInfo,
    pub task_id: String,
    pub worker: WorkerAddress,
    pub source_path: String,
    pub target_path: String,
    pub create_time: i64,
    #[serde(default)]
    pub source_read_plan_json: String,
    #[serde(default)]
    pub transfer_report: Option<TransferTaskReportInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferTaskReportInfo {
    pub run_id: u64,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    #[serde(default)]
    pub report_target: String,
    #[serde(default)]
    pub report_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerTaskProgress {
    pub state: WorkerTaskState,
    pub loaded_size: i64,
    pub total_size: i64,
    pub update_time: i64,
    pub message: String,
}

impl Default for WorkerTaskProgress {
    fn default() -> Self {
        Self {
            state: WorkerTaskState::Pending,
            total_size: 0,
            loaded_size: 0,
            update_time: 0,
            message: String::new(),
        }
    }
}

impl WorkerTaskProgress {
    /// Returns a formatted progress string with percentage and byte counts
    /// Format: "[████████░░░░░░░░░░] 45.2% (123.4 MB / 273.0 MB)"
    /// If show_bar is false, format: "45.2% (123.4 MB / 273.0 MB)"
    pub fn progress_string(&self, show_bar: bool) -> String {
        let loaded = self.loaded_size.max(0) as u64;
        let total = self.total_size.max(0) as u64;
        let display_loaded = if total == 0 {
            loaded
        } else {
            loaded.min(total)
        };

        let percentage = if total == 0 {
            0.0
        } else {
            display_loaded as f64 / total as f64 * 100.0
        };

        if show_bar {
            if total == 0 {
                return format!(
                    "[{}] 0.0% ({} / {})",
                    "░".repeat(20),
                    ByteUnit::byte_to_string(display_loaded),
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
                ByteUnit::byte_to_string(display_loaded),
                ByteUnit::byte_to_string(total)
            )
        } else {
            format!(
                "{:.1}% ({} / {})",
                percentage,
                ByteUnit::byte_to_string(display_loaded),
                ByteUnit::byte_to_string(total)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerTaskProgress;
    use curvine_runtime::common::ByteUnit;

    #[test]
    fn progress_string_caps_loaded_bytes_at_total() {
        let progress = WorkerTaskProgress {
            loaded_size: 66,
            total_size: 33,
            ..Default::default()
        };
        let expected_counts = format!(
            "{} / {}",
            ByteUnit::byte_to_string(33),
            ByteUnit::byte_to_string(33)
        );

        assert!(progress.progress_string(false).contains(&expected_counts));
        assert!(progress.progress_string(true).contains(&expected_counts));
    }
}
