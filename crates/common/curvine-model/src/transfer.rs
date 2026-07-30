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

use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::common::Utils;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const TRANSFER_TEMP_PATH_MARKER: &str = ".curvine-transfer-tmp-";

pub fn is_transfer_temp_path(path: &str) -> bool {
    path.contains(TRANSFER_TEMP_PATH_MARKER)
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferKind {
    #[default]
    Load = 1,
    Export = 2,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferState {
    #[default]
    Pending = 1,
    Planning = 2,
    Dispatching = 3,
    Running = 4,
    Canceling = 5,
    Completed = 6,
    Failed = 7,
    Canceled = 8,
    PartialSuccess = 9,
}

impl TransferState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TransferState::Completed
                | TransferState::Failed
                | TransferState::Canceled
                | TransferState::PartialSuccess
        )
    }

    pub fn is_runnable(self) -> bool {
        matches!(
            self,
            TransferState::Pending
                | TransferState::Planning
                | TransferState::Dispatching
                | TransferState::Running
                | TransferState::Canceling
        )
    }

    pub fn is_executing(self) -> bool {
        matches!(
            self,
            TransferState::Planning
                | TransferState::Dispatching
                | TransferState::Running
                | TransferState::Canceling
        )
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferTaskState {
    #[default]
    Pending = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
    Stale = 6,
}

impl TransferTaskState {
    pub fn is_running(self) -> bool {
        matches!(
            self,
            TransferTaskState::Pending | TransferTaskState::Running
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TransferTaskState::Completed
                | TransferTaskState::Failed
                | TransferTaskState::Canceled
                | TransferTaskState::Stale
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferCommand {
    pub kind: TransferKind,
    pub source_path: String,
    pub target_path: String,
    pub client_request_id: String,
    pub submitter: String,
    pub tenant: String,
    pub options: BTreeMap<String, String>,
}

impl TransferCommand {
    pub const OVERWRITE_OPTION: &'static str = "overwrite";

    pub fn job_key(&self) -> String {
        format!("{:?}:{}:{}", self.kind, self.source_path, self.target_path)
    }

    pub fn default_client_request_id(
        kind: TransferKind,
        source_path: impl AsRef<str>,
        target_path: impl AsRef<str>,
    ) -> String {
        format!(
            "job_{}",
            Utils::md5(format!(
                "{:?}:{}:{}",
                kind,
                source_path.as_ref(),
                target_path.as_ref()
            ))
        )
    }

    pub fn overwrite(&self) -> bool {
        self.options
            .get(Self::OVERWRITE_OPTION)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true)
    }

    pub fn set_overwrite(&mut self, overwrite: bool) {
        self.options
            .insert(Self::OVERWRITE_OPTION.to_string(), overwrite.to_string());
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferProgress {
    pub loaded_size: i64,
    pub total_size: i64,
    pub update_time: i64,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferJobRecord {
    pub job_key: String,
    pub job_id: String,
    pub run_id: u64,
    pub kind: TransferKind,
    pub source_path: String,
    pub target_path: String,
    pub command_json: String,
    pub mount_snapshot_json: String,
    pub secret_ref_json: String,
    pub cluster_snapshot_version: u64,
    pub cv_metadata_epoch: Option<u64>,
    pub state: TransferState,
    pub owner: String,
    pub lease_epoch: u64,
    pub lease_expire_at: i64,
    pub cancel_requested: bool,
    pub summary: TransferProgress,
    pub client_request_id: String,
    pub submitter: String,
    pub tenant: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferTaskRecord {
    pub job_id: String,
    pub run_id: u64,
    pub task_id: String,
    pub attempt_id: u64,
    pub source_path: String,
    pub target_path: String,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub source_read_plan_json: String,
    pub report_target_json: String,
    pub state: TransferTaskState,
    pub progress: TransferProgress,
    pub retry_count: u32,
    pub attempt_started_at: i64,
    pub last_report_at: i64,
    pub stale_deadline_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTaskSummary {
    pub progress: TransferProgress,
    pub counts: TransferTaskCounts,
    pub has_task: bool,
    pub has_failed: bool,
    pub all_completed: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferTaskCounts {
    pub pending: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    pub stale: u64,
    pub completed_size: i64,
}

pub fn summarize_transfer_tasks<'a>(
    tasks: impl IntoIterator<Item = &'a TransferTaskRecord>,
    update_time: i64,
) -> TransferTaskSummary {
    let mut summary = TransferTaskSummary {
        progress: TransferProgress {
            update_time,
            ..Default::default()
        },
        counts: TransferTaskCounts::default(),
        has_task: false,
        has_failed: false,
        all_completed: true,
    };
    let mut failed_count = 0;
    let mut first_error = String::new();
    let mut last_message = String::new();

    for task in tasks {
        summary.has_task = true;
        summary.has_failed |= task.state == TransferTaskState::Failed;
        summary.all_completed &= task.state == TransferTaskState::Completed;
        summary.progress.loaded_size += task.progress.loaded_size;
        summary.progress.total_size += task.progress.total_size;

        match task.state {
            TransferTaskState::Pending => summary.counts.pending += 1,
            TransferTaskState::Running => summary.counts.running += 1,
            TransferTaskState::Completed => {
                summary.counts.completed += 1;
                summary.counts.completed_size += task.progress.loaded_size.max(0);
            }
            TransferTaskState::Failed => summary.counts.failed += 1,
            TransferTaskState::Canceled => summary.counts.canceled += 1,
            TransferTaskState::Stale => summary.counts.stale += 1,
        }

        if !task.progress.message.is_empty() {
            last_message = task.progress.message.clone();
        }
        if task.state == TransferTaskState::Failed {
            failed_count += 1;
            if first_error.is_empty() && !task.progress.message.is_empty() {
                first_error = task.progress.message.clone();
            }
        }
    }

    if failed_count > 0 {
        if first_error.is_empty() {
            first_error = "transfer task failed".to_string();
        }
        summary.progress.message = format!(
            "{} transfer task(s) failed: {}",
            failed_count,
            normalize_summary_message(&first_error)
        );
    } else {
        summary.progress.message = last_message;
    }
    summary
}

fn normalize_summary_message(message: &str) -> String {
    message
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(512)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferLease {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferStateUpdate {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub from_states: Vec<TransferState>,
    pub to_state: TransferState,
    pub message: String,
    pub now_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskAttemptStart {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub task_id: String,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub report_target_json: String,
    pub now_ms: i64,
    pub stale_deadline_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTaskReport {
    pub job_id: String,
    pub run_id: u64,
    pub task_id: String,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub state: TransferTaskState,
    pub progress: TransferProgress,
    pub now_ms: i64,
    pub stale_deadline_at: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferListFilter {
    pub kind: Option<TransferKind>,
    pub state: Option<TransferState>,
    pub submitter: Option<String>,
    pub tenant: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferTenantSummary {
    pub tenant: String,
    pub pending: u64,
    pub executing: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    pub partial_success: u64,
    pub total: u64,
}

impl TransferTenantSummary {
    pub fn active(&self) -> u64 {
        self.pending.saturating_add(self.executing)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleTaskAttempt {
    pub task: TransferTaskRecord,
}
