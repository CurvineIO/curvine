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

use curvine_error::{ErrorKind, FsError};
use curvine_runtime::common::Utils;
use num_enum::{FromPrimitive, IntoPrimitive, TryFromPrimitive};
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
    TryFromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferKind {
    #[default]
    Load = 1,
    Export = 2,
}

impl TransferKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Export => "export",
        }
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
            TransferTaskState::Completed | TransferTaskState::Failed | TransferTaskState::Canceled
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
    pub const REPLICAS_OPTION: &'static str = "replicas";

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

    pub fn default_client_request_id_with_overwrite(
        kind: TransferKind,
        source_path: impl AsRef<str>,
        target_path: impl AsRef<str>,
        overwrite: bool,
    ) -> String {
        format!(
            "job_{}",
            Utils::md5(format!(
                "{:?}:{}:{}:overwrite={}",
                kind,
                source_path.as_ref(),
                target_path.as_ref(),
                overwrite
            ))
        )
    }

    pub fn default_client_request_id_with_overwrite_and_replicas(
        kind: TransferKind,
        source_path: impl AsRef<str>,
        target_path: impl AsRef<str>,
        overwrite: bool,
        replicas: Option<i32>,
    ) -> String {
        let Some(replicas) = replicas else {
            return Self::default_client_request_id_with_overwrite(
                kind,
                source_path,
                target_path,
                overwrite,
            );
        };
        format!(
            "job_{}",
            Utils::md5(format!(
                "{:?}:{}:{}:overwrite={}:replicas={}",
                kind,
                source_path.as_ref(),
                target_path.as_ref(),
                overwrite,
                replicas
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

    pub fn replicas(&self) -> Option<i32> {
        self.options
            .get(Self::REPLICAS_OPTION)
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|replicas| *replicas > 0)
    }

    pub fn set_replicas(&mut self, replicas: i32) {
        self.options
            .insert(Self::REPLICAS_OPTION.to_string(), replicas.to_string());
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
    /// Primary input; multi-input specs belong in `spec_blob`.
    pub source_path: String,
    /// Primary output used for display and target-conflict checks.
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
    /// Zero denotes a legacy job with no opaque spec.
    #[serde(default)]
    pub spec_version: u32,
    #[serde(default)]
    pub spec_blob: Vec<u8>,
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
    #[serde(default)]
    pub spec_blob: Vec<u8>,
    #[serde(default)]
    pub result_json: String,
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

/// Opaque plugin output. Large results are written by the plugin and reported by reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferTaskResult {
    Inline(Vec<u8>),
    Reference { reference: String, size: u64 },
}

impl TransferTaskResult {
    pub fn into_wire_fields(
        result: Option<Self>,
    ) -> (Option<Vec<u8>>, Option<String>, Option<u64>) {
        match result {
            Some(Self::Inline(bytes)) => {
                let size = bytes.len() as u64;
                (Some(bytes), None, Some(size))
            }
            Some(Self::Reference { reference, size }) => (None, Some(reference), Some(size)),
            None => (None, None, None),
        }
    }
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
    pub result_inline: Option<Vec<u8>>,
    pub result_ref: Option<String>,
    pub result_size: Option<u64>,
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

#[cfg(test)]
mod tests {
    use super::{TransferCommand, TransferKind};

    #[test]
    fn overwrite_changes_default_client_request_id() {
        let overwrite = TransferCommand::default_client_request_id_with_overwrite(
            TransferKind::Export,
            "/source",
            "s3://bucket/target",
            true,
        );
        let no_overwrite = TransferCommand::default_client_request_id_with_overwrite(
            TransferKind::Export,
            "/source",
            "s3://bucket/target",
            false,
        );

        assert_ne!(overwrite, no_overwrite);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleTaskAttempt {
    pub task: TransferTaskRecord,
}

pub fn transfer_failure_message(
    kind: TransferKind,
    source_path: &str,
    target_path: &str,
    err: &FsError,
) -> String {
    let source_label = match kind {
        TransferKind::Load => "source object",
        TransferKind::Export => "source file",
    };
    match err.kind() {
        ErrorKind::FileNotFound | ErrorKind::Expired => {
            format!("{source_label} not found: {source_path}")
        }
        ErrorKind::FileAlreadyExists => format!("target already exists: {target_path}"),
        ErrorKind::ParentNotDir | ErrorKind::NotADirectory => {
            format!("target parent is not a directory: {target_path}")
        }
        ErrorKind::IsADirectory => format!("target is a directory: {target_path}"),
        ErrorKind::ReadOnly => format!("target is read-only: {target_path}"),
        ErrorKind::DiskOutOfSpace => {
            format!("Not enough Curvine worker disk space to transfer {source_path}; free space and retry")
        }
        ErrorKind::Timeout => format!(
            "Timed out while accessing {source_path}; verify storage connectivity and retry"
        ),
        ErrorKind::IO | ErrorKind::Ufs | ErrorKind::Pipeline => format!(
            "Cannot access transfer storage for {source_path}; verify the mount, credentials, and network connectivity"
        ),
        ErrorKind::TransferStoreUnavailable => {
            "Transfer metadata store is unavailable; retry after it recovers".to_string()
        }
        ErrorKind::TransferOverloaded => "Transfer service is busy; retry later".to_string(),
        _ => format!("Transfer from {source_path} to {target_path} failed: {err}"),
    }
}
