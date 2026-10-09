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

use super::dispatch::terminal_snapshot_progress;
use super::service::resolve_terminal_summary;
use curvine_model::{
    TransferJobRecord, TransferKind, TransferProgress, TransferState, TransferTaskCounts,
    TransferTaskSummary,
};

fn failed_job(
    summary: TransferProgress,
    terminal_summary: Option<TransferProgress>,
    terminal_counts: Option<TransferTaskCounts>,
) -> TransferJobRecord {
    TransferJobRecord {
        job_key: "Load:oss://bucket/source:/target".to_string(),
        job_id: "job-1".to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "oss://bucket/source".to_string(),
        target_path: "/target".to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 0,
        mount_snapshot_attempts: 0,
        cv_metadata_epoch: None,
        state: TransferState::Failed,
        owner: "owner".to_string(),
        lease_epoch: 1,
        lease_expire_at: 0,
        cancel_requested: false,
        summary,
        client_request_id: "request-1".to_string(),
        submitter: "test".to_string(),
        tenant: String::new(),
        created_at: 0,
        updated_at: 0,
        planning_attempts: 0,
        terminal_summary,
        terminal_counts,
    }
}

fn empty_live_summary() -> TransferTaskSummary {
    TransferTaskSummary {
        progress: TransferProgress::default(),
        counts: TransferTaskCounts::default(),
        has_task: false,
        has_failed: false,
        all_completed: false,
    }
}

#[test]
fn terminal_snapshot_keeps_planning_failure_message() {
    let message = "Load failed during planning: source not found";
    let progress = terminal_snapshot_progress(TransferProgress::default(), message);
    assert_eq!(progress.message, message);
}

#[test]
fn status_uses_persisted_job_message_when_snapshot_is_empty() {
    let message = "Load failed during planning: source not found";
    let job = failed_job(
        TransferProgress {
            message: message.to_string(),
            ..TransferProgress::default()
        },
        Some(TransferProgress::default()),
        Some(TransferTaskCounts::default()),
    );

    let (progress, counts, expired) = resolve_terminal_summary(&job, &empty_live_summary());

    assert_eq!(progress.message, message);
    assert_eq!(counts, TransferTaskCounts::default());
    assert!(!expired);
}

#[test]
fn status_keeps_job_message_when_terminal_snapshot_is_missing() {
    let message = "Load failed during planning: source not found";
    let job = failed_job(
        TransferProgress {
            message: message.to_string(),
            ..TransferProgress::default()
        },
        None,
        None,
    );

    let (progress, counts, expired) = resolve_terminal_summary(&job, &empty_live_summary());

    assert_eq!(progress.message, message);
    assert_eq!(counts, TransferTaskCounts::default());
    assert!(expired);
}
