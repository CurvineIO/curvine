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

use std::sync::Arc;

use curvine_model::{
    FileStatus, StoragePolicy, StorageState, StorageType, TransferKind, TransferState, TtlAction,
};
use curvine_proto::SubmitTransferRequest;

use super::service::{
    auto_repair_eligible, auto_repair_in_cooldown, cv_cache_target_needs_repair,
    AUTO_REPAIR_COOLDOWN_MS,
};
use super::{MemoryTransferStore, TransferService};

fn load_submit_request(client_request_id: &str) -> SubmitTransferRequest {
    SubmitTransferRequest {
        kind: TransferKind::Load as i32,
        source_path: "s3://bucket/source".to_string(),
        target_path: "/cv/target".to_string(),
        client_request_id: client_request_id.to_string(),
        submitter: "test".to_string(),
        tenant: "default".to_string(),
        command: Vec::new(),
        protocol_version: Some(1),
    }
}

#[test]
fn kind_gate_blocks_until_enabled() {
    let store = Arc::new(MemoryTransferStore::new());
    let service =
        TransferService::new(store.clone()).with_enabled_kinds(vec![TransferKind::Export]);
    let req = load_submit_request("disabled-load");
    let err = service.clone().submit_transfer(req.clone()).unwrap_err();
    assert!(err
        .to_string()
        .contains("load is disabled by transfer.enabled_kinds"));
    assert_eq!(store.job_count(), 0);

    let unknown = SubmitTransferRequest {
        kind: i32::MAX,
        ..req.clone()
    };
    assert!(service
        .submit_transfer(unknown)
        .unwrap_err()
        .to_string()
        .contains("Invalid transfer kind"));
    let enabled_without_planner = service
        .clone()
        .with_enabled_kinds(vec![TransferKind::Load])
        .with_planner_registry(Arc::new(super::PlannerRegistry::new()));
    assert!(enabled_without_planner
        .submit_transfer(req.clone())
        .unwrap_err()
        .to_string()
        .contains("no transfer planner registered"));
    assert_eq!(store.job_count(), 0);

    let export = SubmitTransferRequest {
        kind: TransferKind::Export as i32,
        source_path: "/cv/source".into(),
        target_path: "s3://bucket/target".into(),
        client_request_id: "enabled-export".into(),
        ..req
    };
    assert_eq!(
        service.submit_transfer(export).unwrap().kind,
        TransferKind::Export
    );
    assert_eq!(store.job_count(), 1);
}

#[test]
fn disabled_kind_cannot_be_retried_but_remains_queryable() {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store.clone());
    let job = service
        .submit_transfer(load_submit_request("existing-load"))
        .unwrap();
    store.set_job_state(&job.job_id, TransferState::Failed);
    let disabled = service.with_enabled_kinds(Vec::new());
    assert_eq!(
        disabled.get_transfer(&job.job_id).unwrap().state,
        TransferState::Failed
    );
    let err = disabled.retry_transfer(&job.job_id).unwrap_err();
    assert!(err
        .to_string()
        .contains("load is disabled by transfer.enabled_kinds"));
    assert_eq!(store.job_count(), 1);
}

fn resubmit_same_request_keeps_job(state: TransferState) {
    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store.clone());
    let req = load_submit_request("stable-request");

    let first = service.submit_transfer(req.clone()).unwrap();
    store.set_job_state(&first.job_id, state);

    let second = service.submit_transfer(req).unwrap();
    assert_eq!(second.job_id, first.job_id);
    assert_eq!(second.state, state);
    assert_eq!(store.job_count(), 1);
}

fn file_status(is_complete: bool, ufs_mtime: i64, state: StorageState) -> FileStatus {
    FileStatus {
        is_complete,
        is_dir: false,
        storage_policy: StoragePolicy {
            storage_type: StorageType::Disk,
            ttl_ms: 0,
            ttl_action: TtlAction::None,
            ufs_mtime,
            state,
        },
        ..FileStatus::default()
    }
}

#[test]
fn auto_repair_cooldown_is_one_minute() {
    assert_eq!(AUTO_REPAIR_COOLDOWN_MS, 60_000);
}

#[test]
fn auto_repair_only_for_completed_or_partial_success_load() {
    assert!(auto_repair_eligible(
        TransferKind::Load,
        TransferState::Completed
    ));
    assert!(auto_repair_eligible(
        TransferKind::Load,
        TransferState::PartialSuccess
    ));
    assert!(!auto_repair_eligible(
        TransferKind::Load,
        TransferState::Failed
    ));
    assert!(!auto_repair_eligible(
        TransferKind::Load,
        TransferState::Canceled
    ));
    assert!(!auto_repair_eligible(
        TransferKind::Load,
        TransferState::Pending
    ));
    assert!(!auto_repair_eligible(
        TransferKind::Load,
        TransferState::Running
    ));
    assert!(!auto_repair_eligible(
        TransferKind::Export,
        TransferState::Completed
    ));
}

#[test]
fn auto_repair_cooldown_blocks_within_window_then_allows() {
    let first = 1_000_000_i64;
    assert!(auto_repair_in_cooldown(
        first,
        first + AUTO_REPAIR_COOLDOWN_MS - 1
    ));
    assert!(!auto_repair_in_cooldown(
        first,
        first + AUTO_REPAIR_COOLDOWN_MS
    ));
    // Same timestamp: elapsed 0 is still inside cooldown.
    assert!(auto_repair_in_cooldown(first, first));
}

#[test]
fn incomplete_cv_shell_needs_repair() {
    let status = file_status(false, 0, StorageState::Cv);
    assert!(cv_cache_target_needs_repair(&status));
}

#[test]
fn incomplete_with_nonzero_ufs_mtime_needs_repair() {
    let status = file_status(false, 1_700_000_000_000, StorageState::Cv);
    assert!(cv_cache_target_needs_repair(&status));
}

#[test]
fn healthy_both_cache_does_not_need_repair() {
    let status = file_status(true, 1_700_000_000_000, StorageState::Both);
    assert!(!cv_cache_target_needs_repair(&status));
}

#[test]
fn complete_but_zero_ufs_mtime_needs_repair() {
    let status = file_status(true, 0, StorageState::Both);
    assert!(cv_cache_target_needs_repair(&status));
}

#[test]
fn complete_ufs_only_without_cv_copy_needs_repair() {
    let status = file_status(true, 1_700_000_000_000, StorageState::Ufs);
    assert!(cv_cache_target_needs_repair(&status));
}

#[test]
fn directory_never_needs_repair() {
    let mut status = file_status(false, 0, StorageState::Cv);
    status.is_dir = true;
    assert!(!cv_cache_target_needs_repair(&status));
}

#[test]
fn submit_failed_load_does_not_auto_repair() {
    resubmit_same_request_keeps_job(TransferState::Failed);
}

#[test]
fn submit_canceled_load_does_not_auto_repair() {
    resubmit_same_request_keeps_job(TransferState::Canceled);
}

#[test]
fn submit_completed_load_without_cache_does_not_auto_repair() {
    resubmit_same_request_keeps_job(TransferState::Completed);
}

#[test]
fn submit_partial_success_load_without_cache_does_not_auto_repair() {
    resubmit_same_request_keeps_job(TransferState::PartialSuccess);
}

#[test]
fn planned_totals_and_completed_files_land_in_the_job_summary() {
    use super::TransferPlannedTasks;
    use super::TransferStore;
    use curvine_model::{
        TaskAttemptStart, TransferProgress, TransferTaskRecord, TransferTaskReport,
        TransferTaskState,
    };

    let store = Arc::new(MemoryTransferStore::new());
    let service = TransferService::new(store.clone());
    let submitted = service
        .submit_transfer(load_submit_request("counts-request"))
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("test-owner", 60_000, 10, 64)
        .unwrap()
        .expect("pending transfer should be acquired");

    let task = TransferTaskRecord {
        job_id: submitted.job_id.clone(),
        run_id: submitted.run_id,
        task_id: "task-1".to_string(),
        attempt_id: 0,
        source_path: "s3://bucket/source/a".to_string(),
        target_path: "/cv/target/a".to_string(),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: "{}".to_string(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress::default(),
        retry_count: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 10,
    };
    assert!(store
        .persist_planned_tasks(TransferPlannedTasks {
            job_id: submitted.job_id.clone(),
            run_id: submitted.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: vec![task],
            total_files: 1,
            total_size: 100,
            skipped_files: 2,
            skipped_size: 123,
            message: "planned".to_string(),
            now_ms: 11,
        })
        .unwrap());

    let job = store
        .get_transfer(&submitted.job_id)
        .unwrap()
        .expect("job must exist");
    assert_eq!(job.summary.total_files, 1);
    assert_eq!(job.summary.total_size, 100);
    assert_eq!(job.summary.loaded_files, 0);
    assert_eq!(job.summary.skipped_files, 2);
    assert_eq!(job.summary.skipped_size, 123);

    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: submitted.job_id.clone(),
            run_id: submitted.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session-1".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 12,
            stale_deadline_at: 100,
        })
        .unwrap());
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: submitted.job_id.clone(),
            run_id: submitted.run_id,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session-1".to_string(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 100,
                total_size: 100,
                update_time: 13,
                ..Default::default()
            },
            now_ms: 13,
            stale_deadline_at: 100,
        })
        .unwrap());

    let job = store
        .get_transfer(&submitted.job_id)
        .unwrap()
        .expect("job must exist");
    assert_eq!(job.summary.total_files, 1);
    assert_eq!(job.summary.total_size, 100);
    assert_eq!(job.summary.loaded_files, 1);
    assert_eq!(job.summary.loaded_size, 100);
}
