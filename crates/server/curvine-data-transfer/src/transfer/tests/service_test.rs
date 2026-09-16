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
