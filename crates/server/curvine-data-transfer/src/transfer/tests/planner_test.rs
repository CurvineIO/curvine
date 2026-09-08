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

use curvine_config::ClientConf;
use curvine_model::{
    FileStatus, MountInfo, StoragePolicy, StorageState, TransferCommand, TransferJobRecord,
    TransferKind, TransferProgress, TransferState,
};

use super::planner::{load_job_info, needs_source_status_refresh, unchanged_load_target};

fn source_status(mtime: i64, len: i64) -> FileStatus {
    FileStatus {
        mtime,
        len,
        is_complete: true,
        ..Default::default()
    }
}

fn target_status(mtime: i64, len: i64) -> FileStatus {
    FileStatus {
        len,
        is_complete: true,
        storage_policy: StoragePolicy {
            ufs_mtime: mtime,
            state: StorageState::Both,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[test]
fn unchanged_load_output_is_skipped_only_when_source_fingerprint_matches() {
    let source = source_status(10, 1024);
    let target = target_status(10, 1024);

    assert!(unchanged_load_target(&source, Some(&target)));
    assert!(!unchanged_load_target(
        &source_status(11, 1024),
        Some(&target)
    ));
    assert!(!unchanged_load_target(
        &source_status(10, 2048),
        Some(&target)
    ));
}

#[test]
fn incomplete_or_cv_missing_target_is_not_skipped() {
    let source = source_status(10, 1024);
    let mut incomplete = target_status(10, 1024);
    incomplete.is_complete = false;
    assert!(!unchanged_load_target(&source, Some(&incomplete)));

    let mut cv_missing = target_status(10, 1024);
    cv_missing.storage_policy.state = StorageState::Ufs;
    assert!(!unchanged_load_target(&source, Some(&cv_missing)));
}

#[test]
fn equal_size_timestamp_mismatch_refreshes_source_status_before_planning() {
    let target = target_status(10, 1024);

    assert!(needs_source_status_refresh(
        &source_status(11, 1024),
        Some(&target)
    ));
    assert!(!needs_source_status_refresh(
        &source_status(11, 2048),
        Some(&target)
    ));
}

#[test]
fn load_replicas_override_mount_and_service_defaults() {
    let mut command = TransferCommand {
        kind: TransferKind::Load,
        source_path: "s3://bucket/source".to_string(),
        target_path: "/target".to_string(),
        ..Default::default()
    };
    command.set_replicas(3);
    let job = TransferJobRecord {
        job_key: command.job_key(),
        job_id: "job-1".to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: command.source_path.clone(),
        target_path: command.target_path.clone(),
        command_json: serde_json::to_string(&command).unwrap(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 0,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: "request-1".to_string(),
        submitter: "test".to_string(),
        tenant: String::new(),
        created_at: 0,
        updated_at: 0,
    };
    let mount = MountInfo {
        replicas: Some(2),
        ..Default::default()
    };
    let mut client_conf = ClientConf::default();
    client_conf.replicas = 1;

    assert_eq!(load_job_info(&job, &mount, &client_conf).replicas, 3);
}
