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

use super::planner::{
    load_job_info, load_replica_mismatch, needs_source_status_refresh, unchanged_load_target,
};

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
fn replica_mismatch_forces_recopy_even_when_content_unchanged() {
    let source = source_status(10, 1024);
    let mut target = target_status(10, 1024);
    target.replicas = 1;

    // Same fingerprint as the source but the replica count diverges from the
    // mount setting: planner must treat the file as changed.
    assert!(load_replica_mismatch(Some(&target), 3));
    assert!(!load_replica_mismatch(Some(&target), 1));
    assert!(!load_replica_mismatch(None, 3));

    // Replica parity keeps the unchanged skip intact.
    let matched = target_status(10, 1024);
    assert!(unchanged_load_target(&source, Some(&matched)));
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
fn batch_flush_triggers_on_file_count_threshold() {
    use super::planner::{batch_should_flush, BATCH_MAX_FILES};
    // Flush exactly at the file-count boundary, not before.
    assert!(!batch_should_flush(BATCH_MAX_FILES - 1, 0));
    assert!(batch_should_flush(BATCH_MAX_FILES, 0));
}

#[test]
fn batch_flush_triggers_on_byte_threshold() {
    use super::planner::{batch_should_flush, BATCH_TARGET_BYTES};
    assert!(!batch_should_flush(1, BATCH_TARGET_BYTES - 1));
    assert!(batch_should_flush(1, BATCH_TARGET_BYTES));
}

#[test]
fn batch_flush_disabled_for_zero_bytes_with_few_files() {
    use super::planner::batch_should_flush;
    assert!(!batch_should_flush(0, 0));
    assert!(!batch_should_flush(1, 1));
}

#[test]
fn segment_offsets_cover_exact_block_multiple() {
    use super::planner::{segment_offsets, SEGMENT_BYTES};
    // 2.5 blocks -> 3 segments, last is the remainder (default block = SEGMENT_BYTES).
    let ranges = segment_offsets(SEGMENT_BYTES * 2 + SEGMENT_BYTES / 2, SEGMENT_BYTES);
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0], (0, SEGMENT_BYTES));
    assert_eq!(ranges[1], (SEGMENT_BYTES, SEGMENT_BYTES));
    assert_eq!(ranges[2], (SEGMENT_BYTES * 2, SEGMENT_BYTES / 2));
}

#[test]
fn segment_offsets_align_to_smaller_block_size() {
    use super::planner::{segment_offsets, SEGMENT_BYTES};
    // A job with a 64 MiB block size must split at 64 MiB boundaries, not
    // the 128 MiB default, so each segment maps to exactly one CV block
    // (review: run_segment assumed block_size == SEGMENT_BYTES).
    let block = SEGMENT_BYTES / 2;
    let len = block * 3 + block / 2;
    let ranges = segment_offsets(len, block);
    assert_eq!(ranges.len(), 4);
    assert_eq!(ranges[0], (0, block));
    assert_eq!(ranges[1], (block, block));
    assert_eq!(ranges[2], (block * 2, block));
    assert_eq!(ranges[3], (block * 3, block / 2));
}

#[test]
fn segment_offsets_contiguous_and_cover_whole_file() {
    use super::planner::{segment_offsets, SEGMENT_BYTES};
    let len = SEGMENT_BYTES * 3 + 12345;
    let ranges = segment_offsets(len, SEGMENT_BYTES);
    // Contiguous and total == file length.
    let mut next = 0i64;
    let mut total = 0i64;
    for (off, l) in &ranges {
        assert_eq!(*off, next);
        next += *l;
        total += *l;
        assert!(*l <= SEGMENT_BYTES);
    }
    assert_eq!(total, len);
    assert_eq!(next, len);
}

#[test]
fn segment_offsets_small_file_is_single_segment() {
    use super::planner::{segment_offsets, SEGMENT_BYTES};
    assert_eq!(
        segment_offsets(SEGMENT_BYTES - 1, SEGMENT_BYTES),
        vec![(0, SEGMENT_BYTES - 1)]
    );
    assert_eq!(
        segment_offsets(SEGMENT_BYTES, SEGMENT_BYTES),
        vec![(0, SEGMENT_BYTES)]
    );
    assert!(segment_offsets(0, SEGMENT_BYTES).is_empty());
}

#[test]
fn non_splittable_compressed_suffixes_are_recognized() {
    use super::planner::is_non_splittable;
    assert!(is_non_splittable("data.gz"));
    assert!(is_non_splittable("DATA.GZ"));
    assert!(is_non_splittable("archive.zip"));
    assert!(is_non_splittable("file.tar.zst"));
    assert!(!is_non_splittable("file.orc"));
    assert!(!is_non_splittable("file.parquet"));
    assert!(!is_non_splittable("plain"));
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
        mount_snapshot_attempts: 0,
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
        planning_attempts: 0,
    };
    let mount = MountInfo {
        replicas: Some(2),
        ..Default::default()
    };
    let client_conf = ClientConf {
        replicas: 1,
        ..Default::default()
    };

    assert_eq!(
        load_job_info(&job, &mount, None, &client_conf)
            .unwrap()
            .replicas,
        3
    );
}
