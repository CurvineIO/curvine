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
    ExtendedBlock, FileStatus, LocatedBlock, MountInfo, StoragePolicy, StorageState,
    TransferCommand, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    WorkerAddress,
};

use super::plan::{
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
fn transfer_master_rpc_metrics_record_stage_operation_and_result() {
    super::record_master_rpc(
        "test_stage",
        "test_operation",
        "success",
        std::time::Instant::now(),
    );
    super::record_master_rpc(
        "test_stage",
        "test_fallback",
        "fallback",
        std::time::Instant::now(),
    );
    let output = curvine_metrics::Metrics::text_output().unwrap();
    assert!(
        output.contains(
            "transfer_master_rpc_total{operation=\"test_operation\",result=\"success\",stage=\"test_stage\"} 1"
        ),
        "{output}"
    );
    assert!(
        output.contains(
            "transfer_master_rpc_duration_us_count{operation=\"test_operation\",result=\"success\",stage=\"test_stage\"} 1"
        ),
        "{output}"
    );
    assert!(!output.contains("operation=\"test_fallback\""), "{output}");
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
fn shell_cache_with_empty_block_locations_is_not_readable() {
    use super::plan::located_blocks_readable;

    // No data blocks: nothing to lose (empty file).
    assert!(located_blocks_readable(&[]));

    // Healthy blocks keep at least one live location.
    let healthy = LocatedBlock::new(ExtendedBlock::with_id(1), vec![WorkerAddress::default()]);
    assert!(located_blocks_readable(&[healthy]));

    // A single shell block makes the whole file unreadable.
    let partial = LocatedBlock::new(ExtendedBlock::with_id(2), vec![WorkerAddress::default()]);
    let shell = LocatedBlock::new(ExtendedBlock::with_id(3), vec![]);
    assert!(located_blocks_readable(&[partial]));
    assert!(!located_blocks_readable(&[shell]));
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
    use super::plan::{batch_should_flush, BATCH_MAX_FILES};
    // Flush exactly at the file-count boundary, not before.
    assert!(!batch_should_flush(BATCH_MAX_FILES - 1, 0));
    assert!(batch_should_flush(BATCH_MAX_FILES, 0));
}

#[test]
fn batch_flush_triggers_on_byte_threshold() {
    use super::plan::{batch_should_flush, BATCH_TARGET_BYTES};
    assert!(!batch_should_flush(1, BATCH_TARGET_BYTES - 1));
    assert!(batch_should_flush(1, BATCH_TARGET_BYTES));
}

#[test]
fn batch_flush_disabled_for_zero_bytes_with_few_files() {
    use super::plan::batch_should_flush;
    assert!(!batch_should_flush(0, 0));
    assert!(!batch_should_flush(1, 1));
}

#[test]
fn segment_offsets_cover_exact_block_multiple() {
    use super::plan::{segment_offsets, SEGMENT_BYTES};
    // 2.5 blocks -> 3 segments, last is the remainder (default block = SEGMENT_BYTES).
    let ranges = segment_offsets(SEGMENT_BYTES * 2 + SEGMENT_BYTES / 2, SEGMENT_BYTES);
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0], (0, SEGMENT_BYTES));
    assert_eq!(ranges[1], (SEGMENT_BYTES, SEGMENT_BYTES));
    assert_eq!(ranges[2], (SEGMENT_BYTES * 2, SEGMENT_BYTES / 2));
}

#[test]
fn segment_offsets_align_to_smaller_block_size() {
    use super::plan::{segment_offsets, SEGMENT_BYTES};
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
fn segment_offsets_large_block_maps_one_segment_per_block() {
    use super::plan::{segment_offsets, SEGMENT_BYTES};
    let block = SEGMENT_BYTES * 8;
    let ranges = segment_offsets(block * 2 + block / 2, block);
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0], (0, block));
    assert_eq!(ranges[1], (block, block));
    assert_eq!(ranges[2], (block * 2, block / 2));
    let narrow = segment_offsets(block, block);
    assert_eq!(narrow, vec![(0, block)]);
    let mut indices = ranges.iter().map(|(offset, _)| offset / block);
    let first = indices.next().unwrap();
    assert!(indices
        .enumerate()
        .all(|(i, idx)| idx == first + i as i64 + 1));
}

#[test]
fn segment_offsets_tiny_block_stays_per_block() {
    use super::plan::segment_offsets;
    let block = 4 * 1024 * 1024;
    let ranges = segment_offsets(block * 3, block);
    assert_eq!(ranges.len(), 3);
    assert_eq!(ranges[0], (0, block));
    assert_eq!(ranges[2], (block * 2, block));
}

#[test]
fn group_partition_keeps_small_files_per_segment() {
    use super::plan::{plan_segment_groups, SEGMENT_BYTES};
    let groups = plan_segment_groups(
        SEGMENT_BYTES * 8,
        SEGMENT_BYTES,
        84,
        SEGMENT_BYTES * 8,
        7,
        String::new(),
    );
    assert_eq!(groups.len(), 8);
    for group in &groups {
        assert_eq!(group.group_len(), SEGMENT_BYTES);
    }
    assert_eq!(groups[0].start_offset, 0);
    assert_eq!(groups[7].end_offset, SEGMENT_BYTES * 8);
    assert_eq!(groups[3].first_block_index, 3);
}

#[test]
fn group_partition_matches_design_targets() {
    use super::plan::{plan_segment_groups, SEGMENT_BYTES};
    let gb = 1024 * 1024 * 1024;
    let groups_100g = plan_segment_groups(100 * gb, SEGMENT_BYTES, 84, 100 * gb, 0, String::new());
    assert_eq!(groups_100g.len(), 84);
    let groups_5t = plan_segment_groups(
        5 * 1024 * gb,
        SEGMENT_BYTES,
        84,
        5 * 1024 * gb,
        0,
        String::new(),
    );
    assert_eq!(groups_5t.len(), 1024);
    for group in groups_5t.iter().take(1023) {
        assert!(group.group_len() <= 5 * gb);
    }
}

#[test]
fn group_partition_is_contiguous_and_covers_the_file() {
    use super::plan::{plan_segment_groups, SEGMENT_BYTES};
    let len = SEGMENT_BYTES * 100 + 12345;
    let groups = plan_segment_groups(len, SEGMENT_BYTES, 4, len, 0, String::new());
    let mut next = 0i64;
    let mut index = 0i64;
    for group in &groups {
        assert_eq!(group.start_offset, next);
        assert_eq!(group.first_block_index, index);
        assert_eq!(group.total_len, len);
        next = group.end_offset;
        index += (group.group_len() + SEGMENT_BYTES - 1) / SEGMENT_BYTES;
    }
    assert_eq!(next, len);
}

#[test]
fn group_partition_caps_bytes_with_large_blocks() {
    use super::plan::plan_segment_groups;
    let gb = 1024 * 1024 * 1024;
    let groups = plan_segment_groups(100 * gb, gb, 84, 100 * gb, 0, String::new());
    assert_eq!(groups.len(), 84);
    let oversize = plan_segment_groups(6 * gb, 6 * gb, 84, 6 * gb, 0, String::new());
    assert_eq!(oversize.len(), 1);
    assert_eq!(oversize[0].group_len(), 6 * gb);
}

#[test]
fn group_partition_derives_count_from_live_workers_and_budget() {
    use super::plan::{plan_segment_groups, SEGMENT_BYTES, SEGMENT_GROUP_TARGET_BYTES};
    let groups = plan_segment_groups(
        SEGMENT_BYTES * 100,
        SEGMENT_BYTES,
        4,
        SEGMENT_BYTES * 100,
        0,
        "etag".to_string(),
    );
    assert_eq!(groups.len(), 4);
    assert!(groups
        .iter()
        .all(|group| group.group_len() <= SEGMENT_GROUP_TARGET_BYTES));
}

#[test]
fn group_planning_requires_a_nonempty_all_capable_worker_view() {
    use super::plan::supports_segment_grouping;
    use curvine_model::{TransferWorkerCapabilities, WorkerInfo};

    fn capable() -> WorkerInfo {
        WorkerInfo {
            transfer_capabilities: TransferWorkerCapabilities::current(),
            ..WorkerInfo::default()
        }
    }

    assert!(!supports_segment_grouping(&[]));
    assert!(supports_segment_grouping(&[capable()]));

    let mut legacy = capable();
    legacy.transfer_capabilities.segment_group = false;
    assert!(!supports_segment_grouping(&[legacy]));
}

#[test]
fn group_identity_accepts_a_version_or_a_complete_head_stat_pair() {
    use super::plan::has_group_identity;

    // Immutable version: identity regardless of the stat pair.
    assert!(has_group_identity("etag", Some((10, 20))));
    assert!(has_group_identity("etag", None));
    // Version-less (OSS-HDFS): only a complete HEAD pair qualifies, and 0
    // means "not observed" rather than a real length/mtime.
    assert!(has_group_identity("", Some((10, 20))));
    assert!(!has_group_identity("", Some((0, 20))));
    assert!(!has_group_identity("", Some((10, 0))));
    assert!(!has_group_identity("", None));
}

#[test]
fn group_partition_empty_file_has_no_groups() {
    use super::plan::{plan_segment_groups, SEGMENT_BYTES};
    assert!(plan_segment_groups(0, SEGMENT_BYTES, 84, 0, 0, String::new()).is_empty());
}
#[test]
fn group_manifest_carries_source_version() {
    use super::plan::{plan_segment_groups, SEGMENT_BYTES};
    let gb = 1024 * 1024 * 1024;
    let groups = plan_segment_groups(
        100 * gb,
        SEGMENT_BYTES,
        84,
        100 * gb,
        0,
        "etag-abc".to_string(),
    );
    let multi = groups
        .iter()
        .find(|group| group.group_len() > SEGMENT_BYTES)
        .unwrap();
    assert_eq!(multi.source_version, "etag-abc");
    let unversioned = plan_segment_groups(100 * gb, SEGMENT_BYTES, 84, 100 * gb, 0, String::new());
    assert!(unversioned
        .iter()
        .all(|group| group.source_version.is_empty()));
}

#[test]
fn singleton_group_emits_legacy_segment_plan() {
    use super::plan::{build_group_plan_json, plan_segment_groups, SEGMENT_BYTES};
    use curvine_model::{classify_transfer_plan, TransferPlanKind};
    let groups = plan_segment_groups(
        SEGMENT_BYTES * 2,
        SEGMENT_BYTES,
        84,
        SEGMENT_BYTES * 2,
        0,
        String::new(),
    );
    assert_eq!(groups.len(), 2);
    for group in &groups {
        let (json, total) = build_group_plan_json(group, SEGMENT_BYTES * 2).unwrap();
        assert_eq!(classify_transfer_plan(&json), TransferPlanKind::Segment);
        assert_eq!(total, SEGMENT_BYTES);
    }
    let gb = 1024 * 1024 * 1024;
    let big = plan_segment_groups(100 * gb, SEGMENT_BYTES, 84, 100 * gb, 0, String::new());
    let multi = big
        .iter()
        .find(|group| group.group_len() > SEGMENT_BYTES)
        .unwrap();
    let (json, total) = build_group_plan_json(multi, 100 * gb).unwrap();
    assert_eq!(
        classify_transfer_plan(&json),
        TransferPlanKind::SegmentGroup
    );
    assert_eq!(total, multi.group_len());
}

#[test]
fn segment_offsets_contiguous_and_cover_whole_file() {
    use super::plan::{segment_offsets, SEGMENT_BYTES};
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
    use super::plan::{segment_offsets, SEGMENT_BYTES};
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
    use super::plan::is_non_splittable;
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
        terminal_summary: None,
        terminal_counts: None,
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

#[test]
fn group_row_reduction_projection_matches_nightly_load() {
    use super::plan::{plan_segment_groups, segment_offsets, SEGMENT_BYTES};
    use std::time::Instant;
    let gb = 1024 * 1024 * 1024;
    let cases = [
        (gb, 8, 8),
        (100 * gb, 800, 84),
        (5 * 1024 * gb, 40960, 1024),
    ];
    for (len, old_rows, new_rows) in cases {
        assert_eq!(segment_offsets(len, SEGMENT_BYTES).len(), old_rows);
        let start = Instant::now();
        let groups = plan_segment_groups(len, SEGMENT_BYTES, 84, len, 0, String::new());
        assert_eq!(groups.len(), new_rows);
        assert!(start.elapsed().as_secs() < 5);
    }
    let manifest = curvine_model::segment_group_json(
        &plan_segment_groups(
            5 * 1024 * gb,
            SEGMENT_BYTES,
            84,
            5 * 1024 * gb,
            0,
            String::new(),
        )[0],
    )
    .unwrap();
    assert!(manifest.len() < 1024);
}
