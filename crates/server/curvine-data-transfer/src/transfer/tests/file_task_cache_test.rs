// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! File-level tasks live only in the cache: a whole job's file tasks must
//! never reach the store, while its progress, counts and terminal state still
//! come out right.

use std::sync::Arc;

use curvine_model::{
    TaskAttemptStart, TransferKind, TransferProgress, TransferState, TransferTaskRecord,
    TransferTaskReport, TransferTaskState,
};

use super::{MemoryTransferStore, TaskCache, TransferService, TransferStore};

use curvine_model::TransferJobRecord as Record;
use curvine_model::TransferJobRecord;

fn job(job_id: &str) -> Record {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "file:///source".to_string(),
        target_path: "/target".to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Running,
        owner: "owner".to_string(),
        lease_epoch: 1,
        lease_expire_at: i64::MAX,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("req-{job_id}"),
        submitter: "tester".to_string(),
        tenant: "test".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn file_task(job_id: &str, task_id: &str) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 0,
        source_path: format!("/src/{task_id}"),
        target_path: format!("/dst/{task_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        // A bare JSON array is a plain per-file plan: file-level, no segments.
        source_read_plan_json: "[]".to_string(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress::default(),
        estimated_size: 0,
        retry_count: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 0,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        block_locations: Vec::new(),
        replace_target: false,
    }
}

fn start(job_id: &str, task_id: &str) -> TaskAttemptStart {
    TaskAttemptStart {
        job_id: job_id.to_string(),
        run_id: 1,
        owner: "owner".to_string(),
        lease_epoch: 1,
        task_id: task_id.to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        report_target_json: "[]".to_string(),
        now_ms: 100,
        stale_deadline_at: 10_000,
        resume_loaded_size: 0,
    }
}

fn report(job_id: &str, task_id: &str, state: TransferTaskState) -> TransferTaskReport {
    TransferTaskReport {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        state,
        progress: TransferProgress {
            loaded_size: 100,
            total_size: 100,
            update_time: 200,
            message: String::new(),
        },
        now_ms: 200,
        stale_deadline_at: 10_000,
        accepted_at_ms: 150,
        first_byte_at_ms: 160,
        block_locations: Vec::new(),
    }
}

#[test]
fn file_level_tasks_never_reach_the_store() {
    let store = Arc::new(MemoryTransferStore::new());
    store.create_or_get_by_request_id(job("job-a")).unwrap();
    let cache = Arc::new(TaskCache::new());
    cache.seed(
        "job-a",
        1,
        vec![file_task("job-a", "t1"), file_task("job-a", "t2")],
    );
    let service = TransferService::new(store.clone()).with_task_cache(cache.clone());

    // Dispatch selects from the cache, the store has no pending rows.
    assert!(store
        .claim_pending_tasks("job-a", 1, 10)
        .unwrap()
        .is_empty());
    let claimed = cache.claim_pending("job-a", 1, 10, 100);
    assert_eq!(claimed.len(), 2);

    // One task completes through the report path.
    assert!(cache.start_attempt(&start("job-a", "t1")));
    assert!(service
        .report_task(curvine_proto::TransferTaskReportRequest {
            job_id: "job-a".to_string(),
            run_id: 1,
            task_id: "t1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session".to_string(),
            state: TransferTaskState::Completed as i32,
            progress: curvine_proto::TransferProgressProto {
                loaded_size: 100,
                total_size: 100,
                update_time: 200,
                message: String::new(),
            },
            accepted_at_ms: Some(150),
            first_byte_at_ms: Some(160),
            attempt_started_at_ms: Some(140),
            block_locations: Vec::new(),
            protocol_version: Some(1),
        })
        .unwrap());

    // The store never saw a task row for this job.
    assert!(store.list_transfer_tasks("job-a", 1).unwrap().is_empty());

    // Progress is still reported accurately through the merged summary.
    let (_, counts, _, _, _) = service.get_transfer_status("job-a", None, None).unwrap();
    assert_eq!(counts.completed, 1);
    assert_eq!(counts.pending, 1);
}

#[test]
fn stale_cache_attempt_is_recycled_and_reported_again() {
    let cache = TaskCache::new();
    cache.seed("job-b", 1, vec![file_task("job-b", "t1")]);
    assert!(cache.start_attempt(&start("job-b", "t1")));

    // Before the deadline the running attempt is not re-selectable.
    assert!(cache.claim_pending("job-b", 1, 10, 5_000).is_empty());
    // Past it the task returns to Pending for a fresh attempt.
    let recycled = cache.claim_pending("job-b", 1, 10, 20_000);
    assert_eq!(recycled.len(), 1);
    assert_eq!(recycled[0].state, TransferTaskState::Pending);
    assert!(cache.start_attempt(&TaskAttemptStart {
        attempt_id: 2,
        now_ms: 20_000,
        ..start("job-b", "t1")
    }));
    // The superseded attempt can no longer advance the task.
    assert_eq!(
        cache.apply_report(&report("job-b", "t1", TransferTaskState::Completed)),
        Some(false)
    );
    assert_eq!(
        cache.apply_report(&TransferTaskReport {
            attempt_id: 2,
            ..report("job-b", "t1", TransferTaskState::Completed)
        }),
        Some(true)
    );
}

/// B: the cache summary must reach the job row, otherwise the console list
/// shows 0% progress and a stale `updated_at` for a running job.
#[test]
fn cache_progress_is_written_back_to_the_job_row() {
    let store = Arc::new(MemoryTransferStore::new());
    store.create_or_get_by_request_id(job("job-c")).unwrap();
    let cache = Arc::new(TaskCache::new());
    cache.seed(
        "job-c",
        1,
        vec![file_task("job-c", "t1"), file_task("job-c", "t2")],
    );
    let service = TransferService::new(store.clone()).with_task_cache(cache.clone());

    cache.start_attempt(&start("job-c", "t1"));
    cache.apply_report(&report("job-c", "t1", TransferTaskState::Running));

    // Mirrors the write-back the flusher performs: store rows plus cache rows.
    let merged = super::merge_summaries(
        store.get_transfer_task_summary("job-c", 1, 200).unwrap(),
        cache.summary("job-c", 1, 200),
    );
    store
        .update_job_progress_summary("job-c", 1, merged.progress.clone(), 200)
        .unwrap();

    // A reader with no cache of its own (another instance, or list_transfers)
    // now sees the live progress from the job row alone.
    let seen = store.get_transfer("job-c").unwrap().unwrap();
    // Only the started task has a size yet; the pending one has none.
    assert_eq!(seen.summary.loaded_size, 100);
    assert_eq!(seen.summary.total_size, 100);
    assert_eq!(seen.updated_at, 200);

    // The owner still reports through the cache, and the counts agree.
    let (_, counts, _, _, _) = service.get_transfer_status("job-c", None, None).unwrap();
    assert_eq!(counts.running, 1);
    assert_eq!(counts.pending, 1);
}

/// The queued report path must not swallow file-level reports: its store
/// batch cannot see those tasks and would answer "fenced".
#[test]
fn queued_report_path_advances_cached_tasks() {
    let store = Arc::new(MemoryTransferStore::new());
    store.create_or_get_by_request_id(job("job-q")).unwrap();
    let cache = Arc::new(TaskCache::new());
    cache.seed("job-q", 1, vec![file_task("job-q", "t1")]);
    let service = TransferService::with_report_queue(
        store.clone(),
        std::time::Duration::from_secs(60),
        64,
        2,
    )
    .unwrap()
    .with_task_cache(cache.clone());

    assert!(cache.start_attempt(&start("job-q", "t1")));
    let accepted = service
        .report_task(curvine_proto::TransferTaskReportRequest {
            job_id: "job-q".to_string(),
            run_id: 1,
            task_id: "t1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session".to_string(),
            state: TransferTaskState::Completed as i32,
            progress: curvine_proto::TransferProgressProto {
                loaded_size: 100,
                total_size: 100,
                update_time: 200,
                message: String::new(),
            },
            accepted_at_ms: Some(10),
            first_byte_at_ms: Some(11),
            attempt_started_at_ms: Some(10),
            block_locations: Vec::new(),
            protocol_version: Some(1),
        })
        .unwrap();

    assert!(accepted, "cache task report must be accepted, not fenced");
    assert!(cache.summary("job-q", 1, 100).unwrap().all_completed);
    assert!(store.list_transfer_tasks("job-q", 1).unwrap().is_empty());
}
