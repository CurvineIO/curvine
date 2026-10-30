// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0.
// License text: http://www.apache.org/licenses/LICENSE-2.0

//! Store write-volume benchmark for the file-level task cache.
//!
//! Runs the same workload twice against a real SQLite file:
//! - the legacy path: every task has a row and every report is a row update
//! - the cache path: no task rows, job summary written once per interval
//!
//! The comparison is relative (both runs share one build), so it holds for a
//! debug build even though absolute numbers do not reflect production.
//!
//! Ignored by default; run explicitly:
//! `cargo test -p curvine-data-transfer --features transfer-server \
//!     --test task_cache_write_volume -- --ignored --nocapture`
//!
//! The database lives in a temp dir and stays in the low megabytes.

use std::sync::Arc;
use std::time::Instant;

use curvine_model::{
    TransferJobRecord, TransferKind, TransferProgress, TransferState, TransferTaskRecord,
    TransferTaskReport, TransferTaskState,
};

use curvine_data_transfer::transfer::{
    merge_summaries, SqliteTransferStore, TaskCache, TransferStore,
};

const TASKS: usize = 1_000;
const REPORTS_PER_TASK: usize = 5;
const PROGRESS_WRITE_INTERVAL: usize = 64;

/// Removes the temp dir on drop, including when an assertion panics, so a
/// failed benchmark cannot leave files behind.
struct TempDb(std::path::PathBuf);

impl TempDb {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "curvine-transfer-bench-{}-{}-{}",
            tag,
            std::process::id(),
            unique_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir.join("transfer.db"))
    }
}

fn unique_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{nanos}")
}

impl Drop for TempDb {
    fn drop(&mut self) {
        if let Some(parent) = self.0.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }
}

fn job(job_id: &str, state: TransferState) -> TransferJobRecord {
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
        state,
        owner: "owner".to_string(),
        lease_epoch: 1,
        lease_expire_at: i64::MAX,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("req-{job_id}"),
        submitter: "bench".to_string(),
        tenant: "bench".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn task(job_id: &str, index: usize) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: format!("task-{index:06}"),
        attempt_id: 1,
        source_path: format!("/source/file-{index:06}.dat"),
        target_path: format!("/target/file-{index:06}.dat"),
        worker_id: 1,
        worker_session_id: "session".to_string(),
        source_read_plan_json: "[]".to_string(),
        report_target_json: "[]".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress::default(),
        estimated_size: 4096,
        retry_count: 1,
        attempt_started_at: 1,
        last_report_at: 1,
        stale_deadline_at: i64::MAX,
        updated_at: 1,
        accepted_at_ms: 1,
        first_byte_at_ms: 1,
        block_locations: Vec::new(),
        replace_target: false,
    }
}

fn report(task_id: &str, loaded: i64, state: TransferTaskState) -> TransferTaskReport {
    TransferTaskReport {
        job_id: "bench-job".to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 1,
        worker_id: 1,
        worker_session_id: "session".to_string(),
        state,
        progress: TransferProgress {
            loaded_size: loaded,
            total_size: 4096,
            update_time: 100,
            message: String::new(),
        },
        now_ms: 100,
        stale_deadline_at: i64::MAX,
        accepted_at_ms: 1,
        first_byte_at_ms: 1,
        block_locations: Vec::new(),
    }
}

fn db_size(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn task_rows(path: &std::path::Path) -> i64 {
    let store = SqliteTransferStore::open(path).unwrap();
    store.list_transfer_tasks("bench-job", 1).unwrap().len() as i64
}

#[test]
#[ignore = "write-volume benchmark; run explicitly with --ignored --nocapture"]
fn file_level_task_write_volume() {
    // ---- legacy path: one row per task, one UPDATE per report ----
    let legacy = TempDb::new("legacy");
    let legacy_db = legacy.0.clone();
    let legacy_start = Instant::now();
    {
        let store = Arc::new(SqliteTransferStore::open(&legacy_db).unwrap());
        store
            .create_or_get_by_request_id(job("bench-job", TransferState::Planning))
            .unwrap();
        for chunk in (0..TASKS).collect::<Vec<_>>().chunks(500) {
            store
                .append_planned_tasks(curvine_data_transfer::transfer::TransferPlannedTasks {
                    job_id: "bench-job".to_string(),
                    run_id: 1,
                    owner: "owner".to_string(),
                    lease_epoch: 1,
                    tasks: chunk.iter().map(|i| task("bench-job", *i)).collect(),
                    message: String::new(),
                    now_ms: 1,
                })
                .unwrap();
        }
        let planned = legacy_start.elapsed();
        for index in 0..TASKS {
            let task_id = format!("task-{index:06}");
            for step in 1..=REPORTS_PER_TASK {
                let loaded = 4096 * step as i64 / REPORTS_PER_TASK as i64;
                let state = if step == REPORTS_PER_TASK {
                    TransferTaskState::Completed
                } else {
                    TransferTaskState::Running
                };
                store
                    .update_task_report(report(&task_id, loaded, state))
                    .unwrap();
            }
        }
        let total = legacy_start.elapsed();
        println!(
            "legacy: plan={:?} tasks+reports={:?} total={:?} rows={} db={} KiB",
            planned,
            total - planned,
            total,
            task_rows(&legacy_db),
            db_size(&legacy_db) / 1024
        );
        assert_eq!(
            task_rows(&legacy_db),
            TASKS as i64,
            "legacy path must persist one row per task"
        );
    }

    // ---- cache path: no task rows, summary written per interval ----
    let cache = TempDb::new("cache");
    let cache_db = cache.0.clone();
    let cache_start = Instant::now();
    {
        let store = Arc::new(SqliteTransferStore::open(&cache_db).unwrap());
        store
            .create_or_get_by_request_id(job("bench-job", TransferState::Running))
            .unwrap();
        let cache = Arc::new(TaskCache::new());
        let all: Vec<TransferTaskRecord> = (0..TASKS).map(|i| task("bench-job", i)).collect();
        cache.seed("bench-job", 1, all);
        let seeded = cache_start.elapsed();

        let mut summary_writes = 0usize;
        let mut applied = 0usize;
        for index in 0..TASKS {
            let task_id = format!("task-{index:06}");
            for step in 1..=REPORTS_PER_TASK {
                let loaded = 4096 * step as i64 / REPORTS_PER_TASK as i64;
                let state = if step == REPORTS_PER_TASK {
                    TransferTaskState::Completed
                } else {
                    TransferTaskState::Running
                };
                let accepted = cache
                    .apply_report(&report(&task_id, loaded, state))
                    .unwrap();
                assert!(accepted, "report rejected for {task_id}");
                applied += 1;
                // Mirrors the periodic write-back: one job-row write per
                // interval, independent of how many reports arrived.
                if applied.is_multiple_of(PROGRESS_WRITE_INTERVAL * REPORTS_PER_TASK) {
                    let merged = merge_summaries(
                        store
                            .get_transfer_task_summary("bench-job", 1, 100)
                            .unwrap(),
                        cache.summary("bench-job", 1, 100),
                    );
                    store
                        .update_job_progress_summary("bench-job", 1, merged.progress, 100)
                        .unwrap();
                    summary_writes += 1;
                }
            }
        }
        let total = cache_start.elapsed();
        let merged = merge_summaries(
            store
                .get_transfer_task_summary("bench-job", 1, 100)
                .unwrap(),
            cache.summary("bench-job", 1, 100),
        );
        assert_eq!(merged.counts.completed, TASKS as u64, "all tasks completed");
        assert_eq!(merged.progress.loaded_size, 4096 * TASKS as i64);
        println!(
            "cache:  seed={:?} reports={:?} total={:?} task_rows={} summary_writes={} db={} KiB",
            seeded,
            total - seeded,
            total,
            task_rows(&cache_db),
            summary_writes,
            db_size(&cache_db) / 1024
        );
        println!(
            "cache:  {} reports applied, {} task rows, {} job-row writes",
            applied,
            task_rows(&cache_db),
            summary_writes
        );
    }

    let legacy_bytes = db_size(&legacy_db);
    let cache_bytes = db_size(&cache_db);
    println!(
        "db size: legacy={} KiB cache={} KiB",
        legacy_bytes / 1024,
        cache_bytes / 1024
    );
    assert_eq!(
        task_rows(&cache_db),
        0,
        "cache path must not write task rows"
    );
    assert!(
        cache_bytes < legacy_bytes,
        "cache path must write less than the legacy path"
    );
}
