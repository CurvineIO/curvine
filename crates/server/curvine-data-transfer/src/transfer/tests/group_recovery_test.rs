use curvine_model::{
    TaskAttemptStart, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferStateUpdate, TransferTaskCounts, TransferTaskRecord, TransferTaskReport,
    TransferTaskState,
};
use std::sync::{Arc, Barrier};

#[cfg(feature = "transfer-store-sqlite")]
use super::SqliteTransferStore;
use super::{MemoryTransferStore, TransferPlannedTasks, TransferService, TransferStore};

fn job(job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{job_id}"),
        submitter: "test".to_string(),
        tenant: "test".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn task(job_id: &str, task_id: &str, total: i64) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 0,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}/{task_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: "{\"segment\":{\"offset\":0}}".to_string(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress {
            total_size: total,
            ..Default::default()
        },
        estimated_size: total,
        retry_count: 0,
        attempt_started_at: 0,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 1,
        replace_target: false,
    }
}

fn planned_store<S: TransferStore>(
    store: Arc<S>,
    job_id: &str,
    totals: &[i64],
) -> TransferJobRecord {
    let job = job(job_id);
    store
        .create_or_get_by_request_id_checked(job.clone())
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("owner", 60_000, 1, 8, false)
        .unwrap()
        .unwrap();
    store
        .persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: totals
                .iter()
                .enumerate()
                .map(|(i, total)| task(&job.job_id, &format!("task-{i}"), *total))
                .collect(),
            message: "planned".to_string(),
            now_ms: 2,
        })
        .unwrap();
    store.get_transfer(&job.job_id).unwrap().unwrap()
}

fn start(store: &impl TransferStore, job: &TransferJobRecord, task_id: &str, resume: i64) -> bool {
    let lease = store.get_transfer(&job.job_id).unwrap().unwrap();
    store
        .start_task_attempt(TaskAttemptStart {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            task_id: task_id.to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 3,
            stale_deadline_at: 1_000,
            resume_loaded_size: resume,
        })
        .unwrap()
}

fn running_report(
    job: &TransferJobRecord,
    task_id: &str,
    loaded: i64,
    now_ms: i64,
) -> TransferTaskReport {
    TransferTaskReport {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: task_id.to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress {
            loaded_size: loaded,
            total_size: 384,
            update_time: now_ms,
            message: "running".to_string(),
        },
        now_ms,
        stale_deadline_at: 2_000,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
    }
}

fn terminalize(store: &impl TransferStore, job: &TransferJobRecord) {
    let lease = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            from_states: vec![
                TransferState::Planning,
                TransferState::Dispatching,
                TransferState::Running,
            ],
            to_state: TransferState::Completed,
            message: "done".to_string(),
            now_ms: 10,
        })
        .unwrap());
}

fn snapshot_for_purge(store: &impl TransferStore, job: &TransferJobRecord) {
    let summary = store
        .get_transfer_task_summary(&job.job_id, job.run_id, 10)
        .unwrap();
    store
        .update_terminal_snapshot(
            &job.job_id,
            job.run_id,
            summary.progress,
            summary.counts,
            10,
        )
        .unwrap();
}

fn loaded_of(store: &impl TransferStore, job: &TransferJobRecord, task_id: &str) -> i64 {
    store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == task_id)
        .unwrap()
        .progress
        .loaded_size
}

fn assert_running_report_takes_max_loaded<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "max-job", &[384]);
    assert!(start(&*store, &job, "task-0", 0));
    assert!(store
        .update_task_report(running_report(&job, "task-0", 256, 4))
        .unwrap());
    assert!(store
        .update_task_report(running_report(&job, "task-0", 128, 5))
        .unwrap());
    assert_eq!(loaded_of(&*store, &job, "task-0"), 256);
    assert!(store
        .update_task_report(running_report(&job, "task-0", 300, 6))
        .unwrap());
    assert_eq!(loaded_of(&*store, &job, "task-0"), 300);
}

#[test]
fn memory_running_report_takes_max_loaded() {
    assert_running_report_takes_max_loaded(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_running_report_takes_max_loaded() {
    assert_running_report_takes_max_loaded(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

fn assert_terminal_report_is_authoritative<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "terminal-job", &[384]);
    assert!(start(&*store, &job, "task-0", 0));
    assert!(store
        .update_task_report(running_report(&job, "task-0", 300, 4))
        .unwrap());
    let mut done = running_report(&job, "task-0", 384, 5);
    done.state = TransferTaskState::Completed;
    assert!(store.update_task_report(done).unwrap());
    assert_eq!(loaded_of(&*store, &job, "task-0"), 384);
}

#[test]
fn memory_terminal_report_is_authoritative() {
    assert_terminal_report_is_authoritative(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_terminal_report_is_authoritative() {
    assert_terminal_report_is_authoritative(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

fn assert_stale_attempt_report_rejected<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "fence-job", &[384]);
    assert!(start(&*store, &job, "task-0", 0));
    let mut report = running_report(&job, "task-0", 100, 4);
    report.attempt_id = 2;
    assert!(!store.update_task_report(report).unwrap());
    assert_eq!(loaded_of(&*store, &job, "task-0"), 0);
}

#[test]
fn memory_stale_attempt_report_rejected() {
    assert_stale_attempt_report_rejected(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_stale_attempt_report_rejected() {
    assert_stale_attempt_report_rejected(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
}

fn assert_start_attempt_clamps_resume<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "resume-job", &[384]);
    assert!(start(&*store, &job, "task-0", 500));
    assert_eq!(loaded_of(&*store, &job, "task-0"), 384);
}

#[test]
fn memory_start_attempt_clamps_resume() {
    assert_start_attempt_clamps_resume(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_start_attempt_clamps_resume() {
    assert_start_attempt_clamps_resume(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
}

fn snapshot_counts() -> TransferTaskCounts {
    TransferTaskCounts {
        pending: 0,
        running: 0,
        completed: 3,
        failed: 1,
        canceled: 0,
        stale: 0,
        completed_size: 300,
    }
}

fn assert_terminal_snapshot_survives_task_purge<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "snapshot-job", &[100, 100, 100, 100]);
    terminalize(&*store, &job);
    let summary = TransferProgress {
        loaded_size: 300,
        total_size: 400,
        update_time: 10,
        message: "1 transfer task(s) failed; first failure: boom".to_string(),
    };
    store
        .update_terminal_snapshot(&job.job_id, job.run_id, summary, snapshot_counts(), 10)
        .unwrap();
    assert_eq!(store.purge_terminal_tasks(100, 1000).unwrap(), 4);
    let service = TransferService::new(store.clone());
    let (status, counts, tasks, _, expired) = service
        .get_transfer_status(&job.job_id, Some(10), None)
        .unwrap();
    assert!(expired);
    assert!(tasks.is_empty());
    assert_eq!(counts, snapshot_counts());
    assert_eq!(status.summary.loaded_size, 300);
    assert_eq!(status.summary.total_size, 400);
}

#[test]
fn memory_terminal_snapshot_survives_task_purge() {
    assert_terminal_snapshot_survives_task_purge(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_terminal_snapshot_survives_task_purge() {
    assert_terminal_snapshot_survives_task_purge(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

fn assert_purge_tasks_row_limit_spans_rounds<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "rows-job", &[10, 10, 10, 10, 10]);
    terminalize(&*store, &job);
    snapshot_for_purge(&*store, &job);
    assert_eq!(store.purge_terminal_tasks(100, 2).unwrap(), 2);
    assert_eq!(store.purge_terminal_tasks(100, 2).unwrap(), 2);
    assert_eq!(store.purge_terminal_tasks(100, 2).unwrap(), 1);
    assert_eq!(store.purge_terminal_tasks(100, 2).unwrap(), 0);
}

#[test]
fn memory_purge_tasks_row_limit_spans_rounds() {
    assert_purge_tasks_row_limit_spans_rounds(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_purge_tasks_row_limit_spans_rounds() {
    assert_purge_tasks_row_limit_spans_rounds(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

fn assert_purge_never_touches_live_jobs<S: TransferStore>(store: Arc<S>) {
    let live = planned_store(store.clone(), "live-job", &[10, 10]);
    let done = planned_store(store.clone(), "done-job", &[10]);
    terminalize(&*store, &done);
    snapshot_for_purge(&*store, &done);
    assert_eq!(store.purge_terminal_tasks(100, 1000).unwrap(), 1);
    assert_eq!(
        store
            .list_transfer_tasks(&live.job_id, live.run_id)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(store.purge_terminal_jobs(100, 1000).unwrap(), 1);
    assert!(store.get_transfer(&done.job_id).unwrap().is_none());
    assert!(store.get_transfer(&live.job_id).unwrap().is_some());
}

#[test]
fn memory_purge_never_touches_live_jobs() {
    assert_purge_never_touches_live_jobs(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_purge_never_touches_live_jobs() {
    assert_purge_never_touches_live_jobs(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
}

fn assert_purge_jobs_requires_empty_tasks<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "held-job", &[10]);
    terminalize(&*store, &job);
    snapshot_for_purge(&*store, &job);
    assert_eq!(store.purge_terminal_jobs(100, 1000).unwrap(), 0);
    assert_eq!(store.purge_terminal_tasks(100, 1000).unwrap(), 1);
    assert_eq!(store.purge_terminal_jobs(100, 1000).unwrap(), 1);
}

#[test]
fn memory_purge_jobs_requires_empty_tasks() {
    assert_purge_jobs_requires_empty_tasks(Arc::new(MemoryTransferStore::new()));
}

fn assert_purge_skips_jobs_without_snapshot<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "nosnap-job", &[10, 10]);
    terminalize(&*store, &job);
    assert_eq!(store.purge_terminal_tasks(100, 1000).unwrap(), 0);
    assert_eq!(
        store
            .list_transfer_tasks(&job.job_id, job.run_id)
            .unwrap()
            .len(),
        2
    );
    snapshot_for_purge(&*store, &job);
    assert_eq!(store.purge_terminal_tasks(100, 1000).unwrap(), 2);
}

#[test]
fn memory_purge_skips_jobs_without_snapshot() {
    assert_purge_skips_jobs_without_snapshot(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_purge_skips_jobs_without_snapshot() {
    assert_purge_skips_jobs_without_snapshot(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_purge_jobs_requires_empty_tasks() {
    assert_purge_jobs_requires_empty_tasks(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

/// Crash-critical: a dead worker's delayed terminal report must never clobber
/// the fenced state, and a wrong-attempt terminal must not land either.
fn assert_late_terminal_reports_rejected<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "late-term-job", &[384]);
    assert!(start(&*store, &job, "task-0", 0));
    let mut done = running_report(&job, "task-0", 384, 4);
    done.state = TransferTaskState::Completed;
    assert!(store.update_task_report(done).unwrap());
    assert_eq!(loaded_of(&*store, &job, "task-0"), 384);
    // Late Running from the same (now-terminal) attempt is rejected.
    assert!(!store
        .update_task_report(running_report(&job, "task-0", 100, 5))
        .unwrap());
    // Terminal from an attempt that never owned the task is rejected.
    let mut foreign = running_report(&job, "task-0", 384, 6);
    foreign.attempt_id = 99;
    foreign.state = TransferTaskState::Completed;
    assert!(!store.update_task_report(foreign).unwrap());
    assert_eq!(loaded_of(&*store, &job, "task-0"), 384);
}

fn assert_missing_snapshot_reports_expired<S: TransferStore>(store: Arc<S>) {
    // Terminal job with no rows and no snapshot (pre-C1 shape): status must
    // say expired rather than presenting an empty live aggregate as truth.
    let job = planned_store(store.clone(), "nosnap-status-job", &[]);
    terminalize(&*store, &job);
    let service = TransferService::new(store.clone());
    let (_status, _counts, tasks, _, expired) = service
        .get_transfer_status(&job.job_id, Some(10), None)
        .unwrap();
    assert!(tasks.is_empty());
    assert!(expired);
}

fn assert_cleanup_lease_excludes_other_owners<S: TransferStore>(store: Arc<S>) {
    assert!(store
        .try_acquire_cleanup_lease("scheduler-a", 100, 60)
        .unwrap());
    assert!(!store
        .try_acquire_cleanup_lease("scheduler-b", 120, 60)
        .unwrap());
    assert!(store
        .try_acquire_cleanup_lease("scheduler-a", 120, 60)
        .unwrap());
    assert!(store
        .try_acquire_cleanup_lease("scheduler-b", 181, 60)
        .unwrap());
}

fn assert_cleanup_purge_is_lease_fenced<S: TransferStore>(store: Arc<S>) {
    let job = planned_store(store.clone(), "fenced-purge-job", &[10, 10]);
    terminalize(&*store, &job);
    snapshot_for_purge(&*store, &job);
    assert!(store
        .try_acquire_cleanup_lease("scheduler-a", 100, 60)
        .unwrap());
    assert_eq!(
        store
            .purge_terminal_tasks_with_cleanup_lease(100, 1_000, "scheduler-b", 101,)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .list_transfer_tasks(&job.job_id, job.run_id)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .purge_terminal_tasks_with_cleanup_lease(100, 1_000, "scheduler-a", 101,)
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .purge_terminal_jobs_with_cleanup_lease(100, 200, "scheduler-b", 102)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .purge_terminal_jobs_with_cleanup_lease(100, 200, "scheduler-a", 102)
            .unwrap(),
        1
    );
}

fn assert_first_cleanup_lease_is_exclusive<S: TransferStore>(store: Arc<S>) {
    let barrier = Arc::new(Barrier::new(3));
    let attempts = ["scheduler-a", "scheduler-b"].map(|owner| {
        let store = store.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store.try_acquire_cleanup_lease(owner, 100, 60).unwrap()
        })
    });
    barrier.wait();
    let acquired = attempts
        .into_iter()
        .map(|attempt| attempt.join().unwrap())
        .filter(|acquired| *acquired)
        .count();
    assert_eq!(acquired, 1);
}

fn assert_stale_marking_rejects_oversized_transaction<S: TransferStore>(store: Arc<S>) {
    let candidates = (0..=1_000)
        .map(|index| task("bound-job", &format!("task-{index}"), 1))
        .collect();
    let err = store
        .mark_stale_attempts("bound-job", 1, "owner", 1, 1, candidates)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("transfer store transaction exceeds 1000 task rows"));
}

#[test]
fn memory_cleanup_lease_excludes_other_owners() {
    assert_cleanup_lease_excludes_other_owners(Arc::new(MemoryTransferStore::new()));
}

#[test]
fn memory_cleanup_purge_is_lease_fenced() {
    assert_cleanup_purge_is_lease_fenced(Arc::new(MemoryTransferStore::new()));
}

#[test]
fn memory_first_cleanup_lease_is_exclusive() {
    assert_first_cleanup_lease_is_exclusive(Arc::new(MemoryTransferStore::new()));
}

#[test]
fn memory_stale_marking_rejects_oversized_transaction() {
    assert_stale_marking_rejects_oversized_transaction(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_cleanup_lease_excludes_other_owners() {
    assert_cleanup_lease_excludes_other_owners(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_cleanup_purge_is_lease_fenced() {
    assert_cleanup_purge_is_lease_fenced(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_first_cleanup_lease_is_exclusive() {
    assert_first_cleanup_lease_is_exclusive(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_stale_marking_rejects_oversized_transaction() {
    assert_stale_marking_rejects_oversized_transaction(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

#[test]
fn memory_late_terminal_reports_rejected() {
    assert_late_terminal_reports_rejected(Arc::new(MemoryTransferStore::new()));
}

#[test]
fn memory_missing_snapshot_reports_expired() {
    assert_missing_snapshot_reports_expired(Arc::new(MemoryTransferStore::new()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_late_terminal_reports_rejected() {
    assert_late_terminal_reports_rejected(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_missing_snapshot_reports_expired() {
    assert_missing_snapshot_reports_expired(Arc::new(
        SqliteTransferStore::open(":memory:").unwrap(),
    ));
}
