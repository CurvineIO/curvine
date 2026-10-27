use super::super::{now_ms, TransferScheduler};
use crate::common::UfsFactory;
use crate::transfer::memory_store::MemoryTransferStore;
use crate::transfer::plan::TransferPlanner;
use crate::transfer::store::TransferStore;
use crate::transfer::ClusterMetadataCache;
use crate::transfer::TransferRuntimeConfig;
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, TransferConf};
use curvine_model::{
    TransferJobRecord, TransferKind, TransferLease, TransferProgress, TransferState,
    TransferTaskRecord, TransferTaskState,
};
use std::sync::Arc;

fn scheduler(store: Arc<MemoryTransferStore>) -> TransferScheduler<MemoryTransferStore> {
    scheduler_with_state(store, Arc::new(TransferRuntimeConfig::with_limit(1)))
}

fn scheduler_with_state(
    store: Arc<MemoryTransferStore>,
    runtime_config: Arc<TransferRuntimeConfig>,
) -> TransferScheduler<MemoryTransferStore> {
    let conf = ClusterConf::default();
    let rt = Arc::new(conf.transfer_server_conf().create_runtime());
    let fs = CurvineFileSystem::with_rt(ClusterConf::default(), rt.clone()).unwrap();
    let cache = ClusterMetadataCache::new(fs.clone());
    let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt));
    let planner = TransferPlanner::new(
        fs.clone(),
        factory.clone(),
        cache.clone(),
        conf.client.clone(),
        1,
        1,
    );
    TransferScheduler::new(
        store,
        planner,
        cache,
        factory,
        fs,
        "test-owner".to_string(),
        vec![],
        TransferConf::default(),
        runtime_config,
    )
}

fn running_job(job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: String::new(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Running,
        owner: "test-owner".to_string(),
        lease_epoch: 1,
        lease_expire_at: now_ms().saturating_add(60_000),
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{job_id}"),
        submitter: "test".to_string(),
        tenant: "test".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn task(job_id: &str, task_id: &str, state: TransferTaskState) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: task_id.to_string(),
        attempt_id: 0,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{job_id}/{task_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: String::new(),
        report_target_json: String::new(),
        state,
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

fn lease(job: &TransferJobRecord) -> TransferLease {
    TransferLease {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        owner: "test-owner".to_string(),
        lease_epoch: 1,
    }
}

#[tokio::test]
async fn pending_redispatch_is_not_starved_by_backoff_failure() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("pending-redispatch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let mut failed = task(&job.job_id, "failed", TransferTaskState::Failed);
    failed.retry_count = 1;
    failed.progress.update_time = now_ms();
    failed.progress.message = "RateLimited by source service".to_string();
    store
        .insert_tasks(vec![
            failed,
            task(&job.job_id, "pending", TransferTaskState::Pending),
        ])
        .unwrap();

    scheduler(store.clone())
        .check_running_transfer(job.clone(), lease(&job))
        .await
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert!(
        stored
            .summary
            .message
            .contains("waiting for worker capacity"),
        "backoff Failed task must not starve Pending-task redispatch"
    );
}

#[tokio::test]
async fn permanent_failure_is_not_masked_by_backoff_failure() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("permanent-and-retryable");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let mut retryable = task(&job.job_id, "retryable", TransferTaskState::Failed);
    retryable.retry_count = 1;
    retryable.progress.update_time = now_ms();
    retryable.progress.message = "RateLimited by source service".to_string();

    let mut permanent = task(&job.job_id, "permanent", TransferTaskState::Failed);
    permanent.progress.message = "transfer task failed".to_string();
    store.insert_tasks(vec![retryable, permanent]).unwrap();

    scheduler(store.clone())
        .check_running_transfer(job.clone(), lease(&job))
        .await
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(stored.state, TransferState::Failed);
}

#[tokio::test]
async fn permanent_failure_beyond_recovery_batch_is_not_masked() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("terminal-after-batch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let now = now_ms();
    let mut tasks = Vec::with_capacity(65);
    for index in 0..TransferConf::DEFAULT_TASK_PROBE_CONCURRENCY {
        let mut retryable = task(
            &job.job_id,
            &format!("retryable-{index:03}"),
            TransferTaskState::Failed,
        );
        retryable.retry_count = 1;
        retryable.updated_at = now.saturating_sub(64 - index as i64);
        retryable.progress.message = "RateLimited by source service".to_string();
        tasks.push(retryable);
    }
    let mut permanent = task(&job.job_id, "terminal", TransferTaskState::Failed);
    permanent.updated_at = now;
    permanent.progress.message = "transfer task failed".to_string();
    tasks.push(permanent);
    store.insert_tasks(tasks).unwrap();

    scheduler(store.clone())
        .check_running_transfer(job.clone(), lease(&job))
        .await
        .unwrap();

    let stored = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(stored.state, TransferState::Failed);
}

#[tokio::test]
async fn due_failure_beyond_backoff_batch_is_requeued() {
    let store = Arc::new(MemoryTransferStore::new());
    let job = running_job("due-after-backoff-batch");
    store.create_or_get_by_request_id(job.clone()).unwrap();

    let now = now_ms();
    let mut tasks = Vec::with_capacity(65);
    for index in 0..TransferConf::DEFAULT_TASK_PROBE_CONCURRENCY {
        let mut backoff = task(
            &job.job_id,
            &format!("backoff-{index:03}"),
            TransferTaskState::Failed,
        );
        backoff.retry_count = 1;
        backoff.progress.update_time = now;
        backoff.progress.message = "RateLimited by source service".to_string();
        tasks.push(backoff);
    }
    let mut due = task(&job.job_id, "due", TransferTaskState::Failed);
    due.retry_count = 1;
    due.progress.update_time = now.saturating_sub(60_000);
    due.progress.message = "RateLimited by source service".to_string();
    tasks.push(due);
    store.insert_tasks(tasks).unwrap();

    assert!(scheduler(store.clone())
        .recover_transient_failed_tasks(&job, &lease(&job), now)
        .await
        .unwrap());
    let due = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "due")
        .unwrap();
    assert_eq!(due.state, TransferTaskState::Pending);
}

/// The concurrency ceiling must be read at dispatch time, not snapshotted at
/// construction: a runtime-config push has to change the very next decision of
/// an already-running scheduler. Both the state setter and the registry (the
/// only production write path) are exercised so the test fails if either the
/// field goes back to a `usize` snapshot or the read point caches a local.
#[test]
fn dispatch_reads_the_live_worker_task_limit() {
    use crate::transfer::{build_transfer_runtime_registry, WORKER_MAX_CONCURRENT_TASKS_CONFIG};
    use curvine_model::WorkerInfo;

    let conf = ClusterConf::default();
    let state = Arc::new(TransferRuntimeConfig::with_limit(1));
    let scheduler = scheduler_with_state(Arc::new(MemoryTransferStore::new()), state.clone());
    let workers = [WorkerInfo::new(Default::default(), 1)];

    // Ceiling 1: the first decision admits the idle worker and debits the
    // ledger; the second one must be blocked by the same 1-slot ceiling.
    assert!(scheduler.choose_worker(&workers, 10).is_some());
    assert!(scheduler.choose_worker(&workers, 10).is_none());

    // Raising the live value alone (no new scheduler) admits the next decision.
    assert_eq!(state.update_worker_task_limit(2).unwrap(), 1);
    assert!(
        scheduler.choose_worker(&workers, 10).is_some(),
        "scheduler must observe the raised live limit without being rebuilt"
    );

    // The registry is the production write path and must move the same atomic.
    let registry = build_transfer_runtime_registry(state.clone(), &conf);
    registry
        .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "1", None)
        .unwrap();
    assert_eq!(state.worker_task_limit(), 1);
    // Two tasks are already in flight, so a ceiling of 1 blocks dispatch again.
    assert!(scheduler.choose_worker(&workers, 10).is_none());
    registry
        .set(WORKER_MAX_CONCURRENT_TASKS_CONFIG, "9", None)
        .unwrap();
    assert!(scheduler.choose_worker(&workers, 10).is_some());
}
