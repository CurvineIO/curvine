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

use curvine_fs_api::RpcCode;
use curvine_metrics::Metrics;
use curvine_model::{
    TaskAttemptStart, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferTaskRecord, TransferTaskReport, TransferTaskState,
};
use curvine_proto::{
    TransferProgressProto, TransferTaskReportBatchRequest, TransferTaskReportBatchResponse,
    TransferTaskReportRequest,
};
use curvine_rpc::handler::MessageHandler;
use curvine_rpc::message::MessageBuilder;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::scheduler::stale_retry_exhausted_message;
#[cfg(feature = "transfer-store-sqlite")]
use super::SqliteTransferStore;
use super::{
    MemoryTransferStore, TransferHandler, TransferPlannedTasks, TransferService, TransferStore,
    TransferStoreBackend, TransferTaskStateUpdate,
};

fn running_store() -> (Arc<MemoryTransferStore>, TransferJobRecord) {
    running_store_for(Arc::new(MemoryTransferStore::new()))
}

fn running_store_for<S: TransferStore>(store: Arc<S>) -> (Arc<S>, TransferJobRecord) {
    let job = TransferJobRecord {
        job_key: "report-batch-key".to_string(),
        job_id: "report-batch-job".to_string(),
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
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: "report-batch-request".to_string(),
        submitter: "test".to_string(),
        tenant: "test".to_string(),
        created_at: 1,
        updated_at: 1,
    };
    store
        .create_or_get_by_request_id_checked(job.clone())
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("owner", 60_000, 1, 2, false)
        .unwrap()
        .unwrap();
    let task = |task_id: &str| TransferTaskRecord {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: task_id.to_string(),
        attempt_id: 0,
        source_path: "file:///source".to_string(),
        target_path: format!("/target/{task_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: "{}".to_string(),
        report_target_json: "{}".to_string(),
        state: TransferTaskState::Pending,
        progress: TransferProgress::default(),
        estimated_size: 0,
        retry_count: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        block_locations: Vec::new(),
        updated_at: 1,
        replace_target: false,
    };
    assert!(store
        .persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: vec![task("task-1"), task("task-2")],
            message: "planned".to_string(),
            now_ms: 2,
        })
        .unwrap());
    for task_id in ["task-1", "task-2"] {
        assert!(store
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
                resume_loaded_size: 0,
            })
            .unwrap());
    }
    (store, job)
}

fn progress_report(
    job: &TransferJobRecord,
    task_id: &str,
    loaded_size: i64,
) -> TransferTaskReportRequest {
    TransferTaskReportRequest {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: task_id.to_string(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".to_string(),
        state: TransferTaskState::Running as i32,
        progress: TransferProgressProto {
            loaded_size,
            total_size: 100,
            update_time: loaded_size,
            message: task_id.to_string(),
        },
        protocol_version: Some(1),
        accepted_at_ms: None,
        first_byte_at_ms: None,
        attempt_started_at_ms: None,
        block_locations: Vec::new(),
    }
}

fn task_progress(
    store: &MemoryTransferStore,
    job: &TransferJobRecord,
    task_id: &str,
) -> TransferProgress {
    store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == task_id)
        .unwrap()
        .progress
}

fn wait_for_task_progress(
    store: &MemoryTransferStore,
    job: &TransferJobRecord,
    task_id: &str,
    loaded_size: i64,
) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if task_progress(store, job, task_id).loaded_size == loaded_size {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(task_progress(store, job, task_id).loaded_size, loaded_size);
}

#[test]
fn batch_progress_updates_each_task_and_rejects_terminal_members() {
    let (store, job) = running_store();
    let service =
        TransferService::with_report_queue(store.clone(), Duration::from_secs(60), 16, 1).unwrap();
    let handler = TransferHandler::new(service.clone());
    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTaskBatch)
        .proto_header(TransferTaskReportBatchRequest {
            reports: vec![
                progress_report(&job, "task-1", 10),
                progress_report(&job, "task-2", 20),
            ],
            protocol_version: Some(1),
        })
        .build();
    let response: TransferTaskReportBatchResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();
    let accepted = response.accepted;
    assert_eq!(accepted, vec![true, true]);

    wait_for_task_progress(&store, &job, "task-1", 10);
    wait_for_task_progress(&store, &job, "task-2", 20);

    let mut terminal = progress_report(&job, "task-1", 100);
    terminal.state = TransferTaskState::Completed as i32;
    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTaskBatch)
        .proto_header(TransferTaskReportBatchRequest {
            reports: vec![progress_report(&job, "task-2", 30), terminal.clone()],
            protocol_version: Some(1),
        })
        .build();
    let response: TransferTaskReportBatchResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();
    assert_eq!(response.accepted, vec![true, false]);

    wait_for_task_progress(&store, &job, "task-2", 30);

    let mut pending = progress_report(&job, "task-1", 30);
    pending.state = TransferTaskState::Pending as i32;
    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTaskBatch)
        .proto_header(TransferTaskReportBatchRequest {
            reports: vec![pending],
            protocol_version: Some(1),
        })
        .build();
    let response: TransferTaskReportBatchResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();
    assert_eq!(response.accepted, vec![false]);

    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTask)
        .proto_header(terminal)
        .build();
    let response: curvine_proto::TransferTaskReportResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();
    assert!(response.accepted);
    let task = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "task-1")
        .unwrap();
    assert_eq!(task.state, TransferTaskState::Completed);
    assert_eq!(task.progress.loaded_size, 100);
}

#[test]
fn queued_batch_progress_cannot_overwrite_terminal_report() {
    let (store, job) = running_store();
    let service =
        TransferService::with_report_queue(store.clone(), Duration::from_secs(60), 16, 1).unwrap();
    let handler = TransferHandler::new(service);

    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTaskBatch)
        .proto_header(TransferTaskReportBatchRequest {
            reports: vec![progress_report(&job, "task-1", 10)],
            protocol_version: Some(1),
        })
        .build();
    let _: TransferTaskReportBatchResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();

    let mut terminal = progress_report(&job, "task-1", 100);
    terminal.state = TransferTaskState::Completed as i32;
    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTask)
        .proto_header(terminal)
        .build();
    let response: curvine_proto::TransferTaskReportResponse =
        handler.handle(&request).unwrap().parse_header().unwrap();
    assert!(response.accepted);

    std::thread::sleep(Duration::from_millis(20));
    let task = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "task-1")
        .unwrap();
    assert_eq!(task.state, TransferTaskState::Completed);
    assert_eq!(task.progress.loaded_size, 100);
}

#[test]
fn oversized_batch_is_rejected_before_queueing_reports() {
    let (store, job) = running_store();
    let service =
        TransferService::with_report_queue(store.clone(), Duration::from_secs(60), 16, 1).unwrap();
    let handler = TransferHandler::new(service);
    let request = MessageBuilder::new_rpc(RpcCode::ReportTransferTaskBatch)
        .proto_header(TransferTaskReportBatchRequest {
            reports: (0..257)
                .map(|_| progress_report(&job, "task-1", 10))
                .collect(),
            protocol_version: Some(1),
        })
        .build();

    let err = handler.handle(&request).unwrap_err();
    assert!(err.to_string().contains("maximum size"));
    assert_eq!(task_progress(&store, &job, "task-1").loaded_size, 0);
}

fn assert_status_uses_task_summary_and_store_pagination<S: TransferStore>(
    store: Arc<S>,
    job: TransferJobRecord,
) {
    for (task_id, loaded_size) in [("task-1", 10), ("task-2", 20)] {
        assert!(store
            .update_task_report(TransferTaskReport {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id: task_id.to_string(),
                attempt_id: 1,
                worker_id: 7,
                worker_session_id: "session".to_string(),
                state: TransferTaskState::Running,
                progress: TransferProgress {
                    loaded_size,
                    total_size: 100,
                    update_time: loaded_size,
                    message: task_id.to_string(),
                },
                now_ms: 10,
                stale_deadline_at: 1_000,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
            })
            .unwrap());
    }

    let service = TransferService::new(store);
    let (status, summary, tasks, next_page_token, expired) = service
        .get_transfer_status(&job.job_id, Some(1), None)
        .unwrap();

    assert_eq!(status.summary.loaded_size, 30);
    assert_eq!(status.summary.total_size, 200);
    assert_eq!(summary.running, 2);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, "task-1");
    assert_eq!(next_page_token.as_deref(), Some("1"));
    assert!(!expired);
}

fn assert_stale_marking_uses_probe_snapshot<S: TransferStore>(
    store: Arc<S>,
    job: TransferJobRecord,
) {
    let lease = store.get_transfer(&job.job_id).unwrap().unwrap();
    let mut selected = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "task-1")
        .unwrap();
    selected.progress.message = "assigned Worker does not have this task attempt".to_string();
    let marked = store
        .mark_stale_attempts(
            &job.job_id,
            job.run_id,
            &lease.owner,
            lease.lease_epoch,
            1_001,
            vec![selected],
        )
        .unwrap();
    assert_eq!(marked.len(), 1);

    let tasks = store.list_transfer_tasks(&job.job_id, job.run_id).unwrap();
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.task_id == "task-1")
            .unwrap()
            .state,
        TransferTaskState::Stale
    );
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.task_id == "task-1")
            .unwrap()
            .progress
            .message,
        "assigned Worker does not have this task attempt"
    );
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.task_id == "task-2")
            .unwrap()
            .state,
        TransferTaskState::Running,
        "an unprobed stale task must not be redispatched"
    );
}

fn assert_stale_marking_does_not_overwrite_fresh_report<S: TransferStore>(
    store: Arc<S>,
    job: TransferJobRecord,
) {
    let lease = store.get_transfer(&job.job_id).unwrap().unwrap();
    let selected = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "task-1")
        .unwrap();
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id: selected.task_id.clone(),
            attempt_id: selected.attempt_id,
            worker_id: selected.worker_id,
            worker_session_id: selected.worker_session_id.clone(),
            state: TransferTaskState::Running,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 100,
                update_time: 1_001,
                message: "worker still owns task".to_string(),
            },
            now_ms: 1_001,
            stale_deadline_at: 2_000,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
        })
        .unwrap());

    assert!(store
        .mark_stale_attempts(
            &job.job_id,
            job.run_id,
            &lease.owner,
            lease.lease_epoch,
            1_001,
            vec![selected],
        )
        .unwrap()
        .is_empty());
    let task = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .find(|task| task.task_id == "task-1")
        .unwrap();
    assert_eq!(task.state, TransferTaskState::Running);
    assert_eq!(task.stale_deadline_at, 2_000);
    assert_eq!(task.progress.loaded_size, 10);
}

fn assert_failure_summary_uses_stable_task_order<S: TransferStore>(
    store: Arc<S>,
    job: TransferJobRecord,
) {
    let lease = store.get_transfer(&job.job_id).unwrap().unwrap();
    for (task_id, now_ms, message) in [
        ("task-1", 1_001, "first failure"),
        ("task-2", 1_002, "later failure"),
    ] {
        assert!(store
            .update_task_state(TransferTaskStateUpdate {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                task_id: task_id.to_string(),
                from_states: vec![TransferTaskState::Running],
                state: TransferTaskState::Failed,
                message: message.to_string(),
                now_ms,
                bump_retry: false,
            })
            .unwrap());
    }

    assert_eq!(
        store
            .get_transfer_task_summary(&job.job_id, job.run_id, 1_003)
            .unwrap()
            .progress
            .message,
        "2 transfer task(s) failed; first failure: first failure"
    );
}

#[test]
fn stale_retry_exhaustion_keeps_the_confirmed_missing_diagnosis() {
    let (store, job) = running_store();
    let mut task = store
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    task.retry_count = 4;
    task.progress.message = "assigned Worker does not have this task attempt".to_string();

    assert_eq!(
        stale_retry_exhausted_message(&task),
        "transfer task recovery retries exhausted after 4 stale attempt(s): assigned Worker does not have this task attempt"
    );
}

/// `update_task_reports` must behave exactly like one `update_task_report`
/// call per report: same order, one acceptance flag each, and a fenced report
/// (stale attempt) rejected without disturbing its neighbours. Backends
/// override it to share one commit, so the contract is asserted for the
/// default implementation and for the transactional override.
fn assert_batched_report_write_contract<S: TransferStore>(store: Arc<S>, job: TransferJobRecord) {
    let mut reports = Vec::new();
    for (task_id, loaded) in [("task-1", 10i64), ("task-2", 20)] {
        reports.push(TransferTaskReport {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id: task_id.to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session".to_string(),
            state: TransferTaskState::Running,
            progress: TransferProgress {
                loaded_size: loaded,
                total_size: 100,
                update_time: loaded,
                message: task_id.to_string(),
            },
            now_ms: 1_000,
            stale_deadline_at: 2_000,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
        });
    }
    // Stale attempt: the fencing predicate rejects it while the others land.
    let mut fenced = reports[0].clone();
    fenced.task_id = "task-1".to_string();
    fenced.attempt_id = 99;
    fenced.progress.loaded_size = 999;

    let accepted = store
        .update_task_reports(vec![reports[0].clone(), reports[1].clone(), fenced])
        .expect("batched report write");
    assert_eq!(accepted, vec![true, true, false]);
    let loaded = |task_id: &str| {
        store
            .list_transfer_tasks(&job.job_id, job.run_id)
            .unwrap()
            .into_iter()
            .find(|task| task.task_id == task_id)
            .unwrap()
            .progress
            .loaded_size
    };
    assert_eq!(loaded("task-1"), 10);
    assert_eq!(loaded("task-2"), 20);
}

#[test]
fn memory_batched_report_write_keeps_per_report_acceptance() {
    let (store, job) = running_store();
    assert_batched_report_write_contract(store, job);
}

#[test]
fn backend_delegates_batched_reports_in_one_store_operation() {
    let backend = Arc::new(TransferStoreBackend::Memory(MemoryTransferStore::new()));
    let (store, job) = running_store_for(backend);
    assert_batched_report_write_contract(store, job);

    let metrics = Metrics::text_output().unwrap();
    assert!(metrics.contains(
        "transfer_store_operation_total{backend=\"memory\",operation=\"update_task_reports\",result=\"success\"} 1"
    ));
    assert!(!metrics.contains(
        "transfer_store_operation_total{backend=\"memory\",operation=\"update_task_report\",result=\"success\"}"
    ));
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_batched_report_write_keeps_per_report_acceptance() {
    let (store, job) = running_store_for(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
    assert_batched_report_write_contract(store, job);
}

#[test]
fn memory_failure_summary_uses_stable_task_order() {
    let (store, job) = running_store();
    assert_failure_summary_uses_stable_task_order(store, job);
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_failure_summary_uses_stable_task_order() {
    let (store, job) = running_store_for(Arc::new(SqliteTransferStore::open(":memory:").unwrap()));
    assert_failure_summary_uses_stable_task_order(store, job);
}

#[test]
fn memory_status_uses_task_summary_and_store_pagination_not_the_job_snapshot() {
    let (store, job) = running_store();
    assert_status_uses_task_summary_and_store_pagination(store, job);
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_status_uses_task_summary_and_store_pagination_not_the_job_snapshot() {
    let store = Arc::new(SqliteTransferStore::open(":memory:").unwrap());
    let (store, job) = running_store_for(store);
    assert_status_uses_task_summary_and_store_pagination(store, job);
}

#[test]
fn memory_stale_marking_only_changes_probed_missing_attempts() {
    let (store, job) = running_store();
    assert_stale_marking_uses_probe_snapshot(store, job);
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_stale_marking_only_changes_probed_missing_attempts() {
    let store = Arc::new(SqliteTransferStore::open(":memory:").unwrap());
    let (store, job) = running_store_for(store);
    assert_stale_marking_uses_probe_snapshot(store, job);
}

#[test]
fn memory_stale_marking_does_not_overwrite_fresh_report() {
    let (store, job) = running_store();
    assert_stale_marking_does_not_overwrite_fresh_report(store, job);
}

#[test]
#[cfg(feature = "transfer-store-sqlite")]
fn sqlite_stale_marking_does_not_overwrite_fresh_report() {
    let store = Arc::new(SqliteTransferStore::open(":memory:").unwrap());
    let (store, job) = running_store_for(store);
    assert_stale_marking_does_not_overwrite_fresh_report(store, job);
}
