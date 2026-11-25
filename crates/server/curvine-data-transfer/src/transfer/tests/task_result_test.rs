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

use super::store_test::{job, task};
use super::{
    MemoryTransferStore, TransferHandler, TransferService, TransferStore, TransferTaskStateUpdate,
};
use curvine_fs_api::RpcCode;
use curvine_model::{TaskAttemptStart, TransferKind, TransferLease, TransferTaskState};
use curvine_proto::{
    GetTransferStatusRequest, GetTransferStatusResponse, TransferProgressProto,
    TransferTaskReportRequest, TransferTaskReportResponse, TransferTaskStateProto,
};
use curvine_rpc::{handler::MessageHandler, message::Builder};
use std::sync::Arc;
use std::time::Duration;

pub(super) fn create_attempt(store: &impl TransferStore) -> TransferLease {
    let job = job(TransferKind::Load);
    store.create_or_get_by_request_id(job.clone()).unwrap();
    let lease = store
        .acquire_runnable_transfer("scheduler", i64::MAX, 21, 1)
        .unwrap()
        .unwrap();
    store.insert_tasks(vec![task(&job)]).unwrap();
    start_attempt(store, &lease, 1);
    lease
}

fn start_attempt(store: &impl TransferStore, lease: &TransferLease, id: u64) {
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".into(),
            attempt_id: id,
            worker_id: 7,
            worker_session_id: "session".into(),
            report_target_json: String::new(),
            now_ms: 22,
            stale_deadline_at: 100,
        })
        .unwrap());
}

fn report_request(
    lease: &TransferLease,
    state: TransferTaskStateProto,
) -> TransferTaskReportRequest {
    TransferTaskReportRequest {
        job_id: lease.job_id.clone(),
        run_id: lease.run_id,
        task_id: "task-1".into(),
        attempt_id: 1,
        worker_id: 7,
        worker_session_id: "session".into(),
        state: state.into(),
        progress: TransferProgressProto {
            loaded_size: 42,
            total_size: 42,
            update_time: 23,
            message: "reported".into(),
        },
        protocol_version: None,
        result_inline: None,
        result_ref: None,
        result_size: None,
    }
}

fn report(
    handler: &TransferHandler<impl TransferStore>,
    request: TransferTaskReportRequest,
) -> bool {
    let msg = Builder::new()
        .code(RpcCode::ReportTransferTask)
        .proto_header(request)
        .build();
    let response: TransferTaskReportResponse =
        handler.handle(&msg).unwrap().parse_header().unwrap();
    response.accepted
}

fn status(
    handler: &TransferHandler<impl TransferStore>,
    lease: &TransferLease,
) -> GetTransferStatusResponse {
    let msg = Builder::new()
        .code(RpcCode::GetTransferStatus)
        .proto_header(GetTransferStatusRequest {
            job_id: lease.job_id.clone(),
            page_size: Some(1),
            page_token: None,
        })
        .build();
    handler.handle(&msg).unwrap().parse_header().unwrap()
}

pub(super) fn verify_result_lifecycle(store: Arc<impl TransferStore>) {
    let lease = create_attempt(store.as_ref());
    let handler = TransferHandler::new(
        TransferService::with_report_queue(store.clone(), Duration::from_secs(60), 8, 1).unwrap(),
    );
    assert_eq!(status(&handler, &lease).tasks[0].result_size, None);

    let mut req = report_request(&lease, TransferTaskStateProto::TransferTaskRunning);
    req.result_inline = Some(vec![0, 255, 7]);
    req.result_size = Some(3);
    assert!(report(&handler, req.clone()));
    let task = &status(&handler, &lease).tasks[0];
    assert_eq!(task.result_inline, req.result_inline);
    assert_eq!(task.result_size, Some(3));
    assert_eq!(task.result_ref, None);
    assert!(report(&handler, req.clone()));
    assert_eq!(&status(&handler, &lease).tasks[0], task);

    // Progress-only terminal reports must not erase an earlier result.
    let terminal = report_request(&lease, TransferTaskStateProto::TransferTaskCompleted);
    assert!(report(&handler, terminal));
    let response = status(&handler, &lease);
    assert_eq!(response.tasks[0].result_inline, req.result_inline);
    assert_eq!(response.progress.loaded_size, 42);
}

#[test]
fn result_persisted_and_returned() {
    verify_result_lifecycle(Arc::new(MemoryTransferStore::new()));
}

fn verify_stale_attempt_is_dropped(store: Arc<impl TransferStore>) {
    let lease = create_attempt(store.as_ref());
    let handler = TransferHandler::new(TransferService::new(store.clone()));
    let mut old = report_request(&lease, TransferTaskStateProto::TransferTaskRunning);
    old.result_inline = Some(vec![1]);
    old.result_size = Some(1);
    assert!(report(&handler, old.clone()));
    // Expire the old attempt, then put the task back in the scheduler queue.
    assert_eq!(
        store
            .mark_stale_attempts(
                &lease.job_id,
                lease.run_id,
                &lease.owner,
                lease.lease_epoch,
                i64::MAX - 1,
                1
            )
            .unwrap()
            .len(),
        1
    );
    assert_eq!(status(&handler, &lease).tasks[0].result_inline, None);
    assert!(store
        .update_task_state(TransferTaskStateUpdate {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            task_id: old.task_id.clone(),
            from_states: vec![TransferTaskState::Stale],
            state: TransferTaskState::Pending,
            message: "retry".into(),
            now_ms: 24,
        })
        .unwrap());
    assert!(!report(&handler, old.clone()));
    let mut old_progress = old.clone();
    old_progress.result_inline = None;
    old_progress.result_size = None;
    assert!(!report(&handler, old_progress));
    assert_eq!(status(&handler, &lease).tasks[0].result_inline, None);
    start_attempt(store.as_ref(), &lease, 2);
    assert_eq!(status(&handler, &lease).tasks[0].result_inline, None);
    assert!(!report(&handler, old.clone()));
    for (run, session, worker) in [(2, "session", 7), (1, "old-session", 7), (1, "session", 8)] {
        let mut wrong = old.clone();
        wrong.attempt_id = 2;
        wrong.run_id = run;
        wrong.worker_session_id = session.into();
        wrong.worker_id = worker;
        assert!(!report(&handler, wrong));
    }
    old.attempt_id = 2;
    old.result_inline = Some(vec![2]);
    assert!(report(&handler, old));
    assert_eq!(
        status(&handler, &lease).tasks[0].result_inline,
        Some(vec![2])
    );
}

#[test]
fn stale_attempt_result_is_dropped() {
    verify_stale_attempt_is_dropped(Arc::new(MemoryTransferStore::new()));
    #[cfg(feature = "transfer-store-sqlite")]
    verify_stale_attempt_is_dropped(Arc::new(
        super::SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

#[test]
fn invalid_result_does_not_change_progress_or_status() {
    let store = Arc::new(MemoryTransferStore::new());
    let lease = create_attempt(store.as_ref());
    let service = TransferService::new(store);
    let handler = TransferHandler::new(service.clone());
    let before = status(&handler, &lease);
    let mut req = report_request(&lease, TransferTaskStateProto::TransferTaskCompleted);
    req.result_inline = Some(vec![0, 255]);
    req.result_size = Some(1);
    assert!(service.report_task(req.clone()).is_err());
    req.result_size = Some(2);
    req.result_ref = Some("cv:///results/task-1".into());
    assert!(service.report_task(req).is_err());
    assert_eq!(status(&handler, &lease), before);
}

fn verify_late_result(store: Arc<impl TransferStore>) {
    let lease = create_attempt(store.as_ref());
    let handler = TransferHandler::new(TransferService::new(store.clone()));
    let mut req = report_request(&lease, TransferTaskStateProto::TransferTaskCompleted);
    assert!(report(&handler, req.clone()));
    let before = store.list_transfer_tasks(&lease.job_id, lease.run_id).unwrap();
    req.result_inline = Some(vec![7]);
    req.result_size = Some(1);
    req.progress.loaded_size += 1;
    req.progress.update_time += 1;
    let mut stale = req.clone();
    stale.attempt_id += 1;
    assert!(!report(&handler, stale));
    assert!(report(&handler, req.clone()));
    assert!(report(&handler, req.clone()));
    req.result_inline = Some(vec![8]);
    assert!(!report(&handler, req));
    let mut after = store.list_transfer_tasks(&lease.job_id, lease.run_id).unwrap();
    assert!(!after[0].result_json.is_empty());
    after[0].result_json.clear();
    assert_eq!(after, before);
}

#[test]
fn terminal_attempt_accepts_only_its_first_late_result() {
    verify_late_result(Arc::new(MemoryTransferStore::new()));
    #[cfg(feature = "transfer-store-sqlite")]
    verify_late_result(Arc::new(super::SqliteTransferStore::open(":memory:").unwrap()));
}

#[test]
fn repeated_report_is_idempotent() {
    let store = Arc::new(MemoryTransferStore::new());
    let lease = create_attempt(store.as_ref());
    let handler = TransferHandler::new(TransferService::new(store.clone()));
    let mut req = report_request(&lease, TransferTaskStateProto::TransferTaskCompleted);
    req.result_ref = Some("cv:///results/task-1".into());
    req.result_size = Some(u64::MAX);
    assert!(report(&handler, req.clone()));
    let before = status(&handler, &lease);
    assert!(report(&handler, req.clone()));
    req.result_size = Some(1);
    assert!(!report(&handler, req));
    assert_eq!(status(&handler, &lease), before);
    assert_eq!(before.progress.loaded_size, 42);
    assert_eq!(before.tasks[0].result_inline, None);
    assert_eq!(before.tasks[0].result_size, Some(u64::MAX));
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn result_survives_server_restart() {
    use super::SqliteTransferStore;
    let dir = std::env::temp_dir().join(format!("curvine-task-result-{}", uuid::Uuid::new_v4()));
    let path = dir.join("transfer.db");
    let lease = {
        let store = Arc::new(SqliteTransferStore::open(&path).unwrap());
        let lease = create_attempt(store.as_ref());
        let handler = TransferHandler::new(TransferService::new(store));
        let mut req = report_request(&lease, TransferTaskStateProto::TransferTaskCompleted);
        req.result_ref = Some("cv:///results/task-1".into());
        req.result_size = Some(1_000_000);
        assert!(report(&handler, req));
        lease
    };
    let store = Arc::new(SqliteTransferStore::open(&path).unwrap());
    let handler = TransferHandler::new(TransferService::new(store));
    let response = status(&handler, &lease);
    assert_eq!(response.progress.loaded_size, 42);
    assert_eq!(
        response.tasks[0].result_ref.as_deref(),
        Some("cv:///results/task-1")
    );
    assert_eq!(response.tasks[0].result_size, Some(1_000_000));
    assert_eq!(response.tasks[0].result_inline, None);
    drop(handler);
    std::fs::remove_dir_all(dir).unwrap();
}
