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

use curvine_model::{
    TransferJobRecord, TransferKind, TransferProgress, TransferState, TransferStateUpdate,
};
use curvine_server::transfer::{
    MemoryTransferStore, SqliteTransferStore, TransferMountSnapshotUpdate, TransferRequeueUpdate,
    TransferStore,
};
use rusqlite::Connection;

fn transfer_job(id: &str, target_path: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{id}"),
        job_id: format!("job-{id}"),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("file:///source-{id}"),
        target_path: target_path.to_string(),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{id}"),
        submitter: "test".to_string(),
        tenant: String::new(),
        created_at: 1,
        updated_at: 1,
    }
}

fn assert_mount_snapshot_requeue_preserves_request_id<S>(store: &S)
where
    S: TransferStore,
{
    store
        .create_or_get_by_request_id(transfer_job("snapshot", "/cache/snapshot"))
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-1", 60_000, 10, 10, false)
        .unwrap()
        .unwrap();

    assert!(store
        .requeue_transfer(TransferRequeueUpdate {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            message: "mount snapshot refresh delayed".to_string(),
            next_attempt_at_ms: 20,
            mount_snapshot_attempts: Some(1),
            planning_attempts: Some(1),
            now_ms: 11,
        })
        .unwrap());

    let job = store.get_transfer("job-snapshot").unwrap().unwrap();
    assert_eq!(job.state, TransferState::Pending);
    assert_eq!(job.mount_snapshot_attempts, 1);
    assert_eq!(
        store
            .create_or_get_by_request_id(transfer_job("snapshot", "/cache/snapshot"))
            .unwrap()
            .job_id,
        job.job_id
    );
}

#[test]
fn sqlite_mount_snapshot_requeue_preserves_request_id() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-snapshot-requeue-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    assert_mount_snapshot_requeue_preserves_request_id(&store);

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn memory_mount_snapshot_requeue_preserves_request_id() {
    assert_mount_snapshot_requeue_preserves_request_id(&MemoryTransferStore::new());
}

#[test]
fn sqlite_migrates_v2_mount_snapshot_retry_state() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-v2-migration-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    drop(SqliteTransferStore::open(&db_path).unwrap());
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "alter table transfer_jobs drop column mount_snapshot_attempts;
         update transfer_schema_version set version = 2 where id = 1;",
    )
    .unwrap();
    drop(conn);

    let store = SqliteTransferStore::open(&db_path).unwrap();
    assert_mount_snapshot_requeue_preserves_request_id(&store);

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn sqlite_submit_persists_distinct_requests_for_the_same_target() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-durable-submit-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    let first = store
        .create_or_get_by_request_id_checked(transfer_job("first", "/cache/same"))
        .unwrap();
    let second = store
        .create_or_get_by_request_id_checked(transfer_job("second", "/cache/same"))
        .unwrap();

    assert_ne!(first.job_id, second.job_id);
    assert_eq!(store.count_active_transfers().unwrap(), 2);

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn sqlite_target_conflict_ignores_pending_requests() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-pending-conflict-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    store
        .create_or_get_by_request_id_checked(transfer_job("pending", "/cache/same"))
        .unwrap();

    assert!(store
        .find_conflicting_active_transfer("/cache/same", "test", "request-candidate")
        .unwrap()
        .is_none());

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

fn assert_acquire_skips_pending_target_conflicts_without_poisoning_request_ids<S>(store: &S)
where
    S: TransferStore,
{
    assert_acquire_skips_pending_target_conflicts(
        store,
        "/cache/same",
        "/cache/same",
        "/cache/other",
    );
}

fn assert_acquire_skips_pending_target_conflicts<S>(
    store: &S,
    first_target: &str,
    second_target: &str,
    unrelated_target: &str,
) where
    S: TransferStore,
{
    store
        .create_or_get_by_request_id(transfer_job("first", first_target))
        .unwrap();
    let mut second = transfer_job("second", second_target);
    second.updated_at = 2;
    store.create_or_get_by_request_id(second).unwrap();
    let mut unrelated = transfer_job("unrelated", unrelated_target);
    unrelated.updated_at = 3;
    store.create_or_get_by_request_id(unrelated).unwrap();

    let first_lease = store
        .acquire_runnable_transfer("owner-1", 60_000, 10, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(first_lease.job_id, "job-first");
    assert!(store
        .set_transfer_mount_snapshot(TransferMountSnapshotUpdate {
            job_id: "job-first".to_string(),
            run_id: 1,
            owner: "owner-1".to_string(),
            lease_epoch: first_lease.lease_epoch,
            mount_snapshot_json: "{\"mount\":true}".to_string(),
            cluster_snapshot_version: 7,
            now_ms: 11,
        })
        .unwrap());
    assert_eq!(
        store
            .get_transfer("job-first")
            .unwrap()
            .unwrap()
            .mount_snapshot_json,
        "{\"mount\":true}"
    );
    let unrelated_lease = store
        .acquire_runnable_transfer("owner-2", 60_000, 20, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(unrelated_lease.job_id, "job-unrelated");
    assert_eq!(
        store.get_transfer("job-second").unwrap().unwrap().state,
        TransferState::Pending
    );

    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: first_lease.job_id,
            run_id: first_lease.run_id,
            owner: first_lease.owner,
            lease_epoch: first_lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Completed,
            message: "completed".to_string(),
            now_ms: 30,
        })
        .unwrap());

    let second_lease = store
        .acquire_runnable_transfer("owner-3", 60_000, 40, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(second_lease.job_id, "job-second");
    let retried = store
        .create_or_get_by_request_id(transfer_job("second", second_target))
        .unwrap();
    assert_eq!(retried.job_id, "job-second");
    assert_eq!(retried.client_request_id, "request-second");
}

fn assert_adjacent_target_paths_run_concurrently<S>(store: &S)
where
    S: TransferStore,
{
    store
        .create_or_get_by_request_id(transfer_job("parent", "/parent"))
        .unwrap();
    let mut adjacent = transfer_job("adjacent", "/parent-other");
    adjacent.updated_at = 2;
    store.create_or_get_by_request_id(adjacent).unwrap();

    assert_eq!(
        store
            .acquire_runnable_transfer("owner-1", 60_000, 10, 10, false)
            .unwrap()
            .unwrap()
            .job_id,
        "job-parent"
    );
    assert_eq!(
        store
            .acquire_runnable_transfer("owner-2", 60_000, 20, 10, false)
            .unwrap()
            .unwrap()
            .job_id,
        "job-adjacent"
    );
}

fn assert_root_target_waits_for_completion<S>(store: &S)
where
    S: TransferStore,
{
    store
        .create_or_get_by_request_id(transfer_job("root", "/"))
        .unwrap();
    let mut child = transfer_job("root-child", "/parent/child");
    child.updated_at = 2;
    store.create_or_get_by_request_id(child).unwrap();

    let root_lease = store
        .acquire_runnable_transfer("owner-1", 60_000, 10, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(root_lease.job_id, "job-root");
    assert!(store
        .acquire_runnable_transfer("owner-2", 60_000, 20, 10, false)
        .unwrap()
        .is_none());
    assert_eq!(
        store.get_transfer("job-root-child").unwrap().unwrap().state,
        TransferState::Pending
    );

    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: root_lease.job_id,
            run_id: root_lease.run_id,
            owner: root_lease.owner,
            lease_epoch: root_lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Completed,
            message: "completed".to_string(),
            now_ms: 30,
        })
        .unwrap());
    assert_eq!(
        store
            .acquire_runnable_transfer("owner-3", 60_000, 40, 10, false)
            .unwrap()
            .unwrap()
            .job_id,
        "job-root-child"
    );
}

#[test]
fn sqlite_acquire_skips_pending_target_conflicts_without_poisoning_request_ids() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-acquire-conflict-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    assert_acquire_skips_pending_target_conflicts_without_poisoning_request_ids(&store);

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn memory_acquire_skips_pending_target_conflicts_without_poisoning_request_ids() {
    assert_acquire_skips_pending_target_conflicts_without_poisoning_request_ids(
        &MemoryTransferStore::new(),
    );
}

#[test]
fn sqlite_acquire_skips_pending_child_target_conflicts() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-child-conflict-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    assert_acquire_skips_pending_target_conflicts(
        &store,
        "/parent",
        "/parent/child",
        "/parent-other",
    );

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn memory_acquire_skips_pending_child_target_conflicts() {
    assert_acquire_skips_pending_target_conflicts(
        &MemoryTransferStore::new(),
        "/parent",
        "/parent/child",
        "/parent-other",
    );
}

#[test]
fn sqlite_acquire_treats_adjacent_paths_as_non_conflicting() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-adjacent-paths-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    assert_adjacent_target_paths_run_concurrently(&store);

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn memory_acquire_treats_adjacent_paths_as_non_conflicting() {
    assert_adjacent_target_paths_run_concurrently(&MemoryTransferStore::new());
}

#[test]
fn sqlite_acquire_keeps_children_pending_while_root_is_executing() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-root-path-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    assert_root_target_waits_for_completion(&store);

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn memory_acquire_keeps_children_pending_while_root_is_executing() {
    assert_root_target_waits_for_completion(&MemoryTransferStore::new());
}

#[test]
fn sqlite_target_conflict_treats_wildcards_as_path_characters() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    let mut wildcard = transfer_job("wildcard", "/cache/a_b%");
    wildcard.state = TransferState::Planning;
    store.create_or_get_by_request_id(wildcard).unwrap();
    assert!(store
        .find_conflicting_active_transfer("/cache/aXbZ/child", "test", "request-candidate")
        .unwrap()
        .is_none());
    assert!(store
        .find_conflicting_active_transfer("/cache/a_b%/child", "test", "request-candidate")
        .unwrap()
        .is_some());

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn sqlite_target_conflict_does_not_treat_candidate_parent_as_like_pattern() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-wildcard-parent-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    let mut child = transfer_job("child", "/cache/aXbZ/child");
    child.state = TransferState::Planning;
    store.create_or_get_by_request_id(child).unwrap();
    assert!(store
        .find_conflicting_active_transfer("/cache/a_b%", "test", "request-candidate")
        .unwrap()
        .is_none());

    drop(store);
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn sqlite_request_id_cannot_be_reused_for_a_different_command() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-request-id-{}-{}.db",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();
    let job = transfer_job("request-id", "/cache/first");
    store
        .create_or_get_by_request_id_checked(job.clone())
        .unwrap();

    let mut conflicting = job;
    conflicting.target_path = "/cache/second".to_string();
    conflicting.command_json = "{\"overwrite\":true}".to_string();
    assert!(store
        .create_or_get_by_request_id_checked(conflicting)
        .is_err());

    drop(store);
    let _ = std::fs::remove_file(db_path);
}
