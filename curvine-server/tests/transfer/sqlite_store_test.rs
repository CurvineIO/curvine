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

use curvine_common::error::ErrorKind;
use curvine_common::state::{
    TaskAttemptStart, TransferCommand, TransferJobRecord, TransferKind, TransferListFilter,
    TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport,
    TransferTaskState,
};
use curvine_server::transfer::{SqliteTransferStore, TransferStore};
use rusqlite::{params, Connection};

fn sqlite_store(name: &str) -> SqliteTransferStore {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-{name}-{}-{}.db",
        std::process::id(),
        orpc::common::LocalTime::mills()
    ));
    SqliteTransferStore::open(path).unwrap()
}

fn job(job_id: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("Load:s3://bucket/{job_id}:/{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("s3://bucket/{job_id}"),
        target_path: format!("/{job_id}"),
        command_json: "{}".to_string(),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("req-{job_id}"),
        submitter: "test".to_string(),
        tenant: "default".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn task(job_id: &str) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job_id.to_string(),
        run_id: 1,
        task_id: "task-1".to_string(),
        attempt_id: 0,
        source_path: format!("s3://bucket/{job_id}"),
        target_path: format!("/{job_id}"),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: String::new(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress::default(),
        retry_count: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 0,
    }
}

#[test]
fn sqlite_finds_conflicting_active_transfer_by_target_path() {
    let store = sqlite_store("target-conflict");
    let mut parent = job("parent");
    parent.target_path = "/a".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let conflict = store
        .find_conflicting_active_transfer("/a/child", "test", "req-other")
        .unwrap()
        .expect("child target should conflict with active parent target");
    assert_eq!(conflict.job_id, "parent");

    let replay = store
        .find_conflicting_active_transfer("/a", "test", "req-parent")
        .unwrap();
    assert!(
        replay.is_none(),
        "same submitter request should be treated as idempotent replay"
    );
}

#[test]
fn sqlite_root_target_conflicts_with_any_active_transfer() {
    let store = sqlite_store("root-target-conflict");
    let mut root = job("root");
    root.target_path = "/".to_string();
    store.create_or_get_by_request_id(root).unwrap();

    let root_conflict = store
        .find_conflicting_active_transfer("/any/path", "test", "req-third")
        .unwrap()
        .expect("root target should conflict with any target");
    assert_eq!(root_conflict.job_id, "root");
}

#[test]
fn sqlite_submit_rejects_root_when_child_target_is_active() {
    let store = sqlite_store("child-before-root-conflict");
    let mut parent = job("parent");
    parent.target_path = "/a".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let mut root = job("root");
    root.target_path = "/".to_string();
    let error = store.create_or_get_by_request_id(root).unwrap_err();
    assert!(
        matches!(error.kind(), ErrorKind::TransferTargetConflict),
        "unexpected conflict error: {error}"
    );
}

#[test]
fn sqlite_target_conflict_detects_deep_ancestor_without_reverse_like() {
    let store = sqlite_store("deep-ancestor-conflict");
    let mut parent = job("parent");
    parent.target_path = "/a/b".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let conflict = store
        .find_conflicting_active_transfer("/a/b/c/d", "test", "req-other")
        .unwrap()
        .expect("deep child target should conflict with active ancestor target");
    assert_eq!(conflict.job_id, "parent");
}

#[test]
fn sqlite_rejects_same_job_key_with_different_command() {
    let store = sqlite_store("already-running");
    store.create_or_get_by_request_id(job("first")).unwrap();

    let mut second = job("second");
    second.job_key = "Load:s3://bucket/first:/first".to_string();
    second.source_path = "s3://bucket/first".to_string();
    second.target_path = "/first".to_string();
    second.client_request_id = "req-second".to_string();
    second.command_json = "{\"overwrite\":true}".to_string();

    let err = store.create_or_get_by_request_id(second).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::TransferAlreadyRunning));
}

#[test]
fn sqlite_report_requires_current_attempt_and_worker_session() {
    let store = sqlite_store("report-session");
    store.create_or_get_by_request_id(job("job-1")).unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 3,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 10,
            stale_deadline_at: 70,
        })
        .unwrap());

    let mut report = TransferTaskReport {
        job_id: "job-1".to_string(),
        run_id: 1,
        task_id: "task-1".to_string(),
        attempt_id: 3,
        worker_id: 10,
        worker_session_id: "session-old".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress {
            loaded_size: 1,
            total_size: 10,
            update_time: 20,
            message: String::new(),
        },
        now_ms: 20,
        stale_deadline_at: 80,
    };
    assert!(!store.update_task_report(report.clone()).unwrap());

    report.worker_session_id = "session-a".to_string();
    assert!(store.update_task_report(report).unwrap());
}

#[test]
fn sqlite_start_task_attempt_rejects_cancel_requested_job() {
    let store = sqlite_store("cancel-start");
    store.create_or_get_by_request_id(job("job-1")).unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store.request_cancel("job-1", 1, 11).unwrap());
    assert_eq!(
        store.get_transfer("job-1").unwrap().unwrap().state,
        TransferState::Canceling
    );

    assert!(!store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner,
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 12,
            stale_deadline_at: 72,
        })
        .unwrap());

    let task = store.list_transfer_tasks("job-1", 1).unwrap().remove(0);
    assert_eq!(task.state, TransferTaskState::Pending);
}

#[test]
fn sqlite_stale_attempt_requires_current_owner_and_epoch() {
    let store = sqlite_store("stale-owner");
    store.create_or_get_by_request_id(job("job-1")).unwrap();
    store.insert_tasks(vec![task("job-1")]).unwrap();
    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "job-1".to_string(),
            run_id: 1,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 20,
            stale_deadline_at: 30,
        })
        .unwrap());

    let stale = store
        .mark_stale_attempts("job-1", 1, "owner-b", lease.lease_epoch, 40, 10)
        .unwrap();
    assert!(stale.is_empty());

    let stale = store
        .mark_stale_attempts("job-1", 1, &lease.owner, lease.lease_epoch, 40, 10)
        .unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].task.state, TransferTaskState::Stale);
    assert_eq!(stale[0].task.retry_count, 1);
}

#[test]
fn sqlite_lease_takeover_waits_for_expiry_and_blocks_stale_owner() {
    let store = sqlite_store("lease-takeover");
    store.create_or_get_by_request_id(job("job-1")).unwrap();

    let lease_a = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease_a.owner, "owner-a");
    assert_eq!(lease_a.lease_epoch, 1);

    let not_expired = store
        .acquire_runnable_transfer("owner-b", 100, 50, 100)
        .unwrap();
    assert!(not_expired.is_none());

    let lease_b = store
        .acquire_runnable_transfer("owner-b", 100, 111, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease_b.owner, "owner-b");
    assert_eq!(lease_b.lease_epoch, 2);

    let stale_owner_update = TransferStateUpdate {
        job_id: lease_a.job_id.clone(),
        run_id: lease_a.run_id,
        owner: lease_a.owner,
        lease_epoch: lease_a.lease_epoch,
        from_states: vec![TransferState::Pending],
        to_state: TransferState::Planning,
        message: "stale owner should not update".to_string(),
        now_ms: 120,
    };
    assert!(!store.update_transfer_state(stale_owner_update).unwrap());

    let current_owner_update = TransferStateUpdate {
        job_id: lease_b.job_id,
        run_id: lease_b.run_id,
        owner: lease_b.owner,
        lease_epoch: lease_b.lease_epoch,
        from_states: vec![TransferState::Planning],
        to_state: TransferState::Planning,
        message: "current owner may update".to_string(),
        now_ms: 121,
    };
    assert!(store.update_transfer_state(current_owner_update).unwrap());
}

#[test]
fn sqlite_counts_only_active_transfers_and_finds_request_id() {
    let store = sqlite_store("active-count");
    let active = store.create_or_get_by_request_id(job("active")).unwrap();
    let mut completed = job("completed");
    completed.state = TransferState::Completed;
    store.create_or_get_by_request_id(completed).unwrap();

    assert_eq!(store.count_active_transfers().unwrap(), 1);

    let by_request = store
        .get_transfer_by_request(&active.submitter, &active.client_request_id)
        .unwrap()
        .unwrap();
    assert_eq!(by_request.job_id, active.job_id);
}

#[test]
fn sqlite_acquire_keeps_pending_backlog_when_execution_window_is_full() {
    let store = sqlite_store("execution-window");
    store.create_or_get_by_request_id(job("pending")).unwrap();

    let mut running = job("running");
    running.state = TransferState::Running;
    store.create_or_get_by_request_id(running).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 1)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "running");
    assert!(store
        .acquire_runnable_transfer("owner-a", 100, 10, 1)
        .unwrap()
        .is_none());
    assert_eq!(
        store.get_transfer("pending").unwrap().unwrap().state,
        TransferState::Pending
    );
}

#[test]
fn sqlite_acquire_pending_prefers_tenant_with_fewer_executing_jobs() {
    let store = sqlite_store("tenant-fairness");

    let mut running = job("running-noisy");
    running.target_path = "/running-noisy".to_string();
    running.job_key = "Load:s3://bucket/running-noisy:/running-noisy".to_string();
    running.tenant = "noisy".to_string();
    running.state = TransferState::Running;
    running.owner = "owner-running".to_string();
    running.lease_expire_at = 1_000;
    running.updated_at = 10;
    store.create_or_get_by_request_id(running).unwrap();

    let mut noisy_pending = job("pending-noisy");
    noisy_pending.target_path = "/pending-noisy".to_string();
    noisy_pending.job_key = "Load:s3://bucket/pending-noisy:/pending-noisy".to_string();
    noisy_pending.tenant = "noisy".to_string();
    noisy_pending.updated_at = 1;
    store.create_or_get_by_request_id(noisy_pending).unwrap();

    let mut quiet_pending = job("pending-quiet");
    quiet_pending.target_path = "/pending-quiet".to_string();
    quiet_pending.job_key = "Load:s3://bucket/pending-quiet:/pending-quiet".to_string();
    quiet_pending.tenant = "quiet".to_string();
    quiet_pending.updated_at = 100;
    store.create_or_get_by_request_id(quiet_pending).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 200, 2)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending-quiet");
}

#[test]
fn sqlite_acquire_pending_with_builtin_fairness_prefers_tenant_without_executing_jobs() {
    let store = sqlite_store("tenant-fifo");

    let mut running = job("running-noisy");
    running.target_path = "/running-noisy".to_string();
    running.job_key = "Load:s3://bucket/running-noisy:/running-noisy".to_string();
    running.tenant = "noisy".to_string();
    running.state = TransferState::Running;
    running.owner = "owner-running".to_string();
    running.lease_expire_at = 1_000;
    running.updated_at = 10;
    store.create_or_get_by_request_id(running).unwrap();

    let mut noisy_pending = job("pending-noisy");
    noisy_pending.target_path = "/pending-noisy".to_string();
    noisy_pending.job_key = "Load:s3://bucket/pending-noisy:/pending-noisy".to_string();
    noisy_pending.tenant = "noisy".to_string();
    noisy_pending.updated_at = 1;
    store.create_or_get_by_request_id(noisy_pending).unwrap();

    let mut quiet_pending = job("pending-quiet");
    quiet_pending.target_path = "/pending-quiet".to_string();
    quiet_pending.job_key = "Load:s3://bucket/pending-quiet:/pending-quiet".to_string();
    quiet_pending.tenant = "quiet".to_string();
    quiet_pending.updated_at = 100;
    store.create_or_get_by_request_id(quiet_pending).unwrap();

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 200, 2)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending-quiet");
}

#[test]
fn sqlite_list_tenant_summaries_counts_states_and_orders_active_tenants() {
    let store = sqlite_store("tenant-summary");
    for (job_id, tenant, state) in [
        ("a-pending", "tenant-a", TransferState::Pending),
        ("a-running", "tenant-a", TransferState::Running),
        ("a-completed", "tenant-a", TransferState::Completed),
        ("b-failed", "tenant-b", TransferState::Failed),
        ("b-canceled", "tenant-b", TransferState::Canceled),
        ("c-pending", "tenant-c", TransferState::Pending),
    ] {
        let mut job = job(job_id);
        job.tenant = tenant.to_string();
        job.state = state;
        store.create_or_get_by_request_id(job).unwrap();
    }

    let summaries = store.list_tenant_summaries(10, 0).unwrap();
    assert_eq!(summaries.len(), 3);
    assert_eq!(summaries[0].tenant, "tenant-a");
    assert_eq!(summaries[0].pending, 1);
    assert_eq!(summaries[0].executing, 1);
    assert_eq!(summaries[0].completed, 1);
    assert_eq!(summaries[1].tenant, "tenant-c");
    assert_eq!(summaries[2].tenant, "tenant-b");
    assert_eq!(summaries[2].failed, 1);
    assert_eq!(summaries[2].canceled, 1);

    let page = store.list_tenant_summaries(1, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].tenant, "tenant-c");
}

#[test]
fn sqlite_purge_terminal_transfers_keeps_active_jobs() {
    let store = sqlite_store("purge-terminal");
    let mut active = job("active");
    active.updated_at = 1;
    store.create_or_get_by_request_id(active).unwrap();

    let mut completed = job("completed");
    completed.state = TransferState::Completed;
    completed.updated_at = 1;
    store.create_or_get_by_request_id(completed).unwrap();
    store.insert_tasks(vec![task("completed")]).unwrap();

    assert_eq!(store.purge_terminal_transfers(10, 100).unwrap(), 1);
    assert!(store.get_transfer("active").unwrap().is_some());
    assert!(store.get_transfer("completed").unwrap().is_none());
    assert!(store
        .list_transfer_tasks("completed", 1)
        .unwrap()
        .is_empty());
}

#[test]
fn sqlite_list_transfers_filters_by_submitter_and_tenant() {
    let store = sqlite_store("list-filter-tenant");
    let mut flink = job("flink");
    flink.submitter = "flink".to_string();
    flink.tenant = "tenant-a".to_string();
    flink.updated_at = 30;
    store.create_or_get_by_request_id(flink).unwrap();

    let mut starrocks = job("starrocks");
    starrocks.submitter = "starrocks".to_string();
    starrocks.tenant = "tenant-b".to_string();
    starrocks.updated_at = 20;
    store.create_or_get_by_request_id(starrocks).unwrap();

    let tenant_jobs = store
        .list_transfers(TransferListFilter {
            tenant: Some("tenant-a".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(tenant_jobs.len(), 1);
    assert_eq!(tenant_jobs[0].job_id, "flink");

    let submitter_jobs = store
        .list_transfers(TransferListFilter {
            submitter: Some("starrocks".to_string()),
            limit: 10,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(submitter_jobs.len(), 1);
    assert_eq!(submitter_jobs[0].job_id, "starrocks");
}

#[test]
fn sqlite_persists_unfinished_transfer_for_restart_recovery() {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-restart-{}-{}.db",
        std::process::id(),
        orpc::common::LocalTime::mills()
    ));
    {
        let store = SqliteTransferStore::open(&path).unwrap();
        store.create_or_get_by_request_id(job("pending")).unwrap();
        store.insert_tasks(vec![task("pending")]).unwrap();

        let mut completed = job("completed");
        completed.state = TransferState::Completed;
        store.create_or_get_by_request_id(completed).unwrap();
    }

    let reopened = SqliteTransferStore::open(&path).unwrap();
    let pending = reopened.get_transfer("pending").unwrap().unwrap();
    assert_eq!(pending.state, TransferState::Pending);
    assert_eq!(reopened.list_transfer_tasks("pending", 1).unwrap().len(), 1);

    let lease = reopened
        .acquire_runnable_transfer("owner-after-restart", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "pending");
    assert!(reopened
        .acquire_runnable_transfer("owner-after-restart", 100, 10, 100)
        .unwrap()
        .is_none());
}

#[test]
fn sqlite_migrates_v1_schema_and_backfills_target_path() {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-migrate-v1-{}-{}.db",
        std::process::id(),
        orpc::common::LocalTime::mills()
    ));
    let command = TransferCommand {
        kind: TransferKind::Load,
        source_path: "s3://bucket/old".to_string(),
        target_path: "/old-target".to_string(),
        client_request_id: "req-old".to_string(),
        submitter: "test".to_string(),
        tenant: "default".to_string(),
        options: Default::default(),
    };
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "
            create table transfer_schema_version (
                id integer primary key check(id = 1),
                version integer not null,
                updated_at integer not null
            );
            insert into transfer_schema_version(id, version, updated_at) values (1, 1, 1);

            create table transfer_jobs (
                job_id text primary key,
                job_key text not null,
                run_id integer not null,
                kind integer not null,
                source_path text not null,
                command_json text not null,
                mount_snapshot_json text not null,
                secret_ref_json text not null,
                cluster_snapshot_version integer not null,
                cv_metadata_epoch integer,
                state integer not null,
                owner text not null,
                lease_epoch integer not null,
                lease_expire_at integer not null,
                cancel_requested integer not null,
                summary_json text not null,
                client_request_id text not null,
                submitter text not null,
                tenant text not null,
                created_at integer not null,
                updated_at integer not null
            );
            create table transfer_tasks (
                job_id text not null,
                run_id integer not null,
                task_id text not null,
                attempt_id integer not null,
                source_path text not null,
                target_path text not null,
                worker_id integer not null,
                worker_session_id text not null,
                source_read_plan_json text not null,
                report_target_json text not null,
                state integer not null,
                progress_json text not null,
                retry_count integer not null,
                attempt_started_at integer not null,
                last_report_at integer not null,
                stale_deadline_at integer not null,
                updated_at integer not null,
                primary key(job_id, run_id, task_id)
            );
            ",
        )
        .unwrap();
        conn.execute(
            "insert into transfer_jobs (
                job_id, job_key, run_id, kind, source_path, command_json,
                mount_snapshot_json, secret_ref_json, cluster_snapshot_version,
                cv_metadata_epoch, state, owner, lease_epoch, lease_expire_at,
                cancel_requested, summary_json, client_request_id, submitter,
                tenant, created_at, updated_at
            ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                      ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            params![
                "old-job",
                "Load:s3://bucket/old:/old-target",
                1_i64,
                TransferKind::Load as i32,
                "s3://bucket/old",
                serde_json::to_string(&command).unwrap(),
                "{}",
                "{}",
                1_i64,
                Option::<i64>::None,
                TransferState::Pending as i32,
                "",
                0_i64,
                0_i64,
                0_i64,
                serde_json::to_string(&TransferProgress::default()).unwrap(),
                "req-old",
                "test",
                "default",
                1_i64,
                1_i64
            ],
        )
        .unwrap();
    }

    let store = SqliteTransferStore::open(&path).unwrap();
    let migrated = store.get_transfer("old-job").unwrap().unwrap();
    assert_eq!(migrated.target_path, "/old-target");

    let conn = Connection::open(&path).unwrap();
    let version: i64 = conn
        .query_row(
            "select version from transfer_schema_version where id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 2);
}

#[test]
fn sqlite_rejects_future_schema_without_creating_current_tables() {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-future-schema-{}-{}.db",
        std::process::id(),
        orpc::common::LocalTime::mills()
    ));
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "
            create table transfer_schema_version (
                id integer primary key check(id = 1),
                version integer not null,
                updated_at integer not null
            );
            insert into transfer_schema_version(id, version, updated_at) values (1, 999, 1);
            ",
        )
        .unwrap();
    }

    let err = match SqliteTransferStore::open(&path) {
        Ok(_) => panic!("future sqlite schema unexpectedly opened"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("Unsupported sqlite transfer schema version 999"),
        "unexpected error: {err}"
    );

    let conn = Connection::open(&path).unwrap();
    assert!(!sqlite_table_exists(&conn, "transfer_jobs"));
    assert!(!sqlite_table_exists(&conn, "transfer_tasks"));
}

fn sqlite_table_exists(conn: &Connection, table: &str) -> bool {
    conn.query_row(
        "select exists(select 1 from sqlite_master where type = 'table' and name = ?1)",
        params![table],
        |row| row.get::<_, bool>(0),
    )
    .unwrap()
}
