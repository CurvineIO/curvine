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

use curvine_data_transfer::transfer::{
    PostgresTransferStore, TransferMountSnapshotUpdate, TransferPlannedTasks,
    TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate,
};
use curvine_model::{
    TaskAttemptStart, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
};
use native_tls::TlsConnector;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static TEST_SCHEMA_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestSchema {
    url: String,
    name: String,
}

impl Drop for TestSchema {
    fn drop(&mut self) {
        if let Ok(mut client) = admin_client(&self.url) {
            let _ = client.batch_execute(&format!("drop schema if exists {} cascade", self.name));
        }
    }
}

fn admin_client(url: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let config = Config::from_str(url)?;
    let tls = TlsConnector::new()?;
    Ok(config.connect(MakeTlsConnector::new(tls))?)
}

fn test_store_url() -> Option<(String, TestSchema)> {
    let url = std::env::var("CURVINE_TRANSFER_POSTGRES_URL").ok()?;
    let name = format!(
        "curvine_transfer_test_{}_{}_{}",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills(),
        TEST_SCHEMA_SEQUENCE.fetch_add(1, Ordering::Relaxed),
    );
    let mut client = admin_client(&url).unwrap();
    client
        .batch_execute(&format!("create schema {name}"))
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let store_url = format!("{url}{separator}options=-c%20search_path%3D{name}");
    Some((store_url, TestSchema { url, name }))
}

#[test]
fn postgres_store_rejects_an_unreadable_root_certificate() {
    let err = match PostgresTransferStore::open(
        "postgresql://transfer@localhost:5432/curvine_transfer?sslmode=require&sslrootcert=/curvine-missing-root-ca.pem",
    ) {
        Ok(_) => panic!("missing root certificate must reject the store URL"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("Unable to read PostgreSQL root certificate"));
}

#[test]
fn postgres_store_ignores_root_certificate_when_tls_is_disabled() {
    let Some(url) = std::env::var("CURVINE_TRANSFER_POSTGRES_URL").ok() else {
        eprintln!(
            "CURVINE_TRANSFER_POSTGRES_URL is not set; skipping PostgreSQL sslmode=disable test"
        );
        return;
    };
    let config = Config::from_str(&url).unwrap();
    if config.get_ssl_mode() != postgres::config::SslMode::Disable {
        eprintln!("PostgreSQL URL does not use sslmode=disable; skipping test");
        return;
    }
    if url.contains("sslrootcert=") {
        eprintln!("PostgreSQL URL already contains sslrootcert; skipping test");
        return;
    }
    let url = format!("{url}&sslrootcert=/curvine-missing-root-ca.pem");

    PostgresTransferStore::open(&url)
        .unwrap()
        .check_available()
        .unwrap();
}

#[test]
fn postgres_store_connects_with_a_private_ca() {
    let Some(url) = std::env::var("CURVINE_TRANSFER_POSTGRES_TLS_URL").ok() else {
        eprintln!("CURVINE_TRANSFER_POSTGRES_TLS_URL is not set; skipping PostgreSQL TLS test");
        return;
    };

    PostgresTransferStore::open(&url)
        .unwrap()
        .check_available()
        .unwrap();
}

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
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{id}"),
        submitter: "postgres-store-test".to_string(),
        tenant: "postgres-store-test".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn transfer_task(job: &TransferJobRecord) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: "task-1".to_string(),
        attempt_id: 0,
        source_path: job.source_path.clone(),
        target_path: job.target_path.clone(),
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
        updated_at: 1,
    }
}

#[test]
fn postgres_store_persists_transfer_lifecycle() {
    let Some((url, _schema)) = test_store_url() else {
        eprintln!("CURVINE_TRANSFER_POSTGRES_URL is not set; skipping PostgreSQL store test");
        return;
    };
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
    );
    let job = transfer_job(&suffix, &format!("/postgres-store-test/{suffix}"));
    let child = transfer_job(
        &format!("{suffix}-child"),
        &format!("{}/child", job.target_path),
    );
    let store = PostgresTransferStore::open(&url).unwrap();

    assert_eq!(
        store
            .create_or_get_by_request_id_checked(job.clone())
            .unwrap()
            .job_id,
        job.job_id
    );
    assert_eq!(
        store
            .create_or_get_by_request_id_checked(job.clone())
            .unwrap()
            .job_id,
        job.job_id
    );
    assert_eq!(
        store
            .create_or_get_by_request_id_checked(child.clone())
            .unwrap()
            .job_id,
        child.job_id
    );
    let wildcard = transfer_job(&format!("{suffix}-wildcard"), "/cache/a_b%");
    let unrelated = transfer_job(&format!("{suffix}-unrelated"), "/cache/aXbZ/child");
    let wildcard_child = transfer_job(&format!("{suffix}-wildcard-child"), "/cache/a_b%/child");
    store.create_or_get_by_request_id_checked(wildcard).unwrap();
    store
        .create_or_get_by_request_id_checked(unrelated)
        .unwrap();
    store
        .create_or_get_by_request_id_checked(wildcard_child)
        .unwrap();

    let lease = store
        .acquire_runnable_transfer("postgres-store-test", 60_000, 2, 1, false)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, job.job_id);
    let task = transfer_task(&job);
    assert!(store
        .persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: vec![task],
            message: "planned".to_string(),
            now_ms: 3,
        })
        .unwrap());
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session-1".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 4,
            stale_deadline_at: 100,
        })
        .unwrap());
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session-1".to_string(),
            state: TransferTaskState::Running,
            progress: TransferProgress {
                loaded_size: 21,
                total_size: 42,
                update_time: 4,
                message: "running".to_string(),
            },
            now_ms: 4,
            stale_deadline_at: 90,
        })
        .unwrap());
    assert_eq!(
        store
            .get_transfer(&job.job_id)
            .unwrap()
            .unwrap()
            .summary
            .loaded_size,
        0,
        "progress reports must not update job.summary until flush"
    );
    store
        .flush_job_progress_summary(&job.job_id, job.run_id)
        .unwrap();
    assert_eq!(
        store
            .get_transfer(&job.job_id)
            .unwrap()
            .unwrap()
            .summary
            .loaded_size,
        21
    );
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 7,
            worker_session_id: "session-1".to_string(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 42,
                total_size: 42,
                update_time: 5,
                message: "completed".to_string(),
            },
            now_ms: 5,
            stale_deadline_at: 100,
        })
        .unwrap());

    drop(store);
    let reopened = PostgresTransferStore::open(&url).unwrap();
    let stored_job = reopened.get_transfer(&job.job_id).unwrap().unwrap();
    let stored_task = reopened
        .list_transfer_tasks(&job.job_id, job.run_id)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(stored_task.state, TransferTaskState::Completed);
    assert_eq!(stored_task.progress.loaded_size, 42);
    assert_eq!(
        stored_job.summary.loaded_size, 21,
        "terminal reports must not update job.summary until flush"
    );
    reopened
        .flush_job_progress_summary(&job.job_id, job.run_id)
        .unwrap();
    assert_eq!(
        reopened
            .get_transfer(&job.job_id)
            .unwrap()
            .unwrap()
            .summary
            .loaded_size,
        42
    );

    let concurrent_target = format!("/postgres-store-concurrent/{suffix}");
    let concurrent_store = Arc::new(PostgresTransferStore::open(&url).unwrap());
    let handles = (0..4)
        .map(|index| {
            let store = concurrent_store.clone();
            let job = transfer_job(&format!("{suffix}-concurrent-{index}"), &concurrent_target);
            std::thread::spawn(move || store.create_or_get_by_request_id_checked(job).is_ok())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap() as usize)
            .sum::<usize>(),
        4
    );
}

#[test]
fn postgres_store_marks_only_probed_missing_attempts() {
    let Some((url, _schema)) = test_store_url() else {
        eprintln!("CURVINE_TRANSFER_POSTGRES_URL is not set; skipping PostgreSQL store test");
        return;
    };
    let store = PostgresTransferStore::open(&url).unwrap();
    let job = transfer_job("stale-attempt", "/postgres-stale-attempt");
    store
        .create_or_get_by_request_id_checked(job.clone())
        .unwrap();
    let lease = store
        .acquire_runnable_transfer("owner", 60_000, 1, 1, false)
        .unwrap()
        .unwrap();
    let task_1 = transfer_task(&job);
    let mut task_2 = task_1.clone();
    task_2.task_id = "task-2".to_string();
    task_2.target_path.push_str("-2");
    assert!(store
        .persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: vec![task_1, task_2],
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
            })
            .unwrap());
    }
    let tasks = store.list_transfer_tasks(&job.job_id, job.run_id).unwrap();
    let mut task_1 = tasks
        .iter()
        .find(|task| task.task_id == "task-1")
        .unwrap()
        .clone();
    task_1.progress.message = "assigned Worker does not have this task attempt".to_string();
    let task_2 = tasks
        .iter()
        .find(|task| task.task_id == "task-2")
        .unwrap()
        .clone();
    assert_eq!(
        store
            .mark_stale_attempts(
                &job.job_id,
                job.run_id,
                &lease.owner,
                lease.lease_epoch,
                1_001,
                vec![task_1],
            )
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_transfer_tasks(&job.job_id, job.run_id)
            .unwrap()
            .into_iter()
            .find(|task| task.task_id == "task-1")
            .unwrap()
            .progress
            .message,
        "assigned Worker does not have this task attempt"
    );
    assert_eq!(
        store
            .list_transfer_tasks(&job.job_id, job.run_id)
            .unwrap()
            .into_iter()
            .find(|task| task.task_id == "task-2")
            .unwrap()
            .state,
        TransferTaskState::Running
    );
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id: task_2.task_id.clone(),
            attempt_id: task_2.attempt_id,
            worker_id: task_2.worker_id,
            worker_session_id: task_2.worker_session_id.clone(),
            state: TransferTaskState::Running,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 100,
                update_time: 1_001,
                message: "worker still owns task".to_string(),
            },
            now_ms: 1_001,
            stale_deadline_at: 2_000,
        })
        .unwrap());
    assert!(store
        .mark_stale_attempts(
            &job.job_id,
            job.run_id,
            &lease.owner,
            lease.lease_epoch,
            1_001,
            vec![task_2],
        )
        .unwrap()
        .is_empty());
    for (task_id, from_state, now_ms, message) in [
        ("task-1", TransferTaskState::Stale, 1_002, "first failure"),
        ("task-2", TransferTaskState::Running, 1_003, "later failure"),
    ] {
        assert!(store
            .update_task_state(TransferTaskStateUpdate {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                task_id: task_id.to_string(),
                from_states: vec![from_state],
                state: TransferTaskState::Failed,
                message: message.to_string(),
                now_ms,
                bump_retry: false,
            })
            .unwrap());
    }
    assert_eq!(
        store
            .get_transfer_task_summary(&job.job_id, job.run_id, 1_004)
            .unwrap()
            .progress
            .message,
        "2 transfer task(s) failed: first failure"
    );
}

#[test]
fn postgres_store_keeps_conflicting_requests_pending_and_persists_snapshot_retries() {
    let Some((url, _schema)) = test_store_url() else {
        eprintln!("CURVINE_TRANSFER_POSTGRES_URL is not set; skipping PostgreSQL store test");
        return;
    };
    let store = PostgresTransferStore::open(&url).unwrap();

    let mut parent = transfer_job("overlap-parent", "/parent");
    parent.updated_at = 1;
    let mut child = transfer_job("overlap-child", "/parent/child");
    child.updated_at = 2;
    let mut adjacent = transfer_job("overlap-adjacent", "/parent-other");
    adjacent.updated_at = 3;
    for job in [&parent, &child, &adjacent] {
        assert_eq!(
            store
                .create_or_get_by_request_id_checked(job.clone())
                .unwrap()
                .job_id,
            job.job_id
        );
    }

    let parent_lease = store
        .acquire_runnable_transfer("owner-parent", 60_000, 10, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(parent_lease.job_id, parent.job_id);
    let adjacent_lease = store
        .acquire_runnable_transfer("owner-adjacent", 60_000, 11, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(adjacent_lease.job_id, adjacent.job_id);
    assert_eq!(
        store.get_transfer(&child.job_id).unwrap().unwrap().state,
        TransferState::Pending
    );

    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: parent.job_id.clone(),
            run_id: parent.run_id,
            owner: parent_lease.owner.clone(),
            lease_epoch: parent_lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Completed,
            message: "parent complete".to_string(),
            now_ms: 12,
        })
        .unwrap());
    let child_lease = store
        .acquire_runnable_transfer("owner-child", 60_000, 13, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(child_lease.job_id, child.job_id);

    for (job, lease, now_ms) in [(&adjacent, &adjacent_lease, 14), (&child, &child_lease, 15)] {
        assert!(store
            .update_transfer_state(TransferStateUpdate {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                from_states: vec![TransferState::Planning],
                to_state: TransferState::Completed,
                message: "complete".to_string(),
                now_ms,
            })
            .unwrap());
    }

    let mut root = transfer_job("overlap-root", "/");
    root.updated_at = 16;
    let mut root_child = transfer_job("overlap-root-child", "/root-child");
    root_child.updated_at = 17;
    for job in [&root, &root_child] {
        store
            .create_or_get_by_request_id_checked(job.clone())
            .unwrap();
    }
    let root_lease = store
        .acquire_runnable_transfer("owner-root", 60_000, 18, 10, false)
        .unwrap()
        .unwrap();
    assert_eq!(root_lease.job_id, root.job_id);
    assert!(store
        .acquire_runnable_transfer("owner-root-child", 60_000, 19, 10, false)
        .unwrap()
        .is_none());
    assert!(store
        .update_transfer_state(TransferStateUpdate {
            job_id: root.job_id.clone(),
            run_id: root.run_id,
            owner: root_lease.owner.clone(),
            lease_epoch: root_lease.lease_epoch,
            from_states: vec![TransferState::Planning],
            to_state: TransferState::Completed,
            message: "root complete".to_string(),
            now_ms: 20,
        })
        .unwrap());
    assert_eq!(
        store
            .acquire_runnable_transfer("owner-root-child", 60_000, 21, 10, false)
            .unwrap()
            .unwrap()
            .job_id,
        root_child.job_id
    );

    let mut snapshot = transfer_job("snapshot", "/snapshot");
    snapshot.updated_at = 22;
    store
        .create_or_get_by_request_id_checked(snapshot.clone())
        .unwrap();
    let snapshot_lease = store
        .acquire_runnable_transfer("owner-snapshot", 60_000, 23, 10, false)
        .unwrap()
        .unwrap();
    assert!(store
        .set_transfer_mount_snapshot(TransferMountSnapshotUpdate {
            job_id: snapshot.job_id.clone(),
            run_id: snapshot.run_id,
            owner: snapshot_lease.owner.clone(),
            lease_epoch: snapshot_lease.lease_epoch,
            mount_snapshot_json: "{\"mount\":\"snapshot\"}".to_string(),
            cluster_snapshot_version: 2,
            now_ms: 24,
        })
        .unwrap());
    assert!(store
        .requeue_transfer(TransferRequeueUpdate {
            job_id: snapshot.job_id.clone(),
            run_id: snapshot.run_id,
            owner: snapshot_lease.owner,
            lease_epoch: snapshot_lease.lease_epoch,
            message: "mount snapshot refresh delayed".to_string(),
            next_attempt_at_ms: 30,
            mount_snapshot_attempts: Some(1),
            now_ms: 25,
        })
        .unwrap());
    let persisted = store.get_transfer(&snapshot.job_id).unwrap().unwrap();
    assert_eq!(persisted.state, TransferState::Pending);
    assert_eq!(persisted.mount_snapshot_attempts, 1);
    assert_eq!(persisted.mount_snapshot_json, "{\"mount\":\"snapshot\"}");
    assert_eq!(
        store
            .create_or_get_by_request_id_checked(snapshot.clone())
            .unwrap()
            .job_id,
        snapshot.job_id
    );
}

#[test]
fn postgres_store_does_not_backfill_historical_task_projections() {
    let Some((url, _schema)) = test_store_url() else {
        eprintln!("CURVINE_TRANSFER_POSTGRES_URL is not set; skipping PostgreSQL store test");
        return;
    };
    let initial = PostgresTransferStore::open(&url).unwrap();
    drop(initial);

    let mut client = admin_client(&url).unwrap();
    client
        .batch_execute(
            "drop trigger if exists transfer_tasks_record_json_sync on transfer_tasks;
             drop trigger if exists transfer_tasks_record_json_insert_sync on transfer_tasks;
             alter table transfer_tasks
                 drop column source_path,
                 drop column target_path,
                 drop column report_target_json,
                 drop column retry_count,
                 drop column attempt_started_at,
                 drop column last_report_at,
                 drop column progress_loaded_size,
                 drop column progress_total_size,
                 drop column progress_updated_at,
                 drop column progress_message;",
        )
        .unwrap();

    let job = transfer_job("task-json-compat", "/task-json-compat");
    let mut task = transfer_task(&job);
    task.attempt_id = 1;
    task.worker_id = 7;
    task.worker_session_id = "legacy-session".to_string();
    task.report_target_json = "{\"target\":\"legacy\"}".to_string();
    task.state = TransferTaskState::Running;
    task.progress = TransferProgress {
        loaded_size: 11,
        total_size: 42,
        update_time: 10,
        message: "legacy insert".to_string(),
    };
    task.attempt_started_at = 9;
    task.last_report_at = 10;
    task.stale_deadline_at = 100;
    task.updated_at = 10;
    let legacy_json = serde_json::to_string(&task).unwrap();
    let run_id = i64::try_from(task.run_id).unwrap();
    client
        .execute(
            "insert into transfer_tasks (
                job_id, run_id, task_id, state, attempt_id, worker_id, worker_session_id,
                stale_deadline_at, record_json, updated_at
             ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            &[
                &task.job_id,
                &run_id,
                &task.task_id,
                &(task.state as i32),
                &i64::try_from(task.attempt_id).unwrap(),
                &i64::from(task.worker_id),
                &task.worker_session_id,
                &task.stale_deadline_at,
                &legacy_json,
                &task.updated_at,
            ],
        )
        .unwrap();

    let store = PostgresTransferStore::open(&url).unwrap();
    let row = client
        .query_one(
            "select source_path, target_path, report_target_json, progress_loaded_size,
                    progress_total_size, progress_updated_at, progress_message
             from transfer_tasks where job_id = $1 and run_id = $2 and task_id = $3",
            &[&task.job_id, &run_id, &task.task_id],
        )
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "");
    assert_eq!(row.get::<_, String>(1), "");
    assert_eq!(row.get::<_, String>(2), "");
    assert_eq!(row.get::<_, i64>(3), 0);
    assert_eq!(row.get::<_, i64>(4), 0);
    assert_eq!(row.get::<_, i64>(5), 0);
    assert_eq!(row.get::<_, String>(6), "");
    let actual = store
        .list_transfer_tasks(&task.job_id, task.run_id)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(actual.source_path, task.source_path);
    assert_eq!(actual.target_path, task.target_path);
    assert_eq!(actual.progress, task.progress);
    let summary = store
        .get_transfer_task_summary(&task.job_id, task.run_id, 10)
        .unwrap();
    assert_eq!(summary.counts.running, 1);
    assert_eq!(summary.progress, task.progress);
    let (page, has_next) = store
        .list_transfer_task_status_page(&task.job_id, task.run_id, 0, 1)
        .unwrap();
    assert!(!has_next);
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].source_path, task.source_path);
    assert_eq!(page[0].progress, task.progress);

    task.progress = TransferProgress {
        loaded_size: 21,
        total_size: 42,
        update_time: 20,
        message: "legacy update".to_string(),
    };
    task.last_report_at = 20;
    task.stale_deadline_at = 90;
    task.updated_at = 20;
    let legacy_json = serde_json::to_string(&task).unwrap();
    client
        .execute(
            "update transfer_tasks
             set state = $1, attempt_id = $2, worker_id = $3, worker_session_id = $4,
                 stale_deadline_at = $5, record_json = $6, updated_at = $7
             where job_id = $8 and run_id = $9 and task_id = $10",
            &[
                &(task.state as i32),
                &i64::try_from(task.attempt_id).unwrap(),
                &i64::from(task.worker_id),
                &task.worker_session_id,
                &task.stale_deadline_at,
                &legacy_json,
                &task.updated_at,
                &task.job_id,
                &run_id,
                &task.task_id,
            ],
        )
        .unwrap();

    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: task.job_id.clone(),
            run_id: task.run_id,
            task_id: task.task_id.clone(),
            attempt_id: task.attempt_id,
            worker_id: task.worker_id,
            worker_session_id: task.worker_session_id.clone(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 42,
                total_size: 42,
                update_time: 30,
                message: "completed".to_string(),
            },
            now_ms: 30,
            stale_deadline_at: 80,
        })
        .unwrap());
    assert!(!store
        .update_task_report(TransferTaskReport {
            job_id: task.job_id.clone(),
            run_id: task.run_id,
            task_id: task.task_id.clone(),
            attempt_id: task.attempt_id,
            worker_id: task.worker_id,
            worker_session_id: task.worker_session_id.clone(),
            state: TransferTaskState::Running,
            progress: TransferProgress::default(),
            now_ms: 31,
            stale_deadline_at: 70,
        })
        .unwrap());

    let row = client
        .query_one(
            "select state, progress_loaded_size, progress_total_size, progress_updated_at,
                    progress_message, record_json::jsonb ->> 'state',
                    record_json::jsonb #>> '{progress,loaded_size}'
             from transfer_tasks where job_id = $1 and run_id = $2 and task_id = $3",
            &[&task.job_id, &run_id, &task.task_id],
        )
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), TransferTaskState::Completed as i32);
    assert_eq!(row.get::<_, i64>(1), 42);
    assert_eq!(row.get::<_, i64>(2), 42);
    assert_eq!(row.get::<_, i64>(3), 30);
    assert_eq!(row.get::<_, String>(4), "completed");
    assert_eq!(row.get::<_, String>(5), "Completed");
    assert_eq!(row.get::<_, String>(6), "42");
}
