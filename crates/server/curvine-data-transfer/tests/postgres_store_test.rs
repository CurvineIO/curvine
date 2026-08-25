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

use curvine_data_transfer::transfer::{PostgresTransferStore, TransferPlannedTasks, TransferStore};
use curvine_model::{
    TaskAttemptStart, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferTaskRecord, TransferTaskReport, TransferTaskState,
};
use native_tls::TlsConnector;
use postgres::{Client, Config};
use postgres_native_tls::MakeTlsConnector;
use std::str::FromStr;
use std::sync::Arc;

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
        "curvine_transfer_test_{}_{}",
        std::process::id(),
        curvine_runtime::common::LocalTime::mills()
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
    assert!(store.create_or_get_by_request_id_checked(child).is_err());
    let wildcard = transfer_job(&format!("{suffix}-wildcard"), "/cache/a_b%");
    let unrelated = transfer_job(&format!("{suffix}-unrelated"), "/cache/aXbZ/child");
    let wildcard_child = transfer_job(&format!("{suffix}-wildcard-child"), "/cache/a_b%/child");
    store.create_or_get_by_request_id_checked(wildcard).unwrap();
    store
        .create_or_get_by_request_id_checked(unrelated)
        .unwrap();
    assert!(store
        .create_or_get_by_request_id_checked(wildcard_child)
        .is_err());

    let lease = store
        .acquire_runnable_transfer("postgres-store-test", 60_000, 2, 1)
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
    assert_eq!(stored_job.summary.loaded_size, 42);
    assert_eq!(stored_task.state, TransferTaskState::Completed);
    assert_eq!(stored_task.progress.loaded_size, 42);

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
        1
    );
}
