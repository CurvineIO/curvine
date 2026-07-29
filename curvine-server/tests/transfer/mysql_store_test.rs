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
    TaskAttemptStart, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferStateUpdate, TransferTaskRecord, TransferTaskReport, TransferTaskState,
};
use curvine_server::transfer::{MysqlTransferStore, TransferStore};
use mysql::params;
use mysql::prelude::*;
use std::sync::{Arc, Barrier};
use std::thread;
use uuid::Uuid;

fn mysql_store(name: &str) -> Option<(MysqlTransferStore, String, String, String)> {
    let base_url = std::env::var("CURVINE_TRANSFER_MYSQL_URL").ok()?;
    let safe_name = name.replace('-', "_");
    let safe_name = &safe_name[..safe_name.len().min(20)];
    let suffix = Uuid::new_v4().simple().to_string();
    let db_name = format!(
        "cv_transfer_{}_{}_{}",
        safe_name,
        std::process::id(),
        &suffix[..8]
    );
    let pool = mysql::Pool::new(limited_mysql_pool_url(&base_url).as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("create database `{}`", db_name))
        .unwrap();
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    Some((
        MysqlTransferStore::open(&store_url).unwrap(),
        store_url,
        base_url,
        db_name,
    ))
}

fn drop_mysql_database(base_url: &str, db_name: &str) {
    let pool = mysql::Pool::new(limited_mysql_pool_url(base_url).as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("drop database if exists `{}`", db_name))
        .unwrap();
}

fn create_mysql_database(base_url: &str, db_name: &str) {
    let pool = mysql::Pool::new(limited_mysql_pool_url(base_url).as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(format!("create database `{}`", db_name))
        .unwrap();
}

fn limited_mysql_pool_url(url: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}pool_min=0&pool_max=1")
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
        submitter: "mysql-test".to_string(),
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
fn mysql_rejects_conflicting_active_target() {
    let Some((store, _store_url, base_url, db_name)) = mysql_store("target-conflict") else {
        return;
    };
    let mut parent = job("parent");
    parent.target_path = "/a".to_string();
    store.create_or_get_by_request_id(parent).unwrap();

    let conflict = store
        .find_conflicting_active_transfer("/a/child", "mysql-test", "req-other")
        .unwrap()
        .expect("child target should conflict with active parent target");
    assert_eq!(conflict.job_id, "parent");

    let mut child = job("child");
    child.target_path = "/a/child".to_string();
    let err = store.create_or_get_by_request_id(child).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::TransferTargetConflict));

    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_store_supports_lease_report_and_cleanup() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("store-semantics") else {
        return;
    };
    let second_store = MysqlTransferStore::open(&store_url).unwrap();

    store.create_or_get_by_request_id(job("pending")).unwrap();
    let mut running = job("running");
    running.state = TransferState::Running;
    store.create_or_get_by_request_id(running).unwrap();
    assert_eq!(store.count_active_transfers().unwrap(), 2);
    assert_eq!(store.count_executing_transfers().unwrap(), 1);

    let lease = store
        .acquire_runnable_transfer("owner-a", 100, 10, 1)
        .unwrap()
        .unwrap();
    assert_eq!(lease.job_id, "running");
    assert!(store
        .acquire_runnable_transfer("owner-a", 100, 10, 1)
        .unwrap()
        .is_none());

    let lease_a = store
        .acquire_runnable_transfer("owner-a", 100, 10, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease_a.job_id, "pending");
    let lease_b = second_store
        .acquire_runnable_transfer("owner-b", 100, 111, 100)
        .unwrap()
        .unwrap();
    assert_eq!(lease_b.lease_epoch, lease_a.lease_epoch + 1);

    assert!(!store
        .update_transfer_state(TransferStateUpdate {
            job_id: lease_a.job_id.clone(),
            run_id: lease_a.run_id,
            owner: lease_a.owner,
            lease_epoch: lease_a.lease_epoch,
            from_states: vec![TransferState::Pending],
            to_state: TransferState::Planning,
            message: "stale owner should not update".to_string(),
            now_ms: 120,
        })
        .unwrap());

    store.insert_tasks(vec![task("pending")]).unwrap();
    assert!(store
        .start_task_attempt(TaskAttemptStart {
            job_id: "pending".to_string(),
            run_id: 1,
            owner: lease_b.owner.clone(),
            lease_epoch: lease_b.lease_epoch,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            report_target_json: "{}".to_string(),
            now_ms: 130,
            stale_deadline_at: 190,
        })
        .unwrap());
    assert!(store
        .update_task_report(TransferTaskReport {
            job_id: "pending".to_string(),
            run_id: 1,
            task_id: "task-1".to_string(),
            attempt_id: 1,
            worker_id: 10,
            worker_session_id: "session-a".to_string(),
            state: TransferTaskState::Completed,
            progress: TransferProgress {
                loaded_size: 10,
                total_size: 10,
                update_time: 150,
                message: String::new(),
            },
            now_ms: 150,
            stale_deadline_at: 210,
        })
        .unwrap());
    assert_eq!(
        store.get_transfer("pending").unwrap().unwrap().state,
        TransferState::Completed
    );

    assert_eq!(store.purge_terminal_transfers(200, 100).unwrap(), 1);
    assert!(store.get_transfer("pending").unwrap().is_none());
    assert!(store.list_transfer_tasks("pending", 1).unwrap().is_empty());

    drop(second_store);
    drop(store);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_migrates_v2_schema_to_current_version() {
    let base_url = match std::env::var("CURVINE_TRANSFER_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => return,
    };
    let db_name = format!(
        "cv_transfer_migrate_v2_{}_{}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    );
    create_mysql_database(&base_url, &db_name);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    let pool = mysql::Pool::new(store_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(
        "create table transfer_schema_version (
            id tinyint unsigned primary key,
            version bigint unsigned not null,
            updated_at bigint not null
        )",
    )
    .unwrap();
    conn.query_drop(
        "insert into transfer_schema_version(id, version, updated_at) values (1, 2, 1)",
    )
    .unwrap();
    conn.query_drop(
        "create table transfer_jobs (
            job_id varchar(128) primary key,
            submitter varchar(255) not null,
            client_request_id varchar(255) not null,
            job_key varchar(1024) not null,
            run_id bigint unsigned not null,
            kind int not null,
            state int not null,
            owner varchar(255) not null,
            lease_epoch bigint unsigned not null,
            lease_expire_at bigint not null,
            cancel_requested tinyint not null,
            record_json longtext not null,
            created_at bigint not null,
            updated_at bigint not null,
            unique key transfer_jobs_request_idx(submitter, client_request_id)
        )",
    )
    .unwrap();
    let mut legacy = job("legacy-v2");
    legacy.submitter = "legacy-submitter".to_string();
    legacy.tenant = "legacy-tenant".to_string();
    legacy.target_path = "/legacy-target".to_string();
    legacy.job_key = format!("Load:{}:{}", legacy.source_path, legacy.target_path);
    let legacy_json = serde_json::to_string(&legacy).unwrap();
    conn.exec_drop(
        "insert into transfer_jobs (
            job_id, submitter, client_request_id, job_key, run_id, kind, state,
            owner, lease_epoch, lease_expire_at, cancel_requested, record_json, created_at, updated_at
        ) values (
            :job_id, :submitter, :client_request_id, :job_key, :run_id, :kind, :state,
            :owner, :lease_epoch, :lease_expire_at, :cancel_requested, :record_json, :created_at, :updated_at
        )",
        params! {
            "job_id" => &legacy.job_id,
            "submitter" => &legacy.submitter,
            "client_request_id" => &legacy.client_request_id,
            "job_key" => &legacy.job_key,
            "run_id" => legacy.run_id,
            "kind" => legacy.kind as i32,
            "state" => legacy.state as i32,
            "owner" => &legacy.owner,
            "lease_epoch" => legacy.lease_epoch,
            "lease_expire_at" => legacy.lease_expire_at,
            "cancel_requested" => legacy.cancel_requested,
            "record_json" => legacy_json,
            "created_at" => legacy.created_at,
            "updated_at" => legacy.updated_at,
        },
    )
    .unwrap();
    drop(conn);

    let store = MysqlTransferStore::open(&store_url).unwrap();
    let migrated = store.get_transfer("legacy-v2").unwrap().unwrap();
    assert_eq!(migrated.target_path, "/legacy-target");
    assert_eq!(
        store
            .list_transfers(curvine_common::state::TransferListFilter {
                tenant: Some("legacy-tenant".to_string()),
                limit: 10,
                ..Default::default()
            })
            .unwrap()
            .len(),
        1
    );

    let mut conn = pool.get_conn().unwrap();
    let version: u64 = conn
        .exec_first(
            "select version from transfer_schema_version where id = 1",
            mysql::Params::Empty,
        )
        .unwrap()
        .unwrap();
    assert_eq!(version, 4);
    assert!(mysql_column_exists(
        &mut conn,
        "transfer_jobs",
        "target_path"
    ));
    assert!(mysql_column_exists(&mut conn, "transfer_jobs", "tenant"));

    drop(store);
    drop(conn);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_rejects_future_schema_without_creating_tables() {
    let base_url = match std::env::var("CURVINE_TRANSFER_MYSQL_URL") {
        Ok(url) => url,
        Err(_) => return,
    };
    let db_name = format!(
        "cv_transfer_future_schema_{}_{}",
        std::process::id(),
        &Uuid::new_v4().simple().to_string()[..8]
    );
    create_mysql_database(&base_url, &db_name);
    let separator = if base_url.contains('?') { '&' } else { '?' };
    let store_url = format!(
        "{}/{}{}pool_min=0&pool_max=1",
        base_url.trim_end_matches('/'),
        db_name,
        separator
    );
    let pool = mysql::Pool::new(store_url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop(
        "create table transfer_schema_version (
            id tinyint unsigned primary key,
            version bigint unsigned not null,
            updated_at bigint not null
        )",
    )
    .unwrap();
    conn.query_drop(
        "insert into transfer_schema_version(id, version, updated_at) values (1, 999, 1)",
    )
    .unwrap();
    drop(conn);

    let err = match MysqlTransferStore::open(&store_url) {
        Ok(_) => panic!("future MySQL schema unexpectedly opened"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("Unsupported mysql transfer schema version 999"));

    let mut conn = pool.get_conn().unwrap();
    assert!(!mysql_table_exists(&mut conn, "transfer_jobs"));
    assert!(!mysql_table_exists(&mut conn, "transfer_tasks"));
    drop(conn);
    drop_mysql_database(&base_url, &db_name);
}

#[test]
fn mysql_enforces_execution_window_across_store_handles() {
    let Some((store, store_url, base_url, db_name)) = mysql_store("concurrent-acquire") else {
        return;
    };
    for index in 0..8 {
        store
            .create_or_get_by_request_id(job(&format!("pending-{index}")))
            .unwrap();
    }
    drop(store);

    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let handles = (0..workers)
        .map(|index| {
            let barrier = barrier.clone();
            let store_url = store_url.clone();
            thread::spawn(move || {
                let store = MysqlTransferStore::open(&store_url).unwrap();
                barrier.wait();
                store
                    .acquire_runnable_transfer(&format!("owner-{index}"), 1000, 10, 1)
                    .unwrap()
                    .map(|lease| lease.job_id)
            })
        })
        .collect::<Vec<_>>();
    let acquired = handles
        .into_iter()
        .filter_map(|handle| handle.join().unwrap())
        .count();
    assert_eq!(acquired, 1);

    let reopened = MysqlTransferStore::open(&store_url).unwrap();
    assert_eq!(reopened.count_active_transfers().unwrap(), 8);
    assert_eq!(reopened.count_executing_transfers().unwrap(), 1);
    drop(reopened);
    drop_mysql_database(&base_url, &db_name);
}

fn mysql_table_exists(conn: &mut mysql::PooledConn, table: &str) -> bool {
    conn.exec_first::<String, _, _>(
        "select table_name from information_schema.tables
         where table_schema = database() and table_name = :table",
        params! { "table" => table },
    )
    .unwrap()
    .is_some()
}

fn mysql_column_exists(conn: &mut mysql::PooledConn, table: &str, column: &str) -> bool {
    conn.exec_first::<String, _, _>(
        "select column_name from information_schema.columns
         where table_schema = database()
           and table_name = :table
           and column_name = :column",
        params! { "table" => table, "column" => column },
    )
    .unwrap()
    .is_some()
}
