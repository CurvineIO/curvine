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

use curvine_data_transfer::transfer::{MysqlTransferStore, TransferStore};
use curvine_model::{TransferProgress, TransferTaskRecord, TransferTaskState};
use mysql::prelude::Queryable;
use mysql::{params, Opts, OptsBuilder, Pool};
use std::time::Duration;

const MYSQL_TEST_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

fn test_url() -> Option<String> {
    std::env::var("CURVINE_TRANSFER_MYSQL_TEST_URL").ok()
}

fn reset_tables(conn: &mut impl Queryable) {
    for statement in [
        "drop table if exists transfer_tasks",
        "drop table if exists transfer_jobs",
        "drop table if exists transfer_schema_version",
    ] {
        conn.query_drop(statement).unwrap();
    }
}

fn legacy_task() -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: "mysql-json-authoritative".to_string(),
        run_id: 1,
        task_id: "task-1".to_string(),
        attempt_id: 1,
        source_path: "file:///legacy-source".to_string(),
        target_path: "/legacy-target".to_string(),
        worker_id: 7,
        worker_session_id: "legacy-session".to_string(),
        source_read_plan_json: "{}".to_string(),
        report_target_json: "{\"target\":\"legacy\"}".to_string(),
        state: TransferTaskState::Running,
        progress: TransferProgress {
            loaded_size: 11,
            total_size: 42,
            update_time: 10,
            message: "legacy progress".to_string(),
        },
        estimated_size: 0,
        retry_count: 0,
        attempt_started_at: 9,
        accepted_at_ms: 0,
        first_byte_at_ms: 0,
        last_report_at: 10,
        stale_deadline_at: 100,
        updated_at: 10,
    }
}

#[test]
fn mysql_store_does_not_backfill_historical_task_projections() {
    let Some(url) = test_url() else {
        eprintln!("CURVINE_TRANSFER_MYSQL_TEST_URL is not set; skipping MySQL store test");
        return;
    };
    let pool = Pool::new(
        OptsBuilder::from_opts(Opts::from_url(&url).unwrap())
            .tcp_connect_timeout(Some(MYSQL_TEST_COMMAND_TIMEOUT))
            .read_timeout(Some(MYSQL_TEST_COMMAND_TIMEOUT))
            .write_timeout(Some(MYSQL_TEST_COMMAND_TIMEOUT)),
    )
    .unwrap();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("set session lock_wait_timeout = 5")
        .unwrap();
    conn.query_drop("set session innodb_lock_wait_timeout = 5")
        .unwrap();
    reset_tables(&mut conn);

    let initial = MysqlTransferStore::open(&url).unwrap();
    drop(initial);
    conn.query_drop("drop trigger transfer_tasks_record_json_update_sync")
        .unwrap();
    conn.query_drop("drop trigger transfer_tasks_record_json_insert_sync")
        .unwrap();

    let task = legacy_task();
    conn.exec_drop(
        "insert into transfer_tasks (
            job_id, run_id, task_id, state, attempt_id, worker_id, worker_session_id,
            stale_deadline_at, record_json, updated_at
         ) values (
            :job_id, :run_id, :task_id, :state, :attempt_id, :worker_id, :worker_session_id,
            :stale_deadline_at, :record_json, :updated_at
         )",
        params! {
            "job_id" => &task.job_id,
            "run_id" => task.run_id,
            "task_id" => &task.task_id,
            "state" => task.state as i32,
            "attempt_id" => task.attempt_id,
            "worker_id" => task.worker_id,
            "worker_session_id" => &task.worker_session_id,
            "stale_deadline_at" => task.stale_deadline_at,
            "record_json" => serde_json::to_string(&task).unwrap(),
            "updated_at" => task.updated_at,
        },
    )
    .unwrap();

    let store = MysqlTransferStore::open(&url).unwrap();
    let projection: (String, String, i64, Option<String>) = conn
        .exec_first(
            "select source_path, target_path, progress_loaded_size, progress_message
             from transfer_tasks
             where job_id = :job_id and run_id = :run_id and task_id = :task_id",
            params! {
                "job_id" => &task.job_id,
                "run_id" => task.run_id,
                "task_id" => &task.task_id,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(projection, (String::new(), String::new(), 0, None));

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
}
