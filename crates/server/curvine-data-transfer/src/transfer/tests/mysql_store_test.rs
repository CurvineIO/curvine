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

use super::MysqlTransferStore;
use crate::transfer::store_test::{assert_records, job, records_with_spec, task};
use crate::transfer::task_result_test::verify_result_lifecycle;
use crate::transfer::TransferStore;
use curvine_model::{TransferKind, TransferListFilter, TransferTaskState};
use mysql::{prelude::Queryable, Opts, OptsBuilder, Pool};

struct TestDb {
    admin: Pool,
    opts: Opts,
    name: String,
}

impl TestDb {
    fn new() -> Option<Self> {
        let Ok(url) = std::env::var("CURVINE_TRANSFER_MYSQL_URL") else {
            eprintln!("CURVINE_TRANSFER_MYSQL_URL not set; skipping MySQL store test");
            return None;
        };
        let opts = Opts::from_url(&url).unwrap();
        let admin = Pool::new(opts.clone()).unwrap();
        let name = format!("curvine_test_{}", uuid::Uuid::new_v4().simple());
        admin
            .get_conn()
            .unwrap()
            .query_drop(format!("create database `{name}`"))
            .unwrap();
        let opts = OptsBuilder::from_opts(opts).db_name(Some(&name)).into();
        Some(Self { admin, opts, name })
    }

    fn open(&self) -> MysqlTransferStore {
        MysqlTransferStore::from_pool(Pool::new(self.opts.clone()).unwrap()).unwrap()
    }

    fn conn(&self) -> mysql::PooledConn {
        Pool::new(self.opts.clone()).unwrap().get_conn().unwrap()
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if let Ok(mut conn) = self.admin.get_conn() {
            let _ = conn.query_drop(format!("drop database `{}`", self.name));
        }
    }
}

#[test]
fn mysql_migrates_v4_and_resumes_partial_migration() {
    for partial in [false, true] {
        let Some(db) = TestDb::new() else { return };
        let store = db.open();
        let job = job(TransferKind::Load);
        let task = task(&job);
        store.create_or_get_by_request_id(job.clone()).unwrap();
        store.insert_tasks(vec![task.clone()]).unwrap();
        drop(store);
        let mut conn = db.conn();
        if !partial {
            conn.query_drop("alter table transfer_jobs drop column spec_version")
                .unwrap();
        }
        for sql in [
            "alter table transfer_jobs drop column spec_blob",
            "alter table transfer_tasks drop column spec_blob, drop column result_json",
            "update transfer_jobs set record_json = json_remove(record_json, '$.spec_version', '$.spec_blob')",
            "update transfer_tasks set record_json = json_remove(record_json, '$.spec_blob', '$.result_json')",
            "update transfer_schema_version set version = 4",
        ] {
            conn.query_drop(sql).unwrap();
        }
        // Both initializers see the same database; neither may fail on duplicate DDL.
        std::thread::scope(|scope| {
            let other = scope.spawn(|| db.open());
            assert_records(&db.open(), &job, &task);
            assert_records(&other.join().unwrap(), &job, &task);
        });
        assert_eq!(
            conn.query_first::<u64, _>("select version from transfer_schema_version")
                .unwrap(),
            Some(5)
        );
        assert_records(&db.open(), &job, &task);
    }
}

#[test]
fn mysql_report_result_lifecycle() {
    let Some(db) = TestDb::new() else { return };
    verify_result_lifecycle(std::sync::Arc::new(db.open()));
}

#[test]
fn mysql_spec_and_result_roundtrip() {
    let Some(db) = TestDb::new() else { return };
    let (job, task) = records_with_spec();
    let store = db.open();
    store.create_or_get_by_request_id(job.clone()).unwrap();
    store.insert_tasks(vec![task.clone()]).unwrap();
    drop(store);
    let mut conn = db.conn();
    conn.query_drop("update transfer_jobs set record_json = json_remove(record_json, '$.spec_version', '$.spec_blob')").unwrap();
    conn.query_drop("update transfer_tasks set record_json = json_remove(record_json, '$.spec_blob', '$.result_json')").unwrap();
    let store = db.open();
    assert_records(&store, &job, &task);
    assert_eq!(
        store
            .list_transfers(TransferListFilter {
                limit: 1,
                ..Default::default()
            })
            .unwrap(),
        vec![job.clone()]
    );
    assert_eq!(
        store
            .list_tasks_by_state(&job.job_id, job.run_id, TransferTaskState::Pending, 1)
            .unwrap(),
        vec![task]
    );
    conn.query_drop(
        "update transfer_jobs set record_json = json_set(record_json, '$.kind', 'Unknown')",
    )
    .unwrap();
    assert!(store.get_transfer(&job.job_id).is_err());
}
