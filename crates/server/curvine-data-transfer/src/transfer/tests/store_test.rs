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

use super::{MemoryTransferStore, TransferStore};
use curvine_model::{
    TransferCommand, TransferJobRecord, TransferKind, TransferProgress, TransferState,
    TransferTaskRecord, TransferTaskState,
};

fn job(kind: TransferKind) -> TransferJobRecord {
    let command = TransferCommand {
        kind,
        source_path: "file:///source".into(),
        target_path: "/target".into(),
        ..Default::default()
    };
    TransferJobRecord {
        job_key: command.job_key(),
        job_id: "job-1".into(),
        run_id: 1,
        kind,
        source_path: command.source_path.clone(),
        target_path: command.target_path.clone(),
        command_json: serde_json::to_string(&command).unwrap(),
        mount_snapshot_json: "{}".into(),
        secret_ref_json: "{}".into(),
        cluster_snapshot_version: 1,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: "request-1".into(),
        submitter: "test".into(),
        tenant: "test".into(),
        created_at: 10,
        updated_at: 20,
        spec_version: 0,
        spec_blob: Vec::new(),
    }
}

fn task(job: &TransferJobRecord) -> TransferTaskRecord {
    TransferTaskRecord {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: "task-1".into(),
        attempt_id: 0,
        source_path: job.source_path.clone(),
        target_path: job.target_path.clone(),
        worker_id: 0,
        worker_session_id: String::new(),
        source_read_plan_json: "{}".into(),
        report_target_json: String::new(),
        state: TransferTaskState::Pending,
        progress: TransferProgress::default(),
        retry_count: 0,
        attempt_started_at: 0,
        last_report_at: 0,
        stale_deadline_at: 0,
        updated_at: 20,
        spec_blob: Vec::new(),
        result_json: String::new(),
    }
}

fn records_with_spec() -> (TransferJobRecord, TransferTaskRecord) {
    let mut job = job(TransferKind::Export);
    job.spec_version = 7;
    job.spec_blob = vec![0, 255, 3];
    let mut task = task(&job);
    task.spec_blob = vec![255, 0, 2];
    task.result_json = r#"{"count":3}"#.into();
    (job, task)
}

fn assert_records(store: &dyn TransferStore, job: &TransferJobRecord, task: &TransferTaskRecord) {
    assert_eq!(store.get_transfer(&job.job_id).unwrap().as_ref(), Some(job));
    let tasks = store.list_transfer_tasks(&job.job_id, job.run_id).unwrap();
    assert_eq!(tasks, std::slice::from_ref(task));
    let pending = store
        .claim_pending_tasks(&job.job_id, job.run_id, 1)
        .unwrap();
    assert_eq!(pending, std::slice::from_ref(task));
}

#[test]
fn memory_spec_and_result_roundtrip() {
    let store = MemoryTransferStore::new();
    let (job, task) = records_with_spec();
    store.create_or_get_by_request_id(job.clone()).unwrap();
    store.insert_tasks(vec![task.clone()]).unwrap();
    assert_records(&store, &job, &task);
}

#[test]
fn legacy_record_json_defaults_new_fields_and_rejects_unknown_kinds() {
    for kind in [TransferKind::Load, TransferKind::Export] {
        let job = job(kind);
        let task = task(&job);
        let mut job_json = serde_json::to_value(&job).unwrap();
        let mut task_json = serde_json::to_value(&task).unwrap();
        for field in ["spec_version", "spec_blob"] {
            job_json.as_object_mut().unwrap().remove(field).unwrap();
        }
        for field in ["spec_blob", "result_json"] {
            task_json.as_object_mut().unwrap().remove(field).unwrap();
        }
        assert_eq!(
            serde_json::from_value::<TransferJobRecord>(job_json.clone()).unwrap(),
            job
        );
        assert_eq!(
            serde_json::from_value::<TransferTaskRecord>(task_json).unwrap(),
            task
        );
        for unknown in [serde_json::json!(99), serde_json::json!("Unknown")] {
            job_json["kind"] = unknown;
            assert!(serde_json::from_value::<TransferJobRecord>(job_json.clone()).is_err());
        }
    }
}

#[cfg(any(feature = "transfer-store-mysql", feature = "transfer-store-postgres"))]
#[test]
fn json_store_guards_reject_nonlegacy_records() {
    use super::store::{require_legacy_job, require_legacy_task};

    let (mut job, mut task) = records_with_spec();
    assert!(require_legacy_job(&job).is_err());
    job.spec_blob.clear();
    assert!(require_legacy_job(&job).is_err());
    job.spec_version = 0;
    require_legacy_job(&job).unwrap();
    assert!(require_legacy_task(&task).is_err());
    task.spec_blob.clear();
    assert!(require_legacy_task(&task).is_err());
    task.result_json.clear();
    require_legacy_task(&task).unwrap();
}

#[cfg(feature = "transfer-store-sqlite")]
mod sqlite {
    use super::*;
    use crate::transfer::SqliteTransferStore;
    use rusqlite::Connection;
    use std::path::PathBuf;

    struct TestDb(PathBuf);

    impl TestDb {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("curvine-spec-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn open(&self) -> SqliteTransferStore {
            SqliteTransferStore::open(self.path()).unwrap()
        }

        fn path(&self) -> PathBuf {
            self.0.join("transfer.db")
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn sqlite_spec_and_result_roundtrip_after_reopen() {
        let db = TestDb::new();
        let (job, task) = records_with_spec();
        let store = db.open();
        store.create_or_get_by_request_id(job.clone()).unwrap();
        store.insert_tasks(vec![task.clone()]).unwrap();
        drop(store);
        assert_records(&db.open(), &job, &task);
    }

    #[test]
    fn sqlite_migrates_legacy_rows_and_is_idempotent() {
        for version in [1, 2] {
            let db = TestDb::new();
            let job = job(TransferKind::Load);
            let task = task(&job);
            let store = db.open();
            store.create_or_get_by_request_id(job.clone()).unwrap();
            store.insert_tasks(vec![task.clone()]).unwrap();
            drop(store);
            let conn = Connection::open(db.path()).unwrap();
            conn.execute_batch(
                "alter table transfer_jobs drop column spec_version;
                 alter table transfer_jobs drop column spec_blob;
                 alter table transfer_tasks drop column spec_blob;
                 alter table transfer_tasks drop column result_json;",
            )
            .unwrap();
            if version == 1 {
                conn.execute_batch(
                    "drop index transfer_jobs_target_state_idx;
                     alter table transfer_jobs drop column target_path;",
                )
                .unwrap();
            }
            conn.execute("update transfer_schema_version set version = ?1", [version])
                .unwrap();
            drop(conn);
            assert_records(&db.open(), &job, &task);
            assert_records(&db.open(), &job, &task);
            let conn = Connection::open(db.path()).unwrap();
            let upgraded: i64 = conn
                .query_row("select version from transfer_schema_version", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(upgraded, 3);
        }
    }

    #[test]
    fn sqlite_unknown_kind_is_error_not_load() {
        let db = TestDb::new();
        let job = job(TransferKind::Load);
        let store = db.open();
        store.create_or_get_by_request_id(job.clone()).unwrap();
        Connection::open(db.path())
            .unwrap()
            .execute(
                "update transfer_jobs set kind = 99 where job_id = ?1",
                [&job.job_id],
            )
            .unwrap();
        assert!(store.get_transfer(&job.job_id).is_err());
        assert!(store.list_active_transfers().is_err());
    }
}
