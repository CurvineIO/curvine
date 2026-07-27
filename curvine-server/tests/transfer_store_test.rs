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
use curvine_common::state::{TransferJobRecord, TransferKind, TransferProgress, TransferState};
use curvine_server::transfer::{MemoryTransferStore, SqliteTransferStore, TransferStore};

fn job(id: &str, request_id: &str, target: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("Load:s3://bucket/{id}:{target}"),
        job_id: id.to_string(),
        run_id: 1,
        kind: TransferKind::Load,
        source_path: format!("s3://bucket/{id}"),
        target_path: target.to_string(),
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
        client_request_id: request_id.to_string(),
        submitter: "test".to_string(),
        tenant: "default".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn assert_store_contract<S: TransferStore>(store: &S) {
    let first = job("job-1", "request-1", "/target");
    let created = store.create_or_get_by_request_id(first.clone()).unwrap();
    assert_eq!(created.job_id, first.job_id);

    let replay = store.create_or_get_by_request_id(first).unwrap();
    assert_eq!(replay.job_id, "job-1");

    let err = store
        .create_or_get_by_request_id(job("job-2", "request-2", "/target/child"))
        .unwrap_err();
    assert!(
        matches!(err.kind(), ErrorKind::TransferTargetConflict),
        "unexpected error: {err}"
    );
}

#[test]
fn memory_store_enforces_request_and_target_contracts() {
    assert_store_contract(&MemoryTransferStore::new());
}

#[test]
fn sqlite_store_persists_submitted_transfer() {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-{}-{}.db",
        std::process::id(),
        orpc::common::LocalTime::mills()
    ));

    {
        let store = SqliteTransferStore::open(&path).unwrap();
        assert_store_contract(&store);
    }

    let reopened = SqliteTransferStore::open(&path).unwrap();
    assert_eq!(
        reopened.get_transfer("job-1").unwrap().unwrap().target_path,
        "/target"
    );
    let _ = std::fs::remove_file(path);
}
