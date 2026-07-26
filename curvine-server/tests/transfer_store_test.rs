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

use curvine_model::{TransferJobRecord, TransferKind, TransferProgress, TransferState};
use curvine_server::transfer::{SqliteTransferStore, TransferStore};

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
        submitter: "test".to_string(),
        tenant: String::new(),
        created_at: 1,
        updated_at: 1,
    }
}

#[test]
fn sqlite_target_conflict_treats_wildcards_as_path_characters() {
    let db_path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-{}-{}.db",
        std::process::id(),
        orpc_rpc::common::LocalTime::mills()
    ));
    let store = SqliteTransferStore::open(&db_path).unwrap();

    store
        .create_or_get_by_request_id(transfer_job("wildcard", "/cache/a_b%"))
        .unwrap();
    store
        .create_or_get_by_request_id(transfer_job("unrelated", "/cache/aXbZ/child"))
        .unwrap();
    assert!(store
        .create_or_get_by_request_id(transfer_job("child", "/cache/a_b%/child"))
        .is_err());

    drop(store);
    let _ = std::fs::remove_file(db_path);
}
