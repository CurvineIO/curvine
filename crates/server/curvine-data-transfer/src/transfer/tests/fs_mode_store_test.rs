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

//! fs_mode jobs in the stores: request-id dedup is kept, target overlap is not
//! enforced for or against them, and they still take execution slots.

use super::{MemoryTransferStore, TransferStore};
use curvine_model::{TransferJobRecord, TransferKind, TransferProgress, TransferState};

fn job(job_id: &str, submitter: &str, target: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{job_id}"),
        job_id: job_id.to_string(),
        run_id: 1,
        kind: TransferKind::Export,
        source_path: "/mnt".to_string(),
        target_path: target.to_string(),
        command_json: format!("{{\"job\":\"{job_id}\"}}"),
        mount_snapshot_json: "{}".to_string(),
        secret_ref_json: "{}".to_string(),
        cluster_snapshot_version: 1,
        mount_snapshot_attempts: 0,
        planning_attempts: 0,
        terminal_summary: None,
        terminal_counts: None,
        cv_metadata_epoch: None,
        state: TransferState::Pending,
        owner: String::new(),
        lease_epoch: 0,
        lease_expire_at: 0,
        cancel_requested: false,
        summary: TransferProgress::default(),
        client_request_id: format!("request-{job_id}"),
        submitter: submitter.to_string(),
        tenant: "t".to_string(),
        created_at: 1,
        updated_at: 1,
    }
}

fn check_request_dedup(store: &impl TransferStore) {
    let first = store
        .create_or_get_by_request_id_checked(job("a", "fs-mode:c1", "s3://bkt/mnt"))
        .unwrap();
    // Same request id and command: the original job comes back.
    let again = store
        .create_or_get_by_request_id_checked(TransferJobRecord {
            job_id: "a-retry".to_string(),
            ..job("a", "fs-mode:c1", "s3://bkt/mnt")
        })
        .unwrap();
    assert_eq!(again.job_id, first.job_id);
    assert!(store
        .get_transfer_by_request("fs-mode:c1", "request-a")
        .unwrap()
        .is_some());
}

fn check_overlap_exemption(store: &impl TransferStore) {
    // Executing user job on the mount root and an fs_mode job under it.
    store
        .create_or_get_by_request_id_checked(job("user", "cli", "s3://bkt/mnt"))
        .unwrap();
    let user = store
        .acquire_runnable_transfer("o", 60_000, 10, 8, true)
        .unwrap()
        .expect("user job acquired");
    assert_eq!(user.job_id, "user");

    store
        .create_or_get_by_request_id_checked(job("fs", "fs-mode:c1", "s3://bkt/mnt/a"))
        .unwrap();
    let fs = store
        .acquire_runnable_transfer("o", 60_000, 11, 8, true)
        .unwrap()
        .expect("an fs_mode job is not blocked by an overlapping user job");
    assert_eq!(fs.job_id, "fs");

    // Neither side reports the other as a conflict.
    assert!(store
        .find_conflicting_active_transfer("s3://bkt/mnt/a", "fs-mode:c1", "request-fs")
        .unwrap()
        .is_none());
    let conflict = store
        .find_conflicting_active_transfer("s3://bkt/mnt", "cli", "request-other")
        .unwrap()
        .expect("the user job still conflicts with other user jobs");
    assert_eq!(conflict.job_id, "user");
}

fn check_slot_accounting(store: &impl TransferStore) {
    store
        .create_or_get_by_request_id_checked(job("fs1", "fs-mode:c1", "s3://bkt/x"))
        .unwrap();
    store
        .acquire_runnable_transfer("o", 60_000, 10, 1, true)
        .unwrap()
        .expect("first job acquired");
    store
        .create_or_get_by_request_id_checked(job("user2", "cli", "s3://bkt/y"))
        .unwrap();
    // The executing fs_mode job takes the only slot.
    assert!(store
        .acquire_runnable_transfer("o", 60_000, 11, 1, true)
        .unwrap()
        .is_none());
}

#[test]
fn memory_store_keeps_dedup_and_exempts_overlap() {
    check_request_dedup(&MemoryTransferStore::new());
    check_overlap_exemption(&MemoryTransferStore::new());
    check_slot_accounting(&MemoryTransferStore::new());
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_store_keeps_dedup_and_exempts_overlap() {
    use super::SqliteTransferStore;
    let open = |name: &str| {
        let path = std::env::temp_dir().join(format!(
            "curvine-fs-mode-store-{}-{}-{}.db",
            name,
            std::process::id(),
            curvine_runtime::common::LocalTime::mills()
        ));
        SqliteTransferStore::open(path).unwrap()
    };
    check_request_dedup(&open("dedup"));
    check_overlap_exemption(&open("overlap"));
    check_slot_accounting(&open("slots"));
}
