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

use super::{MemoryTransferStore, SchedulingPriority, TransferStore};
use curvine_model::{
    TransferJobRecord, TransferKind, TransferProgress, TransferState, TransferTaskRecord,
    TransferTaskState,
};

fn transfer_job(id: &str, target_path: &str) -> TransferJobRecord {
    TransferJobRecord {
        job_key: format!("key-{id}"),
        job_id: format!("job-{id}"),
        run_id: 1,
        kind: TransferKind::Load,
        spec_version: 0,
        spec_blob: Vec::new(),
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
        spec_blob: Vec::new(),
        result_json: String::new(),
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

fn fairness_contract(store: &impl TransferStore) {
    let mut running = transfer_job("running", "/running");
    running.state = TransferState::Running;
    running.owner = "owner".into();
    running.lease_epoch = 1;
    running.lease_expire_at = 120_000;
    store.create_or_get_by_request_id(running.clone()).unwrap();
    for n in 0..8 {
        store
            .create_or_get_by_request_id(transfer_job(
                &format!("pending-{n}"),
                &format!("/pending-{n}"),
            ))
            .unwrap();
    }
    // A stalled Running task is eligible on every maintenance turn. It must not
    // consume admission turns, even as its epoch/lease are repeatedly renewed.
    for n in 0..8 {
        let now = 1000 + n * 201;
        let admission = store
            .acquire_runnable_transfer_with_priority(
                "owner",
                120_000,
                now,
                32,
                SchedulingPriority::Admission,
            )
            .unwrap()
            .unwrap();
        assert!(admission.job_id.starts_with("job-pending-"));
        let maintenance = store
            .acquire_runnable_transfer_with_priority(
                "owner",
                120_000,
                now + 1,
                32,
                SchedulingPriority::Maintenance,
            )
            .unwrap()
            .unwrap();
        assert_eq!(maintenance.job_id, running.job_id);
    }
    // No early recheck; actual progress timestamps are not used for the cooldown.
    assert!(store
        .acquire_runnable_transfer_with_priority(
            "owner",
            120_000,
            2410,
            32,
            SchedulingPriority::Maintenance
        )
        .unwrap()
        .is_none());
    // Expired ownership remains recoverable, even at the executing limit.
    let lease = store
        .acquire_runnable_transfer_with_priority(
            "replacement",
            120_000,
            200_000,
            1,
            SchedulingPriority::Admission,
        )
        .unwrap()
        .unwrap();
    assert_eq!(lease.owner, "replacement");
}

#[test]
fn memory_progress_fairness() {
    fairness_contract(&MemoryTransferStore::new());
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_progress_fairness() {
    fairness_contract(&super::SqliteTransferStore::open(":memory:").unwrap());
}

// Exercise the public legacy entry point, including its backend forwarding.
fn legacy_contract(store: &impl TransferStore) {
    let mut owned = transfer_job("owned", "/owned");
    owned.state = TransferState::Running;
    owned.owner = "owner".into();
    owned.lease_epoch = 1;
    owned.lease_expire_at = 120_000;
    owned.updated_at = 999;
    let mut expired = transfer_job("expired", "/expired");
    expired.state = TransferState::Running;
    expired.owner = "old-owner".into();
    expired.lease_epoch = 1;
    expired.lease_expire_at = 1000;
    for job in [
        owned.clone(),
        expired.clone(),
        transfer_job("pending", "/pending"),
    ] {
        store.create_or_get_by_request_id(job).unwrap();
    }
    // Owned work takes precedence over an already-expired lease and Pending work.
    let first = store
        .acquire_runnable_transfer("owner", 120_000, 1000, 2)
        .unwrap()
        .unwrap();
    assert_eq!(first.job_id, owned.job_id);
    // Original behavior allows another check the next millisecond, with no cooldown.
    let second = store
        .acquire_runnable_transfer("owner", 120_000, 1001, 2)
        .unwrap()
        .unwrap();
    assert_eq!(second.job_id, owned.job_id);
    assert!(second.lease_epoch > first.lease_epoch);
    assert!(!store
        .renew_lease(
            &first.job_id,
            first.run_id,
            "owner",
            first.lease_epoch,
            120_000,
            1002
        )
        .unwrap());
    // With no owned candidate, recover expired work even at the execution cap.
    let recovered = store
        .acquire_runnable_transfer("replacement", 120_000, 1002, 2)
        .unwrap()
        .unwrap();
    assert_eq!(recovered.job_id, expired.job_id);
    assert!(store
        .acquire_runnable_transfer("third", 120_000, 1003, 2)
        .unwrap()
        .is_none());
    let pending = store
        .acquire_runnable_transfer("third", 120_000, 1003, 3)
        .unwrap()
        .unwrap();
    assert_eq!(pending.job_id, "job-pending");
    assert!(store
        .request_cancel(&owned.job_id, owned.run_id, 1004)
        .unwrap());
    let cancel = store
        .acquire_runnable_transfer("owner", 120_000, 1005, 3)
        .unwrap()
        .unwrap();
    assert_eq!(cancel.job_id, owned.job_id);
    // updated_at < now remains strict; no same-tick reacquisition.
    assert!(store
        .acquire_runnable_transfer("owner", 120_000, 1005, 3)
        .unwrap()
        .is_none());
}

#[test]
fn memory_legacy_scheduling_via_backend() {
    legacy_contract(&super::TransferStoreBackend::Memory(
        MemoryTransferStore::new(),
    ));
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_legacy_scheduling_via_backend() {
    legacy_contract(&super::TransferStoreBackend::Sqlite(
        super::SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

fn expiry_boundary_contract(store: &impl TransferStore) {
    let mut owned = transfer_job("boundary", "/boundary");
    owned.state = TransferState::Running;
    owned.owner = "owner".into();
    owned.lease_epoch = 1;
    owned.lease_expire_at = 1000;
    store.create_or_get_by_request_id(owned).unwrap();
    assert!(store
        .acquire_runnable_transfer("replacement", 120_000, 999, 1)
        .unwrap()
        .is_none());
    let lease = store
        .acquire_runnable_transfer("replacement", 120_000, 1000, 1)
        .unwrap()
        .unwrap();
    assert_eq!(lease.owner, "replacement");
    assert_eq!(lease.lease_epoch, 2);
}

#[test]
fn memory_legacy_expiry_boundary() {
    expiry_boundary_contract(&MemoryTransferStore::new());
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_legacy_expiry_boundary() {
    expiry_boundary_contract(&super::SqliteTransferStore::open(":memory:").unwrap());
}

fn maintenance_order_contract(store: &impl TransferStore) {
    let mut owned = transfer_job("owned", "/owned");
    owned.state = TransferState::Running;
    owned.owner = "owner".into();
    owned.lease_epoch = 1;
    owned.lease_expire_at = 120_000;
    let mut expired = transfer_job("expired", "/expired");
    expired.state = TransferState::Running;
    expired.lease_epoch = 1;
    expired.lease_expire_at = 1000;
    expired.updated_at = 999;
    for job in [owned, expired] {
        store.create_or_get_by_request_id(job).unwrap();
    }
    let lease = store
        .acquire_runnable_transfer_with_priority(
            "owner",
            120_000,
            1000,
            2,
            SchedulingPriority::Maintenance,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        lease.job_id, "job-expired",
        "enabled maintenance must prioritize recovery"
    );
}

#[test]
fn memory_enabled_maintenance_prioritizes_expired() {
    maintenance_order_contract(&super::TransferStoreBackend::Memory(
        MemoryTransferStore::new(),
    ));
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_enabled_maintenance_prioritizes_expired() {
    maintenance_order_contract(&super::TransferStoreBackend::Sqlite(
        super::SqliteTransferStore::open(":memory:").unwrap(),
    ));
}

fn report_contract(store: &impl TransferStore) {
    use curvine_model::TransferTaskReport;
    let mut job = transfer_job("report", "/report");
    job.state = TransferState::Running;
    store.create_or_get_by_request_id(job.clone()).unwrap();
    let mut task = transfer_task(&job);
    task.state = TransferTaskState::Running;
    task.attempt_id = 2;
    task.worker_id = 7;
    task.worker_session_id = "session".into();
    store.insert_tasks(vec![task.clone()]).unwrap();
    let report = TransferTaskReport {
        job_id: job.job_id.clone(),
        run_id: job.run_id,
        task_id: task.task_id.clone(),
        attempt_id: task.attempt_id,
        worker_id: task.worker_id,
        worker_session_id: task.worker_session_id.clone(),
        state: TransferTaskState::Completed,
        progress: TransferProgress {
            loaded_size: 4_194_304,
            total_size: 4_194_304,
            update_time: 10,
            message: "done".into(),
        },
        result_inline: None,
        result_ref: None,
        result_size: None,
        now_ms: 10,
        stale_deadline_at: 60_010,
    };
    assert!(store.update_task_report(report.clone()).unwrap());
    let first_job = store.get_transfer(&job.job_id).unwrap().unwrap();
    let first_tasks = store.list_transfer_tasks(&job.job_id, job.run_id).unwrap();
    // Lost ACK after commit: exact replay is accepted, but no timestamps or counters move.
    let mut replay = report.clone();
    replay.now_ms = 5000;
    replay.stale_deadline_at = 65000;
    assert!(store.update_task_report(replay).unwrap());
    for variant in 0..7 {
        let mut stale = report.clone();
        match variant {
            0 => stale.attempt_id -= 1,
            1 => stale.worker_session_id = "old-session".into(),
            2 => stale.worker_id += 1,
            3 => stale.run_id += 1,
            4 => stale.state = TransferTaskState::Running,
            5 => stale.state = TransferTaskState::Failed,
            _ => stale.progress.loaded_size -= 1,
        }
        assert!(
            !store.update_task_report(stale).unwrap(),
            "variant {variant}"
        );
    }
    let after = store.get_transfer(&job.job_id).unwrap().unwrap();
    assert_eq!(after.summary, first_job.summary);
    assert_eq!(after.updated_at, first_job.updated_at);
    assert_eq!(
        store.list_transfer_tasks(&job.job_id, job.run_id).unwrap(),
        first_tasks
    );
}

#[test]
fn memory_progress_report_replay() {
    report_contract(&MemoryTransferStore::new());
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_progress_report_replay() {
    report_contract(&super::SqliteTransferStore::open(":memory:").unwrap());
}

fn capacity_and_cancel_contract(
    store: impl TransferStore,
    priorities: &'static [SchedulingPriority],
) {
    use std::sync::{Arc, Barrier};
    let store = Arc::new(store);
    for n in 0..8 {
        store
            .create_or_get_by_request_id(transfer_job(
                &format!("capacity-{n}"),
                &format!("/capacity-{n}"),
            ))
            .unwrap();
    }
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|n| {
            let priority = priorities[n % priorities.len()];
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .acquire_runnable_transfer_with_priority("owner", 120_000, 1000, 1, priority)
                    .unwrap()
            })
        })
        .collect();
    let claims: Vec<_> = threads
        .into_iter()
        .filter_map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(claims.len(), 1);
    assert_eq!(store.count_executing_transfers().unwrap(), 1);
    let claim = &claims[0];
    assert!(store
        .request_cancel(&claim.job_id, claim.run_id, 1001)
        .unwrap());
    let cancel = store
        .acquire_runnable_transfer_with_priority(
            "owner",
            120_000,
            1002,
            1,
            SchedulingPriority::Maintenance,
        )
        .unwrap()
        .unwrap();
    assert_eq!(cancel.job_id, claim.job_id);
    assert!(!store
        .renew_lease(
            &claim.job_id,
            claim.run_id,
            "owner",
            claim.lease_epoch,
            120_000,
            1003
        )
        .unwrap());
}

#[test]
fn memory_progress_capacity_and_cancel() {
    for priorities in [
        &[SchedulingPriority::Legacy][..],
        &[
            SchedulingPriority::Admission,
            SchedulingPriority::Maintenance,
        ][..],
        &[
            SchedulingPriority::Legacy,
            SchedulingPriority::Admission,
            SchedulingPriority::Maintenance,
        ][..],
    ] {
        capacity_and_cancel_contract(MemoryTransferStore::new(), priorities);
    }
}

#[cfg(feature = "transfer-store-sqlite")]
#[test]
fn sqlite_progress_capacity_and_cancel() {
    for priorities in [
        &[SchedulingPriority::Legacy][..],
        &[
            SchedulingPriority::Admission,
            SchedulingPriority::Maintenance,
        ][..],
        &[
            SchedulingPriority::Legacy,
            SchedulingPriority::Admission,
            SchedulingPriority::Maintenance,
        ][..],
    ] {
        capacity_and_cancel_contract(
            super::SqliteTransferStore::open(":memory:").unwrap(),
            priorities,
        );
    }
}

#[test]
fn memory_pending_selection_matches_scan_order() {
    // Keep the original scan-based ordering as an independent test oracle.
    for priority in [
        SchedulingPriority::Legacy,
        SchedulingPriority::Admission,
        SchedulingPriority::Maintenance,
    ] {
        for kind in [TransferKind::Load, TransferKind::Export] {
            let store = MemoryTransferStore::new();
            let states = [
                TransferState::Planning,
                TransferState::Dispatching,
                TransferState::Running,
                TransferState::Canceling,
                TransferState::Completed,
                TransferState::Failed,
                TransferState::Canceled,
                TransferState::PartialSuccess,
            ];
            let mut jobs = Vec::new();
            for (n, state) in states.into_iter().enumerate() {
                let mut job = transfer_job(&format!("active-{n}"), &format!("/active-{n}"));
                job.kind = kind;
                job.tenant = format!("tenant-{n}");
                job.state = state;
                job.owner = "other-owner".into();
                job.lease_expire_at = 100_000;
                jobs.push(job);
            }
            for n in 0..24 {
                let mut job = transfer_job(&format!("pending-{n:02}"), &format!("/pending-{n}"));
                job.kind = kind;
                job.tenant = if n % 9 == 8 {
                    String::new()
                } else {
                    format!("tenant-{}", n % 9)
                };
                job.updated_at = 1 + n % 3; // Include timestamp and job-ID ties.
                                            // One ineligible Pending job; the others are eligible exactly at now.
                job.lease_expire_at = if n == 23 { 1001 } else { 1000 };
                jobs.push(job);
            }
            for job in &jobs {
                store.create_or_get_by_request_id(job.clone()).unwrap();
            }
            loop {
                let expected = jobs
                    .iter()
                    .filter(|job| {
                        job.state == TransferState::Pending && job.lease_expire_at <= 1000
                    })
                    .min_by_key(|job| {
                        let executing = jobs
                            .iter()
                            .filter(|other| {
                                other.tenant == job.tenant && other.state.is_executing()
                            })
                            .count();
                        (u64::from(executing > 0), job.updated_at, &job.job_id)
                    })
                    .map(|job| job.job_id.clone());
                let lease = store
                    .acquire_runnable_transfer_with_priority("owner", 120_000, 1000, 100, priority)
                    .unwrap();
                assert_eq!(lease.as_ref().map(|v| &v.job_id), expected.as_ref());
                let Some(lease) = lease else { break };
                let job = jobs
                    .iter_mut()
                    .find(|job| job.job_id == lease.job_id)
                    .unwrap();
                job.state = TransferState::Planning;
                let claimed = store.get_transfer(&lease.job_id).unwrap().unwrap();
                assert_eq!(claimed.state, TransferState::Planning);
                assert_eq!(claimed.owner, "owner");
                assert_eq!(claimed.lease_epoch, 1);
                assert_eq!(claimed.lease_expire_at, 121_000);
            }
        }
    }
}

#[test]
fn memory_pending_pressure_tracks_expiry_cancel_and_completion() {
    use curvine_model::TransferStateUpdate;

    let store = MemoryTransferStore::new();
    let mut active = transfer_job("active", "/active");
    active.tenant = "busy".into();
    active.state = TransferState::Running;
    active.owner = "old-owner".into();
    active.lease_epoch = 1;
    active.lease_expire_at = 1000;
    store.create_or_get_by_request_id(active.clone()).unwrap();
    for (id, tenant, time) in [("busy", "busy", 1), ("idle", "idle", 2)] {
        let mut job = transfer_job(id, &format!("/{id}"));
        job.tenant = tenant.into();
        job.updated_at = time;
        store.create_or_get_by_request_id(job).unwrap();
    }
    // Admission still counts an expired Running job as tenant pressure.
    let idle = store
        .acquire_runnable_transfer_with_priority(
            "owner",
            120_000,
            1000,
            3,
            SchedulingPriority::Admission,
        )
        .unwrap()
        .unwrap();
    assert_eq!(idle.job_id, "job-idle");
    // At the cap, expired work is recoverable but Pending work is not admitted.
    let recovered = store
        .acquire_runnable_transfer_with_priority(
            "replacement",
            120_000,
            1000,
            2,
            SchedulingPriority::Maintenance,
        )
        .unwrap()
        .unwrap();
    assert_eq!(recovered.job_id, active.job_id);
    assert_eq!(recovered.lease_epoch, 2);
    assert!(!store
        .renew_lease(&active.job_id, 1, "old-owner", 1, 120_000, 1001)
        .unwrap());
    assert!(store.request_cancel(&active.job_id, 1, 1001).unwrap());
    let mut fresh = transfer_job("fresh", "/fresh");
    fresh.tenant = "fresh".into();
    fresh.updated_at = 2;
    store.create_or_get_by_request_id(fresh).unwrap();
    let update = |epoch| TransferStateUpdate {
        job_id: active.job_id.clone(),
        run_id: 1,
        owner: "replacement".into(),
        lease_epoch: epoch,
        from_states: vec![TransferState::Canceling],
        to_state: TransferState::Canceled,
        message: "canceled".into(),
        now_ms: 1002,
    };
    assert!(!store.update_transfer_state(update(1)).unwrap());
    assert!(store
        .acquire_runnable_transfer_with_priority(
            "third",
            120_000,
            1002,
            2,
            SchedulingPriority::Admission,
        )
        .unwrap()
        .is_none());
    assert!(store.update_transfer_state(update(2)).unwrap());
    // Finishing cancellation frees capacity and removes pressure for the busy tenant.
    let busy = store
        .acquire_runnable_transfer_with_priority(
            "third",
            120_000,
            1003,
            2,
            SchedulingPriority::Admission,
        )
        .unwrap()
        .unwrap();
    assert_eq!(busy.job_id, "job-busy");
    assert_eq!(store.count_executing_transfers().unwrap(), 2);
}

#[test]
fn memory_concurrent_pending_claims_spread_tenants_within_cap() {
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};

    let store = Arc::new(MemoryTransferStore::new());
    for n in 0..8 {
        let mut job = transfer_job(&format!("spread-{n}"), &format!("/spread-{n}"));
        job.tenant = if n < 4 { "a" } else { "b" }.into();
        job.kind = if n < 4 {
            TransferKind::Load
        } else {
            TransferKind::Export
        };
        job.updated_at = n + 1;
        store.create_or_get_by_request_id(job).unwrap();
    }
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|n| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let priority = [
                    SchedulingPriority::Legacy,
                    SchedulingPriority::Admission,
                    SchedulingPriority::Maintenance,
                ][n % 3];
                store
                    .acquire_runnable_transfer_with_priority("owner", 120_000, 1000, 2, priority)
                    .unwrap()
            })
        })
        .collect();
    let claims: Vec<_> = threads
        .into_iter()
        .filter_map(|t| t.join().unwrap())
        .collect();
    assert_eq!(claims.len(), 2);
    let ids: HashSet<_> = claims.iter().map(|lease| lease.job_id.as_str()).collect();
    assert_eq!(ids, HashSet::from(["job-spread-0", "job-spread-4"]));
    assert_eq!(store.count_executing_transfers().unwrap(), 2);
}
