use super::*;

#[cfg(feature = "fault-injection")]
#[test]
fn replication_rejects_detached_metadata_store() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("replication-detached-store");
    let block_id = replication_file(&fs, "/replication-detached.log");
    let (checkpoint, data_dir) = {
        let dir = fs.fs_dir.read();
        (
            dir.create_checkpoint(1).unwrap(),
            dir.store_ref().store().db.conf().data_dir.clone(),
        )
    };
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.restore.after_store_detach")
        .matches("data_dir", data_dir.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected restore failure")
        .unwrap();
    faults
        .configure("replication-detached-store", rule)
        .unwrap();
    let result = fs.fs_dir.write().restore(&checkpoint, 0);
    faults.remove("replication-detached-store").unwrap();
    assert!(result.is_err());
    let dir = fs.fs_dir.read();
    assert!(dir.replication_block_state(block_id).is_err());
    assert!(dir
        .add_replication_location_if_needed(block_id, BlockLocation::with_id(200))
        .is_err());
}

#[test]
fn replica_completion_invalidates_older_full_report_reconcile() {
    let fs = test_fs("replica-completion-invalidates-full-report");
    let block_id = replication_file(&fs, "/replica-completion.log");
    fs.full_block_reconciles.lock().insert(
        200,
        FullBlockReconcileState {
            running: true,
            generation: 1,
            pending: None,
        },
    );

    assert!(fs
        .commit_replication_location(block_id, BlockLocation::with_id(200), "stale-session")
        .is_err());
    assert!(fs.is_full_block_reconcile_current(200, 1));
    assert!(fs
        .commit_replication_location(block_id, BlockLocation::with_id(200), "")
        .unwrap());
    assert!(fs
        .reconcile_full_block_report(200, 1, HashSet::new())
        .unwrap()
        .is_empty());
    fs.full_block_reconciles.lock().insert(
        200,
        FullBlockReconcileState {
            running: true,
            generation: 2,
            pending: None,
        },
    );
    assert!(!fs
        .commit_replication_location(block_id, BlockLocation::with_id(200), "")
        .unwrap());
    assert!(fs
        .reconcile_full_block_report(200, 2, HashSet::new())
        .unwrap()
        .is_empty());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .any(|location| location.worker_id == 200));
}

#[test]
fn replica_completion_invalidates_partial_full_report() {
    let fs = test_fs("replica-completion-invalidates-partial-report");
    let block_id = replication_file(&fs, "/replica-partial.log");
    let mut report = BlockReportList {
        cluster_id: String::new(),
        worker_id: 200,
        full_report: true,
        total_len: 2,
        report_id: 42,
        blocks: vec![BlockReportInfo::new(
            1,
            BlockReportStatus::Finalized,
            StorageType::Disk,
            1,
        )],
    };
    assert!(matches!(
        fs.full_report_tracker.collect(&report, LocalTime::mills()),
        FullReportProgress::Partial
    ));
    assert!(fs
        .commit_replication_location(block_id, BlockLocation::with_id(200), "")
        .unwrap());
    report.blocks[0].id = 2;
    assert!(matches!(
        fs.full_report_tracker.collect(&report, LocalTime::mills()),
        FullReportProgress::Partial
    ));
}

#[test]
fn replica_completion_restarts_begun_full_report() {
    let fs = test_fs("replica-completion-restarts-begun-report");
    let block_id = replication_file(&fs, "/replica-begun-report.log");
    let worker_id = 200;
    let session_id = "report-session";
    {
        let mut workers = fs.worker_manager.write();
        workers
            .worker_map
            .workers
            .get_mut(&worker_id)
            .unwrap()
            .worker_session_id = session_id.to_string();
    }

    assert_eq!(
        fs.begin_full_block_report_for_live_worker_session(worker_id, 7, session_id),
        Some(true)
    );
    assert!(fs
        .commit_replication_location(block_id, BlockLocation::with_id(worker_id), session_id)
        .unwrap());

    let stale = fs
        .block_report_for_live_worker_session(
            BlockReportList {
                cluster_id: String::new(),
                worker_id,
                full_report: true,
                total_len: 1,
                report_id: 7,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            },
            session_id,
            None,
        )
        .unwrap()
        .unwrap();
    assert!(stale.restart_full_report);
    assert!(stale.delete_blocks.is_empty());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .any(|location| location.worker_id == worker_id));
    assert_eq!(
        fs.begin_full_block_report_for_live_worker_session(worker_id, 7, session_id),
        Some(false)
    );
    assert_eq!(
        fs.begin_full_block_report_for_live_worker_session(worker_id, 8, session_id),
        Some(true)
    );
}

#[test]
fn replica_completion_and_full_report_share_block_lock() {
    let fs = test_fs("replica-full-report-race");
    let block_id = replication_file(&fs, "/replica-full-report-race.log");
    let start = Arc::new(std::sync::Barrier::new(3));
    let replication = {
        let fs = fs.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            fs.commit_replication_location(block_id, BlockLocation::with_id(200), "")
                .unwrap()
        })
    };
    let report = {
        let fs = fs.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            let report = BlockReportList {
                cluster_id: String::new(),
                worker_id: 300,
                full_report: true,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            };
            start.wait();
            fs.block_report(report, None).unwrap()
        })
    };
    start.wait();
    let added = replication.join().unwrap();
    let report = report.join().unwrap();
    let state = fs
        .fs_dir
        .read()
        .replication_block_state(block_id)
        .unwrap()
        .unwrap();
    assert_eq!(state.locations.len(), 2);
    assert_eq!(
        state
            .locations
            .iter()
            .any(|location| location.worker_id == 200),
        added
    );
    assert_eq!(report.delete_blocks.contains(&block_id), added);
    assert_eq!(
        state
            .locations
            .iter()
            .any(|location| location.worker_id == 300),
        !added
    );
}

#[test]
fn concurrent_full_reports_do_not_overfill_a_block() {
    let fs = test_fs("concurrent-full-report-replica-limit");
    let block_id = replication_file(&fs, "/concurrent-full-report.log");
    let start = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for worker_id in [300, 301] {
        let fs = fs.clone();
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            let report = BlockReportList {
                cluster_id: String::new(),
                worker_id,
                full_report: true,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            };
            start.wait();
            fs.block_report(report, None).unwrap()
        }));
    }
    start.wait();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(
        results
            .iter()
            .filter(|result| result.delete_blocks.contains(&block_id))
            .count(),
        1
    );
    let state = fs
        .fs_dir
        .read()
        .replication_block_state(block_id)
        .unwrap()
        .unwrap();
    assert_eq!(state.locations.len(), 2);
    assert_eq!(
        state
            .locations
            .iter()
            .filter(|location| [300, 301].contains(&location.worker_id))
            .count(),
        1
    );
}

#[test]
fn full_block_report_reclaims_excess_replica() {
    let fs = test_fs("full-block-report-excess-replica");
    let block_id = replication_file(&fs, "/full-block-report-excess-replica.log");
    fs.fs_dir
        .write()
        .add_block_location(block_id, BlockLocation::with_id(200))
        .unwrap();

    let result = fs
        .block_report(
            BlockReportList {
                cluster_id: String::new(),
                worker_id: 300,
                full_report: true,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            },
            None,
        )
        .unwrap();

    assert_eq!(result.delete_blocks, vec![block_id]);
    let state = fs
        .fs_dir
        .read()
        .replication_block_state(block_id)
        .unwrap()
        .unwrap();
    assert_eq!(state.locations.len(), 2);
    assert!(state
        .locations
        .iter()
        .all(|location| location.worker_id != 300));
}

#[test]
fn stale_block_report_does_not_register_location() {
    let fs = test_fs("stale-block-report-session");
    let block_id = replication_file(&fs, "/stale-block-report-session.log");
    fs.worker_manager
        .write()
        .worker_map
        .workers
        .get_mut(&300)
        .unwrap()
        .worker_session_id = "current-session".to_string();

    let result = fs
        .block_report_for_live_worker_session(
            BlockReportList {
                cluster_id: String::new(),
                worker_id: 300,
                full_report: false,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            },
            "stale-session",
            None,
        )
        .unwrap();

    assert!(result.is_none());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .all(|location| location.worker_id != 300));

    assert!(fs
        .block_report_for_legacy_live_worker(
            BlockReportList {
                cluster_id: String::new(),
                worker_id: 300,
                full_report: false,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            },
            None,
        )
        .unwrap()
        .is_none());
}

#[test]
fn lost_cleanup_only_removes_matching_session_locations() {
    let fs = test_fs("lost-worker-session-cleanup");
    let block_id = replication_file(&fs, "/lost-worker-session-cleanup.log");
    let session_id = "lost-session";
    {
        let mut worker_manager = fs.worker_manager.write();
        worker_manager
            .worker_map
            .workers
            .get_mut(&100)
            .unwrap()
            .worker_session_id = session_id.to_string();
        assert!(worker_manager.remove_expired_worker(100).is_some());
    }

    assert!(fs
        .delete_lost_worker_locations(100, "other-session")
        .unwrap()
        .is_none());
    let cleanup = fs
        .delete_lost_worker_locations(100, session_id)
        .unwrap()
        .expect("matching lost session must be cleaned");
    assert_eq!(cleanup.removed_block_ids, vec![block_id]);
    assert_eq!(cleanup.replication_block_ids, vec![block_id]);
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .is_empty());
}

#[test]
fn old_lost_cleanup_skips_recovered_worker_session() {
    let fs = test_fs("lost-worker-session-fence");
    let block_id = replication_file(&fs, "/lost-worker-session-fence.log");
    let old_session = "old-session";
    {
        let mut worker_manager = fs.worker_manager.write();
        let worker = worker_manager.worker_map.workers.get_mut(&100).unwrap();
        worker.worker_session_id = old_session.to_string();
        worker.startup_time_ms = 10;
        let address = worker.address.clone();
        assert!(worker_manager.remove_expired_worker(100).is_some());
        let cluster_id = worker_manager.cluster_id.clone();

        worker_manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                address.clone(),
                WorkerInfo::default_weight(),
                "new-session".to_string(),
                TransferWorkerCapabilities::default(),
                0,
                0,
                String::new(),
                20,
                Vec::new(),
                None,
                false,
            )
            .unwrap();
        worker_manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                address,
                WorkerInfo::default_weight(),
                "new-session".to_string(),
                TransferWorkerCapabilities::default(),
                0,
                0,
                String::new(),
                20,
                Vec::new(),
                None,
                false,
            )
            .unwrap();
    }

    assert!(fs
        .delete_lost_worker_locations(100, old_session)
        .unwrap()
        .is_none());
    assert_eq!(
        fs.fs_dir
            .read()
            .get_block_locations(block_id)
            .unwrap()
            .len(),
        1
    );
}
