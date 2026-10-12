use super::*;

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
