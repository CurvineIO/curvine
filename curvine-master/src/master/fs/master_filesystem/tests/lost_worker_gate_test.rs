use super::*;

fn block_events(fs: &MasterFilesystem) -> usize {
    fs.worker_manager
        .write()
        .block_map
        .handle_heartbeat(100)
        .len()
}

#[test]
fn lost_worker_cleanup_does_not_cross_a_closed_commit_gate() {
    let fs = test_fs("lost-worker-commit-gate");
    let block_id = replication_file(&fs, "/lost-gate.log");
    assert_eq!(block_events(&fs), 0);

    let gate = fs.metadata_commit_gate();
    let barrier = gate.close_owned();
    let worker_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx.send(worker_fs.delete_locations(100)).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    assert_eq!(
        fs.fs_dir
            .read()
            .get_block_locations(block_id)
            .unwrap()
            .len(),
        1
    );

    drop(barrier);
    let cleanup = done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    assert_eq!(cleanup.removed_block_ids, vec![block_id]);
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .is_empty());
}

#[test]
fn close_file_waits_for_commit_gate() {
    let fs = test_fs("close-file-commit-gate");
    fs.create("/file", true).unwrap();
    let client = {
        let dir = fs.fs_dir.read();
        let path = MasterFilesystem::resolve_path(&dir, "/file").unwrap();
        let inode = path.get_last_inode().unwrap();
        inode
            .as_file_ref()
            .unwrap()
            .features
            .file_write
            .as_ref()
            .unwrap()
            .clients
            .iter()
            .next()
            .unwrap()
            .clone()
    };
    let gate = fs.metadata_commit_gate();
    let barrier = gate.close_owned();
    let worker_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker_fs.close_file("/file", &client))
            .unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let blocked = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    let writing = {
        let dir = fs.fs_dir.read();
        let path = MasterFilesystem::resolve_path(&dir, "/file").unwrap();
        path.get_last_inode()
            .unwrap()
            .as_file_ref()
            .unwrap()
            .is_writing()
    };
    drop(barrier);
    worker.join().unwrap();
    assert!(matches!(
        blocked,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(writing);
    done_rx.recv().unwrap().unwrap();
    let dir = fs.fs_dir.read();
    let path = MasterFilesystem::resolve_path(&dir, "/file").unwrap();
    assert!(!path
        .get_last_inode()
        .unwrap()
        .as_file_ref()
        .unwrap()
        .is_writing());
}

#[test]
fn independent_block_reports_progress_with_namespace_reader() {
    let fs = test_fs("block-report-shared-namespace");
    let blocks = [
        replication_file(&fs, "/report-a"),
        replication_file(&fs, "/report-b"),
    ];
    let reader = fs.fs_dir.read();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let mut workers = Vec::new();
    for (index, block_id) in blocks.into_iter().enumerate() {
        let worker_fs = fs.clone();
        let done = done_tx.clone();
        workers.push(std::thread::spawn(move || {
            let report = BlockReportList {
                cluster_id: String::new(),
                worker_id: 300 + index as u32,
                full_report: false,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            };
            done.send(worker_fs.block_report(report, None)).unwrap();
        }));
    }
    let first = done_rx.recv_timeout(std::time::Duration::from_secs(2));
    let second = done_rx.recv_timeout(std::time::Duration::from_secs(2));
    drop(reader);
    for worker in workers {
        worker.join().unwrap();
    }
    first
        .expect("first report blocked on namespace reader")
        .unwrap();
    second
        .expect("second report blocked on namespace reader")
        .unwrap();
    for (index, block_id) in blocks.into_iter().enumerate() {
        assert!(fs
            .fs_dir
            .read()
            .get_block_locations(block_id)
            .unwrap()
            .iter()
            .any(|location| location.worker_id == 300 + index as u32));
    }
}

#[test]
fn block_report_locks_only_conflicting_blocks() {
    let fs = test_fs("block-report-conflict");
    let blocked_block = replication_file(&fs, "/blocked-report");
    let other_block = replication_file(&fs, "/independent-report");
    let held = fs.block_location_locks.write_blocks(&[blocked_block]);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let mut workers = Vec::new();
    for (worker_id, block_id) in [(300, blocked_block), (301, other_block)] {
        let worker_fs = fs.clone();
        let done = done_tx.clone();
        workers.push(std::thread::spawn(move || {
            let report = BlockReportList {
                cluster_id: String::new(),
                worker_id,
                full_report: false,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            };
            done.send((worker_id, worker_fs.block_report(report, None)))
                .unwrap();
        }));
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut entered = false;
    while std::time::Instant::now() < deadline {
        if fs.worker_location_lock(300).try_lock().is_none() {
            entered = true;
            break;
        }
        std::thread::yield_now();
    }
    let first = done_rx.recv_timeout(std::time::Duration::from_secs(5));
    let early_second = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(held);
    for worker in workers {
        worker.join().unwrap();
    }
    assert!(entered, "conflicting report did not reach its lock");
    let (first_worker, first_result) = first.expect("independent report did not complete");
    first_result.unwrap();
    assert_eq!(first_worker, 301);
    assert!(
        early_second.is_err(),
        "conflicting report bypassed the block lock"
    );
    let (second_worker, second_result) = done_rx.recv().unwrap();
    second_result.unwrap();
    assert_eq!(second_worker, 300);
}

#[test]
fn block_report_waits_for_commit_gate() {
    let fs = test_fs("block-report-commit-gate");
    let block_id = replication_file(&fs, "/report.log");
    let gate = fs.metadata_commit_gate();
    let barrier = gate.close_owned();
    let worker_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let report = BlockReportList {
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
        };
        started_tx.send(()).unwrap();
        done_tx.send(worker_fs.block_report(report, None)).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let blocked = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    let locations = fs.fs_dir.read().get_block_locations(block_id).unwrap();
    drop(barrier);
    worker.join().unwrap();
    assert!(matches!(
        blocked,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(locations.iter().all(|location| location.worker_id != 300));
    done_rx.recv().unwrap().unwrap();
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .any(|location| location.worker_id == 300));
}

#[test]
fn reconcile_rechecks_generation_after_gate_wait() {
    let fs = test_fs("reconcile-gate-generation");
    let block_id = replication_file(&fs, "/reconcile.log");
    fs.full_block_reconciles.lock().insert(
        100,
        FullBlockReconcileState {
            running: true,
            generation: 1,
            pending: None,
        },
    );
    let gate = fs.metadata_commit_gate();
    let barrier = gate.close_owned();
    let worker_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        worker_fs.reconcile_full_block_report(100, 1, HashSet::new())
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut holding_worker_lock = false;
    while std::time::Instant::now() < deadline {
        if fs.worker_location_lock(100).try_lock().is_none() {
            holding_worker_lock = true;
            break;
        }
        std::thread::yield_now();
    }
    fs.invalidate_full_block_reconcile(100);
    drop(barrier);
    assert!(worker.join().unwrap().unwrap().is_empty());
    assert!(
        holding_worker_lock,
        "reconcile did not reach gate admission"
    );
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .any(|location| location.worker_id == 100));
}

#[test]
fn replication_location_progresses_with_namespace_reader() {
    let fs = test_fs("replication-location-shared-namespace");
    let block_id = replication_file(&fs, "/replication-shared.log");
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx
            .send(worker_fs.commit_replication_location(block_id, BlockLocation::with_id(200), ""))
            .unwrap();
    });
    let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
    drop(reader);
    worker.join().unwrap();
    assert!(result
        .expect("replication blocked on namespace reader")
        .unwrap());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .any(|location| location.worker_id == 200));
}

#[test]
fn replication_location_waits_for_commit_gate_and_checks_session() {
    let fs = test_fs("replication-location-commit-gate");
    let block_id = replication_file(&fs, "/replication.log");
    let gate = fs.metadata_commit_gate();
    let barrier = gate.close_owned();
    let worker_fs = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker_fs.commit_replication_location(block_id, BlockLocation::with_id(200), ""))
            .unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let blocked = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    let locations = fs.fs_dir.read().get_block_locations(block_id).unwrap();
    drop(barrier);
    worker.join().unwrap();
    assert!(matches!(
        blocked,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(locations.iter().all(|location| location.worker_id != 200));
    assert!(done_rx.recv().unwrap().unwrap());
    assert!(fs
        .commit_replication_location(block_id, BlockLocation::with_id(300), "stale")
        .is_err());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .all(|location| location.worker_id != 300));
}

#[test]
fn replication_location_waits_for_block_location_lock() {
    let fs = test_fs("replication-location-lock");
    let block_id = replication_file(&fs, "/replication-lock.log");
    let held = fs
        .block_location_locks
        .write_worker_blocks(200, &[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.commit_replication_location(
            block_id,
            BlockLocation::with_id(200),
            "",
        ))
    });

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .iter()
        .all(|location| location.worker_id != 200));

    drop(held);
    assert!(done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap());
    worker.join().unwrap().unwrap();
}
