use super::super::{InodeLockManager, InodeLockRequest};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[test]
fn single_inode_lock_many_records_one_acquisition() {
    const ENV: &str = "CURVINE_INODE_LOCK_METRIC_TEST";
    if std::env::var_os(ENV).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "master::meta::tests::inode_lock_pool_test::single_inode_lock_many_records_one_acquisition",
            ])
            .env(ENV, "1")
            .status()
            .unwrap();
        assert!(status.success(), "isolated metric test failed: {status}");
        return;
    }

    crate::master::Master::init_test_metrics();
    let histogram = crate::master::Master::get_metrics()
        .unwrap()
        .inode_lock_acquire_us
        .with_label_values(&["write"]);
    let before = histogram.get_sample_count();
    let read_histogram = crate::master::Master::get_metrics()
        .unwrap()
        .inode_lock_acquire_us
        .with_label_values(&["read"]);
    let read_before = read_histogram.get_sample_count();
    let manager = InodeLockManager::new(4);
    thread::scope(|scope| {
        for id in 0..8 {
            let manager = &manager;
            scope.spawn(move || drop(manager.write_inode(id)));
        }
    });
    drop(manager.lock_many(&[InodeLockRequest::read(0, 41)]));
    drop(manager.lock_many(&[InodeLockRequest::write(0, 42)]));
    drop(manager.write_inode(43));
    drop(manager.lock_many(&[
        InodeLockRequest::read(0, 44),
        InodeLockRequest::write(1, 45),
    ]));
    assert_eq!(histogram.get_sample_count() - before, 11);
    assert_eq!(read_histogram.get_sample_count() - read_before, 1);
}

#[test]
fn released_inode_locks_are_reclaimed_after_a_concurrent_peak() {
    let manager = InodeLockManager::new(1);
    let guards: Vec<_> = (0..128).map(|id| manager.write_inode(id)).collect();
    assert_eq!(manager.cached_lock_count(), 128);
    drop(guards);
    assert!(manager.cached_lock_count() <= 64);

    let requests: Vec<_> = (0..128).map(|id| InodeLockRequest::write(0, id)).collect();
    let guard = manager.lock_many(&requests);
    assert_eq!(manager.cached_lock_count(), 128);
    drop(guard);
    assert!(manager.cached_lock_count() <= 64);
}

#[test]
fn cleanup_keeps_active_inode_lock() {
    let manager = Arc::new(InodeLockManager::new(1));
    let held = manager.write_inode(0);
    let guards: Vec<_> = (1..129).map(|id| manager.write_inode(id)).collect();
    drop(guards);

    let (started_tx, started_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let other = manager.clone();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _guard = other.write_inode(0);
        acquired_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let acquired_while_held = acquired_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    drop(held);
    waiter.join().unwrap();
    assert!(
        !acquired_while_held,
        "cleanup replaced an active inode lock"
    );
}
