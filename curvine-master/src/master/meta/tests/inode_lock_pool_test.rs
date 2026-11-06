use super::super::{InodeLockManager, InodeLockRequest};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

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
