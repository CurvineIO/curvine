use curvine_master::master::meta::{InodeLockManager, InodeLockRequest};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[test]
fn separate_managers_may_lock_the_same_inode_on_one_thread() {
    let first = InodeLockManager::new(1);
    let second = InodeLockManager::new(1);
    let _first_guard = first.lock_many(&[InodeLockRequest::write(0, 42)]);
    let _second_guard = second.lock_many(&[InodeLockRequest::write(0, 42)]);
}

#[cfg(debug_assertions)]
#[test]
fn same_manager_reentrant_lock_is_rejected() {
    let manager = InodeLockManager::new(1);
    let guard = manager.lock_many(&[InodeLockRequest::write(0, 42)]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        manager.lock_many(&[InodeLockRequest::read(0, 42)]);
    }));
    assert!(result.is_err());
    drop(guard);
    let _guard = manager.lock_many(&[InodeLockRequest::write(0, 42)]);
}

#[test]
fn duplicate_requests_are_merged_with_write_access() {
    let locks = InodeLockManager::new(1);
    let guard = locks.lock_many(&[
        InodeLockRequest::read(0, 42),
        InodeLockRequest::write(1, 42),
    ]);
    assert!(guard.covers_request(InodeLockRequest::write(0, 42)));
    assert!(!guard.covers_request(InodeLockRequest::read(0, 43)));
}

#[test]
fn same_shard_different_inodes_do_not_serialize() {
    let locks = Arc::new(InodeLockManager::new(1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.lock_many(&[InodeLockRequest::write(1, 1001)]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.lock_many(&[InodeLockRequest::write(1, 1002)]);
        acquired_tx.send(()).unwrap();
    });

    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}

#[test]
fn same_inode_write_locks_are_mutually_exclusive() {
    let locks = Arc::new(InodeLockManager::new(1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.lock_many(&[InodeLockRequest::write(1, 1001)]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.lock_many(&[InodeLockRequest::write(1, 1001)]);
        acquired_tx.send(()).unwrap();
    });

    assert!(acquired_rx
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_tx.send(()).unwrap();
    acquired_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}
