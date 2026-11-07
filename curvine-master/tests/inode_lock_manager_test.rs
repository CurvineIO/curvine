use curvine_master::master::meta::{InodeLockManager, InodeLockRequest};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
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
fn coverage_handles_sorted_lock_ids_and_modes() {
    let manager = InodeLockManager::new(2);
    let guard = manager.lock_many(&[
        InodeLockRequest::read(2, 99),
        InodeLockRequest::write(1, -7),
        InodeLockRequest::read(0, 42),
        InodeLockRequest::write(2, 99),
    ]);
    assert!(guard.covers_requests(&[
        InodeLockRequest::read(8, -7),
        InodeLockRequest::read(1, 42),
        InodeLockRequest::write(0, 99),
    ]));
    assert!(!guard.covers_request(InodeLockRequest::write(0, 42)));
    for id in [-8, 0, 43, 100] {
        assert!(!guard.covers_request(InodeLockRequest::read(0, id)));
    }
    assert!(manager.lock_many(&[]).covers_requests(&[]));
}

#[test]
fn large_unsorted_duplicate_requests_keep_strongest_mode() {
    let locks = InodeLockManager::new(64);
    let mut requests = Vec::new();
    for i in 0..64 {
        let id = (i * 37 % 64) as i64;
        requests.push(InodeLockRequest::read(i, id));
        if id % 2 == 0 {
            requests.push(InodeLockRequest::write(64 - i, id));
        }
    }
    let guard = locks.lock_many(&requests);
    for id in 0..64 {
        assert!(guard.covers_request(InodeLockRequest::read(0, id)));
        assert_eq!(
            guard.covers_request(InodeLockRequest::write(0, id)),
            id % 2 == 0
        );
    }
}

#[test]
fn opposite_path_orders_do_not_deadlock() {
    let locks = Arc::new(InodeLockManager::new(1));
    let start = Arc::new(Barrier::new(3));
    let active = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = mpsc::channel();
    let mut workers = Vec::new();
    for reverse in [false, true] {
        let locks = locks.clone();
        let start = start.clone();
        let active = active.clone();
        let done = done_tx.clone();
        workers.push(thread::spawn(move || {
            start.wait();
            for _ in 0..1_000 {
                let requests = if reverse {
                    [InodeLockRequest::write(0, 2), InodeLockRequest::write(1, 1)]
                } else {
                    [InodeLockRequest::write(0, 1), InodeLockRequest::write(1, 2)]
                };
                let _guard = locks.lock_many(&requests);
                assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
            }
            done.send(()).unwrap();
        }));
    }
    start.wait();
    for _ in 0..2 {
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("opposite lock requests deadlocked");
    }
    for worker in workers {
        worker.join().unwrap();
    }
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
