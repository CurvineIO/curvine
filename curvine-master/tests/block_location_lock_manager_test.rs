use curvine_master::master::meta::BlockLocationLockManager;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

#[test]
fn reversed_and_duplicate_keys_use_the_same_lock_order() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let start = Arc::new(std::sync::Barrier::new(3));
    let (done, completed) = mpsc::channel();
    let mut threads = Vec::new();
    for (workers, blocks) in [
        (vec![1, 2, 1], vec![10, 20, 10]),
        (vec![2, 1, 2], vec![20, 10, 20]),
    ] {
        let locks = locks.clone();
        let start = start.clone();
        let done = done.clone();
        threads.push(thread::spawn(move || {
            start.wait();
            let _guard = locks.write_workers_blocks(&workers, &blocks);
            done.send(()).unwrap();
        }));
    }
    start.wait();
    completed.recv_timeout(Duration::from_secs(5)).unwrap();
    completed.recv_timeout(Duration::from_secs(5)).unwrap();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn same_shard_different_blocks_do_not_serialize() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.write_blocks(&[1001]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.write_blocks(&[1002]);
        acquired_tx.send(()).unwrap();
    });

    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}

#[test]
fn same_block_write_locks_are_mutually_exclusive() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.write_blocks(&[1001]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.write_blocks(&[1001]);
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

#[test]
fn same_shard_different_workers_do_not_serialize() {
    let locks = Arc::new(BlockLocationLockManager::new(1, 1));
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let holder_locks = locks.clone();
    let holder = thread::spawn(move || {
        let _guard = holder_locks.write_worker_blocks(1001, &[]);
        held_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });

    held_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let waiter_locks = locks.clone();
    let waiter = thread::spawn(move || {
        let _guard = waiter_locks.write_worker_blocks(1002, &[]);
        acquired_tx.send(()).unwrap();
    });

    acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    waiter.join().unwrap();
}
