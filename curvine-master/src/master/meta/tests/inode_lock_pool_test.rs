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

use super::super::{InodeLockManager, InodeLockRequest};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
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
fn same_stripe_writers_serialise() {
    // With a striped table, ids sharing a stripe must serialise just as they
    // did with the per-key pool; a distinct stripe must not contend.
    let manager = Arc::new(InodeLockManager::new(1));
    let held = manager.write_inode(0);
    let other = manager.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _guard = other.write_inode(1);
        acquired_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let acquired_while_held = acquired_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    drop(held);
    waiter.join().unwrap();
    assert!(
        !acquired_while_held,
        "single-stripe table must serialise same-stripe writers"
    );
}

#[test]
fn different_stripes_do_not_block() {
    let pool = super::super::object_lock_pool::ObjectLockPool::new(8);
    let other_id = (1..64).find(|&id| pool.index(id) != pool.index(0)).unwrap() as i64;
    let manager = Arc::new(InodeLockManager::new(8));
    let held = manager.write_inode(0);
    let other = manager.clone();
    let (tx, rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let _guard = other.write_inode(other_id);
        tx.send(()).unwrap();
    });
    let acquired = rx.recv_timeout(Duration::from_secs(5)).is_ok();
    drop(held);
    waiter.join().unwrap();
    assert!(acquired, "different stripes must not block each other");
}

#[test]
fn same_stripe_write_blocks_read() {
    let manager = Arc::new(InodeLockManager::new(1));
    let held = manager.write_inode(0);
    let other = manager.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _guard = other.read_inode(1);
        acquired_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let acquired_while_held = acquired_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    drop(held);
    waiter.join().unwrap();
    assert!(!acquired_while_held, "writer must exclude stripe readers");
}

#[test]
fn same_stripe_lock_many_takes_one_guard_and_does_not_deadlock() {
    // ids 0 and 1 share stripe 0 of a single-stripe table; a self-deadlock
    // would hang the test. The guard list must carry one guard per stripe.
    let manager = Arc::new(InodeLockManager::new(1));
    let requests = [InodeLockRequest::write(0, 0), InodeLockRequest::read(1, 1)];
    let guard = manager.lock_many(&requests);
    let other = manager.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (acquired_tx, acquired_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _guard = other.read_inode(1);
        acquired_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let acquired_while_held = acquired_rx.recv_timeout(Duration::from_millis(100)).is_ok();
    drop(guard);
    waiter.join().unwrap();
    assert!(!acquired_while_held, "merged stripe must be write locked");
}

#[test]
fn opposite_stripe_orders_do_not_deadlock() {
    let pool = super::super::object_lock_pool::ObjectLockPool::new(8);
    let (a, b) = (0..64)
        .find_map(|a| {
            (a + 1..64)
                .find(|&b| pool.index(a) == pool.index(b))
                .map(|b| (a, b))
        })
        .unwrap();
    let c = (0..64).find(|&c| pool.index(c) != pool.index(a)).unwrap();
    let (a, b, c) = (a as i64, b as i64, c as i64);
    let locks = Arc::new(InodeLockManager::new(8));
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
                    [
                        InodeLockRequest::write(0, c),
                        InodeLockRequest::write(1, a),
                        InodeLockRequest::write(2, b),
                    ]
                } else {
                    [
                        InodeLockRequest::write(0, b),
                        InodeLockRequest::write(1, a),
                        InodeLockRequest::write(2, c),
                    ]
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
            .expect("opposite stripe-order requests deadlocked");
    }
    for worker in workers {
        worker.join().unwrap();
    }
}
