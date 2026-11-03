use curvine_master::master::meta::CommitGate;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

#[test]
fn multiple_barriers_keep_gate_closed_until_last_drop() {
    let gate = Arc::new(CommitGate::new());
    let first = gate.close_owned();
    let second = gate.close_owned();
    drop(first);
    assert!(gate.try_enter().is_none());
    drop(second);
    assert!(gate.try_enter().is_some());
}

#[test]
fn exclusive_writer_keeps_gate_closed_until_drop() {
    let gate = CommitGate::new();
    let guard = gate.close_and_enter_if_open().unwrap();
    assert!(gate.has_exclusive_writer());
    assert!(gate.try_enter().is_none());
    assert!(gate.close_and_enter_if_open().is_none());
    drop(guard);
    assert!(!gate.has_exclusive_writer());
    assert!(gate.try_enter().is_some());
}

#[test]
fn barrier_waits_for_exclusive_writer() {
    let gate = Arc::new(CommitGate::new());
    let exclusive = gate.close_and_enter_if_open().unwrap();
    let barrier = gate.close_owned();
    let (done, waiting) = mpsc::sync_channel(1);
    let waiter_gate = gate.clone();
    let waiter = std::thread::spawn(move || {
        waiter_gate.wait_for_quiescence();
        done.send(()).unwrap();
    });

    assert!(waiting.recv_timeout(Duration::from_millis(30)).is_err());
    drop(exclusive);
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    waiter.join().unwrap();
    assert!(gate.try_enter().is_none());
    drop(barrier);
    assert!(gate.try_enter().is_some());
}

#[test]
fn close_owned_fences_writers_without_waiting_for_in_flight_commit() {
    let gate = Arc::new(CommitGate::new());
    let in_flight = gate.enter();
    let started = Instant::now();
    let barrier = gate.close_owned();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(gate.try_enter().is_none());

    let (done, waiting) = mpsc::sync_channel(1);
    let wait_gate = gate.clone();
    let worker = std::thread::spawn(move || {
        wait_gate.wait_for_quiescence();
        done.send(()).unwrap();
    });
    assert!(waiting.recv_timeout(Duration::from_millis(30)).is_err());
    drop(in_flight);
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    worker.join().unwrap();
    assert!(gate.try_enter().is_none());
    drop(barrier);
    assert!(gate.try_enter().is_some());
}

#[test]
fn transient_barrier_invalidates_waiting_snapshot() {
    let gate = Arc::new(CommitGate::new());
    let writer = gate.enter();
    let snapshot_gate = gate.clone();
    let (done, result) = mpsc::sync_channel(1);
    let snapshot = std::thread::spawn(move || {
        done.send(snapshot_gate.close_and_enter_if_open().is_some())
            .unwrap();
    });
    let start = Instant::now();
    while gate.try_enter().is_some() {
        assert!(start.elapsed() < Duration::from_secs(2));
        std::thread::yield_now();
    }
    let barrier = gate.close_owned();
    drop(barrier);
    drop(writer);
    assert!(!result.recv_timeout(Duration::from_secs(2)).unwrap());
    snapshot.join().unwrap();
    assert!(gate.try_enter().is_some());
}

#[test]
fn restore_closes_admission_while_snapshot_waits_for_writers() {
    let gate = Arc::new(CommitGate::new());
    let in_flight = gate.enter();
    let snapshot_gate = gate.clone();
    let (done, result) = mpsc::sync_channel(1);
    let snapshot = std::thread::spawn(move || {
        done.send(snapshot_gate.close_and_enter_if_open().is_some())
            .unwrap();
    });
    let start = Instant::now();
    while gate.try_enter().is_some() {
        assert!(start.elapsed() < Duration::from_secs(2));
        std::thread::yield_now();
    }
    let close_gate = gate.clone();
    let (closed, closed_rx) = mpsc::sync_channel(1);
    let closer = std::thread::spawn(move || {
        let barrier = close_gate.close_owned();
        closed.send(()).unwrap();
        barrier
    });
    let closed_before_writer_exits = closed_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    drop(in_flight);
    assert!(!result.recv_timeout(Duration::from_secs(2)).unwrap());
    snapshot.join().unwrap();
    let barrier = closer.join().unwrap();
    assert!(
        closed_before_writer_exits,
        "snapshot waiter held the admission mutex"
    );
    drop(barrier);
    assert!(gate.try_enter().is_some());
}
