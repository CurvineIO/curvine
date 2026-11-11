use curvine_master::master::meta::CommitGate;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use curvine_master::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
#[test]
fn filesystem_handles_for_same_journal_share_commit_gate() {
    use curvine_master::master::fs::MasterFilesystem;
    use curvine_master::master::journal::JournalSystem;
    use curvine_master::Master;

    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "shared-commit-gate-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let original = journal.fs();
    let other = MasterFilesystem::with_js(&conf, &journal);
    let original_gate = original.metadata_commit_gate();
    let other_gate = other.metadata_commit_gate();
    assert!(Arc::ptr_eq(&original_gate, &other_gate));
    let barrier = original_gate.close_owned();
    assert!(other_gate.try_enter().is_none());
    drop(barrier);
    assert!(other_gate.try_enter().is_some());
    drop(other);
    drop(original);
    journal.shutdown();
}

#[test]
fn raft_snapshot_waits_for_inflight_metadata_writers() {
    use curvine_master::master::journal::JournalSystem;
    use curvine_master::Master;
    use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};

    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "snapshot-commit-gate-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let gate = journal.fs().metadata_commit_gate();
    let in_flight = gate.enter();
    let loader = journal.journal_loader();
    let snapshot_loader = loader.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let snapshot = std::thread::spawn(move || {
        let rt = AsyncRuntime::single();
        started_tx.send(()).unwrap();
        done_tx
            .send(rt.block_on(snapshot_loader.create_snapshot()))
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while gate.try_enter().is_some() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let admission_closed = gate.try_enter().is_none();
    let entry = raft::eraftpb::Entry {
        term: 1,
        index: 1,
        ..Default::default()
    };
    journal
        .append_committed_entry_for_test(entry.clone())
        .unwrap();
    let runtime = AsyncRuntime::single();
    let applied = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            loader.apply(true, ApplyMsg::new_entry(entry)),
        )
        .await
    });
    let finished_early = done_rx.try_recv().ok();
    let completed_before_drain = finished_early.is_some();
    drop(in_flight);
    let result = match finished_early {
        Some(result) => result,
        None => done_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
    };
    snapshot.join().unwrap();
    journal.shutdown();
    assert!(admission_closed, "snapshot must close writer admission");
    assert!(
        matches!(applied, Ok(Ok(()))),
        "apply worker must remain live"
    );
    assert!(
        !completed_before_drain,
        "snapshot must wait for in-flight writers"
    );
    assert!(result.is_ok(), "snapshot failed: {result:?}");
}

#[test]
fn cancelling_snapshot_request_keeps_gate_closed_until_apply_finishes() {
    use curvine_master::master::journal::JournalSystem;
    use curvine_master::Master;
    use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
    use curvine_runtime::sync::channel::CallChannel;

    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "snapshot-cancel-gate-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let gate = fs.metadata_commit_gate();
    let fs_dir = fs.fs_dir.clone();
    let fs_lock = fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    let loader = journal.journal_loader();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let snapshot_loader = loader.clone();
    let snapshot = runtime.spawn(async move { snapshot_loader.create_snapshot().await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while gate.try_enter().is_some() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let closed_before_cancel = gate.try_enter().is_none();
    // An acknowledged no-op queued behind the snapshot cannot finish while
    // the apply worker is blocked by fs_lock. Retry if it overtakes the snapshot.
    let deadline = Instant::now() + Duration::from_secs(5);
    let marker = loop {
        let (tx, rx) = CallChannel::channel();
        runtime
            .block_on(loader.apply(false, ApplyMsg::new_entry_with_ack(Default::default(), tx)))
            .unwrap();
        let mut marker = runtime.spawn(async move { rx.receive().await });
        if runtime
            .block_on(async { tokio::time::timeout(Duration::from_millis(100), &mut marker).await })
            .is_err()
        {
            break marker;
        }
        assert!(
            Instant::now() < deadline,
            "snapshot never blocked the apply queue"
        );
    };
    snapshot.abort();
    let _ = runtime.block_on(snapshot);
    let closed_after_cancel = gate.try_enter().is_none();
    drop(fs_lock);
    runtime.block_on(marker).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while gate.try_enter().is_none() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let reopened = gate.try_enter().is_some();
    drop(fs);
    journal.shutdown();
    assert!(closed_before_cancel, "snapshot must close admission");
    assert!(
        closed_after_cancel,
        "cancelled caller must not release the snapshot fence"
    );
    assert!(reopened, "snapshot fence must eventually reopen");
}

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
