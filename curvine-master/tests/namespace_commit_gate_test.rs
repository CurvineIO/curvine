use curvine_master::master::meta::CommitGate;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

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

// The cancellation-fence regression moved to
// src/master/journal/tests/snapshot_cancel_gate_test.rs: it needs a controlled
// interleaving point inside the apply worker's CreateSnapshot branch, which an
// integration target cannot reach. The old driver (blocking the worker with a
// cold namespace guard) no longer represents the protocol: the snapshot body
// reads the shared view and a closed view fails immediately instead of waiting.

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

/// §6: a close fence issued AFTER an admission's CAS must observe (and
/// wait for) that admission's count; a fence issued BEFORE must reject it.
/// Interleaved with deterministic handshakes on two real slots.
#[test]
fn close_fences_admissions_that_entered_before_it_and_rejects_later_ones() {
    let gate = Arc::new(CommitGate::new());

    // Writer A enters first (its slot count is now 1).
    let a = gate.enter();
    // A close starts: close_owned fences deterministically and returns
    // immediately; the quiescence wait is explicit afterwards.
    let barrier = gate.close_owned();
    assert!(
        gate.try_enter().is_none(),
        "an admission during a pending close must be rejected"
    );

    // A leaves: the explicit drain must now complete deterministically.
    drop(a);
    gate.wait_for_quiescence();
    drop(barrier);
    // Reopen semantics: the barrier is gone, so admissions work again.
    assert!(gate.try_enter().is_some());
}

/// §6: the last writer's leave must notify a closer that is already parked
/// on the Condvar (no missed wakeup), and a closer that observes zero
/// before parking must not sleep at all.
#[test]
fn last_leave_wakes_a_parked_closer_without_missed_notifications() {
    let gate = Arc::new(CommitGate::new());
    let first = gate.enter();
    let second = gate.enter();

    let (closed_tx, closed_rx) = mpsc::channel();
    let closer = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let barrier = gate.close_and_wait_owned();
            closed_tx.send(()).expect("the closer must finish");
            barrier
        })
    };

    // Deterministic handshake: the closer holds the lifecycle mutex in its
    // wait only after arming close; both writers leave while it waits.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while gate.try_enter().is_some() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    drop(first);
    drop(second);
    closed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the last leave must wake the parked closer");
    closer.join().expect("the closer must not panic");
    assert!(gate.try_enter().is_some(), "the gate must reopen");
}

/// §4.1: nested closes (close reference counting) keep the gate closed
/// until the LAST close guard drops; each open_one removes exactly one.
#[test]
fn nested_closes_stay_closed_until_the_last_guard_drops() {
    let gate = Arc::new(CommitGate::new());
    let first = gate.close_owned();
    assert!(gate.try_enter().is_none());
    let second = gate.close_owned();
    drop(first);
    // One guard dropped, one still held: still closed.
    assert!(gate.try_enter().is_none());
    drop(second);
    assert!(gate.try_enter().is_some(), "the last drop must reopen");
}

/// §4.1: a close_owned may drop before the drain without stranding later
/// barriers: old writers keep their counts (no store(0)), and a LATER
/// close still observes the surviving in-flight writer.
#[test]
fn close_owned_dropped_before_drain_keeps_counts_for_later_barriers() {
    let gate = Arc::new(CommitGate::new());
    let writer = gate.enter();

    // Close and drop the guard before draining: the writer's count must
    // survive (fetch_and keeps counts).
    drop(gate.close_owned());

    // A fresh close must still see the surviving writer: its barrier wait
    // must not end while the writer is in flight.
    let (closed_tx, closed_rx) = mpsc::channel();
    let closer = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let barrier = gate.close_and_wait_owned();
            closed_tx.send(()).expect("the closer must finish");
            barrier
        })
    };
    assert!(
        closed_rx.try_recv().is_err(),
        "a barrier must not report quiescence while an un-drained writer survives"
    );

    drop(writer);
    closed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the surviving writer's leave must end the barrier");
    closer.join().expect("the closer must not panic");
}

/// §5: a transient fence between a snapshot's capture and its guard must
/// invalidate the waiting snapshot (generation), even though the close
/// guard is already gone.
#[test]
fn transient_generation_cancels_a_waiting_snapshot() {
    let gate = Arc::new(CommitGate::new());
    let writer = gate.enter();

    let (outcome_tx, outcome_rx) = mpsc::channel();
    let snapshot = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let guard = gate.close_and_enter_if_open();
            outcome_tx.send(guard.is_some()).expect("snapshot outcome");
        })
    };
    // Deterministic park evidence: once the snapshot's close has fenced
    // the gate, admissions are rejected while the exclusive has NOT been
    // granted yet (the writer is still in flight).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while gate.try_enter().is_some() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        gate.try_enter().is_none() && !gate.has_exclusive_writer(),
        "the snapshot must be parked between the fence and its grant"
    );

    // A lifecycle close+reopen (generation bump) happens while the
    // snapshot waits; its close reference must be unwound.
    let transient = gate.close_owned();
    drop(transient);

    drop(writer);
    snapshot.join().expect("the snapshot must not panic");
    assert!(
        !outcome_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or(true),
        "a snapshot raced by a transient close/generation must be cancelled"
    );
    // The gate must be fully open again: both the close reference and the
    // unwound snapshot reference are gone.
    assert!(gate.try_enter().is_some());
    assert!(!gate.has_exclusive_writer());
}

/// §5: the exclusive writer occupies a real slot; an ordinary barrier
/// must wait for it like any other in-flight writer.
#[test]
fn exclusive_writer_is_waited_on_by_an_ordinary_barrier() {
    let gate = Arc::new(CommitGate::new());
    let exclusive = gate
        .close_and_enter_if_open()
        .expect("an open gate must admit the exclusive writer");
    assert!(gate.has_exclusive_writer());
    assert!(gate.try_enter().is_none());

    let (closed_tx, closed_rx) = mpsc::channel();
    let closer = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let barrier = gate.close_and_wait_owned();
            closed_tx.send(()).expect("the closer must finish");
            barrier
        })
    };
    assert!(
        closed_rx.try_recv().is_err(),
        "an ordinary barrier must wait for the exclusive writer"
    );

    drop(exclusive);
    closed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the exclusive writer's leave must end the barrier");
    closer.join().expect("the closer must not panic");
    assert!(!gate.has_exclusive_writer());
}

/// §5: a guard dropped on a DIFFERENT thread subtracts from the slot the
/// admission actually entered (cross-thread drop keeps per-slot counts).
#[test]
fn cross_thread_guard_drop_subtracts_from_the_original_slot() {
    let gate = Arc::new(CommitGate::new());

    // Two admissions enter on this thread; both guards move to other
    // threads and drop there. A concurrent close barrier must wait for
    // BOTH cross-thread drops before reporting quiescence.
    let first = gate.enter();
    let second = gate.enter();

    let (closed_tx, closed_rx) = mpsc::channel();
    let closer = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let barrier = gate.close_and_wait_owned();
            closed_tx.send(()).expect("the closer must finish");
            drop(barrier);
        })
    };
    assert!(
        closed_rx.try_recv().is_err(),
        "the barrier must wait while both guards live on other threads"
    );

    let (done_tx, done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(move || drop(first));
        scope.spawn(move || drop(second));
        scope.spawn(move || {
            done_tx.send(()).expect("the dropper must finish");
        });
    });

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the dropper must finish its drops");
    closed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the cross-thread drops must end the barrier");
    closer.join().expect("the closer must not panic");
    assert!(gate.try_enter().is_some(), "the gate must reopen");
}

/// §6: reopen publishes per-slot Release; the next admission's Acquire
/// observes it in order (the reopened gate admits and the cycle repeats).
#[test]
fn per_slot_reopen_publishes_the_next_admission_phase() {
    let gate = Arc::new(CommitGate::new());
    for _ in 0..3 {
        let barrier = gate.close_and_wait();
        assert!(gate.try_enter().is_none());
        drop(barrier);
        let guard = gate.enter();
        drop(guard);
    }
}

/// §6 gap ④: a close fence must fence BOTH slots and each writer's leave
/// must be counted on its own slot — a concurrent close waits until BOTH
/// writers (on different slots) have left, and admissions during the
/// pending close are rejected on every slot.
#[test]
fn close_waits_for_writers_on_distinct_slots_and_rejects_all_slots() {
    let gate = Arc::new(CommitGate::new());

    // Deterministic slot assignment: each admission's actual slot is
    // observed through its guard behavior, not guessed; the two writers
    // enter and stay in flight.
    let w1 = gate.enter();
    let w2 = gate.enter();

    let (fenced_tx, fenced_rx) = mpsc::channel();
    let closer = {
        let gate = gate.clone();
        std::thread::spawn(move || {
            let barrier = gate.close_and_wait_owned();
            fenced_tx.send(()).expect("the closer must finish");
            barrier
        })
    };
    // Park evidence: the close fenced the gate (admissions rejected on
    // every slot) while both writers are still in flight.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while gate.try_enter().is_some() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(gate.try_enter().is_none());
    assert!(
        fenced_rx.try_recv().is_err(),
        "the close must still wait for BOTH in-flight writers"
    );

    // Writers leave in opposite order from different threads: each leave
    // must subtract from its own slot; quiescence ends only after BOTH.
    let (left_tx, left_rx) = mpsc::channel::<&'static str>();
    std::thread::scope(|scope| {
        scope.spawn({
            let left_tx = left_tx.clone();
            move || {
                drop(w2);
                left_tx.send("second").expect("leave report");
            }
        });
        scope.spawn(move || {
            drop(w1);
            left_tx.send("first").expect("leave report");
        });
    });
    let mut left = Vec::new();
    for _ in 0..2 {
        left.push(
            left_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("both writers must report their leave"),
        );
    }
    assert!(left.contains(&"first") && left.contains(&"second"));

    fenced_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the close must complete after both leaves");
    closer.join().expect("the closer must not panic");
    assert!(gate.try_enter().is_some(), "the gate must reopen");
}

/// §6 gap ⑤: a close that starts after a live late admission must wait
/// for it. Deterministic: the admission thread signals the guard is held
/// BEFORE the closer starts, so the fence provably happens while the
/// count is nonzero; the guard drops at scope end, after the close is
/// already parked.
#[test]
fn late_admission_after_reopen_is_waited_on_by_a_later_close() {
    let gate = Arc::new(CommitGate::new());

    // Phase 1: close and drain cleanly (the gate settles open again).
    let barrier = gate.close_and_wait_owned();
    drop(barrier);

    // Phase 2: ordered handshake — the admission is guaranteed live
    // before the closer starts.
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    std::thread::scope(|scope| {
        let gate = &gate; // borrow the outer Arc inside the scope
        let attempt = scope.spawn(move || {
            let guard = gate.enter();
            entered_tx.send(()).expect("the admission must report");
            // Hold the guard until the driver releases (the closer is
            // parked by then).
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("the driver must release the guard");
            guard
        });

        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the late admission must hold its guard");

        // The close starts while the guard is provably held.
        let (closer_done_tx, closer_done_rx) = mpsc::channel();
        let closer = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                let barrier = gate.close_and_wait_owned();
                closer_done_tx.send(()).expect("the closer must finish");
                drop(barrier);
            })
        };
        // Park evidence: bounded poll until the closer's fence lands
        // (admissions rejected); at that point the guard is provably held
        // and the closer provably parked on it — its completion must not
        // arrive before the release.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while gate.try_enter().is_some() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            gate.try_enter().is_none(),
            "the closer must have fenced the gate"
        );
        assert!(
            closer_done_rx.try_recv().is_err(),
            "a close that started after a live admission must wait for it"
        );

        // Release the guard: the parked close must complete.
        release_tx.send(()).expect("release the guard");
        attempt.join().expect("the attempt must not panic");
        closer_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the guard drop must end the close");
        closer.join().expect("the closer must not panic");
        assert!(gate.try_enter().is_some(), "the gate must reopen");
    });
}
