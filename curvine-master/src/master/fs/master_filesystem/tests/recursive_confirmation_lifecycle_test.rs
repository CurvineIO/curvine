use super::recursive_confirmation_support::*;
use super::recursive_confirmation_support::{await_worker, WORKER_TIMEOUT};
use super::*;
use crate::master::meta::NamespaceError;
use std::sync::mpsc::RecvTimeoutError;

/// §8.1: a panic inside the body is a real unwind; the guard declared inside
/// the body's lexical scope goes terminal before the view and object locks
/// drop, so a waiter that queued for the lock sees the failed holder.
#[test]
fn recursive_body_unwind_terminals_before_unlock() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-body-unwind");
    fs.mkdir("/dir", true).unwrap();
    drain_receipts(&fs.journal_writer.clone());

    let writer = swap_receipts_writer(&mut fs, &journal, &conf);
    let seq_before = writer.failure_seq();
    let dir_id = fs.file_status("/dir").unwrap().id;

    let (body_ready_tx, body_ready_rx) = std::sync::mpsc::channel();
    let (reader_ready_tx, reader_ready_rx) = std::sync::mpsc::channel::<()>();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    // The hook also fires on the Drop path: the unwind drops the guard,
    // fail_terminal consumes it while the locks are still being unwound.
    let (terminal_tx, terminal_rx) = std::sync::mpsc::channel::<()>();
    let (terminal_release_tx, terminal_release_rx) = std::sync::mpsc::channel::<()>();
    writer.arm_terminal_hook(terminal_tx, terminal_release_rx);
    let worker_fs = fs.clone();
    let worker: std::thread::JoinHandle<Result<(), curvine_error::FsError>> =
        std::thread::spawn(move || {
            let _stay = done_tx;
            run_admitted::<()>(&worker_fs, "/dir", InodeLockMode::Write, true, |_fs_dir| {
                body_ready_tx.send(()).unwrap();
                reader_ready_rx
                    .recv_timeout(WORKER_TIMEOUT)
                    .expect("the driver must confirm the waiter is queued");
                panic!("recursive body unwind")
            })
        });

    body_ready_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the body must run while the frame holds the object locks");

    let (queued_tx, queued_rx) = std::sync::mpsc::channel();
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let reader = spawn_lock_waiter(fs.clone(), dir_id, queued_tx, acquired_tx);
    queued_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the waiter must reach the lock acquisition");
    reader_ready_tx
        .send(())
        .expect("the unwinding body must still be waiting");
    assert!(
        acquired_rx.recv_timeout(LOCK_HELD_PROBE).is_err(),
        "the waiter must not acquire the stripe while the unwinding body holds it"
    );

    // The strict in-lock terminal proof for the unwind path: the guard
    // drops before the locks do, so at the hook point an actual non-blocking
    // shared acquisition of the target stripe returns None (the stripe is
    // still held by the unwinding frame).
    terminal_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the unwind terminal must reach the hook while the locks unwind");
    assert!(
        fs.inode_locks.try_read_inode(dir_id).is_none(),
        "at the terminal point the target stripe must still be held by the unwind"
    );
    terminal_release_tx
        .send(())
        .expect("the driver must release the armed terminal hook");

    // The unwind drops the channel sender, so the driver sees a disconnect
    // only after the worker thread finished; the join afterwards is bounded.
    match await_worker(&done_rx, "the unwinding body") {
        Err(RecvTimeoutError::Disconnected) => {}
        Ok(()) => panic!("the body unwind must abort the worker"),
        Err(RecvTimeoutError::Timeout) => unreachable!("await_worker panics on Timeout"),
    }
    let join_err = worker
        .join()
        .expect_err("the body unwind must abort the worker");
    // The waiter picks up the terminal state after the unwind releases; its
    // guard must be joined (dropped) before the final try-read, or the legal
    // reader could still hold the stripe and race the Some assertion.
    let (failed_at_lock, view_err) = acquired_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the waiter must acquire after the unwind releases");
    reader.join().expect("the waiter must not panic");
    // After the release (and with the waiter's guard dropped) an actual
    // non-blocking acquisition succeeds again.
    assert!(
        fs.inode_locks.try_read_inode(dir_id).is_some(),
        "after the release the stripe must be acquirable again"
    );

    assert!(
        unwind_message(join_err).contains("recursive body unwind"),
        "the body panic must propagate as a real unwind"
    );
    assert!(
        failed_at_lock,
        "the unwind must go terminal inside the locks"
    );
    assert!(
        matches!(view_err.unwrap(), NamespaceError::Unavailable),
        "the waiter must find the view rejected after the unwind"
    );

    assert_holder_terminal(&fs, &writer, seq_before);
    assert!(
        journal.master_monitor().recovery().fatal_source().is_none(),
        "an unwind terminal must not invent a fatal source"
    );

    drop(writer);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
    Ok(())
}

/// §8.1: the wait unwind is exercised on the real scope wait via the
/// instance-isolated hook. Before the release the driver observes that the
/// body already published and the holder is still usable (the object locks
/// dropped before the wait); after the release the real wait panics and the
/// armed guard terminalizes the holder without undoing the publication.
#[test]
fn recursive_wait_unwind_observes_then_terminals() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-wait-unwind");
    fs.mkdir("/tree", true).unwrap();
    drain_receipts(&fs.journal_writer.clone());

    let writer = swap_receipts_writer(&mut fs, &journal, &conf);
    let seq_before = writer.failure_seq();
    let tree_id = fs.file_status("/tree").unwrap().id;

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    writer.arm_wait_unwind_hook(reached_tx, release_rx);

    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        let _stay = done_tx;
        worker_fs.set_attr(
            "/tree",
            SetAttrOpts {
                recursive: true,
                mode: Some(0o600),
                ..Default::default()
            },
        )
    });

    reached_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the real wait must be reached while the receipt is pending");

    // Pre-release observation: published, holder usable, locks released.
    assert_eq!(
        fs.file_status("/tree").unwrap().mode,
        0o600,
        "the recursive set_attr body must have published before the wait"
    );
    assert_eq!(
        writer.failure_seq(),
        seq_before,
        "no terminal must happen while the receipt is still pending"
    );
    assert!(!fs.fs_dir.is_failed());
    let (queued_tx, queued_rx) = std::sync::mpsc::channel();
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let reader = spawn_lock_waiter(fs.clone(), tree_id, queued_tx, acquired_tx);
    queued_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the probe reader must reach the lock acquisition");
    let (probe_failed, probe_view) = acquired_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the object locks are released before the wait, so the probe acquires");
    reader.join().expect("the probe reader must not panic");
    assert!(
        !probe_failed && probe_view.is_none(),
        "before the wait unwinds the holder must still be usable"
    );

    release_tx.send(()).unwrap();
    match await_worker(&done_rx, "the wait unwind") {
        Err(RecvTimeoutError::Disconnected) => {}
        Ok(()) => panic!("the injected wait unwind must abort the worker"),
        Err(RecvTimeoutError::Timeout) => unreachable!("await_worker panics on Timeout"),
    }
    let join_err = worker
        .join()
        .expect_err("the injected wait unwind must abort the worker");
    assert!(
        unwind_message(join_err).contains("unwind"),
        "the injected wait unwind must propagate from the real wait"
    );

    assert_holder_terminal(&fs, &writer, seq_before);
    assert!(
        fs.file_status("/tree").is_err(),
        "a fresh status read must be rejected, not silently served as unchanged"
    );

    drop(writer);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
    Ok(())
}

/// §8.1: an Ok body followed by a real commit failure returns the commit
/// error and the guard's Drop terminalizes the holder before the slot ends.
#[test]
fn recursive_commit_error_after_ok_body_terminals_holder() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-commit-error-after-ok");
    fs.mkdir("/tree", true).unwrap();
    drain_receipts(&fs.journal_writer.clone());

    let writer = swap_receipts_writer(&mut fs, &journal, &conf);
    let seq_before = writer.failure_seq();

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = worker_fs.set_attr(
            "/tree",
            SetAttrOpts {
                recursive: true,
                mode: Some(0o620),
                ..Default::default()
            },
        );
        let _ = done_tx.send(result);
    });

    let queued = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(WORKER_TIMEOUT)
                .expect("missing the recursive set_attr journal entry")
        })
        .unwrap();
    let _commit_failed = drop(queued.completion);

    let commit_err = await_worker(&done_rx, "the commit failure")
        .expect("the commit failure must report its result")
        .unwrap_err();
    worker.join().expect("the worker must not panic");
    assert!(
        commit_err
            .to_string()
            .contains("journal commit failed after metadata commit"),
        "the real commit error must be returned, got {commit_err}"
    );

    assert_holder_terminal(&fs, &writer, seq_before);

    drop(writer);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
    Ok(())
}

/// §8.1: an Ok body followed by a real commit failure returns the commit
/// error and the guard's Drop terminalizes the holder before the slot ends.
#[test]
fn recursive_disjoint_writer_completes_while_another_waits() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-disjoint-inflight-writer");
    fs.mkdir("/a", true).unwrap();
    fs.mkdir("/b", true).unwrap();
    drain_receipts(&fs.journal_writer.clone());

    let writer = swap_receipts_writer(&mut fs, &journal, &conf);
    let seq_before = writer.failure_seq();

    // A publishes a real durable mutation and then waits for its receipt.
    let (a_ready_tx, a_ready_rx) = std::sync::mpsc::channel();
    let (a_done_tx, a_done_rx) = std::sync::mpsc::channel();
    let a_fs = fs.clone();
    let first = std::thread::spawn(move || {
        let result = run_admitted(&a_fs, "/a", InodeLockMode::Read, false, |fs_dir| {
            let inp = MasterFilesystem::resolve_path(fs_dir, "/a")?;
            fs_dir.set_attr(
                inp,
                SetAttrOpts {
                    mode: Some(0o600),
                    ..Default::default()
                },
            )?;
            a_ready_tx
                .send(())
                .expect("the driver must observe the body");
            Ok(())
        });
        let _ = a_done_tx.send(result);
    });
    a_ready_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("A must publish its body");
    let a_entry = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(WORKER_TIMEOUT)
                .expect("missing A's journal entry")
        })
        .unwrap();

    // A still holds its ordinary slot while waiting for the receipt: the gate
    // is not closed for exclusive use and a fresh admission succeeds.
    assert!(
        !fs.metadata_commit_gate().has_exclusive_writer(),
        "a pending confirmation must not hold an exclusive gate"
    );
    assert!(
        fs.metadata_commit_gate().try_enter().is_some(),
        "the ordinary slot must stay available while another writer waits"
    );

    // B is a disjoint in-flight writer that must still be inside its
    // confirmation window when A goes terminal: the driver holds B's real
    // receipt without resolving it until after A's terminal, so the §8.2
    // second half - terminal is not a cancellation of already-admitted
    // writers - is observed on a genuinely in-flight admission, not a
    // completed one.
    let (b_done_tx, b_done_rx) = std::sync::mpsc::channel();
    let b_fs = fs.clone();
    let second = std::thread::spawn(move || {
        let result = run_admitted(&b_fs, "/b", InodeLockMode::Read, false, |fs_dir| {
            let inp = MasterFilesystem::resolve_path(fs_dir, "/b")?;
            fs_dir.set_attr(
                inp,
                SetAttrOpts {
                    mode: Some(0o640),
                    ..Default::default()
                },
            )?;
            Ok(())
        });
        let _ = b_done_tx.send(result);
    });
    let b_entry = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(WORKER_TIMEOUT)
                .expect("missing B's journal entry")
        })
        .unwrap();
    assert_eq!(
        fs.file_status("/b").unwrap().mode,
        0o640,
        "B's body must have published while A still waits"
    );
    assert!(!fs.fs_dir.is_failed(), "B must not terminalize the holder");

    // Now A's confirmation fails while B's real receipt is still pending.
    let a_entry = a_entry;
    drop(a_entry.completion);
    let a_result =
        await_worker(&a_done_rx, "A").expect("A must report after its confirmation fails");
    first.join().expect("A must not panic");
    assert!(
        a_result
            .unwrap_err()
            .to_string()
            .contains("journal commit failed"),
        "A must surface its real commit error"
    );
    assert_holder_terminal(&fs, &writer, seq_before);

    // B's terminal-time observation: its admission was live across A's
    // terminal, and its own already-published body stays intact (terminal
    // is not a cancellation); B then finishes under its original rules when
    // the driver acks its real receipt: a real successful confirmation is a
    // send on the completion channel, not a drop (drop = confirmation
    // failure, which is A's path above).
    b_entry
        .completion
        .as_ref()
        .expect("B's receipt must still carry its completion sender")
        .send(())
        .expect("acking B's real receipt must succeed");
    drop(b_entry.completion);
    let b_result =
        await_worker(&b_done_rx, "B after the terminal").expect("B must report its result");
    second.join().expect("B must not panic");
    b_result.unwrap();

    drop(writer);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
    Ok(())
}
