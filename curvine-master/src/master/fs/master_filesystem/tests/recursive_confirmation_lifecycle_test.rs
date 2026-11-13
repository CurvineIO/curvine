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
    // After the release the stripe is free again: an actual non-blocking
    // acquisition now succeeds, and the queued waiter picks up the terminal
    // state.
    assert!(
        fs.inode_locks.try_read_inode(dir_id).is_some(),
        "after the release the stripe must be acquirable again"
    );
    let (failed_at_lock, view_err) = acquired_rx
        .recv_timeout(WORKER_TIMEOUT)
        .expect("the waiter must acquire after the unwind releases");
    reader.join().expect("the waiter must not panic");

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
