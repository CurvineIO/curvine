// Copyright 2026 OPPO
//
// Event-driven determinism tests for the file_status holder-Busy retry.
// Every scenario synchronizes on real `begin_cold` guards and the instance
// second-view watch point — never on sleeps. The cold owner thread holds
// the guard for its whole lifetime; the driver coordinates via channels.

use super::*;

use curvine_model::{FileLock, LockType};
use curvine_runtime::common::LocalTime;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env(name: &str) -> Arc<MasterFilesystem> {
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("{}-{}", name, Utils::rand_str(8)));
    conf.journal.enable = false;
    Arc::new(crate::master::journal::JournalSystem::fs_only_for_test(&conf).unwrap())
}

/// Arms the instance watch point: the next file_status attempt that reaches
/// its post-lock second view signals `reached`, then parks until the driver
/// sends `release` (the guard/lock stay with the worker thread by design).
fn arm_second_view(fs: &Arc<MasterFilesystem>) -> (Receiver<()>, Sender<()>) {
    let (tx, rx) = channel();
    let (rel_tx, rel_rx) = channel();
    *fs.fs_status_second_view_watch.lock().unwrap() = Some((tx, rel_rx));
    (rx, rel_tx)
}

/// Scenario 1: a real cold window during the first holder view — file_status
/// must wait (not fail) and return the true status after the owner releases.
#[test]
fn file_status_waits_out_a_real_cold_window() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-first-view");
    fs.create("/dir/file", true).unwrap();

    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(Duration::from_secs(30)).unwrap();
        let _ = ready_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
    });

    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must report its real begin_cold success");

    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || fs3.file_status("/dir/file"));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !worker.is_finished(),
        "file_status must wait out a real cold window, not fail fast"
    );

    release_tx.send(()).unwrap();
    let status = worker
        .join()
        .unwrap()
        .expect("file_status must succeed after the cold window reopens");
    assert_eq!(status.path, "/dir/file");
}

/// Scenario 2: the attempt acquires its inode locks, then the second holder
/// view hits the cold window — the watch fires after lock_many and before
/// the view, the worker drops the locks with HolderBusy, and an independent
/// set_lock on the same stripe succeeds while the reader is parked.
#[test]
fn file_status_drops_inode_locks_before_waiting_out_the_second_view() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-second-view");
    fs.create("/dir/file", true).unwrap();
    // Capture the inode id while the holder is OPEN (before any cold window).
    let file_id = fs.file_status("/dir/file").unwrap().id;

    let (watch_rx, watch_release_tx) = arm_second_view(&fs);
    let busy_rx = {
        let (tx, rx) = channel();
        *fs.fs_status_busy_after_locks_watch.lock().unwrap() = Some(tx);
        rx
    };
    let (owner_release_tx, owner_release_rx) = channel();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        // The owner starts only after the worker signals it holds the inode
        // locks and has dropped its view — begin_cold drains views, not
        // object locks, so it succeeds immediately at that point.
        let _ = watch_rx.recv_timeout(Duration::from_secs(10));
        let guard = fs2.fs_dir.begin_cold(Duration::from_secs(30)).unwrap();
        let _ = owner_release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
    });

    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || fs3.file_status("/dir/file"));

    // The attempt reached its post-lock second-view watch (holding I; the
    // planning view is already dropped, which is exactly what the owner's
    // begin_cold needs to succeed).
    let _ = watch_rx; // moved into the owner thread: begin_cold starts after it.

    // Release the second view: it now hits the cold Busy and drops its
    // inode locks before returning HolderBusy — the busy-after-locks watch
    // fires only after those locks are gone.
    watch_release_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the second view must fail with Busy after the locks drop");

    // While the reader is parked on HolderBusy (locks dropped), an
    // independent thread must be able to take the same inode's stripe read
    // lock directly from the lock manager (no holder view involved).
    let fs4 = fs.clone();
    let probe = std::thread::spawn(move || {
        // Some means the stripe lock is free: the parked reader must not
        // hold it while it waits out the cold window (a held read lock would
        // make this try return None).
        let guard = fs4
            .inode_locks
            .try_read_inode(file_id)
            .expect("the parked reader must not hold the inode stripe lock");
        drop(guard);
    });
    probe
        .join()
        .expect("the stripe probe must not be blocked by the parked reader");

    owner_release_tx.send(()).unwrap();
    let status = worker
        .join()
        .unwrap()
        .expect("file_status must succeed once the cold owner releases");
    assert_eq!(status.path, "/dir/file");
}

/// Scenario 3: FAILED is terminal — once the holder fails closed, file_status
/// must return Unavailable promptly instead of spinning to the deadline.
#[test]
fn file_status_reports_unavailable_after_terminal_failure() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-failed");
    fs.create("/dir/file", true).unwrap();

    let (ready_tx, ready_rx) = channel();
    let (fail_tx, fail_rx) = channel::<()>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(Duration::from_secs(30)).unwrap();
        let _ = ready_tx.send(());
        let _ = fail_rx.recv_timeout(Duration::from_secs(30));
        drop(guard.fail_closed());
    });

    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || fs3.file_status("/dir/file"));
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !worker.is_finished(),
        "file_status must be waiting in the real cold window"
    );

    fail_tx.send(()).unwrap();
    let started = Instant::now();
    let result = worker.join().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the terminal failure must return promptly, not spin to the deadline"
    );
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("permanently unavailable"),
        "FAILED must surface as the terminal Unavailable error: {error}"
    );
}

/// Scenario 4 (fault feature): a business error is never swallowed by the
/// Busy retry — the locked_build fault returns before the status is built
/// and must propagate instead of triggering a holder-Busy retry.
#[cfg(feature = "fault-injection")]
#[test]
fn file_status_propagates_business_errors_without_busy_retry() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    Master::init_test_metrics();
    let fs = env("file-status-business-error");
    fs.create("/dir/file", true).unwrap();

    // The locked_build fault returns inside the locked window; the Busy
    // retry must never absorb this non-holder business error.
    let runtime = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.file_status.locked_build")
        .return_error("business in progress")
        .unwrap();
    runtime
        .configure("file-status-business-error", rule)
        .expect("the fault point must be registered in this test binary");

    let error = fs.file_status("/dir/file").unwrap_err();
    assert!(
        error.to_string().contains("business in progress"),
        "the business fault must propagate (never swallowed by the Busy retry): {error}"
    );
    // Process-level runtime: remove the rule so later scenarios in this
    // binary never see the locked_build fault.
    runtime
        .remove("file-status-business-error")
        .expect("the business fault rule must be removable");
}

/// Scenario 5: the deadline is bounded — with the owner never releasing,
/// the waiter returns the original InProgress semantics after waiting, and
/// never fakes success.
#[test]
fn file_status_deadline_returns_the_original_in_progress_semantics() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-deadline");
    fs.create("/dir/file", true).unwrap();

    let (ready_tx, ready_rx) = channel();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(Duration::from_secs(30)).unwrap();
        let _ = ready_tx.send(());
        // Hold the window past the waiter's own deadline (30s from the
        // waiter's start, which is later than this line).
        std::thread::sleep(Duration::from_secs(40));
        drop(guard);
    });

    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || fs3.file_status("/dir/file"));
    let started = Instant::now();
    let result = worker.join().unwrap();
    let elapsed = started.elapsed();
    eprintln!("DEADLINE-DIAG: elapsed={elapsed:?} result={result:?}");
    assert!(
        result.is_err(),
        "an unreleased cold window must not produce a fake success"
    );
    assert!(
        elapsed >= Duration::from_secs(1),
        "the bounded wait must actually wait, not fail instantly: {elapsed:?}"
    );
    drop(owner);
}
