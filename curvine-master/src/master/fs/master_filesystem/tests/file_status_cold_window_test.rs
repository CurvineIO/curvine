// Copyright 2026 OPPO
//
// Event-driven determinism tests for the file_status holder-Busy retry.
// Every scenario synchronizes on real `begin_cold` guards, the instance
// second-view watch (signal + release), and the busy-after-locks watch —
// never on sleeps-as-arrival proofs.

use super::*;

use curvine_model::FileLock;
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

/// Arms both instance watch points. `second_view` fires (take-once) right
/// before the second holder view inside the locked attempt — the worker then
/// parks until the driver releases. `busy` fires (signal-only, take-once)
/// right after a holder view failed with Busy and every inode lock was
/// explicitly dropped — proving no lock crosses the wait.
fn arm_watches(fs: &Arc<MasterFilesystem>) -> (Receiver<()>, Sender<()>, Receiver<()>) {
    let (sv_tx, sv_rx) = channel();
    let (sv_rel_tx, sv_rel_rx) = channel();
    *fs.fs_status_second_view_watch.lock().unwrap() = Some((sv_tx, sv_rel_rx));
    let (busy_tx, busy_rx) = channel();
    *fs.fs_status_busy_after_locks_watch.lock().unwrap() = Some(busy_tx);
    (sv_rx, sv_rel_tx, busy_rx)
}

fn arm_busy_only(fs: &Arc<MasterFilesystem>) -> Receiver<()> {
    let (busy_tx, busy_rx) = channel();
    *fs.fs_status_busy_after_locks_watch.lock().unwrap() = Some(busy_tx);
    busy_rx
}

/// Scenario 1: a real cold window during the first holder view — file_status
/// must wait (HolderBusy observed) and return the true status after the
/// owner releases.
#[test]
fn file_status_waits_out_a_real_cold_window() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-first-view");
    fs.create("/dir/file", true).unwrap();

    let busy_rx = arm_busy_only(&fs);
    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
    });
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });

    // The owner must hold the window first, then the worker must hit a real
    // holder Busy (the typed signal-only event proves it reached the holder
    // view — it is not merely unscheduled).
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must hit a real holder Busy in the cold window");

    release_tx.send(()).unwrap();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must report its real result after release");
    let status = result.expect("file_status must succeed after the window reopens");
    assert_eq!(status.path, "/dir/file");
    owner.join().expect("the cold owner must not panic");
    worker.join().expect("the worker must not panic");
}

/// Scenario 2: the attempt acquires its inode locks, reaches the second-view
/// watch, then the second holder view hits the cold window — the busy event
/// must fire AFTER the inode locks are explicitly dropped, and a write probe
/// on the same stripe must succeed (a read probe would coexist and prove
/// nothing).
#[test]
fn file_status_drops_inode_locks_before_waiting_out_the_second_view() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-second-view");
    fs.create("/dir/file", true).unwrap();
    let file_id = fs.file_status("/dir/file").unwrap().id;

    let (watch_rx, watch_release_tx, busy_rx) = arm_watches(&fs);
    let (owner_release_tx, owner_release_rx) = channel();
    let (owner_cold_tx, owner_cold_rx) = channel();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();

    // Worker first: it walks the OPEN live tree, takes the inode locks, and
    // parks at the second-view watch (its planning view is already dropped —
    // exactly what a begin_cold drain needs to succeed).
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });
    // The attempt reached its post-lock second view (holding I, no view).
    watch_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the attempt must reach its post-lock second view");

    // Only now does the owner begin_cold: the reader holds no view, so the
    // drain succeeds immediately; the reader's inode locks do NOT block it.
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = owner_cold_tx.send(());
        let _ = owner_release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
    });
    owner_cold_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must confirm begin_cold success");

    // Release the second view: it hits the cold Busy, the attempt drops its
    // inode locks BEFORE signaling, then returns HolderBusy and the shared
    // retry parks outside object locks.
    watch_release_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the busy-after-locks event must fire after the locks drop");

    // Write probe on the SAME stripe: a write lock can only be taken when no
    // read lock remains — the parked reader must not hold it.
    let probe = fs
        .inode_locks
        .try_write_inode(file_id)
        .expect("the parked reader must not hold the inode stripe (write probe)");
    drop(probe);

    owner_release_tx.send(()).unwrap();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must report its real result after release");
    let status = result.expect("file_status must succeed once the cold owner releases");
    assert_eq!(status.path, "/dir/file");
    owner.join().expect("the cold owner must not panic");
    worker.join().expect("the worker must not panic");
}

/// Scenario 3: FAILED is terminal — the worker first proves it hit a real
/// holder Busy, then the holder fails closed; the original Result arrives
/// through the completion channel with the typed JournalUnavailable error,
/// promptly (not after the full deadline).
#[test]
fn file_status_reports_unavailable_after_terminal_failure() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-failed");
    fs.create("/dir/file", true).unwrap();

    let busy_rx = arm_busy_only(&fs);
    let (ready_tx, ready_rx) = channel();
    let (fail_tx, fail_rx) = channel::<()>();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        let _ = fail_rx.recv_timeout(Duration::from_secs(30));
        drop(guard.fail_closed());
    });
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });

    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    // First prove the worker really hit a holder Busy in the cold window.
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must hit a real holder Busy before FAILED");

    // Then the holder fails closed while the reader is parked on the retry.
    fail_tx.send(()).unwrap();
    let started = Instant::now();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the terminal failure must deliver its original result promptly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the terminal failure must return promptly, not spin to the deadline"
    );
    let error = result.unwrap_err();
    assert!(
        error.to_string().contains("permanently unavailable"),
        "FAILED must surface as the terminal Unavailable error: {error}"
    );
    owner.join().expect("the cold owner must not panic");
    worker.join().expect("the worker must not panic");
}

/// Scenario 4 (fault feature): a business error is never swallowed by the
/// Busy retry — the locked_build fault returns before the status is built
/// and must propagate instead of triggering a holder-Busy retry. The
/// process-level rule is removed afterwards so later scenarios never see it.
#[cfg(feature = "fault-injection")]
#[test]
fn file_status_propagates_business_errors_without_busy_retry() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    Master::init_test_metrics();
    let fs = env("file-status-business-error");
    fs.create("/dir/file", true).unwrap();

    let runtime = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.file_status.locked_build")
        .return_error("business in progress")
        .unwrap();
    runtime
        .configure("file-status-business-error", rule)
        .expect("the fault point must be registered in this test binary");

    let busy_rx = arm_busy_only(&fs);
    let error = fs.file_status("/dir/file").unwrap_err();
    assert!(
        error.to_string().contains("business in progress"),
        "the business fault must propagate: {error}"
    );
    // The business InProgress must NOT have been treated as a holder Busy:
    // no holder-Busy event may have fired for this call.
    assert!(
        busy_rx.try_recv().is_err(),
        "a business fault must not enter the holder-Busy retry path"
    );
    runtime
        .remove("file-status-business-error")
        .expect("the business fault rule must be removable");
}

/// Scenario 5: the deadline is bounded and typed — with a real cold owner
/// that never releases, the shared retry helper returns the original typed
/// InProgress semantics (not a fake success, not Unavailable) after waiting,
/// through the SAME production helper with a short injected budget.
#[test]
fn file_status_deadline_returns_the_original_in_progress_semantics() {
    Master::init_test_metrics();
    let fs = env("file-status-cold-deadline");
    fs.create("/dir/file", true).unwrap();
    let file_id = fs.file_status("/dir/file").unwrap().id;

    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(Duration::from_secs(60)).unwrap();
        let _ = ready_tx.send(());
        // Hold the window far past the injected short deadline; the driver
        // releases it explicitly after the assertions.
        let _ = release_rx.recv_timeout(Duration::from_secs(60));
        drop(guard);
    });

    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let started = Instant::now();
    let result = MasterFilesystem::retry_on_holder_busy(Duration::from_millis(300), || {
        fs.file_status_memory_attempt("/dir/file")
    });
    let elapsed = started.elapsed();
    // Typed original semantics: InProgress, not Unavailable, not success.
    match &result {
        Err(error) => {
            assert!(
                error.to_string().contains("temporarily unavailable"),
                "the deadline must surface the original InProgress semantics: {error}"
            );
        }
        Ok(_) => panic!("an unreleased cold window must not produce a fake success"),
    }
    assert!(
        elapsed >= Duration::from_millis(300),
        "the bounded wait must actually wait: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the injected short deadline must bound the wait: {elapsed:?}"
    );
    let _ = file_id;
    // Release the owner and join both threads so nothing leaks past the test.
    release_tx.send(()).unwrap();
    owner.join().expect("the cold owner must not panic");
}
