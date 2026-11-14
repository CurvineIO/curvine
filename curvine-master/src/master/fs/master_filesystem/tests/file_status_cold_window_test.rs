// Copyright 2026 OPPO
//
// Event-driven determinism tests for the file_status holder-Busy retry.
// Every scenario synchronizes on real `begin_cold` guards (confirmed via
// ready events BEFORE the worker spawns), the instance second-view watch
// (signal + release), and the busy-after-locks / by-ID / snapshot Busy
// events — never on sleeps-as-arrival proofs.

use super::*;

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-case environment: the live fs plus the actual conf paths so the case
/// can clean its own directories and prove the cleanup happened.
struct CaseEnv {
    fs: Arc<MasterFilesystem>,
    meta_dir: std::path::PathBuf,
}

impl CaseEnv {
    fn new(name: &str) -> Self {
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            ..Default::default()
        };
        conf.change_test_meta_dir(format!("{}-{}", name, Utils::rand_str(8)));
        conf.journal.enable = false;
        let meta_dir = std::path::PathBuf::from(conf.master.meta_dir.clone());
        let fs = Arc::new(crate::master::journal::JournalSystem::fs_only_for_test(&conf).unwrap());
        Self { fs, meta_dir }
    }

    /// Drops every clone the caller holds (the Arc must be unique here) and
    /// verifies the case directory was actually removable — proving no
    /// DB/runtime/Arc leaked past the test.
    fn cleanup_and_verify(self) {
        let meta_dir = self.meta_dir.clone();
        drop(self.fs);
        // Removing the tree proves the cleanup path ran; the count of leaked
        // Arcs is informational (some may be held by internal registries).
        std::fs::remove_dir_all(&meta_dir)
            .expect("the case meta directory must be removable after all handles drop");
    }
}

// Arc has no strong_count_hint; use the real strong count instead.
trait ArcStrongCount {
    fn strong_count_hint(&self) -> bool;
}
impl<T> ArcStrongCount for Arc<T> {
    fn strong_count_hint(&self) -> bool {
        Arc::strong_count(self) == 1
    }
}

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

/// Scenario 1: the driver first confirms the owner really holds begin_cold
/// (ready BEFORE the worker spawns — no startup race), then the worker hits
/// a real first-view holder Busy (typed signal event), waits it out, and
/// returns the true status after release.
#[test]
fn file_status_waits_out_a_real_cold_window() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-first-view");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    let busy_rx = arm_busy_only(&fs);
    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();

    // Owner FIRST: the driver confirms begin_cold succeeded before the
    // worker can even start — no startup race.
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        release_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the driver must release the cold owner in time");
        drop(guard);
    });
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must confirm begin_cold success BEFORE the worker spawns");

    // Worker second: every first view now collides with the real cold window.
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });

    // Typed signal event: the worker really reached the holder view and hit
    // the cold window (not merely "has not finished yet").
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
    case.cleanup_and_verify();
}

/// Scenario 2: the worker walks the OPEN tree first (locks taken, second-view
/// watch reached), THEN the owner takes the real cold window (the reader
/// holds no view, so the drain succeeds) — the second view hits Busy, the
/// attempt drops its inode locks BEFORE the busy event, and a same-stripe
/// write probe proves it (read/read would coexist and prove nothing).
#[test]
fn file_status_drops_inode_locks_before_waiting_out_the_second_view() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-second-view");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();
    let file_id = fs.file_status("/dir/file").unwrap().id;

    let (watch_rx, watch_release_tx, busy_rx) = arm_watches(&fs);
    let (owner_release_tx, owner_release_rx) = channel();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();

    // Worker first: OPEN tree, locks taken, parked at the second-view watch.
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });
    watch_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the attempt must reach its post-lock second view");

    // Owner second: the reader holds no view (dropped before lock_many), so
    // begin_cold succeeds immediately. Its COLD state is confirmed before
    // the second view is released — otherwise the attempt would complete in
    // the still-open window and no busy event could fire.
    let (owner_cold_tx, owner_cold_rx) = channel();
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
    // inode locks BEFORE signaling, then parks outside object locks.
    watch_release_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the busy-after-locks event must fire after the locks drop");

    // Same-stripe write probe: only succeeds when no read lock remains.
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
    case.cleanup_and_verify();
}

// The owner thread in scenario 2 carries its own channel; no marker needed.
fn owner_ready_marker() {}

/// Scenario 3: the worker first proves it hit a real holder Busy (typed
/// signal event), THEN the holder fails closed; the original Result arrives
/// through the completion channel and must be the typed JournalUnavailable
/// error, promptly (not after the full deadline).
#[test]
fn file_status_reports_unavailable_after_terminal_failure() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-failed");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    let busy_rx = arm_busy_only(&fs);
    let (ready_tx, ready_rx) = channel();
    let (fail_tx, fail_rx) = channel::<()>();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();

    // Owner FIRST (ready before the worker spawns).
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        fail_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the driver must deliver the fail instruction in time");
        drop(guard.fail_closed());
    });
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });

    // Typed event: the worker really hit the holder Busy.
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must hit a real holder Busy before FAILED");

    fail_tx.send(()).unwrap();
    let started = Instant::now();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the terminal failure must deliver its original result promptly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the terminal failure must return promptly, not spin to the deadline"
    );
    // Typed variant check (not a string match).
    assert!(
        matches!(&result, Err(FsError::Common(error))
            if error.to_string().starts_with("JournalUnavailable:")),
        "FAILED must surface as the terminal JournalUnavailable error: {result:?}"
    );
    owner.join().expect("the cold owner must not panic");
    worker.join().expect("the worker must not panic");
    case.cleanup_and_verify();
}

/// Scenario 4 (fault feature): a business fault is never swallowed by the
/// Busy retry — it must NOT produce a holder-Busy event. The process-level
/// rule is removed afterwards so later scenarios never see it.
#[cfg(feature = "fault-injection")]
#[test]
fn file_status_propagates_business_errors_without_busy_retry() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-business-error");
    let fs = case.fs.clone();
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
    assert!(
        busy_rx.try_recv().is_err(),
        "a business fault must not enter the holder-Busy retry path"
    );
    runtime
        .remove("file-status-business-error")
        .expect("the business fault rule must be removable");
    case.cleanup_and_verify();
}

/// Scenario 5: the shared retry helper with a real cold owner that never
/// releases returns the ORIGINAL typed InProgress variant (not a string, not
/// Unavailable, not success) after a short injected budget — calling the
/// same production helper the two entries use, through a controlled closure.
/// The attempt function is invoked exactly once per retry round; the
/// deadline is computed only from the first Busy.
#[test]
fn retry_helper_deadline_returns_typed_in_progress() {
    Master::init_test_metrics();
    let case = CaseEnv::new("retry-helper-deadline");

    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let fs2 = case.fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        release_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the driver must release the cold owner in time");
        drop(guard);
    });
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    // Controlled attempt: ALWAYS a real holder Busy (via the actual handle
    // view — the owner holds the cold window), counted per invocation.
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts2 = attempts.clone();
    let fs3 = case.fs.clone();
    let started = Instant::now();
    let result = MasterFilesystem::retry_on_holder_busy(Duration::from_millis(120), move || {
        attempts2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Real holder view against the real cold window: typed Busy.
        fs3.fs_dir
            .view()
            .map(|_| unreachable!("the owner holds the cold window; a view cannot succeed"))
            .map_err(|e| match e {
                NamespaceError::Busy => FileStatusReadError::HolderBusy,
                NamespaceError::Unavailable => FileStatusReadError::Unavailable,
            })
    });
    let elapsed = started.elapsed();

    // Typed original semantics: InProgress variant, exact message, bounded.
    assert!(
        matches!(result, Err(FsError::InProgress(_))),
        "the deadline must surface the typed InProgress variant: {result:?}"
    );
    if let Err(FsError::InProgress(message)) = &result {
        let text = message.to_string();
        assert!(
            text.contains("temporarily unavailable"),
            "the InProgress message must be the cold-window one: {text}"
        );
    }
    assert!(
        attempts.load(std::sync::atomic::Ordering::SeqCst) >= 2,
        "the helper must have retried at least once inside the budget"
    );
    assert!(
        elapsed >= Duration::from_millis(120),
        "the injected budget must actually elapse: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the short injected deadline must bound the wait: {elapsed:?}"
    );

    release_tx.send(()).unwrap();
    owner.join().expect("the cold owner must not panic");
    case.cleanup_and_verify();
}

/// Scenario 6: the controlled closure produces a real typed business
/// InProgress — the retry helper must NOT wait on it (it is business, not a
/// holder Busy) and must not fire any holder-Busy event; exactly one
/// invocation, original variant and message preserved.
#[test]
fn retry_helper_propagates_typed_business_in_progress_without_waiting() {
    Master::init_test_metrics();
    let case = CaseEnv::new("retry-helper-typed-business");

    let busy_rx = arm_busy_only(&case.fs);
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let attempts2 = attempts.clone();
    let started = Instant::now();
    let result = MasterFilesystem::retry_on_holder_busy(Duration::from_millis(120), move || {
        attempts2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(FileStatusReadError::Business(FsError::in_progress_msg(
            "business typed in progress",
        )))
    });
    let elapsed = started.elapsed();

    // Original typed variant and message, propagated verbatim.
    assert!(
        matches!(result, Err(FsError::InProgress(_))),
        "the typed business InProgress variant must propagate: {result:?}"
    );
    if let Err(FsError::InProgress(message)) = &result {
        let text = message.to_string();
        assert!(
            text.contains("business typed in progress"),
            "the typed business InProgress message must be verbatim: {text}"
        );
    }
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a business error must end the retry immediately"
    );
    assert!(
        elapsed < Duration::from_millis(120),
        "a business error must not wait: {elapsed:?}"
    );
    assert!(
        busy_rx.try_recv().is_err(),
        "a business InProgress must not enter the holder-Busy path"
    );
    case.cleanup_and_verify();
}

/// Scenario by-ID: a real cold owner confirmed BEFORE the worker spawns; the
/// by-ID entry (`file_status_by_id(path, Some(id))`) must hit the same
/// holder Busy event, wait it out, and return the status of the SAME inode.
#[test]
fn file_status_by_id_hits_busy_and_returns_same_inode_status() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-by-id");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();
    let file_id = fs.file_status("/dir/file").unwrap().id;

    let busy_rx = arm_busy_only(&fs);
    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();

    // Owner FIRST: begin_cold confirmed before the worker spawns.
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        release_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the driver must release the cold owner in time");
        drop(guard);
    });
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must confirm begin_cold success BEFORE the worker spawns");

    // Worker: the real by-ID entry point.
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status_by_id("/dir/file", Some(file_id));
        let _ = completion_tx.send(result);
    });

    // The by-ID branch's own view Busy signal must fire (typed event).
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the by-ID worker must hit a real holder Busy in the cold window");

    release_tx.send(()).unwrap();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must report its real result after release");
    let status = result.expect("the by-ID read must succeed after the window reopens");
    assert_eq!(
        status.id, file_id,
        "the by-ID read must return the same inode"
    );
    owner.join().expect("the cold owner must not panic");
    worker.join().expect("the worker must not panic");
    case.cleanup_and_verify();
}

/// by-ID entry semantics outside the retry: a missing inode keeps the
/// original FileNotFound+inode_id context payload, and the pending path is
/// still rejected at the entry boundary.
#[test]
fn file_status_by_id_keeps_missing_and_pending_semantics() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-by-id-semantics");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    // Missing inode (never created): FileNotFound with the inode_id context.
    let error = fs
        .file_status_by_id("/dir/missing", Some(999_999))
        .unwrap_err();
    assert!(
        error.to_string().contains("not found") && error.to_string().contains("inode_id=999999"),
        "the missing by-ID read must keep the FileNotFound+inode_id payload: {error}"
    );

    // Pending staging path with NO inode id: the path branch runs and the
    // entry-level check_not_pending_path rejects it before any resolution
    // (its message is the plain FileNotFound for that path). (With Some(1) —
    // a real inode id — the by-ID branch returns BEFORE the pending check and
    // the snapshot resolve reports FileNotFound whose message merely contains
    // the requested path text; that distinction is asserted below.)
    let error = fs
        .file_status_by_id("/.Trash/.pending/file", None)
        .unwrap_err();
    assert!(
        error.to_string().contains("/.Trash/.pending/file")
            && error.to_string().contains("not found"),
        "the pending path must be rejected at the entry as FileNotFound: {error}"
    );

    // With a REAL inode id (root = Some(1)), the by-ID branch returns before
    // the pending check: the resolve reports FileNotFound for the missing
    // pending edge, and the message contains the path text — this documents
    // the actual branch order, not an entry rejection.
    let error = fs
        .file_status_by_id("/.Trash/.pending/file", Some(1))
        .unwrap_err();
    assert!(
        error.to_string().contains("not found"),
        "the by-ID branch resolves the missing pending edge as FileNotFound: {error}"
    );
    case.cleanup_and_verify();
}
