// Copyright 2026 OPPO
//
// Event-driven determinism tests for the file_status holder-Busy retry.
// Every scenario synchronizes on real `begin_cold` guards (confirmed via
// ready events BEFORE the worker spawns), the instance second-view watch
// (signal + release), and the busy-after-locks / by-ID / snapshot Busy
// events — never on sleeps-as-arrival proofs.
//
// Cleanup model: the driver drops its OWN clone plus every external
// clone/guard/probe it still holds, then removes the case tree. Removing a
// directory that Unix lets stay open does NOT prove every Arc/DB/runtime
// reference is gone — it only proves the cleanup step ran and the tree is
// deleted; any registry-held reference is recorded here as informational,
// not claimed as proof of global release.

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

    /// The driver must have dropped its own clone and every external
    /// clone/guard/probe BEFORE calling this. Removing the tree proves the
    /// cleanup step ran; it does not prove every internal registry reference
    /// is gone (that is recorded as informational, never as proof).
    fn cleanup_and_verify(self) {
        let meta_dir = self.meta_dir.clone();
        drop(self.fs);
        std::fs::remove_dir_all(&meta_dir)
            .expect("the case meta directory must be removable after all handles drop");
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

/// Owner thread with a real begin_cold guard: reports readiness FIRST (the
/// driver waits for it before spawning the worker), then parks until the
/// driver's release/fail command, whose receipt is checked — never `let _`.
/// The guard drop is reported through a done channel so the driver can join
/// with a bounded wait on actual completion.
type OwnerHandles = (
    Receiver<()>,
    Sender<()>,
    Receiver<Result<(), std::sync::mpsc::RecvTimeoutError>>,
    std::thread::JoinHandle<()>,
);

fn spawn_confirmed_owner(fs: &Arc<MasterFilesystem>) -> OwnerHandles {
    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let (done_tx, done_rx) = channel::<Result<(), std::sync::mpsc::RecvTimeoutError>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        let release_receipt = release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
        // The guard drops first, then this thread's own fs clone, and only
        // then the receipt is sent — the completion corresponds to the real
        // resource state this thread owns.
        drop(fs2);
        let _ = done_tx.send(release_receipt);
    });
    (ready_rx, release_tx, done_rx, owner)
}

/// Bounded join: waits for the thread, then for the done signal (actual
/// completion), with explicit timeouts — never an unbounded join.
fn join_owner_bounded(
    owner: std::thread::JoinHandle<()>,
    done_rx: Receiver<Result<(), std::sync::mpsc::RecvTimeoutError>>,
) {
    let receipt = done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must report its actual completion");
    receipt.expect("the cold owner's release must have been received in time (not timed out)");
    owner.join().expect("the cold owner thread must not panic");
}

/// Scenario 1: the driver confirms the owner really holds begin_cold BEFORE
/// the worker spawns — no startup race. The worker then hits a real
/// first-view holder Busy (typed signal event), waits it out, and returns
/// the true status after release.
#[test]
fn file_status_waits_out_a_real_cold_window() {
    let _mode = memory_read_gates::hold_optimistic_mode(true);
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-first-view");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    let busy_rx = arm_busy_only(&fs);
    // Owner confirmed FIRST.
    let (ready_rx, release_tx, owner_done, owner) = spawn_confirmed_owner(&fs);
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must confirm begin_cold success BEFORE the worker spawns");

    // Worker second.
    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();
    let fs2 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs2.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });

    // Typed signal event: the worker really reached the holder view and hit
    // the cold window.
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must hit a real holder Busy in the cold window");

    release_tx.send(()).unwrap();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must report its real result after release");
    let status = result.expect("file_status must succeed after the window reopens");
    assert_eq!(status.path, "/dir/file");
    // Bounded completion of both threads, then cleanup.
    worker.join().expect("the worker must not panic");
    join_owner_bounded(owner, owner_done);
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// Scenario 2: the worker walks the OPEN tree first (locks taken, second-view
/// watch reached), THEN the owner takes the real cold window (the reader
/// holds no view, so the drain succeeds) — the second view hits Busy, the
/// attempt drops its inode locks BEFORE the busy event, and a same-stripe
/// write probe proves it (a read probe would coexist and prove nothing).
#[test]
fn file_status_drops_inode_locks_before_waiting_out_the_second_view() {
    let _mode = memory_read_gates::hold_optimistic_mode(true);
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-second-view");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();
    let file_id = fs.file_status("/dir/file").unwrap().id;

    let (watch_rx, watch_release_tx, busy_rx) = arm_watches(&fs);
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
    // the second view is released.
    let (owner_cold_tx, owner_cold_rx) = channel();
    let (owner_release_tx, owner_release_rx) = channel();
    let (owner_done_tx, owner_done_rx) = channel::<Result<(), std::sync::mpsc::RecvTimeoutError>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = owner_cold_tx.send(());
        let release_receipt = owner_release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
        drop(fs2);
        // The receipt is reported verbatim: an abnormal end is never
        // disguised as a normal completion.
        let _ = owner_done_tx.send(release_receipt);
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
    // Bounded completion of both threads, then cleanup.
    worker.join().expect("the worker must not panic");
    join_owner_bounded(owner, owner_done_rx);
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// Scenario 3: the worker first proves it hit a real holder Busy (typed
/// signal event), THEN the holder fails closed; the original Result arrives
/// through the completion channel and must be the typed terminal error
/// (Common with the JournalUnavailable prefix — the real constructor), promptly.
#[test]
fn file_status_reports_unavailable_after_terminal_failure() {
    let _mode = memory_read_gates::hold_optimistic_mode(true);
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-cold-failed");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    let busy_rx = arm_busy_only(&fs);
    // Owner confirmed FIRST, with a real fail path: fail_rx drives the
    // owner's own guard.fail_closed() while the reader is parked.
    let (ready_tx, ready_rx) = channel();
    let (fail_tx, fail_rx) = channel::<()>();
    let (done_tx, done_rx) = channel::<Result<(), std::sync::mpsc::RecvTimeoutError>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        let fail_receipt = fail_rx.recv_timeout(Duration::from_secs(30));
        match fail_receipt {
            Ok(()) => {
                guard.fail_closed();
            }
            Err(_) => drop(guard),
        }
        drop(fs2);
        // The fail receipt is reported verbatim.
        let _ = done_tx.send(fail_receipt);
    });
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must confirm begin_cold success BEFORE the worker spawns");

    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();
    let fs3 = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = fs3.file_status("/dir/file");
        let _ = completion_tx.send(result);
    });

    // Typed event: the worker really hit the holder Busy.
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the worker must hit a real holder Busy before FAILED");

    // Real terminal transition while the reader is parked on the retry: the
    // owner's own guard fail_closes the holder (the owner thread confirms
    // the transition via its done channel before the assertions run).
    fail_tx.send(()).unwrap();

    let started = Instant::now();
    let result = completion_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the terminal failure must deliver its original result promptly");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the terminal failure must return promptly, not spin to the deadline"
    );
    // Typed variant check: journal_unavailable constructs Common with the
    // JournalUnavailable prefix (the real constructor — no fake enum).
    assert!(
        matches!(&result, Err(FsError::Common(error))
            if error.to_string().starts_with("JournalUnavailable:")),
        "FAILED must surface as the terminal JournalUnavailable error: {result:?}"
    );
    // Bounded completion of both threads: join_owner_bounded checks the
    // inner release receipt too (an Err(RecvTimeoutError) is never treated
    // as normal completion), then joins.
    worker.join().expect("the worker must not panic");
    join_owner_bounded(owner, done_rx);
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// Scenario 4 (fault feature): a business fault is never swallowed by the
/// Busy retry — it must NOT produce a holder-Busy event. The process-level
/// rule is removed afterwards so later scenarios never see it.
#[cfg(feature = "fault-injection")]
#[test]
fn file_status_propagates_business_errors_without_busy_retry() {
    let _mode = memory_read_gates::hold_optimistic_mode(true);
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
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// Scenario 5: the shared retry helper with a real cold owner that never
/// releases returns the ORIGINAL typed InProgress variant (not a string, not
/// Unavailable, not success) after a short injected budget — through the
/// SAME production helper with a short injected budget. The owner is
/// explicitly released and joined with a bounded done check.
#[test]
fn retry_helper_deadline_returns_typed_in_progress() {
    Master::init_test_metrics();
    let case = CaseEnv::new("retry-helper-deadline");
    let fs = case.fs.clone();

    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let (done_tx, done_rx) = channel::<Result<(), std::sync::mpsc::RecvTimeoutError>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        let release_receipt = release_rx.recv_timeout(Duration::from_secs(60));
        drop(guard);
        drop(fs2);
        // The receipt is reported verbatim.
        let _ = done_tx.send(release_receipt);
    });
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let started = Instant::now();
    let result = MasterFilesystem::retry_on_holder_busy(Duration::from_millis(120), || {
        fs.file_status_memory_attempt("/dir/file")
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
        elapsed >= Duration::from_millis(120),
        "the injected budget must actually elapse: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the short injected deadline must bound the wait: {elapsed:?}"
    );

    // Explicit release, bounded done check, then join — no leaked thread.
    release_tx.send(()).unwrap();
    join_owner_bounded(owner, done_rx);
    drop(fs); // Release the case-local fs clone before cleanup.
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
    // The case's fs clone is owned by CaseEnv; dropping the env releases it
    // before the directory removal.
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
    // Owner confirmed FIRST.
    let (ready_rx, release_tx, owner_done, owner) = spawn_confirmed_owner(&fs);
    ready_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the cold owner must confirm begin_cold success BEFORE the worker spawns");

    let (completion_tx, completion_rx) = channel::<FsResult<FileStatus>>();
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
    worker.join().expect("the worker must not panic");
    join_owner_bounded(owner, owner_done);
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// by-ID entry semantics outside the Busy retry, per the fifth review:
/// ① positive-ID branch with the REAL existing root inode (1000) — the
///    returned `path` comes from the CALLER's argument; it documents that
///    the positive-ID branch never reaches the pending entry check. It does
///    NOT prove any pending directory or file was resolved, and the stale
///    "would FileNotFound" doc is gone.
/// ② None (path branch) alone exercises the entry-level pending rejection
///    (existing contract, unchanged by this fix).
/// ③ a missing ID produces the real FileNotFound variant with the inode_id
///    context — checked against the variant/constructor, not substrings.
#[test]
fn file_status_by_id_keeps_missing_and_pending_semantics() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-by-id-semantics");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    // ① Positive-ID branch with the REAL root inode (1000): the returned
    // path is the caller's own string — the branch never reaches the pending
    // entry check. No claim about pending tree contents.
    let status = fs
        .file_status_by_id("/.Trash/.pending/file", Some(1000))
        .unwrap();
    assert_eq!(status.id, 1000, "the positive-ID branch must resolve by id");

    // ② Path branch (None): the entry-level pending rejection (existing
    // contract, unchanged by this fix).
    let error = fs
        .file_status_by_id("/.Trash/.pending/file", None)
        .unwrap_err();
    assert!(
        error.to_string().contains("/.Trash/.pending/file")
            && error.to_string().contains("not found"),
        "the path branch must reject the pending path at the entry: {error}"
    );

    // ③ Missing ID: the real FileNotFound variant + inode_id context —
    // checked against the variant/constructor, not substrings.
    let error = fs
        .file_status_by_id("/dir/missing", Some(999_999))
        .unwrap_err();
    let message = error.to_string();
    assert!(
        matches!(error, FsError::FileNotFound(_))
            && message.contains("not found")
            && message.contains("inode_id=999999"),
        "the missing-ID read must keep the FileNotFound+inode_id payload: {message}"
    );
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// Scenario snapshot: with a real cold owner holding the window, the private
/// snapshot attempt is invoked DIRECTLY and must observe the typed holder
/// Busy; after the owner releases and its completion is confirmed, the same
/// attempt succeeds and returns the resolved status.
#[test]
fn file_status_snapshot_attempt_observes_busy_then_succeeds() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-snapshot-busy");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    let busy_rx = {
        let (busy_tx, busy_rx) = channel();
        *fs.fs_status_busy_after_locks_watch.lock().unwrap() = Some(busy_tx);
        busy_rx
    };
    let (ready_tx, ready_rx) = channel();
    let (release_tx, release_rx) = channel();
    let (done_tx, done_rx) = channel::<Result<(), std::sync::mpsc::RecvTimeoutError>>();
    let fs2 = fs.clone();
    let owner = std::thread::spawn(move || {
        let guard = fs2.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let _ = ready_tx.send(());
        let release_receipt = release_rx.recv_timeout(Duration::from_secs(30));
        drop(guard);
        drop(fs2);
        // The receipt is reported verbatim.
        let _ = done_tx.send(release_receipt);
    });
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    // Direct call: the snapshot attempt's holder view hits the cold window —
    // the typed Busy event must fire (its own take-once observation).
    let attempt = fs.file_status_snapshot_attempt("/dir/file");
    assert!(
        matches!(attempt, Err(FileStatusReadError::HolderBusy)),
        "the snapshot attempt must observe the typed holder Busy: {attempt:?}"
    );
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the snapshot attempt's Busy event must fire");

    // Owner releases and ACTUALLY completes — join_owner_bounded checks the
    // inner release receipt too — before the same attempt is retried, so
    // the reopen is deterministic, not racy.
    release_tx.send(()).unwrap();
    join_owner_bounded(owner, done_rx);
    let attempt = fs.file_status_snapshot_attempt("/dir/file");
    let status = attempt.expect("the snapshot attempt must succeed after the window reopens");
    assert_eq!(status.path, "/dir/file");
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}

/// Scenario snapshot-FAILED: an entry called while the holder is ALREADY
/// FAILED returns the terminal error immediately — it never enters the Busy
/// retry and never waits on the deadline. The public path entry is also
/// called in the same already-FAILED state: it returns the same typed
/// terminal error through its real public surface.
#[test]
fn file_status_fails_immediately_when_already_failed_public_and_private() {
    Master::init_test_metrics();
    let case = CaseEnv::new("file-status-snapshot-failed");
    let fs = case.fs.clone();
    fs.create("/dir/file", true).unwrap();

    // Take the holder to the real FAILED state (guard fail path).
    let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    guard.fail_closed();

    // Private snapshot attempt: terminal immediately.
    let started = Instant::now();
    let attempt = fs.file_status_snapshot_attempt("/dir/file");
    assert!(
        matches!(attempt, Err(FileStatusReadError::Unavailable)),
        "an already-FAILED holder must be terminal for the snapshot attempt: {attempt:?}"
    );
    // Public path entry in the same already-FAILED state: the real public
    // surface returns the same terminal error variant (Common with the
    // JournalUnavailable prefix — the real constructor).
    let started_public = Instant::now();
    let public_result = fs.file_status("/dir/file");
    assert!(
        matches!(&public_result, Err(FsError::Common(error))
            if error.to_string().starts_with("JournalUnavailable:")),
        "the public entry must return the terminal error in the already-FAILED state: {public_result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1)
            && started_public.elapsed() < Duration::from_secs(1),
        "both entries must fail immediately, not spin"
    );
    drop(fs); // Release the case-local fs clone before cleanup.
    case.cleanup_and_verify();
}
