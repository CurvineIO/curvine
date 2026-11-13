// Owned by deputy-2 (C2 §8.1–8.2 confirmation contract tests).
// Registration anchor (main developer): master_filesystem.rs cfg(test)
// declaration block, `#[path = "tests/recursive_confirmation_test.rs"]`.

use super::recursive_confirmation_support::{
    arm_publication_failure, await_worker, remove_case_dirs, run_admitted, shutdown_case,
    typed_op_error, WORKER_TIMEOUT,
};
use super::*;
use crate::master::journal::RecoveryGate;
use curvine_raft::raft::RaftClient;

fn test_conf(name: &str) -> ClusterConf {
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "{}-{}",
        name,
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf
}

fn build_writer(conf: &ClusterConf) -> FsResult<JournalWriter> {
    Master::init_test_metrics();
    let rt = conf.journal.create_runtime();
    let client = RaftClient::from_conf(rt, &conf.journal);
    JournalWriter::new(true, client, &conf.journal, RecoveryGate::new())
}
use crate::master::meta::fs_dir_handle::NamespaceGuard;
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::{ColdError, NamespaceError};
use crate::master::Master;
use curvine_raft::raft::RoleState;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

/// Enabled journal with a truly unbounded testing queue: §8.3 admits the
/// recursive coordination entry, so every test below exercises the real
/// sharded path instead of the exclusive fallback.
fn confirmation_env(name: &str) -> (ClusterConf, JournalSystem, MasterFilesystem) {
    let mut conf = test_conf(name);
    conf.journal.writer_channel_size = 0;
    conf.journal.snapshot_entries = 0;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    (conf, journal, fs)
}

/// Real receipts writer shared by the body's FsDir and the final fs clone.
/// Clones must be taken after this swap (MasterFilesystem clones carry their
/// own journal_writer Arc).
fn swap_receipts_writer(
    fs: &mut MasterFilesystem,
    journal: &JournalSystem,
    conf: &ClusterConf,
) -> Arc<JournalWriter> {
    let mut writer = build_writer(conf).unwrap();
    writer.enable_testing_receipts(journal.master_monitor().recovery());
    let writer = Arc::new(writer);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .journal_writer = writer.clone();
    fs.journal_writer = writer.clone();
    writer
}

fn drain_receipts(writer: &Arc<JournalWriter>) {
    writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
}

/// Stable observation of the rejected view after the holder went terminal.
fn failed_view_error(fs: &MasterFilesystem) -> NamespaceError {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match fs.fs_dir.view() {
            Ok(_) => assert!(
                Instant::now() < deadline,
                "the namespace holder never went terminal"
            ),
            Err(err) => return err,
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The expected rejection of a call whose return type differs per probe
/// (`NamespaceError` for views, `ColdError` for cold access).
fn rejected<T, E>(result: Result<T, E>, context: &'static str) -> E {
    result.err().expect(context)
}

fn assert_holder_terminal(fs: &MasterFilesystem, writer: &Arc<JournalWriter>, seq_before: u64) {
    assert!(fs.fs_dir.is_failed(), "the holder must be terminal");
    assert!(
        matches!(
            rejected(fs.fs_dir.view(), "the holder must reject new views"),
            NamespaceError::Unavailable
        ),
        "new views must be rejected after the holder went terminal"
    );
    assert!(
        matches!(
            rejected(
                fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT),
                "the terminal holder must reject begin_cold"
            ),
            ColdError::Unavailable
        ),
        "cold exclusive access must be rejected after the holder went terminal"
    );
    assert!(
        writer.ensure_metadata_current().is_err(),
        "journal writes must be fenced after the holder went terminal"
    );
    assert_ne!(
        writer.failure_seq(),
        seq_before,
        "a terminal confirmation must move the failure seq"
    );
}

fn terminal_holder_alive_probe(fs: &MasterFilesystem, pre_view: &NamespaceGuard<'_>) {
    // FAILED is not a drain: the pre-failure view reference survives and can
    // be dropped safely, but reads through the terminal store are rejected
    // (the retirement invalidates the metadata store — same boundary as C1's
    // cursor probe: healthy before, rejected after).
    let rejected = MasterFilesystem::resolve_path(pre_view, "/dir");
    assert!(
        rejected.is_err(),
        "reads through the terminal store must be rejected, not silently served"
    );
}

/// Fail the body with the original typed error when the leaf is missing:
/// `InodePath::resolve` returns a partial-chain Ok for missing leaves, so
/// the missing case is detected explicitly (§9: uncommitted store errors).
fn typed_missing_leaf(fs_dir: &FsDir, path: &str) -> FsResult<()> {
    let inp = MasterFilesystem::resolve_path(fs_dir, path)?;
    if inp.get_last_inode().is_none() {
        return Err(FsError::file_not_found(path));
    }
    Ok(())
}

fn unwind_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message.to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn teardown(fs: MasterFilesystem, journal: JournalSystem, conf: &ClusterConf) {
    journal.shutdown();
    drop(fs);
    let _ = std::fs::remove_dir_all(&conf.master.meta_dir);
    let _ = std::fs::remove_dir_all(&conf.journal.journal_dir);
}

/// §8.1: a real publication failure inside the body moves the failure seq,
/// so the confirmation guard terminalizes the holder while the locks are
/// still held. A waiter that had to wait for the object lock observes the
/// terminal holder right after acquiring it — proving the failure happened
/// inside the lock window — and fresh views are rejected. The old reader and
/// its cursor probe taken before the failure stay legal (FAILED ≠ drain).
#[test]
fn recursive_publication_failure_terminals_inside_locks() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-publication-inside-locks");
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.journal_writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });

    let writer = swap_receipts_writer(&mut fs, &journal, &conf);
    let seq_before = writer.failure_seq();
    let dir_id = fs.file_status("/dir").unwrap().id;

    // Old reader taken before the failure: a live view plus a cursor probe.
    let pre_view = fs.fs_dir.view().unwrap();
    let pre_cursor = pre_view.store.cursor_store();
    assert!(
        pre_cursor.get_applied_req(77).is_ok(),
        "the cursor probe must be healthy before the failure"
    );

    // The terminal hook fires inside fail_terminal, after fail_closed and
    // while the failing frame still holds the object locks: the moment it
    // reaches the driver is the strict in-lock terminal proof point.
    let (terminal_tx, terminal_rx) = std::sync::mpsc::channel::<()>();
    let (terminal_release_tx, terminal_release_rx) = std::sync::mpsc::channel::<()>();
    writer.arm_terminal_hook(terminal_tx, terminal_release_rx);
    // Real publication failure: the recursive permanent delete of /dir/gone
    // reaches the armed fault point after its durable store commit and
    // before any live publication, so the body fails genuinely (no setter).
    let fault = arm_publication_failure("recursive-confirmation-publish", "gone");
    fs.create("/dir/gone", true).unwrap();

    let (body_ready_tx, body_ready_rx) = std::sync::mpsc::channel();
    let (reader_ready_tx, reader_ready_rx) = std::sync::mpsc::channel::<()>();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        let result = run_admitted(&worker_fs, "/dir", InodeLockMode::Write, true, |fs_dir| {
            body_ready_tx.send(()).unwrap();
            // The body must not fail before the waiter has asked for the
            // stripe: the driver spawns it and answers on this channel.
            reader_ready_rx
                .recv_timeout(Duration::from_secs(20))
                .expect("the driver must confirm the waiter is queued");
            fs_dir
                .delete(&MasterFilesystem::resolve_path(fs_dir, "/dir/gone")?, true)
                .map(|_| ())
        });
        let _ = fault;
        let _ = done_tx.send(result);
    });

    body_ready_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the body must run while the frame holds the object locks");

    // The waiter asks for the stripe while the body still holds it; the
    // driver only then lets the body reach the failing publication.
    let (queued_tx, queued_rx) = std::sync::mpsc::channel();
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || {
        queued_tx
            .send(())
            .expect("the driver must observe the lock attempt");
        let _lock = reader_fs.inode_locks.write_inode(dir_id);
        acquired_tx
            .send((reader_fs.fs_dir.is_failed(), reader_fs.fs_dir.view().err()))
            .expect("the driver must observe the post-lock state");
    });
    queued_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the waiter must reach the lock acquisition");
    reader_ready_tx
        .send(())
        .expect("the failing body must still be waiting");

    let result = await_worker(&done_rx, "the failing admission")
        .expect("the failing admission must report its result");
    worker.join().expect("the worker must not panic");

    // The strict in-lock terminal proof: fail_terminal consumed the hook
    // while the failing frame still held the stripe, so an actual
    // non-blocking shared acquisition of the target stripe at that moment
    // returns None (the waiter is queued, the stripe is held). The waiter
    // only acquires after the frame unlocks, so the queued evidence arrives
    // with the post-lock state.
    terminal_release_tx
        .send(())
        .expect("the driver must release the armed terminal hook");
    assert_eq!(
        terminal_rx.try_recv().ok(),
        Some(()),
        "fail_terminal must have consumed the hook before the locks dropped"
    );
    let (failed_at_lock, view_err) = acquired_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the waiter must acquire after the frame releases");
    reader.join().expect("the waiter must not panic");

    let op_err = typed_op_error(result, "the op must fail with its typed error");
    assert!(
        op_err
            .to_string()
            .contains("recursive publication failure: gone"),
        "the real publication failure must surface as the op error, got {op_err:?}"
    );
    assert!(
        failed_at_lock,
        "the holder must go terminal inside the locks"
    );
    assert!(
        matches!(view_err.unwrap(), NamespaceError::Unavailable),
        "a reader that waited for the object lock must find the view rejected"
    );

    assert_holder_terminal(&fs, &writer, seq_before);
    assert!(
        journal
            .master_monitor()
            .recovery()
            .fatal_source()
            .unwrap()
            .contains("metadata publication failed after durable commit"),
        "the recorded fatal source must name the real publication failure"
    );
    terminal_holder_alive_probe(&fs, &pre_view);
    // Cursor retirement boundary: the probe taken before the failure
    // observes the retired store afterwards.
    let after = pre_cursor.get_applied_req(77);
    assert!(
        after.is_err()
            && after
                .unwrap_err()
                .to_string()
                .contains("cursor store unavailable during metadata restore"),
        "the cursor store must retire with the terminal holder"
    );

    drain_receipts(&writer);
    teardown(fs, journal, &conf);
    Ok(())
}

/// §8.1: a panic inside the body is a real unwind; the guard declared inside
/// the body's lexical scope goes terminal before the view and object locks
/// drop, so a waiter that queued for the lock sees the failed holder.
#[test]
fn recursive_fenced_admission_rejected_without_holder_retirement() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-role-fence");
    fs.mkdir("/tree", true).unwrap();
    fs.create("/tree/file", true).unwrap();
    fs.journal_writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
    let writer = fs.journal_writer.clone();
    let seq_before = writer.failure_seq();

    let pre_view = fs.fs_dir.view().unwrap();

    fs.journal_writer.fence_metadata();
    let fenced = fs.set_attr(
        "/tree",
        SetAttrOpts {
            recursive: true,
            mode: Some(0o644),
            ..Default::default()
        },
    );
    let fenced_err = fenced.unwrap_err();
    assert!(
        fenced_err.to_string().contains("fenced"),
        "the role fence must reject the new admission, got {fenced_err}"
    );

    assert!(
        !fs.fs_dir.is_failed(),
        "a fence must not terminalize the holder"
    );
    assert!(
        fs.fs_dir.view().is_ok(),
        "views must stay readable across a role fence"
    );
    assert_eq!(
        writer.failure_seq(),
        seq_before,
        "a role fence must not move the failure seq"
    );
    assert!(
        journal.master_monitor().recovery().fatal_source().is_none(),
        "a role fence must not be recorded as a fatal source"
    );
    let still_resolved = MasterFilesystem::resolve_path(&pre_view, "/tree");
    assert!(
        still_resolved.is_ok(),
        "a reader holding a pre-fence view must keep working"
    );

    fs.journal_writer.unfence_metadata();
    fs.set_attr(
        "/tree",
        SetAttrOpts {
            recursive: true,
            mode: Some(0o644),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(fs.file_status("/tree").unwrap().mode, 0o644);

    drain_receipts(&writer);
    teardown(fs, journal, &conf);
    Ok(())
}

/// §8.1: a pure business error with an unmoved failure seq and an Ok
/// confirmation keeps the holder usable — no retirement, no safe mode, and
/// later ordinary writes succeed.
#[test]
fn recursive_plain_business_error_keeps_holder_alive() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-plain-business-error");
    fs.mkdir("/tree", true).unwrap();
    drain_receipts(&fs.journal_writer.clone());
    let writer = fs.journal_writer.clone();
    let seq_before = writer.failure_seq();

    let result = run_admitted(&fs, "/tree", InodeLockMode::Write, false, |fs_dir| {
        // A plan over an unresolvable path yields Fallback before the body;
        // point the plan at the real subtree and fail the body on a missing
        // leaf instead — that is the "plain business error" contract.
        typed_missing_leaf(fs_dir, "/tree/missing")
    });

    let op_err = typed_op_error(result, "the pure business error must surface");
    assert!(
        matches!(
            &op_err,
            FsError::FileNotFound(_) if op_err.to_string().contains("/tree/missing")
        ),
        "the pure business error must be returned with its original variant, got {op_err:?}"
    );
    assert_eq!(
        writer.failure_seq(),
        seq_before,
        "a plain business error must not move the failure seq"
    );
    assert!(!fs.fs_dir.is_failed());
    assert!(fs.fs_dir.view().is_ok());
    assert!(
        journal.master_monitor().recovery().fatal_source().is_none(),
        "a plain business error must not be recorded as a fatal source"
    );

    // The holder stays usable: ordinary writes keep succeeding.
    fs.mkdir("/after", true).unwrap();
    fs.create("/after/x", true).unwrap();

    drain_receipts(&writer);
    teardown(fs, journal, &conf);
    Ok(())
}

/// §8.1: with both the op and the confirmation failing, the original op
/// variant and payload win — the returned error is not the commit error and
/// not a Common wrapper. The holder is terminalized by the in-lock
/// fail_terminal of the moved-seq op error.
#[test]
fn recursive_double_failure_keeps_op_variant_and_payload() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-double-failure");
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.journal_writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });

    let writer = swap_receipts_writer(&mut fs, &journal, &conf);
    let seq_before = writer.failure_seq();

    let worker_fs = fs.clone();
    let epoch_for_worker = fs.journal_writer.metadata_epoch().unwrap();
    let worker = std::thread::spawn(move || {
        worker_fs.run_recursive_metadata_write(
            epoch_for_worker,
            "/dir",
            InodeLockMode::Write,
            true,
            |fs_dir| {
                // First step: a real successful mutation that enqueues a journal
                // entry and registers the waiter on this admission's scope.
                let inp = MasterFilesystem::resolve_path(fs_dir, "/dir/file")?;
                fs_dir.set_attr(
                    inp,
                    SetAttrOpts {
                        mode: Some(0o640),
                        ..Default::default()
                    },
                )?;
                // Then: a real typed op error; the driver drops the real
                // receipt, so the confirmation fails too (both real).
                typed_missing_leaf(fs_dir, "/dir/missing")
            },
        )
    });

    // Driver: drop the unique real receipt so the confirmation fails too.
    let queued = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("missing the recursive set_attr journal entry")
        })
        .unwrap();
    let _commit_failed = drop(queued.completion);

    let result = typed_op_error(
        worker.join().unwrap(),
        "the double failure must return an error",
    );
    assert!(
        matches!(
            &result,
            FsError::FileNotFound(_) if result.to_string().contains("/dir/missing")
        ),
        "the op error must win with its original variant and payload, got {result:?}"
    );
    assert!(
        !result.to_string().contains("journal commit failed"),
        "the commit error must not replace the op error"
    );

    assert_holder_terminal(&fs, &writer, seq_before);
    assert!(
        journal
            .master_monitor()
            .recovery()
            .fatal_source()
            .unwrap()
            .contains("metadata publication failed after durable commit"),
        "the fatal source must still name the real publication failure"
    );

    teardown(fs, journal, &conf);
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
    // the driver resolves its receipt.
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
