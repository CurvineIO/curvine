// Owned by deputy-2 (C2 §8.1–8.2 confirmation contract tests).
// Registration anchor (main developer): master_filesystem.rs cfg(test)
// declaration block, `#[path = "tests/recursive_confirmation_test.rs"]`.

use super::recursive_confirmation_support::*;
use super::recursive_confirmation_support::{
    arm_publication_failure, await_worker, remove_case_dirs, run_admitted, shutdown_case,
    typed_op_error, WORKER_TIMEOUT,
};
use super::*;
use crate::master::journal::RecoveryGate;
use curvine_raft::raft::RaftClient;

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

/// §8.1: a real publication failure inside the body moves the failure seq,
/// so the confirmation guard terminalizes the holder while the locks are
/// still held. A waiter that had to wait for the object lock observes the
/// terminal holder right after acquiring it — proving the failure happened
/// inside the lock window — and fresh views are rejected. The old reader and
/// its cursor probe taken before the failure stay legal (FAILED ≠ drain).
// The publication-failure contract needs the fault-injection runtime:
// the injection point only registers under the fault feature (the shared
// FAULT_POINT_REGISTRATIONS slice is feature-gated), so this case runs
// only there — the default-mode publication contract is covered by the
// confirmation's commit-failure cases that don't need an injection point.
#[cfg(feature = "fault-injection")]
#[test]
fn recursive_publication_failure_terminals_inside_locks() -> FsResult<()> {
    let (conf, journal, mut fs) = confirmation_env("recursive-publication-inside-locks");
    // All setup writes run on the ORIGINAL testing writer: the receipts
    // writer has require_raft_commit=true and no sender/driver, so a setup
    // write after the swap would block forever on its own scope.wait (the
    // create used to hang before the worker existed). Setup, drain, then
    // swap - the C1 fixture order.
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.create("/dir/gone", true).unwrap();
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
    // (created above, before the swap) reaches the armed fault point after
    // its durable store commit and before any live publication, so the
    // body fails genuinely (no setter).
    let fault = arm_publication_failure("recursive-confirmation-publish", "gone");

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

    // fail_terminal consumes the hook and pauses on the release channel
    // while the failing frame still holds the stripe; the reached signal is
    // the strict in-lock terminal proof point. The release must be sent
    // from here - the worker cannot reach its done send until the hook is
    // released, so awaiting the worker first would deadlock.
    terminal_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("fail_terminal must reach the hook while the locks are held");
    // Non-blocking evidence at the exact terminal point: an actual shared
    // try-read of the target stripe fails (the stripe is held by the
    // failing frame).
    assert!(
        fs.inode_locks.try_read_inode(dir_id).is_none(),
        "at the terminal point the target stripe must still be held"
    );
    terminal_release_tx
        .send(())
        .expect("the driver must release the armed terminal hook");

    let result = await_worker(&done_rx, "the failing admission")
        .expect("the failing admission must report its result");
    worker.join().expect("the worker must not panic");
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
    drop(pre_cursor);
    drop(pre_view);
    drop(writer);
    disarm_publication_failure(&fault, "recursive-confirmation-publish");
    drop(fault);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
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
    drop(pre_view);
    drop(writer);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
    Ok(())
}

/// G3 final matrix: one shared real-completed-file fixture drives the three
/// real public recursive RPCs (delete/free/set_attr) through a fenced
/// pre-hot window. The common invariants (same inode, mode 644, same
/// non-empty blocks) reject each op's real side effect; the single accepted
/// error is the original-epoch NotLeader.
mod fenced_window_matrix {
    use super::*;
    use curvine_model::{
        BlockLocation, ClientAddress, CreateFileOptsBuilder, OpenFlags, TtlAction, WorkerAddress,
        WorkerInfo, WorkerStatus,
    };

    /// A real, completed, free-able file: 4096 bytes, one committed block
    /// with a real location row, TTL Delete policy (the free contract
    /// requires a policy that may drop the Curvine copy), and a live
    /// worker. Not an empty stub - a rejected free on an empty file would
    /// be a vacuous oracle.
    fn completed_file_fixture(name: &str) -> (ClusterConf, JournalSystem, MasterFilesystem, i64) {
        let (conf, journal, fs) = confirmation_env(name);
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            1,
        );
        worker.status = WorkerStatus::Live;
        worker.capacity = i64::MAX / 4;
        worker.available = i64::MAX / 4;
        fs.add_test_worker(worker);

        fs.mkdir("/dir", true).unwrap();
        // replicas(2) keeps room for the explicit location row; ttl_action
        // Delete makes the file actually free-able.
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_action(TtlAction::Delete)
            .replicas(2)
            .build();
        fs.create_with_opts(
            "/dir/victim",
            opts,
            OpenFlags::new_create().set_overwrite(true),
        )
        .expect("create the victim file");
        fs.set_attr(
            "/dir/victim",
            SetAttrOpts {
                mode: Some(0o644),
                ..Default::default()
            },
        )
        .unwrap();
        let block = fs
            .add_block(
                "/dir/victim",
                None,
                ClientAddress::default(),
                vec![],
                vec![],
                0,
                None,
            )
            .expect("allocate the block");
        {
            let view = fs.fs_dir.view().expect("namespace view");
            view.store
                .add_block_location(block.id, BlockLocation::with_id(1))
                .expect("record the block location");
        }
        let commit = curvine_model::CommitBlock {
            block_id: block.id,
            block_len: 4096,
            locations: vec![BlockLocation::with_id(1)],
        };
        fs.complete_file(
            "/dir/victim".to_string(),
            None,
            4096,
            vec![commit],
            "fenced-window-fixture".to_string(),
            false,
            None,
        )
        .expect("complete the block");
        drain_receipts(&fs.journal_writer.clone());

        // Pin the initial state once, before any op: inode, mode, blocks.
        let status = fs.file_status("/dir/victim").expect("victim status");
        assert_eq!(status.mode, 0o644, "the fixture must pin mode 644");
        let blocks_before = block_signature(&fs);
        assert!(
            !blocks_before.is_empty(),
            "the fixture must have a real non-empty block set"
        );
        (conf, journal, fs, status.id)
    }

    /// The shared invariants for a REJECTED op: the same inode survives
    /// with the pinned mode and the original non-empty block set - the
    /// real side effects of delete (removal), free (reclaim), and chmod
    /// (mode rewrite) would each break at least one of these.
    /// (block_id, len) pairs: primitive, order-preserving, comparable
    /// (FileBlocks has no PartialEq derive; the pair list is the honest
    /// signature of the non-empty block set).
    fn block_signature(fs: &MasterFilesystem) -> Vec<(i64, i64)> {
        fs.get_block_locations("/dir/victim")
            .expect("victim block locations")
            .block_locs
            .iter()
            .map(|located| (located.block.id, located.block.len))
            .collect()
    }

    fn assert_common_invariants(
        fs: &MasterFilesystem,
        inode_id: i64,
        blocks_before: &[(i64, i64)],
    ) {
        let status = fs
            .file_status("/dir/victim")
            .expect("a rejected op must not remove the file");
        assert_eq!(status.id, inode_id, "the same inode must survive");
        assert_eq!(status.mode, 0o644, "the pinned mode must survive");
        assert_eq!(
            block_signature(fs),
            blocks_before,
            "the non-empty block set must be untouched"
        );
    }

    #[test]
    fn fenced_window_rejects_real_recursive_rpcs_by_original_epoch() -> FsResult<()> {
        let (conf, journal, fs, inode_id) =
            completed_file_fixture("recursive-fenced-window-matrix");
        let writer = fs.journal_writer.clone();
        let blocks_before = block_signature(&fs);

        // (name, real public op mapped to (), post-check) - three rows, one
        // shared driver; no per-op copies, no generic runner framework.
        type Op = Box<dyn FnOnce(&MasterFilesystem) -> FsResult<()> + Send>;
        let ops: Vec<(&'static str, Op)> = vec![
            (
                "recursive delete",
                Box::new(|fs: &MasterFilesystem| {
                    fs.delete_with_mode("/dir", true, DeleteMode::SkipTrash)
                        .map(|_| ())
                }),
            ),
            (
                "recursive free",
                Box::new(|fs: &MasterFilesystem| fs.free_or_defer("/dir", true).map(|_| ())),
            ),
            (
                "recursive set_attr",
                Box::new(|fs: &MasterFilesystem| {
                    fs.set_attr(
                        "/dir",
                        SetAttrOpts {
                            recursive: true,
                            mode: Some(0o600),
                            ..Default::default()
                        },
                    )
                    .map(|_| ())
                }),
            ),
        ];

        for (case, op) in ops {
            let seq_before = writer.failure_seq();
            let (pre_hot_tx, pre_hot_rx) = std::sync::mpsc::channel::<()>();
            let (pre_hot_release_tx, pre_hot_release_rx) = std::sync::mpsc::channel::<()>();
            writer.arm_pre_hot_hook(pre_hot_tx, pre_hot_release_rx);

            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let worker_fs = fs.clone();
            let worker = std::thread::spawn(move || {
                let _ = done_tx.send(op(&worker_fs));
            });

            // At this point the outer epoch is captured and ensured; the
            // commit slot is held; hot has not run.
            pre_hot_rx
                .recv_timeout(WORKER_TIMEOUT)
                .expect("the real public admission must reach the pre-hot checkpoint");
            // G3: fence AFTER the outer epoch is captured+ensured and BEFORE
            // the body runs. The epoch advances (+1) and stays advanced —
            // the locked ensure(e0) in the recursive entry must reject with
            // NotLeader. If we unfence here, the epoch returns and ensure
            // passes — that would NOT detect a re-sampling bug.
            writer.fence_metadata();
            pre_hot_release_tx
                .send(())
                .expect("the driver must release the pre-hot pause");

            let result = await_worker(&done_rx, case)
                .expect("the fenced-window admission must report its result");
            worker.join().expect("the worker must not panic");
            // The single accepted error: the original-epoch NotLeader from
            // the locked re-verify of the caller's own epoch.
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("role changed"),
                "{case}: the rejection must be the original-epoch error, got {err:?}"
            );
            assert_common_invariants(&fs, inode_id, &blocks_before);
            assert!(
                !fs.fs_dir.is_failed(),
                "{case}: the fence must not terminalize the holder"
            );
            assert_eq!(
                writer.failure_seq(),
                seq_before,
                "{case}: a role fence must not move the failure seq"
            );
            assert!(
                journal.master_monitor().recovery().fatal_source().is_none(),
                "{case}: a role fence must not be recorded as a fatal source"
            );
            // The take-once hook must not fire again for the next op.
            assert!(writer.consume_pre_hot_hook().is_none());
        }
        // Restore the writer for subsequent tests (fence was NOT unfenced
        // during the matrix — unfence now so the epoch returns to normal).
        writer.unfence_metadata();

        drop(writer);
        drop(fs);
        shutdown_case(journal);
        remove_case_dirs(&conf);
        Ok(())
    }
}
