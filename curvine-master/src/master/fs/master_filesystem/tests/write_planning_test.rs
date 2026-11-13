// Write-path identity planning regression: `resolve_edge_ids` must walk live
// child edges only (no RocksDB point reads) while keeping the documented
// contract: (ids, components_len, full, epoch), partial ids on negative
// lookups, a stop at intermediate files, and rejection of invalid or
// pending-namespace paths.
use super::*;
use crate::master::meta::inode::DirNode;
use crate::master::meta::BlockMeta;
use curvine_model::BlockLocation;
use curvine_raft::raft::RoleState;
use std::collections::HashMap;

#[test]
fn write_planning_walks_live_edges_and_keeps_partial_ids() {
    let fs = test_fs("write-planning-edges");
    fs.mkdir("/a", false).unwrap();
    fs.mkdir("/a/b", false).unwrap();
    fs.create("/a/f", false).unwrap();
    let a_id = fs.file_status("/a").unwrap().id;
    let b_id = fs.file_status("/a/b").unwrap().id;
    let f_id = fs.file_status("/a/f").unwrap().id;

    // Full directory path: every edge resolves.
    let (ids, len, full, _) = fs.resolve_edge_ids("/a/b").unwrap();
    assert_eq!(ids, vec![ROOT_INODE_ID, a_id, b_id]);
    assert_eq!(len, 3);
    assert!(full);

    // Negative lookup: ids stop at the deepest existing directory.
    let (ids, len, full, _) = fs.resolve_edge_ids("/a/missing/deeper").unwrap();
    assert_eq!(ids, vec![ROOT_INODE_ID, a_id]);
    assert_eq!(len, 4);
    assert!(!full);

    // A file in a middle component ends the walk but keeps its id: callers
    // revalidate under the locks and report the right error.
    let (ids, len, full, _) = fs.resolve_edge_ids("/a/f/child").unwrap();
    assert_eq!(ids, vec![ROOT_INODE_ID, a_id, f_id]);
    assert_eq!(len, 4);
    assert!(!full);

    // The root itself plans as a single identity.
    let (ids, len, full, _) = fs.resolve_edge_ids("/").unwrap();
    assert_eq!(ids, vec![ROOT_INODE_ID]);
    assert_eq!(len, 1);
    assert!(full);

    // Reserved pending namespace and relative paths are rejected.
    assert!(fs.resolve_edge_ids("/.Trash/.pending/1-2").is_err());
    assert!(fs.resolve_edge_ids("relative/path").is_err());

    // The planned epoch matches the live scan control.
    let epoch = fs
        .fs_dir
        .view()
        .unwrap()
        .store
        .scan_control()
        .cancel_epoch();
    let (_, _, _, planned) = fs.resolve_edge_ids("/a").unwrap();
    assert_eq!(planned, epoch);
}

#[test]
fn get_lock_rejects_the_pending_staging_namespace() {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    conf.master.trash_enabled = true;
    conf.master.meta_dir = Utils::test_sub_dir(format!(
        "master-fs-getlock-pending/meta-{}",
        Utils::rand_str(6)
    ));
    conf.journal.journal_dir = Utils::test_sub_dir(format!(
        "master-fs-getlock-pending/journal-{}",
        Utils::rand_str(6)
    ));
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Leader);

    // Build a real pending entry through the trash flow: create, soft-delete,
    // then claim it into /.Trash/.pending.
    fs.create("/data/a.log", true).unwrap();
    fs.delete_with_mode("/data/a.log", false, DeleteMode::Default)
        .unwrap();
    let entry = fs.list_trash().unwrap().remove(0);
    let claimed = fs.claim_trash_entry_for_purge(&entry.path).unwrap();
    assert!(claimed.is_some(), "the entry must be claimable");
    let pending_path = trash::pending_entry_path(entry.path.rsplit('/').next().unwrap());

    // The pending entry really exists and resolves internally.
    let internal = InodePath::resolve(
        fs.fs_dir.view().unwrap().root_node(),
        &pending_path,
        &fs.fs_dir.view().unwrap().store,
    )
    .unwrap();
    assert!(
        internal.get_last_inode().is_some() && internal.is_full(),
        "the pending entry must exist internally"
    );

    // The public entry must reject it with the same FileNotFound boundary as
    // resolve_path, for the locked path and the snapshot fallback alike.
    let err = fs.get_lock(&pending_path, FileLock::default()).unwrap_err();
    assert!(
        matches!(err, FsError::FileNotFound(_)),
        "get_lock on a pending path must be FileNotFound, got {err:?}"
    );
}

#[test]
fn get_block_locations_missing_path_is_timely_under_a_shared_view() {
    let fs = test_fs("gbl-missing-shared-view");
    fs.create("/data/a.log", true).unwrap();

    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<FileBlocks>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.get_block_locations("/data/missing.log"))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");

    // The shared view is held: the read must return the real error promptly,
    // not queue behind a 30s cold drain.
    let outcome = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("get_block_locations must not wait for a cold window");
    worker.join().expect("worker must not panic");
    let err = outcome.unwrap_err();
    assert!(
        matches!(err, FsError::FileNotFound(_)),
        "a missing path must be FileNotFound, got {err:?}"
    );
    drop(held);
}

#[test]
fn get_file_block_details_missing_path_is_timely_under_a_shared_view() {
    let fs = test_fs("gfd-missing-shared-view");
    fs.create("/data/a.log", true).unwrap();

    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<FileBlockDetails>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.get_file_block_details("/data/missing.log"))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");

    let outcome = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("get_file_block_details must not wait for a cold window");
    worker.join().expect("worker must not panic");
    let err = outcome.unwrap_err();
    assert!(
        matches!(err, FsError::FileNotFound(_)),
        "a missing path must be FileNotFound, got {err:?}"
    );
    drop(held);
}

#[test]
fn fs_mode_snapshot_succeeds_under_a_shared_view() {
    let fs = test_fs("fsm-shared-view");
    fs.create("/data/a.log", true).unwrap();
    let id = fs.file_status("/data/a.log").unwrap().id;
    // Reference DTO taken without a held view: the shared-view read must
    // return the same identity fields.
    let reference = fs
        .fs_mode_snapshot(id)
        .unwrap()
        .expect("reference snapshot");

    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<Option<FsModeSnapshot>>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.fs_mode_snapshot(id))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");

    let snapshot = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("fs_mode_snapshot must not wait for a cold window")
        .expect("snapshot read must succeed")
        .expect("the file must have a snapshot");
    worker.join().expect("worker must not panic");
    assert_eq!(snapshot.path, reference.path);
    assert_eq!(snapshot.gen, reference.gen);
    assert_eq!(snapshot.state, reference.state);
    assert_eq!(snapshot.ufs_mtime, reference.ufs_mtime);
    drop(held);
}

#[test]
fn build_located_blocks_rejects_short_non_tail_block_like_the_live_path() {
    let fs = test_fs("located-blocks-builder");

    // Two blocks: the non-tail one is shorter than the file's block size —
    // the same corruption the live path rejects.
    let mut file = InodeFile::new(1, 0);
    file.block_size = 1024;
    file.blocks = vec![BlockMeta::new(101, 512), BlockMeta::new(102, 256)];
    let mut locations = HashMap::new();
    locations.insert(101, vec![BlockLocation::with_id(100)]);
    locations.insert(102, vec![BlockLocation::with_id(100)]);

    let err = fs
        .build_located_blocks("/file", &file, locations)
        .unwrap_err();
    assert!(
        err.to_string().contains("block status abnormal"),
        "the non-tail length check must fire, got {err}"
    );
}

#[test]
fn build_located_blocks_orders_blocks_and_reports_status() {
    let fs = test_fs("located-blocks-builder-ok");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);

    let mut file = InodeFile::new(2, 0);
    file.block_size = 1024;
    file.blocks = vec![BlockMeta::new(201, 1024), BlockMeta::new(202, 512)];
    let mut locations = HashMap::new();
    locations.insert(201, vec![BlockLocation::with_id(100)]);
    locations.insert(202, vec![BlockLocation::with_id(100)]);

    let blocks = fs.build_located_blocks("/file", &file, locations).unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].block.id, 201);
    assert_eq!(blocks[0].block.len, 1024);
    assert_eq!(
        blocks[0]
            .locs
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![100]
    );
    assert_eq!(blocks[1].block.id, 202);
    assert_eq!(blocks[1].block.len, 512);
    assert_eq!(
        blocks[1]
            .locs
            .iter()
            .map(|l| l.worker_id)
            .collect::<Vec<_>>(),
        vec![100]
    );
}

#[test]
fn build_located_blocks_reports_lost_blocks_like_the_live_path() {
    let fs = test_fs("located-blocks-builder-lost");

    let mut file = InodeFile::new(3, 0);
    file.block_size = 1024;
    file.blocks = vec![BlockMeta::new(301, 1024)];
    // Missing map key for the block: the shared builder must report the
    // same lost-block error as the live path.
    let locations = HashMap::new();

    let err = fs
        .build_located_blocks("/file", &file, locations)
        .unwrap_err();
    assert!(
        err.to_string().contains("Lost (no worker can read)"),
        "the lost-block check must fire, got {err}"
    );
}

#[test]
fn exclusive_op_and_commit_error_preserves_original_op_error() {
    // Real production-helper contract: the worker thread calls
    // run_metadata_write_exclusive(epoch, body). The body uses the passed-in
    // FsDir to perform a real journalled delete (enqueuing a receipt), then
    // returns a distinguishable typed op error. The driver drops the unique
    // real completion, so the helper's own result is the (Err(op),
    // Err(commit)) arm — the test never re-implements the gate/scope/RAII
    // match.
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "exclusive-op-commit-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 8;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let mut fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.journal_writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
    let failure_seq_before = fs.journal_writer.failure_seq();

    // Install the receipts writer (require_raft_commit=true) into both the
    // FsDir and the filesystem.
    let writer = {
        let rt = conf.journal.create_runtime();
        let client = curvine_raft::raft::RaftClient::from_conf(rt, &conf.journal);
        let recovery = journal.master_monitor().recovery();
        let mut writer = JournalWriter::new(true, client, &conf.journal, recovery.clone()).unwrap();
        writer.enable_testing_receipts(recovery);
        let writer = Arc::new(writer);
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .journal_writer = writer.clone();
        writer
    };
    fs.journal_writer = writer.clone();

    let fs_worker = fs.clone();
    let worker = std::thread::spawn(move || {
        let fs = fs_worker;
        let epoch = fs.journal_writer.metadata_epoch().unwrap();
        fs.run_metadata_write_exclusive(epoch, |fs_dir: &mut FsDir| {
            // A real journalled operation through the passed-in owner: the
            // receipt is enqueued inside the exclusive scope.
            let inp = InodePath::resolve(fs_dir.root_node(), "/dir/file", &fs_dir.store)?;
            fs_dir.delete(&inp, false)?;
            // The send_and_wait inside fs_dir.delete has already enqueued the
            // receipt. Now return a distinguishable typed op error.
            err_box!("exclusive body: typed op error after enqueue")
        })
    });

    // Driver: take the unique real completion from the retained receiver and
    // drop it, failing the commit for real.
    let queued = writer
        .with_testing_receiver(|receiver| {
            receiver
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("missing delete journal entry")
        })
        .expect("the receipts receiver must exist");
    drop(queued.completion.expect("missing commit confirmation"));

    // The helper's real result: op error keeps its original variant and
    // context over the commit error. Bounded join first: the worker must
    // finish on its own; a stuck worker is reported, not silently waited on.
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let joiner = std::thread::spawn(move || {
        let joined = worker.join().expect("the exclusive worker must not abort");
        result_tx
            .send(joined)
            .expect("the driver must be alive to receive the worker result");
    });
    let joined: Option<FsResult<()>> =
        match result_rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(joined) => Some(joined),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("the joiner must stay alive until the worker result is sent")
            }
        };
    let result: FsResult<()> = joined.expect("the exclusive worker must finish within the bound");
    joiner.join().expect("the joiner thread must finish");
    let delete_err = result.expect_err("the exclusive op must fail with the op error");
    assert!(
        matches!(&delete_err, curvine_error::FsError::Common(_)),
        "the original op error must keep its Common variant, got {delete_err:?}"
    );
    assert!(
        delete_err
            .to_string()
            .contains("exclusive body: typed op error after enqueue"),
        "the original op error must keep its context, got {delete_err}"
    );

    // The holder must be terminal (body Err + seq moved → fail_closed).
    assert!(
        fs.fs_dir.is_failed(),
        "both failures must fail the holder closed"
    );
    assert!(
        matches!(
            fs.fs_dir.view().err(),
            Some(crate::master::meta::NamespaceError::Unavailable)
        ),
        "the view must be Unavailable after both failures"
    );
    assert_ne!(
        fs.journal_writer.failure_seq(),
        failure_seq_before,
        "the failure_seq must advance on the terminal failure"
    );

    // Teardown: release handles before shutdown; save each remove result.
    drop(fs);
    drop(writer);
    journal.shutdown();
    let meta_cleanup = std::fs::remove_dir_all(&conf.master.meta_dir);
    let journal_cleanup = std::fs::remove_dir_all(&conf.journal.journal_dir);
    meta_cleanup.expect("the case meta dir must be removable");
    journal_cleanup.expect("the case journal dir must be removable");
}

#[test]
fn exclusive_real_wait_unwind_fails_holder_via_raii_drop() {
    // Real pending-wait unwind: the worker calls the production exclusive
    // helper; the instance-isolated hook signals `reached` from inside the
    // real JournalCommitScope::wait (with pending waiters), the driver then
    // observes the reopened view and the still-closed gate, releases, and
    // the wait panics — the RAII Drop fails the holder closed before the
    // gate opens.
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::time::Duration;

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        ..Default::default()
    };
    let tmp_root = std::env::temp_dir().join(format!(
        "exclusive-wait-unwind-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.master.meta_dir = tmp_root.join("meta").to_str().unwrap().to_string();
    conf.journal.journal_dir = tmp_root.join("journal").to_str().unwrap().to_string();
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 8;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let mut fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs.mkdir("/dir", true).unwrap();
    fs.create("/dir/file", true).unwrap();
    fs.journal_writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });

    // Swap in the real receipts writer (require_raft_commit=true) so the
    // exclusive body's delete produces a real queued receipt.
    let writer = {
        let rt = conf.journal.create_runtime();
        let client = curvine_raft::raft::RaftClient::from_conf(rt, &conf.journal);
        let recovery = journal.master_monitor().recovery();
        let mut writer = JournalWriter::new(true, client, &conf.journal, recovery.clone()).unwrap();
        writer.enable_testing_receipts(recovery);
        let writer = Arc::new(writer);
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .journal_writer = writer.clone();
        writer
    };
    fs.journal_writer = writer.clone();
    let failure_seq_before = writer.failure_seq();

    // Arm the instance-isolated hook before spawning: the reached signal is
    // sent from inside the real pending wait; the release is consumed once.
    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    writer.arm_wait_unwind_hook(reached_tx, release_rx);

    // Pre-operation opaque cursor probe (handle 77 must answer before).
    let pre_cursor = fs.fs_dir.view().unwrap().store.cursor_store();
    let pre_cursor_probe = pre_cursor.get_applied_req(77);
    assert!(
        matches!(&pre_cursor_probe, Ok(None)),
        "the live cursor must answer point lookups, got {pre_cursor_probe:?}"
    );

    // Worker: run the real production helper; catch the injected unwind.
    let fs_worker = fs.clone();
    let worker = std::thread::spawn(move || {
        let fs = fs_worker;
        let epoch = fs.journal_writer.metadata_epoch().unwrap();
        catch_unwind(AssertUnwindSafe(|| {
            fs.run_metadata_write_exclusive(epoch, |fs_dir: &mut FsDir| {
                // A real journalled operation: delete the file. The receipt
                // is enqueued inside the exclusive scope.
                let inp = InodePath::resolve(fs_dir.root_node(), "/dir/file", &fs_dir.store)?;
                fs_dir.delete(&inp, false)?;
                Ok(())
            })
        }))
    });

    // Phase 1: the hook reached the real pending wait — the body has
    // returned, drop(cold) has run, and the driver can now observe the
    // reopened view and the still-closed exclusive gate without polling.
    reached_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the exclusive wait must be entered with a real pending waiter");
    assert!(
        fs.fs_dir.view().is_ok(),
        "the view must reopen once the body completes"
    );
    assert!(
        fs.metadata_commit_gate().has_exclusive_writer(),
        "the exclusive gate must stay closed across the real wait"
    );

    // Phase 2: release the hook — the wait panics on the real unwind path.
    release_tx.send(()).expect("the hook must still be armed");

    // Phase 3: bounded join — the worker must finish on its own.
    let (worker_result_tx, worker_result_rx) = std::sync::mpsc::channel();
    let joiner = std::thread::spawn(move || {
        let joined = worker.join().expect("the exclusive worker must finish");
        worker_result_tx
            .send(joined)
            .expect("the driver must be alive");
    });
    let joined = loop {
        match worker_result_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(joined) => break Some(joined),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("the joiner must stay alive until the worker result is sent")
            }
        }
    };
    let joined = joined.expect("the exclusive worker must finish within the bound");
    joiner.join().expect("the joiner thread must finish");
    let panic_message = match joined {
        Err(payload) => payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "<non-string panic>".to_string()),
        Ok(result) => panic!("the exclusive wait must panic, got {result:?}"),
    };
    assert!(
        panic_message.contains("injected journal commit wait unwind"),
        "the panic must come from the real wait hook, got {panic_message}"
    );

    // The RAII Drop ran on unwind: the holder is terminal before the gate
    // opens, the writer is fenced and the failure_seq advanced.
    assert!(
        fs.fs_dir.is_failed(),
        "the holder must be terminal after the real wait unwind"
    );
    assert!(
        matches!(
            fs.fs_dir.view().err(),
            Some(crate::master::meta::NamespaceError::Unavailable)
        ),
        "the view must be Unavailable after the terminal failure"
    );
    assert_ne!(
        writer.failure_seq(),
        failure_seq_before,
        "the failure_seq must advance on the terminal failure"
    );
    let cursor_retired = pre_cursor.get_applied_req(77);
    let cursor_retired_err =
        cursor_retired.expect_err("the retired cursor must fail point lookups");
    assert!(
        cursor_retired_err
            .to_string()
            .contains("cursor store unavailable during metadata restore"),
        "the retired cursor must report the restore-unavailable error, got {cursor_retired_err}"
    );
    let fence_err = writer
        .ensure_metadata_current()
        .expect_err("the fenced writer must reject new admissions");
    assert!(
        fence_err.to_string().contains("not accepting writes"),
        "the writer fence must reject admissions, got {fence_err}"
    );

    // Teardown: drain receipts, release handles, shutdown, save the real
    // remove result.
    writer.with_testing_receiver(|receiver| loop {
        match receiver.try_recv() {
            Ok(entry) => drop(entry),
            Err(_) => break,
        }
    });
    drop(fs);
    drop(writer);
    journal.shutdown();
    let cleanup = std::fs::remove_dir_all(&tmp_root);
    cleanup.expect("the case temp root must be removable");
}
