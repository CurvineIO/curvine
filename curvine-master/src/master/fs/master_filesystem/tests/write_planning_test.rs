// Write-path identity planning regression: `resolve_edge_ids` must walk live
// child edges only (no RocksDB point reads) while keeping the documented
// contract: (ids, components_len, full, epoch), partial ids on negative
// lookups, a stop at intermediate files, and rejection of invalid or
// pending-namespace paths.
use super::*;

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
