// Write-path identity planning regression: `resolve_edge_ids` must walk live
// child edges only (no RocksDB point reads) while keeping the documented
// contract: (ids, components_len, full, epoch), partial ids on negative
// lookups, a stop at intermediate files, and rejection of invalid or
// pending-namespace paths.
use super::*;
use crate::master::meta::inode::DirNode;
use crate::master::meta::BlockMeta;
use curvine_model::BlockLocation;
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
    // No location row for the block: the shared builder must report the same
    // lost-block error as the live path.
    let locations = HashMap::new();

    let err = fs
        .build_located_blocks("/file", &file, locations)
        .unwrap_err();
    assert!(
        err.to_string().contains("Lost (no worker can read)"),
        "the lost-block check must fire, got {err}"
    );
}

/// Removes the live child edge for `path` (a file under /data) while keeping
/// the store row, forcing the public block-read to fall back to the store
/// snapshot. Returns the owned inode view for restoration.
fn hide_live_edge_for_snapshot_fallback(
    fs: &MasterFilesystem,
    inode_locks: &InodeLockManager,
    name: &str,
) -> InodeView {
    // Fixture-only live mutation under the parent's inode write lock. Every
    // id is resolved before the lock: file_status inside the locked scope
    // would take a read lock on the same stripe and trip the debug
    // re-entrancy check.
    let parent_id = fs.file_status("/data").unwrap().id;
    let child_id = fs.file_status(&format!("/data/{name}")).unwrap().id;
    let locks = inode_locks.write_inode(parent_id);
    let view = fs.fs_dir.view().unwrap();
    let root = view.root_node();
    let data_view = root
        .children
        .read_child_view("data")
        .0
        .expect("data dir must exist in the live tree");
    let dir_node = match &data_view {
        InodeView::Dir(dir) => dir
            .node
            .clone()
            .unwrap_or_else(|| DirNode::for_edge(&data_view)),
        _ => panic!("data must be a directory"),
    };
    let owned = dir_node
        .delete_child_edge(child_id, name)
        .expect("live edge must be removable");
    // Confirm the split: the live edge is gone, the store row remains.
    assert!(dir_node.children.get_child_edge(name).is_none());
    {
        let snapshot = InodeStoreSnapshot::new(view.store.checked_read_snapshot().unwrap());
        let resolved = snapshot.resolve_path(&format!("/data/{name}")).unwrap();
        assert!(resolved.target().is_some(), "the store row must remain");
    }
    drop(view);
    drop(locks);
    owned
}

fn restore_live_edge(fs: &MasterFilesystem, inode_locks: &InodeLockManager, owned: InodeView) {
    // Same pre-resolved id discipline: nothing inside the locked scope takes
    // another inode lock.
    let parent_id = fs.file_status("/data").unwrap().id;
    let locks = inode_locks.write_inode(parent_id);
    let view = fs.fs_dir.view().unwrap();
    let root = view.root_node();
    let data_view = root
        .children
        .read_child_view("data")
        .0
        .expect("data dir must exist in the live tree");
    let dir_node = match &data_view {
        InodeView::Dir(dir) => dir
            .node
            .clone()
            .unwrap_or_else(|| DirNode::for_edge(&data_view)),
        _ => panic!("data must be a directory"),
    };
    dir_node.add_child_edge(owned).unwrap();
    drop(view);
    drop(locks);
}

#[test]
fn block_locations_snapshot_fallback_rejects_corrupt_non_tail_block() {
    let fs = test_fs("gbl-fallback-corrupt");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.mkdir("/data", true).unwrap();
    fs.create_with_opts(
        "/data/a.log",
        CreateFileOptsBuilder::new().block_size(1024 * 1024).build(),
        OpenFlags::new_create().set_overwrite(true),
    )
    .unwrap();
    let full_len: i64 = 1024 * 1024; // matches the file's block_size option
    let first = fs
        .add_block(
            "/data/a.log",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    fs.complete_file(
        "/data/a.log",
        None,
        full_len,
        vec![CommitBlock {
            block_id: first.block.id,
            block_len: full_len,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        true,
        None,
    )
    .unwrap();
    // Corrupt the stored metadata through the existing store batch API: a
    // short non-tail block is appended after the committed full block.
    let file_id = fs.file_status("/data/a.log").unwrap().id;
    {
        let view = fs.fs_dir.view().unwrap();
        let mut inode = view
            .store
            .get_inode(file_id, None)
            .unwrap()
            .expect("file row exists");
        let second_id = {
            let file = inode.as_file_ref().unwrap();
            crate::master::meta::InodeId::create_block_id(file.id, 1).unwrap()
        };
        if let InodeView::File(f) = &mut inode {
            f.blocks[0].len = 512;
            f.blocks.push(BlockMeta::new(second_id, full_len));
        } else {
            panic!("expected a file inode");
        }
        let mut batch = view.store.store.new_batch();
        batch.write_inode(&inode).unwrap();
        batch
            .add_location(second_id, &BlockLocation::with_id(100))
            .unwrap();
        batch.commit().unwrap();
    }

    let inode_locks = fs.inode_locks_for_test();
    let owned = hide_live_edge_for_snapshot_fallback(&fs, &inode_locks, "a.log");
    // Public entry, held shared view, store-only target: the fallback must
    // run the shared builder's non-tail length check without any cold drain.
    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<FileBlocks>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.get_block_locations("/data/a.log"))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");
    let outcome = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("the fallback must not wait for a cold window");
    worker.join().expect("worker must not panic");
    let err = outcome.unwrap_err();
    assert!(
        err.to_string().contains("block status abnormal"),
        "the snapshot fallback must reject a corrupt non-tail block, got {err}"
    );
    drop(held);
    restore_live_edge(&fs, &inode_locks, owned);
    // The hot path over the restored live edge reports the same contract.
    let err = fs.get_block_locations("/data/a.log").unwrap_err();
    assert!(
        err.to_string().contains("block status abnormal"),
        "the live path must agree with the fallback, got {err}"
    );
}

#[test]
fn block_locations_snapshot_fallback_matches_the_live_result() {
    let fs = test_fs("gbl-fallback-match");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.mkdir("/data", true).unwrap();
    fs.create_with_opts(
        "/data/a.log",
        CreateFileOptsBuilder::new().block_size(1024 * 1024).build(),
        OpenFlags::new_create().set_overwrite(true),
    )
    .unwrap();
    let first = fs
        .add_block(
            "/data/a.log",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let full_len: i64 = 1024 * 1024; // matches the file's block_size option
    fs.complete_file(
        "/data/a.log",
        None,
        full_len,
        vec![CommitBlock {
            block_id: first.block.id,
            block_len: full_len,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        true,
        None,
    )
    .unwrap();
    // Append a second full-size block directly in the store (same batch API),
    // so the snapshot fallback has two ordered blocks to report.
    let file_id = fs.file_status("/data/a.log").unwrap().id;
    let second_id = {
        let view = fs.fs_dir.view().unwrap();
        let mut inode = view
            .store
            .get_inode(file_id, None)
            .unwrap()
            .expect("file row exists");
        let second_id = {
            let file = inode.as_file_ref().unwrap();
            crate::master::meta::InodeId::create_block_id(file.id, 1).unwrap()
        };
        if let InodeView::File(f) = &mut inode {
            f.blocks.push(BlockMeta::new(second_id, full_len));
        } else {
            panic!("expected a file inode");
        }
        let mut batch = view.store.store.new_batch();
        batch.write_inode(&inode).unwrap();
        batch
            .add_location(second_id, &BlockLocation::with_id(100))
            .unwrap();
        batch.commit().unwrap();
        second_id
    };
    let live_status = fs.file_status("/data/a.log").unwrap();

    let inode_locks = fs.inode_locks_for_test();
    let owned = hide_live_edge_for_snapshot_fallback(&fs, &inode_locks, "a.log");
    // Held shared view + bounded worker: the snapshot fallback must serve the
    // store-resident file without any cold drain.
    let held = fs.fs_dir.view().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let (tx, rx) = std::sync::mpsc::channel::<FsResult<FileBlocks>>();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).expect("worker started channel");
        tx.send(worker_fs.get_block_locations("/data/a.log"))
            .expect("result channel");
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("worker started");
    let blocks = rx
        .recv_timeout(std::time::Duration::from_millis(500))
        .expect("the fallback must not wait for a cold window");
    worker.join().expect("worker must not panic");
    let blocks = blocks.expect("the snapshot fallback must serve the store-resident file");
    drop(held);
    restore_live_edge(&fs, &inode_locks, owned);

    assert_eq!(blocks.status.id, file_id);
    assert_eq!(blocks.status.len, live_status.len);
    assert_eq!(blocks.block_locs.len(), 2);
    assert_eq!(blocks.block_locs[0].block.id, first.block.id);
    assert_eq!(blocks.block_locs[0].block.len, full_len);
    assert_eq!(blocks.block_locs[1].block.id, second_id);
    assert_eq!(blocks.block_locs[1].block.len, full_len);
    for located in &blocks.block_locs {
        assert_eq!(
            located.locs.iter().map(|l| l.worker_id).collect::<Vec<_>>(),
            vec![100]
        );
    }
    // The hot path over the restored live edge agrees with the fallback.
    let hot = fs.get_block_locations("/data/a.log").unwrap();
    let hot_ids: Vec<_> = hot.block_locs.iter().map(|l| l.block.id).collect();
    let fallback_ids: Vec<_> = blocks.block_locs.iter().map(|l| l.block.id).collect();
    assert_eq!(hot_ids, fallback_ids);
}
