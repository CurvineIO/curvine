use super::*;

static CHILDREN_COUNT_PERSISTENCE: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn root_lock_filter_keeps_only_explicit_root_writes() {
    let mut requests = SmallVec::<[InodeLockRequest; 8]>::from_vec(vec![
        InodeLockRequest::read(0, ROOT_INODE_ID),
        InodeLockRequest::write(0, ROOT_INODE_ID),
        InodeLockRequest::read(1, 42),
    ]);

    MasterFilesystem::filter_root_requests(&mut requests, false);

    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0], InodeLockRequest::read(1, 42));

    requests.insert(0, InodeLockRequest::write(0, ROOT_INODE_ID));
    MasterFilesystem::filter_root_requests(&mut requests, true);

    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], InodeLockRequest::write(0, ROOT_INODE_ID));
    assert_eq!(requests[1], InodeLockRequest::read(1, 42));
}

struct ResetChildrenCountPersistence;

impl Drop for ResetChildrenCountPersistence {
    fn drop(&mut self) {
        curvine_model::persist_children_count(false);
    }
}

#[test]
fn existing_mkdir_waits_for_target_inode_lock() {
    let fs = test_fs("mkdir-existing-inode-lock");
    let id = fs.mkdir("/dir", false).unwrap().id;
    let held = fs.inode_locks.write_inode(id);
    // An idle namespace guard does not mean inode locks are idle. An
    // exclusive fast path must not bypass an already admitted inode holder.
    assert!(!fs.fs_dir.is_cold());
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        tx.send(worker_fs.mkdir("/dir", true)).unwrap();
    });
    started_rx.recv().unwrap();
    let early = rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(held);
    let result = match early {
        Ok(result) => {
            worker.join().unwrap();
            panic!("mkdir bypassed the target inode lock: {result:?}");
        }
        Err(_) => rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
    };
    worker.join().unwrap();
    assert_eq!(result.unwrap().id, id);
}

#[test]
fn edge_plan_handles_survive_directory_replacement() {
    let fs = test_fs("edge-plan-directory-replacement");
    let old_id = fs.mkdir("/parent", false).unwrap().id;
    let old_child_id = fs.mkdir("/parent/child", false).unwrap().id;
    let root = fs.fs_dir.view().unwrap().root_node().children.clone();
    let (old_parent, _) = root.read_child_shared("parent");
    let old_children = old_parent.unwrap().children_handle().unwrap();

    fs.delete("/parent", true).unwrap();
    let new_id = fs.mkdir("/parent", false).unwrap().id;
    let new_child_id = fs.mkdir("/parent/child", false).unwrap().id;
    assert_ne!(old_id, new_id);
    assert_ne!(old_child_id, new_child_id);
    {
        let guard = fs.fs_dir.view().unwrap();
        assert!(MasterFilesystem::resolve_locked_path(
            &guard,
            "/parent/child",
            &[ROOT_INODE_ID, old_id, old_child_id],
            3,
        )
        .unwrap()
        .is_none());
    }
    let (old_child, _) = old_children.read_child_shared("child");
    assert_eq!(old_child.unwrap().id(), old_child_id);
    let (ids, components, full, _) = fs.resolve_edge_ids("/parent/child").unwrap();
    assert!(full);
    assert_eq!(components, ids.len());
    assert_eq!(&ids[1..], &[new_id, new_child_id]);
}

#[test]
fn edge_plan_reads_store_edges() {
    let fs = test_fs("edge-plan-store-snapshot");
    let id = fs.mkdir("/parent", false).unwrap().id;
    {
        // Deliberately diverge the live ID from the store to check the read
        // source; this is not a valid namespace state or a race detector.
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let path = MasterFilesystem::resolve_path(&guard, "/parent").unwrap();
        let inode = path.get_last_inode().unwrap();
        inode.as_mut().as_dir_mut().unwrap().id = id + 1;
    }
    let (ids, components, full, _) = fs.resolve_edge_ids("/parent").unwrap();
    assert!(full);
    assert_eq!(components, 2);
    assert_eq!(ids, [ROOT_INODE_ID, id]);
}

#[test]
fn edge_plan_walks_the_live_tree_after_a_create() {
    // The store edge for a just-created child can lag its live publication; a
    // live walk lets writers reach the new entry without waiting for a store
    // refresh. Both views stay consistent here because the create already
    // committed its store batch.
    let fs = test_fs("edge-plan-live-walk");
    fs.mkdir("/parent", false).unwrap();
    fs.mkdir("/parent/child", false).unwrap();
    let (ids, components, full, _) = fs.resolve_edge_ids("/parent/child").unwrap();
    assert!(full);
    assert_eq!(components, 3);
    assert!(ids.len() >= 2);
}

#[test]
fn exists_reads_topology_from_store_snapshot() {
    let fs = test_fs("exists-store-snapshot");
    fs.mkdir("/parent/child", true).unwrap();
    {
        // Deliberately detach the live subtree to check the read source; this
        // is not a valid namespace state or a concurrency test.
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let path = MasterFilesystem::resolve_path(&guard, "/parent").unwrap();
        let inode = path.get_last_inode().unwrap();
        let detached = inode.as_ref().clone_metadata();
        *inode.as_mut() = detached;
    }
    assert!(fs.exists("/parent/child").unwrap());
}

#[test]
fn edge_plan_stops_at_a_file_component() {
    let fs = test_fs("edge-plan-file-component");
    let file = fs.create("/file", true).unwrap();
    let (ids, components, full, _) = fs.resolve_edge_ids("/file/child").unwrap();
    assert!(!full);
    assert_eq!(components, 3);
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[1], file.id);
    let guard = fs.fs_dir.view().unwrap();
    assert!(
        MasterFilesystem::resolve_locked_path(&guard, "/file/child", &ids, components)
            .unwrap()
            .is_some()
    );
}

#[test]
fn namespace_validation_checks_first_missing_component() {
    let fs = test_fs("validate-first-missing-component");
    fs.mkdir("/parent", false).unwrap();
    let path = "/parent/missing/leaf";
    let (ids, components, full, _) = fs.resolve_edge_ids(path).unwrap();
    assert!(!full);
    {
        let guard = fs.fs_dir.view().unwrap();
        assert!(
            MasterFilesystem::resolve_locked_path(&guard, path, &ids, components)
                .unwrap()
                .is_some()
        );
    }

    fs.mkdir("/parent/missing", false).unwrap();
    let guard = fs.fs_dir.view().unwrap();
    assert!(
        MasterFilesystem::resolve_locked_path(&guard, path, &ids, components)
            .unwrap()
            .is_none()
    );
}

#[test]
fn topology_reads_wait_for_shared_namespace_writers() {
    let fs = test_fs("status-list-exclusive");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.create("/dir/file", true).unwrap();
    // Allocate a real block so the shared-view get_file_block_details has
    // content to report.
    let block = fs
        .add_block(
            "/dir/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let held = fs.fs_dir.view().unwrap();
    // Two-phase start: the cold workers spawn and confirm they are alive
    // (started ack), then main runs the shared-view get_lock on the held
    // guard, and only afterwards releases them. A plain barrier would let the
    // workers race ahead into a cold window and make get_lock read Busy.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let mut release_senders: Vec<std::sync::mpsc::Sender<()>> = Vec::new();
    let shared = fs.clone();
    // get_file_block_details (op 2) is a shared-view read since cb9ba3dae:
    // confirmed separately below, not a cold participant.
    let (tx, rx) = std::sync::mpsc::channel();
    let mut threads = Vec::new();
    let cold_ops: [u8; 5] = [3, 5, 6, 7, 8];
    for (index, &operation) in cold_ops.iter().enumerate() {
        let fs = fs.clone();
        let started_tx = started_tx.clone();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        release_senders.push(release_tx);
        let tx = tx.clone();
        threads.push(std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            let success = match operation {
                3 => fs.cv_metadata_snapshot_page(None, 10).is_ok(),
                5 => fs.list_trash_page(None, 10).is_ok(),
                6 => fs.scan_expired_trash(i64::MAX, 10, None).is_ok(),
                7 => fs.scan_pending_trash(10, None).is_ok(),
                8 => fs.sum_hash().is_ok(),
                _ => unreachable!("non-participant operation"),
            };
            tx.send((operation, success)).unwrap();
        }));
    }
    drop(started_tx);
    for _ in 0..5 {
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("all cold workers started");
    }
    // Shared-view reads succeed while the view is held, before any cold read
    // begins: get_lock (per-path locks) and get_file_block_details (snapshot).
    fs.get_lock("/dir/file", FileLock::default())
        .expect("get_lock must succeed on the shared view");
    let details = shared
        .get_file_block_details("/dir/file")
        .expect("get_file_block_details must succeed on the shared view");
    assert!(details
        .blocks
        .iter()
        .any(|detail| detail.block_id == block.block.id));
    // Release the cold workers; the held guard must drain them all.
    for sender in release_senders.iter() {
        sender.send(()).unwrap();
    }
    release_senders.clear();
    let mut early = Vec::new();
    while let Ok(result) = rx.recv_timeout(std::time::Duration::from_millis(100)) {
        early.push(result);
    }
    assert!(
        early.is_empty(),
        "cold reads completed while the view was held: {early:?}"
    );
    drop(held);
    for thread in threads {
        thread.join().unwrap();
    }
    for _ in 0..5 {
        let (op, success) = rx.recv().unwrap();
        assert!(success, "cold read op {op} failed");
    }
}

#[test]
fn path_status_uses_one_snapshot_without_exclusive_namespace_lock() {
    let fs = test_fs("path-status-snapshot");
    fs.create("/dir/file", true).unwrap();
    fs.mkdir("/dir/sub", true).unwrap();
    fs.set_attr("/dir", SetAttrOptsBuilder::new().mode(0o700).build())
        .unwrap();

    let held = fs.fs_dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || tx.send(worker_fs.file_status("/dir")).unwrap());
    let result = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    drop(held);
    worker.join().unwrap();

    let status = result.unwrap();
    assert!(status.is_dir);
    assert_eq!(status.mode, 0o700);
    assert_eq!(status.children_num, 2);
    assert_eq!(status.len, 2);
}

#[test]
fn path_status_reads_inode_from_store_snapshot() {
    let fs = test_fs("status-store-snapshot");
    fs.mkdir("/dir", false).unwrap();
    fs.set_attr("/dir", SetAttrOptsBuilder::new().mode(0o700).build())
        .unwrap();
    {
        // Simulate a live view from a different generation. Path status must
        // not combine it with edges or directory attributes from the store.
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let path = MasterFilesystem::resolve_path(&guard, "/dir").unwrap();
        let inode = path.get_last_inode().unwrap();
        inode
            .as_mut()
            .set_attr(SetAttrOptsBuilder::new().mode(0o755).build())
            .unwrap();
    }
    assert_eq!(fs.file_status("/dir").unwrap().mode, 0o700);
    assert_eq!(fs.list_status("/").unwrap()[0].mode, 0o700);
    assert_eq!(
        fs.list_options("/", ListOptions::with_limit(1)).unwrap()[0].mode,
        0o700
    );
}

#[test]
fn path_status_remains_valid_during_metadata_replacement() {
    let fs = test_fs("status-metadata-replacement");
    fs.mkdir("/dir", false).unwrap();
    fs.set_attr("/dir", SetAttrOptsBuilder::new().mode(0o700).build())
        .unwrap();
    let writer_fs = fs.clone();
    let start = Arc::new(std::sync::Barrier::new(2));
    let writer_start = start.clone();
    let writer = std::thread::spawn(move || {
        writer_start.wait();
        for i in 0..200 {
            let mode = if i % 2 == 0 { 0o755 } else { 0o700 };
            writer_fs
                .set_attr("/dir", SetAttrOptsBuilder::new().mode(mode).build())
                .unwrap();
        }
    });
    start.wait();
    for _ in 0..200 {
        let status = fs.file_status("/dir").unwrap();
        assert!(status.mode == 0o700 || status.mode == 0o755);
        assert!(status.is_dir);
    }
    writer.join().unwrap();
}

#[test]
fn path_status_uses_persisted_child_count_after_namespace_mutations() {
    let _guard = CHILDREN_COUNT_PERSISTENCE.lock().unwrap();
    let _reset = ResetChildrenCountPersistence;
    curvine_model::persist_children_count(true);

    let fs = test_fs("path-status-persisted-child-count");
    fs.create("/dir/file", true).unwrap();
    fs.mkdir("/dir/sub", true).unwrap();
    let dir = fs.file_status("/dir").unwrap();
    assert_eq!((dir.children_num, dir.len), (2, 2));

    let attributes = fs
        .fs_dir
        .view()
        .unwrap()
        .store
        .store
        .get_directory_attributes(dir.id)
        .unwrap()
        .unwrap();
    assert_eq!(attributes.children(), Some(2));

    fs.delete("/dir/file", false).unwrap();
    fs.rename("/dir/sub", "/dir/renamed", RenameFlags::empty())
        .unwrap();
    fs.mkdir("/dir/replaced", true).unwrap();
    fs.rename("/dir/renamed", "/dir/replaced", RenameFlags::empty())
        .unwrap();
    fs.mkdir("/outside", true).unwrap();
    fs.rename("/dir/replaced", "/outside/moved", RenameFlags::empty())
        .unwrap();
    let dir = fs.file_status("/dir").unwrap();
    assert_eq!((dir.children_num, dir.len), (0, 0));
    assert_eq!(
        fs.fs_dir
            .view()
            .unwrap()
            .store
            .store
            .get_directory_attributes(dir.id)
            .unwrap()
            .unwrap()
            .children(),
        Some(0)
    );
    assert_eq!(
        fs.file_status("/outside").unwrap().children_num,
        1,
        "cross-directory rename must add one destination child"
    );

    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore_from_rocksdb()
        .unwrap();
    assert_eq!(fs.file_status("/dir").unwrap().children_num, 0);
    assert_eq!(fs.file_status("/outside").unwrap().children_num, 1);

    // Existing v4 records must fail closed when legacy deltas are written
    // after disabling the format, including across restore and setattr.
    curvine_model::persist_children_count(false);
    fs.create("/outside/second", false).unwrap();
    let outside = fs.file_status("/outside").unwrap();
    assert_eq!(outside.children_num, 2);
    assert_eq!(
        fs.fs_dir
            .view()
            .unwrap()
            .store
            .store
            .get_directory_attributes(outside.id)
            .unwrap()
            .unwrap()
            .children(),
        None
    );
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore_from_rocksdb()
        .unwrap();
    fs.set_attr("/outside", SetAttrOptsBuilder::new().mode(0o700).build())
        .unwrap();
    assert_eq!(fs.file_status("/outside").unwrap().children_num, 2);
}

#[test]
fn cross_parent_rename_tracks_live_and_stored_child_counts() {
    let _guard = CHILDREN_COUNT_PERSISTENCE.lock().unwrap();
    let _reset = ResetChildrenCountPersistence;
    for enabled in [false, true] {
        curvine_model::persist_children_count(enabled);
        let fs = test_fs(if enabled {
            "rename-count-v4"
        } else {
            "rename-count-legacy"
        });
        fs.create("/a/file", true).unwrap();
        fs.mkdir("/b", false).unwrap();
        for _ in 0..3 {
            for (src, dst, expected_a, expected_b) in
                [("/a/file", "/b/file", 0, 1), ("/b/file", "/a/file", 1, 0)]
            {
                fs.rename(src, dst, RenameFlags::empty()).unwrap();
                for (path, expected) in [("/a", expected_a), ("/b", expected_b)] {
                    assert_eq!(fs.file_status(path).unwrap().children_num, expected);
                    let guard = fs.fs_dir.view().unwrap();
                    let inode_path = MasterFilesystem::resolve_path(&guard, path).unwrap();
                    let parent = inode_path.get_last_inode().unwrap();
                    assert_eq!(
                        parent.as_dir_ref().unwrap().directory_attributes().children,
                        Some(expected)
                    );
                }
            }
        }
        fs.create("/b/target", false).unwrap();
        fs.rename("/a/file", "/b/target", RenameFlags::empty())
            .unwrap();
        for (path, expected) in [("/a", 0), ("/b", 1)] {
            assert_eq!(fs.file_status(path).unwrap().children_num, expected);
            let guard = fs.fs_dir.view().unwrap();
            let inode_path = MasterFilesystem::resolve_path(&guard, path).unwrap();
            let parent = inode_path.get_last_inode().unwrap();
            assert_eq!(
                parent.as_dir_ref().unwrap().directory_attributes().children,
                Some(expected)
            );
        }
        fs.rename("/b/target", "/a/file", RenameFlags::empty())
            .unwrap();
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .restore_from_rocksdb()
            .unwrap();
        assert_eq!(fs.file_status("/a").unwrap().children_num, 1);
        assert_eq!(fs.file_status("/b").unwrap().children_num, 0);
    }
}

#[test]
fn same_parent_directory_replacement_persists_link_count() {
    let _guard = CHILDREN_COUNT_PERSISTENCE.lock().unwrap();
    let _reset = ResetChildrenCountPersistence;
    for enabled in [false, true] {
        curvine_model::persist_children_count(enabled);
        let fs = test_fs(if enabled {
            "replace-dir-v4"
        } else {
            "replace-dir-legacy"
        });
        fs.mkdir("/p/a", true).unwrap();
        fs.mkdir("/p/b", false).unwrap();
        assert_eq!(fs.file_status("/p").unwrap().nlink, 4);
        fs.rename("/p/a", "/p/b", RenameFlags::empty()).unwrap();
        let status = fs.file_status("/p").unwrap();
        assert_eq!((status.nlink, status.children_num), (3, 1));
        {
            let guard = fs.fs_dir.view().unwrap();
            let path = MasterFilesystem::resolve_path(&guard, "/p").unwrap();
            let parent = path.get_last_inode().unwrap();
            let attributes = parent.as_dir_ref().unwrap().directory_attributes();
            assert_eq!((attributes.nlink, attributes.children), (3, Some(1)));
        }
        fs.fs_dir
            .begin_cold(DEFAULT_COLD_TIMEOUT)
            .unwrap()
            .restore_from_rocksdb()
            .unwrap();
        let status = fs.file_status("/p").unwrap();
        assert_eq!((status.nlink, status.children_num), (3, 1));
        fs.delete("/p/b", false).unwrap();
        let status = fs.file_status("/p").unwrap();
        assert_eq!((status.nlink, status.children_num), (2, 0));
    }
}

#[test]
fn directory_publication_failure_fences_subsequent_writes() {
    let fs = test_fs("directory-publication-fence");
    fs.mkdir("/parent", false).unwrap();
    {
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let path = MasterFilesystem::resolve_path(&guard, "/parent").unwrap();
        let node = path.get_last_dir_node().expect("live dir node");
        let mut attributes = node.directory_attributes();
        attributes.version = u64::MAX;
        node.set_attribute_values(attributes);
    }

    let error = fs.create("/parent/child", false).unwrap_err();
    assert!(error.to_string().contains("version overflow"));
    // The direct snapshot read demonstrates that the store batch committed;
    // the live tree did not publish the child, the writer is fenced, and the
    // no-snapshot rebuild reconciles the store on restart.
    {
        let guard = fs.fs_dir.view().unwrap();
        let lease = guard.begin_snapshot_scan().unwrap();
        let resolved = lease
            .snapshot_reader()
            .unwrap()
            .resolve_path("/parent/child")
            .unwrap();
        assert!(resolved.is_full(), "the store batch committed the child");
    }
    assert!(fs.journal_writer.ensure_metadata_current().is_err());
    assert!(!fs.exists("/parent/child").unwrap());
    assert!(fs.mkdir("/another", false).is_err());
}

#[test]
fn child_publication_failure_after_delete_fences_writes() {
    let fs = test_fs("child-publication-fence");
    fs.create("/parent/child", true).unwrap();
    {
        let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let path = MasterFilesystem::resolve_path(&guard, "/parent/child").unwrap();
        let id = path.get_last_inode().unwrap().id();
        let mut parent = path.get_inode(-2).unwrap();
        // Simulate a stale live edge after resolution while retaining the
        // persisted edge. The store deletion commits before publication fails.
        parent.delete_child(id, "child").unwrap();
        let error = guard.unprotected_delete(&path, 1).unwrap_err();
        assert!(error.to_string().contains("not exists"));
    }
    assert!(!fs.exists("/parent/child").unwrap());
    assert!(fs.journal_writer.ensure_metadata_current().is_err());
    assert!(fs.mkdir("/another", false).is_err());
}

#[cfg(feature = "fault-injection")]
#[test]
fn purge_child_publication_failure_fences_writes() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("purge-child-publication-fence");
    let name = format!("child-{}", curvine_runtime::common::Utils::rand_str(12));
    let path = format!("/parent/{name}");
    let mut worker = WorkerInfo::default();
    worker.address.worker_id = 100;
    fs.add_test_worker(worker);
    fs.create_with_opts(
        &path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(1)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let block_id = fs
        .add_block(
            &path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap()
        .block
        .id;
    let location = BlockLocation::with_id(100);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .add_block_location(block_id, location.clone())
        .unwrap();
    fs.complete_file(
        path.as_str(),
        None,
        4096,
        vec![CommitBlock {
            block_id,
            block_len: 4096,
            locations: vec![location],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    {
        let inode_id = fs.file_status(&path).unwrap().id;
        let guard = fs.fs_dir.view().unwrap();
        let inode = guard.store.get_inode(inode_id, None).unwrap();
        let locs = inode
            .unwrap()
            .as_file_ref()
            .unwrap()
            .get_locs(&guard.store)
            .unwrap();
        assert_eq!(locs.get(&block_id).unwrap().len(), 1);
    }
    fs.master_monitor
        .set_role_for_test(curvine_raft::raft::RoleState::Leader);
    let runtime = FaultRuntime::process();
    let rule_id = format!("purge-publication-{name}");
    let rule = FaultRuleBuilder::named("master.metadata.purge.before_child_publication")
        .matches("child_name", name.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected purge publication failure")
        .unwrap();
    runtime.configure(&rule_id, rule).unwrap();
    let result = fs.purge_trash_batch("/parent", std::slice::from_ref(&path));
    runtime.remove(&rule_id).unwrap();
    // Keep the original FsError: the injected fault produces a Common error,
    // and its variant must survive the exclusive publication failure path.
    let purge_error = result.expect_err("the injected publication failure must surface");
    assert!(
        matches!(&purge_error, curvine_error::FsError::Common(_)),
        "the publication error must keep its original Common variant, got {purge_error:?}"
    );
    assert!(
        purge_error
            .to_string()
            .contains("injected purge publication failure"),
        "the publication error must keep its original message, got {purge_error}"
    );
    // The exclusive confirmation failure (seq moved) went terminal: the
    // holder is FAILED and all subsequent access reports Unavailable.
    assert!(
        fs.fs_dir.is_failed(),
        "a publication failure in the exclusive path must fail the holder"
    );
    assert!(
        matches!(
            fs.fs_dir.view().err(),
            Some(crate::master::meta::NamespaceError::Unavailable)
        ),
        "the view must be Unavailable after the terminal failure"
    );
    assert!(
        matches!(
            fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).err(),
            Some(ColdError::Unavailable)
        ),
        "begin_cold must be Unavailable after the terminal failure"
    );
    assert!(fs.journal_writer.ensure_metadata_current().is_err());
    assert!(fs.mkdir("/another", false).is_err());
    // Independent of the namespace state: a failed purge must not have queued
    // any worker block deletion for this worker.
    assert!(
        fs.worker_manager
            .write()
            .block_map
            .handle_heartbeat(100)
            .is_empty(),
        "a failed purge publication must not queue worker block deletions"
    );
}

#[test]
fn delete_and_rename_publication_failures_fence_writes() {
    for operation in ["delete", "rename"] {
        let fs = test_fs(&format!("directory-publication-{operation}"));
        fs.create("/parent/child", true).unwrap();
        {
            let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
            let path = MasterFilesystem::resolve_path(&guard, "/parent").unwrap();
            let node = path.get_last_dir_node().expect("live dir node");
            let mut attributes = node.directory_attributes();
            attributes.version = u64::MAX;
            node.set_attribute_values(attributes);
        }
        let result = if operation == "delete" {
            fs.delete("/parent/child", false).map(|_| ())
        } else {
            fs.rename("/parent/child", "/parent/moved", RenameFlags::empty())
                .map(|_| ())
        };
        assert!(result.unwrap_err().to_string().contains("version overflow"));
        assert!(fs.journal_writer.ensure_metadata_current().is_err());
        assert!(fs.mkdir("/another", false).is_err());
    }
}

#[test]
fn rejected_namespace_operation_does_not_fence_writes() {
    let fs = test_fs("directory-no-publication-fence");
    fs.create("/parent/child", true).unwrap();
    assert!(fs.mkdir("/parent/child", false).is_err());
    assert!(fs.journal_writer.ensure_metadata_current().is_ok());
    assert!(fs.mkdir("/another", false).is_ok());
}

#[test]
fn directory_space_query_resolves_with_a_shared_namespace_guard() {
    use crate::master::space::{DirSpaceManager, DirSpaceQueryError};
    use crate::master::MountManager;

    let fs = test_fs("space-query-snapshot");
    fs.mkdir("/dir", true).unwrap();
    let mut conf = (*fs.conf).clone();
    conf.dir_space.enabled = true;
    let manager = DirSpaceManager::new(
        fs.clone(),
        Arc::new(MountManager::new(fs.clone())),
        Arc::new(conf),
    );
    let guard = fs.fs_dir.view().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || tx.send(manager.query("/dir")).unwrap());
    let result = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(guard);
    worker.join().unwrap();
    assert!(matches!(
        result.unwrap(),
        Err(DirSpaceQueryError::SnapshotMiss(_))
    ));
}

#[test]
fn status_by_id_reads_owned_file_and_directory_without_exclusive_namespace_lock() {
    let fs = test_fs("status-by-id-snapshot");
    let file = fs.create("/dir/file", true).unwrap();
    let directory = fs
        .set_attr("/dir", SetAttrOptsBuilder::new().mode(0o700).build())
        .unwrap();
    let guard = fs.fs_dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send((
            worker_fs.file_status_by_id("/old/file", Some(file.id)),
            worker_fs.file_status_by_id("/old/dir", Some(directory.id)),
        ))
        .unwrap();
    });
    let result = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(guard);
    worker.join().unwrap();
    let (file, directory) = result.unwrap();
    assert_eq!(file.unwrap().path, "/old/file");
    let directory = directory.unwrap();
    assert_eq!(directory.path, "/old/dir");
    assert_eq!(directory.mode, 0o700);
}

#[test]
fn inode_path_uses_the_same_snapshot_as_file_metadata() {
    let fs = test_fs("inode-path-snapshot");
    let file = fs.create("/old/file", true).unwrap();
    fs.mkdir("/new", true).unwrap();
    let lease = fs.fs_dir.view().unwrap().begin_snapshot_scan().unwrap();
    let snapshot = lease.snapshot_reader().unwrap();
    fs.rename("/old/file", "/new/file", RenameFlags::empty())
        .unwrap();
    let old_inode = snapshot.get_inode(file.id).unwrap();
    assert_eq!(old_inode.unwrap().name(), "file");
    assert_eq!(
        FsDir::inode_path_in_snapshot(&snapshot, file.id).unwrap(),
        "/old/file"
    );
    assert_eq!(
        fs.fs_dir.view().unwrap().get_inode_path(file.id).unwrap(),
        "/new/file"
    );
}

#[test]
fn link_and_symlink_do_not_wait_for_shared_namespace_readers() {
    let fs = test_fs("link-shared-reader");
    fs.create("/source", true).unwrap();
    let held = fs.fs_dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = worker_fs
            .link("/source", "/hard")
            .and_then(|_| worker_fs.symlink("/source", "/sym", false, 0o777));
        tx.send(result).unwrap();
    });
    let outer = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(held);
    // Both layers must succeed: the channel delivery and the link+symlink
    // business result itself.
    let inner = outer.expect("worker must report within the bound");
    worker.join().expect("worker must not panic");
    inner.expect("link and symlink must succeed on the shared view");
    assert_eq!(
        fs.file_status("/hard").unwrap().id,
        fs.file_status("/source").unwrap().id,
        "the hardlink must share the source inode id"
    );
    assert_eq!(
        fs.file_status("/sym").unwrap().target.as_deref(),
        Some("/source"),
        "the symlink must target the source path"
    );
}

#[test]
fn mkdir_checks_existing_targets_and_recursive_parents() {
    let fs = test_fs("mkdir-snapshot-precheck");
    fs.create("/file", true).unwrap();
    assert!(fs.mkdir("/file", true).is_err());
    assert!(fs.mkdir("/", true).is_err());
    assert!(fs.mkdir("/missing/child", false).is_err());
    fs.mkdir("/missing/child", true).unwrap();
    assert!(fs.mkdir("/missing/child", true).is_ok());
    assert!(fs.mkdir("/missing/child", false).is_err());
    assert!(fs
        .set_attr(
            "/missing/no-such-file",
            SetAttrOptsBuilder::new().mode(0o600).build()
        )
        .is_err());
}

#[cfg(feature = "fault-injection")]
#[test]
fn open_missing_path_fallback_excludes_namespace_readers() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("open-missing-exclusive");
    fs.mkdir("/parent", false).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "open-missing-exclusive";
    let rule = FaultRuleBuilder::named("master.metadata.open_fallback")
        .matches("path", "/parent/missing")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.fs_dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(worker_fs.open_file(
            "/parent/missing",
            CreateFileOpts::with_create(false),
            OpenFlags::new_write_only(),
        ))
        .unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let early = rx.recv_timeout(Duration::from_millis(100));
    drop(held);
    let blocked = early.is_err();
    let result = match early {
        Ok(result) => result,
        Err(_) => rx.recv_timeout(Duration::from_secs(5)).unwrap(),
    };
    worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "open did not reach the fallback");
    assert!(blocked, "fallback did not exclude namespace readers");
    assert!(result.is_err());
}

#[test]
fn create_and_open_preserve_path_and_flag_validation() {
    let fs = test_fs("create-open-snapshot-precheck");
    assert!(fs.create("/missing/file", false).is_err());
    fs.create("/parent/file", true).unwrap();
    assert!(fs.create("/parent/file/child", false).is_err());
    assert!(fs.create("/parent", true).is_err());
    assert!(fs
        .create_with_opts(
            "/parent/file",
            CreateFileOpts::with_create(false),
            OpenFlags::new_create().set_exclusive(true),
        )
        .is_err());
    assert!(fs
        .open_file(
            "/parent",
            CreateFileOpts::with_create(false),
            OpenFlags::new_write_only(),
        )
        .is_err());
    assert!(fs
        .open_file(
            "/missing-file",
            CreateFileOpts::with_create(false),
            OpenFlags::new_write_only(),
        )
        .is_err());
}

#[test]
fn existing_create_errors_match_global_path_without_namespace_write_lock() {
    let fs = test_fs("create-existing-errors-shared-reader");
    fs.create("/parent/file", true).unwrap();
    let cases = [
        ("/parent/file", OpenFlags::new_create().set_exclusive(true)),
        ("/parent/file", OpenFlags::new_create().set_overwrite(false)),
        ("/parent", OpenFlags::new_create().set_overwrite(true)),
    ];
    let held = fs.fs_dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let errors: Vec<_> = cases
            .iter()
            .map(|(path, flags)| {
                worker_fs
                    .create_with_opts(path, CreateFileOpts::with_create(false), *flags)
                    .unwrap_err()
            })
            .collect();
        tx.send(errors).unwrap();
    });
    let early = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(held);
    worker.join().unwrap();
    let errors = early.expect("existing creates must not require namespace write lock");
    assert!(matches!(errors[0], FsError::FileAlreadyExists(_)));
    assert!(matches!(errors[1], FsError::FileAlreadyExists(_)));
    assert!(matches!(errors[2], FsError::Common(_)));
    assert!(errors[0]
        .to_string()
        .contains("/parent/file  already exists"));
    assert!(errors[1]
        .to_string()
        .contains("/parent/file  already exists"));
    assert!(errors[2]
        .to_string()
        .contains("/parent  already exists as a dir"));
    for ((path, flags), error) in cases.iter().zip(errors) {
        let expected = fs
            .run_metadata_write_with_fallback(
                "shared_handles_probe",
                |_epoch| Ok(ShardedOutcome::Fallback),
                |fs_dir| {
                    fs.create_cold_body(fs_dir, path, CreateFileOpts::with_create(false), *flags)
                },
            )
            .unwrap_err();
        assert_eq!(
            std::mem::discriminant(&error),
            std::mem::discriminant(&expected)
        );
    }
}

#[test]
fn rename_does_not_wait_for_shared_namespace_readers() {
    let fs = test_fs("rename-shared-reader");
    fs.create("/source", true).unwrap();
    let held = fs.fs_dir.view().unwrap();
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(worker_fs.rename("/source", "/target", RenameFlags::empty()))
            .unwrap();
    });
    let result = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(held);
    worker.join().unwrap();
    assert!(result.unwrap().unwrap());
    assert!(!fs.exists("/source").unwrap());
    assert!(fs.exists("/target").unwrap());
}

#[test]
fn free_rejects_files_in_the_recycle_bin() {
    let fs = test_fs_with_trash("free-reserved-file");
    fs.create("/file", true).unwrap();
    fs.complete_file("/file", None, 0, vec![], "", false, None)
        .unwrap();
    fs.delete_with_mode("/file", false, DeleteMode::Default)
        .unwrap();
    let entry = fs
        .list_options(trash::TRASH_ROOT, ListOptions::default())
        .unwrap();
    let path = format!("{}/{}", trash::TRASH_ROOT, entry[0].name);
    let id = fs.file_status(&path).unwrap().id;
    for recursive in [false, true] {
        let error = fs.free(&path, recursive).unwrap_err();
        assert!(error.to_string().contains("reserved for the recycle bin"));
        assert_eq!(fs.file_status(&path).unwrap().id, id);
    }
}

#[test]
fn filesystem_handles_share_worker_and_lifecycle_state() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("shared-fs-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let first = journal.fs();
    let second = MasterFilesystem::with_js(&conf, &journal);
    assert!(Arc::ptr_eq(
        &first.worker_location_locks,
        &second.worker_location_locks
    ));
    assert!(Arc::ptr_eq(
        &first.block_location_locks,
        &second.block_location_locks
    ));
    assert!(Arc::ptr_eq(
        &first.full_block_reconciles,
        &second.full_block_reconciles
    ));
    assert!(Arc::ptr_eq(&first.pending_drain, &second.pending_drain));
    assert!(Arc::ptr_eq(
        &first.fs_mode_releaser,
        &second.fs_mode_releaser
    ));
    let held = first.worker_location_lock(42).lock();
    assert!(second.worker_location_lock(42).try_lock().is_none());
    drop(held);
    assert!(second.worker_location_lock(42).try_lock().is_some());

    let partial = BlockReportList {
        cluster_id: String::new(),
        worker_id: 42,
        full_report: true,
        total_len: 2,
        report_id: 17,
        blocks: vec![BlockReportInfo::new(
            1,
            BlockReportStatus::Finalized,
            StorageType::Disk,
            1,
        )],
    };
    assert!(matches!(
        first.full_report_tracker.collect(&partial, 1),
        FullReportProgress::Partial
    ));
    let mut last = partial;
    last.blocks = vec![BlockReportInfo::new(
        2,
        BlockReportStatus::Finalized,
        StorageType::Disk,
        1,
    )];
    assert!(
        matches!(second.full_report_tracker.collect(&last, 1), FullReportProgress::Complete(ids) if ids.len() == 2)
    );
    journal.shutdown();
}
