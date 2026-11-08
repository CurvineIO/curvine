use super::*;

#[cfg(feature = "fault-injection")]
#[test]
fn delete_reloads_inode_after_a_hard_link_is_added() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("delete-reload-hardlink");
    let original = fs.create("/a/file", true).unwrap();
    fs.mkdir("/b", false).unwrap();
    fs.mkdir("/c", false).unwrap();
    fs.link("/a/file", "/b/alias").unwrap();
    let parent = fs.file_status("/a").unwrap().id;
    let runtime = FaultRuntime::process();
    let rule_id = "delete-reload-hardlink";
    let rule = FaultRuleBuilder::named("master.metadata.delete_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(parent);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.delete("/a/file", false));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let link_result = if planned {
        Some(fs.link("/b/alias", "/c/alias"))
    } else {
        None
    };
    drop(held);
    let delete_result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "delete did not reach lock planning");
    link_result.unwrap().unwrap();
    delete_result.unwrap();
    fs.delete("/b/alias", false).unwrap();
    assert_eq!(fs.file_status("/c/alias").unwrap().id, original.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/c/alias").unwrap().id, original.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn overwrite_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("overwrite-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "overwrite-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.overwrite_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.create("/a/file", true));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "overwrite did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(result.unwrap().id, replacement.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/a/file").unwrap().id, replacement.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn close_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("close-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "close-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.close_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.close_file("/a/file", ""));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "close did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    result.unwrap();
    let status = fs.file_status("/a/file").unwrap();
    assert_eq!(status.id, replacement.id);
    assert!(status.is_complete);
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.file_status("/a/file").unwrap().is_complete);
}

#[cfg(feature = "fault-injection")]
#[test]
fn truncate_open_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("truncate-open-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "truncate-open-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.overwrite_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.open_file(
            "/a/file",
            CreateFileOpts::with_create(false),
            OpenFlags::new_write_only().set_truncate(true),
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "overwrite did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(result.unwrap().status.id, replacement.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/a/file").unwrap().id, replacement.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn reopen_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("reopen-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "reopen-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.reopen_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.open_file(
            "/a/file",
            CreateFileOpts::with_create(false),
            OpenFlags::new_write_only(),
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "reopen did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(result.unwrap().status.id, replacement.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/a/file").unwrap().id, replacement.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn block_locations_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("locations-read-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "locations-read-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.locations_read_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.get_block_locations("/a/file"));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "block location read did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(result.unwrap().status.id, replacement.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/a/file").unwrap().id, replacement.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn complete_by_id_survives_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("complete-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    fs.mkdir("/other", false).unwrap();
    fs.link("/a/file", "/other/old").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "complete-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.complete_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.complete_file("/a/file", Some(old.id), 0, vec![], "", false, None)
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "complete did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    result.unwrap();
    assert!(fs.file_status("/other/old").unwrap().is_complete);
    assert!(!fs.file_status("/a/file").unwrap().is_complete);
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.file_status("/other/old").unwrap().is_complete);
    assert!(!fs.file_status("/a/file").unwrap().is_complete);
}

#[cfg(feature = "fault-injection")]
#[test]
fn free_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("free-replaced-parent");
    let old = fs.create("/a/file", true).unwrap();
    fs.complete_file("/a/file", None, 0, vec![], "", false, None)
        .unwrap();
    fs.set_attr(
        "/a/file",
        SetAttrOptsBuilder::new().ufs_mtime(1).ttl_action(TtlAction::Free).build(),
    )
    .unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "free-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.locations_sampled")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.free("/a/file", false));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        let replacement = fs.create("/a/file", true).unwrap();
        fs.complete_file("/a/file", None, 0, vec![], "", false, None)
            .unwrap();
        fs.set_attr(
            "/a/file",
            SetAttrOptsBuilder::new().ufs_mtime(1).ttl_action(TtlAction::Free).build(),
        )
        .unwrap();
        Some(replacement)
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "free did not reach location sampling");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(result.unwrap().inodes, 1);
    assert_eq!(fs.file_status("/a/file").unwrap().id, replacement.id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/a/file").unwrap().id, replacement.id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn add_block_by_id_survives_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("add-block-replaced-parent");
    let old_block = replication_file(&fs, "/a/file");
    let old = fs.file_status("/a/file").unwrap();
    fs.mkdir("/other", false).unwrap();
    fs.link("/a/file", "/other/old").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "add-block-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.add_block_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.add_block(
            "/a/file",
            Some(old.id),
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "add_block did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    assert_eq!(result.unwrap().block.id, old_block);
    assert!(fs.get_block_locations("/a/file").unwrap().block_locs.is_empty());
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/other/old").unwrap().id, old.id);
    assert!(fs.get_block_locations("/a/file").unwrap().block_locs.is_empty());
}

#[test]
fn concurrent_overwrites_of_the_same_target_keep_one_consistent_file_version() {
    let fs = test_fs("overwrite-same-target-race");
    let block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();

    let start = Arc::new(std::sync::Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let fs = fs.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                fs.create("/file", true)
            })
        })
        .collect::<Vec<_>>();
    start.wait();

    for worker in workers {
        worker.join().unwrap().unwrap();
    }

    assert_eq!(fs.file_status("/file").unwrap().len, 0);
    let commands = fs.worker_manager.write().block_map.handle_heartbeat(100);
    let deleted = commands
        .iter()
        .flat_map(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.iter(),
        })
        .copied()
        .collect::<HashSet<_>>();
    assert!(deleted.contains(&block_id));
}

#[test]
fn overwrite_is_rejected_during_metadata_restore_generation() {
    let fs = test_fs("overwrite-restore-generation");
    let block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let before = fs.file_status("/file").unwrap();
    let restore_guard = fs
        .fs_dir
        .read()
        .store
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();

    let result = fs.create("/file", true);

    assert!(result.is_err());
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
    assert_eq!(
        fs.fs_dir
            .read()
            .store
            .get_locations(block_id)
            .unwrap()
            .len(),
        1
    );
    drop(restore_guard);
    fs.create("/file", true).unwrap();
    assert_eq!(fs.file_status("/file").unwrap().len, 0);
}
#[test]
fn complete_file_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("complete-file-shared-reader");
    fs.create("/file", true).unwrap();

    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.complete_file("/file", None, 0, vec![], "", false, None))
    });

    let result = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("sharded complete_file was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
    assert!(result.is_none());
    assert!(fs.file_status("/file").unwrap().is_complete);
}

#[test]
fn complete_file_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("complete-file-block-location-lock");
    let block_id = replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.complete_file(
            "/file",
            None,
            1,
            vec![CommitBlock {
                block_id,
                block_len: 1,
                locations: vec![BlockLocation::with_id(100)],
            }],
            "",
            false,
            None,
        ))
    });

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    assert!(done_rx.recv().unwrap().unwrap().is_none());
    assert!(fs.file_status("/file").unwrap().is_complete);
}

#[test]
fn close_file_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("close-file-shared-reader");
    fs.create("/file", true).unwrap();
    let client = {
        let dir = fs.fs_dir.read();
        MasterFilesystem::resolve_path(&dir, "/file")
            .unwrap()
            .get_last_inode()
            .unwrap()
            .as_file_ref()
            .unwrap()
            .features
            .file_write
            .as_ref()
            .unwrap()
            .clients
            .iter()
            .next()
            .unwrap()
            .clone()
    };

    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.close_file("/file", &client)));
    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("sharded close_file was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn reopen_file_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("reopen-file-shared-reader");
    fs.create("/file", true).unwrap();
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.open_file(
            "/file",
            CreateFileOpts::with_create(false),
            OpenFlags::new_append(),
        ))
    });

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("reopen was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn overwrite_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("overwrite-shared-reader");
    fs.create("/file", true).unwrap();
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.create("/file", true)));

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("sharded overwrite was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn overwrite_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("overwrite-block-location-lock");
    let block_id = replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.create("/file", true)));

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    done_rx.recv().unwrap().unwrap();
}

#[test]
fn overwrite_queues_old_worker_blocks_after_commit() {
    let fs = test_fs("overwrite-worker-cleanup");
    let old_block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id: old_block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();

    fs.create("/file", true).unwrap();
    let commands = fs.worker_manager.write().block_map.handle_heartbeat(100);
    let deleted = commands
        .iter()
        .flat_map(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.iter(),
        })
        .copied()
        .collect::<HashSet<_>>();
    assert!(deleted.contains(&old_block_id));
    assert_eq!(fs.file_status("/file").unwrap().len, 0);
}

#[test]
fn truncate_open_queues_old_worker_blocks_after_commit() {
    let fs = test_fs("truncate-open-worker-cleanup");
    let old_block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id: old_block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();

    fs.open_file(
        "/file",
        CreateFileOpts::with_create(false),
        OpenFlags::new_write_only().set_truncate(true),
    )
    .unwrap();

    let commands = fs.worker_manager.write().block_map.handle_heartbeat(100);
    let deleted = commands
        .iter()
        .flat_map(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.iter(),
        })
        .copied()
        .collect::<HashSet<_>>();
    assert!(deleted.contains(&old_block_id));
    assert_eq!(fs.file_status("/file").unwrap().len, 0);
}

#[cfg(feature = "fault-injection")]
#[test]
fn overwrite_commit_failure_keeps_file_state_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("overwrite-commit-failure");
    let old_block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id: old_block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let before = fs.file_status("/file").unwrap();
    assert_eq!(before.len, 1);

    let runtime = FaultRuntime::process();
    let rule_id = "overwrite-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.overwrite.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected overwrite commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.create("/file", true);
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected overwrite commit failure"));
    assert_eq!(fs.file_status("/file").unwrap().id, before.id);
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
    assert_eq!(
        fs.fs_dir
            .read()
            .store
            .get_locations(old_block_id)
            .unwrap()
            .len(),
        1
    );

    fs.restore_from_rocksdb().unwrap();
    let after_restore = fs.file_status("/file").unwrap();
    assert_eq!(after_restore.id, before.id);
    assert_eq!(after_restore.len, before.len);
}

#[cfg(feature = "fault-injection")]
#[test]
fn truncate_open_commit_failure_keeps_file_state_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("truncate-open-commit-failure");
    let old_block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id: old_block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let before = fs.file_status("/file").unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "truncate-open-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.overwrite.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected truncate commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.open_file(
        "/file",
        CreateFileOpts::with_create(false),
        OpenFlags::new_write_only().set_truncate(true),
    );
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected truncate commit failure"));
    assert_eq!(fs.file_status("/file").unwrap().id, before.id);
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
    assert_eq!(
        fs.fs_dir
            .read()
            .store
            .get_locations(old_block_id)
            .unwrap()
            .len(),
        1
    );

    fs.restore_from_rocksdb().unwrap();
    let after_restore = fs.file_status("/file").unwrap();
    assert_eq!(after_restore.id, before.id);
    assert_eq!(after_restore.len, before.len);
}

fn freeable_replication_file(fs: &MasterFilesystem, path: &str) -> i64 {
    let block_id = replication_file(fs, path);
    fs.set_attr(
        path,
        SetAttrOptsBuilder::new()
            .ufs_mtime(1)
            .ttl_action(TtlAction::Free)
            .build(),
    )
    .unwrap();
    block_id
}

#[cfg(feature = "fault-injection")]
#[test]
fn close_file_commit_failure_keeps_writer_state_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("close-file-commit-failure");
    fs.create("/file", true).unwrap();
    let before = fs.file_status("/file").unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "close-file-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.writer.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected writer commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.close_file("/file", "");
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected writer commit failure"));
    assert_eq!(
        fs.file_status("/file").unwrap().is_complete,
        before.is_complete
    );

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(
        fs.file_status("/file").unwrap().is_complete,
        before.is_complete
    );

    fs.close_file("/file", "").unwrap();
    assert!(fs.file_status("/file").unwrap().is_complete);
}

#[test]
fn expired_delete_is_rejected_during_metadata_restore_generation() {
    let fs = test_fs("expired-delete-restore-generation");
    let inode_id = expired_delete_file(&fs, "/ttl/file");
    let restore_guard = fs
        .fs_dir
        .read()
        .store
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();

    let result = fs.delete_expired_inode("/ttl/file", inode_id, DeleteMode::SkipTrash);

    assert!(result.is_err());
    assert_eq!(fs.file_status("/ttl/file").unwrap().id, inode_id);
    drop(restore_guard);
    assert!(fs
        .delete_expired_inode("/ttl/file", inode_id, DeleteMode::SkipTrash)
        .unwrap()
        .is_some());
    assert!(fs.file_status("/ttl/file").is_err());
}

#[test]
fn free_file_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("free-file-shared-reader");
    let block_id = freeable_replication_file(&fs, "/file");
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.free("/file", false)));

    let result = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("sharded free was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
    assert_eq!(result.inodes, 1);
    assert!(
        fs.fs_dir
            .read()
            .store
            .get_locations(block_id)
            .unwrap()
            .len()
            > 0
    );
}

#[test]
fn free_file_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("free-file-block-location-lock");
    let block_id = freeable_replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.free("/file", false)));

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    assert_eq!(done_rx.recv().unwrap().unwrap().inodes, 1);
}

#[test]
fn free_file_waits_for_conflicting_worker_location_lock() {
    let fs = test_fs("free-file-worker-location-lock");
    let block_id = freeable_replication_file(&fs, "/file");
    let held = fs
        .block_location_locks
        .write_worker_blocks(100, &[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.free("/file", false)));

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    assert_eq!(done_rx.recv().unwrap().unwrap().inodes, 1);
}

#[test]
fn delete_file_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("delete-file-block-location-lock");
    let block_id = replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.delete("/file", false)));

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    assert_eq!(done_rx.recv().unwrap().unwrap().inodes, 1);
}

#[test]
fn delete_file_waits_for_conflicting_worker_location_lock() {
    let fs = test_fs("delete-file-worker-location-lock");
    let block_id = replication_file(&fs, "/file");
    let held = fs
        .block_location_locks
        .write_worker_blocks(100, &[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.delete("/file", false)));

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    assert_eq!(done_rx.recv().unwrap().unwrap().inodes, 1);
}

#[cfg(feature = "fault-injection")]
fn assert_stale_worker_set_retry(
    fs: &MasterFilesystem,
    path: &'static str,
    block_id: i64,
    operation: impl FnOnce(MasterFilesystem) -> FsResult<bool> + Send + 'static,
) {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let inode_id = fs.file_status(path).unwrap().id;
    let runtime = FaultRuntime::process();
    let rule_id = path.trim_start_matches('/');
    let rule = FaultRuleBuilder::named("master.metadata.locations_sampled")
        .matches("path", path)
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();

    let held = fs.inode_locks.write_inode(inode_id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || operation(worker_fs));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let sampled = runtime.rule(rule_id).unwrap().unwrap().executions > 0;

    let replication_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let replication = std::thread::spawn(move || {
        tx.send(replication_fs.commit_replication_location(
            block_id,
            BlockLocation::with_id(200),
            "",
        ))
        .unwrap();
    });
    let update = rx.recv_timeout(Duration::from_secs(5));
    let updated_before_unlock = update.is_ok();
    drop(held);
    let update = match update {
        Ok(result) => result,
        Err(_) => rx.recv_timeout(Duration::from_secs(5)).unwrap(),
    };
    replication.join().unwrap();
    let result = worker.join().unwrap();
    let executions = runtime.rule(rule_id).unwrap().unwrap().executions;
    runtime.remove(rule_id).unwrap();

    assert!(
        sampled,
        "operation did not sample locations while the inode was locked"
    );
    assert!(
        updated_before_unlock,
        "replication waited for the inode lock"
    );
    assert!(update.unwrap(), "replication did not add a new location");
    assert!(
        executions >= 2,
        "operation did not retry the stale worker set"
    );
    assert!(result.unwrap());
}

#[cfg(feature = "fault-injection")]
#[test]
fn delete_retries_when_replication_changes_sampled_worker_set() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("delete-stale-worker-set");
    let path = "/delete-stale-worker-set-file";
    let block_id = replication_file(&fs, path);
    fs.complete_file(
        path,
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "delete-stale-worker-set";
    let rule = FaultRuleBuilder::named("master.metadata.locations_sampled")
        .matches("path", path)
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    // Delete samples locations under the inode lock, then waits for this
    // worker lock. Replication to another worker can change the sampled set.
    let held = fs.block_location_locks.write_worker(100);
    let delete_fs = fs.clone();
    let delete = std::thread::spawn(move || delete_fs.delete(path, false));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let sampled = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let updated = fs.commit_replication_location(block_id, BlockLocation::with_id(200), "");
    drop(held);
    let deleted = delete.join().unwrap();
    runtime.remove(rule_id).unwrap();

    assert!(sampled, "delete did not sample its worker set");
    assert!(updated.unwrap(), "replication did not add the new location");
    assert_eq!(deleted.unwrap().inodes, 1);
    for worker_id in [100, 200] {
        let commands = fs
            .worker_manager
            .write()
            .block_map
            .handle_heartbeat(worker_id);
        assert!(commands.iter().any(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.contains(&block_id),
        }));
    }
    assert!(fs.file_status(path).is_err());
}

#[cfg(feature = "fault-injection")]
#[test]
fn free_retries_when_replication_changes_sampled_worker_set() {
    let fs = test_fs("free-stale-worker-set");
    let path = "/free-stale-worker-set-file";
    let block_id = freeable_replication_file(&fs, path);
    assert_stale_worker_set_retry(&fs, path, block_id, move |fs| {
        Ok(fs.free(path, false)?.inodes == 1)
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn verified_release_retries_when_replication_changes_sampled_worker_set() {
    let fs = test_fs("verified-stale-worker-set");
    let path = "/verified-stale-worker-set-file";
    let block_id = freeable_replication_file(&fs, path);
    let snapshot = fs
        .fs_mode_snapshot(fs.file_status(path).unwrap().id)
        .unwrap()
        .unwrap();
    assert_stale_worker_set_retry(&fs, path, block_id, move |fs| {
        fs.release_verified(
            snapshot.gen.inode_id,
            &snapshot.path,
            snapshot.gen,
            snapshot.ufs_mtime,
        )
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn expired_delete_retries_when_replication_changes_sampled_worker_set() {
    let fs = test_fs("ttl-stale-worker-set");
    let path = "/ttl-stale-worker-set-file";
    let block_id = replication_file(&fs, path);
    fs.complete_file(
        path,
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    fs.set_attr(
        path,
        SetAttrOptsBuilder::new()
            .mtime(1)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .unwrap();
    let inode_id = fs.file_status(path).unwrap().id;
    assert_stale_worker_set_retry(&fs, path, block_id, move |fs| {
        Ok(fs
            .delete_expired_inode(path, inode_id, DeleteMode::SkipTrash)?
            .is_some())
    });
    assert!(fs.file_status(path).is_err());
}

#[test]
fn delete_serializes_with_a_concurrent_replication_location_update() {
    let fs = test_fs("delete-replication-location-race");
    let block_id = replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block_id]);
    let start = Arc::new(std::sync::Barrier::new(3));

    let replication_fs = fs.clone();
    let replication_start = start.clone();
    let replication = std::thread::spawn(move || {
        replication_start.wait();
        replication_fs.commit_replication_location(block_id, BlockLocation::with_id(200), "")
    });
    let delete_fs = fs.clone();
    let delete_start = start.clone();
    let delete = std::thread::spawn(move || {
        delete_start.wait();
        delete_fs.delete("/file", false)
    });

    start.wait();
    drop(held);
    assert!(replication.join().unwrap().is_ok());
    assert_eq!(delete.join().unwrap().unwrap().inodes, 1);
    assert!(fs.file_status("/file").is_err());
}

#[test]
fn delete_serializes_with_a_concurrent_block_report() {
    let fs = test_fs("delete-block-report-race");
    let block_id = replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block_id]);

    let report_fs = fs.clone();
    let (report_tx, report_rx) = std::sync::mpsc::channel();
    let report = std::thread::spawn(move || {
        report_tx.send(report_fs.block_report(
            BlockReportList {
                cluster_id: String::new(),
                worker_id: 100,
                full_report: false,
                total_len: 1,
                report_id: 0,
                blocks: vec![BlockReportInfo::new(
                    block_id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    1,
                )],
            },
            None,
        ))
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline && fs.worker_location_lock(100).try_lock().is_some()
    {
        std::thread::yield_now();
    }
    assert!(
        fs.worker_location_lock(100).try_lock().is_none(),
        "block report did not enter the worker-location critical section"
    );

    let delete_fs = fs.clone();
    let (delete_tx, delete_rx) = std::sync::mpsc::channel();
    let delete = std::thread::spawn(move || delete_tx.send(delete_fs.delete("/file", false)));
    assert!(delete_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());

    drop(held);
    report.join().unwrap().unwrap();
    report_rx.recv().unwrap().unwrap();
    delete.join().unwrap().unwrap();
    let result = delete_rx.recv().unwrap().unwrap();
    assert_eq!(result.inodes, 1);
    assert!(fs.file_status("/file").is_err());
    let commands = fs.worker_manager.write().block_map.handle_heartbeat(100);
    let deleted = commands
        .iter()
        .flat_map(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.iter(),
        })
        .copied()
        .collect::<HashSet<_>>();
    assert!(deleted.contains(&block_id));
}

#[cfg(feature = "fault-injection")]
#[test]
fn delete_does_not_hold_namespace_read_while_waiting_for_block_lock() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("delete-block-lock-namespace-order");
    let path = "/delete-block-lock-file";
    let block_id = replication_file(&fs, path);
    let held = fs
        .block_location_locks
        .write_worker_blocks(100, &[block_id]);
    let runtime = FaultRuntime::process();
    let rule_id = "delete-block-lock-namespace-order";
    let rule = FaultRuleBuilder::named("master.metadata.locations_sampled")
        .matches("path", path)
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();

    let delete_fs = fs.clone();
    let delete = std::thread::spawn(move || delete_fs.delete(path, false));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let sampled = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let writer_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let _guard = writer_fs.fs_dir.write();
        tx.send(()).unwrap();
    });
    let writer_entered = rx.recv_timeout(Duration::from_millis(500)).is_ok();
    drop(held);
    let deleted = delete.join().unwrap();
    writer.join().unwrap();
    runtime.remove(rule_id).unwrap();

    assert!(sampled, "delete did not reach the block-lock boundary");
    assert!(
        writer_entered,
        "delete held the namespace lock while waiting for a block lock"
    );
    assert_eq!(deleted.unwrap().inodes, 1);
}

#[test]
fn delete_serializes_with_worker_location_cleanup() {
    let fs = test_fs("delete-worker-cleanup-race");
    let block_id = replication_file(&fs, "/file");
    let reader = fs.fs_dir.read();

    let cleanup_fs = fs.clone();
    let (cleanup_tx, cleanup_rx) = std::sync::mpsc::channel();
    let cleanup = std::thread::spawn(move || cleanup_tx.send(cleanup_fs.delete_locations(100)));

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline && fs.worker_location_lock(100).try_lock().is_some()
    {
        std::thread::yield_now();
    }
    assert!(
        fs.worker_location_lock(100).try_lock().is_none(),
        "worker cleanup did not enter the worker-location critical section"
    );

    let delete_fs = fs.clone();
    let (delete_tx, delete_rx) = std::sync::mpsc::channel();
    let delete = std::thread::spawn(move || delete_tx.send(delete_fs.delete("/file", false)));
    assert!(delete_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());

    drop(reader);
    cleanup.join().unwrap().unwrap();
    let cleanup_result = cleanup_rx.recv().unwrap().unwrap();
    assert_eq!(cleanup_result.removed_block_ids, vec![block_id]);
    delete.join().unwrap().unwrap();
    let result = delete_rx.recv().unwrap().unwrap();
    assert_eq!(result.inodes, 1);
    assert!(fs.file_status("/file").is_err());
    assert!(fs
        .fs_dir
        .read()
        .get_block_locations(block_id)
        .unwrap()
        .is_empty());
}

#[test]
fn delete_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("delete-shared-reader");
    fs.create("/file", true).unwrap();
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.delete("/file", false)).unwrap();
    });

    let result = done_rx.recv_timeout(std::time::Duration::from_secs(2));
    drop(reader);
    worker.join().unwrap();
    assert_eq!(result.unwrap().unwrap().inodes, 1);
    assert!(!fs.exists("/file").unwrap());
}

#[test]
fn delete_is_rejected_during_metadata_restore_generation() {
    let fs = test_fs("delete-restore-generation");
    let before = fs.create("/file", true).unwrap();
    let restore_guard = fs
        .fs_dir
        .read()
        .store
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();

    let result = fs.delete("/file", false);

    assert!(result.is_err());
    assert_eq!(fs.file_status("/file").unwrap().id, before.id);
    drop(restore_guard);
    assert_eq!(fs.delete("/file", false).unwrap().inodes, 1);
    assert!(fs.file_status("/file").is_err());
}

#[test]
fn free_is_rejected_during_metadata_restore_generation() {
    let fs = test_fs("free-restore-generation");
    let block_id = freeable_replication_file(&fs, "/file");
    let before = fs.file_status("/file").unwrap();
    let restore_guard = fs
        .fs_dir
        .read()
        .store
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();

    let result = fs.free("/file", false);

    assert!(result.is_err());
    assert_eq!(fs.file_status("/file").unwrap().id, before.id);
    assert!(!fs
        .fs_dir
        .read()
        .store
        .get_locations(block_id)
        .unwrap()
        .is_empty());
    drop(restore_guard);
    assert_eq!(fs.free("/file", false).unwrap().inodes, 1);
}

#[cfg(feature = "fault-injection")]
#[test]
fn delete_commit_failure_keeps_file_state_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("delete-commit-failure");
    let block_id = replication_file(&fs, "/file");
    let before = fs.file_status("/file").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "delete-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.delete.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected delete commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();

    let result = fs.delete("/file", false);

    runtime.remove(rule_id).unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected delete commit failure"));
    assert_eq!(fs.file_status("/file").unwrap().id, before.id);
    assert!(!fs
        .fs_dir
        .read()
        .store
        .get_locations(block_id)
        .unwrap()
        .is_empty());
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/file").unwrap().id, before.id);
}

fn expired_delete_file(fs: &MasterFilesystem, path: &str) -> i64 {
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    fs.complete_file(path, None, 0, vec![], "", false, None)
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    fs.file_status(path).unwrap().id
}

fn expired_writing_file(fs: &MasterFilesystem, path: &str) -> i64 {
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap()
    .id
}

fn expired_delete_dir(fs: &MasterFilesystem, path: &str) -> i64 {
    let mut opts = MkdirOpts::with_create(true);
    opts.storage_policy.ttl_ms = 1;
    opts.storage_policy.ttl_action = TtlAction::Delete;
    let id = fs.mkdir_with_opts(path, opts).unwrap().id;
    std::thread::sleep(std::time::Duration::from_millis(10));
    id
}

#[test]
fn expired_root_is_not_deleted_by_sharded_ttl_path() {
    let fs = test_fs("expired-root");
    fs.set_attr(
        "/",
        SetAttrOptsBuilder::new()
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .unwrap();
    let root_id = fs.file_status("/").unwrap().id;
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(fs
        .delete_expired_inode_sharded("/", root_id)
        .unwrap()
        .is_none());
    assert!(fs
        .delete_expired_inode("/", root_id, DeleteMode::SkipTrash)
        .is_err());
    assert_eq!(fs.file_status("/").unwrap().id, root_id);
}

#[test]
fn expired_trash_root_keeps_reserved_delete_protection() {
    let fs = test_fs("expired-trash-root");
    let id = {
        let fs_dir = fs.fs_dir.write();
        let path = MasterFilesystem::resolve_path(&fs_dir, trash::TRASH_ROOT).unwrap();
        let mut opts = MkdirOpts::with_create(false);
        opts.storage_policy.ttl_ms = 1;
        opts.storage_policy.ttl_action = TtlAction::Delete;
        fs_dir
            .mkdir(path, opts)
            .unwrap()
            .get_last_inode()
            .unwrap()
            .id()
    };
    std::thread::sleep(std::time::Duration::from_millis(10));
    let error = fs
        .delete_expired_inode(trash::TRASH_ROOT, id, DeleteMode::SkipTrash)
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("recycle bin root cannot be deleted"));
    assert_eq!(fs.file_status(trash::TRASH_ROOT).unwrap().id, id);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status(trash::TRASH_ROOT).unwrap().id, id);
}

#[test]
fn expired_delete_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("expired-delete-shared-reader");
    let inode_id = expired_delete_file(&fs, "/ttl/file");
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.delete_expired_inode("/ttl/file", inode_id, DeleteMode::SkipTrash))
    });

    let result = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("expired delete was blocked by a shared namespace reader")
        .unwrap()
        .expect("expired file should be deleted");
    drop(reader);
    worker.join().unwrap().unwrap();
    assert_eq!(result.0.inodes, 1);
    assert_eq!(result.1.id(), inode_id);
    assert!(fs.file_status("/ttl/file").is_err());
}

#[test]
fn expired_delete_waits_for_conflicting_worker_location_lock() {
    let fs = test_fs("expired-delete-worker-location-lock");
    let block_id = replication_file(&fs, "/ttl/file");
    fs.complete_file(
        "/ttl/file",
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    fs.set_attr(
        "/ttl/file",
        SetAttrOptsBuilder::new()
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let inode_id = fs.file_status("/ttl/file").unwrap().id;
    let held = fs
        .block_location_locks
        .write_worker_blocks(100, &[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.delete_expired_inode("/ttl/file", inode_id, DeleteMode::SkipTrash))
    });

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
    assert_eq!(done_rx.recv().unwrap().unwrap().unwrap().0.inodes, 1);
}

#[test]
fn expired_delete_removes_an_empty_directory() {
    let fs = test_fs("expired-delete-empty-directory");
    let inode_id = expired_delete_dir(&fs, "/ttl/empty");

    let result = fs
        .delete_expired_inode("/ttl/empty", inode_id, DeleteMode::SkipTrash)
        .unwrap()
        .expect("expired empty directory should be deleted");

    assert_eq!(result.0.inodes, 0);
    assert!(fs.file_status("/ttl/empty").is_err());
}

#[test]
fn expired_delete_skips_writing_files_and_non_empty_directories() {
    let fs = test_fs("expired-delete-eligibility");
    let file_id = expired_writing_file(&fs, "/ttl/writing");
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(fs
        .delete_expired_inode("/ttl/writing", file_id, DeleteMode::SkipTrash)
        .unwrap()
        .is_none());
    assert_eq!(fs.file_status("/ttl/writing").unwrap().id, file_id);

    let dir_id = expired_delete_dir(&fs, "/ttl/parent");
    fs.create("/ttl/parent/child", false).unwrap();
    assert!(fs
        .delete_expired_inode("/ttl/parent", dir_id, DeleteMode::SkipTrash)
        .unwrap()
        .is_none());
    assert_eq!(fs.file_status("/ttl/parent").unwrap().id, dir_id);
}

#[test]
fn expired_delete_preserves_a_hard_link_to_the_same_inode() {
    let fs = test_fs("expired-delete-hard-link");
    let inode_id = expired_delete_file(&fs, "/ttl/file");
    fs.link("/ttl/file", "/ttl/alias").unwrap();

    let result = fs
        .delete_expired_inode("/ttl/file", inode_id, DeleteMode::SkipTrash)
        .unwrap()
        .expect("expired hard-link entry should be deleted");

    assert_eq!(result.0.inodes, 0);
    assert!(fs.file_status("/ttl/file").is_err());
    let alias = fs.file_status("/ttl/alias").unwrap();
    assert_eq!((alias.id, alias.nlink), (inode_id, 1));
}

#[test]
fn expired_delete_queues_worker_blocks_after_commit() {
    let fs = test_fs("expired-delete-worker-cleanup");
    for worker_id in [100, 200, 300] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);
    }
    let path = "/ttl/file";
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let block_id = fs
        .add_block(
            path,
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
    fs.complete_file(
        path,
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let inode_id = fs.file_status(path).unwrap().id;

    let result = fs
        .delete_expired_inode(path, inode_id, DeleteMode::SkipTrash)
        .unwrap()
        .expect("expired file should be deleted");
    assert_eq!(result.0.inodes, 1);

    let commands = fs.worker_manager.write().block_map.handle_heartbeat(100);
    let deleted = commands
        .iter()
        .flat_map(|command| match command {
            WorkerCommand::DeleteBlock(command) => command.blocks.iter(),
        })
        .copied()
        .collect::<HashSet<_>>();
    assert!(deleted.contains(&block_id));
    assert!(fs.file_status(path).is_err());
}

#[cfg(feature = "fault-injection")]
#[test]
fn expired_delete_commit_failure_keeps_memory_and_store_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("expired-delete-commit-failure");
    let path = "/ttl/file";
    for worker_id in [100, 200, 300] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);
    }
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap();
    let block_id = fs
        .add_block(
            path,
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
    fs.complete_file(
        path,
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let inode_id = fs.file_status(path).unwrap().id;
    let before_parent = fs.file_status("/ttl").unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "expired-delete-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.delete.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected delete commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.delete_expired_inode(path, inode_id, DeleteMode::SkipTrash);
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected delete commit failure"));
    assert_eq!(fs.file_status(path).unwrap().id, inode_id);
    assert_eq!(fs.file_status(path).unwrap().len, 1);
    assert_eq!(fs.file_status("/ttl").unwrap().mtime, before_parent.mtime);
    assert!(!fs
        .fs_dir
        .read()
        .store
        .get_locations(block_id)
        .unwrap()
        .is_empty());

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status(path).unwrap().id, inode_id);
    assert_eq!(fs.file_status(path).unwrap().len, 1);
    assert!(!fs
        .fs_dir
        .read()
        .store
        .get_locations(block_id)
        .unwrap()
        .is_empty());
}

#[test]
fn assign_worker_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("assign-worker-shared-reader");
    let block = replication_file(&fs, "/file");
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.assign_worker(
            "/file",
            ExtendedBlock {
                id: block,
                ..Default::default()
            },
            ClientAddress::default(),
            vec![],
        ))
    });

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("assign worker was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn assign_worker_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("assign-worker-block-location-lock");
    let block = replication_file(&fs, "/file");
    let held = fs.block_location_locks.write_blocks(&[block]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.assign_worker(
            "/file",
            ExtendedBlock {
                id: block,
                ..Default::default()
            },
            ClientAddress::default(),
            vec![],
        ))
    });

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
}

#[test]
fn resize_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("resize-shared-reader");
    let block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.resize("/file", FileAllocOpts::with_truncate(0)))
    });

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("resize was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn resize_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("resize-block-location-lock");
    let block_id = replication_file(&fs, "/file");
    fs.complete_file(
        "/file",
        None,
        1,
        vec![CommitBlock {
            block_id,
            block_len: 1,
            locations: vec![BlockLocation::with_id(100)],
        }],
        "",
        false,
        None,
    )
    .unwrap();
    let held = fs.block_location_locks.write_blocks(&[block_id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.resize("/file", FileAllocOpts::with_truncate(0)))
    });

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
}

#[test]
fn resize_by_id_falls_back_for_a_stale_path() {
    let fs = test_fs("resize-by-id-stale-path");
    let created = fs.create("/old", true).unwrap();
    fs.rename("/old", "/new", RenameFlags::empty()).unwrap();

    let blocks = fs
        .resize_by_id("/old", Some(created.id), FileAllocOpts::with_truncate(128))
        .unwrap();

    assert_eq!(blocks.status.id, created.id);
    assert_eq!(blocks.status.path, "/old");
    assert_eq!(blocks.status.len, 128);
    assert_eq!(fs.file_status("/new").unwrap().len, 128);
}

fn add_block_test_file(fs: &MasterFilesystem, path: &str) {
    for worker_id in [100, 200, 300] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = worker_id;
        fs.add_test_worker(worker);
    }
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new().replicas(2).build(),
        OpenFlags::new_create(),
    )
    .unwrap();
}

#[test]
fn add_block_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("add-block-shared-reader");
    add_block_test_file(&fs, "/file");
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.add_block(
            "/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        ))
    });

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("add block was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn add_block_waits_for_conflicting_block_location_lock() {
    let fs = test_fs("add-block-location-lock");
    add_block_test_file(&fs, "/file");
    let first = fs
        .add_block(
            "/file",
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    let held = fs.block_location_locks.write_blocks(&[first.block.id]);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.add_block(
            "/file",
            None,
            ClientAddress::default(),
            vec![CommitBlock {
                block_id: first.block.id,
                block_len: 1,
                locations: vec![BlockLocation::with_id(100)],
            }],
            vec![],
            1,
            Some(first.block),
        ))
    });

    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err());
    drop(held);
    worker.join().unwrap().unwrap();
}

#[cfg(feature = "fault-injection")]
#[test]
fn add_block_commit_failure_keeps_file_state_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("add-block-commit-failure");
    add_block_test_file(&fs, "/file");
    let before = fs.file_status("/file").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "add-block-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.add_block.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected add-block commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.add_block(
        "/file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    );
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected add-block commit failure"));
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
}

#[cfg(feature = "fault-injection")]
#[test]
fn free_file_commit_failure_keeps_memory_and_store_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("free-file-commit-failure");
    let block_id = freeable_replication_file(&fs, "/file");
    let before = fs.file_status("/file").unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "free-file-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.free.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected free commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.free("/file", false);
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected free commit failure"));
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
    assert!(!fs
        .fs_dir
        .read()
        .store
        .get_locations(block_id)
        .unwrap()
        .is_empty());

    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/file").unwrap().len, before.len);
    assert!(!fs
        .fs_dir
        .read()
        .store
        .get_locations(block_id)
        .unwrap()
        .is_empty());
}

#[test]
fn block_location_read_waits_for_inode_writer() {
    let fs = test_fs("block-location-inode-read-lock");
    fs.create("/file", true).unwrap();
    let inode_id = fs.file_status("/file").unwrap().id;
    let held = fs.inode_locks.write_inode(inode_id);
    let worker_fs = fs.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(worker_fs.get_block_locations("/file")).unwrap();
    });
    let early = rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(held);
    worker.join().unwrap();
    assert!(
        early.is_err(),
        "block-location read bypassed the inode lock"
    );
    assert_eq!(rx.recv().unwrap().unwrap().status.id, inode_id);
}

#[cfg(feature = "fault-injection")]
#[test]
fn atime_refresh_does_not_touch_replaced_inode() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("atime-replaced-parent");
    atime_ttl_file_due(&fs, "a/file");
    let old = fs.file_status("/a/file").unwrap();
    let epoch = fs.fs_dir.read().store.scan_control().cancel_epoch();
    let runtime = FaultRuntime::process();
    let rule_id = "atime-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.atime_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.touch_atime_sharded("/a/file", old.id, epoch));
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let replacement = if planned {
        fs.delete("/a", true).unwrap();
        atime_ttl_file_due(&fs, "a/file");
        Some(fs.file_status("/a/file").unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "atime update did not reach lock planning");
    let replacement = replacement.unwrap();
    assert_ne!(old.id, replacement.id);
    result.unwrap();
    let status = fs.file_status("/a/file").unwrap();
    assert_eq!((status.id, status.atime), (replacement.id, replacement.atime));
    fs.restore_from_rocksdb().unwrap();
    let status = fs.file_status("/a/file").unwrap();
    assert_eq!((status.id, status.atime), (replacement.id, replacement.atime));
}

#[test]
fn atime_refresh_ignores_previous_restore_epoch() {
    let fs = test_fs("atime-restore-epoch");
    atime_ttl_file_due(&fs, "file");
    let original = fs.file_status("/file").unwrap();
    let epoch = fs.fs_dir.read().store.scan_control().cancel_epoch();
    let guard = fs
        .fs_dir
        .read()
        .store
        .begin_restore(std::time::Duration::from_secs(1))
        .unwrap();
    drop(guard);
    assert_ne!(fs.fs_dir.read().store.scan_control().cancel_epoch(), epoch);
    fs.touch_atime_sharded("/file", original.id, epoch).unwrap();
    let status = fs.file_status("/file").unwrap();
    assert_eq!((status.id, status.atime), (original.id, original.atime));
}

#[test]
fn atime_refresh_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("atime-refresh-shared-reader");
    atime_ttl_file_due(&fs, "file");
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || done_tx.send(worker_fs.get_block_locations("/file")));

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("atime refresh was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
}

#[test]
fn verified_release_waits_for_conflicting_worker_location_lock() {
    let fs = test_fs("verified-release-worker-location-lock");
    freeable_replication_file(&fs, "/file");
    let snapshot = fs
        .fs_mode_snapshot(fs.file_status("/file").unwrap().id)
        .unwrap()
        .unwrap();
    let held = fs.block_location_locks.write_worker(100);
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx
            .send(worker_fs.release_verified(
                snapshot.gen.inode_id,
                &snapshot.path,
                snapshot.gen,
                snapshot.ufs_mtime,
            ))
            .unwrap();
    });
    let early = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(held);
    worker.join().unwrap();
    assert!(
        early.is_err(),
        "verified release did not acquire the worker lock"
    );
    assert!(done_rx.recv().unwrap().unwrap());
}

#[test]
fn verified_release_does_not_wait_for_a_shared_namespace_reader() {
    let fs = test_fs("verified-release-shared-reader");
    let _block_id = freeable_replication_file(&fs, "/file");
    let snapshot = fs
        .fs_mode_snapshot(fs.file_status("/file").unwrap().id)
        .unwrap()
        .unwrap();
    let reader = fs.fs_dir.read();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx.send(worker_fs.release_verified(
            snapshot.gen.inode_id,
            &snapshot.path,
            snapshot.gen,
            snapshot.ufs_mtime,
        ))
    });

    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("verified release was blocked by a shared namespace reader")
        .unwrap();
    drop(reader);
    worker.join().unwrap().unwrap();
    assert_eq!(
        fs.file_status("/file").unwrap().storage_policy.state,
        StorageState::Ufs
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn verified_release_commit_failure_keeps_memory_and_store_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("verified-release-commit-failure");
    let _block_id = freeable_replication_file(&fs, "/file");
    let status = fs.file_status("/file").unwrap();
    let snapshot = fs.fs_mode_snapshot(status.id).unwrap().unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "verified-release-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.free.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected verified release failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.release_verified(status.id, &snapshot.path, snapshot.gen, snapshot.ufs_mtime);
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected verified release failure"));
    assert_eq!(
        fs.file_status("/file").unwrap().storage_policy.state,
        StorageState::Both
    );
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(
        fs.file_status("/file").unwrap().storage_policy.state,
        StorageState::Both
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn complete_file_commit_failure_keeps_memory_and_store_unchanged() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    let fs = test_fs("complete-file-commit-failure");
    fs.create("/file", true).unwrap();
    let before = fs.file_status("/file").unwrap();

    let runtime = FaultRuntime::process();
    let rule_id = "complete-file-commit-failure";
    let rule = FaultRuleBuilder::named("master.metadata.complete_file.before_commit")
        .times(1)
        .unwrap()
        .return_error("injected complete_file commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let result = fs.complete_file("/file", None, 0, vec![], "", false, None);
    runtime.remove(rule_id).unwrap();

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected complete_file commit failure"));
    let after_failure = fs.file_status("/file").unwrap();
    assert_eq!(after_failure.is_complete, before.is_complete);
    assert_eq!(after_failure.len, before.len);

    fs.restore_from_rocksdb().unwrap();
    let after_restore = fs.file_status("/file").unwrap();
    assert_eq!(after_restore.is_complete, before.is_complete);
    assert_eq!(after_restore.len, before.len);

    fs.complete_file("/file", None, 0, vec![], "", false, None)
        .unwrap();
    assert!(fs.file_status("/file").unwrap().is_complete);
}
