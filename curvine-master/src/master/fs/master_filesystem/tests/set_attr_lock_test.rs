use super::*;
use crate::master::meta::subtree_write_plan::SUBTREE_PLAN_LIMIT;
use curvine_model::{FileLock, LockFlags, LockType};

#[cfg(feature = "fault-injection")]
#[test]
fn set_lock_replans_after_parent_replacement() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("set-lock-replaced-parent");
    let old_file = fs.create("/a/file", true).unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "set-lock-replaced-parent";
    let rule = FaultRuleBuilder::named("master.metadata.set_lock_planned")
        .matches("path", "/a/file")
        .unwrap()
        .record()
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let held = fs.inode_locks.write_inode(old_file.id);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.set_lock(
            "/a/file",
            FileLock {
                client_id: "owner".into(),
                owner_id: 1,
                acquire_time: curvine_runtime::common::LocalTime::mills(),
                lock_type: LockType::WriteLock,
                end: 100,
                ..Default::default()
            },
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let planned = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    // The worker parks at its pre-lock fault point (set_lock_planned gate),
    // then waits on the held inode lock. The driver replays the parent
    // replacement with plain sharded operations (no exclusive window), then
    // drops the held lock so the worker resumes on the new identity.
    let new_file = if planned {
        fs.rename("/a", "/a-moved", RenameFlags::empty()).unwrap();
        fs.mkdir("/a", false).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
    // Cleanup of the moved-away path after the worker finished.
    fs.delete("/a-moved", true).unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(planned, "set_lock did not reach lock planning");
    assert_ne!(old_file.id, new_file.unwrap().id);
    assert!(result.unwrap().is_none());
    let query = FileLock {
        client_id: "other".into(),
        owner_id: 2,
        end: 100,
        ..Default::default()
    };
    assert!(fs.get_lock("/a/file", query.clone()).unwrap().is_some());
    fs.restore_from_rocksdb().unwrap();
    assert!(fs.get_lock("/a/file", query).unwrap().is_some());
}

#[cfg(feature = "fault-injection")]
#[test]
fn recursive_set_attr_preserves_concurrent_hardlink_update() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use std::time::{Duration, Instant};

    let fs = test_fs("recursive-set-attr-hardlink");
    fs.create("/tree/file", true).unwrap();
    fs.link("/tree/file", "/alias").unwrap();
    let runtime = FaultRuntime::process();
    let rule_id = "recursive-set-attr-hardlink";
    let rule = FaultRuleBuilder::named("master.metadata.set_attr.before_commit")
        .matches("root_name", "tree")
        .unwrap()
        .times(1)
        .unwrap()
        .delay(1500)
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.set_attr(
            "/tree",
            SetAttrOpts {
                recursive: true,
                mode: Some(0o600),
                ..Default::default()
            },
        )
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.rule(rule_id).unwrap().unwrap().executions == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let delayed = runtime.rule(rule_id).unwrap().unwrap().executions > 0;
    let update = if delayed {
        Some(fs.set_attr(
            "/alias",
            SetAttrOpts {
                mtime: Some(1_234_567),
                ..Default::default()
            },
        ))
    } else {
        None
    };
    let result = worker.join().unwrap();
    runtime.remove(rule_id).unwrap();
    assert!(delayed, "recursive set_attr did not reach the commit point");
    update.unwrap().unwrap();
    result.unwrap();
    let status = fs.file_status("/alias").unwrap();
    assert_eq!(status.mtime, 1_234_567);
    assert_eq!(status.mode, 0o600);
    fs.restore_from_rocksdb().unwrap();
    let status = fs.file_status("/alias").unwrap();
    assert_eq!(status.mtime, 1_234_567);
    assert_eq!(status.mode, 0o600);
}

#[test]
fn single_inode_set_attr_does_not_wait_for_fs_dir_readers() {
    let fs = test_fs("set-attr-shared-fs-dir");
    fs.create("/file", true).unwrap();
    let read_guard = fs.fs_dir.view().unwrap();
    let worker = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = worker.set_attr("/file", SetAttrOptsBuilder::new().mode(0o600).build());
        done_tx.send(result.map(|_| ())).unwrap();
    });
    started_rx.recv().unwrap();
    let outer = done_rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(read_guard);
    task.join().unwrap();
    let inner = outer.expect("set_attr must finish within the bound");
    inner.expect("set_attr must succeed on the shared view");
    assert_eq!(fs.file_status("/file").unwrap().mode, 0o600);
}

#[test]
fn recursive_set_attr_waits_for_shared_namespace_readers() {
    // C2: a small recursive set_attr runs under the bounded subtree-lock
    // path and completes concurrently with a held namespace view — the
    // view guard is a reader, not a lock the recursive path must drain.
    // The over-budget fallback (which still drains via the exclusive
    // bridge) keeps the original waiting contract observable.
    let fs = test_fs("recursive-set-attr-shared-reader");
    fs.create("/tree/dir/file", true).unwrap();
    let read_guard = fs.fs_dir.view().unwrap();
    let worker = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        done_tx
            .send(
                worker.set_attr(
                    "/tree",
                    SetAttrOptsBuilder::new()
                        .recursive(true)
                        .mode(0o700)
                        .build(),
                ),
            )
            .unwrap();
    });

    // The locked path must finish while the reader still holds its view.
    let status = done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(status.mode, 0o700);
    task.join().unwrap();
    assert_eq!(fs.file_status("/tree/dir").unwrap().mode, 0o700);
    assert_eq!(fs.file_status("/tree/dir/file").unwrap().mode, 0o700);
    // The reader's view stays usable (FAILED is not a drain) and the
    // versioned walk still validates.
    let snapshot_inp = crate::master::meta::inode::InodePath::resolve(
        read_guard.root_node(),
        "/tree/dir/file",
        &read_guard.store,
    )
    .expect("the held view must still resolve after the locked write");
    let snapshot_status = read_guard
        .file_status(&snapshot_inp)
        .expect("the held view must stay usable after the locked write");
    assert_eq!(snapshot_status.mode, 0o700);
    drop(read_guard);

    // Over-budget subtree: the plan returns None before any side effect
    // and the exclusive bridge drains the held view — the original
    // waiting contract.
    let wide = test_fs("recursive-set-attr-shared-reader-fallback");
    wide.create("/wide/dir/file", true).unwrap();
    for index in 0..SUBTREE_PLAN_LIMIT {
        wide.create(format!("/wide/f{index}"), false).unwrap();
    }
    let read_guard = wide.fs_dir.view().unwrap();
    let worker = wide.clone();
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        done_tx
            .send(
                worker.set_attr(
                    "/wide",
                    SetAttrOptsBuilder::new()
                        .recursive(true)
                        .mode(0o700)
                        .build(),
                ),
            )
            .unwrap();
    });
    let early = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(read_guard);
    match early {
        Ok(result) => {
            task.join().unwrap();
            panic!("an over-budget recursive set_attr must not bypass the cold drain: {result:?}");
        }
        Err(_) => done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap(),
    };
    // R8: the success branch must also join its task (no unbounded leak).
    task.join().unwrap();
    assert_eq!(wide.file_status("/wide/dir/file").unwrap().mode, 0o700);
    assert_eq!(wide.file_status("/wide/dir").unwrap().mode, 0o700);

    // R8: restore the persistence regression the C1 contract carried — the
    // recursive write must survive a restore-from-store on BOTH paths.
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/tree/dir").unwrap().mode, 0o700);
    assert_eq!(fs.file_status("/tree/dir/file").unwrap().mode, 0o700);
    wide.restore_from_rocksdb().unwrap();
    assert_eq!(wide.file_status("/wide/dir").unwrap().mode, 0o700);
    assert_eq!(wide.file_status("/wide/dir/file").unwrap().mode, 0o700);
}

#[test]
fn set_lock_waits_for_target_inode_lock() {
    let fs = test_fs("set-lock-inode-order");
    let inode_id = fs.create("/file", true).unwrap().id;
    let inode_guard = fs.inode_locks.write_inode(inode_id);
    let worker = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let task = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker.set_lock("/file", FileLock::default()))
            .unwrap();
    });
    started_rx.recv().unwrap();
    let early = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(inode_guard);
    task.join().unwrap();
    assert!(early.is_err(), "set_lock bypassed the target inode lock");
    let inner = done_rx
        .recv()
        .expect("set_lock must finish after the lock is released");
    inner.expect("set_lock must succeed after the target inode lock is released");
}

#[test]
fn set_lock_root_waits_for_target_inode_lock() {
    let fs = test_fs("set-lock-root-inode-order");
    let inode_guard = fs.inode_locks.write_inode(ROOT_INODE_ID);
    let worker = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let task = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker.set_lock("/", FileLock::default()))
            .unwrap();
    });
    started_rx.recv().unwrap();
    let early = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(inode_guard);
    task.join().unwrap();
    assert!(early.is_err(), "set_lock('/') bypassed the root inode lock");
    let inner = done_rx
        .recv()
        .expect("set_lock must finish after the lock is released");
    inner.expect("set_lock must succeed after the root inode lock is released");
}

#[test]
fn set_lock_does_not_wait_for_fs_dir_readers() {
    let fs = test_fs("set-lock-shared-fs-dir");
    fs.create("/file", true).unwrap();
    let read_guard = fs.fs_dir.view().unwrap();
    let worker = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        let lock = FileLock {
            client_id: "client".to_string(),
            owner_id: 1,
            pid: 1,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Flock,
            ..FileLock::default()
        };
        started_tx.send(()).unwrap();
        done_tx
            .send(worker.set_lock("/file", lock).map(|_| ()))
            .unwrap();
    });
    started_rx.recv().unwrap();
    let outer = done_rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(read_guard);
    task.join().unwrap();
    let inner = outer.expect("set_lock must finish within the bound");
    inner.expect("set_lock must succeed on the shared view");
}
