use super::*;
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
    let new_file = if planned {
        fs.delete("/a", true).unwrap();
        Some(fs.create("/a/file", true).unwrap())
    } else {
        None
    };
    drop(held);
    let result = worker.join().unwrap();
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
    let read_guard = fs.fs_dir.read();
    let worker = fs.clone();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = worker.set_attr("/file", SetAttrOptsBuilder::new().mode(0o600).build());
        done_tx.send(result.map(|_| ())).unwrap();
    });
    started_rx.recv().unwrap();
    let result = done_rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(read_guard);
    task.join().unwrap();
    result.unwrap().unwrap();
    assert_eq!(fs.file_status("/file").unwrap().mode, 0o600);
}

#[test]
fn recursive_set_attr_waits_for_shared_namespace_readers() {
    let fs = test_fs("recursive-set-attr-shared-reader");
    fs.create("/tree/dir/file", true).unwrap();
    let read_guard = fs.fs_dir.read();
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

    let early = done_rx.recv_timeout(std::time::Duration::from_millis(100));
    drop(read_guard);
    let status = match early {
        Ok(result) => {
            task.join().unwrap();
            panic!("recursive set_attr bypassed the namespace writer: {result:?}");
        }
        Err(_) => done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap(),
    };
    assert_eq!(status.mode, 0o700);
    task.join().unwrap();
    assert_eq!(fs.file_status("/tree/dir").unwrap().mode, 0o700);
    assert_eq!(fs.file_status("/tree/dir/file").unwrap().mode, 0o700);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/tree/dir").unwrap().mode, 0o700);
    assert_eq!(fs.file_status("/tree/dir/file").unwrap().mode, 0o700);
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
    done_rx.recv().unwrap().unwrap();
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
    done_rx.recv().unwrap().unwrap();
}

#[test]
fn set_lock_does_not_wait_for_fs_dir_readers() {
    let fs = test_fs("set-lock-shared-fs-dir");
    fs.create("/file", true).unwrap();
    let read_guard = fs.fs_dir.read();
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
    let result = done_rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(read_guard);
    task.join().unwrap();
    result.unwrap().unwrap();
}
