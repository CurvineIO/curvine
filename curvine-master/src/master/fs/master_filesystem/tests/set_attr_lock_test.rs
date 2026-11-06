use super::*;
use curvine_model::{FileLock, LockFlags, LockType};

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
fn recursive_set_attr_does_not_wait_for_fs_dir_readers() {
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

    let status = done_rx
        .recv_timeout(std::time::Duration::from_secs(3))
        .expect("recursive set_attr was blocked by a shared namespace reader")
        .unwrap();
    assert_eq!(status.mode, 0o700);
    drop(read_guard);
    task.join().unwrap();
    assert_eq!(fs.file_status("/tree/dir").unwrap().mode, 0o700);
    assert_eq!(fs.file_status("/tree/dir/file").unwrap().mode, 0o700);
    fs.restore_from_rocksdb().unwrap();
    assert_eq!(fs.file_status("/tree/dir").unwrap().mode, 0o700);
    assert_eq!(fs.file_status("/tree/dir/file").unwrap().mode, 0o700);
}

#[test]
fn recursive_set_attr_waits_for_the_recursive_root_lock() {
    let fs = test_fs("recursive-set-attr-root-lock");
    fs.create("/tree/dir/file", true).unwrap();
    let tree_id = fs.file_status("/tree").unwrap().id;
    let held = fs.inode_locks.write_inode(tree_id);
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

    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_err(),
        "recursive set_attr bypassed the recursive root inode lock"
    );
    drop(held);
    task.join().unwrap();
    assert_eq!(done_rx.recv().unwrap().unwrap().mode, 0o700);
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
