// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use curvine_master::master::mount::MountTable;
use curvine_master::Master;
use curvine_model::MountOptions;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn mount_mutations_wait_for_commit_gate() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "mount-gate-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/mnt", false).unwrap();
    let manager = journal.mount_manager();
    let gate = fs.metadata_commit_gate();

    let barrier = gate.close_owned();
    let worker_manager = manager.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker_manager.mount(
                None,
                "/mnt",
                "file:///tmp/curvine-mount-gate",
                &MountOptions::builder().build(),
            ))
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100));
    let before = manager.get_mount_table().unwrap();
    drop(barrier);
    worker.join().unwrap();
    assert!(matches!(blocked, Err(mpsc::RecvTimeoutError::Timeout)));
    assert!(!before.iter().any(|mount| mount.cv_path == "/mnt"));
    done_rx.recv().unwrap();
    assert!(manager
        .get_mount_table()
        .unwrap()
        .iter()
        .any(|mount| mount.cv_path == "/mnt"));

    let barrier = gate.close_owned();
    let worker_manager = manager.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx
            .send(worker_manager.mount(
                None,
                "/mnt",
                "file:///tmp/curvine-mount-gate",
                &MountOptions::builder().update(true).ttl_ms(1234).build(),
            ))
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100));
    let before = manager
        .get_mount_table()
        .unwrap()
        .into_iter()
        .find(|mount| mount.cv_path == "/mnt")
        .unwrap();
    drop(barrier);
    worker.join().unwrap();
    assert!(matches!(blocked, Err(mpsc::RecvTimeoutError::Timeout)));
    assert_ne!(before.ttl_ms, 1234);
    done_rx.recv().unwrap();
    assert_eq!(
        manager
            .get_mount_table()
            .unwrap()
            .into_iter()
            .find(|mount| mount.cv_path == "/mnt")
            .unwrap()
            .ttl_ms,
        1234
    );

    let barrier = gate.close_owned();
    let worker_manager = manager.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx.send(worker_manager.umount("/mnt")).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100));
    let before = manager.get_mount_table().unwrap();
    drop(barrier);
    worker.join().unwrap();
    assert!(matches!(blocked, Err(mpsc::RecvTimeoutError::Timeout)));
    assert!(before.iter().any(|mount| mount.cv_path == "/mnt"));
    done_rx.recv().unwrap();
    assert!(!manager
        .get_mount_table()
        .unwrap()
        .iter()
        .any(|mount| mount.cv_path == "/mnt"));
    journal.shutdown();
}

#[test]
fn mount_publication_waits_for_namespace_lock() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "mount-publication-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let table = Arc::new(MountTable::new());
    let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    let worker_table = table.clone();
    let worker_fs = fs.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        // The caller acquires the namespace guard before the table
        // transaction, matching the production ordering.
        let guard_dir = worker_fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let result = worker_table.add_mount(
            &guard_dir,
            42,
            "/mount",
            "file:///tmp/mount-publication",
            &MountOptions::builder().build(),
        );
        drop(guard_dir);
        done_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked = done_rx.recv_timeout(Duration::from_millis(100));
    let snapshot = table.snapshot().unwrap();
    drop(guard);
    worker.join().unwrap();
    assert!(matches!(blocked, Err(mpsc::RecvTimeoutError::Timeout)));
    assert!(snapshot.entries.is_empty());
    done_rx.recv().unwrap();
    let version = table.snapshot().unwrap().version;
    {
        let fs_dir = fs.fs_dir.view().unwrap();
        assert!(table
            .add_mount(
                &fs_dir,
                42,
                "/other",
                "file:///tmp/mount-publication-other",
                &MountOptions::builder().build(),
            )
            .is_err());
    }
    assert_eq!(table.snapshot().unwrap().version, version);
    let restored = MountTable::new();
    {
        let fs_dir = fs.fs_dir.view().unwrap();
        restored.restore(&fs_dir).unwrap();
    }
    let current = table.snapshot().unwrap();
    let recovered = restored.snapshot().unwrap();
    assert_eq!(current.version, recovered.version);
    assert_eq!(current.entries.len(), recovered.entries.len());
    assert_eq!(recovered.entries[0].cv_path, "/mount");
    journal.shutdown();
}
