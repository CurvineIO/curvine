// Copyright 2026 OPPO
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

// The mount-table business lock spans the whole mutation: a journal-queue
// pause between the row write and the version save is observable as
// `WouldBlock` on `inner`, while a poisoned lock reports `Poisoned`.

use super::*;
use crate::master::fs::MasterFilesystem;
use crate::master::journal::JournalSystem;
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::Master;
use curvine_config::ClusterConf;
use curvine_runtime::common::Utils;
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn txn_journal(name: &str) -> (JournalSystem, MasterFilesystem, Arc<MountTable>) {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("mount-txn-{}-{}", name, Utils::rand_str(8)));
    conf.journal.enable = true;
    conf.journal.writer_channel_size = 1;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let table = Arc::new(MountTable::new());
    (journal, fs, table)
}

/// Drains the retained journal queue until at least `count` entries were
/// observed, releasing the backpressure that paused the writer.
fn drain(journal: &JournalSystem, count: usize, deadline: Instant) {
    loop {
        let drained = journal.fs().fs_dir.view().unwrap().take_entries().len() >= count;
        if drained || Instant::now() >= deadline {
            return;
        }
        std::thread::yield_now();
    }
}

#[test]
fn online_add_persists_row_and_version_under_one_lock() {
    let (journal, fs, table) = txn_journal("row-version-atomic");
    fs.mkdir("/mnt", false).unwrap();

    let (done_tx, done_rx) = mpsc::channel();
    let worker_table = Arc::clone(&table);
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        // The business critical section only needs &FsDir: hold a read guard
        // so the main thread can still observe the row while this worker is
        // paused on journal backpressure.
        let guard_dir = worker_fs.fs_dir.view().unwrap();
        done_tx
            .send(worker_table.add_mount(
                &guard_dir,
                1,
                "/mnt",
                "file:///tmp/mount-txn",
                &curvine_model::MountOptions::builder().build(),
            ))
            .unwrap();
    });

    // Phase one: the mount row is already persisted while the version save
    // may still be pending (journal backpressure of channel size 1).
    let deadline = Instant::now() + Duration::from_secs(5);
    let row_visible = loop {
        if Instant::now() >= deadline {
            break false;
        }
        let visible = {
            let dir = fs.fs_dir.view().unwrap();
            dir.get_mount_point(1).unwrap().is_some()
        };
        if visible {
            break true;
        }
        std::thread::yield_now();
    };
    assert!(row_visible, "mount row was never persisted");

    // The business lock is still held at this point: a competing writer sees
    // `WouldBlock`, not `Poisoned`.
    let probe = table.try_write_inner();
    match probe {
        Err(TryLockError::WouldBlock) => {}
        other => panic!(
            "expected WouldBlock while the row is pending, got {:?}",
            other.map(|_| ())
        ),
    }

    // Drain to release backpressure and let the worker finish.
    drain(&journal, 1, Instant::now() + Duration::from_secs(5));
    let result = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("add_mount never finished after queue drain");
    worker.join().unwrap();
    result.unwrap();

    // Snapshot version and persisted version agree after completion.
    let snapshot_version = table.snapshot().unwrap().version;
    let persisted_version = {
        let dir = fs.fs_dir.view().unwrap();
        dir.get_mount_table_version().unwrap()
    };
    assert_eq!(snapshot_version, persisted_version);
    journal.shutdown();
}

#[test]
fn replay_mount_does_not_echo_journal() {
    let (journal, fs, table) = txn_journal("replay-no-echo");
    fs.mkdir("/mnt", false).unwrap();

    // Drain any pre-existing entries, then the replay itself must leave the
    // queue empty (no journal echo).
    let _ = fs.fs_dir.view().unwrap().take_entries();
    {
        let fs_dir = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let info = curvine_model::MountOptions::builder().build().to_info(
            7,
            "/mnt",
            "file:///tmp/mount-replay",
        );
        table.replay_mount(&fs_dir, info).unwrap();
    }
    let echoed = fs.fs_dir.view().unwrap().take_entries().len();

    // Row and version persisted; the queue gained no journal echo.
    assert_eq!(echoed, 0, "replay echoed a journal entry");
    let snapshot_version = table.snapshot().unwrap().version;
    let persisted_version = {
        let dir = fs.fs_dir.view().unwrap();
        dir.get_mount_table_version().unwrap()
    };
    assert_eq!(snapshot_version, persisted_version);
    let row = {
        let dir = fs.fs_dir.view().unwrap();
        dir.get_mount_point(7)
            .unwrap()
            .expect("replay row persisted")
    };
    assert_eq!(row.mount_id, 7);
    journal.shutdown();
}

#[test]
fn replay_unmount_idempotent_without_echo() {
    let (journal, fs, table) = txn_journal("replay-unmount");
    fs.mkdir("/mnt", false).unwrap();

    // Drain history first: a replay add and a replay unmount pair must
    // leave the queue empty (neither echoes a journal entry).
    let _ = fs.fs_dir.view().unwrap().take_entries();
    {
        let fs_dir = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
        let info = curvine_model::MountOptions::builder().build().to_info(
            9,
            "/mnt",
            "file:///tmp/mount-replay-unmount",
        );
        table.replay_mount(&fs_dir, info).unwrap();
    }
    table
        .replay_unmount(&fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap(), 9)
        .unwrap();
    let echoed = fs.fs_dir.view().unwrap().take_entries().len();

    assert_eq!(echoed, 0, "replay add/unmount echoed a journal entry");
    assert!(table.snapshot().unwrap().entries.is_empty());
    // Replaying the same unmount again is an idempotent no-op.
    table
        .replay_unmount(&fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap(), 9)
        .unwrap();
    journal.shutdown();
}
