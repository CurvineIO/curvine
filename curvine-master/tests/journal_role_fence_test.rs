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

//! Role-fence regression: a follower must hold namespace writes until it
//! catches up as leader (the read barrier inside `run_metadata_write`),
//! matching Astra's `journal_role_fence_test`.

use curvine_config::ClusterConf;
use curvine_master::master::fs::DeleteMode;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_model::{
    BlockReportList, ClientAddress, CreateFileOpts, ExtendedBlock, FileAllocOpts, FileLock,
    MountOptions, OpenFlags, SetAttrOpts,
};
use curvine_raft::raft::storage::{AppStorage, ApplyMsg};
use curvine_raft::raft::RoleState;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use raft::StateRole;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[test]
fn follower_role_holds_metadata_writes_until_leader_is_current() {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir("journal-role-fence");

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal_system.journal_loader();
    let rt = AsyncRuntime::single();
    rt.block_on(AppStorage::role_change(&loader, StateRole::Follower))
        .unwrap();

    let fs = journal_system.fs();
    let verify_fs = fs.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        // mkdir enters run_metadata_write: ensure_metadata_current must reject
        // while the follower barrier is not current.
        finished_tx.send(fs.mkdir("/role-fenced", false)).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let result = finished_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("follower write must be rejected promptly");
    assert!(result.is_err(), "follower must fence namespace writes");
    writer.join().unwrap();

    // Follower replay must not accidentally reopen the write path.
    let entry = raft::eraftpb::Entry {
        term: 1,
        index: 1,
        ..Default::default()
    };
    journal_system
        .append_committed_entry_for_test(entry.clone())
        .unwrap();
    rt.block_on(loader.apply(true, ApplyMsg::new_entry(entry)))
        .unwrap();
    assert!(verify_fs.mkdir("/still-fenced", false).is_err());

    // Promote: the loader catches up committed metadata and the barrier
    // turns current, releasing the writer.
    rt.block_on(AppStorage::role_change(&loader, StateRole::Leader))
        .unwrap();
    verify_fs.mkdir("/role-fenced", false).unwrap();
    assert!(verify_fs.exists("/role-fenced").unwrap());
}

#[test]
fn follower_rejects_inode_attribute_mutations() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "journal-role-inode-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.create("/file", false).unwrap();
    fs.mkdir("/mnt", false).unwrap();
    let mounts = journal.mount_manager();
    mounts
        .mount(
            None,
            "/mnt",
            "file:///tmp/role-fence-mount",
            &MountOptions::builder().build(),
        )
        .unwrap();
    fs.open_file(
        "/created-via-open",
        CreateFileOpts::with_create(false),
        OpenFlags::WRONLY | OpenFlags::CREAT,
    )
    .unwrap();
    assert!(fs.exists("/created-via-open").unwrap());
    let before = fs.file_status("/file").unwrap();
    let runtime = AsyncRuntime::single();
    runtime
        .block_on(journal.journal_loader().role_change(StateRole::Follower))
        .unwrap();
    assert!(mounts.umount("/mnt").is_err());
    assert!(mounts
        .mount(
            None,
            "/mnt",
            "file:///tmp/role-fence-mount",
            &MountOptions::builder().update(true).ttl_ms(1234).build()
        )
        .is_err());
    assert!(mounts
        .get_mount_table()
        .unwrap()
        .iter()
        .any(|mount| mount.cv_path == "/mnt" && mount.ttl_ms != 1234));
    assert!(fs
        .set_attr(
            "/file",
            SetAttrOpts {
                mode: Some(0o600),
                ..Default::default()
            }
        )
        .is_err());
    assert_eq!(fs.file_status("/file").unwrap().mode, before.mode);
    assert!(fs
        .close_file("/file", "")
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    let report = BlockReportList {
        cluster_id: String::new(),
        worker_id: 100,
        full_report: false,
        total_len: 0,
        report_id: 0,
        blocks: vec![],
    };
    assert!(fs
        .block_report(report, None)
        .err()
        .expect("follower must reject block reports")
        .to_string()
        .contains("metadata is fenced"));
    let complete = fs.complete_file("/file", None, 0, vec![], "", false, None);
    assert!(complete
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    let allocate = fs.add_block(
        "/file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    );
    assert!(allocate
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    let symlink = fs.symlink("/file", "/sym", false, 0o777);
    let link = fs.link("/file", "/hard");
    let open = fs.open_file(
        "/file",
        CreateFileOpts::with_create(false),
        OpenFlags::WRONLY,
    );
    assert!(symlink
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    assert!(link.unwrap_err().to_string().contains("metadata is fenced"));
    assert!(open.unwrap_err().to_string().contains("metadata is fenced"));
    let free = fs.free("/file", false);
    let restore = fs.restore_trash("/.Trash/missing", None, false);
    assert!(free.unwrap_err().to_string().contains("metadata is fenced"));
    assert!(restore
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    let resize = fs.resize("/file", FileAllocOpts::with_truncate(0));
    let assign = fs.assign_worker(
        "/file",
        ExtendedBlock::default(),
        ClientAddress::default(),
        vec![],
    );
    let lock = fs.set_lock("/file", FileLock::default());
    assert!(resize
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    assert!(assign
        .unwrap_err()
        .to_string()
        .contains("metadata is fenced"));
    assert!(lock.unwrap_err().to_string().contains("metadata is fenced"));
    assert!(!fs.exists("/sym").unwrap());
    assert!(!fs.exists("/hard").unwrap());
    fs.open_file(
        "/file",
        CreateFileOpts::with_create(false),
        OpenFlags::RDONLY,
    )
    .unwrap();
}

#[test]
fn background_delete_respects_fence_before_monitor_updates() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "journal-background-fence-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.master_monitor.set_role_for_test(RoleState::Leader);
    fs.create("/victim", false).unwrap();
    let runtime = AsyncRuntime::single();
    runtime
        .block_on(journal.journal_loader().role_change(StateRole::Follower))
        .unwrap();
    assert!(fs
        .delete_with_mode_if_active("/victim", false, DeleteMode::SkipTrash)
        .is_err());
    assert!(fs.claim_trash_entry_for_purge("/.Trash/missing").is_err());
    assert!(fs.exists("/victim").unwrap());
}

#[test]
fn failed_leader_catch_up_keeps_writes_fenced() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "journal-role-fence-failed-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal_system.journal_loader();
    let rt = AsyncRuntime::single();
    rt.block_on(loader.role_change(StateRole::Follower))
        .unwrap();
    journal_system
        .append_committed_entry_for_test(raft::eraftpb::Entry {
            term: 1,
            index: 1,
            data: vec![0xff].into(),
            ..Default::default()
        })
        .unwrap();
    assert!(rt.block_on(loader.role_change(StateRole::Leader)).is_err());
    assert!(journal_system.fs().mkdir("/still-fenced", false).is_err());
}
