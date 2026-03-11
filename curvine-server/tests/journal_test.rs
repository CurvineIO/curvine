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

use curvine_common::conf::ClusterConf;
use curvine_common::fs::CurvineURI;
use curvine_common::raft::{NodeId, RaftPeer};
use curvine_common::state::{
    BlockLocation, ClientAddress, CommitBlock, CreateFileOpts, FileType, MountOptions, OpenFlags,
    RenameFlags, SetAttrOptsBuilder, TtlAction, WorkerInfo,
};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{JournalLoader, JournalSystem};
use curvine_server::master::{Master, MountManager};
use log::info;
use orpc::common::{LocalTime, Logger, TimeSpent};
use orpc::io::net::NetUtils;
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// First start a master and perform the operation; then start 1 stand by, manually replay the log to check consistency.
#[test]
fn test_journal_replay_consistency_between_leader_and_follower() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("meta-js1");
    let journal_system = JournalSystem::from_conf(&conf)?;
    let fs_leader = MasterFilesystem::with_js(&conf, &journal_system);
    let mnt_mgr1 = journal_system.mount_manager();
    fs_leader.add_test_worker(worker.clone());

    run(&fs_leader, &worker)?;
    run_mnt(mnt_mgr1.clone())?;

    /************* Replay log from node **************/
    conf.change_test_meta_dir("meta-js2");
    let follower_journal_system = JournalSystem::from_conf(&conf)?;
    let fs_follower = MasterFilesystem::with_js(&conf, &follower_journal_system);
    let mnt_mgr2 = follower_journal_system.mount_manager();
    let journal_loader = JournalLoader::new_replay_loader(
        fs_follower.fs_dir(),
        mnt_mgr2.clone(),
        &conf.journal,
        follower_journal_system.job_manager(),
    );

    let entries = journal_system.fs().fs_dir.read().take_entries();
    info!("entries size {}", entries.len());
    for entry in entries {
        journal_loader.apply_entry(entry).unwrap();
    }

    fs_leader.print_tree();
    fs_follower.print_tree();
    assert_eq!(fs_leader.last_inode_id(), fs_follower.last_inode_id());
    assert_eq!(fs_leader.sum_hash(), fs_follower.sum_hash());

    let leader_mnt = mnt_mgr1.get_mount_table().unwrap();
    let follower_mnt = mnt_mgr2.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);
    assert_eq!(leader_mnt.len(), follower_mnt.len());
    assert_eq!(leader_mnt[0], follower_mnt[0]);

    Ok(())
}

// Start 2 masters at the same time to check the correctness of log playback.
#[test]
fn test_raft_consensus_and_state_synchronization_between_two_masters() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    let port1 = NetUtils::get_available_port();
    let port2 = NetUtils::get_available_port();

    let mut conf = ClusterConf::default();
    conf.journal.writer_flush_batch_size = 1;
    conf.journal.writer_flush_batch_ms = 10;
    conf.journal.raft_tick_interval_ms = 100;
    conf.journal.journal_addrs = vec![
        RaftPeer::new(port1 as NodeId, &conf.master.hostname, port1),
        RaftPeer::new(port2 as NodeId, &conf.master.hostname, port2),
    ];
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("raft-1");
    conf.journal.rpc_port = port1;
    let js1 = JournalSystem::from_conf(&conf).unwrap();
    let fs1 = MasterFilesystem::with_js(&conf, &js1);
    let mnt_mgr1 = js1.mount_manager();
    fs1.add_test_worker(worker.clone());
    let fs_monitor1 = js1.master_monitor();

    conf.change_test_meta_dir("raft-2");
    conf.journal.rpc_port = port2;
    let js2 = JournalSystem::from_conf(&conf).unwrap();
    let fs2 = MasterFilesystem::with_js(&conf, &js2);
    let mnt_mgr2 = js2.mount_manager();
    fs2.add_test_worker(worker.clone());
    let fs_monitor2 = js2.master_monitor();

    js1.start_blocking()?;
    js2.start_blocking()?;

    // Wait for the success of the choice of the owner.
    let mut wait = 30 * 1000;
    while wait > 0 {
        let start = TimeSpent::new();
        if fs_monitor1.is_active() || fs_monitor2.is_active() {
            break;
        }
        wait -= start.used_ms();
        thread::sleep(Duration::from_millis(100));
    }

    let (active, standby, mnt_mgr) = {
        if fs_monitor1.is_active() {
            (fs1, fs2, mnt_mgr1.clone())
        } else if fs_monitor2.is_active() {
            (fs2, fs1, mnt_mgr2.clone())
        } else {
            return err_box!("Not found active master");
        }
    };

    info!("state 1 {:?}", fs_monitor1.journal_state());
    info!("state 2 {:?}", fs_monitor2.journal_state());

    run(&active, &worker)?;
    run_mnt(mnt_mgr.clone())?;

    thread::sleep(Duration::from_secs(30));

    active.print_tree();
    standby.print_tree();

    assert_eq!(active.last_inode_id(), standby.last_inode_id());
    assert_eq!(active.sum_hash(), standby.sum_hash());

    let leader_mnt = mnt_mgr1.get_mount_table().unwrap();
    let follower_mnt = mnt_mgr2.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);
    assert_eq!(leader_mnt.len(), follower_mnt.len());
    assert_eq!(leader_mnt[0], follower_mnt[0]);

    Ok(())
}

fn run(fs_leader: &MasterFilesystem, worker: &WorkerInfo) -> CommonResult<()> {
    let address = ClientAddress::default();
    /************* Master node execution log **************/
    // Create a directory
    fs_leader.mkdir("/journal/a", true)?;
    fs_leader.mkdir("/journal_1/a", true)?;
    fs_leader.mkdir("/journal_1/b", true)?;
    fs_leader.mkdir("/journal_2/a", true)?;
    fs_leader.mkdir("/journal_2/b", true)?;

    // Create a file.
    let status = fs_leader.create("/journal/b/test.log", true)?;

    // Assign block
    let block = fs_leader.add_block(&status.path, address.clone(), vec![], vec![], 0, None)?;

    // Complete the file.
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 10,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.complete_file(&status.path, 10, vec![commit], &address.client_name, false)?;

    // File renaming
    fs_leader.rename(
        "/journal/b/test.log",
        "/journal/a/test.log",
        RenameFlags::empty(),
    )?;

    // delete
    fs_leader.delete("/journal_2", true)?;

    let path = "/journal/append.log";
    fs_leader.create(path, true)?;

    let block = fs_leader.add_block(path, address.clone(), vec![], vec![], 0, None)?;
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 10,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.complete_file(path, 10, vec![commit], "", false)?;

    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 13,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.open_file(
        path,
        CreateFileOpts::with_create(true),
        OpenFlags::new_create(),
    )?;
    fs_leader.complete_file(path, 13, vec![commit], "", false)?;

    Ok(())
}

fn run_mnt(mnt_mgr: Arc<MountManager>) -> CommonResult<()> {
    /************* Master node execution log **************/
    //mount oss://cluster1/ -> /x/y/z
    let mgr = mnt_mgr;
    let mount_uri = CurvineURI::new("/x/y/z")?;
    let ufs_uri = CurvineURI::new("oss://cluster1/")?;
    let mut config = HashMap::new();
    config.insert("k1".to_string(), "v1".to_string());
    let mnt_opt = MountOptions::builder().set_properties(config).build();
    mgr.mount(
        None,
        mount_uri.path(),
        ufs_uri.encode_uri().as_ref(),
        &mnt_opt,
    )?;

    //mount hdfs://cluster1/ -> /x/z/y
    let mount_uri = CurvineURI::new("/x/z/y")?;
    let ufs_uri = CurvineURI::new("hdfs://cluster1/")?;
    let mut config = HashMap::new();
    config.insert("k2".to_string(), "v1".to_string());
    let mnt_opt = MountOptions::builder().build();
    mgr.mount(
        None,
        mount_uri.path(),
        ufs_uri.encode_uri().as_ref(),
        &mnt_opt,
    )?;

    // umount
    let mount_uri = CurvineURI::new("/x/z/y")?;
    mgr.umount(mount_uri.path())?;

    Ok(())
}

// Replay all pending journal entries from the leader to the follower loader.
fn replay_entries(js: &JournalSystem, loader: &JournalLoader) -> CommonResult<()> {
    let entries = js.fs().fs_dir.read().take_entries();
    for entry in entries {
        loader.apply_entry(entry)?;
    }
    Ok(())
}

// Assert that a path exists on both filesystems and that all tracked fields are identical.
fn assert_path_consistent(
    path: &str,
    leader: &MasterFilesystem,
    follower: &MasterFilesystem,
) -> CommonResult<()> {
    let l_exists = leader.exists(path)?;
    let f_exists = follower.exists(path)?;
    assert_eq!(
        l_exists, f_exists,
        "exists mismatch for path {path}: leader={l_exists} follower={f_exists}"
    );
    if l_exists {
        let l = leader.file_status(path)?;
        let f = follower.file_status(path)?;
        assert_eq!(l.id, f.id, "[{path}] id mismatch");
        assert_eq!(l.is_dir, f.is_dir, "[{path}] is_dir mismatch");
        assert_eq!(
            l.is_complete, f.is_complete,
            "[{path}] is_complete mismatch"
        );
        assert_eq!(l.len, f.len, "[{path}] len mismatch");
        assert_eq!(l.file_type, f.file_type, "[{path}] file_type mismatch");
        assert_eq!(l.owner, f.owner, "[{path}] owner mismatch");
        assert_eq!(l.group, f.group, "[{path}] group mismatch");
        assert_eq!(l.mode, f.mode, "[{path}] mode mismatch");
        assert_eq!(l.nlink, f.nlink, "[{path}] nlink mismatch");
        assert_eq!(l.target, f.target, "[{path}] symlink target mismatch");
        assert_eq!(
            l.storage_policy.ttl_ms, f.storage_policy.ttl_ms,
            "[{path}] ttl_ms mismatch"
        );
        assert_eq!(
            l.storage_policy.ufs_mtime, f.storage_policy.ufs_mtime,
            "[{path}] ufs_mtime mismatch"
        );
    }
    Ok(())
}

// Assert that a path does NOT exist on either filesystem.
fn assert_path_deleted(
    path: &str,
    leader: &MasterFilesystem,
    follower: &MasterFilesystem,
) -> CommonResult<()> {
    assert!(
        !leader.exists(path)?,
        "leader: path {path} should not exist"
    );
    assert!(
        !follower.exists(path)?,
        "follower: path {path} should not exist"
    );
    Ok(())
}

// Assert that directory listing counts match between leader and follower.
fn assert_list_consistent(
    path: &str,
    leader: &MasterFilesystem,
    follower: &MasterFilesystem,
) -> CommonResult<()> {
    let l_list = leader.list_status(path)?;
    let f_list = follower.list_status(path)?;
    assert_eq!(
        l_list.len(),
        f_list.len(),
        "list_status count mismatch for {path}: leader={} follower={}",
        l_list.len(),
        f_list.len()
    );
    Ok(())
}

/// Comprehensive journal replay test covering all journal entry types.
///
/// For every operation the entry is immediately replayed on a follower filesystem,
/// and the specific properties changed by that operation are verified for consistency.
/// A final sum_hash / last_inode_id check guarantees full state equivalence.
#[test]
fn test_journal_replay_all_operations_with_per_op_consistency() {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    // ---------- leader ----------
    conf.change_test_meta_dir("meta-full-ops-leader");
    let js_leader = JournalSystem::from_conf(&conf).unwrap();
    let fs_leader = MasterFilesystem::with_js(&conf, &js_leader);
    let mnt_leader = js_leader.mount_manager();
    fs_leader.add_test_worker(worker.clone());

    // ---------- follower ----------
    conf.change_test_meta_dir("meta-full-ops-follower");
    let js_follower = JournalSystem::from_conf(&conf).unwrap();
    let fs_follower = MasterFilesystem::with_js(&conf, &js_follower);
    let mnt_follower = js_follower.mount_manager();
    let loader = JournalLoader::new_replay_loader(
        fs_follower.fs_dir(),
        mnt_follower.clone(),
        &conf.journal,
        js_follower.job_manager(),
    );

    let addr = ClientAddress::default();

    // ===== 1. mkdir non-recursive =====
    info!("=== 1. mkdir non-recursive ===");
    fs_leader.mkdir("/t", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t", &fs_leader, &fs_follower).unwrap();
    assert!(
        fs_leader.file_status("/t").unwrap().is_dir,
        "mkdir: should be dir"
    );
    assert!(
        fs_follower.file_status("/t").unwrap().is_dir,
        "mkdir: follower should be dir"
    );

    // ===== 2. mkdir recursive (creates parent directories) =====
    info!("=== 2. mkdir recursive ===");
    fs_leader.mkdir("/t/a/b/c", true).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    for p in ["/t/a", "/t/a/b", "/t/a/b/c"] {
        assert_path_consistent(p, &fs_leader, &fs_follower).unwrap();
        assert!(
            fs_leader.file_status(p).unwrap().is_dir,
            "mkdir recursive: {p} should be dir"
        );
    }
    assert_list_consistent("/t/a/b", &fs_leader, &fs_follower).unwrap();

    // ===== 3. create file =====
    info!("=== 3. create file ===");
    fs_leader.mkdir("/t/files", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    let cr = fs_leader.create("/t/files/hello.txt", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/hello.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/hello.txt").unwrap();
    assert!(!l_st.is_dir, "create: should not be dir");
    assert!(!l_st.is_complete, "create: should not be complete yet");
    assert_eq!(
        cr.id,
        fs_follower.file_status("/t/files/hello.txt").unwrap().id,
        "create: inode id mismatch"
    );

    // ===== 4. add_block =====
    info!("=== 4. add_block ===");
    let blk1 = fs_leader
        .add_block("/t/files/hello.txt", addr.clone(), vec![], vec![], 0, None)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    // File is still incomplete after add_block alone.
    assert!(
        !fs_follower
            .file_status("/t/files/hello.txt")
            .unwrap()
            .is_complete
    );

    // ===== 5. complete_file =====
    info!("=== 5. complete_file ===");
    let commit1 = CommitBlock {
        block_id: blk1.block.id,
        block_len: 128,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader
        .complete_file("/t/files/hello.txt", 128, vec![commit1], "", false)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/hello.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/hello.txt").unwrap();
    assert!(l_st.is_complete, "complete_file: should be complete");
    assert_eq!(l_st.len, 128, "complete_file: len should be 128");
    assert_eq!(
        fs_follower.file_status("/t/files/hello.txt").unwrap().len,
        128,
        "complete_file: follower len mismatch"
    );

    // ===== 6. set_attr: owner / group / mode =====
    info!("=== 6. set_attr owner/group/mode ===");
    let attr = SetAttrOptsBuilder::new()
        .owner("alice")
        .group("devs")
        .mode(0o644)
        .build();
    fs_leader.set_attr("/t/files/hello.txt", attr).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/hello.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/hello.txt").unwrap();
    assert_eq!(l_st.owner, "alice", "set_attr: owner should be alice");
    assert_eq!(l_st.group, "devs", "set_attr: group should be devs");
    assert_eq!(l_st.mode, 0o644, "set_attr: mode should be 0o644");

    // ===== 7. set_attr: TTL =====
    info!("=== 7. set_attr TTL ===");
    let attr = SetAttrOptsBuilder::new()
        .ttl_ms(3_600_000)
        .ttl_action(TtlAction::Free)
        .build();
    fs_leader.set_attr("/t/files/hello.txt", attr).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/hello.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/hello.txt").unwrap();
    assert_eq!(
        l_st.storage_policy.ttl_ms, 3_600_000,
        "set_attr: ttl_ms should be 3600000"
    );

    // ===== 8. rename file =====
    info!("=== 8. rename file ===");
    fs_leader
        .rename(
            "/t/files/hello.txt",
            "/t/files/world.txt",
            RenameFlags::empty(),
        )
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_deleted("/t/files/hello.txt", &fs_leader, &fs_follower).unwrap();
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();
    // Attributes should survive the rename.
    let l_st = fs_leader.file_status("/t/files/world.txt").unwrap();
    assert_eq!(l_st.owner, "alice", "rename: owner should be preserved");
    assert_eq!(
        fs_follower.file_status("/t/files/world.txt").unwrap().owner,
        "alice",
        "rename: follower owner mismatch"
    );

    // ===== 9. rename directory (includes children) =====
    info!("=== 9. rename directory ===");
    fs_leader.mkdir("/t/old_dir", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    fs_leader.mkdir("/t/old_dir/child", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    fs_leader
        .rename("/t/old_dir", "/t/new_dir", RenameFlags::empty())
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_deleted("/t/old_dir", &fs_leader, &fs_follower).unwrap();
    assert_path_consistent("/t/new_dir", &fs_leader, &fs_follower).unwrap();
    assert_path_consistent("/t/new_dir/child", &fs_leader, &fs_follower).unwrap();

    // ===== 10. symlink =====
    info!("=== 10. symlink ===");
    fs_leader.mkdir("/t/links", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    // symlink(target, link_path, force, mode)
    fs_leader
        .symlink("/t/files/world.txt", "/t/links/sym.txt", false, 0o644)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/links/sym.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/links/sym.txt").unwrap();
    let f_st = fs_follower.file_status("/t/links/sym.txt").unwrap();
    assert_eq!(
        l_st.file_type,
        FileType::Link,
        "symlink: leader should have Link type"
    );
    assert_eq!(
        l_st.target,
        Some("/t/files/world.txt".to_string()),
        "symlink: target wrong on leader"
    );
    assert_eq!(l_st.target, f_st.target, "symlink: target mismatch");

    // ===== 11. hard link =====
    info!("=== 11. hard link ===");
    fs_leader
        .link("/t/files/world.txt", "/t/links/hard.txt")
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/links/hard.txt", &fs_leader, &fs_follower).unwrap();
    let l_src = fs_leader.file_status("/t/files/world.txt").unwrap();
    let l_lnk = fs_leader.file_status("/t/links/hard.txt").unwrap();
    let f_src = fs_follower.file_status("/t/files/world.txt").unwrap();
    let f_lnk = fs_follower.file_status("/t/links/hard.txt").unwrap();
    assert_eq!(
        l_src.id, l_lnk.id,
        "hard link: leader src and link should share inode id"
    );
    assert_eq!(
        f_src.id, f_lnk.id,
        "hard link: follower src and link should share inode id"
    );
    assert!(
        l_src.nlink >= 2,
        "hard link: nlink should be >= 2 on leader"
    );
    assert_eq!(l_src.nlink, f_src.nlink, "hard link: nlink mismatch");

    // ===== 12. delete hard link (nlink decreases) =====
    info!("=== 12. delete hard link ===");
    fs_leader.delete("/t/links/hard.txt", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_deleted("/t/links/hard.txt", &fs_leader, &fs_follower).unwrap();
    // Original file still exists; nlink must have decreased consistently.
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();
    let l_src = fs_leader.file_status("/t/files/world.txt").unwrap();
    let f_src = fs_follower.file_status("/t/files/world.txt").unwrap();
    assert_eq!(l_src.nlink, f_src.nlink, "delete hard link: nlink mismatch");

    // ===== 13. delete symlink (original file unaffected) =====
    info!("=== 13. delete symlink ===");
    fs_leader.delete("/t/links/sym.txt", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_deleted("/t/links/sym.txt", &fs_leader, &fs_follower).unwrap();
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();

    // ===== 14. reopen file (append) =====
    info!("=== 14. reopen file (append) ===");
    let opts = CreateFileOpts::with_create(false);
    fs_leader
        .open_file("/t/files/world.txt", opts, OpenFlags::new_append())
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    // File should still show the previous len while reopened.
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();

    // ===== 15. add_block + complete_file in append context =====
    info!("=== 15. add_block + complete_file (append) ===");
    let blk2 = fs_leader
        .add_block("/t/files/world.txt", addr.clone(), vec![], vec![], 0, None)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    let commit2 = CommitBlock {
        block_id: blk2.block.id,
        block_len: 64,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader
        .complete_file("/t/files/world.txt", 192, vec![commit2], "", false)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/world.txt").unwrap();
    assert_eq!(l_st.len, 192, "append: len should be 192");
    assert!(l_st.is_complete, "append: file should be complete");
    assert_eq!(
        fs_follower.file_status("/t/files/world.txt").unwrap().len,
        192,
        "append: follower len mismatch"
    );

    // ===== 16. overwrite file (create on existing path -> OverWriteFile entry) =====
    info!("=== 16. overwrite file ===");
    fs_leader.create("/t/files/world.txt", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/world.txt").unwrap();
    assert_eq!(l_st.len, 0, "overwrite: len should be 0 after truncate");
    assert!(!l_st.is_complete, "overwrite: file should not be complete");
    assert_eq!(
        fs_follower.file_status("/t/files/world.txt").unwrap().len,
        0,
        "overwrite: follower len should also be 0"
    );

    // ===== 17. complete the overwritten file, then set ufs_mtime =====
    info!("=== 17. complete overwritten file + set ufs_mtime ===");
    let blk3 = fs_leader
        .add_block("/t/files/world.txt", addr.clone(), vec![], vec![], 0, None)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    let commit3 = CommitBlock {
        block_id: blk3.block.id,
        block_len: 50,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader
        .complete_file("/t/files/world.txt", 50, vec![commit3], "", false)
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    // Simulate UFS sync by setting ufs_mtime via set_attr.
    let ufs_now = LocalTime::mills() as i64;
    let attr = SetAttrOptsBuilder::new().ufs_mtime(ufs_now).build();
    fs_leader.set_attr("/t/files/world.txt", attr).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_consistent("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();
    let l_st = fs_leader.file_status("/t/files/world.txt").unwrap();
    let f_st = fs_follower.file_status("/t/files/world.txt").unwrap();
    assert!(
        l_st.storage_policy.ufs_mtime > 0,
        "set_attr: ufs_mtime should be > 0 on leader"
    );
    assert_eq!(
        l_st.storage_policy.ufs_mtime, f_st.storage_policy.ufs_mtime,
        "set_attr: ufs_mtime mismatch"
    );

    // ===== 18. free (cache eviction; requires ufs_mtime > 0) =====
    info!("=== 18. free ===");
    fs_leader.free("/t/files/world.txt").unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    assert_path_deleted("/t/files/world.txt", &fs_leader, &fs_follower).unwrap();

    // ===== 19. delete directory (recursive, with nested files) =====
    info!("=== 19. delete directory recursive ===");
    fs_leader.mkdir("/t/to_del/d1/d2", true).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    fs_leader.create("/t/to_del/d1/f1.txt", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    fs_leader.create("/t/to_del/d1/d2/f2.txt", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    // Verify all paths exist on both sides before deletion.
    for p in [
        "/t/to_del",
        "/t/to_del/d1",
        "/t/to_del/d1/d2",
        "/t/to_del/d1/f1.txt",
    ] {
        assert_path_consistent(p, &fs_leader, &fs_follower).unwrap();
    }
    fs_leader.delete("/t/to_del", true).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    for p in ["/t/to_del", "/t/to_del/d1", "/t/to_del/d1/f1.txt"] {
        assert_path_deleted(p, &fs_leader, &fs_follower).unwrap();
    }

    // ===== 20. multi-block file (3 blocks) =====
    info!("=== 20. multi-block file ===");
    fs_leader.mkdir("/t/multi", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    fs_leader.create("/t/multi/big.dat", false).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    let mut total_len: i64 = 0;
    for i in 0..3 {
        let blk = fs_leader
            .add_block("/t/multi/big.dat", addr.clone(), vec![], vec![], 0, None)
            .unwrap();
        replay_entries(&js_leader, &loader).unwrap();
        let blen: i64 = 100 * (i + 1);
        let commit = CommitBlock {
            block_id: blk.block.id,
            block_len: blen,
            locations: vec![BlockLocation::with_id(worker.worker_id())],
        };
        total_len += blen;
        // Only complete on the last block; intermediate blocks just get add_block.
        if i == 2 {
            fs_leader
                .complete_file("/t/multi/big.dat", total_len, vec![commit], "", false)
                .unwrap();
            replay_entries(&js_leader, &loader).unwrap();
        } else {
            // Commit the intermediate block without completing the file.
            fs_leader
                .complete_file(
                    "/t/multi/big.dat",
                    total_len - blen, // file_len stays at previous length
                    vec![commit],
                    "",
                    false,
                )
                .unwrap();
            replay_entries(&js_leader, &loader).unwrap();
        }
    }
    assert_path_consistent("/t/multi/big.dat", &fs_leader, &fs_follower).unwrap();
    assert!(
        fs_leader
            .file_status("/t/multi/big.dat")
            .unwrap()
            .is_complete,
        "multi-block: file should be complete"
    );
    assert_eq!(
        fs_leader.file_status("/t/multi/big.dat").unwrap().len,
        fs_follower.file_status("/t/multi/big.dat").unwrap().len,
        "multi-block: len mismatch"
    );

    // ===== 21. mount =====
    info!("=== 21. mount ===");
    let mount_path = CurvineURI::new("/t/mnt").unwrap();
    let ufs_path = CurvineURI::new("oss://test-bucket/").unwrap();
    let mnt_opt = MountOptions::builder().build();
    mnt_leader
        .mount(
            None,
            mount_path.path(),
            ufs_path.encode_uri().as_ref(),
            &mnt_opt,
        )
        .unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    let l_tbl = mnt_leader.get_mount_table().unwrap();
    let f_tbl = mnt_follower.get_mount_table().unwrap();
    assert_eq!(
        l_tbl.len(),
        f_tbl.len(),
        "mount: table size mismatch after mount"
    );
    let l_entry = l_tbl.iter().find(|m| m.cv_path == "/t/mnt");
    let f_entry = f_tbl.iter().find(|m| m.cv_path == "/t/mnt");
    assert!(
        l_entry.is_some(),
        "mount: leader entry for /t/mnt not found"
    );
    assert!(
        f_entry.is_some(),
        "mount: follower entry for /t/mnt not found"
    );
    assert_eq!(
        l_entry.unwrap().ufs_path,
        f_entry.unwrap().ufs_path,
        "mount: ufs_path mismatch"
    );

    // ===== 22. umount =====
    info!("=== 22. umount ===");
    mnt_leader.umount(mount_path.path()).unwrap();
    replay_entries(&js_leader, &loader).unwrap();
    let l_tbl = mnt_leader.get_mount_table().unwrap();
    let f_tbl = mnt_follower.get_mount_table().unwrap();
    assert_eq!(
        l_tbl.len(),
        f_tbl.len(),
        "umount: table size mismatch after umount"
    );
    assert!(
        l_tbl.iter().all(|m| m.cv_path != "/t/mnt"),
        "umount: leader entry still present"
    );
    assert!(
        f_tbl.iter().all(|m| m.cv_path != "/t/mnt"),
        "umount: follower entry still present"
    );

    // ===== Final global consistency check =====
    info!("=== Final global consistency check ===");
    fs_leader.print_tree();
    fs_follower.print_tree();
    assert_eq!(
        fs_leader.last_inode_id(),
        fs_follower.last_inode_id(),
        "final: last_inode_id mismatch"
    );
    assert_eq!(
        fs_leader.sum_hash(),
        fs_follower.sum_hash(),
        "final: sum_hash mismatch"
    );
    let l_tbl = mnt_leader.get_mount_table().unwrap();
    let f_tbl = mnt_follower.get_mount_table().unwrap();
    assert_eq!(l_tbl.len(), f_tbl.len(), "final: mount table size mismatch");
}

// Test snapshot restart
#[test]
fn test_master_restart_with_snapshot_recovery() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("meta-test-restart");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    let mnt_mgr = js.mount_manager();
    fs.add_test_worker(worker.clone());

    fs.mkdir("/a", false)?;
    run_mnt(mnt_mgr.clone())?;

    assert!(fs.exists("/a")?);

    let leader_mnt = mnt_mgr.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);

    // Create a snapshot manually.
    js.create_snapshot()?;

    drop(fs);
    drop(js);
    drop(mnt_mgr);

    conf.format_master = false;
    let js = JournalSystem::from_conf(&conf)?;
    js.apply_snapshot()?;
    let fs = MasterFilesystem::with_js(&conf, &js);
    let mnt_mgr = js.mount_manager();
    fs.add_test_worker(worker.clone());
    assert!(fs.exists("/a")?);
    let leader_mnt = mnt_mgr.get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);

    Ok(())
}
