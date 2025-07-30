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
use curvine_common::proto::{ConsistencyConfig, MountOptions};
use curvine_common::raft::{NodeId, RaftPeer};
use curvine_common::state::{
    BlockLocation, ClientAddress, CommitBlock, CreateFileOpts, WorkerInfo,
};
use curvine_server::master::fs::MasterFilesystem;
use curvine_server::master::journal::{JournalLoader, JournalSystem};
use curvine_server::master::mount::ConsistencyStrategy;
use curvine_server::master::{Master, SyncMountManager};
use log::info;
use orpc::common::{Logger, TimeSpent};
use orpc::io::net::NetUtils;
use orpc::{err_box, CommonResult};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

// First start a master and perform the operation; then start 1 stand by, manually replay the log to check consistency.
#[test]
fn check_journal_state() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("meta-js1");
    let journal_system = JournalSystem::from_conf(&conf)?;
    let fs_leader = MasterFilesystem::new(&conf, &journal_system)?;
    let mnt_mgr1 = journal_system.mount_manager();
    mnt_mgr1.write().set_master_fs(fs_leader.clone());
    fs_leader.add_test_worker(worker.clone());

    run(&fs_leader, &worker)?;
    run_mnt(mnt_mgr1.clone())?;

    /************* Replay log from node **************/
    conf.change_test_meta_dir("meta-js2");
    let follower_journal_system = JournalSystem::from_conf(&conf)?;
    let fs_follower = MasterFilesystem::new(&conf, &follower_journal_system)?;
    let mnt_mgr2 = follower_journal_system.mount_manager();
    mnt_mgr2.write().set_master_fs(fs_follower.clone());
    let journal_loader = JournalLoader::new(fs_follower.fs_dir(), mnt_mgr2.clone(), &conf.journal);

    let entries = journal_system.fs_dir().read().take_entries();
    info!("entries size {}", entries.len());
    for entry in entries {
        journal_loader.apply_entry(entry).unwrap();
    }

    fs_leader.print_tree();
    fs_follower.print_tree();
    assert_eq!(fs_leader.last_inode_id(), fs_follower.last_inode_id());
    assert_eq!(fs_leader.sum_hash(), fs_follower.sum_hash());

    let leader_mnt = mnt_mgr1.read().get_mount_table().unwrap();
    let follower_mnt = mnt_mgr2.read().get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);
    assert_eq!(leader_mnt.len(), follower_mnt.len());
    assert_eq!(leader_mnt[0], follower_mnt[0]);

    Ok(())
}

// Start 2 masters at the same time to check the correctness of log playback.
#[test]
fn check_raft_state() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();

    let port1 = NetUtils::get_available_port();
    let port2 = NetUtils::get_available_port();

    let mut conf = ClusterConf::default();
    conf.journal.writer_flush_batch_size = 1;
    conf.journal.writer_flush_batch_ms = 10;
    conf.journal.journal_addrs = vec![
        RaftPeer::new(port1 as NodeId, &conf.master.hostname, port1),
        RaftPeer::new(port2 as NodeId, &conf.master.hostname, port2),
    ];
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("raft-1");
    conf.journal.rpc_port = port1;
    let js1 = JournalSystem::from_conf(&conf).unwrap();
    let fs1 = MasterFilesystem::new(&conf, &js1).unwrap();
    let mnt_mgr1 = js1.mount_manager();
    mnt_mgr1.write().set_master_fs(fs1.clone());
    fs1.add_test_worker(worker.clone());
    let fs_monitor1 = js1.master_monitor();

    conf.change_test_meta_dir("raft-2");
    conf.journal.rpc_port = port2;
    let js2 = JournalSystem::from_conf(&conf).unwrap();
    let fs2 = MasterFilesystem::new(&conf, &js2).unwrap();
    let mnt_mgr2 = js2.mount_manager();
    mnt_mgr2.write().set_master_fs(fs2.clone());
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

    let leader_mnt = mnt_mgr1.read().get_mount_table().unwrap();
    let follower_mnt = mnt_mgr2.read().get_mount_table().unwrap();
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
    let status =
        fs_leader.create_with_opts("/journal/b/test.log", CreateFileOpts::with_create(true))?;

    // Assign block
    let block = fs_leader.add_block(&status.path, address.clone(), None, vec![])?;

    // Complete the file.
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 10,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.complete_file(&status.path, 10, Some(commit), &address.client_name)?;

    // File renaming
    fs_leader.rename("/journal/b/test.log", "/journal/a/test.log")?;

    // delete
    fs_leader.delete("/journal_2", true)?;

    let path = "/journal/append.log";
    fs_leader.create(path, true)?;

    let block = fs_leader.add_block(path, address.clone(), None, vec![])?;
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 10,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.complete_file(path, 10, Some(commit), "")?;

    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: 13,
        locations: vec![BlockLocation::with_id(worker.worker_id())],
    };
    fs_leader.append_file(path, CreateFileOpts::with_append())?;
    fs_leader.complete_file(path, 13, Some(commit), "")?;

    Ok(())
}

fn run_mnt(mnt_mgr: SyncMountManager) -> CommonResult<()> {
    /************* Master node execution log **************/
    //mount oss://cluster1/ -> /x/y/z
    let mgr = mnt_mgr.read();
    let mount_uri = CurvineURI::new("/x/y/z")?;
    let ufs_uri = CurvineURI::new("oss://cluster1/")?;
    let mut config = HashMap::new();
    config.insert("k1".to_string(), "v1".to_string());
    let mnt_opt = MountOptions {
        update: false,
        properties: config,
        auto_cache: false,
        cache_ttl_secs: None,
        consistency_config: None,
    };
    mgr.mount(None, &mount_uri, &ufs_uri, &mnt_opt)?;

    //mount hdfs://cluster1/ -> /x/z/y
    let mount_uri = CurvineURI::new("/x/z/y")?;
    let ufs_uri = CurvineURI::new("hdfs://cluster1/")?;
    let mut config = HashMap::new();
    config.insert("k2".to_string(), "v1".to_string());
    let mnt_opt = MountOptions {
        update: false,
        properties: config,
        auto_cache: true,
        cache_ttl_secs: Some(10),
        consistency_config: Some(ConsistencyConfig {
            strategy: ConsistencyStrategy::Always.into(),
            period_seconds: Some(10),
        }),
    };
    mgr.mount(None, &mount_uri, &ufs_uri, &mnt_opt)?;

    // umount
    let mount_uri = CurvineURI::new("/x/z/y")?;
    mgr.umount(&mount_uri)?;

    Ok(())
}

// Test snapshot restart
#[test]
fn test_restart() -> CommonResult<()> {
    Logger::default();
    Master::init_test_metrics();
    let mut conf = ClusterConf::default();
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir("meta-test-restart");
    let js = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::new(&conf, &js)?;
    let mnt_mgr = js.mount_manager();
    mnt_mgr.write().set_master_fs(fs.clone());
    fs.add_test_worker(worker.clone());

    fs.mkdir("/a", false)?;
    run_mnt(mnt_mgr.clone())?;

    assert!(fs.exists("/a")?);

    let leader_mnt = mnt_mgr.read().get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);

    // Create a snapshot manually.
    js.create_snapshot()?;

    drop(fs);
    drop(js);
    drop(mnt_mgr);

    conf.format_master = false;
    let js = JournalSystem::from_conf(&conf)?;
    js.apply_snapshot()?;
    let fs = MasterFilesystem::new(&conf, &js)?;
    let mnt_mgr = js.mount_manager();
    mnt_mgr.write().set_master_fs(fs.clone());
    fs.add_test_worker(worker.clone());
    assert!(fs.exists("/a")?);
    let leader_mnt = mnt_mgr.read().get_mount_table().unwrap();
    assert_eq!(leader_mnt.len(), 1);

    Ok(())
}
