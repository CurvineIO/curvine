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

use curvine_common::conf::{ClusterConf, JournalConf, MasterConf};
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CreateFileRequest, DeleteRequest, MkdirOptsProto, MkdirRequest, RenameRequest,
};
use curvine_common::state::{
    BlockLocation, ClientAddress, CommitBlock, CreateFileOpts, WorkerInfo,
};
use curvine_common::state::{OpenFlags, RenameFlags, SetAttrOptsBuilder};
use curvine_server::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::replication::master_replication_manager::MasterReplicationManager;
use curvine_server::master::{JobHandler, JobManager, Master, MasterHandler, RpcContext};
use orpc::common::LocalTime;
use orpc::common::Utils;
use orpc::message::Builder;
use orpc::runtime::AsyncRuntime;
use orpc::CommonResult;
use std::sync::Arc;
// Test the master filesystem function separately.
// This test does not require a cluster startup
fn new_fs(format: bool, name: &str) -> MasterFilesystem {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: format,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("master-fs-test/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!("master-fs-test/journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    fs.add_test_worker(WorkerInfo::default());

    fs
}

fn new_handler() -> MasterHandler {
    Master::init_test_metrics();

    let mut conf = ClusterConf::format();
    conf.journal.enable = false;

    conf.master.meta_dir = Utils::test_sub_dir("master-fs-test/meta-retry");
    conf.journal.journal_dir = Utils::test_sub_dir("master-fs-test/journal-retry");

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    fs.add_test_worker(WorkerInfo::default());
    let retry_cache = FsRetryCache::with_conf(&conf.master);

    let mount_manager = journal_system.mount_manager();
    let rt = Arc::new(AsyncRuntime::single());
    let replication_manager =
        MasterReplicationManager::new(&fs, &conf, &rt, &journal_system.worker_manager());
    let job_manager = Arc::new(JobManager::from_cluster_conf(
        fs.clone(),
        mount_manager.clone(),
        rt.clone(),
        &conf,
    ));
    MasterHandler::new(
        &conf,
        fs,
        retry_cache,
        None,
        mount_manager,
        JobHandler::new(job_manager),
        replication_manager,
    )
}

#[test]
fn fs_test() -> CommonResult<()> {
    let fs = new_fs(true, "fs_test");

    mkdir(&fs)?;
    delete(&fs)?;
    rename(&fs)?;
    create_file(&fs)?;
    get_file_info(&fs)?;
    list_status(&fs)?;
    state(&fs)?;

    Ok(())
}

#[test]
fn retry_test() -> CommonResult<()> {
    let mut handler = new_handler();
    let fs = handler.clone_fs();

    create_file_retry(&mut handler).unwrap();
    add_block_retry(&fs).unwrap();
    complete_file_retry(&fs).unwrap();
    delete_file_retry(&mut handler).unwrap();
    rename_retry(&mut handler).unwrap();

    Ok(())
}

#[test]
fn restore() -> CommonResult<()> {
    let fs = new_fs(true, "restore");
    fs.mkdir("/a", false)?;
    fs.mkdir("/x1/x2/x3", true)?;
    let hash1 = fs.sum_hash();
    drop(fs);

    let fs = new_fs(false, "restore");
    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    let hash2 = fs.sum_hash();
    assert_eq!(hash1, hash2);

    Ok(())
}

fn mkdir(fs: &MasterFilesystem) -> CommonResult<()> {
    let res1 = fs.mkdir("/a/b", false);
    assert!(res1.is_err());

    let _ = fs.mkdir("/a1", true)?;
    let _ = fs.mkdir("/a2", true)?;

    let res2 = fs.mkdir("/a3/b/c", true);
    assert!(res2.is_ok());

    // Verify directories exist after creation
    assert!(fs.exists("/a1")?);
    assert!(fs.exists("/a2")?);
    assert!(fs.exists("/a3")?);
    assert!(fs.exists("/a3/b")?);
    assert!(fs.exists("/a3/b/c")?);

    let list = fs.list_status("/")?;
    assert_eq!(list.len(), 3);

    fs.print_tree();

    Ok(())
}

fn delete(fs: &MasterFilesystem) -> CommonResult<()> {
    let res1 = fs.delete("/a", false);
    assert!(res1.is_err());

    fs.mkdir("/a/b/c/d", true)?;

    // Verify directory structure exists before deletion
    assert!(fs.exists("/a")?);
    assert!(fs.exists("/a/b")?);
    assert!(fs.exists("/a/b/c")?);
    assert!(fs.exists("/a/b/c/d")?);

    fs.delete("/a/b/c", true)?;

    // Verify deletion results
    assert!(!fs.exists("/a/b/c")?);
    assert!(!fs.exists("/a/b/c/d")?);
    // Parent directories should still exist
    assert!(fs.exists("/a")?);
    assert!(fs.exists("/a/b")?);

    fs.print_tree();
    Ok(())
}

fn rename(fs: &MasterFilesystem) -> CommonResult<()> {
    // Test directory rename
    fs.mkdir("/a/b/c", true)?;
    println!("=== Before directory rename ===");
    fs.print_tree();

    // Verify original paths exist
    assert!(fs.exists("/a/b/c")?);
    assert!(fs.exists("/a/b")?);
    assert!(fs.exists("/a")?);

    // Execute rename operation
    fs.rename("/a/b/c", "/a/x", RenameFlags::empty())?;

    println!("=== After directory rename ===");
    fs.print_tree();

    // Verify rename results
    // Original path should not exist
    assert!(!fs.exists("/a/b/c")?);
    // New path should exist
    assert!(fs.exists("/a/x")?);
    // Parent directory should still exist
    assert!(fs.exists("/a")?);
    // Intermediate directory b should still exist (since rename only moved c)
    assert!(fs.exists("/a/b")?);

    // Test file rename
    fs.create("/a.txt", true)?;

    println!("=== Before file rename ===");
    fs.print_tree();

    // Verify original file exists
    assert!(fs.exists("/a.txt")?);

    // Execute file rename operation
    fs.rename("/a.txt", "/aaa.txt", RenameFlags::empty())?;

    println!("=== After file rename ===");
    fs.print_tree();

    // Verify file rename results
    // Original file should not exist
    assert!(!fs.exists("/a.txt")?);
    // New file should exist
    assert!(fs.exists("/aaa.txt")?);

    // Test file rename to existing directory scenario
    // Create directory /a/b
    fs.mkdir("/a/b", true)?;
    // Create file /a/1.log
    fs.create("/a/1.log", true)?;

    println!("=== Before file rename to directory ===");
    fs.print_tree();

    // Verify original file exists
    assert!(fs.exists("/a/1.log")?);
    assert!(fs.exists("/a/b")?);

    // Execute file rename to directory operation
    // Expected result: /a/1.log -> /a/b/1.log
    fs.rename("/a/1.log", "/a/b", RenameFlags::empty())?;

    println!("=== After file rename to directory ===");
    fs.print_tree();

    // Verify rename results
    // Original file should not exist
    assert!(!fs.exists("/a/1.log")?);
    // New file should exist under directory b
    assert!(fs.exists("/a/b/1.log")?);
    // Directory b should still exist
    assert!(fs.exists("/a/b")?);

    Ok(())
}

fn create_file(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/test_dir/subdir", true)?;

    // Verify directory exists before file creation
    assert!(fs.exists("/test_dir/subdir")?);

    fs.create("/test_dir/subdir/file1.log", false)?;
    fs.create("/test_dir/subdir/file2.log", false)?;

    // Verify files exist after creation
    assert!(fs.exists("/test_dir/subdir/file1.log")?);
    assert!(fs.exists("/test_dir/subdir/file2.log")?);
    // Verify directory still exists
    assert!(fs.exists("/test_dir/subdir")?);

    fs.print_tree();

    // overwrite file
    let oldid = fs.file_status("/test_dir/subdir/file1.log")?.id;
    let opts = CreateFileOpts::with_create(false);
    fs.create_with_opts(
        "/test_dir/subdir/file1.log",
        opts.clone(),
        OpenFlags::new_create().set_overwrite(true),
    )?;
    assert_eq!(oldid, fs.file_status("/test_dir/subdir/file1.log")?.id);

    fs.print_tree();
    Ok(())
}

fn get_file_info(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.create("/a/b/xx.log", true)?;
    fs.print_tree();

    let info = fs.file_status("/a/b/xx.log")?;
    println!("info = {:#?}", info);
    Ok(())
}

fn list_status_with_glob(fs: &MasterFilesystem) -> CommonResult<()> {
    // test 1
    let list_1 = fs.list_status("/*/*.log")?;
    assert_eq!(list_1.len(), 2, "Should find exactly 2 log files");
    println!("list_1: {:?}", list_1);
    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_1 = list_1.clone();
    sorted_list_1.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify first file: /a/1.log
    assert_eq!(
        sorted_list_1[0].path, "/a/b1.log",
        "First file path mismatch"
    );
    assert_eq!(sorted_list_1[0].name, "b1.log", "First file name mismatch");

    // Verify second file: /a/2.log
    assert_eq!(
        sorted_list_1[1].path, "/a/b2.log",
        "Second file path mismatch"
    );
    assert_eq!(sorted_list_1[1].name, "b2.log", "Second file name mismatch");

    // test 2
    let list_2 = fs.list_status("/a/[ac]2.*")?;
    assert_eq!(list_2.len(), 1, "Should find exactly 1 log files");

    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_2 = list_2.clone();
    sorted_list_2.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify second file: /a/c2.txt
    assert_eq!(
        sorted_list_2[0].path, "/a/c2.txt",
        "Second file path mismatch"
    );
    assert_eq!(sorted_list_2[0].name, "c2.txt", "Second file name mismatch");

    // test 3
    let list_3 = fs.list_status("/a/*")?;
    assert_eq!(list_3.len(), 4, "Should find exactly 1 log files");

    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_3 = list_3.clone();
    sorted_list_3.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify second file: /a/b1.txt
    assert_eq!(
        sorted_list_3[0].path, "/a/b1.log",
        "Second file path mismatch"
    );
    assert_eq!(sorted_list_3[0].name, "b1.log", "Second file name mismatch");

    assert_eq!(
        sorted_list_3[3].path, "/a/c2.txt",
        "Second file path mismatch"
    );
    assert_eq!(sorted_list_3[3].name, "c2.txt", "Second file name mismatch");

    Ok(())
}
fn list_status(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.create("/a/b1.log", true)?;
    fs.create("/a/b2.log", true)?;
    fs.create("/a/c1.txt", true)?;
    fs.create("/a/c2.txt", true)?;

    fs.mkdir("/a/d1", true)?;
    fs.mkdir("/a/d2", true)?;

    // list_status without glob pattern

    // list status with glob pattern

    // println!("find 1.log");
    // let list = fs.list_status("/a/1.log")?;
    // println!("list = {:#?}", list);

    // println!("find files in /a/");
    // let list = fs.list_status("/a")?;
    // println!("list = {:#?}", list);

    // println!("find /a/1.log");
    // let list1 = fs.list_status("/a/1.log")?;
    // println!("list1 = {:#?}", list1);

    // println!("find /a/2.log");
    // let list2 = fs.list_status("/a/2.log")?;
    // println!("list2 = {:#?}", list1);

    // println!("find /a/*.log");
    // let list1 = fs.list_status("/a/*.log")?;
    // println!("list1 = {:#?}", list1);

    // println!("find /a/*");
    // let list2 = fs.list_status("/*/*.log")?;
    // println!("list2 = {:#?}", list2);
    // for item in list2.iter() {
    //     println!("item: {:?}, {:?}", item.path, item.name)
    // }
    fs.print_tree();

    let _ = list_status_with_glob(fs);
    Ok(())
}

#[test]
fn link() -> CommonResult<()> {
    let fs = new_fs(true, "link_test");
    fs.mkdir("/a/b", true)?;
    fs.create("/a/b/file.log", true)?;
    fs.print_tree();
    fs.link("/a/b/file.log", "/a/b/file2.log")?;
    assert!(fs.exists("/a/b/file2.log")?);
    fs.print_tree();

    fs.link("/a/b/file.log", "/a/d/file.log")?;
    assert!(fs.exists("/a/d/file.log")?);
    fs.print_tree();

    let inode1 = fs.file_status("/a/b/file.log")?.id;
    let inode2 = fs.file_status("/a/b/file2.log")?.id;
    let inode3 = fs.file_status("/a/d/file.log")?.id;
    assert_eq!(inode1, inode2);
    assert_eq!(inode1, inode3);

    //update to check all linked file attr is same
    let time = LocalTime::mills() as i64;
    let opts = SetAttrOptsBuilder::new().mtime(time).build();
    fs.set_attr("/a/b/file2.log", opts)?;
    fs.print_tree();
    let mtime_t = fs.file_status("/a/b/file2.log")?.mtime;
    assert_eq!(mtime_t, fs.file_status("/a/b/file.log")?.mtime);
    assert_eq!(mtime_t, fs.file_status("/a/d/file.log")?.mtime);

    let nlink_t = fs.file_status("/a/b/file.log")?.nlink;
    assert_eq!(nlink_t, 3);
    let nlink_t = fs.file_status("/a/b/file2.log")?.nlink;
    assert_eq!(nlink_t, 3);
    let nlink_t = fs.file_status("/a/d/file.log")?.nlink;
    assert_eq!(nlink_t, 3);

    fs.delete("/a/b/file.log", true)?;
    assert!(!fs.exists("/a/b/file.log")?);
    assert!(fs.exists("/a/b/file2.log")?);
    assert!(fs.exists("/a/d/file.log")?);
    fs.print_tree();

    let nlink_t = fs.file_status("/a/b/file2.log")?.nlink;
    assert_eq!(nlink_t, 2);
    let nlink_t = fs.file_status("/a/d/file.log")?.nlink;
    assert_eq!(nlink_t, 2);

    //rename file2.log
    fs.rename("/a/b/file2.log", "/a/b/file3.log", RenameFlags::empty())?;
    assert!(!fs.exists("/a/b/file2.log")?);
    assert!(fs.exists("/a/b/file3.log")?);
    fs.print_tree();

    //let nlink_t = fs.file_status("/a/b/file3.log")?.nlink;
    //assert_eq!(nlink_t, 2);

    Ok(())
}

fn state(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b", true)?;
    fs.mkdir("/a/c", true)?;
    fs.create("/a/file/1.log", true)?;
    fs.create("/a/file/2.log", true)?;

    fs.create("/a/rename/old.log", true)?;
    fs.rename("/a/rename/old.log", "/a/c/new.log", RenameFlags::empty())?;
    fs.delete("/a/file/2.log", true)?;

    fs.print_tree();
    let fs_dir = fs.fs_dir.read();
    let mem_hash = fs_dir.root_dir().sum_hash();

    let state_tree = fs_dir.create_tree()?;
    state_tree.print_tree();
    let state_hash = state_tree.sum_hash();

    println!("mem_hash = {}, state_hash = {}", mem_hash, state_hash);
    assert_eq!(mem_hash, state_hash);

    Ok(())
}

fn create_file_retry(handler: &mut MasterHandler) -> CommonResult<()> {
    let req = CreateFileRequest {
        path: "/create_file_retry.log".to_string(),
        flags: OpenFlags::new_create().value(),
        ..Default::default()
    };
    let req_id = Utils::req_id();

    let msg = Builder::new_rpc(RpcCode::CreateFile)
        .req_id(req_id)
        .proto_header(req.clone())
        .build();

    assert!(handler.get_req_cache(req_id).is_none());

    let mut ctx = RpcContext::new(&msg);
    let _ = handler.retry_check_create_file(&mut ctx)?;

    assert_eq!(
        handler.get_req_cache(req_id).unwrap(),
        OperationStatus::Success
    );
    let is_retry = handler.check_is_retry(req_id)?;
    println!("is_retry: {:?}", is_retry);
    assert!(is_retry);

    // Retry request is normal
    let _ = handler.retry_check_create_file(&mut ctx)?;

    Ok(())
}

fn add_block_retry(fs: &MasterFilesystem) -> CommonResult<()> {
    let path = "/add_block_retry.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false).unwrap();

    let b1 = fs.add_block(path, addr.clone(), None, vec![], 0).unwrap();
    let b2 = fs.add_block(path, addr.clone(), None, vec![], 0).unwrap();

    assert_eq!(b1.block.id, b2.block.id);

    let locs = fs.get_block_locations(path).unwrap();
    println!("locs = {:?}", locs);
    assert_eq!(locs.block_locs.len(), 1);

    let commit = CommitBlock {
        block_id: b1.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation {
            worker_id: b1.locs[0].worker_id,
            storage_type: Default::default(),
        }],
    };

    let b1 = fs
        .add_block(
            path,
            addr.clone(),
            Some(commit.clone()),
            vec![],
            status.block_size,
        )
        .unwrap();
    let b2 = fs
        .add_block(path, addr.clone(), Some(commit), vec![], status.block_size)
        .unwrap();
    assert_eq!(b1.block.id, b2.block.id);

    let locs = fs.get_block_locations(path).unwrap();
    println!("locs = {:?}", locs);
    assert_eq!(locs.block_locs.len(), 2);

    Ok(())
}

fn complete_file_retry(fs: &MasterFilesystem) -> CommonResult<()> {
    let path = "/complete_file_retry.log";
    let addr = ClientAddress::default();
    fs.create(path, false)?;

    let b1 = fs.add_block(path, addr.clone(), None, vec![], 0)?;

    let commit = CommitBlock {
        block_id: b1.block.id,
        block_len: b1.block.len,
        locations: vec![BlockLocation {
            worker_id: b1.locs[0].worker_id,
            storage_type: Default::default(),
        }],
    };

    let f1 = fs.complete_file(
        path,
        b1.block.len,
        vec![commit.clone()],
        &addr.client_name,
        false,
    );
    assert!(f1.is_ok());

    let f2 = fs.complete_file(
        path,
        b1.block.len,
        vec![commit.clone()],
        &addr.client_name,
        false,
    );
    assert!(f2.is_ok());

    let status = fs.file_status(path)?;
    println!("status = {:?}", status);
    assert!(status.is_complete);

    Ok(())
}

fn delete_file_retry(handler: &mut MasterHandler) -> CommonResult<()> {
    let msg = Builder::new_rpc(RpcCode::Mkdir)
        .proto_header(MkdirRequest {
            path: "/delete_file_retry".to_string(),
            opts: MkdirOptsProto {
                create_parent: false,
                ..Default::default()
            },
        })
        .build();

    let mut ctx = RpcContext::new(&msg);
    handler.mkdir(&mut ctx)?;

    let id = Utils::req_id();
    let req = DeleteRequest {
        path: "/delete_file_retry".to_string(),
        recursive: false,
    };

    let f1 = handler.delete0(id, req.clone())?;
    assert!(f1);

    let f2 = handler.delete0(id, req.clone())?;
    assert!(f2);

    Ok(())
}

fn rename_retry(handler: &mut MasterHandler) -> CommonResult<()> {
    let msg = Builder::new_rpc(RpcCode::Mkdir)
        .proto_header(MkdirRequest {
            path: "/rename_retry".to_string(),
            opts: MkdirOptsProto {
                create_parent: false,
                ..Default::default()
            },
        })
        .build();
    println!("msg: {:?}", msg);
    let mut ctx = RpcContext::new(&msg);
    handler.mkdir(&mut ctx)?;

    let id = Utils::req_id();
    let req = RenameRequest {
        src: "/rename_retry".to_string(),
        dst: "/rename_retry1".to_string(),
        flags: RenameFlags::empty().value(),
    };

    let f1 = handler.rename0(id, req.clone())?;
    println!("f1: {:?}", f1);
    assert!(f1);

    let f2 = handler.rename0(id, req.clone())?;
    println!("f2: {:?}", f2);
    assert!(f2);

    Ok(())
}
