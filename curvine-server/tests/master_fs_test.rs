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
use curvine_server::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use curvine_server::master::journal::JournalSystem;
use curvine_server::master::{JobHandler, JobManager, Master, MasterHandler, RpcContext};
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
    let job_manager = Arc::new(JobManager::from_cluster_conf(
        fs.clone(),
        mount_manager.clone(),
        rt.clone(),
        &conf,
    ));
    let job_handler = JobHandler::new(job_manager);
    MasterHandler::new(&conf, fs, retry_cache, None, mount_manager, job_handler)
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

    let res2 = fs.mkdir("/a3/b/c", true)?;
    assert!(res2);

    let list = fs.list_status("/")?;
    assert_eq!(list.len(), 3);

    fs.print_tree();

    Ok(())
}

fn delete(fs: &MasterFilesystem) -> CommonResult<()> {
    let res1 = fs.delete("/a", false);
    assert!(res1.is_err());

    fs.mkdir("/a/b/c/d", true)?;

    fs.delete("/a/b/c", true)?;

    fs.print_tree();
    Ok(())
}

fn rename(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b/c", true)?;
    fs.print_tree();

    fs.rename("/a/b/c", "/a/x")?;
    fs.print_tree();
    Ok(())
}

fn create_file(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b", true)?;

    let opts = CreateFileOpts::with_create(false);
    fs.create_with_opts("/a/b/1.log", opts.clone())?;

    fs.create_with_opts("/a/b/2.log", opts)?;

    fs.print_tree();

    Ok(())
}

fn get_file_info(fs: &MasterFilesystem) -> CommonResult<()> {
    let opts = CreateFileOpts::with_create(true);
    fs.create_with_opts("/a/b/xx.log", opts)?;
    fs.print_tree();

    let info = fs.file_status("/a/b/xx.log")?;
    println!("info = {:#?}", info);
    Ok(())
}

fn list_status(fs: &MasterFilesystem) -> CommonResult<()> {
    let opts = CreateFileOpts::with_create(true);
    fs.create_with_opts("/a/1.log", opts)?;

    fs.mkdir("/a/d1", true)?;
    fs.mkdir("/a/d2", true)?;

    let list = fs.list_status("/a")?;
    println!("list = {:#?}", list);

    fs.print_tree();

    Ok(())
}

fn state(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b", true)?;
    fs.mkdir("/a/c", true)?;
    fs.create("/a/file/1.log", true)?;
    fs.create("/a/file/2.log", true)?;

    fs.create("/a/rename/old.log", true)?;
    fs.rename("/a/rename/old.log", "/a/c/new.log")?;
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
    assert!(is_retry);

    // Retry request is normal
    let _ = handler.retry_check_create_file(&mut ctx)?;

    Ok(())
}

fn add_block_retry(fs: &MasterFilesystem) -> CommonResult<()> {
    let path = "/add_block_retry.log";
    let addr = ClientAddress::default();
    let opts = CreateFileOpts::with_create(false);
    let status = fs.create_with_opts(path, opts).unwrap();

    let b1 = fs.add_block(path, addr.clone(), None, vec![]).unwrap();
    let b2 = fs.add_block(path, addr.clone(), None, vec![]).unwrap();

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
        .add_block(path, addr.clone(), Some(commit.clone()), vec![])
        .unwrap();
    let b2 = fs
        .add_block(path, addr.clone(), Some(commit), vec![])
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
    let opts = CreateFileOpts::with_create(false);
    fs.create_with_opts(path, opts)?;

    let b1 = fs.add_block(path, addr.clone(), None, vec![])?;

    let commit = CommitBlock {
        block_id: b1.block.id,
        block_len: b1.block.len,
        locations: vec![BlockLocation {
            worker_id: b1.locs[0].worker_id,
            storage_type: Default::default(),
        }],
    };

    let f1 = fs.complete_file(path, b1.block.len, Some(commit.clone()), &addr.client_name)?;
    assert!(f1);

    let f2 = fs.complete_file(path, b1.block.len, Some(commit.clone()), &addr.client_name)?;
    assert!(!f2);

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
    let mut ctx = RpcContext::new(&msg);
    handler.mkdir(&mut ctx)?;

    let id = Utils::req_id();
    let req = RenameRequest {
        src: "/rename_retry".to_string(),
        dst: "/rename_retry1".to_string(),
    };

    let f1 = handler.rename0(id, req.clone())?;
    assert!(f1);

    let f2 = handler.rename0(id, req.clone())?;
    assert!(f2);

    Ok(())
}
