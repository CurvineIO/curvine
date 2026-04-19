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
use curvine_common::fs::CurvineURI;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CreateFileRequest, DeleteRequest, MkdirOptsProto, MkdirRequest, RenameRequest,
};
use curvine_common::raft::storage::{AppStorage, ApplyMsg};
use curvine_common::state::MountOptions;
use curvine_common::state::{
    BlockLocation, ClientAddress, CommitBlock, CreateFileOpts, WorkerInfo,
};
use curvine_common::state::{OpenFlags, RenameFlags, SetAttrOptsBuilder};
use curvine_common::utils::SerdeUtils;
use curvine_server::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::replication::master_replication_manager::MasterReplicationManager;
use curvine_server::master::{JobHandler, JobManager, Master, MasterHandler, RpcContext};
use orpc::common::LocalTime;
use orpc::common::Utils;
use orpc::message::Builder;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use raft::eraftpb::Entry;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

// Master metrics gauges are process-wide; tests that assert inode_file_num / inode_dir_num must
// not run in parallel with each other or counts race with other tests' format/init.
static INODE_COUNT_METRICS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn inode_count_metrics_test_lock() -> std::sync::MutexGuard<'static, ()> {
    INODE_COUNT_METRICS_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap()
}

// Use a lightweight filesystem-only setup for tests that do not need the full
// journal runtime lifecycle.
fn new_fs(format: bool, name: &str) -> MasterFilesystem {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: format,
        testing: true, // Enable testing mode to prevent background thread spawn
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("master-fs-test/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            journal_dir: Utils::test_sub_dir(format!(
                "master-fs-test/journal-{}-{}",
                name,
                Utils::rand_str(6)
            )),
            ..Default::default()
        },
        ..Default::default()
    };

    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    fs.add_test_worker(WorkerInfo::default());
    fs
}

fn new_fs_with_journal(
    format: bool,
    name: &str,
) -> CommonResult<(MasterFilesystem, JournalSystem)> {
    Master::init_test_metrics();

    let conf = ClusterConf {
        format_master: format,
        testing: true,
        master: MasterConf {
            meta_dir: Utils::test_sub_dir(format!("master-fs-test/meta-{}", name)),
            ..Default::default()
        },
        journal: JournalConf {
            enable: false,
            // Reuse the same journal_dir across reopen phases so the test hits
            // the real RocksDB reopen path instead of a fresh directory.
            journal_dir: Utils::test_sub_dir(format!("master-fs-test/journal-{}", name)),
            ..Default::default()
        },
        ..Default::default()
    };

    let journal_system = JournalSystem::from_conf(&conf)?;
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    fs.add_test_worker(WorkerInfo::default());
    Ok((fs, journal_system))
}

fn reopen_fs_with_journal(
    format: bool,
    name: &str,
) -> CommonResult<(MasterFilesystem, JournalSystem)> {
    for _ in 0..50 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            new_fs_with_journal(format, name)
        })) {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(e)) if e.to_string().contains("lock hold by current process") => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(Err(e)) => return Err(e),
            Err(panic) if panic_message(&panic).contains("lock hold by current process") => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    new_fs_with_journal(format, name)
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = payload.downcast_ref::<String>() {
        return msg.clone();
    }
    if let Some(msg) = payload.downcast_ref::<&str>() {
        return (*msg).to_string();
    }
    String::new()
}

fn file_counts(fs: &MasterFilesystem) -> (i64, i64) {
    fs.get_file_counts()
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
fn test_delete_duplicate_journal_on_leader_crash_retry() -> CommonResult<()> {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    // node1 (initial leader)
    conf.change_test_meta_dir("raft-delete-idem-1");
    let js1 = JournalSystem::from_conf(&conf)?;
    let fs1 = MasterFilesystem::with_js(&conf, &js1);
    fs1.add_test_worker(worker.clone());

    // node1: mkdir /data => capture MkdirEntry
    fs1.mkdir("/data", false)?;
    let mkdir_entries = js1.fs().fs_dir.read().take_entries();
    assert_eq!(mkdir_entries.len(), 1, "Expected 1 mkdir entry from node1");
    let mkdir_entry_from_node1 = mkdir_entries.into_iter().next().unwrap();

    // node1: delete /data => capture DeleteEntry
    fs1.delete("/data", false)?;
    let delete_entries1 = js1.fs().fs_dir.read().take_entries();
    assert_eq!(
        delete_entries1.len(),
        1,
        "Expected 1 delete entry from node1"
    );
    let delete_entry_from_node1 = delete_entries1.into_iter().next().unwrap();

    //  node2 (new leader after node1 crash)
    // node2 received node1's mkdir but NOT node1's delete
    conf.change_test_meta_dir("raft-delete-idem-2");
    let js2 = JournalSystem::from_conf(&conf)?;
    let fs2 = MasterFilesystem::with_js(&conf, &js2);
    fs2.add_test_worker(worker.clone());
    let mnt_mgr2 = js2.mount_manager();

    let loader2 = JournalLoader::new(
        fs2.fs_dir(),
        mnt_mgr2.clone(),
        &conf.journal,
        js2.job_manager(),
    );
    loader2.apply_entry(mkdir_entry_from_node1.clone())?;
    println!("node2 state after receiving node1's mkdir:");
    fs2.print_tree();

    // node2 (new leader) receives client retry: delete("/data")
    // node2 has no knowledge of node1's req_id => executes delete => produces new DeleteEntry
    fs2.delete("/data", false)?;
    let delete_entries2 = js2.fs().fs_dir.read().take_entries();
    assert_eq!(
        delete_entries2.len(),
        1,
        "Expected 1 delete entry from node2"
    );
    let delete_entry_from_node2 = delete_entries2.into_iter().next().unwrap();

    println!("delete_entry_from_node1: {:?}", delete_entry_from_node1);
    println!("delete_entry_from_node2: {:?}", delete_entry_from_node2);

    //  node3 as a follower and applies both delete operations from node1 and node2
    conf.change_test_meta_dir("raft-delete-idem-3");
    let js3 = JournalSystem::from_conf(&conf)?;
    let fs3 = MasterFilesystem::with_js(&conf, &js3);
    fs3.add_test_worker(worker.clone());
    let mnt_mgr3 = js3.mount_manager();
    let loader3 = JournalLoader::new(
        fs3.fs_dir(),
        mnt_mgr3.clone(),
        &conf.journal,
        js3.job_manager(),
    );

    // Step 1: node3 receives node1's mkdir (normal Raft replication from node1)
    loader3.apply_entry(mkdir_entry_from_node1.clone())?;
    println!("node3 after node1 mkdir replication — /data should exist");
    fs3.print_tree();

    // Step 2: node3 receives node1's delete (normal Raft replication from node1)
    loader3.apply_entry(delete_entry_from_node1.clone())?;
    println!("node3 after node1 delete replication — /data should be gone");
    fs3.print_tree();

    // Step 3: node3 receives node2's delete (client retry to new leader)
    // BUG: /data is already gone on node3 => should skip.

    let result = loader3.apply_entry(delete_entry_from_node2.clone());

    // Without idempotency handling, it raises error.
    println!("node2's delete apply result on node3: {:?}", result);

    assert!(
        result.is_ok(),
        "BUG: duplicate delete entry not handled idempotently — got: {:?}",
        result
    );

    Ok(())
}

#[test]
fn test_master_filesystem_core_operations() -> CommonResult<()> {
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
fn test_rpc_retry_cache_for_idempotent_operations() -> CommonResult<()> {
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
fn test_filesystem_metadata_persistence_and_restore() -> CommonResult<()> {
    // First phase: create metadata and persist to RocksDB
    let hash1 = {
        let fs = new_fs(true, "restore");
        fs.mkdir("/a", false)?;
        fs.mkdir("/x1/x2/x3", true)?;
        let hash = fs.sum_hash();
        drop(fs);
        hash
    }; // Scope ensures all resources are dropped before reopening DB

    // Second phase: restore from persisted metadata
    let fs = new_fs(false, "restore");
    fs.restore_from_rocksdb()?;

    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    let hash2 = fs.sum_hash();
    assert_eq!(hash1, hash2);

    Ok(())
}

#[test]
fn test_filesystem_metadata_restore_with_full_journal_system_reopen() -> CommonResult<()> {
    let test_name = format!("restore-full-{}", Utils::rand_str(6));
    let hash1 = {
        let (fs, js) = new_fs_with_journal(true, &test_name)?;
        fs.mkdir("/a", false)?;
        fs.mkdir("/x1/x2/x3", true)?;
        let hash = fs.sum_hash();
        drop(fs);
        js.shutdown();
        hash
    };

    let (fs, js) = reopen_fs_with_journal(false, &test_name)?;
    fs.restore_from_rocksdb()?;

    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    assert_eq!(hash1, fs.sum_hash());

    drop(fs);
    js.shutdown();

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
    let list_1 = fs
        .list_status("/*/*.log")
        .expect("list_1 failed to get status");
    assert_eq!(list_1.len(), 2, "Should find exactly 2 log files");

    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_1 = list_1.clone();
    sorted_list_1.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify first file: /a/1.log
    assert_eq!(sorted_list_1[0].path, "/a/b1.log", "file path mismatch");
    assert_eq!(sorted_list_1[0].name, "b1.log", "file name mismatch");

    // Verify second file: /a/2.log
    assert_eq!(sorted_list_1[1].path, "/a/b2.log", "file path mismatch");
    assert_eq!(sorted_list_1[1].name, "b2.log", "file name mismatch");

    // test 2
    let list_2 = fs
        .list_status("/a/[ac]2.*")
        .expect("list_2 failed to get status");
    assert_eq!(list_2.len(), 1, "Should find exactly 1 log files");

    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_2 = list_2.clone();
    sorted_list_2.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify second file: /a/c2.txt
    assert_eq!(sorted_list_2[0].path, "/a/c2.txt", "file path mismatch");
    assert_eq!(sorted_list_2[0].name, "c2.txt", "file name mismatch");

    // test 3
    let list_3 = fs.list_status("/a/*").expect("list_2 failed to get status");
    assert_eq!(list_3.len(), 6, "Should find exactly 6 log files");
    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_3 = list_3.clone();
    sorted_list_3.sort_by(|a, b| a.name.cmp(&b.name));

    // Verify second file: /a/b/xx.log

    assert_eq!(sorted_list_3[0].path, "/a/b/1.log", "file path mismatch");

    // Verify second file: /a/b2.txt
    assert_eq!(sorted_list_3[2].path, "/a/b2.log", "file path mismatch");
    assert_eq!(sorted_list_3[2].name, "b2.log", "file name mismatch");

    // Verify second file: /a/b/xx.log
    assert_eq!(sorted_list_3[5].path, "/a/b/xx.log", "file path mismatch");
    assert_eq!(sorted_list_3[5].name, "xx.log", "file name mismatch");

    // test 4
    assert!(fs.list_status("/a/[a").is_err());

    let list_5 = fs.list_status("/*").expect("list_5 failed to get status");
    assert_eq!(list_5.len(), 11, "should find exactly 11 log files");

    Ok(())
}

fn list_status_without_glob(fs: &MasterFilesystem) -> CommonResult<()> {
    // Verify directories exist after creation
    assert!(fs.exists("/a1")?);
    assert!(fs.exists("/a2")?);
    assert!(fs.exists("/a3")?);
    assert!(fs.exists("/a3/b")?);
    assert!(fs.exists("/a3/b/c")?);

    let list = fs.list_status("/")?;
    assert_eq!(list.len(), 6);

    Ok(())
}

fn list_status(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.create("/a/b1.log", true)?;
    fs.create("/a/b2.log", true)?;
    fs.create("/a/c1.txt", true)?;
    fs.create("/a/c2.txt", true)?;

    fs.mkdir("/a/d1", true)?;
    fs.mkdir("/a/d2", true)?;

    assert!(fs.mkdir("/a/b", false).is_err());

    fs.mkdir("/a1", true)?;
    fs.mkdir("/a2", true)?;

    assert!(fs.mkdir("/a3/b/c", true).is_ok());

    fs.print_tree();

    let _ = list_status_with_glob(fs);
    let _ = list_status_without_glob(fs);
    Ok(())
}

#[test]
fn test_hardlink_creation_and_nlink_counting() -> CommonResult<()> {
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
    assert!(is_retry);

    // Retry request is normal
    let _ = handler.retry_check_create_file(&mut ctx)?;

    Ok(())
}

fn add_block_retry(fs: &MasterFilesystem) -> CommonResult<()> {
    let path = "/add_block_retry.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false).unwrap();

    let b1 = fs
        .add_block(path, addr.clone(), vec![], vec![], 0, None)
        .unwrap();
    let b2 = fs
        .add_block(path, addr.clone(), vec![], vec![], 0, None)
        .unwrap();

    assert_eq!(b1.block.id, b2.block.id);

    let locs = fs.get_block_locations(path).unwrap();
    println!("locs = {:?}", locs);
    assert_eq!(locs.block_locs.len(), 1);

    // Get the first block info to use as last_block parameter
    let first_block = b1.block.clone();

    let commit = CommitBlock {
        block_id: first_block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation {
            worker_id: b1.locs[0].worker_id,
            storage_type: Default::default(),
        }],
    };

    // Add second block with first block as last_block parameter
    let b1 = fs
        .add_block(
            path,
            addr.clone(),
            vec![commit.clone()],
            vec![],
            status.block_size,
            Some(first_block.clone()), // Specify we want block after first_block
        )
        .unwrap();
    let b2 = fs
        .add_block(
            path,
            addr.clone(),
            vec![commit],
            vec![],
            status.block_size,
            Some(first_block), // Retry with same parameters (should return b1)
        )
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

    let b1 = fs.add_block(path, addr.clone(), vec![], vec![], 0, None)?;

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

// Helper: creates a leader + follower pair, returns (leader_fs, leader_js, loader, follower_js)
fn setup_pair(
    name: &str,
) -> (
    MasterFilesystem,
    JournalSystem,
    JournalLoader,
    JournalSystem,
    MasterFilesystem,
) {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    let worker = WorkerInfo::default();

    conf.change_test_meta_dir(format!("idem-{}-leader", name));
    let js1 = JournalSystem::from_conf(&conf).unwrap();
    let fs1 = MasterFilesystem::with_js(&conf, &js1);
    fs1.add_test_worker(worker.clone());

    conf.change_test_meta_dir(format!("idem-{}-follower", name));
    let js2 = JournalSystem::from_conf(&conf).unwrap();
    let fs2 = MasterFilesystem::with_js(&conf, &js2);
    fs2.add_test_worker(worker);
    let loader = js2.journal_loader();

    (fs1, js1, loader, js2, fs2)
}

fn apply_entries(
    loader: &JournalLoader,
    entries: &[JournalEntry],
    start_index: u64,
) -> CommonResult<()> {
    let rt = AsyncRuntime::single();
    rt.block_on(async {
        for (offset, entry) in entries.iter().cloned().enumerate() {
            let index = start_index + offset as u64;
            let mut batch = JournalBatch::new(index);
            batch.push(entry);
            let entry = Entry {
                term: 1,
                index,
                data: SerdeUtils::serialize(&batch)?,
                ..Default::default()
            };
            loader.apply(true, ApplyMsg::new_entry(entry)).await?;
        }
        Ok(())
    })
}

/// Simulate follower replay through the real AppStorage apply path.
fn replay_all_then_duplicate_last(js: &JournalSystem, loader: &JournalLoader) -> CommonResult<()> {
    let entries = js.fs().fs_dir.read().take_entries();
    assert!(!entries.is_empty());

    apply_entries(loader, &entries, 1)?;

    let dup_start = entries.len() - 1;
    apply_entries(loader, &entries[dup_start..], entries.len() as u64)
}

#[test]
fn test_idempotent_mkdir() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("mkdir");
    fs.mkdir("/data", false)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_create_file() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("create-file");
    fs.create("/file.log", true)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_delete() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("delete");
    fs.mkdir("/data", false)?;
    let after_mkdir = file_counts(&fs);
    eprintln!("delete counts after mkdir = {:?}", after_mkdir);
    fs.delete("/data", true)?;
    let after_delete = file_counts(&fs);
    eprintln!("delete counts after delete = {:?}", after_delete);
    replay_all_then_duplicate_last(&js, &loader)?;
    let leader_counts = file_counts(&fs);
    let follower_counts = file_counts(&fs2);
    eprintln!("delete leader counts after replay = {:?}", leader_counts);
    eprintln!(
        "delete follower counts after replay = {:?}",
        follower_counts
    );
    assert!(
        leader_counts.0 >= 0 && leader_counts.1 >= 0,
        "leader file counts must stay non-negative: {:?}",
        leader_counts
    );
    assert!(
        follower_counts.0 >= 0 && follower_counts.1 >= 0,
        "follower file counts must stay non-negative: {:?}",
        follower_counts
    );
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_rename() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("rename");
    fs.mkdir("/src", false)?;
    fs.rename("/src", "/dst", RenameFlags::empty())?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_free() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("free");
    fs.create("/file.log", true)?;
    // Set ufs_mtime > 0 so the free function passes the ufs_exists() check
    let set_opts = SetAttrOptsBuilder::new().ufs_mtime(1).build();
    fs.set_attr("/file.log", set_opts)?;
    fs.free("/file.log", false)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_set_attr() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("set-attr");
    fs.mkdir("/data", false)?;
    let opts = SetAttrOptsBuilder::new().owner("test_owner").build();
    fs.set_attr("/data", opts)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_unmount() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("unmount");
    let mnt_mgr = js.mount_manager();
    let mnt_opt = MountOptions::builder().build();
    mnt_mgr.mount(None, "/mnt/test", "oss://bucket/", &mnt_opt)?;
    mnt_mgr.umount("/mnt/test")?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_inode_file_num_stays_non_negative_for_symlink_create_delete() -> CommonResult<()> {
    let _lock = inode_count_metrics_test_lock();
    let fs = new_fs(true, "inode-file-num-symlink");

    let (dir_count, file_count) = file_counts(&fs);
    assert_eq!(dir_count, 0);
    assert_eq!(file_count, 0);

    fs.mkdir("/dir", false)?;
    let (dir_count, file_count) = file_counts(&fs);
    assert_eq!(dir_count, 1);
    assert_eq!(file_count, 0);

    fs.symlink("/target", "/dir/link", false, 0o777)?;
    let (dir_count_after_create, file_count_after_create) = file_counts(&fs);

    fs.delete("/dir/link", false)?;
    let (dir_count_after_delete, file_count_after_delete) = file_counts(&fs);

    assert_eq!(dir_count_after_create, dir_count_after_delete);
    assert_eq!(
        file_count_after_create,
        file_count_after_delete + 1,
        "symlink create/delete should change file count symmetrically"
    );
    assert!(
        file_count_after_delete >= 0,
        "inode_file_num must never be negative, got {}",
        file_count_after_delete
    );
    Ok(())
}

#[test]
fn test_inode_file_num_stable_on_forced_symlink_rewrite() -> CommonResult<()> {
    let _lock = inode_count_metrics_test_lock();
    let fs = new_fs(true, "inode-file-num-symlink-force");

    fs.mkdir("/dir", false)?;
    fs.symlink("/target-a", "/dir/link", false, 0o777)?;
    let file_count_after_first = file_counts(&fs).1;

    fs.symlink("/target-b", "/dir/link", true, 0o777)?;
    fs.symlink("/target-c", "/dir/link", true, 0o777)?;
    let file_count_after_rewrites = file_counts(&fs).1;

    assert_eq!(
        file_count_after_first, file_count_after_rewrites,
        "force symlink replace must not inflate inode_file_num (was {}, after rewrites {})",
        file_count_after_first, file_count_after_rewrites
    );
    Ok(())
}

#[test]
fn test_inode_file_num_stays_non_negative_when_renaming_over_symlink() -> CommonResult<()> {
    let _lock = inode_count_metrics_test_lock();
    let fs = new_fs(true, "inode-file-num-rename-over-link");

    fs.mkdir("/dir", false)?;
    fs.create("/dir/file.log", true)?;
    fs.symlink("/target", "/dir/link", false, 0o777)?;

    fs.rename("/dir/file.log", "/dir/link", RenameFlags::empty())?;

    let (_dir_count, file_count) = file_counts(&fs);
    assert!(
        file_count >= 0,
        "inode_file_num must never be negative after rename-overwrite, got {}",
        file_count
    );
    Ok(())
}

#[test]
fn test_idempotent_symlink() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("symlink");
    fs.mkdir("/dir", false)?;
    let after_mkdir = file_counts(&fs);
    eprintln!("symlink counts after mkdir = {:?}", after_mkdir);
    fs.symlink("/target", "/dir/link", false, 0o777)?;
    let after_create = file_counts(&fs);
    eprintln!("symlink counts after create = {:?}", after_create);
    replay_all_then_duplicate_last(&js, &loader)?;
    let leader_counts = file_counts(&fs);
    let follower_counts = file_counts(&fs2);
    eprintln!("symlink leader counts after replay = {:?}", leader_counts);
    eprintln!(
        "symlink follower counts after replay = {:?}",
        follower_counts
    );
    assert!(
        leader_counts.0 >= 0 && leader_counts.1 >= 0,
        "leader file counts must stay non-negative: {:?}",
        leader_counts
    );
    assert!(
        follower_counts.0 >= 0 && follower_counts.1 >= 0,
        "follower file counts must stay non-negative: {:?}",
        follower_counts
    );
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_link() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("link");
    fs.create("/original.txt", true)?;
    let after_create = file_counts(&fs);
    eprintln!("link counts after create = {:?}", after_create);
    fs.link("/original.txt", "/hardlink.txt")?;
    let after_hardlink = file_counts(&fs);
    eprintln!("link counts after hardlink = {:?}", after_hardlink);
    replay_all_then_duplicate_last(&js, &loader)?;

    let leader_counts = file_counts(&fs);
    let follower_counts = file_counts(&fs2);
    let file_count_drift = follower_counts.0 - leader_counts.0;
    let dir_count_drift = follower_counts.1 - leader_counts.1;
    eprintln!("link leader counts after replay = {:?}", leader_counts);
    eprintln!("link follower counts after replay = {:?}", follower_counts);
    eprintln!(
        "link count drift after replay: files={}, dirs={}",
        file_count_drift, dir_count_drift
    );
    assert!(
        leader_counts.0 >= 0 && leader_counts.1 >= 0,
        "leader file counts must stay non-negative: {:?}",
        leader_counts
    );
    assert!(
        follower_counts.0 >= 0 && follower_counts.1 >= 0,
        "follower file counts must stay non-negative: {:?}",
        follower_counts
    );
    assert_eq!(
        dir_count_drift, 0,
        "hardlink replay should not change directory counts: leader={:?}, follower={:?}",
        leader_counts, follower_counts
    );
    assert_eq!(
        file_count_drift, 0,
        "hardlink replay should preserve file counts: leader={:?}, follower={:?}",
        leader_counts, follower_counts
    );

    let original = fs.file_status("/original.txt")?;
    let hardlink = fs.file_status("/hardlink.txt")?;
    let replay_original = fs2.file_status("/original.txt")?;
    let replay_hardlink = fs2.file_status("/hardlink.txt")?;

    assert_eq!(original.id, hardlink.id);
    assert_eq!(replay_original.id, replay_hardlink.id);
    assert_eq!(original.id, replay_original.id);

    assert_eq!(original.nlink, 2);
    assert_eq!(hardlink.nlink, 2);
    assert_eq!(replay_original.nlink, 2);
    assert_eq!(replay_hardlink.nlink, 2);
    Ok(())
}

#[test]
fn test_idempotent_mount() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("mount");
    let mnt_mgr = js.mount_manager();
    let mount_uri = CurvineURI::new("/mnt/test")?;
    let ufs_uri = CurvineURI::new("oss://bucket1/")?;
    let mnt_opt = MountOptions::builder().build();
    mnt_mgr.mount(
        None,
        mount_uri.path(),
        ufs_uri.encode_uri().as_ref(),
        &mnt_opt,
    )?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}

#[test]
fn test_idempotent_set_locks() -> CommonResult<()> {
    let (fs, js, loader, _js2, fs2) = setup_pair("set-locks");
    fs.create("/lockfile.log", true)?;
    let lock = curvine_common::state::FileLock {
        client_id: "client1".to_string(),
        owner_id: 1,
        lock_type: curvine_common::state::LockType::WriteLock,
        lock_flags: curvine_common::state::LockFlags::Plock,
        start: 0,
        end: 100,
        ..Default::default()
    };
    fs.set_lock("/lockfile.log", lock)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash(), fs2.sum_hash());
    Ok(())
}
