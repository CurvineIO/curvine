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
use curvine_common::error::FsError;
use curvine_common::fs::CurvineURI;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    CreateFileRequest, DeleteRequest, GetMasterInfoRequest, MkdirOptsProto, MkdirRequest,
    RenameRequest,
};
use curvine_common::raft::storage::{AppStorage, ApplyMsg};
use curvine_common::state::MountOptions;
use curvine_common::state::{
    BlockLocation, BlockReportInfo, BlockReportList, BlockReportStatus, ClientAddress, CommitBlock,
    CreateFileOpts, CreateFileOptsBuilder, FileAllocOpts, MkdirOptsBuilder, StorageType, TtlAction,
    WorkerAddress, WorkerInfo,
};
use curvine_common::state::{OpenFlags, RenameFlags, SetAttrOptsBuilder};
use curvine_common::utils::SerdeUtils;
use curvine_server::master::fs::{FsRetryCache, MasterFilesystem, OperationStatus};
use curvine_server::master::journal::{JournalBatch, JournalEntry, JournalLoader, JournalSystem};
use curvine_server::master::meta::inode::ttl::InodeTtlExecutor;
use curvine_server::master::meta::InodeId;
use curvine_server::master::replication::master_replication_manager::MasterReplicationManager;
use curvine_server::master::{JobHandler, JobManager, Master, MasterHandler, RpcContext};
use orpc::common::LocalTime;
use orpc::common::Utils;
use orpc::handler::MessageHandler;
use orpc::message::Builder;
#[cfg(feature = "fault-injection")]
use orpc::message::ResponseStatus;
use orpc::runtime::{AsyncRuntime, GroupExecutor, RpcRuntime};
use orpc::CommonResult;
use raft::eraftpb::Entry;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

#[cfg(feature = "fault-injection")]
use curvine_fault::{FaultRuleBuilder, FaultRuntime};

// Master metrics gauges are process-wide; master_fs_test cases must not run in parallel or
// inode_file_num / inode_dir_num race with other tests' format/init.
static MASTER_FS_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

fn master_fs_test_serial() -> std::sync::MutexGuard<'static, ()> {
    MASTER_FS_TEST_SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    new_handler_for_test("retry")
}

fn new_handler_for_test(test_name: &str) -> MasterHandler {
    Master::init_test_metrics();

    let test_id = Utils::rand_str(8);
    let mut conf = ClusterConf::format();
    conf.journal.enable = false;

    conf.master.meta_dir =
        Utils::test_sub_dir(format!("master-fs-test/meta-{test_name}-{test_id}"));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("master-fs-test/journal-{test_name}-{test_id}"));

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let fs = MasterFilesystem::with_js(&conf, &journal_system);
    fs.add_test_worker(WorkerInfo::default());
    let retry_cache = FsRetryCache::with_conf(&conf.master)
        .expect("test master retry cache configuration should be valid");

    let mount_manager = journal_system.mount_manager();
    let rt = Arc::new(AsyncRuntime::single());
    let replication_manager =
        MasterReplicationManager::new(&fs, &conf, &rt, &journal_system.worker_manager())
            .expect("test master replication manager should initialize");
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
        Arc::new(GroupExecutor::new("test-master-heartbeat-rpc", 1, 8)),
        Arc::new(GroupExecutor::new("test-master-block-report-rpc", 1, 8)),
        Arc::new(GroupExecutor::new("test-master-control-rpc", 1, 8)),
        Arc::new(GroupExecutor::new("test-master-list-rpc", 1, 8)),
        Arc::new(GroupExecutor::new(
            "test-master-get-block-locations-rpc",
            1,
            8,
        )),
        replication_manager,
        Master::get_metrics().expect("test master metrics should initialize"),
    )
}

#[cfg(feature = "fault-injection")]
#[test]
fn test_master_sync_and_async_rpc_points_follow_dispatch_paths() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let faults = FaultRuntime::process();
    faults.clear()?;
    for (id, point, code) in [
        (
            "sync-dispatch",
            "master.rpc.before_sync_dispatch",
            RpcCode::Mkdir,
        ),
        (
            "async-dispatch",
            "master.rpc.before_async_dispatch",
            RpcCode::SubmitJob,
        ),
    ] {
        let rule = FaultRuleBuilder::named(point)
            .matches("rpc_code", code as i32)?
            .times(1)?
            .return_error(id)?;
        faults.configure(id, rule)?;
    }

    let mut handler = new_handler_for_test("fault-dispatch");
    let sync_request = Builder::new_rpc(RpcCode::Mkdir).build();
    let sync_response = handler.handle(&sync_request)?;

    let async_request = Builder::new_rpc(RpcCode::SubmitJob).build();
    let rt = AsyncRuntime::single();
    let async_response = rt.block_on(handler.async_handle(async_request))?;
    for response in [sync_response, async_response] {
        assert_eq!(response.response_status(), ResponseStatus::Error);
        assert!(matches!(
            response.check_error_ext::<FsError>(),
            Err(FsError::Common(_))
        ));
    }

    let status = faults.status();
    assert!(status.rules.iter().all(|rule| rule.executions == 1));
    faults.clear()?;
    Ok(())
}

#[test]
fn worker_reports_and_metadata_reads_do_not_use_master_sync_pool() {
    let _serial = master_fs_test_serial();
    let handler = new_handler();

    for code in [
        RpcCode::SubmitJob,
        RpcCode::GetJobStatus,
        RpcCode::CancelJob,
        RpcCode::ReportTask,
        RpcCode::GetBlockLocations,
        RpcCode::GetMasterInfo,
        RpcCode::ListStatus,
        RpcCode::ListOptions,
        RpcCode::WorkerHeartbeat,
        RpcCode::WorkerBlockReport,
    ] {
        let msg = Builder::new_rpc(code).build();
        assert!(!handler.is_sync(&msg), "{code:?} must use an async lane");
    }

    let mkdir = Builder::new_rpc(RpcCode::Mkdir).build();
    assert!(handler.is_sync(&mkdir));
}

#[test]
fn async_rpc_to_standby_returns_rpc_error_response() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let mut handler = new_handler();
    let msg = Builder::new_rpc(RpcCode::GetMasterInfo)
        .proto_header(GetMasterInfoRequest::default())
        .build();

    let rt = AsyncRuntime::single();
    let response = rt.block_on(handler.async_handle(msg))?;

    assert!(!response.is_success());
    let err = response.check_error_ext::<FsError>().unwrap_err();
    assert!(matches!(err, FsError::NotLeaderMaster(_)));
    Ok(())
}

#[test]
fn test_master_filesystem_core_operations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
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
fn block_report_for_non_file_inode_schedules_worker_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "block-report-non-file");
    fs.mkdir("/dir-block", true)?;
    let dir_status = fs.file_status("/dir-block")?;
    let block_id = InodeId::create_block_id(dir_status.id, 0)?;

    let result = fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 0,
            full_report: false,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                block_id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                1,
            )],
        },
        None,
    )?;

    assert_eq!(result.delete_blocks, vec![block_id]);
    Ok(())
}

#[test]
fn full_block_report_reconcile_removes_stale_location_async() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-reconcile-async");
    let path = "/full-block-reconcile.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false)?;

    let first = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;
    let first_commit = CommitBlock {
        block_id: first.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation::with_id(100)],
    };
    let second = fs.add_block(
        path,
        None,
        addr,
        vec![first_commit],
        vec![],
        status.block_size,
        Some(first.block.clone()),
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            total_len: 0,
            blocks: vec![
                BlockReportInfo::new(
                    first.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    first.block.len,
                ),
                BlockReportInfo::new(
                    second.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    second.block.len,
                ),
            ],
        },
        None,
    )?;

    let before = fs.get_block_locations(path)?;
    assert_eq!(before.block_locs.len(), 2);
    assert!(!before.block_locs[1].locs.is_empty());

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            total_len: 1,
            blocks: vec![BlockReportInfo::new(
                first.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                first.block.len,
            )],
        },
        None,
    )?;

    for _ in 0..50 {
        let blocks = fs.get_block_locations(path)?;
        let stale = blocks
            .block_locs
            .iter()
            .find(|block| block.block.id == second.block.id)
            .expect("second block metadata should remain");
        if stale.locs.is_empty() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let blocks = fs.get_block_locations(path)?;
    panic!(
        "stale worker location for block {} was not reconciled: {:?}",
        second.block.id, blocks
    );
}

#[test]
fn incremental_report_invalidates_incomplete_full_report_session() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "full-block-report-invalidate-session");
    let path = "/full-block-report-invalidate-session.log";
    let addr = ClientAddress::default();
    let status = fs.create(path, false)?;

    let first = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;
    let first_commit = CommitBlock {
        block_id: first.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation::with_id(100)],
    };
    let second = fs.add_block(
        path,
        None,
        addr.clone(),
        vec![first_commit],
        vec![],
        status.block_size,
        Some(first.block.clone()),
    )?;
    let second_commit = CommitBlock {
        block_id: second.block.id,
        block_len: status.block_size,
        locations: vec![BlockLocation::with_id(100)],
    };
    let third = fs.add_block(
        path,
        None,
        addr,
        vec![second_commit],
        vec![],
        status.block_size * 2,
        Some(second.block.clone()),
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            total_len: 0,
            blocks: vec![
                BlockReportInfo::new(
                    first.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    first.block.len,
                ),
                BlockReportInfo::new(
                    second.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    second.block.len,
                ),
                BlockReportInfo::new(
                    third.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    third.block.len,
                ),
            ],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            total_len: 2,
            blocks: vec![BlockReportInfo::new(
                first.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                first.block.len,
            )],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: false,
            total_len: 0,
            blocks: vec![BlockReportInfo::new(
                second.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                second.block.len,
            )],
        },
        None,
    )?;

    fs.block_report(
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id: 100,
            full_report: true,
            total_len: 2,
            blocks: vec![BlockReportInfo::new(
                third.block.id,
                BlockReportStatus::Finalized,
                StorageType::Disk,
                third.block.len,
            )],
        },
        None,
    )?;

    for _ in 0..50 {
        let blocks = fs.get_block_locations(path)?;
        let protected = blocks
            .block_locs
            .iter()
            .find(|block| block.block.id == second.block.id)
            .expect("second block metadata should remain");
        assert!(
            !protected.locs.is_empty(),
            "incremental report should protect block {} from stale full-report reconciliation",
            second.block.id
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    Ok(())
}

#[test]
fn ttl_executor_deletes_nested_expired_inode() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "ttl-executor-nested-delete");
    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_ms(1)
        .ttl_action(TtlAction::Delete)
        .build();
    let status = fs.create_with_opts(
        "/ttl/a/b/file.log",
        opts,
        OpenFlags::new_create().set_overwrite(true),
    )?;

    std::thread::sleep(Duration::from_millis(10));
    let executor = InodeTtlExecutor::with_managers(fs.clone());
    let (processed, inode) = executor.execute_by_id(status.id)?;

    assert!(
        processed,
        "expired inode should be processed by TTL executor"
    );
    assert_eq!(inode.id(), status.id);
    assert!(
        fs.file_status("/ttl/a/b/file.log").is_err(),
        "TTL delete should remove the nested file path resolved from inode id"
    );

    Ok(())
}

// Regression: TTL path resolution must not re-acquire the fs_dir read lock while
// already holding it. std::sync::RwLock is writer-preferring, so a reentrant read
// deadlocks once a writer is queued. This reproduces the 2026-07-08 freeze shape:
// a deep path resolved under continuous concurrent writers. It must complete
// within the timeout.
#[test]
fn ttl_path_resolution_no_reentrant_deadlock_under_writers() -> CommonResult<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "ttl-reentrant-deadlock");

    let mut deep_path = String::new();
    for level in 0..12 {
        deep_path.push_str(&format!("/d{}", level));
    }
    deep_path.push_str("/file.log");

    let opts = CreateFileOptsBuilder::new()
        .create_parent(true)
        .ttl_ms(1)
        .ttl_action(TtlAction::Delete)
        .build();
    let status = fs.create_with_opts(
        &deep_path,
        opts,
        OpenFlags::new_create().set_overwrite(true),
    )?;
    std::thread::sleep(Duration::from_millis(10));

    let stop = Arc::new(AtomicBool::new(false));
    let writer_fs = fs.clone();
    let writer_stop = stop.clone();
    let writer = std::thread::spawn(move || {
        let mut i = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let _ = writer_fs.mkdir(format!("/writer/{}", i), true);
            i += 1;
        }
    });

    let (tx, rx) = mpsc::channel();
    let exec_fs = fs.clone();
    let inode_id = status.id;
    let resolver = std::thread::spawn(move || {
        let executor = InodeTtlExecutor::with_managers(exec_fs);
        let _ = tx.send(executor.execute_by_id(inode_id));
    });

    let result = rx.recv_timeout(Duration::from_secs(10));
    stop.store(true, Ordering::Relaxed);
    let _ = writer.join();
    let _ = resolver.join();

    let (processed, inode) = result
        .expect("TTL path resolution deadlocked: no result within 10s under concurrent writers")?;
    assert!(processed, "expired inode should be processed");
    assert_eq!(inode.id(), status.id);
    assert!(
        fs.file_status(&deep_path).is_err(),
        "TTL delete should remove the deep path resolved from inode id"
    );

    Ok(())
}

#[test]
fn test_rename_posix_semantics() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "rename-posix");
    rename_posix_semantics(&fs)?;
    Ok(())
}

#[test]
fn test_rpc_retry_cache_for_idempotent_operations() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
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
    let _serial = master_fs_test_serial();
    // First phase: create metadata and persist to RocksDB
    let hash1 = {
        let fs = new_fs(true, "restore");
        fs.mkdir("/a", false)?;
        fs.mkdir("/x1/x2/x3", true)?;
        let hash = fs.sum_hash()?;
        drop(fs);
        hash
    }; // Scope ensures all resources are dropped before reopening DB

    // Second phase: restore from persisted metadata
    let fs = new_fs(false, "restore");
    fs.restore_from_rocksdb()?;

    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    let hash2 = fs.sum_hash()?;
    assert_eq!(hash1, hash2);

    Ok(())
}

#[test]
fn test_filesystem_metadata_restore_with_full_journal_system_reopen() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let test_name = format!("restore-full-{}", Utils::rand_str(6));
    let hash1 = {
        let (fs, js) = new_fs_with_journal(true, &test_name)?;
        fs.mkdir("/a", false)?;
        fs.mkdir("/x1/x2/x3", true)?;
        let hash = fs.sum_hash()?;
        drop(fs);
        js.shutdown();
        hash
    };

    let (fs, js) = reopen_fs_with_journal(false, &test_name)?;
    fs.restore_from_rocksdb()?;

    assert!(fs.exists("/a")?);
    assert!(fs.exists("/x1/x2/x3")?);
    assert_eq!(hash1, fs.sum_hash()?);

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

#[test]
fn mkdir_inherits_setgid_parent_group_and_mode() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "mkdir-setgid-inherit");

    let parent_opts = MkdirOptsBuilder::new()
        .owner("parent-owner".to_string())
        .group("parent-group".to_string())
        .mode(0o2775)
        .build();
    fs.mkdir_with_opts("/parent", parent_opts)?;

    let child_opts = MkdirOptsBuilder::new()
        .owner("child-owner".to_string())
        .group("child-group".to_string())
        .mode(0o775)
        .build();
    fs.mkdir_with_opts("/parent/child", child_opts)?;

    let child = fs.file_status("/parent/child")?;
    assert_eq!("parent-group", child.group);
    assert_eq!(0o2000, child.mode & 0o2000);
    assert_eq!(0o775, child.mode & 0o777);

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

    Ok(())
}

fn rename_posix_semantics(fs: &MasterFilesystem) -> CommonResult<()> {
    fs.mkdir("/a/b", true)?;

    // Test file rename to existing directory: POSIX rename must fail with EISDIR.
    fs.create("/a/1.log", true)?;

    let err = fs
        .rename("/a/1.log", "/a/b", RenameFlags::empty())
        .expect_err("rename file to directory must fail");
    assert!(matches!(err, FsError::IsADirectory(_)));
    assert!(fs.exists("/a/1.log")?);
    assert!(fs.exists("/a/b")?);
    assert!(!fs.exists("/a/b/1.log")?);

    // src file, dst file -> overwrite existing file.
    fs.create("/a/old.log", true)?;
    fs.create("/a/new.log", true)?;
    fs.rename("/a/old.log", "/a/new.log", RenameFlags::empty())?;
    assert!(!fs.exists("/a/old.log")?);
    assert!(fs.exists("/a/new.log")?);

    // src directory, dst empty directory -> overwrite empty directory.
    fs.mkdir("/a/src_dir", true)?;
    fs.create("/a/src_dir/child.txt", true)?;
    fs.mkdir("/a/empty_dir", true)?;
    fs.rename("/a/src_dir", "/a/empty_dir", RenameFlags::empty())?;
    assert!(!fs.exists("/a/src_dir")?);
    assert!(fs.exists("/a/empty_dir")?);
    assert!(fs.exists("/a/empty_dir/child.txt")?);

    // src directory, dst non-empty directory -> ENOTEMPTY.
    fs.mkdir("/a/rename_src", true)?;
    fs.create("/a/rename_src/file.txt", true)?;
    fs.mkdir("/a/rename_dst", true)?;
    fs.create("/a/rename_dst/keep.txt", true)?;

    let err = fs
        .rename("/a/rename_src", "/a/rename_dst", RenameFlags::empty())
        .expect_err("rename to non-empty directory must fail");
    assert!(matches!(err, FsError::DirNotEmpty(_)));
    assert!(fs.exists("/a/rename_src")?);
    assert!(fs.exists("/a/rename_src/file.txt")?);
    assert!(fs.exists("/a/rename_dst/keep.txt")?);
    assert!(!fs.exists("/a/rename_dst/rename_src")?);

    // src directory, dst file -> ENOTDIR.
    fs.mkdir("/a/dir_src", true)?;
    fs.create("/a/file_dst", true)?;
    let err = fs
        .rename("/a/dir_src", "/a/file_dst", RenameFlags::empty())
        .expect_err("rename directory to file must fail");
    assert!(matches!(err, FsError::NotADirectory(_)));
    assert!(fs.exists("/a/dir_src")?);
    assert!(fs.exists("/a/file_dst")?);

    // src directory -> dst under src: POSIX EINVAL.
    fs.mkdir("/a/rename_parent", true)?;
    let err = fs
        .rename(
            "/a/rename_parent",
            "/a/rename_parent/child",
            RenameFlags::empty(),
        )
        .expect_err("rename into subdirectory must fail");
    assert!(matches!(err, FsError::InvalidArgument(_)));
    assert!(fs.exists("/a/rename_parent")?);
    assert!(!fs.exists("/a/rename_parent/child")?);

    // same-path rename is a no-op for files and directories (including non-empty dirs).
    fs.create("/a/same_file.txt", true)?;
    fs.rename("/a/same_file.txt", "/a/same_file.txt", RenameFlags::empty())?;
    assert!(fs.exists("/a/same_file.txt")?);

    fs.mkdir("/a/same_empty_dir", true)?;
    fs.rename(
        "/a/same_empty_dir",
        "/a/same_empty_dir",
        RenameFlags::empty(),
    )?;
    assert!(fs.exists("/a/same_empty_dir")?);

    fs.mkdir("/a/same_full_dir", true)?;
    fs.create("/a/same_full_dir/child.txt", true)?;
    fs.rename("/a/same_full_dir", "/a/same_full_dir", RenameFlags::empty())?;
    assert!(fs.exists("/a/same_full_dir")?);
    assert!(fs.exists("/a/same_full_dir/child.txt")?);

    // RENAME_NOREPLACE: fail when dst exists, succeed when absent.
    fs.create("/a/noreplace_dst", true)?;
    fs.create("/a/noreplace_src", true)?;
    let err = fs
        .rename(
            "/a/noreplace_src",
            "/a/noreplace_dst",
            RenameFlags::NO_REPLACE,
        )
        .expect_err("no_replace must fail when dst exists");
    assert!(matches!(err, FsError::FileAlreadyExists(_)));
    assert!(fs.exists("/a/noreplace_src")?);
    assert!(fs.exists("/a/noreplace_dst")?);

    fs.rename(
        "/a/noreplace_src",
        "/a/noreplace_new",
        RenameFlags::NO_REPLACE,
    )?;
    assert!(!fs.exists("/a/noreplace_src")?);
    assert!(fs.exists("/a/noreplace_new")?);

    // src symlink, dst symlink -> overwrite existing symlink and keep dst deletable.
    fs.symlink("nobody", "/a/symbolic", false, 0o777)?;
    fs.rename("/a/symbolic", "/a/asymbolic", RenameFlags::empty())?;
    assert!(!fs.exists("/a/symbolic")?);
    assert!(fs.exists("/a/asymbolic")?);

    fs.create("/a/object", true)?;
    fs.symlink("object", "/a/symbolic", false, 0o777)?;
    fs.rename("/a/symbolic", "/a/asymbolic", RenameFlags::empty())?;
    assert!(!fs.exists("/a/symbolic")?);
    assert!(fs.exists("/a/asymbolic")?);
    fs.delete("/a/asymbolic", false)?;
    assert!(!fs.exists("/a/asymbolic")?);
    fs.delete("/a/object", false)?;

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

    // test 3: /a/* matches direct children; list_status expands matched dirs (e.g. /a/b -> xx.log).
    let list_3 = fs.list_status("/a/*").expect("list_3 failed to get status");
    assert_eq!(
        list_3.len(),
        5,
        "Should find exactly 5 entries for /a/* glob"
    );
    // Sort for consistent ordering (if order not guaranteed)
    let mut sorted_list_3 = list_3.clone();
    sorted_list_3.sort_by(|a, b| a.name.cmp(&b.name));

    assert_eq!(sorted_list_3[0].path, "/a/b1.log", "file path mismatch");
    assert_eq!(sorted_list_3[0].name, "b1.log", "file name mismatch");

    assert_eq!(sorted_list_3[1].path, "/a/b2.log", "file path mismatch");
    assert_eq!(sorted_list_3[1].name, "b2.log", "file name mismatch");

    assert_eq!(sorted_list_3[2].path, "/a/c1.txt", "file path mismatch");
    assert_eq!(sorted_list_3[2].name, "c1.txt", "file name mismatch");

    assert_eq!(sorted_list_3[3].path, "/a/c2.txt", "file path mismatch");
    assert_eq!(sorted_list_3[3].name, "c2.txt", "file name mismatch");

    assert_eq!(sorted_list_3[4].path, "/a/b/xx.log", "file path mismatch");
    assert_eq!(sorted_list_3[4].name, "xx.log", "file name mismatch");

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
    let _serial = master_fs_test_serial();
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
    let mem_hash = fs_dir.root_dir().sum_hash()?;

    let state_tree = fs_dir.create_tree()?;
    state_tree.print_tree();
    let state_hash = state_tree.sum_hash()?;

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
        .add_block(path, None, addr.clone(), vec![], vec![], 0, None)
        .unwrap();
    let b2 = fs
        .add_block(path, None, addr.clone(), vec![], vec![], 0, None)
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
            None,
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
            None,
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

    let b1 = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

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
        None,
        b1.block.len,
        vec![commit.clone()],
        &addr.client_name,
        false,
    );
    assert!(f1.is_ok());

    let f2 = fs.complete_file(
        path,
        None,
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
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("mkdir");
    fs.mkdir("/data", false)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_create_file() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("create-file");
    fs.create("/file.log", true)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
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
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_rename() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("rename");
    fs.mkdir("/src", false)?;
    fs.rename("/src", "/dst", RenameFlags::empty())?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_free() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("free");
    fs.create("/file.log", true)?;
    // Set ufs_mtime > 0 so the free function passes the ufs_exists() check
    let set_opts = SetAttrOptsBuilder::new().ufs_mtime(1).build();
    fs.set_attr("/file.log", set_opts)?;
    fs.free("/file.log", false)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_set_attr() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("set-attr");
    fs.mkdir("/data", false)?;
    let opts = SetAttrOptsBuilder::new().owner("test_owner").build();
    fs.set_attr("/data", opts)?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_unmount() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
    let (fs, js, loader, _js2, fs2) = setup_pair("unmount");
    let mnt_mgr = js.mount_manager();
    let mnt_opt = MountOptions::builder().build();
    mnt_mgr.mount(None, "/mnt/test", "oss://bucket/", &mnt_opt)?;
    mnt_mgr.umount("/mnt/test")?;
    replay_all_then_duplicate_last(&js, &loader)?;
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_inode_file_num_stays_non_negative_for_symlink_create_delete() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
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
    let _serial = master_fs_test_serial();
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
    let _serial = master_fs_test_serial();
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
    let _serial = master_fs_test_serial();
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
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_link() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
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
    let _serial = master_fs_test_serial();
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
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn test_idempotent_set_locks() -> CommonResult<()> {
    let _serial = master_fs_test_serial();
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
    assert_eq!(fs.sum_hash()?, fs2.sum_hash()?);
    Ok(())
}

#[test]
fn resize_rejects_extreme_file_size() {
    let _serial = master_fs_test_serial();
    let fs = new_fs(true, "resize-extreme");
    fs.create("/extreme.log", true).unwrap();

    let err = fs
        .resize("/extreme.log", FileAllocOpts::with_truncate(1_i64 << 60))
        .unwrap_err();
    assert!(matches!(err, FsError::InvalidFileSize(_)));

    let status = fs.file_status("/extreme.log").unwrap();
    assert_eq!(status.len, 0);
}

#[test]
fn located_block_has_spdk_reflects_worker_reported_storage_type() -> CommonResult<()> {
    let _serial = master_fs_test_serial();

    // Scenario A: worker reports SpdkDisk -> has_spdk should be true
    {
        let fs = new_fs(true, "has-spdk-spdk");
        let path = "/has-spdk-spdk.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: block.locs[0].worker_id,
                full_report: true,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::SpdkDisk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert!(
            fb.block_locs[0].has_spdk,
            "has_spdk should be true when worker reports SpdkDisk"
        );
    }

    // Scenario B: worker reports Mem -> has_spdk should be false
    {
        let fs = new_fs(true, "has-spdk-mem");
        let path = "/has-spdk-mem.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: block.locs[0].worker_id,
                full_report: true,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Mem,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert!(
            !fb.block_locs[0].has_spdk,
            "has_spdk should be false when worker reports Mem"
        );
    }

    // Scenario C: worker reports Disk -> has_spdk should be false
    {
        let fs = new_fs(true, "has-spdk-disk");
        let path = "/has-spdk-disk.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: block.locs[0].worker_id,
                full_report: true,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert!(
            !fb.block_locs[0].has_spdk,
            "has_spdk should be false when worker reports Disk"
        );
    }

    // Scenario D: mixed replicas across 2 workers — one SpdkDisk, one Disk -> has_spdk should be true
    {
        let fs = new_fs(true, "has-spdk-mixed");
        let path = "/has-spdk-mixed.log";
        let addr = ClientAddress::default();
        fs.create(path, false)?;

        // Add second worker (worker_id=200) so we can have 2 replicas on different workers
        let worker2_addr = WorkerAddress {
            worker_id: 200,
            ip_addr: "127.0.0.2".to_string(),
            rpc_port: 667,
            ..Default::default()
        };
        let worker2 = WorkerInfo::new(worker2_addr, 0);
        fs.add_test_worker(worker2);

        let block = fs.add_block(path, None, addr.clone(), vec![], vec![], 0, None)?;

        // Worker 100 (default) reports block as SpdkDisk
        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 100,
                full_report: true,
                total_len: 1,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::SpdkDisk,
                    block.block.len,
                )],
            },
            None,
        )?;

        // Worker 200 reports same block as Disk
        fs.block_report(
            BlockReportList {
                cluster_id: "curvine".into(),
                worker_id: 200,
                full_report: false,
                total_len: 0,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let fb = fs.get_block_locations(path)?;
        assert_eq!(fb.block_locs.len(), 1);
        assert_eq!(fb.block_locs[0].locs.len(), 2, "should have 2 replicas");
        assert!(
            fb.block_locs[0].has_spdk,
            "has_spdk should be true when any replica reports SpdkDisk"
        );
    }

    Ok(())
}
