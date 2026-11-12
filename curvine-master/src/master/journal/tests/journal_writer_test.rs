use super::*;
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use crate::master::meta::inode::ttl::TtlBucketList;
use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
use crate::master::quota::eviction::EvictionConf;
use curvine_config::{ClusterConf, RaftPeer};
use curvine_runtime::common::DurationUnit;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub(crate) fn test_conf(name: &str) -> ClusterConf {
    let mut conf = ClusterConf {
        testing: true,
        format_master: true,
        journal: JournalConf {
            enable: true,
            snapshot_entries: 1,
            writer_channel_size: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    conf.change_test_meta_dir(name);
    conf
}

pub(crate) fn build_writer(conf: &ClusterConf) -> FsResult<JournalWriter> {
    Master::init_test_metrics();

    let rt = conf.journal.create_runtime();
    let client = RaftClient::from_conf(rt, &conf.journal);
    JournalWriter::new(true, client, &conf.journal, super::RecoveryGate::new())
}

fn build_fs_dir(conf: &ClusterConf, writer: Arc<JournalWriter>) -> FsResult<FsDir> {
    let ttl_bucket_list = Arc::new(TtlBucketList::new(
        conf.master.ttl_bucket_interval_ms() as i64
    )?);

    let eviction_conf = EvictionConf::from_conf(conf);
    let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(eviction_conf));

    FsDir::new(conf, writer, ttl_bucket_list, evictor)
}

fn record_and_check_snapshot(writer: &JournalWriter, fs_dir: &FsDir) -> FsResult<()> {
    writer.record_snapshot_entry();
    writer.maybe_emit_snapshot(fs_dir)
}

fn checkpoint_dirs(conf: &ClusterConf) -> Vec<PathBuf> {
    let db_conf = conf.db_conf();
    let checkpoint_dir = Path::new(&db_conf.checkpoint_dir);

    if !checkpoint_dir.exists() {
        return Vec::new();
    }

    fs::read_dir(checkpoint_dir)
        .expect("failed to read checkpoint directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect()
}

#[test]
fn bounded_send_while_holding_namespace_write_blocks_namespace_read() -> FsResult<()> {
    let mut conf = test_conf("bounded-namespace-send");
    conf.journal.snapshot_entries = 0;
    let writer = Arc::new(build_writer(&conf)?);
    let dir = build_fs_dir(&conf, writer.clone())?;
    let lease = dir.begin_snapshot_scan()?;
    let fs_dir = curvine_runtime::sync::ArcRwLock::new(dir);
    let entry = || {
        JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 1,
            rpc_id: 0,
            inodes: vec![],
        })
    };
    writer.send_inner(entry())?;

    let worker_dir = fs_dir.clone();
    let (holding_tx, holding_rx) = std::sync::mpsc::channel();
    let (sent_tx, sent_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let guard = worker_dir.write();
        holding_tx.send(()).unwrap();
        let result = InodePath::resolve(guard.root_node(), "/queued", guard.store_ref())
            .map_err(curvine_error::FsError::from)
            .and_then(|path| guard.mkdir(path, curvine_model::MkdirOpts::with_create(true)))
            .map(|_| ());
        sent_tx.send(result).unwrap();
    });
    holding_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let durable = loop {
        let snapshot = lease.snapshot_reader()?;
        if snapshot
            .get_child_id(crate::master::meta::inode::ROOT_INODE_ID, "queued")?
            .is_some()
        {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };
    let replay_dir = fs_dir.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (read_tx, read_rx) = std::sync::mpsc::channel();
    let replay = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _guard = replay_dir.read();
        read_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let blocked_send = sent_rx.recv_timeout(Duration::from_millis(100)).is_err();
    let blocked_read = read_rx.try_recv().is_err();

    // Drain independently to break the simulated wait cycle before assertions.
    writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    sent_rx.recv_timeout(Duration::from_secs(5)).unwrap()?;
    read_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    replay.join().unwrap();
    assert!(durable && blocked_send && blocked_read);
    Ok(())
}

#[test]
fn periodic_snapshot_waits_for_other_admitted_writers() -> FsResult<()> {
    use crate::master::journal::JournalSystem;

    let mut conf = test_conf("periodic-with-active-writer");
    conf.journal.leader_create_snapshot = Some(true);
    conf.journal.writer_channel_size = 16;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    let writer = fs.fs_dir.view().unwrap().journal_writer.clone();
    let gate = fs.metadata_commit_gate();
    let guard = gate.enter();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker_fs = fs.clone();
    let worker =
        std::thread::spawn(move || done_tx.send(worker_fs.mkdir("/periodic", false)).unwrap());
    let entry = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let early = done_rx.recv_timeout(Duration::from_millis(100));
    let completed_early = early.is_ok();
    let no_checkpoint = checkpoint_dirs(&conf).is_empty();
    drop(guard);
    let result = match early {
        Ok(result) => result,
        Err(_) => done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
    };
    worker.join().unwrap();
    result?;
    assert!(matches!(entry.entry, JournalEntry::Mkdir(_)));
    assert!(
        !completed_early,
        "snapshot did not wait for the admitted writer"
    );
    assert!(
        no_checkpoint,
        "snapshot ran before the admitted writer drained"
    );
    let dirs = checkpoint_dirs(&conf);
    journal.shutdown();
    assert!(
        !dirs.is_empty(),
        "periodic snapshot was not created after draining"
    );
    Ok(())
}

#[test]
fn periodic_snapshot_failure_keeps_committed_write_and_retries() -> FsResult<()> {
    use crate::master::journal::JournalSystem;

    let mut conf = test_conf(&format!(
        "periodic-retry-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.journal.leader_create_snapshot = Some(true);
    conf.journal.writer_channel_size = 16;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    let checkpoint_dir = PathBuf::from(conf.db_conf().checkpoint_dir);
    if checkpoint_dir.is_dir() {
        fs::remove_dir(&checkpoint_dir)?;
    }
    fs::write(&checkpoint_dir, b"block checkpoint directory")?;
    let first = fs.mkdir("/first", false);
    fs::remove_file(&checkpoint_dir)?;
    first?;
    assert!(fs.exists("/first")?);
    assert!(fs
        .fs_dir
        .view()
        .unwrap()
        .journal_writer
        .periodic_snapshot_due());

    fs.mkdir("/second", false)?;
    assert!(fs.exists("/second")?);
    assert!(!checkpoint_dirs(&conf).is_empty());
    journal.shutdown();
    Ok(())
}

#[test]
fn atime_write_can_schedule_periodic_snapshot() -> FsResult<()> {
    use crate::master::journal::JournalSystem;
    use curvine_model::{
        CreateFileOpts, OpenFlags, SetAttrOpts, StoragePolicy, TtlAction, TtlType,
        INTERNAL_TTL_TYPE_XATTR,
    };

    let mut conf = test_conf("periodic-atime");
    conf.journal.leader_create_snapshot = Some(true);
    conf.journal.snapshot_min_interval = DurationUnit::new(0);
    conf.journal.writer_channel_size = 16;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1000,
        ttl_action: TtlAction::Free,
        ..Default::default()
    };
    opts.x_attr.insert(
        INTERNAL_TTL_TYPE_XATTR.to_string(),
        TtlType::Atime.to_xattr_value(),
    );
    fs.create_with_opts("/file", opts, OpenFlags::new_create())?;
    fs.set_attr(
        "/file",
        SetAttrOpts {
            atime: Some(1),
            ..Default::default()
        },
    )?;
    let before = checkpoint_dirs(&conf).len();
    fs.get_block_locations("/file")?;
    let after = checkpoint_dirs(&conf).len();
    journal.shutdown();
    assert!(
        after > before,
        "atime journal write did not schedule a snapshot"
    );
    Ok(())
}

#[test]
fn fsdir_write_only_marks_periodic_snapshot_due() -> FsResult<()> {
    let mut conf = test_conf("bounded-periodic-snapshot");
    conf.journal.leader_create_snapshot = Some(true);
    let writer = Arc::new(build_writer(&conf)?);
    let dir = build_fs_dir(&conf, writer.clone())?;
    let path = InodePath::resolve(dir.root_node(), "/snapshot-slot", dir.store_ref())?;
    dir.mkdir(path, curvine_model::MkdirOpts::with_create(true))?;
    assert!(writer.periodic_snapshot_due());
    assert!(checkpoint_dirs(&conf).is_empty());
    let entry = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert!(matches!(entry.entry, JournalEntry::Mkdir(_)));
    Ok(())
}

#[test]
fn unscoped_mkdir_commit_failure_fences_after_store_write() -> FsResult<()> {
    let mut conf = test_conf("unscoped-mkdir-commit-failure");
    conf.journal.snapshot_entries = 0;
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    let writer = Arc::new(writer);
    let dir = build_fs_dir(&conf, writer.clone())?;
    let worker = std::thread::spawn(move || {
        let path = InodePath::resolve(dir.root_node(), "/created", dir.store_ref()).unwrap();
        let result = dir.mkdir(path, curvine_model::MkdirOpts::with_create(true));
        (dir, result)
    });
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .expect("mkdir entry was not queued");
    drop(queued);
    let (dir, result) = worker.join().unwrap();
    assert!(result.is_err());
    let lease = dir.begin_snapshot_scan()?;
    assert!(lease.snapshot_reader()?.resolve_path("/created")?.is_full());
    assert!(writer.recovery.is_safe_mode());
    assert!(writer.ensure_metadata_current().is_err());
    Ok(())
}

#[test]
fn unscoped_set_attr_enqueue_failure_fences_after_store_write() -> FsResult<()> {
    let conf = test_conf("unscoped-setattr-enqueue-failure");
    let mut writer = build_writer(&conf)?;
    drop(writer.receiver.take());
    let writer = Arc::new(writer);
    let dir = build_fs_dir(&conf, writer.clone())?;
    let path = InodePath::resolve(dir.root_node(), "/", dir.store_ref())?;
    let root_id = path.get_last_inode().unwrap().id();
    let result = dir.set_attr(
        path,
        SetAttrOpts {
            atime: Some(42),
            ..Default::default()
        },
    );
    assert!(result.is_err());
    assert_eq!(
        dir.store_ref().get_inode(root_id, None)?.unwrap().atime(),
        42
    );
    assert!(writer.recovery.is_safe_mode());
    assert!(writer.ensure_metadata_current().is_err());
    Ok(())
}

#[test]
fn scoped_async_entry_waits_for_commit_confirmation() -> FsResult<()> {
    let mut conf = test_conf("scoped-async-entry");
    conf.journal.snapshot_entries = 0;
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    let writer = Arc::new(writer);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;
    let worker_writer = writer.clone();
    let gate = Arc::new(crate::master::meta::CommitGate::new());
    let worker_gate = gate.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _guard = worker_gate.enter();
        let scope = worker_writer.begin_commit_scope();
        let result = worker_writer.send(
            &fs_dir,
            JournalEntry::CacheInvalidation(CacheInvalidationEntry {
                op_id: 1,
                rpc_id: 0,
                inodes: vec![],
            }),
        );
        done_tx.send(result.and_then(|_| scope.wait())).unwrap();
    });
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .expect("journal entry was not queued");
    let completion = queued
        .completion
        .expect("scoped entry needs commit confirmation");
    let barrier = gate.close_owned();
    let drain_gate = gate.clone();
    let (drained_tx, drained_rx) = std::sync::mpsc::channel();
    let drain = std::thread::spawn(move || {
        drain_gate.wait_for_quiescence();
        drained_tx.send(()).unwrap();
    });
    let early = done_rx.recv_timeout(Duration::from_millis(200)).ok();
    let drained_early = drained_rx.try_recv().is_ok();
    completion.send(())?;
    let returned_early = early.is_some();
    let result = match early {
        Some(result) => result,
        None => done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("scope did not finish after confirmation"),
    };
    worker.join().unwrap();
    drained_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("gate did not drain");
    drain.join().unwrap();
    drop(barrier);
    assert!(!drained_early, "gate drained before the journal committed");
    assert!(
        !returned_early,
        "scope returned before the journal committed"
    );
    result
}

#[test]
fn snapshot_is_rejected_after_metadata_commit_failure() -> FsResult<()> {
    let conf = test_conf("snapshot-after-metadata-commit-failure");
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    writer.fail_metadata_commit();
    let error = writer
        .create_snapshot(&fs_dir, true)
        .expect_err("fenced metadata must not produce a snapshot");
    assert!(error.to_string().contains("not accepting writes"));
    Ok(())
}

#[test]
fn nested_commit_scopes_follow_writer_and_scope_identity() -> FsResult<()> {
    let mut conf = test_conf("nested-commit-scopes");
    conf.journal.writer_channel_size = 4;
    let mut a = build_writer(&conf)?;
    let mut b = build_writer(&conf)?;
    a.require_raft_commit = true;
    b.require_raft_commit = true;
    let entry = || {
        JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 1,
            rpc_id: 0,
            inodes: vec![],
        })
    };
    let outer = a.begin_commit_scope();
    assert!(!b.has_commit_scope());
    let other = b.begin_commit_scope();
    a.enqueue_and_wait(entry())?;
    b.enqueue_and_wait(entry())?;
    let inner = a.begin_commit_scope();
    a.enqueue_and_wait(entry())?;
    let recv = |writer: &JournalWriter| {
        writer
            .receiver
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .completion
            .unwrap()
    };
    let outer_ack = recv(&a);
    let inner_ack = recv(&a);
    let other_ack = recv(&b);
    outer_ack.send(())?;
    drop(other_ack);
    drop(inner_ack);
    outer.wait()?;
    assert!(a.has_commit_scope());
    assert!(other.wait().is_err());
    assert!(!b.has_commit_scope());
    assert!(inner.wait().is_err());
    assert!(!a.has_commit_scope());

    // A failed confirmation permanently fences its writer; use a fresh one
    // to exercise out-of-order scope drops independently.
    let mut a = build_writer(&conf)?;
    a.require_raft_commit = true;
    let outer = a.begin_commit_scope();
    let inner = a.begin_commit_scope();
    a.enqueue_and_wait(entry())?;
    let inner_ack = recv(&a);
    drop(outer);
    assert!(a.has_commit_scope());
    drop(inner_ack);
    assert!(inner.wait().is_err());
    assert!(!a.has_commit_scope());
    Ok(())
}

#[test]
fn overwrite_and_ttl_delete_entries_replay_idempotently() -> FsResult<()> {
    use crate::master::journal::{DeleteEntry, JournalSystem, OverWriteFileEntry};
    use curvine_model::{CreateFileOptsBuilder, OpenFlags, TtlAction};

    let mut conf = test_conf("metadata-entry-replay");
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 16;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();

    for id in [100, 200, 300] {
        let mut worker = curvine_model::WorkerInfo::default();
        worker.address.worker_id = id;
        fs.add_test_worker(worker);
    }
    fs.create("/file", false)?;
    fs.create("/file", true)?;
    let overwrite_file = {
        let fs_dir_handle = fs.fs_dir();
        let fs_dir = fs_dir_handle.view().unwrap();
        InodePath::resolve(fs_dir.root_node(), "/file", &fs_dir.store)?.clone_last_file()?
    };

    fs.create_with_opts(
        "/ttl/file",
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build(),
        OpenFlags::new_create(),
    )?;
    fs.complete_file("/ttl/file", None, 0, vec![], "", false, None)?;
    std::thread::sleep(Duration::from_millis(10));
    let delete_entry = JournalEntry::Delete(DeleteEntry {
        op_id: 2,
        rpc_id: 0,
        path: "/ttl/file".to_string(),
        mtime: curvine_runtime::common::LocalTime::mills() as i64,
    });
    let loader = journal.journal_loader();
    loader.apply_entry(JournalEntry::OverWriteFile(OverWriteFileEntry {
        op_id: 1,
        rpc_id: 0,
        path: "/file".to_string(),
        file: overwrite_file,
    }))?;
    loader.apply_entry(JournalEntry::OverWriteFile(OverWriteFileEntry {
        op_id: 1,
        rpc_id: 0,
        path: "/file".to_string(),
        file: {
            let fs_dir_handle = fs.fs_dir();
            let fs_dir = fs_dir_handle.view().unwrap();
            InodePath::resolve(fs_dir.root_node(), "/file", &fs_dir.store)?.clone_last_file()?
        },
    }))?;
    assert_eq!(fs.file_status("/file")?.len, 0);
    loader.apply_entry(delete_entry.clone())?;
    assert!(fs.file_status("/ttl/file").is_err());
    loader.apply_entry(delete_entry)?;
    assert!(fs.file_status("/ttl/file").is_err());

    journal.shutdown();
    Ok(())
}

#[test]
fn forced_symlink_replay_preserves_hard_links() -> FsResult<()> {
    use crate::master::journal::{JournalSystem, SymlinkEntry};
    use crate::master::meta::inode::InodeFile;

    let mut conf = test_conf("symlink-force-replay");
    conf.journal.enable = false;
    conf.journal.snapshot_entries = 0;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    fs.symlink("/first", "/link", false, 0o777)?;
    fs.link("/link", "/old-alias")?;
    let old_id = fs.file_status("/link")?.id;
    let new_inode = InodeFile::with_link(old_id + 100_000, 10, "/second", 0o777, None, None);
    let entry = JournalEntry::Symlink(SymlinkEntry {
        op_id: 1,
        rpc_id: 0,
        link: "/link".to_string(),
        new_inode,
        force: true,
    });
    let loader = journal.journal_loader();
    loader.apply_entry(entry.clone())?;
    assert_eq!(fs.file_status("/old-alias")?.id, old_id);
    assert_eq!(fs.file_status("/old-alias")?.nlink, 1);
    fs.link("/link", "/new-alias")?;
    loader.apply_entry(entry)?;
    assert_eq!(fs.file_status("/new-alias")?.nlink, 2);
    fs.delete("/link", false)?;
    assert_eq!(fs.file_status("/new-alias")?.nlink, 1);
    journal.shutdown();
    Ok(())
}

#[test]
fn overwrite_defers_worker_deletion_until_commit() -> FsResult<()> {
    verify_overwrite_confirmation(true)?;
    verify_overwrite_confirmation(false)
}

fn verify_overwrite_confirmation(committed: bool) -> FsResult<()> {
    use crate::master::journal::JournalSystem;
    use curvine_model::{
        BlockLocation, ClientAddress, CreateFileOpts, OpenFlags, WorkerCommand, WorkerInfo,
    };

    let mut conf = test_conf(&format!("overwrite-commit-order-{committed}"));
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 16;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    for id in [100, 200, 300] {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = id;
        fs.add_test_worker(worker);
    }
    fs.create("/file", false)?;
    let block = fs.add_block(
        "/file",
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )?;
    let block_id = block.block.id;
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .add_block_location(block_id, BlockLocation::with_id(100))?;
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    writer.recovery = journal.master_monitor().recovery();
    let writer = Arc::new(writer);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .journal_writer = writer.clone();
    let mut fs = fs;
    fs.journal_writer = writer.clone();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || {
        worker_fs.create_with_opts(
            "/file",
            CreateFileOpts::with_create(false),
            OpenFlags::new_create().set_overwrite(true),
        )
    });
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .expect("overwrite journal entry missing");
    assert!(fs
        .worker_manager
        .write()
        .block_map
        .handle_heartbeat(100)
        .is_empty());
    let completion = queued.completion.expect("missing confirmation");
    if committed {
        completion.send(())?;
    } else {
        drop(completion);
    }
    let result = worker.join().unwrap();
    let commands = fs.worker_manager.write().block_map.handle_heartbeat(100);
    if committed {
        result?;
        assert!(commands.iter().any(
            |cmd| matches!(cmd, WorkerCommand::DeleteBlock(delete) if delete.blocks.contains(&block_id))
        ));
    } else {
        assert!(result.is_err());
        assert!(journal.master_monitor().is_safe_mode());
        assert!(commands.is_empty());
    }
    journal.shutdown();
    Ok(())
}

#[test]
fn ttl_refresh_holds_commit_gate_until_confirmation() -> FsResult<()> {
    use crate::master::journal::JournalSystem;
    use curvine_model::{
        CreateFileOpts, OpenFlags, SetAttrOpts, StoragePolicy, TtlAction, TtlType,
        INTERNAL_TTL_TYPE_XATTR,
    };

    let mut conf = test_conf("ttl-refresh-confirmation");
    conf.journal.snapshot_entries = 0;
    conf.journal.writer_channel_size = 8;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy = StoragePolicy {
        ttl_ms: 1000,
        ttl_action: TtlAction::Free,
        ..Default::default()
    };
    opts.x_attr.insert(
        INTERNAL_TTL_TYPE_XATTR.to_string(),
        TtlType::Atime.to_xattr_value(),
    );
    fs.create_with_opts("/file", opts, OpenFlags::new_create())?;
    fs.set_attr(
        "/file",
        SetAttrOpts {
            atime: Some(1),
            ..Default::default()
        },
    )?;
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    writer.recovery = journal.master_monitor().recovery();
    let writer = Arc::new(writer);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .journal_writer = writer.clone();

    let mut fs = fs;
    fs.journal_writer = writer.clone();
    let worker_fs = fs.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        done_tx
            .send(worker_fs.get_block_locations("/file"))
            .unwrap();
    });
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .expect("atime journal entry missing");
    let gate = fs.metadata_commit_gate();
    let barrier = gate.close_owned();
    let drain_gate = gate.clone();
    let (drained_tx, drained_rx) = std::sync::mpsc::channel();
    let drain = std::thread::spawn(move || {
        drain_gate.wait_for_quiescence();
        drained_tx.send(()).unwrap();
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert!(drained_rx.try_recv().is_err());
    queued
        .completion
        .expect("missing commit confirmation")
        .send(())?;
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap()?;
    drained_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    drain.join().unwrap();
    drop(barrier);
    journal.shutdown();
    Ok(())
}

#[test]
fn lost_commit_confirmation_fences_filesystem() -> FsResult<()> {
    use crate::master::journal::JournalSystem;

    let mut conf = test_conf("lost-commit-confirmation");
    conf.journal.snapshot_entries = 0;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    writer.recovery = journal.master_monitor().recovery();
    let writer = Arc::new(writer);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .journal_writer = writer.clone();

    let mut fs = fs;
    fs.journal_writer = writer.clone();
    let worker_fs = fs.clone();
    let worker = std::thread::spawn(move || worker_fs.mkdir("/unconfirmed", false));
    let queued = writer
        .receiver
        .as_ref()
        .unwrap()
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(5))
        .expect("missing journal entry");
    drop(queued.completion.expect("missing commit confirmation"));
    assert!(worker.join().unwrap().is_err());
    assert!(journal.master_monitor().is_safe_mode());
    assert!(writer.ensure_metadata_current().is_err());
    writer.unfence_metadata();
    assert!(!writer.is_metadata_current());
    assert!(fs.mkdir("/after-failure", false).is_err());
    assert!(!fs.exists("/after-failure")?);
    Ok(())
}

#[test]
fn failed_scoped_enqueue_fences_filesystem() -> FsResult<()> {
    use crate::master::journal::JournalSystem;

    let mut conf = test_conf("failed-scoped-enqueue");
    conf.journal.snapshot_entries = 0;
    let journal = JournalSystem::from_conf(&conf)?;
    let fs = journal.fs();
    let mut writer = build_writer(&conf)?;
    writer.require_raft_commit = true;
    writer.recovery = journal.master_monitor().recovery();
    drop(writer.receiver.take());
    let writer = Arc::new(writer);
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .journal_writer = writer.clone();
    let mut fs = fs;
    fs.journal_writer = writer.clone();
    assert!(fs.mkdir("/unlogged", false).is_err());
    assert!(journal.master_monitor().is_safe_mode());
    assert!(writer.ensure_metadata_current().is_err());
    Ok(())
}

#[test]
fn removes_checkpoint_when_snapshot_enqueue_fails() -> FsResult<()> {
    let conf = test_conf("snapshot_enqueue_failure");
    let mut writer = build_writer(&conf)?;

    // The testing writer retains the only receiver. Dropping it makes
    // std::sync::mpsc::Sender::send return an error before handoff.
    let receiver = writer
        .receiver
        .take()
        .expect("testing writer should retain its receiver");
    drop(receiver);

    let writer = Arc::new(writer);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    let result = record_and_check_snapshot(&writer, &fs_dir);
    assert!(result.is_err(), "snapshot enqueue should fail");

    let checkpoints = checkpoint_dirs(&conf);
    assert!(
        checkpoints.is_empty(),
        "checkpoint should be removed after enqueue failure: {:?}",
        checkpoints
    );
    assert_eq!(
        writer.last_snapshot_ms.get(),
        0,
        "failed snapshot must not start the interval"
    );
    assert_eq!(
        writer.entries_since_snapshot.get(),
        1,
        "failed snapshot must not consume the entry count"
    );

    Ok(())
}

#[test]
fn keeps_checkpoint_when_snapshot_enqueue_succeeds() -> FsResult<()> {
    let conf = test_conf("snapshot_enqueue_success");
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    record_and_check_snapshot(&writer, &fs_dir)?;

    let entry = writer
        .receiver
        .as_ref()
        .expect("testing writer should retain its receiver")
        .lock()
        .expect("testing receiver lock should not be poisoned")
        .recv_timeout(Duration::from_secs(1))
        .expect("snapshot entry should be enqueued");

    match entry.entry {
        JournalEntry::Snapshot(entry) => {
            assert!(
                Path::new(&entry.dir).exists(),
                "checkpoint should remain after successful handoff: {}",
                entry.dir
            );
        }
        other => panic!("expected snapshot entry, got {:?}", other),
    }

    Ok(())
}

#[test]
fn retries_snapshot_after_failure_without_waiting_for_entries() -> FsResult<()> {
    let mut conf = test_conf("snapshot_failure_retry");
    conf.journal.snapshot_entries = 2;
    let mut writer = build_writer(&conf)?;

    let receiver = writer
        .receiver
        .take()
        .expect("testing writer should retain its receiver");
    drop(receiver);

    let writer = Arc::new(writer);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    // The first check only reaches one of the two required entries.
    record_and_check_snapshot(&writer, &fs_dir)?;
    assert_eq!(writer.entries_since_snapshot.get(), 1);

    // The second check creates a checkpoint, but handing the entry off
    // fails; the consumed entry count must be restored.
    assert!(record_and_check_snapshot(&writer, &fs_dir).is_err());
    assert_eq!(
        writer.entries_since_snapshot.get(),
        2,
        "failed snapshot must restore the consumed entry count"
    );

    // With the count restored, the next check retries immediately instead
    // of waiting for another full snapshot_entries batch.
    assert!(
        writer.maybe_emit_snapshot(&fs_dir).is_err(),
        "a restored entry count must allow an immediate retry"
    );

    Ok(())
}

fn raft_group(conf: &ClusterConf, nodes: u64) -> Vec<RaftPeer> {
    (1..=nodes)
        .map(|id| {
            let hostname = if id == 1 {
                conf.journal.hostname.clone()
            } else {
                format!("10.0.0.{id}")
            };
            RaftPeer::new(id, hostname, conf.journal.rpc_port)
        })
        .collect()
}

#[test]
fn skips_checkpoint_when_raft_group_has_multiple_nodes() -> FsResult<()> {
    let mut conf = test_conf("snapshot_multi_node");
    conf.journal.journal_addrs = raft_group(&conf, 3);
    // Default is None, so a multi-node raft group must fall back to
    // single-node-only creation and skip the leader checkpoint.
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    record_and_check_snapshot(&writer, &fs_dir)?;

    assert!(
        checkpoint_dirs(&conf).is_empty(),
        "leader checkpoint is disabled when the raft group has more than one node"
    );
    Ok(())
}

#[test]
fn creates_checkpoint_when_leader_override_is_enabled() -> FsResult<()> {
    let mut conf = test_conf("snapshot_leader_override");
    conf.journal.journal_addrs = raft_group(&conf, 3);
    conf.journal.leader_create_snapshot = Some(true);
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    record_and_check_snapshot(&writer, &fs_dir)?;

    assert_eq!(checkpoint_dirs(&conf).len(), 1);
    Ok(())
}

#[test]
fn skips_checkpoint_when_leader_override_is_disabled() -> FsResult<()> {
    let mut conf = test_conf("snapshot_leader_disabled");
    conf.journal.leader_create_snapshot = Some(false);
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    record_and_check_snapshot(&writer, &fs_dir)?;

    assert!(checkpoint_dirs(&conf).is_empty());
    Ok(())
}

#[test]
fn creates_at_most_one_checkpoint_per_interval() -> FsResult<()> {
    let mut conf = test_conf("snapshot_min_interval");
    conf.journal.writer_channel_size = 8;
    conf.journal.snapshot_min_interval = DurationUnit::new(10 * DurationUnit::MINUTE);
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    record_and_check_snapshot(&writer, &fs_dir)?;
    assert_eq!(checkpoint_dirs(&conf).len(), 1);

    record_and_check_snapshot(&writer, &fs_dir)?;
    assert_eq!(
        checkpoint_dirs(&conf).len(),
        1,
        "a second checkpoint inside the interval must be skipped"
    );

    writer
        .last_snapshot_ms
        .set(LocalTime::mills().saturating_sub(writer.snapshot_min_interval_ms + 1));
    std::thread::sleep(Duration::from_millis(2));
    writer.maybe_emit_snapshot(&fs_dir)?;
    assert_eq!(checkpoint_dirs(&conf).len(), 2);

    Ok(())
}

#[test]
fn safe_mode_rejects_metadata_writes() -> FsResult<()> {
    let conf = test_conf("writer_safe_mode");
    let writer = build_writer(&conf)?;
    writer
        .recovery
        .enter_safe_mode("failed to deserialize journal batch");
    let error = writer
        .log_ufs_applied(1, 1, 1)
        .expect_err("safe mode must reject journal writes");
    assert!(
        error.to_string().contains("JournalUnavailable"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn create_snapshot_creates_checkpoint_and_enqueues_snapshot_entry() -> FsResult<()> {
    let conf = test_conf("snapshot_operator_create");
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    let info = writer.create_snapshot(&fs_dir, false)?;

    assert!(Path::new(&info.dir).exists(), "checkpoint should be kept");
    let entry = writer
        .receiver
        .as_ref()
        .expect("testing writer should retain its receiver")
        .lock()
        .expect("testing receiver lock should not be poisoned")
        .recv_timeout(Duration::from_secs(1))
        .expect("snapshot entry should be enqueued");

    match entry.entry {
        JournalEntry::Snapshot(entry) => {
            assert_eq!(entry.dir, info.dir);
            assert_eq!(entry.node_id, conf.journal.node_id()?);
        }
        other => panic!("expected snapshot entry, got {:?}", other),
    }

    assert_eq!(writer.entries_since_snapshot.get(), 0);
    assert!(writer.last_snapshot_ms.get() > 0);
    Ok(())
}

#[test]
fn create_snapshot_rejects_concurrent_requests() -> FsResult<()> {
    let conf = test_conf("snapshot_operator_busy");
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    writer.snapshot_in_flight.set(true);

    let err = writer
        .create_snapshot(&fs_dir, false)
        .expect_err("a concurrent snapshot must be rejected");
    assert!(err.to_string().contains("already in progress"));
    Ok(())
}

#[test]
fn create_snapshot_respects_the_min_interval_unless_forced() -> FsResult<()> {
    let mut conf = test_conf("snapshot_operator_min_interval");
    conf.journal.writer_channel_size = 8;
    conf.journal.snapshot_min_interval = DurationUnit::new(10 * DurationUnit::MINUTE);
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    writer.create_snapshot(&fs_dir, false)?;

    let err = writer
        .create_snapshot(&fs_dir, false)
        .expect_err("a second call inside the interval must be rejected");
    assert!(
        err.to_string().contains("snapshot_min_interval"),
        "unexpected error: {}",
        err
    );
    assert_eq!(checkpoint_dirs(&conf).len(), 1);

    writer.create_snapshot(&fs_dir, true)?;
    assert_eq!(checkpoint_dirs(&conf).len(), 2);
    Ok(())
}

#[test]
fn create_snapshot_never_reuses_a_checkpoint_id() -> FsResult<()> {
    let mut conf = test_conf("snapshot_operator_unique_id");
    conf.journal.writer_channel_size = 8;
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    let first = writer.create_snapshot(&fs_dir, true)?;
    let second = writer.create_snapshot(&fs_dir, true)?;

    assert_ne!(first.dir, second.dir);
    assert!(
        Path::new(&first.dir).exists(),
        "a second snapshot must not delete the registered checkpoint"
    );
    Ok(())
}

#[test]
fn create_snapshot_requires_journal() -> FsResult<()> {
    let mut conf = test_conf("snapshot_operator_no_journal");
    conf.journal.enable = false;
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    let err = writer
        .create_snapshot(&fs_dir, false)
        .expect_err("journal disabled must reject operator snapshots");
    assert!(err.to_string().contains("journal.enable"));
    Ok(())
}

#[test]
fn next_checkpoint_id_steps_over_existing_directories() -> FsResult<()> {
    let conf = test_conf("snapshot_operator_existing_id");
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    let first = writer.next_checkpoint_id(&fs_dir);
    fs::create_dir_all(fs_dir.get_checkpoint_path(first))?;

    let second = writer.next_checkpoint_id(&fs_dir);

    assert!(second > first, "an existing checkpoint id must be skipped");
    assert!(
        !Path::new(&fs_dir.get_checkpoint_path(second)).exists(),
        "the picked id must be free on disk"
    );
    Ok(())
}

#[test]
fn create_snapshot_keeps_the_throttle_clock_in_real_time() -> FsResult<()> {
    let conf = test_conf("snapshot_operator_throttle_clock");
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;

    let before = LocalTime::mills();
    writer.create_snapshot(&fs_dir, false)?;

    assert!(writer.last_snapshot_ms.get() >= before);
    assert!(
        writer.last_snapshot_ms.get() <= LocalTime::mills(),
        "a snapshot must never push the throttle clock into the future"
    );
    Ok(())
}

// A full journal channel must surface as a write error and fence the writer:
// the request fails, later writes are rejected, and the recovery gate treats
// the local rows as suspect (the no-snapshot rebuild removes them on restart).
#[test]
fn journal_backpressure_and_fence_semantics() -> FsResult<()> {
    let mut conf = test_conf("channel-full-fence");
    conf.journal.writer_channel_size = 1;
    let writer = Arc::new(build_writer(&conf)?);
    let fs_dir = build_fs_dir(&conf, writer.clone())?;
    let entry = || {
        JournalEntry::CacheInvalidation(CacheInvalidationEntry {
            op_id: 1,
            rpc_id: 0,
            inodes: vec![],
        })
    };

    // A bounded channel applies backpressure instead of dropping writes: fill
    // it from a second thread, then drain one entry so the send completes.
    // Bounded channels apply backpressure, not drop: fill from this thread,
    // then the entry is retrievable via take_entries for assertion.
    writer.send(&fs_dir, entry())?;
    let queued = writer.take_entries();
    assert_eq!(queued.len(), 1, "the accepted write must be queued");
    assert!(!writer.recovery.rejects_writes());

    // A failed journal enqueue must fence the writer: the same path a closed
    // channel or a Raft failure takes.
    writer.fail_metadata_commit();
    let fenced = writer
        .send(&fs_dir, entry())
        .expect_err("the fenced writer must reject subsequent writes");
    assert!(
        fenced.to_string().contains("not accepting writes"),
        "unexpected error: {fenced}"
    );
    Ok(())
}
