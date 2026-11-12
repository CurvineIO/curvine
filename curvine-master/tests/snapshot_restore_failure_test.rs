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
use curvine_master::master::meta::inode::ttl::TtlBucketList;
use curvine_master::master::meta::store::{InodeStore, RocksInodeStore};
use curvine_master::Master;
use curvine_raft::raft::storage::AppStorage;
use curvine_rocksdb::DBConf;
use curvine_runtime::common::DurationUnit;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn restore_rebuilds_ttl_index_from_checkpoint() {
    use curvine_model::{CreateFileOpts, OpenFlags, TtlAction};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-ttl-index-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy.ttl_ms = 1_000;
    opts.storage_policy.ttl_action = TtlAction::Delete;
    let kept = fs
        .create_with_opts("/kept", opts.clone(), OpenFlags::new_create())
        .unwrap();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    let removed = fs
        .create_with_opts("/removed", opts, OpenFlags::new_create())
        .unwrap();
    let buckets = fs.fs_dir.view().unwrap().store_ref().get_ttl_bucket_list();
    assert_eq!(buckets.total_inodes(), 2);

    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0)
        .unwrap();
    assert!(fs.exists("/kept").unwrap());
    assert!(!fs.exists("/removed").unwrap());
    let ids: Vec<_> = buckets
        .get_expired_buckets_at(i64::MAX)
        .iter()
        .flat_map(|bucket| bucket.inodes.lock().iter().collect::<Vec<_>>())
        .collect();
    assert_eq!(ids, vec![kept.id as u64]);
    assert_ne!(kept.id, removed.id);
    journal.shutdown();
}

#[test]
fn checkpoint_is_unchanged_after_restore_and_new_wal_writes() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-wal-isolation-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/baseline", false).unwrap();
    fs.fs_dir
        .view()
        .unwrap()
        .store_journal_applied(b"baseline")
        .unwrap();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();

    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0)
        .unwrap();
    fs.mkdir("/later", false).unwrap();
    fs.fs_dir
        .view()
        .unwrap()
        .store_journal_applied(b"later")
        .unwrap();
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0)
        .unwrap();

    assert!(fs.exists("/baseline").unwrap());
    assert!(!fs.exists("/later").unwrap());
    assert_eq!(
        fs.fs_dir
            .view()
            .unwrap()
            .load_journal_applied()
            .unwrap()
            .unwrap(),
        b"baseline"
    );
    journal.shutdown();
}

#[test]
fn snapshot_restore_waits_for_admitted_metadata_commit() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-commit-gate-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/baseline", false).unwrap();
    let loader = journal.journal_loader();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let snapshot = rt.block_on(loader.create_snapshot()).unwrap();
    let gate = fs.metadata_commit_gate();
    let admitted = gate.enter();
    let (done, result) = std::sync::mpsc::channel();
    let (started, start) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        started.send(()).unwrap();
        done.send(rt.block_on(loader.apply_snapshot(snapshot)))
            .unwrap();
    });
    start.recv_timeout(Duration::from_secs(5)).unwrap();
    let early = result.recv_timeout(Duration::from_secs(2));
    let was_early = early.is_ok();
    drop(admitted);
    let applied = match early {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            result.recv_timeout(Duration::from_secs(5)).unwrap()
        }
        Err(error) => panic!("restore worker disconnected: {error}"),
    };
    worker.join().unwrap();
    applied.unwrap();
    journal.shutdown();
    assert!(!was_early, "snapshot restored while a commit was admitted");
}

#[test]
fn restore_rejects_guard_from_another_store() {
    Master::init_test_metrics();
    let new_store = || {
        let path = curvine_runtime::common::Utils::test_sub_dir(format!(
            "restore-guard-owner-{}",
            curvine_runtime::common::Utils::rand_str(8)
        ));
        let rocks = RocksInodeStore::new(DBConf::new(path), true).unwrap();
        InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000).unwrap())).unwrap()
    };
    let first = new_store();
    let mut second = new_store();
    let guard = first.begin_restore(Duration::from_secs(1)).unwrap();
    let error = second
        .restore_with_guard(&guard, "/nonexistent-checkpoint")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("guard belongs to a different store"));
    assert!(second.begin_snapshot_scan().is_ok());
    drop(guard);
}

#[cfg(feature = "fault-injection")]
#[test]
fn restore_failure_before_replacement_preserves_cursor_store() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let path = curvine_runtime::common::Utils::test_sub_dir(format!(
        "restore-cursor-failure-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let conf = DBConf::new(&path);
    let data_dir = conf.data_dir.clone();
    let rocks = RocksInodeStore::new(conf, true).unwrap();
    let mut store = InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000).unwrap())).unwrap();
    let checkpoint = store.create_checkpoint(1).unwrap();
    let cursor = store.cursor_store();
    let guard = store.begin_restore(Duration::from_secs(1)).unwrap();
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.restore.before_temp_store")
        .matches("data_dir", data_dir.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected temporary store failure")
        .unwrap();
    faults.configure("restore-cursor-failure", rule).unwrap();
    let result = store.restore_with_guard(&guard, &checkpoint);
    faults.remove("restore-cursor-failure").unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected temporary store failure"));
    cursor.put("test-cursor", b"original").unwrap();
    assert_eq!(
        store
            .store()
            .get_common_kv("test-cursor")
            .unwrap()
            .as_deref(),
        Some(&b"original"[..])
    );
    drop(guard);
    let guard = store.begin_restore(Duration::from_secs(1)).unwrap();
    store.restore_with_guard(&guard, &checkpoint).unwrap();
    cursor.put("test-cursor", b"restored").unwrap();
    assert_eq!(
        store
            .store()
            .get_common_kv("test-cursor")
            .unwrap()
            .as_deref(),
        Some(&b"restored"[..])
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn filesystem_restore_failure_before_replacement_preserves_live_tree() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use curvine_model::RenameFlags;

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-live-tree-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/parent/child", true).unwrap();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    let rule = FaultRuleBuilder::named("master.metadata.restore.before_temp_store")
        .matches("data_dir", conf.db_conf().data_dir.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected pre-replacement failure")
        .unwrap();
    let faults = FaultRuntime::process();
    faults.configure("restore-live-tree-failure", rule).unwrap();
    let result = fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0);
    faults.remove("restore-live-tree-failure").unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected pre-replacement failure"));
    fs.rename("/parent/child", "/parent/moved", RenameFlags::empty())
        .unwrap();
    assert!(fs.exists("/parent/moved").unwrap());
    journal.shutdown();
}

#[cfg(feature = "fault-injection")]
#[test]
fn restore_failure_after_detach_fences_new_writes() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-detach-failure-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/existing", false).unwrap();
    let mut space_conf = conf.master.clone();
    space_conf.dir_space.enabled = true;
    let space = curvine_master::master::space::DirSpaceManager::new(
        fs.clone(),
        Arc::new(curvine_master::master::mount::MountManager::new(fs.clone())),
        Arc::new(space_conf),
    );
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.restore.after_store_detach")
        .matches("data_dir", conf.db_conf().data_dir.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected post-detach failure")
        .unwrap();
    faults.configure("restore-detach-failure", rule).unwrap();
    let result = fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0);
    faults.remove("restore-detach-failure").unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected post-detach failure"));
    assert!(journal.master_monitor().is_safe_mode());
    assert!(fs.mkdir("/must-not-create", false).is_err());
    assert!(matches!(
        space.query("/existing"),
        Err(
            curvine_master::master::space::DirSpaceQueryError::SnapshotMiss(
                curvine_master::master::space::SnapshotMiss::NoSnapshot
            )
        )
    ));
    journal.shutdown();
}

#[cfg(feature = "fault-injection")]
#[test]
fn restore_failure_after_detach_rejects_direct_reads() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-detach-read-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.mkdir("/existing", false).unwrap();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.metadata.restore.after_store_detach")
        .matches("data_dir", conf.db_conf().data_dir.as_str())
        .unwrap()
        .times(1)
        .unwrap()
        .return_error("injected post-detach failure")
        .unwrap();
    faults.configure("restore-detach-read", rule).unwrap();
    let result = fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0);
    faults.remove("restore-detach-read").unwrap();
    assert!(result.is_err());
    let read = fs.exists("/existing");
    assert!(
        read.is_err(),
        "direct reads must not use an invalid store: {read:?}"
    );
    assert!(fs.file_status("/existing").is_err());
    assert!(fs.list_status("/").is_err());
    assert!(fs.list_status("/*").is_err());
    assert!(fs.list_options("/", Default::default()).is_err());
    {
        let dir = fs.fs_dir.view().unwrap();
        assert!(dir.begin_snapshot_scan().is_err());
        assert!(dir.create_checkpoint(2).is_err());
        assert!(dir.sum_hash().is_err());
        assert!(dir.create_tree().is_err());
        assert!(dir.get_inode_path(1).is_err());
        assert!(dir.get_block_locations(1).is_err());
        assert!(dir
            .add_block_location(1, curvine_model::BlockLocation::with_id(100))
            .is_err());
        assert!(dir.block_ids_for_worker(100).is_err());
        assert!(dir.delete_locations(100).is_err());
        assert!(curvine_master::master::meta::inode::InodePath::resolve(
            dir.root_node(),
            "/existing",
            dir.store_ref()
        )
        .is_err());
    }
    assert!(fs.restore_from_rocksdb().is_err());
    assert!(fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .block_report(vec![
            (false, 1, curvine_model::BlockLocation::with_id(100),)
        ])
        .is_err());
    journal.shutdown();
}

#[test]
fn drained_lease_releases_store_before_restore_proceeds() {
    Master::init_test_metrics();
    let path = curvine_runtime::common::Utils::test_sub_dir(format!(
        "restore-lease-release-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let rocks = RocksInodeStore::new(DBConf::new(path), true).unwrap();
    let mut store = InodeStore::new(rocks, Arc::new(TtlBucketList::new(60_000).unwrap())).unwrap();
    for id in 1..=16 {
        let checkpoint = store.create_checkpoint(id).unwrap();
        let lease = store.begin_snapshot_scan().unwrap();
        let control = store.scan_control().clone();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let guard = store.begin_restore(Duration::from_secs(5)).unwrap();
                store.restore_with_guard(&guard, &checkpoint)
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while !control.is_restoring() && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(control.is_restoring());
            drop(lease);
            // Two layers: the thread must not panic, and the restore itself
            // must succeed. Dropping the inner Result would hide a business
            // error behind a clean thread exit.
            worker
                .join()
                .expect("restore worker must not panic")
                .expect("restore must succeed once the scan lease is released");
        });
    }
}

#[test]
fn overlapping_restores_cannot_reopen_scan_admission() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-overlap-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    {
        let dir = fs.fs_dir.view().unwrap();
        let store = dir.store_ref();
        let first = store.begin_restore(Duration::from_secs(1)).unwrap();
        let epoch = store.scan_control().cancel_epoch();
        assert!(store.begin_restore(Duration::from_secs(1)).is_err());
        assert_eq!(store.scan_control().cancel_epoch(), epoch);
        assert!(store.begin_snapshot_scan().is_err());
        drop(first);
        let lease = store.begin_snapshot_scan().unwrap();
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| store.begin_restore(Duration::from_secs(5)));
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while !store.scan_control().is_restoring() && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(store.scan_control().is_restoring());
            assert!(store.begin_restore(Duration::from_secs(1)).is_err());
            assert!(store.begin_snapshot_scan().is_err());
            drop(lease);
            // `begin_restore` returns the guard itself, so unwrap it to hold a
            // real guard: dropping the Result only simulates holding one.
            let pending = worker
                .join()
                .expect("restore worker must not panic")
                .expect("restore must acquire the guard once the scan lease is released");
            assert!(
                store.begin_snapshot_scan().is_err(),
                "scan admission must stay closed while the restore guard is held"
            );
            drop(pending);
        });
        assert!(store.begin_snapshot_scan().is_ok());
    }
    journal.shutdown();
}

#[test]
fn restore_scan_timeout_returns_error_without_reentering_fs_lock() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-error-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    conf.master.dir_space.restore_wait_timeout_unit = DurationUnit::new(50);
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.create("/existing", false).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let loader = journal.journal_loader();
    let snapshot = runtime.block_on(loader.create_snapshot()).unwrap();
    let lease = fs.fs_dir.view().unwrap().begin_snapshot_scan().unwrap();
    let result = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), loader.apply_snapshot(snapshot)).await
    });
    drop(lease);
    let error = result
        .expect("restore must not hang")
        .expect_err("active lease must prevent restore");
    assert!(
        error.to_string().contains("failed to restore metadata"),
        "{error}"
    );
    assert!(fs.exists("/existing").unwrap());
    journal.shutdown();
}

#[cfg(feature = "fault-injection")]
#[test]
fn snapshot_cursor_write_failure_fences_restore() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "snapshot-cursor-failure-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    journal.fs().mkdir("/existing", false).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let loader = journal.journal_loader();
    let mut snapshot = runtime.block_on(loader.create_snapshot()).unwrap();
    snapshot.fsm_state.applied.index = 1;
    let faults = FaultRuntime::process();
    let rule = FaultRuleBuilder::named("master.journal.cursor.before_write")
        .times(1)
        .unwrap()
        .return_error("injected snapshot cursor failure")
        .unwrap();
    faults.configure("snapshot-cursor-failure", rule).unwrap();
    let result = runtime.block_on(loader.apply_snapshot(snapshot));
    faults.remove("snapshot-cursor-failure").unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected snapshot cursor failure"));
    assert_eq!(loader.get_fsm_state().applied.index, 0);
    assert!(journal.master_monitor().is_safe_mode());
    assert!(journal.fs().exists("/existing").is_err());
    journal.shutdown();
}

#[test]
fn empty_checkpoint_is_rejected_before_replacing_an_empty_store() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-empty-checkpoint-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let loader = journal.journal_loader();
    let mut snapshot = runtime.block_on(loader.create_snapshot()).unwrap();
    let path = std::env::temp_dir().join(format!(
        "curvine-empty-checkpoint-{}",
        curvine_runtime::common::Utils::rand_str(12)
    ));
    std::fs::create_dir(&path).unwrap();
    snapshot.files_data.as_mut().unwrap().dir = path.to_str().unwrap().to_owned();
    let result = runtime.block_on(loader.apply_snapshot(snapshot.clone()));
    assert!(result.is_err(), "empty checkpoint must be rejected");
    std::fs::write(path.join("garbage"), b"not a database").unwrap();
    let result = runtime.block_on(loader.apply_snapshot(snapshot));
    assert!(result.is_err(), "non-RocksDB checkpoint must be rejected");
    assert!(!journal.master_monitor().is_safe_mode());
    journal.fs().mkdir("/still-usable", false).unwrap();
    assert!(journal.fs().exists("/still-usable").unwrap());
    journal.shutdown();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_failure_returns_error_without_reentering_fs_lock() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "checkpoint-error-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let checkpoint_dir = conf.db_conf().checkpoint_dir;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    fs.create("/existing", false).unwrap();
    if std::path::Path::new(&checkpoint_dir).exists() {
        std::fs::remove_dir_all(&checkpoint_dir).unwrap();
    }
    std::fs::write(&checkpoint_dir, b"not a directory").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let loader = journal.journal_loader();
    let result = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), loader.create_snapshot()).await
    });
    let error = result
        .expect("checkpoint must not hang")
        .expect_err("invalid checkpoint directory must fail");
    assert!(
        error
            .to_string()
            .contains("failed to create metadata checkpoint"),
        "{error}"
    );
    assert!(fs.exists("/existing").unwrap());
    journal.shutdown();
}

#[cfg(feature = "fault-injection")]
#[test]
fn restore_from_rocksdb_repair_failure_keeps_old_tree_and_ttl_rebuilt() {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    use curvine_master::master::meta::inode::ROOT_INODE_ID;
    use curvine_model::{CreateFileOpts, OpenFlags, TtlAction};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-rebuild-repair-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy.ttl_ms = 1_000;
    opts.storage_policy.ttl_action = TtlAction::Delete;
    let kept = fs
        .create_with_opts("/kept", opts, OpenFlags::new_create())
        .unwrap();
    let buckets = fs.fs_dir.view().unwrap().store_ref().get_ttl_bucket_list();
    assert_eq!(buckets.total_inodes(), 1);

    // Plant an orphan edge under the root (no inode row for 999_999): the
    // rebuild must repair it via delete_child, guaranteeing
    // `has_repairs == true` so the repair-batch commit fault point fires.
    {
        let dir = fs.fs_dir.view().unwrap();
        let mut batch = dir.store_ref().new_batch();
        batch.add_child(ROOT_INODE_ID, "orphan", 999_999).unwrap();
        batch.commit().unwrap();
    }

    // Inject a repair-batch commit failure: the rebuild fails, the old tree
    // stays visible, and the TTL list has already been repopulated from the
    // current database (the walk runs before the repair commit).
    let runtime = FaultRuntime::process();
    let rule_id = "restore-rebuild-repair-failure";
    let rule = FaultRuleBuilder::named("master.metadata.restore.repair_commit")
        .matches("phase", "restore_from_rocksdb")
        .unwrap()
        .return_error("injected repair batch commit failure")
        .unwrap();
    runtime.configure(rule_id, rule).unwrap();

    let rebuild = fs
        .fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore_from_rocksdb();
    let executions = runtime
        .rule(rule_id)
        .unwrap()
        .map(|rule| rule.executions)
        .unwrap_or(0);
    runtime.remove(rule_id).unwrap();
    assert!(rebuild.is_err(), "injected repair failure must propagate");
    assert_eq!(
        executions, 1,
        "repair commit fault point must fire exactly once"
    );

    // Old tree still readable. The repair commit failed after the TTL walk
    // had repopulated the list from the current database: /kept is present.
    assert!(fs.exists("/kept").unwrap());
    let ttl_ids: Vec<_> = buckets
        .get_expired_buckets_at(i64::MAX)
        .iter()
        .flat_map(|bucket| bucket.inodes.lock().iter().collect::<Vec<_>>())
        .collect();
    assert_eq!(ttl_ids, vec![kept.id as u64]);
    journal.shutdown();
}

#[test]
fn restore_from_rocksdb_rebuilds_ttl_from_current_db() {
    use curvine_model::{CreateFileOpts, OpenFlags, TtlAction};

    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!(
        "restore-rebuild-ttl-{}",
        curvine_runtime::common::Utils::rand_str(8)
    ));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut opts = CreateFileOpts::with_create(true);
    opts.storage_policy.ttl_ms = 1_000;
    opts.storage_policy.ttl_action = TtlAction::Delete;
    let kept = fs
        .create_with_opts("/kept", opts, OpenFlags::new_create())
        .unwrap();

    // Successful rebuild: the TTL list is cleared and repopulated from the
    // current database (only /kept remains).
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore_from_rocksdb()
        .unwrap();
    assert!(fs.exists("/kept").unwrap());
    let ids: Vec<_> = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .get_ttl_bucket_list()
        .get_expired_buckets_at(i64::MAX)
        .iter()
        .flat_map(|bucket| bucket.inodes.lock().iter().collect::<Vec<_>>())
        .collect();
    assert_eq!(ids, vec![kept.id as u64]);
    journal.shutdown();
}
