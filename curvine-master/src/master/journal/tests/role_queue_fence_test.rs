// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::master::meta::fs_dir_handle::DEFAULT_COLD_TIMEOUT;
use curvine_config::ClusterConf;
use curvine_runtime::common::Utils;

#[test]
fn leader_replay_advances_ids_without_namespace_lock() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("leader-replay-lock-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let entry = JournalEntry::Mkdir(MkdirEntry {
            op_id: 1234,
            rpc_id: 0,
            path: "/replayed".to_string(),
            dir: crate::master::meta::inode::InodeDir::new(4321, 0),
        });
        let mut batch = JournalBatch::new(1);
        batch.push(entry);
        let raft_entry = Entry {
            index: 1,
            term: 1,
            data: batch.encode().unwrap(),
            ..Default::default()
        };
        tx.send(runtime.block_on(loader.apply0(true, &raft_entry, false)))
            .unwrap();
    });
    let early = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(guard);
    let result = match early {
        Ok(result) => result,
        Err(error) => {
            worker.join().unwrap();
            journal.shutdown();
            panic!("leader replay waited for the namespace lock: {error}");
        }
    };
    worker.join().unwrap();
    result.unwrap();
    let dir = fs.fs_dir.view().unwrap();
    assert!(dir.op_id.get() >= 1234);
    assert!(dir.last_inode_id() >= 4321);
    drop(dir);
    journal.shutdown();
}

#[test]
fn leader_snapshot_packaging_does_not_wait_for_namespace_lock() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("leader-snapshot-lock-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(123).unwrap();
    let guard = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tx.send(loader.create_snapshot0(Some(checkpoint))).unwrap();
    });
    let early = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(guard);
    match early {
        Ok(result) => {
            result.unwrap();
        }
        Err(error) => {
            worker.join().unwrap();
            journal.shutdown();
            panic!("leader snapshot packaging waited for the namespace lock: {error}");
        }
    }
    worker.join().unwrap();
    journal.shutdown();
}

#[test]
fn snapshot_rejects_partially_applied_retrying_batch() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-retry-partial-{}", Utils::rand_str(8)));
    conf.journal.retry_interval_secs = 60;
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let mut batch = JournalBatch::new(1);
    for (path, id, parent) in [("/applied", 5001, 0), ("/missing/child", 5002, 9999)] {
        batch.push(JournalEntry::Mkdir(MkdirEntry {
            op_id: id as u64,
            rpc_id: 0,
            path: path.to_string(),
            dir: crate::master::meta::inode::InodeDir::new(id, parent),
        }));
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        loader
            .sender
            .send(ApplyMsg::new_entry(Entry {
                index: 1,
                term: 1,
                data: batch.encode().unwrap(),
                ..Default::default()
            }))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), loader.create_snapshot())
            .await
            .unwrap()
    });
    assert!(
        fs.exists("/applied").unwrap(),
        "first operation should have applied"
    );
    assert_eq!(loader.get_fsm_state().applied.index, 0);
    assert!(
        matches!(result, Err(RaftError::Raft(error)) if matches!(error.source, raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable)))
    );

    // The Raft background job must treat this as a retryable absence, not a
    // failed job that panics the next time the node checks snapshot state.
    let rt = conf.journal.create_runtime();
    let client = curvine_raft::raft::RaftClient::from_conf(rt.clone(), &conf.journal);
    let peer = curvine_raft::raft::storage::PeerStorage::new(
        rt,
        1,
        loader.log_store.clone(),
        loader.clone(),
        client,
        &conf.journal,
    );
    peer.gen_create_snapshot_job().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !peer.can_generate_snapshot() {
        assert!(
            std::time::Instant::now() < deadline,
            "snapshot job did not finish"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    drop(peer);
    journal.shutdown();
}

#[test]
fn snapshot_rejects_safe_mode() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-safe-mode-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal.journal_loader();
    loader.recovery.enter_safe_mode("injected partial apply");
    assert!(loader.create_snapshot0(None).is_err());
    journal.shutdown();
}

#[test]
fn snapshot_directory_check_waits_for_active_restore() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-check-restore-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let restore = fs
        .fs_dir
        .view()
        .unwrap()
        .store_ref()
        .begin_restore(std::time::Duration::from_secs(5))
        .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        done_tx.send(loader.check_meta_data_dir("test")).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let blocked = done_rx
        .recv_timeout(std::time::Duration::from_millis(100))
        .is_err();
    drop(restore);
    done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    assert!(blocked);
    journal.shutdown();
}

#[test]
fn snapshot_only_entry_publishes_when_ufs_was_caught_up() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-caught-up-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(123).unwrap();
    let mut batch = JournalBatch::new(1);
    batch.push(JournalEntry::Snapshot(SnapshotEntry {
        op_id: 1,
        rpc_id: 0,
        node_id: loader.node_id,
        dir: checkpoint,
    }));
    let entry = Entry {
        index: 1,
        term: 1,
        data: batch.encode().unwrap(),
        ..Default::default()
    };
    journal
        .append_committed_entry_for_test(entry.clone())
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.apply0(true, &entry, false))
        .unwrap();
    let state = loader.fsm_state_snapshot().unwrap();
    let snapshot = raft::Storage::snapshot(&loader.log_store, 1, 0);
    drop(runtime);
    journal.shutdown();
    assert_eq!(state.applied.index, 1);
    assert_eq!(state.ufs_applied.index, 1);
    assert_eq!(snapshot.unwrap().get_metadata().index, 1);
}

#[test]
fn snapshot_with_ufs_operation_does_not_advance_ufs_cursor() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-mixed-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(123).unwrap();
    let mut batch = JournalBatch::new(1);
    batch.push(JournalEntry::Mkdir(MkdirEntry {
        op_id: 1,
        rpc_id: 0,
        path: "/pending".to_string(),
        dir: crate::master::meta::inode::InodeDir::new(10, 0),
    }));
    batch.push(JournalEntry::Snapshot(SnapshotEntry {
        op_id: 2,
        rpc_id: 0,
        node_id: loader.node_id,
        dir: checkpoint,
    }));
    let entry = Entry {
        index: 1,
        term: 1,
        data: batch.encode().unwrap(),
        ..Default::default()
    };
    journal
        .append_committed_entry_for_test(entry.clone())
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.apply0(true, &entry, false))
        .unwrap();
    let state = loader.fsm_state_snapshot().unwrap();
    let snapshot = raft::Storage::snapshot(&loader.log_store, 1, 0);
    drop(runtime);
    journal.shutdown();
    assert_eq!(state.applied.index, 1);
    assert_eq!(state.ufs_applied.index, 0);
    assert!(snapshot.is_err());
}

#[test]
fn snapshot_only_entry_does_not_skip_earlier_ufs_backlog() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("snapshot-backlog-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(123).unwrap();
    let mut pending = JournalBatch::new(1);
    pending.push(JournalEntry::Mkdir(MkdirEntry {
        op_id: 1,
        rpc_id: 0,
        path: "/pending".to_string(),
        dir: crate::master::meta::inode::InodeDir::new(10, 0),
    }));
    let first = Entry {
        index: 1,
        term: 1,
        data: pending.encode().unwrap(),
        ..Default::default()
    };
    let mut batch = JournalBatch::new(2);
    batch.push(JournalEntry::Snapshot(SnapshotEntry {
        op_id: 2,
        rpc_id: 0,
        node_id: loader.node_id,
        dir: checkpoint,
    }));
    let second = Entry {
        index: 2,
        term: 1,
        data: batch.encode().unwrap(),
        ..Default::default()
    };
    journal
        .append_committed_entry_for_test(first.clone())
        .unwrap();
    journal
        .append_committed_entry_for_test(second.clone())
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(loader.apply0(true, &first, false))
        .unwrap();
    runtime
        .block_on(loader.apply0(true, &second, false))
        .unwrap();
    let state = loader.fsm_state_snapshot().unwrap();
    let snapshot = raft::Storage::snapshot(&loader.log_store, 2, 0);
    drop(runtime);
    journal.shutdown();
    assert_eq!(state.applied.index, 2);
    assert_eq!(state.ufs_applied.index, 0);
    assert!(snapshot.is_err());
}

#[test]
fn leader_replay_updates_ids_after_snapshot_restore() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("leader-replay-restore-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let loader = journal.journal_loader();
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(123).unwrap();
    fs.fs_dir
        .begin_cold(DEFAULT_COLD_TIMEOUT)
        .unwrap()
        .restore(&checkpoint, 0)
        .unwrap();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let entry = JournalEntry::Mkdir(MkdirEntry {
        op_id: 2000,
        rpc_id: 0,
        path: "/restored".to_string(),
        dir: crate::master::meta::inode::InodeDir::new(5000, 0),
    });
    runtime.block_on(loader.replay_entry(true, &entry)).unwrap();
    let older = JournalEntry::Mkdir(MkdirEntry {
        op_id: 10,
        rpc_id: 0,
        path: "/older".to_string(),
        dir: crate::master::meta::inode::InodeDir::new(20, 0),
    });
    runtime.block_on(loader.replay_entry(true, &older)).unwrap();
    let dir = fs.fs_dir.view().unwrap();
    assert!(dir.op_id.get() >= 2000);
    assert!(dir.last_inode_id() >= 5000);
    drop(dir);
    drop(runtime);
    journal.shutdown();
}

#[test]
fn queued_old_promotion_cannot_clear_newer_follower_fence() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("role-queue-generation-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut loader = journal.journal_loader();
    let (sender, receiver) = AsyncChannel::new(4).split();
    loader.sender = sender;
    loader.has_apply_worker = true;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut leader = Box::pin(loader.role_change(StateRole::Leader));
        assert!(futures::poll!(leader.as_mut()).is_pending());
        let mut follower = Box::pin(loader.role_change(StateRole::Follower));
        assert!(futures::poll!(follower.as_mut()).is_pending());
        assert!(fs.mkdir("/before-apply", false).is_err());
        let worker = tokio::spawn(loader.clone().run_apply(receiver));
        assert!(leader.await.is_err());
        follower.await.unwrap();
        assert!(!loader.is_leader.load(Ordering::Acquire));
        assert!(fs.mkdir("/after-follower", false).is_err());
        let (tx, rx) = CallChannel::channel();
        loader.sender.send(ApplyMsg::Shutdown(tx)).await.unwrap();
        rx.receive().await.unwrap();
        worker.await.unwrap();
    });
    journal.shutdown();
}

#[test]
fn queued_follower_does_not_invalidate_newer_promotion() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("role-queue-new-leader-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut loader = journal.journal_loader();
    let (sender, receiver) = AsyncChannel::new(4).split();
    loader.sender = sender;
    loader.has_apply_worker = true;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut follower = Box::pin(loader.role_change(StateRole::Follower));
        assert!(futures::poll!(follower.as_mut()).is_pending());
        let mut leader = Box::pin(loader.role_change(StateRole::Leader));
        assert!(futures::poll!(leader.as_mut()).is_pending());
        let worker = tokio::spawn(loader.clone().run_apply(receiver));
        assert!(follower.await.is_err());
        leader.await.unwrap();
        assert!(loader.is_leader.load(Ordering::Acquire));
        fs.mkdir("/leader-ready", false).unwrap();
        let (tx, rx) = CallChannel::channel();
        loader.sender.send(ApplyMsg::Shutdown(tx)).await.unwrap();
        rx.receive().await.unwrap();
        worker.await.unwrap();
    });
    journal.shutdown();
}

#[test]
fn delayed_old_follower_cannot_demote_newer_leader() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("role-queue-reordered-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut loader = journal.journal_loader();
    let (sender, mut receiver) = AsyncChannel::new(4).split();
    loader.sender = sender;
    loader.has_apply_worker = true;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut follower = Box::pin(loader.role_change(StateRole::Follower));
        assert!(futures::poll!(follower.as_mut()).is_pending());
        let old_follower = receiver.recv().await.unwrap();
        let mut leader = Box::pin(loader.role_change(StateRole::Leader));
        assert!(futures::poll!(leader.as_mut()).is_pending());
        let new_leader = receiver.recv().await.unwrap();
        loader.sender.send(new_leader).await.unwrap();
        loader.sender.send(old_follower).await.unwrap();
        let worker = tokio::spawn(loader.clone().run_apply(receiver));
        leader.await.unwrap();
        assert!(follower.await.is_err());
        assert!(loader.is_leader.load(Ordering::Acquire));
        fs.mkdir("/still-leader", false).unwrap();
        let (tx, rx) = CallChannel::channel();
        loader.sender.send(ApplyMsg::Shutdown(tx)).await.unwrap();
        rx.receive().await.unwrap();
        worker.await.unwrap();
    });
    journal.shutdown();
}

#[test]
fn role_change_fences_before_apply_worker_acknowledges() {
    Master::init_test_metrics();
    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("role-queue-fence-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    let fs = journal.fs();
    let mut loader = journal.journal_loader();
    let (sender, mut receiver) = AsyncChannel::new(1).split();
    loader.sender = sender;
    loader.has_apply_worker = true;
    assert!(loader.journal_writer.is_metadata_current());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        // With the bounded queue full, the first poll must fence before the
        // role-change message can even be enqueued. Cancellation stays fenced.
        let (shutdown_tx, _shutdown_rx) = CallChannel::channel();
        loader
            .sender
            .send(ApplyMsg::Shutdown(shutdown_tx))
            .await
            .unwrap();
        loader.is_leader.store(true, Ordering::Release);
        let mut blocked = Box::pin(loader.role_change(StateRole::Follower));
        assert!(futures::poll!(blocked.as_mut()).is_pending());
        assert!(!loader.is_leader.load(Ordering::Acquire));
        assert!(fs.mkdir("/queue-full", false).is_err());
        drop(blocked);
        assert!(fs.mkdir("/cancelled", false).is_err());
        assert!(matches!(receiver.recv().await, Some(ApplyMsg::Shutdown(_))));
        loader.journal_writer.unfence_metadata();

        let task = tokio::spawn(async move { loader.role_change(StateRole::Follower).await });
        let msg = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("role change not queued")
            .expect("role queue closed");
        assert!(fs.mkdir("/must-not-write", false).is_err());
        assert!(!fs.exists("/must-not-write").unwrap());
        match msg {
            ApplyMsg::RoleChange((StateRole::Follower, _, tx)) => tx.send(Ok(())).unwrap(),
            _ => panic!("unexpected apply message"),
        }
        task.await.unwrap();
    });
    journal.shutdown();
}
