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

#![allow(unused)]

use curvine_core_error::CommonResult;
use curvine_raft::conf::JournalConf;
use curvine_raft::proto::raft::{FsmState, SnapshotData, SnapshotFileInfo, SnapshotFileList};
use curvine_raft::raft::snapshot::SnapshotFetcher;
use curvine_raft::raft::storage::{
    AppStorage, ApplyMsg, HashAppStorage, LogStorage, MemLogStorage, PeerStorage, RocksAppStorage,
    RocksLogStorage,
};
use curvine_raft::raft::{
    RaftClient, RaftCode, RaftError, RaftGroup, RaftJournal, RaftNode, RaftResult, RoleMonitor,
};
use curvine_raft::utils::SerdeUtils;
use curvine_rpc::client::{ClientConf, RpcClient};
use curvine_rpc::message::{Builder, ResponseStatus};
use curvine_runtime::common::{FileUtils, Logger, Utils};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use prost::bytes::BytesMut;
use prost::Message;
use raft::eraftpb::{
    ConfChange, ConfState, Entry, EntryType, HardState, Message as RaftMessage, MessageType,
    Snapshot,
};
use raft::{Config, RawNode};
use raft::{GetEntriesContext, RaftState, StateRole, Storage, StorageError};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

// Single-node memory storage test.
// #[test]
fn one_node_mem() -> CommonResult<()> {
    Logger::default();

    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();

    let log_store = MemLogStorage::new();
    let _ = create_node(log_store, rt.clone(), &conf)?;

    rt.block_on(send_pair(rt.clone(), &conf, "name", "curvine"))
        .unwrap();

    // loop {
    //     Utils::sleep(10000);
    //     info!("store1-name = {:?}", store.get(&"name".to_string()));
    // }

    Ok(())
}

// rocksdb storage, snapshot testing.
//#[test]
fn rocks_snap_test() -> CommonResult<()> {
    let conf = JournalConf {
        snapshot_interval: "2s".to_string(),
        journal_dir: "../testing/rocks_snap_test".to_string(),
        ..Default::default()
    };

    let rt = conf.create_runtime();

    let log_store = RocksLogStorage::from_conf(&conf, true);
    let core = log_store.clone_store();
    let store = create_node(log_store, rt.clone(), &conf)?;

    for i in 0..10 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        rt.block_on(send_pair(rt.clone(), &conf, &key, &value))?;
    }

    Utils::sleep(20000);
    assert_eq!(store.len(), 10);

    let snap = core.write().unwrap().last_snapshot()?;
    let store_snap: HashAppStorage<String, String> = HashAppStorage::new();
    let data: SnapshotData = SnapshotData::decode(snap.get_data())?;
    rt.block_on(store_snap.apply_snapshot(data))?;
    assert_eq!(store_snap.len(), 10);

    Ok(())
}

async fn send_pair(
    rt: Arc<Runtime>,
    conf: &JournalConf,
    key: &str,
    value: &str,
) -> CommonResult<()> {
    let client = RaftClient::from_conf(rt, conf);
    let msg = SerdeUtils::serialize(&(key.to_string(), value.to_string()))?;
    client.send_propose(msg).await?;
    Ok(())
}

// Create a node.
fn create_node<T>(
    log_store: T,
    rt: Arc<Runtime>,
    conf: &JournalConf,
) -> CommonResult<HashAppStorage<String, String>>
where
    T: LogStorage + Send + Sync + 'static,
{
    let app_store: HashAppStorage<String, String> = HashAppStorage::new();
    let raft = RaftJournal::new(
        rt.clone(),
        log_store,
        app_store.clone(),
        conf.clone(),
        RoleMonitor::new(),
    );

    rt.spawn(async move {
        raft.run().await.unwrap();
    });

    Ok(app_store)
}
#[derive(Clone, Default)]
struct FailingSnapshotAppStorage;

impl AppStorage for FailingSnapshotAppStorage {
    async fn apply(&self, _: bool, _: curvine_raft::raft::storage::ApplyMsg) -> RaftResult<()> {
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        FsmState::default()
    }

    async fn role_change(&self, _: StateRole) -> RaftResult<()> {
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        Ok(SnapshotData::default())
    }

    async fn apply_snapshot(&self, _: SnapshotData) -> RaftResult<()> {
        Err(RaftError::other("injected snapshot restore failure".into()))
    }

    fn snapshot_dir(&self, _: u64) -> RaftResult<String> {
        Ok(String::new())
    }
}

#[derive(Clone, Default)]
struct TestKvAppStorage {
    map: Arc<RwLock<std::collections::HashMap<String, String>>>,
    fsm_state: Arc<Mutex<FsmState>>,
}

impl TestKvAppStorage {
    fn get(&self, key: &str) -> Option<String> {
        self.map.read().unwrap().get(key).cloned()
    }

    fn apply_entry(&self, entry: Entry) -> RaftResult<()> {
        if entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty() {
            let pair: (String, String) = SerdeUtils::deserialize(&entry.data)?;
            self.map.write().unwrap().insert(pair.0, pair.1);
        }
        self.fsm_state.lock().unwrap().applied = curvine_raft::proto::raft::AppliedIndex {
            term: entry.term,
            index: entry.index,
            op_id: 0,
            rpc_id: 0,
        };
        Ok(())
    }
}

impl AppStorage for TestKvAppStorage {
    async fn apply(&self, _: bool, msg: curvine_raft::raft::storage::ApplyMsg) -> RaftResult<()> {
        match msg {
            curvine_raft::raft::storage::ApplyMsg::Entry(entry) => {
                self.apply_entry(entry)?;
            }
            curvine_raft::raft::storage::ApplyMsg::EntryWithAck((entry, tx)) => {
                let result = self.apply_entry(entry);
                let _ = tx.send(result);
            }
            curvine_raft::raft::storage::ApplyMsg::Scan(applied) => {
                self.fsm_state.lock().unwrap().applied = applied;
            }
            _ => {}
        }
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        self.fsm_state.lock().unwrap().clone()
    }

    async fn role_change(&self, _: StateRole) -> RaftResult<()> {
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        Ok(SnapshotData {
            snapshot_id: self.get_fsm_state().applied.index,
            node_id: 0,
            create_time: 0,
            bytes_data: Some(Vec::new()),
            files_data: None,
            fsm_state: self.get_fsm_state(),
        })
    }

    async fn apply_snapshot(&self, _: SnapshotData) -> RaftResult<()> {
        Ok(())
    }

    fn snapshot_dir(&self, _: u64) -> RaftResult<String> {
        Ok(String::new())
    }
}

#[derive(Clone)]
struct BlockingApplyAppStorage {
    map: Arc<RwLock<std::collections::HashMap<String, String>>>,
    fsm_state: Arc<Mutex<FsmState>>,
    started: Arc<AtomicBool>,
    started_notify: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    completed: Arc<AtomicBool>,
}

impl Default for BlockingApplyAppStorage {
    fn default() -> Self {
        Self {
            map: Arc::default(),
            fsm_state: Arc::default(),
            started: Arc::default(),
            started_notify: Arc::default(),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            completed: Arc::default(),
        }
    }
}

impl BlockingApplyAppStorage {
    fn get(&self, key: &str) -> Option<String> {
        self.map.read().unwrap().get(key).cloned()
    }

    async fn apply_entry(&self, entry: Entry, wait: bool) -> RaftResult<()> {
        if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
            self.fsm_state.lock().unwrap().applied = curvine_raft::proto::raft::AppliedIndex {
                term: entry.term,
                index: entry.index,
                op_id: 0,
                rpc_id: 0,
            };
            return Ok(());
        }

        let pair: (String, String) = SerdeUtils::deserialize(&entry.data)?;
        self.started.store(true, Ordering::SeqCst);
        self.started_notify.notify_one();
        if wait {
            self.release
                .clone()
                .acquire_owned()
                .await
                .expect("test semaphore is never closed");
        }
        self.map.write().unwrap().insert(pair.0, pair.1);
        self.fsm_state.lock().unwrap().applied = curvine_raft::proto::raft::AppliedIndex {
            term: entry.term,
            index: entry.index,
            op_id: 0,
            rpc_id: 0,
        };
        Ok(())
    }

    async fn wait_started(&self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started_notify.notified().await;
    }

    fn release_apply(&self) {
        self.release.add_permits(1);
    }

    fn proposal_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

impl AppStorage for BlockingApplyAppStorage {
    async fn apply(
        &self,
        wait: bool,
        msg: curvine_raft::raft::storage::ApplyMsg,
    ) -> RaftResult<()> {
        match msg {
            curvine_raft::raft::storage::ApplyMsg::Entry(entry) => {
                self.apply_entry(entry, wait).await?;
            }
            curvine_raft::raft::storage::ApplyMsg::EntryWithAck((entry, tx)) => {
                let result = self.apply_entry(entry, true).await;
                let _ = tx.send(result);
            }
            curvine_raft::raft::storage::ApplyMsg::Scan(applied) => {
                self.fsm_state.lock().unwrap().applied = applied;
            }
            _ => {}
        }
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        self.fsm_state.lock().unwrap().clone()
    }

    async fn role_change(&self, _: StateRole) -> RaftResult<()> {
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        Ok(SnapshotData {
            snapshot_id: self.get_fsm_state().applied.index,
            node_id: 0,
            create_time: 0,
            bytes_data: Some(Vec::new()),
            files_data: None,
            fsm_state: self.get_fsm_state(),
        })
    }

    async fn apply_snapshot(&self, _: SnapshotData) -> RaftResult<()> {
        Ok(())
    }

    fn snapshot_dir(&self, _: u64) -> RaftResult<String> {
        Ok(String::new())
    }
}

#[derive(Clone, Default)]
struct NoSnapshotLogStorage;

impl LogStorage for NoSnapshotLogStorage {
    fn append(&self, _: &[Entry]) -> RaftResult<()> {
        Ok(())
    }

    fn scan_entries(&self, _: u64, _: u64) -> RaftResult<Vec<Entry>> {
        Ok(vec![])
    }

    fn set_hard_state(&self, _: &HardState) -> RaftResult<()> {
        Ok(())
    }

    fn set_hard_state_commit(&self, commit: u64) -> RaftResult<HardState> {
        Ok(HardState {
            commit,
            ..Default::default()
        })
    }

    fn set_conf_state(&self, _: &ConfState) -> RaftResult<()> {
        Ok(())
    }

    fn create_snapshot(&self, _: SnapshotData) -> RaftResult<()> {
        Ok(())
    }

    fn apply_snapshot(&self, _: Snapshot) -> RaftResult<()> {
        Ok(())
    }

    fn compact(&self, _: u64) -> RaftResult<()> {
        Ok(())
    }
}

impl Storage for NoSnapshotLogStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::default())
    }

    fn entries(
        &self,
        _: u64,
        _: u64,
        _: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        Ok(vec![])
    }

    fn term(&self, _: u64) -> raft::Result<u64> {
        Ok(0)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(0)
    }

    fn snapshot(&self, _: u64, _: u64) -> raft::Result<Snapshot> {
        Err(raft::Error::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

#[test]
fn malformed_propose_request_does_not_stop_raft_node() -> CommonResult<()> {
    Logger::default();

    let mut conf = JournalConf::with_test();
    conf.journal_dir = format!("../testing/malformed-propose-{}", Utils::rand_id());
    FileUtils::delete_path(&conf.journal_dir, true)?;

    let rt = conf.create_runtime();
    let store = TestKvAppStorage::default();
    let raft = RaftJournal::new(
        rt.clone(),
        RocksLogStorage::from_conf(&conf, true),
        store.clone(),
        conf.clone(),
        RoleMonitor::new(),
    );
    let mut listener = rt.block_on(raft.run())?;
    rt.block_on(listener.wait_leader())?;

    let client = RaftClient::from_conf(rt.clone(), &conf);
    let malformed_header = SerdeUtils::serialize(&("bad".to_string(), "payload".to_string()))?;
    let malformed_req = Builder::new_rpc(RaftCode::Propose)
        .header(BytesMut::from(&malformed_header[..]))
        .build();
    let raw_client = rt.block_on(RpcClient::new(
        false,
        rt.clone(),
        &conf.local_addr(),
        &ClientConf::default(),
    ))?;
    let malformed_rep = rt.block_on(raw_client.rpc(malformed_req))?;
    assert_eq!(malformed_rep.response_status(), ResponseStatus::Error);

    rt.block_on(send_pair(rt.clone(), &conf, "name", "curvine"))?;
    Utils::sleep(1000);
    assert_eq!(store.get("name"), Some("curvine".to_string()));
    FileUtils::delete_path(&conf.journal_dir, true)?;

    Ok(())
}

#[test]
fn committed_leader_noop_advances_app_storage_applied_index() -> CommonResult<()> {
    Logger::default();

    let mut conf = JournalConf::with_test();
    conf.journal_dir = format!("../testing/leader-noop-{}", Utils::rand_id());
    FileUtils::delete_path(&conf.journal_dir, true)?;

    let rt = conf.create_runtime();
    let store = TestKvAppStorage::default();
    let raft = RaftJournal::new(
        rt.clone(),
        RocksLogStorage::from_conf(&conf, true),
        store.clone(),
        conf.clone(),
        RoleMonitor::new(),
    );
    let mut listener = rt.block_on(raft.run())?;
    rt.block_on(listener.wait_leader())?;

    rt.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while store.get_fsm_state().applied.index == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("committed leader no-op should reach app storage");
    });

    FileUtils::delete_path(&conf.journal_dir, true)?;
    Ok(())
}

/// AppStorage that already has local applied state and refuses empty snapshot installs.
#[derive(Clone)]
struct PopulatedRefuseEmptySnapshotAppStorage {
    fsm_state: Arc<Mutex<FsmState>>,
    apply_snapshot_calls: Arc<Mutex<u32>>,
    snapshot_root: Option<String>,
}

impl PopulatedRefuseEmptySnapshotAppStorage {
    fn with_applied_index(index: u64) -> Self {
        let mut fsm = FsmState::default();
        fsm.applied.index = index;
        Self {
            fsm_state: Arc::new(Mutex::new(fsm)),
            apply_snapshot_calls: Arc::new(Mutex::new(0)),
            snapshot_root: None,
        }
    }

    // A store that owns a snapshot directory can receive a private copy of a
    // snapshot another member created; without one it can only download.
    fn with_snapshot_root(mut self, root: impl Into<String>) -> Self {
        self.snapshot_root = Some(root.into());
        self
    }
}

impl AppStorage for PopulatedRefuseEmptySnapshotAppStorage {
    async fn apply(&self, _: bool, msg: curvine_raft::raft::storage::ApplyMsg) -> RaftResult<()> {
        if let curvine_raft::raft::storage::ApplyMsg::Scan(applied) = msg {
            self.fsm_state.lock().unwrap().applied = applied;
        }
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        self.fsm_state.lock().unwrap().clone()
    }

    async fn role_change(&self, _: StateRole) -> RaftResult<()> {
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        Ok(SnapshotData::default())
    }

    async fn apply_snapshot(&self, _: SnapshotData) -> RaftResult<()> {
        *self.apply_snapshot_calls.lock().unwrap() += 1;
        Err(RaftError::other(
            "populated app store must not install empty snapshot".into(),
        ))
    }

    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String> {
        Ok(match &self.snapshot_root {
            Some(root) => format!("{root}/ck-{snapshot_id}"),
            None => String::new(),
        })
    }
}

#[test]
fn install_snapshot_skips_empty_app_snapshot_when_applied_index_nonzero() -> CommonResult<()> {
    // Focused coverage for the step-down path: LogStorage has no persisted
    // snapshot, but AppStorage already has applied.index > 0. install_snapshot
    // must skip app_store.apply_snapshot(empty) so local metadata is preserved.
    let rt = JournalConf::with_test().create_runtime();
    let conf = JournalConf::with_test();
    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(7);
    let group = RaftGroup::from_conf(&conf);
    let client = RaftClient::new(rt.clone(), &group, conf.new_client_conf());
    let storage = PeerStorage::new(
        rt.clone(),
        1,
        NoSnapshotLogStorage,
        app.clone(),
        client,
        &conf,
    );

    let index = rt.block_on(RaftNode::<
        NoSnapshotLogStorage,
        PopulatedRefuseEmptySnapshotAppStorage,
    >::install_snapshot(&storage, vec![1]))?;

    assert_eq!(index, 7);
    assert_eq!(
        *app.apply_snapshot_calls.lock().unwrap(),
        0,
        "empty app snapshot must not be applied when applied.index > 0"
    );
    assert_eq!(app.get_fsm_state().applied.index, 7);

    Ok(())
}

#[test]
fn propose_response_waits_until_committed_entry_is_applied() -> CommonResult<()> {
    Logger::default();

    let mut conf = JournalConf::with_test();
    conf.journal_dir = format!("../testing/propose-apply-{}", Utils::rand_id());
    FileUtils::delete_path(&conf.journal_dir, true)?;

    let rt = conf.create_runtime();
    let store = BlockingApplyAppStorage::default();
    let raft = RaftJournal::new(
        rt.clone(),
        RocksLogStorage::from_conf(&conf, true),
        store.clone(),
        conf.clone(),
        RoleMonitor::new(),
    );
    let mut listener = rt.block_on(raft.run())?;
    rt.block_on(listener.wait_leader())?;

    let client = RaftClient::from_conf(rt.clone(), &conf);
    let msg = SerdeUtils::serialize(&("name".to_string(), "curvine".to_string()))?;
    let completed = store.completed.clone();
    let handle = rt.spawn(async move {
        let result = client.send_propose_response(msg).await;
        completed.store(true, Ordering::SeqCst);
        result
    });

    rt.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3), store.wait_started())
            .await
            .expect("committed entry should reach app storage apply");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    assert!(
        !store.proposal_completed(),
        "propose RPC returned before the committed entry finished app storage apply"
    );
    assert_eq!(store.get("name"), None);

    store.release_apply();
    let response = rt.block_on(handle).unwrap()?;
    assert!(response.applied_index.unwrap_or_default() > 0);
    assert_eq!(store.get("name"), Some("curvine".to_string()));
    FileUtils::delete_path(&conf.journal_dir, true)?;

    Ok(())
}

#[test]
fn kv_app_storages_accept_non_data_entries() -> CommonResult<()> {
    let rt = AsyncRuntime::single();
    let applied = curvine_raft::proto::raft::AppliedIndex {
        term: 1,
        index: 1,
        ..Default::default()
    };

    let hash: HashAppStorage<String, String> = HashAppStorage::new();
    rt.block_on(hash.apply(true, ApplyMsg::new_scan(applied.clone())))?;
    rt.block_on(hash.apply(
        true,
        ApplyMsg::new_entry(Entry {
            term: 1,
            index: 2,
            ..Default::default()
        }),
    ))?;
    rt.block_on(hash.apply(
        true,
        ApplyMsg::new_entry(Entry {
            term: 1,
            index: 3,
            entry_type: EntryType::EntryConfChange as i32,
            data: ConfChange::default().encode_to_vec(),
            ..Default::default()
        }),
    ))?;
    assert_eq!(hash.get_fsm_state().applied.index, 3);

    let dir = Utils::test_sub_dir(format!("rocks-app-storage-scan-{}", Utils::rand_id()));
    FileUtils::delete_path(&dir, true)?;
    let rocks: RocksAppStorage<String, String> = RocksAppStorage::new(&dir);
    rt.block_on(rocks.apply(true, ApplyMsg::new_scan(applied)))?;
    rt.block_on(rocks.apply(
        true,
        ApplyMsg::new_entry(Entry {
            term: 1,
            index: 2,
            ..Default::default()
        }),
    ))?;
    rt.block_on(rocks.apply(
        true,
        ApplyMsg::new_entry(Entry {
            term: 1,
            index: 3,
            entry_type: EntryType::EntryConfChange as i32,
            data: ConfChange::default().encode_to_vec(),
            ..Default::default()
        }),
    ))?;
    assert_eq!(rocks.get_fsm_state().applied.index, 3);
    drop(rocks);
    FileUtils::delete_path(&dir, true)?;

    Ok(())
}

#[test]
fn run_candidate_returns_snapshot_restore_error_without_panicking() -> CommonResult<()> {
    Logger::default();

    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let raft = RaftJournal::new(
        rt.clone(),
        NoSnapshotLogStorage,
        FailingSnapshotAppStorage,
        conf,
        RoleMonitor::new(),
    );

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(raft.run_candidate())
    }));

    let err = match result.expect("run_candidate should return an error instead of panicking") {
        Ok(_) => panic!("snapshot restore failure should be returned to the caller"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("injected snapshot restore failure"),
        "unexpected error: {err}"
    );

    Ok(())
}

#[test]
fn empty_static_voter_panics_on_leader_commit_above_local_last_index() -> CommonResult<()> {
    // Characterization test for raft-rs: a heartbeat that advances commit past
    // last_index must panic. Use NoSnapshotLogStorage directly — wrapping it in
    // PeerStorage + RaftClient + Runtime previously left executor threads alive
    // after catch_unwind and hung under nextest until SIGKILL (~25min).
    let config = Config {
        id: 3,
        applied: 0,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let mut node = RawNode::new(&config, NoSnapshotLogStorage, &logger)?;

    let mut heartbeat = RaftMessage::default();
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    heartbeat.from = 1;
    heartbeat.to = 3;
    heartbeat.term = 2;
    heartbeat.commit = 10;

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| node.step(heartbeat)));
    assert!(
        result.is_err(),
        "raft-rs must reject commit 10 when the local voter has last_index 0"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Snapshot reporting (follower -> leader) and locality-aware fetch on restart.
// ---------------------------------------------------------------------------

/// Build a reported snapshot owned by `node_id` at applied `index`.
fn reported_snapshot(index: u64, node_id: u64) -> SnapshotData {
    let mut fsm = FsmState::default();
    fsm.applied.index = index;
    SnapshotData {
        snapshot_id: index,
        node_id,
        fsm_state: fsm,
        ..Default::default()
    }
}

fn remote_files() -> SnapshotFileList {
    SnapshotFileList {
        dir: "/remote/snapshot".to_string(),
        files: vec![SnapshotFileInfo {
            path: "checkpoint.db".to_string(),
            mtime: 0,
            ctime: 0,
            len: 16,
        }],
    }
}

/// The same manifest as `remote_files`, but rooted at `dir` — mirrors the
/// `SnapshotData.files_data.dir` a snapshot records at creation.
fn files_in_dir(dir: &str) -> SnapshotFileList {
    SnapshotFileList {
        dir: dir.to_string(),
        files: remote_files().files,
    }
}

/// The leader registers the first follower report and remembers the owner so a
/// later restart knows the snapshot files live on another member.
#[test]
fn register_snapshot_accepts_first_report_and_records_owner() {
    let log = MemLogStorage::new();

    assert!(log.register_snapshot(reported_snapshot(5, 2)).unwrap());

    let latest = log
        .latest_snapshot()
        .unwrap()
        .expect("reported snapshot must be registered");
    assert_eq!(latest.get_metadata().index, 5);
    let data = SnapshotData::decode(latest.get_data()).unwrap();
    assert_eq!(
        data.node_id, 2,
        "leader must remember the reporting follower"
    );
}

/// A stale or equal-index report must never overwrite the registered snapshot.
#[test]
fn register_snapshot_rejects_stale_or_equal_index() {
    let log = MemLogStorage::new();
    assert!(log.register_snapshot(reported_snapshot(5, 2)).unwrap());

    // Older snapshot from another follower.
    assert!(!log.register_snapshot(reported_snapshot(3, 3)).unwrap());
    // Same index is not newer, so it must not replace the current owner.
    assert!(!log.register_snapshot(reported_snapshot(5, 3)).unwrap());

    let latest = log.latest_snapshot().unwrap().unwrap();
    assert_eq!(latest.get_metadata().index, 5);
    assert_eq!(
        SnapshotData::decode(latest.get_data()).unwrap().node_id,
        2,
        "stale reports must not change the registered owner"
    );
}

/// A strictly newer report replaces the registered snapshot and its owner.
#[test]
fn register_snapshot_accepts_newer_index() {
    let log = MemLogStorage::new();
    assert!(log.register_snapshot(reported_snapshot(5, 2)).unwrap());

    assert!(log.register_snapshot(reported_snapshot(7, 3)).unwrap());

    let latest = log.latest_snapshot().unwrap().unwrap();
    assert_eq!(latest.get_metadata().index, 7);
    assert_eq!(SnapshotData::decode(latest.get_data()).unwrap().node_id, 3);
}

fn test_client(conf: &JournalConf, rt: Arc<Runtime>) -> RaftClient {
    let group = RaftGroup::from_conf(conf);
    RaftClient::new(rt, &group, conf.new_client_conf())
}

/// A snapshot this member owns is reused without any download, but only while
/// its files still exist: a wiped local data dir must fail instead of reuse.
#[test]
fn snapshot_fetcher_treats_matching_node_as_local() -> CommonResult<()> {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let dir = Utils::test_sub_dir(format!("snapshot-owned-{}", Utils::rand_id()));
    FileUtils::delete_path(&dir, true)?;
    FileUtils::create_dir(&dir, true)?;
    std::fs::write(Path::new(&dir).join("checkpoint.db"), vec![0u8; 16])?;

    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0);
    let mut data = reported_snapshot(5, 7);
    data.files_data = Some(files_in_dir(&dir));

    let fetcher =
        SnapshotFetcher::new(7, &app, &conf, test_client(&conf, rt.clone()), data.clone()).unwrap();

    assert!(
        matches!(fetcher, SnapshotFetcher::Local(_)),
        "snapshot owned by this member must not be downloaded"
    );

    // The leader keeps this member registered as the owner, so the same
    // snapshot comes back after the files are gone.
    FileUtils::delete_path(&dir, true)?;
    let err = SnapshotFetcher::new(7, &app, &conf, test_client(&conf, rt.clone()), data)
        .err()
        .expect("an owned snapshot whose files are gone must not be reused");
    assert!(
        err.to_string().contains(&dir),
        "the error must name the missing directory: {err}"
    );
    Ok(())
}

/// A remote snapshot without a file list cannot be downloaded and must error.
#[test]
fn snapshot_fetcher_requires_files_for_remote_snapshot() {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0);
    let data = reported_snapshot(5, 7); // files_data is None

    let result = SnapshotFetcher::new(9, &app, &conf, test_client(&conf, rt.clone()), data);
    let err = result
        .err()
        .expect("remote snapshot without files must fail to build a fetcher");

    assert!(
        err.to_string().contains("Not found snapshot data"),
        "unexpected error: {err}"
    );
}

/// A remote snapshot with a file list prepares a download instead of a local read.
#[test]
fn snapshot_fetcher_builds_download_for_remote_snapshot() {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0);
    let mut data = reported_snapshot(5, 7);
    data.files_data = Some(remote_files());

    let fetcher =
        SnapshotFetcher::new(9, &app, &conf, test_client(&conf, rt.clone()), data).unwrap();

    assert!(
        matches!(fetcher, SnapshotFetcher::Remote(_, _)),
        "snapshot owned by another member must be downloaded"
    );
}

/// A complete local copy of another member's snapshot is copied into this
/// member's own snapshot directory: a recovering member must install a snapshot
/// it owns, and must not re-download files that are already readable here.
#[test]
fn snapshot_fetcher_owns_a_local_copy_of_remote_snapshot() -> CommonResult<()> {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let source = Utils::test_sub_dir(format!("snapshot-local-copy-{}", Utils::rand_id()));
    let root = Utils::test_sub_dir(format!("snapshot-own-root-{}", Utils::rand_id()));
    FileUtils::delete_path(&source, true)?;
    FileUtils::create_dir(&source, true)?;
    FileUtils::delete_path(&root, true)?;
    FileUtils::create_dir(&root, true)?;
    std::fs::write(Path::new(&source).join("checkpoint.db"), vec![7u8; 16])?;

    let app =
        PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0).with_snapshot_root(&root);
    let mut data = reported_snapshot(5, 7);
    data.files_data = Some(files_in_dir(&source));

    let fetcher =
        SnapshotFetcher::new(9, &app, &conf, test_client(&conf, rt.clone()), data).unwrap();

    let dir = match &fetcher {
        SnapshotFetcher::OwnedCopy(_, dir) => dir.clone(),
        SnapshotFetcher::Local(_) => {
            panic!("a snapshot owned by another member must not be treated as local")
        }
        SnapshotFetcher::Remote(..) => {
            panic!("a complete local copy must not be downloaded")
        }
    };
    assert_ne!(
        dir, source,
        "the directory of another member must not be adopted"
    );
    assert!(
        dir.starts_with(&root),
        "the copy must live in this member's own storage: {dir}"
    );

    let fetched = rt.block_on(fetcher.fetch())?;
    let files = fetched
        .files_data
        .expect("a copied snapshot must keep its file manifest");
    assert_eq!(
        files.dir, dir,
        "the installed snapshot must name the copy this member owns"
    );
    assert_eq!(
        std::fs::read(Path::new(&files.dir).join("checkpoint.db"))?,
        vec![7u8; 16],
        "the copied file must carry the manifest content"
    );

    FileUtils::delete_path(&source, true)?;
    FileUtils::delete_path(&root, true)?;
    Ok(())
}

/// A store that cannot name a snapshot directory cannot own a copy, so building
/// the fetcher fails instead of silently adopting another member's directory.
#[test]
fn snapshot_fetcher_errors_without_a_private_snapshot_dir() -> CommonResult<()> {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let source = Utils::test_sub_dir(format!("snapshot-no-own-root-{}", Utils::rand_id()));
    FileUtils::delete_path(&source, true)?;
    FileUtils::create_dir(&source, true)?;
    std::fs::write(Path::new(&source).join("checkpoint.db"), vec![0u8; 16])?;

    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0);
    let mut data = reported_snapshot(5, 7);
    data.files_data = Some(files_in_dir(&source));

    let err = SnapshotFetcher::new(9, &app, &conf, test_client(&conf, rt.clone()), data)
        .err()
        .expect("a store without a snapshot directory must not own a copy");
    assert!(
        err.to_string().contains("empty snapshot directory"),
        "unexpected error: {err}"
    );

    FileUtils::delete_path(&source, true)?;
    Ok(())
}

/// A local copy with a wrong file length is incomplete and must fall back to
/// the download path.
#[test]
fn snapshot_fetcher_falls_back_on_local_length_mismatch() -> CommonResult<()> {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let dir = Utils::test_sub_dir(format!("snapshot-short-copy-{}", Utils::rand_id()));
    FileUtils::delete_path(&dir, true)?;
    FileUtils::create_dir(&dir, true)?;
    std::fs::write(Path::new(&dir).join("checkpoint.db"), vec![0u8; 15])?;

    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0);
    let mut data = reported_snapshot(5, 7);
    data.files_data = Some(files_in_dir(&dir));

    let fetcher =
        SnapshotFetcher::new(9, &app, &conf, test_client(&conf, rt.clone()), data).unwrap();

    assert!(
        matches!(fetcher, SnapshotFetcher::Remote(_, _)),
        "truncated local copy must fall back to download"
    );

    FileUtils::delete_path(&dir, true)?;
    Ok(())
}

/// A local directory without the manifest files is not a usable copy and must
/// fall back to the download path.
#[test]
fn snapshot_fetcher_falls_back_on_missing_local_files() -> CommonResult<()> {
    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let dir = Utils::test_sub_dir(format!("snapshot-missing-copy-{}", Utils::rand_id()));
    FileUtils::delete_path(&dir, true)?;
    FileUtils::create_dir(&dir, true)?;

    let app = PopulatedRefuseEmptySnapshotAppStorage::with_applied_index(0);
    let mut data = reported_snapshot(5, 7);
    data.files_data = Some(files_in_dir(&dir));

    let fetcher =
        SnapshotFetcher::new(9, &app, &conf, test_client(&conf, rt.clone()), data).unwrap();

    assert!(
        matches!(fetcher, SnapshotFetcher::Remote(_, _)),
        "missing local copy must fall back to download"
    );

    FileUtils::delete_path(&dir, true)?;
    Ok(())
}

/// AppStorage that records every snapshot applied to it and reports a fixed
/// applied index from `create_snapshot`. It deliberately stamps `node_id: 0` so
/// tests can prove the raft layer overwrites it with the real raft node id.
#[derive(Clone)]
struct RecordingSnapshotAppStorage {
    fsm_state: Arc<Mutex<FsmState>>,
    applied_snapshots: Arc<Mutex<Vec<SnapshotData>>>,
}

impl RecordingSnapshotAppStorage {
    fn with_applied_index(index: u64) -> Self {
        let mut fsm = FsmState::default();
        fsm.applied.index = index;
        Self {
            fsm_state: Arc::new(Mutex::new(fsm)),
            applied_snapshots: Arc::new(Mutex::new(vec![])),
        }
    }

    fn applied_snapshots(&self) -> Vec<SnapshotData> {
        self.applied_snapshots.lock().unwrap().clone()
    }
}

impl AppStorage for RecordingSnapshotAppStorage {
    async fn apply(&self, _: bool, msg: ApplyMsg) -> RaftResult<()> {
        if let ApplyMsg::Scan(applied) = msg {
            self.fsm_state.lock().unwrap().applied = applied;
        }
        Ok(())
    }

    fn get_fsm_state(&self) -> FsmState {
        self.fsm_state.lock().unwrap().clone()
    }

    async fn role_change(&self, _: StateRole) -> RaftResult<()> {
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<SnapshotData> {
        // node_id is intentionally wrong (0); the raft layer must fix it.
        Ok(SnapshotData {
            snapshot_id: self.get_fsm_state().applied.index,
            node_id: 0,
            create_time: 0,
            bytes_data: Some(Vec::new()),
            files_data: None,
            fsm_state: self.get_fsm_state(),
        })
    }

    async fn apply_snapshot(&self, data: SnapshotData) -> RaftResult<()> {
        *self.fsm_state.lock().unwrap() = data.fsm_state.clone();
        self.applied_snapshots.lock().unwrap().push(data);
        Ok(())
    }

    fn snapshot_dir(&self, _: u64) -> RaftResult<String> {
        Ok(String::new())
    }
}

/// An empty group makes `report_snapshot` fail fast (no nodes to contact), so a
/// create-snapshot job finishes without blocking on RPC retries during tests.
fn empty_group_client(rt: Arc<Runtime>, conf: &JournalConf, name: &str) -> RaftClient {
    let group = RaftGroup::new(name, std::collections::HashMap::new());
    RaftClient::new(rt, &group, conf.new_client_conf())
}

/// #5: `gen_create_snapshot_job` must overwrite the app-store `node_id` with the
/// raft node id, so the creator later recognizes its own snapshot as local
/// instead of re-downloading it, and the leader records the true owner.
#[test]
fn create_snapshot_job_stamps_raft_node_id() -> CommonResult<()> {
    Logger::default();

    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let log = MemLogStorage::new();
    let app = RecordingSnapshotAppStorage::with_applied_index(5);
    let client = empty_group_client(rt.clone(), &conf, "stamp-node-id");

    let storage = PeerStorage::new(rt.clone(), 42, log.clone(), app.clone(), client, &conf);
    storage.gen_create_snapshot_job()?;

    // The job runs on the executor; poll until it persists the snapshot.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let stamped = loop {
        if let Ok(Some(snap)) = log.latest_snapshot() {
            if let Ok(data) = SnapshotData::decode(snap.get_data()) {
                if data.node_id == 42 {
                    break data;
                }
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("create snapshot job did not stamp the raft node id");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };

    assert_eq!(
        stamped.node_id, 42,
        "persisted snapshot must be owned by the raft node, not the app store default"
    );
    assert_eq!(stamped.fsm_state.applied.index, 5);
    Ok(())
}

/// #1/#3b: on restart, a persisted snapshot this member owns is re-fetched
/// locally (no download), stamped with the local node id, and re-applied to the
/// app store, advancing the applied index. Covers the `Some(snapshot)` branch of
/// `install_snapshot`, which the empty-snapshot test does not exercise.
#[test]
fn install_snapshot_reapplies_local_snapshot() -> CommonResult<()> {
    Logger::default();

    let conf = JournalConf::with_test();
    let rt = conf.create_runtime();
    let log = MemLogStorage::new();
    // Register a snapshot owned by node 1 at applied index 9.
    assert!(log.register_snapshot(reported_snapshot(9, 1))?);

    let app = RecordingSnapshotAppStorage::with_applied_index(0);
    let client = empty_group_client(rt.clone(), &conf, "install-local");
    let storage = PeerStorage::new(rt.clone(), 1, log.clone(), app.clone(), client, &conf);

    let index = rt.block_on(
        RaftNode::<MemLogStorage, RecordingSnapshotAppStorage>::install_snapshot(&storage, vec![1]),
    )?;

    assert_eq!(
        index, 9,
        "install_snapshot must restore the persisted applied index"
    );
    let applied = app.applied_snapshots();
    assert_eq!(
        applied.len(),
        1,
        "local snapshot must be applied exactly once"
    );
    assert_eq!(
        applied[0].node_id, 1,
        "applied snapshot must be stamped with the local node id"
    );
    Ok(())
}
