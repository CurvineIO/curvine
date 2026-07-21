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

use curvine_common::conf::JournalConf;
use curvine_common::proto::raft::{FsmState, SnapshotData};
use curvine_common::raft::storage::{
    AppStorage, HashAppStorage, LogStorage, MemLogStorage, RocksLogStorage,
};
use curvine_common::raft::{RaftClient, RaftCode, RaftError, RaftJournal, RaftResult, RoleMonitor};
use curvine_common::utils::SerdeUtils;
use orpc::client::{ClientConf, RpcClient};
use orpc::common::{FileUtils, Logger, Utils};
use orpc::message::{Builder, ResponseStatus};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use prost::bytes::BytesMut;
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Message as RaftMessage, MessageType, Snapshot};
use raft::{Config, RawNode};
use raft::{GetEntriesContext, RaftState, StateRole, Storage, StorageError};
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
    async fn apply(&self, _: bool, _: curvine_common::raft::storage::ApplyMsg) -> RaftResult<()> {
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
}

impl AppStorage for TestKvAppStorage {
    async fn apply(&self, _: bool, msg: curvine_common::raft::storage::ApplyMsg) -> RaftResult<()> {
        match msg {
            curvine_common::raft::storage::ApplyMsg::Entry(entry) => {
                let pair: (String, String) = SerdeUtils::deserialize(&entry.data)?;
                self.map.write().unwrap().insert(pair.0, pair.1);
                self.fsm_state.lock().unwrap().applied =
                    curvine_common::proto::raft::AppliedIndex {
                        term: entry.term,
                        index: entry.index,
                        op_id: 0,
                        rpc_id: 0,
                    };
            }
            curvine_common::raft::storage::ApplyMsg::Scan(applied) => {
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

    fn set_hard_state_commit(&self, _: u64) -> RaftResult<()> {
        Ok(())
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
