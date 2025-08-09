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
use curvine_common::proto::raft::SnapshotData;
use curvine_common::raft::storage::{
    AppStorage, HashAppStorage, LogStorage, MemLogStorage, RocksLogStorage,
};
use curvine_common::raft::{RaftClient, RaftJournal, RoleMonitor};
use curvine_common::utils::SerdeUtils;
use orpc::common::{Logger, Utils};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use prost::Message;
use std::sync::Arc;

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
    store_snap.apply_snapshot(&data)?;
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
        RoleMonitor::new(),
        conf.clone(),
    );

    rt.spawn(async move {
        raft.run().await.unwrap();
    });

    Ok(app_store)
}
