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

use curvine_common::conf::JournalConf;
use curvine_common::raft::storage::{LogStorage, RocksAppStorage, RocksLogStorage};
use curvine_common::raft::{RaftClient, RaftJournal, RaftPeer, RoleMonitor};
use curvine_common::utils::SerdeUtils;
use log::info;
use orpc::common::{Logger, Utils};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::CommonResult;
use std::sync::Arc;

fn main() -> CommonResult<()> {
    Logger::default();

    let mut conf = JournalConf::default();
    conf.journal_dir = Utils::test_sub_dir("raft-1");
    conf.rpc_port = 9001;
    conf.journal_addrs = vec![
        RaftPeer::new(9001, &conf.hostname, 9001),
        RaftPeer::new(9002, &conf.hostname, 9002),
    ];

    conf.snapshot_interval = "2s".to_string();

    let rt = conf.create_runtime();

    let log_store = RocksLogStorage::from_conf(&conf, true);
    // let log_store = MemLogStorage::new();
    let store = create_node(log_store, rt.clone(), &conf)?;

    let client = Arc::new(RaftClient::from_conf(rt.clone(), &conf));
    for i in 0..10 {
        let key = format!("k{}", i);
        let value = format!("v{}", i);
        rt.block_on(send_pair(client.clone(), &key, &value))
            .unwrap();
    }

    /*rt.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let res = client.ping(9001).await.unwrap();
            info!("res {:?}", res);
        }
    });
    */

    loop {
        Utils::sleep(10000);
        info!("k9 = {:?}", store.get(&"k9".to_string()));
    }
}

pub async fn send_pair(client: Arc<RaftClient>, key: &str, value: &str) -> CommonResult<()> {
    let msg = SerdeUtils::serialize(&(key.to_string(), value.to_string()))?;
    client.send_propose(msg).await?;
    Ok(())
}

// Create a node.
fn create_node<T>(
    log_store: T,
    rt: Arc<Runtime>,
    conf: &JournalConf,
) -> CommonResult<RocksAppStorage<String, String>>
where
    T: LogStorage + Send + Sync + 'static,
{
    let app_store: RocksAppStorage<String, String> = RocksAppStorage::new("../target/data/raft-1");
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
