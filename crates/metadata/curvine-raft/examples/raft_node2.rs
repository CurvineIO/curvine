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

use curvine_core_error::CommonResult;
use curvine_raft::conf::JournalConf;
use curvine_raft::raft::storage::{RocksAppStorage, RocksLogStorage};
use curvine_raft::raft::{RaftJournal, RaftPeer, RoleMonitor};
use curvine_runtime::common::{Logger, Utils};
use curvine_runtime::runtime::RpcRuntime;
use log::info;

// rocksdb storage, snapshot testing.
fn main() -> CommonResult<()> {
    Logger::default();

    let default = JournalConf::default();
    let conf = JournalConf {
        journal_dir: Utils::test_sub_dir("raft-2"),
        rpc_port: 9002,
        snapshot_rpc_port: 9004,
        journal_addrs: vec![
            RaftPeer::new(9001, &default.hostname, 9001).with_snapshot_port(9003),
            RaftPeer::new(9002, &default.hostname, 9002).with_snapshot_port(9004),
        ],
        snapshot_interval: "2s".to_string(),
        ..default
    };

    let rt = conf.create_runtime();

    let log_store = RocksLogStorage::from_conf(&conf, true);
    let app_store: RocksAppStorage<String, String> = RocksAppStorage::new("../target/data/raft-2");
    let raft = RaftJournal::new(
        rt.clone(),
        log_store,
        app_store.clone(),
        conf.clone(),
        RoleMonitor::new(),
    );

    rt.spawn(async move {
        raft.run_candidate().await.unwrap();
    });

    loop {
        Utils::sleep(10000);
        info!("k9 = {:?}", app_store.get(&"k9".to_string()));
    }
}
