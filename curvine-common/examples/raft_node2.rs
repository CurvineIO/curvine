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
use curvine_common::raft::storage::{RocksAppStorage, RocksLogStorage};
use curvine_common::raft::{RaftJournal, RaftPeer, RoleMonitor};
use log::info;
use orpc::common::{Logger, Utils};
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;

// rocksdb storage, snapshot testing.
fn main() -> CommonResult<()> {
    Logger::default();

    let mut conf = JournalConf::default();
    conf.journal_dir = Utils::test_sub_dir("raft-2");
    conf.rpc_port = 9002;

    conf.journal_addrs = vec![
        RaftPeer::new(9001, &conf.hostname, 9001),
        RaftPeer::new(9002, &conf.hostname, 9002),
    ];

    conf.snapshot_interval = "2s".to_string();

    let rt = conf.create_runtime();

    let log_store = RocksLogStorage::from_conf(&conf, true);
    let app_store: RocksAppStorage<String, String> = RocksAppStorage::new("../target/data/raft-2");
    let raft = RaftJournal::new(
        rt.clone(),
        log_store,
        app_store.clone(),
        RoleMonitor::new(),
        conf.clone(),
    );

    rt.spawn(async move {
        raft.run_candidate().await.unwrap();
    });

    loop {
        Utils::sleep(10000);
        info!("k9 = {:?}", app_store.get(&"k9".to_string()));
    }
}
