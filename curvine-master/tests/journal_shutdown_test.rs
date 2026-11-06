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

use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_raft::raft::storage::AppStorage;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use raft::StateRole;

#[test]
fn shutdown_releases_journal_for_restart() {
    Master::init_test_metrics();
    let mut conf = curvine_config::ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir(format!("journal-shutdown-{}", Utils::rand_str(8)));
    let journal = JournalSystem::from_conf(&conf).unwrap();
    journal.shutdown();
    conf.format_master = false;
    let reopened = JournalSystem::from_conf(&conf).unwrap();
    let loader = reopened.journal_loader();
    let runtime = AsyncRuntime::single();
    runtime
        .block_on(loader.role_change(StateRole::Leader))
        .unwrap();
    drop(loader);
    reopened.shutdown();
    let reopened_again = JournalSystem::from_conf(&conf).unwrap();
    reopened_again.shutdown();
}
