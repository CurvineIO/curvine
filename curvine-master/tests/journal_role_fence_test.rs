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

//! Role-fence regression: a follower must hold namespace writes until it
//! catches up as leader (the read barrier inside `run_metadata_write`),
//! matching Astra's `journal_role_fence_test`.

use curvine_config::ClusterConf;
use curvine_master::master::journal::JournalSystem;
use curvine_master::Master;
use curvine_raft::raft::storage::AppStorage;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use raft::StateRole;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[test]
fn follower_role_holds_metadata_writes_until_leader_is_current() {
    Master::init_test_metrics();

    let mut conf = ClusterConf {
        testing: true,
        ..Default::default()
    };
    conf.change_test_meta_dir("journal-role-fence");

    let journal_system = JournalSystem::from_conf(&conf).unwrap();
    let loader = journal_system.journal_loader();
    let rt = AsyncRuntime::single();
    rt.block_on(AppStorage::role_change(&loader, StateRole::Follower))
        .unwrap();

    let fs = journal_system.fs();
    let verify_fs = fs.clone();
    let verify_fs = fs.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        // mkdir enters run_metadata_write: ensure_metadata_current must reject
        // while the follower barrier is not current.
        finished_tx.send(fs.mkdir("/role-fenced", false)).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let result = finished_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("follower write must be rejected promptly");
    assert!(result.is_err(), "follower must fence namespace writes");
    writer.join().unwrap();

    // Promote: the loader catches up committed metadata and the barrier
    // turns current, releasing the writer.
    rt.block_on(AppStorage::role_change(&loader, StateRole::Leader))
        .unwrap();
    verify_fs.mkdir("/role-fenced", false).unwrap();
    assert!(verify_fs.exists("/role-fenced").unwrap());
}
