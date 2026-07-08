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

use curvine_common::conf::ClusterConf;
use curvine_common::state::{WorkerInfo, WorkerStatus};
use curvine_server::master::fs::WorkerManager;

#[test]
fn add_blacklist_worker_is_idempotent() {
    let conf = ClusterConf::default();
    let mut manager = WorkerManager::new(&conf).unwrap();
    let worker = WorkerInfo::default();
    let worker_id = worker.worker_id();
    manager.add_test_worker(worker);

    let first = manager.add_blacklist_worker(worker_id);
    assert!(matches!(
        first.as_ref().map(|worker| worker.status),
        Some(WorkerStatus::Blacklist)
    ));

    let second = manager.add_blacklist_worker(worker_id);
    assert!(
        second.is_none(),
        "blacklisting the same worker twice should not report a new transition"
    );
}
