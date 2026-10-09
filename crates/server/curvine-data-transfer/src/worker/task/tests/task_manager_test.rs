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

use crate::worker::task::TaskManager;
use curvine_config::ClusterConf;
use curvine_model::WorkerAddress;
use std::sync::Arc;

/// Regression: the worker-embedded transfer client must keep the configured
/// `client.hostname` instead of overriding it to "localhost".
///
/// Forcing "localhost" made `FsContext::is_local_worker` false on every node,
/// silently disabling the short-circuit block write path: each chunk went
/// through the worker RPC port again (a self-loop when the block lived on the
/// node running the task).
#[test]
fn keeps_client_hostname_so_local_writes_work() {
    let mut conf = ClusterConf::default();
    conf.client.hostname = "10.144.226.210".to_string();
    conf.client.short_circuit = true;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let mgr = TaskManager::with_rt(rt, &conf, "session-hostname").unwrap();
    let fs_context = mgr.get_fs_context();

    assert_eq!(fs_context.client_addr_pb().hostname, "10.144.226.210");

    let local = WorkerAddress {
        hostname: "10.144.226.210".to_string(),
        ip_addr: "10.144.226.210".to_string(),
        rpc_port: 8997,
        ..Default::default()
    };
    let remote = WorkerAddress {
        hostname: "10.144.226.211".to_string(),
        ip_addr: "10.144.226.211".to_string(),
        rpc_port: 8997,
        ..Default::default()
    };
    assert!(fs_context.is_local_worker(&local));
    assert!(!fs_context.is_local_worker(&remote));
}
