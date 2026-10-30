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

use crate::worker::block::BlockStore;
use crate::worker::Worker;
use curvine_error::FsResult;
use curvine_rpc::server::ServerState;
use curvine_runtime::runtime::{LoopTask, ScheduledExecutor};
use curvine_runtime::sync::StateCtl;
use log::error;

/// Periodic storage health probe.
///
/// Kept off the heartbeat loop on purpose: probes issue filesystem syscalls,
/// and a hung mount must never delay heartbeats (that would turn one bad disk
/// into a lost worker). The heartbeat only reads the resulting snapshot.
pub struct HealthCheckTask {
    pub(crate) store: BlockStore,
    pub(crate) worker_ctl: StateCtl,
}

impl HealthCheckTask {
    pub fn start(worker_ctl: StateCtl, store: BlockStore, interval_ms: u64) -> FsResult<()> {
        let task = Self { store, worker_ctl };
        ScheduledExecutor::new("worker-health-check", interval_ms).start(task)?;
        Ok(())
    }
}

impl LoopTask for HealthCheckTask {
    type Error = curvine_error::FsError;

    fn run(&self) -> FsResult<()> {
        match self.store.probe_storages() {
            Ok(reports) => {
                if let Ok(metrics) = Worker::get_metrics() {
                    for report in &reports {
                        metrics.record_probe(report);
                    }
                }
            }
            Err(e) => error!("storage health probe failed: {}", e),
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        let state: ServerState = self.worker_ctl.state();
        state == ServerState::Stop
    }
}
