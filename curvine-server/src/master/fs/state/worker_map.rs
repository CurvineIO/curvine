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

use curvine_common::state::{StorageInfo, WorkerAddress, WorkerInfo, WorkerStatus};
use curvine_common::FsResult;
use indexmap::IndexMap;
use log::{error, info, warn};
use orpc::err_box;

// Store all worker information.
pub struct WorkerMap {
    pub(crate) workers: IndexMap<u32, WorkerInfo>,
    pub(crate) lost_workers: IndexMap<u32, WorkerInfo>,
}

impl Default for WorkerMap {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerMap {
    pub fn new() -> Self {
        Self {
            workers: IndexMap::new(),
            lost_workers: IndexMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        addr: WorkerAddress,
        storages: Vec<StorageInfo>,
        version: curvine_common::version::Version,
    ) -> FsResult<()> {
        // Check whether the address and worker id conflict.
        if let Some(v) = self.workers.get(&addr.worker_id) {
            if v.address != addr {
                return err_box!(
                    "worker id addr mismatch,  expected {}, actual: {}",
                    v.address,
                    addr
                );
            }
        }

        // Register the worker and update the storage information.
        // @todo Heartbeat may be too simple and directly covers historical data.
        let mut info = WorkerInfo::new(addr);
        let worker_id = info.worker_id();
        for item in storages {
            info.add_storage(item);
        }

        // Set version information
        info.version = Some(version);

        // The worker dcm state does not change
        info.status = match self.workers.get(&worker_id) {
            Some(v) if v.status == WorkerStatus::Decommission => v.status,
            _ => WorkerStatus::Live,
        };

        self.workers.insert(worker_id, info);
        if let Some(lost) = self.lost_workers.swap_remove(&worker_id) {
            info!("Lost Worker Recovery: {}", lost.simple_debug())
        }

        Ok(())
    }

    // Delete the worker with heartbeat timeout
    pub fn remove_expired(&mut self, id: u32) -> Option<WorkerInfo> {
        let worker = match self.workers.swap_remove(&id) {
            None => {
                warn!("Not found worker {}", id);
                return None;
            }

            Some(v) => v,
        };

        error!("remove expired worker {}", worker.address);
        self.lost_workers.insert(id, worker.clone());
        Some(worker)
    }

    // Delete blocks that have been offline
    pub fn remove_offline(&mut self, id: u32) -> Option<WorkerInfo> {
        self.remove_expired(id)
    }

    pub fn workers(&self) -> &IndexMap<u32, WorkerInfo> {
        &self.workers
    }

    pub fn lost_workers(&self) -> &IndexMap<u32, WorkerInfo> {
        &self.lost_workers
    }
}
