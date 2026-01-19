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

use crate::master::fs::policy::{ChooseContext, WorkerPolicyAdapter};
use crate::master::fs::state::{BlockMap, WorkerMap};
use crate::master::fs::DeleteResult;
use curvine_common::conf::ClusterConf;
use curvine_common::state::{
    BlockLocation, ExtendedBlock, HeartbeatStatus, LocatedBlock, StorageInfo, WorkerAddress,
    WorkerCommand, WorkerInfo, WorkerStatus,
};
use curvine_common::version::{CompatibilityPolicy, CompatibilityResult, Version, VersionChecker};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::ByteUnit;
use orpc::{err_box, CommonResult};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

pub struct WorkerManager {
    pub(crate) worker_map: WorkerMap,
    pub(crate) block_map: BlockMap,
    pub(crate) worker_policy: WorkerPolicyAdapter,
    pub(crate) cluster_id: String,
    pub(crate) conf: ClusterConf,
    pub(crate) version_checker: VersionChecker,
}

impl WorkerManager {
    pub fn new(conf: &ClusterConf) -> Self {
        let worker_policy = WorkerPolicyAdapter::from_conf(conf).unwrap();

        // Parse master version and compatibility policy
        let master_version = Version::current();
        let policy = Self::parse_compatibility_policy(conf);
        let version_checker = VersionChecker::new(master_version, policy);

        Self {
            worker_map: WorkerMap::new(),
            block_map: BlockMap::new(),
            worker_policy,
            cluster_id: conf.cluster_id.to_string(),
            conf: conf.clone(),
            version_checker,
        }
    }

    fn parse_compatibility_policy(conf: &ClusterConf) -> CompatibilityPolicy {
        // Parse min_worker_version from configuration
        let min_version = Version::from_str(&conf.master.min_worker_version).unwrap_or_else(|e| {
            warn!(
                "Failed to parse min_worker_version '{}': {}, using default 0.1.0",
                conf.master.min_worker_version, e
            );
            Version::new(0, 1, 0)
        });

        CompatibilityPolicy::new(min_version)
    }

    pub fn heartbeat(
        &mut self,
        cluster_id: &str,
        status: HeartbeatStatus,
        addr: WorkerAddress,
        storages: Vec<StorageInfo>,
        version: Version,
    ) -> FsResult<Vec<WorkerCommand>> {
        // The cluster id must match to prevent misregistration.
        if cluster_id != self.cluster_id {
            return err_box!(
                "Registered cluster_id mismatch, expected {}, actual: {}",
                self.cluster_id,
                cluster_id
            );
        }

        // Check version compatibility
        let compat_result = self.version_checker.check_compatibility(&version);
        if let CompatibilityResult::Incompatible(reason) = compat_result {
            warn!(
                "Worker {} registration rejected due to version incompatibility: {}",
                addr, reason
            );
            return Err(curvine_common::error::FsError::version_incompatible(reason));
        }

        info!(
            "Worker {} registered successfully with version {}",
            addr, version
        );

        let cmds = match status {
            HeartbeatStatus::Start => {
                info!("Worker register: {}", addr);
                vec![]
            }

            HeartbeatStatus::Running => self.block_map.handle_heartbeat(addr.worker_id),

            HeartbeatStatus::End => {
                info!("Worker unregister: {}", addr);
                let _ = self.worker_map.remove_offline(addr.worker_id);
                return Ok(vec![]);
            }
        };

        self.worker_map.insert(addr, storages, version)?;
        Ok(cmds)
    }

    pub fn choose_worker(&self, ctx: ChooseContext) -> CommonResult<Vec<WorkerAddress>> {
        let replicas = ctx.replicas;
        let workers = self.worker_policy.choose(self.worker_map.workers(), ctx)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > replicas as usize {
            err_box!("The number of workers exceeds the number of replicas")
        } else {
            Ok(workers)
        }
    }

    /// Select the specified number of workers, do not rely on block information
    pub fn choose_workers(
        &self,
        count: usize,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let workers =
            self.worker_policy
                .choose_workers(self.worker_map.workers(), count, exclude_workers)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > count {
            err_box!("The number of workers exceeds the requested count")
        } else {
            Ok(workers)
        }
    }

    pub fn get_last_heartbeat(&self) -> Vec<(u32, u64)> {
        let mut res = vec![];
        for worker in self.worker_map.workers() {
            res.push((*worker.0, worker.1.last_update));
        }
        res
    }

    pub fn remove_expired_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        self.worker_map.remove_expired(id)
    }

    pub fn add_blacklist_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        match self.worker_map.workers.get_mut(&id) {
            Some(v) => {
                v.status = WorkerStatus::Blacklist;
                Some(v.clone())
            }

            None => None,
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.remove_block(worker_id, block_id)
    }

    // Indicates the block that needs to be deleted.
    pub fn remove_blocks(&mut self, del_res: &DeleteResult) {
        self.block_map.remove_blocks(del_res)
    }

    pub fn deleted_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.deleted_block(worker_id, block_id)
    }

    pub fn get_worker(&self, id: u32) -> Option<&WorkerInfo> {
        self.worker_map.workers.get(&id)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        let mut addrs = Vec::with_capacity(locs.len());
        for loc in locs {
            if let Some(info) = self.get_worker(loc.worker_id) {
                addrs.push(info.address.clone());
            } else {
                warn!(
                    "File {} block {}, worker {} replicas has been lost",
                    path.as_ref(),
                    block.id,
                    loc.worker_id
                );
            }
        }

        if addrs.is_empty() && !locs.is_empty() {
            return err_box!(
                "File {} block {}, all replicas has been lost",
                path.as_ref(),
                block.id
            );
        }

        let lb = LocatedBlock { block, locs: addrs };

        Ok(lb)
    }

    pub fn add_test_worker(&mut self, worker: WorkerInfo) {
        self.worker_map.workers.insert(worker.worker_id(), worker);
    }

    pub fn add_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        for addr in list {
            set.insert(addr);
        }

        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter_mut() {
            if set.contains(&worker.address.hostname) {
                worker.status = WorkerStatus::Decommission;
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn get_dcm(&self) -> Vec<String> {
        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter() {
            if worker.status == WorkerStatus::Decommission {
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn remove_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        for addr in list {
            set.insert(addr);
        }

        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter_mut() {
            if set.contains(&worker.address.hostname) {
                worker.status = WorkerStatus::Live;
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn worker_list(&self) -> Vec<String> {
        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter() {
            res.push(worker.simple_string())
        }
        res
    }
}

impl Display for WorkerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut str = String::new();
        for (_, item) in self.worker_map.workers() {
            let s = format!(
                "worker_id={}, address={}, capacity={}, available={}\n",
                item.worker_id(),
                item.address,
                ByteUnit::byte_to_string(item.capacity as u64),
                ByteUnit::byte_to_string(item.available as u64),
            );
            str.push_str(&s)
        }

        write!(f, "{}", str)
    }
}

impl Default for WorkerManager {
    fn default() -> Self {
        let conf = ClusterConf::default();
        Self::new(&conf)
    }
}
