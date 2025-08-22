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

use crate::proto::WorkerAddressProto;
use crate::state::{StorageInfo, WorkerAddress, WorkerStatus};
use orpc::common::LocalTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

// Describes a worker, which is the basic unit of master management worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerInfo {
    pub address: WorkerAddress,
    pub capacity: i64,
    pub available: i64,
    pub fs_used: i64,
    pub non_fs_used: i64,
    pub reserved_bytes: i64,
    pub last_update: u64,
    pub block_num: i64,
    pub storage_map: HashMap<String, StorageInfo>,
    pub status: WorkerStatus,
}

impl WorkerInfo {
    pub fn new(addr: WorkerAddress) -> Self {
        Self {
            address: addr,
            capacity: 0,
            available: 0,
            fs_used: 0,
            non_fs_used: 0,
            reserved_bytes: 0,
            block_num: 0,
            last_update: LocalTime::mills(),
            storage_map: Default::default(),
            status: WorkerStatus::Live,
        }
    }

    pub fn add_storage(&mut self, storage: StorageInfo) {
        // failed storage is not counted.
        if !storage.failed {
            self.capacity += storage.capacity;
            self.available += storage.available;
            self.fs_used += storage.fs_used;
            self.non_fs_used += storage.non_fs_used;
            self.reserved_bytes += storage.reserved_bytes;
            self.block_num += storage.block_num;
        }

        self.storage_map
            .insert(storage.storage_id.to_string(), storage);
    }

    pub fn worker_id(&self) -> u32 {
        self.address.worker_id
    }

    pub fn simple_debug(&self) -> String {
        format!(
            "worker_id={}, hostname={}, port={}, last_update={}",
            self.worker_id(),
            self.address.hostname,
            self.address.rpc_port,
            self.last_update
        )
    }

    pub fn is_live(&self) -> bool {
        self.status == WorkerStatus::Live
    }

    pub fn rpc_addr(&self) -> String {
        self.address.connect_addr()
    }

    pub fn simple_string(&self) -> String {
        format!(
            "{},{}:{},{:?}",
            self.worker_id(),
            self.address.hostname,
            self.address.rpc_port,
            self.status
        )
    }
}

impl Default for WorkerInfo {
    fn default() -> Self {
        let address = WorkerAddress {
            worker_id: 100,
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 666,
            ..Default::default()
        };

        Self {
            address,
            capacity: 1 << 30,
            available: 1 << 30,
            fs_used: 0,
            non_fs_used: 0,
            reserved_bytes: 0,
            last_update: 0,
            block_num: 0,
            storage_map: Default::default(),
            status: WorkerStatus::Live,
        }
    }
}

impl PartialEq for WorkerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.worker_id() == other.worker_id()
    }
}

impl Display for WorkerInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}:{})",
            self.worker_id(),
            self.address.hostname,
            self.address.rpc_port
        )
    }
}
