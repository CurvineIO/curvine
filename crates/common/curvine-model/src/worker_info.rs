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

use crate::state::{StorageInfo, WorkerAddress, WorkerStatus};
use curvine_proto::ComponentInfoProto;
use curvine_runtime::common::LocalTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TransferWorkerCapabilities {
    pub task_submit: bool,
    pub report_target: bool,
    pub query_task: bool,
    pub attempt_safe_output: bool,
    pub source_read_plan: bool,
}

impl TransferWorkerCapabilities {
    pub fn current() -> Self {
        Self {
            task_submit: true,
            report_target: true,
            query_task: true,
            attempt_safe_output: true,
            source_read_plan: true,
        }
    }

    pub fn supports_transfer(&self) -> bool {
        self.task_submit
            && self.report_target
            && self.query_task
            && self.attempt_safe_output
            && self.source_read_plan
    }
}

// Describes a worker, which is the basic unit of master management worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerInfo {
    pub address: WorkerAddress,
    #[serde(default = "WorkerInfo::default_weight")]
    pub weight: u32,
    pub software_version: String,
    pub startup_time_ms: u64,
    pub capacity: i64,
    pub available: i64,
    pub fs_used: i64,
    pub non_fs_used: i64,
    pub reserved_bytes: i64,
    pub last_update: u64,
    pub block_num: i64,
    pub storage_map: HashMap<String, StorageInfo>,
    pub status: WorkerStatus,
    pub worker_session_id: String,
    pub transfer_capabilities: TransferWorkerCapabilities,
    /// Structured version/protocol metadata reported by the worker on its
    /// heartbeat. `None` means a legacy worker that only sent the display
    /// string `software_version`.
    pub component_info: Option<ComponentInfoProto>,
    /// Master-only in-flight reservation. Not on proto or `/api/workers`.
    #[serde(skip)]
    pub scheduled_bytes: i64,
    #[serde(skip)]
    pub scheduled_since_ms: u64,
}

impl WorkerInfo {
    pub const fn default_weight() -> u32 {
        1
    }

    pub fn new(addr: WorkerAddress, weight: u32) -> Self {
        Self {
            address: addr,
            weight,
            software_version: String::new(),
            startup_time_ms: 0,
            capacity: 0,
            available: 0,
            fs_used: 0,
            non_fs_used: 0,
            reserved_bytes: 0,
            block_num: 0,
            last_update: LocalTime::mills(),
            storage_map: Default::default(),
            status: WorkerStatus::Live,
            worker_session_id: String::new(),
            transfer_capabilities: TransferWorkerCapabilities::default(),
            component_info: None,
            scheduled_bytes: 0,
            scheduled_since_ms: 0,
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

    pub fn allocatable_available(&self) -> i64 {
        self.available.saturating_sub(self.scheduled_bytes)
    }

    pub fn can_allocate(&self, block_size: i64) -> bool {
        if !self.is_live() {
            return false;
        }
        // 0 is a valid size: no capacity constraint, pick any live worker.
        if block_size <= 0 {
            return true;
        }
        if self.storage_map.is_empty() {
            return self.allocatable_available() >= block_size;
        }
        self.storage_map.values().any(|storage| {
            !storage.failed && storage.available.saturating_sub(self.scheduled_bytes) >= block_size
        })
    }

    pub fn schedule_bytes(&mut self, bytes: i64) {
        self.adjust_scheduled_bytes(bytes);
    }

    pub fn unschedule_bytes(&mut self, bytes: i64) {
        self.adjust_scheduled_bytes(bytes.saturating_neg());
    }

    fn adjust_scheduled_bytes(&mut self, delta: i64) {
        if delta == 0 {
            return;
        }
        self.scheduled_bytes = self.scheduled_bytes.saturating_add(delta).max(0);
        if self.scheduled_bytes == 0 {
            self.scheduled_since_ms = 0;
        } else if delta > 0 && self.scheduled_since_ms == 0 {
            self.scheduled_since_ms = LocalTime::mills();
        }
    }

    pub fn reclaim_scheduled_from_heartbeat(&mut self, previous_available: i64) {
        let consumed = previous_available.saturating_sub(self.available).max(0);
        self.adjust_scheduled_bytes(consumed.saturating_neg());
    }

    pub fn expire_scheduled_bytes(&mut self, now_ms: u64, timeout_ms: u64) {
        if timeout_ms == 0 || self.scheduled_bytes == 0 {
            return;
        }
        if self.scheduled_since_ms == 0 {
            self.scheduled_since_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.scheduled_since_ms) >= timeout_ms {
            self.scheduled_bytes = 0;
            self.scheduled_since_ms = 0;
        }
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
            weight: Self::default_weight(),
            software_version: String::new(),
            startup_time_ms: 0,
            capacity: 1 << 30,
            available: 1 << 30,
            fs_used: 0,
            non_fs_used: 0,
            reserved_bytes: 0,
            last_update: 0,
            block_num: 0,
            storage_map: Default::default(),
            status: WorkerStatus::Live,
            worker_session_id: String::new(),
            transfer_capabilities: TransferWorkerCapabilities::default(),
            component_info: None,
            scheduled_bytes: 0,
            scheduled_since_ms: 0,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocatable_available_subtracts_scheduled_bytes() {
        let worker = WorkerInfo {
            available: 1 << 30,
            scheduled_bytes: 8 * (128 << 20),
            ..Default::default()
        };
        assert_eq!(worker.allocatable_available(), 0);
        assert!(!worker.can_allocate(128 << 20));
        assert!(worker.can_allocate(0));
    }

    #[test]
    fn can_allocate_zero_block_size_picks_live_worker() {
        let worker = WorkerInfo {
            available: 0,
            scheduled_bytes: 0,
            ..Default::default()
        };
        assert!(worker.can_allocate(0));
        assert!(!worker.can_allocate(1));
    }

    #[test]
    fn can_allocate_rejects_non_live_worker() {
        let worker = WorkerInfo {
            status: WorkerStatus::Blacklist,
            ..Default::default()
        };
        assert!(!worker.can_allocate(0));
        assert!(!worker.can_allocate(128));
    }

    #[test]
    fn heartbeat_reclaim_keeps_unwritten_reservations() {
        let mut worker = WorkerInfo {
            available: 1 << 30,
            ..Default::default()
        };
        worker.schedule_bytes(1 << 30);
        worker.reclaim_scheduled_from_heartbeat(1 << 30);
        assert_eq!(worker.scheduled_bytes, 1 << 30);
        assert_eq!(worker.allocatable_available(), 0);
    }

    #[test]
    fn heartbeat_reclaim_drops_capacity_already_reported() {
        let prev = 1 << 30;
        let mut worker = WorkerInfo {
            available: prev - 4 * (128 << 20),
            scheduled_bytes: 8 * (128 << 20),
            ..Default::default()
        };
        worker.reclaim_scheduled_from_heartbeat(prev);
        assert_eq!(worker.scheduled_bytes, 4 * (128 << 20));
        assert_eq!(worker.allocatable_available(), 0);
    }

    #[test]
    fn heartbeat_reclaim_does_not_go_negative() {
        let prev = 1 << 30;
        let mut worker = WorkerInfo {
            available: 0,
            scheduled_bytes: 128 << 20,
            ..Default::default()
        };
        worker.reclaim_scheduled_from_heartbeat(prev);
        assert_eq!(worker.scheduled_bytes, 0);
    }

    fn worker_with_dirs(dirs: &[StorageInfo]) -> WorkerInfo {
        let mut worker = WorkerInfo::new(WorkerAddress::default(), 1);
        for dir in dirs {
            worker.add_storage(dir.clone());
        }
        worker
    }

    fn dir(storage_id: &str, available: i64) -> StorageInfo {
        StorageInfo {
            storage_id: storage_id.to_string(),
            available,
            capacity: available,
            ..Default::default()
        }
    }

    #[test]
    fn can_allocate_rejects_when_no_single_dir_fits_block() {
        let worker = worker_with_dirs(&[dir("disk-0", 100), dir("disk-1", 100)]);
        assert_eq!(worker.available, 200);
        assert!(!worker.can_allocate(128));
    }

    #[test]
    fn can_allocate_accepts_when_one_dir_fits_block() {
        let worker = worker_with_dirs(&[dir("disk-0", 128), dir("disk-1", 10)]);
        assert!(worker.can_allocate(128));
    }

    #[test]
    fn can_allocate_deducts_scheduled_bytes_from_each_dir() {
        let mut worker = worker_with_dirs(&[dir("disk-0", 200), dir("disk-1", 200)]);
        assert!(worker.can_allocate(128));
        worker.schedule_bytes(128);
        assert!(!worker.can_allocate(128));
    }

    #[test]
    fn unschedule_bytes_restores_reservation() {
        let mut worker = WorkerInfo::default();
        worker.schedule_bytes(128);
        worker.unschedule_bytes(128);
        assert_eq!(worker.scheduled_bytes, 0);
        worker.unschedule_bytes(64);
        assert_eq!(worker.scheduled_bytes, 0);
    }

    #[test]
    fn expire_scheduled_bytes_after_timeout() {
        let mut worker = WorkerInfo::default();
        worker.schedule_bytes(128);
        worker.scheduled_since_ms = 10;
        worker.expire_scheduled_bytes(20, 10);
        assert_eq!(worker.scheduled_bytes, 0);
        assert_eq!(worker.scheduled_since_ms, 0);
    }

    #[test]
    fn expire_scheduled_bytes_keeps_reservation_before_timeout() {
        let mut worker = WorkerInfo::default();
        worker.schedule_bytes(128);
        worker.scheduled_since_ms = 10;
        worker.expire_scheduled_bytes(19, 10);
        assert_eq!(worker.scheduled_bytes, 128);
    }
}
