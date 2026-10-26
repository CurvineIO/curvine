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
use std::collections::{BTreeMap, HashMap};
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
    /// Per-block share of `scheduled_bytes`, keyed by block id. The value is
    /// `(allocation sequence, credit bytes)`: block ids group by file
    /// creation order (`InodeId::create_block_id`), so reclaim trimming
    /// orders by the sequence, which tracks reservation age. The credit sum
    /// (`scheduled_block_bytes`) never exceeds `scheduled_bytes`; the
    /// difference is reservation not yet bound to a block.
    #[serde(skip)]
    pub scheduled_blocks: BTreeMap<i64, (u64, i64)>,
    /// Running sum of `scheduled_blocks` credits, so the anonymous remainder
    /// and the reclaim excess are O(1) on the allocation and commit paths.
    #[serde(skip)]
    pub scheduled_block_bytes: i64,
    /// Monotonic allocation sequence for `scheduled_blocks` values.
    #[serde(skip)]
    pub scheduled_seq: u64,
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
            scheduled_blocks: BTreeMap::new(),
            scheduled_block_bytes: 0,
            scheduled_seq: 0,
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

    /// Binds part of the anonymous reservation to a block, so a later commit
    /// settles exactly the credit this block still owns. Credit a heartbeat
    /// already reclaimed is gone and cannot be bound.
    pub fn bind_scheduled_block(&mut self, block_id: i64, bytes: i64) {
        let anonymous = self
            .scheduled_bytes
            .saturating_sub(self.scheduled_block_bytes);
        let credit = bytes.min(anonymous);
        if credit <= 0 {
            return;
        }
        self.scheduled_seq += 1;
        // A rebind of the same block keeps its original sequence: the oldest
        // reservation age is the correct trim priority.
        let entry = self
            .scheduled_blocks
            .entry(block_id)
            .or_insert((self.scheduled_seq, 0));
        entry.1 = entry.1.saturating_add(credit);
        self.scheduled_block_bytes = self.scheduled_block_bytes.saturating_add(credit);
    }

    /// Releases the committed block's credit above its final length. A block
    /// with no bound credit (a rewrite that never re-reserved, a repeated
    /// flush, or credit a heartbeat already reclaimed) settles nothing, so
    /// other blocks' reservations stay intact. The remaining `block_len`
    /// credit is left for the heartbeat to reclaim once the worker reports
    /// the reduced available space.
    pub fn settle_scheduled_block(&mut self, block_id: i64, block_len: i64) {
        let Some(&(seq, credit)) = self.scheduled_blocks.get(&block_id) else {
            return;
        };
        let keep = block_len.clamp(0, credit);
        let release = credit - keep;
        if keep == 0 {
            self.scheduled_blocks.remove(&block_id);
        } else {
            self.scheduled_blocks.insert(block_id, (seq, keep));
        }
        // Reduce the credit sum first: the aggregate adjustment below then
        // sees the invariant already restored and skips the trim scan.
        self.scheduled_block_bytes = self.scheduled_block_bytes.saturating_sub(release).max(0);
        if release > 0 {
            self.adjust_scheduled_bytes(release.saturating_neg());
        }
    }

    fn adjust_scheduled_bytes(&mut self, delta: i64) {
        if delta == 0 {
            return;
        }
        self.scheduled_bytes = self.scheduled_bytes.saturating_add(delta).max(0);
        if delta < 0 {
            self.shrink_block_credits();
        }
        if self.scheduled_bytes == 0 {
            self.scheduled_since_ms = 0;
        } else if delta > 0 && self.scheduled_since_ms == 0 {
            self.scheduled_since_ms = LocalTime::mills();
        }
    }

    /// Keeps the per-block credit sum within `scheduled_bytes` after a
    /// reclaim, consuming the anonymous remainder first and then block
    /// credits by reservation age (allocation sequence), oldest first. The
    /// running sum makes the no-excess case O(1); the sort only runs when a
    /// heartbeat reclaimed more than the anonymous remainder.
    fn shrink_block_credits(&mut self) {
        if self.scheduled_bytes == 0 {
            self.scheduled_blocks.clear();
            self.scheduled_block_bytes = 0;
            return;
        }
        let mut excess = self
            .scheduled_block_bytes
            .saturating_sub(self.scheduled_bytes);
        if excess <= 0 {
            return;
        }
        let mut by_age: Vec<(u64, i64, i64)> = self
            .scheduled_blocks
            .iter()
            .map(|(&block_id, &(seq, credit))| (seq, block_id, credit))
            .collect();
        by_age.sort_unstable();
        for (seq, block_id, credit) in by_age {
            if excess <= 0 {
                break;
            }
            if credit <= excess {
                self.scheduled_blocks.remove(&block_id);
                self.scheduled_block_bytes =
                    self.scheduled_block_bytes.saturating_sub(credit).max(0);
                excess -= credit;
            } else {
                self.scheduled_blocks
                    .insert(block_id, (seq, credit - excess));
                self.scheduled_block_bytes =
                    self.scheduled_block_bytes.saturating_sub(excess).max(0);
                break;
            }
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
            self.scheduled_blocks.clear();
            self.scheduled_block_bytes = 0;
        }
    }

    /// Carries the in-flight reservation state from the previous
    /// registration of the same worker across a heartbeat re-insert.
    pub fn carry_scheduled_from(&mut self, previous: &mut WorkerInfo) {
        self.scheduled_bytes = previous.scheduled_bytes;
        self.scheduled_since_ms = previous.scheduled_since_ms;
        self.scheduled_blocks = std::mem::take(&mut previous.scheduled_blocks);
        self.scheduled_block_bytes = previous.scheduled_block_bytes;
        self.scheduled_seq = previous.scheduled_seq;
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
            scheduled_blocks: BTreeMap::new(),
            scheduled_block_bytes: 0,
            scheduled_seq: 0,
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

    fn block_credit(worker: &WorkerInfo, block_id: i64) -> Option<i64> {
        worker
            .scheduled_blocks
            .get(&block_id)
            .map(|&(_, credit)| credit)
    }

    #[test]
    fn bind_scheduled_block_caps_credit_at_anonymous_reservation() {
        let mut worker = WorkerInfo::default();
        worker.schedule_bytes(128);
        // A heartbeat already reclaimed part of the reservation; the block
        // can only own what is still outstanding.
        worker.unschedule_bytes(100);
        worker.bind_scheduled_block(1, 128);
        assert_eq!(block_credit(&worker, 1), Some(28));
        assert_eq!(worker.scheduled_block_bytes, 28);
        // Nothing anonymous is left for a second block.
        worker.bind_scheduled_block(2, 128);
        assert_eq!(block_credit(&worker, 2), None);
        assert_eq!(worker.scheduled_block_bytes, 28);
    }

    #[test]
    fn settle_scheduled_block_converges_to_block_len_once() {
        let mut worker = WorkerInfo::default();
        worker.schedule_bytes(128);
        worker.bind_scheduled_block(1, 128);

        worker.settle_scheduled_block(1, 32);
        assert_eq!(worker.scheduled_bytes, 32);
        assert_eq!(worker.scheduled_block_bytes, 32);
        // A repeated flush or a larger commit releases nothing further.
        worker.settle_scheduled_block(1, 32);
        worker.settle_scheduled_block(1, 64);
        assert_eq!(worker.scheduled_bytes, 32);
        // A block without credit settles nothing.
        worker.settle_scheduled_block(2, 0);
        assert_eq!(worker.scheduled_bytes, 32);
        assert_eq!(worker.scheduled_block_bytes, 32);
    }

    #[test]
    fn heartbeat_reclaim_trims_by_reservation_age_not_block_id() {
        let mut worker = WorkerInfo {
            available: 1 << 30,
            ..Default::default()
        };
        // Block ids group by file creation order, so the numerically larger
        // id can hold the older reservation. Bind id 10 first: its credit
        // must be trimmed first even though 2 < 10.
        worker.schedule_bytes(256);
        worker.bind_scheduled_block(10, 128);
        worker.bind_scheduled_block(2, 128);

        worker.available -= 160;
        worker.reclaim_scheduled_from_heartbeat(worker.available + 160);
        assert_eq!(worker.scheduled_bytes, 96);
        assert_eq!(block_credit(&worker, 10), None);
        assert_eq!(block_credit(&worker, 2), Some(96));
        assert_eq!(worker.scheduled_block_bytes, 96);
    }

    #[test]
    fn expire_scheduled_bytes_clears_block_credits() {
        let mut worker = WorkerInfo::default();
        worker.schedule_bytes(128);
        worker.bind_scheduled_block(1, 128);
        worker.scheduled_since_ms = 10;
        worker.expire_scheduled_bytes(20, 10);
        assert_eq!(worker.scheduled_bytes, 0);
        assert!(worker.scheduled_blocks.is_empty());
        assert_eq!(worker.scheduled_block_bytes, 0);
    }
}
