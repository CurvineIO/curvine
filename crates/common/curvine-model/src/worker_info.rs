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
use std::sync::{Arc, Weak};

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

/// Master-local, bounded queue for completed cache reservations. A challenge
/// freezes a batch; later completions cannot be released by an earlier snapshot.
#[derive(Debug, Clone, Default)]
pub struct CacheReservationFence {
    pub snapshot_seq: u64,
    term: Option<u64>,
    generation: u64,
    pending: BTreeMap<i64, (u64, u64)>,
    inflight: Option<CacheReservationBatch>,
    // A global reclaim cannot identify which write consumed capacity. Only
    // allocations made since the last such reclaim may use extra settlement.
    reclaim_epoch: u64,
    anonymous_epoch: Option<u64>,
    allocation_epochs: BTreeMap<i64, u64>,
    leases: BTreeMap<i64, Weak<()>>,
    events: CacheReservationEvents,
}

#[derive(Debug, Clone)]
struct CacheReservationBatch {
    token: String,
    issued_at_ms: u64,
    entries: Vec<(i64, u64, u64)>, // block_id, allocation sequence, enrollment generation
}

/// Event deltas drained by the master into process-wide counters.
#[derive(Debug, Clone, Default)]
pub struct CacheReservationEvents {
    pub confirmed: u64,
    pub expired: u64,
    pub released_bytes: u64,
    pub skipped: BTreeMap<&'static str, u64>,
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
    #[serde(skip)]
    pub cache_reservation_fence: CacheReservationFence,
}

impl WorkerInfo {
    /// Keep at most 16 batches per worker; overflow keeps legacy credit.
    pub const CACHE_RESERVATION_PENDING_LIMIT: usize = 65_536;
    /// Bound work under the worker-manager lock on each heartbeat.
    pub const CACHE_RESERVATION_BATCH_LIMIT: usize = 4096;
    /// Retry the same immutable batch every heartbeat for up to one minute.
    /// Expiry drops only confirmation eligibility, never reservation bytes.
    pub const CACHE_RESERVATION_ACK_TIMEOUT_MS: u64 = 60_000;

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
            cache_reservation_fence: Default::default(),
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
        // Several allocations may be anonymous at once. Keep their earliest
        // epoch until all are bound: a heartbeat between choose and bind must
        // not give an older reservation a fresh eligibility epoch.
        if bytes > 0 && self.scheduled_bytes == self.scheduled_block_bytes {
            self.cache_reservation_fence.anonymous_epoch =
                Some(self.cache_reservation_fence.reclaim_epoch);
        }
        self.adjust_scheduled_bytes(bytes);
    }

    fn invalidate_reclaim_epoch(&mut self) {
        let fence = &mut self.cache_reservation_fence;
        if let Some(next) = fence.reclaim_epoch.checked_add(1) {
            fence.reclaim_epoch = next;
        } else {
            // Fail closed on wraparound; never revive an old allocation.
            fence.allocation_epochs.clear();
            fence.anonymous_epoch = None;
        }
    }

    pub fn unschedule_bytes(&mut self, bytes: i64) {
        if bytes > 0 && self.scheduled_bytes > 0 {
            self.invalidate_reclaim_epoch();
        }
        self.adjust_scheduled_bytes(bytes.saturating_neg());
    }

    /// Binds part of the anonymous reservation to a block, so a later commit
    /// settles exactly the credit this block still owns. Credit a heartbeat
    /// already reclaimed is gone and cannot be bound.
    pub fn bind_scheduled_block(&mut self, block_id: i64, bytes: i64) {
        self.invalidate_cache_reservation(block_id);
        let anonymous = self
            .scheduled_bytes
            .saturating_sub(self.scheduled_block_bytes);
        let credit = bytes.min(anonymous);
        if credit <= 0 {
            return;
        }
        let fence = &mut self.cache_reservation_fence;
        if !self.scheduled_blocks.contains_key(&block_id)
            && fence.anonymous_epoch == Some(fence.reclaim_epoch)
        {
            fence
                .allocation_epochs
                .insert(block_id, fence.reclaim_epoch);
        } else {
            // Rebinding never refreshes proof for any old credit it retains.
            fence.allocation_epochs.remove(&block_id);
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
        if self.scheduled_bytes == self.scheduled_block_bytes {
            self.cache_reservation_fence.anonymous_epoch = None;
        }
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
            self.cache_reservation_fence
                .allocation_epochs
                .remove(&block_id);
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
            self.cache_reservation_fence.allocation_epochs.clear();
            self.cache_reservation_fence.anonymous_epoch = None;
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
                self.cache_reservation_fence
                    .allocation_epochs
                    .remove(&block_id);
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

    /// A promoted master must never reuse a batch from an earlier leadership.
    /// Keep the worker sequence: old capacity snapshots are still stale.
    pub fn adopt_cache_reservation_term(&mut self, term: u64) {
        let fence = &mut self.cache_reservation_fence;
        if fence.term != Some(term) {
            fence.pending.clear();
            fence.leases.clear();
            fence.inflight = None;
            fence.term = Some(term);
        }
    }

    #[cfg(test)]
    fn mark_completed_cache_reservation(&mut self, block_id: i64) {
        self.enroll_cache_reservation(block_id, None);
    }

    /// The inode tracker owns the lease. Dropping it revokes every block of
    /// that inode, including entries already frozen in a batch, in O(1).
    pub fn mark_completed_cache_reservation_with_lease(&mut self, block_id: i64, lease: &Arc<()>) {
        self.enroll_cache_reservation(block_id, Some(Arc::downgrade(lease)));
    }

    fn skip_cache_reservation(&mut self, reason: &'static str) {
        *self
            .cache_reservation_fence
            .events
            .skipped
            .entry(reason)
            .or_default() += 1;
    }

    fn enroll_cache_reservation(&mut self, block_id: i64, lease: Option<Weak<()>>) {
        let fence = &self.cache_reservation_fence;
        if fence.snapshot_seq == 0 || self.worker_session_id.is_empty() {
            self.skip_cache_reservation("legacy_worker");
            return;
        }
        if !self.scheduled_blocks.contains_key(&block_id) {
            return;
        }
        if fence.allocation_epochs.get(&block_id) != Some(&fence.reclaim_epoch) {
            self.skip_cache_reservation("legacy_reclaim");
            return;
        }
        if fence.pending.contains_key(&block_id) {
            return;
        }
        if fence.pending.len() >= Self::CACHE_RESERVATION_PENDING_LIMIT {
            self.skip_cache_reservation("pending_limit");
            return;
        }
        let seq = self.scheduled_blocks[&block_id].0;
        let fence = &mut self.cache_reservation_fence;
        fence.generation = fence
            .generation
            .checked_add(1)
            .expect("cache fence generation exhausted");
        fence.pending.insert(block_id, (seq, fence.generation));
        if let Some(lease) = lease {
            fence.leases.insert(block_id, lease);
        }
    }

    pub fn invalidate_cache_reservation(&mut self, block_id: i64) {
        self.cache_reservation_fence.pending.remove(&block_id);
        self.cache_reservation_fence.leases.remove(&block_id);
    }

    fn cache_reservation_eligible(&self, id: i64, seq: u64) -> bool {
        let fence = &self.cache_reservation_fence;
        self.scheduled_blocks.get(&id).is_some_and(|v| v.0 == seq)
            && fence.allocation_epochs.get(&id) == Some(&fence.reclaim_epoch)
            && fence
                .leases
                .get(&id)
                .is_none_or(|lease| lease.strong_count() > 0)
    }

    pub fn cache_reservation_challenge(
        &mut self,
        token: impl FnOnce() -> String,
    ) -> Option<String> {
        self.cache_reservation_challenge_at(token, LocalTime::mills())
    }

    fn cache_reservation_challenge_at(
        &mut self,
        token: impl FnOnce() -> String,
        now: u64,
    ) -> Option<String> {
        if self
            .cache_reservation_fence
            .inflight
            .as_ref()
            .is_some_and(|batch| {
                now.saturating_sub(batch.issued_at_ms) >= Self::CACHE_RESERVATION_ACK_TIMEOUT_MS
            })
        {
            let batch = self.cache_reservation_fence.inflight.take().unwrap();
            self.cache_reservation_fence.events.expired += 1;
            for (id, seq, generation) in batch.entries {
                if self.cache_reservation_fence.pending.get(&id) == Some(&(seq, generation)) {
                    self.invalidate_cache_reservation(id);
                    self.skip_cache_reservation("ack_timeout");
                }
            }
        }
        // Redeliver the SAME token and batch after a lost response/ack.
        if let Some(batch) = &self.cache_reservation_fence.inflight {
            return Some(batch.token.clone());
        }
        // Inspect at most one batch, including invalidated entries. Cleanup
        // must not turn a heartbeat into a full pending-map scan under wm.
        let candidates: Vec<_> = self
            .cache_reservation_fence
            .pending
            .iter()
            .take(Self::CACHE_RESERVATION_BATCH_LIMIT)
            .map(|(&id, &(seq, generation))| (id, seq, generation))
            .collect();
        let mut entries = Vec::with_capacity(candidates.len());
        for (id, seq, generation) in candidates {
            if self.cache_reservation_eligible(id, seq) {
                entries.push((id, seq, generation));
            } else {
                self.invalidate_cache_reservation(id);
                self.skip_cache_reservation("invalidated");
            }
        }
        if entries.is_empty() {
            return None;
        }
        let token = token();
        self.cache_reservation_fence.inflight = Some(CacheReservationBatch {
            token: token.clone(),
            issued_at_ms: now,
            entries,
        });
        Some(token)
    }

    /// Capacity was updated under the same worker-manager lock. If ANY
    /// un-attributed reclaim occurred since allocation, do not add another
    /// release: the earlier reclaim may have charged an unfinished block.
    pub fn acknowledge_cache_reservations(&mut self, token: Option<&str>) {
        let Some(batch) = &self.cache_reservation_fence.inflight else {
            return;
        };
        if token != Some(batch.token.as_str()) {
            return;
        }
        let batch = self.cache_reservation_fence.inflight.take().unwrap();
        self.cache_reservation_fence.events.confirmed += 1;
        for (id, seq, generation) in batch.entries {
            if self.cache_reservation_fence.pending.get(&id) != Some(&(seq, generation)) {
                continue;
            }
            let eligible = self.cache_reservation_eligible(id, seq);
            self.invalidate_cache_reservation(id);
            if eligible {
                let before = self.scheduled_bytes;
                self.settle_scheduled_block(id, 0);
                self.cache_reservation_fence.events.released_bytes +=
                    before.saturating_sub(self.scheduled_bytes) as u64;
            } else {
                self.skip_cache_reservation("invalidated");
            }
        }
    }

    pub fn take_cache_reservation_events(&mut self) -> CacheReservationEvents {
        std::mem::take(&mut self.cache_reservation_fence.events)
    }

    pub fn cache_reservation_observation(&self, now: u64) -> (usize, u64) {
        let fence = &self.cache_reservation_fence;
        (
            fence.pending.len(),
            fence
                .inflight
                .as_ref()
                .map_or(0, |b| now.saturating_sub(b.issued_at_ms)),
        )
    }

    pub fn reclaim_scheduled_from_heartbeat(&mut self, previous_available: i64) {
        let consumed = previous_available.saturating_sub(self.available).max(0);
        if consumed > 0 && self.scheduled_bytes > 0 {
            self.invalidate_reclaim_epoch();
        }
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
            self.cache_reservation_fence.pending.clear();
            self.cache_reservation_fence.leases.clear();
            self.cache_reservation_fence.allocation_epochs.clear();
            self.cache_reservation_fence.anonymous_epoch = None;
            if self.cache_reservation_fence.inflight.take().is_some() {
                self.cache_reservation_fence.events.expired += 1;
            }
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
        if self.worker_session_id == previous.worker_session_id {
            self.cache_reservation_fence = std::mem::take(&mut previous.cache_reservation_fence);
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
            scheduled_blocks: BTreeMap::new(),
            scheduled_block_bytes: 0,
            scheduled_seq: 0,
            cache_reservation_fence: Default::default(),
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
    fn confirm_cache(worker: &mut WorkerInfo, blocks: &[i64]) {
        worker.worker_session_id = "session".into();
        worker.cache_reservation_fence.snapshot_seq = 1;
        for &id in blocks {
            worker.mark_completed_cache_reservation(id);
        }
        if let Some(token) = worker.cache_reservation_challenge(|| "fence".into()) {
            worker.cache_reservation_fence.snapshot_seq = 2;
            worker.acknowledge_cache_reservations(Some(&token));
        }
    }

    // Model real 4 MiB short files in 16 MiB blocks. Only production reservation
    // methods are exercised; no sleeps, timeout expiry, or synthetic releases.
    fn short_file_pair_after_heartbeats(newer_finishes_first: bool) -> WorkerInfo {
        const MIB: i64 = 1 << 20;
        let mut worker = WorkerInfo {
            available: 1 << 30,
            ..Default::default()
        };
        for id in [1, 2] {
            worker.schedule_bytes(16 * MIB);
            worker.bind_scheduled_block(id, 16 * MIB);
        }
        let first = if newer_finishes_first { 2 } else { 1 };
        let second = if newer_finishes_first { 1 } else { 2 };

        // The first write reaches the worker heartbeat before its commit.
        let previous_available = worker.available;
        worker.available -= 4 * MIB;
        worker.reclaim_scheduled_from_heartbeat(previous_available);
        worker.settle_scheduled_block(first, 4 * MIB);

        // The other commit arrives before the heartbeat reporting its write.
        worker.settle_scheduled_block(second, 4 * MIB);
        let previous_available = worker.available;
        worker.available -= 4 * MIB;
        worker.reclaim_scheduled_from_heartbeat(previous_available);
        worker
    }

    #[test]
    fn reservation_gap_commit_before_heartbeat_settles_without_expiry() {
        let mut worker = WorkerInfo {
            available: 1 << 30,
            ..Default::default()
        };
        for id in [1, 2] {
            worker.schedule_bytes(16 << 20);
            worker.bind_scheduled_block(id, 16 << 20);
            worker.settle_scheduled_block(id, 4 << 20);
            let previous_available = worker.available;
            worker.available -= 4 << 20;
            worker.reclaim_scheduled_from_heartbeat(previous_available);
        }
        assert_eq!(worker.scheduled_bytes, 0);
        assert_eq!(worker.scheduled_block_bytes, 0);
        assert_eq!(worker.allocatable_available(), worker.available);
    }

    #[test]
    fn reservation_gap_heartbeat_before_commit_falls_back_after_legacy_reclaim() {
        let mut worker = short_file_pair_after_heartbeats(false);
        assert_eq!(worker.available, (1 << 30) - (8 << 20));
        assert_eq!(
            worker.scheduled_bytes,
            4 << 20,
            "legacy net-delta accounting strands credit"
        );
        confirm_cache(&mut worker, &[1, 2]);
        // Even in order, the first block's heartbeat trims 16 to 12 MiB;
        // its later 4 MiB commit retains 4 MiB already reported as consumed.
        assert_eq!(worker.scheduled_bytes, 4 << 20);
        assert_eq!(worker.allocatable_available(), worker.available - (4 << 20));
    }

    #[test]
    fn reservation_gap_out_of_order_short_files_fall_back_after_legacy_reclaim() {
        let mut worker = short_file_pair_after_heartbeats(true);
        assert_eq!(worker.available, (1 << 30) - (8 << 20));
        assert_eq!(
            worker.scheduled_bytes,
            4 << 20,
            "legacy net-delta accounting strands credit"
        );
        confirm_cache(&mut worker, &[1, 2]);
        // Before confirmation, the heartbeat for block 2 trims block 1's credit, so
        // short-file settlement leaves 4 MiB despite both writes being reported.
        assert_eq!(
            worker.scheduled_bytes,
            4 << 20,
            "ambiguous legacy reclamation must retain conservative credit"
        );
        assert_eq!(worker.allocatable_available(), worker.available - (4 << 20));
        assert_eq!(worker.scheduled_block_bytes, 4 << 20);
    }

    #[test]
    fn reservation_gap_write_and_unrelated_free_should_settle_without_expiry() {
        const MIB: i64 = 1 << 20;
        // An older, already-accounted file occupies 4 MiB. It has no reservation.
        let mut worker = WorkerInfo {
            available: (1 << 30) - 4 * MIB,
            ..Default::default()
        };
        worker.schedule_bytes(16 * MIB);
        worker.bind_scheduled_block(1, 16 * MIB);
        worker.settle_scheduled_block(1, 4 * MIB);
        assert_eq!(worker.scheduled_bytes, 4 * MIB);

        // Between heartbeats, the new file consumes 4 MiB and the unrelated
        // old file frees 4 MiB. Net availability is unchanged, but the new
        // write is fully accounted for. Repeated idle heartbeats cannot fix it.
        let previous_available = worker.available;
        worker.available -= 4 * MIB;
        worker.available += 4 * MIB;
        worker.reclaim_scheduled_from_heartbeat(previous_available);
        worker.reclaim_scheduled_from_heartbeat(worker.available);
        assert_eq!(worker.scheduled_bytes, 4 * MIB);
        confirm_cache(&mut worker, &[1]);
        assert_eq!(
            worker.scheduled_bytes, 0,
            "net-zero space change must not strand a completed write reservation"
        );
        assert_eq!(worker.allocatable_available(), worker.available);
    }

    #[test]
    fn cache_fence_keeps_uncommitted_and_later_completions() {
        let mut w = WorkerInfo {
            worker_session_id: "s".into(),
            ..Default::default()
        };
        w.cache_reservation_fence.snapshot_seq = 1;
        for id in 1..=3 {
            w.schedule_bytes(16);
            w.bind_scheduled_block(id, 16);
        }
        w.settle_scheduled_block(1, 4);
        w.mark_completed_cache_reservation(1);
        assert_eq!(
            w.cache_reservation_challenge(|| "first".into()).as_deref(),
            Some("first")
        );
        w.settle_scheduled_block(2, 4);
        w.mark_completed_cache_reservation(2);
        w.acknowledge_cache_reservations(Some("wrong"));
        assert_eq!(w.scheduled_bytes, 24);
        assert_eq!(
            w.cache_reservation_challenge(|| panic!("must retransmit"))
                .as_deref(),
            Some("first")
        );
        w.acknowledge_cache_reservations(Some("first"));
        assert_eq!(w.scheduled_bytes, 20);
        w.acknowledge_cache_reservations(Some("first"));
        assert_eq!(w.scheduled_bytes, 20);
        assert_eq!(
            w.cache_reservation_challenge(|| "second".into()).as_deref(),
            Some("second")
        );
        w.acknowledge_cache_reservations(Some("second"));
        assert_eq!(w.scheduled_bytes, 16, "uncommitted block is untouched");
    }

    // Capacity: 1024 MiB. Old caches A1/A2 occupy 4 MiB each. B completes
    // with 4 MiB while A1 is evicted; C remains open throughout confirmation.
    fn assert_cache_fence_concurrent_write_and_eviction(b_first: bool, c_written_mib: i64) {
        const MIB: i64 = 1 << 20;
        const B: i64 = 10;
        const C: i64 = 20;
        let mut w = WorkerInfo {
            available: (1024 - 8) * MIB,
            worker_session_id: "concurrent-session".into(),
            ..Default::default()
        };
        w.cache_reservation_fence.snapshot_seq = 1;
        for id in if b_first { [B, C] } else { [C, B] } {
            w.schedule_bytes(16 * MIB);
            w.bind_scheduled_block(id, 16 * MIB);
        }
        w.settle_scheduled_block(B, 4 * MIB);
        w.mark_completed_cache_reservation(B);
        // H1: B's 4 MiB write and A1's 4 MiB eviction cancel out.
        let previous_available = w.available;
        w.available -= 4 * MIB;
        w.available += 4 * MIB;
        w.reclaim_scheduled_from_heartbeat(previous_available);
        w.cache_reservation_fence.snapshot_seq = 2;
        assert_eq!(w.scheduled_bytes, 20 * MIB);
        assert_eq!(w.scheduled_blocks[&B].1, 4 * MIB);
        assert_eq!(w.scheduled_blocks[&C].1, 16 * MIB);
        assert_eq!(w.allocatable_available(), 996 * MIB);
        let token = w
            .cache_reservation_challenge(|| "completed-B".into())
            .unwrap();

        // H2 is collected after receiving the challenge. C writes 6 (or 8)
        // MiB and A2 frees 4 MiB: the legacy heartbeat reclaims 2 (or 4) MiB.
        let previous_available = w.available;
        w.available -= c_written_mib * MIB;
        w.available += 4 * MIB;
        w.reclaim_scheduled_from_heartbeat(previous_available);
        w.cache_reservation_fence.snapshot_seq = 3;
        let reclaimed = (c_written_mib - 4) * MIB;
        let expected_b = if b_first {
            4 * MIB - reclaimed
        } else {
            4 * MIB
        };
        let expected_c = if b_first {
            16 * MIB
        } else {
            16 * MIB - reclaimed
        };
        assert_eq!(w.scheduled_bytes, 20 * MIB - reclaimed);
        assert_eq!(w.scheduled_block_bytes, w.scheduled_bytes);
        assert_eq!(w.scheduled_blocks.get(&B).map_or(0, |v| v.1), expected_b);
        assert_eq!(w.scheduled_blocks[&C].1, expected_c);
        if expected_b == 0 {
            assert!(
                !w.scheduled_blocks.contains_key(&B),
                "heartbeat already removed B"
            );
        }
        let c_before_ack = w.scheduled_blocks[&C];
        let total_before_ack = w.scheduled_bytes;

        w.acknowledge_cache_reservations(Some(&token));
        assert_eq!(
            w.scheduled_bytes, total_before_ack,
            "legacy net-delta reclaim invalidates additional release eligibility"
        );
        assert_eq!(w.scheduled_blocks.get(&B).map_or(0, |v| v.1), expected_b);
        assert_eq!(w.scheduled_blocks[&C], c_before_ack);
        assert_eq!(w.scheduled_block_bytes, total_before_ack);
        assert_eq!(w.available, (1024 - 4 - c_written_mib) * MIB);
        assert_eq!(w.allocatable_available(), w.available - total_before_ack);
        w.acknowledge_cache_reservations(Some(&token));
        w.reclaim_scheduled_from_heartbeat(w.available);
        w.cache_reservation_fence.snapshot_seq = 4;
        w.acknowledge_cache_reservations(Some(&token));
        assert_eq!(w.scheduled_blocks[&C], c_before_ack);
        assert_eq!(w.scheduled_bytes, total_before_ack);
        assert_eq!(w.scheduled_block_bytes, total_before_ack);
        assert!(w
            .cache_reservation_challenge(|| panic!("C is unfinished; B lost proof"))
            .is_none());
    }

    #[test]
    fn cache_fence_concurrent_eviction_b_first_preserves_c_credit() {
        assert_cache_fence_concurrent_write_and_eviction(true, 6);
    }

    #[test]
    fn cache_fence_concurrent_eviction_c_first_preserves_c_credit() {
        assert_cache_fence_concurrent_write_and_eviction(false, 6);
    }

    #[test]
    fn cache_fence_concurrent_eviction_fully_reclaimed_b_does_not_trim_c() {
        assert_cache_fence_concurrent_write_and_eviction(true, 8);
    }

    #[test]
    fn cache_fence_unwritten_c_before_b_without_eviction_preserves_promised_capacity() {
        const MIB: i64 = 1 << 20;
        const C: i64 = 10;
        const B: i64 = 20;
        let mut w = WorkerInfo {
            available: 1024 * MIB,
            worker_session_id: "unwritten-c".into(),
            ..Default::default()
        };
        w.cache_reservation_fence.snapshot_seq = 1;
        // C reserves first and never writes. Only B consumes physical space.
        for id in [C, B] {
            w.schedule_bytes(16 * MIB);
            w.bind_scheduled_block(id, 16 * MIB);
        }
        w.settle_scheduled_block(B, 4 * MIB);
        w.mark_completed_cache_reservation(B);
        assert_eq!(w.scheduled_bytes, 20 * MIB);
        assert_eq!(w.allocatable_available(), 1004 * MIB);

        // H1 reports B's 4 MiB, with no eviction. Legacy FIFO reclamation
        // charges C, although C has not written a byte.
        let previous_available = w.available;
        w.available -= 4 * MIB;
        w.reclaim_scheduled_from_heartbeat(previous_available);
        w.cache_reservation_fence.snapshot_seq = 2;
        assert_eq!(w.scheduled_blocks[&C].1, 12 * MIB);
        assert_eq!(w.scheduled_blocks[&B].1, 4 * MIB);
        assert_eq!(w.allocatable_available(), 1004 * MIB);
        assert!(w.cache_reservation_challenge(|| "B-only".into()).is_none());

        // H2 confirms B with a fresh, unchanged snapshot. C still needs the
        // entire promised 16 MiB. Confirming B must not advertise that space.
        w.reclaim_scheduled_from_heartbeat(w.available);
        w.cache_reservation_fence.snapshot_seq = 3;
        w.acknowledge_cache_reservations(Some("B-only"));
        eprintln!(
            "after B confirmation: available={} MiB, scheduled={} MiB, allocatable={} MiB; expected allocatable=1004 MiB",
            w.available / MIB, w.scheduled_bytes / MIB, w.allocatable_available() / MIB
        );
        assert_eq!(w.available, 1020 * MIB);
        assert_eq!(
            w.allocatable_available(),
            1004 * MIB,
            "B's consumption must not both trim unwritten C and release B's credit"
        );
        assert_eq!(w.scheduled_bytes, 16 * MIB);
        assert_eq!(w.scheduled_block_bytes, 16 * MIB);
    }

    #[test]
    fn cache_fence_invalidated_or_rebound_credit_is_not_released() {
        for rebind in [false, true] {
            let mut w = WorkerInfo {
                worker_session_id: "s".into(),
                ..Default::default()
            };
            w.cache_reservation_fence.snapshot_seq = 1;
            w.schedule_bytes(16);
            w.bind_scheduled_block(1, 16);
            w.settle_scheduled_block(1, 4);
            w.mark_completed_cache_reservation(1);
            w.cache_reservation_challenge(|| "old".into());
            if rebind {
                w.schedule_bytes(16);
                w.bind_scheduled_block(1, 16);
            } else {
                w.invalidate_cache_reservation(1);
            }
            // A later completion of the same block cannot join the old batch.
            w.mark_completed_cache_reservation(1);
            let before = w.scheduled_bytes;
            w.acknowledge_cache_reservations(Some("old"));
            assert_eq!(w.scheduled_bytes, before);
            let next = w.cache_reservation_challenge(|| "new".into());
            if rebind {
                assert!(
                    next.is_none(),
                    "rebind cannot refresh proof for retained old credit"
                );
            } else {
                assert_eq!(next.as_deref(), Some("new"));
            }
        }
    }

    #[test]
    fn cache_fence_legacy_reclaim_blocks_release_and_expires_safely() {
        let mut w = WorkerInfo {
            worker_session_id: "s".into(),
            ..Default::default()
        };
        w.cache_reservation_fence.snapshot_seq = 1;
        w.schedule_bytes(16);
        w.bind_scheduled_block(1, 16);
        w.settle_scheduled_block(1, 4);
        w.mark_completed_cache_reservation(1);
        w.cache_reservation_challenge(|| "old".into());
        w.reclaim_scheduled_from_heartbeat(w.available + 3);
        w.acknowledge_cache_reservations(Some("old"));
        assert_eq!(w.scheduled_bytes, 1);
        w.schedule_bytes(16);
        w.bind_scheduled_block(2, 16);
        w.mark_completed_cache_reservation(2);
        w.cache_reservation_challenge(|| "expires".into());
        w.expire_scheduled_bytes(w.scheduled_since_ms + 10, 10);
        w.schedule_bytes(16);
        w.bind_scheduled_block(2, 16);
        w.acknowledge_cache_reservations(Some("expires"));
        assert_eq!(w.scheduled_bytes, 16);
    }

    #[test]
    fn cache_fence_legacy_and_new_worker_session_do_not_release() {
        let mut w = WorkerInfo::default();
        w.schedule_bytes(16);
        w.bind_scheduled_block(1, 16);
        w.mark_completed_cache_reservation(1);
        assert!(w.cache_reservation_challenge(|| panic!("legacy")).is_none());
        w.worker_session_id = "old".into();
        w.cache_reservation_fence.snapshot_seq = 1;
        w.mark_completed_cache_reservation(1);
        w.cache_reservation_challenge(|| "old-token".into());
        let mut restarted = WorkerInfo {
            worker_session_id: "new".into(),
            ..Default::default()
        };
        restarted.carry_scheduled_from(&mut w);
        restarted.acknowledge_cache_reservations(Some("old-token"));
        assert_eq!(restarted.scheduled_bytes, 16);
        assert_eq!(restarted.cache_reservation_fence.snapshot_seq, 0);
    }

    #[test]
    fn cache_fence_batches_are_bounded() {
        let mut w = WorkerInfo {
            worker_session_id: "s".into(),
            ..Default::default()
        };
        w.cache_reservation_fence.snapshot_seq = 1;
        for id in 0..4140 {
            w.schedule_bytes(1);
            w.bind_scheduled_block(id, 1);
            w.mark_completed_cache_reservation(id);
        }
        w.cache_reservation_challenge(|| "one".into());
        w.acknowledge_cache_reservations(Some("one"));
        assert_eq!(w.scheduled_bytes, 44);
        w.cache_reservation_challenge(|| "two".into());
        w.acknowledge_cache_reservations(Some("two"));
        assert_eq!(w.scheduled_bytes, 0);
    }
    #[test]
    fn cache_fence_stale_cleanup_is_bounded_and_pending_overflow_keeps_credit() {
        let mut w = fence_worker();
        let cap = WorkerInfo::CACHE_RESERVATION_PENDING_LIMIT;
        for id in 0..=cap as i64 {
            enroll_test_block(&mut w, id);
        }
        assert_eq!(w.cache_reservation_fence.pending.len(), cap);
        assert_eq!(
            w.take_cache_reservation_events().skipped["pending_limit"],
            1
        );
        assert_eq!(w.scheduled_bytes, 4 * (cap as i64 + 1));
        w.invalidate_reclaim_epoch();
        assert!(w.cache_reservation_challenge(|| panic!("stale")).is_none());
        assert_eq!(
            w.cache_reservation_fence.pending.len(),
            cap - WorkerInfo::CACHE_RESERVATION_BATCH_LIMIT
        );
        assert_eq!(w.scheduled_bytes, 4 * (cap as i64 + 1));
    }

    fn fence_worker() -> WorkerInfo {
        let mut w = WorkerInfo {
            available: 1024,
            worker_session_id: "s".into(),
            ..Default::default()
        };
        w.cache_reservation_fence.snapshot_seq = 1;
        w
    }

    fn enroll_test_block(w: &mut WorkerInfo, id: i64) {
        w.schedule_bytes(16);
        w.bind_scheduled_block(id, 16);
        w.settle_scheduled_block(id, 4);
        w.mark_completed_cache_reservation(id);
    }

    #[test]
    fn cache_fence_reclaim_between_schedule_and_bind_cannot_refresh_proof() {
        let mut w = fence_worker();
        w.schedule_bytes(16);
        // Reservation is anonymous when this heartbeat consumes 4 bytes.
        w.available -= 4;
        w.reclaim_scheduled_from_heartbeat(1024);
        w.schedule_bytes(16); // a second anonymous allocation must not reset the epoch
        w.bind_scheduled_block(1, 16);
        w.bind_scheduled_block(2, 16);
        w.settle_scheduled_block(1, 4);
        w.settle_scheduled_block(2, 4);
        for id in [1, 2] {
            w.mark_completed_cache_reservation(id);
        }
        assert!(w.cache_reservation_challenge(|| "unsafe".into()).is_none());
        assert_eq!(w.scheduled_bytes, 8);
        // A genuinely new allocation after all anonymous credit is bound can
        // participate again. Old ineligible credits remain untouched.
        enroll_test_block(&mut w, 3);
        let token = w
            .cache_reservation_challenge(|| "new-allocation".into())
            .unwrap();
        w.acknowledge_cache_reservations(Some(&token));
        assert_eq!(w.scheduled_bytes, 8);
    }

    #[test]
    fn cache_fence_anonymous_rollback_revokes_existing_confirmation() {
        let mut w = fence_worker();
        enroll_test_block(&mut w, 1);
        let token = w
            .cache_reservation_challenge(|| "before-rollback".into())
            .unwrap();
        w.schedule_bytes(16);
        w.unschedule_bytes(16);
        w.acknowledge_cache_reservations(Some(&token));
        assert_eq!(w.scheduled_bytes, 4);
    }

    #[test]
    fn cache_fence_dropped_responses_redeliver_same_batch_and_ack_once() {
        let mut w = fence_worker();
        enroll_test_block(&mut w, 1);
        let first = w
            .cache_reservation_challenge_at(|| "first".into(), 100)
            .unwrap();
        enroll_test_block(&mut w, 2);
        // The first two responses are lost; neither can enroll block 2 in F1.
        for now in [200, 300] {
            assert_eq!(
                w.cache_reservation_challenge_at(|| panic!("retry must retain token"), now),
                Some(first.clone())
            );
        }
        w.acknowledge_cache_reservations(Some(&first));
        assert_eq!(w.scheduled_bytes, 4);
        // Ack was processed but its response was lost: retry F1 is harmless.
        w.acknowledge_cache_reservations(Some(&first));
        assert_eq!(w.scheduled_bytes, 4);
        let next = w
            .cache_reservation_challenge_at(|| "second".into(), 400)
            .unwrap();
        w.acknowledge_cache_reservations(Some(&first));
        assert_eq!(w.scheduled_bytes, 4);
        w.acknowledge_cache_reservations(Some(&next));
        let events = w.take_cache_reservation_events();
        assert_eq!(events.confirmed, 2);
        assert_eq!(events.released_bytes, 8);
        assert_eq!(w.take_cache_reservation_events().confirmed, 0);
    }

    #[test]
    fn cache_fence_unacked_timeout_preserves_credit_and_allows_next_batch() {
        let mut w = fence_worker();
        enroll_test_block(&mut w, 1);
        let first = w
            .cache_reservation_challenge_at(|| "first".into(), 100)
            .unwrap();
        enroll_test_block(&mut w, 2);
        let next = w
            .cache_reservation_challenge_at(
                || "second".into(),
                100 + WorkerInfo::CACHE_RESERVATION_ACK_TIMEOUT_MS,
            )
            .unwrap();
        assert_ne!(next, first);
        assert_eq!(
            w.scheduled_bytes, 8,
            "timeout never frees reservation bytes"
        );
        w.acknowledge_cache_reservations(Some(&first));
        assert_eq!(w.scheduled_bytes, 8);
        w.acknowledge_cache_reservations(Some(&next));
        assert_eq!(w.scheduled_bytes, 4);
        let events = w.take_cache_reservation_events();
        assert_eq!(events.expired, 1);
        assert_eq!(events.skipped["ack_timeout"], 1);
        assert_eq!(events.released_bytes, 4);
    }

    #[test]
    fn cache_fence_revoked_inode_lease_blocks_frozen_ack() {
        let mut w = fence_worker();
        w.schedule_bytes(16);
        w.bind_scheduled_block(1, 16);
        w.settle_scheduled_block(1, 4);
        let lease = Arc::new(());
        w.mark_completed_cache_reservation_with_lease(1, &lease);
        let token = w.cache_reservation_challenge(|| "frozen".into()).unwrap();
        drop(lease); // reopen, tracker expiry, capacity pruning, or term change
        w.acknowledge_cache_reservations(Some(&token));
        assert_eq!(w.scheduled_bytes, 4);
        assert_eq!(w.cache_reservation_observation(0).0, 0);
        assert_eq!(w.take_cache_reservation_events().skipped["invalidated"], 1);
    }

    #[test]
    fn cache_fence_worker_reinsert_carries_anonymous_epoch_and_reclaim_history() {
        let mut w = fence_worker();
        w.schedule_bytes(16);
        w.available -= 4;
        w.reclaim_scheduled_from_heartbeat(1024);
        let mut next = fence_worker();
        next.carry_scheduled_from(&mut w);
        next.bind_scheduled_block(1, 16);
        next.settle_scheduled_block(1, 4);
        next.mark_completed_cache_reservation(1);
        assert!(next
            .cache_reservation_challenge(|| "unsafe".into())
            .is_none());
        assert_eq!(next.scheduled_bytes, 4);
    }
}
