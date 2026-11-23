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
use crate::{ReadCacheReservationEvents, ReadCacheSkipReason};
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
    #[serde(skip)]
    pub read_cache_reservation: Option<crate::ReadCacheReservation>,
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
            read_cache_reservation: None,
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
        if bytes > 0 {
            self.discard_read_cache_proof();
        }
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
        if let Some(proof) = &mut self.read_cache_reservation {
            if proof.anonymous_tainted || self.scheduled_blocks.contains_key(&block_id) {
                if proof.anonymous_tainted {
                    proof.events.skip(ReadCacheSkipReason::AnonymousReclaim);
                }
                proof.discard(self.scheduled_seq, false);
            }
        }
        // A rebind of the same block keeps its original sequence: the oldest
        // reservation age is the correct trim priority.
        let entry = self
            .scheduled_blocks
            .entry(block_id)
            .or_insert((self.scheduled_seq, 0));
        entry.1 = entry.1.saturating_add(credit);
        self.scheduled_block_bytes = self.scheduled_block_bytes.saturating_add(credit);
        if self.scheduled_block_bytes == self.scheduled_bytes {
            if let Some(proof) = &mut self.read_cache_reservation {
                proof.anonymous_tainted = false;
            }
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
            if let Some(proof) = &mut self.read_cache_reservation {
                proof.pending.remove(&block_id);
                if let Some(batch) = &mut proof.inflight {
                    batch.entries.remove(&block_id);
                }
            }
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
        if consumed > 0 {
            self.discard_read_cache_proof();
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
            self.discard_read_cache_proof();
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
        if self.worker_session_id == previous.worker_session_id {
            self.read_cache_reservation = previous.read_cache_reservation.take();
        }
    }

    /// Revoke settlement proof without forgetting the accepted capacity sequence.
    pub fn discard_read_cache_proof(&mut self) {
        if let Some(proof) = &mut self.read_cache_reservation {
            proof.discard(
                self.scheduled_seq,
                self.scheduled_bytes > self.scheduled_block_bytes,
            );
        }
    }

    /// Once this session reports sequenced capacity, only a newer sample may
    /// replace it. Legacy sessions without a high-water mark keep normal behavior.
    pub fn accepts_read_cache_capacity(&self, snapshot: Option<u64>) -> bool {
        self.read_cache_reservation.as_ref().is_none_or(|proof| {
            proof.snapshot_seq == 0 || snapshot.is_some_and(|seq| seq > proof.snapshot_seq)
        })
    }

    /// A follower tracks accepted sample ordering but never settles credits or
    /// keeps a former leader's confirmation batch alive.
    pub fn observe_read_cache_capacity(&mut self, snapshot: Option<u64>) {
        self.discard_read_cache_proof();
        if let (Some(proof), Some(sequence)) = (&mut self.read_cache_reservation, snapshot) {
            proof.snapshot_seq = proof.snapshot_seq.max(sequence);
        }
    }

    /// Only new allocations in a known leadership/session may be confirmed.
    pub fn prepare_read_cache_proof(&mut self, epoch: (u64, u64)) {
        let proof = self
            .read_cache_reservation
            .get_or_insert_with(Default::default);
        if proof.epoch != Some(epoch) {
            proof.discard(
                self.scheduled_seq,
                self.scheduled_bytes > self.scheduled_block_bytes,
            );
            proof.epoch = Some(epoch);
        }
    }

    pub fn enroll_read_cache_credit(&mut self, block_id: i64, lease: &std::sync::Arc<()>) {
        let Some(proof) = &mut self.read_cache_reservation else {
            return;
        };
        let Some(&(sequence, _)) = self.scheduled_blocks.get(&block_id) else {
            proof.events.skip(ReadCacheSkipReason::NoCredit);
            return;
        };
        if proof.snapshot_seq == 0
            || sequence <= proof.allocation_floor
            || proof.pending.len() >= crate::READ_CACHE_PENDING_LIMIT
            || proof
                .inflight
                .as_ref()
                .is_some_and(|b| b.entries.contains_key(&block_id))
        {
            // Retries of already-enrolled credits are not skipped work.
            if proof.pending.contains_key(&block_id)
                || proof
                    .inflight
                    .as_ref()
                    .is_some_and(|batch| batch.entries.contains_key(&block_id))
            {
                return;
            }
            if proof.snapshot_seq == 0 {
                proof.events.skip(ReadCacheSkipReason::UnsequencedWorker);
            } else if sequence <= proof.allocation_floor {
                proof.events.skip(ReadCacheSkipReason::AllocationFloor);
            } else if proof.pending.len() >= crate::READ_CACHE_PENDING_LIMIT {
                proof.events.skip(ReadCacheSkipReason::PendingLimit);
            }
            return;
        }
        proof
            .pending
            .entry(block_id)
            .or_insert(crate::read_cache_reservation::CacheCredit {
                sequence,
                lease: std::sync::Arc::downgrade(lease),
            });
    }

    /// Called on the real worker record under the worker-manager lock, never on
    /// a filesystem-info clone. Taking twice cannot re-emit the same events.
    pub fn take_read_cache_reservation_events(&mut self) -> ReadCacheReservationEvents {
        self.read_cache_reservation
            .as_mut()
            .map(|proof| std::mem::take(&mut proof.events))
            .unwrap_or_default()
    }

    /// Called after heartbeat accounting, which must first filter capacity using
    /// accepts_read_cache_capacity. Retransmissions only redeliver unexpired proof;
    /// neither they nor older samples acknowledge a batch or extend its deadline.
    pub fn confirm_read_cache_credit(
        &mut self,
        snapshot: Option<u64>,
        ack: Option<&str>,
        now: u64,
        heartbeat_ms: u64,
        token: impl FnOnce() -> String,
    ) -> Option<String> {
        let mut proof = self.read_cache_reservation.take()?;
        if proof
            .inflight
            .as_ref()
            .is_some_and(|b| now.saturating_sub(b.issued_at) >= b.timeout_ms)
        {
            proof.inflight = None;
            proof.events.expired = proof.events.expired.saturating_add(1);
        }
        let valid = snapshot.is_some_and(|seq| seq > 0) && !self.worker_session_id.is_empty();
        if !valid {
            proof.discard(
                self.scheduled_seq,
                self.scheduled_bytes > self.scheduled_block_bytes,
            );
            self.read_cache_reservation = Some(proof);
            return None;
        }
        let sequence = snapshot.unwrap();
        if sequence <= proof.snapshot_seq {
            let response = if sequence == proof.snapshot_seq {
                proof.inflight.as_ref().map(|b| b.token.clone())
            } else {
                None
            };
            self.read_cache_reservation = Some(proof);
            return response;
        }
        proof.snapshot_seq = sequence;
        if proof
            .inflight
            .as_ref()
            .is_some_and(|b| Some(b.token.as_str()) == ack)
        {
            let batch = proof.inflight.take().unwrap();
            proof.events.confirmed = proof.events.confirmed.saturating_add(1);
            for (id, credit) in batch.entries {
                if credit.sequence > proof.allocation_floor
                    && credit.lease.upgrade().is_some()
                    && self
                        .scheduled_blocks
                        .get(&id)
                        .is_some_and(|v| v.0 == credit.sequence)
                {
                    let before = self.scheduled_bytes;
                    self.settle_scheduled_block(id, 0);
                    proof.events.released_bytes = proof
                        .events
                        .released_bytes
                        .saturating_add(before.saturating_sub(self.scheduled_bytes).max(0));
                } else {
                    proof
                        .events
                        .skip(if credit.sequence <= proof.allocation_floor {
                            ReadCacheSkipReason::AllocationFloor
                        } else if credit.lease.strong_count() == 0 {
                            ReadCacheSkipReason::LeaseRevoked
                        } else {
                            ReadCacheSkipReason::NoCredit
                        });
                }
            }
        }
        if proof.inflight.is_none() {
            let mut entries = BTreeMap::new();
            for _ in 0..crate::READ_CACHE_BATCH_LIMIT {
                let Some((id, credit)) = proof.pending.pop_first() else {
                    break;
                };
                if credit.sequence > proof.allocation_floor
                    && credit.lease.upgrade().is_some()
                    && self
                        .scheduled_blocks
                        .get(&id)
                        .is_some_and(|v| v.0 == credit.sequence)
                {
                    entries.insert(id, credit);
                } else {
                    proof
                        .events
                        .skip(if credit.sequence <= proof.allocation_floor {
                            ReadCacheSkipReason::AllocationFloor
                        } else if credit.lease.strong_count() == 0 {
                            ReadCacheSkipReason::LeaseRevoked
                        } else {
                            ReadCacheSkipReason::NoCredit
                        });
                }
            }
            if !entries.is_empty() {
                proof.events.issued = proof.events.issued.saturating_add(1);
                proof.inflight = Some(crate::read_cache_reservation::CacheBatch {
                    token: token(),
                    issued_at: now,
                    timeout_ms: crate::READ_CACHE_ACK_MIN_MS.max(heartbeat_ms.saturating_mul(5)),
                    entries,
                });
            }
        }
        let response = proof.inflight.as_ref().map(|b| b.token.clone());
        self.read_cache_reservation = Some(proof);
        response
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
            read_cache_reservation: None,
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

#[cfg(test)]
mod read_cache_tests {
    use super::*;
    use std::sync::Arc;
    const M: i64 = 1 << 20;
    fn worker() -> WorkerInfo {
        let mut w = WorkerInfo {
            worker_session_id: "s1".into(),
            ..Default::default()
        };
        w.prepare_read_cache_proof((1, 1));
        assert!(w
            .confirm_read_cache_credit(Some(1), None, 0, 3000, || "unused".into())
            .is_none());
        w
    }
    fn allocate(w: &mut WorkerInfo, id: i64) {
        w.schedule_bytes(16 * M);
        w.bind_scheduled_block(id, 16 * M);
    }
    fn complete(w: &mut WorkerInfo, id: i64, lease: &Arc<()>) {
        w.settle_scheduled_block(id, 4 * M);
        w.enroll_read_cache_credit(id, lease);
    }
    fn beat(w: &mut WorkerInfo, seq: u64, ack: Option<&str>, now: u64) -> Option<String> {
        w.confirm_read_cache_credit(Some(seq), ack, now, 3000, || format!("token-{seq}"))
    }

    #[test]
    fn read_cache_eviction_cancellation_preserves_concurrent_credit_in_both_orders() {
        for c_first in [false, true] {
            let mut w = worker();
            let lease = Arc::new(());
            for id in if c_first { [3, 2] } else { [2, 3] } {
                allocate(&mut w, id);
            }
            complete(&mut w, 2, &lease);
            assert_eq!(w.scheduled_bytes, 20 * M);
            w.reclaim_scheduled_from_heartbeat(w.available); // write 4, evict 4
            let token = beat(&mut w, 2, None, 1).unwrap();
            // Lost response: same token and frozen batch are redelivered.
            assert_eq!(beat(&mut w, 3, None, 2).as_deref(), Some(token.as_str()));
            beat(&mut w, 4, Some(&token), 3);
            assert_eq!(w.scheduled_bytes, 16 * M);
            assert_eq!(w.scheduled_blocks[&3].1, 16 * M);
            assert!(!w.scheduled_blocks.contains_key(&2));
            beat(&mut w, 5, Some(&token), 4);
            assert_eq!(w.scheduled_bytes, 16 * M);
            assert_eq!(
                w.take_read_cache_reservation_events(),
                ReadCacheReservationEvents {
                    issued: 1,
                    confirmed: 1,
                    released_bytes: 4 * M,
                    ..Default::default()
                }
            );
            assert_eq!(w.take_read_cache_reservation_events(), Default::default());
        }
    }

    #[test]
    fn read_cache_prior_heartbeat_and_fully_reclaimed_b_never_double_release() {
        for c_first in [false, true] {
            for complete_first in [false, true] {
                let mut w = worker();
                let lease = Arc::new(());
                for id in if c_first { [3, 2] } else { [2, 3] } {
                    allocate(&mut w, id);
                }
                if complete_first {
                    complete(&mut w, 2, &lease);
                }
                w.reclaim_scheduled_from_heartbeat(w.available + 4 * M);
                if !complete_first {
                    complete(&mut w, 2, &lease);
                }
                let before = (w.scheduled_bytes, w.scheduled_blocks[&3].1);
                assert!(beat(&mut w, 2, None, 1).is_none());
                beat(&mut w, 3, Some("old"), 2);
                assert_eq!((w.scheduled_bytes, w.scheduled_blocks[&3].1), before);
                let events = w.take_read_cache_reservation_events();
                assert_eq!((events.confirmed, events.released_bytes), (0, 0));
            }
        }
    }

    #[test]
    fn read_cache_reclaim_between_challenge_and_ack_abandons_optimization() {
        let mut w = worker();
        let lease = Arc::new(());
        allocate(&mut w, 3);
        allocate(&mut w, 2);
        complete(&mut w, 2, &lease);
        let token = beat(&mut w, 2, None, 1).unwrap();
        w.reclaim_scheduled_from_heartbeat(w.available + 4 * M);
        beat(&mut w, 3, Some(&token), 2);
        assert_eq!(w.scheduled_bytes, 16 * M);
        assert_eq!(w.scheduled_blocks[&3].1, 12 * M);
    }

    #[test]
    fn read_cache_anonymous_reclaim_and_rebind_cannot_gain_new_proof() {
        let mut w = worker();
        let lease = Arc::new(());
        w.schedule_bytes(16 * M);
        w.reclaim_scheduled_from_heartbeat(w.available + 4 * M);
        w.bind_scheduled_block(2, 16 * M);
        complete(&mut w, 2, &lease);
        assert!(beat(&mut w, 2, None, 1).is_none());
        allocate(&mut w, 4);
        complete(&mut w, 4, &lease);
        let old = beat(&mut w, 3, None, 2).unwrap();
        allocate(&mut w, 4); // rebind revokes this and older allocations
        let before = w.scheduled_bytes;
        beat(&mut w, 4, Some(&old), 3);
        assert_eq!(w.scheduled_bytes, before);
        let events = w.take_read_cache_reservation_events();
        assert_eq!(
            events.skipped[ReadCacheSkipReason::AnonymousReclaim as usize],
            1
        );
        assert_eq!(
            events.skipped[ReadCacheSkipReason::AllocationFloor as usize],
            1
        );
        assert_eq!(events.released_bytes, 0);
    }

    #[test]
    fn read_cache_legacy_session_and_leadership_discard_only_proof() {
        for event in 1..6 {
            let mut w = worker();
            let lease = Arc::new(());
            allocate(&mut w, 2);
            complete(&mut w, 2, &lease);
            let token = beat(&mut w, 2, None, 1).unwrap();
            match event {
                1 => {
                    w.confirm_read_cache_credit(None, Some(&token), 2, 3000, || "x".into());
                }
                2 => w.prepare_read_cache_proof((2, 2)),
                3 => w.prepare_read_cache_proof((1, 2)),
                4 => {
                    let mut next = WorkerInfo {
                        worker_session_id: "s2".into(),
                        ..Default::default()
                    };
                    next.carry_scheduled_from(&mut w);
                    w = next;
                    w.prepare_read_cache_proof((1, 1));
                }
                _ => {
                    w.read_cache_reservation = None;
                    w.prepare_read_cache_proof((1, 1));
                }
            }
            beat(&mut w, 3, Some(&token), 3);
            assert_eq!(w.scheduled_bytes, 4 * M, "event={event}");
        }
    }

    #[test]
    fn read_cache_expiry_revocation_deletion_and_long_heartbeat() {
        for event in 0..4 {
            let mut w = worker();
            let lease = Arc::new(());
            allocate(&mut w, 2);
            complete(&mut w, 2, &lease);
            let token = beat(&mut w, 2, None, 1).unwrap();
            let now = match event {
                0 => 60_001, // expiry is checked before matching the ACK
                1 => {
                    drop(lease);
                    2
                }
                2 => {
                    w.settle_scheduled_block(2, 0);
                    2
                }
                _ => {
                    w.expire_scheduled_bytes(u64::MAX, 1);
                    2
                }
            };
            let before = w.scheduled_bytes;
            beat(&mut w, 3, Some(&token), now);
            assert_eq!(w.scheduled_bytes, before);
        }
        let mut w = worker();
        let lease = Arc::new(());
        allocate(&mut w, 2);
        complete(&mut w, 2, &lease);
        let token = w
            .confirm_read_cache_credit(Some(2), None, 0, 60_000, || "slow".into())
            .unwrap();
        w.confirm_read_cache_credit(Some(3), Some(&token), 60_000, 60_000, || "unused".into());
        assert_eq!(w.scheduled_bytes, 0);
    }

    #[test]
    fn read_cache_disabled_keeps_native_accounting_and_bounds_batches() {
        let mut w = WorkerInfo::default();
        let lease = Arc::new(());
        allocate(&mut w, 2);
        complete(&mut w, 2, &lease);
        assert!(beat(&mut w, 1, None, 0).is_none());
        assert!(w.read_cache_reservation.is_none());
        assert_eq!(w.scheduled_bytes, 4 * M);
        assert_eq!(w.take_read_cache_reservation_events(), Default::default());
        let mut w = worker();
        for id in 0..(crate::READ_CACHE_PENDING_LIMIT + 10) {
            allocate(&mut w, id as i64);
            complete(&mut w, id as i64, &lease);
        }
        assert_eq!(
            w.read_cache_reservation.as_ref().unwrap().pending.len(),
            crate::READ_CACHE_PENDING_LIMIT
        );
        beat(&mut w, 2, None, 1);
        assert_eq!(
            w.read_cache_reservation
                .as_ref()
                .unwrap()
                .inflight
                .as_ref()
                .unwrap()
                .entries
                .len(),
            crate::READ_CACHE_BATCH_LIMIT
        );
        let events = w.take_read_cache_reservation_events();
        assert_eq!(events.issued, 1);
        assert_eq!(
            events.skipped[ReadCacheSkipReason::PendingLimit as usize],
            10
        );
    }

    #[test]
    fn read_cache_lost_ack_expires_without_blocking_later_completions() {
        let mut w = worker();
        let lease = Arc::new(());
        allocate(&mut w, 2);
        complete(&mut w, 2, &lease);
        let old = beat(&mut w, 2, None, 1).unwrap();
        allocate(&mut w, 3);
        complete(&mut w, 3, &lease);
        // The first challenge was delivered, but every ACK is lost. Keep its
        // batch immutable until expiry; do not enroll later files in it.
        assert_eq!(beat(&mut w, 3, None, 2), Some(old.clone()));
        assert_eq!(w.scheduled_bytes, 8 * M);
        let next = beat(&mut w, 4, Some(&old), 60_001).unwrap();
        assert_ne!(old, next);
        assert_eq!(w.scheduled_bytes, 8 * M);
        beat(&mut w, 5, Some(&old), 60_002);
        assert_eq!(w.scheduled_bytes, 8 * M);
        beat(&mut w, 6, Some(&next), 60_003);
        assert_eq!(w.scheduled_bytes, 4 * M);
        assert_eq!(w.scheduled_blocks[&2].1, 4 * M);
        assert!(!w.scheduled_blocks.contains_key(&3));
    }

    #[test]
    fn read_cache_replica_confirmation_is_worker_local() {
        let mut first = worker();
        let mut second = worker();
        let lease = Arc::new(());
        for w in [&mut first, &mut second] {
            allocate(w, 2);
            complete(w, 2, &lease);
        }
        let token = beat(&mut first, 2, None, 1).unwrap();
        let other = second
            .confirm_read_cache_credit(Some(2), None, 1, 3000, || "other-worker".into())
            .unwrap();
        // Each worker has its own frozen batch and independently random token.
        beat(&mut second, 3, Some(&token), 2);
        assert_eq!(second.scheduled_bytes, 4 * M);
        beat(&mut first, 3, Some(&other), 2);
        assert_eq!(first.scheduled_bytes, 4 * M);
        beat(&mut first, 4, Some(&token), 3);
        assert_eq!(first.scheduled_bytes, 0);
        assert_eq!(second.scheduled_bytes, 4 * M);
    }
    #[test]
    fn read_cache_identical_retries_redeliver_without_acknowledging() {
        let mut w = worker();
        let lease = Arc::new(());
        allocate(&mut w, 2);
        complete(&mut w, 2, &lease);
        let token = beat(&mut w, 2, None, 1).unwrap();
        allocate(&mut w, 3);
        complete(&mut w, 3, &lease);
        // The transport resends the same serialized request after losing a
        // response. It must neither discard this batch nor enroll block 3 in it.
        assert_eq!(beat(&mut w, 2, None, 20), Some(token.clone()));
        assert_eq!(beat(&mut w, 2, Some(&token), 30), Some(token.clone()));
        assert!(beat(&mut w, 1, Some(&token), 31).is_none());
        assert_eq!(w.scheduled_bytes, 8 * M);
        assert_eq!(
            w.read_cache_reservation
                .as_ref()
                .unwrap()
                .inflight
                .as_ref()
                .unwrap()
                .issued_at,
            1
        );
        let next = beat(&mut w, 3, Some(&token), 40).unwrap();
        assert_ne!(next, token);
        assert_eq!(w.scheduled_bytes, 4 * M);
        // Lose the ACK response, which contained the next batch's challenge.
        assert_eq!(beat(&mut w, 3, Some(&token), 41), Some(next.clone()));
        assert_eq!(beat(&mut w, 3, Some(&next), 42), Some(next.clone()));
        assert_eq!(w.scheduled_blocks[&3].1, 4 * M);
        beat(&mut w, 4, Some(&next), 43);
        assert_eq!(w.scheduled_bytes, 0);
        assert_eq!(
            w.take_read_cache_reservation_events(),
            ReadCacheReservationEvents {
                issued: 2,
                confirmed: 2,
                released_bytes: 8 * M,
                ..Default::default()
            }
        );
    }

    #[test]
    fn read_cache_duplicate_expiry_does_not_renew_or_freeze_a_batch() {
        let mut w = worker();
        let lease = Arc::new(());
        allocate(&mut w, 2);
        complete(&mut w, 2, &lease);
        let token = beat(&mut w, 2, None, 1).unwrap();
        allocate(&mut w, 3);
        complete(&mut w, 3, &lease);
        assert_eq!(beat(&mut w, 2, None, 60_000), Some(token.clone()));
        assert!(beat(&mut w, 2, Some(&token), 60_001).is_none());
        assert!(beat(&mut w, 2, None, 60_002).is_none());
        assert!(w
            .read_cache_reservation
            .as_ref()
            .unwrap()
            .inflight
            .is_none());
        assert_eq!(w.scheduled_bytes, 8 * M);
        let next = beat(&mut w, 3, Some(&token), 60_003).unwrap();
        beat(&mut w, 4, Some(&next), 60_004);
        assert_eq!(w.scheduled_bytes, 4 * M);
        assert_eq!(w.scheduled_blocks[&2].1, 4 * M);
        assert_eq!(
            w.take_read_cache_reservation_events(),
            ReadCacheReservationEvents {
                issued: 2,
                confirmed: 1,
                released_bytes: 4 * M,
                expired: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn read_cache_metric_events_report_revocation_and_unavailable_credit() {
        for frozen in [false, true] {
            let mut w = worker();
            let lease = Arc::new(());
            allocate(&mut w, 1);
            complete(&mut w, 1, &lease);
            let token = if frozen {
                beat(&mut w, 2, None, 1)
            } else {
                None
            };
            drop(lease);
            beat(&mut w, 3, token.as_deref(), 2);
            assert_eq!(w.scheduled_bytes, 4 * M);
            let events = w.take_read_cache_reservation_events();
            assert_eq!(
                events.skipped[ReadCacheSkipReason::LeaseRevoked as usize],
                1
            );
            assert_eq!(events.released_bytes, 0);
            assert_eq!(events.confirmed, i64::from(frozen));
        }
        let mut w = WorkerInfo::default();
        let lease = Arc::new(());
        w.prepare_read_cache_proof((1, 1));
        allocate(&mut w, 1);
        w.enroll_read_cache_credit(1, &lease);
        w.enroll_read_cache_credit(2, &lease);
        let events = w.take_read_cache_reservation_events();
        assert_eq!(
            events.skipped[ReadCacheSkipReason::UnsequencedWorker as usize],
            1
        );
        assert_eq!(events.skipped[ReadCacheSkipReason::NoCredit as usize], 1);
        assert_eq!(w.scheduled_bytes, 16 * M);
    }
}
