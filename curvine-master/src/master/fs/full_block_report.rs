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

use curvine_model::BlockReportList;
use log::warn;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// How long a partial session or a completed round is remembered.
const FULL_BLOCK_REPORT_TTL_MS: u64 = 60 * 60 * 1000;

/// Block ids accumulated for one worker's in-progress full report.
struct Session {
    report_id: u64,
    total_len: u64,
    update_time_ms: u64,
    reported_blocks: HashSet<i64>,
    inventory_slot: Option<ReportInventorySlot>,
}

/// The last round whose completing batch was accepted for a worker.
struct Round {
    report_id: u64,
    time_ms: u64,
    /// False while the master is still applying the completing batch. The round
    /// only counts as done once that succeeded, so a failed attempt can be retried.
    done: bool,
}

/// Reserves one bounded full-report inventory from its first batch through
/// reconciliation. The slot moves from Session into the completed inventory,
/// which remains owned by the asynchronous job until it finishes.
struct ReportInventorySlot(Arc<AtomicUsize>);

impl ReportInventorySlot {
    fn try_acquire(counter: &Arc<AtomicUsize>, limit: usize) -> Option<Self> {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current >= limit {
                return None;
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(Self(counter.clone())),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for ReportInventorySlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A completed inventory keeps its bounded slot while it is owned by an async
/// reconcile job, including after a worker restart invalidates tracker state.
pub(super) struct FullReportInventory {
    blocks: HashSet<i64>,
    inventory_slot: Option<ReportInventorySlot>,
}

impl FullReportInventory {
    pub(super) fn blocks(&self) -> &HashSet<i64> {
        &self.blocks
    }

    pub(super) fn into_blocks(self) -> HashSet<i64> {
        self.blocks
    }

    pub(super) fn without_slot(blocks: HashSet<i64>) -> Self {
        Self {
            blocks,
            inventory_slot: None,
        }
    }
}

/// Outcome of folding one full report batch into the master's state.
pub(super) enum FullReportProgress {
    /// Incremental report, or a full report that is still missing batches.
    Partial,
    /// The batch completed the report; holds every reported block id. The caller
    /// must call `finish` or `abort` once it has processed the report.
    Complete(FullReportInventory),
    /// The batch belongs to a round that was already processed successfully.
    Duplicate,
    /// The batch belongs to a round whose completing batch is still being processed.
    InFlight,
    /// The tracker has reached its configured limit of retained report inventories.
    CapacityExceeded,
}

#[derive(Default)]
struct State {
    sessions: HashMap<u32, Session>,
    rounds: HashMap<u32, Round>,
}

/// Tracks full block reports per worker.
///
/// A worker sends a full report as several batches that share a `report_id`. The
/// master accumulates block ids until `total_len` is reached, then reconciles. If
/// the response to the completing batch is lost the worker resends it, so a round
/// that was processed is remembered and its resent batches are ignored.
/// Workers that send no `report_id` (0) keep the accumulate-only behavior.
#[derive(Clone)]
pub(super) struct FullReportTracker {
    state: Arc<Mutex<State>>,
    max_inflight_inventories: Option<usize>,
    inflight_inventories: Arc<AtomicUsize>,
}

impl Default for FullReportTracker {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            max_inflight_inventories: None,
            inflight_inventories: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl FullReportTracker {
    pub(super) fn with_inventory_limit(limit: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            max_inflight_inventories: Some(limit),
            inflight_inventories: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn collect(&self, list: &BlockReportList, now: u64) -> FullReportProgress {
        if !list.full_report {
            return FullReportProgress::Partial;
        }

        let mut guard = self.state.lock();
        let state = &mut *guard;
        state.rounds.retain(|_, round| {
            !round.done || now.saturating_sub(round.time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        });
        state.sessions.retain(|_, session| {
            now.saturating_sub(session.update_time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        });

        // A batch of a round whose completing batch was already accepted was resent
        // after its response was lost. Accepting it as a new batch would open a
        // session holding only this batch.
        if list.report_id != 0 {
            if let Some(round) = state.rounds.get(&list.worker_id) {
                if round.report_id == list.report_id {
                    return if round.done {
                        FullReportProgress::Duplicate
                    } else {
                        FullReportProgress::InFlight
                    };
                }
            }
        }

        let sessions = &mut state.sessions;
        let rounds = &mut state.rounds;
        // A different round id means the worker started a new full report, so the
        // blocks accumulated for the previous round no longer apply.
        if list.report_id != 0
            && sessions
                .get(&list.worker_id)
                .is_some_and(|s| s.report_id != 0 && s.report_id != list.report_id)
        {
            sessions.remove(&list.worker_id);
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = sessions.entry(list.worker_id) {
            let inventory_slot = match self.max_inflight_inventories {
                Some(limit) => {
                    match ReportInventorySlot::try_acquire(&self.inflight_inventories, limit) {
                        Some(slot) => Some(slot),
                        None => return FullReportProgress::CapacityExceeded,
                    }
                }
                None => None,
            };

            entry.insert(Session {
                report_id: list.report_id,
                total_len: list.total_len,
                update_time_ms: now,
                // Avoid trusting the worker's total_len as an allocation
                // hint. The bounded slot limits concurrent inventories;
                // gradual growth follows the actual received block ids.
                reported_blocks: HashSet::with_capacity((list.total_len as usize).min(4096)),
                inventory_slot,
            });
        }

        let session = sessions.get_mut(&list.worker_id).expect("session inserted");

        if session.total_len != list.total_len {
            warn!(
                "full block report for worker {} restarted because total_len changed from {} to {}; discarding {} accumulated block ids",
                list.worker_id,
                session.total_len,
                list.total_len,
                session.reported_blocks.len()
            );
            session.total_len = list.total_len;
            session.reported_blocks.clear();
            session
                .reported_blocks
                .reserve((list.total_len as usize).min(4096));
        }
        session.update_time_ms = now;

        for block in &list.blocks {
            session.reported_blocks.insert(block.id);
        }

        if (session.reported_blocks.len() as u64) < session.total_len {
            return FullReportProgress::Partial;
        }

        match sessions.remove(&list.worker_id) {
            Some(session) => {
                rounds.insert(
                    list.worker_id,
                    Round {
                        report_id: list.report_id,
                        time_ms: now,
                        done: false,
                    },
                );
                FullReportProgress::Complete(FullReportInventory {
                    blocks: session.reported_blocks,
                    inventory_slot: session.inventory_slot,
                })
            }
            None => FullReportProgress::Partial,
        }
    }

    /// The completing batch was processed successfully.
    pub(super) fn finish(&self, worker_id: u32, report_id: u64, now: u64) {
        if let Some(round) = self.state.lock().rounds.get_mut(&worker_id) {
            if round.report_id == report_id {
                round.done = true;
                round.time_ms = now;
            }
        }
    }

    /// Processing the completing batch failed. Put the accumulated block ids back
    /// so the worker's retry completes the round again and the work is redone.
    pub(super) fn abort(
        &self,
        worker_id: u32,
        report_id: u64,
        total_len: u64,
        inventory: FullReportInventory,
        now: u64,
    ) {
        let mut state = self.state.lock();
        let matching_round = if state
            .rounds
            .get(&worker_id)
            .is_some_and(|r| r.report_id == report_id && !r.done)
        {
            state.rounds.remove(&worker_id).is_some()
        } else {
            false
        };
        let inventory_slot = if matching_round {
            inventory.inventory_slot
        } else {
            None
        };
        let reported_blocks = inventory.blocks;
        // Do not clobber a session the worker already opened for a newer round.
        state.sessions.entry(worker_id).or_insert(Session {
            report_id,
            total_len,
            update_time_ms: now,
            reported_blocks,
            inventory_slot,
        });
    }

    pub(super) fn completed_round(&self, worker_id: u32) -> Option<u64> {
        let state = self.state.lock();
        state
            .rounds
            .get(&worker_id)
            .filter(|r| r.done && r.report_id != 0)
            .map(|r| r.report_id)
    }

    pub(super) fn open_sessions(&self) -> usize {
        self.state.lock().sessions.len()
    }

    /// Forget everything about the worker, e.g. when it registers again.
    pub(super) fn reset(&self, worker_id: u32) {
        let mut state = self.state.lock();
        state.sessions.remove(&worker_id);
        state.rounds.remove(&worker_id);
    }

    /// Whether a full report has batches still in flight for this worker. A
    /// follower heartbeat must not request a second snapshot between batches;
    /// the sender preserves FIFO order and will finish the open round or mark
    /// the peer stale when delivery fails.
    pub(super) fn has_open_session(&self, worker_id: u32, now: u64) -> bool {
        let mut state = self.state.lock();
        if state.sessions.get(&worker_id).is_some_and(|session| {
            now.saturating_sub(session.update_time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        }) {
            return true;
        }
        state.sessions.remove(&worker_id);
        false
    }

    /// An incremental report arrived while a full report was in progress, so the
    /// blocks accumulated so far can no longer be trusted.
    pub(super) fn invalidate_session(&self, worker_id: u32, now: u64) {
        let mut state = self.state.lock();
        // Drop an in-flight inventory immediately. The next full report must
        // start clean, and a bounded tracker must not reserve capacity for a
        // snapshot that can no longer complete.
        if state.sessions.get(&worker_id).is_some_and(|session| {
            now.saturating_sub(session.update_time_ms) <= FULL_BLOCK_REPORT_TTL_MS
        }) {
            state.sessions.remove(&worker_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{BlockReportInfo, BlockReportStatus, StorageType};

    const T0: u64 = 1_000;

    fn batch(worker_id: u32, report_id: u64, total_len: u64, ids: &[i64]) -> BlockReportList {
        BlockReportList {
            cluster_id: "curvine".into(),
            worker_id,
            full_report: true,
            total_len,
            report_id,
            blocks: ids
                .iter()
                .map(|id| {
                    BlockReportInfo::new(*id, BlockReportStatus::Finalized, StorageType::Disk, 1)
                })
                .collect(),
        }
    }

    fn complete(progress: FullReportProgress) -> HashSet<i64> {
        complete_inventory(progress).into_blocks()
    }

    fn complete_inventory(progress: FullReportProgress) -> FullReportInventory {
        match progress {
            FullReportProgress::Complete(inventory) => inventory,
            _ => panic!("expected the report to complete"),
        }
    }

    #[test]
    fn resent_final_batch_is_in_flight_until_finished_then_duplicate() {
        let tracker = FullReportTracker::default();
        assert!(matches!(
            tracker.collect(&batch(1, 7, 3, &[1, 2]), T0),
            FullReportProgress::Partial
        ));
        let blocks = complete(tracker.collect(&batch(1, 7, 3, &[3]), T0));
        assert_eq!(blocks, HashSet::from([1, 2, 3]));

        // The first attempt is still being applied: a resend must not be acked.
        assert!(matches!(
            tracker.collect(&batch(1, 7, 3, &[3]), T0),
            FullReportProgress::InFlight
        ));

        tracker.finish(1, 7, T0);
        assert!(matches!(
            tracker.collect(&batch(1, 7, 3, &[3]), T0),
            FullReportProgress::Duplicate
        ));
    }

    #[test]
    fn open_session_is_visible_until_report_ttl_expires() {
        let tracker = FullReportTracker::default();
        assert!(matches!(
            tracker.collect(&batch(1, 7, 3, &[1]), T0),
            FullReportProgress::Partial
        ));
        assert!(tracker.has_open_session(1, T0));
        assert!(!tracker.has_open_session(1, T0 + FULL_BLOCK_REPORT_TTL_MS + 1));
        assert!(!tracker.has_open_session(1, T0 + FULL_BLOCK_REPORT_TTL_MS + 1));
    }

    #[test]
    fn failed_processing_is_redone_by_the_resent_batch() {
        let tracker = FullReportTracker::default();
        tracker.collect(&batch(1, 7, 3, &[1, 2]), T0);
        let blocks = complete(tracker.collect(&batch(1, 7, 3, &[3]), T0));

        // Reconcile submission or the metadata write failed.
        tracker.abort(1, 7, 3, FullReportInventory::without_slot(blocks), T0);

        // The resend must complete the round again with every block, not just
        // the resent batch, and must not be acknowledged as a duplicate.
        let blocks = complete(tracker.collect(&batch(1, 7, 3, &[3]), T0));
        assert_eq!(blocks, HashSet::from([1, 2, 3]));

        tracker.finish(1, 7, T0);
        assert!(matches!(
            tracker.collect(&batch(1, 7, 3, &[3]), T0),
            FullReportProgress::Duplicate
        ));
    }

    #[test]
    fn abort_does_not_clobber_a_newer_round() {
        let tracker = FullReportTracker::default();
        let blocks = complete(tracker.collect(&batch(1, 7, 1, &[1]), T0));
        // The worker already started round 8 while round 7 was being processed.
        tracker.collect(&batch(1, 8, 2, &[10]), T0);
        tracker.abort(1, 7, 1, FullReportInventory::without_slot(blocks), T0);

        let blocks = complete(tracker.collect(&batch(1, 8, 2, &[11]), T0));
        assert_eq!(blocks, HashSet::from([10, 11]));
    }

    #[test]
    fn new_round_id_discards_the_partial_previous_round() {
        let tracker = FullReportTracker::default();
        tracker.collect(&batch(1, 1, 2, &[1]), T0);
        assert!(matches!(
            tracker.collect(&batch(1, 2, 2, &[2]), T0),
            FullReportProgress::Partial
        ));
        let blocks = complete(tracker.collect(&batch(1, 2, 2, &[3]), T0));
        assert_eq!(blocks, HashSet::from([2, 3]));
    }

    #[test]
    fn reports_without_an_id_are_never_deduplicated() {
        let tracker = FullReportTracker::default();
        complete(tracker.collect(&batch(1, 0, 1, &[1]), T0));
        tracker.finish(1, 0, T0);
        complete(tracker.collect(&batch(1, 0, 1, &[1]), T0));
    }

    #[test]
    fn reset_forgets_a_finished_round() {
        let tracker = FullReportTracker::default();
        complete(tracker.collect(&batch(1, 7, 1, &[1]), T0));
        tracker.finish(1, 7, T0);
        tracker.reset(1);
        complete(tracker.collect(&batch(1, 7, 1, &[1]), T0));
    }

    #[test]
    fn finished_round_expires_after_the_ttl() {
        let tracker = FullReportTracker::default();
        complete(tracker.collect(&batch(1, 7, 1, &[1]), T0));
        tracker.finish(1, 7, T0);
        let later = T0 + FULL_BLOCK_REPORT_TTL_MS + 1;
        complete(tracker.collect(&batch(1, 7, 1, &[1]), later));
    }

    #[test]
    fn workers_are_tracked_independently() {
        let tracker = FullReportTracker::default();
        complete(tracker.collect(&batch(1, 7, 1, &[1]), T0));
        tracker.finish(1, 7, T0);
        complete(tracker.collect(&batch(2, 7, 1, &[1]), T0));
    }

    #[test]
    fn bounded_inventory_slots_cover_partial_and_reconciling_reports() {
        let tracker = FullReportTracker::with_inventory_limit(1);
        assert!(matches!(
            tracker.collect(&batch(1, 7, 2, &[1]), T0),
            FullReportProgress::Partial
        ));
        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::CapacityExceeded
        ));

        let inventory = complete_inventory(tracker.collect(&batch(1, 7, 2, &[3]), T0));
        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::CapacityExceeded
        ));
        tracker.finish(1, 7, T0);
        drop(inventory);
        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::Complete(_)
        ));
    }

    #[test]
    fn bounded_inventory_abort_keeps_the_slot_for_a_retry() {
        let tracker = FullReportTracker::with_inventory_limit(1);
        let inventory = complete_inventory(tracker.collect(&batch(1, 7, 1, &[1]), T0));
        tracker.abort(1, 7, 1, inventory, T0);

        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::CapacityExceeded
        ));
        let inventory = complete_inventory(tracker.collect(&batch(1, 7, 1, &[1]), T0));
        assert_eq!(inventory.blocks(), &HashSet::from([1]));
        tracker.finish(1, 7, T0);
        drop(inventory);
        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::Complete(_)
        ));
    }

    #[test]
    fn bounded_inventory_slot_covers_reports_without_an_id() {
        let tracker = FullReportTracker::with_inventory_limit(1);
        let inventory = complete_inventory(tracker.collect(&batch(1, 0, 1, &[1]), T0));
        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::CapacityExceeded
        ));
        drop(inventory);
        assert!(matches!(
            tracker.collect(&batch(2, 8, 1, &[2]), T0),
            FullReportProgress::Complete(_)
        ));
    }

    #[test]
    fn incremental_report_invalidates_the_partial_session() {
        let tracker = FullReportTracker::default();
        tracker.collect(&batch(1, 7, 2, &[1]), T0);
        tracker.invalidate_session(1, T0);

        // The invalidated session is dropped, so accumulation starts over.
        assert!(matches!(
            tracker.collect(&batch(1, 7, 2, &[2]), T0),
            FullReportProgress::Partial
        ));
        let blocks = complete(tracker.collect(&batch(1, 7, 2, &[1]), T0));
        assert_eq!(blocks, HashSet::from([1, 2]));
    }
}
