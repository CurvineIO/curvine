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

use curvine_model::StorageType;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Bound memory used for reports whose inode is not present in the local FSM yet.
pub(super) const MAX_DEFERRED_FOLLOWER_REPORTS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DeferredBlockReport {
    pub worker_id: u32,
    pub block_id: i64,
    pub storage_type: StorageType,
    pub remove_if_missing: bool,
    pub leader_contact_sequence: u64,
}

#[derive(Default)]
struct State {
    term: Option<u64>,
    workers: HashSet<u32>,
    /// Whether each currently known worker has sent a complete full report in
    /// its current registration session.
    full_report_complete: HashSet<u32>,
    deferred: HashMap<(u32, i64), DeferredBlockReport>,
    /// Number of deferred workers per block, for constant-time read gating.
    deferred_by_block: HashMap<i64, usize>,
    /// The worker must complete another full report before its locations can be
    /// used, for example after a bounded-queue overflow.
    needs_full_report: HashSet<u32>,
}

impl State {
    fn remove_block_index(index: &mut HashMap<i64, usize>, block_id: i64) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) = index.entry(block_id) {
            if *entry.get() == 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }

    fn retain_deferred(&mut self, mut keep: impl FnMut(u32, i64) -> bool) {
        let index = &mut self.deferred_by_block;
        self.deferred.retain(|&(worker_id, block_id), _| {
            if keep(worker_id, block_id) {
                true
            } else {
                Self::remove_block_index(index, block_id);
                false
            }
        });
    }

    fn remove_deferred(&mut self, worker_id: u32, block_id: i64) {
        if self.deferred.remove(&(worker_id, block_id)).is_some() {
            Self::remove_block_index(&mut self.deferred_by_block, block_id);
        }
    }
}

/// A borrowed readiness view keeps all checks under one state lock per read.
pub(super) struct FollowerBlockReadiness<'a>(&'a State);

impl FollowerBlockReadiness<'_> {
    pub(super) fn worker_ready(&self, worker_id: u32) -> bool {
        self.0.workers.contains(&worker_id)
            && self.0.full_report_complete.contains(&worker_id)
            && !self.0.needs_full_report.contains(&worker_id)
    }

    pub(super) fn has_deferred_block(&self, block_id: i64) -> bool {
        self.0.deferred_by_block.contains_key(&block_id)
    }
}

/// Local follower location-view state. These rows are soft state and are never
/// journaled or allowed to trigger worker commands or replication work.
#[derive(Clone, Default)]
pub(super) struct FollowerBlockReportState(Arc<Mutex<State>>);

impl FollowerBlockReportState {
    /// A new Raft term is a new passive worker-view epoch. Discard report state
    /// from the previous leader before accepting or serving this term's view.
    pub(super) fn reset_for_term(&self, term: u64) -> Vec<u32> {
        let mut state = self.0.lock();
        if state.term == Some(term) {
            return Vec::new();
        }
        let workers: Vec<_> = state.workers.drain().collect();
        state.full_report_complete.clear();
        state.deferred.clear();
        state.deferred_by_block.clear();
        state.needs_full_report.clear();
        state.needs_full_report.extend(workers.iter().copied());
        state.term = Some(term);
        workers
    }

    pub(super) fn observe_worker(&self, worker_id: u32, restarted: bool) {
        let mut state = self.0.lock();
        state.workers.insert(worker_id);
        if restarted {
            state.retain_deferred(|id, _| id != worker_id);
            state.needs_full_report.remove(&worker_id);
            state.full_report_complete.remove(&worker_id);
        }
    }

    pub(super) fn forget_worker(&self, worker_id: u32) {
        let mut state = self.0.lock();
        state.workers.remove(&worker_id);
        state.full_report_complete.remove(&worker_id);
        state.needs_full_report.remove(&worker_id);
        state.retain_deferred(|id, _| id != worker_id);
    }

    pub(super) fn retain_workers(&self, worker_ids: &HashSet<u32>) -> Vec<u32> {
        let mut state = self.0.lock();
        let removed: Vec<u32> = state.workers.difference(worker_ids).copied().collect();
        state
            .workers
            .retain(|worker_id| worker_ids.contains(worker_id));
        state
            .full_report_complete
            .retain(|worker_id| worker_ids.contains(worker_id));
        state
            .needs_full_report
            .retain(|worker_id| worker_ids.contains(worker_id));
        state.retain_deferred(|worker_id, _| worker_ids.contains(&worker_id));
        removed
    }

    /// Enqueue or replace one unresolved add. Replacing the same worker/block
    /// pair keeps the queue bounded and preserves the latest worker report.
    pub(super) fn defer(&self, report: DeferredBlockReport) -> bool {
        let mut state = self.0.lock();
        let key = (report.worker_id, report.block_id);
        if !state.deferred.contains_key(&key)
            && state.deferred.len() >= MAX_DEFERRED_FOLLOWER_REPORTS
        {
            state.workers.insert(report.worker_id);
            state.full_report_complete.remove(&report.worker_id);
            state.needs_full_report.insert(report.worker_id);
            return false;
        }

        state.workers.insert(report.worker_id);
        if let Some(existing) = state.deferred.get_mut(&key) {
            existing.storage_type = report.storage_type;
            existing.remove_if_missing |= report.remove_if_missing;
            existing.leader_contact_sequence = report.leader_contact_sequence;
        } else {
            state.deferred.insert(key, report);
            *state.deferred_by_block.entry(report.block_id).or_default() += 1;
        }
        true
    }

    pub(super) fn remove_deferred(&self, worker_id: u32, block_id: i64) {
        self.0.lock().remove_deferred(worker_id, block_id);
    }

    /// Remove stale entries for a worker after its full report completes.
    pub(super) fn retain_reported_blocks(&self, worker_id: u32, reported: &HashSet<i64>) {
        self.0
            .lock()
            .retain_deferred(|id, block_id| id != worker_id || reported.contains(&block_id));
    }

    pub(super) fn deferred_snapshot(&self) -> Vec<DeferredBlockReport> {
        self.0.lock().deferred.values().copied().collect()
    }

    pub(super) fn mark_deferred_resolved(&self, worker_id: u32, block_id: i64) {
        self.remove_deferred(worker_id, block_id);
    }

    pub(super) fn mark_full_report_complete(&self, worker_id: u32) {
        let mut state = self.0.lock();
        state.workers.insert(worker_id);
        state.needs_full_report.remove(&worker_id);
        state.full_report_complete.insert(worker_id);
    }

    /// Keep this worker's locations out of follower reads until a new full
    /// report has been completely applied. This also fails closed if a report
    /// batch errors after applying only part of its location updates.
    pub(super) fn require_full_report(&self, worker_id: u32) {
        let mut state = self.0.lock();
        state.workers.insert(worker_id);
        state.full_report_complete.remove(&worker_id);
        state.needs_full_report.insert(worker_id);
    }

    pub(super) fn requires_full_report(&self, worker_id: u32) -> bool {
        let state = self.0.lock();
        !state.full_report_complete.contains(&worker_id)
            || state.needs_full_report.contains(&worker_id)
    }

    pub(super) fn with_readiness<T>(
        &self,
        check: impl FnOnce(&FollowerBlockReadiness<'_>) -> T,
    ) -> T {
        let state = self.0.lock();
        check(&FollowerBlockReadiness(&state))
    }

    #[cfg(test)]
    pub(super) fn worker_ready(&self, worker_id: u32) -> bool {
        self.with_readiness(|state| state.worker_ready(worker_id))
    }

    #[cfg(test)]
    pub(super) fn has_deferred_block(&self, block_id: i64) -> bool {
        self.with_readiness(|state| state.has_deferred_block(block_id))
    }

    #[cfg(test)]
    pub(super) fn is_ready(&self) -> bool {
        let state = self.0.lock();
        state.deferred.is_empty()
            && state.needs_full_report.is_empty()
            && state
                .workers
                .iter()
                .all(|worker_id| state.full_report_complete.contains(worker_id))
    }

    #[cfg(test)]
    pub(super) fn deferred_len(&self) -> usize {
        self.0.lock().deferred.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deferred(worker_id: u32, block_id: i64) -> DeferredBlockReport {
        DeferredBlockReport {
            worker_id,
            block_id,
            storage_type: StorageType::Disk,
            remove_if_missing: true,
            leader_contact_sequence: 1,
        }
    }

    #[test]
    fn registration_requires_full_report_and_restart_clears_old_state() {
        let state = FollowerBlockReportState::default();
        assert!(state.is_ready());
        state.observe_worker(7, false);
        assert!(!state.is_ready());
        state.mark_full_report_complete(7);
        assert!(state.is_ready());

        state.observe_worker(7, true);
        assert!(!state.is_ready());
        assert!(!state.worker_ready(7));
    }

    #[test]
    fn a_new_full_report_invalidates_the_previous_location_snapshot() {
        let state = FollowerBlockReportState::default();
        state.observe_worker(7, false);
        state.mark_full_report_complete(7);
        assert!(state.worker_ready(7));

        state.require_full_report(7);
        assert!(!state.worker_ready(7));

        state.mark_full_report_complete(7);
        assert!(state.worker_ready(7));
    }

    #[test]
    fn deferred_reports_are_deduplicated_and_gate_their_block() {
        let state = FollowerBlockReportState::default();
        state.observe_worker(7, false);
        assert!(state.defer(deferred(7, 42)));
        assert!(state.defer(deferred(7, 42)));
        assert_eq!(state.deferred_len(), 1);
        assert!(state.has_deferred_block(42));
        assert!(!state.is_ready());
        state.mark_full_report_complete(7);
        assert!(state.worker_ready(7));
        assert!(state.has_deferred_block(42));

        let mut later_writing = deferred(7, 42);
        later_writing.remove_if_missing = false;
        later_writing.leader_contact_sequence = 2;
        assert!(state.defer(later_writing));
        let latest = state.deferred_snapshot()[0];
        assert!(latest.remove_if_missing);
        assert_eq!(latest.leader_contact_sequence, 2);

        state.mark_deferred_resolved(7, 42);
        assert!(!state.has_deferred_block(42));
        assert!(state.is_ready());
    }

    #[test]
    fn deferred_block_index_tracks_each_worker_and_all_removal_paths() {
        let state = FollowerBlockReportState::default();
        state.reset_for_term(1);
        assert!(state.defer(deferred(7, 42)));
        assert!(state.defer(deferred(7, 42))); // replacement does not add a count
        assert!(state.defer(deferred(8, 42)));
        assert!(state.defer(deferred(7, 43)));

        state.remove_deferred(7, 42);
        assert!(state.has_deferred_block(42)); // worker 8 still defers it
        state.retain_reported_blocks(8, &HashSet::new());
        assert!(!state.has_deferred_block(42));
        assert!(state.has_deferred_block(43));

        assert!(state.defer(deferred(8, 44)));
        assert!(state.defer(deferred(9, 46)));
        state.observe_worker(7, true);
        assert!(!state.has_deferred_block(43));
        state.retain_workers(&HashSet::from([8]));
        assert!(state.has_deferred_block(44));
        assert!(!state.has_deferred_block(46));
        state.forget_worker(8);
        assert!(!state.has_deferred_block(44));

        assert!(state.defer(deferred(9, 45)));
        state.reset_for_term(2);
        assert!(!state.has_deferred_block(45));
    }

    #[test]
    fn worker_removal_drops_only_its_passive_state() {
        let state = FollowerBlockReportState::default();
        state.observe_worker(7, false);
        state.observe_worker(8, false);
        state.mark_full_report_complete(7);
        state.mark_full_report_complete(8);
        assert!(state.is_ready());

        state.forget_worker(7);
        assert!(state.is_ready());
        assert!(state.worker_ready(8));
    }

    #[test]
    fn queue_overflow_fails_closed_until_a_full_report() {
        let state = FollowerBlockReportState::default();
        state.observe_worker(7, false);
        state.mark_full_report_complete(7);
        for block_id in 0..MAX_DEFERRED_FOLLOWER_REPORTS as i64 {
            assert!(state.defer(deferred(7, block_id)));
        }

        assert!(!state.defer(deferred(7, MAX_DEFERRED_FOLLOWER_REPORTS as i64)));
        assert!(!state.worker_ready(7));
        state.mark_full_report_complete(7);
        assert!(state.worker_ready(7));
        assert!(!state.is_ready());
    }

    #[test]
    fn a_new_term_drops_reports_from_the_previous_leader_epoch() {
        let state = FollowerBlockReportState::default();
        assert!(state.reset_for_term(1).is_empty());
        state.observe_worker(7, false);
        assert!(state.defer(deferred(7, 42)));

        assert_eq!(state.reset_for_term(2), vec![7]);
        assert_eq!(state.deferred_len(), 0);
        assert!(state.requires_full_report(7));
    }
}
