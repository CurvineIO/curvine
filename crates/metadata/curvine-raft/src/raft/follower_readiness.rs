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

use crate::raft::RoleState;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Shared observation of Raft leadership contact and metadata replay progress.
///
/// Raft publishes role, term, leader, local commit, and verified leader-commit
/// observations. The journal FSM separately publishes its actual applied
/// index after replay completes. These watermarks are not inferred from each
/// other.
#[derive(Clone, Default)]
pub struct FollowerReadinessMonitor(Arc<Mutex<FollowerReadinessState>>);

struct FollowerReadinessState {
    role: RoleState,
    term: u64,
    leader_id: u64,
    leadership_epoch: u64,
    commit_index: u64,
    leader_commit_index: Option<u64>,
    leader_commit_contact_proven: bool,
    applied_index: Option<u64>,
    replay_ready: bool,
    leader_contact: Option<LeaderContact>,
    leader_contact_sequence: u64,
    lag_started_at: Option<Instant>,
    replication_lag_started_at: Option<Instant>,
}

impl Default for FollowerReadinessState {
    fn default() -> Self {
        Self {
            role: RoleState::Init,
            term: 0,
            leader_id: 0,
            leadership_epoch: 0,
            commit_index: 0,
            leader_commit_index: None,
            leader_commit_contact_proven: false,
            applied_index: None,
            replay_ready: false,
            leader_contact: None,
            leader_contact_sequence: 0,
            lag_started_at: None,
            replication_lag_started_at: None,
        }
    }
}

#[derive(Clone, Copy)]
struct LeaderContact {
    term: u64,
    leader_id: u64,
    at: Instant,
}

/// A point-in-time view used by master admission and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerReadinessSnapshot {
    pub role: RoleState,
    pub term: u64,
    pub leader_id: u64,
    /// Monotonic view identifier advanced whenever role, term, or recognized
    /// leader changes. Request handlers compare it across build/serve windows.
    pub leadership_epoch: u64,
    pub commit_index: u64,
    /// Highest leader commit observed in this leadership epoch. This is
    /// exposed only when the most recent accepted leader contact carried the
    /// optional commit proof; otherwise follower reads must fail closed.
    pub leader_commit_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub replay_ready: bool,
    /// Monotonic counter incremented only for accepted current-term leader
    /// traffic. Consumers can distinguish a contact that arrived after an
    /// observation from a merely young contact that predates it.
    pub leader_contact_sequence: u64,
    /// Present only when the last observed contact belongs to the current
    /// follower term and leader.
    pub leader_contact_age: Option<Duration>,
    /// Present only while local committed work remains unapplied. This timer
    /// resets whenever the applied index advances and stays absent for network
    /// replication lag.
    pub apply_no_progress_age: Option<Duration>,
    /// Present only while the advertised leader commit is ahead of the local
    /// Raft commit. This timer resets when local replication advances.
    pub replication_no_progress_age: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerReadRefusal {
    NotFollower,
    ReplayNotReady,
    NoLeader,
    NoCurrentTermLeaderContact,
    StaleLeaderContact,
    LeaderCommitUnknown,
    ApplyProgressUnknown,
    ApplyStalled,
    ReplicationStalled,
    CommitLag,
}

impl FollowerReadinessSnapshot {
    /// Applies a caller-selected freshness policy to an immutable snapshot.
    /// `max_contact_age` should be derived from Raft heartbeat/election timing.
    pub fn refusal_reason(
        &self,
        max_contact_age: Duration,
        max_commit_lag: u64,
        apply_stall_timeout: Duration,
    ) -> Option<FollowerReadRefusal> {
        if self.role != RoleState::Follower {
            return Some(FollowerReadRefusal::NotFollower);
        }
        if !self.replay_ready || self.applied_index.is_none() {
            return Some(FollowerReadRefusal::ReplayNotReady);
        }
        if self.leader_id == 0 {
            return Some(FollowerReadRefusal::NoLeader);
        }
        let Some(contact_age) = self.leader_contact_age else {
            return Some(FollowerReadRefusal::NoCurrentTermLeaderContact);
        };
        if contact_age > max_contact_age {
            return Some(FollowerReadRefusal::StaleLeaderContact);
        }
        let Some(leader_commit_index) = self.leader_commit_index else {
            return Some(FollowerReadRefusal::LeaderCommitUnknown);
        };

        let applied_index = self.applied_index.unwrap_or_default();
        let local_apply_lag = self.commit_index.saturating_sub(applied_index);
        if local_apply_lag > 0 {
            match self.apply_no_progress_age {
                Some(age) if age >= apply_stall_timeout => {
                    return Some(FollowerReadRefusal::ApplyStalled);
                }
                None => return Some(FollowerReadRefusal::ApplyProgressUnknown),
                _ => {}
            }
        }
        let lag = self
            .commit_index
            .max(leader_commit_index)
            .saturating_sub(applied_index);
        if lag > max_commit_lag {
            return Some(FollowerReadRefusal::CommitLag);
        }
        if self
            .replication_no_progress_age
            .is_some_and(|age| age >= apply_stall_timeout)
        {
            return Some(FollowerReadRefusal::ReplicationStalled);
        }

        None
    }
}

impl FollowerReadinessMonitor {
    fn lock(&self) -> MutexGuard<'_, FollowerReadinessState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Publishes current volatile Raft state. A role, term, or leader change
    /// invalidates contact from the previous leadership epoch.
    pub fn observe_raft_state(
        &self,
        role: RoleState,
        term: u64,
        leader_id: u64,
        commit_index: u64,
    ) {
        let now = Instant::now();
        let mut state = self.lock();
        Self::observe_raft_state_locked(&mut state, role, term, leader_id, commit_index, now);
    }

    /// Atomically publishes a verified leader contact alongside the local Raft
    /// state that accepted it. Readers can never observe a newer contact while
    /// retaining the previous request's leader-commit proof.
    pub fn observe_raft_state_with_leader_contact(
        &self,
        role: RoleState,
        term: u64,
        leader_id: u64,
        commit_index: u64,
        leader_commit_index: Option<u64>,
    ) {
        let now = Instant::now();
        let mut state = self.lock();
        Self::observe_raft_state_locked(&mut state, role, term, leader_id, commit_index, now);
        Self::observe_leader_contact_locked(&mut state, term, leader_id, leader_commit_index, now);
    }

    fn observe_raft_state_locked(
        state: &mut FollowerReadinessState,
        role: RoleState,
        term: u64,
        leader_id: u64,
        commit_index: u64,
        now: Instant,
    ) {
        if state.role != role || state.term != term || state.leader_id != leader_id {
            state.leadership_epoch = state.leadership_epoch.saturating_add(1);
            state.leader_commit_index = None;
            state.leader_commit_contact_proven = false;
            state.replication_lag_started_at = None;
            if role != RoleState::Follower
                || state
                    .leader_contact
                    .is_some_and(|contact| contact.term != term || contact.leader_id != leader_id)
            {
                state.leader_contact = None;
            }
        }

        let local_commit_advanced = commit_index > state.commit_index;
        state.role = role;
        state.term = term;
        state.leader_id = leader_id;
        state.commit_index = commit_index;
        Self::update_lag_timer(&mut state, now);
        Self::update_replication_lag_timer(&mut state, now, local_commit_advanced);
    }

    /// Records contact only when the Raft caller has verified that the
    /// accepted message came from the currently recognized leader and term.
    pub fn observe_leader_contact(
        &self,
        term: u64,
        leader_id: u64,
        leader_commit_index: Option<u64>,
    ) {
        let mut state = self.lock();
        Self::observe_leader_contact_locked(
            &mut state,
            term,
            leader_id,
            leader_commit_index,
            Instant::now(),
        );
    }

    fn observe_leader_contact_locked(
        state: &mut FollowerReadinessState,
        term: u64,
        leader_id: u64,
        leader_commit_index: Option<u64>,
        now: Instant,
    ) {
        if state.role == RoleState::Follower
            && state.term == term
            && state.leader_id == leader_id
            && leader_id != 0
        {
            state.leader_contact_sequence = state.leader_contact_sequence.saturating_add(1);
            state.leader_contact = Some(LeaderContact {
                term,
                leader_id,
                at: now,
            });
            match leader_commit_index {
                Some(commit) => {
                    state.leader_commit_index = Some(
                        state
                            .leader_commit_index
                            .map_or(commit, |observed| observed.max(commit)),
                    );
                    state.leader_commit_contact_proven = true;
                    Self::update_replication_lag_timer(&mut state, now, false);
                }
                None => {
                    // Preserve the epoch's high watermark for a later
                    // out-of-order proof, but never let this contact refresh
                    // read eligibility using an older proof.
                    state.leader_commit_contact_proven = false;
                }
            }
        }
    }

    /// Publishes the journal FSM's real metadata apply watermark.
    pub fn observe_applied(&self, applied_index: u64) {
        let now = Instant::now();
        let mut state = self.lock();
        let advanced = state
            .applied_index
            .is_some_and(|current| applied_index > current);
        state.applied_index = Some(applied_index);
        if advanced {
            if state.commit_index > applied_index {
                state.lag_started_at = Some(now);
            } else {
                state.lag_started_at = None;
            }
        }
        Self::update_lag_timer(&mut state, now);
    }

    /// Marks startup replay or snapshot installation complete only after the
    /// journal confirms that its actual metadata state covers local commit.
    pub fn set_replay_ready(&self, replay_ready: bool) {
        self.lock().replay_ready = replay_ready;
    }

    pub fn snapshot(&self) -> FollowerReadinessSnapshot {
        let now = Instant::now();
        let state = self.lock();
        let leader_contact_age = state.leader_contact.and_then(|contact| {
            (state.role == RoleState::Follower
                && contact.term == state.term
                && contact.leader_id == state.leader_id)
                .then(|| now.saturating_duration_since(contact.at))
        });
        let apply_no_progress_age = state
            .lag_started_at
            .filter(|_| {
                state
                    .applied_index
                    .is_some_and(|applied| state.commit_index > applied)
            })
            .map(|started_at| now.saturating_duration_since(started_at));
        let replication_no_progress_age = state
            .replication_lag_started_at
            .filter(|_| {
                state.leader_commit_contact_proven
                    && state
                        .leader_commit_index
                        .is_some_and(|leader_commit| leader_commit > state.commit_index)
            })
            .map(|started_at| now.saturating_duration_since(started_at));

        FollowerReadinessSnapshot {
            role: state.role,
            term: state.term,
            leader_id: state.leader_id,
            leadership_epoch: state.leadership_epoch,
            commit_index: state.commit_index,
            leader_commit_index: state
                .leader_commit_contact_proven
                .then_some(state.leader_commit_index)
                .flatten(),
            applied_index: state.applied_index,
            replay_ready: state.replay_ready,
            leader_contact_sequence: state.leader_contact_sequence,
            leader_contact_age,
            apply_no_progress_age,
            replication_no_progress_age,
        }
    }

    fn update_lag_timer(state: &mut FollowerReadinessState, now: Instant) {
        match state.applied_index {
            Some(applied) if state.commit_index > applied => {
                state.lag_started_at.get_or_insert(now);
            }
            _ => state.lag_started_at = None,
        }
    }

    fn update_replication_lag_timer(
        state: &mut FollowerReadinessState,
        now: Instant,
        local_commit_advanced: bool,
    ) {
        match state.leader_commit_index {
            Some(leader_commit) if leader_commit > state.commit_index => {
                if state.replication_lag_started_at.is_none() || local_commit_advanced {
                    state.replication_lag_started_at = Some(now);
                }
            }
            _ => state.replication_lag_started_at = None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FollowerReadRefusal, FollowerReadinessMonitor};
    use crate::raft::RoleState;
    use std::time::Duration;

    fn initialized_follower() -> FollowerReadinessMonitor {
        let monitor = FollowerReadinessMonitor::default();
        monitor.observe_applied(10);
        monitor.set_replay_ready(true);
        monitor.observe_raft_state(RoleState::Follower, 4, 8, 10);
        monitor.observe_leader_contact(4, 8, Some(10));
        monitor
    }

    #[test]
    fn fresh_idle_follower_is_ready_without_apply_progress() {
        let monitor = initialized_follower();
        let snapshot = monitor.snapshot();

        assert_eq!(snapshot.apply_no_progress_age, None);
        assert_eq!(
            snapshot.refusal_reason(Duration::from_secs(5), 0, Duration::from_millis(1)),
            None
        );
    }

    #[test]
    fn startup_and_snapshot_application_require_explicit_replay_completion() {
        let monitor = FollowerReadinessMonitor::default();
        monitor.observe_raft_state(RoleState::Follower, 4, 8, 0);
        monitor.observe_leader_contact(4, 8, Some(0));

        let before_replay = monitor.snapshot();
        assert_eq!(before_replay.applied_index, None);
        assert_eq!(
            before_replay.refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(1)),
            Some(FollowerReadRefusal::ReplayNotReady)
        );

        monitor.observe_applied(0);
        monitor.set_replay_ready(true);
        assert_eq!(
            monitor
                .snapshot()
                .refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(1)),
            None
        );

        // Snapshot replacement begins by clearing this bit. A failed apply
        // leaves it cleared and cannot authorize reads from the old state.
        monitor.set_replay_ready(false);
        assert_eq!(
            monitor
                .snapshot()
                .refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(1)),
            Some(FollowerReadRefusal::ReplayNotReady)
        );
    }

    #[test]
    fn commit_lag_is_measured_from_actual_apply_and_progress_resets_stall_timer() {
        let monitor = initialized_follower();
        monitor.observe_raft_state(RoleState::Follower, 4, 8, 12);
        let lagging = monitor.snapshot();
        assert_eq!(lagging.applied_index, Some(10));
        assert_eq!(
            lagging.refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(60)),
            Some(FollowerReadRefusal::CommitLag)
        );
        assert!(lagging.apply_no_progress_age.is_some());
        assert_eq!(lagging.replication_no_progress_age, None);

        monitor.observe_applied(11);
        let progressing = monitor.snapshot();
        assert_eq!(progressing.applied_index, Some(11));
        assert!(progressing
            .apply_no_progress_age
            .is_some_and(|age| age < Duration::from_secs(1)));
        assert_eq!(
            progressing.refusal_reason(Duration::from_secs(5), 1, Duration::from_secs(60)),
            None
        );
    }

    #[test]
    fn stalled_apply_and_missing_replay_have_distinct_refusals() {
        let monitor = initialized_follower();
        monitor.observe_raft_state(RoleState::Follower, 4, 8, 11);

        let mut snapshot = monitor.snapshot();
        snapshot.apply_no_progress_age = Some(Duration::from_secs(2));
        assert_eq!(
            snapshot.refusal_reason(Duration::from_secs(5), 1, Duration::from_secs(1)),
            Some(FollowerReadRefusal::ApplyStalled)
        );

        monitor.set_replay_ready(false);
        assert_eq!(
            monitor
                .snapshot()
                .refusal_reason(Duration::from_secs(5), 1, Duration::from_secs(1)),
            Some(FollowerReadRefusal::ReplayNotReady)
        );
    }

    #[test]
    fn commit_lag_without_a_progress_clock_fails_closed() {
        let monitor = initialized_follower();
        monitor.observe_raft_state(RoleState::Follower, 4, 8, 11);
        let mut snapshot = monitor.snapshot();
        snapshot.apply_no_progress_age = None;

        assert_eq!(
            snapshot.refusal_reason(Duration::from_secs(5), 1, Duration::from_secs(1)),
            Some(FollowerReadRefusal::ApplyProgressUnknown)
        );
    }

    #[test]
    fn leader_commit_watermark_is_monotonic_and_missing_proof_refuses() {
        let monitor = initialized_follower();
        monitor.observe_leader_contact(4, 8, Some(20));
        monitor.observe_leader_contact(4, 8, Some(15));

        let reordered = monitor.snapshot();
        assert_eq!(reordered.leader_commit_index, Some(20));
        assert!(reordered.leader_contact_age.is_some());
        assert_eq!(
            reordered.refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(60)),
            Some(FollowerReadRefusal::CommitLag)
        );

        monitor.observe_leader_contact(4, 8, None);
        let missing_proof = monitor.snapshot();
        assert_eq!(missing_proof.leader_commit_index, None);
        assert!(missing_proof.leader_contact_age.is_some());
        assert_eq!(
            missing_proof.refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(60)),
            Some(FollowerReadRefusal::LeaderCommitUnknown)
        );

        // A later proof from an older reordered request restores proof only at
        // the highest watermark already observed in this leadership epoch.
        monitor.observe_leader_contact(4, 8, Some(12));
        assert_eq!(monitor.snapshot().leader_commit_index, Some(20));
    }

    #[test]
    fn network_replication_lag_uses_its_own_no_progress_clock() {
        let monitor = initialized_follower();
        monitor.observe_leader_contact(4, 8, Some(12));

        let mut stalled = monitor.snapshot();
        assert_eq!(stalled.commit_index, 10);
        assert_eq!(stalled.applied_index, Some(10));
        assert_eq!(stalled.apply_no_progress_age, None);
        assert!(stalled.replication_no_progress_age.is_some());
        assert_eq!(
            stalled.refusal_reason(Duration::from_secs(5), 2, Duration::from_secs(60)),
            None
        );
        stalled.replication_no_progress_age = Some(Duration::from_secs(2));
        assert_eq!(
            stalled.refusal_reason(Duration::from_secs(5), 2, Duration::from_secs(1)),
            Some(FollowerReadRefusal::ReplicationStalled)
        );

        // Local commit progress resets the replication timer. Remote lag within
        // the configured index allowance remains eligible while progress is
        // recent, and is not misclassified as an apply stall.
        monitor.observe_raft_state(RoleState::Follower, 4, 8, 11);
        monitor.observe_applied(11);
        let progressing = monitor.snapshot();
        assert_eq!(progressing.apply_no_progress_age, None);
        assert!(progressing
            .replication_no_progress_age
            .is_some_and(|age| age < Duration::from_secs(1)));
        assert_eq!(
            progressing.refusal_reason(Duration::from_secs(5), 1, Duration::from_secs(60)),
            None
        );
    }

    #[test]
    fn term_or_leader_change_discards_old_contact() {
        let monitor = initialized_follower();
        monitor.observe_raft_state(RoleState::Follower, 5, 8, 10);

        let changed_term = monitor.snapshot();
        assert_eq!(changed_term.leader_contact_age, None);
        assert_eq!(
            changed_term.refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(1)),
            Some(FollowerReadRefusal::NoCurrentTermLeaderContact)
        );

        monitor.observe_leader_contact(5, 8, Some(10));
        let current_term_contact_sequence = monitor.snapshot().leader_contact_sequence;
        assert_eq!(current_term_contact_sequence, 2);
        monitor.observe_raft_state(RoleState::Follower, 5, 9, 10);
        assert_eq!(monitor.snapshot().leader_contact_age, None);
        monitor.observe_leader_contact(5, 8, Some(10));
        assert_eq!(
            monitor.snapshot().leader_contact_sequence,
            current_term_contact_sequence
        );
    }

    #[test]
    fn leadership_epoch_changes_across_role_transitions_even_in_one_term() {
        let monitor = initialized_follower();
        let follower_epoch = monitor.snapshot().leadership_epoch;

        monitor.observe_raft_state(RoleState::Exit, 4, 8, 10);
        let exit_epoch = monitor.snapshot().leadership_epoch;
        assert!(exit_epoch > follower_epoch);
        assert_eq!(monitor.snapshot().leader_commit_index, None);

        monitor.observe_raft_state(RoleState::Follower, 4, 8, 10);
        let follower_again_epoch = monitor.snapshot().leadership_epoch;
        assert!(follower_again_epoch > exit_epoch);
        assert_eq!(monitor.snapshot().leader_contact_age, None);
    }

    #[test]
    fn old_contact_is_stale_and_vote_traffic_cannot_refresh_it() {
        let monitor = initialized_follower();
        let mut snapshot = monitor.snapshot();
        snapshot.leader_contact_age = Some(Duration::from_secs(6));
        assert_eq!(
            snapshot.refusal_reason(Duration::from_secs(5), 0, Duration::from_secs(1)),
            Some(FollowerReadRefusal::StaleLeaderContact)
        );

        monitor.observe_raft_state(RoleState::Follower, 4, 0, 10);
        monitor.observe_leader_contact(4, 8, Some(10));
        assert_eq!(monitor.snapshot().leader_contact_age, None);
    }
}
