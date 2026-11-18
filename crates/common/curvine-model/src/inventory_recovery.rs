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

/// Master-local recovery of a complete, fenced worker inventory. The owning
/// WorkerInfo supplies session identity; no state is serialized or journaled.
#[derive(Debug, Clone, Default)]
pub struct InventoryRecovery {
    epoch: Option<(u64, u64)>,
    token: Option<String>,
    phase: Phase,
    attempts: u32,
    due_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Needed,
    Requested,
    Reporting,
    Reconciling,
    Complete,
}

impl InventoryRecovery {
    pub const RETRY_MIN_MS: u64 = 3_000;
    pub const RETRY_MAX_MS: u64 = 60_000;
    pub const REPORT_IDLE_MIN_MS: u64 = 60_000;

    fn adopt_epoch(&mut self, epoch: (u64, u64), now: u64) {
        if self.epoch != Some(epoch) {
            *self = Self {
                epoch: Some(epoch),
                due_ms: now,
                ..Self::default()
            };
        }
    }

    fn delay(&self, heartbeat_ms: u64) -> u64 {
        let base = heartbeat_ms.max(Self::RETRY_MIN_MS);
        base.saturating_mul(1u64 << self.attempts.min(6))
            .min(base.max(Self::RETRY_MAX_MS))
    }

    fn idle_timeout(heartbeat_ms: u64) -> u64 {
        Self::REPORT_IDLE_MIN_MS.max(heartbeat_ms.saturating_mul(5))
    }

    /// Start replaces the round, but is not proof of successful reconciliation.
    /// Preserve backoff across failed rounds in the same leadership epoch.
    pub fn start(&mut self, epoch: (u64, u64), token: String, now: u64, heartbeat_ms: u64) {
        self.adopt_epoch(epoch, now);
        self.token = Some(token);
        self.phase = Phase::Reporting;
        self.due_ms = now.saturating_add(Self::idle_timeout(heartbeat_ms));
    }

    pub fn matches(&self, epoch: (u64, u64), token: &str) -> bool {
        self.epoch == Some(epoch) && self.token.as_deref() == Some(token)
    }

    /// Only a matching, accepted report refreshes progress; duplicates must not
    /// undo a deferred round or a successful reconciliation.
    pub fn progress(&mut self, epoch: (u64, u64), token: &str, now: u64, heartbeat_ms: u64) {
        if self.matches(epoch, token) && self.phase == Phase::Reporting {
            self.due_ms = now.saturating_add(Self::idle_timeout(heartbeat_ms));
        }
    }

    pub fn reconciling(&mut self, epoch: (u64, u64), token: &str) {
        if self.matches(epoch, token) && self.phase == Phase::Reporting {
            self.phase = Phase::Reconciling;
        }
    }

    pub fn defer(&mut self, epoch: (u64, u64), token: &str, now: u64, heartbeat_ms: u64) -> bool {
        if !self.matches(epoch, token)
            || matches!(
                self.phase,
                Phase::Needed | Phase::Requested | Phase::Complete
            )
        {
            return false;
        }
        self.phase = Phase::Needed;
        self.due_ms = now.saturating_add(self.delay(heartbeat_ms));
        true
    }

    pub fn complete(&mut self, epoch: (u64, u64), token: &str) {
        if self.matches(epoch, token) && self.phase == Phase::Reconciling {
            self.phase = Phase::Complete;
            self.attempts = 0;
        }
    }

    /// A new leader has no proof of completed reconciliation. Reconstruct the
    /// obligation on its first fresh heartbeat instead of forgetting old-term
    /// retry state. Reissue lost requests; never abandon after N attempts.
    pub fn request_due(&mut self, epoch: (u64, u64), now: u64, heartbeat_ms: u64) -> bool {
        self.adopt_epoch(epoch, now);
        if matches!(self.phase, Phase::Complete | Phase::Reconciling) || now < self.due_ms {
            return false;
        }
        self.phase = Phase::Requested;
        self.due_ms = now.saturating_add(self.delay(heartbeat_ms));
        self.attempts = self.attempts.saturating_add(1);
        true
    }

    pub fn pending(&self, epoch: (u64, u64)) -> bool {
        self.epoch == Some(epoch) && self.phase != Phase::Complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const EPOCH: (u64, u64) = (1, 1);

    #[test]
    fn inventory_recovery_lost_request_is_redelivered_with_capped_backoff() {
        let mut state = InventoryRecovery::default();
        assert!(state.request_due(EPOCH, 0, 3_000));
        assert!(!state.request_due(EPOCH, 2_999, 3_000));
        assert!(state.request_due(EPOCH, 3_000, 3_000));
        for _ in 0..1000 {
            let due = state.due_ms;
            assert!(!state.request_due(EPOCH, due - 1, 3_000));
            assert!(state.request_due(EPOCH, due, 3_000));
            assert!(state.due_ms - due <= InventoryRecovery::RETRY_MAX_MS);
        }
        assert!(state.pending(EPOCH));
    }

    #[test]
    fn inventory_recovery_start_and_rpc_success_do_not_clear_obligation() {
        let mut state = InventoryRecovery::default();
        state.request_due(EPOCH, 0, 3_000);
        state.start(EPOCH, "one".into(), 1, 3_000);
        state.start(EPOCH, "two".into(), 2, 3_000); // Start response lost
        state.complete(EPOCH, "one");
        assert!(state.pending(EPOCH));
        state.progress(EPOCH, "two", 50_000, 3_000);
        assert!(!state.request_due(EPOCH, 100_000, 3_000));
        state.reconciling(EPOCH, "two");
        assert!(
            !state.request_due(EPOCH, u64::MAX, 3_000),
            "slow queued/running cleanup must not be restarted"
        );
        state.complete(EPOCH, "two");
        assert!(!state.pending(EPOCH));
        assert!(!state.request_due(EPOCH, u64::MAX, 3_000));
    }

    #[test]
    fn inventory_recovery_duplicate_report_cannot_clear_deferred_round() {
        let mut state = InventoryRecovery::default();
        state.start(EPOCH, "one".into(), 0, 3_000);
        state.reconciling(EPOCH, "one");
        assert!(state.defer(EPOCH, "one", 10, 3_000));
        let due = state.due_ms;
        state.progress(EPOCH, "one", 20, 3_000);
        state.reconciling(EPOCH, "one");
        state.complete(EPOCH, "one");
        assert_eq!(state.due_ms, due);
        assert!(state.request_due(EPOCH, due, 3_000));
        state.start(EPOCH, "replacement".into(), due + 1, 3_000);
        assert!(!state.defer(EPOCH, "one", due + 2, 3_000));
        state.complete(EPOCH, "one");
        assert!(state.pending(EPOCH));
        state.reconciling(EPOCH, "replacement");
        state.complete(EPOCH, "replacement");
        assert!(!state.pending(EPOCH));
    }

    #[test]
    fn inventory_recovery_new_leader_reconstructs_missing_obligation() {
        for completed in [false, true] {
            let mut state = InventoryRecovery::default();
            state.start(EPOCH, "old-leader".into(), 0, 3_000);
            state.reconciling(EPOCH, "old-leader");
            if completed {
                state.complete(EPOCH, "old-leader");
            }
            let promoted = (2, 7);
            assert!(state.request_due(promoted, 10, 3_000));
            state.complete(EPOCH, "old-leader");
            assert!(!state.defer(EPOCH, "old-leader", 11, 3_000));
            assert!(state.pending(promoted));
            state.start(promoted, "new-leader".into(), 12, 3_000);
            state.reconciling(promoted, "new-leader");
            state.complete(promoted, "new-leader");
            assert!(!state.pending(promoted));
        }
    }

    #[test]
    fn inventory_recovery_repeated_rounds_keep_backoff_until_success() {
        let mut state = InventoryRecovery::default();
        let mut now = 0;
        for round in 0..20 {
            assert!(state.request_due(EPOCH, now, 3_000));
            let token = round.to_string();
            state.start(EPOCH, token.clone(), now + 1, 3_000);
            state.reconciling(EPOCH, &token);
            assert!(state.defer(EPOCH, &token, now + 2, 3_000));
            assert!(!state.request_due(EPOCH, now + 3, 3_000));
            assert_eq!(state.attempts, round + 1);
            now = state.due_ms;
        }
        state.reconciling(EPOCH, "19"); // a duplicate cannot undo Needed
        assert!(state.request_due(EPOCH, now, 3_000));
        state.start(EPOCH, "quiet".into(), now + 1, 3_000);
        state.reconciling(EPOCH, "quiet");
        state.complete(EPOCH, "quiet");
        assert_eq!(state.attempts, 0);
        assert!(!state.pending(EPOCH));
    }

    #[test]
    fn inventory_recovery_stalled_report_and_slow_heartbeat() {
        let mut state = InventoryRecovery::default();
        state.start(EPOCH, "slow".into(), 100, 60_000);
        assert!(!state.request_due(EPOCH, 60_100, 60_000));
        state.progress(EPOCH, "slow", 299_100, 60_000);
        assert!(!state.request_due(EPOCH, 599_099, 60_000));
        assert!(state.request_due(EPOCH, 599_100, 60_000));
    }
}
