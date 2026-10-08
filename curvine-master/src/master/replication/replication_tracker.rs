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

use super::master_replication_manager::BlockId;
use curvine_model::WorkerAddress;
use curvine_proto::ReportBlockReplicationRequest;
use curvine_runtime::sync::FastDashMap;
use dashmap::mapref::entry::Entry;
use tokio::sync::OwnedSemaphorePermit;

pub(super) type WorkerId = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReportProtocol {
    AwaitingAck,
    AttemptAware,
    Legacy,
}

#[derive(Clone, Debug)]
pub(super) struct ReplicationAttempt {
    pub(super) attempt_id: String,
    pub(super) source_worker_id: WorkerId,
    pub(super) source_worker_session_id: String,
    pub(super) target_worker: WorkerAddress,
    pub(super) target_worker_session_id: String,
    report_protocol: ReportProtocol,
    pending_legacy_report: Option<ReportBlockReplicationRequest>,
}

impl ReplicationAttempt {
    pub(super) fn new(
        attempt_id: String,
        source_worker_id: WorkerId,
        source_worker_session_id: String,
        target_worker: WorkerAddress,
        target_worker_session_id: String,
    ) -> Self {
        Self {
            attempt_id,
            source_worker_id,
            source_worker_session_id,
            target_worker,
            target_worker_session_id,
            report_protocol: ReportProtocol::AwaitingAck,
            pending_legacy_report: None,
        }
    }
}

pub(super) struct InflightReplicationJob {
    pub(super) attempt: ReplicationAttempt,
    pub(super) deadline_ms: u64,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone, Debug)]
pub(super) struct UncertainReplicationJob {
    pub(super) attempt: ReplicationAttempt,
    pub(super) since_ms: u64,
}

pub(super) struct CompletingReplicationJob {
    pub(super) attempt: ReplicationAttempt,
}

pub(super) enum ReplicationState {
    Queued,
    Inflight(InflightReplicationJob),
    Uncertain(UncertainReplicationJob),
    Completing(CompletingReplicationJob),
    Quarantined { until_ms: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TrackedStateKind {
    Inflight,
    Uncertain,
}

#[derive(Clone, Debug)]
pub(super) struct AttemptSnapshot {
    pub(super) attempt: ReplicationAttempt,
    pub(super) kind: TrackedStateKind,
}

#[derive(Clone, Debug)]
pub(super) struct InflightSnapshot {
    pub(super) block_id: BlockId,
    pub(super) attempt_id: String,
    pub(super) deadline_ms: u64,
    pub(super) source_worker_id: WorkerId,
    pub(super) source_worker_session_id: String,
    pub(super) target_worker_id: WorkerId,
    pub(super) target_worker_session_id: String,
}

pub(super) struct AcceptedReport {
    pub(super) request: ReportBlockReplicationRequest,
    pub(super) snapshot: AttemptSnapshot,
    pub(super) previous_state: ReplicationState,
}

pub(super) enum ReportDisposition {
    Process(Box<AcceptedReport>),
    Deferred,
    Ignore,
}

pub(super) enum AckDisposition {
    Accepted(Option<ReportBlockReplicationRequest>),
    Mismatch,
    Inactive,
}

pub(super) struct ReplicationTracker {
    active: FastDashMap<BlockId, ReplicationState>,
    // Legacy reports have no attempt ID, so a delayed duplicate cannot be distinguished from a
    // later attempt for the same block. Keep the quarantine in the same map as active attempts so
    // completion and quarantine publication are one atomic state transition.
    legacy_quarantine_ms: u64,
}

impl ReplicationTracker {
    pub(super) fn new(legacy_quarantine_ms: u64) -> Self {
        Self {
            active: Default::default(),
            legacy_quarantine_ms,
        }
    }

    pub(super) fn queue(&self, block_id: BlockId, now_ms: u64) -> bool {
        match self.active.entry(block_id) {
            Entry::Vacant(entry) => {
                entry.insert(ReplicationState::Queued);
                true
            }
            Entry::Occupied(mut entry) => match entry.get() {
                ReplicationState::Quarantined { until_ms } if now_ms >= *until_ms => {
                    entry.insert(ReplicationState::Queued);
                    true
                }
                _ => false,
            },
        }
    }

    pub(super) fn cancel_queued(&self, block_id: BlockId) -> bool {
        self.active
            .remove_if(&block_id, |_, state| {
                matches!(state, ReplicationState::Queued)
            })
            .is_some()
    }

    pub(super) fn promote(
        &self,
        block_id: BlockId,
        attempt: ReplicationAttempt,
        deadline_ms: u64,
        permit: OwnedSemaphorePermit,
    ) -> bool {
        match self.active.entry(block_id) {
            Entry::Occupied(mut entry) if matches!(entry.get(), ReplicationState::Queued) => {
                entry.insert(ReplicationState::Inflight(InflightReplicationJob {
                    attempt,
                    deadline_ms,
                    _permit: permit,
                }));
                true
            }
            _ => false,
        }
    }

    pub(super) fn acknowledge(
        &self,
        block_id: BlockId,
        attempt_id: &str,
        response_attempt_id: Option<&str>,
    ) -> AckDisposition {
        let Some(mut state) = self.active.get_mut(&block_id) else {
            return AckDisposition::Inactive;
        };
        let attempt = match state.value_mut() {
            ReplicationState::Inflight(job) if job.attempt.attempt_id == attempt_id => {
                &mut job.attempt
            }
            ReplicationState::Uncertain(job) if job.attempt.attempt_id == attempt_id => {
                &mut job.attempt
            }
            _ => return AckDisposition::Inactive,
        };

        match response_attempt_id {
            Some(response_id) if response_id == attempt_id => {
                attempt.report_protocol = ReportProtocol::AttemptAware;
                // A modern worker always reports the attempt ID. Any early report without one is
                // stale or malformed and must not complete this attempt.
                attempt.pending_legacy_report = None;
                AckDisposition::Accepted(None)
            }
            Some(_) => AckDisposition::Mismatch,
            None => {
                attempt.report_protocol = ReportProtocol::Legacy;
                AckDisposition::Accepted(attempt.pending_legacy_report.take())
            }
        }
    }

    pub(super) fn accept_report(
        &self,
        request: ReportBlockReplicationRequest,
    ) -> ReportDisposition {
        let block_id = request.block_id;
        let Entry::Occupied(mut entry) = self.active.entry(block_id) else {
            return ReportDisposition::Ignore;
        };

        let (attempt, kind) = match entry.get_mut() {
            ReplicationState::Inflight(job) => (&mut job.attempt, TrackedStateKind::Inflight),
            ReplicationState::Uncertain(job) => (&mut job.attempt, TrackedStateKind::Uncertain),
            ReplicationState::Queued
            | ReplicationState::Completing(_)
            | ReplicationState::Quarantined { .. } => {
                return ReportDisposition::Ignore;
            }
        };

        if let Some(report_attempt_id) = request.attempt_id.as_deref() {
            if report_attempt_id != attempt.attempt_id {
                return ReportDisposition::Ignore;
            }
            attempt.report_protocol = ReportProtocol::AttemptAware;
            attempt.pending_legacy_report = None;
        } else {
            match attempt.report_protocol {
                ReportProtocol::AwaitingAck => {
                    // accept_job sends its ACK immediately after queueing, but the worker task may
                    // still win the race and report first. Hold one such report until the ACK tells
                    // us whether this worker understands attempt IDs.
                    attempt.pending_legacy_report = Some(request);
                    return ReportDisposition::Deferred;
                }
                ReportProtocol::AttemptAware => return ReportDisposition::Ignore,
                ReportProtocol::Legacy => {}
            }
        }

        let snapshot = AttemptSnapshot {
            attempt: attempt.clone(),
            kind,
        };
        // Claim completion while holding the shard lock. This prevents duplicate reports and the
        // timeout reaper from both processing the same attempt. The permit is carried in the
        // returned previous state until the caller accounts for the transition.
        let previous_state = entry.insert(ReplicationState::Completing(CompletingReplicationJob {
            attempt: snapshot.attempt.clone(),
        }));
        ReportDisposition::Process(Box::new(AcceptedReport {
            request,
            snapshot,
            previous_state,
        }))
    }

    pub(super) fn finish_completing(
        &self,
        block_id: BlockId,
        attempt_id: &str,
        now_ms: u64,
    ) -> bool {
        let Entry::Occupied(mut entry) = self.active.entry(block_id) else {
            return false;
        };
        let protocol = match entry.get() {
            ReplicationState::Completing(job) if job.attempt.attempt_id == attempt_id => {
                job.attempt.report_protocol
            }
            _ => return false,
        };
        if protocol == ReportProtocol::AttemptAware {
            entry.remove();
        } else {
            entry.insert(ReplicationState::Quarantined {
                until_ms: now_ms.saturating_add(self.legacy_quarantine_ms),
            });
        }
        true
    }

    pub(super) fn restore_completing_as_uncertain(
        &self,
        block_id: BlockId,
        attempt_id: &str,
        now_ms: u64,
    ) -> bool {
        match self.active.entry(block_id) {
            Entry::Occupied(mut entry) => {
                let attempt = match entry.get() {
                    ReplicationState::Completing(job) if job.attempt.attempt_id == attempt_id => {
                        job.attempt.clone()
                    }
                    _ => return false,
                };
                entry.insert(ReplicationState::Uncertain(UncertainReplicationJob {
                    attempt,
                    since_ms: now_ms,
                }));
                true
            }
            Entry::Vacant(_) => false,
        }
    }

    pub(super) fn cancel_known_inactive(
        &self,
        block_id: BlockId,
        attempt_id: &str,
    ) -> Option<ReplicationState> {
        self.active
            .remove_if(&block_id, |_, state| match state {
                ReplicationState::Inflight(job) => job.attempt.attempt_id == attempt_id,
                ReplicationState::Uncertain(job) => job.attempt.attempt_id == attempt_id,
                ReplicationState::Queued
                | ReplicationState::Completing(_)
                | ReplicationState::Quarantined { .. } => false,
            })
            .map(|(_, state)| state)
    }

    pub(super) fn remove_matching(
        &self,
        block_id: BlockId,
        attempt_id: &str,
        now_ms: u64,
    ) -> Option<ReplicationState> {
        let Entry::Occupied(mut entry) = self.active.entry(block_id) else {
            return None;
        };
        let protocol = match entry.get() {
            ReplicationState::Inflight(job) if job.attempt.attempt_id == attempt_id => {
                job.attempt.report_protocol
            }
            ReplicationState::Uncertain(job) if job.attempt.attempt_id == attempt_id => {
                job.attempt.report_protocol
            }
            _ => return None,
        };
        if protocol == ReportProtocol::AttemptAware {
            Some(entry.remove())
        } else {
            Some(entry.insert(ReplicationState::Quarantined {
                until_ms: now_ms.saturating_add(self.legacy_quarantine_ms),
            }))
        }
    }

    pub(super) fn mark_uncertain(&self, block_id: BlockId, attempt_id: &str, now_ms: u64) -> bool {
        match self.active.entry(block_id) {
            Entry::Occupied(mut entry) => {
                let attempt = match entry.get() {
                    ReplicationState::Inflight(job) if job.attempt.attempt_id == attempt_id => {
                        job.attempt.clone()
                    }
                    _ => return false,
                };
                entry.insert(ReplicationState::Uncertain(UncertainReplicationJob {
                    attempt,
                    since_ms: now_ms,
                }));
                true
            }
            Entry::Vacant(_) => false,
        }
    }

    pub(super) fn inflight_snapshots(&self) -> Vec<InflightSnapshot> {
        self.active
            .iter()
            .filter_map(|entry| match entry.value() {
                ReplicationState::Inflight(job) => Some(InflightSnapshot {
                    block_id: *entry.key(),
                    attempt_id: job.attempt.attempt_id.clone(),
                    deadline_ms: job.deadline_ms,
                    source_worker_id: job.attempt.source_worker_id,
                    source_worker_session_id: job.attempt.source_worker_session_id.clone(),
                    target_worker_id: job.attempt.target_worker.worker_id,
                    target_worker_session_id: job.attempt.target_worker_session_id.clone(),
                }),
                _ => None,
            })
            .collect()
    }

    pub(super) fn uncertain_snapshots(&self) -> Vec<(BlockId, String, u64)> {
        self.active
            .iter()
            .filter_map(|entry| match entry.value() {
                ReplicationState::Uncertain(job) => {
                    Some((*entry.key(), job.attempt.attempt_id.clone(), job.since_ms))
                }
                _ => None,
            })
            .collect()
    }

    pub(super) fn prune_expired_quarantine(&self, now_ms: u64) {
        self.active.retain(|_, state| {
            !matches!(state, ReplicationState::Quarantined { until_ms } if now_ms >= *until_ms)
        });
    }

    #[cfg(test)]
    fn active_len(&self) -> usize {
        self.active.len()
    }

    #[cfg(test)]
    fn quarantine_len(&self) -> usize {
        self.active
            .iter()
            .filter(|entry| matches!(entry.value(), ReplicationState::Quarantined { .. }))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::StorageTypeProto;
    use std::sync::Arc;
    use tokio::sync::{Barrier, Semaphore};

    fn attempt(id: &str) -> ReplicationAttempt {
        ReplicationAttempt::new(
            id.to_string(),
            1,
            "source-session".to_string(),
            WorkerAddress {
                worker_id: 2,
                ..Default::default()
            },
            "target-session".to_string(),
        )
    }

    fn report(attempt_id: Option<&str>) -> ReportBlockReplicationRequest {
        ReportBlockReplicationRequest {
            block_id: 1,
            storage_type: StorageTypeProto::Disk.into(),
            success: true,
            message: None,
            attempt_id: attempt_id.map(str::to_string),
        }
    }

    #[test]
    fn duplicate_enqueue_has_one_active_block() {
        let tracker = ReplicationTracker::new(100);
        assert!(tracker.queue(1, 0));
        assert!(!tracker.queue(1, 0));
        assert_eq!(tracker.active_len(), 1);
    }

    #[tokio::test]
    async fn early_attempt_aware_report_finishes_registered_attempt() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.clone().acquire_owned().await.unwrap(),
        ));

        let ReportDisposition::Process(accepted) = tracker.accept_report(report(Some("attempt-1")))
        else {
            panic!("attempt-aware report must be processed");
        };
        assert_eq!(accepted.snapshot.kind, TrackedStateKind::Inflight);
        assert!(matches!(
            &accepted.previous_state,
            ReplicationState::Inflight(_)
        ));
        drop(accepted);
        assert_eq!(semaphore.available_permits(), 1);
        assert!(tracker.finish_completing(1, "attempt-1", 1));
        assert!(tracker.queue(1, 1));
    }

    #[tokio::test]
    async fn duplicate_reports_claim_completion_once() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.clone().acquire_owned().await.unwrap(),
        ));

        let ReportDisposition::Process(accepted) = tracker.accept_report(report(Some("attempt-1")))
        else {
            panic!("first report must claim completion");
        };
        assert!(matches!(
            tracker.accept_report(report(Some("attempt-1"))),
            ReportDisposition::Ignore
        ));
        drop(accepted);
        assert_eq!(semaphore.available_permits(), 1);
        assert!(tracker.finish_completing(1, "attempt-1", 1));
        assert!(!tracker.finish_completing(1, "attempt-1", 1));
    }

    #[tokio::test]
    async fn metadata_failure_restores_completing_as_uncertain() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.clone().acquire_owned().await.unwrap(),
        ));

        let ReportDisposition::Process(accepted) = tracker.accept_report(report(Some("attempt-1")))
        else {
            panic!("report must claim completion");
        };
        drop(accepted);
        assert_eq!(semaphore.available_permits(), 1);

        assert!(tracker.restore_completing_as_uncertain(1, "attempt-1", 10));
        assert!(matches!(
            tracker.uncertain_snapshots().as_slice(),
            [(1, attempt_id, 10)] if attempt_id == "attempt-1"
        ));
        assert!(!tracker.queue(1, 10));
    }

    #[tokio::test]
    async fn timeout_cannot_replace_completing_state() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.clone().acquire_owned().await.unwrap(),
        ));

        let ReportDisposition::Process(accepted) = tracker.accept_report(report(Some("attempt-1")))
        else {
            panic!("report must claim completion");
        };
        assert!(!tracker.mark_uncertain(1, "attempt-1", 10));
        assert!(tracker.inflight_snapshots().is_empty());
        assert!(tracker.uncertain_snapshots().is_empty());

        drop(accepted);
        assert_eq!(semaphore.available_permits(), 1);
        assert!(tracker.finish_completing(1, "attempt-1", 10));
    }

    #[tokio::test]
    async fn completing_legacy_attempt_is_temporarily_quarantined() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));
        assert!(matches!(
            tracker.acknowledge(1, "attempt-1", None),
            AckDisposition::Accepted(None)
        ));

        let ReportDisposition::Process(accepted) = tracker.accept_report(report(None)) else {
            panic!("legacy report must claim completion after legacy ACK");
        };
        drop(accepted);
        assert!(tracker.finish_completing(1, "attempt-1", 10));
        assert!(!tracker.queue(1, 109));
        assert!(tracker.queue(1, 110));
    }

    #[tokio::test]
    async fn early_legacy_report_waits_for_legacy_ack() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));
        assert!(matches!(
            tracker.accept_report(report(None)),
            ReportDisposition::Deferred
        ));
        let AckDisposition::Accepted(Some(pending)) = tracker.acknowledge(1, "attempt-1", None)
        else {
            panic!("legacy ACK must release the deferred report");
        };
        assert!(matches!(
            tracker.accept_report(pending),
            ReportDisposition::Process(_)
        ));
    }

    #[tokio::test]
    async fn modern_ack_discards_legacy_report() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));
        assert!(matches!(
            tracker.accept_report(report(None)),
            ReportDisposition::Deferred
        ));
        assert!(matches!(
            tracker.acknowledge(1, "attempt-1", Some("attempt-1")),
            AckDisposition::Accepted(None)
        ));
        assert!(matches!(
            tracker.accept_report(report(None)),
            ReportDisposition::Ignore
        ));
    }

    #[tokio::test]
    async fn known_unsent_attempt_can_be_requeued_without_quarantine() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));

        tracker
            .cancel_known_inactive(1, "attempt-1")
            .expect("known-unsent attempt must be removed");
        assert!(tracker.queue(1, 0));
    }

    #[tokio::test]
    async fn rejected_attempt_aware_submit_can_be_requeued_immediately() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));
        assert!(matches!(
            tracker.acknowledge(1, "attempt-1", Some("attempt-1")),
            AckDisposition::Accepted(None)
        ));

        tracker.remove_matching(1, "attempt-1", 10).unwrap();
        assert!(tracker.queue(1, 10));
    }

    #[tokio::test]
    async fn completed_legacy_attempt_is_temporarily_quarantined() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));
        assert!(matches!(
            tracker.acknowledge(1, "attempt-1", None),
            AckDisposition::Accepted(None)
        ));
        tracker.remove_matching(1, "attempt-1", 10).unwrap();
        assert!(!tracker.queue(1, 109));
        assert!(tracker.queue(1, 110));
    }

    #[tokio::test]
    async fn expired_legacy_quarantine_is_pruned_without_requeue() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(2));
        for block_id in [1, 2] {
            assert!(tracker.queue(block_id, 0));
            assert!(tracker.promote(
                block_id,
                attempt(&format!("attempt-{block_id}")),
                u64::MAX,
                semaphore.clone().acquire_owned().await.unwrap(),
            ));
            assert!(matches!(
                tracker.acknowledge(block_id, &format!("attempt-{block_id}"), None),
                AckDisposition::Accepted(None)
            ));
            tracker
                .remove_matching(block_id, &format!("attempt-{block_id}"), 10)
                .unwrap();
        }

        assert_eq!(tracker.quarantine_len(), 2);
        tracker.prune_expired_quarantine(109);
        assert_eq!(tracker.quarantine_len(), 2);
        tracker.prune_expired_quarantine(110);
        assert_eq!(tracker.quarantine_len(), 0);
    }

    #[tokio::test]
    async fn timed_out_attempts_release_all_scheduler_permits() {
        let tracker = ReplicationTracker::new(100);
        let semaphore = Arc::new(Semaphore::new(3));

        for block_id in [1, 2, 3] {
            assert!(tracker.queue(block_id, 0));
            assert!(tracker.promote(
                block_id,
                attempt(&format!("attempt-{block_id}")),
                10,
                semaphore.clone().acquire_owned().await.unwrap(),
            ));
        }
        assert_eq!(semaphore.available_permits(), 0);

        for block_id in [1, 2, 3] {
            assert!(tracker.mark_uncertain(block_id, &format!("attempt-{block_id}"), 10,));
        }

        assert_eq!(semaphore.available_permits(), 3);
        assert_eq!(tracker.inflight_snapshots().len(), 0);
        assert_eq!(tracker.uncertain_snapshots().len(), 3);
    }

    #[tokio::test]
    async fn timeout_and_report_race_completes_once() {
        let tracker = Arc::new(ReplicationTracker::new(100));
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.clone().acquire_owned().await.unwrap(),
        ));

        let barrier = Arc::new(Barrier::new(3));
        let timeout_tracker = tracker.clone();
        let timeout_barrier = barrier.clone();
        let timeout_task = tokio::spawn(async move {
            timeout_barrier.wait().await;
            timeout_tracker.mark_uncertain(1, "attempt-1", 10)
        });
        let report_tracker = tracker.clone();
        let report_barrier = barrier.clone();
        let report_task = tokio::spawn(async move {
            report_barrier.wait().await;
            match report_tracker.accept_report(report(Some("attempt-1"))) {
                ReportDisposition::Process(accepted) => {
                    drop(accepted);
                    report_tracker.finish_completing(1, "attempt-1", 10)
                }
                ReportDisposition::Ignore => false,
                ReportDisposition::Deferred => panic!("attempt-aware report cannot be deferred"),
            }
        });
        barrier.wait().await;
        let marked_uncertain = timeout_task.await.unwrap();
        let report_completed = report_task.await.unwrap();

        assert!(report_completed);
        assert!(marked_uncertain || semaphore.available_permits() == 1);
        assert_eq!(tracker.active_len(), 0);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn old_attempt_does_not_finish_new_attempt() {
        let tracker = ReplicationTracker::new(0);
        let semaphore = Arc::new(Semaphore::new(2));
        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-1"),
            u64::MAX,
            semaphore.clone().acquire_owned().await.unwrap(),
        ));
        tracker.remove_matching(1, "attempt-1", 0).unwrap();

        assert!(tracker.queue(1, 0));
        assert!(tracker.promote(
            1,
            attempt("attempt-2"),
            u64::MAX,
            semaphore.acquire_owned().await.unwrap(),
        ));
        assert!(matches!(
            tracker.accept_report(report(Some("attempt-1"))),
            ReportDisposition::Ignore
        ));
        let ReportDisposition::Process(accepted) = tracker.accept_report(report(Some("attempt-2")))
        else {
            panic!("current attempt must be processed");
        };
        drop(accepted);
        assert!(tracker.finish_completing(1, "attempt-2", 0));
    }

    #[test]
    fn unknown_attempt_does_not_change_active_state() {
        let tracker = ReplicationTracker::new(100);
        assert!(tracker.queue(1, 0));
        assert!(tracker.remove_matching(1, "unknown", 0).is_none());
        assert_eq!(tracker.active_len(), 1);
    }
}
