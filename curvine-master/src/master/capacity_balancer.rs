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

use super::capacity_balance::{
    plan_capacity_balance, PlanOutcome, WorkerCapacity, DEFAULT_THRESHOLD_BPS,
};
use super::fs::policy::{ChooseContext, WeightedWorkerPolicy, WorkerPolicy};
use super::fs::MasterFilesystem;
use super::master_metrics::MasterMetrics;
use super::replication::master_replication_manager::{MasterReplicationManager, MoveRequest};
use curvine_error::{FsError, FsResult};
use curvine_model::WorkerInfo;
use curvine_runtime::common::LocalTime;
use curvine_runtime::sync::FastMutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const BLOCKS_PER_TICK: usize = 128;
const TICK: Duration = Duration::from_millis(200);
type Epoch = (u64, u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Disabled,
    #[default]
    Idle,
    Running,
    Stopping,
    Stopped,
    Balanced,
    Blocked,
    LeadershipChanged,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Balanced => "balanced",
            Self::Blocked => "blocked",
            Self::LeadershipChanged => "leadership_changed",
        }
    }
}
#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct BalanceStatus {
    phase: Phase,
    threshold_bps: Option<u16>,
    round: u64,
    round_budget_bytes: u64,
    round_admitted_bytes: u64,
    completed_moves: u64,
    inflight_block: Option<i64>,
    detail: Option<String>,
}

#[derive(Default)]
struct State {
    status: BalanceStatus,
    epoch: Option<Epoch>,
    round: Option<Round>,
    pending: Option<Pending>,
}

struct Round {
    sources: Vec<(u32, u64)>,
    targets: BTreeMap<u32, u64>,
    sessions: BTreeMap<u32, String>,
    source_index: usize,
    cursor: Option<i64>,
    remaining: u64,
}
struct Pending {
    len: u64,
    block_id: i64,
    request: MoveRequest,
    /// A successful Move has been acknowledged by metadata. The watermark is
    /// captured only then, so pre-delete heartbeats cannot satisfy settlement.
    observed_at_ms: Option<u64>,
    refresh_deadline: Option<Instant>,
}

pub(crate) struct CapacityBalancer {
    // Serializes control operations with each bounded scan/admission step.
    state: FastMutex<State>,
    metrics: &'static MasterMetrics,
}

impl CapacityBalancer {
    pub(crate) fn new(metrics: &'static MasterMetrics, enabled: bool) -> Self {
        let balancer = Self {
            state: FastMutex::new(State::default()),
            metrics,
        };
        balancer
            .metrics
            .balancer
            .set_state(if enabled { "idle" } else { "disabled" });
        balancer
    }

    pub(crate) fn start(
        &self,
        fs: &MasterFilesystem,
        threshold: Option<u16>,
    ) -> FsResult<BalanceStatus> {
        let mut state = self.state.lock();
        let epoch = fs.drain_delete_epoch();
        fs.master_monitor.check_serving()?;
        if !fs.conf.balancer_enabled {
            return Err(FsError::unsupported("capacity balancer is disabled"));
        }
        let threshold = threshold.unwrap_or(DEFAULT_THRESHOLD_BPS);
        if threshold > 10_000 {
            return Err(FsError::invalid_argument(
                "threshold must be between 0 and 1",
            ));
        }
        Self::observe_epoch(fs, &mut state);
        if state.status.phase == Phase::Running {
            return if state.status.threshold_bps == Some(threshold) {
                Ok(state.status.clone())
            } else {
                Err(FsError::in_progress_msg(
                    "stop the current balancing run before changing its threshold",
                ))
            };
        }
        if state.pending.is_some() {
            return Err(FsError::in_progress_msg("previous Move has not settled"));
        }
        if !fs.can_dispatch_drain_deletes(epoch) {
            return Err(FsError::not_leader("balancer leadership changed"));
        }
        *state = State {
            epoch: Some(epoch),
            status: BalanceStatus {
                phase: Phase::Running,
                threshold_bps: Some(threshold),
                ..Default::default()
            },
            ..Default::default()
        };
        self.metrics.balancer.set_state("running");
        Ok(state.status.clone())
    }

    pub(crate) fn stop(&self, fs: &MasterFilesystem) -> FsResult<BalanceStatus> {
        let mut state = self.state.lock();
        fs.master_monitor.check_serving()?;
        Self::observe_epoch(fs, &mut state);
        if state.status.phase == Phase::Running {
            state.stop();
        }
        let status = if fs.conf.balancer_enabled {
            state.status.phase.as_str()
        } else {
            Phase::Disabled.as_str()
        };
        self.metrics.balancer.set_state(status);
        Ok(state.status.clone())
    }

    pub(crate) fn status(&self, fs: &MasterFilesystem) -> FsResult<BalanceStatus> {
        let mut state = self.state.lock();
        fs.master_monitor.check_serving()?;
        Self::observe_epoch(fs, &mut state);
        let phase = if fs.conf.balancer_enabled {
            state.status.phase
        } else {
            Phase::Disabled
        };
        self.metrics.balancer.set_state(phase.as_str());
        Ok(if fs.conf.balancer_enabled {
            state.status.clone()
        } else {
            BalanceStatus {
                phase: Phase::Disabled,
                ..Default::default()
            }
        })
    }

    fn observe_epoch(fs: &MasterFilesystem, state: &mut State) {
        if state
            .epoch
            .is_some_and(|epoch| !fs.can_dispatch_drain_deletes(epoch))
            && matches!(state.status.phase, Phase::Running | Phase::Stopping)
        {
            state.leadership_changed();
        }
    }

    pub(crate) async fn run(
        self: Arc<Self>,
        fs: MasterFilesystem,
        replication: Arc<MasterReplicationManager>,
        mut stopped: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => break,
                _ = ticker.tick() => {
                    let (balancer, fs, replication) = (self.clone(), fs.clone(), replication.clone());
                    if let Err(error) = tokio::task::spawn_blocking(move || balancer.tick(&fs, &replication)).await {
                        log::error!("Capacity balancer task exited: {}", error);
                        break;
                    }
                }
            }
        }
    }

    fn tick(&self, fs: &MasterFilesystem, replication: &MasterReplicationManager) {
        let mut state = self.state.lock();
        Self::observe_epoch(fs, &mut state);
        self.metrics.balancer.set_state(state.status.phase.as_str());
        if let Err(error) = state.advance(fs, replication, &self.metrics.balancer) {
            self.metrics.balancer.error("internal_error");
            state.block(error.to_string());
        }
        self.metrics.balancer.set_state(state.status.phase.as_str());
    }
}

impl State {
    fn stop(&mut self) {
        self.round = None;
        self.status.phase = if self.pending.is_some() {
            Phase::Stopping
        } else {
            Phase::Stopped
        };
    }

    fn leadership_changed(&mut self) {
        self.round = None;
        self.pending = None;
        self.status.inflight_block = None;
        self.status.phase = Phase::LeadershipChanged;
        self.status.detail = Some("serving epoch changed; a new manual start is required".into());
    }

    fn block(&mut self, detail: impl Into<String>) {
        self.round = None;
        self.status.phase = Phase::Blocked;
        self.status.detail = Some(detail.into());
    }

    fn advance(
        &mut self,
        fs: &MasterFilesystem,
        replication: &MasterReplicationManager,
        metrics: &super::master_metrics::BalancerMetrics,
    ) -> FsResult<()> {
        if let Some(pending) = self.pending.as_mut() {
            let Some(settlement) = pending.settled(fs, replication)? else {
                return Ok(());
            };
            let (confirmed, fresh) = settlement;
            let pending_len = pending.len;
            self.pending = None;
            self.status.inflight_block = None;
            metrics.inflight_moves.set(0);
            if confirmed {
                self.status.completed_moves = self.status.completed_moves.saturating_add(1);
                metrics.bytes_moved_total.inc_by(pending_len as i64);
            }
            if self.status.phase == Phase::Stopping {
                if !confirmed {
                    metrics.error("move_not_settled");
                }
                self.status.phase = Phase::Stopped;
            } else if self.status.phase == Phase::Running && (!confirmed || !fresh) {
                if !confirmed {
                    metrics.error("move_not_settled");
                }
                self.block(if confirmed {
                    "Move completed but fresh capacity reports were not observed"
                } else {
                    "Move did not settle with a released source and fresh capacity reports"
                });
            }
        }
        if self.status.phase != Phase::Running {
            return Ok(());
        }
        let epoch = self.epoch.expect("running balancer has a serving epoch");
        if self.round.is_none() {
            self.begin_round(fs, metrics)?;
        }
        let Some(round) = self.round.as_mut() else {
            return Ok(());
        };
        if let Some((len, pending)) = round.scan(fs, replication, metrics, epoch)? {
            self.status.round_admitted_bytes += len;
            self.status.inflight_block = Some(pending.block_id);
            metrics.inflight_moves.set(1);
            metrics.bytes_admitted_total.inc_by(len as i64);
            self.pending = Some(pending);
        } else if round.source_index == round.sources.len() {
            if self.status.round_admitted_bytes == 0 {
                self.block("no eligible block fits the current source, target and round budgets");
                metrics.error("no_progress");
            } else {
                // All admitted Moves have settled, including fresh capacity evidence.
                self.round = None;
            }
        }
        Ok(())
    }

    fn begin_round(
        &mut self,
        fs: &MasterFilesystem,
        metrics: &super::master_metrics::BalancerMetrics,
    ) -> FsResult<()> {
        let mut samples = Vec::new();
        let mut sessions = BTreeMap::new();
        let now = LocalTime::mills();
        {
            let workers = fs.worker_manager.read();
            for worker in workers
                .worker_map
                .workers()
                .values()
                .filter(|w| fresh(fs, w, now))
            {
                if let Some(sample) = WorkerCapacity::from_live_worker(worker) {
                    samples.push(sample);
                    sessions.insert(sample.worker_id, worker.worker_session_id.clone());
                }
            }
        }
        let plan = plan_capacity_balance(
            &samples,
            self.status.threshold_bps.unwrap_or(DEFAULT_THRESHOLD_BPS),
            fs.conf.balancer_bytes_per_iteration,
        )
        .map_err(|error| FsError::invalid_argument(error.to_string()))?;
        match plan.outcome {
            PlanOutcome::Balanced if sessions.len() >= 2 => {
                self.status.phase = Phase::Balanced;
                metrics.set_state("balanced");
            }
            PlanOutcome::Planned => {
                self.status.round = self.status.round.saturating_add(1);
                metrics.rounds_total.inc();
                metrics
                    .round_bytes_planned_total
                    .inc_by(plan.total_bytes as i64);
                self.status.round_budget_bytes = plan.total_bytes;
                self.status.round_admitted_bytes = 0;
                self.round = Some(Round {
                    sources: plan
                        .sources
                        .into_iter()
                        .filter(|b| b.limit_bytes > 0)
                        .map(|b| (b.worker_id, b.limit_bytes))
                        .collect(),
                    targets: plan
                        .targets
                        .into_iter()
                        .filter(|b| b.limit_bytes > 0)
                        .map(|b| (b.worker_id, b.limit_bytes))
                        .collect(),
                    sessions,
                    source_index: 0,
                    cursor: None,
                    remaining: plan.total_bytes,
                });
            }
            _ => {
                self.block("no feasible source and target byte budgets");
                metrics.set_state("blocked");
                metrics.error("no_feasible_move");
            }
        }
        Ok(())
    }
}

fn fresh(fs: &MasterFilesystem, worker: &WorkerInfo, now: u64) -> bool {
    worker.weight > 0
        && worker.capacity > 0
        && !worker.worker_session_id.is_empty()
        && now.saturating_sub(worker.last_update) <= fs.conf.worker_lost_interval_ms()
}

impl Round {
    /// Read at most one reverse-index page, without holding the namespace lock
    /// across ticks. Consume both budgets only when the tracker admits a Move.
    fn scan(
        &mut self,
        fs: &MasterFilesystem,
        replication: &MasterReplicationManager,
        metrics: &super::master_metrics::BalancerMetrics,
        epoch: Epoch,
    ) -> FsResult<Option<(u64, Pending)>> {
        let Some(&(source, source_left)) = self.sources.get(self.source_index) else {
            return Ok(None);
        };
        if source_left == 0 || self.remaining == 0 || self.targets.values().all(|bytes| *bytes == 0)
        {
            self.next_source();
            return Ok(None);
        }
        let now = LocalTime::mills();
        let source_session = &self.sessions[&source];
        let (ids, next) = {
            let dir = fs.fs_dir.read();
            let workers = fs.worker_manager.read();
            if !workers.get_worker(source).is_some_and(|w| {
                w.is_live() && fresh(fs, w, now) && w.worker_session_id == *source_session
            }) {
                self.next_source();
                return Ok(None);
            }
            dir.scan_worker_block_ids(source, self.cursor, BLOCKS_PER_TICK)?
        };
        for block_id in ids {
            if replication.block_copy_busy(block_id) {
                metrics.skipped("busy");
                continue;
            }
            if !fs.can_dispatch_drain_deletes(epoch) {
                metrics.skipped("leadership_changed");
                continue;
            }
            let dir = fs.fs_dir.read();
            let workers = fs.worker_manager.read();
            let Some(block) = dir.replication_block_state(block_id)? else {
                continue;
            };
            if !block.complete {
                metrics.skipped("incomplete");
                continue;
            }
            if block.len <= 0 || block.len as u64 > source_left {
                metrics.skipped("oversized_or_invalid");
                continue;
            }
            let len = block.len as u64;
            let registered = workers.worker_map.workers();
            let excluded = registered
                .keys()
                .copied()
                .filter(|id| {
                    !self
                        .targets
                        .get(id)
                        .is_some_and(|remaining| *remaining >= len)
                        || block.locations.iter().any(|loc| loc.worker_id == *id)
                        || !registered.get(id).is_some_and(|w| {
                            fresh(fs, w, now)
                                && self.sessions.get(id) == Some(&w.worker_session_id)
                                && w.allocatable_available() >= block.len
                                && w.replication_reservations.len()
                                    < fs.conf.replication_max_inflight_per_target
                                && !w.replication_reservations.contains_key(&block_id)
                                && workers.drain_witness(block_id, *id).is_some()
                        })
                })
                .collect();
            let Ok(chosen) = WeightedWorkerPolicy::new()
                .choose(registered, ChooseContext::with_num(1, block.len, excluded))
            else {
                metrics.skipped("no_target");
                continue;
            };
            let Some(target) = chosen.first().map(|w| w.worker_id) else {
                continue;
            };
            let Some(target_worker) = registered.get(&target) else {
                continue;
            };
            let request = MoveRequest {
                source,
                target,
                source_session: source_session.clone(),
                target_session: target_worker.worker_session_id.clone(),
            };
            let eligible =
                MasterReplicationManager::eligible_move(&block, &workers, block_id, &request);
            drop(workers);
            drop(dir);
            if eligible && replication.try_queue_move_in_epoch(block_id, request.clone(), epoch) {
                self.sources[self.source_index].1 -= len;
                self.remaining -= len;
                *self
                    .targets
                    .get_mut(&target)
                    .expect("selected target has a budget") -= len;
                self.cursor = Some(block_id);
                return Ok(Some((
                    len,
                    Pending {
                        len,
                        block_id,
                        request,
                        observed_at_ms: None,
                        refresh_deadline: None,
                    },
                )));
            }
        }
        if next.is_none() {
            self.next_source();
        } else {
            self.cursor = next;
        }
        Ok(None)
    }

    fn next_source(&mut self) {
        self.source_index += 1;
        self.cursor = None;
    }
}

impl Pending {
    fn settled(
        &mut self,
        fs: &MasterFilesystem,
        replication: &MasterReplicationManager,
    ) -> FsResult<Option<(bool, bool)>> {
        if replication.block_copy_busy(self.block_id) {
            return Ok(None);
        }
        let dir = fs.fs_dir.read();
        let workers = fs.worker_manager.read();
        let (Some(source), Some(target)) = (
            workers.get_worker(self.request.source),
            workers.get_worker(self.request.target),
        ) else {
            return Ok(Some((false, false)));
        };
        if source.worker_session_id != self.request.source_session
            || target.worker_session_id != self.request.target_session
        {
            return Ok(Some((false, false)));
        }
        if workers.has_replica_release(self.block_id, self.request.source) {
            // The release is authoritative only after the Worker has reported
            // the deletion and the metadata callback has removed the source.
            return Ok(None);
        }
        let Some(state) = dir.replication_block_state(self.block_id)? else {
            return Ok(Some((false, false)));
        };
        let source_present = state
            .locations
            .iter()
            .any(|loc| loc.worker_id == self.request.source);
        let target_present = state
            .locations
            .iter()
            .any(|loc| loc.worker_id == self.request.target);
        if source_present {
            // A successful Move first commits the replacement and then queues
            // the fenced source release. Keep the single in-flight Move fenced
            // until the deletion acknowledgement removes the source metadata.
            return if workers.has_replica_release(self.block_id, self.request.source) {
                Ok(None)
            } else {
                Ok(Some((false, false)))
            };
        }
        if !target_present || state.needs_replica(self.block_id, &workers) {
            return Ok(Some((false, false)));
        }
        let observed = match self.observed_at_ms {
            Some(observed) => observed,
            None => {
                self.observed_at_ms = Some(source.last_update.max(target.last_update));
                self.refresh_deadline = Some(
                    Instant::now()
                        + Duration::from_millis(fs.conf.worker_lost_interval_ms().max(1)),
                );
                return Ok(None);
            }
        };
        let deadline = self.refresh_deadline.unwrap_or_else(|| {
            Instant::now() + Duration::from_millis(fs.conf.worker_lost_interval_ms().max(1))
        });
        if source.last_update > observed && target.last_update > observed {
            Ok(Some((true, true)))
        } else if Instant::now() >= deadline {
            Ok(Some((true, false)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_keeps_a_pending_move_for_settlement_without_new_admission() {
        let mut state = State {
            status: BalanceStatus {
                phase: Phase::Running,
                ..Default::default()
            },
            pending: Some(Pending {
                len: 1_024,
                block_id: 7,
                request: MoveRequest {
                    source: 1,
                    target: 2,
                    source_session: "source".into(),
                    target_session: "target".into(),
                },
                observed_at_ms: None,
                refresh_deadline: None,
            }),
            ..Default::default()
        };
        state.stop();
        assert_eq!(state.status.phase, Phase::Stopping);
        assert!(state.pending.is_some());
        assert!(state.round.is_none());
    }

    #[test]
    fn leadership_change_clears_only_leader_local_controller_state() {
        let mut state = State {
            status: BalanceStatus {
                phase: Phase::Running,
                ..Default::default()
            },
            epoch: Some((1, 2)),
            pending: Some(Pending {
                len: 1_024,
                block_id: 8,
                request: MoveRequest {
                    source: 1,
                    target: 2,
                    source_session: "source".into(),
                    target_session: "target".into(),
                },
                observed_at_ms: None,
                refresh_deadline: None,
            }),
            ..Default::default()
        };
        state.leadership_changed();
        assert_eq!(state.status.phase, Phase::LeadershipChanged);
        assert!(state.pending.is_none());
        assert!(state.round.is_none());
        assert_eq!(state.status.inflight_block, None);
    }
}
