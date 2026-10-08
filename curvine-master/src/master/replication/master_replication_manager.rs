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

use super::replication_tracker::{
    AckDisposition, InflightSnapshot, ReplicationAttempt, ReplicationState, ReplicationTracker,
    ReportDisposition,
};
use crate::master::fs::MasterFilesystem;
use crate::master::{Master, MasterMetrics, SyncWorkerManager};
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_fs_api::RpcCode;
use curvine_model::{BlockLocation, ProtoUtils, WorkerAddress};
use curvine_proto::{
    ReportBlockReplicationRequest, SubmitBlockReplicationRequest, SubmitBlockReplicationResponse,
};
use curvine_rpc::client::ClientFactory;
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use log::{error, info, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout, Instant};

pub type BlockId = i64;
type WorkerId = u32;

const UNCERTAIN_RECONCILIATION_BATCH_SIZE: usize = 128;
const UNCERTAIN_RECONCILIATION_MAX_BACKOFF_MULTIPLIER: u32 = 64;

#[derive(Clone)]
pub struct MasterReplicationManager {
    fs: MasterFilesystem,
    worker_manager: SyncWorkerManager,

    replication_semaphore: Arc<Semaphore>,
    staging_queue_sender: Arc<Sender<BlockId>>,
    tracker: Arc<ReplicationTracker>,

    worker_client_factory: Arc<ClientFactory>,
    replication_enabled: bool,
    submit_timeout: Duration,
    job_timeout_ms: u64,
    reaper_interval: Duration,
    clock_start: Instant,

    metrics: &'static MasterMetrics,
}

#[derive(Clone)]
struct WorkerEndpoint {
    address: WorkerAddress,
    session_id: String,
}

impl MasterReplicationManager {
    pub fn new(
        fs: &MasterFilesystem,
        conf: &ClusterConf,
        rt: &Arc<AsyncRuntime>,
        worker_manager: &SyncWorkerManager,
    ) -> CommonResult<Arc<Self>> {
        let async_runtime = rt.clone();
        let semaphore = Semaphore::new(conf.master.block_replication_concurrency_limit);
        let (send, recv) = tokio::sync::mpsc::channel(Semaphore::MAX_PERMITS);

        let manager = Self {
            fs: fs.clone(),
            worker_manager: worker_manager.clone(),
            replication_semaphore: Arc::new(semaphore),
            staging_queue_sender: Arc::new(send),
            tracker: Arc::new(ReplicationTracker::new(
                conf.master.block_replication_job_timeout_ms(),
            )),
            worker_client_factory: Arc::new(Default::default()),
            replication_enabled: conf.master.block_replication_enabled,
            submit_timeout: Duration::from_millis(
                conf.master.block_replication_submit_timeout_ms(),
            ),
            job_timeout_ms: conf.master.block_replication_job_timeout_ms(),
            reaper_interval: Duration::from_millis(
                conf.master.block_replication_retry_interval_ms().max(100),
            ),
            clock_start: Instant::now(),
            metrics: Master::get_metrics()?,
        };
        let manager = Arc::new(manager);
        Self::handle(async_runtime.clone(), manager.clone(), recv);
        Self::start_reaper(async_runtime, manager.clone());

        info!("Master replication manager is initialized");
        Ok(manager)
    }

    fn handle(async_runtime: Arc<AsyncRuntime>, me: Arc<Self>, mut recv: Receiver<BlockId>) {
        async_runtime.spawn(async move {
            while let Some(block_id) = recv.recv().await {
                let permit = match me.replication_semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(e) => {
                        error!("Block replication loop stopped: semaphore closed: {}", e);
                        break;
                    }
                };
                if let Err(e) = me.replicate_block(block_id, permit).await {
                    if me.tracker.cancel_queued(block_id) {
                        me.metrics.replication_staging_number.dec();
                    }
                    error!("Failed to replicate block: {}. err: {}", block_id, e);
                }
            }
        });
    }

    fn start_reaper(async_runtime: Arc<AsyncRuntime>, me: Arc<Self>) {
        async_runtime.spawn(async move {
            loop {
                sleep(me.reaper_interval).await;
                let now_ms = me.monotonic_millis();
                me.tracker.prune_expired_quarantine(now_ms);
                me.reap_inflight_jobs(now_ms);
                me.reconcile_uncertain_jobs(now_ms);
            }
        });
    }

    fn monotonic_millis(&self) -> u64 {
        self.clock_start
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn worker_endpoint(&self, worker_id: WorkerId) -> Option<WorkerEndpoint> {
        let worker_manager = self.worker_manager.read();
        let worker = worker_manager.get_worker(worker_id)?;
        Some(WorkerEndpoint {
            address: worker.address.clone(),
            session_id: worker.worker_session_id.clone(),
        })
    }

    fn source_endpoint(&self, locations: &[BlockLocation]) -> CommonResult<WorkerEndpoint> {
        let worker_manager = self.worker_manager.read();
        for location in locations {
            if let Some(worker) = worker_manager.get_worker(location.worker_id) {
                return Ok(WorkerEndpoint {
                    address: worker.address.clone(),
                    session_id: worker.worker_session_id.clone(),
                });
            }
        }
        err_box!("No live source worker found for locations: {:?}", locations)
    }

    fn assign(&self, exclusive_worker_ids: Vec<WorkerId>) -> CommonResult<WorkerEndpoint> {
        let worker_manager = self.worker_manager.read();
        let mut assignment = worker_manager.choose_workers(1, exclusive_worker_ids)?;
        let Some(worker_id) = assignment.pop().map(|worker| worker.worker_id) else {
            return err_box!("no target worker selected for block replication");
        };
        let Some(worker) = worker_manager.get_worker(worker_id) else {
            return err_box!(
                "selected replication target worker {} no longer exists",
                worker_id
            );
        };
        Ok(WorkerEndpoint {
            address: worker.address.clone(),
            session_id: worker.worker_session_id.clone(),
        })
    }

    fn worker_session_matches(&self, worker_id: WorkerId, expected_session: &str) -> bool {
        self.worker_endpoint(worker_id)
            .is_some_and(|worker| worker.session_id == expected_session)
    }

    fn attempt_sessions_match(&self, snapshot: &InflightSnapshot) -> bool {
        self.worker_session_matches(
            snapshot.source_worker_id,
            &snapshot.source_worker_session_id,
        ) && self.worker_session_matches(
            snapshot.target_worker_id,
            &snapshot.target_worker_session_id,
        )
    }

    fn transition_to_uncertain(&self, block_id: BlockId, attempt_id: &str, reason: &str) -> bool {
        if self
            .tracker
            .mark_uncertain(block_id, attempt_id, self.monotonic_millis())
        {
            self.metrics.replication_inflight_number.dec();
            self.metrics.replication_uncertain_number.inc();
            warn!(
                "Replication attempt became uncertain: block={}, attempt={}, reason={}",
                block_id, attempt_id, reason
            );
            true
        } else {
            false
        }
    }

    fn account_removed(&self, state: ReplicationState) {
        match state {
            ReplicationState::Queued => self.metrics.replication_staging_number.dec(),
            ReplicationState::Inflight(_) => self.metrics.replication_inflight_number.dec(),
            ReplicationState::Uncertain(_) => self.metrics.replication_uncertain_number.dec(),
            ReplicationState::Completing(_) | ReplicationState::Quarantined { .. } => {}
        }
    }

    fn cancel_known_inactive_attempt(&self, block_id: BlockId, attempt_id: &str) -> bool {
        if let Some(state) = self.tracker.cancel_known_inactive(block_id, attempt_id) {
            self.account_removed(state);
            true
        } else {
            false
        }
    }

    fn remove_attempt(&self, block_id: BlockId, attempt_id: &str) -> bool {
        if let Some(state) =
            self.tracker
                .remove_matching(block_id, attempt_id, self.monotonic_millis())
        {
            self.account_removed(state);
            true
        } else {
            false
        }
    }

    fn reap_inflight_jobs(&self, now_ms: u64) {
        for snapshot in self.tracker.inflight_snapshots() {
            if now_ms >= snapshot.deadline_ms {
                if self.transition_to_uncertain(
                    snapshot.block_id,
                    &snapshot.attempt_id,
                    "job result deadline exceeded",
                ) {
                    self.metrics.replication_timeout_count.inc();
                }
            } else if !self.attempt_sessions_match(&snapshot) {
                self.transition_to_uncertain(
                    snapshot.block_id,
                    &snapshot.attempt_id,
                    "source or target worker session changed",
                );
            }
        }
    }

    fn reconcile_uncertain_jobs(&self, now_ms: u64) {
        for reconciliation in self
            .tracker
            .take_due_uncertain(now_ms, UNCERTAIN_RECONCILIATION_BATCH_SIZE)
        {
            if !self.tracker.is_current_uncertain(&reconciliation) {
                continue;
            }

            let resolved = match self
                .fs
                .fs_dir
                .read()
                .replication_block_state(reconciliation.block_id)
            {
                Ok(None) => true,
                Ok(Some(state)) => state.locations.len() >= state.replicas as usize,
                Err(e) => {
                    warn!(
                        "Failed to reconcile uncertain replication for block {}: {}",
                        reconciliation.block_id, e
                    );
                    false
                }
            };
            if resolved {
                if self.remove_attempt(reconciliation.block_id, &reconciliation.attempt_id) {
                    info!(
                        "Reconciled uncertain replication attempt: block={}, attempt={}, uncertain_age_ms={}",
                        reconciliation.block_id,
                        reconciliation.attempt_id,
                        now_ms.saturating_sub(reconciliation.since_ms)
                    );
                }
            } else {
                let next_check_ms = now_ms.saturating_add(uncertain_reconciliation_backoff_ms(
                    self.reaper_interval,
                    reconciliation.retry_count,
                ));
                self.tracker
                    .reschedule_uncertain(reconciliation, next_check_ms);
            }
        }
    }

    async fn replicate_block(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
    ) -> CommonResult<()> {
        let block_state = {
            let fs_dir = self.fs.fs_dir.read();
            fs_dir.replication_block_state(block_id)?
        };
        let Some(block_state) = block_state else {
            if self.tracker.cancel_queued(block_id) {
                self.metrics.replication_staging_number.dec();
            }
            info!("Skip obsolete replication request for block {}", block_id);
            return Ok(());
        };
        if block_state.locations.len() >= block_state.replicas as usize {
            if self.tracker.cancel_queued(block_id) {
                self.metrics.replication_staging_number.dec();
            }
            info!("Skip already replicated block {}", block_id);
            return Ok(());
        }
        if block_state.locations.is_empty() {
            return err_box!("missing source location for block {}", block_id);
        }

        let source = self.source_endpoint(&block_state.locations)?;
        let target = self.assign(
            block_state
                .locations
                .iter()
                .map(|location| location.worker_id)
                .collect(),
        )?;
        info!(
            "block_id: {}. locations: {:?}, target: {}",
            block_id, block_state.locations, target.address
        );

        let submit_deadline = Instant::now() + self.submit_timeout;
        let source_addr = target_duration(submit_deadline)?;
        let source_worker_addr = source.address.inet_addr();
        let source_worker_client = match timeout(
            source_addr,
            self.worker_client_factory.create_raw(&source_worker_addr),
        )
        .await
        {
            Ok(Ok(client)) => client,
            Ok(Err(e)) => {
                return err_box!(
                    "Errors on connecting to replication source {}, err: {:?}",
                    source_worker_addr,
                    e
                );
            }
            Err(e) => {
                self.metrics.replication_timeout_count.inc();
                return err_box!(
                    "Timed out connecting to replication source {}: {}",
                    source_worker_addr,
                    e
                );
            }
        };

        let attempt_id = Utils::uuid();
        let attempt = ReplicationAttempt::new(
            attempt_id.clone(),
            source.address.worker_id,
            source.session_id,
            target.address.clone(),
            target.session_id,
        );
        let deadline_ms = self.monotonic_millis().saturating_add(self.job_timeout_ms);
        if !self.tracker.promote(block_id, attempt, deadline_ms, permit) {
            return err_box!("Replication block {} is no longer queued", block_id);
        }
        self.metrics.replication_staging_number.dec();
        self.metrics.replication_inflight_number.inc();

        let request = SubmitBlockReplicationRequest {
            block_id,
            target_worker_info: ProtoUtils::worker_address_to_pb(&target.address),
            attempt_id: Some(attempt_id.clone()),
        };
        let msg = Builder::new_rpc(RpcCode::SubmitBlockReplicationJob)
            .request(RequestStatus::Rpc)
            .proto_header(request)
            .build();

        let remaining = match target_duration(submit_deadline) {
            Ok(duration) => duration,
            Err(e) => {
                // No submit bytes were sent, so this attempt is known not to be executing.
                self.metrics.replication_timeout_count.inc();
                self.cancel_known_inactive_attempt(block_id, &attempt_id);
                return Err(e);
            }
        };
        match timeout(remaining, source_worker_client.rpc(msg)).await {
            Ok(Ok(response)) => {
                let response: SubmitBlockReplicationResponse = match response.parse_header() {
                    Ok(response) => response,
                    Err(e) => {
                        self.transition_to_uncertain(
                            block_id,
                            &attempt_id,
                            "submit response could not be decoded",
                        );
                        return Err(e);
                    }
                };
                let ack =
                    self.tracker
                        .acknowledge(block_id, &attempt_id, response.attempt_id.as_deref());
                if matches!(ack, AckDisposition::Mismatch) {
                    self.transition_to_uncertain(
                        block_id,
                        &attempt_id,
                        "worker acknowledged a different attempt id",
                    );
                    return err_box!(
                        "Replication source {} acknowledged unexpected attempt {:?}",
                        source_worker_addr,
                        response.attempt_id
                    );
                }
                if !response.success {
                    // Learn the peer protocol from the ACK before removing the attempt. A modern
                    // worker's explicit rejection is safe to retry immediately; only legacy or
                    // protocol-unknown attempts need quarantine against an unidentifiable report.
                    self.metrics.replication_failure_count.inc();
                    self.remove_attempt(block_id, &attempt_id);
                    return err_box!(
                        "Errors on submit replication job to {}. err: {:?}",
                        source_worker_addr,
                        response.message
                    );
                }
                match ack {
                    AckDisposition::Accepted(Some(report)) => {
                        self.finish_replicated_block(report)?;
                    }
                    AckDisposition::Accepted(None) | AckDisposition::Inactive => {}
                    AckDisposition::Mismatch => unreachable!("mismatch handled above"),
                }
            }
            Ok(Err(e)) => {
                self.transition_to_uncertain(
                    block_id,
                    &attempt_id,
                    "submit RPC failed after the attempt was registered",
                );
                return err_box!(
                    "Errors on sending replication job to {}, err: {:?}",
                    source_worker_addr,
                    e
                );
            }
            Err(e) => {
                if self.transition_to_uncertain(block_id, &attempt_id, "submit RPC timed out") {
                    self.metrics.replication_timeout_count.inc();
                }
                return err_box!(
                    "Timed out sending replication job to {}, err: {}",
                    source_worker_addr,
                    e
                );
            }
        }

        Ok(())
    }

    pub fn report_under_replicated_blocks(
        &self,
        _worker_id: WorkerId,
        block_ids: Vec<i64>,
    ) -> CommonResult<()> {
        if !self.replication_enabled {
            return Ok(());
        }

        for block_id in block_ids {
            if !self.tracker.queue(block_id, self.monotonic_millis()) {
                continue;
            }
            info!("Accepting block {} replication job", block_id);
            self.metrics.replication_staging_number.inc();
            if let Err(e) = self.staging_queue_sender.try_send(block_id) {
                if self.tracker.cancel_queued(block_id) {
                    self.metrics.replication_staging_number.dec();
                }
                error!(
                    "Failed to queue replication job for block {}: {}. Queue may be full. Will retry on next heartbeat check.",
                    block_id, e
                );
            }
        }
        Ok(())
    }

    pub fn finish_replicated_block(&self, req: ReportBlockReplicationRequest) -> CommonResult<()> {
        let block_id = req.block_id;
        let report_attempt_id = req.attempt_id.clone();
        let (req, snapshot, previous_state) = match self.tracker.accept_report(req) {
            ReportDisposition::Process(accepted) => {
                let accepted = *accepted;
                (accepted.request, accepted.snapshot, accepted.previous_state)
            }
            ReportDisposition::Deferred => {
                info!(
                    "Deferred an early legacy replication report until submit acknowledgement: block={}",
                    block_id
                );
                return Ok(());
            }
            ReportDisposition::Ignore => {
                warn!(
                    "Ignoring unknown or stale replication report: block={}, attempt={:?}",
                    block_id, report_attempt_id
                );
                return Ok(());
            }
        };

        // accept_report atomically claimed this attempt by moving it to Completing. Account the
        // previous state now so its permit and gauge are released exactly once, even if metadata
        // validation below fails.
        self.account_removed(previous_state);

        let mut metadata_result = Ok(());
        let mut success = req.success;
        if success
            && !self.worker_session_matches(
                snapshot.attempt.target_worker.worker_id,
                &snapshot.attempt.target_worker_session_id,
            )
        {
            success = false;
            warn!(
                "Ignoring successful replication result for block {} because target worker {} session changed",
                block_id, snapshot.attempt.target_worker.worker_id
            );
        }

        if success {
            let location = BlockLocation::new(
                snapshot.attempt.target_worker.worker_id,
                req.storage_type.into(),
            );
            metadata_result = self
                .fs
                .fs_dir
                .write()
                .add_replication_location_if_needed(block_id, location)
                .map(|added| {
                    if added {
                        info!("Successfully replicated {}", block_id);
                    } else {
                        info!(
                            "Replication result for block {} no longer needs a metadata update",
                            block_id
                        );
                    }
                });
        } else {
            error!(
                "Errors on block replication for block_id: {} to worker: {}. error: {:?}",
                block_id, snapshot.attempt.target_worker, req.message
            );
            self.metrics.replication_failure_count.inc();
        }

        let now_ms = self.monotonic_millis();
        if metadata_result.is_err() {
            if self.tracker.restore_completing_as_uncertain(
                block_id,
                &snapshot.attempt.attempt_id,
                now_ms,
            ) {
                self.metrics.replication_uncertain_number.inc();
            } else {
                warn!(
                    "Replication attempt completion marker disappeared after metadata failure: block={}, attempt={}, previous_state={:?}",
                    block_id, snapshot.attempt.attempt_id, snapshot.kind
                );
            }
        } else if !self
            .tracker
            .finish_completing(block_id, &snapshot.attempt.attempt_id, now_ms)
        {
            warn!(
                "Replication attempt completion marker disappeared: block={}, attempt={}, previous_state={:?}",
                block_id, snapshot.attempt.attempt_id, snapshot.kind
            );
        }
        Ok(metadata_result?)
    }
}

fn uncertain_reconciliation_backoff_ms(interval: Duration, retry_count: u32) -> u64 {
    let interval_ms = interval.as_millis().try_into().unwrap_or(u64::MAX);
    let shift = retry_count.min(UNCERTAIN_RECONCILIATION_MAX_BACKOFF_MULTIPLIER.ilog2());
    interval_ms.saturating_mul(1_u64 << shift)
}

fn target_duration(deadline: Instant) -> CommonResult<Duration> {
    let now = Instant::now();
    if now >= deadline {
        err_box!("replication submit deadline exceeded")
    } else {
        Ok(deadline.duration_since(now))
    }
}
