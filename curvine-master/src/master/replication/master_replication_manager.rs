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
    ReportDisposition, UncertainReconciliation,
};
use crate::master::fs::{MasterFilesystem, ReplicaRole, WorkerManager};
use crate::master::meta::ReplicationBlockState;
use crate::master::{Master, MasterMetrics, SyncWorkerManager};
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_fs_api::RpcCode;
use curvine_model::{BlockLocation, ProtoUtils, WorkerAddress, WorkerInfo};
use curvine_proto::{
    PrepareReplicationRequest, PrepareReplicationResponse, ReconcileReplicationRequest,
    ReconcileReplicationResponse, ReportBlockReplicationRequest, SubmitBlockReplicationRequest,
    SubmitBlockReplicationResponse,
};
use curvine_rpc::client::ClientFactory;
use curvine_rpc::message::{Builder, RequestStatus};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_runtime::sync::FastMutex;
use log::{error, info, warn};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
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
    runtime: Arc<AsyncRuntime>,
    recovery_semaphore: Arc<Semaphore>,
    repairs: Arc<FastMutex<RepairQueue>>,
}

/// Repair demand outlives an individual attempt. One due entry per block, independent of
/// heartbeat events. Both retry queues are bounded per tick and back off independently.
#[derive(Default)]
struct RepairQueue {
    blocks: HashMap<BlockId, u64>,
    due: BinaryHeap<Reverse<(u64, BlockId, u32)>>,
}

impl RepairQueue {
    fn record(&mut self, block_id: BlockId, now_ms: u64) {
        if let Some(generation) = self.blocks.get_mut(&block_id) {
            *generation = generation.wrapping_add(1);
        } else {
            self.blocks.insert(block_id, 0);
            self.due.push(Reverse((now_ms, block_id, 0)));
        }
    }

    fn retire_if_unchanged(&mut self, block_id: BlockId, generation: Option<u64>, now_ms: u64) {
        if self.blocks.get(&block_id).copied() == generation {
            self.blocks.remove(&block_id);
        } else {
            // A new loss notification raced the healthy/deleted snapshot.
            // Preserve that demand rather than losing its only wakeup.
            self.due.push(Reverse((now_ms, block_id, 0)));
        }
    }
}

#[derive(Clone)]
struct WorkerEndpoint {
    address: WorkerAddress,
    session_id: String,
}

impl WorkerEndpoint {
    fn from_worker(worker: &WorkerInfo) -> Self {
        Self {
            address: worker.address.clone(),
            session_id: worker.worker_session_id.clone(),
        }
    }
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
            runtime: rt.clone(),
            recovery_semaphore: Arc::new(Semaphore::new(8)),
            repairs: Arc::new(FastMutex::new(RepairQueue::default())),
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
                me.retry_repairs(now_ms);
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
        worker_manager
            .get_worker(worker_id)
            .map(WorkerEndpoint::from_worker)
    }

    fn source_endpoint(
        workers: &WorkerManager,
        block_id: BlockId,
        locations: &[BlockLocation],
    ) -> CommonResult<WorkerEndpoint> {
        let mut fallback = None;
        for worker in locations
            .iter()
            .filter(|loc| !workers.drain_delete_sent(block_id, loc.worker_id))
            .filter_map(|loc| workers.get_worker(loc.worker_id))
        {
            match ReplicaRole::from(worker.status) {
                ReplicaRole::Counted => return Ok(WorkerEndpoint::from_worker(worker)),
                ReplicaRole::Fallback if fallback.is_none() => fallback = Some(worker),
                _ => {}
            }
        }
        match fallback {
            Some(worker) => Ok(WorkerEndpoint::from_worker(worker)),
            None => err_box!(
                "No usable replication source for locations: {:?}",
                locations
            ),
        }
    }

    /// Load the current block and check its Live count under the usual
    /// namespace -> Worker lock order. A deleted or healthy block needs no repair.
    fn under_replicated_block(
        &self,
        block_id: BlockId,
    ) -> curvine_error::FsResult<Option<ReplicationBlockState>> {
        let fs_dir = self.fs.fs_dir.read();
        let state = fs_dir.replication_block_state(block_id)?;
        Ok(state.filter(|state| state.needs_replica(block_id, &self.worker_manager.read())))
    }

    fn assign(
        &self,
        block_id: BlockId,
        attempt_id: &str,
        block_len: i64,
        excluded: Vec<WorkerId>,
    ) -> CommonResult<WorkerEndpoint> {
        let (address, session_id) = self
            .worker_manager
            .write()
            .reserve_replication_target(block_id, attempt_id, block_len, excluded)?;
        Ok(WorkerEndpoint {
            address,
            session_id,
        })
    }

    fn settle_target(&self, block_id: BlockId, attempt: &ReplicationAttempt, finalized: bool) {
        self.worker_manager.write().settle_replication_target(
            attempt.target_worker.worker_id,
            &attempt.target_worker_session_id,
            block_id,
            &attempt.attempt_id,
            finalized,
        );
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

    fn cancel_known_inactive_attempt(&self, block_id: BlockId, attempt: &ReplicationAttempt) {
        if let Some(state) = self
            .tracker
            .cancel_known_inactive(block_id, &attempt.attempt_id)
        {
            self.settle_target(block_id, attempt, false);
            self.account_removed(state);
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

    fn retry_repairs(&self, now_ms: u64) {
        for _ in 0..UNCERTAIN_RECONCILIATION_BATCH_SIZE {
            let entry = {
                let mut repairs = self.repairs.lock();
                match repairs.due.peek() {
                    Some(Reverse((due, _, _))) if *due <= now_ms => repairs.due.pop(),
                    _ => None,
                }
            };
            let Some(Reverse((_, block_id, retry))) = entry else {
                break;
            };
            let generation = self.repairs.lock().blocks.get(&block_id).copied();
            // Never hold the demand mutex across filesystem reads or tracker operations.
            if matches!(self.under_replicated_block(block_id), Ok(None)) {
                self.repairs
                    .lock()
                    .retire_if_unchanged(block_id, generation, now_ms);
                continue;
            }
            self.queue_block(block_id);
            let next = now_ms.saturating_add(uncertain_reconciliation_backoff_ms(
                self.reaper_interval,
                retry,
            ));
            self.repairs
                .lock()
                .due
                .push(Reverse((next, block_id, retry.saturating_add(1))));
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
            let Ok(permit) = self.recovery_semaphore.clone().try_acquire_owned() else {
                self.reschedule_reconciliation(reconciliation);
                continue;
            };
            let me = self.clone();
            self.runtime.spawn(async move {
                let _permit = permit;
                if let Err(e) = me.reconcile_attempt(&reconciliation).await {
                    warn!(
                        "Replication reconciliation failed for block {}: {}",
                        reconciliation.block_id, e
                    );
                }
                me.reschedule_reconciliation(reconciliation);
            });
        }
    }

    fn reschedule_reconciliation(&self, reconciliation: UncertainReconciliation) {
        let next = self
            .monotonic_millis()
            .saturating_add(uncertain_reconciliation_backoff_ms(
                self.reaper_interval,
                reconciliation.retry_count,
            ));
        self.tracker.reschedule_uncertain(reconciliation, next);
    }

    async fn reconcile_attempt(&self, item: &UncertainReconciliation) -> CommonResult<()> {
        let Some(attempt) = self
            .tracker
            .uncertain_attempt(item.block_id, &item.attempt_id)
        else {
            return Ok(());
        };
        // Obsolete metadata is not evidence that the target stopped writing.
        let obsolete = self.under_replicated_block(item.block_id)?.is_none();
        // Missing heartbeats do not prove termination. A *different live session* plus
        // destination-issued tokens does: old tokens cannot survive the target's restart.
        if self
            .worker_endpoint(attempt.target_worker.worker_id)
            .is_some_and(|w| w.session_id != attempt.target_worker_session_id)
        {
            self.cancel_known_inactive_attempt(item.block_id, &attempt);
            return Ok(());
        }
        let req = ReconcileReplicationRequest {
            block_id: item.block_id,
            attempt_id: item.attempt_id.clone(),
            target_session_id: attempt.target_worker_session_id.clone(),
            release: false,
        };
        let response: ReconcileReplicationResponse = timeout(self.submit_timeout, async {
            let client = self
                .worker_client_factory
                .create_raw(&attempt.target_worker.inet_addr())
                .await?;
            let response = client
                .rpc(
                    Builder::new()
                        .code(RpcCode::ReconcileReplication)
                        .request(RequestStatus::Rpc)
                        .proto_header(req)
                        .build(),
                )
                .await?;
            response.check_error_ext::<curvine_error::FsError>()?;
            response.parse_header::<ReconcileReplicationResponse>()
        })
        .await??;
        if obsolete {
            if self.remove_attempt(item.block_id, &item.attempt_id) {
                self.settle_target(item.block_id, &attempt, response.storage_type.is_some());
                self.release_target(item.block_id, &attempt);
            }
        } else if let Some(storage_type) = response.storage_type {
            self.finish_replicated_block(ReportBlockReplicationRequest {
                block_id: item.block_id,
                attempt_id: Some(item.attempt_id.clone()),
                storage_type,
                success: true,
                message: None,
            })?;
        } else {
            // The target has dropped/closed the old writer under its gate. Only now can
            // the independently retained repair demand schedule a successor.
            self.cancel_known_inactive_attempt(item.block_id, &attempt);
        }
        Ok(())
    }

    async fn replicate_block(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
    ) -> CommonResult<()> {
        let Some(block_state) = self.under_replicated_block(block_id)? else {
            if self.tracker.cancel_queued(block_id) {
                self.metrics.replication_staging_number.dec();
            }
            info!(
                "Skip obsolete or already satisfied replication request for block {}",
                block_id
            );
            return Ok(());
        };
        if block_state.locations.is_empty() {
            return err_box!("missing source location for block {}", block_id);
        }

        let source = Self::source_endpoint(
            &self.worker_manager.read(),
            block_id,
            &block_state.locations,
        )?;
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
        let target = {
            let fs_dir = self.fs.fs_dir.read();
            let block_state = fs_dir.replication_block_state(block_id)?;
            let Some(block_state) = block_state.filter(|s| s.needs_replica(block_id, &self.worker_manager.read()))
            else {
                if self.tracker.cancel_queued(block_id) {
                    self.metrics.replication_staging_number.dec();
                }
                return Ok(());
            };
            // Namespace before WorkerManager, as in foreground placement. Do not
            // reserve from a stale block length or location list after connecting.
            if !block_state
                .locations
                .iter()
                .any(|loc| loc.worker_id == source.address.worker_id)
            {
                return err_box!("Replication source no longer holds block {}", block_id);
            }
            self.assign(
                block_id,
                &attempt_id,
                block_state.len,
                block_state
                    .locations
                    .iter()
                    .map(|loc| loc.worker_id)
                    .collect(),
            )?
        };
        let attempt = ReplicationAttempt::new(
            attempt_id.clone(),
            source.address.worker_id,
            source.session_id,
            target.address.clone(),
            target.session_id.clone(),
        );
        let deadline_ms = self.monotonic_millis().saturating_add(self.job_timeout_ms);
        if !self
            .tracker
            .promote(block_id, attempt.clone(), deadline_ms, permit)
        {
            self.settle_target(block_id, &attempt, false);
            return err_box!("Replication block {} is no longer queued", block_id);
        }
        self.metrics.replication_staging_number.dec();
        self.metrics.replication_inflight_number.inc();

        let remaining = match target_duration(submit_deadline) {
            Ok(remaining) => remaining,
            Err(e) => {
                self.cancel_known_inactive_attempt(block_id, &attempt);
                return Err(e);
            }
        };
        let target_addr = target.address.inet_addr();
        let client = match timeout(
            remaining,
            self.worker_client_factory.create_raw(&target_addr),
        )
        .await
        {
            Ok(Ok(client)) => client,
            Ok(Err(e)) => {
                self.cancel_known_inactive_attempt(block_id, &attempt);
                return err_box!(
                    "Cannot connect to replication target {}: {}",
                    target_addr,
                    e
                );
            }
            Err(e) => {
                self.cancel_known_inactive_attempt(block_id, &attempt);
                self.metrics.replication_timeout_count.inc();
                return err_box!(
                    "Timed out connecting to replication target {}: {}",
                    target_addr,
                    e
                );
            }
        };
        let remaining = match target_duration(submit_deadline) {
            Ok(duration) => duration,
            Err(e) => {
                self.cancel_known_inactive_attempt(block_id, &attempt);
                return Err(e);
            }
        };
        let prepared = timeout(remaining, async {
            let request = PrepareReplicationRequest {
                block_id,
                attempt_id: attempt_id.clone(),
                target_session_id: target.session_id.clone(),
                lifetime_ms: self
                    .job_timeout_ms
                    .saturating_add(self.submit_timeout.as_millis() as u64)
                    .saturating_mul(2),
            };
            let response = client
                .rpc(
                    Builder::new()
                        .code(RpcCode::PrepareReplication)
                        .request(RequestStatus::Rpc)
                        .proto_header(request)
                        .build(),
                )
                .await?;
            response.check_error_ext::<curvine_error::FsError>()?;
            response.parse_header::<PrepareReplicationResponse>()
        })
        .await;
        let prepared = match prepared {
            Ok(Ok(response)) => response,
            other => {
                if other.is_err() {
                    self.metrics.replication_timeout_count.inc();
                }
                // Prepare may have reserved the target even if its response was lost. Keep
                // cleanup ownership until reconciliation confirms revocation; Uncertain
                // releases the scheduler permit without abandoning that reservation.
                self.transition_to_uncertain(block_id, &attempt_id, "target prepare failed");
                return err_box!("Cannot prepare fenced replication target: {:?}", other);
            }
        };
        crate::fault_point! {
            async,
            name: "master.replication.prepared",
            description: "after receiving a target token but before checking the submit deadline",
            context: {"block_id" => block_id},
        }
        // Reconciliation can overtake a slow Prepare. A token returned after
        // the attempt stopped owning its credit must never be sent to a source.
        if !self.tracker.is_inflight(block_id, &attempt_id) {
            return err_box!(
                "Replication attempt {} no longer active after Prepare",
                attempt_id
            );
        }
        let request = SubmitBlockReplicationRequest {
            block_id,
            target_worker_info: ProtoUtils::worker_address_to_pb(&target.address),
            attempt_id: Some(attempt_id.clone()),
            target_token: Some(prepared.token),
            job_timeout_ms: Some(self.job_timeout_ms),
        };
        let msg = Builder::new_rpc(RpcCode::SubmitFencedReplication)
            .request(RequestStatus::Rpc)
            .proto_header(request)
            .build();

        let remaining = match target_duration(submit_deadline) {
            Ok(duration) => duration,
            Err(e) => {
                // The source was not submitted, but Prepare already reserved the target.
                // Reconciliation must revoke that token before forgetting this attempt.
                self.metrics.replication_timeout_count.inc();
                self.transition_to_uncertain(
                    block_id,
                    &attempt_id,
                    "submit deadline expired after target prepare",
                );
                return Err(e);
            }
        };
        match timeout(remaining, source_worker_client.rpc(msg)).await {
            Ok(Ok(response)) => {
                if let Err(e) = response.check_error_ext::<curvine_error::FsError>() {
                    self.transition_to_uncertain(
                        block_id,
                        &attempt_id,
                        "source returned RPC error",
                    );
                    return Err(e.into());
                }
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
                    // Revoke the prepared destination token before scheduling another attempt.
                    self.metrics.replication_failure_count.inc();
                    self.transition_to_uncertain(block_id, &attempt_id, "source rejected submit");
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

        crate::fault_point! {
            sync,
            name: "master.replication.submit_acked",
            description: "After the source acknowledges a replication submission",
            context: { "block_id" => block_id },
            return_error: |fault| err_box!("{}", fault.message),
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
            self.repairs
                .lock()
                .record(block_id, self.monotonic_millis());
            self.queue_block(block_id);
        }
        Ok(())
    }

    /// Whether a repair demand is recorded for `block_id`.
    #[cfg(test)]
    pub(crate) fn has_pending_repair(&self, block_id: BlockId) -> bool {
        self.repairs.lock().blocks.contains_key(&block_id)
    }

    fn queue_block(&self, block_id: BlockId) {
        if !self.tracker.queue(block_id, self.monotonic_millis()) {
            return;
        }
        self.metrics.replication_staging_number.inc();
        if let Err(e) = self.staging_queue_sender.try_send(block_id) {
            if self.tracker.cancel_queued(block_id) {
                self.metrics.replication_staging_number.dec();
            }
            error!(
                "Failed to queue replication block {} (repair demand retained): {}",
                block_id, e
            );
        }
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
        if success {
            let location = BlockLocation::new(
                snapshot.attempt.target_worker.worker_id,
                req.storage_type.into(),
            );
            metadata_result = self
                .commit_replication_location(block_id, location, &snapshot.attempt.target_worker_session_id, req.attempt_id.is_some())
                .map(|added| match added {
                    Some(true) => info!("Successfully replicated {}", block_id),
                    Some(false) => info!(
                        "Replication result for block {} no longer needs a metadata update or its target is no longer Live",
                        block_id
                    ),
                    None => {
                        success = false;
                        warn!("Ignoring replication result for block {} because target worker {} session is no longer current", block_id, snapshot.attempt.target_worker.worker_id);
                    }
                });
        }
        if !success {
            error!(
                "Errors on block replication for block_id: {} to worker: {}. error: {:?}",
                block_id, snapshot.attempt.target_worker, req.message
            );
            self.metrics.replication_failure_count.inc();
        }

        let now_ms = self.monotonic_millis();
        if metadata_result.is_err() || !success {
            // Reconciliation rechecks destination evidence before retrying metadata.
            // Never replay a cached success over a later failure without that check.
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
        } else {
            self.settle_target(block_id, &snapshot.attempt, true);
            if !self
                .tracker
                .finish_completing(block_id, &snapshot.attempt.attempt_id, now_ms)
            {
                warn!(
                    "Replication attempt completion marker disappeared: block={}, attempt={}, previous_state={:?}",
                    block_id, snapshot.attempt.attempt_id, snapshot.kind
                );
            }
            self.release_target(block_id, &snapshot.attempt);
        }
        Ok(metadata_result?)
    }

    /// `None` means the target session is no longer current, so the attempt
    /// still needs reconciliation rather than normal completion.
    fn commit_replication_location(
        &self,
        block_id: BlockId,
        location: BlockLocation,
        target_session: &str,
        fenced_result: bool,
    ) -> curvine_error::FsResult<Option<bool>> {
        crate::fault_point! {
            sync,
            name: "master.replication.before_metadata_commit",
            description: "Before a successful replication result is inserted into metadata",
            context: { "block_id" => block_id },
            return_error: |fault| Err(curvine_error::FsError::common(fault.message)),
        }
        let mut fs_dir = self.fs.fs_dir.write();
        let session_matches = |workers: &WorkerManager| {
            workers
                .get_worker(location.worker_id)
                .is_some_and(|worker| worker.worker_session_id == target_session)
        };
        if !self.fs.conf.decommission_drain_enabled || !fenced_result || target_session.is_empty() {
            let workers = self.worker_manager.read();
            if !session_matches(&workers) {
                return Ok(None);
            }
            return fs_dir
                .add_replication_location_if_needed(block_id, location, &workers)
                .map(Some);
        }
        let epoch = self.fs.drain_delete_epoch();
        let mut workers = self.worker_manager.write();
        if !session_matches(&workers) {
            return Ok(None);
        }
        let replacement_id = location.worker_id;
        let added = fs_dir.add_replication_location_if_needed(block_id, location, &workers)?;
        if added && self.fs.can_dispatch_drain_deletes(epoch) {
            // Queue only after the location commit. If the follow-up read fails,
            // the commit still succeeded; leave the old replicas alone.
            match fs_dir.replication_block_state(block_id) {
                Ok(Some(state)) => {
                    workers.queue_drain_deletes(block_id, &state, replacement_id, epoch)
                }
                Ok(None) => (),
                Err(error) => warn!(
                    "Cannot plan drain deletion for block {}: {}",
                    block_id, error
                ),
            }
        }
        Ok(Some(added))
    }

    fn release_target(&self, block_id: BlockId, attempt: &ReplicationAttempt) {
        let Ok(permit) = self.recovery_semaphore.clone().try_acquire_owned() else {
            return;
        };
        let me = self.clone();
        let attempt = attempt.clone();
        self.runtime.spawn(async move {
            let _permit = permit;
            let result = timeout(me.submit_timeout, async {
                let client = me
                    .worker_client_factory
                    .create_raw(&attempt.target_worker.inet_addr())
                    .await?;
                let response = client
                    .rpc(
                        Builder::new()
                            .code(RpcCode::ReconcileReplication)
                            .request(RequestStatus::Rpc)
                            .proto_header(ReconcileReplicationRequest {
                                block_id,
                                attempt_id: attempt.attempt_id,
                                target_session_id: attempt.target_worker_session_id,
                                release: true,
                            })
                            .build(),
                    )
                    .await?;
                response.check_error_ext::<curvine_error::FsError>()
            })
            .await;
            if !matches!(result, Ok(Ok(()))) {
                warn!(
                    "Replication target cleanup deferred to lease expiry for block {}",
                    block_id
                );
            }
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_source_prefers_live_and_skips_unusable_locations() -> CommonResult<()> {
        use curvine_model::WorkerStatus;

        let mut workers = WorkerManager::new(&ClusterConf::default())?;
        for (id, status) in [
            (1, WorkerStatus::Blacklist),
            (2, WorkerStatus::Lost), // Passive expiry can leave a Lost registration.
            (3, WorkerStatus::Unknown),
            (4, WorkerStatus::Decommission),
            (5, WorkerStatus::Live),
        ] {
            workers.add_test_worker(WorkerInfo {
                address: WorkerAddress {
                    worker_id: id,
                    ..Default::default()
                },
                status,
                worker_session_id: format!("session-{id}"),
                ..Default::default()
            });
        }
        let locations = [6, 1, 2, 3, 4, 5].map(BlockLocation::with_id);
        assert_eq!(workers.counted_replicas(1, &locations), 1);
        let live = MasterReplicationManager::source_endpoint(&workers, 1, &locations)?;
        assert_eq!(live.address.worker_id, 5);
        assert_eq!(live.session_id, "session-5");

        assert_eq!(workers.counted_replicas(1, &locations[..5]), 0);
        let fallback = MasterReplicationManager::source_endpoint(&workers, 1, &locations[..5])?;
        assert_eq!(fallback.address.worker_id, 4);
        assert_eq!(fallback.session_id, "session-4");
        assert!(MasterReplicationManager::source_endpoint(&workers, 1, &locations[..4]).is_err());

        // The lost map can retain a decommissioned host after eviction. It
        // must not be used as a fallback regardless of its displayed status.
        workers.remove_expired_worker(4).unwrap();
        assert!(workers.get_known_worker(4).is_some());
        assert!(MasterReplicationManager::source_endpoint(&workers, 1, &locations[..5]).is_err());
        Ok(())
    }

    #[test]
    fn repeated_loss_notifications_keep_one_due_entry() {
        let mut repairs = RepairQueue::default();
        for now in 0..100 {
            repairs.record(7, now);
        }
        assert_eq!(repairs.blocks.len(), 1);
        assert_eq!(repairs.due.len(), 1);
        assert_eq!(repairs.due.pop(), Some(Reverse((0, 7, 0))));
        assert_eq!(repairs.blocks.get(&7), Some(&99));
    }

    #[test]
    fn new_loss_notification_survives_retirement_of_an_old_snapshot() {
        let mut repairs = RepairQueue::default();
        repairs.record(7, 0);
        repairs.due.pop();
        let generation = repairs.blocks.get(&7).copied();
        // The filesystem snapshot says healthy, then a new loss is reported.
        repairs.record(7, 1);
        repairs.retire_if_unchanged(7, generation, 2);
        assert!(repairs.blocks.contains_key(&7));
        assert_eq!(repairs.due.pop(), Some(Reverse((2, 7, 0))));
        // With no further notification, a fresh healthy snapshot can retire it.
        let generation = repairs.blocks.get(&7).copied();
        repairs.retire_if_unchanged(7, generation, 3);
        assert!(repairs.blocks.is_empty());
        assert!(repairs.due.is_empty());
    }
}
