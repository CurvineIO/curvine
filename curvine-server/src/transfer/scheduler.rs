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

use crate::common::UfsFactory;
use crate::transfer::{
    is_store_unavailable_error, job_mount_snapshot, transfer_failure_message, TransferPlanner,
    TransferRequeueUpdate, TransferStore,
};
use curvine_common::conf::TransferConf;
use curvine_common::error::FsError;
use curvine_common::state::{
    summarize_transfer_tasks, LoadJobInfo, LoadTaskInfo, TaskAttemptStart, TransferCommand,
    TransferJobRecord, TransferLease, TransferProgress, TransferState, TransferStateUpdate,
    TransferTaskRecord, TransferTaskReport, TransferTaskReportInfo, TransferTaskState, WorkerInfo,
};
use curvine_common::FsResult;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use orpc::common::LocalTime;
use orpc::runtime::RpcRuntime;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PLANNING_REQUEUE_DELAY_MS: i64 = 1_000;

use crate::transfer::{ClusterMetadataCache, TransferMetrics};

const TRANSFER_CLEANUP_INTERVAL_MS: u64 = 60_000;

pub struct TransferScheduler<S> {
    store: Arc<S>,
    planner: TransferPlanner,
    cache: ClusterMetadataCache,
    factory: Arc<UfsFactory>,
    owner: String,
    report_target: String,
    conf: TransferConf,
    worker_cursor: Arc<AtomicUsize>,
    last_cleanup_ms: Arc<AtomicU64>,
}

impl<S> Clone for TransferScheduler<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            planner: self.planner.clone(),
            cache: self.cache.clone(),
            factory: self.factory.clone(),
            owner: self.owner.clone(),
            report_target: self.report_target.clone(),
            conf: self.conf.clone(),
            worker_cursor: self.worker_cursor.clone(),
            last_cleanup_ms: self.last_cleanup_ms.clone(),
        }
    }
}

impl<S> TransferScheduler<S>
where
    S: TransferStore,
{
    pub fn new(
        store: Arc<S>,
        planner: TransferPlanner,
        cache: ClusterMetadataCache,
        factory: Arc<UfsFactory>,
        owner: String,
        report_target: String,
        conf: TransferConf,
    ) -> Self {
        Self {
            store,
            planner,
            cache,
            factory,
            owner,
            report_target,
            conf,
            worker_cursor: Arc::new(AtomicUsize::new(0)),
            last_cleanup_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn start(self, rt: Arc<orpc::runtime::Runtime>, stop: Arc<AtomicBool>) {
        let workers = self.conf.scheduler_workers();
        for index in 0..workers {
            let scheduler = self.clone();
            let stop = stop.clone();
            rt.spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    if let Err(err) = scheduler.run_once().await {
                        warn!("transfer scheduler worker {} tick failed: {}", index, err);
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            });
        }
    }

    async fn run_once(&self) -> FsResult<()> {
        let now_ms = now_ms();
        self.cleanup_terminal_transfers(now_ms)?;
        let (active, executing) = self.transfer_counts()?;
        self.record_job_counts(active, executing);
        let Some(lease) = self.store.acquire_runnable_transfer(
            &self.owner,
            duration_ms(self.conf.lease_timeout),
            now_ms,
            self.conf.max_executing_transfers(),
        )?
        else {
            record_metric(|metrics| metrics.inc_acquire("empty"));
            return Ok(());
        };
        record_metric(|metrics| metrics.inc_acquire("ok"));

        let Some(job) = self.store.get_transfer(&lease.job_id)? else {
            record_metric(|metrics| metrics.inc_acquire("missing_job"));
            return Ok(());
        };

        if job.cancel_requested {
            self.cancel_transfer(&job, &lease).await?;
            return Ok(());
        }

        match job.state {
            TransferState::Pending | TransferState::Planning => {
                self.plan_transfer(job, lease).await?;
            }
            TransferState::Dispatching => {
                self.dispatch_pending_tasks(job, lease).await?;
            }
            TransferState::Running => {
                self.check_running_transfer(job, lease).await?;
            }
            TransferState::Canceling => {
                self.cancel_transfer(&job, &lease).await?;
            }
            TransferState::Completed | TransferState::Failed | TransferState::Canceled => {}
        }
        Ok(())
    }

    fn transfer_counts(&self) -> FsResult<(u64, u64)> {
        Ok((
            self.store.count_active_transfers()?,
            self.store.count_executing_transfers()?,
        ))
    }

    fn record_job_counts(&self, active: u64, executing: u64) {
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_job_counts(active, executing);
        }
    }

    fn cleanup_terminal_transfers(&self, now_ms: i64) -> FsResult<()> {
        let now = now_ms.max(0) as u64;
        let last = self.last_cleanup_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < TRANSFER_CLEANUP_INTERVAL_MS {
            return Ok(());
        }
        if self
            .last_cleanup_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }
        let retention_ms =
            i64::try_from(self.conf.terminal_retention.as_millis()).unwrap_or(i64::MAX);
        let older_than = now_ms.saturating_sub(retention_ms);
        let purged = self
            .store
            .purge_terminal_transfers(older_than, self.conf.cleanup_batch_size())?;
        if purged >= self.conf.cleanup_batch_size() {
            self.last_cleanup_ms.store(0, Ordering::Relaxed);
        }
        if purged > 0 {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.inc_cleanup_purged(purged);
            }
            info!(
                "purged {} terminal transfer jobs older than {}",
                purged, older_than
            );
        }
        Ok(())
    }

    async fn plan_transfer(&self, job: TransferJobRecord, lease: TransferLease) -> FsResult<()> {
        if !self.transition(
            &job,
            &lease,
            &[TransferState::Pending, TransferState::Planning],
            TransferState::Planning,
            "planning transfer",
        )? {
            record_metric(|metrics| metrics.inc_planning(transfer_kind_label(job.kind), "stale"));
            return Ok(());
        }

        let planned = match self.planner.plan(&job).await {
            Ok(planned) => planned,
            Err(err @ FsError::InProgress(_)) => {
                record_metric(|metrics| {
                    metrics.inc_planning(transfer_kind_label(job.kind), "retry")
                });
                let now = now_ms();
                let requeued = self.store.requeue_transfer(TransferRequeueUpdate {
                    job_id: job.job_id.clone(),
                    run_id: job.run_id,
                    owner: lease.owner.clone(),
                    lease_epoch: lease.lease_epoch,
                    message: format!("planning delayed: {}", err),
                    next_attempt_at_ms: now.saturating_add(PLANNING_REQUEUE_DELAY_MS),
                    now_ms: now,
                })?;
                if requeued {
                    info!(
                        "transfer state requeue job_id={} run_id={} kind={:?} tenant={} owner={} lease_epoch={} state={:?} next_attempt_at_ms={} reason={}",
                        job.job_id,
                        job.run_id,
                        job.kind,
                        job.tenant,
                        lease.owner,
                        lease.lease_epoch,
                        job.state,
                        now.saturating_add(PLANNING_REQUEUE_DELAY_MS),
                        err
                    );
                }
                return Ok(());
            }
            Err(err) => {
                record_metric(|metrics| {
                    metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                });
                self.fail_transfer(
                    &job,
                    transfer_failure_message(job.kind, &job.source_path, &job.target_path, &err),
                )?;
                return Err(err);
            }
        };

        if let Some(cv_metadata_epoch) = planned.cv_metadata_epoch {
            let updated = self.store.set_transfer_cv_metadata_epoch(
                &job.job_id,
                job.run_id,
                &lease.owner,
                lease.lease_epoch,
                cv_metadata_epoch,
                now_ms(),
            )?;
            if !updated {
                warn!(
                    "stop planning export transfer {} because cv metadata epoch {} could not be persisted",
                    job.job_id, cv_metadata_epoch
                );
                return Ok(());
            }
        }

        if planned.tasks.is_empty() {
            self.set_transfer_state(
                &job,
                TransferState::Completed,
                "transfer has no file tasks",
                now_ms(),
            )?;
            record_metric(|metrics| {
                metrics.inc_planning(transfer_kind_label(job.kind), "empty");
                metrics.inc_terminal("completed", "empty");
            });
            info!("transfer {} completed with no file tasks", job.job_id);
            return Ok(());
        }

        let task_count = planned.tasks.len();
        self.store.insert_tasks(planned.tasks)?;
        self.set_transfer_state(
            &job,
            TransferState::Dispatching,
            format!("planned total_size={}", planned.total_size),
            now_ms(),
        )?;
        debug!(
            "transfer {} planned with job info source={}, target={}",
            job.job_id, planned.job_info.source_path, planned.job_info.target_path
        );
        record_metric(|metrics| {
            metrics.inc_planning(transfer_kind_label(job.kind), "success");
            metrics.inc_dispatch("planned_tasks", task_count);
        });

        self.dispatch_pending_tasks(job, lease).await
    }

    async fn dispatch_pending_tasks(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let mut dispatched = 0usize;
        loop {
            if self.cancel_requested(&job, &lease).await? {
                return Ok(());
            }
            let tasks = self.store.claim_pending_tasks(
                &job.job_id,
                job.run_id,
                self.conf.planning_batch_size(),
            )?;
            if tasks.is_empty() {
                break;
            }

            let started = match self.dispatch_batch(&job, &lease, tasks).await {
                Ok(started) => started,
                Err(err) => {
                    record_metric(|metrics| metrics.inc_dispatch("failure", 1));
                    if is_store_unavailable_error(&err) {
                        warn!(
                            "pause dispatching transfer {} because transfer store is unavailable: {}",
                            job.job_id, err
                        );
                        return Err(err);
                    }
                    self.fail_transfer(
                        &job,
                        transfer_failure_message(
                            job.kind,
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            dispatched += started;
            record_metric(|metrics| metrics.inc_dispatch("started", started));
            if started == 0 {
                record_metric(|metrics| metrics.inc_dispatch("not_started", 1));
                break;
            }
            let renewed = self.store.renew_lease(
                &lease.job_id,
                lease.run_id,
                &lease.owner,
                lease.lease_epoch,
                duration_ms(self.conf.lease_timeout),
                now_ms(),
            )?;
            record_metric(|metrics| {
                metrics.inc_lease_renew(if renewed { "success" } else { "stale" })
            });
            if !renewed {
                warn!(
                    "stop dispatching transfer {} because owner {} lease epoch {} is stale",
                    lease.job_id, lease.owner, lease.lease_epoch
                );
                return Ok(());
            }
        }

        if self.cancel_requested(&job, &lease).await? {
            return Ok(());
        }
        let _ = self.transition(
            &job,
            &lease,
            &[TransferState::Dispatching],
            TransferState::Running,
            format!("dispatched {} tasks", dispatched),
        )?;
        Ok(())
    }

    async fn cancel_requested(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
    ) -> FsResult<bool> {
        let Some(current) = self.store.get_transfer(&job.job_id)? else {
            return Ok(false);
        };
        if current.run_id != job.run_id || !current.cancel_requested {
            return Ok(false);
        }
        self.cancel_transfer(&current, lease).await?;
        Ok(true)
    }

    async fn dispatch_batch(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        tasks: Vec<TransferTaskRecord>,
    ) -> FsResult<usize> {
        let workers = match self.cache.live_workers() {
            Ok(workers) => workers,
            Err(err) => {
                debug!(
                    "no live worker with required transfer capabilities is available for transfer {}: {}",
                    job.job_id, err
                );
                return Ok(0);
            }
        };
        let mount = job_mount_snapshot(job, &self.cache)?;
        let job_info = self.planner_job_info(job, &mount);
        let limit = self.conf.worker_dispatch_concurrency();

        let started = stream::iter(tasks.into_iter().map(|task| {
            let worker = self.choose_worker(&workers);
            let store = self.store.clone();
            let factory = self.factory.clone();
            let cache = self.cache.clone();
            let job_info = job_info.clone();
            let owner = lease.owner.clone();
            let lease_epoch = lease.lease_epoch;
            let report_target = self.report_target.clone();
            let task_stale_timeout_ms = duration_ms(self.conf.task_stale_timeout);
            async move {
                dispatch_one(DispatchTaskRequest {
                    store,
                    factory,
                    cache,
                    job_info,
                    owner,
                    lease_epoch,
                    task,
                    worker,
                    report_target,
                    task_stale_timeout_ms,
                })
                .await
            }
        }))
        .buffer_unordered(limit)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(started.into_iter().filter(|value| *value).count())
    }

    fn planner_job_info(
        &self,
        job: &TransferJobRecord,
        mount: &curvine_common::state::MountInfo,
    ) -> curvine_common::state::LoadJobInfo {
        let overwrite = transfer_command(job)
            .map(|command| command.overwrite())
            .unwrap_or(true);
        curvine_common::state::LoadJobInfo {
            job_id: job.job_id.clone(),
            source_path: job.source_path.clone(),
            target_path: job.target_path.clone(),
            replicas: mount.replicas.unwrap_or(self.conf_default_replicas()),
            block_size: mount.block_size.unwrap_or(self.conf_default_block_size()),
            storage_type: mount
                .storage_type
                .unwrap_or(self.conf_default_storage_type()),
            ttl_ms: mount.ttl_ms,
            ttl_action: mount.ttl_action,
            mount_info: mount.clone(),
            create_time: job.created_at,
            overwrite: Some(overwrite),
        }
    }

    fn conf_default_replicas(&self) -> i32 {
        curvine_common::conf::ClientConf::default().replicas
    }

    fn conf_default_block_size(&self) -> i64 {
        curvine_common::conf::ClientConf::default().block_size
    }

    fn conf_default_storage_type(&self) -> curvine_common::state::StorageType {
        curvine_common::conf::ClientConf::default().storage_type
    }

    async fn check_running_transfer(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let now = now_ms();
        self.probe_stale_running_tasks(&job, now).await?;
        let stale = self.store.mark_stale_attempts(
            &job.job_id,
            job.run_id,
            &lease.owner,
            lease.lease_epoch,
            now,
            self.conf.task_probe_concurrency(),
        )?;
        for attempt in stale {
            if attempt.task.retry_count as usize > self.conf.task_max_retries {
                self.store.update_task_state(
                    &attempt.task.job_id,
                    attempt.task.run_id,
                    &attempt.task.task_id,
                    TransferTaskState::Failed,
                    "transfer task did not report progress before the retry limit was reached; check Transfer worker health",
                    now,
                )?;
                self.fail_transfer(
                    &job,
                    "transfer task did not report progress before the retry limit was reached; check Transfer worker health",
                )?;
                record_metric(|metrics| metrics.inc_stale_retry("exhausted"));
                return Ok(());
            }
            self.store.update_task_state(
                &attempt.task.job_id,
                attempt.task.run_id,
                &attempt.task.task_id,
                TransferTaskState::Pending,
                "retry stale task attempt",
                now,
            )?;
            record_metric(|metrics| metrics.inc_stale_retry("scheduled"));
        }

        if !self
            .store
            .claim_pending_tasks(&job.job_id, job.run_id, 1)?
            .is_empty()
        {
            self.set_transfer_state(
                &job,
                TransferState::Dispatching,
                "redispatch stale tasks",
                now,
            )?;
            self.dispatch_pending_tasks(job, lease).await?;
            return Ok(());
        }

        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        if tasks.is_empty() {
            return Ok(());
        }

        let summary = summarize_transfer_tasks(&tasks, now);
        if summary.has_failed {
            self.fail_transfer(&job, summary.progress.message)?;
            return Ok(());
        }

        if summary.all_completed {
            self.set_transfer_state(&job, TransferState::Completed, "all tasks completed", now)?;
            record_metric(|metrics| metrics.inc_terminal("completed", "all_tasks_completed"));
        } else {
            let renewed = self.store.renew_lease(
                &lease.job_id,
                lease.run_id,
                &lease.owner,
                lease.lease_epoch,
                duration_ms(self.conf.lease_timeout),
                now,
            )?;
            record_metric(|metrics| {
                metrics.inc_lease_renew(if renewed { "success" } else { "stale" })
            });
        }
        Ok(())
    }

    async fn probe_stale_running_tasks(&self, job: &TransferJobRecord, now: i64) -> FsResult<()> {
        let tasks = self.store.list_recoverable_tasks(&job.job_id, job.run_id)?;
        let workers = self.cache.live_workers().unwrap_or_default();
        for task in tasks
            .into_iter()
            .filter(|task| task.state == TransferTaskState::Running && task.stale_deadline_at < now)
            .take(self.conf.task_probe_concurrency())
        {
            let Some(worker) = workers.iter().find(|worker| {
                worker.worker_id() == task.worker_id
                    && worker.worker_session_id == task.worker_session_id
            }) else {
                continue;
            };
            let Ok(client) = self.factory.get_worker_client(&worker.address).await else {
                continue;
            };
            let Ok(response) = client
                .query_transfer_task(
                    &task.job_id,
                    task.run_id,
                    &task.task_id,
                    task.attempt_id,
                    &task.worker_session_id,
                )
                .await
            else {
                continue;
            };
            if !response.found {
                continue;
            }
            let report = TransferTaskReport {
                job_id: task.job_id,
                run_id: task.run_id,
                task_id: task.task_id,
                attempt_id: task.attempt_id,
                worker_id: task.worker_id,
                worker_session_id: task.worker_session_id,
                state: transfer_task_state(response.state),
                progress: TransferProgress {
                    loaded_size: response.progress.loaded_size,
                    total_size: response.progress.total_size,
                    update_time: response.progress.update_time,
                    message: response.progress.message,
                },
                now_ms: now,
                stale_deadline_at: now.saturating_add(duration_ms(self.conf.task_stale_timeout)),
            };
            let _ = self.store.update_task_report(report)?;
        }
        Ok(())
    }

    async fn cancel_transfer(
        &self,
        job: &TransferJobRecord,
        _lease: &TransferLease,
    ) -> FsResult<()> {
        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        let mut workers = HashSet::new();
        for task in tasks {
            if task.worker_id != 0 {
                workers.insert(task.worker_id);
            }
            self.store.update_task_state(
                &task.job_id,
                task.run_id,
                &task.task_id,
                TransferTaskState::Canceled,
                "transfer canceled",
                now_ms(),
            )?;
        }

        let live_workers = match self.cache.live_workers() {
            Ok(workers) => workers,
            Err(err) => {
                warn!(
                    "cancel transfer {} without live worker snapshot: {}",
                    job.job_id, err
                );
                Vec::new()
            }
        };
        for worker in live_workers {
            if workers.contains(&worker.worker_id()) {
                match self.factory.get_worker_client(&worker.address).await {
                    Ok(client) => {
                        if let Err(err) = client.cancel_job(&job.job_id).await {
                            warn!(
                                "cancel transfer {} on worker {} failed: {}",
                                job.job_id, worker, err
                            );
                        }
                    }
                    Err(err) => warn!("connect worker {} for cancel failed: {}", worker, err),
                }
            }
        }

        self.set_transfer_state(job, TransferState::Canceled, "transfer canceled", now_ms())?;
        record_metric(|metrics| metrics.inc_terminal("canceled", "cancel_requested"));
        Ok(())
    }

    fn choose_worker(&self, workers: &[WorkerInfo]) -> WorkerInfo {
        let index = self.worker_cursor.fetch_add(1, Ordering::Relaxed) % workers.len();
        workers[index].clone()
    }

    fn transition(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        from_states: &[TransferState],
        to_state: TransferState,
        message: impl Into<String>,
    ) -> FsResult<bool> {
        let message = message.into();
        let updated = self.store.update_transfer_state(TransferStateUpdate {
            job_id: lease.job_id.clone(),
            run_id: lease.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            from_states: from_states.to_vec(),
            to_state,
            message: message.clone(),
            now_ms: now_ms(),
        })?;
        if updated {
            info!(
                "transfer state transition job_id={} run_id={} kind={:?} tenant={} owner={} lease_epoch={} from={:?} to={:?} message={}",
                lease.job_id,
                lease.run_id,
                job.kind,
                job.tenant,
                lease.owner,
                lease.lease_epoch,
                from_states,
                to_state,
                message
            );
        }
        Ok(updated)
    }

    fn set_transfer_state(
        &self,
        job: &TransferJobRecord,
        to_state: TransferState,
        message: impl Into<String>,
        now_ms: i64,
    ) -> FsResult<bool> {
        let message = message.into();
        let updated = self.store.set_transfer_state(
            &job.job_id,
            job.run_id,
            to_state,
            message.clone(),
            now_ms,
        )?;
        if updated {
            info!(
                "transfer state set job_id={} run_id={} kind={:?} tenant={} owner={} lease_epoch={} from={:?} to={:?} message={}",
                job.job_id,
                job.run_id,
                job.kind,
                job.tenant,
                job.owner,
                job.lease_epoch,
                job.state,
                to_state,
                message
            );
        }
        Ok(updated)
    }

    fn fail_transfer(&self, job: &TransferJobRecord, message: impl Into<String>) -> FsResult<()> {
        let message = message.into();
        error!("transfer {} failed: {}", job.job_id, message);
        self.set_transfer_state(job, TransferState::Failed, message, now_ms())?;
        record_metric(|metrics| metrics.inc_terminal("failed", "error"));
        Ok(())
    }
}

struct DispatchTaskRequest<S> {
    store: Arc<S>,
    factory: Arc<UfsFactory>,
    cache: ClusterMetadataCache,
    job_info: LoadJobInfo,
    owner: String,
    lease_epoch: u64,
    task: TransferTaskRecord,
    worker: WorkerInfo,
    report_target: String,
    task_stale_timeout_ms: i64,
}

async fn dispatch_one<S>(request: DispatchTaskRequest<S>) -> FsResult<bool>
where
    S: TransferStore,
{
    let DispatchTaskRequest {
        store,
        factory,
        cache,
        job_info,
        owner,
        lease_epoch,
        task,
        worker,
        report_target,
        task_stale_timeout_ms,
    } = request;
    let now = now_ms();
    let attempt_id = task.attempt_id.saturating_add(1);
    let client = match factory.get_worker_client(&worker.address).await {
        Ok(client) => client,
        Err(err) => {
            warn!(
                "connect worker {} for transfer task {} failed before submit; retry in next scheduler tick: {}",
                worker, task.task_id, err
            );
            return Ok(false);
        }
    };
    let started = store.start_task_attempt(TaskAttemptStart {
        job_id: task.job_id.clone(),
        run_id: task.run_id,
        owner,
        lease_epoch,
        task_id: task.task_id.clone(),
        attempt_id,
        worker_id: worker.worker_id(),
        worker_session_id: worker.worker_session_id.clone(),
        report_target_json: report_target.clone(),
        now_ms: now,
        stale_deadline_at: now.saturating_add(task_stale_timeout_ms),
    })?;
    if !started {
        return Ok(false);
    }

    let load_task = LoadTaskInfo {
        job: job_info,
        task_id: task.task_id.clone(),
        worker: worker.address.clone(),
        source_path: task.source_path.clone(),
        target_path: task.target_path.clone(),
        create_time: now,
        source_read_plan_json: task.source_read_plan_json.clone(),
        transfer_report: Some(TransferTaskReportInfo {
            run_id: task.run_id,
            attempt_id,
            worker_id: worker.worker_id(),
            worker_session_id: worker.worker_session_id.clone(),
            report_target,
        }),
    };

    match client.submit_load_task_response(load_task).await {
        Ok(response) if response.accepted.unwrap_or(true) => Ok(true),
        Ok(response) => {
            let reason = response.reject_reason.unwrap_or_default();
            warn!(
                "worker {} rejected transfer task {} attempt {} before accepting it: {}",
                worker, task.task_id, attempt_id, reason
            );
            store.update_task_state(
                &task.job_id,
                task.run_id,
                &task.task_id,
                TransferTaskState::Pending,
                format!("worker rejected task before accept: {}", reason),
                now_ms(),
            )?;
            if let Err(err) = cache.refresh().await {
                warn!(
                    "refresh cluster metadata after worker rejection failed: {}",
                    err
                );
            }
            cache.remove_worker_session(worker.worker_id(), &worker.worker_session_id);
            Ok(false)
        }
        Err(err) => {
            warn!(
                "submit transfer task {} attempt {} to worker {} returned error after attempt start; keep running and let query/stale recovery decide final state: {}",
                task.task_id, attempt_id, worker, err
            );
            Ok(true)
        }
    }
}

fn now_ms() -> i64 {
    LocalTime::mills() as i64
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn transfer_command(job: &TransferJobRecord) -> FsResult<TransferCommand> {
    serde_json::from_str(&job.command_json).map_err(|_| {
        FsError::common(format!(
            "Stored transfer command for job {} is invalid",
            job.job_id
        ))
    })
}

fn transfer_task_state(state: i32) -> TransferTaskState {
    match state {
        1 => TransferTaskState::Pending,
        2 => TransferTaskState::Running,
        3 => TransferTaskState::Completed,
        4 => TransferTaskState::Failed,
        5 => TransferTaskState::Canceled,
        6 => TransferTaskState::Stale,
        _ => TransferTaskState::Failed,
    }
}

fn record_metric(f: impl FnOnce(&TransferMetrics)) {
    if let Ok(metrics) = TransferMetrics::get() {
        f(metrics);
    }
}

fn transfer_kind_label(kind: curvine_common::state::TransferKind) -> &'static str {
    match kind {
        curvine_common::state::TransferKind::Load => "load",
        curvine_common::state::TransferKind::Export => "export",
    }
}
