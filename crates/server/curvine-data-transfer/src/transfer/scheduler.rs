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
    is_store_unavailable_error, job_mount_snapshot, transfer_failure_message, TransferPlannedTasks,
    TransferPlanner, TransferRequeueUpdate, TransferStore, TransferTaskStateUpdate,
};
use curvine_config::TransferConf;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{
    summarize_transfer_tasks, LoadJobInfo, LoadTaskInfo, TaskAttemptStart, TransferJobRecord,
    TransferLease, TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord,
    TransferTaskReport, TransferTaskReportInfo, TransferTaskState, WorkerInfo,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::RpcRuntime;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
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
    report_endpoints: Vec<String>,
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
            report_endpoints: self.report_endpoints.clone(),
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
        report_endpoints: Vec<String>,
        conf: TransferConf,
    ) -> Self {
        Self {
            store,
            planner,
            cache,
            factory,
            owner,
            report_endpoints,
            conf,
            worker_cursor: Arc::new(AtomicUsize::new(0)),
            last_cleanup_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn start(self, rt: Arc<curvine_runtime::runtime::Runtime>, stop: Arc<AtomicBool>) {
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
            TransferState::Completed
            | TransferState::Failed
            | TransferState::Canceled
            | TransferState::PartialSuccess => {}
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
                    &lease,
                    &[TransferState::Planning],
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
            let message = format!(
                "incremental transfer complete: skipped_files={} skipped_size={}",
                planned.skipped_files, planned.skipped_size
            );
            self.transition(
                &job,
                &lease,
                &[TransferState::Planning],
                TransferState::Completed,
                &message,
            )?;
            record_metric(|metrics| {
                metrics.inc_planning(transfer_kind_label(job.kind), "empty");
                metrics.inc_terminal("completed", "empty");
            });
            info!(
                "transfer {} completed with no file tasks: skipped_files={} skipped_size={}",
                job.job_id, planned.skipped_files, planned.skipped_size
            );
            return Ok(());
        }

        let task_count = planned.tasks.len();
        let total_size = planned.total_size;
        let skipped_files = planned.skipped_files;
        let skipped_size = planned.skipped_size;
        let job_info = planned.job_info;
        let persisted = self.store.persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: planned.tasks,
            message: format!(
                "planned total_size={total_size} skipped_files={skipped_files} skipped_size={skipped_size}"
            ),
            now_ms: now_ms(),
        })?;
        if !persisted {
            warn!(
                "stop planning transfer {} because owner {} lease epoch {} is stale",
                job.job_id, lease.owner, lease.lease_epoch
            );
            return Ok(());
        }
        debug!(
            "transfer {} planned with job info source={}, target={}",
            job.job_id, job_info.source_path, job_info.target_path
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
                        &lease,
                        &[TransferState::Dispatching],
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
            let report_endpoints = self.report_endpoints.clone();
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
                    report_endpoints,
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
        mount: &curvine_model::MountInfo,
    ) -> curvine_model::LoadJobInfo {
        self.planner.load_job_info(job, mount)
    }

    async fn check_running_transfer(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let now = now_ms();
        self.probe_stale_running_tasks(&job, now).await?;
        let _ = self.store.mark_stale_attempts(
            &job.job_id,
            job.run_id,
            &lease.owner,
            lease.lease_epoch,
            now,
            self.conf.task_probe_concurrency(),
        )?;
        // Recover all Stale tasks (newly marked + orphan leftovers from CAS races).
        let stale_tasks = self.store.list_tasks_by_state(
            &job.job_id,
            job.run_id,
            TransferTaskState::Stale,
            self.conf.task_probe_concurrency(),
        )?;
        for task in stale_tasks {
            if task.retry_count as usize > self.conf.task_max_retries {
                if !self.store.update_task_state(TransferTaskStateUpdate {
                    job_id: task.job_id.clone(),
                    run_id: task.run_id,
                    owner: lease.owner.clone(),
                    lease_epoch: lease.lease_epoch,
                    task_id: task.task_id.clone(),
                    from_states: vec![TransferTaskState::Stale],
                    state: TransferTaskState::Failed,
                    message: "transfer task did not report progress before the retry limit was reached; check Transfer worker health".to_string(),
                    now_ms: now,
                })? {
                    return Ok(());
                }
                self.finish_failed_transfer(&job, &lease).await?;
                record_metric(|metrics| metrics.inc_stale_retry("exhausted"));
                return Ok(());
            }
            if !self.store.update_task_state(TransferTaskStateUpdate {
                job_id: task.job_id.clone(),
                run_id: task.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                task_id: task.task_id.clone(),
                from_states: vec![TransferTaskState::Stale],
                state: TransferTaskState::Pending,
                message: "retry stale task attempt".to_string(),
                now_ms: now,
            })? {
                return Ok(());
            }
            record_metric(|metrics| metrics.inc_stale_retry("scheduled"));
        }

        if self.store.has_failed_tasks(&job.job_id, job.run_id)? {
            self.finish_failed_transfer(&job, &lease).await?;
            return Ok(());
        }

        if !self
            .store
            .claim_pending_tasks(&job.job_id, job.run_id, 1)?
            .is_empty()
        {
            self.transition(
                &job,
                &lease,
                &[TransferState::Running],
                TransferState::Dispatching,
                "redispatch stale tasks",
            )?;
            self.dispatch_pending_tasks(job, lease).await?;
            return Ok(());
        }

        if self.store.has_recoverable_tasks(&job.job_id, job.run_id)? {
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
        } else {
            let _ = self.transition(
                &job,
                &lease,
                &[TransferState::Running],
                TransferState::Completed,
                "all tasks completed",
            )?;
            record_metric(|metrics| metrics.inc_terminal("completed", "all_tasks_completed"));
        }
        Ok(())
    }

    async fn probe_stale_running_tasks(&self, job: &TransferJobRecord, now: i64) -> FsResult<()> {
        let tasks = self.store.list_stale_running_tasks(
            &job.job_id,
            job.run_id,
            now,
            self.conf.task_probe_concurrency(),
        )?;
        let workers = self.cache.live_workers();
        for task in tasks {
            let worker = match &workers {
                Ok(workers) => workers
                    .iter()
                    .find(|worker| {
                        worker.worker_id() == task.worker_id
                            && worker.worker_session_id == task.worker_session_id
                    })
                    .cloned(),
                Err(err) => {
                    self.defer_stale_task_retry(
                        &task,
                        now,
                        "cluster worker snapshot unavailable",
                        err,
                    )?;
                    continue;
                }
            };
            let Some(worker) = worker else {
                continue;
            };
            let client = match self.factory.get_worker_client(&worker.address).await {
                Ok(client) => client,
                Err(err) => {
                    self.defer_stale_task_retry(&task, now, "cannot connect to worker", &err)?;
                    continue;
                }
            };
            let response = match client
                .query_transfer_task(
                    &task.job_id,
                    task.run_id,
                    &task.task_id,
                    task.attempt_id,
                    &task.worker_session_id,
                )
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    self.defer_stale_task_retry(
                        &task,
                        now,
                        "worker did not answer task probe",
                        &err,
                    )?;
                    continue;
                }
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

    fn defer_stale_task_retry(
        &self,
        task: &TransferTaskRecord,
        now: i64,
        reason: &str,
        err: &impl std::fmt::Display,
    ) -> FsResult<()> {
        let mut progress = task.progress.clone();
        progress.message =
            "worker status is temporarily unavailable; preserving the current attempt before retry"
                .to_string();
        progress.update_time = now;
        let deferred = self.store.update_task_report(TransferTaskReport {
            job_id: task.job_id.clone(),
            run_id: task.run_id,
            task_id: task.task_id.clone(),
            attempt_id: task.attempt_id,
            worker_id: task.worker_id,
            worker_session_id: task.worker_session_id.clone(),
            state: TransferTaskState::Running,
            progress,
            now_ms: now,
            stale_deadline_at: now.saturating_add(duration_ms(self.conf.task_stale_timeout)),
        })?;
        if deferred {
            debug!(
                "defer stale transfer task retry because {}: job={} run={} task={} attempt={} worker_id={} worker_session={} err={}",
                reason,
                task.job_id,
                task.run_id,
                task.task_id,
                task.attempt_id,
                task.worker_id,
                task.worker_session_id,
                err,
            );
        }
        Ok(())
    }

    async fn cancel_transfer(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
    ) -> FsResult<()> {
        self.stop_recoverable_tasks(job, lease, "transfer canceled")
            .await?;
        if self.store.has_recoverable_tasks(&job.job_id, job.run_id)? {
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
            return Ok(());
        }

        let transitioned = self.transition(
            job,
            lease,
            &[TransferState::Canceling],
            TransferState::Canceled,
            "transfer canceled",
        )?;
        if transitioned {
            record_metric(|metrics| metrics.inc_terminal("canceled", "cancel_requested"));
        }
        Ok(())
    }

    async fn finish_failed_transfer(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
    ) -> FsResult<()> {
        self.stop_recoverable_tasks(job, lease, "transfer stopped after a task failed")
            .await?;
        if self.store.has_recoverable_tasks(&job.job_id, job.run_id)? {
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
            return Ok(());
        }
        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        let summary = summarize_transfer_tasks(&tasks, now_ms());
        let state = if summary.counts.completed > 0 {
            TransferState::PartialSuccess
        } else {
            TransferState::Failed
        };
        let message = if state == TransferState::PartialSuccess {
            format!(
                "{}; {} file(s) completed and remain cached",
                summary.progress.message, summary.counts.completed
            )
        } else {
            summary.progress.message
        };
        if self.transition(job, lease, &[TransferState::Running], state, message)? {
            record_metric(|metrics| {
                metrics.inc_terminal(
                    if state == TransferState::PartialSuccess {
                        "partial_success"
                    } else {
                        "failed"
                    },
                    "task_failed",
                )
            });
        }
        Ok(())
    }

    async fn stop_recoverable_tasks(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        message: &str,
    ) -> FsResult<()> {
        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        let mut workers = HashSet::new();
        let now = now_ms();
        for task in tasks {
            if !matches!(
                task.state,
                TransferTaskState::Pending | TransferTaskState::Running | TransferTaskState::Stale
            ) {
                continue;
            }
            if task.state == TransferTaskState::Pending
                || task.state == TransferTaskState::Stale
                || (task.state == TransferTaskState::Running && task.stale_deadline_at <= now)
            {
                let _ = self.store.update_task_state(TransferTaskStateUpdate {
                    job_id: task.job_id.clone(),
                    run_id: task.run_id,
                    owner: lease.owner.clone(),
                    lease_epoch: lease.lease_epoch,
                    task_id: task.task_id,
                    from_states: vec![task.state],
                    state: TransferTaskState::Canceled,
                    message: message.to_string(),
                    now_ms: now,
                })?;
            } else if task.worker_id != 0 {
                workers.insert((task.worker_id, task.worker_session_id));
            }
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
            if workers.contains(&(worker.worker_id(), worker.worker_session_id.clone())) {
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

    fn fail_transfer(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        from_states: &[TransferState],
        message: impl Into<String>,
    ) -> FsResult<()> {
        let message = message.into();
        error!("transfer {} failed: {}", job.job_id, message);
        let _ = self.transition(job, lease, from_states, TransferState::Failed, message)?;
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
    report_endpoints: Vec<String>,
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
        report_endpoints,
        task_stale_timeout_ms,
    } = request;
    let now = now_ms();
    let attempt_id = task.attempt_id.saturating_add(1);
    let client = match factory.get_worker_client(&worker.address).await {
        Ok(client) => client,
        Err(err) => {
            warn!(
                "connect worker for transfer task failed before submit; retry in next scheduler tick: job={} run={} task={} attempt={} worker_id={} worker_session={} source={} target={} err={}",
                task.job_id,
                task.run_id,
                task.task_id,
                attempt_id,
                worker.worker_id(),
                worker.worker_session_id,
                task.source_path,
                task.target_path,
                err
            );
            return Ok(false);
        }
    };
    let started = store.start_task_attempt(TaskAttemptStart {
        job_id: task.job_id.clone(),
        run_id: task.run_id,
        owner: owner.clone(),
        lease_epoch,
        task_id: task.task_id.clone(),
        attempt_id,
        worker_id: worker.worker_id(),
        worker_session_id: worker.worker_session_id.clone(),
        report_target_json: serde_json::to_string(&report_endpoints)
            .map_err(|_| FsError::common("Unable to prepare transfer report endpoints"))?,
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
            report_target: report_endpoints.first().cloned().unwrap_or_default(),
            report_endpoints,
        }),
    };

    match client.submit_load_task_response(load_task).await {
        Ok(response) if response.accepted.unwrap_or(true) => Ok(true),
        Ok(response) => {
            let reason = response.reject_reason.unwrap_or_default();
            warn!(
                "worker rejected transfer task before accepting it: job={} run={} task={} attempt={} worker_id={} worker_session={} source={} target={} reason={}",
                task.job_id,
                task.run_id,
                task.task_id,
                attempt_id,
                worker.worker_id(),
                worker.worker_session_id,
                task.source_path,
                task.target_path,
                reason
            );
            if !store.update_task_state(TransferTaskStateUpdate {
                job_id: task.job_id.clone(),
                run_id: task.run_id,
                owner,
                lease_epoch,
                task_id: task.task_id.clone(),
                from_states: vec![TransferTaskState::Running],
                state: TransferTaskState::Pending,
                message: format!("worker rejected task before accept: {}", reason),
                now_ms: now_ms(),
            })? {
                return Ok(false);
            }
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
                "submit transfer task returned an error after attempt start; keep running and let query/stale recovery decide final state: job={} run={} task={} attempt={} worker_id={} worker_session={} source={} target={} err={}",
                task.job_id,
                task.run_id,
                task.task_id,
                attempt_id,
                worker.worker_id(),
                worker.worker_session_id,
                task.source_path,
                task.target_path,
                err
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

fn transfer_kind_label(kind: curvine_model::TransferKind) -> &'static str {
    match kind {
        curvine_model::TransferKind::Load => "load",
        curvine_model::TransferKind::Export => "export",
    }
}
