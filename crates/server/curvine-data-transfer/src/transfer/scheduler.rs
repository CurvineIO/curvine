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
    is_store_unavailable_error, job_mount_snapshot_async, job_mount_snapshot_with_version_async,
    transfer_failure_message, MountSnapshotError, TransferMountSnapshotUpdate,
    TransferPlannedTasks, TransferPlanner, TransferRequeueUpdate, TransferStore,
    TransferTaskStateUpdate,
};
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::TransferConf;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{
    LoadJobInfo, LoadTaskInfo, TaskAttemptStart, TransferJobRecord, TransferLease,
    TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport,
    TransferTaskReportInfo, TransferTaskState, WorkerInflight, WorkerInfo,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::RpcRuntime;
use dashmap::DashMap;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Scheduler-side per-worker in-flight ledger (design doc §4.1).
///
/// Lazy TTL refresh: consumed only by dispatch decisions, so the store
/// aggregation runs when the TTL expires, not on a timer. Within one refresh
/// window the ledger only grows (dispatch-time atomic increments) — overcount
/// means under-dispatch, which is the conservative direction. Correction
/// happens wholesale on the next refresh; no report-path bookkeeping.
/// Estimated in-flight bytes for a worker aggregation: unknown-size rows
/// (count minus non-zero rows) count at the configured default estimate so
/// they cannot bypass the byte budget (design §3.1 invariant 3).
fn inflight_bytes_with_default(estimated_bytes: i64, unknown_count: u64, default_est: i64) -> i64 {
    estimated_bytes.saturating_add(
        i64::try_from(unknown_count)
            .unwrap_or(i64::MAX)
            .saturating_mul(default_est),
    )
}

struct InflightLedger {
    ttl_ms: u64,
    refreshed_at_ms: AtomicU64,
    entries: DashMap<u32, (u64, i64)>, // worker_id -> (task_count, estimated_bytes)
    // Serializes refresh (clear+rebuild) against concurrent account()/get()
    // so a refresh never wipes a just-accounted dispatch (review Required).
    refresh_lock: std::sync::Mutex<()>,
}

impl InflightLedger {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl_ms: ttl.as_millis() as u64,
            refreshed_at_ms: AtomicU64::new(0),
            entries: DashMap::new(),
            refresh_lock: std::sync::Mutex::new(()),
        }
    }

    /// Returns true when the cached snapshot is usable for decisions.
    fn ensure_fresh(
        &self,
        store: &dyn TransferStore,
        now_ms: u64,
        default_est: i64,
    ) -> FsResult<()> {
        let refreshed_at = self.refreshed_at_ms.load(Ordering::Relaxed);
        if refreshed_at != 0 && now_ms.saturating_sub(refreshed_at) < self.ttl_ms {
            return Ok(());
        }
        // Serialize refresh against concurrent account() so a dispatch that
        // landed between clear() and insert() is not lost (review Required).
        let _guard = self.refresh_lock.lock().unwrap_or_else(|e| e.into_inner());
        // Re-check under the lock: another refresh may have just completed.
        let refreshed_at = self.refreshed_at_ms.load(Ordering::Relaxed);
        if refreshed_at != 0 && now_ms.saturating_sub(refreshed_at) < self.ttl_ms {
            return Ok(());
        }
        let inflight = store.select_inflight_by_worker()?;
        // Remember previously-reported worker ids so workers that now have no
        // in-flight tasks get their gauges reset (review Required); otherwise
        // per-worker gauges report stale non-zero values forever.
        let mut previous: std::collections::HashSet<u32> = Default::default();
        for entry in self.entries.iter() {
            previous.insert(*entry.key());
        }
        self.entries.clear();
        for WorkerInflight {
            worker_id,
            task_count,
            estimated_bytes,
            unknown_bytes_count,
        } in inflight
        {
            // Unknown-size rows count at the configured default so they cannot
            // bypass the byte budget (design §3.1 invariant 3).
            previous.remove(&worker_id);
            self.entries.insert(
                worker_id,
                (
                    task_count,
                    inflight_bytes_with_default(estimated_bytes, unknown_bytes_count, default_est),
                ),
            );
            record_metric(|metrics| {
                metrics.set_worker_inflight(worker_id, task_count, estimated_bytes)
            });
        }
        // Zero out gauges for workers that no longer have in-flight tasks.
        for gone in previous {
            record_metric(|metrics| metrics.set_worker_inflight(gone, 0, 0));
        }
        self.refreshed_at_ms.store(now_ms, Ordering::Relaxed);
        Ok(())
    }

    fn account(&self, worker_id: u32, count: u64, bytes: i64) {
        let mut entry = self.entries.entry(worker_id).or_insert((0u64, 0i64));
        entry.0 += count;
        entry.1 += bytes;
    }

    fn get(&self, worker_id: u32) -> (u64, i64) {
        self.entries
            .get(&worker_id)
            .map(|entry| *entry)
            .unwrap_or((0, 0))
    }
}

const PLANNING_REQUEUE_DELAY_MS: i64 = 1_000;
const MOUNT_SNAPSHOT_MAX_RETRIES: u32 = 3;

#[derive(Debug, PartialEq, Eq)]
enum MountSnapshotFailureAction {
    Requeue { retry_count: u32, delay_ms: i64 },
    Fail,
}

enum StaleTaskProbeOutcome {
    Found(TransferTaskReport),
    Retry {
        task: TransferTaskRecord,
        reason: &'static str,
        error: String,
    },
    Missing {
        task: TransferTaskRecord,
        reason: StaleTaskMissingReason,
    },
}

#[derive(Clone, Copy)]
enum StaleTaskMissingReason {
    WorkerSessionGone,
    TaskAttemptMissing,
}

impl StaleTaskMissingReason {
    fn message(self) -> &'static str {
        match self {
            Self::WorkerSessionGone => "assigned Worker session is no longer live",
            Self::TaskAttemptMissing => "assigned Worker does not have this task attempt",
        }
    }
}

#[derive(Default)]
struct StaleTaskMissingCandidates {
    worker_session_gone: Vec<TransferTaskRecord>,
    task_attempt_missing: Vec<TransferTaskRecord>,
}

fn mount_snapshot_failure_action(attempts: u32) -> MountSnapshotFailureAction {
    if attempts >= MOUNT_SNAPSHOT_MAX_RETRIES {
        return MountSnapshotFailureAction::Fail;
    }
    MountSnapshotFailureAction::Requeue {
        retry_count: attempts.saturating_add(1),
        delay_ms: PLANNING_REQUEUE_DELAY_MS.saturating_mul(2_i64.saturating_pow(attempts)),
    }
}

use crate::transfer::{ClusterMetadataCache, TransferMetrics};

const TRANSFER_CLEANUP_INTERVAL_MS: u64 = 60_000;
const TRANSFER_METRICS_INTERVAL_MS: u64 = 10_000;
const TRANSFER_TASK_COUNTS_INTERVAL_MS: u64 = 60_000;
const SCHEDULER_ACTIVE_INTERVAL: Duration = Duration::from_millis(200);
const SCHEDULER_IDLE_INTERVAL: Duration = Duration::from_secs(1);
const SCHEDULER_MIN_IDLE_INTERVAL: Duration = Duration::from_millis(1);
/// Every N scheduler ticks, prefer acquiring a new Pending job over
/// continuing to service already-held jobs (anti-starvation fairness).
const ACQUIRE_PENDING_EVERY_N_TICKS: u64 = 4;

pub struct TransferScheduler<S> {
    store: Arc<S>,
    planner: TransferPlanner,
    cache: ClusterMetadataCache,
    factory: Arc<UfsFactory>,
    /// CV client used to finalize segmented target files (design §14 T4):
    /// after all segment tasks complete, the scheduler queries each
    /// segmented file's block layout and issues complete_file_by_id.
    fs: CurvineFileSystem,
    owner: String,
    report_endpoints: Vec<String>,
    conf: TransferConf,
    worker_cursor: Arc<AtomicUsize>,
    acquire_tick: Arc<AtomicU64>,
    last_cleanup_ms: Arc<AtomicU64>,
    last_metrics_ms: Arc<AtomicU64>,
    last_task_counts_ms: Arc<AtomicU64>,
    inflight: Arc<InflightLedger>,
}

impl<S> Clone for TransferScheduler<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            planner: self.planner.clone(),
            cache: self.cache.clone(),
            factory: self.factory.clone(),
            fs: self.fs.clone(),
            owner: self.owner.clone(),
            report_endpoints: self.report_endpoints.clone(),
            conf: self.conf.clone(),
            worker_cursor: self.worker_cursor.clone(),
            acquire_tick: self.acquire_tick.clone(),
            last_cleanup_ms: self.last_cleanup_ms.clone(),
            last_metrics_ms: self.last_metrics_ms.clone(),
            last_task_counts_ms: self.last_task_counts_ms.clone(),
            inflight: self.inflight.clone(),
        }
    }
}

impl<S> TransferScheduler<S>
where
    S: TransferStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<S>,
        planner: TransferPlanner,
        cache: ClusterMetadataCache,
        factory: Arc<UfsFactory>,
        fs: CurvineFileSystem,
        owner: String,
        report_endpoints: Vec<String>,
        conf: TransferConf,
    ) -> Self {
        let inflight_ttl =
            Duration::from_millis(TransferConf::DEFAULT_SCHEDULER_INFLIGHT_CACHE_TTL_MS);
        Self {
            store,
            planner,
            cache,
            factory,
            fs,
            owner,
            report_endpoints,
            conf,
            worker_cursor: Arc::new(AtomicUsize::new(0)),
            acquire_tick: Arc::new(AtomicU64::new(0)),
            last_cleanup_ms: Arc::new(AtomicU64::new(0)),
            last_metrics_ms: Arc::new(AtomicU64::new(0)),
            last_task_counts_ms: Arc::new(AtomicU64::new(0)),
            inflight: Arc::new(InflightLedger::new(inflight_ttl)),
        }
    }

    pub fn start(self, rt: Arc<curvine_runtime::runtime::Runtime>, stop: Arc<AtomicBool>) {
        let workers = self.conf.scheduler_workers();
        for index in 0..workers {
            let scheduler = self.clone();
            let stop = stop.clone();
            rt.spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let delay = match scheduler.run_once().await {
                        Ok(true) => SCHEDULER_ACTIVE_INTERVAL,
                        Ok(false) => scheduler.idle_interval(),
                        Err(err) => {
                            warn!("transfer scheduler worker {} tick failed: {}", index, err);
                            SCHEDULER_ACTIVE_INTERVAL
                        }
                    };
                    tokio::time::sleep(delay).await;
                }
            });
        }
    }

    async fn run_once(&self) -> FsResult<bool> {
        let now_ms = now_ms();
        self.cleanup_terminal_transfers(now_ms)?;
        self.record_job_counts_if_due(now_ms)?;
        // Fairness: every N ticks, prefer acquiring a brand-new Pending job
        // over continuing to service this scheduler's already-held jobs.
        // Without this, a scheduler holding many long-running jobs services
        // them forever and a new Pending job starves (observed: a Pending job
        // waited 34 minutes while 19 existing jobs were dispatched).
        let tick = self.acquire_tick.fetch_add(1, Ordering::Relaxed);
        let prefer_pending = tick.is_multiple_of(ACQUIRE_PENDING_EVERY_N_TICKS);
        let Some(lease) = self.store.acquire_runnable_transfer(
            &self.owner,
            duration_ms(self.conf.lease_timeout),
            now_ms,
            self.conf.max_executing_transfers(),
            prefer_pending,
        )?
        else {
            record_metric(|metrics| metrics.inc_acquire("empty"));
            return Ok(false);
        };
        record_metric(|metrics| metrics.inc_acquire("ok"));

        let Some(job) = self.store.get_transfer(&lease.job_id)? else {
            record_metric(|metrics| metrics.inc_acquire("missing_job"));
            return Ok(true);
        };

        if job.cancel_requested {
            self.cancel_transfer(&job, &lease).await?;
            return Ok(true);
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
        Ok(true)
    }

    fn idle_interval(&self) -> Duration {
        scheduler_idle_interval(self.conf.lease_timeout)
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

    fn record_job_counts_if_due(&self, now_ms: i64) -> FsResult<()> {
        let now = now_ms.max(0) as u64;
        let last = self.last_metrics_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < TRANSFER_METRICS_INTERVAL_MS
            || self
                .last_metrics_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return Ok(());
        }
        let (active, executing) = match self.transfer_counts() {
            Ok(counts) => counts,
            Err(err) => {
                self.last_metrics_ms.store(0, Ordering::Relaxed);
                return Err(err);
            }
        };
        self.record_job_counts(active, executing);
        // Global task-state counts are a full-range GROUP BY on transfer_tasks,
        // so throttle them well below the job-count poll (60s). Observability
        // only; failures must not perturb the scheduling path.
        let now = now_ms.max(0) as u64;
        let last_tasks = self.last_task_counts_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last_tasks) >= TRANSFER_TASK_COUNTS_INTERVAL_MS
            && self
                .last_task_counts_ms
                .compare_exchange(last_tasks, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            match self.store.count_tasks_global() {
                Ok(counts) => {
                    if let Ok(metrics) = TransferMetrics::get() {
                        metrics.set_task_counts(&counts);
                    }
                }
                Err(err) => {
                    self.last_task_counts_ms.store(0, Ordering::Relaxed);
                    warn!("count_tasks_global failed: {}", err);
                }
            }
        }
        Ok(())
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

    async fn plan_transfer(
        &self,
        mut job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
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

        if job.mount_snapshot_json.trim().is_empty() || job.mount_snapshot_json == "{}" {
            let snapshot = match job_mount_snapshot_with_version_async(&job, &self.cache).await {
                Ok(snapshot) => Ok(snapshot),
                Err(MountSnapshotError::Retryable(err)) => {
                    match mount_snapshot_failure_action(job.mount_snapshot_attempts) {
                        MountSnapshotFailureAction::Requeue {
                            retry_count,
                            delay_ms,
                        } => {
                            let now = now_ms();
                            let requeued = self.store.requeue_transfer(TransferRequeueUpdate {
                                job_id: job.job_id.clone(),
                                run_id: job.run_id,
                                owner: lease.owner.clone(),
                                lease_epoch: lease.lease_epoch,
                                message: format!("mount snapshot refresh delayed: {}", err),
                                next_attempt_at_ms: now.saturating_add(delay_ms),
                                mount_snapshot_attempts: Some(retry_count),
                                now_ms: now,
                            })?;
                            if requeued {
                                record_metric(|metrics| {
                                    metrics.inc_planning(transfer_kind_label(job.kind), "retry")
                                });
                                info!(
                                    "transfer mount snapshot requeued job_id={} run_id={} kind={:?} tenant={} owner={} lease_epoch={} retry_count={} next_attempt_at_ms={} reason={}",
                                    job.job_id,
                                    job.run_id,
                                    job.kind,
                                    job.tenant,
                                    lease.owner,
                                    lease.lease_epoch,
                                    retry_count,
                                    now.saturating_add(delay_ms),
                                    err
                                );
                            }
                            return Ok(());
                        }
                        MountSnapshotFailureAction::Fail => Err(err),
                    }
                }
                Err(MountSnapshotError::Terminal(err)) => Err(err),
            };
            let (mount, version) = match snapshot {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    record_metric(|metrics| {
                        metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                    });
                    self.fail_transfer(
                        &job,
                        &lease,
                        &[TransferState::Planning],
                        transfer_failure_message(
                            job.kind,
                            "mount snapshot",
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            let snapshot_json = match serde_json::to_string(&mount) {
                Ok(json) => json,
                Err(err) => {
                    let err = FsError::common(format!(
                        "Unable to serialize transfer mount snapshot: {}",
                        err
                    ));
                    record_metric(|metrics| {
                        metrics.inc_planning(transfer_kind_label(job.kind), "failure")
                    });
                    self.fail_transfer(
                        &job,
                        &lease,
                        &[TransferState::Planning],
                        transfer_failure_message(
                            job.kind,
                            "mount snapshot",
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            if !self
                .store
                .set_transfer_mount_snapshot(TransferMountSnapshotUpdate {
                    job_id: job.job_id.clone(),
                    run_id: job.run_id,
                    owner: lease.owner.clone(),
                    lease_epoch: lease.lease_epoch,
                    mount_snapshot_json: snapshot_json.clone(),
                    cluster_snapshot_version: version,
                    now_ms: now_ms(),
                })?
            {
                warn!(
                    "stop planning transfer {} because mount snapshot could not be persisted",
                    job.job_id
                );
                return Ok(());
            }
            job.mount_snapshot_json = snapshot_json;
            job.cluster_snapshot_version = version;
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
                    mount_snapshot_attempts: None,
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
                    transfer_failure_message(
                        job.kind,
                        "planning",
                        &job.source_path,
                        &job.target_path,
                        &err,
                    ),
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
        // Lazy ledger refresh (design §4.1): only on dispatch; a failed
        // aggregation skips dispatch this tick (fail-safe, no blind dispatch).
        if let Err(err) = self.inflight.ensure_fresh(
            self.store.as_ref(),
            now_ms() as u64,
            TransferConf::DEFAULT_TASK_ESTIMATED_BYTES,
        ) {
            record_metric(|metrics| metrics.inc_dispatch("failure", 1));
            warn!(
                "skip dispatching transfer {} because inflight aggregation failed: {}",
                job.job_id, err
            );
            return Ok(());
        }
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
                            "dispatch",
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
        let mount = job_mount_snapshot_async(job, &self.cache).await?;
        let job_info = self.planner_job_info(job, &mount)?;
        let limit = self.conf.worker_dispatch_concurrency();
        // Count tasks that could not be placed this round because no worker had
        // capacity (choose_worker returned None). They stay Pending in store and
        // are retried next round; this counter makes the backpressure visible.
        let mut blocked_no_worker = 0usize;

        let started = stream::iter(tasks.into_iter().filter_map(|task| {
            let estimated_bytes = self.task_estimated_bytes(&task);
            // No worker has room (design §4.2): drop the task from this batch
            // (stays Pending in store) so remaining tasks still get placed.
            let Some(worker) = self.choose_worker(&workers, estimated_bytes) else {
                blocked_no_worker += 1;
                return None;
            };
            let store = self.store.clone();
            let factory = self.factory.clone();
            let cache = self.cache.clone();
            let job_info = job_info.clone();
            let owner = lease.owner.clone();
            let lease_epoch = lease.lease_epoch;
            let report_endpoints = self.report_endpoints.clone();
            let task_stale_timeout_ms = duration_ms(self.conf.task_stale_timeout);
            Some(async move {
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
            })
        }))
        .buffer_unordered(limit)
        .try_collect::<Vec<_>>()
        .await?;
        if blocked_no_worker > 0 {
            record_metric(|metrics| {
                metrics.inc_dispatch("blocked_no_worker", blocked_no_worker);
            });
        }
        Ok(started.into_iter().filter(|value| *value).count())
    }

    fn planner_job_info(
        &self,
        job: &TransferJobRecord,
        mount: &curvine_model::MountInfo,
    ) -> FsResult<curvine_model::LoadJobInfo> {
        self.planner.load_job_info(job, mount)
    }

    async fn check_running_transfer(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let now = now_ms();
        let missing_tasks = self.probe_stale_running_tasks(&job, now).await?;
        for (mut candidates, message) in [
            (
                missing_tasks.worker_session_gone,
                StaleTaskMissingReason::WorkerSessionGone.message(),
            ),
            (
                missing_tasks.task_attempt_missing,
                StaleTaskMissingReason::TaskAttemptMissing.message(),
            ),
        ] {
            for candidate in &mut candidates {
                candidate.progress.message = message.to_string();
            }
            let _ = self.store.mark_stale_attempts(
                &job.job_id,
                job.run_id,
                &lease.owner,
                lease.lease_epoch,
                now,
                candidates,
            )?;
        }
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
                    message: stale_retry_exhausted_message(&task),
                    now_ms: now,
                    bump_retry: false,
                })? {
                    return Ok(());
                }
                self.finish_failed_transfer(&job, &lease).await?;
                record_metric(|metrics| metrics.inc_stale_retry("exhausted"));
                return Ok(());
            }
            // Backoff: do not redeliver a stale task immediately. The task was
            // stale because its worker did not report in time; hammering it
            // every scheduler tick (200ms) turns one bad worker into a
            // redispatch storm that starves other jobs (observed: 121
            // redispatch cycles in 35min blocked a new Pending job for 34min).
            // Delay grows exponentially with retry_count, capped.
            let backoff_ms = stale_retry_backoff_ms(task.retry_count);
            let last_attempt_ms = task.progress.update_time.max(task.updated_at);
            if now.saturating_sub(last_attempt_ms) < backoff_ms {
                record_metric(|metrics| metrics.inc_stale_retry("backoff"));
                continue;
            }
            if !self.store.update_task_state(TransferTaskStateUpdate {
                job_id: task.job_id.clone(),
                run_id: task.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                task_id: task.task_id.clone(),
                from_states: vec![TransferTaskState::Stale],
                state: TransferTaskState::Pending,
                message: format!("retrying after stale attempt: {}", task.progress.message),
                now_ms: now,
                bump_retry: true,
            })? {
                return Ok(());
            }
            record_metric(|metrics| {
                metrics.inc_stale_retry("scheduled");
                metrics.inc_requeue("stale");
            });
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
            // acquire_runnable_transfer already refreshed this lease for the current tick.
            return Ok(());
        }
        // Finalize segmented targets BEFORE marking the job Completed: the file
        // only becomes readable once every segment's blocks are committed via
        // complete_file_by_id. If finalize fails, the data is not fully
        // converged, so the job must not report success (review Critical).
        if let Err(err) = self.finalize_segment_files(&job).await {
            warn!(
                "finalize segment files for transfer {} failed: {}",
                job.job_id, err
            );
            record_metric(|metrics| metrics.inc_terminal("failed", "finalize_segment_failed"));
            self.fail_transfer(
                &job,
                &lease,
                &[TransferState::Running],
                format!("finalize segment files failed: {}", err),
            )?;
            return Ok(());
        }
        let _ = self.transition(
            &job,
            &lease,
            &[TransferState::Running],
            TransferState::Completed,
            "all tasks completed",
        )?;
        record_metric(|metrics| metrics.inc_terminal("completed", "all_tasks_completed"));
        let completed_size = self
            .store
            .get_transfer_task_summary(&job.job_id, job.run_id, now_ms())
            .map(|summary| summary.counts.completed_size)
            .unwrap_or(0);
        record_metric(|metrics| metrics.inc_completed_bytes("completed", completed_size));
        Ok(())
    }

    /// Finalize every CV target file written by segment tasks (design §14 T4):
    /// collect distinct targets whose tasks carry a `{"segment": ...}` plan,
    /// query the master block layout for each, and issue complete_file_by_id
    /// so the file becomes visible with its full block list.
    async fn finalize_segment_files(&self, job: &TransferJobRecord) -> FsResult<()> {
        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        let mut targets: std::collections::BTreeSet<String> = Default::default();
        for task in &tasks {
            if task
                .source_read_plan_json
                .trim_start()
                .starts_with("{\"segment\"")
            {
                targets.insert(task.target_path.clone());
            }
        }
        if targets.is_empty() {
            return Ok(());
        }
        for target in targets {
            let path = Path::from_str(&target)?;
            let blocks = self.fs.get_block_locations(&path).await?;
            let commit_blocks: Vec<_> = blocks
                .block_locs
                .iter()
                .map(curvine_model::CommitBlock::from)
                .collect();
            let block_count = commit_blocks.len();
            let file_len = blocks.status.len;
            self.fs
                .fs_client()
                .complete_file_by_id(
                    &path,
                    blocks.status.id,
                    file_len,
                    commit_blocks,
                    false,
                    None,
                )
                .await?;
            info!(
                "finalized segmented target file {} blocks={} len={}",
                target, block_count, file_len
            );
        }
        Ok(())
    }

    async fn probe_stale_running_tasks(
        &self,
        job: &TransferJobRecord,
        now: i64,
    ) -> FsResult<StaleTaskMissingCandidates> {
        let tasks = self.store.list_stale_running_tasks(
            &job.job_id,
            job.run_id,
            now,
            self.conf.task_probe_concurrency(),
        )?;
        if tasks.is_empty() {
            return Ok(StaleTaskMissingCandidates::default());
        }
        let workers = match self.cache.live_workers() {
            Ok(workers) => Arc::new(workers),
            Err(err) => {
                let mut summary_dirty = false;
                for task in tasks {
                    summary_dirty |= self.defer_stale_task_retry(
                        &task,
                        now,
                        "cluster worker snapshot unavailable",
                        &err,
                    )?;
                }
                if summary_dirty {
                    self.flush_job_summary_best_effort(&job.job_id, job.run_id);
                }
                return Ok(StaleTaskMissingCandidates::default());
            }
        };
        let outcomes = stream::iter(tasks.into_iter().map(|task| {
            let scheduler = self.clone();
            let workers = workers.clone();
            async move { scheduler.probe_stale_task(task, workers, now).await }
        }))
        .buffer_unordered(self.conf.task_probe_concurrency())
        .collect::<Vec<_>>()
        .await;
        let mut summary_dirty = false;
        let mut missing_tasks = StaleTaskMissingCandidates::default();
        for outcome in outcomes {
            let updated = match outcome {
                StaleTaskProbeOutcome::Found(report) => self.store.update_task_report(report),
                StaleTaskProbeOutcome::Retry {
                    task,
                    reason,
                    error,
                } => self.defer_stale_task_retry(&task, now, reason, &error),
                StaleTaskProbeOutcome::Missing { task, reason } => {
                    match reason {
                        StaleTaskMissingReason::WorkerSessionGone => {
                            missing_tasks.worker_session_gone.push(task)
                        }
                        StaleTaskMissingReason::TaskAttemptMissing => {
                            missing_tasks.task_attempt_missing.push(task)
                        }
                    }
                    continue;
                }
            };
            match updated {
                Ok(updated) => summary_dirty |= updated,
                Err(err) => {
                    if summary_dirty {
                        self.flush_job_summary_best_effort(&job.job_id, job.run_id);
                    }
                    return Err(err);
                }
            }
        }
        if summary_dirty {
            self.flush_job_summary_best_effort(&job.job_id, job.run_id);
        }
        Ok(missing_tasks)
    }

    async fn probe_stale_task(
        &self,
        task: TransferTaskRecord,
        workers: Arc<Vec<WorkerInfo>>,
        now: i64,
    ) -> StaleTaskProbeOutcome {
        let Some(worker) = workers
            .iter()
            .find(|worker| {
                worker.worker_id() == task.worker_id
                    && worker.worker_session_id == task.worker_session_id
            })
            .cloned()
        else {
            return StaleTaskProbeOutcome::Missing {
                task,
                reason: StaleTaskMissingReason::WorkerSessionGone,
            };
        };
        let client = match self.factory.get_worker_client(&worker.address).await {
            Ok(client) => client,
            Err(err) => {
                return StaleTaskProbeOutcome::Retry {
                    task,
                    reason: "cannot connect to worker",
                    error: err.to_string(),
                };
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
                return StaleTaskProbeOutcome::Retry {
                    task,
                    reason: "worker did not answer task probe",
                    error: err.to_string(),
                };
            }
        };
        if !response.found {
            return StaleTaskProbeOutcome::Missing {
                task,
                reason: StaleTaskMissingReason::TaskAttemptMissing,
            };
        }
        StaleTaskProbeOutcome::Found(TransferTaskReport {
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
        })
    }

    fn defer_stale_task_retry(
        &self,
        task: &TransferTaskRecord,
        now: i64,
        reason: &str,
        err: &impl std::fmt::Display,
    ) -> FsResult<bool> {
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
        Ok(deferred)
    }

    fn flush_job_summary_best_effort(&self, job_id: &str, run_id: u64) {
        if let Err(err) = self.store.flush_job_progress_summary(job_id, run_id) {
            warn!(
                "flush transfer job progress summary failed after scheduler report: job={} run={} err={}",
                job_id, run_id, err
            );
        }
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
        let summary = self
            .store
            .get_transfer_task_summary(&job.job_id, job.run_id, now_ms())?;
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
                );
                metrics.inc_completed_bytes(
                    if state == TransferState::PartialSuccess {
                        "partial_success"
                    } else {
                        "failed"
                    },
                    summary.counts.completed_size,
                );
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
                    bump_retry: false,
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

    /// Effective estimated size used by scheduler-side gating. Unknown (0)
    /// sizes fall back to the configured default so they cannot bypass the
    /// byte budget (design §3.1).
    fn task_estimated_bytes(&self, task: &TransferTaskRecord) -> i64 {
        let size = if task.estimated_size > 0 {
            task.estimated_size
        } else {
            task.progress.total_size
        };
        if size > 0 {
            size
        } else {
            TransferConf::DEFAULT_TASK_ESTIMATED_BYTES
        }
    }

    /// Least-loaded worker choice under count/byte gates (design §4.2).
    /// Returns None when no worker has room: callers leave the task Pending.
    /// The winning worker's ledger is debited synchronously so concurrent
    /// decisions observe updated accounts instead of stampeding one worker;
    /// exact ties are broken by the polling cursor.
    fn choose_worker(&self, workers: &[WorkerInfo], estimated_bytes: i64) -> Option<WorkerInfo> {
        let admitted = choose_worker_admitted(
            workers,
            estimated_bytes,
            self.conf.worker_max_inflight_tasks,
            TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES,
            |worker_id| self.inflight.get(worker_id),
        );
        if admitted.is_empty() {
            return None;
        }
        // Tie-break among equally loaded workers via the polling cursor.
        let min_bytes = admitted.iter().map(|(bytes, _)| *bytes).min().unwrap_or(0);
        let mut ties: Vec<usize> = admitted
            .into_iter()
            .filter(|(bytes, _)| *bytes == min_bytes)
            .map(|(_, index)| index)
            .collect();
        let pick = if ties.len() == 1 {
            ties.remove(0)
        } else {
            ties[self.worker_cursor.fetch_add(1, Ordering::Relaxed) % ties.len()]
        };
        let worker = workers[pick].clone();
        self.inflight
            .account(worker.worker_id(), 1, estimated_bytes);
        Some(worker)
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
        estimated_size: task.estimated_size.max(task.progress.total_size),
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
                // Capacity rejections (worker busy, temporary) must not count
                // against the retry ceiling: the worker is healthy and will
                // accept the task once load drops. Only session/capability
                // rejections indicate a real problem worth bounding.
                bump_retry: is_session_rejection(&reason),
            })? {
                return Ok(false);
            }
            // Capacity rejections must NOT remove the worker session: the worker
            // is healthy and other tasks on it would be misdiagnosed as
            // WorkerSessionGone by later probes (design §5.2). Only session/
            // capability-class rejections evict the cached session.
            let rejection_class = if is_session_rejection(&reason) {
                "session"
            } else {
                "capacity"
            };
            record_metric(|metrics| {
                metrics.inc_reject(rejection_class);
                metrics.inc_requeue("reject");
            });
            if is_session_rejection(&reason) {
                if let Err(err) = cache.refresh().await {
                    warn!(
                        "refresh cluster metadata after worker rejection failed: {}",
                        err
                    );
                }
                cache.remove_worker_session(worker.worker_id(), &worker.worker_session_id);
            }
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

fn scheduler_idle_interval(lease_timeout: Duration) -> Duration {
    SCHEDULER_IDLE_INTERVAL
        .min(lease_timeout / 2)
        .max(SCHEDULER_MIN_IDLE_INTERVAL)
}

/// Candidates for [`TransferScheduler::choose_worker`]: workers that pass the
/// batch-capability gate, the count gate, and the byte gate (oversize tasks
/// only on idle workers, design §4.2). Pure - the ledger lookup is injected so
/// the gating rules can be unit-tested without a full scheduler.
fn choose_worker_admitted(
    workers: &[WorkerInfo],
    estimated_bytes: i64,
    count_limit: usize,
    byte_limit: i64,
    inflight: impl Fn(u32) -> (u64, i64),
) -> Vec<(i64, usize)> {
    let oversize = byte_limit > 0 && estimated_bytes > byte_limit;
    let mut admitted: Vec<(i64, usize)> = Vec::with_capacity(workers.len());
    for (index, worker) in workers.iter().enumerate() {
        let (count, bytes) = inflight(worker.worker_id());
        if count_limit > 0 && count >= count_limit as u64 {
            continue;
        }
        // Oversize tasks only run on an idle worker to avoid starvation.
        let fits = byte_limit <= 0 || bytes + estimated_bytes <= byte_limit;
        let take = if oversize { bytes == 0 } else { fits };
        if take {
            admitted.push((bytes, index));
        }
    }
    admitted
}

/// Session/capability-class worker rejections invalidate the cached session;
/// capacity-class rejections do not (design §5.2/§5.3). Classification is an
/// explicit prefix match on the worker's reject_reason. Unknown reasons are
/// treated as session-class (conservative: evict) because the only known
/// capacity reason is `worker_at_capacity`; future capacity reasons must add
/// their prefix here.
/// A reject reason that indicates the worker's session is genuinely stale
/// (worker restarted or session rotated) and warrants evicting the cached
/// worker session. Capacity rejections (worker busy, temporary) and stale
/// attempt rejections (an outdated run/attempt of an already-retried task)
/// are NOT session failures: evicting on them would misdiagnose a healthy
/// worker and force its other tasks to be re-probed (review Required).
fn is_session_rejection(reason: &str) -> bool {
    if reason.starts_with(crate::worker::task::WORKER_AT_CAPACITY_REJECT_PREFIX) {
        return false;
    }
    // "Reject stale transfer task ..." is an outdated attempt, not a session
    // failure; "stale worker session" is a real session rotation.
    if reason.starts_with("Reject stale transfer task") {
        return false;
    }
    if reason.contains("stale worker session") {
        return true;
    }
    // Conservative default: an unrecognized reason may indicate a real
    // problem, so treat it as session-class (matches prior behavior).
    true
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

/// Exponential backoff (ms) before redelivering a stale task, based on its
/// retry count. Base 10s, doubling per retry, capped at 120s. This converts a
/// per-tick redispatch storm into a bounded retry cadence.
pub(super) fn stale_retry_backoff_ms(retry_count: u32) -> i64 {
    const BASE_MS: i64 = 10_000;
    const CAP_MS: i64 = 120_000;
    let shift = retry_count.min(4); // 2^4 = 16x -> 160s, capped to 120s
    let backoff = BASE_MS.saturating_mul(1i64 << shift);
    backoff.min(CAP_MS)
}

pub(super) fn stale_retry_exhausted_message(task: &TransferTaskRecord) -> String {
    let reason = match task.progress.message.as_str() {
        "" | "task stale timeout" | "retry stale task attempt" => {            "no worker diagnosis was recorded for this stale attempt; inspect the assigned Worker logs"
        }
        message => message,
    };
    format!(
        "transfer task recovery retries exhausted after {} stale attempt(s): {reason}",
        task.retry_count
    )
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

#[cfg(test)]
mod tests {
    use super::mount_snapshot_failure_action;
    use super::scheduler_idle_interval;
    use super::InflightLedger;
    use super::MountSnapshotFailureAction;
    use super::MOUNT_SNAPSHOT_MAX_RETRIES;
    use curvine_model::WorkerInfo;
    use std::time::Duration;

    #[test]
    fn ledger_increment_only_grows_within_ttl_window() {
        let ledger = InflightLedger::new(Duration::from_secs(15));
        ledger.account(1, 1, 1024);
        ledger.account(1, 1, 1024);
        assert_eq!(ledger.get(1), (2, 2048));
        assert_eq!(ledger.get(2), (0, 0));
    }

    #[test]
    fn choose_worker_enforces_byte_budget_and_oversize_idle_only() {
        use super::choose_worker_admitted;
        use curvine_model::WorkerAddress;
        let w1 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            1,
        );
        let w2 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 2,
                ..Default::default()
            },
            1,
        );
        let workers = [w1, w2];
        // 100-byte budget, 80 already used on w1: only w2 fits a 30-byte task.
        let admitted =
            choose_worker_admitted(
                &workers,
                30,
                0,
                100,
                |id| {
                    if id == 1 {
                        (1, 80)
                    } else {
                        (0, 0)
                    }
                },
            );
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].1, 1); // w2 (index 1) is the only fit
                                      // Oversize task (150 > 100) only admitted when the worker is idle.
        let admitted = choose_worker_admitted(&workers, 150, 0, 100, |_| (0, 0));
        assert_eq!(admitted.len(), 2); // both idle
        let admitted = choose_worker_admitted(&workers, 150, 0, 100, |_| (0, 5));
        assert_eq!(admitted.len(), 0); // neither idle
                                       // Zero byte_limit disables byte gating entirely.
        let admitted = choose_worker_admitted(&workers, 150, 0, 0, |_| (0, 1000));
        assert_eq!(admitted.len(), 2);
    }

    #[test]
    fn choose_worker_enforces_count_gate() {
        use super::choose_worker_admitted;
        let w1 = WorkerInfo::new(Default::default(), 1);
        // count_limit=2, w1 already at 2: excluded.
        let admitted = choose_worker_admitted(std::slice::from_ref(&w1), 10, 2, 0, |_| (2, 0));
        assert!(admitted.is_empty());
        // count_limit=0 disables the count gate.
        let admitted = choose_worker_admitted(&[w1], 10, 0, 0, |_| (2, 0));
        assert_eq!(admitted.len(), 1);
    }

    #[test]
    fn unknown_size_rows_count_at_default_estimate() {
        use super::inflight_bytes_with_default;
        // 2 unknown rows at 256MiB default each add 512MiB to known bytes.
        assert_eq!(
            inflight_bytes_with_default(100, 2, 256 * 1024 * 1024),
            100 + 2 * 256 * 1024 * 1024
        );
        // No unknown rows: no addition.
        assert_eq!(inflight_bytes_with_default(100, 0, 256 * 1024 * 1024), 100);
        // Unknown rows with default 0 (disabled batching estimate) add nothing.
        assert_eq!(inflight_bytes_with_default(100, 2, 0), 100);
        // Saturation: enormous unknown counts do not overflow.
        assert_eq!(
            inflight_bytes_with_default(i64::MAX - 1, 1, 256 * 1024 * 1024),
            i64::MAX
        );
    }

    #[test]
    fn ledger_refresh_replaces_snapshot_wholesale() {
        let ledger = InflightLedger::new(Duration::from_secs(15));
        ledger.account(1, 5, 5000);
        // Simulate a store snapshot where worker 1 has less in flight.
        ledger.entries.clear();
        ledger.entries.insert(1, (1, 1000));
        ledger
            .refreshed_at_ms
            .store(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(ledger.get(1), (1, 1000));
    }

    #[test]
    fn retryable_mount_snapshot_failures_use_bounded_backoff() {
        assert_eq!(
            mount_snapshot_failure_action(0),
            MountSnapshotFailureAction::Requeue {
                retry_count: 1,
                delay_ms: 1_000,
            }
        );
        assert_eq!(
            mount_snapshot_failure_action(1),
            MountSnapshotFailureAction::Requeue {
                retry_count: 2,
                delay_ms: 2_000,
            }
        );
        assert_eq!(
            mount_snapshot_failure_action(MOUNT_SNAPSHOT_MAX_RETRIES),
            MountSnapshotFailureAction::Fail
        );
    }

    #[test]
    fn idle_backoff_renews_short_leases_before_they_expire() {
        assert_eq!(
            scheduler_idle_interval(Duration::from_secs(120)),
            Duration::from_secs(1)
        );
        assert_eq!(
            scheduler_idle_interval(Duration::from_millis(100)),
            Duration::from_millis(50)
        );
    }
}
