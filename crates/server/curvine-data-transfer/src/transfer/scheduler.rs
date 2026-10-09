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
    agreed_plan_source_mtime, classify_transfer_plan, normalize_group_resume, BatchManifest,
    BlockLocation, CommitBlock, LoadJobInfo, LoadTaskInfo, LocatedBlock, SetAttrOptsBuilder,
    TaskAttemptStart, TaskBlockLocation, TransferJobRecord, TransferLease, TransferPlanKind,
    TransferProgress, TransferState, TransferStateUpdate, TransferTaskRecord, TransferTaskReport,
    TransferTaskReportInfo, TransferTaskState, WorkerInflight, WorkerInfo,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::RpcRuntime;
use dashmap::DashMap;
use futures::stream::{self, StreamExt, TryStreamExt};
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::future::Future;
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

fn join_transfer_target(dir: &Path, relative_path: &str) -> FsResult<String> {
    let full = dir.full_path();
    let joined = if full.ends_with(Path::SEPARATOR) {
        format!("{full}{relative_path}")
    } else {
        format!("{full}{}{relative_path}", Path::SEPARATOR)
    };
    Ok(Path::from_str(&joined)?.clone_uri())
}

/// Targets whose segment tasks all completed, paired with the plan-time
/// source mtime their plans agree on (`None` when a plan pins none, or when
/// the plans disagree because the source moved while it was being copied).
///
/// Convergence stamps that mtime as the target's UFS identity. Without it a
/// directory-loaded large file stays `{ufs_mtime: 0, state: Cv}`, which
/// `cv_valid` rejects outright: every read falls back to UFS and every
/// incremental load re-copies the whole file.
pub(crate) fn converged_segment_targets(
    tasks: &[TransferTaskRecord],
) -> Vec<(String, Option<i64>)> {
    use std::collections::HashMap;

    let mut total: HashMap<&str, usize> = HashMap::new();
    let mut completed: HashMap<&str, usize> = HashMap::new();
    let mut plans: HashMap<&str, Vec<&str>> = HashMap::new();
    for task in tasks {
        if !matches!(
            classify_transfer_plan(&task.source_read_plan_json),
            TransferPlanKind::Segment | TransferPlanKind::SegmentGroup
        ) {
            continue;
        }
        let target = task.target_path.as_str();
        *total.entry(target).or_insert(0) += 1;
        plans
            .entry(target)
            .or_default()
            .push(task.source_read_plan_json.as_str());
        if task.state == TransferTaskState::Completed {
            *completed.entry(target).or_insert(0) += 1;
        }
    }

    let mut targets: Vec<(String, Option<i64>)> = total
        .into_iter()
        .filter(|(target, count)| completed.get(target).copied().unwrap_or(0) == *count)
        .map(|(target, _)| {
            let mtime = agreed_plan_source_mtime(plans.remove(target).unwrap_or_default());
            (target.to_string(), mtime)
        })
        .collect();
    targets.sort();
    targets
}

/// Target path → (block id → worker ids) reported by the completed segment
/// tasks of a run. Only completed segment-task reports carry locations.
pub(crate) fn reported_block_locations(
    tasks: &[TransferTaskRecord],
) -> HashMap<String, HashMap<i64, Vec<u32>>> {
    let mut reported: HashMap<String, HashMap<i64, Vec<u32>>> = HashMap::new();
    for task in tasks {
        if task.state != TransferTaskState::Completed || task.block_locations.is_empty() {
            continue;
        }
        if !matches!(
            classify_transfer_plan(&task.source_read_plan_json),
            TransferPlanKind::Segment | TransferPlanKind::SegmentGroup
        ) {
            continue;
        }
        let entry = reported.entry(task.target_path.clone()).or_default();
        for location in &task.block_locations {
            entry.insert(location.block_id, location.worker_ids.clone());
        }
    }
    reported
}

/// Build the commit entry for one layout block. Reported worker ids win: the
/// master layout carries no locations for blocks no client has committed yet.
pub(crate) fn commit_block_for(
    located: &LocatedBlock,
    reported: Option<&HashMap<i64, Vec<u32>>>,
) -> CommitBlock {
    let mut commit = CommitBlock::from(located);
    if commit.locations.is_empty() {
        if let Some(worker_ids) = reported.and_then(|map| map.get(&located.block.id)) {
            commit.locations = worker_ids
                .iter()
                .map(|worker_id| BlockLocation::new(*worker_id, located.block.storage_type))
                .collect();
        }
    }
    commit
}

fn planned_target_replacements(
    tasks: &[TransferTaskRecord],
) -> FsResult<(HashSet<String>, HashSet<String>)> {
    let mut segment_targets = HashSet::new();
    let mut replacement_targets = HashSet::new();
    for task in tasks {
        match classify_transfer_plan(&task.source_read_plan_json) {
            TransferPlanKind::Segment | TransferPlanKind::SegmentGroup => {
                segment_targets.insert(task.target_path.clone());
                if task.replace_target {
                    replacement_targets.insert(task.target_path.clone());
                }
            }
            TransferPlanKind::Batch => {
                let manifest: BatchManifest = serde_json::from_str(&task.source_read_plan_json)
                    .map_err(|err| FsError::common(format!("invalid batch manifest: {err}")))?;
                let target_dir = Path::from_str(&task.target_path)?;
                for entry in manifest.batch {
                    if entry.replace_target {
                        replacement_targets
                            .insert(join_transfer_target(&target_dir, &entry.rel_path)?);
                    }
                }
            }
            TransferPlanKind::Plain if task.replace_target => {
                replacement_targets.insert(task.target_path.clone());
            }
            TransferPlanKind::Plain => {}
        }
    }
    Ok((segment_targets, replacement_targets))
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
        entry.0 = entry.0.saturating_add(count);
        entry.1 = entry.1.saturating_add(bytes);
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
/// Max automatic retries for a retryable planning failure (rate limiting,
/// timeouts). Budget: ~1s + 2s + 4s + 8s + jitter, then terminal failure.
const PLANNING_LIST_MAX_RETRIES: u32 = 4;

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

/// Retryable planning failures get bounded exponential backoff instead of an
/// immediate terminal failure (e.g. UFS rate limiting during list/stat).
/// Returns the next attempt count, or None when the budget is exhausted.
fn planning_retry_attempt(attempts: u32) -> Option<u32> {
    if attempts >= PLANNING_LIST_MAX_RETRIES {
        return None;
    }
    Some(attempts.saturating_add(1))
}

/// Backoff for a planning retry: base * 2^n plus jitter. Jitter spreads jobs
/// that hit the same rate limiter so they do not retry in lockstep and
/// amplify the limit.
fn planning_retry_delay_ms(attempts: u32, job_key: &str, now_ms: i64) -> i64 {
    let base = PLANNING_REQUEUE_DELAY_MS.saturating_mul(2_i64.saturating_pow(attempts.min(10)));
    // Per-job entropy (hash) so a 429 burst hitting several planning workers
    // does not retry in lockstep; wall-clock alone maps nearby failures to
    // nearby retries.
    let mut hash: u64 = 1469598103934665603;
    for b in job_key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    let jitter = (hash % (PLANNING_REQUEUE_DELAY_MS.max(1) as u64)) as i64;
    base.saturating_add(jitter)
        .saturating_add(now_ms.abs() % 64)
}

/// Whether a planning failure is worth an automatic retry. Structural errors
/// (missing source, oversized directory, bad args) fail fast; only
/// throttling/timeout/overload signals back off. Message matching is
/// intentionally narrow: misclassifying permanent as retryable only delays
/// the failure within a small bounded budget, while the reverse drops jobs
/// that would succeed minutes later.
fn is_planning_retryable(err: &FsError) -> bool {
    use curvine_error::ErrorKind;
    if matches!(
        err.kind(),
        ErrorKind::Timeout | ErrorKind::TransferOverloaded | ErrorKind::TransferStoreUnavailable
    ) {
        return true;
    }
    // UFS adapters surface OpenDAL throttling as a generic error; match the
    // stable upstream markers.
    let msg = err.to_string();
    msg.contains("RateLimited")
        || msg.contains("TooManyRequests")
        || msg.contains("too many requests")
}

use crate::transfer::{
    ClusterMetadataCache, TransferMetrics, MAX_JOBS_PER_STORE_TRANSACTION,
    MAX_TASKS_PER_STORE_TRANSACTION,
};

const TRANSFER_CLEANUP_INTERVAL_MS: u64 = 60_000;
const TRANSFER_METRICS_INTERVAL_MS: u64 = 10_000;
const TRANSFER_TASK_COUNTS_INTERVAL_MS: u64 = 3_600_000;
const TRANSFER_CLEANUP_LEASE_MS: i64 = 120_000;
/// Max terminal task rows deleted per cleanup transaction (§8 C2).
/// A huge job spans multiple rounds instead of one giant transaction.
const TERMINAL_TASK_PURGE_ROW_LIMIT: usize = MAX_TASKS_PER_STORE_TRANSACTION;
const TERMINAL_JOB_PURGE_LIMIT: usize = MAX_JOBS_PER_STORE_TRANSACTION;
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
    worker_task_limit: usize,
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
            worker_task_limit: self.worker_task_limit,
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
        worker_task_limit: usize,
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
            worker_task_limit: worker_task_limit.max(1),
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
        // Global task-state counts are a full-range GROUP BY on transfer_tasks
        // (hourly calibration). Between calibrations the gauges track
        // planning/assign/terminal deltas incrementally; cancel/purge drift
        // is corrected by the next calibration. Observability only; failures
        // must not perturb the scheduling path.
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

    fn cleanup_terminal_transfers(&self, tick_now_ms: i64) -> FsResult<()> {
        let now = tick_now_ms.max(0) as u64;
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
        let task_retention_ms =
            i64::try_from(self.conf.task_retention.as_millis()).unwrap_or(i64::MAX);
        let task_older_than = tick_now_ms.saturating_sub(task_retention_ms);
        let job_older_than = tick_now_ms.saturating_sub(retention_ms);
        // Renew immediately before each bounded delete phase. The Store also
        // locks and validates this lease in the delete transaction; renewal
        // keeps a slow first phase from making the second phase run on an
        // already-expired lease.
        let task_cleanup_now_ms = now_ms();
        if !self.store.try_acquire_cleanup_lease(
            &self.owner,
            task_cleanup_now_ms,
            TRANSFER_CLEANUP_LEASE_MS,
        )? {
            return Ok(());
        }
        let purged_tasks = self.store.purge_terminal_tasks_with_cleanup_lease(
            task_older_than,
            TERMINAL_TASK_PURGE_ROW_LIMIT,
            &self.owner,
            task_cleanup_now_ms,
        )?;
        let job_cleanup_now_ms = now_ms();
        let purged_jobs = if self.store.try_acquire_cleanup_lease(
            &self.owner,
            job_cleanup_now_ms,
            TRANSFER_CLEANUP_LEASE_MS,
        )? {
            self.store.purge_terminal_jobs_with_cleanup_lease(
                job_older_than,
                TERMINAL_JOB_PURGE_LIMIT,
                &self.owner,
                job_cleanup_now_ms,
            )?
        } else {
            0
        };
        if purged_tasks >= TERMINAL_TASK_PURGE_ROW_LIMIT || purged_jobs >= TERMINAL_JOB_PURGE_LIMIT
        {
            self.last_cleanup_ms.store(0, Ordering::Relaxed);
        }
        if purged_jobs > 0 {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.inc_cleanup_purged(purged_jobs);
                metrics.inc_cleanup_purged_tasks(purged_tasks);
            }
        } else if purged_tasks > 0 {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.inc_cleanup_purged_tasks(purged_tasks);
            }
        }
        if purged_tasks > 0 || purged_jobs > 0 {
            info!(
                "purged {} terminal transfer tasks (older than {}) and {} jobs (older than {})",
                purged_tasks, task_older_than, purged_jobs, job_older_than
            );
        }
        Ok(())
    }

    async fn plan_transfer(
        &self,
        mut job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let resuming_planning = job.state == TransferState::Planning;
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

        // Observability only: how long the job waited in Pending before its
        // first planning pass. A resumed Planning attempt is not queue time, so
        // only the Pending -> Planning edge is observed. This is the term that
        // explains "why is my transfer taking so long" when the executing-slot
        // cap or the worker byte budget is saturated.
        if !resuming_planning {
            let wait_ms = now_ms().saturating_sub(job.created_at);
            let kind = transfer_kind_label(job.kind);
            record_metric(|metrics| metrics.observe_job_pending_wait(kind, wait_ms));
        }

        if resuming_planning && self.store.has_transfer_tasks(&job.job_id, job.run_id)? {
            let err = FsError::common(
                "interrupted task-plan persistence cannot be safely recomputed; retry transfer",
            );
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
                                planning_attempts: None,
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
                            "planning",
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
                            "planning",
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

        let Some(planning_result) = self
            .await_with_planning_lease(&lease, self.planner.plan(&job))
            .await?
        else {
            return Ok(());
        };
        let planned = match planning_result {
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
                    planning_attempts: None,
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
            Err(err) if is_planning_retryable(&err) => {
                match planning_retry_attempt(job.planning_attempts) {
                    Some(retry_count) => {
                        let now = now_ms();
                        let delay_ms =
                            planning_retry_delay_ms(job.planning_attempts, &job.job_id, now);
                        record_metric(|metrics| {
                            metrics.inc_planning(transfer_kind_label(job.kind), "retry")
                        });
                        let requeued = self.store.requeue_transfer(TransferRequeueUpdate {
                            job_id: job.job_id.clone(),
                            run_id: job.run_id,
                            owner: lease.owner.clone(),
                            lease_epoch: lease.lease_epoch,
                            message: format!(
                                "planning throttled, retry {}/{}: {}",
                                retry_count, PLANNING_LIST_MAX_RETRIES, err
                            ),
                            next_attempt_at_ms: now.saturating_add(delay_ms),
                            mount_snapshot_attempts: None,
                            planning_attempts: Some(retry_count),
                            now_ms: now,
                        })?;
                        if requeued {
                            info!(
                                "transfer planning throttled job_id={} run_id={} kind={:?} tenant={} owner={} lease_epoch={} retry_count={} next_attempt_at_ms={} reason={}",
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
                    None => {
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
                }
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
        let message = format!(
            "planned total_size={total_size} skipped_files={skipped_files} skipped_size={skipped_size}"
        );
        let batch_size = self
            .conf
            .planning_batch_size()
            .clamp(1, MAX_TASKS_PER_STORE_TRANSACTION);
        for tasks in planned.tasks.chunks(batch_size) {
            let appended = self.store.append_planned_tasks(TransferPlannedTasks {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                tasks: tasks.to_vec(),
                message: String::new(),
                now_ms: now_ms(),
            })?;
            if !appended {
                warn!(
                    "stop planning transfer {} because owner {} lease epoch {} is stale",
                    job.job_id, lease.owner, lease.lease_epoch
                );
                return Ok(());
            }
            if !self.renew_planning_lease(&lease)? {
                return Ok(());
            }
        }
        let Some(preparation_result) = self
            .await_with_planning_lease(
                &lease,
                self.prepare_transfer_targets(&planned.tasks, job_info.overwrite.unwrap_or(false)),
            )
            .await?
        else {
            return Ok(());
        };
        if let Err(err) = preparation_result {
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
        if !self.renew_planning_lease(&lease)? {
            return Ok(());
        }
        let persisted = self.store.persist_planned_tasks(TransferPlannedTasks {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            owner: lease.owner.clone(),
            lease_epoch: lease.lease_epoch,
            tasks: Vec::new(),
            message,
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
            metrics.task_planned(task_count);
        });

        self.dispatch_pending_tasks(job, lease).await
    }

    async fn await_with_planning_lease<T, F>(
        &self,
        lease: &TransferLease,
        work: F,
    ) -> FsResult<Option<T>>
    where
        F: Future<Output = T>,
    {
        await_with_lease_renewal(work, lease_renew_interval(self.conf.lease_timeout), || {
            self.renew_planning_lease(lease)
        })
        .await
    }

    fn renew_planning_lease(&self, lease: &TransferLease) -> FsResult<bool> {
        let renewed = self.store.renew_lease(
            &lease.job_id,
            lease.run_id,
            &lease.owner,
            lease.lease_epoch,
            duration_ms(self.conf.lease_timeout),
            now_ms(),
        )?;
        record_metric(|metrics| metrics.inc_lease_renew(if renewed { "success" } else { "stale" }));
        if !renewed {
            warn!(
                "stop planning transfer {} because owner {} lease epoch {} is stale",
                lease.job_id, lease.owner, lease.lease_epoch
            );
        }
        Ok(renewed)
    }

    /// Remove targets that need replacement only after all plan rows are
    /// durable while the job remains Planning. This covers shared segment
    /// targets and incremental-load files with mismatched replicas.
    async fn prepare_transfer_targets(
        &self,
        tasks: &[TransferTaskRecord],
        overwrite: bool,
    ) -> FsResult<()> {
        let (segment_targets, replacement_targets) = planned_target_replacements(tasks)?;

        for target_path in &segment_targets {
            let target = Path::from_str(target_path)?;
            if !target.is_cv() {
                continue;
            }
            match self.fs.get_status(&target).await {
                Ok(status) if status.is_dir => {
                    return Err(FsError::common(format!(
                        "Transfer target {} is a directory; refusing to overwrite directory with file",
                        target.full_path()
                    )));
                }
                Ok(_) if !overwrite && !replacement_targets.contains(target_path) => {
                    return Err(FsError::file_exists(target.full_path()));
                }
                Ok(_) => {
                    self.fs.delete(&target, false).await?;
                }
                Err(FsError::FileNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        for target_path in replacement_targets {
            if segment_targets.contains(&target_path) {
                continue;
            }
            let target = Path::from_str(&target_path)?;
            if !target.is_cv() {
                continue;
            }
            match self.fs.get_status(&target).await {
                Ok(status) if status.is_dir => {
                    return Err(FsError::common(format!(
                        "Transfer target {} is a directory; refusing to replace directory with file",
                        target.full_path()
                    )));
                }
                Ok(_) => {
                    self.fs.delete(&target, false).await?;
                }
                Err(FsError::FileNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    async fn dispatch_pending_tasks(
        &self,
        job: TransferJobRecord,
        lease: TransferLease,
    ) -> FsResult<()> {
        let mut dispatched = 0usize;
        let mut blocked_total = 0usize;
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

            let (started, blocked) = match self.dispatch_batch(&job, &lease, tasks).await {
                Ok((started, blocked)) => (started, blocked),
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
                            "planning",
                            &job.source_path,
                            &job.target_path,
                            &err,
                        ),
                    )?;
                    return Err(err);
                }
            };
            dispatched += started;
            blocked_total += blocked;
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
        // "dispatched 0 tasks" explains nothing when tasks were blocked by
        // worker capacity; surface the real reason instead (and flush it so
        // the persisted message matches even before this transition).
        let message = dispatch_result_message(dispatched, blocked_total);
        if blocked_total > 0 {
            record_metric(|metrics| {
                metrics.inc_dispatch("blocked_no_worker", blocked_total);
            });
            let _ = self
                .store
                .flush_job_progress_message(&job.job_id, job.run_id, &message);
        }
        let _ = self.transition(
            &job,
            &lease,
            &[TransferState::Dispatching],
            TransferState::Running,
            message,
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
    ) -> FsResult<(usize, usize)> {
        let workers = match self.cache.live_workers() {
            Ok(workers) if !workers.is_empty() => workers,
            Ok(_) | Err(_) => {
                // No candidate worker at all: the remaining tasks stay
                // Pending and the caller must see why.
                return Ok((0, tasks.len()));
            }
        };
        let mount = job_mount_snapshot_async(job, &self.cache).await?;
        let job_info = self.planner_job_info(job, &mount).await?;
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
        let started_count = started.into_iter().filter(|value| *value).count();
        Ok((started_count, blocked_no_worker))
    }

    async fn planner_job_info(
        &self,
        job: &TransferJobRecord,
        mount: &curvine_model::MountInfo,
    ) -> FsResult<curvine_model::LoadJobInfo> {
        self.planner.load_job_info(job, mount).await
    }

    /// Redeliver transiently failed tasks instead of failing the job.
    /// Returns true while any retryable failed task remains (requeued this
    /// tick or waiting inside its backoff window); the job must not fail
    /// during that window. Budget and backoff reuse the stale-retry
    /// machinery so recovery cadence stays bounded.
    async fn recover_transient_failed_tasks(
        &self,
        job: &TransferJobRecord,
        lease: &TransferLease,
        now: i64,
    ) -> FsResult<bool> {
        let failed = self.store.list_tasks_by_state(
            &job.job_id,
            job.run_id,
            TransferTaskState::Failed,
            self.conf.task_probe_concurrency(),
        )?;
        let mut retryable_remaining = false;
        for task in failed {
            if !is_task_retryable(&task) {
                continue;
            }
            if task.retry_count as usize >= self.conf.task_max_retries {
                continue;
            }
            // Same backoff cadence as stale redelivery: do not hammer a
            // throttled/limited substrate every tick. A task inside its
            // backoff window still counts as retryable-remaining: the job
            // must NOT fail while a transient failure is merely waiting to
            // be redelivered.
            let last_attempt_ms = task.progress.update_time.max(task.updated_at);
            if now.saturating_sub(last_attempt_ms) < stale_retry_backoff_ms(task.retry_count) {
                retryable_remaining = true;
                continue;
            }
            if !self.store.update_task_state(TransferTaskStateUpdate {
                job_id: task.job_id.clone(),
                run_id: task.run_id,
                owner: lease.owner.clone(),
                lease_epoch: lease.lease_epoch,
                task_id: task.task_id.clone(),
                from_states: vec![TransferTaskState::Failed],
                state: TransferTaskState::Pending,
                message: format!("auto-retry transient failure: {}", task.progress.message),
                now_ms: now,
                bump_retry: true,
            })? {
                return Ok(retryable_remaining);
            }
            retryable_remaining = true;
            record_metric(|metrics| {
                metrics.inc_stale_retry("transient_retry");
                metrics.inc_requeue("transient_retry");
            });
        }
        Ok(retryable_remaining)
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
            for candidates in candidates.chunks(MAX_TASKS_PER_STORE_TRANSACTION) {
                let staled = self.store.mark_stale_attempts(
                    &job.job_id,
                    job.run_id,
                    &lease.owner,
                    lease.lease_epoch,
                    now,
                    candidates.to_vec(),
                )?;
                record_metric(|metrics| {
                    for _ in &staled {
                        metrics.task_staled();
                    }
                });
            }
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
                record_metric(|metrics| {
                    metrics.inc_stale_retry("exhausted");
                    metrics.task_stale_exhausted();
                });
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
                metrics.task_stale_requeued();
            });
        }

        // Transient task failures (throttling, timeouts, capacity, session
        // loss) are redelivered automatically within the retry budget instead
        // of failing the whole job; permanent failures fall through to
        // finish_failed_transfer below.
        if self
            .recover_transient_failed_tasks(&job, &lease, now)
            .await?
        {
            return Ok(());
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
        // A target is only finalized when EVERY segment task targeting it has
        // completed. Block metadata cannot distinguish a pre-allocated block
        // (resize records the requested length before any payload lands)
        // from a written one, so file length heuristics would cements holes;
        // the per-task convergence fact is the reliable signal.
        let targets = converged_segment_targets(&tasks);
        if targets.is_empty() {
            return Ok(());
        }
        // The master keeps no locations for pre-allocated segment blocks until
        // a commit carries them, and workers never report writes back, so the
        // layout query alone would complete files with empty locations — shells
        // every read must fall back to UFS from. The completing tasks report
        // the workers they actually wrote to; commit those.
        let mut reported = reported_block_locations(&tasks);
        let target_count = targets.len();
        let mut unstamped = 0usize;
        for (target, source_mtime) in targets {
            let path = Path::from_str(&target)?;
            let blocks = self.fs.get_block_locations(&path).await?;
            if blocks.status.is_complete() {
                continue;
            }
            if blocks.block_locs.is_empty() {
                warn!(
                    "skip finalizing segmented target file {} with empty layout",
                    target,
                );
                continue;
            }
            let located = reported.remove(&target);
            let commit_blocks: Vec<_> = blocks
                .block_locs
                .iter()
                .map(|block| commit_block_for(block, located.as_ref()))
                .collect();
            let located_blocks = commit_blocks
                .iter()
                .filter(|block| !block.locations.is_empty())
                .count();
            if located_blocks < commit_blocks.len() {
                warn!(
                    "finalizing segmented target file {} with {} of {} blocks unlocated: no task reported workers for them (older worker? retry?), reads of those blocks fall back to UFS",
                    target,
                    commit_blocks.len() - located_blocks,
                    commit_blocks.len(),
                );
            }
            let block_count = commit_blocks.len();
            let file_len = blocks.status.len;
            // Stamp the plan-time UFS identity together with the completion.
            // No agreed mtime (missing, or the segments disagree because the
            // source moved mid-copy) leaves the target cache-invalid on
            // purpose: readers then fall back to UFS instead of trusting bytes
            // nobody can identify.
            let set_attr_opts = source_mtime
                .map(|ufs_mtime| SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime).build());
            let stamped = set_attr_opts.is_some();
            if !stamped {
                unstamped += 1;
            }
            self.fs
                .fs_client()
                .complete_file_by_id(
                    &path,
                    blocks.status.id,
                    file_len,
                    commit_blocks,
                    false,
                    set_attr_opts,
                )
                .await?;
            info!(
                "finalized segmented target file {} blocks={} located_blocks={} len={} ufs_mtime_stamped={}",
                target, block_count, located_blocks, file_len, stamped
            );
        }
        if unstamped > 0 {
            warn!(
                "finalized {} of {} segmented target(s) for transfer {} without a UFS identity: no agreed plan-time source mtime, so reads keep falling back to UFS until a later load stamps it",
                unstamped, target_count, job.job_id
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
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            // The probe recovers tasks whose terminal report was lost; without
            // these workers the attempt's blocks would finalize as shells.
            block_locations: response
                .block_locations
                .iter()
                .map(|loc| TaskBlockLocation {
                    block_id: loc.block_id,
                    worker_ids: loc.worker_ids.clone(),
                })
                .collect(),
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
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            block_locations: Vec::new(),
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
        // Complete converged segment files before landing the terminal
        // state, so a retry skips them instead of retransmitting everything.
        // Best-effort: finalize trouble must not block failure handling.
        if let Err(err) = self.finalize_segment_files(job).await {
            warn!(
                "finalize segment files for failed transfer {} failed: {}",
                job.job_id, err
            );
        }
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
            self.worker_task_limit,
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
        if to_state.is_terminal() {
            self.snapshot_terminal(job)?;
        }
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

    fn snapshot_terminal(&self, job: &TransferJobRecord) -> FsResult<()> {
        let summary = self
            .store
            .get_transfer_task_summary(&job.job_id, job.run_id, now_ms())?;
        self.store.update_terminal_snapshot(
            &job.job_id,
            job.run_id,
            summary.progress.clone(),
            summary.counts,
            now_ms(),
        )
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
    let resume_loaded_size = match classify_transfer_plan(&task.source_read_plan_json) {
        TransferPlanKind::SegmentGroup => {
            normalize_group_resume(&task.source_read_plan_json, task.progress.loaded_size)
        }
        _ => 0,
    };
    if resume_loaded_size > 0 {
        record_metric(|metrics| metrics.inc_group_resume(resume_loaded_size));
    }
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
        resume_loaded_size,
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
            resume_loaded_size,
            // Same instant `start_task_attempt` persisted above; the worker
            // echoes it so the report path never re-reads the task row.
            attempt_started_at_ms: now,
        }),
    };

    match client.submit_load_task_response(load_task).await {
        Ok(response) if response.accepted.unwrap_or(true) => {
            record_metric(|metrics| metrics.task_assigned());
            Ok(true)
        }
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

pub(crate) fn lease_renew_interval(lease_timeout: Duration) -> Duration {
    (lease_timeout / 2).max(SCHEDULER_MIN_IDLE_INTERVAL)
}

/// Poll work while extending its fenced lease; `None` means ownership changed.
pub(crate) async fn await_with_lease_renewal<T, F, R>(
    work: F,
    renew_interval: Duration,
    mut renew: R,
) -> FsResult<Option<T>>
where
    F: Future<Output = T>,
    R: FnMut() -> FsResult<bool>,
{
    let mut work = std::pin::pin!(work);
    let renewal = tokio::time::sleep(renew_interval);
    tokio::pin!(renewal);

    loop {
        tokio::select! {
            result = &mut work => return Ok(Some(result)),
            _ = &mut renewal => {
                if !renew()? {
                    return Ok(None);
                }
                renewal.as_mut().reset(tokio::time::Instant::now() + renew_interval);
            }
        }
    }
}

/// Message for a dispatch round: capacity backpressure must be visible in the
/// job status Message column, so blocked reasons win over a bare count.
/// Message for a dispatch round: capacity backpressure must be visible in the
/// job status Message column, so blocked reasons win over a bare count.
pub(crate) fn dispatch_result_message(dispatched: usize, blocked: usize) -> String {
    if blocked > 0 {
        format!(
            "waiting for worker capacity: {} task(s) blocked this round (worker in-flight byte budget or concurrency limit reached)",
            blocked
        )
    } else {
        format!("dispatched {} tasks", dispatched)
    }
}

/// Candidates for [`TransferScheduler::choose_worker`]: workers that pass the
/// batch-capability gate, their heartbeat-reported count/byte gates (oversize
/// tasks only on idle workers, design §4.2). Pure - the ledger lookup is
/// injected so the gating rules can be unit-tested without a full scheduler.
fn choose_worker_admitted(
    workers: &[WorkerInfo],
    estimated_bytes: i64,
    default_count_limit: usize,
    default_byte_limit: i64,
    inflight: impl Fn(u32) -> (u64, i64),
) -> Vec<(i64, usize)> {
    let mut admitted: Vec<(i64, usize)> = Vec::with_capacity(workers.len());
    for (index, worker) in workers.iter().enumerate() {
        let (count, bytes) = inflight(worker.worker_id());
        let count_limit = worker.effective_worker_max_concurrent_tasks(default_count_limit);
        if count_limit > 0 && count >= count_limit as u64 {
            continue;
        }
        let byte_limit = worker.effective_transfer_inflight_budget_bytes(default_byte_limit);
        let oversize = byte_limit > 0 && estimated_bytes > byte_limit;
        // Oversize tasks only run on an idle worker to avoid starvation.
        let fits = byte_limit <= 0 || bytes.saturating_add(estimated_bytes) <= byte_limit;
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
/// Whether a terminally failed task is worth an automatic redelivery instead
/// of failing the whole job. Allowlist is intentionally narrow: anything
/// unrecognized stays permanent (fail fast, current behavior). Throttling,
/// timeouts, capacity pressure, session loss, report races and an
/// unready segment layout are transient — the retry is idempotent
/// (positional segment writes, batch commit markers, temp+rename singles)
/// and bounded by task_max_retries with stale-style backoff.
fn is_task_retryable(task: &TransferTaskRecord) -> bool {
    // Terminal failure messages are strings (no structured kind survives
    // the report path), so match narrow transient markers. Allowlist is
    // intentionally narrow (see doc above).
    let msg = task.progress.message.as_str();
    msg.contains("RateLimited")
        || msg.contains("TooManyRequests")
        || msg.contains("too many requests")
        || msg.contains("Timed out")
        || msg.contains("timed out")
        // Worker task timeout copy: "Task {} exceed timeout {} ms".
        || msg.contains("exceed timeout")
        || msg.contains(crate::worker::task::WORKER_AT_CAPACITY_REJECT_PREFIX)
        || msg.contains("not found in target layout")
        || msg.contains("Transfer task report timed out")
        || msg.contains("WorkerSessionGone")
        || msg.contains("stale worker session")
}

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
    use super::is_planning_retryable;
    use super::is_task_retryable;
    use super::mount_snapshot_failure_action;
    use super::planning_retry_attempt;
    use super::planning_retry_delay_ms;
    use super::scheduler_idle_interval;
    use super::InflightLedger;
    use super::MountSnapshotFailureAction;
    use super::MOUNT_SNAPSHOT_MAX_RETRIES;
    use super::PLANNING_LIST_MAX_RETRIES;
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
    fn delayed_target_replacement_preserves_plan_type_boundaries() {
        use super::planned_target_replacements;
        use curvine_model::{
            BatchManifest, BatchManifestEntry, TransferProgress, TransferTaskRecord,
            TransferTaskState,
        };

        let task = |task_id: &str, plan: String, target_path: &str, replace_target: bool| {
            TransferTaskRecord {
                job_id: "job".to_string(),
                run_id: 1,
                task_id: task_id.to_string(),
                attempt_id: 0,
                source_path: "file:///source".to_string(),
                target_path: target_path.to_string(),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: plan,
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
                progress: TransferProgress::default(),
                estimated_size: 0,
                retry_count: 0,
                attempt_started_at: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: 0,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
                replace_target,
            }
        };
        let batch = serde_json::to_string(&BatchManifest {
            batch: vec![
                BatchManifestEntry {
                    rel_path: "keep".to_string(),
                    size: 1,
                    replace_target: false,
                },
                BatchManifestEntry {
                    rel_path: "nested/replace".to_string(),
                    size: 1,
                    replace_target: true,
                },
            ],
        })
        .unwrap();

        let (segments, replacements) = planned_target_replacements(&[
            task("plain", "{}".to_string(), "cv:///plain", true),
            task(
                "segment",
                "{\"segment\":{\"offset\":0}}".to_string(),
                "cv:///segment",
                false,
            ),
            task("batch", batch, "cv:///batch", false),
        ])
        .unwrap();

        assert_eq!(
            segments.into_iter().collect::<Vec<_>>(),
            vec!["cv:///segment"]
        );
        assert!(replacements.contains("cv:///plain"));
        assert!(replacements.contains("cv:///batch/nested/replace"));
        assert!(!replacements.contains("cv:///batch/keep"));
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
        // Legacy workers use the supplied 100-byte compatibility default. With
        // 80 already used on w1, only w2 fits a 30-byte task.
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

        // A worker that reported 200 bytes may receive a 150-byte task while
        // a non-idle worker that kept the legacy 100-byte default cannot.
        let mut w1 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 1,
                ..Default::default()
            },
            1,
        );
        let mut w2 = WorkerInfo::new(
            WorkerAddress {
                worker_id: 2,
                ..Default::default()
            },
            1,
        );
        w1.transfer_inflight_budget_bytes = 100;
        w2.transfer_inflight_budget_bytes = 200;
        let admitted = choose_worker_admitted(&[w1, w2], 150, 0, 100, |id| {
            if id == 1 {
                (1, 1)
            } else {
                (0, 0)
            }
        });
        assert_eq!(admitted, vec![(0, 1)]);
    }

    #[test]
    fn choose_worker_enforces_count_gate() {
        use super::choose_worker_admitted;
        let w1 = WorkerInfo::new(Default::default(), 1);
        // Default count_limit=2, w1 already at 2: excluded.
        let admitted = choose_worker_admitted(std::slice::from_ref(&w1), 10, 2, 0, |_| (2, 0));
        assert!(admitted.is_empty());
        // count_limit=0 disables the count gate.
        let admitted = choose_worker_admitted(&[w1], 10, 0, 0, |_| (2, 0));
        assert_eq!(admitted.len(), 1);
        // A worker that reported its own ceiling overrides the default: at 4
        // in-flight it still fits under its reported limit of 5.
        let mut w2 = WorkerInfo::new(Default::default(), 1);
        w2.worker_max_concurrent_tasks = 5;
        let admitted = choose_worker_admitted(std::slice::from_ref(&w2), 10, 2, 0, |_| (4, 0));
        assert_eq!(admitted.len(), 1);
        // ...and is excluded once it reaches its own reported limit.
        let admitted = choose_worker_admitted(std::slice::from_ref(&w2), 10, 2, 0, |_| (5, 0));
        assert!(admitted.is_empty());
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
    fn planning_throttle_uses_bounded_backoff_with_jitter() {
        assert_eq!(planning_retry_attempt(0), Some(1));
        assert_eq!(planning_retry_attempt(PLANNING_LIST_MAX_RETRIES), None);
        // Exponential growth with bounded jitter.
        let d0 = planning_retry_delay_ms(0, "job-a", 1_000_000);
        let d1 = planning_retry_delay_ms(1, "job-a", 1_000_000);
        let d2 = planning_retry_delay_ms(2, "job-a", 1_000_000);
        assert!((1_000..2_000).contains(&d0));
        assert!((2_000..3_000).contains(&d1));
        assert!((4_000..5_000).contains(&d2));
        // Same attempt, different timestamps spread out.
        assert_ne!(
            planning_retry_delay_ms(1, "job-a", 1_000_000),
            planning_retry_delay_ms(1, "job-a", 1_000_001)
        );
    }

    #[test]
    fn task_retryable_covers_transient_signals_only() {
        use curvine_model::{TransferProgress, TransferTaskRecord, TransferTaskState};
        fn failed_task(message: &str) -> TransferTaskRecord {
            TransferTaskRecord {
                job_id: "job".to_string(),
                run_id: 1,
                task_id: "task".to_string(),
                attempt_id: 1,
                source_path: "/s".to_string(),
                target_path: "/t".to_string(),
                worker_id: 1,
                worker_session_id: "session".to_string(),
                source_read_plan_json: "{}".to_string(),
                report_target_json: "{}".to_string(),
                state: TransferTaskState::Failed,
                progress: TransferProgress {
                    message: message.to_string(),
                    ..Default::default()
                },
                estimated_size: 0,
                retry_count: 0,
                attempt_started_at: 0,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                block_locations: Vec::new(),
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: 0,
                replace_target: false,
            }
        }
        // Transient: throttling, timeouts, capacity, session loss, report
        // races, unready segment layout.
        for message in [
            "Failed to list directory x: RateLimited (persistent) at list",
            "S3Error { code: \"TooManyRequests\" }",
            "Timed out while accessing s3://bucket/key; retry",
            "worker_at_capacity: busy, retry later",
            "segment task t block 1 not found in target layout (blocks=0)",
            "Transfer task report timed out after 5000 ms: job=..",
            "WorkerSessionGone: worker restarted",
            "stale worker session rotated",
        ] {
            assert!(is_task_retryable(&failed_task(message)), "{}", message);
        }
        // Permanent: missing source, bad args, oversized data, short reads.
        for message in [
            "Load (UFS -> Curvine) failed during execution: source=oss://b/k; target=/t; cause=object not found",
            "Transfer target /t already exists and overwrite=false",
            "segment task t short read: 100 bytes missing",
            "Not enough Curvine worker disk space",
            "transfer task failed",
        ] {
            assert!(!is_task_retryable(&failed_task(message)), "{}", message);
        }
    }

    #[test]
    fn planning_retryable_covers_throttle_signals_only() {
        use curvine_error::FsError;
        assert!(is_planning_retryable(&FsError::common(
            "Failed to list directory x: RateLimited (persistent) at list"
        )));
        assert!(is_planning_retryable(&FsError::common(
            "S3Error { code: \"TooManyRequests\" }"
        )));
        assert!(!is_planning_retryable(&FsError::file_not_found("/x")));
        assert!(!is_planning_retryable(&FsError::common(
            "Transfer target /x already exists and overwrite=false"
        )));
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
