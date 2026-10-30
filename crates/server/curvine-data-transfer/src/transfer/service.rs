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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use crossbeam::channel;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{
    FileStatus, TaskBlockLocation, TransferCommand, TransferCountFilter, TransferJobRecord,
    TransferKind, TransferListFilter, TransferProgress, TransferState, TransferStateCounts,
    TransferTaskCounts, TransferTaskReport, TransferTaskState, TransferTaskStatus,
    TransferTaskSummary,
};
use curvine_proto::{
    ListTransferTenantsRequest, ListTransferTenantsResponse, ListTransfersRequest,
    ListTransfersResponse, SubmitTransferRequest, TransferJobStatusProto, TransferKindProto,
    TransferProgressProto, TransferStateCountProto, TransferTaskReportBatchRequest,
    TransferTaskReportRequest, TransferTaskStateProto, TransferTaskStatusProto,
    TransferTaskSummaryProto, TransferTenantSummaryProto,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::common::SerdeUtils;
use log::{info, warn};
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::transfer::{
    merge_summaries, ClusterMetadataCache, TaskCache, TransferMetrics, TransferStore,
};

const PROGRESS_REPORT_COALESCE_BATCH: usize = 256;
const MAX_BATCH_TASK_REPORTS: usize = PROGRESS_REPORT_COALESCE_BATCH;
const JOB_SUMMARY_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Highest protocol version this server understands. Version 2 adds fs_mode
/// export batches (`fs_mode_files`); an older server rejects them instead of
/// silently running them as unguarded exports.
const TRANSFER_PROTOCOL_VERSION: u32 = 2;
/// Oldest accepted. Version 1 clients never send fs_mode options, so serving
/// them is safe.
const MIN_TRANSFER_PROTOCOL_VERSION: u32 = 1;
/// First version that understands `fs_mode_files`.
const FS_MODE_PROTOCOL_VERSION: u32 = 2;
pub(super) const FS_MODE_BATCH_MAX_FILES: usize = TransferCommand::FS_MODE_BATCH_MAX_FILES;
const DEFAULT_LIST_PAGE_SIZE: usize = 20;
const MAX_LIST_PAGE_SIZE: usize = 1000;
/// Minimum interval between auto-repair retries for the same load job_key.
/// Prevents submit storms when a broken CV target keeps failing to load.
pub(crate) const AUTO_REPAIR_COOLDOWN_MS: i64 = 60 * 1000;

type TransferStatusPage = (
    TransferJobRecord,
    TransferTaskCounts,
    Vec<TransferTaskStatusProto>,
    Option<String>,
    bool,
);
type TransferWatchPage = (
    TransferJobRecord,
    TransferTaskCounts,
    Vec<TransferTaskStatusProto>,
    Option<String>,
    bool,
    bool,
);

pub struct TransferService<S> {
    store: Arc<S>,
    cache: Option<ClusterMetadataCache>,
    task_stale_timeout_ms: i64,
    report_dispatcher: Option<TransferReportDispatcher<S>>,
    /// job_key -> last auto-repair attempt time (ms).
    /// Cooldown is per-process / per-replica, not shared across transfer instances.
    auto_repair_last_attempt_ms: Arc<Mutex<HashMap<String, i64>>>,
    /// File-level task state (None = every task lives in the store).
    task_cache: Option<Arc<TaskCache>>,
    /// Cached discovery topology version, piggybacked onto report acks.
    topology_version: Arc<AtomicI64>,
}

impl<S> Clone for TransferService<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            cache: self.cache.clone(),
            task_stale_timeout_ms: self.task_stale_timeout_ms,
            report_dispatcher: self.report_dispatcher.clone(),
            auto_repair_last_attempt_ms: self.auto_repair_last_attempt_ms.clone(),
            task_cache: self.task_cache.clone(),
            topology_version: self.topology_version.clone(),
        }
    }
}

impl<S> TransferService<S>
where
    S: TransferStore,
{
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            cache: None,
            task_stale_timeout_ms: 60_000,
            report_dispatcher: None,
            auto_repair_last_attempt_ms: Arc::new(Mutex::new(HashMap::new())),
            task_cache: None,
            topology_version: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn with_task_stale_timeout(store: Arc<S>, task_stale_timeout: Duration) -> Self {
        Self {
            store,
            cache: None,
            task_stale_timeout_ms: i64::try_from(task_stale_timeout.as_millis())
                .unwrap_or(i64::MAX),
            report_dispatcher: None,
            auto_repair_last_attempt_ms: Arc::new(Mutex::new(HashMap::new())),
            task_cache: None,
            topology_version: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn with_report_queue(
        store: Arc<S>,
        task_stale_timeout: Duration,
        task_report_queue_size: usize,
        task_report_workers: usize,
    ) -> FsResult<Self> {
        let task_stale_timeout_ms =
            i64::try_from(task_stale_timeout.as_millis()).unwrap_or(i64::MAX);
        Ok(Self {
            store: store.clone(),
            cache: None,
            task_stale_timeout_ms,
            report_dispatcher: Some(TransferReportDispatcher::new(
                store,
                task_stale_timeout_ms,
                task_report_queue_size,
                task_report_workers,
            )?),
            auto_repair_last_attempt_ms: Arc::new(Mutex::new(HashMap::new())),
            task_cache: None,
            topology_version: Arc::new(AtomicI64::new(0)),
        })
    }

    pub fn with_cache(
        store: Arc<S>,
        cache: ClusterMetadataCache,
        task_stale_timeout: Duration,
    ) -> Self {
        Self {
            store,
            cache: Some(cache),
            task_stale_timeout_ms: i64::try_from(task_stale_timeout.as_millis())
                .unwrap_or(i64::MAX),
            report_dispatcher: None,
            auto_repair_last_attempt_ms: Arc::new(Mutex::new(HashMap::new())),
            task_cache: None,
            topology_version: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn with_cache_and_report_queue(
        store: Arc<S>,
        cache: ClusterMetadataCache,
        task_stale_timeout: Duration,
        task_report_queue_size: usize,
        task_report_workers: usize,
    ) -> FsResult<Self> {
        let task_stale_timeout_ms =
            i64::try_from(task_stale_timeout.as_millis()).unwrap_or(i64::MAX);
        Ok(Self {
            store: store.clone(),
            cache: Some(cache),
            task_stale_timeout_ms,
            report_dispatcher: Some(TransferReportDispatcher::new(
                store,
                task_stale_timeout_ms,
                task_report_queue_size,
                task_report_workers,
            )?),
            auto_repair_last_attempt_ms: Arc::new(Mutex::new(HashMap::new())),
            task_cache: None,
            topology_version: Arc::new(AtomicI64::new(0)),
        })
    }

    pub fn submit_transfer(&self, req: SubmitTransferRequest) -> FsResult<TransferJobRecord> {
        let job = self.submit_transfer_inner(req, true)?;
        self.record_submit_accepted(&job);
        Ok(job)
    }

    fn submit_transfer_inner(
        &self,
        req: SubmitTransferRequest,
        allow_auto_repair: bool,
    ) -> FsResult<TransferJobRecord> {
        validate_protocol_version(req.protocol_version)?;
        let kind = transfer_kind(req.kind)?;
        let client_request_id = if req.client_request_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            req.client_request_id.clone()
        };
        let mut command = TransferCommand {
            kind,
            source_path: req.source_path.clone(),
            target_path: req.target_path.clone(),
            client_request_id: client_request_id.clone(),
            submitter: req.submitter.clone(),
            tenant: req.tenant.clone(),
            options: Default::default(),
            fs_mode_files: Vec::new(),
        };
        if !req.command.is_empty() {
            command = decode_transfer_command(&req.command)?;
            if command.kind != kind
                || command.source_path != req.source_path
                || command.target_path != req.target_path
                || command.client_request_id != client_request_id
                || command.submitter != req.submitter
                || command.tenant != req.tenant
            {
                return Err(FsError::common(format!(
                    "Transfer command payload does not match request identity fields: request kind={:?}, source={}, target={}, client request id={}, submitter={}, tenant={}; payload kind={:?}, source={}, target={}, client request id={}, submitter={}, tenant={}",
                    kind,
                    req.source_path,
                    req.target_path,
                    client_request_id,
                    req.submitter,
                    req.tenant,
                    command.kind,
                    command.source_path,
                    command.target_path,
                    command.client_request_id,
                    command.submitter,
                    command.tenant,
                )));
            }
        }
        validate_transfer_options(&command)?;
        if command.is_fs_mode_batch()
            && req
                .protocol_version
                .unwrap_or(MIN_TRANSFER_PROTOCOL_VERSION)
                < FS_MODE_PROTOCOL_VERSION
        {
            return Err(FsError::common(format!(
                "fs_mode_files requires transfer protocol version {}",
                FS_MODE_PROTOCOL_VERSION
            )));
        }
        validate_transfer_paths(command.kind, &command.source_path, &command.target_path)?;
        command.target_path = normalized_transfer_path(&Path::from_str(&command.target_path)?);
        let now_ms = now_ms();
        let command_json = String::from_utf8(encode_transfer_command(&command)?)
            .map_err(|_| FsError::common("Unable to prepare the transfer command"))?;
        let job = TransferJobRecord {
            job_key: command.job_key(),
            job_id: Uuid::new_v4().to_string(),
            run_id: 1,
            kind,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            command_json,
            mount_snapshot_json: "{}".to_string(),
            secret_ref_json: "{}".to_string(),
            cluster_snapshot_version: 0,
            mount_snapshot_attempts: 0,
            planning_attempts: 0,
            terminal_summary: None,
            terminal_counts: None,
            cv_metadata_epoch: None,
            state: TransferState::Pending,
            owner: String::new(),
            lease_epoch: 0,
            lease_expire_at: 0,
            cancel_requested: false,
            summary: TransferProgress {
                message: "waiting for scheduling".to_string(),
                ..Default::default()
            },
            client_request_id: command.client_request_id,
            submitter: command.submitter,
            tenant: command.tenant,
            created_at: now_ms,
            updated_at: now_ms,
        };
        let job = self.store.create_or_get_by_request_id_checked(job)?;
        if allow_auto_repair {
            self.maybe_auto_repair_terminal_load(job)
        } else {
            Ok(job)
        }
    }

    fn record_submit_accepted(&self, job: &TransferJobRecord) {
        record_submit_metric(job.kind, "accepted");
        info!(
            "transfer submit accepted job_id={} run_id={} kind={:?} tenant={} submitter={} source={} target={} state={:?}",
            job.job_id,
            job.run_id,
            job.kind,
            job.tenant,
            job.submitter,
            job.source_path,
            job.target_path,
            job.state
        );
    }

    /// When auto_cache reuses a stable client_request_id, a Completed/PartialSuccess Load can
    /// leave behind an unusable CV cache target (`!is_complete` or `!cv_valid`). Re-submits
    /// then keep returning that job and never reload. If the CV target is still invalid, retry
    /// once (with cooldown) so load can repair the shell without racing an in-flight job.
    ///
    /// Failed/Canceled are intentionally excluded: callers should use explicit retry or a new
    /// request id rather than treating same-id re-submit as an implicit retry.
    fn maybe_auto_repair_terminal_load(
        &self,
        job: TransferJobRecord,
    ) -> FsResult<TransferJobRecord> {
        if !auto_repair_eligible(job.kind, job.state) {
            return Ok(job);
        }

        let Some(cache) = &self.cache else {
            return Ok(job);
        };

        let target = match Path::from_str(&job.target_path) {
            Ok(path) => path,
            Err(err) => {
                warn!(
                    "skip auto-repair for job {} target {}: invalid path: {}",
                    job.job_id, job.target_path, err
                );
                return Ok(job);
            }
        };
        if !target.is_cv() {
            return Ok(job);
        }

        // Check cooldown before get_status so same-id storms do not hammer Master.
        let now = now_ms();
        {
            let last_attempts = self
                .auto_repair_last_attempt_ms
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if let Some(last) = last_attempts.get(&job.job_key).copied() {
                if auto_repair_in_cooldown(last, now) {
                    info!(
                        "skip auto-repair for job_key={} target={} due to cooldown ({} ms remaining)",
                        job.job_key,
                        job.target_path,
                        AUTO_REPAIR_COOLDOWN_MS - now.saturating_sub(last)
                    );
                    return Ok(job);
                }
            }
        }

        let needs_repair = match cache.get_status_blocking(&target) {
            Ok(status) if status.is_dir => {
                info!(
                    "skip auto-repair for job {} target {}: directory target is not auto-repaired",
                    job.job_id, job.target_path
                );
                false
            }
            Ok(status) => cv_cache_target_needs_repair(&status),
            Err(FsError::FileNotFound(_)) => true,
            Err(err) => {
                warn!(
                    "skip auto-repair for job {} target {}: status check failed: {}",
                    job.job_id, job.target_path, err
                );
                return Ok(job);
            }
        };
        if !needs_repair {
            return Ok(job);
        }

        // Another run for this job_key may already be repairing; do not burn cooldown.
        let has_active_job = match self.store.has_active_transfer_by_key(&job.job_key) {
            Ok(active) => active,
            Err(err) => {
                warn!(
                    "skip auto-repair for job_key={} target={}: active job lookup failed: {}",
                    job.job_key, job.target_path, err
                );
                return Ok(job);
            }
        };
        if has_active_job {
            info!(
                "skip auto-repair for job_key={} target={}: active transfer already running",
                job.job_key, job.target_path
            );
            return Ok(job);
        }

        {
            let now = now_ms();
            let mut last_attempts = self
                .auto_repair_last_attempt_ms
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            // Re-check under write lock in case another submit raced past the early check.
            if let Some(last) = last_attempts.get(&job.job_key).copied() {
                if auto_repair_in_cooldown(last, now) {
                    info!(
                        "skip auto-repair for job_key={} target={} due to cooldown ({} ms remaining)",
                        job.job_key,
                        job.target_path,
                        AUTO_REPAIR_COOLDOWN_MS - now.saturating_sub(last)
                    );
                    return Ok(job);
                }
            }
            last_attempts.retain(|_, ts| auto_repair_in_cooldown(*ts, now));
            last_attempts.insert(job.job_key.clone(), now);
        }

        info!(
            "auto-repair completed load job_id={} state={:?} target={} still invalid; retrying",
            job.job_id, job.state, job.target_path
        );
        match self.retry_transfer_inner(&job.job_id) {
            Ok(repaired) => Ok(repaired),
            Err(err) => {
                // Concurrent submit may have won the race after our active-job check.
                warn!(
                    "auto-repair retry failed for job_id={} target={}: {}; returning original job",
                    job.job_id, job.target_path, err
                );
                Ok(job)
            }
        }
    }

    /// Attaches the process-wide file-level task cache. Without it every task
    /// read/write goes to the store (the pre-cache behavior).
    pub fn with_task_cache(mut self, task_cache: Arc<TaskCache>) -> Self {
        self.task_cache = Some(task_cache.clone());
        if let Err(err) = self.spawn_cache_summary_writer(task_cache) {
            log::warn!("unable to start the transfer cache summary writer: {}", err);
        }
        self
    }

    /// Writes the merged (store + cache) job summary back periodically.
    ///
    /// File-level tasks have no rows, so nothing else refreshes the job row:
    /// without this the console list would show 0% progress and a stale
    /// `updated_at` for a running job. One write per active job per interval,
    /// which is orders of magnitude below the per-task writes it replaces.
    fn spawn_cache_summary_writer(&self, cache: Arc<TaskCache>) -> FsResult<()> {
        let store = self.store.clone();
        std::thread::Builder::new()
            .name("transfer-cache-summary-write".to_string())
            .spawn(move || loop {
                std::thread::sleep(JOB_SUMMARY_FLUSH_INTERVAL);
                for (job_id, run_id) in cache.active_jobs() {
                    let Ok(store_summary) =
                        store.get_transfer_task_summary(&job_id, run_id, now_ms())
                    else {
                        continue;
                    };
                    let summary =
                        merge_summaries(store_summary, cache.summary(&job_id, run_id, now_ms()));
                    if let Err(err) = store.update_job_progress_summary(
                        &job_id,
                        run_id,
                        summary.progress,
                        now_ms(),
                    ) {
                        log::warn!(
                            "write back transfer job summary failed: job={} run={} err={}",
                            job_id,
                            run_id,
                            err
                        );
                    }
                }
            })
            .map_err(|_| FsError::common("Unable to start the transfer cache summary writer"))?;
        Ok(())
    }

    /// Shares the discovery topology version with report responses. 0 means
    /// discovery is off, and the worker then ignores the field.
    pub fn with_topology_version(&mut self, version: Arc<AtomicI64>) {
        self.topology_version = version;
    }

    pub fn topology_version(&self) -> i64 {
        self.topology_version.load(Ordering::Relaxed)
    }

    /// Store summary merged with the file-level task cache.
    fn task_summary(&self, job_id: &str, run_id: u64) -> FsResult<TransferTaskSummary> {
        let store = self
            .store
            .get_transfer_task_summary(job_id, run_id, now_ms())?;
        let cached = self
            .task_cache
            .as_ref()
            .and_then(|cache| cache.summary(job_id, run_id, now_ms()));
        Ok(merge_summaries(store, cached))
    }

    pub fn check_store_available(&self) -> FsResult<()> {
        self.store.check_available()
    }

    pub fn get_transfer(&self, job_id: &str) -> FsResult<TransferJobRecord> {
        self.store
            .get_transfer(job_id)?
            .ok_or_else(|| FsError::job_not_found(job_id))
    }

    pub fn retry_transfer(&self, job_id: &str) -> FsResult<TransferJobRecord> {
        let job = self.retry_transfer_inner(job_id)?;
        self.record_submit_accepted(&job);
        Ok(job)
    }

    fn retry_transfer_inner(&self, job_id: &str) -> FsResult<TransferJobRecord> {
        let job = self.get_transfer(job_id)?;
        if !matches!(
            job.state,
            TransferState::Completed
                | TransferState::Failed
                | TransferState::Canceled
                | TransferState::PartialSuccess
        ) {
            return Err(FsError::common(format!(
                "Only completed, failed, canceled, or partially successful transfers can be rerun; job {} is currently {}",
                job_id,
                transfer_state_name(job.state)
            )));
        }

        let mut command = decode_transfer_command(job.command_json.as_bytes())?;
        command.client_request_id = Uuid::new_v4().to_string();
        // Nested auto-repair must stay off: retry already opens a fresh request_id run.
        self.submit_transfer_inner(
            SubmitTransferRequest {
                kind: command.kind as i32,
                source_path: command.source_path.clone(),
                target_path: command.target_path.clone(),
                client_request_id: command.client_request_id.clone(),
                submitter: command.submitter.clone(),
                tenant: command.tenant.clone(),
                command: encode_transfer_command(&command)?,
                protocol_version: Some(if command.is_fs_mode_batch() {
                    FS_MODE_PROTOCOL_VERSION
                } else {
                    TRANSFER_PROTOCOL_VERSION
                }),
            },
            false,
        )
    }

    pub fn get_transfer_status(
        &self,
        job_id: &str,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<TransferStatusPage> {
        let mut job = self.get_transfer(job_id)?;
        let summary = self.task_summary(&job.job_id, job.run_id)?;
        let (mut progress, counts, expired) = resolve_terminal_summary(&job, &summary);
        // The live aggregate derives its message from task rows; scheduling-phase
        // reasons ("waiting for worker capacity", planning requeues) live on the
        // persisted job summary, so fall back to it while tasks carry no message.
        if progress.message.is_empty() && !job.summary.message.is_empty() {
            progress.message = job.summary.message.clone();
        }
        job.summary = progress;
        let page_size = page_size.unwrap_or(0) as usize;
        let offset = parse_page_token(page_token)?;
        if page_size == 0 {
            return Ok((job, counts, Vec::new(), None, expired));
        }
        // Segment rows come from the store, file-level tasks from memory. They
        // concatenate in that order (store first), so a page is only backed by
        // the cache once the store page is exhausted.
        let (mut tasks, store_has_next) = self.store.list_transfer_task_status_page(
            &job.job_id,
            job.run_id,
            offset,
            page_size + 1,
        )?;
        if !store_has_next && tasks.len() < page_size + 1 {
            let mut cached: Vec<TransferTaskStatus> = self
                .task_cache
                .as_ref()
                .map(|cache| {
                    let mut tasks = cache.tasks(&job.job_id, job.run_id);
                    tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
                    tasks.into_iter().map(Into::into).collect()
                })
                .unwrap_or_default();
            tasks.append(&mut cached);
            tasks.truncate(page_size + 1);
        }
        let has_next = tasks.len() > page_size;
        tasks.truncate(page_size);
        let next_page_token = has_next.then(|| offset.saturating_add(tasks.len()).to_string());
        Ok((
            job,
            counts,
            tasks.into_iter().map(task_status_to_proto).collect(),
            next_page_token,
            expired,
        ))
    }

    pub fn list_transfers(&self, req: ListTransfersRequest) -> FsResult<ListTransfersResponse> {
        let page_size = bounded_page_size(req.page_size);
        let offset = parse_page_token(req.page_token)?;
        let kind = req.kind.map(transfer_kind).transpose()?;
        // The bucket arrives as a set of states; a single `state` is the older,
        // single-valued form of the same filter.
        let states: Vec<TransferState> = if !req.states.is_empty() {
            req.states
                .iter()
                .map(|state| transfer_state(*state))
                .collect::<FsResult<Vec<_>>>()?
        } else {
            req.state
                .map(transfer_state)
                .transpose()?
                .into_iter()
                .collect()
        };
        let submitter = non_empty_filter(req.submitter);
        let tenant = non_empty_filter(req.tenant);
        let filter = TransferListFilter {
            kind,
            states: states.clone(),
            submitter: submitter.clone(),
            tenant: tenant.clone(),
            limit: page_size + 1,
            offset,
        };
        let mut jobs = self.store.list_transfers(filter)?;
        let next_page_token = if jobs.len() > page_size {
            jobs.truncate(page_size);
            Some((offset + page_size).to_string())
        } else {
            None
        };
        // One grouped count serves the page total and every UI bucket badge.
        let counts = self.store.count_transfers_by_state(&TransferCountFilter {
            kind,
            submitter,
            tenant,
        })?;
        let total = counts.total_for(&states);
        Ok(ListTransfersResponse {
            jobs: jobs.into_iter().map(job_to_proto).collect(),
            next_page_token,
            total: Some(total),
            counts: state_counts_to_proto(&counts),
        })
    }

    pub fn list_tenant_summaries(
        &self,
        req: ListTransferTenantsRequest,
    ) -> FsResult<ListTransferTenantsResponse> {
        let page_size = bounded_page_size(req.page_size);
        let offset = parse_page_token(req.page_token)?;
        let mut summaries = self.store.list_tenant_summaries(page_size + 1, offset)?;
        let next_page_token = if summaries.len() > page_size {
            summaries.truncate(page_size);
            Some((offset + page_size).to_string())
        } else {
            None
        };
        Ok(ListTransferTenantsResponse {
            tenants: summaries
                .into_iter()
                .map(|summary| TransferTenantSummaryProto {
                    tenant: summary.tenant,
                    pending: summary.pending,
                    executing: summary.executing,
                    completed: summary.completed,
                    failed: summary.failed,
                    canceled: summary.canceled,
                    total: summary.total,
                    partial_success: summary.partial_success,
                })
                .collect(),
            next_page_token,
        })
    }

    pub fn watch_transfer(
        &self,
        job_id: &str,
        since_updated_at: Option<u64>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<TransferWatchPage> {
        let (job, counts, tasks, next_page_token, expired) =
            self.get_transfer_status(job_id, page_size, page_token)?;
        let changed = since_updated_at
            .map(|since| job.updated_at as u64 > since)
            .unwrap_or(true);
        Ok((job, counts, tasks, next_page_token, changed, expired))
    }

    pub fn request_cancel(&self, job_id: &str, run_id: Option<u64>) -> FsResult<TransferState> {
        let job = self.get_transfer(job_id)?;
        let target_run_id = run_id.unwrap_or(job.run_id);
        if target_run_id == job.run_id
            && (job.state == TransferState::Canceling || job.state.is_terminal())
        {
            return Ok(job.state);
        }
        if !self.store.request_cancel(job_id, target_run_id, now_ms())? {
            let current = self.get_transfer(job_id)?;
            if target_run_id == current.run_id
                && (current.state == TransferState::Canceling || current.state.is_terminal())
            {
                return Ok(current.state);
            }
            return Err(FsError::common(format!(
                "Transfer {} run {} cannot be canceled because it is not active",
                job_id, target_run_id
            )));
        }
        Ok(self.get_transfer(job_id)?.state)
    }

    pub fn report_task(&self, req: TransferTaskReportRequest) -> FsResult<bool> {
        validate_protocol_version(req.protocol_version)?;
        let state = transfer_task_state(req.state)?;
        self.report_task_with_state(req, state)
    }

    pub fn report_tasks(&self, req: TransferTaskReportBatchRequest) -> FsResult<Vec<bool>> {
        validate_protocol_version(req.protocol_version)?;
        if req.reports.len() > MAX_BATCH_TASK_REPORTS {
            return Err(FsError::invalid_argument(format!(
                "Transfer task report batch exceeds maximum size {}",
                MAX_BATCH_TASK_REPORTS
            )));
        }

        req.reports
            .into_iter()
            .map(|report| self.report_batch_member(report))
            .collect()
    }

    fn report_batch_member(&self, report: TransferTaskReportRequest) -> FsResult<bool> {
        if validate_protocol_version(report.protocol_version).is_err() {
            return Ok(false);
        }
        let Ok(state) = transfer_task_state(report.state) else {
            return Ok(false);
        };
        if state != TransferTaskState::Running {
            return Ok(false);
        }
        // Overload is not a verdict. `false` means the store refused this report
        // because the attempt lost ownership, and a worker treats that as final;
        // a full queue is transient and must stay retryable. The batch shares one
        // error for the whole call, so report it as an error and let the worker
        // retry the group rather than telling it every task was fenced.
        self.report_task_with_state(report, state)
    }

    /// Applies a report for a file-level (memory-only) task.
    ///
    /// Both the queued and the direct report paths land here, so a cache task
    /// advances exactly once regardless of how the report arrived.
    fn report_cached_task(
        &self,
        req: TransferTaskReportRequest,
        state: TransferTaskState,
        cache: &Arc<TaskCache>,
    ) -> FsResult<bool> {
        let now = now_ms();
        let report = TransferTaskReport {
            job_id: req.job_id.clone(),
            run_id: req.run_id,
            task_id: req.task_id.clone(),
            attempt_id: req.attempt_id,
            worker_id: req.worker_id,
            worker_session_id: req.worker_session_id.clone(),
            state,
            progress: progress_from_proto(req.progress.clone()),
            now_ms: now,
            stale_deadline_at: now.saturating_add(self.task_stale_timeout_ms),
            accepted_at_ms: req.accepted_at_ms.unwrap_or(0),
            first_byte_at_ms: req.first_byte_at_ms.unwrap_or(0),
            block_locations: block_locations_from_proto(&req.block_locations),
        };
        let accepted = cache.apply_report(&report).unwrap_or(false);
        if accepted && state.is_terminal() {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.task_terminal(state);
            }
            self.observe_task_phases(
                &req.job_id,
                req.run_id,
                &req.task_id,
                ReportPhaseMarkers {
                    attempt_started_at_ms: req.attempt_started_at_ms.unwrap_or(0),
                    accepted_at_ms: req.accepted_at_ms.unwrap_or(0),
                    first_byte_at_ms: req.first_byte_at_ms.unwrap_or(0),
                    terminal_ms: req.progress.update_time,
                },
            );
        }
        Ok(accepted)
    }

    fn report_task_with_state(
        &self,
        req: TransferTaskReportRequest,
        state: TransferTaskState,
    ) -> FsResult<bool> {
        if let Some(dispatcher) = &self.report_dispatcher {
            // A file-level task lives only in memory, so it must not enter the
            // report queue: the queue's store batch would reject it as fenced.
            // Answer such reports here, ahead of the dispatcher.
            if let Some(cache) = &self.task_cache {
                if cache.owns_task(&req.job_id, req.run_id, &req.task_id) {
                    return self.report_cached_task(req, state, cache);
                }
            }
            return dispatcher.report_task(req, state);
        }
        let now_ms = now_ms();
        let (job_id, run_id, task_id) = (req.job_id.clone(), req.run_id, req.task_id.clone());
        let report = TransferTaskReport {
            job_id: req.job_id.clone(),
            run_id: req.run_id,
            task_id: req.task_id.clone(),
            attempt_id: req.attempt_id,
            worker_id: req.worker_id,
            worker_session_id: req.worker_session_id.clone(),
            state,
            progress: progress_from_proto(req.progress.clone()),
            now_ms,
            stale_deadline_at: now_ms.saturating_add(self.task_stale_timeout_ms),
            accepted_at_ms: req.accepted_at_ms.unwrap_or(0),
            first_byte_at_ms: req.first_byte_at_ms.unwrap_or(0),
            block_locations: block_locations_from_proto(&req.block_locations),
        };
        // File-level tasks are memory-only: their whole lifecycle happens here.
        if let Some(cache) = &self.task_cache {
            if cache.owns_task(&req.job_id, req.run_id, &req.task_id) {
                return self.report_cached_task(req, state, cache);
            }
        }
        let accepted = self.store.update_task_report(report.clone())?;
        // A rejected report whose task does not exist at all means this
        // instance is not the job's owner (a rolling deploy left the worker's
        // ring stale). Answer with a retryable error rather than a `false`
        // verdict: the worker treats `false` as "this attempt is fenced" and
        // would drop the result. The check runs only on the rejection path, so
        // the accepted hot path still costs exactly one store call.
        if !accepted
            && self.task_cache.is_some()
            && !self
                .store
                .has_transfer_tasks(&report.job_id, report.run_id)?
        {
            return Err(FsError::common(format!(
                "transfer task {}/{}/{} has no state on this instance; retry after refreshing the topology",
                report.job_id, report.run_id, report.task_id
            )));
        }
        if accepted && state.is_terminal() {
            if let Ok(metrics) = TransferMetrics::get() {
                metrics.task_terminal(state);
            }
            let terminal_ms = req.progress.update_time;
            self.observe_task_phases(
                &job_id,
                run_id,
                &task_id,
                ReportPhaseMarkers {
                    attempt_started_at_ms: req.attempt_started_at_ms.unwrap_or(0),
                    accepted_at_ms: req.accepted_at_ms.unwrap_or(0),
                    first_byte_at_ms: req.first_byte_at_ms.unwrap_or(0),
                    terminal_ms,
                },
            );
        }
        if accepted {
            // Sync path has no background flusher; keep job.summary up to date.
            // Flush failure must not fail the accepted task persist (retry would be rejected).
            if let Err(err) = self
                .store
                .flush_job_progress_summary(&req.job_id, req.run_id)
            {
                log::warn!(
                    "flush transfer job progress summary failed after sync report: job={} run={} err={}",
                    req.job_id,
                    req.run_id,
                    err
                );
            }
        }
        Ok(accepted)
    }

    /// Record queue/start/transfer phase latencies for a terminal report.
    /// Skips silently when the worker omitted markers (old version) or the
    /// task row is gone; observability must never fail task persist.
    fn observe_task_phases(
        &self,
        job_id: &str,
        run_id: u64,
        task_id: &str,
        markers: ReportPhaseMarkers,
    ) {
        let ReportPhaseMarkers {
            attempt_started_at_ms,
            accepted_at_ms,
            first_byte_at_ms,
            terminal_ms,
        } = markers;
        if accepted_at_ms <= 0 || first_byte_at_ms <= 0 {
            return;
        }
        let Some(started) = resolve_attempt_started_at(attempt_started_at_ms, || {
            self.store
                .get_task_attempt_started_at(job_id, run_id, task_id)
                .ok()
                .flatten()
        }) else {
            return;
        };
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.observe_task_phase("dispatch_wait", accepted_at_ms.saturating_sub(started));
            metrics.observe_task_phase(
                "start_wait",
                first_byte_at_ms.saturating_sub(accepted_at_ms),
            );
            metrics.observe_task_phase("transfer", terminal_ms.saturating_sub(first_byte_at_ms));
        }
    }
}

/// Worker-reported phase markers (ms since epoch, 0 = unknown). Grouped so the
/// two phase observers stay under the argument limit and share one shape.
#[derive(Clone, Copy, Default)]
struct ReportPhaseMarkers {
    attempt_started_at_ms: i64,
    accepted_at_ms: i64,
    first_byte_at_ms: i64,
    terminal_ms: i64,
}

/// Everything needed to settle one persisted report after the store write:
/// what to mark dirty, which phase markers to observe, and where to answer.
struct ReportSettle {
    job_id: String,
    run_id: u64,
    task_id: String,
    state: TransferTaskState,
    markers: ReportPhaseMarkers,
    reply: Option<mpsc::Sender<FsResult<bool>>>,
}

struct TransferReportEnvelope {
    req: TransferTaskReportRequest,
    state: TransferTaskState,
    reply: Option<mpsc::Sender<FsResult<bool>>>,
}

#[derive(Clone, Hash, Eq, PartialEq)]
struct DirtyJobKey {
    job_id: String,
    run_id: u64,
}

struct JobSummaryDirtySet {
    dirty: Mutex<HashSet<DirtyJobKey>>,
}

impl JobSummaryDirtySet {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            dirty: Mutex::new(HashSet::new()),
        })
    }

    fn mark(&self, job_id: String, run_id: u64) {
        let mut dirty = self.dirty.lock().unwrap_or_else(|err| err.into_inner());
        dirty.insert(DirtyJobKey { job_id, run_id });
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_job_summary_dirty_len(dirty.len());
        }
    }

    fn take_all(&self) -> Vec<DirtyJobKey> {
        let mut dirty = self.dirty.lock().unwrap_or_else(|err| err.into_inner());
        let jobs = dirty.drain().collect::<Vec<_>>();
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_job_summary_dirty_len(0);
        }
        jobs
    }

    fn requeue(&self, jobs: impl IntoIterator<Item = DirtyJobKey>) {
        let mut dirty = self.dirty.lock().unwrap_or_else(|err| err.into_inner());
        dirty.extend(jobs);
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_job_summary_dirty_len(dirty.len());
        }
    }
}

#[derive(Hash, Eq, PartialEq)]
struct ProgressReportKey {
    job_id: String,
    run_id: u64,
    task_id: String,
    attempt_id: u64,
    worker_session_id: String,
}

impl ProgressReportKey {
    fn new(envelope: &TransferReportEnvelope) -> Self {
        Self {
            job_id: envelope.req.job_id.clone(),
            run_id: envelope.req.run_id,
            task_id: envelope.req.task_id.clone(),
            attempt_id: envelope.req.attempt_id,
            worker_session_id: envelope.req.worker_session_id.clone(),
        }
    }
}

struct TransferReportDispatcher<S> {
    progress_sender: channel::Sender<TransferReportEnvelope>,
    terminal_sender: channel::Sender<TransferReportEnvelope>,
    _store: Arc<S>,
}

impl<S> Clone for TransferReportDispatcher<S> {
    fn clone(&self) -> Self {
        Self {
            progress_sender: self.progress_sender.clone(),
            terminal_sender: self.terminal_sender.clone(),
            _store: self._store.clone(),
        }
    }
}

impl<S> TransferReportDispatcher<S>
where
    S: TransferStore,
{
    fn new(
        store: Arc<S>,
        task_stale_timeout_ms: i64,
        queue_size: usize,
        workers: usize,
    ) -> FsResult<Self> {
        let (progress_sender, progress_receiver) =
            channel::bounded::<TransferReportEnvelope>(queue_size.max(1));
        let (terminal_sender, terminal_receiver) =
            channel::bounded::<TransferReportEnvelope>(queue_size.max(1));
        let worker_count = workers.max(1);
        let dirty_jobs = JobSummaryDirtySet::new();
        Self::spawn_report_workers(
            "progress",
            progress_receiver,
            store.clone(),
            dirty_jobs.clone(),
            task_stale_timeout_ms,
            worker_count,
        )?;
        Self::spawn_report_workers(
            "terminal",
            terminal_receiver,
            store.clone(),
            dirty_jobs.clone(),
            task_stale_timeout_ms,
            worker_count,
        )?;
        Self::spawn_summary_flusher(store.clone(), dirty_jobs.clone())?;
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_report_queue_len_by_lane(0, 0);
            metrics.set_job_summary_dirty_len(0);
        }
        Ok(Self {
            progress_sender,
            terminal_sender,
            _store: store,
        })
    }

    fn spawn_summary_flusher(store: Arc<S>, dirty_jobs: Arc<JobSummaryDirtySet>) -> FsResult<()> {
        std::thread::Builder::new()
            .name("transfer-job-summary-flush".to_string())
            .spawn(move || loop {
                std::thread::sleep(JOB_SUMMARY_FLUSH_INTERVAL);
                let jobs = dirty_jobs.take_all();
                if jobs.is_empty() {
                    continue;
                }
                let mut failed = Vec::new();
                for job in jobs {
                    let started = Instant::now();
                    match store.flush_job_progress_summary(&job.job_id, job.run_id) {
                        Ok(()) => {
                            if let Ok(metrics) = TransferMetrics::get() {
                                metrics
                                    .observe_job_summary_flush("ok", started.elapsed().as_micros());
                            }
                        }
                        Err(err) => {
                            if let Ok(metrics) = TransferMetrics::get() {
                                metrics.observe_job_summary_flush(
                                    "error",
                                    started.elapsed().as_micros(),
                                );
                            }
                            log::warn!(
                                "flush transfer job progress summary failed: job={} run={} err={}",
                                job.job_id,
                                job.run_id,
                                err
                            );
                            failed.push(job);
                        }
                    }
                }
                if !failed.is_empty() {
                    dirty_jobs.requeue(failed);
                }
            })
            .map_err(|_| FsError::common("Unable to start the transfer job summary flusher"))?;
        Ok(())
    }

    fn spawn_report_workers(
        lane: &'static str,
        receiver: channel::Receiver<TransferReportEnvelope>,
        store: Arc<S>,
        dirty_jobs: Arc<JobSummaryDirtySet>,
        task_stale_timeout_ms: i64,
        worker_count: usize,
    ) -> FsResult<()> {
        for index in 0..worker_count {
            let receiver = receiver.clone();
            let store = store.clone();
            let dirty_jobs = dirty_jobs.clone();
            std::thread::Builder::new()
                .name(format!("transfer-report-{lane}-{index}"))
                .spawn(move || {
                    while let Ok(envelope) = receiver.recv() {
                        if lane == "progress" {
                            let mut reports = HashMap::new();
                            reports.insert(ProgressReportKey::new(&envelope), envelope);
                            for _ in 1..PROGRESS_REPORT_COALESCE_BATCH {
                                match receiver.try_recv() {
                                    Ok(envelope) => {
                                        reports.insert(ProgressReportKey::new(&envelope), envelope);
                                    }
                                    Err(channel::TryRecvError::Empty) => break,
                                    Err(channel::TryRecvError::Disconnected) => return,
                                }
                            }
                            // One store call for the whole drained group so the
                            // reports share a single commit.
                            Self::handle_report_envelopes(
                                &store,
                                &dirty_jobs,
                                task_stale_timeout_ms,
                                reports.into_values().collect(),
                            );
                        } else {
                            Self::handle_report_envelope(
                                &store,
                                &dirty_jobs,
                                task_stale_timeout_ms,
                                envelope,
                            );
                        }
                    }
                })
                .map_err(|_| FsError::common("Unable to start the transfer report processor"))?;
        }
        Ok(())
    }

    fn handle_report_envelope(
        store: &S,
        dirty_jobs: &JobSummaryDirtySet,
        task_stale_timeout_ms: i64,
        envelope: TransferReportEnvelope,
    ) {
        Self::handle_report_envelopes(store, dirty_jobs, task_stale_timeout_ms, vec![envelope]);
    }

    /// Persist a drained group of reports with one store call so the whole
    /// group shares a single commit, then settle each report on its own
    /// (acceptance, dirty-summary marking, terminal phases, reply).
    ///
    /// A failed batch is logged and dropped rather than replayed one by one:
    /// the progress lane carries running progress only, and the next report of
    /// each task carries a greater-or-equal loaded size, so a lost group
    /// self-heals within one report interval. Terminal reports keep their own
    /// lane and their per-report reply.
    fn handle_report_envelopes(
        store: &S,
        dirty_jobs: &JobSummaryDirtySet,
        task_stale_timeout_ms: i64,
        envelopes: Vec<TransferReportEnvelope>,
    ) {
        if envelopes.is_empty() {
            return;
        }
        let mut pending: Vec<(TransferTaskReport, ReportSettle)> =
            Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            pending.push(Self::prepare_report(envelope, task_stale_timeout_ms));
        }
        // Deterministic lock order: two report workers can each drain a report
        // for the same task, and unordered batches would then take row locks in
        // opposite orders. Sorting by key keeps the transaction deadlock-free
        // and leaves the per-report acceptance order aligned with `settled`.
        pending.sort_by(|(left, _), (right, _)| {
            (&left.job_id, left.run_id, &left.task_id).cmp(&(
                &right.job_id,
                right.run_id,
                &right.task_id,
            ))
        });
        let (reports, settled): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
        match store.update_task_reports(reports) {
            Ok(accepted) => {
                for (settle, accepted) in settled.into_iter().zip(accepted) {
                    Self::settle_report(store, dirty_jobs, settle, Some(accepted));
                }
            }
            Err(err) => {
                // Log once for the group; each report still answers its caller.
                log::warn!(
                    "batched transfer task report update failed, {} report(s) dropped: err={}",
                    settled.len(),
                    err
                );
                for settle in settled {
                    Self::settle_report(store, dirty_jobs, settle, None);
                }
            }
        }
    }

    fn prepare_report(
        envelope: TransferReportEnvelope,
        task_stale_timeout_ms: i64,
    ) -> (TransferTaskReport, ReportSettle) {
        let now_ms = now_ms();
        let state = envelope.state;
        let accepted_at_ms = envelope.req.accepted_at_ms.unwrap_or(0);
        let first_byte_at_ms = envelope.req.first_byte_at_ms.unwrap_or(0);
        let settle = ReportSettle {
            job_id: envelope.req.job_id.clone(),
            run_id: envelope.req.run_id,
            task_id: envelope.req.task_id.clone(),
            state,
            markers: ReportPhaseMarkers {
                attempt_started_at_ms: envelope.req.attempt_started_at_ms.unwrap_or(0),
                accepted_at_ms,
                first_byte_at_ms,
                terminal_ms: envelope.req.progress.update_time,
            },
            reply: envelope.reply,
        };
        let report = TransferTaskReport {
            job_id: envelope.req.job_id,
            run_id: envelope.req.run_id,
            task_id: envelope.req.task_id,
            attempt_id: envelope.req.attempt_id,
            worker_id: envelope.req.worker_id,
            worker_session_id: envelope.req.worker_session_id,
            state,
            progress: progress_from_proto(envelope.req.progress),
            now_ms,
            stale_deadline_at: now_ms.saturating_add(task_stale_timeout_ms),
            accepted_at_ms,
            first_byte_at_ms,
            block_locations: block_locations_from_proto(&envelope.req.block_locations),
        };
        (report, settle)
    }

    /// `accepted` is `Some(true)`/`Some(false)` for a persisted report and
    /// `None` when the store write failed (the batch caller logs that once).
    fn settle_report(
        store: &S,
        dirty_jobs: &JobSummaryDirtySet,
        settle: ReportSettle,
        accepted: Option<bool>,
    ) {
        if accepted == Some(true) {
            dirty_jobs.mark(settle.job_id.clone(), settle.run_id);
            if settle.state.is_terminal() {
                observe_task_phases_static(
                    store,
                    &settle.job_id,
                    settle.run_id,
                    &settle.task_id,
                    settle.markers,
                );
                if let Ok(metrics) = TransferMetrics::get() {
                    metrics.task_terminal(settle.state);
                }
            }
        }
        match settle.reply {
            Some(reply) => {
                let result = match accepted {
                    Some(accepted) => Ok(accepted),
                    None => Err(FsError::common(
                        "Transfer task report update failed; retry the report",
                    )),
                };
                let _ = reply.send(result);
            }
            None => match accepted {
                Some(true) => record_report_metric("accepted_async"),
                Some(false) => record_report_metric("rejected_async"),
                None => record_report_metric("failure_async"),
            },
        }
    }

    fn report_task(
        &self,
        req: TransferTaskReportRequest,
        state: TransferTaskState,
    ) -> FsResult<bool> {
        let wait_store_result = state.is_terminal();
        let (reply_tx, reply_rx) = if wait_store_result {
            let (tx, rx) = mpsc::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let lane = if wait_store_result {
            "terminal"
        } else {
            "progress"
        };
        let sender = if wait_store_result {
            &self.terminal_sender
        } else {
            &self.progress_sender
        };
        sender
            .try_send(TransferReportEnvelope {
                req,
                state,
                reply: reply_tx,
            })
            .map_err(|err| match err {
                channel::TrySendError::Full(_) => {
                    record_report_metric(&format!("{lane}_queue_full"));
                    FsError::transfer_overloaded(format!(
                        "Transfer task report queue is full; retry shortly ({lane})"
                    ))
                }
                channel::TrySendError::Disconnected(_) => {
                    record_report_metric(&format!("{lane}_unavailable"));
                    FsError::common(format!(
                        "Transfer task report service is unavailable; retry shortly ({lane})"
                    ))
                }
            })?;
        if let Ok(metrics) = TransferMetrics::get() {
            let progress_len = self.progress_sender.len();
            let terminal_len = self.terminal_sender.len();
            metrics.set_report_queue_len(progress_len + terminal_len);
            metrics.set_report_queue_len_by_lane(progress_len, terminal_len);
        }
        let Some(reply_rx) = reply_rx else {
            record_report_metric("queued");
            return Ok(true);
        };
        let accepted = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| {
                FsError::common("Transfer task report was not processed in time; retry shortly")
            })??;
        record_report_metric(if accepted { "accepted" } else { "rejected" });
        Ok(accepted)
    }
}

fn record_submit_metric(kind: TransferKind, result: &str) {
    if let Ok(metrics) = TransferMetrics::get() {
        let kind = match kind {
            TransferKind::Load => "load",
            TransferKind::Export => "export",
        };
        metrics.inc_submit(kind, result);
    }
}

fn record_report_metric(result: &str) {
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.inc_report(result);
    }
}

/// Phase breakdown for reports processed on the queued path (the production
/// default). Same rules as TransferService::observe_task_phases: skip when
/// the worker omitted markers or the task row is gone; start <= 0 is unknown.
#[allow(clippy::too_many_arguments)]
/// Attempt start time for the phase metrics.
///
/// The worker echoes the dispatch time it was handed, so the normal path needs
/// no store round trip; only a report from an older worker (field absent, 0)
/// falls back to reading the task row. `None` means no usable timestamp, and
/// the caller must skip the observation rather than invent one.
pub(crate) fn resolve_attempt_started_at(
    reported_ms: i64,
    lookup: impl FnOnce() -> Option<i64>,
) -> Option<i64> {
    match reported_ms > 0 {
        true => Some(reported_ms),
        false => lookup().filter(|started| *started > 0),
    }
}

fn observe_task_phases_static(
    store: &impl TransferStore,
    job_id: &str,
    run_id: u64,
    task_id: &str,
    markers: ReportPhaseMarkers,
) {
    let ReportPhaseMarkers {
        attempt_started_at_ms,
        accepted_at_ms,
        first_byte_at_ms,
        terminal_ms,
    } = markers;
    if accepted_at_ms <= 0 || first_byte_at_ms <= 0 {
        return;
    }
    let Some(started) = resolve_attempt_started_at(attempt_started_at_ms, || {
        store
            .get_task_attempt_started_at(job_id, run_id, task_id)
            .ok()
            .flatten()
    }) else {
        return;
    };
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.observe_task_phase("dispatch_wait", accepted_at_ms.saturating_sub(started));
        metrics.observe_task_phase(
            "start_wait",
            first_byte_at_ms.saturating_sub(accepted_at_ms),
        );
        metrics.observe_task_phase("transfer", terminal_ms.saturating_sub(first_byte_at_ms));
    }
}

fn validate_protocol_version(protocol_version: Option<u32>) -> FsResult<()> {
    let protocol_version = protocol_version.unwrap_or(MIN_TRANSFER_PROTOCOL_VERSION);
    if !(MIN_TRANSFER_PROTOCOL_VERSION..=TRANSFER_PROTOCOL_VERSION).contains(&protocol_version) {
        return Err(FsError::common(format!(
            "Unsupported transfer protocol version {}, supported {}..={}",
            protocol_version, MIN_TRANSFER_PROTOCOL_VERSION, TRANSFER_PROTOCOL_VERSION
        )));
    }
    Ok(())
}

fn bounded_page_size(page_size: Option<u32>) -> usize {
    page_size
        .map(|value| value as usize)
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LIST_PAGE_SIZE)
        .min(MAX_LIST_PAGE_SIZE)
}

fn non_empty_filter(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    })
}

fn transfer_kind(kind: i32) -> FsResult<TransferKind> {
    match TransferKindProto::from_i32(kind) {
        Some(TransferKindProto::TransferLoad) => Ok(TransferKind::Load),
        Some(TransferKindProto::TransferExport) => Ok(TransferKind::Export),
        None => Err(FsError::common(format!("Invalid transfer kind {}", kind))),
    }
}

pub(super) fn validate_transfer_options(command: &TransferCommand) -> FsResult<()> {
    if let Some(value) = command.options.get(TransferCommand::REPLICAS_OPTION) {
        if command.kind != TransferKind::Load {
            return Err(FsError::common(
                "replicas is supported only for load transfers",
            ));
        }
        let replicas = value
            .parse::<i32>()
            .map_err(|_| FsError::common("transfer replicas must be a positive integer"))?;
        if replicas <= 0 {
            return Err(FsError::common(
                "transfer replicas must be a positive integer",
            ));
        }
    }

    validate_fs_mode_options(command)
}

/// Validates the fs_mode export shape.
///
/// The `fs-mode:` submitter and `fs_mode_files` must appear together: the
/// submitter exempts a job from target-overlap scheduling, which is only safe
/// for generation-guarded single-file copies. A malformed batch is rejected,
/// never repaired, because guessing would export the wrong data.
fn validate_fs_mode_options(command: &TransferCommand) -> FsResult<()> {
    let fs_mode_submitter = TransferCommand::is_fs_mode_submitter(&command.submitter);
    if fs_mode_submitter != command.is_fs_mode_batch() {
        return Err(FsError::common(
            "the fs-mode submitter and fs_mode_files must be used together",
        ));
    }
    if !command.is_fs_mode_batch() {
        return Ok(());
    }
    if command.kind != TransferKind::Export {
        return Err(FsError::common(
            "fs_mode_files is supported only for export transfers",
        ));
    }
    if command.fs_mode_files.len() > FS_MODE_BATCH_MAX_FILES {
        return Err(FsError::common(format!(
            "fs_mode batch has {} files, limit is {}",
            command.fs_mode_files.len(),
            FS_MODE_BATCH_MAX_FILES
        )));
    }
    for file in &command.fs_mode_files {
        ensure_direction(&file.source, &file.target)?;
    }
    Ok(())
}

/// fs_mode exports always copy from Curvine into UFS.
fn ensure_direction(source: &str, target: &str) -> FsResult<()> {
    let source = Path::from_str(source)
        .map_err(|e| FsError::common(format!("invalid fs_mode source {}: {}", source, e)))?;
    let target = Path::from_str(target)
        .map_err(|e| FsError::common(format!("invalid fs_mode target {}: {}", target, e)))?;
    if !source.is_cv() {
        return Err(FsError::common(format!(
            "fs_mode source {} must be a Curvine path",
            source.full_path()
        )));
    }
    if target.is_cv() {
        return Err(FsError::common(format!(
            "fs_mode target {} must be a UFS path",
            target.full_path()
        )));
    }
    Ok(())
}

fn transfer_state(state: i32) -> FsResult<TransferState> {
    match state {
        1 => Ok(TransferState::Pending),
        2 => Ok(TransferState::Planning),
        3 => Ok(TransferState::Dispatching),
        4 => Ok(TransferState::Running),
        5 => Ok(TransferState::Canceling),
        6 => Ok(TransferState::Completed),
        7 => Ok(TransferState::Failed),
        8 => Ok(TransferState::Canceled),
        9 => Ok(TransferState::PartialSuccess),
        _ => Err(FsError::common(format!("Unknown transfer state {}", state))),
    }
}

/// Every state is emitted, zeros included, so the console can index the response
/// by state instead of guessing which ones were absent.
fn state_counts_to_proto(counts: &TransferStateCounts) -> Vec<TransferStateCountProto> {
    const STATES: [TransferState; 9] = [
        TransferState::Pending,
        TransferState::Planning,
        TransferState::Dispatching,
        TransferState::Running,
        TransferState::Canceling,
        TransferState::Completed,
        TransferState::Failed,
        TransferState::Canceled,
        TransferState::PartialSuccess,
    ];
    STATES
        .iter()
        .map(|state| TransferStateCountProto {
            state: i32::from(*state),
            count: counts.count_of(*state),
        })
        .collect()
}

fn transfer_state_name(state: TransferState) -> &'static str {
    match state {
        TransferState::Pending => "Pending",
        TransferState::Planning => "Planning",
        TransferState::Dispatching => "Dispatching",
        TransferState::Running => "Running",
        TransferState::Canceling => "Canceling",
        TransferState::Completed => "Completed",
        TransferState::Failed => "Failed",
        TransferState::Canceled => "Canceled",
        TransferState::PartialSuccess => "PartialSuccess",
    }
}

fn validate_transfer_paths(kind: TransferKind, source: &str, target: &str) -> FsResult<()> {
    let source_path = Path::from_str(source)
        .map_err(|_| FsError::common(format!("Invalid transfer source path: {source}")))?;
    let target_path = Path::from_str(target)
        .map_err(|_| FsError::common(format!("Invalid transfer target path: {target}")))?;
    reject_relative_segments(&source_path)?;
    reject_relative_segments(&target_path)?;
    match kind {
        TransferKind::Load if !source_path.is_cv() && target_path.is_cv() => Ok(()),
        TransferKind::Export if source_path.is_cv() && !target_path.is_cv() => Ok(()),
        TransferKind::Load => Err(FsError::common(format!(
            "Invalid Load direction: source={}, target={}",
            source, target
        ))),
        TransferKind::Export => Err(FsError::common(format!(
            "Invalid Export direction: source={}, target={}",
            source, target
        ))),
    }
}

fn reject_relative_segments(path: &Path) -> FsResult<()> {
    if path
        .path()
        .split('/')
        .any(|part| matches!(part, "." | ".."))
    {
        return Err(FsError::common(format!(
            "Invalid transfer path contains relative segment: {}",
            path.full_path()
        )));
    }
    Ok(())
}

fn normalized_transfer_path(path: &Path) -> String {
    let text = if path.is_cv() {
        path.path().to_string()
    } else {
        path.full_path().to_string()
    };
    let trimmed = text.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn transfer_task_state(state: i32) -> FsResult<TransferTaskState> {
    match state {
        1 => Ok(TransferTaskState::Pending),
        2 => Ok(TransferTaskState::Running),
        3 => Ok(TransferTaskState::Completed),
        4 => Ok(TransferTaskState::Failed),
        5 => Ok(TransferTaskState::Canceled),
        6 => Ok(TransferTaskState::Stale),
        _ => Err(FsError::common(format!(
            "Invalid transfer task state {}",
            state
        ))),
    }
}

fn progress_from_proto(value: TransferProgressProto) -> TransferProgress {
    TransferProgress {
        loaded_size: value.loaded_size,
        total_size: value.total_size,
        update_time: value.update_time,
        message: value.message,
    }
}

fn block_locations_from_proto(
    locations: &[curvine_proto::TaskBlockLocationProto],
) -> Vec<TaskBlockLocation> {
    locations
        .iter()
        .map(|loc| TaskBlockLocation {
            block_id: loc.block_id,
            worker_ids: loc.worker_ids.clone(),
        })
        .collect()
}

pub fn progress_to_proto(value: TransferProgress) -> TransferProgressProto {
    TransferProgressProto {
        loaded_size: value.loaded_size,
        total_size: value.total_size,
        update_time: value.update_time,
        message: value.message,
    }
}

pub fn task_summary_to_proto(value: TransferTaskCounts) -> TransferTaskSummaryProto {
    TransferTaskSummaryProto {
        pending: value.pending,
        running: value.running,
        completed: value.completed,
        failed: value.failed,
        canceled: value.canceled,
        stale: value.stale,
        completed_size: value.completed_size,
    }
}

fn snapshot_has_tasks(summary: &TransferProgress, counts: &TransferTaskCounts) -> bool {
    summary.total_size > 0
        || counts.pending
            + counts.running
            + counts.completed
            + counts.failed
            + counts.canceled
            + counts.stale
            > 0
}

pub(crate) fn resolve_terminal_summary(
    job: &TransferJobRecord,
    live: &curvine_model::TransferTaskSummary,
) -> (TransferProgress, TransferTaskCounts, bool) {
    if !job.state.is_terminal() || live.has_task {
        return (live.progress.clone(), live.counts, false);
    }
    let job_summary_has_progress = job.summary.loaded_size > 0
        || job.summary.total_size > 0
        || !job.summary.message.trim().is_empty();
    let (Some(snapshot), Some(counts)) = (job.terminal_summary.clone(), job.terminal_counts) else {
        // Terminal with no rows and no snapshot means details are gone. Keep
        // the persisted job-level progress message when one is available so
        // callers still receive the terminal failure reason.
        if job_summary_has_progress {
            return (job.summary.clone(), live.counts, true);
        }
        return (live.progress.clone(), live.counts, true);
    };
    if snapshot_has_tasks(&snapshot, &counts) {
        let mut snapshot = snapshot;
        if snapshot.message.trim().is_empty() && !job.summary.message.trim().is_empty() {
            snapshot.message = job.summary.message.clone();
        }
        return (snapshot, counts, true);
    }
    if !snapshot.message.trim().is_empty() {
        return (snapshot, counts, false);
    }
    if job_summary_has_progress {
        return (job.summary.clone(), counts, false);
    }
    (live.progress.clone(), live.counts, false)
}

pub fn task_status_to_proto(task: TransferTaskStatus) -> TransferTaskStatusProto {
    TransferTaskStatusProto {
        task_id: task.task_id,
        attempt_id: task.attempt_id,
        source_path: task.source_path,
        target_path: task.target_path,
        worker_id: task.worker_id,
        worker_session_id: task.worker_session_id,
        state: task_state_code(task.state),
        progress: progress_to_proto(task.progress),
        retry_count: task.retry_count,
        updated_at: task.updated_at,
    }
}

pub fn job_to_proto(job: TransferJobRecord) -> TransferJobStatusProto {
    TransferJobStatusProto {
        job_id: job.job_id,
        run_id: job.run_id,
        kind: job.kind as i32,
        state: job.state as i32,
        source_path: job.source_path,
        target_path: job.target_path,
        progress: progress_to_proto(job.summary),
        submitter: job.submitter,
        tenant: job.tenant,
        created_at: job.created_at,
        updated_at: job.updated_at,
        owner: Some(job.owner),
        lease_epoch: Some(job.lease_epoch),
        lease_expire_at: Some(job.lease_expire_at),
        cv_metadata_epoch: job.cv_metadata_epoch,
    }
}

fn task_state_code(state: TransferTaskState) -> i32 {
    match state {
        TransferTaskState::Pending => TransferTaskStateProto::TransferTaskPending as i32,
        TransferTaskState::Running => TransferTaskStateProto::TransferTaskRunning as i32,
        TransferTaskState::Completed => TransferTaskStateProto::TransferTaskCompleted as i32,
        TransferTaskState::Failed => TransferTaskStateProto::TransferTaskFailed as i32,
        TransferTaskState::Canceled => TransferTaskStateProto::TransferTaskCanceled as i32,
        TransferTaskState::Stale => TransferTaskStateProto::TransferTaskStale as i32,
    }
}

fn parse_page_token(page_token: Option<String>) -> FsResult<usize> {
    match page_token.filter(|token| !token.is_empty()) {
        Some(token) => token.parse::<usize>().map_err(|_| {
            FsError::common("Invalid transfer page token; use a non-negative integer")
        }),
        None => Ok(0),
    }
}

fn now_ms() -> i64 {
    i64::try_from(LocalTime::mills()).unwrap_or(i64::MAX)
}

/// Only Completed/PartialSuccess Load jobs are auto-repaired; Failed/Canceled need explicit retry.
pub(crate) fn auto_repair_eligible(kind: TransferKind, state: TransferState) -> bool {
    kind == TransferKind::Load
        && matches!(
            state,
            TransferState::Completed | TransferState::PartialSuccess
        )
}

pub(crate) fn auto_repair_in_cooldown(last_attempt_ms: i64, now_ms: i64) -> bool {
    now_ms.saturating_sub(last_attempt_ms) < AUTO_REPAIR_COOLDOWN_MS
}

/// True when a CV cache path left by Load is not usable and should be reloaded.
///
/// Treat as invalid when the file is incomplete or `cv_valid(None)` fails
/// (e.g. `ufs_mtime==0`, no CV copy / `StorageState::Ufs`, expired).
pub(crate) fn cv_cache_target_needs_repair(status: &FileStatus) -> bool {
    if status.is_dir {
        return false;
    }
    !status.is_complete() || !status.cv_valid(None)
}

pub fn encode_transfer_command(command: &TransferCommand) -> FsResult<Vec<u8>> {
    Ok(SerdeUtils::serialize_json(command)?)
}

fn decode_transfer_command(bytes: &[u8]) -> FsResult<TransferCommand> {
    serde_json::from_slice(bytes).map_err(|_| FsError::common("Invalid transfer command payload"))
}

#[cfg(test)]
mod tests {
    use super::JobSummaryDirtySet;

    #[test]
    fn dirty_set_keeps_job_when_progress_is_followed_by_terminal_mark() {
        let dirty = JobSummaryDirtySet::new();
        dirty.mark("job-a".to_string(), 1);
        dirty.mark("job-a".to_string(), 1);
        dirty.mark("job-b".to_string(), 1);
        let mut jobs = dirty.take_all();
        jobs.sort_by(|left, right| left.job_id.cmp(&right.job_id));
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, "job-a");
        assert_eq!(jobs[1].job_id, "job-b");
        assert!(dirty.take_all().is_empty());
    }
}
