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

use std::collections::HashMap;
use std::sync::{mpsc, Arc};

use crossbeam::channel;
use curvine_config::DEFAULT_ENABLED_TRANSFER_KINDS;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{
    summarize_transfer_tasks, FileStatus, TransferCommand, TransferJobRecord, TransferKind,
    TransferListFilter, TransferProgress, TransferState, TransferTaskCounts, TransferTaskRecord,
    TransferTaskReport, TransferTaskState,
};
use curvine_proto::{
    ListTransferTenantsRequest, ListTransferTenantsResponse, ListTransfersRequest,
    ListTransfersResponse, SubmitTransferRequest, TransferJobStatusProto, TransferProgressProto,
    TransferTaskReportRequest, TransferTaskStateProto, TransferTaskStatusProto,
    TransferTaskSummaryProto, TransferTenantSummaryProto,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::common::SerdeUtils;
use log::{info, warn};
use parking_lot::Mutex;
use prost::Message;
use std::time::Duration;
use uuid::Uuid;

use super::task_result::StoredTaskResult;
use crate::transfer::{ClusterMetadataCache, PlannerRegistry, TransferMetrics, TransferStore};

const PROGRESS_REPORT_COALESCE_BATCH: usize = 256;

const TRANSFER_PROTOCOL_VERSION: u32 = 1;
const TRANSFER_FILES_PROTOCOL_VERSION: u32 = 2;
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
);
type TransferWatchPage = (
    TransferJobRecord,
    TransferTaskCounts,
    Vec<TransferTaskStatusProto>,
    Option<String>,
    bool,
);

pub struct TransferService<S> {
    store: Arc<S>,
    enabled_kinds: Arc<[TransferKind]>,
    cache: Option<ClusterMetadataCache>,
    planners: Arc<PlannerRegistry>,
    task_stale_timeout_ms: i64,
    report_dispatcher: Option<TransferReportDispatcher<S>>,
    /// job_key -> last auto-repair attempt time (ms).
    /// Cooldown is per-process / per-replica, not shared across transfer instances.
    auto_repair_last_attempt_ms: Arc<Mutex<HashMap<String, i64>>>,
}

impl<S> Clone for TransferService<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            enabled_kinds: self.enabled_kinds.clone(),
            cache: self.cache.clone(),
            planners: self.planners.clone(),
            task_stale_timeout_ms: self.task_stale_timeout_ms,
            report_dispatcher: self.report_dispatcher.clone(),
            auto_repair_last_attempt_ms: self.auto_repair_last_attempt_ms.clone(),
        }
    }
}

impl<S> TransferService<S>
where
    S: TransferStore,
{
    pub fn new(store: Arc<S>) -> Self {
        Self::with_task_stale_timeout(store, Duration::from_secs(60))
    }

    pub fn with_task_stale_timeout(store: Arc<S>, task_stale_timeout: Duration) -> Self {
        Self {
            store,
            enabled_kinds: DEFAULT_ENABLED_TRANSFER_KINDS.into(),
            cache: None,
            planners: PlannerRegistry::with_load_export(),
            task_stale_timeout_ms: i64::try_from(task_stale_timeout.as_millis())
                .unwrap_or(i64::MAX),
            report_dispatcher: None,
            auto_repair_last_attempt_ms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_report_queue(
        store: Arc<S>,
        task_stale_timeout: Duration,
        task_report_queue_size: usize,
        task_report_workers: usize,
    ) -> FsResult<Self> {
        let mut service = Self::with_task_stale_timeout(store.clone(), task_stale_timeout);
        service.report_dispatcher = Some(TransferReportDispatcher::new(
            store,
            service.task_stale_timeout_ms,
            task_report_queue_size,
            task_report_workers,
        )?);
        Ok(service)
    }

    pub fn with_cache(
        store: Arc<S>,
        cache: ClusterMetadataCache,
        task_stale_timeout: Duration,
    ) -> Self {
        Self {
            cache: Some(cache),
            ..Self::with_task_stale_timeout(store, task_stale_timeout)
        }
    }

    pub fn with_cache_and_report_queue(
        store: Arc<S>,
        cache: ClusterMetadataCache,
        task_stale_timeout: Duration,
        task_report_queue_size: usize,
        task_report_workers: usize,
    ) -> FsResult<Self> {
        Ok(Self {
            cache: Some(cache),
            ..Self::with_report_queue(
                store,
                task_stale_timeout,
                task_report_queue_size,
                task_report_workers,
            )?
        })
    }

    pub(super) fn with_enabled_kinds(mut self, kinds: Vec<TransferKind>) -> Self {
        self.enabled_kinds = kinds.into();
        self
    }

    pub fn with_planner_registry(mut self, planners: Arc<PlannerRegistry>) -> Self {
        self.planners = planners;
        self
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
        if !self.enabled_kinds.contains(&kind) {
            record_submit_metric(kind, "disabled");
            return Err(FsError::common(format!(
                "Transfer kind {} is disabled by transfer.enabled_kinds",
                kind.as_str()
            )));
        }
        let planner = self.planners.get(kind)?;
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
            files: Vec::new(),
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
        planner.validate_options(&command)?;
        let (source, target) = parse_transfer_paths(&command.source_path, &command.target_path)?;
        planner.validate_direction(&command, &source, &target)?;
        command.target_path = normalized_transfer_path(&target);
        let snapshot = match &self.cache {
            Some(cache) => planner
                .submit_mount_snapshot(&command, cache)?
                .map(|mount| TransferSubmitSnapshot::from_mount(mount, &command))
                .transpose()?
                .unwrap_or_default(),
            None => TransferSubmitSnapshot::default(),
        };
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
            spec_version: 0,
            spec_blob: Vec::new(),
            mount_snapshot_json: snapshot.mount_snapshot_json,
            secret_ref_json: "{}".to_string(),
            cluster_snapshot_version: snapshot.cluster_snapshot_version,
            cv_metadata_epoch: None,
            state: TransferState::Pending,
            owner: String::new(),
            lease_epoch: 0,
            lease_expire_at: 0,
            cancel_requested: false,
            summary: TransferProgress::default(),
            client_request_id: command.client_request_id,
            submitter: command.submitter,
            tenant: command.tenant,
            created_at: now_ms,
            updated_at: now_ms,
        };
        let job = self.store.create_or_get_by_request_id_checked(job)?;
        if allow_auto_repair && planner.auto_repair_eligible(job.kind, job.state) {
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
            let last_attempts = self.auto_repair_last_attempt_ms.lock();
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
            let mut last_attempts = self.auto_repair_last_attempt_ms.lock();
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

    pub fn check_store_available(&self) -> FsResult<()> {
        self.store.check_available()
    }

    pub fn default_target_for_source(&self, kind: TransferKind, source: &str) -> FsResult<String> {
        let cache = self.cache.as_ref().ok_or_else(|| {
            FsError::common("Cluster metadata is unavailable; target_path is required")
        })?;
        cache.target_path_for_source(kind, &Path::from_str(source)?)
    }

    pub fn get_transfer_by_request(
        &self,
        submitter: &str,
        client_request_id: &str,
    ) -> FsResult<Option<TransferJobRecord>> {
        self.store
            .get_transfer_by_request(submitter, client_request_id)
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
                protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
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
        let job = self.get_transfer(job_id)?;
        let progress = self.store.list_task_progress(&job.job_id, job.run_id)?;
        let counts = summarize_transfer_tasks(&progress, now_ms()).counts;
        let total = progress.len();
        drop(progress);
        let (tasks, next_page_token) =
            self.page_tasks(&job, &counts, total, page_size, page_token)?;
        Ok((job, counts, tasks, next_page_token))
    }

    /// One page of transfer jobs, before the proto conversion.
    fn list_transfer_records(
        &self,
        req: ListTransfersRequest,
    ) -> FsResult<(Vec<TransferJobRecord>, Option<String>)> {
        let page_size = bounded_page_size(req.page_size);
        let offset = parse_page_token(req.page_token)?;
        let filter = TransferListFilter {
            kind: req.kind.map(transfer_kind).transpose()?,
            state: req.state.map(transfer_state).transpose()?,
            submitter: non_empty_filter(req.submitter),
            tenant: non_empty_filter(req.tenant),
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
        Ok((jobs, next_page_token))
    }

    pub fn list_transfers(&self, req: ListTransfersRequest) -> FsResult<ListTransfersResponse> {
        let (jobs, next_page_token) = self.list_transfer_records(req)?;
        Ok(ListTransfersResponse {
            jobs: jobs.into_iter().map(job_to_proto).collect(),
            next_page_token,
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
        let (job, counts, tasks, next_page_token) =
            self.get_transfer_status(job_id, page_size, page_token)?;
        let changed = since_updated_at
            .map(|since| job.updated_at as u64 > since)
            .unwrap_or(true);
        Ok((job, counts, tasks, next_page_token, changed))
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
        validate_report_result(&req)?;
        let state = transfer_task_state(req.state)?;
        if let Some(dispatcher) = &self.report_dispatcher {
            return dispatcher.report_task(req, state);
        }
        let now_ms = now_ms();
        let report = TransferTaskReport {
            job_id: req.job_id,
            run_id: req.run_id,
            task_id: req.task_id,
            attempt_id: req.attempt_id,
            worker_id: req.worker_id,
            worker_session_id: req.worker_session_id,
            state,
            progress: progress_from_proto(req.progress),
            result_inline: req.result_inline,
            result_ref: req.result_ref,
            result_size: req.result_size,
            now_ms,
            stale_deadline_at: now_ms.saturating_add(self.task_stale_timeout_ms),
        };
        self.store.update_task_report(report)
    }

    fn page_tasks(
        &self,
        job: &TransferJobRecord,
        counts: &TransferTaskCounts,
        total: usize,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<(Vec<TransferTaskStatusProto>, Option<String>)> {
        let offset = parse_page_token(page_token)?;
        let page_size = (page_size.unwrap_or(0) as usize).min(MAX_LIST_PAGE_SIZE);
        if page_size == 0 || offset >= total {
            return Ok((Vec::new(), None));
        }
        // Reserve ample space for RPC framing, tags, continuation and Watch fields.
        const MAX_STATUS_BYTES: usize = curvine_rpc::message::MAX_FRAME_SIZE as usize - 4096;
        let mut bytes =
            job_to_proto(job.clone()).encoded_len() + task_summary_to_proto(*counts).encoded_len();
        let mut page = Vec::new();
        'page: while page.len() < page_size {
            let next = offset + page.len();
            if next >= total {
                break;
            }
            // Read in small batches instead of materializing a count-sized page of blobs.
            let limit = (page_size - page.len()).min(16);
            let rows = self
                .store
                .list_task_page(&job.job_id, job.run_id, next, limit)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let task = task_to_proto(row)?;
                let size = task.encoded_len().saturating_add(16);
                if bytes.saturating_add(size) > MAX_STATUS_BYTES {
                    if page.is_empty() {
                        return Err(FsError::common(
                            "Single transfer task status exceeds RPC response budget",
                        ));
                    }
                    break 'page;
                }
                bytes += size;
                page.push(task);
            }
        }
        let next = offset + page.len();
        let token = (!page.is_empty() && next < total).then(|| next.to_string());
        Ok((page, token))
    }
}

struct TransferReportEnvelope {
    queued_at: std::time::Instant,
    req: TransferTaskReportRequest,
    state: TransferTaskState,
    reply: Option<mpsc::Sender<FsResult<bool>>>,
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
        Self::spawn_report_workers(
            "progress",
            progress_receiver,
            store.clone(),
            task_stale_timeout_ms,
            worker_count,
        )?;
        Self::spawn_report_workers(
            "terminal",
            terminal_receiver,
            store.clone(),
            task_stale_timeout_ms,
            worker_count,
        )?;
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_report_queue_len_by_lane(0, 0);
        }
        Ok(Self {
            progress_sender,
            terminal_sender,
            _store: store,
        })
    }

    fn spawn_report_workers(
        lane: &'static str,
        receiver: channel::Receiver<TransferReportEnvelope>,
        store: Arc<S>,
        task_stale_timeout_ms: i64,
        worker_count: usize,
    ) -> FsResult<()> {
        for index in 0..worker_count {
            let receiver = receiver.clone();
            let store = store.clone();
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
                            for envelope in reports.into_values() {
                                Self::handle_report_envelope(
                                    &store,
                                    task_stale_timeout_ms,
                                    envelope,
                                );
                            }
                        } else {
                            Self::handle_report_envelope(&store, task_stale_timeout_ms, envelope);
                        }
                    }
                })
                .map_err(|_| FsError::common("Unable to start the transfer report processor"))?;
        }
        Ok(())
    }

    fn handle_report_envelope(
        store: &S,
        task_stale_timeout_ms: i64,
        envelope: TransferReportEnvelope,
    ) {
        let now_ms = now_ms();
        let state = envelope.state;
        let report = TransferTaskReport {
            job_id: envelope.req.job_id,
            run_id: envelope.req.run_id,
            task_id: envelope.req.task_id,
            attempt_id: envelope.req.attempt_id,
            worker_id: envelope.req.worker_id,
            worker_session_id: envelope.req.worker_session_id,
            state,
            progress: progress_from_proto(envelope.req.progress),
            result_inline: envelope.req.result_inline,
            result_ref: envelope.req.result_ref,
            result_size: envelope.req.result_size,
            now_ms,
            stale_deadline_at: now_ms.saturating_add(task_stale_timeout_ms),
        };
        let queue_ms = envelope.queued_at.elapsed().as_millis();
        let store_started = std::time::Instant::now();
        let result = store.update_task_report(report);
        log::debug!(
            "transfer report processed: state={:?} queue_ms={} store_ms={}",
            state,
            queue_ms,
            store_started.elapsed().as_millis()
        );
        match envelope.reply {
            Some(reply) => {
                let _ = reply.send(result);
            }
            None => match result {
                Ok(true) => record_report_metric("accepted_async"),
                Ok(false) => record_report_metric("rejected_async"),
                Err(err) => {
                    record_report_metric("failure_async");
                    log::warn!(
                        "async transfer task report update failed: state={:?}, err={}",
                        state,
                        err
                    );
                }
            },
        }
    }

    fn report_task(
        &self,
        req: TransferTaskReportRequest,
        state: TransferTaskState,
    ) -> FsResult<bool> {
        // Result-bearing reports are never coalesced or acknowledged before
        // the store checks the attempt and commits the result.
        let wait_store_result = state.is_terminal()
            || req.result_inline.is_some()
            || req.result_ref.is_some()
            || req.result_size.is_some();
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
                queued_at: std::time::Instant::now(),
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
                    FsError::transfer_overloaded(format!(
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
                FsError::transfer_overloaded(
                    "Transfer task report was not processed in time; retry shortly",
                )
            })??;
        record_report_metric(if accepted { "accepted" } else { "rejected" });
        Ok(accepted)
    }
}

fn record_submit_metric(kind: TransferKind, result: &str) {
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.inc_submit(kind.as_str(), result);
    }
}

fn record_report_metric(result: &str) {
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.inc_report(result);
    }
}

struct TransferSubmitSnapshot {
    mount_snapshot_json: String,
    cluster_snapshot_version: u64,
}

impl TransferSubmitSnapshot {
    fn from_mount(snapshot: super::MountSnapshot, command: &TransferCommand) -> FsResult<Self> {
        let mount_snapshot_json = serde_json::to_string(&snapshot.mount).map_err(|_| {
            FsError::common(format!(
                "Unable to save the transfer mount snapshot for {} -> {}",
                command.source_path, command.target_path
            ))
        })?;
        Ok(Self {
            mount_snapshot_json,
            cluster_snapshot_version: snapshot.version,
        })
    }
}

impl Default for TransferSubmitSnapshot {
    fn default() -> Self {
        Self {
            mount_snapshot_json: "{}".to_string(),
            cluster_snapshot_version: 0,
        }
    }
}

fn validate_report_result(req: &TransferTaskReportRequest) -> FsResult<()> {
    validate_result_fields(
        req.result_inline.as_deref(),
        req.result_ref.as_deref(),
        req.result_size,
    )
}

pub(super) fn validate_result_fields(
    inline: Option<&[u8]>,
    reference: Option<&str>,
    size: Option<u64>,
) -> FsResult<()> {
    const MAX_INLINE_BYTES: usize = curvine_config::JobConf::MAX_INLINE_RESULT_BYTES;
    match (inline, reference, size) {
        (None, None, None) => Ok(()),
        (Some(bytes), None, Some(size))
            if bytes.len() <= MAX_INLINE_BYTES && size == bytes.len() as u64 =>
        {
            Ok(())
        }
        (None, Some(reference), _) if !reference.trim().is_empty() => Ok(()),
        _ => Err(FsError::invalid_argument(
            "Invalid transfer task result fields or inline result exceeds server limit",
        )),
    }
}

#[test]
fn invalid_report_result_fields_are_rejected() {
    let mut req = TransferTaskReportRequest::default();
    assert!(validate_report_result(&req).is_ok());
    req.result_inline = Some(vec![1]);
    req.result_size = Some(2);
    assert!(validate_report_result(&req).is_err());
    req.result_size = Some(1);
    assert!(validate_report_result(&req).is_ok());
    req.result_ref = Some("example".into());
    assert!(validate_report_result(&req).is_err());
    req.result_ref = None;
    let size = curvine_config::JobConf::DEFAULT_WORKER_MAX_INLINE_RESULT_BYTES + 1;
    req.result_inline = Some(vec![0; size]);
    req.result_size = Some(size as u64);
    assert!(validate_report_result(&req).is_err());
    req.result_inline = None;
    req.result_ref = Some(" ".into());
    assert!(validate_report_result(&req).is_err());
}

fn validate_protocol_version(protocol_version: Option<u32>) -> FsResult<()> {
    let protocol_version = protocol_version.unwrap_or(TRANSFER_PROTOCOL_VERSION);
    if !(TRANSFER_PROTOCOL_VERSION..=TRANSFER_FILES_PROTOCOL_VERSION).contains(&protocol_version) {
        return Err(FsError::common(format!(
            "Unsupported transfer protocol version {}, supported {}..={}",
            protocol_version, TRANSFER_PROTOCOL_VERSION, TRANSFER_FILES_PROTOCOL_VERSION
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
    TransferKind::try_from(kind)
        .map_err(|_| FsError::common(format!("Invalid transfer kind {}", kind)))
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

fn parse_transfer_paths(source: &str, target: &str) -> FsResult<(Path, Path)> {
    let source_path = Path::from_str(source)
        .map_err(|_| FsError::common(format!("Invalid transfer source path: {source}")))?;
    let target_path = Path::from_str(target)
        .map_err(|_| FsError::common(format!("Invalid transfer target path: {target}")))?;
    reject_relative_segments(&source_path)?;
    reject_relative_segments(&target_path)?;
    Ok((source_path, target_path))
}

pub(crate) fn reject_relative_segments(path: &Path) -> FsResult<()> {
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
        total_files: value.total_files.unwrap_or_default(),
        loaded_files: value.loaded_files.unwrap_or_default(),
        skipped_files: value.skipped_files.unwrap_or_default(),
        skipped_size: value.skipped_size.unwrap_or_default(),
        update_time: value.update_time,
        message: value.message,
    }
}

pub fn progress_to_proto(value: TransferProgress) -> TransferProgressProto {
    TransferProgressProto {
        loaded_size: value.loaded_size,
        total_size: value.total_size,
        update_time: value.update_time,
        message: value.message,
        total_files: Some(value.total_files),
        loaded_files: Some(value.loaded_files),
        skipped_files: Some(value.skipped_files),
        skipped_size: Some(value.skipped_size),
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

fn task_to_proto(task: TransferTaskRecord) -> FsResult<TransferTaskStatusProto> {
    let mut status = TransferTaskStatusProto {
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
        result_inline: None,
        result_ref: None,
        result_size: None,
    };
    StoredTaskResult::populate_status(&task.result_json, &mut status)?;
    Ok(status)
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

#[cfg(test)]
pub(crate) fn auto_repair_eligible(kind: TransferKind, state: TransferState) -> bool {
    use super::{LoadExportPlanner, TransferPlannerPlugin};
    LoadExportPlanner.auto_repair_eligible(kind, state)
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
