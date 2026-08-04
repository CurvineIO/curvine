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
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{
    summarize_transfer_tasks, TransferCommand, TransferJobRecord, TransferKind, TransferListFilter,
    TransferProgress, TransferState, TransferTaskCounts, TransferTaskRecord, TransferTaskReport,
    TransferTaskState,
};
use curvine_proto::{
    ListTransferTenantsRequest, ListTransferTenantsResponse, ListTransfersRequest,
    ListTransfersResponse, SubmitTransferRequest, TransferJobStatusProto, TransferKindProto,
    TransferProgressProto, TransferTaskReportRequest, TransferTaskStateProto,
    TransferTaskStatusProto, TransferTaskSummaryProto, TransferTenantSummaryProto,
};
use curvine_runtime::common::LocalTime;
use curvine_runtime::common::SerdeUtils;
use std::time::Duration;
use uuid::Uuid;

use crate::transfer::{ClusterMetadataCache, TransferMetrics, TransferStore};

const PROGRESS_REPORT_COALESCE_BATCH: usize = 256;

const TRANSFER_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_LIST_PAGE_SIZE: usize = 20;
const MAX_LIST_PAGE_SIZE: usize = 1000;

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
    cache: Option<ClusterMetadataCache>,
    task_stale_timeout_ms: i64,
    report_dispatcher: Option<TransferReportDispatcher<S>>,
}

impl<S> Clone for TransferService<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            cache: self.cache.clone(),
            task_stale_timeout_ms: self.task_stale_timeout_ms,
            report_dispatcher: self.report_dispatcher.clone(),
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
        }
    }

    pub fn with_task_stale_timeout(store: Arc<S>, task_stale_timeout: Duration) -> Self {
        Self {
            store,
            cache: None,
            task_stale_timeout_ms: i64::try_from(task_stale_timeout.as_millis())
                .unwrap_or(i64::MAX),
            report_dispatcher: None,
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
        })
    }

    pub fn submit_transfer(&self, req: SubmitTransferRequest) -> FsResult<TransferJobRecord> {
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
        validate_transfer_paths(command.kind, &command.source_path, &command.target_path)?;
        command.target_path = normalized_transfer_path(&Path::from_str(&command.target_path)?);
        let snapshot = self.transfer_snapshot(&command)?;
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
        record_submit_metric(kind, "accepted");
        log::info!(
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
        Ok(job)
    }

    pub fn check_store_available(&self) -> FsResult<()> {
        self.store.check_available()
    }

    fn transfer_snapshot(&self, command: &TransferCommand) -> FsResult<TransferSubmitSnapshot> {
        let Some(cache) = &self.cache else {
            return Ok(TransferSubmitSnapshot::default());
        };
        let source = Path::from_str(&command.source_path)?;
        let target = Path::from_str(&command.target_path)?;
        let snapshot = cache.find_mount_with_refresh(command.kind, &source, &target)?;
        let mount_snapshot_json = serde_json::to_string(&snapshot.mount).map_err(|_| {
            FsError::common(format!(
                "Unable to save the transfer mount snapshot for {} -> {}",
                command.source_path, command.target_path
            ))
        })?;
        Ok(TransferSubmitSnapshot {
            mount_snapshot_json,
            cluster_snapshot_version: snapshot.version,
        })
    }

    pub fn get_transfer(&self, job_id: &str) -> FsResult<TransferJobRecord> {
        self.store
            .get_transfer(job_id)?
            .ok_or_else(|| FsError::job_not_found(job_id))
    }

    pub fn retry_transfer(&self, job_id: &str) -> FsResult<TransferJobRecord> {
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
        self.submit_transfer(SubmitTransferRequest {
            kind: command.kind as i32,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: encode_transfer_command(&command)?,
            protocol_version: Some(TRANSFER_PROTOCOL_VERSION),
        })
    }

    pub fn get_transfer_status(
        &self,
        job_id: &str,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<TransferStatusPage> {
        let job = self.get_transfer(job_id)?;
        let tasks = self.store.list_transfer_tasks(&job.job_id, job.run_id)?;
        let counts = summarize_transfer_tasks(&tasks, now_ms()).counts;
        let (tasks, next_page_token) = self.page_tasks(tasks, page_size, page_token)?;
        Ok((job, counts, tasks, next_page_token))
    }

    pub fn list_transfers(&self, req: ListTransfersRequest) -> FsResult<ListTransfersResponse> {
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
            now_ms,
            stale_deadline_at: now_ms.saturating_add(self.task_stale_timeout_ms),
        };
        self.store.update_task_report(report)
    }

    fn page_tasks(
        &self,
        mut tasks: Vec<TransferTaskRecord>,
        page_size: Option<u32>,
        page_token: Option<String>,
    ) -> FsResult<(Vec<TransferTaskStatusProto>, Option<String>)> {
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        let offset = parse_page_token(page_token)?;
        let page_size = page_size.unwrap_or(0) as usize;
        if page_size == 0 {
            return Ok((Vec::new(), None));
        }
        let page = tasks
            .iter()
            .skip(offset)
            .take(page_size)
            .cloned()
            .map(task_to_proto)
            .collect::<Vec<_>>();
        let next = offset.saturating_add(page.len());
        let next_page_token = if next < tasks.len() {
            Some(next.to_string())
        } else {
            None
        };
        Ok((page, next_page_token))
    }
}

struct TransferReportEnvelope {
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
            now_ms,
            stale_deadline_at: now_ms.saturating_add(task_stale_timeout_ms),
        };
        let result = store.update_task_report(report);
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

struct TransferSubmitSnapshot {
    mount_snapshot_json: String,
    cluster_snapshot_version: u64,
}

impl Default for TransferSubmitSnapshot {
    fn default() -> Self {
        Self {
            mount_snapshot_json: "{}".to_string(),
            cluster_snapshot_version: 0,
        }
    }
}

fn validate_protocol_version(protocol_version: Option<u32>) -> FsResult<()> {
    let protocol_version = protocol_version.unwrap_or(TRANSFER_PROTOCOL_VERSION);
    if protocol_version != TRANSFER_PROTOCOL_VERSION {
        return Err(FsError::common(format!(
            "Unsupported transfer protocol version {}, supported {}",
            protocol_version, TRANSFER_PROTOCOL_VERSION
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

pub fn task_to_proto(task: TransferTaskRecord) -> TransferTaskStatusProto {
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

pub fn encode_transfer_command(command: &TransferCommand) -> FsResult<Vec<u8>> {
    Ok(SerdeUtils::serialize_json(command)?)
}

fn decode_transfer_command(bytes: &[u8]) -> FsResult<TransferCommand> {
    serde_json::from_slice(bytes).map_err(|_| FsError::common("Invalid transfer command payload"))
}
