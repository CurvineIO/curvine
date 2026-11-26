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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_data_transfer::transfer::EmbeddedTransfer;
use curvine_error::{ErrorKind, FsResult};
use curvine_fault::FaultHttpControl;
use curvine_model::{
    FileBlocks, FileStatus, TransferCommand, TransferJobRecord, TransferKind, TransferState,
    WorkerInfo,
};
use curvine_proto::{
    GetTransferStatusResponse, ListTransfersRequest, SubmitTransferRequest, TransferJobStatusProto,
    TransferKindProto, TransferProgressProto, TransferStateProto,
};
use curvine_raft::raft::storage::RocksLogStorage;
use curvine_runtime::common::LocalTime;
use curvine_transfer_client::TransferClient;
use curvine_web::router::RouterHandler;
use uuid::Uuid;

use crate::master::fs::{MasterFilesystem, SessionSummary};
use crate::master::MasterMetrics;

const FREE_JOB_LIMIT: usize = 200;

#[derive(Clone, Debug, Serialize)]
struct FreeJobRecord {
    job_id: String,
    path: String,
    recursive: bool,
    state: String,
    freed_inodes: i64,
    freed_bytes: i64,
    message: String,
    created_at: i64,
    updated_at: i64,
}

#[derive(Default)]
struct FreeJobStore {
    jobs: VecDeque<FreeJobRecord>,
}

impl FreeJobStore {
    fn insert(&mut self, job: FreeJobRecord) {
        if self.jobs.len() >= FREE_JOB_LIMIT {
            self.jobs.pop_front();
        }
        self.jobs.push_back(job);
    }

    fn update(&mut self, job_id: &str, state: &str, inodes: i64, bytes: i64, message: String) {
        if let Some(job) = self.jobs.iter_mut().find(|job| job.job_id == job_id) {
            job.state = state.to_string();
            job.freed_inodes = inodes;
            job.freed_bytes = bytes;
            job.message = message;
            job.updated_at = LocalTime::mills() as i64;
        }
    }

    fn get(&self, job_id: &str) -> Option<FreeJobRecord> {
        self.jobs.iter().find(|job| job.job_id == job_id).cloned()
    }

    fn list(&self, limit: usize) -> Vec<FreeJobRecord> {
        self.jobs.iter().rev().take(limit).cloned().collect()
    }
}

#[derive(Clone)]
pub struct MasterRouterHandler {
    fs: MasterFilesystem,
    conf: ClusterConf,
    start_time: String,
    /// The same moment as `start_time` in milliseconds since the Unix epoch.
    /// `start_time` is local wall-clock time without a zone, so the Web UI
    /// computes uptime from this instead.
    start_time_ms: i64,
    metrics: &'static MasterMetrics,
    fault_http: FaultHttpControl,
    journal_store: RocksLogStorage,
    /// The in-process transfer service, present when `transfer.endpoints` is
    /// empty. `None` means a standalone `curvine-transfer` owns the jobs; the
    /// client below then proxies reads to it.
    transfer: Option<Arc<EmbeddedTransfer>>,
    /// RPC client for a standalone transfer service. This is an implementation
    /// detail: the HTTP surface is the same in both modes.
    transfer_client: Option<TransferClient>,
    free_jobs: Arc<Mutex<FreeJobStore>>,
    /// Serializes the rendering of the metrics text output, so that concurrent
    /// scrapes never interleave partially observed values.
    render_lock: Arc<Mutex<()>>,
}

impl MasterRouterHandler {
    pub(crate) fn new(
        conf: ClusterConf,
        fs: MasterFilesystem,
        metrics: &'static MasterMetrics,
        fault_http: FaultHttpControl,
        journal_store: RocksLogStorage,
        transfer: Option<Arc<EmbeddedTransfer>>,
        transfer_client: Option<TransferClient>,
    ) -> Self {
        Self {
            fs,
            conf,
            start_time: LocalTime::now_datetime(),
            start_time_ms: LocalTime::mills() as i64,
            metrics,
            fault_http,
            journal_store,
            transfer,
            transfer_client,
            free_jobs: Arc::new(Mutex::new(FreeJobStore::default())),
            render_lock: Arc::new(Mutex::new(())),
        }
    }

    fn text_output(&self) -> FsResult<String> {
        let _guard = self.render_lock.lock().unwrap_or_else(|e| e.into_inner());
        Ok(self
            .metrics
            .text_output(self.fs.clone(), Some(&self.journal_store))?)
    }
}

fn get_report(instance: &MasterRouterHandler) -> FsResult<HashMap<String, String>> {
    let output = instance.text_output()?;
    Ok(report_values(&output))
}

/// The `/report` values, read from the Prometheus text output: the capacity
/// gauges in GB, every other sample as printed. The worker counts are one
/// labelled gauge (`worker_num{tag="live"}`, ...), so each is renamed to the
/// `<tag>_workers` key the summary prints (`live_workers`, `lost_workers`).
fn report_values(output: &str) -> HashMap<String, String> {
    output
        .lines()
        .filter(|line| !line.starts_with("#"))
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let name = parts.next()?;
            let value = parts.next()?;
            let value = match name {
                "capacity" | "available" | "fs_used" => {
                    let v = value.parse::<f64>().unwrap_or(0.0);
                    let v = v / 1024.0 / 1024.0 / 1024.0;
                    format!("{:.2}GB", v)
                }
                _ => value.to_string(),
            };
            let tag = name
                .strip_prefix("worker_num{tag=\"")
                .and_then(|rest| rest.strip_suffix("\"}"));
            let name = match tag {
                Some(tag) => format!("{tag}_workers"),
                None => name.to_string(),
            };
            Some((name, value))
        })
        .collect()
}

async fn metrics(Extension(instance): Extension<Arc<MasterRouterHandler>>) -> FsResult<String> {
    instance.text_output()
}

async fn report(Extension(instance): Extension<Arc<MasterRouterHandler>>) -> FsResult<String> {
    let report = get_report(&instance)?;
    let metric = |key: &str| report.get(key).map(String::as_str).unwrap_or("N/A");
    let available = metric("available");
    let capacity = metric("capacity");
    let fs_used = metric("fs_used");
    let dir_total = metric("inode_dir_num");
    let files_total = metric("inode_file_num");
    let live_workers = metric("live_workers");
    let lost_workers = metric("lost_workers");

    let result = format!(
        r#"Curvine cluster summary:
    available: {available}
    capacity: {capacity}
    fs_used: {fs_used}
    dir_total: {dir_total}
    files_total: {files_total}
    live_workers: {live_workers}
    lost_workers: {lost_workers}
    "#
    );
    Ok(result)
}

async fn overview(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Value>> {
    let fs = &instance.fs;
    let conf = &instance.conf;
    let start_time = &instance.start_time;
    let filesystem_info = fs.filesystem_info()?;

    let (files_total, dir_total) = {
        let (f, d) = fs.get_file_counts();
        (f as usize, d as usize)
    };

    let expected_capacity = filesystem_info.available + filesystem_info.fs_used;
    if filesystem_info.capacity != expected_capacity {
        log::warn!(
            "Capacity inconsistency detected: capacity={}, available={}, fs_used={}, expected_capacity={}",
            filesystem_info.capacity,
            filesystem_info.available,
            filesystem_info.fs_used,
            expected_capacity
        );
    } else {
        log::debug!(
            "Capacity consistency verified: capacity={}, available={}, fs_used={}",
            filesystem_info.capacity,
            filesystem_info.available,
            filesystem_info.fs_used
        );
    }

    let master_state = format!("{:?}", fs.master_monitor.journal_state());
    // A standby master cannot serve writes; expose the leader's web address so the
    // Web UI can link to the active node. All masters share the configured web port.
    let active_master_web_addr = if fs.master_monitor.is_active() {
        None
    } else {
        let leader_id = fs.master_monitor.follower_readiness_snapshot().leader_id;
        conf.journal
            .journal_addrs
            .iter()
            .find(|peer| peer.id == leader_id)
            .map(|peer| format!("{}:{}", peer.hostname, conf.master.web_port))
    };

    let mut res = json!({
        "cluster_id": conf.cluster_id,
        "master_addr": conf.master_addr().to_string(),
        "start_time": start_time,
        "start_time_ms": instance.start_time_ms,
        "live_workers": filesystem_info.live_workers.len(),
        "lost_workers": filesystem_info.lost_workers.len(),
        "available": filesystem_info.available,
        "capacity": filesystem_info.capacity,
        "fs_used": filesystem_info.fs_used,
        "reserved_bytes": filesystem_info.reserved_bytes,
        "files_total": files_total,
        "dir_total": dir_total,
        "master_state": master_state,
        "active_master_web_addr": active_master_web_addr,
    });
    if let Value::Object(fields) = &mut res {
        fields.extend(overview_version_fields());
    }
    Ok(Json(res))
}

/// Version metadata of the running master, added to the `/api/overview` response so the
/// Web UI can show the real build instead of a hard-coded version.
fn overview_version_fields() -> serde_json::Map<String, Value> {
    let version = curvine_sys::version::component_version("master");
    let mut fields = serde_json::Map::new();
    fields.insert("version".into(), Value::String(version.release_version));
    fields.insert("git_commit".into(), Value::String(version.git_commit));
    fields.insert("git_branch".into(), Value::String(version.git_branch));
    fields
}

async fn browse(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Vec<FileStatus>>> {
    let fs = &instance.fs;
    let root_path = fs.fs_dir.read().root_dir().name().to_string();
    let path = params.get("path").unwrap_or(&root_path);
    let files = fs.list_status(path)?;
    Ok(Json(files))
}

async fn block_locations(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<FileBlocks>> {
    let fs = &instance.fs;
    let path = match params.get("path") {
        Some(path) => path,
        None => return err_box!("missing required query parameter: path"),
    };
    let files = fs.get_block_locations(path)?;
    Ok(Json(files))
}

async fn workers(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<HashMap<String, Vec<WorkerInfo>>>> {
    let fs = &instance.fs;
    let wm = fs.worker_manager.read();
    let mut workers = HashMap::new();
    let mut live_workers = vec![];
    for live_worker in wm.worker_map.workers() {
        live_workers.push(live_worker.1.clone());
    }
    let mut lost_workers = vec![];
    for lost_worker in &wm.worker_map.lost_workers {
        lost_workers.push(lost_worker.1.clone());
    }
    workers.insert("live_workers".to_string(), live_workers);
    workers.insert("lost_workers".to_string(), lost_workers);
    Ok(Json(workers))
}

async fn add_dcm(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Vec<String>>> {
    let fs = &instance.fs;
    let mut wm = fs.worker_manager.write();
    match params.get("workers") {
        None => err_box!("not params workers"),
        Some(v) => {
            let list = v
                .split(",")
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect();
            let res = wm.add_dcm(list);
            Ok(Json(res))
        }
    }
}

async fn get_dcm(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Vec<String>>> {
    let fs = &instance.fs;
    let wm = fs.worker_manager.read();
    Ok(Json(wm.get_dcm()))
}

async fn remove_dcm(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Vec<String>>> {
    let fs = &instance.fs;
    let mut wm = fs.worker_manager.write();
    match params.get("workers") {
        None => err_box!("not params workers"),
        Some(v) => {
            let list = v
                .split(",")
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect();
            let res = wm.remove_dcm(list);
            Ok(Json(res))
        }
    }
}

/// Client session leases with their ownership footprint (#1885). A zombie mount
/// is identifiable here — host, mount point, whether its lease is still live — without
/// reading the metadata database.
async fn sessions(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Vec<SessionSummary>>> {
    Ok(Json(instance.fs.session_summary()?))
}

/// Default page size of `/api/transfers`, matching `cv transfer list`.
const TRANSFER_LIST_DEFAULT_LIMIT: u32 = 20;
/// Upper bound of `/api/transfers?limit=`, matching the RPC page cap.
const TRANSFER_LIST_MAX_LIMIT: u32 = 1000;

fn transfer_kind_name(kind: TransferKind) -> &'static str {
    match kind {
        TransferKind::Load => "load",
        TransferKind::Export => "export",
    }
}

fn transfer_state_name(state: TransferState) -> &'static str {
    match state {
        TransferState::Pending => "pending",
        TransferState::Planning => "planning",
        TransferState::Dispatching => "dispatching",
        TransferState::Running => "running",
        TransferState::Canceling => "canceling",
        TransferState::Completed => "completed",
        TransferState::Failed => "failed",
        TransferState::Canceled => "canceled",
        TransferState::PartialSuccess => "partial-success",
    }
}

fn parse_transfer_kind(raw: &str) -> Option<TransferKind> {
    match raw.to_ascii_lowercase().as_str() {
        "load" => Some(TransferKind::Load),
        "export" => Some(TransferKind::Export),
        _ => None,
    }
}

fn parse_transfer_state(raw: &str) -> Option<TransferState> {
    match raw.to_ascii_lowercase().replace('_', "-").as_str() {
        "pending" => Some(TransferState::Pending),
        "planning" => Some(TransferState::Planning),
        "dispatching" => Some(TransferState::Dispatching),
        "running" => Some(TransferState::Running),
        "canceling" => Some(TransferState::Canceling),
        "completed" => Some(TransferState::Completed),
        "failed" => Some(TransferState::Failed),
        "canceled" => Some(TransferState::Canceled),
        "partial-success" => Some(TransferState::PartialSuccess),
        _ => None,
    }
}

/// One job, without the planner inputs (`command_json`, mount and secret
/// snapshots) that `TransferJobRecord` also carries.
fn transfer_job_json(job: &TransferJobRecord) -> Value {
    json!({
        "job_id": job.job_id,
        "run_id": job.run_id,
        "kind": transfer_kind_name(job.kind),
        "state": transfer_state_name(job.state),
        "source_path": job.source_path,
        "target_path": job.target_path,
        "submitter": job.submitter,
        "tenant": job.tenant,
        "owner": job.owner,
        "progress": {
            "loaded_size": job.summary.loaded_size,
            "total_size": job.summary.total_size,
            "total_files": job.summary.total_files,
            "loaded_files": job.summary.loaded_files,
            "skipped_files": job.summary.skipped_files,
            "skipped_size": job.summary.skipped_size,
            "update_time": job.summary.update_time,
            "message": job.summary.message,
        },
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    })
}

fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "message": message.into() }))).into_response()
}

fn proto_kind_name(code: i32) -> &'static str {
    match TransferKindProto::from_i32(code) {
        Some(TransferKindProto::TransferLoad) => "load",
        Some(TransferKindProto::TransferExport) => "export",
        None => "unknown",
    }
}

fn proto_state_name(code: i32) -> &'static str {
    match TransferStateProto::from_i32(code) {
        Some(TransferStateProto::TransferPending) => "pending",
        Some(TransferStateProto::TransferPlanning) => "planning",
        Some(TransferStateProto::TransferDispatching) => "dispatching",
        Some(TransferStateProto::TransferRunning) => "running",
        Some(TransferStateProto::TransferCanceling) => "canceling",
        Some(TransferStateProto::TransferCompleted) => "completed",
        Some(TransferStateProto::TransferFailed) => "failed",
        Some(TransferStateProto::TransferCanceled) => "canceled",
        Some(TransferStateProto::TransferPartialSuccess) => "partial-success",
        None => "unknown",
    }
}

fn transfer_proto_progress_json(progress: &TransferProgressProto) -> Value {
    json!({
        "loaded_size": progress.loaded_size,
        "total_size": progress.total_size,
        "total_files": progress.total_files.unwrap_or_default(),
        "loaded_files": progress.loaded_files.unwrap_or_default(),
        "skipped_files": progress.skipped_files.unwrap_or_default(),
        "skipped_size": progress.skipped_size.unwrap_or_default(),
        "update_time": progress.update_time,
        "message": progress.message,
    })
}

fn transfer_proto_job_json(job: &TransferJobStatusProto) -> Value {
    json!({
        "job_id": job.job_id,
        "run_id": job.run_id,
        "kind": proto_kind_name(job.kind),
        "state": proto_state_name(job.state),
        "source_path": job.source_path,
        "target_path": job.target_path,
        "submitter": job.submitter,
        "tenant": job.tenant,
        "owner": job.owner.clone().unwrap_or_default(),
        "progress": transfer_proto_progress_json(&job.progress),
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    })
}

fn transfer_proto_status_json(detail: &GetTransferStatusResponse) -> Value {
    json!({
        "job_id": detail.job_id,
        "run_id": detail.run_id,
        "kind": detail.kind.map(proto_kind_name).unwrap_or("unknown"),
        "state": proto_state_name(detail.state),
        "source_path": detail.source_path.clone().unwrap_or_default(),
        "target_path": detail.target_path.clone().unwrap_or_default(),
        "submitter": detail.submitter.clone().unwrap_or_default(),
        "tenant": detail.tenant.clone().unwrap_or_default(),
        "owner": detail.owner.clone().unwrap_or_default(),
        "progress": transfer_proto_progress_json(&detail.progress),
        "created_at": detail.created_at.unwrap_or_default(),
        "updated_at": detail.updated_at.unwrap_or_default(),
    })
}

#[derive(Deserialize)]
struct SubmitTransferBody {
    source_path: String,
    target_path: String,
    #[serde(default)]
    files: Vec<String>,
    #[serde(default)]
    overwrite: Option<bool>,
    #[serde(default)]
    submitter: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    force: bool,
}

fn normalized_file_list(files: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(files.len());
    for file in files {
        let file = file.trim();
        if file.is_empty() || file.starts_with('#') {
            continue;
        }
        out.push(file.to_string());
    }
    out
}

/// `POST /api/load/jobs` — submit a UFS-to-Curvine load. JSON body:
/// `{source_path, target_path, files?: [...], overwrite?, submitter?, tenant?, force?}`.
/// A `text/plain` body is treated as the file list, with `source_path` and
/// `target_path` given as query parameters.
async fn submit_transfer(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    submit_transfer_request(instance, params, headers, body, TransferKind::Load).await
}

/// `POST /api/export/jobs` — submit a Curvine-to-UFS export from an explicit
/// source/target pair. File lists are a load-only feature.
async fn submit_export(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    submit_transfer_request(instance, params, headers, body, TransferKind::Export).await
}

async fn submit_transfer_request(
    instance: Arc<MasterRouterHandler>,
    params: HashMap<String, String>,
    headers: HeaderMap,
    body: Bytes,
    kind: TransferKind,
) -> Response {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let body = if content_type.starts_with("application/json") {
        match serde_json::from_slice::<SubmitTransferBody>(&body) {
            Ok(body) => body,
            Err(err) => {
                return json_error(StatusCode::BAD_REQUEST, format!("invalid JSON body: {err}"))
            }
        }
    } else {
        if kind != TransferKind::Load {
            return json_error(
                StatusCode::BAD_REQUEST,
                "a text file list is only supported for load jobs",
            );
        }
        let source_path = match params.get("source_path") {
            Some(path) if !path.trim().is_empty() => path.clone(),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "source_path query parameter is required for a text file list",
                )
            }
        };
        let target_path = match params.get("target_path") {
            Some(path) if !path.trim().is_empty() => path.clone(),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "target_path query parameter is required for a text file list",
                )
            }
        };
        let text = match std::str::from_utf8(&body) {
            Ok(text) => text,
            Err(_) => return json_error(StatusCode::BAD_REQUEST, "file list must be UTF-8 text"),
        };
        SubmitTransferBody {
            source_path,
            target_path,
            files: normalized_file_list(text.lines().map(str::to_string).collect()),
            overwrite: params.get("overwrite").map(|value| value != "false"),
            submitter: params.get("submitter").cloned(),
            tenant: params.get("tenant").cloned(),
            force: params.get("force").is_some_and(|value| value != "false"),
        }
    };

    if body.source_path.trim().is_empty() || body.target_path.trim().is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "source_path and target_path are required",
        );
    }
    let files = normalized_file_list(body.files);
    if kind != TransferKind::Load && !files.is_empty() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "the files list is supported for load jobs only",
        );
    }
    let overwrite = body.overwrite.unwrap_or(true);
    let submitter = body
        .submitter
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "http-api".to_string());
    let tenant = body.tenant.unwrap_or_default();
    let mut command = TransferCommand {
        kind,
        source_path: body.source_path.clone(),
        target_path: body.target_path.clone(),
        client_request_id: TransferCommand::default_client_request_id_with_overwrite(
            kind,
            &body.source_path,
            &body.target_path,
            overwrite,
        ),
        submitter: submitter.clone(),
        tenant: tenant.clone(),
        options: BTreeMap::new(),
        files: files.clone(),
    };
    if let Some(digest) = command.files_digest() {
        command.client_request_id = format!("{}_files_{digest}", command.client_request_id);
    }
    command.set_overwrite(overwrite);

    if let Some(transfer) = instance.transfer.as_ref() {
        let command_bytes = match serde_json::to_vec(&command) {
            Ok(bytes) => bytes,
            Err(err) => {
                return json_error(StatusCode::BAD_REQUEST, format!("invalid command: {err}"))
            }
        };
        let req = SubmitTransferRequest {
            kind: kind as i32,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            client_request_id: command.client_request_id.clone(),
            submitter: command.submitter.clone(),
            tenant: command.tenant.clone(),
            command: command_bytes,
            protocol_version: Some(1),
        };
        return match transfer.service().submit_transfer(req) {
            Ok(job) => {
                let mut job = job;
                if body.force && job.state.is_terminal() {
                    match transfer.service().retry_transfer(&job.job_id) {
                        Ok(retried) => job = retried,
                        Err(err) => {
                            return json_error(StatusCode::BAD_REQUEST, err.to_string());
                        }
                    }
                }
                (
                    StatusCode::ACCEPTED,
                    Json(json!({
                        "job_id": job.job_id,
                        "run_id": job.run_id,
                        "state": transfer_state_name(job.state),
                        "files": files.len(),
                    })),
                )
                    .into_response()
            }
            Err(err) => json_error(StatusCode::BAD_REQUEST, err.to_string()),
        };
    }

    let Some(client) = instance.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client.submit(command).await {
        Ok(mut resp) => {
            if body.force && TransferState::from(resp.state).is_terminal() {
                match client.retry(&resp.job_id).await {
                    Ok(retried) => resp = retried,
                    Err(err) => {
                        log::error!("retrying transfer submit failed: {err}");
                        return json_error(
                            StatusCode::BAD_GATEWAY,
                            "transfer service is unavailable",
                        );
                    }
                }
            }
            (
                StatusCode::ACCEPTED,
                Json(json!({
                    "job_id": resp.job_id,
                    "run_id": resp.run_id,
                    "state": transfer_state_name(TransferState::from(resp.state)),
                    "files": files.len(),
                })),
            )
                .into_response()
        }
        Err(err) => {
            log::error!("proxying transfer submit failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `GET /api/transfers` — one page of transfer jobs, newest first.
///
/// Read-only, like the rest of `/api/*`. In the default embedded mode the jobs
/// live in this master's memory, so only the Active master has them and only the
/// last `transfer.terminal_retention` (10 min by default) of finished jobs
/// survive.
async fn transfers(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let kind = match params.get("kind") {
        Some(raw) => match parse_transfer_kind(raw) {
            Some(kind) => Some(kind),
            None => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("unknown kind '{raw}'; expected load or export"),
                )
            }
        },
        None => None,
    };
    let state = match params.get("state") {
        Some(raw) => match parse_transfer_state(raw) {
            Some(state) => Some(state),
            None => return json_error(StatusCode::BAD_REQUEST, format!("unknown state '{raw}'")),
        },
        None => None,
    };
    let page_size = match params.get("limit") {
        Some(raw) => match raw.parse::<u32>() {
            Ok(limit) if limit > 0 => Some(limit.min(TRANSFER_LIST_MAX_LIMIT)),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    format!("invalid limit '{raw}'; expected a positive integer"),
                )
            }
        },
        None => Some(TRANSFER_LIST_DEFAULT_LIMIT),
    };
    let page_token = params.get("page_token").cloned();

    if let Some(transfer) = instance.transfer.as_ref() {
        let req = ListTransfersRequest {
            kind: kind.map(|kind| kind as i32),
            state: state.map(|state| state as i32),
            page_size,
            page_token,
            submitter: params.get("submitter").cloned(),
            tenant: params.get("tenant").cloned(),
        };
        return match transfer.service().list_transfer_records(req) {
            Ok((jobs, next_page_token)) => Json(json!({
                "jobs": jobs.iter().map(transfer_job_json).collect::<Vec<_>>(),
                "next_page_token": next_page_token,
            }))
            .into_response(),
            Err(err) => err.into_response(),
        };
    }

    let Some(client) = instance.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client
        .list(
            kind,
            state,
            params.get("submitter").cloned(),
            params.get("tenant").cloned(),
            page_size,
            page_token,
        )
        .await
    {
        Ok(resp) => Json(json!({
            "jobs": resp.jobs.iter().map(transfer_proto_job_json).collect::<Vec<_>>(),
            "next_page_token": resp.next_page_token,
        }))
        .into_response(),
        Err(err) => {
            log::error!("proxying transfer list failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `GET /api/transfers/{job_id}` — one job, including finished ones still held
/// by the store.
/// `GET /api/load/jobs/{job_id}`.
async fn load_job_detail(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Path(job_id): Path<String>,
) -> Response {
    transfer_detail_for(instance, job_id, Some(TransferKind::Load)).await
}

async fn transfer_detail_for(
    instance: Arc<MasterRouterHandler>,
    job_id: String,
    expected_kind: Option<TransferKind>,
) -> Response {
    let mismatch = || json_error(StatusCode::NOT_FOUND, format!("job {job_id} not found"));

    if let Some(transfer) = instance.transfer.as_ref() {
        return match transfer.service().get_transfer(&job_id) {
            Ok(job) if expected_kind.is_none_or(|kind| job.kind == kind) => {
                Json(transfer_job_json(&job)).into_response()
            }
            Ok(_) => mismatch(),
            Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
                json_error(StatusCode::NOT_FOUND, err.to_string())
            }
            Err(err) => err.into_response(),
        };
    }

    let Some(client) = instance.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client.status_summary(&job_id).await {
        Ok(detail) if expected_kind.is_none_or(|kind| detail.kind == Some(kind as i32)) => {
            Json(transfer_proto_status_json(&detail)).into_response()
        }
        Ok(_) => mismatch(),
        Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
            json_error(StatusCode::NOT_FOUND, err.to_string())
        }
        Err(err) => {
            log::error!("proxying transfer detail failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

/// `GET /api/load/jobs` — load jobs only; the transfer kind is fixed so the
/// public cache API never exposes export/job internals as a filter.
async fn load_jobs(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Response {
    params.insert("kind".to_string(), "load".to_string());
    transfers(Extension(instance), Query(params)).await
}

/// `GET /api/export/jobs/{job_id}`.
async fn export_job_detail(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Path(job_id): Path<String>,
) -> Response {
    transfer_detail_for(instance, job_id, Some(TransferKind::Export)).await
}

/// `GET /api/export/jobs` — export jobs only.
async fn export_jobs(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(mut params): Query<HashMap<String, String>>,
) -> Response {
    params.insert("kind".to_string(), "export".to_string());
    transfers(Extension(instance), Query(params)).await
}

/// `POST /api/load/jobs/{job_id}/cancel`.
async fn cancel_load_job(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Path(job_id): Path<String>,
) -> Response {
    cancel_job_for(instance, job_id, TransferKind::Load).await
}

/// `POST /api/export/jobs/{job_id}/cancel`.
async fn cancel_export_job(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Path(job_id): Path<String>,
) -> Response {
    cancel_job_for(instance, job_id, TransferKind::Export).await
}

async fn cancel_job_for(
    instance: Arc<MasterRouterHandler>,
    job_id: String,
    expected_kind: TransferKind,
) -> Response {
    let mismatch = || json_error(StatusCode::NOT_FOUND, format!("job {job_id} not found"));
    if let Some(transfer) = instance.transfer.as_ref() {
        let job = match transfer.service().get_transfer(&job_id) {
            Ok(job) if job.kind == expected_kind => job,
            Ok(_) => return mismatch(),
            Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
                return json_error(StatusCode::NOT_FOUND, err.to_string())
            }
            Err(err) => return err.into_response(),
        };
        return match transfer.service().request_cancel(&job.job_id, None) {
            Ok(state) => Json(json!({
                "job_id": job.job_id,
                "state": transfer_state_name(state),
            }))
            .into_response(),
            Err(err) => json_error(StatusCode::BAD_REQUEST, err.to_string()),
        };
    }

    let Some(client) = instance.transfer_client.as_ref() else {
        return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
    };
    match client.status_summary(&job_id).await {
        Ok(detail) if detail.kind == Some(expected_kind as i32) => {}
        Ok(_) => return mismatch(),
        Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => {
            return json_error(StatusCode::NOT_FOUND, err.to_string())
        }
        Err(err) => {
            log::error!("proxying transfer kind check failed: {err}");
            return json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable");
        }
    }
    match client.cancel(&job_id, None).await {
        Ok(resp) => Json(json!({
            "job_id": resp.job_id,
            "state": transfer_state_name(TransferState::from(resp.state)),
        }))
        .into_response(),
        Err(err) => {
            log::error!("proxying transfer cancel failed: {err}");
            json_error(StatusCode::BAD_GATEWAY, "transfer service is unavailable")
        }
    }
}

#[derive(Deserialize)]
struct FreeJobBody {
    path: String,
    #[serde(default)]
    recursive: bool,
}

fn free_job_json(job: &FreeJobRecord) -> Value {
    json!({
        "job_id": job.job_id,
        "path": job.path,
        "recursive": job.recursive,
        "state": job.state,
        "freed_inodes": job.freed_inodes,
        "freed_bytes": job.freed_bytes,
        "message": job.message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    })
}

/// `GET /api/free/jobs` — recently submitted cache-release jobs, newest first.
async fn free_jobs(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let limit = match params.get("limit") {
        Some(raw) => match raw.parse::<usize>() {
            Ok(limit) if limit > 0 => limit.min(1000),
            _ => return json_error(StatusCode::BAD_REQUEST, format!("invalid limit '{raw}'")),
        },
        None => 20,
    };
    let jobs = instance
        .free_jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .list(limit);
    Json(json!({ "jobs": jobs.iter().map(free_job_json).collect::<Vec<_>>() })).into_response()
}

/// `GET /api/free/jobs/{job_id}`.
async fn free_job_detail(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Path(job_id): Path<String>,
) -> Response {
    match instance
        .free_jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&job_id)
    {
        Some(job) => Json(free_job_json(&job)).into_response(),
        None => json_error(
            StatusCode::NOT_FOUND,
            format!("free job {job_id} not found"),
        ),
    }
}

/// `POST /api/free/jobs` {path, recursive} — run a cache-release job.
async fn submit_free_job(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Json(body): Json<FreeJobBody>,
) -> Response {
    if body.path.trim().is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "path is required");
    }
    let job_id = Uuid::new_v4().to_string();
    let now = LocalTime::mills() as i64;
    let record = FreeJobRecord {
        job_id: job_id.clone(),
        path: body.path.clone(),
        recursive: body.recursive,
        state: "running".to_string(),
        freed_inodes: 0,
        freed_bytes: 0,
        message: String::new(),
        created_at: now,
        updated_at: now,
    };
    instance
        .free_jobs
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(record.clone());

    let fs = instance.fs.clone();
    let jobs = instance.free_jobs.clone();
    let path = body.path.clone();
    let recursive = body.recursive;
    let background_job_id = job_id.clone();
    tokio::task::spawn_blocking(move || {
        let (state, inodes, bytes, message) = match fs.free(&path, recursive) {
            Ok(result) => (
                "completed".to_string(),
                result.inodes,
                result.bytes,
                String::new(),
            ),
            Err(err) => ("failed".to_string(), 0, 0, err.to_string()),
        };
        jobs.lock().unwrap_or_else(|e| e.into_inner()).update(
            &background_job_id,
            &state,
            inodes,
            bytes,
            message,
        );
    });

    (StatusCode::ACCEPTED, Json(free_job_json(&record))).into_response()
}

impl RouterHandler for MasterRouterHandler {
    fn router(&self) -> Router {
        let instance = Arc::new(self.clone());
        let conf = self.conf.clone();
        let router = Router::new()
            .route("/metrics", get(metrics))
            .route("/report", get(report))
            .route("/api/overview", get(overview))
            .route("/api/config", get(|| async { Json(conf) }))
            .route("/api/browse", get(browse))
            .route("/api/block_locations", get(block_locations))
            .route("/api/workers", get(workers))
            .route("/add-dcm", get(add_dcm))
            .route("/get-dcm", get(get_dcm))
            .route("/remove-dcm", get(remove_dcm))
            .route("/api/sessions", get(sessions))
            .route(
                "/api/load/jobs",
                get(load_jobs)
                    .post(submit_transfer)
                    .layer(DefaultBodyLimit::max(64 * 1024 * 1024)),
            )
            .route("/api/load/jobs/:job_id", get(load_job_detail))
            .route("/api/load/jobs/:job_id/cancel", post(cancel_load_job))
            .route("/api/export/jobs", get(export_jobs).post(submit_export))
            .route("/api/export/jobs/:job_id", get(export_job_detail))
            .route("/api/export/jobs/:job_id/cancel", post(cancel_export_job))
            .route("/api/free/jobs", get(free_jobs).post(submit_free_job))
            .route("/api/free/jobs/:job_id", get(free_job_detail))
            .layer(Extension(instance));

        self.fault_http.mount(router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_values_name_the_worker_counts() {
        let output = "# HELP capacity Total storage capacity
# TYPE capacity gauge
capacity 2147483648
inode_dir_num 3
# HELP worker_num The number of lived workers
# TYPE worker_num gauge
worker_num{tag=\"blacklist\"} 0
worker_num{tag=\"live\"} 2
worker_num{tag=\"lost\"} 1
";
        let report = report_values(output);

        assert_eq!(report["capacity"], "2.00GB");
        assert_eq!(report["inode_dir_num"], "3");
        assert_eq!(report["live_workers"], "2");
        assert_eq!(report["lost_workers"], "1");
        assert_eq!(report["blacklist_workers"], "0");
        assert!(!report.keys().any(|k| k.starts_with("worker_num")));
    }

    #[test]
    fn overview_version_fields_report_the_build_version() {
        let version = curvine_sys::version::component_version("master");
        let fields = overview_version_fields();

        assert!(!version.release_version.is_empty());
        assert_eq!(fields["version"], version.release_version.as_str());
        assert_eq!(fields["git_commit"], version.git_commit.as_str());
        assert_eq!(fields["git_branch"], version.git_branch.as_str());
    }

    #[test]
    fn transfer_query_names_round_trip() {
        assert_eq!(transfer_kind_name(TransferKind::Load), "load");
        assert_eq!(parse_transfer_kind("LOAD"), Some(TransferKind::Load));
        assert_eq!(parse_transfer_kind("export"), Some(TransferKind::Export));
        assert_eq!(parse_transfer_kind("copy"), None);

        for state in [
            TransferState::Pending,
            TransferState::Planning,
            TransferState::Dispatching,
            TransferState::Running,
            TransferState::Canceling,
            TransferState::Completed,
            TransferState::Failed,
            TransferState::Canceled,
            TransferState::PartialSuccess,
        ] {
            let name = transfer_state_name(state);
            assert_eq!(parse_transfer_state(name), Some(state));
        }
        assert_eq!(
            parse_transfer_state("Partial_Success"),
            Some(TransferState::PartialSuccess)
        );
        assert_eq!(parse_transfer_state("done"), None);
    }

    #[test]
    fn free_jobs_prune_old_entries_and_track_results() {
        let mut store = FreeJobStore::default();
        for index in 0..(FREE_JOB_LIMIT + 5) {
            store.insert(FreeJobRecord {
                job_id: format!("job-{index}"),
                path: format!("/data/{index}"),
                recursive: false,
                state: "running".to_string(),
                freed_inodes: 0,
                freed_bytes: 0,
                message: String::new(),
                created_at: index as i64,
                updated_at: index as i64,
            });
        }
        assert_eq!(store.list(FREE_JOB_LIMIT + 5).len(), FREE_JOB_LIMIT);
        store.update("job-204", "completed", 1, 1024, String::new());
        let job = store.get("job-204").expect("recent job must be kept");
        assert_eq!(job.state, "completed");
        assert_eq!(job.freed_bytes, 1024);
        assert!(store.get("job-0").is_none());
    }

    #[test]
    fn submit_file_list_skips_blank_comments_and_changes_the_job_key() {
        assert_eq!(
            normalized_file_list(vec![
                " s3://b/a ".to_string(),
                String::new(),
                "# comment".to_string(),
                "s3://b/b".to_string(),
            ]),
            vec!["s3://b/a".to_string(), "s3://b/b".to_string()]
        );

        let command = TransferCommand {
            kind: TransferKind::Load,
            source_path: "s3://b".to_string(),
            target_path: "/data/b".to_string(),
            client_request_id: "req".to_string(),
            submitter: "test".to_string(),
            tenant: String::new(),
            options: Default::default(),
            files: vec!["s3://b/a".to_string()],
        };
        let without_files = TransferCommand {
            files: Vec::new(),
            ..command.clone()
        };
        assert_ne!(command.job_key(), without_files.job_key());
        assert!(command.files_digest().is_some());
        assert!(without_files.files_digest().is_none());
    }

    #[test]
    fn transfer_job_json_hides_planner_inputs() {
        let job = TransferJobRecord {
            job_key: "key-1".to_string(),
            job_id: "job-1".to_string(),
            run_id: 7,
            kind: TransferKind::Load,
            source_path: "s3://bucket/a".to_string(),
            target_path: "/data/a".to_string(),
            command_json: "{\"secret\":1}".to_string(),
            mount_snapshot_json: "{\"access_key\":\"x\"}".to_string(),
            secret_ref_json: "{\"secret_ref\":\"y\"}".to_string(),
            cluster_snapshot_version: 3,
            cv_metadata_epoch: None,
            state: TransferState::Running,
            owner: "master-1".to_string(),
            lease_epoch: 2,
            lease_expire_at: 30,
            cancel_requested: false,
            summary: curvine_model::TransferProgress {
                total_files: 7,
                loaded_files: 3,
                skipped_files: 2,
                skipped_size: 100,
                ..Default::default()
            },
            client_request_id: "req-1".to_string(),
            submitter: "syz".to_string(),
            tenant: "default".to_string(),
            spec_version: 0,
            spec_blob: Vec::new(),
            created_at: 10,
            updated_at: 20,
        };

        let value = transfer_job_json(&job);

        assert_eq!(value["job_id"], "job-1");
        assert_eq!(value["kind"], "load");
        assert_eq!(value["state"], "running");
        assert_eq!(value["progress"]["loaded_size"], 0);
        assert_eq!(value["progress"]["total_files"], 7);
        assert_eq!(value["progress"]["loaded_files"], 3);
        assert_eq!(value["progress"]["skipped_files"], 2);
        assert_eq!(value["progress"]["skipped_size"], 100);
        assert!(value.get("command_json").is_none());
        assert!(value.get("mount_snapshot_json").is_none());
        assert!(value.get("secret_ref_json").is_none());
    }
}
