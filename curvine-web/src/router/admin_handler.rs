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

use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{timeout, Duration};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, Request};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::job_display::{build_job_display_name, format_job_timestamp_ms, job_path_slug};
use crate::observability::{self, ObservabilityService};
use crate::router::RouterHandler;
use crate::util::addr::{
    configured_master_addrs, display_addr_text_for_cluster, display_inet_addr_with_hint,
    display_master_endpoint, hosts_match_for_cluster_mapping, resolve_display_hostname,
    resolve_display_ip,
};
use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_error::{ErrorKind, FsError};
use curvine_fs_api::{FileSystem, Path as FsPath, Reader, RpcCode, Writer};
use curvine_job_client::{JobListOptions, JobMasterClient, TransferClient};
use curvine_model::{
    AccessMode, FileBlocks, FileStatus, JobSourceType, JobStatus, JobTaskState, LoadJobCommand,
    MasterInfo, MountInfo, MountOptions, Provider, StorageState, StorageType, TransferCommand,
    TransferKind, TransferState, TtlAction, WorkerInfo, WriteType,
};
use curvine_net::net::InetAddr;
use curvine_proto::{
    GetTransferStatusResponse, SubmitTransferResponse, TransferJobStatusProto,
    TransferTaskStatusProto, TransferTaskSummaryProto,
};
use curvine_raft::raft::RaftClient;
use curvine_runtime::common::CommonUtils;
use curvine_runtime::common::{ByteUnit, DurationUnit, LocalTime};
use curvine_runtime::runtime::Runtime;
use curvine_unified_fs::{UfsFileSystem, UnifiedFileSystem};

/// Stable host identity for HA comparisons across DNS suffix drift
/// (e.g. `*.svc.cluster.local` reported by master vs `*.common1ack.oppo.local` in web conf).
fn journal_peer_id_for_host(conf: &ClusterConf, hostname: &str) -> Option<u64> {
    conf.journal
        .journal_addrs
        .iter()
        .find(|peer| hosts_match_for_cluster_mapping(&peer.hostname, hostname))
        .map(|peer| peer.id)
}

fn configured_master_addr_for_raft_id(conf: &ClusterConf, raft_id: u64) -> Option<String> {
    let peer = conf
        .journal
        .journal_addrs
        .iter()
        .find(|peer| peer.id == raft_id)?;
    configured_master_addrs(conf)
        .iter()
        .find(|addr| hosts_match_for_cluster_mapping(&addr.hostname, &peer.hostname))
        .map(|addr| addr.to_string())
}

/// Returns true when two master endpoint strings refer to the same raft peer.
fn master_endpoints_equivalent(conf: &ClusterConf, left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left.is_empty() || right.is_empty() {
        return false;
    }
    if left == right {
        return true;
    }
    match (
        target_master_node_id(conf, left),
        target_master_node_id(conf, right),
    ) {
        (Some(left_id), Some(right_id)) => left_id == right_id,
        _ => false,
    }
}

fn master_is_active_target(
    conf: &ClusterConf,
    active_master: &str,
    active_raft_id: Option<u64>,
    target_master: &str,
    target_raft_id: Option<u64>,
) -> bool {
    match (active_raft_id, target_raft_id) {
        (Some(active_id), Some(target_id)) => active_id == target_id,
        _ => master_endpoints_equivalent(conf, active_master, target_master),
    }
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct FsPathRequest {
    path: String,
    #[serde(default)]
    create_parent: Option<bool>,
    #[serde(default)]
    recursive: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct WorkerActionRequest {
    worker: String,
    action: String,
}

#[derive(Debug, Deserialize)]
struct MasterFailoverRequest {
    target_master: String,
}

#[derive(Debug, Deserialize)]
struct MountRequest {
    cv_path: String,
    ufs_path: String,
    #[serde(default)]
    update: bool,
    #[serde(default = "default_write_type")]
    write_type: String,
    #[serde(default = "default_ttl")]
    ttl: String,
    #[serde(default)]
    read_verify_ufs: bool,
    #[serde(default)]
    auto_cache: Option<bool>,
    #[serde(default)]
    access_mode: Option<String>,
    #[serde(default)]
    replicas: Option<i32>,
    #[serde(default)]
    block_size: Option<String>,
    #[serde(default)]
    storage_type: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct MountPathRequest {
    cv_path: String,
}

#[derive(Debug, Deserialize)]
struct MountValidateRequest {
    ufs_path: String,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct MountResyncRequest {
    cv_path: String,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
struct SubmitLoadRequest {
    path: String,
    #[serde(default)]
    target_path: Option<String>,
    #[serde(default)]
    ttl: Option<String>,
    #[serde(default)]
    ttl_action: Option<String>,
    #[serde(default)]
    recursive: bool,
    #[serde(default)]
    configs: HashMap<String, String>,
    #[serde(default)]
    replicas: Option<i32>,
    #[serde(default)]
    block_size: Option<String>,
    #[serde(default)]
    storage_type: Option<String>,
    #[serde(default)]
    overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct JobListQuery {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    page: Option<usize>,
    #[serde(default)]
    page_size: Option<usize>,
    #[serde(default)]
    include_tasks: Option<bool>,
    #[serde(default)]
    failed_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct TransferListQuery {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    page: Option<usize>,
    #[serde(default)]
    page_size: Option<usize>,
    #[serde(default)]
    failed_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct SubmitTransferRequest {
    #[serde(default)]
    kind: Option<String>,
    source_path: String,
    #[serde(default)]
    target_path: Option<String>,
    #[serde(default)]
    overwrite: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct UfsSyncJobsQuery {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_finished: Option<bool>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    include_tasks: Option<bool>,
    #[serde(default)]
    failed_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct ResyncTaskState {
    id: String,
    cv_path: String,
    mount_path: String,
    write_type: String,
    status: String,
    scanned: usize,
    skipped: usize,
    recreated: usize,
    failed: usize,
    pending_dirs: usize,
    message: String,
    started_at: String,
    updated_at: String,
    done: bool,
    dry_run: bool,
}

fn default_write_type() -> String {
    "fs_mode".to_string()
}

fn default_ttl() -> String {
    "7d".to_string()
}

fn normalize_ufs_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.starts_with('/') {
        format!("file://{}", trimmed)
    } else {
        trimmed.to_string()
    }
}

fn worker_matches(worker: &WorkerInfo, selector: &str) -> bool {
    let selector = selector.trim();
    if selector.contains(':') {
        return worker.address.connect_addr() == selector;
    }
    worker.worker_id().to_string() == selector
        || worker.address.worker_id.to_string() == selector
        || worker.address.hostname == selector
        || worker.address.ip_addr == selector
}

fn worker_json(worker: WorkerInfo) -> Value {
    let display_ip = resolve_display_ip(&worker.address.hostname, &worker.address.ip_addr);
    let mut value = json!(worker);
    if let Some(object) = value.as_object_mut() {
        object.insert("display_ip".to_string(), json!(display_ip));
    }
    value
}

fn worker_payload(worker: WorkerInfo, state: &str) -> Value {
    let usage_percent = if worker.capacity > 0 {
        ((worker.capacity - worker.available) as f64 / worker.capacity as f64 * 100.0).round()
    } else {
        0.0
    };
    let storage_count = worker.storage_map.len();
    json!({
        "worker": worker_json(worker),
        "state": state,
        "usage_percent": usage_percent,
        "storage_count": storage_count,
        "version": Value::Null,
        "file_count": Value::Null
    })
}

#[derive(Clone)]
pub struct AdminRouterHandler {
    conf: ClusterConf,
    unified_fs: UnifiedFileSystem,
    rt: Arc<Runtime>,
    start_time: String,
    resync_tasks: Arc<Mutex<HashMap<String, ResyncTaskState>>>,
    load_jobs: Arc<Mutex<VecDeque<String>>>,
    load_job_cache: Arc<Mutex<HashMap<String, Value>>>,
    auth_sessions: Arc<Mutex<HashMap<String, i64>>>,
    observability: Arc<ObservabilityService>,
}

const AUTH_COOKIE_NAME: &str = "curvine_web_session";
const DEFAULT_AUTH_SESSION_TTL_SECS: i64 = 12 * 60 * 60;
const DEFAULT_AUTH_PASSWORD_FILE: &str = "data/web-password";
const MASTER_INFO_TIMEOUT_SECS: u64 = 5;
const MASTER_HA_PING_TIMEOUT_SECS: u64 = 3;
const MASTER_FAILOVER_CONFIRM_TIMEOUT_SECS: u64 = 45;
const MASTER_FAILOVER_TRANSFER_TIMEOUT_SECS: u64 = 30;
const MASTER_FAILOVER_MAX_ATTEMPTS: usize = 3;
// Overall wall-clock budget for the whole failover request (all attempts combined).
// Kept below the web client request timeout (180s) so the backend stops before the
// client gives up, even when master-info probes are slow during a leaderless window.
const MASTER_FAILOVER_TOTAL_BUDGET_SECS: u64 = 150;
const WEB_UPLOAD_MAX_BYTES: usize = 512 * 1024 * 1024;
// Deep cache-summary is diagnostic-only; bounded to avoid unbounded UFS/CV walks.
const CACHE_SUMMARY_DEEP_TIMEOUT_SECS: u64 = 30;
const CACHE_SUMMARY_DEEP_MAX_ENTRIES: usize = 10_000;
// Upper bound the master enforces on a single job listing. The web layer fetches
// this fixed window and paginates locally so `total` stays stable across pages.
const JOB_LIST_MAX_FETCH: usize = 500;
const TRANSFER_RPC_TIMEOUT_SECS: u64 = 10;
const TRANSFER_SUBMIT_RPC_TIMEOUT_SECS: u64 = 30;
const TRANSFER_CANCEL_RPC_TIMEOUT_SECS: u64 = 15;
const TRANSFER_LIST_MAX_FETCH: usize = 1000;
const TRANSFER_LIST_MAX_PAGES: usize = 5;
const WEB_TRANSFER_SUBMITTER: &str = "curvine-web";

fn auth_username() -> String {
    env::var("CURVINE_WEB_USERNAME").unwrap_or_else(|_| "admin".to_string())
}

fn auth_password() -> Option<String> {
    if let Ok(password) = env::var("CURVINE_WEB_PASSWORD") {
        let password = password.trim().to_string();
        if !password.is_empty() {
            return Some(password);
        }
    }

    let password_file = env::var("CURVINE_WEB_PASSWORD_FILE")
        .unwrap_or_else(|_| DEFAULT_AUTH_PASSWORD_FILE.to_string());
    std::fs::read_to_string(password_file)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn auth_session_ttl_secs() -> i64 {
    env::var("CURVINE_WEB_SESSION_TTL_SECS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_AUTH_SESSION_TTL_SECS)
}

fn auth_cookie_secure() -> bool {
    env::var("CURVINE_WEB_COOKIE_SECURE")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut diff = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        let a = left.get(index).copied().unwrap_or_default();
        let b = right.get(index).copied().unwrap_or_default();
        diff |= (a ^ b) as usize;
    }
    diff == 0
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs() as i64)
        .unwrap_or_default()
}

fn new_session_token() -> String {
    Uuid::new_v4().to_string()
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    for item in cookies.split(';') {
        let trimmed = item.trim();
        if let Some((key, value)) = trimmed.split_once('=') {
            if key == name {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn session_cookie(token: &str) -> String {
    format!(
        "{}={}; Path=/; Max-Age={}; HttpOnly; SameSite=Lax{}",
        AUTH_COOKIE_NAME,
        token,
        auth_session_ttl_secs(),
        if auth_cookie_secure() { "; Secure" } else { "" }
    )
}

fn expired_session_cookie() -> String {
    format!(
        "{}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax",
        AUTH_COOKIE_NAME
    )
}

fn is_authenticated(instance: &AdminRouterHandler, headers: &HeaderMap) -> bool {
    let Some(token) = cookie_value(headers, AUTH_COOKIE_NAME) else {
        return false;
    };
    let now = now_secs();
    match instance.auth_sessions.lock() {
        Ok(mut sessions) => {
            sessions.retain(|_, expires_at| *expires_at > now);
            sessions
                .get(&token)
                .map(|expires_at| *expires_at > now)
                .unwrap_or(false)
        }
        Err(_) => false,
    }
}

fn unauthorized_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "success": false,
            "data": null,
            "error": { "code": "UNAUTHORIZED", "message": "login required" }
        })),
    )
        .into_response()
}

/// Whether a request path must carry a valid session before it is served.
///
/// Auth endpoints (login/session/logout) stay open so the UI can authenticate.
/// Every data-bearing API is protected, including the legacy non-versioned
/// routes (`/api/overview`, `/api/browse`, `/api/config`, `/api/workers`,
/// `/api/block_locations`) and the human-readable `/report`, which previously
/// leaked filesystem listings and full cluster config to anonymous callers.
/// `/metrics` is intentionally left open for Prometheus scraping; restrict it at
/// the network layer if needed.
fn path_requires_auth(path: &str) -> bool {
    if path.starts_with("/api/v1/auth") {
        return false;
    }
    path.starts_with("/api/") || path == "/report"
}

async fn require_auth(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if !path_requires_auth(path) || is_authenticated(&instance, &headers) {
        return next.run(request).await;
    }
    unauthorized_response()
}

fn api_success(data: Value) -> Json<Value> {
    Json(json!({
        "success": true,
        "data": data,
        "error": null
    }))
}

fn api_error_response(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "success": false,
            "data": null,
            "error": { "code": code, "message": message.into() }
        })),
    )
        .into_response()
}

fn journal_addr_for_id(conf: &ClusterConf, node_id: u64) -> Option<String> {
    conf.journal
        .journal_addrs
        .iter()
        .find(|peer| peer.id == node_id)
        .map(|peer| format!("{}:{}", peer.hostname, peer.port))
}

async fn raft_ping_status(
    raft_client: &RaftClient,
    node_id: u64,
) -> (bool, Option<u64>, Option<String>) {
    match timeout(
        Duration::from_secs(MASTER_HA_PING_TIMEOUT_SECS),
        raft_client.ping(node_id),
    )
    .await
    {
        Ok(Ok(response)) => (true, Some(response.leader_id), None),
        Ok(Err(err)) => (false, None, Some(err.to_string())),
        Err(_) => (
            false,
            None,
            Some(format!(
                "raft ping timed out after {MASTER_HA_PING_TIMEOUT_SECS}s"
            )),
        ),
    }
}

async fn master_ha_payload(instance: &AdminRouterHandler, master_info: &MasterInfo) -> Value {
    let conf = &instance.conf;
    let active_master = master_info.active_master.trim().to_string();
    let active_raft_id = master_info
        .active_raft_id
        .or_else(|| target_master_node_id(conf, &active_master));
    let active_master_configured = active_raft_id
        .and_then(|id| configured_master_addr_for_raft_id(conf, id))
        .unwrap_or_else(|| active_master.clone());
    let display_hostname = resolve_display_hostname(conf, &active_master);
    let configured_master = display_inet_addr_with_hint(&conf.master_addr(), &display_hostname);
    let master_addrs = configured_master_addrs(conf);
    let failover_supported = conf.journal.enable && master_addrs.len() > 1;
    let raft_client = RaftClient::from_conf(instance.rt.clone(), &instance.conf.journal);
    let mut nodes = Vec::with_capacity(master_addrs.len());

    for addr in &master_addrs {
        let addr_text = addr.to_string();
        let display_addr = display_inet_addr_with_hint(addr, &display_hostname);
        let raft_id = target_master_node_id(conf, &addr_text);
        let journal_addr = raft_id
            .and_then(|id| journal_addr_for_id(conf, id))
            .unwrap_or_default();
        let (reachable, leader_id, reachable_error) = match raft_id {
            Some(id) => raft_ping_status(&raft_client, id).await,
            None => (
                false,
                None,
                Some("master address cannot be mapped to a raft node".to_string()),
            ),
        };
        let is_current = match (active_raft_id, raft_id) {
            (Some(active_id), Some(node_id)) => active_id == node_id,
            _ if active_master.is_empty() => false,
            _ => master_is_active_target(conf, &active_master, active_raft_id, &addr_text, raft_id),
        };
        let role = if active_master.is_empty() {
            "Unknown"
        } else if is_current {
            "Active"
        } else {
            "Standby"
        };

        nodes.push(json!({
            "addr": addr_text,
            "display_addr": display_addr,
            "journal_addr": journal_addr,
            "raft_id": raft_id,
            "raft_leader_id": leader_id,
            "role": role,
            "reachable": reachable,
            "reachable_error": reachable_error,
            "current": is_current,
            "switchable": failover_supported && !is_current && reachable
        }));
    }

    json!({
        "active_master": active_master,
        "active_raft_id": active_raft_id,
        "active_master_configured": active_master_configured,
        "active_master_display": if active_master.is_empty() {
            String::new()
        } else {
            display_addr_text_for_cluster(&active_master_configured, conf, &active_master)
        },
        "current_master": active_master_configured.clone(),
        "configured_master": configured_master,
        "local_hostname": display_hostname,
        "journal_nodes": master_info.journal_nodes.clone(),
        "nodes": nodes,
        "failover_supported": failover_supported,
        "failover_error_codes": {
            "INVALID_TARGET_MASTER": "target_master is required",
            "MASTER_NOT_FOUND": "target_master is not configured in this cluster",
            "MASTER_ALREADY_ACTIVE": "target_master is already the active master",
            "MASTER_LEADER_UNAVAILABLE": "cluster currently has no active leader",
            "TARGET_MASTER_UNREACHABLE": "target master raft node cannot be reached",
            "MASTER_FAILOVER_FAILED": "raft leadership transfer request failed",
            "MASTER_FAILOVER_TARGET_MISMATCH": "target master did not become active before timeout"
        }
    })
}

fn target_master_node_id(conf: &ClusterConf, target_master: &str) -> Option<u64> {
    let target = target_master.trim();
    if target.is_empty() {
        return None;
    }

    let Ok(addr) = InetAddr::from_str(target) else {
        return None;
    };
    if let Some(id) = configured_master_addrs(conf)
        .iter()
        .find(|configured| {
            configured.to_string() == target
                || (configured.port == addr.port
                    && hosts_match_for_cluster_mapping(&configured.hostname, &addr.hostname))
        })
        .and_then(|configured| journal_peer_id_for_host(conf, &configured.hostname))
    {
        return Some(id);
    }

    journal_peer_id_for_host(conf, &addr.hostname)
}

fn new_task_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or_default();
    format!("resync-{millis}")
}

fn update_resync_task<F>(
    tasks: &Arc<Mutex<HashMap<String, ResyncTaskState>>>,
    task_id: &str,
    updater: F,
) where
    F: FnOnce(&mut ResyncTaskState),
{
    if let Ok(mut map) = tasks.lock() {
        if let Some(task) = map.get_mut(task_id) {
            updater(task);
            task.updated_at = LocalTime::now_datetime();
        }
    }
}

fn is_cv_dir_missing(err: &FsError) -> bool {
    let err_text = err.to_string();
    matches!(err.kind(), ErrorKind::FileNotFound | ErrorKind::Expired)
        || err_text.contains("No such file or directory")
        || err_text.contains("not exists")
        || err_text.contains("not found")
}

fn is_recoverable_download_missing(err: &FsError) -> bool {
    is_cv_dir_missing(err) || err.to_string().contains("Not found block for pos 0")
}

fn map_ufs_path_to_cv(path: &str, mounts: &[MountInfo]) -> Option<String> {
    for mount in mounts {
        let ufs_root = mount.ufs_path.trim_end_matches('/');
        if path == ufs_root {
            return Some(mount.cv_path.clone());
        }
        if let Some(suffix) = path.strip_prefix(&format!("{}/", ufs_root)) {
            return Some(format!(
                "{}/{}",
                mount.cv_path.trim_end_matches('/'),
                suffix.trim_start_matches('/')
            ));
        }
    }
    None
}

fn display_cv_path(path: &str, mounts: &[MountInfo]) -> String {
    map_ufs_path_to_cv(path, mounts).unwrap_or_else(|| path.to_string())
}

async fn merged_storage_state(
    client: &UnifiedFileSystem,
    entry: &FileStatus,
    cv_path_text: &str,
) -> StorageState {
    if entry.path.contains("://") {
        let cv_path = match FsPath::from_str(cv_path_text) {
            Ok(value) => value,
            Err(_) => return StorageState::Ufs,
        };
        match client.cv().get_status(&cv_path).await {
            Ok(status) => status.storage_policy.state,
            Err(_) => StorageState::Ufs,
        }
    } else {
        entry.storage_policy.state
    }
}

fn job_state_name(state: JobTaskState) -> &'static str {
    match state {
        JobTaskState::Pending => "Pending",
        JobTaskState::Loading => "Loading",
        JobTaskState::Completed => "Completed",
        JobTaskState::Failed => "Failed",
        JobTaskState::Canceled => "Canceled",
        JobTaskState::UNKNOWN => "Unknown",
    }
}

/// True when a cached job payload is already in a terminal state (by `done` or state name).
fn cached_job_payload_is_terminal(cached: &Value) -> bool {
    if cached
        .get("done")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return true;
    }
    let state = cached
        .get("state")
        .or_else(|| cached.get("status"))
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        state.as_str(),
        "completed" | "complete" | "done" | "failed" | "fail" | "canceled" | "cancelled" | "cancel"
    )
}

fn parse_job_state(value: &str) -> Option<JobTaskState> {
    match value.to_ascii_lowercase().as_str() {
        "pending" => Some(JobTaskState::Pending),
        "loading" | "running" => Some(JobTaskState::Loading),
        "completed" | "complete" | "done" => Some(JobTaskState::Completed),
        "failed" | "fail" => Some(JobTaskState::Failed),
        "canceled" | "cancelled" | "cancel" => Some(JobTaskState::Canceled),
        "unknown" => Some(JobTaskState::UNKNOWN),
        _ => None,
    }
}

fn job_payload_state_name(payload: &Value) -> String {
    payload
        .get("state")
        .or_else(|| payload.get("status"))
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn job_payload_failed_relevant(payload: &Value) -> bool {
    if parse_job_state(&job_payload_state_name(payload)) == Some(JobTaskState::Failed) {
        return true;
    }
    for key in ["failed_files", "failed_tasks"] {
        if payload.get(key).and_then(Value::as_u64).unwrap_or(0) > 0 {
            return true;
        }
    }
    payload
        .get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks.iter().any(|task| {
                parse_job_state(&job_payload_state_name(task)) == Some(JobTaskState::Failed)
            })
        })
        .unwrap_or(false)
}

fn include_job_in_list(payload: &Value, failed_only: bool) -> bool {
    !failed_only || job_payload_failed_relevant(payload)
}

/// Whether a job payload's state matches an API list filter.
/// `running` / `active` include both Pending and Loading jobs.
fn job_matches_state_filter(state_name: &str, filter: &str) -> bool {
    let filter = filter.to_ascii_lowercase();
    if filter.is_empty() || filter == "all" {
        return true;
    }
    let state_name = state_name.to_ascii_lowercase();
    match filter.as_str() {
        "running" | "active" => matches!(
            parse_job_state(&state_name),
            Some(JobTaskState::Pending | JobTaskState::Loading)
        ),
        _ => match (parse_job_state(&state_name), parse_job_state(&filter)) {
            (Some(state), Some(expected)) => state == expected,
            _ => state_name == filter,
        },
    }
}

/// State filter passed to master list RPC. `running` is applied locally (Pending + Loading).
fn master_list_state_filter(filter: &str) -> Option<JobTaskState> {
    let filter = filter.to_ascii_lowercase();
    if filter.is_empty() || filter == "all" || filter == "running" || filter == "active" {
        return None;
    }
    parse_job_state(&filter)
}

fn retain_jobs_matching_state_filter(items: &mut Vec<Value>, filter: &str) {
    if filter.is_empty() || filter.eq_ignore_ascii_case("all") {
        return;
    }
    items.retain(|item| job_matches_state_filter(&job_payload_state_name(item), filter));
}

fn parse_job_source_type(value: &str) -> JobSourceType {
    match value.to_ascii_lowercase().as_str() {
        "fs_mode_auto" | "fs_mode" | "auto" => JobSourceType::FsModeAuto,
        _ => JobSourceType::Manual,
    }
}

fn job_create_time_ms(status: &JobStatus) -> i64 {
    status
        .tasks
        .iter()
        .map(|task| task.create_time)
        .filter(|time| *time > 0)
        .min()
        .unwrap_or(status.progress.update_time)
}

fn finalize_job_display_name(payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    let source_type = object
        .get("source_type")
        .and_then(|value| value.as_str())
        .map(parse_job_source_type)
        .unwrap_or(JobSourceType::Manual);
    let source_path = object
        .get("source_path")
        .or_else(|| object.get("path"))
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let create_time_ms = object
        .get("create_time_ms")
        .and_then(|value| value.as_i64())
        .filter(|value| *value > 0)
        .or_else(|| {
            object
                .get("update_time_ms")
                .and_then(|value| value.as_i64())
                .filter(|value| *value > 0)
        })
        .unwrap_or(0);
    let submitter = object
        .get("submitter")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    let display_name = build_job_display_name(source_type, source_path, create_time_ms, submitter);
    object.insert("display_name".to_string(), json!(display_name));
}

fn job_progress_percent(status: &JobStatus) -> f64 {
    if status.progress.total_size > 0 {
        (status.progress.loaded_size as f64 / status.progress.total_size as f64 * 100.0)
            .clamp(0.0, 100.0)
    } else if status.state == JobTaskState::Completed {
        100.0
    } else {
        0.0
    }
}

fn job_status_payload(status: JobStatus) -> Value {
    let progress = job_progress_percent(&status);
    let mut total_files = status.total_files;
    let mut completed_files = status.completed_files;
    let mut failed_files = status.failed_files;
    let mut running_files = status.running_files;
    if total_files == 0 && status.progress.total_size > 0 {
        total_files = 1;
        if status.state == JobTaskState::Completed {
            completed_files = 1;
        } else if status.state == JobTaskState::Failed {
            failed_files = 1;
        } else if !status.state.is_finish() {
            running_files = 1;
        }
    }
    let create_time_ms = job_create_time_ms(&status);
    let display_name = build_job_display_name(
        status.source_type,
        &status.source_path,
        create_time_ms,
        None,
    );
    json!({
        "job_id": status.job_id,
        "id": status.job_id,
        "display_name": display_name,
        "path": status.source_path,
        "source_path": status.source_path,
        "target_path": status.target_path,
        "state": job_state_name(status.state),
        "status": job_state_name(status.state),
        "message": status.progress.message,
        "total_size": status.progress.total_size,
        "loaded_size": status.progress.loaded_size,
        "progress": progress,
        "total_files": total_files,
        "completed_files": completed_files,
        "failed_files": failed_files,
        "running_files": running_files,
        "pending_files": status.pending_files,
        "loading_files": status.loading_files,
        "source_type": status.source_type.as_str(),
        "created_by": match status.source_type {
            JobSourceType::Manual => "User",
            JobSourceType::FsModeAuto => "UfsLoader",
        },
        "trigger_event": match status.source_type {
            JobSourceType::Manual => "Manual",
            JobSourceType::FsModeAuto => "CompleteFile / Rename",
        },
        "tasks": status.tasks.into_iter().map(|task| json!({
            "task_id": task.task_id,
            "source_path": task.source_path,
            "target_path": task.target_path,
            "worker": task.worker,
            "state": job_state_name(task.state),
            "status": job_state_name(task.state),
            "message": task.progress.message,
            "total_size": task.progress.total_size,
            "loaded_size": task.progress.loaded_size,
            "update_time_ms": task.update_time,
            "create_time_ms": task.create_time,
        })).collect::<Vec<_>>(),
        "update_time_ms": status.progress.update_time,
        "create_time_ms": create_time_ms,
        "done": status.state.is_finish(),
    })
}

fn transfer_kind_name(kind: TransferKind) -> &'static str {
    match kind {
        TransferKind::Load => "Load",
        TransferKind::Export => "Export",
    }
}

fn transfer_kind_from_name(name: &str) -> FsResult<TransferKind> {
    match name.trim().to_lowercase().as_str() {
        "load" | "transfer_load" => Ok(TransferKind::Load),
        "export" | "transfer_export" => Ok(TransferKind::Export),
        _ => err_box!("unsupported transfer kind: {}", name),
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

fn transfer_state_from_name(name: &str) -> FsResult<TransferState> {
    match name.trim().to_lowercase().as_str() {
        "pending" => Ok(TransferState::Pending),
        "planning" => Ok(TransferState::Planning),
        "dispatching" => Ok(TransferState::Dispatching),
        "running" => Ok(TransferState::Running),
        "canceling" => Ok(TransferState::Canceling),
        "completed" => Ok(TransferState::Completed),
        "failed" => Ok(TransferState::Failed),
        "canceled" => Ok(TransferState::Canceled),
        "partial_success" | "partialsuccess" | "partial" => Ok(TransferState::PartialSuccess),
        _ => err_box!("unsupported transfer state: {}", name),
    }
}

fn transfer_task_state_name(state: i32) -> &'static str {
    match state {
        1 => "Pending",
        2 => "Running",
        3 => "Completed",
        4 => "Failed",
        5 => "Canceled",
        6 => "Stale",
        _ => "Unknown",
    }
}

fn transfer_progress_percent(state: TransferState, total_size: i64, loaded_size: i64) -> f64 {
    if total_size > 0 {
        (loaded_size as f64 / total_size as f64 * 100.0).clamp(0.0, 100.0)
    } else if state == TransferState::Completed {
        100.0
    } else {
        0.0
    }
}

fn transfer_actor_name(name: &str) -> String {
    let mut out = String::new();
    for ch in name.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "web".to_string()
    } else {
        trimmed.chars().take(16).collect()
    }
}

fn transfer_display_name(submitter: &str, source_path: &str, create_time_ms: i64) -> String {
    let actor = if submitter.trim().is_empty() {
        "web".to_string()
    } else {
        transfer_actor_name(submitter)
    };
    format!(
        "transfer_{}_{}_{}",
        actor,
        format_job_timestamp_ms(create_time_ms),
        job_path_slug(source_path)
    )
}

fn extend_json_object(base: &mut Value, extra: &Value) {
    let Some(object) = base.as_object_mut() else {
        return;
    };
    let Some(fields) = extra.as_object() else {
        return;
    };
    for (key, value) in fields {
        object.insert(key.clone(), value.clone());
    }
}

fn transfer_counter_fields(
    summary: Option<&TransferTaskSummaryProto>,
    state: TransferState,
    total_size: i64,
) -> Value {
    if let Some(summary) = summary {
        return json!({
            "total_files": summary.pending + summary.running + summary.completed + summary.failed + summary.canceled + summary.stale,
            "completed_files": summary.completed,
            "failed_files": summary.failed,
            "running_files": summary.pending + summary.running,
            "pending_files": summary.pending,
            "loading_files": summary.running,
        });
    }

    let mut total_files = 0_u64;
    let mut completed_files = 0_u64;
    let mut failed_files = 0_u64;
    let mut running_files = 0_u64;
    let mut pending_files = 0_u64;
    let mut loading_files = 0_u64;
    if total_size > 0 {
        total_files = 1;
        match state {
            TransferState::Completed => completed_files = 1,
            TransferState::Failed | TransferState::PartialSuccess => failed_files = 1,
            TransferState::Pending => {
                pending_files = 1;
                running_files = 1;
            }
            TransferState::Planning
            | TransferState::Dispatching
            | TransferState::Running
            | TransferState::Canceling => {
                loading_files = 1;
                running_files = 1;
            }
            TransferState::Canceled => {}
        }
    }
    json!({
        "total_files": total_files,
        "completed_files": completed_files,
        "failed_files": failed_files,
        "running_files": running_files,
        "pending_files": pending_files,
        "loading_files": loading_files,
    })
}

fn transfer_task_payload(task: TransferTaskStatusProto) -> Value {
    let state = transfer_task_state_name(task.state);
    let worker = if task.worker_session_id.is_empty() {
        format!("worker-{}", task.worker_id)
    } else {
        task.worker_session_id.clone()
    };
    json!({
        "task_id": task.task_id,
        "source_path": task.source_path,
        "target_path": task.target_path,
        "worker": worker,
        "worker_id": task.worker_id,
        "worker_session_id": task.worker_session_id,
        "state": state,
        "status": state,
        "message": task.progress.message,
        "retry_count": task.retry_count,
        "total_size": task.progress.total_size,
        "loaded_size": task.progress.loaded_size,
        "update_time_ms": task.updated_at,
    })
}

fn transfer_job_status_payload(status: TransferJobStatusProto) -> Value {
    let kind = TransferKind::from(status.kind);
    let state = TransferState::from(status.state);
    let source_path = status.source_path;
    let target_path = status.target_path;
    let submitter = status.submitter;
    let tenant = status.tenant;
    let progress = status.progress;
    let create_time_ms = status.created_at;
    let update_time_ms = status.updated_at;
    let mut payload = json!({
        "job_id": status.job_id,
        "id": status.job_id,
        "run_id": status.run_id,
        "display_name": transfer_display_name(&submitter, &source_path, create_time_ms),
        "path": source_path,
        "source_path": source_path,
        "target_path": target_path,
        "kind": transfer_kind_name(kind),
        "state": transfer_state_name(state),
        "status": transfer_state_name(state),
        "message": progress.message,
        "submitter": submitter,
        "tenant": tenant,
        "owner": status.owner.unwrap_or_default(),
        "total_size": progress.total_size,
        "loaded_size": progress.loaded_size,
        "progress": transfer_progress_percent(state, progress.total_size, progress.loaded_size),
        "create_time_ms": create_time_ms,
        "update_time_ms": update_time_ms,
        "done": state.is_terminal(),
        "tasks": []
    });
    extend_json_object(
        &mut payload,
        &transfer_counter_fields(None, state, progress.total_size),
    );
    payload
}

fn transfer_status_payload(status: GetTransferStatusResponse) -> Value {
    let kind = TransferKind::from(status.kind.unwrap_or(TransferKind::Load as i32));
    let state = TransferState::from(status.state);
    let source_path = status.source_path.unwrap_or_default();
    let target_path = status.target_path.unwrap_or_default();
    let submitter = status.submitter.unwrap_or_default();
    let tenant = status.tenant.unwrap_or_default();
    let create_time_ms = status.created_at.unwrap_or(status.progress.update_time);
    let update_time_ms = status.updated_at.unwrap_or(status.progress.update_time);
    let tasks = status
        .tasks
        .into_iter()
        .map(transfer_task_payload)
        .collect::<Vec<_>>();
    let mut payload = json!({
        "job_id": status.job_id,
        "id": status.job_id,
        "run_id": status.run_id,
        "display_name": transfer_display_name(&submitter, &source_path, create_time_ms),
        "path": source_path,
        "source_path": source_path,
        "target_path": target_path,
        "kind": transfer_kind_name(kind),
        "state": transfer_state_name(state),
        "status": transfer_state_name(state),
        "message": status.progress.message,
        "submitter": submitter,
        "tenant": tenant,
        "owner": status.owner.unwrap_or_default(),
        "lease_epoch": status.lease_epoch.unwrap_or(0),
        "lease_expire_at": status.lease_expire_at.unwrap_or(0),
        "cv_metadata_epoch": status.cv_metadata_epoch.unwrap_or(0),
        "total_size": status.progress.total_size,
        "loaded_size": status.progress.loaded_size,
        "progress": transfer_progress_percent(state, status.progress.total_size, status.progress.loaded_size),
        "create_time_ms": create_time_ms,
        "update_time_ms": update_time_ms,
        "done": state.is_terminal(),
        "tasks": tasks,
        "next_page_token": status.next_page_token,
    });
    extend_json_object(
        &mut payload,
        &transfer_counter_fields(
            status.task_summary.as_ref(),
            state,
            status.progress.total_size,
        ),
    );
    payload
}

fn transfer_submit_payload(
    response: SubmitTransferResponse,
    source_path: String,
    target_path: String,
    kind: TransferKind,
    submitter: String,
) -> Value {
    let state = TransferState::from(response.state);
    let now_ms = LocalTime::mills() as i64;
    let mut payload = json!({
        "job_id": response.job_id,
        "id": response.job_id,
        "run_id": response.run_id,
        "display_name": transfer_display_name(&submitter, &source_path, now_ms),
        "path": source_path,
        "source_path": source_path,
        "target_path": target_path,
        "kind": transfer_kind_name(kind),
        "state": transfer_state_name(state),
        "status": transfer_state_name(state),
        "message": "",
        "submitter": submitter,
        "total_size": 0,
        "loaded_size": 0,
        "progress": if state == TransferState::Completed { 100.0 } else { 0.0 },
        "create_time_ms": now_ms,
        "update_time_ms": now_ms,
        "done": state.is_terminal(),
        "tasks": []
    });
    extend_json_object(&mut payload, &transfer_counter_fields(None, state, 0));
    payload
}

fn merge_cached_job_metadata(payload: &mut Value, cached: Option<&Value>) {
    let Some(cached) = cached else {
        return;
    };
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    let auto_job = object
        .get("source_type")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains("fs_mode")
        || object
            .get("source_type")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains("auto");
    for key in [
        "source_type",
        "trigger_event",
        "created_by",
        "submitter",
        "create_time_ms",
    ] {
        if let Some(value) = cached.get(key) {
            if key == "submitter" {
                if auto_job {
                    continue;
                }
                if value.as_str().is_some_and(|name| !name.trim().is_empty()) {
                    object.insert(key.to_string(), value.clone());
                }
                continue;
            }
            object.insert(key.to_string(), value.clone());
        }
    }
    finalize_job_display_name(payload);
}

fn remember_load_job(
    history: &Arc<Mutex<VecDeque<String>>>,
    cache: &Arc<Mutex<HashMap<String, Value>>>,
    job_id: String,
) {
    if let Ok(mut jobs) = history.lock() {
        if let Some(index) = jobs.iter().position(|item| item == &job_id) {
            jobs.remove(index);
        }
        jobs.push_front(job_id);
        while jobs.len() > 100 {
            if let Some(evicted) = jobs.pop_back() {
                if let Ok(mut map) = cache.lock() {
                    map.remove(&evicted);
                }
            }
        }
    }
}

fn is_cv_to_ufs_sync(source_path: &str, target_path: &str) -> bool {
    let Ok(source) = FsPath::from_str(source_path) else {
        return false;
    };
    let Ok(target) = FsPath::from_str(target_path) else {
        return false;
    };
    source.is_cv() && !target.is_cv()
}

fn is_cv_to_ufs_sync_payload(payload: &Value) -> bool {
    let source_path = payload
        .get("source_path")
        .or_else(|| payload.get("path"))
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let target_path = payload
        .get("target_path")
        .or_else(|| payload.get("ufs_path"))
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    is_cv_to_ufs_sync(source_path, target_path)
}

fn cache_load_job(cache: &Arc<Mutex<HashMap<String, Value>>>, job_id: &str, payload: Value) {
    if let Ok(mut map) = cache.lock() {
        map.insert(job_id.to_string(), payload);
    }
}

// Default budget for lightweight, single-job RPCs (status/submit).
const JOB_RPC_TIMEOUT_SECS: u64 = 5;
// Budget for cancel RPCs: the master fans out to every assigned worker, which
// multiplies latency. Even after making the fan-out concurrent on the master
// side, we keep a generous client-side limit so a slow cluster does not leave
// the user with a spinner forever.
const JOB_CANCEL_RPC_TIMEOUT_SECS: u64 = 30;
// Larger budget for job *listing* RPCs: with `include_tasks=true` the master has
// to assemble per-task detail for many jobs, which can exceed the single-job
// budget under load. A too-tight timeout here makes the Jobs page silently fall
// back to stale cached data, so give listing more room.
const JOB_LIST_RPC_TIMEOUT_SECS: u64 = 20;

async fn with_job_timeout<T>(
    future: impl std::future::Future<Output = FsResult<T>>,
) -> FsResult<T> {
    with_job_timeout_secs(JOB_RPC_TIMEOUT_SECS, future).await
}

async fn with_job_timeout_secs<T>(
    secs: u64,
    future: impl std::future::Future<Output = FsResult<T>>,
) -> FsResult<T> {
    match timeout(Duration::from_secs(secs), future).await {
        Ok(result) => result,
        Err(_) => err_box!("RPC request timed out"),
    }
}

impl AdminRouterHandler {
    pub fn new(conf: ClusterConf) -> FsResult<Self> {
        Self::with_rt(conf, Arc::new(Runtime::single()))
    }

    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let unified_fs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
        let observability = Arc::new(ObservabilityService::new(conf.clone(), unified_fs.clone()));
        observability.start(rt.clone());

        Ok(Self {
            conf,
            unified_fs,
            rt,
            start_time: LocalTime::now_datetime(),
            resync_tasks: Arc::new(Mutex::new(HashMap::new())),
            load_jobs: Arc::new(Mutex::new(VecDeque::new())),
            load_job_cache: Arc::new(Mutex::new(HashMap::new())),
            auth_sessions: Arc::new(Mutex::new(HashMap::new())),
            observability,
        })
    }

    fn unified_client(&self, cache_only: bool) -> UnifiedFileSystem {
        let mut client = self.unified_fs.clone();
        if cache_only {
            client.disable_unified();
        }
        client
    }

    fn job_client(&self) -> JobMasterClient {
        JobMasterClient::new(self.unified_fs.fs_client())
    }

    fn transfer_client(&self) -> FsResult<TransferClient> {
        TransferClient::with_rt(&self.conf, self.rt.clone())
    }
}

async fn current_master_info(instance: &AdminRouterHandler) -> FsResult<MasterInfo> {
    match timeout(
        Duration::from_secs(MASTER_INFO_TIMEOUT_SECS),
        instance.unified_fs.get_master_info(),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => err_box!("master info request timed out"),
    }
}

async fn metrics(Extension(instance): Extension<Arc<AdminRouterHandler>>) -> String {
    match current_master_info(&instance).await {
        Ok(info) => format!(
            "capacity {}
available {}
fs_used {}
inode_dir_num {}
inode_file_num {}
live_workers {}
lost_workers {}
",
            info.capacity,
            info.available,
            info.fs_used,
            info.inode_dir_num,
            info.inode_file_num,
            info.live_workers.len(),
            info.lost_workers.len()
        ),
        Err(err) => format!(
            "web_master_info_error {}
",
            err
        ),
    }
}

async fn report(Extension(instance): Extension<Arc<AdminRouterHandler>>) -> String {
    match current_master_info(&instance).await {
        Ok(info) => format!(
            "Curvine cluster summary:
    available: {}
    capacity: {}
    fs_used: {}
    dir_total: {}
    files_total: {}
    live_workers: {}
    lost_workers: {}
",
            info.available,
            info.capacity,
            info.fs_used,
            info.inode_dir_num,
            info.inode_file_num,
            info.live_workers.len(),
            info.lost_workers.len()
        ),
        Err(err) => format!("Curvine cluster summary unavailable: {}", err),
    }
}

async fn overview(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let conf = &instance.conf;
    let start_time = &instance.start_time;
    let master_info = current_master_info(&instance).await?;

    let expected_capacity = master_info.available + master_info.fs_used;
    if master_info.capacity != expected_capacity {
        log::warn!(
            "Capacity inconsistency detected: capacity={}, available={}, fs_used={}, expected_capacity={}",
            master_info.capacity,
            master_info.available,
            master_info.fs_used,
            expected_capacity
        );
    }

    let active_master = master_info.active_master.trim().to_string();
    let master_state = if active_master.is_empty() {
        "Unknown"
    } else {
        "Active"
    };

    let res = Json(json!({
        "cluster_id": conf.cluster_id,
        "master_addr": display_master_endpoint(conf, &active_master),
        "active_master": active_master,
        "active_master_display": if active_master.is_empty() {
            String::new()
        } else {
            display_addr_text_for_cluster(&active_master, conf, &active_master)
        },
        "local_hostname": resolve_display_hostname(conf, &active_master),
        "journal_nodes": master_info.journal_nodes.clone(),
        "start_time": start_time,
        "live_workers": master_info.live_workers.len(),
        "lost_workers": master_info.lost_workers.len(),
        "available": master_info.available,
        "capacity": master_info.capacity,
        "fs_used": master_info.fs_used,
        "reserved_bytes": master_info.reserved_bytes,
        "files_total": master_info.inode_file_num.max(0),
        "dir_total": master_info.inode_dir_num.max(0),
        "block_total": master_info.block_num.max(0),
        "master_state": master_state,
    }));
    Ok(res)
}

async fn browse(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Vec<FileStatus>>> {
    let root_path = "/".to_string();
    let path = params.get("path").unwrap_or(&root_path);
    let files = instance
        .unified_fs
        .list_status(&FsPath::from_str(path)?)
        .await?;
    Ok(Json(files))
}

async fn block_locations(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<FileBlocks>> {
    let path = match params.get("path") {
        Some(path) => path,
        None => return err_box!("not found path"),
    };
    let files = instance
        .unified_fs
        .fs_client()
        .get_block_locations(&FsPath::from_str(path)?)
        .await?;
    Ok(Json(files))
}

async fn workers(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<HashMap<String, Vec<WorkerInfo>>>> {
    let master_info = current_master_info(&instance).await?;
    let mut workers = HashMap::new();
    workers.insert("live_workers".to_string(), master_info.live_workers);
    workers.insert("lost_workers".to_string(), master_info.lost_workers);
    Ok(Json(workers))
}

async fn auth_login_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<LoginRequest>,
) -> FsResult<Response> {
    let username = auth_username();
    let Some(password) = auth_password() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "success": false,
                "data": null,
                "error": { "code": "AUTH_NOT_CONFIGURED", "message": "web login password is not configured" }
            })),
        )
            .into_response());
    };

    if payload.username != username || !constant_time_eq(&payload.password, &password) {
        return Ok((
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "success": false,
                "data": null,
                "error": { "code": "INVALID_CREDENTIALS", "message": "invalid username or password" }
            })),
        )
            .into_response());
    }

    let token = new_session_token();
    let expires_at = now_secs() + auth_session_ttl_secs();
    if let Ok(mut sessions) = instance.auth_sessions.lock() {
        sessions.insert(token.clone(), expires_at);
    }

    Ok((
        StatusCode::OK,
        [(header::SET_COOKIE, session_cookie(&token))],
        Json(json!({
            "success": true,
            "data": { "username": payload.username, "expires_at": expires_at },
            "error": null
        })),
    )
        .into_response())
}

async fn auth_session_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    headers: HeaderMap,
) -> FsResult<Json<Value>> {
    let authenticated = is_authenticated(&instance, &headers);
    Ok(api_success(json!({
        "authenticated": authenticated,
        "username": if authenticated { Value::String(auth_username()) } else { Value::Null },
        "configured": auth_password().is_some()
    })))
}

async fn auth_logout_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    headers: HeaderMap,
) -> FsResult<Response> {
    if let Some(token) = cookie_value(&headers, AUTH_COOKIE_NAME) {
        if let Ok(mut sessions) = instance.auth_sessions.lock() {
            sessions.remove(&token);
        }
    }

    Ok((
        StatusCode::OK,
        [(header::SET_COOKIE, expired_session_cookie())],
        Json(json!({ "success": true, "data": { "logged_out": true }, "error": null })),
    )
        .into_response())
}

async fn overview_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let Json(data) = overview(Extension(instance)).await?;
    Ok(api_success(data))
}

async fn master_ha_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let master_info = current_master_info(&instance).await?;
    Ok(api_success(
        master_ha_payload(&instance, &master_info).await,
    ))
}

async fn master_failover_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<MasterFailoverRequest>,
) -> FsResult<Response> {
    let target_master = payload.target_master.trim().to_string();
    if target_master.is_empty() {
        return Ok(api_error_response(
            StatusCode::BAD_REQUEST,
            "INVALID_TARGET_MASTER",
            "target_master is required",
        ));
    }

    let master_info = current_master_info(&instance).await?;
    let ha = master_ha_payload(&instance, &master_info).await;
    let mut active_master = ha
        .get("active_master")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let configured = ha
        .get("nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes.iter().any(|node| {
                node.get("addr").and_then(Value::as_str) == Some(target_master.as_str())
            })
        })
        .unwrap_or(false);

    if !configured {
        return Ok(api_error_response(
            StatusCode::NOT_FOUND,
            "MASTER_NOT_FOUND",
            format!("target_master {target_master} is not configured in this cluster"),
        ));
    }

    if active_master.is_empty() {
        return Ok(api_error_response(
            StatusCode::CONFLICT,
            "MASTER_LEADER_UNAVAILABLE",
            "cluster currently has no active leader",
        ));
    }

    let active_raft_id = ha.get("active_raft_id").and_then(Value::as_u64);
    let Some(target_id) = target_master_node_id(&instance.conf, &target_master) else {
        return Ok(api_error_response(
            StatusCode::NOT_FOUND,
            "MASTER_NOT_FOUND",
            format!("target_master {target_master} cannot be mapped to a raft node"),
        ));
    };

    if master_is_active_target(
        &instance.conf,
        &active_master,
        active_raft_id,
        &target_master,
        Some(target_id),
    ) {
        return Ok(api_error_response(
            StatusCode::CONFLICT,
            "MASTER_ALREADY_ACTIVE",
            format!("target_master {target_master} is already active"),
        ));
    }

    let raft_client = RaftClient::from_conf(instance.rt.clone(), &instance.conf.journal);
    let (target_reachable, _, target_reachable_error) =
        raft_ping_status(&raft_client, target_id).await;
    if !target_reachable {
        let detail = target_reachable_error
            .map(|err| format!("target_master {target_master} is unreachable: {err}"))
            .unwrap_or_else(|| format!("target_master {target_master} is unreachable"));
        return Ok(api_error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "TARGET_MASTER_UNREACHABLE",
            detail,
        ));
    }

    let previous_active = active_master.clone();
    let mut last_error = String::new();
    let overall_deadline = Instant::now() + Duration::from_secs(MASTER_FAILOVER_TOTAL_BUDGET_SECS);

    for attempt in 1..=MASTER_FAILOVER_MAX_ATTEMPTS {
        let transfer_budget = overall_deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(MASTER_FAILOVER_TRANSFER_TIMEOUT_SECS));
        if transfer_budget.is_zero() {
            break;
        }

        let previous_leader_id =
            match timeout(transfer_budget, raft_client.transfer_leader(target_id)).await {
                Ok(Ok(result)) => result.previous_leader_id,
                Ok(Err(err)) => {
                    return Ok(api_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "MASTER_FAILOVER_FAILED",
                        format!("master failover request failed: {err}"),
                    ));
                }
                Err(_) => {
                    return Ok(api_error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "MASTER_FAILOVER_FAILED",
                        format!(
                            "transfer_leader timed out after {}s",
                            transfer_budget.as_secs().max(1)
                        ),
                    ));
                }
            };

        // Bound the confirmation window by wall-clock time (not iteration count), capped by
        // the overall budget, so a slow current_master_info during a brief leaderless window
        // cannot push total latency past the web client timeout.
        let attempt_deadline = (Instant::now()
            + Duration::from_secs(MASTER_FAILOVER_CONFIRM_TIMEOUT_SECS))
        .min(overall_deadline);
        while Instant::now() < attempt_deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
            match current_master_info(&instance).await {
                Ok(info) => {
                    active_master = info.active_master.trim().to_string();
                    if master_is_active_target(
                        &instance.conf,
                        &active_master,
                        info.active_raft_id,
                        &target_master,
                        Some(target_id),
                    ) {
                        return Ok(api_success(json!({
                            "accepted": true,
                            "confirmed": true,
                            "attempts": attempt,
                            "previous_active": previous_active,
                            "target_master": target_master,
                            "active_master": active_master,
                            "previous_leader_id": previous_leader_id,
                            "target_id": target_id,
                            "message": "leadership transfer confirmed"
                        }))
                        .into_response());
                    }
                    last_error.clear();
                }
                Err(err) => {
                    last_error = err.to_string();
                }
            }
        }

        if Instant::now() >= overall_deadline {
            log::warn!(
                "master failover budget ({}s) exhausted after attempt {}, current active: {}, last_error: {}. giving up",
                MASTER_FAILOVER_TOTAL_BUDGET_SECS,
                attempt,
                if active_master.is_empty() { "<none>" } else { active_master.as_str() },
                last_error
            );
            break;
        }

        if attempt < MASTER_FAILOVER_MAX_ATTEMPTS {
            log::warn!(
                "master failover target {} not active after attempt {}, current active: {}, last_error: {}. retrying",
                target_master,
                attempt,
                if active_master.is_empty() { "<none>" } else { active_master.as_str() },
                last_error
            );
        }
    }

    let detail = if last_error.is_empty() {
        format!(
            "target_master {target_master} did not become active; current active master is {}",
            if active_master.is_empty() {
                "<none>"
            } else {
                active_master.as_str()
            }
        )
    } else {
        format!(
            "target_master {target_master} did not become active; current active master is {}; last error: {last_error}",
            if active_master.is_empty() { "<none>" } else { active_master.as_str() }
        )
    };

    Ok(api_error_response(
        StatusCode::CONFLICT,
        "MASTER_FAILOVER_TARGET_MISMATCH",
        detail,
    ))
}

async fn config_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    Ok(api_success(json!(instance.conf)))
}

fn add_cache_state_count(
    state: StorageState,
    cv_only: &mut usize,
    ufs_only: &mut usize,
    both: &mut usize,
) {
    match state {
        StorageState::Both => *both += 1,
        StorageState::Ufs => *ufs_only += 1,
        StorageState::Cv => *cv_only += 1,
    }
}

async fn directory_own_cache_state(
    client: &UnifiedFileSystem,
    path: &FsPath,
) -> FsResult<StorageState> {
    if !path.is_cv() {
        return Ok(StorageState::Ufs);
    }

    let cv_exists = client.cv().get_status(path).await.is_ok();
    let ufs_exists = match client.get_mount(path, RpcCode::GetMountInfo).await {
        Ok(Some((ufs_path, mount))) => match mount.ufs()?.get_status(&ufs_path).await {
            Ok(status) => status.is_dir,
            Err(err) if is_cv_dir_missing(&err) => false,
            Err(err) => return Err(err),
        },
        Ok(None) => false,
        Err(err) if is_cv_dir_missing(&err) => false,
        Err(err) => return Err(err),
    };

    Ok(match (cv_exists, ufs_exists) {
        (true, true) => StorageState::Both,
        (false, true) => StorageState::Ufs,
        _ => StorageState::Cv,
    })
}

async fn directory_cache_state_summary(
    client: &UnifiedFileSystem,
    path: &FsPath,
    mounts: &[MountInfo],
    max_entries: usize,
) -> FsResult<(String, bool, usize)> {
    let mut dirs = VecDeque::from([path.clone()]);
    let mut cv_only = 0usize;
    let mut ufs_only = 0usize;
    let mut both = 0usize;
    let mut scanned_entries = 0usize;
    let mut truncated = false;

    while let Some(dir) = dirs.pop_front() {
        scanned_entries += 1;
        if scanned_entries > max_entries {
            truncated = true;
            break;
        }

        let entries = match client.list_status(&dir).await {
            Ok(value) => value,
            Err(err) if is_cv_dir_missing(&err) => vec![],
            Err(err) => return Err(err),
        };
        if entries.is_empty() {
            add_cache_state_count(
                directory_own_cache_state(client, &dir).await?,
                &mut cv_only,
                &mut ufs_only,
                &mut both,
            );
            continue;
        }

        for entry in entries {
            let entry_path_text = display_cv_path(&entry.path, mounts);
            let entry_path = FsPath::from_str(&entry_path_text)?;
            if entry.is_dir {
                dirs.push_back(entry_path);
                continue;
            }

            scanned_entries += 1;
            if scanned_entries > max_entries {
                truncated = true;
                break;
            }

            add_cache_state_count(
                merged_storage_state(client, &entry, &entry_path_text).await,
                &mut cv_only,
                &mut ufs_only,
                &mut both,
            );
        }

        if truncated {
            break;
        }
    }

    let kinds = [cv_only > 0, ufs_only > 0, both > 0]
        .into_iter()
        .filter(|value| *value)
        .count();

    let mut state = if kinds > 1 {
        "Mixed"
    } else if both > 0 {
        "Cached"
    } else if ufs_only > 0 {
        "UFS only"
    } else {
        "CV only"
    };
    if truncated {
        state = "Too large (scan limit exceeded)";
    }

    Ok((state.to_string(), truncated, scanned_entries))
}

async fn browse_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Value>> {
    let root_path = "/".to_string();
    let path = params.get("path").unwrap_or(&root_path);
    let cache_only = params
        .get("cache_only")
        .map(|value| value == "true" || value == "1")
        .unwrap_or(false);

    let list_client = instance.unified_client(cache_only);
    let fs_path = FsPath::from_str(path)?;
    let mut items = match list_client.list_status(&fs_path).await {
        Ok(items) => items,
        Err(err) if cache_only && is_cv_dir_missing(&err) => vec![],
        Err(err) => return Err(err),
    };

    if !cache_only {
        let summary_client = instance.unified_client(false);
        let mounts = summary_client.get_mount_table().await.unwrap_or_default();
        for item in &mut items {
            if item.path.contains("://") {
                if let Some(cv_path) = map_ufs_path_to_cv(&item.path, &mounts) {
                    if let Ok(cv_status) = summary_client
                        .cv()
                        .get_status(&FsPath::from_str(&cv_path)?)
                        .await
                    {
                        item.storage_policy = cv_status.storage_policy;
                        item.block_size = cv_status.block_size;
                        item.replicas = cv_status.replicas;
                        item.is_complete = cv_status.is_complete;
                    } else {
                        item.storage_policy.state = StorageState::Ufs;
                    }
                    item.path = cv_path;
                } else {
                    item.storage_policy.state = StorageState::Ufs;
                }
            }
        }
    }

    Ok(api_success(json!({
        "items": items,
        "cache_only": cache_only,
        "page": 1,
        "page_size": items.len(),
        "total": items.len()
    })))
}

async fn fs_cache_summary_deprecated_v1() -> Response {
    api_error_response(
        StatusCode::GONE,
        "DEPRECATED_ENDPOINT",
        "GET /api/v1/fs/cache-summary is removed. Use GET /api/v1/fs/diagnostics/cache-summary-deep for operator diagnostics only.",
    )
}

async fn fs_cache_summary_deep_diagnostic_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Response> {
    let path = match params.get("path") {
        Some(path) => path,
        None => {
            return Ok(api_error_response(
                StatusCode::BAD_REQUEST,
                "INVALID_PATH",
                "path is required",
            ));
        }
    };

    let client = instance.unified_client(false);
    let mounts = client.get_mount_table().await.unwrap_or_default();
    let fs_path = FsPath::from_str(path)?;

    let scan = timeout(
        Duration::from_secs(CACHE_SUMMARY_DEEP_TIMEOUT_SECS),
        directory_cache_state_summary(&client, &fs_path, &mounts, CACHE_SUMMARY_DEEP_MAX_ENTRIES),
    )
    .await;

    match scan {
        Ok(Ok((summary, truncated, scanned_entries))) => Ok(api_success(json!({
            "path": path,
            "summary": summary,
            "diagnostic": true,
            "warning": "Deep subtree scan for operators only; not for routine File System UI flows.",
            "truncated": truncated,
            "scanned_entries": scanned_entries,
            "limits": {
                "max_entries": CACHE_SUMMARY_DEEP_MAX_ENTRIES,
                "timeout_secs": CACHE_SUMMARY_DEEP_TIMEOUT_SECS
            }
        }))
        .into_response()),
        Ok(Err(err)) if is_cv_dir_missing(&err) => Ok(api_success(json!({
            "path": path,
            "summary": "CV only",
            "diagnostic": true,
            "warning": "Deep subtree scan for operators only; not for routine File System UI flows.",
            "truncated": false,
            "scanned_entries": 0,
            "limits": {
                "max_entries": CACHE_SUMMARY_DEEP_MAX_ENTRIES,
                "timeout_secs": CACHE_SUMMARY_DEEP_TIMEOUT_SECS
            }
        }))
        .into_response()),
        Ok(Err(err)) => Err(err),
        Err(_) => Ok(api_error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "CACHE_SUMMARY_DEEP_TIMEOUT",
            format!(
                "deep cache summary timed out after {CACHE_SUMMARY_DEEP_TIMEOUT_SECS}s"
            ),
        )),
    }
}

async fn block_locations_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Value>> {
    let Json(data) = block_locations(Extension(instance), Query(params)).await?;
    Ok(api_success(json!(data)))
}

async fn fs_stat_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Value>> {
    let path = match params.get("path") {
        Some(path) => path,
        None => return err_box!("not found path"),
    };
    let status = instance
        .unified_fs
        .fs_client()
        .file_status(&FsPath::from_str(path)?)
        .await?;
    Ok(api_success(json!(status)))
}

async fn fs_mkdir_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<FsPathRequest>,
) -> FsResult<Json<Value>> {
    let create_parent = payload.create_parent.unwrap_or(true);
    let created = instance
        .unified_fs
        .mkdir(&FsPath::from_str(&payload.path)?, create_parent)
        .await?;
    Ok(api_success(json!({ "created": created })))
}

async fn fs_delete_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<FsPathRequest>,
) -> FsResult<Json<Value>> {
    instance
        .unified_fs
        .delete(
            &FsPath::from_str(&payload.path)?,
            payload.recursive.unwrap_or(false),
        )
        .await?;
    Ok(api_success(json!({ "deleted": true })))
}

async fn fs_free_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<FsPathRequest>,
) -> FsResult<Json<Value>> {
    let result = instance
        .unified_fs
        .free(
            &FsPath::from_str(&payload.path)?,
            payload.recursive.unwrap_or(false),
        )
        .await?;

    Ok(api_success(json!({
        "inodes": result.inodes,
        "bytes": result.bytes
    })))
}

/// Build the Curvine -> UFS sync status payload for a single path.
///
/// Returns the inner data object (not wrapped in `api_success`) so it can be
/// served directly by `fs_ufs_sync_v1` or embedded into the upload response.
/// For non-mounted or non-fs_mode paths the returned object has
/// `sync_supported = false`; callers can use that to decide whether to track.
async fn ufs_sync_status_value(instance: &AdminRouterHandler, path: &str) -> FsResult<Value> {
    let fs_path = FsPath::from_str(path)?;
    let client = instance.unified_client(false);
    let Some((_ufs_path, mount)) = client.get_mount(&fs_path, RpcCode::GetMountInfo).await? else {
        return Ok(json!({
            "path": path,
            "mounted": false,
            "sync_supported": false,
            "state": "NotMounted",
            "done": true
        }));
    };

    if !mount.info.is_fs_mode() {
        return Ok(json!({
            "path": path,
            "mounted": true,
            "sync_supported": false,
            "write_type": format!("{:?}", mount.info.write_type),
            "state": "Unsupported",
            "done": true
        }));
    }

    let job_id = CommonUtils::create_job_id(fs_path.full_path());
    match with_job_timeout(instance.job_client().get_job_status(&job_id)).await {
        Ok(status) => {
            if !is_cv_to_ufs_sync(&status.source_path, &status.target_path) {
                return Ok(json!({
                    "path": path,
                    "job_id": status.job_id,
                    "id": status.job_id,
                    "mounted": true,
                    "sync_supported": true,
                    "state": "NotStarted",
                    "status": "NotStarted",
                    "message": "No Curvine to UFS sync job is running for this path",
                    "progress": 0.0,
                    "total_size": 0,
                    "loaded_size": 0,
                    "total_files": 0,
                    "completed_files": 0,
                    "failed_files": 0,
                    "running_files": 0,
                    "done": false
                }));
            }
            remember_load_job(
                &instance.load_jobs,
                &instance.load_job_cache,
                status.job_id.clone(),
            );
            let mut payload = job_status_payload(status);
            if let Some(object) = payload.as_object_mut() {
                object.insert("path".to_string(), json!(path));
                object.insert("mounted".to_string(), json!(true));
                object.insert("sync_supported".to_string(), json!(true));
                object.insert("mount_path".to_string(), json!(mount.info.cv_path.clone()));
                object.insert("ufs_path".to_string(), json!(_ufs_path.full_path()));
            }
            cache_load_job(&instance.load_job_cache, &job_id, payload.clone());
            Ok(payload)
        }
        Err(err) if matches!(err.kind(), ErrorKind::JobNotFound) => Ok(json!({
            "path": path,
            "job_id": job_id,
            "id": job_id,
            "mounted": true,
            "sync_supported": true,
            "state": "NotStarted",
            "status": "NotStarted",
            "message": "Waiting for UFS sync job to be created",
            "progress": 0.0,
            "total_size": 0,
            "loaded_size": 0,
            "total_files": 0,
            "completed_files": 0,
            "failed_files": 0,
            "running_files": 0,
            "done": false
        })),
        Err(err) => Err(err),
    }
}

async fn fs_ufs_sync_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Value>> {
    let path = match params.get("path") {
        Some(path) => path,
        None => return err_box!("not found path"),
    };
    Ok(api_success(ufs_sync_status_value(&instance, path).await?))
}

async fn fs_upload_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> FsResult<Json<Value>> {
    let path = match params.get("path") {
        Some(path) => path,
        None => return err_box!("not found path"),
    };
    let overwrite = params
        .get("overwrite")
        .map(|value| value == "true" || value == "1")
        .unwrap_or(true);
    let client = instance.unified_client(false);

    // Whether the caller wants the UFS-sync status of the freshly written file.
    // For fs_mode mounts, completing the write triggers an automatic Curvine ->
    // UFS export job; returning its (possibly still-pending) status lets the UI
    // keep tracking the sync instead of clearing the panel.
    let sync_ufs = params
        .get("sync_ufs")
        .map(|value| value == "true" || value == "1")
        .unwrap_or(false);

    let mut writer = client.create(&FsPath::from_str(path)?, overwrite).await?;
    writer.write(&body).await?;
    writer.complete().await?;

    let mut response = json!({
        "path": path,
        "bytes": body.len()
    });

    if sync_ufs {
        if let Ok(sync) = ufs_sync_status_value(&instance, path).await {
            let supported = sync
                .get("sync_supported")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if supported {
                if let Some(object) = response.as_object_mut() {
                    object.insert("ufs_sync".to_string(), sync);
                }
            }
        }
    }

    Ok(api_success(response))
}

fn tar_checksum(header: &[u8; 512]) -> u32 {
    header.iter().map(|value| *value as u32).sum()
}

fn write_tar_octal(field: &mut [u8], value: u64) {
    field.fill(0);
    let width = field.len();
    let text = format!("{:0width$o}", value, width = width.saturating_sub(1));
    let bytes = text.as_bytes();
    let start = width.saturating_sub(1 + bytes.len());
    field[start..start + bytes.len()].copy_from_slice(bytes);
    field[width - 1] = 0;
}

fn write_tar_string(field: &mut [u8], value: &str) {
    field.fill(0);
    let bytes = value.as_bytes();
    let len = std::cmp::min(field.len(), bytes.len());
    field[..len].copy_from_slice(&bytes[..len]);
}

fn append_tar_entry(
    archive: &mut Vec<u8>,
    name: &str,
    data: &[u8],
    is_dir: bool,
    mtime: i64,
) -> FsResult<()> {
    let mut entry_name = name.trim_start_matches('/').to_string();
    if is_dir && !entry_name.ends_with('/') {
        entry_name.push('/');
    }
    if entry_name.is_empty() {
        return Ok(());
    }
    if entry_name.len() > 100 {
        log::warn!(
            "skip tar entry longer than 100 bytes while downloading directory: {}",
            entry_name
        );
        return Ok(());
    }

    let mut header = [0u8; 512];
    write_tar_string(&mut header[0..100], &entry_name);
    write_tar_octal(&mut header[100..108], if is_dir { 0o755 } else { 0o644 });
    write_tar_octal(&mut header[108..116], 0);
    write_tar_octal(&mut header[116..124], 0);
    write_tar_octal(
        &mut header[124..136],
        if is_dir { 0 } else { data.len() as u64 },
    );
    write_tar_octal(&mut header[136..148], mtime.max(0) as u64 / 1000);
    for value in &mut header[148..156] {
        *value = b' ';
    }
    header[156] = if is_dir { b'5' } else { b'0' };
    write_tar_string(&mut header[257..263], "ustar");
    write_tar_string(&mut header[263..265], "00");
    let checksum = tar_checksum(&header);
    let checksum_text = format!("{:06o}\0 ", checksum);
    header[148..156].copy_from_slice(checksum_text.as_bytes());

    archive.extend_from_slice(&header);
    if !is_dir {
        archive.extend_from_slice(data);
        let padding = (512 - (data.len() % 512)) % 512;
        archive.extend(std::iter::repeat_n(0, padding));
    }
    Ok(())
}

fn tar_entry_name(root: &FsPath, path: &FsPath) -> String {
    let root_text = root.to_string();
    let path_text = path.to_string();
    let root_name = root.name();
    if path_text == root_text {
        return root_name.to_string();
    }
    match path_text.strip_prefix(&format!("{}/", root_text.trim_end_matches('/'))) {
        Some(suffix) => format!("{}/{}", root_name, suffix.trim_start_matches('/')),
        None => path.name().to_string(),
    }
}

async fn read_file_bytes(client: &UnifiedFileSystem, path: &FsPath) -> FsResult<BytesMut> {
    if client
        .cv()
        .get_status(path)
        .await
        .map(|status| !status.is_dir)
        .unwrap_or(false)
    {
        let cv_result: FsResult<BytesMut> = async {
            let mut reader = client.cv().open(path).await?;
            let total_size = reader.len();
            let chunk_size = client.conf().client.read_chunk_size;
            let mut remaining = total_size;
            let mut content = BytesMut::with_capacity(total_size.max(0) as usize);
            while remaining > 0 {
                let to_read = std::cmp::min(remaining as usize, chunk_size);
                let start = content.len();
                content.resize(start + to_read, 0);
                reader
                    .read_full(&mut content[start..start + to_read])
                    .await?;
                remaining -= to_read as i64;
            }
            reader.complete().await?;
            Ok(content)
        }
        .await;
        match cv_result {
            Ok(content) => return Ok(content),
            Err(err) => log::warn!("download fallback to unified read for {}: {}", path, err),
        }
    }

    let mut reader = client.open(path).await?;
    let total_size = reader.len();
    let chunk_size = client.conf().client.read_chunk_size;
    let mut remaining = total_size;
    let mut content = BytesMut::with_capacity(total_size.max(0) as usize);
    while remaining > 0 {
        let to_read = std::cmp::min(remaining as usize, chunk_size);
        let start = content.len();
        content.resize(start + to_read, 0);
        reader
            .read_full(&mut content[start..start + to_read])
            .await?;
        remaining -= to_read as i64;
    }
    reader.complete().await?;
    Ok(content)
}

async fn list_download_dir(client: &UnifiedFileSystem, dir: &FsPath) -> FsResult<Vec<FileStatus>> {
    match client.list_status(dir).await {
        Ok(entries) => Ok(entries),
        Err(err) if is_cv_dir_missing(&err) => match client.cv().list_status(dir).await {
            Ok(entries) => Ok(entries),
            Err(cv_err) if is_cv_dir_missing(&cv_err) => Ok(vec![]),
            Err(cv_err) => Err(cv_err),
        },
        Err(err) => Err(err),
    }
}

async fn build_directory_tar(client: &UnifiedFileSystem, root: &FsPath) -> FsResult<Vec<u8>> {
    let mounts = client.get_mount_table().await.unwrap_or_default();
    let mut archive = Vec::new();
    let mut dirs = VecDeque::from([root.clone()]);
    let root_status = match client.cv().get_status(root).await {
        Ok(status) => status,
        Err(_) => client.get_status(root).await?,
    };
    append_tar_entry(
        &mut archive,
        &tar_entry_name(root, root),
        &[],
        true,
        root_status.mtime,
    )?;

    while let Some(dir) = dirs.pop_front() {
        let entries = list_download_dir(client, &dir).await?;
        for entry in entries {
            let cv_path_text = display_cv_path(&entry.path, &mounts);
            let cv_path = FsPath::from_str(&cv_path_text)?;
            let entry_name = tar_entry_name(root, &cv_path);
            if entry.is_dir {
                append_tar_entry(&mut archive, &entry_name, &[], true, entry.mtime)?;
                dirs.push_back(cv_path);
            } else {
                match read_file_bytes(client, &cv_path).await {
                    Ok(content) => {
                        append_tar_entry(&mut archive, &entry_name, &content, false, entry.mtime)?
                    }
                    Err(err) if is_recoverable_download_missing(&err) => {
                        log::warn!(
                            "skip unreadable file while downloading directory {}: {}",
                            cv_path,
                            err
                        );
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    archive.extend_from_slice(&[0u8; 1024]);
    Ok(archive)
}

async fn fs_download_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Response> {
    let path = match params.get("path") {
        Some(path) => path,
        None => return err_box!("not found path"),
    };
    let client = instance.unified_client(false);

    let fs_path = FsPath::from_str(path)?;
    let status = match client.cv().get_status(&fs_path).await {
        Ok(status) => status,
        Err(_) => client.get_status(&fs_path).await?,
    };
    let (filename, content_type, body) = if status.is_dir {
        (
            format!("{}.tar", fs_path.name().replace('"', "")),
            "application/x-tar".to_string(),
            Body::from(build_directory_tar(&client, &fs_path).await?),
        )
    } else {
        (
            fs_path.name().replace('"', ""),
            "application/octet-stream".to_string(),
            Body::from(read_file_bytes(&client, &fs_path).await?.freeze()),
        )
    };

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", filename),
            ),
        ],
        body,
    )
        .into_response())
}

async fn workers_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let master_info = current_master_info(&instance).await?;
    let live_workers = master_info.live_workers;
    let blacklist_workers = master_info.blacklist_workers;
    let decommission_workers = master_info.decommission_workers;
    let lost_workers = master_info.lost_workers;
    let total = live_workers.len()
        + blacklist_workers.len()
        + decommission_workers.len()
        + lost_workers.len();

    Ok(api_success(json!({
        "live_workers": live_workers.into_iter().map(worker_json).collect::<Vec<_>>(),
        "blacklist_workers": blacklist_workers.into_iter().map(worker_json).collect::<Vec<_>>(),
        "decommission_workers": decommission_workers.into_iter().map(worker_json).collect::<Vec<_>>(),
        "lost_workers": lost_workers.into_iter().map(worker_json).collect::<Vec<_>>(),
        "total": total
    })))
}

async fn worker_detail_v1(
    Path(worker): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let master_info = current_master_info(&instance).await?;
    for item in master_info.live_workers {
        if worker_matches(&item, &worker) {
            let state = format!("{:?}", item.status);
            return Ok(api_success(worker_payload(item, &state)));
        }
    }
    for item in master_info.blacklist_workers {
        if worker_matches(&item, &worker) {
            let state = format!("{:?}", item.status);
            return Ok(api_success(worker_payload(item, &state)));
        }
    }
    for item in master_info.decommission_workers {
        if worker_matches(&item, &worker) {
            let state = format!("{:?}", item.status);
            return Ok(api_success(worker_payload(item, &state)));
        }
    }
    for item in master_info.lost_workers {
        if worker_matches(&item, &worker) {
            return Ok(api_success(worker_payload(item, "Lost")));
        }
    }

    err_box!("worker not found: {}", worker)
}

fn worker_action_target_status(action: &str) -> FsResult<&'static str> {
    match action.trim().to_lowercase().as_str() {
        "blacklist" | "block" => Ok("Blacklist"),
        "allow" | "unblacklist" | "remove_blacklist" => Ok("Live"),
        "decommission" | "retire" => Ok("Decommission"),
        "recommission" | "remove_decommission" => Ok("Live"),
        action => err_box!("unsupported worker action: {}", action),
    }
}

async fn worker_action_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<WorkerActionRequest>,
) -> FsResult<Json<Value>> {
    let target_status = worker_action_target_status(&payload.action)?;
    let (worker, status) = instance
        .unified_fs
        .fs_client()
        .set_worker_status(payload.worker.trim(), target_status)
        .await?;
    Ok(api_success(worker_payload(worker, &status)))
}

async fn worker_decommission_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<WorkerActionRequest>,
) -> FsResult<Json<Value>> {
    let (worker, status) = instance
        .unified_fs
        .fs_client()
        .set_worker_status(payload.worker.trim(), "Decommission")
        .await?;
    Ok(api_success(worker_payload(worker, &status)))
}

async fn worker_remove_decommission_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<WorkerActionRequest>,
) -> FsResult<Json<Value>> {
    let (worker, status) = instance
        .unified_fs
        .fs_client()
        .set_worker_status(payload.worker.trim(), "Live")
        .await?;
    Ok(api_success(worker_payload(worker, &status)))
}

fn parse_mount_options(payload: &MountRequest) -> FsResult<MountOptions> {
    let write_type = WriteType::try_from(payload.write_type.as_str())?;
    let ttl_ms = DurationUnit::from_str(payload.ttl.as_str())?.as_millis() as i64;
    let ttl_action = if matches!(write_type, WriteType::FsMode) {
        TtlAction::Free
    } else {
        TtlAction::Delete
    };

    let mut opts = MountOptions::builder()
        .update(payload.update)
        .set_properties(payload.properties.clone())
        .write_type(write_type)
        .read_verify_ufs(payload.read_verify_ufs)
        .ttl_ms(ttl_ms)
        .ttl_action(ttl_action);

    if let Some(replicas) = payload.replicas {
        opts = opts.replicas(replicas);
    }
    if let Some(block_size) = payload.block_size.as_ref().filter(|v| !v.trim().is_empty()) {
        opts = opts.block_size(ByteUnit::from_str(block_size.as_str())?.as_byte() as i64);
    }
    if let Some(storage_type) = payload
        .storage_type
        .as_ref()
        .filter(|v| !v.trim().is_empty())
    {
        opts = opts.storage_type(StorageType::try_from(storage_type.as_str())?);
    }
    if let Some(provider) = payload.provider.as_ref().filter(|v| !v.trim().is_empty()) {
        opts = opts.provider(Provider::try_from(provider.as_str())?);
    }
    if let Some(auto_cache) = payload.auto_cache {
        opts = opts.auto_cache(auto_cache);
    }
    if let Some(access_mode) = payload
        .access_mode
        .as_ref()
        .filter(|v| !v.trim().is_empty())
    {
        opts = opts.access_mode(AccessMode::try_from(access_mode.as_str())?);
    }

    Ok(opts.build())
}

async fn mounts_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let items = instance.unified_client(false).get_mount_table().await?;

    Ok(api_success(json!({
        "items": items,
        "page": 1,
        "page_size": items.len(),
        "total": items.len()
    })))
}

async fn mount_create_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<MountRequest>,
) -> FsResult<Json<Value>> {
    let client = instance.unified_client(false);

    if payload.cv_path.trim().is_empty() {
        return err_box!("curvine path is required");
    }
    if payload.ufs_path.trim().is_empty() {
        return err_box!("ufs path is required");
    }

    let normalized_ufs_path = normalize_ufs_path(&payload.ufs_path);
    let ufs_path = FsPath::from_str(&normalized_ufs_path)?;
    let cv_path = FsPath::from_str(payload.cv_path.trim())?;
    let opts = parse_mount_options(&payload)?;

    let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties.clone(), opts.provider)?;
    ufs.list_status(&ufs_path).await?;

    client.mount(&ufs_path, &cv_path, opts).await?;
    Ok(api_success(json!({
        "mounted": true,
        "cv_path": payload.cv_path,
        "ufs_path": normalized_ufs_path
    })))
}

async fn mount_delete_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<MountPathRequest>,
) -> FsResult<Json<Value>> {
    instance
        .unified_client(false)
        .umount(&FsPath::from_str(&payload.cv_path)?)
        .await?;
    Ok(api_success(
        json!({ "deleted": true, "cv_path": payload.cv_path }),
    ))
}

async fn mount_validate_v1(Json(payload): Json<MountValidateRequest>) -> FsResult<Json<Value>> {
    if payload.ufs_path.trim().is_empty() {
        return err_box!("ufs path is required");
    }
    let normalized_ufs_path = normalize_ufs_path(&payload.ufs_path);
    let ufs_path = FsPath::from_str(&normalized_ufs_path)?;
    let provider = match payload.provider.as_ref().filter(|v| !v.trim().is_empty()) {
        Some(provider) => Some(Provider::try_from(provider.as_str())?),
        None => None,
    };
    let ufs = UfsFileSystem::new(&ufs_path, payload.properties.clone(), provider)?;
    let status = ufs.get_status(&ufs_path).await?;
    let entries = if status.is_dir {
        ufs.list_status(&ufs_path).await?.len()
    } else {
        0
    };
    Ok(api_success(json!({
        "valid": true,
        "entries": entries,
        "is_dir": status.is_dir,
        "ufs_path": normalized_ufs_path
    })))
}

async fn mount_resync_start_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Json(payload): Json<MountResyncRequest>,
) -> FsResult<Json<Value>> {
    let client = instance.unified_client(false);

    let cv_path = FsPath::from_str(&payload.cv_path)?;
    if !cv_path.is_cv() {
        return err_box!("resync requires a curvine path, got: {}", payload.cv_path);
    }

    let mount = match client.get_mount_info(&cv_path).await? {
        Some(value) => value,
        None => return err_box!("mount info not found for {}", payload.cv_path),
    };
    if !matches!(mount.write_type, WriteType::FsMode) {
        return err_box!(
            "resync is only allowed for fs_mode mount: {}",
            mount.cv_path
        );
    }

    let task_id = new_task_id();
    let now = LocalTime::now_datetime();
    let task = ResyncTaskState {
        id: task_id.clone(),
        cv_path: payload.cv_path.clone(),
        mount_path: mount.cv_path.clone(),
        write_type: "fs_mode".to_string(),
        status: "queued".to_string(),
        scanned: 0,
        skipped: 0,
        recreated: 0,
        failed: 0,
        pending_dirs: 0,
        message: "queued".to_string(),
        started_at: now.clone(),
        updated_at: now,
        done: false,
        dry_run: payload.dry_run,
    };

    if let Ok(mut tasks) = instance.resync_tasks.lock() {
        tasks.insert(task_id.clone(), task.clone());
    }

    let tasks = instance.resync_tasks.clone();
    let cv_path_text = payload.cv_path.clone();
    tokio::spawn(async move {
        run_mount_resync_task(client, tasks, task_id, cv_path_text, payload.dry_run).await;
    });

    Ok(api_success(json!(task)))
}

async fn mount_resync_status_v1(
    Path(task_id): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    let task = match instance.resync_tasks.lock() {
        Ok(tasks) => tasks.get(&task_id).cloned(),
        Err(_) => None,
    };

    match task {
        Some(task) => Ok(api_success(json!(task))),
        None => err_box!("resync task not found: {}", task_id),
    }
}

async fn run_mount_resync_task(
    fs: UnifiedFileSystem,
    tasks: Arc<Mutex<HashMap<String, ResyncTaskState>>>,
    task_id: String,
    cv_path_text: String,
    dry_run: bool,
) {
    update_resync_task(&tasks, &task_id, |task| {
        task.status = "running".to_string();
        task.message = "starting".to_string();
    });

    let result: FsResult<()> = async {
        let cv_path = FsPath::from_str(&cv_path_text)?;
        let client = fs.fs_client();
        let mount = match client.get_mount_info(&cv_path).await? {
            Some(value) => value,
            None => return err_box!("mount info not found for {}", cv_path),
        };

        if !matches!(mount.write_type, WriteType::FsMode) {
            return err_box!(
                "resync is only allowed for fs_mode mount: {}",
                mount.cv_path
            );
        }

        let ufs_root = FsPath::from_str(&mount.ufs_path)?;
        let ufs = UfsFileSystem::new(&ufs_root, mount.properties.clone(), mount.provider)?;
        let mut queue = VecDeque::from([ufs_root.clone()]);
        update_resync_task(&tasks, &task_id, |task| {
            task.mount_path = mount.cv_path.clone();
            task.pending_dirs = queue.len();
            task.message = format!("scanning {}", ufs_root);
        });

        while let Some(ufs_dir) = queue.pop_front() {
            let ufs_entries = match ufs.list_status(&ufs_dir).await {
                Ok(value) => value,
                Err(err) => {
                    update_resync_task(&tasks, &task_id, |task| {
                        task.failed += 1;
                        task.pending_dirs = queue.len();
                        task.message = format!("failed to list ufs dir {}: {}", ufs_dir, err);
                    });
                    continue;
                }
            };

            let cv_dir = mount.get_cv_path(&ufs_dir)?;
            if !dry_run {
                if let Err(err) = fs.cv().mkdir(&cv_dir, true).await {
                    update_resync_task(&tasks, &task_id, |task| {
                        task.failed += 1;
                        task.pending_dirs = queue.len();
                        task.message = format!("failed to create cv dir {}: {}", cv_dir, err);
                    });
                    continue;
                }
            }

            let cv_entries = match fs.cv().list_status(&cv_dir).await {
                Ok(value) => value,
                Err(err) if is_cv_dir_missing(&err) => vec![],
                Err(err) => {
                    update_resync_task(&tasks, &task_id, |task| {
                        task.failed += 1;
                        task.pending_dirs = queue.len();
                        task.message = format!("failed to list cv dir {}: {}", cv_dir, err);
                    });
                    continue;
                }
            };

            let mut cv_map = HashMap::new();
            for entry in cv_entries {
                cv_map.insert(entry.path.to_string(), entry);
            }

            for ufs_entry in ufs_entries {
                let ufs_path = FsPath::from_str(ufs_entry.path)?;
                if ufs_entry.is_dir {
                    queue.push_back(ufs_path);
                    update_resync_task(&tasks, &task_id, |task| {
                        task.pending_dirs = queue.len();
                        task.message = format!("queued {} directories", queue.len());
                    });
                    continue;
                }

                let ufs_mtime = ufs_entry.mtime;
                let ufs_len = ufs_entry.len;
                let cv_path = mount.get_cv_path(&ufs_path)?;
                let cv_key = cv_path.full_path().to_string();

                let mut recreate = true;
                if let Some(cv_status) = cv_map.get(&cv_key) {
                    let cv_ufs_mtime = cv_status.storage_policy.ufs_mtime;
                    if cv_ufs_mtime == 0 || cv_ufs_mtime == ufs_mtime {
                        recreate = false;
                    } else if !dry_run {
                        if let Err(err) = client.delete(&cv_path, false).await {
                            update_resync_task(&tasks, &task_id, |task| {
                                task.scanned += 1;
                                task.failed += 1;
                                task.pending_dirs = queue.len();
                                task.message = format!("failed to delete {}: {}", cv_path, err);
                            });
                            continue;
                        }
                    }
                }

                if !recreate {
                    update_resync_task(&tasks, &task_id, |task| {
                        task.scanned += 1;
                        task.skipped += 1;
                        task.pending_dirs = queue.len();
                        task.message = format!("skipped {}", cv_path);
                    });
                    continue;
                }

                if !dry_run {
                    let create_opts = mount.get_sync_opts(&fs.conf().client, ufs_mtime, ufs_len);
                    if let Err(err) = client.create_with_opts(&cv_path, create_opts, false).await {
                        update_resync_task(&tasks, &task_id, |task| {
                            task.scanned += 1;
                            task.failed += 1;
                            task.pending_dirs = queue.len();
                            task.message = format!("failed to create {}: {}", cv_path, err);
                        });
                        continue;
                    }
                }

                update_resync_task(&tasks, &task_id, |task| {
                    task.scanned += 1;
                    task.recreated += 1;
                    task.pending_dirs = queue.len();
                    task.message = format!("synced {}", cv_path);
                });
            }
        }

        Ok(())
    }
    .await;

    match result {
        Ok(()) => update_resync_task(&tasks, &task_id, |task| {
            task.status = "completed".to_string();
            task.pending_dirs = 0;
            task.done = true;
            task.message = "completed".to_string();
        }),
        Err(err) => update_resync_task(&tasks, &task_id, |task| {
            task.status = "failed".to_string();
            task.done = true;
            task.message = err.to_string();
        }),
    }
}

async fn fs_ufs_sync_jobs_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(query): Query<UfsSyncJobsQuery>,
) -> FsResult<Json<Value>> {
    let path_prefix = query
        .path
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("/");
    let fs_path = FsPath::from_str(path_prefix)?;
    let normalized_prefix = fs_path.full_path().to_string();
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let include_finished = query.include_finished.unwrap_or(true);
    let state_filter = query.state.as_deref().unwrap_or("").to_lowercase();
    let failed_only = query.failed_only.unwrap_or(false);
    let client = instance.unified_client(false);
    let statuses = match with_job_timeout_secs(
        JOB_LIST_RPC_TIMEOUT_SECS,
        instance
            .job_client()
            .list_job_statuses_with_options(JobListOptions {
                path_prefix: Some(normalized_prefix.clone()),
                limit,
                include_finished,
                state: query.state.as_deref().and_then(master_list_state_filter),
                include_tasks: query.include_tasks.unwrap_or(false),
                // Push failed_only to master so limit applies after failure filtering;
                // include_job_in_list below still filters cache-only rows locally.
                failed_only,
                ..Default::default()
            }),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(err) if err.to_string().contains("Unsupported operation") => Vec::new(),
        Err(err) => return Err(err),
    };

    let mut items = Vec::new();
    let mut seen = HashSet::new();
    for status in statuses {
        if !is_cv_to_ufs_sync(&status.source_path, &status.target_path) {
            continue;
        }
        let source_path = status.source_path.clone();
        let Ok(source_fs_path) = FsPath::from_str(&source_path) else {
            continue;
        };
        let Ok(Some((_ufs_path, mount))) = client
            .get_mount(&source_fs_path, RpcCode::GetMountInfo)
            .await
        else {
            continue;
        };
        if !mount.info.is_fs_mode() {
            continue;
        }
        let mut payload = job_status_payload(status);
        let cached_metadata = payload
            .get("job_id")
            .and_then(|value| value.as_str())
            .and_then(|job_id| {
                instance
                    .load_job_cache
                    .lock()
                    .ok()
                    .and_then(|cache| cache.get(job_id).cloned())
            });
        merge_cached_job_metadata(&mut payload, cached_metadata.as_ref());
        if let Some(object) = payload.as_object_mut() {
            object.insert("path".to_string(), json!(source_path));
            object.insert("mounted".to_string(), json!(true));
            object.insert("sync_supported".to_string(), json!(true));
            object.insert("mount_path".to_string(), json!(mount.info.cv_path.clone()));
            object.insert("ufs_path".to_string(), json!(_ufs_path.full_path()));
        }
        if let Some(job_id) = payload.get("job_id").and_then(|value| value.as_str()) {
            seen.insert(job_id.to_string());
            remember_load_job(
                &instance.load_jobs,
                &instance.load_job_cache,
                job_id.to_string(),
            );
            cache_load_job(&instance.load_job_cache, job_id, payload.clone());
        }
        if !include_job_in_list(&payload, failed_only) {
            continue;
        }
        items.push(payload);
    }

    let cached = instance
        .load_job_cache
        .lock()
        .map(|cache| cache.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    for payload in cached {
        if !is_cv_to_ufs_sync_payload(&payload) {
            continue;
        }
        if let Some(job_id) = payload.get("job_id").and_then(|value| value.as_str()) {
            if seen.contains(job_id) {
                continue;
            }
        }
        let Some(path) = payload.get("path").and_then(|value| value.as_str()) else {
            continue;
        };
        let path_matches = normalized_prefix == "/"
            || path == normalized_prefix
            || path.starts_with(&format!("{}/", normalized_prefix.trim_end_matches('/')));
        if !path_matches {
            continue;
        }
        let Ok(source_fs_path) = FsPath::from_str(path) else {
            continue;
        };
        let Ok(Some((_ufs_path, mount))) = client
            .get_mount(&source_fs_path, RpcCode::GetMountInfo)
            .await
        else {
            continue;
        };
        if !mount.info.is_fs_mode() {
            continue;
        }
        // Apply failed_only here; state filter is applied uniformly below.
        if !include_job_in_list(&payload, failed_only) {
            continue;
        }
        if let Some(job_id) = payload.get("job_id").and_then(|value| value.as_str()) {
            seen.insert(job_id.to_string());
        }
        items.push(payload);
    }

    retain_jobs_matching_state_filter(&mut items, &state_filter);

    let total = items.len();
    Ok(api_success(json!({
        "items": items,
        "path": normalized_prefix,
        "total": total
    })))
}

async fn jobs_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(query): Query<JobListQuery>,
) -> FsResult<Json<Value>> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).clamp(1, JOB_LIST_MAX_FETCH);
    let state_filter = query.state.unwrap_or_default().to_lowercase();
    let include_tasks = query.include_tasks.unwrap_or(false);
    let failed_only = query.failed_only.unwrap_or(false);
    let job_ids: Vec<String> = instance
        .load_jobs
        .lock()
        .map(|jobs| jobs.iter().cloned().collect())
        .unwrap_or_default();
    let cached = instance
        .load_job_cache
        .lock()
        .map(|cache| cache.clone())
        .unwrap_or_default();
    let mut items = Vec::new();
    let mut seen = HashSet::new();

    // Pull a fixed, page-independent window of jobs from the master (the master
    // sorts by update_time desc and caps at 500) and merge locally remembered
    // jobs, then sort + paginate locally below. Tying the fetch `limit` to the
    // page number made `total` change per page and silently truncated deep
    // pages once `page * page_size` exceeded 500.
    if let Ok(statuses) = with_job_timeout_secs(
        JOB_LIST_RPC_TIMEOUT_SECS,
        instance
            .job_client()
            .list_job_statuses_with_options(JobListOptions {
                limit: JOB_LIST_MAX_FETCH,
                offset: 0,
                include_finished: true,
                state: master_list_state_filter(&state_filter),
                include_tasks,
                // Push failed_only to master so the 500-row window is failure-ranked
                // first; include_job_in_list still filters cache-only rows locally.
                failed_only,
                ..Default::default()
            }),
    )
    .await
    {
        for status in statuses {
            let job_id = status.job_id.clone();
            let mut payload = job_status_payload(status);
            merge_cached_job_metadata(&mut payload, cached.get(&job_id));
            remember_load_job(
                &instance.load_jobs,
                &instance.load_job_cache,
                job_id.clone(),
            );
            cache_load_job(&instance.load_job_cache, &job_id, payload.clone());
            seen.insert(job_id);
            if !include_job_in_list(&payload, failed_only) {
                continue;
            }
            items.push(payload);
        }
    }

    for job_id in job_ids {
        if seen.contains(&job_id) {
            continue;
        }
        let item = cached.get(&job_id).cloned().unwrap_or_else(|| {
            json!({
                "job_id": job_id,
                "id": job_id,
                "state": "Unknown",
                "status": "Unknown",
                "message": "status has not been queried yet",
                "progress": 0.0,
                "done": false
            })
        });
        if !include_job_in_list(&item, failed_only) {
            continue;
        }
        items.push(item);
    }

    retain_jobs_matching_state_filter(&mut items, &state_filter);

    // Deterministic order so pages never overlap or reshuffle between requests:
    // newest update first (matching the master's ordering), job_id as tiebreak.
    items.sort_by(|a, b| {
        let ta = a.get("update_time_ms").and_then(Value::as_i64).unwrap_or(0);
        let tb = b.get("update_time_ms").and_then(Value::as_i64).unwrap_or(0);
        tb.cmp(&ta).then_with(|| {
            let ia = a.get("job_id").and_then(Value::as_str).unwrap_or("");
            let ib = b.get("job_id").and_then(Value::as_str).unwrap_or("");
            ia.cmp(ib)
        })
    });

    let total = items.len();
    let start = (page - 1) * page_size;
    let paged = items
        .into_iter()
        .skip(start)
        .take(page_size)
        .collect::<Vec<_>>();
    Ok(api_success(json!({
        "items": paged,
        "page": page,
        "page_size": page_size,
        "total": total
    })))
}

async fn submit_load_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    headers: HeaderMap,
    Json(payload): Json<SubmitLoadRequest>,
) -> FsResult<Json<Value>> {
    let source_path = payload.path.trim();
    if source_path.is_empty() {
        return err_box!("path is required");
    }

    let mut builder = LoadJobCommand::builder(source_path);
    if let Some(target_path) = payload
        .target_path
        .as_ref()
        .filter(|v| !v.trim().is_empty())
    {
        builder = builder.target_path(target_path.trim());
    }
    if let Some(replicas) = payload.replicas {
        builder = builder.replicas(replicas);
    }
    if let Some(block_size) = payload.block_size.as_ref().filter(|v| !v.trim().is_empty()) {
        builder = builder.block_size(ByteUnit::from_str(block_size.as_str())?.as_byte() as i64);
    }
    if let Some(storage_type) = payload
        .storage_type
        .as_ref()
        .filter(|v| !v.trim().is_empty())
    {
        builder = builder.storage_type(StorageType::try_from(storage_type.as_str())?);
    }
    if let Some(ttl) = payload.ttl.as_ref().filter(|v| !v.trim().is_empty()) {
        builder = builder.ttl_ms(DurationUnit::from_str(ttl.as_str())?.as_millis() as i64);
    }
    if let Some(ttl_action) = payload.ttl_action.as_ref().filter(|v| !v.trim().is_empty()) {
        builder = builder.ttl_action(TtlAction::try_from(ttl_action.as_str())?);
    }
    if let Some(overwrite) = payload.overwrite {
        builder = builder.overwrite(overwrite);
    }

    if payload.recursive {
        log::info!("web load request recursive=true; current load command expands directories on the job runner side when supported");
    }
    if !payload.configs.is_empty() {
        log::info!("web load request included {} config entries; mounted UFS configuration is used by current backend", payload.configs.len());
    }

    let result =
        match with_job_timeout(instance.job_client().submit_load_job(builder.build())).await {
            Ok(result) => result,
            Err(err) => return err_box!("load source path {} failed: {}", source_path, err),
        };
    remember_load_job(
        &instance.load_jobs,
        &instance.load_job_cache,
        result.job_id.clone(),
    );
    let submitter = is_authenticated(&instance, &headers).then(auth_username);
    let now_ms = LocalTime::mills() as i64;
    let mut payload = match with_job_timeout(
        instance.job_client().get_job_status_verbose(&result.job_id),
    )
    .await
    {
        Ok(status) => job_status_payload(status),
        Err(_) => json!({
            "job_id": result.job_id,
            "id": result.job_id,
            "path": source_path,
            "source_path": source_path,
            "target_path": result.target_path,
            "state": job_state_name(result.state),
            "status": job_state_name(result.state),
            "source_type": "manual",
            "trigger_event": "Manual",
            "created_by": "User",
            "submitter": submitter,
            "progress": if result.state == JobTaskState::Completed { 100.0 } else { 0.0 },
            "create_time_ms": now_ms,
            "update_time_ms": now_ms,
            "done": result.state.is_finish()
        }),
    };
    if let Some(object) = payload.as_object_mut() {
        object.insert("path".to_string(), json!(source_path));
        object.insert("source_path".to_string(), json!(source_path));
        object.insert("target_path".to_string(), json!(result.target_path));
        object.insert("source_type".to_string(), json!("manual"));
        object.insert("trigger_event".to_string(), json!("Manual"));
        object.insert("created_by".to_string(), json!("User"));
        if let Some(name) = submitter.as_ref() {
            object.insert("submitter".to_string(), json!(name));
        }
        let create_time_ms = object
            .get("create_time_ms")
            .and_then(|value| value.as_i64())
            .filter(|value| *value > 0)
            .unwrap_or(now_ms);
        let update_time_ms = object
            .get("update_time_ms")
            .and_then(|value| value.as_i64())
            .filter(|value| *value > 0)
            .unwrap_or(create_time_ms);
        object.insert("create_time_ms".to_string(), json!(create_time_ms));
        object.insert("update_time_ms".to_string(), json!(update_time_ms));
        object.insert("state".to_string(), json!(job_state_name(result.state)));
        object.insert("status".to_string(), json!(job_state_name(result.state)));
        object.insert(
            "progress".to_string(),
            json!(if result.state == JobTaskState::Completed {
                100.0
            } else {
                object
                    .get("progress")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0)
            }),
        );
        object.insert("done".to_string(), json!(result.state.is_finish()));
    }
    finalize_job_display_name(&mut payload);
    cache_load_job(
        &instance.load_job_cache,
        result.job_id.as_str(),
        payload.clone(),
    );

    Ok(api_success(payload))
}

async fn job_status_v1(
    Path(job_id): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    if job_id.trim().is_empty() {
        return err_box!("job_id is required");
    }
    let status = with_job_timeout(instance.job_client().get_job_status_verbose(&job_id)).await?;
    remember_load_job(
        &instance.load_jobs,
        &instance.load_job_cache,
        status.job_id.clone(),
    );
    let cached = instance
        .load_job_cache
        .lock()
        .ok()
        .and_then(|cache| cache.get(&job_id).cloned());
    let mut payload = job_status_payload(status);
    merge_cached_job_metadata(&mut payload, cached.as_ref());
    cache_load_job(&instance.load_job_cache, &job_id, payload.clone());
    Ok(api_success(payload))
}

async fn job_cancel_v1(
    Path(job_id): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    if job_id.trim().is_empty() {
        return err_box!("job_id is required");
    }

    if let Some(cached) = instance
        .load_job_cache
        .lock()
        .ok()
        .and_then(|cache| cache.get(&job_id).cloned())
    {
        if cached_job_payload_is_terminal(&cached) {
            return Ok(api_success(cached));
        }
    }

    with_job_timeout_secs(
        JOB_CANCEL_RPC_TIMEOUT_SECS,
        instance.job_client().cancel_job(&job_id),
    )
    .await?;
    remember_load_job(
        &instance.load_jobs,
        &instance.load_job_cache,
        job_id.clone(),
    );

    // Return the master's authoritative status. The cancel RPC is a no-op when the
    // job is already terminal; synthesizing "Canceled" here would overwrite Completed.
    let status = with_job_timeout_secs(
        JOB_CANCEL_RPC_TIMEOUT_SECS,
        instance.job_client().get_job_status_verbose(&job_id),
    )
    .await?;
    let cached = instance
        .load_job_cache
        .lock()
        .ok()
        .and_then(|cache| cache.get(&job_id).cloned());
    let mut payload = job_status_payload(status);
    merge_cached_job_metadata(&mut payload, cached.as_ref());
    cache_load_job(&instance.load_job_cache, &job_id, payload.clone());
    Ok(api_success(payload))
}

async fn resolve_transfer_paths(
    instance: &AdminRouterHandler,
    kind: TransferKind,
    source_path: &str,
    target_path: Option<&str>,
) -> FsResult<(String, String)> {
    let input = FsPath::from_str(source_path)?;
    let (source, target) =
        if let Some(target) = target_path.filter(|value| !value.trim().is_empty()) {
            (input, FsPath::from_str(target)?)
        } else {
            let peer = match instance.unified_fs.toggle_path(&input, true).await? {
                Some(peer) => peer,
                None => return err_box!("{} is not mounted", source_path),
            };
            if input.is_cv() {
                (peer, input)
            } else {
                (input, peer)
            }
        };
    match kind {
        TransferKind::Load if !source.is_cv() && target.is_cv() => {}
        TransferKind::Export if source.is_cv() && !target.is_cv() => {}
        TransferKind::Load => {
            return err_box!(
                "load requires a UFS source and Curvine target: {} -> {}",
                source.full_path(),
                target.full_path()
            )
        }
        TransferKind::Export => {
            return err_box!(
                "export requires a Curvine source and UFS target: {} -> {}",
                source.full_path(),
                target.full_path()
            )
        }
    }
    Ok((source.clone_uri(), target.clone_uri()))
}

async fn transfers_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(query): Query<TransferListQuery>,
) -> FsResult<Json<Value>> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query
        .page_size
        .unwrap_or(20)
        .clamp(1, TRANSFER_LIST_MAX_FETCH);
    let kind_filter = query
        .kind
        .as_deref()
        .map(transfer_kind_from_name)
        .transpose()?;
    let state_filter = query
        .state
        .as_deref()
        .map(transfer_state_from_name)
        .transpose()?;
    let client = instance.transfer_client()?;
    let mut items = Vec::new();
    let mut page_token = None;
    for _ in 0..TRANSFER_LIST_MAX_PAGES {
        let response = with_job_timeout_secs(
            TRANSFER_RPC_TIMEOUT_SECS,
            client.list(
                kind_filter,
                state_filter,
                None,
                None,
                Some(TRANSFER_LIST_MAX_FETCH as u32),
                page_token.clone(),
            ),
        )
        .await?;
        let fetched = response.jobs.len();
        items.extend(response.jobs.into_iter().map(transfer_job_status_payload));
        match response.next_page_token {
            Some(next) if !next.is_empty() => page_token = Some(next),
            _ => break,
        }
        if fetched == 0 {
            break;
        }
    }

    if query.failed_only.unwrap_or(false) {
        items.retain(|item| {
            matches!(
                item.get("state").and_then(Value::as_str),
                Some("Failed" | "PartialSuccess")
            )
        });
    }
    items.sort_by(|left, right| {
        let left_time = left
            .get("update_time_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let right_time = right
            .get("update_time_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        right_time.cmp(&left_time).then_with(|| {
            let left_id = left.get("job_id").and_then(Value::as_str).unwrap_or("");
            let right_id = right.get("job_id").and_then(Value::as_str).unwrap_or("");
            right_id.cmp(left_id)
        })
    });

    let total = items.len();
    let start = (page - 1) * page_size;
    let paged = items
        .into_iter()
        .skip(start)
        .take(page_size)
        .collect::<Vec<_>>();
    Ok(api_success(json!({
        "items": paged,
        "page": page,
        "page_size": page_size,
        "total": total
    })))
}

async fn submit_transfer_v1(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    headers: HeaderMap,
    Json(payload): Json<SubmitTransferRequest>,
) -> FsResult<Json<Value>> {
    let source_path = payload.source_path.trim();
    if source_path.is_empty() {
        return err_box!("source_path is required");
    }
    let kind = transfer_kind_from_name(payload.kind.as_deref().unwrap_or("load"))?;
    let overwrite = payload.overwrite.unwrap_or(true);
    let (source_path, target_path) =
        resolve_transfer_paths(&instance, kind, source_path, payload.target_path.as_deref())
            .await?;
    let submitter = if is_authenticated(&instance, &headers) {
        auth_username()
    } else {
        WEB_TRANSFER_SUBMITTER.to_string()
    };
    let mut command = TransferCommand {
        kind,
        source_path: source_path.clone(),
        target_path: target_path.clone(),
        client_request_id: TransferCommand::default_client_request_id_with_overwrite(
            kind,
            &source_path,
            &target_path,
            overwrite,
        ),
        submitter: submitter.clone(),
        tenant: String::new(),
        options: Default::default(),
    };
    command.set_overwrite(overwrite);
    let client = instance.transfer_client()?;
    let response =
        with_job_timeout_secs(TRANSFER_SUBMIT_RPC_TIMEOUT_SECS, client.submit(command)).await?;
    let payload =
        match with_job_timeout_secs(TRANSFER_RPC_TIMEOUT_SECS, client.status(&response.job_id))
            .await
        {
            Ok(status) => transfer_status_payload(status),
            Err(_) => transfer_submit_payload(response, source_path, target_path, kind, submitter),
        };
    Ok(api_success(payload))
}

async fn transfer_status_v1(
    Path(job_id): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    if job_id.trim().is_empty() {
        return err_box!("job_id is required");
    }
    let client = instance.transfer_client()?;
    let status = with_job_timeout_secs(TRANSFER_RPC_TIMEOUT_SECS, client.status(&job_id)).await?;
    Ok(api_success(transfer_status_payload(status)))
}

async fn transfer_cancel_v1(
    Path(job_id): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    if job_id.trim().is_empty() {
        return err_box!("job_id is required");
    }
    let client = instance.transfer_client()?;
    with_job_timeout_secs(
        TRANSFER_CANCEL_RPC_TIMEOUT_SECS,
        client.cancel(&job_id, None),
    )
    .await?;
    let status = with_job_timeout_secs(TRANSFER_RPC_TIMEOUT_SECS, client.status(&job_id)).await?;
    Ok(api_success(transfer_status_payload(status)))
}

async fn transfer_retry_v1(
    Path(job_id): Path<String>,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Value>> {
    if job_id.trim().is_empty() {
        return err_box!("job_id is required");
    }
    let client = instance.transfer_client()?;
    let response =
        with_job_timeout_secs(TRANSFER_SUBMIT_RPC_TIMEOUT_SECS, client.retry(&job_id)).await?;
    let status =
        with_job_timeout_secs(TRANSFER_RPC_TIMEOUT_SECS, client.status(&response.job_id)).await?;
    Ok(api_success(transfer_status_payload(status)))
}

/// Parse the comma-separated `workers` query parameter into trimmed hostnames.
fn parse_worker_list(params: &HashMap<String, String>) -> Vec<String> {
    params
        .get("workers")
        .map(|value| {
            value
                .split(',')
                .map(|item| item.trim().to_string())
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Apply `target_status` to each requested worker via the master RPC and return
/// the hostnames that were updated successfully (matching the legacy CLI's
/// `Vec<String>` contract). Previously these endpoints only echoed the input /
/// returned an empty list without touching the cluster.
async fn apply_worker_decommission(
    instance: &AdminRouterHandler,
    params: &HashMap<String, String>,
    target_status: &str,
) -> Json<Vec<String>> {
    let mut updated = Vec::new();
    for worker in parse_worker_list(params) {
        match instance
            .unified_fs
            .fs_client()
            .set_worker_status(worker.as_str(), target_status)
            .await
        {
            Ok(_) => updated.push(worker),
            Err(err) => log::warn!(
                "failed to set worker {} status to {}: {}",
                worker,
                target_status,
                err
            ),
        }
    }
    Json(updated)
}

async fn add_dcm(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Vec<String>>> {
    Ok(apply_worker_decommission(&instance, &params, "Decommission").await)
}

async fn get_dcm(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Json<Vec<String>>> {
    let master_info = current_master_info(&instance).await?;
    let workers = master_info
        .decommission_workers
        .into_iter()
        .map(|worker| worker.address.hostname)
        .collect();
    Ok(Json(workers))
}

fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("text/html"))
        .unwrap_or(false)
}

fn resolve_webui_index_path() -> std::path::PathBuf {
    env::var("CURVINE_WEBUI_DIR")
        .ok()
        .map(|path| std::path::PathBuf::from(path).join("index.html"))
        .filter(|path| path.exists())
        .or_else(|| {
            [
                "webui/dist/index.html",
                "webui/public/index.html",
                "webui/index.html",
                "curvine-web/webui/dist/index.html",
                "curvine-web/webui/public/index.html",
                "curvine-web/webui/index.html",
                "/workspace/curvine-web/webui/dist/index.html",
                "/workspace/curvine-web/webui/public/index.html",
                "/workspace/curvine-web/webui/index.html",
            ]
            .iter()
            .map(std::path::PathBuf::from)
            .find(|path| path.exists())
        })
        .unwrap_or_else(|| std::path::PathBuf::from("webui/index.html"))
}

fn webui_index_html() -> FsResult<String> {
    Ok(std::fs::read_to_string(resolve_webui_index_path())?)
}

fn webui_index_response() -> FsResult<Response> {
    let body = webui_index_html()?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response())
}

async fn spa_index() -> FsResult<Response> {
    webui_index_response()
}

async fn remove_dcm(
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Vec<String>>> {
    Ok(apply_worker_decommission(&instance, &params, "Live").await)
}

async fn workers1(
    headers: HeaderMap,
    Extension(instance): Extension<Arc<AdminRouterHandler>>,
) -> FsResult<Response> {
    if wants_html(&headers) {
        return webui_index_response();
    }

    let master_info = current_master_info(&instance).await?;
    Ok(Json(master_info.live_workers).into_response())
}

impl RouterHandler for AdminRouterHandler {
    fn router(&self) -> Router {
        let instance = Arc::new(self.clone());
        let conf = self.conf.clone();
        Router::new()
            .route("/metrics", get(metrics))
            .route("/report", get(report))
            .route("/api/overview", get(overview))
            .route("/api/config", get(|| async { Json(conf) }))
            .route("/api/browse", get(browse))
            .route("/api/block_locations", get(block_locations))
            .route("/api/workers", get(workers))
            .route("/api/v1/auth/login", post(auth_login_v1))
            .route("/api/v1/auth/session", get(auth_session_v1))
            .route("/api/v1/auth/logout", post(auth_logout_v1))
            .route("/api/v1/overview", get(overview_v1))
            .route("/api/v1/master/ha", get(master_ha_v1))
            .route("/api/v1/master/failover", post(master_failover_v1))
            .route("/api/v1/config", get(config_v1))
            .route("/api/v1/fs/list", get(browse_v1))
            .route("/api/v1/fs/blocks", get(block_locations_v1))
            .route(
                "/api/v1/fs/cache-summary",
                get(fs_cache_summary_deprecated_v1),
            )
            .route(
                "/api/v1/fs/diagnostics/cache-summary-deep",
                get(fs_cache_summary_deep_diagnostic_v1),
            )
            .route("/api/v1/fs/stat", get(fs_stat_v1))
            .route("/api/v1/fs/ufs-sync", get(fs_ufs_sync_v1))
            .route("/api/v1/fs/ufs-sync/jobs", get(fs_ufs_sync_jobs_v1))
            .route("/api/v1/fs/mkdir", post(fs_mkdir_v1))
            .route("/api/v1/fs/delete", post(fs_delete_v1))
            .route("/api/v1/fs/free", post(fs_free_v1))
            .route(
                "/api/v1/fs/upload",
                post(fs_upload_v1).layer(DefaultBodyLimit::max(WEB_UPLOAD_MAX_BYTES)),
            )
            .route("/api/v1/fs/download", get(fs_download_v1))
            .route("/api/v1/workers", get(workers_v1))
            .route("/api/v1/workers/detail/:worker", get(worker_detail_v1))
            .route("/api/v1/workers/action", post(worker_action_v1))
            .route(
                "/api/v1/workers/decommission",
                post(worker_decommission_v1).delete(worker_remove_decommission_v1),
            )
            .route("/api/v1/mounts", get(mounts_v1).post(mount_create_v1))
            .route("/api/v1/mounts/delete", post(mount_delete_v1))
            .route("/api/v1/mounts/validate", post(mount_validate_v1))
            .route("/api/v1/mounts/resync", post(mount_resync_start_v1))
            .route(
                "/api/v1/mounts/resync/:task_id",
                get(mount_resync_status_v1),
            )
            .route("/api/v1/jobs", get(jobs_v1))
            .route("/api/v1/jobs/load", get(jobs_v1).post(submit_load_v1))
            .route(
                "/api/v1/jobs/:job_id",
                get(job_status_v1).delete(job_cancel_v1),
            )
            .route(
                "/api/v1/transfers",
                get(transfers_v1).post(submit_transfer_v1),
            )
            .route(
                "/api/v1/transfers/:job_id",
                get(transfer_status_v1).delete(transfer_cancel_v1),
            )
            .route("/api/v1/transfers/:job_id/retry", post(transfer_retry_v1))
            .nest(
                "/api/v1/observability",
                observability::router(self.observability.clone()),
            )
            .route("/add-dcm", get(add_dcm))
            .route("/get-dcm", get(get_dcm))
            .route("/remove-dcm", get(remove_dcm))
            .route("/login", get(spa_index))
            .route("/overview", get(spa_index))
            .route("/browse", get(spa_index))
            .route("/mounts", get(spa_index))
            .route("/jobs", get(spa_index))
            .route("/transfers", get(spa_index))
            .route("/config", get(spa_index))
            .route("/blocks", get(spa_index))
            .route("/preview", get(spa_index))
            .route("/observability", get(spa_index))
            .route("/operations", get(spa_index))
            .route("/workers", get(workers1))
            .route("/workers/:id", get(spa_index))
            .route_layer(middleware::from_fn(require_auth))
            .layer(Extension(instance))
    }
}

#[cfg(test)]
mod master_ha_tests {
    use super::*;
    use curvine_config::{ClientConf, MasterConf, RaftPeer};

    fn nailu_like_conf() -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.journal.enable = true;
        conf.journal.journal_addrs = vec![
            RaftPeer::new(
                1,
                "cv-curvine-master-0.cv-curvine-master.default.svc.common1ack.oppo.local",
                8996,
            ),
            RaftPeer::new(
                2,
                "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local",
                8996,
            ),
            RaftPeer::new(
                3,
                "cv-curvine-master-2.cv-curvine-master.default.svc.common1ack.oppo.local",
                8996,
            ),
        ];
        conf.client = ClientConf {
            master_addrs: vec![
                InetAddr::new(
                    "cv-curvine-master-0.cv-curvine-master.default.svc.common1ack.oppo.local",
                    8995,
                ),
                InetAddr::new(
                    "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local",
                    8995,
                ),
                InetAddr::new(
                    "cv-curvine-master-2.cv-curvine-master.default.svc.common1ack.oppo.local",
                    8995,
                ),
            ],
            ..Default::default()
        };
        conf.master = MasterConf::default();
        conf
    }

    #[test]
    fn maps_active_master_cluster_local_to_configured_raft_id() {
        let conf = nailu_like_conf();
        let active = "cv-curvine-master-1.cv-curvine-master.default.svc.cluster.local:8995";
        let configured =
            "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local:8995";

        assert_eq!(target_master_node_id(&conf, active), Some(2));
        assert_eq!(target_master_node_id(&conf, configured), Some(2));
        assert!(master_endpoints_equivalent(&conf, active, configured));
    }

    #[test]
    fn distinguishes_different_master_peers_with_same_dns_suffix() {
        let conf = nailu_like_conf();
        let master0 =
            "cv-curvine-master-0.cv-curvine-master.default.svc.common1ack.oppo.local:8995";
        let master1 = "cv-curvine-master-1.cv-curvine-master.default.svc.cluster.local:8995";

        assert_eq!(target_master_node_id(&conf, master0), Some(1));
        assert_eq!(target_master_node_id(&conf, master1), Some(2));
        assert!(!master_endpoints_equivalent(&conf, master0, master1));
    }

    #[test]
    fn configured_master_addr_for_raft_id_uses_client_addrs() {
        let conf = nailu_like_conf();
        let configured = configured_master_addr_for_raft_id(&conf, 2).expect("raft id 2");
        assert_eq!(
            configured,
            "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local:8995"
        );
    }

    #[test]
    fn active_target_prefers_raft_id_over_endpoint_text() {
        let conf = nailu_like_conf();
        let active = "10.0.0.21:8995";
        let target = "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local:8995";

        assert!(master_is_active_target(
            &conf,
            active,
            Some(2),
            target,
            Some(2)
        ));
    }

    #[test]
    fn active_target_rejects_different_raft_ids_even_with_endpoint_fallback_available() {
        let conf = nailu_like_conf();
        let active = "cv-curvine-master-1.cv-curvine-master.default.svc.cluster.local:8995";
        let target = "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local:8995";

        assert!(!master_is_active_target(
            &conf,
            active,
            Some(1),
            target,
            Some(2)
        ));
    }

    #[test]
    fn active_target_falls_back_to_endpoint_when_raft_id_is_missing() {
        let conf = nailu_like_conf();
        let active = "cv-curvine-master-1.cv-curvine-master.default.svc.cluster.local:8995";
        let target = "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local:8995";

        assert!(master_is_active_target(
            &conf,
            active,
            None,
            target,
            Some(2)
        ));
    }

    #[test]
    fn maps_distinct_ip_master_peers_to_raft_ids() {
        let mut conf = ClusterConf::default();
        conf.journal.enable = true;
        conf.journal.journal_addrs = vec![
            RaftPeer::new(1, "10.41.54.240", 8996),
            RaftPeer::new(2, "10.41.54.239", 8996),
            RaftPeer::new(3, "10.41.54.242", 8996),
        ];
        conf.client = ClientConf {
            master_addrs: vec![
                InetAddr::new("10.41.54.240", 8995),
                InetAddr::new("10.41.54.239", 8995),
                InetAddr::new("10.41.54.242", 8995),
            ],
            ..Default::default()
        };

        assert_eq!(target_master_node_id(&conf, "10.41.54.240:8995"), Some(1));
        assert_eq!(target_master_node_id(&conf, "10.41.54.239:8995"), Some(2));
        assert_eq!(target_master_node_id(&conf, "10.41.54.242:8995"), Some(3));
        assert_eq!(
            configured_master_addr_for_raft_id(&conf, 2),
            Some("10.41.54.239:8995".to_string())
        );
    }

    #[test]
    fn observability_query_requires_auth() {
        assert!(path_requires_auth("/api/v1/observability/query"));
        assert!(path_requires_auth("/api/v1/observability/targets"));
    }
}

#[cfg(test)]
mod job_state_filter_tests {
    use super::*;

    #[test]
    fn running_filter_matches_pending_and_loading() {
        assert!(job_matches_state_filter("pending", "running"));
        assert!(job_matches_state_filter("loading", "running"));
        assert!(job_matches_state_filter("Pending", "running"));
        assert!(!job_matches_state_filter("completed", "running"));
        assert!(!job_matches_state_filter("failed", "running"));
    }

    #[test]
    fn loading_filter_matches_only_loading() {
        assert!(job_matches_state_filter("loading", "loading"));
        assert!(!job_matches_state_filter("pending", "loading"));
    }

    #[test]
    fn state_filter_matches_aliases_after_master_filtering() {
        assert!(job_matches_state_filter("Completed", "complete"));
        assert!(job_matches_state_filter("Completed", "done"));
        assert!(job_matches_state_filter("Failed", "fail"));
        assert!(job_matches_state_filter("Canceled", "cancelled"));
        assert!(!job_matches_state_filter("Completed", "failed"));
    }

    #[test]
    fn failed_relevant_matches_state_counts_and_tasks() {
        assert!(job_payload_failed_relevant(&json!({"state": "Failed"})));
        assert!(job_payload_failed_relevant(
            &json!({"state": "Loading", "failed_files": 1})
        ));
        assert!(job_payload_failed_relevant(&json!({
            "state": "Loading",
            "tasks": [{"state": "Failed"}]
        })));
        assert!(!job_payload_failed_relevant(
            &json!({"state": "Loading", "failed_files": 0})
        ));
    }

    #[test]
    fn include_job_in_list_honors_failed_only() {
        let failed = json!({"state": "Failed"});
        let healthy = json!({"state": "Completed"});
        assert!(include_job_in_list(&failed, false));
        assert!(include_job_in_list(&healthy, false));
        assert!(include_job_in_list(&failed, true));
        assert!(!include_job_in_list(&healthy, true));
    }

    #[test]
    fn master_list_state_filter_does_not_push_running_to_master() {
        assert_eq!(master_list_state_filter("running"), None);
        assert_eq!(master_list_state_filter("active"), None);
        assert_eq!(
            master_list_state_filter("loading"),
            Some(JobTaskState::Loading)
        );
        assert_eq!(
            master_list_state_filter("pending"),
            Some(JobTaskState::Pending)
        );
    }

    #[test]
    fn retain_jobs_matching_state_filter_keeps_active_jobs() {
        let mut items = vec![
            json!({"state": "Pending", "job_id": "a"}),
            json!({"state": "Loading", "job_id": "b"}),
            json!({"state": "Completed", "job_id": "c"}),
        ];
        retain_jobs_matching_state_filter(&mut items, "running");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["job_id"], "a");
        assert_eq!(items[1]["job_id"], "b");
    }
}

#[cfg(test)]
mod transfer_tests {
    use super::*;

    #[test]
    fn parses_transfer_kinds() {
        assert_eq!(transfer_kind_from_name("load").unwrap(), TransferKind::Load);
        assert_eq!(
            transfer_kind_from_name("transfer_export").unwrap(),
            TransferKind::Export
        );
        assert!(transfer_kind_from_name("sync").is_err());
    }

    #[test]
    fn parses_transfer_states() {
        assert_eq!(
            transfer_state_from_name("pending").unwrap(),
            TransferState::Pending
        );
        assert_eq!(
            transfer_state_from_name("partial_success").unwrap(),
            TransferState::PartialSuccess
        );
        assert!(transfer_state_from_name("active").is_err());
    }

    #[test]
    fn transfer_progress_uses_total_size_when_available() {
        assert_eq!(
            transfer_progress_percent(TransferState::Running, 200, 50),
            25.0
        );
        assert_eq!(
            transfer_progress_percent(TransferState::Completed, 0, 0),
            100.0
        );
        assert_eq!(transfer_progress_percent(TransferState::Pending, 0, 0), 0.0);
    }

    #[test]
    fn transfer_counters_use_task_summary_when_present() {
        let summary = TransferTaskSummaryProto {
            pending: 2,
            running: 1,
            completed: 5,
            failed: 1,
            canceled: 0,
            stale: 1,
            completed_size: 1024,
        };
        let value = transfer_counter_fields(Some(&summary), TransferState::Running, 0);
        assert_eq!(value["total_files"], 10);
        assert_eq!(value["completed_files"], 5);
        assert_eq!(value["failed_files"], 1);
        assert_eq!(value["running_files"], 3);
    }

    #[test]
    fn transfer_display_name_is_stable() {
        let name = transfer_display_name("alice", "/tmp/foo/bar", 1_719_763_852_000);
        assert!(name.starts_with("transfer_alice_"));
        assert!(name.ends_with("_foo-bar"));
    }
}
