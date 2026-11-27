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
use std::sync::{Arc, Mutex};

use axum::extract::Query;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use curvine_client_core::file::{FsClient, FsContext};
use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fault::FaultHttpControl;
use curvine_model::{
    AccessMode, FileBlocks, FileStatus, FilesystemInfo, ListOptions, MountInfo, MountOptions,
    Provider, StorageType, TtlAction, WorkerInfo, WriteType,
};
use curvine_raft::raft::storage::RocksLogStorage;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::Runtime;
use curvine_unified_fs::UfsFileSystem;
use curvine_web::router::RouterHandler;
use futures::future::BoxFuture;

use crate::master::fs::{MasterFilesystem, SessionSummary};
use crate::master::meta::parse_glob_pattern;
use crate::master::mount::{run_resync, ResyncManager, ResyncStats, ResyncTask};
use crate::master::{MasterMetrics, MountManager};
use curvine_fs_api::{FileSystem, Path};

/// Small seam around the one RPC the standby overview needs, so the fallback
/// and success paths are testable without a live multi-master cluster.
trait OverviewInfoSource: Send + Sync {
    fn get_filesystem_info(&self) -> BoxFuture<'_, FsResult<FilesystemInfo>>;
}

impl OverviewInfoSource for FsClient {
    fn get_filesystem_info(&self) -> BoxFuture<'_, FsResult<FilesystemInfo>> {
        Box::pin(FsClient::get_filesystem_info(self))
    }
}

#[derive(Clone)]
pub struct MasterRouterHandler {
    fs: MasterFilesystem,
    mount_manager: Arc<MountManager>,
    conf: ClusterConf,
    start_time: String,
    /// The same moment as `start_time` in milliseconds since the Unix epoch.
    /// `start_time` is local wall-clock time without a zone, so the Web UI
    /// computes uptime from this instead.
    start_time_ms: i64,
    metrics: &'static MasterMetrics,
    fault_http: FaultHttpControl,
    journal_store: RocksLogStorage,
    /// A standby's worker manager and inode gauges are empty, so `/api/overview`
    /// must ask the active master through the normal client RPC path.
    standby_client: Option<Arc<dyn OverviewInfoSource>>,
    /// Serializes the rendering of the metrics text output, so that concurrent
    /// scrapes never interleave partially observed values.
    render_lock: Arc<Mutex<()>>,
    /// In-memory table of Web-triggered resync tasks, keyed by mount path.
    resync: Arc<ResyncManager>,
}

impl MasterRouterHandler {
    pub(crate) fn new(
        conf: ClusterConf,
        fs: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        metrics: &'static MasterMetrics,
        fault_http: FaultHttpControl,
        journal_store: RocksLogStorage,
        rt: Arc<Runtime>,
    ) -> Self {
        // `/api/overview` is polled by the UI every few seconds. Do not inherit
        // the 60-second metadata retry budget here: a leader outage must reach
        // the local fallback promptly instead of stacking overlapping requests.
        let mut client_conf = conf.clone();
        client_conf.client.rpc_retry_max_duration_ms =
            client_conf.client.rpc_retry_max_duration_ms.min(3_000);
        client_conf.client.conn_retry_max_duration_ms =
            client_conf.client.conn_retry_max_duration_ms.min(1_000);
        client_conf.client.rpc_timeout_ms = client_conf.client.rpc_timeout_ms.min(3_000);
        client_conf.client.conn_timeout_ms = client_conf.client.conn_timeout_ms.min(1_000);
        let standby_client = match FsContext::with_rt(client_conf, rt) {
            Ok(context) => {
                let client: Arc<dyn OverviewInfoSource> =
                    Arc::new(FsClient::new(Arc::new(context)));
                Some(client)
            }
            Err(err) => {
                log::warn!("cannot create standby overview client: {err}");
                None
            }
        };
        Self {
            fs,
            mount_manager,
            conf,
            start_time: LocalTime::now_datetime(),
            start_time_ms: LocalTime::mills() as i64,
            metrics,
            fault_http,
            journal_store,
            standby_client,
            render_lock: Arc::new(Mutex::new(())),
            resync: Arc::new(ResyncManager::new()),
        }
    }

    fn text_output(&self) -> FsResult<String> {
        let _guard = self.render_lock.lock().unwrap_or_else(|e| e.into_inner());
        Ok(self
            .metrics
            .text_output(self.fs.clone(), Some(&self.journal_store))?)
    }

    /// The active leader's Web address, or `None` when this master is the
    /// leader. A standby cannot serve writes; the Web UI uses this to link to
    /// the active node. All masters share the configured web port.
    fn active_master_web_addr(&self) -> Option<String> {
        if self.fs.master_monitor.is_active() {
            return None;
        }
        let leader_id = self
            .fs
            .master_monitor
            .follower_readiness_snapshot()
            .leader_id;
        self.conf
            .journal
            .journal_addrs
            .iter()
            .find(|peer| peer.id == leader_id)
            .map(|peer| format!("{}:{}", peer.hostname, self.conf.master.web_port))
    }

    /// Gate for the Web write APIs: requires `master.web_write_enabled` and an
    /// active leader. A standby rejection points at the leader's Web address.
    fn check_write_allowed(&self) -> FsResult<()> {
        let active = self.fs.master_monitor.is_active();
        let addr = self.active_master_web_addr();
        if let Some(msg) =
            write_rejection(self.conf.master.web_write_enabled, active, addr.as_deref())
        {
            return err_box!("{}", msg);
        }
        Ok(())
    }

    /// Register and launch a background resync for `mount`. The walk uses the
    /// stored mount properties, so credentials never come from a request.
    /// Fails when a resync is already running for the same mount point.
    fn spawn_resync(&self, mount: &MountInfo, trigger: &str, dry_run: bool) -> FsResult<()> {
        self.resync.try_start(&mount.cv_path, trigger)?;
        let fs = self.fs.clone();
        let defaults = self.conf.client.clone();
        let resync = self.resync.clone();
        let mount = mount.clone();
        tokio::spawn(async move {
            let mut stats = ResyncStats::default();
            let result = run_resync(&fs, &mount, &defaults, dry_run, &mut stats).await;
            resync.finish(&mount.cv_path, stats, result.err().map(|e| e.to_string()));
        });
        Ok(())
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

/// Overview data source: active masters can answer from their local state, but a
/// standby's worker manager and inode gauges are empty. Fetch the leader's
/// `GetFilesystemInfo` instead of returning those empty values. A failed fetch
/// falls back to the local snapshot so a transient RPC error does not turn the
/// overview endpoint into a hard failure.
async fn overview_state(
    instance: &MasterRouterHandler,
) -> FsResult<(FilesystemInfo, usize, usize)> {
    let local = || -> FsResult<(FilesystemInfo, usize, usize)> {
        let info = instance.fs.filesystem_info()?;
        let (files, dirs) = overview_file_dir_counts(instance.fs.get_file_counts());
        Ok((info, files, dirs))
    };
    overview_state_from(
        instance.fs.master_monitor.is_active(),
        local,
        instance.standby_client.as_deref(),
    )
    .await
}

async fn overview_state_from<F>(
    active: bool,
    local: F,
    client: Option<&dyn OverviewInfoSource>,
) -> FsResult<(FilesystemInfo, usize, usize)>
where
    F: FnOnce() -> FsResult<(FilesystemInfo, usize, usize)>,
{
    if active {
        return local();
    }
    let Some(client) = client else {
        return local();
    };
    match client.get_filesystem_info().await {
        Ok(info) => {
            let files = info.inode_file_num as usize;
            let dirs = info.inode_dir_num as usize;
            Ok((info, files, dirs))
        }
        Err(err) => {
            log::warn!("standby overview fetch failed: {err}; using local snapshot");
            local()
        }
    }
}

async fn overview(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Value>> {
    let fs = &instance.fs;
    let conf = &instance.conf;
    let start_time = &instance.start_time;
    let (filesystem_info, files_total, dir_total) = overview_state(&instance).await?;

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
    let active_master_web_addr = instance.active_master_web_addr();

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
        "web_write_enabled": conf.master.web_write_enabled,
    });
    if let Value::Object(fields) = &mut res {
        fields.extend(overview_version_fields());
    }
    Ok(Json(res))
}

/// Map the `(dir_count, file_count)` tuple returned by
/// `MasterFilesystem::get_file_counts()` to the overview response's
/// `(files_total, dir_total)` fields. Centralising the order here lets the
/// regression test below catch an accidental swap of the two counts.
fn overview_file_dir_counts(counts: (i64, i64)) -> (usize, usize) {
    let (dir_count, file_count) = counts;
    (file_count as usize, dir_count as usize)
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
    let files = match browse_list_options(path, &params)? {
        Some(opts) => fs.list_options(path, opts)?,
        None => fs.list_status(path)?,
    };
    Ok(Json(files))
}

/// Optional pagination for the web file browser: `limit` caps the page size
/// and `start_after` resumes after the last name of the previous page.
/// Children are name-ordered, so the last name is a stable cursor. Glob
/// patterns keep the full listing because `list_options` does not support
/// them.
fn browse_list_options(
    path: &str,
    params: &HashMap<String, String>,
) -> FsResult<Option<ListOptions>> {
    let limit = match params.get("limit") {
        Some(raw) => match raw.parse::<usize>() {
            Ok(n) => Some(n),
            Err(_) => return err_box!("invalid limit parameter: {}", raw),
        },
        None => None,
    };
    let start_after = params.get("start_after").cloned();
    if limit.is_none() && start_after.is_none() {
        return Ok(None);
    }
    if parse_glob_pattern(path).0 {
        return Ok(None);
    }
    Ok(Some(ListOptions { limit, start_after }))
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

/// Client session leases with their ownership footprint (#1885). A zombie mount is
/// identifiable here — host, mount point, whether its lease is still live — without
/// reading the metadata database.
async fn sessions(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Vec<SessionSummary>>> {
    Ok(Json(instance.fs.session_summary()?))
}

/// Whether a mount property key names a secret that the Web API must never
/// echo back. Ported from the CLI `cv mount` display masking so the browser
/// sees the same redacted view as the terminal.
fn is_sensitive_config_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();

    // Structural fields under the credential namespace that carry no secret.
    const NON_SECRET: &[&str] = &[
        "s3.credentials.type",
        "s3.credentials.role_arn",
        "s3.credentials.role_session_name",
        "s3.credentials.external_id",
        "s3.credentials.disable_config_load",
        "s3.credentials.disable_ec2_metadata",
    ];
    if NON_SECRET.contains(&key.as_str()) {
        return false;
    }

    key.contains("credential")
        || key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("access_key")
        || key.ends_with(".access")
        || key.ends_with(".ak")
        || key.ends_with(".sk")
        || key.contains("api_key")
        || key.ends_with(".service_account")
        || key.ends_with(".account_key")
}

/// A copy of `info` with secret property values masked, so read APIs never
/// return credentials to the browser.
fn masked_mount_info(info: &MountInfo) -> MountInfo {
    let mut masked = info.clone();
    for (key, value) in masked.properties.iter_mut() {
        if is_sensitive_config_key(key) {
            *value = "******".to_string();
        }
    }
    masked
}

async fn mounts(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Vec<MountInfo>>> {
    let table = instance.mount_manager.get_mount_table()?;
    Ok(Json(table.iter().map(masked_mount_info).collect()))
}

async fn mount_info(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Query(params): Query<HashMap<String, String>>,
) -> FsResult<Json<Option<MountInfo>>> {
    let path = match params.get("path") {
        Some(p) => p,
        None => return err_box!("missing path parameter"),
    };
    let path = Path::from_str(path)?;
    let info = instance.mount_manager.get_mount_info(&path)?;
    Ok(Json(info.as_ref().map(masked_mount_info)))
}

/// Pure gate for the Web write APIs: both the configuration flag and active
/// leadership are required.
fn write_allowed(enabled: bool, active: bool) -> bool {
    enabled && active
}

/// The rejection message for a refused Web write, or `None` when the write may
/// proceed. `active_web_addr` is the leader's Web address (as exposed by
/// `/api/overview`) so a standby caller can be redirected to the active node.
fn write_rejection(enabled: bool, active: bool, active_web_addr: Option<&str>) -> Option<String> {
    if write_allowed(enabled, active) {
        return None;
    }
    if !enabled {
        return Some("Web write APIs are disabled (master.web_write_enabled = false)".to_string());
    }
    Some(match active_web_addr {
        Some(addr) => {
            format!("this master is standby; send Web writes to the active master at {addr}")
        }
        None => {
            "this master is not the active leader; Web writes are only served by the active master"
                .to_string()
        }
    })
}

/// Body of `POST /api/free`: release the cached blocks of `path`, mirroring
/// `cv free [-r]`. Metadata and UFS data are untouched.
#[derive(Deserialize)]
struct FreeRequest {
    path: String,
    #[serde(default)]
    recursive: bool,
}

async fn free(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Json(req): Json<FreeRequest>,
) -> FsResult<Json<Value>> {
    instance.check_write_allowed()?;
    let res = instance.fs.free(&req.path, req.recursive)?;
    Ok(Json(json!({ "inodes": res.inodes, "bytes": res.bytes })))
}

/// Body of `POST /api/mount`: create a mount point, mirroring `cv mount`, or
/// update the existing mount at `cv_path` when `update` is set
/// (`cv mount --update`).
///
/// For a new mount, optional fields left absent keep the CLI builder defaults
/// (7d TTL, cache mode, read-only, auto-cache on). For an update, absent
/// optional fields keep the stored mount's values, and the stored UFS path and
/// write type are never changed.
#[derive(Deserialize)]
struct MountRequest {
    cv_path: String,
    ufs_path: String,
    /// Update the existing mount at `cv_path` instead of creating a new one.
    /// The request `ufs_path` is ignored on update; the stored one is kept.
    #[serde(default)]
    update: bool,
    #[serde(default)]
    properties: HashMap<String, String>,
    /// Require the UFS authority path and the Curvine path to match, as
    /// `cv mount --check-path-consist` does by default.
    check_path_consist: Option<bool>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    read_verify_ufs: Option<bool>,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: Option<WriteType>,
    provider: Option<Provider>,
    auto_cache: Option<bool>,
    access_mode: Option<AccessMode>,
    write_cache: Option<bool>,
}

/// Translate a mount request into [`MountOptions`].
///
/// For a new mount, absent fields fall back to the CLI builder defaults. For an
/// update (`req.update`), absent optional fields stay `None` so
/// [`MountInfo::merge_with`] keeps the stored values instead of resetting them
/// to defaults. `ttl_action` is never sent on update: the stored write type is
/// immutable, so its stored action is the only consistent one.
fn mount_options(req: &MountRequest) -> MountOptions {
    let defaults = MountOptions::builder().build();
    if req.update {
        return MountOptions {
            update: true,
            add_properties: req.properties.clone(),
            ttl_ms: req.ttl_ms,
            ttl_action: None,
            read_verify_ufs: req.read_verify_ufs,
            storage_type: req.storage_type,
            block_size: req.block_size,
            replicas: req.replicas,
            remove_properties: Vec::new(),
            write_type: req.write_type.unwrap_or(defaults.write_type),
            provider: req.provider,
            auto_cache: req.auto_cache,
            access_mode: req.access_mode,
            write_cache: req.write_cache,
        };
    }
    MountOptions {
        update: false,
        add_properties: req.properties.clone(),
        ttl_ms: req.ttl_ms.or(defaults.ttl_ms),
        ttl_action: req.ttl_action.or(defaults.ttl_action),
        read_verify_ufs: req.read_verify_ufs,
        storage_type: req.storage_type,
        block_size: req.block_size,
        replicas: req.replicas,
        remove_properties: Vec::new(),
        write_type: req.write_type.unwrap_or(defaults.write_type),
        provider: req.provider,
        auto_cache: req.auto_cache.or(defaults.auto_cache),
        access_mode: req.access_mode.or(defaults.access_mode),
        write_cache: req.write_cache.or(defaults.write_cache),
    }
}

async fn mount(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Json(req): Json<MountRequest>,
) -> FsResult<Json<Value>> {
    instance.check_write_allowed()?;
    if req.cv_path.trim().is_empty() {
        return err_box!("Curvine path cannot be empty");
    }
    let cv_path = Path::from_str(&req.cv_path)?;

    if req.update {
        // An update keeps the stored UFS path and write type; the request
        // ufs_path is ignored by MountManager::update_mount. No UFS probe runs
        // here: update_mount canonicalizes and gates the merged configuration
        // (including the s3_allow_env_credentials policy) before storing it,
        // and UFS reachability surfaces on the next data access.
        let opts = mount_options(&req);
        instance
            .mount_manager
            .mount(None, &req.cv_path, &req.ufs_path, &opts)?;
        let info = instance.mount_manager.get_mount_info(&cv_path)?;
        return Ok(Json(
            json!({ "mount": info.as_ref().map(masked_mount_info) }),
        ));
    }

    if req.ufs_path.trim().is_empty() {
        return err_box!("UFS path cannot be empty");
    }
    let ufs_path = Path::from_str(&req.ufs_path)?;
    if req.check_path_consist.unwrap_or(true) && ufs_path.authority_path() != cv_path.path() {
        return err_box!(
            "UFS and Curvine paths must match (check_path_consist). UFS: {}, Curvine: {}",
            ufs_path.authority_path(),
            cv_path.path()
        );
    }

    let mut opts = mount_options(&req);
    // Canonicalize properties and apply the credential gate before probing, so
    // the probe uses the same configuration the stored mount will use and an
    // environment-resolved S3 credential is refused before any UFS access.
    let mut probe_info = opts.clone().to_info(0, &req.cv_path, &req.ufs_path);
    instance
        .mount_manager
        .validate_mount_config(&mut probe_info)?;
    opts.add_properties = probe_info.properties;

    let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties.clone(), opts.provider).map_err(
        |err| {
            FsError::common(format!(
                "Unable to initialize UFS {}: {}. Verify the URI, mount configuration, and credentials.",
                ufs_path.full_path(),
                err
            ))
        },
    )?;
    ufs.list_status(&ufs_path).await.map_err(|err| {
        FsError::common(format!(
            "Unable to access UFS {}: {}. Verify the endpoint, credentials, and network connectivity.",
            ufs_path.full_path(),
            err
        ))
    })?;

    instance
        .mount_manager
        .mount(None, &req.cv_path, &req.ufs_path, &opts)?;
    let info = instance.mount_manager.get_mount_info(&cv_path)?;
    // A first fs_mode mount reconciles its UFS tree in the background, like
    // the CLI's first-mount auto resync. The mount is already stored, so a
    // resync that cannot start is logged, not a mount failure. Updates never
    // trigger a resync.
    if let Some(info) = &info {
        if info.is_fs_mode() {
            if let Err(e) = instance.spawn_resync(info, "auto", false) {
                log::warn!("auto resync for {} did not start: {}", info.cv_path, e);
            }
        }
    }
    Ok(Json(
        json!({ "mount": info.as_ref().map(masked_mount_info) }),
    ))
}

/// Body of `POST /api/umount`: remove a mount point by Curvine path or by mount
/// id, mirroring `cv umount`. A non-empty `cv_path` takes precedence over
/// `mount_id`; at least one selector is required.
#[derive(Deserialize)]
struct UmountRequest {
    cv_path: Option<String>,
    mount_id: Option<u32>,
}

async fn umount(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Json(req): Json<UmountRequest>,
) -> FsResult<Json<Value>> {
    instance.check_write_allowed()?;
    let cv = req.cv_path.as_deref().unwrap_or("").trim();
    if !cv.is_empty() {
        instance.mount_manager.umount(cv)?;
    } else if let Some(id) = req.mount_id {
        instance.mount_manager.unmount_by_id(id)?;
    } else {
        return err_box!("umount requires a non-empty cv_path or a mount_id");
    }
    Ok(Json(json!({ "unmounted": true })))
}

/// Body of `POST /api/resync`: reconcile an fs_mode mount point with its UFS,
/// mirroring `cv mount resync <cv_path>`. The walk runs in the background;
/// progress and results are read from `GET /api/resync`.
#[derive(Deserialize)]
struct ResyncRequest {
    cv_path: String,
    #[serde(default)]
    dry_run: bool,
}

async fn resync_submit(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
    Json(req): Json<ResyncRequest>,
) -> FsResult<Json<Value>> {
    instance.check_write_allowed()?;
    if req.cv_path.trim().is_empty() {
        return err_box!("Curvine path cannot be empty");
    }
    let cv_path = Path::from_str(&req.cv_path)?;
    if !cv_path.is_cv() {
        return err_box!("resync requires a curvine path, got: {}", req.cv_path);
    }
    let mount = match instance.mount_manager.get_mount_info(&cv_path)? {
        Some(v) => v,
        None => return err_box!("mount info not found for {}", req.cv_path),
    };
    if !mount.is_fs_mode() {
        return err_box!(
            "resync is only allowed for fs_mode mount; mount point {} has write_type {:?}",
            mount.cv_path,
            mount.write_type
        );
    }
    instance.spawn_resync(&mount, "manual", req.dry_run)?;
    Ok(Json(json!({ "started": true, "cv_path": mount.cv_path })))
}

/// All resync tasks, oldest first. Read-only, so no write gate applies.
async fn resync_status(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Vec<ResyncTask>>> {
    Ok(Json(instance.resync.tasks()))
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
            .route("/api/mounts", get(mounts))
            .route("/api/mount_info", get(mount_info))
            .route("/api/free", post(free))
            .route("/api/mount", post(mount))
            .route("/api/umount", post(umount))
            .route("/api/resync", post(resync_submit).get(resync_status))
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

    struct FakeOverviewSource(Result<FilesystemInfo, &'static str>);

    impl OverviewInfoSource for FakeOverviewSource {
        fn get_filesystem_info(&self) -> BoxFuture<'_, FsResult<FilesystemInfo>> {
            Box::pin(async move {
                match &self.0 {
                    Ok(info) => Ok(info.clone()),
                    Err(message) => Err(curvine_error::FsError::common(*message)),
                }
            })
        }
    }

    #[test]
    fn standby_overview_uses_the_leader_values() {
        let info = FilesystemInfo {
            available: 100,
            capacity: 200,
            fs_used: 100,
            inode_file_num: 42,
            inode_dir_num: 7,
            ..Default::default()
        };
        let source = FakeOverviewSource(Ok(info));
        let (result, files, dirs) = futures::executor::block_on(overview_state_from(
            false,
            || Ok((FilesystemInfo::default(), 0, 0)),
            Some(&source),
        ))
        .unwrap();
        assert_eq!(result.capacity, 200);
        assert_eq!(result.available, 100);
        assert_eq!(files, 42);
        assert_eq!(dirs, 7);
    }

    #[test]
    fn standby_overview_falls_back_when_the_leader_fetch_fails() {
        let source = FakeOverviewSource(Err("leader unavailable"));
        let (result, files, dirs) = futures::executor::block_on(overview_state_from(
            false,
            || {
                Ok((
                    FilesystemInfo {
                        capacity: 1,
                        ..Default::default()
                    },
                    2,
                    3,
                ))
            },
            Some(&source),
        ))
        .unwrap();
        assert_eq!(result.capacity, 1);
        assert_eq!(files, 2);
        assert_eq!(dirs, 3);
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
    fn browse_list_options_defaults_to_full_listing() {
        let params = HashMap::new();
        assert!(browse_list_options("/bigdir", &params).unwrap().is_none());
    }

    #[test]
    fn browse_list_options_builds_page_options() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "100".to_string());
        params.insert("start_after".to_string(), "file_0099".to_string());
        let opts = browse_list_options("/bigdir", &params).unwrap().unwrap();
        assert_eq!(opts.limit, Some(100));
        assert_eq!(opts.start_after.as_deref(), Some("file_0099"));
    }

    #[test]
    fn browse_list_options_rejects_invalid_limit() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "abc".to_string());
        assert!(browse_list_options("/bigdir", &params).is_err());
    }

    #[test]
    fn browse_list_options_keeps_glob_full_listing() {
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "100".to_string());
        assert!(browse_list_options("/bigdir/*.log", &params)
            .unwrap()
            .is_none());
    }

    #[test]
    fn overview_counts_keep_files_and_dirs_unswapped() {
        // get_file_counts() returns (dir_count, file_count). Use unequal values
        // so a swapped mapping cannot satisfy both assertions.
        let (files_total, dir_total) = overview_file_dir_counts((3, 7));
        assert_eq!(files_total, 7);
        assert_eq!(dir_total, 3);
    }

    #[test]
    fn sensitive_mount_keys_are_masked() {
        for key in [
            "s3.credentials.access",
            "s3.credentials.secret",
            "s3.credentials.token",
            "s3.session_token",
            "s3.security_token",
            "gcs.service_account",
            "azure.account_key",
            "fs.adapter.api_key",
        ] {
            assert!(is_sensitive_config_key(key), "{key} must be masked");
        }
    }

    #[test]
    fn non_secret_mount_keys_are_not_masked() {
        for key in [
            "s3.credentials.type",
            "s3.credentials.role_arn",
            "s3.credentials.role_session_name",
            "s3.credentials.external_id",
            "s3.credentials.disable_config_load",
            "s3.credentials.disable_ec2_metadata",
            "s3.endpoint_url",
        ] {
            assert!(!is_sensitive_config_key(key), "{key} must not be masked");
        }
    }

    #[test]
    fn unknown_credential_keys_stay_masked() {
        // Fail safe: a key the allowlist does not know about is treated as a secret.
        for key in ["s3.credentials.foo", "s3.credentials.token_v2"] {
            assert!(is_sensitive_config_key(key), "{key} must stay masked");
        }
    }

    #[test]
    fn masked_mount_info_redacts_secret_properties() {
        let info = MountInfo {
            cv_path: "/mnt".to_string(),
            ufs_path: "s3://bucket/prefix".to_string(),
            properties: HashMap::from([
                (
                    "s3.credentials.access".to_string(),
                    "access-value".to_string(),
                ),
                (
                    "s3.credentials.secret".to_string(),
                    "secret-value".to_string(),
                ),
                ("s3.credentials.type".to_string(), "static".to_string()),
                (
                    "s3.endpoint_url".to_string(),
                    "https://example.com".to_string(),
                ),
            ]),
            ..Default::default()
        };

        let masked = masked_mount_info(&info);
        assert_eq!(masked.properties["s3.credentials.access"], "******");
        assert_eq!(masked.properties["s3.credentials.secret"], "******");
        assert_eq!(masked.properties["s3.credentials.type"], "static");
        assert_eq!(masked.properties["s3.endpoint_url"], "https://example.com");
        // The source entry is untouched.
        assert_eq!(info.properties["s3.credentials.access"], "access-value");
    }

    #[test]
    fn write_allowed_requires_flag_and_leadership() {
        assert!(!write_allowed(false, false));
        assert!(!write_allowed(false, true));
        assert!(!write_allowed(true, false));
        assert!(write_allowed(true, true));
    }

    #[test]
    fn write_rejection_is_empty_when_allowed() {
        assert!(write_rejection(true, true, None).is_none());
    }

    #[test]
    fn write_rejection_names_the_disabled_flag() {
        let msg = write_rejection(false, true, None).unwrap();
        assert!(msg.contains("web_write_enabled"), "{msg}");
    }

    #[test]
    fn write_rejection_redirects_standby_to_the_leader() {
        let msg = write_rejection(true, false, Some("leader-host:9001")).unwrap();
        assert!(msg.contains("standby"), "{msg}");
        assert!(msg.contains("leader-host:9001"), "{msg}");
    }

    #[test]
    fn write_rejection_without_a_known_leader_still_refuses() {
        let msg = write_rejection(true, false, None).unwrap();
        assert!(msg.contains("active"), "{msg}");
    }

    /// A handler backed by an in-memory filesystem, with the write gate and
    /// role set explicitly. Shared by the Web write API tests.
    fn test_handler(web_write_enabled: bool, active: bool) -> MasterRouterHandler {
        use crate::master::journal::JournalSystem;
        use crate::master::Master;
        use curvine_runtime::common::Utils;

        Master::init_test_metrics();
        let test_name = Utils::rand_str(6);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.web_write_enabled = web_write_enabled;
        conf.master.meta_dir = Utils::test_sub_dir(format!("router-handler-test/meta-{test_name}"));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("router-handler-test/journal-{test_name}"));

        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        if active {
            fs.master_monitor.set_term_for_test(1);
            fs.master_monitor.set_applied_term_for_test(1);
        } else {
            fs.master_monitor
                .set_role_for_test(curvine_raft::raft::RoleState::Follower);
        }
        let mount_manager = Arc::new(MountManager::new(fs.clone()));
        let journal_store = RocksLogStorage::from_conf(&conf.journal, true);
        let rt = Arc::new(Runtime::single());
        MasterRouterHandler::new(
            conf,
            fs,
            mount_manager,
            Master::get_metrics().unwrap(),
            FaultHttpControl::default(),
            journal_store,
            rt,
        )
    }

    async fn call_free(handler: &MasterRouterHandler, path: &str) -> FsResult<Json<Value>> {
        free(
            Extension(Arc::new(handler.clone())),
            Json(FreeRequest {
                path: path.to_string(),
                recursive: false,
            }),
        )
        .await
    }

    #[tokio::test]
    async fn free_rejects_when_the_write_gate_is_disabled() {
        let handler = test_handler(false, true);
        handler.fs.create("/free-gate-off", true).unwrap();

        let err = call_free(&handler, "/free-gate-off").await.unwrap_err();
        assert!(err.to_string().contains("web_write_enabled"), "{err}");
        // The refused write left the file in place.
        assert!(handler.fs.file_status("/free-gate-off").is_ok());
    }

    #[tokio::test]
    async fn free_rejects_on_standby() {
        let handler = test_handler(true, false);
        let err = call_free(&handler, "/anything").await.unwrap_err();
        assert!(err.to_string().contains("active"), "{err}");
    }

    #[tokio::test]
    async fn free_serves_an_active_leader_with_the_gate_open() {
        let handler = test_handler(true, true);
        handler.fs.create("/free-plain", true).unwrap();

        // A plain (unmounted, uncached) file has no cached blocks to release.
        let Json(res) = call_free(&handler, "/free-plain").await.unwrap();
        assert_eq!(res["inodes"], 0);
        assert_eq!(res["bytes"], 0);

        // Filesystem errors surface unchanged: root and missing paths.
        assert!(call_free(&handler, "/").await.is_err());
        assert!(call_free(&handler, "/free-missing").await.is_err());
    }

    fn mount_request(cv: &str, ufs: &str) -> MountRequest {
        MountRequest {
            cv_path: cv.to_string(),
            ufs_path: ufs.to_string(),
            update: false,
            properties: HashMap::new(),
            check_path_consist: None,
            ttl_ms: None,
            ttl_action: None,
            read_verify_ufs: None,
            storage_type: None,
            block_size: None,
            replicas: None,
            write_type: None,
            provider: None,
            auto_cache: None,
            access_mode: None,
            write_cache: None,
        }
    }

    async fn call_mount(handler: &MasterRouterHandler, req: MountRequest) -> FsResult<Json<Value>> {
        mount(Extension(Arc::new(handler.clone())), Json(req)).await
    }

    async fn call_umount(
        handler: &MasterRouterHandler,
        req: UmountRequest,
    ) -> FsResult<Json<Value>> {
        umount(Extension(Arc::new(handler.clone())), Json(req)).await
    }

    fn umount_by_path(cv: &str) -> UmountRequest {
        UmountRequest {
            cv_path: Some(cv.to_string()),
            mount_id: None,
        }
    }

    #[test]
    fn mount_options_defaults_match_cli_builder() {
        let opts = mount_options(&mount_request("/mnt", "file:///tmp/mnt"));
        assert!(!opts.update);
        assert_eq!(opts.ttl_ms, Some(7 * 24 * 3600 * 1000));
        assert_eq!(opts.write_type, WriteType::CacheMode);
        assert_eq!(opts.auto_cache, Some(true));
        assert_eq!(opts.access_mode, Some(AccessMode::ReadOnly));
        assert_eq!(opts.write_cache, Some(false));
    }

    #[test]
    fn mount_options_applies_explicit_overrides() {
        let mut req = mount_request("/mnt", "s3://bucket/mnt");
        req.write_type = Some(WriteType::FsMode);
        req.access_mode = Some(AccessMode::ReadWrite);
        req.auto_cache = Some(false);
        req.write_cache = Some(true);
        req.ttl_ms = Some(1000);
        req.replicas = Some(2);
        req.properties = HashMap::from([("k".to_string(), "v".to_string())]);

        let opts = mount_options(&req);
        assert_eq!(opts.write_type, WriteType::FsMode);
        assert_eq!(opts.access_mode, Some(AccessMode::ReadWrite));
        assert_eq!(opts.auto_cache, Some(false));
        assert_eq!(opts.write_cache, Some(true));
        assert_eq!(opts.ttl_ms, Some(1000));
        assert_eq!(opts.replicas, Some(2));
        assert_eq!(opts.add_properties["k"], "v");
    }

    #[tokio::test]
    async fn mount_rejects_when_the_write_gate_is_disabled() {
        let handler = test_handler(false, true);
        let err = call_mount(
            &handler,
            mount_request("/mnt-gate-off", "file:///tmp/mnt-gate-off"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("web_write_enabled"), "{err}");
        let stored = handler
            .mount_manager
            .get_mount_info(&Path::from_str("/mnt-gate-off").unwrap())
            .unwrap();
        assert!(stored.is_none());
    }

    #[tokio::test]
    async fn mount_rejects_on_standby() {
        let handler = test_handler(true, false);
        let err = call_mount(
            &handler,
            mount_request("/mnt-standby", "file:///tmp/mnt-standby"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("active"), "{err}");
    }

    #[tokio::test]
    async fn mount_rejects_empty_paths() {
        let handler = test_handler(true, true);
        assert!(call_mount(&handler, mount_request("", "file:///tmp/x"))
            .await
            .is_err());
        assert!(call_mount(&handler, mount_request("/x", "")).await.is_err());
    }

    #[tokio::test]
    async fn mount_rejects_path_mismatch_by_default() {
        let handler = test_handler(true, true);
        // Fails the consistency check before any UFS probe is attempted.
        let err = call_mount(&handler, mount_request("/different", "s3://bucket/data"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("check_path_consist"), "{err}");
    }

    #[tokio::test]
    async fn mount_rejects_env_resolved_s3_before_probing() {
        let handler = test_handler(true, true);
        let mut req = mount_request("/s3env", "s3://bucket/s3env");
        req.check_path_consist = Some(false);
        req.properties = HashMap::from([
            (
                "s3.endpoint_url".to_string(),
                "http://s3.example.com".to_string(),
            ),
            ("s3.region_name".to_string(), "cn-test-1".to_string()),
            ("s3.credentials.type".to_string(), "auto".to_string()),
        ]);

        let err = call_mount(&handler, req).await.unwrap_err();
        assert!(
            err.to_string().contains("s3_allow_env_credentials"),
            "{err}"
        );
        // Refused before any UFS probe: no mount point was created.
        let stored = handler
            .mount_manager
            .get_mount_info(&Path::from_str("/s3env").unwrap())
            .unwrap();
        assert!(stored.is_none());
    }

    #[tokio::test]
    async fn mount_succeeds_for_local_ufs() {
        use curvine_runtime::common::Utils;

        let handler = test_handler(true, true);
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/mnt-ufs-{}",
            Utils::rand_str(6)
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut req = mount_request("/web-mnt-local", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        req.properties = HashMap::from([(
            "s3.credentials.secret".to_string(),
            "secret-value".to_string(),
        )]);

        let Json(res) = call_mount(&handler, req).await.unwrap();
        let mount = &res["mount"];
        assert_eq!(mount["cv_path"], "/web-mnt-local");
        assert_eq!(mount["ufs_path"], format!("file://{dir}"));
        // Secret properties are masked in the response.
        assert_eq!(mount["properties"]["s3.credentials.secret"], "******");

        let stored = handler
            .mount_manager
            .get_mount_info(&Path::from_str("/web-mnt-local").unwrap())
            .unwrap();
        assert!(stored.is_some());
    }

    #[test]
    fn mount_options_update_keeps_omitted_fields_none() {
        let mut req = mount_request("/mnt", "file:///tmp/mnt");
        req.update = true;
        let opts = mount_options(&req);
        assert!(opts.update);
        // Omitted optional fields stay None so merge_with keeps the stored values.
        assert_eq!(opts.ttl_ms, None);
        assert_eq!(opts.ttl_action, None);
        assert_eq!(opts.read_verify_ufs, None);
        assert_eq!(opts.auto_cache, None);
        assert_eq!(opts.access_mode, None);
        assert_eq!(opts.write_cache, None);
    }

    #[test]
    fn mount_options_update_applies_explicit_fields() {
        let mut req = mount_request("/mnt", "file:///tmp/mnt");
        req.update = true;
        req.auto_cache = Some(false);
        req.ttl_ms = Some(1000);
        req.read_verify_ufs = Some(true);
        req.ttl_action = Some(TtlAction::Free);
        let opts = mount_options(&req);
        assert!(opts.update);
        assert_eq!(opts.auto_cache, Some(false));
        assert_eq!(opts.ttl_ms, Some(1000));
        assert_eq!(opts.read_verify_ufs, Some(true));
        // ttl_action is never sent on update: the stored write type is immutable.
        assert_eq!(opts.ttl_action, None);
    }

    #[tokio::test]
    async fn umount_rejects_when_the_write_gate_is_disabled() {
        let handler = test_handler(false, true);
        let err = call_umount(&handler, umount_by_path("/x"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("web_write_enabled"), "{err}");
    }

    #[tokio::test]
    async fn umount_rejects_on_standby() {
        let handler = test_handler(true, false);
        let err = call_umount(&handler, umount_by_path("/x"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("active"), "{err}");
    }

    #[tokio::test]
    async fn umount_requires_a_selector() {
        let handler = test_handler(true, true);
        let err = call_umount(
            &handler,
            UmountRequest {
                cv_path: None,
                mount_id: None,
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("cv_path"), "{err}");
        // A blank cv_path with no id is refused as well.
        let err = call_umount(&handler, umount_by_path("   "))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("cv_path"), "{err}");
    }

    #[tokio::test]
    async fn umount_removes_a_mount_by_path_and_by_id() {
        use curvine_runtime::common::Utils;
        let handler = test_handler(true, true);

        // By Curvine path.
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/umount-a-{}",
            Utils::rand_str(6)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut req = mount_request("/web-umount-path", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        let _ = call_mount(&handler, req).await.unwrap();
        call_umount(&handler, umount_by_path("/web-umount-path"))
            .await
            .unwrap();
        assert!(handler
            .mount_manager
            .get_mount_info(&Path::from_str("/web-umount-path").unwrap())
            .unwrap()
            .is_none());

        // By mount id.
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/umount-b-{}",
            Utils::rand_str(6)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut req = mount_request("/web-umount-id", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        let Json(res) = call_mount(&handler, req).await.unwrap();
        let id = res["mount"]["mount_id"].as_u64().unwrap() as u32;
        call_umount(
            &handler,
            UmountRequest {
                cv_path: None,
                mount_id: Some(id),
            },
        )
        .await
        .unwrap();
        assert!(handler
            .mount_manager
            .get_mount_info(&Path::from_str("/web-umount-id").unwrap())
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn umount_rejects_missing_mount() {
        let handler = test_handler(true, true);
        let err = call_umount(&handler, umount_by_path("/no-such-mount"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("failed found"), "{err}");
        let err = call_umount(
            &handler,
            UmountRequest {
                cv_path: None,
                mount_id: Some(999_999),
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("failed found"), "{err}");
    }

    #[tokio::test]
    async fn mount_update_preserves_stored_values() {
        use curvine_runtime::common::Utils;
        let handler = test_handler(true, true);
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/upd-ufs-{}",
            Utils::rand_str(6)
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut req = mount_request("/web-update", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        req.ttl_ms = Some(123_456);
        req.auto_cache = Some(false);
        req.read_verify_ufs = Some(true);
        let _ = call_mount(&handler, req).await.unwrap();

        // Update only auto_cache; the omitted TTL and read_verify_ufs survive,
        // and the stored UFS path is kept even though the request carries none.
        let mut upd = mount_request("/web-update", "");
        upd.update = true;
        upd.auto_cache = Some(true);
        let Json(res) = call_mount(&handler, upd).await.unwrap();
        let mount = &res["mount"];
        assert_eq!(mount["ttl_ms"], 123_456);
        assert_eq!(mount["auto_cache"], true);
        assert_eq!(mount["read_verify_ufs"], true);
        assert_eq!(mount["ufs_path"], format!("file://{dir}"));
    }

    #[tokio::test]
    async fn mount_update_rejects_missing_mount() {
        let handler = test_handler(true, true);
        let mut req = mount_request("/no-such-mount", "");
        req.update = true;
        let err = call_mount(&handler, req).await.unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[tokio::test]
    async fn mount_update_rejects_when_the_write_gate_is_disabled() {
        let handler = test_handler(false, true);
        let mut req = mount_request("/x", "");
        req.update = true;
        let err = call_mount(&handler, req).await.unwrap_err();
        assert!(err.to_string().contains("web_write_enabled"), "{err}");
    }

    #[tokio::test]
    async fn mount_update_rejects_env_resolved_s3_credential() {
        let handler = test_handler(true, true);
        // Seed an environment-resolved S3 mount directly, bypassing the UFS probe.
        let mut info = MountOptions::builder()
            .build()
            .to_info(1, "/s3upd", "s3://bucket/s3upd");
        info.properties = HashMap::from([
            (
                "s3.endpoint_url".to_string(),
                "http://s3.example.com".to_string(),
            ),
            ("s3.region_name".to_string(), "cn-test-1".to_string()),
            ("s3.credentials.type".to_string(), "auto".to_string()),
        ]);
        handler.mount_manager.unprotected_add_mount(info).unwrap();

        // The update path must enforce the same credential gate as a new mount.
        let mut req = mount_request("/s3upd", "");
        req.update = true;
        let err = call_mount(&handler, req).await.unwrap_err();
        assert!(
            err.to_string().contains("s3_allow_env_credentials"),
            "{err}"
        );
    }

    use crate::master::mount::ResyncState;

    async fn call_resync(
        handler: &MasterRouterHandler,
        cv: &str,
        dry_run: bool,
    ) -> FsResult<Json<Value>> {
        resync_submit(
            Extension(Arc::new(handler.clone())),
            Json(ResyncRequest {
                cv_path: cv.to_string(),
                dry_run,
            }),
        )
        .await
    }

    /// Poll the task table until the resync for `cv` leaves the Running state.
    async fn wait_resync_finished(handler: &MasterRouterHandler, cv: &str) -> ResyncTask {
        for _ in 0..200 {
            if let Some(task) = handler.resync.get(cv) {
                if task.state != ResyncState::Running {
                    return task;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("resync for {cv} did not finish within 2s");
    }

    #[tokio::test]
    async fn resync_rejects_when_the_write_gate_is_disabled() {
        let handler = test_handler(false, true);
        let err = call_resync(&handler, "/x", false).await.unwrap_err();
        assert!(err.to_string().contains("web_write_enabled"), "{err}");
    }

    #[tokio::test]
    async fn resync_rejects_on_standby() {
        let handler = test_handler(true, false);
        let err = call_resync(&handler, "/x", false).await.unwrap_err();
        assert!(err.to_string().contains("active"), "{err}");
    }

    #[tokio::test]
    async fn resync_rejects_invalid_and_missing_paths() {
        let handler = test_handler(true, true);
        assert!(call_resync(&handler, "   ", false).await.is_err());
        let err = call_resync(&handler, "s3://bucket/x", false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("curvine path"), "{err}");
        let err = call_resync(&handler, "/no-such-mount", false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("mount info not found"), "{err}");
    }

    #[tokio::test]
    async fn resync_rejects_cache_mode_mount() {
        let handler = test_handler(true, true);
        let info = MountOptions::builder().build().to_info(
            1,
            "/mnt-cache-api",
            "file:///tmp/mnt-cache-api",
        );
        handler.mount_manager.unprotected_add_mount(info).unwrap();

        let err = call_resync(&handler, "/mnt-cache-api", false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("fs_mode"), "{err}");
        assert!(handler.resync.tasks().is_empty());
    }

    #[tokio::test]
    async fn resync_rejects_a_duplicate_running_task() {
        let handler = test_handler(true, true);
        let info = MountOptions::builder()
            .write_type(WriteType::FsMode)
            .build()
            .to_info(1, "/mnt-dup", "file:///tmp/mnt-dup");
        handler.mount_manager.unprotected_add_mount(info).unwrap();
        handler.resync.try_start("/mnt-dup", "manual").unwrap();

        let err = call_resync(&handler, "/mnt-dup", false).await.unwrap_err();
        assert!(err.to_string().contains("already running"), "{err}");
    }

    #[tokio::test]
    async fn resync_manual_dry_run_then_real_run() {
        use curvine_runtime::common::Utils;
        let handler = test_handler(true, true);
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/resync-ufs-{}",
            Utils::rand_str(6)
        ));
        std::fs::write(format!("{dir}/a.txt"), "hello").unwrap();
        let info = MountOptions::builder()
            .write_type(WriteType::FsMode)
            .build()
            .to_info(1, "/web-manual-resync", &format!("file://{dir}"));
        handler.mount_manager.unprotected_add_mount(info).unwrap();

        // Dry run reports the pending create without writing metadata.
        let Json(res) = call_resync(&handler, "/web-manual-resync", true)
            .await
            .unwrap();
        assert_eq!(res["started"], true);
        assert_eq!(res["cv_path"], "/web-manual-resync");
        let task = wait_resync_finished(&handler, "/web-manual-resync").await;
        assert_eq!(task.state, ResyncState::Done);
        assert_eq!(task.trigger, "manual");
        assert_eq!(task.stats.recreated, 1);
        assert!(handler.fs.file_status("/web-manual-resync/a.txt").is_err());

        // The real run creates the metadata.
        call_resync(&handler, "/web-manual-resync", false)
            .await
            .unwrap();
        let task = wait_resync_finished(&handler, "/web-manual-resync").await;
        assert_eq!(task.state, ResyncState::Done);
        assert_eq!(task.stats.recreated, 1);
        assert!(handler.fs.file_status("/web-manual-resync/a.txt").is_ok());

        // The status API lists the finished task.
        let Json(tasks) = resync_status(Extension(Arc::new(handler.clone())))
            .await
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].cv_path, "/web-manual-resync");
        assert_eq!(tasks[0].state, ResyncState::Done);
    }

    #[tokio::test]
    async fn first_fs_mode_mount_auto_resyncs() {
        use curvine_runtime::common::Utils;
        let handler = test_handler(true, true);
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/auto-resync-{}",
            Utils::rand_str(6)
        ));
        std::fs::write(format!("{dir}/a.txt"), "hello").unwrap();

        let mut req = mount_request("/web-auto-resync", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        req.write_type = Some(WriteType::FsMode);
        call_mount(&handler, req).await.unwrap();

        let task = wait_resync_finished(&handler, "/web-auto-resync").await;
        assert_eq!(task.state, ResyncState::Done);
        assert_eq!(task.trigger, "auto");
        assert_eq!(task.stats.recreated, 1);
        assert!(handler.fs.file_status("/web-auto-resync/a.txt").is_ok());
    }

    #[tokio::test]
    async fn cache_mode_mount_does_not_auto_resync() {
        use curvine_runtime::common::Utils;
        let handler = test_handler(true, true);
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/cache-no-resync-{}",
            Utils::rand_str(6)
        ));
        std::fs::write(format!("{dir}/a.txt"), "hello").unwrap();

        let mut req = mount_request("/web-cache-no-resync", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        call_mount(&handler, req).await.unwrap();

        assert!(handler.resync.tasks().is_empty());
    }

    #[tokio::test]
    async fn mount_update_does_not_start_a_resync() {
        use curvine_runtime::common::Utils;
        let handler = test_handler(true, true);
        let dir = Utils::test_sub_dir(format!(
            "router-handler-test/upd-no-resync-{}",
            Utils::rand_str(6)
        ));
        let mut req = mount_request("/web-upd-no-resync", &format!("file://{dir}"));
        req.check_path_consist = Some(false);
        req.write_type = Some(WriteType::FsMode);
        call_mount(&handler, req).await.unwrap();
        wait_resync_finished(&handler, "/web-upd-no-resync").await;

        let mut upd = mount_request("/web-upd-no-resync", "");
        upd.update = true;
        upd.ttl_ms = Some(999);
        call_mount(&handler, upd).await.unwrap();

        // Only the first-mount auto task exists; the update added none.
        assert_eq!(handler.resync.tasks().len(), 1);
    }
}
