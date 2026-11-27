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
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde_json::{json, Value};

use curvine_client_core::file::{FsClient, FsContext};
use curvine_config::ClusterConf;
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_fault::FaultHttpControl;
use curvine_model::{FileBlocks, FileStatus, FilesystemInfo, ListOptions, MountInfo, WorkerInfo};
use curvine_raft::raft::storage::RocksLogStorage;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::Runtime;
use curvine_web::router::RouterHandler;
use futures::future::BoxFuture;

use crate::master::fs::{MasterFilesystem, SessionSummary};
use crate::master::meta::parse_glob_pattern;
use crate::master::{MasterMetrics, MountManager};
use curvine_fs_api::Path;

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
                ("s3.credentials.access".to_string(), "access-value".to_string()),
                ("s3.credentials.secret".to_string(), "secret-value".to_string()),
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
        assert_eq!(
            masked.properties["s3.endpoint_url"],
            "https://example.com"
        );
        // The source entry is untouched.
        assert_eq!(info.properties["s3.credentials.access"], "access-value");
    }
}
