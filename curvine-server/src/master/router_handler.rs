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
use std::sync::Arc;

use axum::extract::Query;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde_json::{json, Value};

use curvine_common::conf::ClusterConf;
use curvine_common::state::{FileBlocks, FileStatus, WorkerInfo};
use curvine_common::FsResult;
use curvine_web::router::RouterHandler;
use orpc::common::LocalTime;
use orpc::err_box;

use crate::master::fs::MasterFilesystem;
use crate::master::Master;

#[derive(Clone)]
pub struct MasterRouterHandler {
    fs: MasterFilesystem,
    conf: ClusterConf,
    start_time: String,
}

impl MasterRouterHandler {
    pub fn new(conf: ClusterConf, fs: MasterFilesystem) -> Self {
        Self {
            fs,
            conf,
            start_time: LocalTime::now_datetime(),
        }
    }
}

fn get_report(fs: MasterFilesystem) -> HashMap<String, String> {
    let metrics = Master::get_metrics();
    let output = metrics.text_output(fs).unwrap();
    let report = output
        .lines()
        .filter(|line| !line.starts_with("#"))
        .map(|line| {
            let mut parts = line.split_whitespace();
            let name = parts.next().unwrap();
            let value = parts.next().unwrap();
            let value = match name {
                "capacity" | "available" | "fs_used" => {
                    let v = value.parse::<f64>().unwrap_or(0.0);
                    let v = v / 1024.0 / 1024.0 / 1024.0;
                    format!("{:.2}GB", v)
                }
                _ => value.to_string(),
            };
            let name = name.to_string();
            (name, value)
        })
        .collect();
    report
}

async fn metrics(Extension(instance): Extension<Arc<MasterRouterHandler>>) -> String {
    let metrics = Master::get_metrics();
    metrics.text_output(instance.fs.clone()).unwrap()
}

async fn report(Extension(instance): Extension<Arc<MasterRouterHandler>>) -> String {
    let report = get_report(instance.fs.clone());
    let available = &report.get("available").unwrap();
    let capacity = &report.get("capacity").unwrap();
    let fs_used = &report.get("fs_used").unwrap();
    let dir_total = &report.get("inode_dir_num").unwrap();
    let files_total = &report.get("inode_file_num").unwrap();
    let live_workers = &report.get("live_workers").unwrap();
    let lost_workers = &report.get("lost_workers").unwrap();

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
    result
}

async fn overview(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Value>> {
    let fs = &instance.fs;
    let conf = &instance.conf;
    let start_time = &instance.start_time;
    let master_info = fs.master_info().unwrap();

    let (files_total, dir_total) = {
        let (f, d) = fs.get_file_counts();
        (f as usize, d as usize)
    };

    let expected_capacity = master_info.available + master_info.fs_used;
    if master_info.capacity != expected_capacity {
        log::warn!(
            "Capacity inconsistency detected: capacity={}, available={}, fs_used={}, expected_capacity={}",
            master_info.capacity,
            master_info.available,
            master_info.fs_used,
            expected_capacity
        );
    } else {
        log::debug!(
            "Capacity consistency verified: capacity={}, available={}, fs_used={}",
            master_info.capacity,
            master_info.available,
            master_info.fs_used
        );
    }

    let master_state = format!("{:?}", fs.master_monitor.journal_state());

    let res = Json(json!({
        "cluster_id": conf.cluster_id,
        "master_addr": conf.master_addr().to_string(),
        "start_time": start_time,
        "live_workers": master_info.live_workers.len(),
        "lost_workers": master_info.lost_workers.len(),
        "available": master_info.available,
        "capacity": master_info.capacity,
        "fs_used": master_info.fs_used,
        "reserved_bytes": master_info.reserved_bytes,
        "files_total": files_total,
        "dir_total": dir_total,
        "master_state": master_state,
    }));
    Ok(res)
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
    let path = params.get("path").expect("not found path");
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
            let list = v.split(",").map(|x| x.to_string()).collect();
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
            let list = v.split(",").map(|x| x.to_string()).collect();
            let res = wm.remove_dcm(list);
            Ok(Json(res))
        }
    }
}

async fn workers1(
    Extension(instance): Extension<Arc<MasterRouterHandler>>,
) -> FsResult<Json<Vec<String>>> {
    let fs = &instance.fs;
    let wm = fs.worker_manager.read();
    Ok(Json(wm.worker_list()))
}

impl RouterHandler for MasterRouterHandler {
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
            .route("/add-dcm", get(add_dcm))
            .route("/get-dcm", get(get_dcm))
            .route("/remove-dcm", get(remove_dcm))
            .route("/workers", get(workers1))
            .layer(Extension(instance))
    }
}
