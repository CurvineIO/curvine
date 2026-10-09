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

use crate::worker::runtime_config;
use crate::worker::task::TaskManager;
use crate::worker::Worker;
use axum::extract::{Extension, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use curvine_config::RuntimeConfigRegistry;
use curvine_error::FsResult;
use curvine_fault::FaultHttpControl;
use curvine_rpc::server::ServerState;
use curvine_runtime::sync::StateCtl;
use curvine_web::router::{
    get_runtime_config, post_update_config, RouterHandler, RuntimeConfigHttpResult,
};
use once_cell::sync::OnceCell;
use serde_json::{json, Value};
use std::sync::Arc;

pub use crate::worker::runtime_config::{
    TASK_REPORT_INTERVAL_CONFIG, TASK_TIMEOUT_CONFIG, TRANSFER_INFLIGHT_BUDGET_CONFIG,
    WORKER_MAX_CONCURRENT_TASKS_CONFIG,
};

#[derive(Clone)]
pub struct WorkerRouterHandler {
    pub(crate) fault_http: FaultHttpControl,
    pub(crate) server_state: Arc<OnceCell<StateCtl>>,
    pub(crate) task_manager: Arc<TaskManager>,
    pub(crate) runtime_config: Arc<RuntimeConfigRegistry>,
}

fn worker_config_extra() -> Value {
    json!({
        "scheduler_propagation": "next worker heartbeat",
    })
}

async fn metrics() -> FsResult<String> {
    let metrics = Worker::get_metrics()?;
    Ok(metrics.text_output()?)
}

async fn healthz(server_state: Arc<OnceCell<StateCtl>>) -> (StatusCode, &'static str) {
    if is_serving(&server_state) {
        (StatusCode::OK, "ok\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "worker is not running\n")
    }
}

fn is_serving(server_state: &OnceCell<StateCtl>) -> bool {
    server_state
        .get()
        .is_some_and(|ctl| ServerState::from(ctl.value()) == ServerState::Running)
}

async fn runtime_config(
    Extension(instance): Extension<Arc<WorkerRouterHandler>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    get_runtime_config(instance.runtime_config.clone(), worker_config_extra()).await
}

async fn update_runtime_config(
    Extension(instance): Extension<Arc<WorkerRouterHandler>>,
    query: Query<Vec<(String, String)>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    post_update_config(
        instance.runtime_config.clone(),
        query,
        worker_config_extra(),
    )
    .await
}

impl RouterHandler for WorkerRouterHandler {
    fn router(&self) -> Router {
        let server_state = self.server_state.clone();
        let instance = Arc::new(self.clone());
        let router = Router::new()
            .route("/metrics", get(metrics))
            .route("/healthz", get(move || healthz(server_state.clone())))
            .route("/api/v1/worker/config", get(runtime_config))
            .route("/api/update_config", post(update_runtime_config))
            .layer(Extension(instance));

        self.fault_http.mount(router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::ClusterConf;

    #[test]
    fn healthz_ok_only_while_running() {
        let ctl = StateCtl::new(ServerState::Init.into());
        let cell = OnceCell::new();

        assert!(!is_serving(&cell));

        cell.set(ctl.read_only()).ok();
        assert!(!is_serving(&cell));

        ctl.set_state(ServerState::Running);
        assert!(is_serving(&cell));

        ctl.set_state(ServerState::Shutdown);
        assert!(!is_serving(&cell));

        ctl.set_state(ServerState::Stop);
        assert!(!is_serving(&cell));
    }

    #[test]
    fn parse_helpers_match_registry() {
        let conf = ClusterConf::default();
        let tm = Arc::new(
            TaskManager::with_rt(
                Arc::new(curvine_runtime::runtime::Runtime::single()),
                &conf,
                "parse",
            )
            .unwrap(),
        );
        let registry = runtime_config::build_worker_runtime_registry(tm, &conf);
        registry
            .set(TRANSFER_INFLIGHT_BUDGET_CONFIG, "100GB", None)
            .unwrap();
    }
}

#[cfg(test)]
#[path = "tests/runtime_config_test.rs"]
mod runtime_config_test;
