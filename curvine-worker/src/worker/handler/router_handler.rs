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

use crate::worker::block::MasterRegistration;
use crate::worker::task::TaskManager;
use crate::worker::Worker;
use axum::extract::{Extension, Query};
use axum::http::StatusCode;
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

#[derive(Clone)]
pub struct WorkerRouterHandler {
    pub(crate) fault_http: FaultHttpControl,
    pub(crate) server_state: Arc<OnceCell<StateCtl>>,
    #[allow(dead_code)] // used by integration tests and registry wiring
    pub(crate) task_manager: Arc<TaskManager>,
    pub(crate) runtime_config: Arc<RuntimeConfigRegistry>,
    pub(crate) registration: MasterRegistration,
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

async fn readyz(
    server_state: Arc<OnceCell<StateCtl>>,
    registration: MasterRegistration,
) -> (StatusCode, &'static str) {
    worker_ready(is_serving(&server_state), registration.ready())
}

fn worker_ready(serving: bool, registered: bool) -> (StatusCode, &'static str) {
    if serving && registered {
        (StatusCode::OK, "ok\n")
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "worker is not registered\n",
        )
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
        let ready_state = self.server_state.clone();
        let registration = self.registration.clone();
        let instance = Arc::new(self.clone());
        let router = Router::new()
            .route("/metrics", get(metrics))
            .route("/healthz", get(move || healthz(server_state.clone())))
            .route(
                "/readyz",
                get(move || readyz(ready_state.clone(), registration.clone())),
            )
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
    }

    #[test]
    fn readyz_requires_start_and_running_heartbeat() {
        assert_eq!(worker_ready(true, false).0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(worker_ready(false, true).0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(worker_ready(true, true).0, StatusCode::OK);
    }

    #[test]
    fn parse_helpers_match_registry() {
        use crate::worker::runtime_config::{
            build_worker_runtime_registry, TRANSFER_INFLIGHT_BUDGET_CONFIG,
        };

        let conf = ClusterConf::default();
        let tm = Arc::new(
            TaskManager::with_rt(
                Arc::new(curvine_runtime::runtime::Runtime::single()),
                &conf,
                "parse",
            )
            .unwrap(),
        );
        let registry = build_worker_runtime_registry(tm, &conf);
        registry
            .set(TRANSFER_INFLIGHT_BUDGET_CONFIG, "100GB", None)
            .unwrap();
    }
}

#[cfg(test)]
#[path = "tests/runtime_config_test.rs"]
mod runtime_config_test;
