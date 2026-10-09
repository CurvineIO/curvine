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

use crate::worker::task::TaskManager;
use crate::worker::Worker;
use axum::extract::{Extension, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use curvine_error::FsResult;
use curvine_fault::FaultHttpControl;
use curvine_rpc::server::ServerState;
use curvine_runtime::common::{ByteUnit, DurationUnit};
use curvine_runtime::sync::StateCtl;
use curvine_web::router::RouterHandler;
use once_cell::sync::OnceCell;
use serde_json::{json, Value};
use std::sync::Arc;

const TRANSFER_INFLIGHT_BUDGET_CONFIG: &str = "transfer_inflight_budget_bytes";
const WORKER_MAX_CONCURRENT_TASKS_CONFIG: &str = "worker_max_concurrent_tasks";
const TASK_TIMEOUT_CONFIG: &str = "task_timeout";
const TASK_REPORT_INTERVAL_CONFIG: &str = "task_report_interval";

#[derive(Debug)]
struct RuntimeConfigHttpError {
    status: StatusCode,
    message: String,
}

type RuntimeConfigHttpResult<T> = Result<T, RuntimeConfigHttpError>;

impl RuntimeConfigHttpError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: message.into(),
        }
    }
}

impl IntoResponse for RuntimeConfigHttpError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"message": self.message}))).into_response()
    }
}

#[derive(Clone)]
pub struct WorkerRouterHandler {
    pub(crate) fault_http: FaultHttpControl,
    pub(crate) server_state: Arc<OnceCell<StateCtl>>,
    pub(crate) task_manager: Arc<TaskManager>,
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

/// Parses one raw query value. StarRocks-style: sizes accept ByteUnit forms
/// ("100GB") or raw byte counts.
fn parse_inflight_budget(value: &str) -> RuntimeConfigHttpResult<i64> {
    let bytes = ByteUnit::from_str(value).map_err(|error| {
        RuntimeConfigHttpError::invalid(format!(
            "invalid {} value {:?}: {}",
            TRANSFER_INFLIGHT_BUDGET_CONFIG, value, error
        ))
    })?;
    let bytes = i64::try_from(bytes.as_byte()).map_err(|_| {
        RuntimeConfigHttpError::invalid(format!(
            "{} exceeds the maximum supported byte value",
            TRANSFER_INFLIGHT_BUDGET_CONFIG
        ))
    })?;
    if bytes <= 0 {
        return Err(RuntimeConfigHttpError::invalid(format!(
            "{} must be greater than zero",
            TRANSFER_INFLIGHT_BUDGET_CONFIG
        )));
    }
    Ok(bytes)
}

/// Parses a duration value ("30m", "1h", "500ms") into milliseconds.
fn parse_duration_ms(config: &str, value: &str) -> RuntimeConfigHttpResult<u64> {
    let ms = DurationUnit::from_str(value)
        .map_err(|error| {
            RuntimeConfigHttpError::invalid(format!(
                "invalid {} value {:?}: {}",
                config, value, error
            ))
        })?
        .as_millis();
    if ms == 0 {
        return Err(RuntimeConfigHttpError::invalid(format!(
            "{} must be greater than zero",
            config
        )));
    }
    Ok(ms)
}

/// Parses a positive integer value.
fn parse_usize(config: &str, value: &str) -> RuntimeConfigHttpResult<usize> {
    let parsed: usize = value.parse().map_err(|_| {
        RuntimeConfigHttpError::invalid(format!(
            "invalid {} value {:?}: must be a positive integer",
            config, value
        ))
    })?;
    if parsed == 0 {
        return Err(RuntimeConfigHttpError::invalid(format!(
            "{} must be greater than zero",
            config
        )));
    }
    Ok(parsed)
}

/// Writable runtime configs. Names mirror their config-file keys so an
/// update looks exactly like the corresponding TOML line. Every entry here
/// takes effect without restarting the worker.
const WRITABLE_CONFIGS: &[&str] = &[
    TRANSFER_INFLIGHT_BUDGET_CONFIG,
    WORKER_MAX_CONCURRENT_TASKS_CONFIG,
    TASK_TIMEOUT_CONFIG,
    TASK_REPORT_INTERVAL_CONFIG,
];

/// Dispatches `config_name=value` to its runtime setter, returning
/// (config, previous, new). The four writable configs take effect as follows:
/// byte budget immediately for new admissions; concurrency resizes the
/// permit pool at once; timeout/report interval apply to tasks submitted
/// afterwards.
fn apply_runtime_config(
    task_manager: &TaskManager,
    name: &str,
    value: &str,
) -> RuntimeConfigHttpResult<(&'static str, Value)> {
    let (config, previous, new_value) = match name {
        TRANSFER_INFLIGHT_BUDGET_CONFIG => {
            let bytes = parse_inflight_budget(value)?;
            let previous = task_manager
                .update_transfer_inflight_budget_bytes(bytes)
                .map_err(|error| RuntimeConfigHttpError::invalid(error.to_string()))?;
            (
                TRANSFER_INFLIGHT_BUDGET_CONFIG,
                Value::from(previous),
                Value::from(bytes),
            )
        }
        WORKER_MAX_CONCURRENT_TASKS_CONFIG => {
            let target = parse_usize(name, value)?;
            let previous = task_manager
                .update_worker_max_concurrent_tasks(target)
                .map_err(|error| RuntimeConfigHttpError::invalid(error.to_string()))?;
            (
                WORKER_MAX_CONCURRENT_TASKS_CONFIG,
                Value::from(previous),
                Value::from(target),
            )
        }
        TASK_TIMEOUT_CONFIG => {
            let ms = parse_duration_ms(name, value)?;
            let previous = task_manager
                .update_task_timeout(ms)
                .map_err(|error| RuntimeConfigHttpError::invalid(error.to_string()))?;
            (TASK_TIMEOUT_CONFIG, Value::from(previous), Value::from(ms))
        }
        TASK_REPORT_INTERVAL_CONFIG => {
            let ms = parse_duration_ms(name, value)?;
            let previous = task_manager
                .update_task_report_interval(ms)
                .map_err(|error| RuntimeConfigHttpError::invalid(error.to_string()))?;
            (
                TASK_REPORT_INTERVAL_CONFIG,
                Value::from(previous),
                Value::from(ms),
            )
        }
        _ => {
            return Err(RuntimeConfigHttpError::invalid(format!(
                "unsupported runtime configuration {:?}; writable: {:?}",
                name, WRITABLE_CONFIGS
            )))
        }
    };
    Ok((
        config,
        serde_json::json!({"previous": previous, "value": new_value}),
    ))
}

fn runtime_config_view(task_manager: &TaskManager) -> Value {
    json!({
        TRANSFER_INFLIGHT_BUDGET_CONFIG: task_manager.transfer_inflight_budget_bytes(),
        WORKER_MAX_CONCURRENT_TASKS_CONFIG: task_manager.worker_max_concurrent_tasks(),
        TASK_TIMEOUT_CONFIG: format!("{}ms", task_manager.task_timeout_ms()),
        TASK_REPORT_INTERVAL_CONFIG: format!("{}ms", task_manager.task_report_interval_ms()),
        "persistent": false,
        "scheduler_propagation": "next worker heartbeat",
    })
}

async fn runtime_config(
    Extension(instance): Extension<Arc<WorkerRouterHandler>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    Ok(Json(runtime_config_view(&instance.task_manager)))
}

/// Follows StarRocks' one-config-per-request convention:
/// `POST /api/update_config?transfer_inflight_budget_bytes=100GB`.
async fn update_runtime_config(
    Extension(instance): Extension<Arc<WorkerRouterHandler>>,
    Query(params): Query<Vec<(String, String)>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    let [(name, value)] = params.as_slice() else {
        return Err(RuntimeConfigHttpError::invalid(
            "exactly one config_name=value parameter is required",
        ));
    };
    let (config, values) = apply_runtime_config(&instance.task_manager, name, value)?;
    Ok(Json(json!({
        "config": config,
        "previous_value": values["previous"],
        "value": values["value"],
        "persistent": false,
        "scheduler_propagation": "next worker heartbeat",
    })))
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
}

#[cfg(test)]
#[path = "tests/runtime_config_test.rs"]
mod runtime_config_test;
