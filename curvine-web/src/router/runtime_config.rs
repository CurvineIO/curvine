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

//! Shared HTTP handlers for StarRocks-style runtime config push (`#236` / `#266`).
//!
//! Two surfaces live here:
//!
//! * [`get_config_view`] — the full effective view (values + provenance +
//!   tunables). This is what a process publishes about itself; every service
//!   mounts it on `GET /api/v1/{role}/config`.
//! * [`get_runtime_config`] / [`post_update_config`] — the original flat
//!   key/value surface. `post_update_config` still backs `POST /api/update_config`
//!   on all three processes; `get_runtime_config` is kept only as the deprecated
//!   predecessor of [`get_config_view`].

use axum::extract::{Extension, Query};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use curvine_config::{effective_config_view, ConfigView, ViewProfile};
use curvine_config::{ClusterConf, ConfigSourceMap, RuntimeConfigRegistry};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug)]
pub struct RuntimeConfigHttpError {
    status: StatusCode,
    message: String,
}

impl RuntimeConfigHttpError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: message.into(),
        }
    }

    /// The request was well formed; the process failed to answer it.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for RuntimeConfigHttpError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"message": self.message}))).into_response()
    }
}

pub type RuntimeConfigHttpResult<T> = Result<T, RuntimeConfigHttpError>;

/// Everything a process needs to publish its own effective configuration view.
///
/// The configuration is held as `Arc<ClusterConf>` because the same snapshot is
/// already shared with the registry and the service; cloning it per request
/// would copy the whole tree on every scrape.
///
/// `extra` carries the small number of response fields that are process
/// specific rather than configuration derived — today only the worker's
/// `scheduler_propagation` hint. It is merged into the body *after* the view
/// fields are written, so it can never shadow `values` / `runtime` / `sources`.
#[derive(Clone)]
pub struct ConfigViewState {
    /// Process role published as `role`, e.g. `worker`.
    pub role: &'static str,
    /// Address this process serves from, published as `node`.
    pub node: String,
    /// The configuration snapshot this process is running with.
    pub conf: Arc<ClusterConf>,
    /// Which validation profile applies to `conf`.
    pub profile: ViewProfile,
    /// Layer attribution recorded when `conf` was loaded.
    pub sources: ConfigSourceMap,
    /// Process-local runtime overrides to overlay onto `conf`.
    pub registry: Arc<RuntimeConfigRegistry>,
    /// Process-specific response fields merged into the published body.
    pub extra: Value,
}

impl ConfigViewState {
    pub fn new(
        role: &'static str,
        node: String,
        conf: Arc<ClusterConf>,
        profile: ViewProfile,
        sources: ConfigSourceMap,
        registry: Arc<RuntimeConfigRegistry>,
    ) -> Self {
        Self {
            role,
            node,
            conf,
            profile,
            sources,
            registry,
            extra: Value::Null,
        }
    }
}

/// Builds the JSON body for `GET …/config` style endpoints — the full view.
///
/// `role` and `node` are process facts, not configuration, so they are passed in
/// rather than derived: the console uses them to label which process answered.
pub fn config_view_response(view: &ConfigView, role: &str, node: &str) -> Value {
    json!({
        "role": role,
        "node": node,
        "values": view.values,
        "sources": view.sources,
        "runtime": view.runtime,
        "writable": view.writable,
        "unresolved": view.unresolved,
        // Runtime overrides are process-local and volatile; nothing here
        // survives a restart, and the console must not imply otherwise.
        "persistent": false,
    })
}

/// Builds the current body from `state`'s snapshot and live overrides.
///
/// Split out from the handler so the response can be produced (and tested)
/// without an HTTP round trip.
pub fn config_view_for_state(state: &ConfigViewState) -> RuntimeConfigHttpResult<Value> {
    let view = effective_config_view(&state.conf, state.profile, &state.registry, &state.sources)
        .map_err(|error| RuntimeConfigHttpError::internal(error.to_string()))?;
    let mut body = config_view_response(&view, state.role, &state.node);
    if let Value::Object(ref mut map) = body {
        if let Value::Object(extra) = state.extra.clone() {
            for (k, v) in extra {
                // The view owns its own fields; a process-specific extra may add
                // to the body but must never replace the view it describes.
                if !map.contains_key(&k) {
                    map.insert(k, v);
                }
            }
        }
    }
    Ok(body)
}

/// `GET /api/v1/{role}/config` — the process's own effective configuration.
pub async fn get_config_view(
    Extension(state): Extension<Arc<ConfigViewState>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    Ok(Json(config_view_for_state(&state)?))
}

/// Builds the JSON body for the legacy flat `GET …/config` surface.
pub fn runtime_config_view(registry: &RuntimeConfigRegistry, extra: Value) -> Value {
    let mut body = registry.list_as_json();
    if let Value::Object(ref mut map) = body {
        if let Value::Object(extra_map) = extra {
            for (k, v) in extra_map {
                map.insert(k, v);
            }
        }
        map.insert("persistent".to_string(), Value::Bool(false));
    }
    body
}

/// Legacy flat read surface.
///
/// Superseded by [`get_config_view`], which additionally reports provenance and
/// the values the process is *not* running with. Kept because the flat shape is
/// still the documented shape of nothing else — no route mounts it any more.
#[deprecated(note = "use get_config_view")]
pub async fn get_runtime_config(
    registry: Arc<RuntimeConfigRegistry>,
    extra: Value,
) -> RuntimeConfigHttpResult<Json<Value>> {
    Ok(Json(runtime_config_view(&registry, extra)))
}

/// `POST /api/update_config?config_name=value` — exactly one query parameter.
pub async fn post_update_config(
    registry: Arc<RuntimeConfigRegistry>,
    Query(params): Query<Vec<(String, String)>>,
    extra_on_success: Value,
) -> RuntimeConfigHttpResult<Json<Value>> {
    let [(name, value)] = params.as_slice() else {
        return Err(RuntimeConfigHttpError::invalid(
            "exactly one config_name=value parameter is required",
        ));
    };
    let result = registry
        .set(name, value, None)
        .map_err(|error| RuntimeConfigHttpError::invalid(error.to_string()))?;
    let mut body = json!({
        "config": result.config,
        "previous_value": result.previous_value,
        "value": result.value,
        "persistent": result.persistent,
    });
    if let Value::Object(ref mut map) = body {
        if let Value::Object(extra_map) = extra_on_success {
            for (k, v) in extra_map {
                map.insert(k, v);
            }
        }
    }
    Ok(Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::{ConfigSource, RuntimeEntryView};

    fn sample_view() -> ConfigView {
        let mut sources = ConfigSourceMap::new();
        sources.insert("worker.hostname".to_string(), ConfigSource::Env);
        ConfigView {
            values: json!({"worker": {"hostname": "node-1"}}),
            sources,
            runtime: vec![RuntimeEntryView {
                key: "worker_max_concurrent_tasks".to_string(),
                path: "job.worker_max_concurrent_tasks".to_string(),
                file_value: json!(64),
                override_value: Some(json!(32)),
                effective_value: json!(32),
            }],
            writable: vec![
                "transfer_inflight_budget_bytes".to_string(),
                "worker_max_concurrent_tasks".to_string(),
            ],
            unresolved: vec![],
        }
    }

    #[test]
    fn config_view_response_shape() {
        let body = config_view_response(&sample_view(), "worker", "node-1:9011");

        // Every field the console reads must be present; a missing one is
        // rendered as "unknown" rather than failing loudly, so assert presence.
        for field in [
            "role",
            "node",
            "values",
            "sources",
            "runtime",
            "writable",
            "unresolved",
            "persistent",
        ] {
            assert!(
                body.get(field).is_some(),
                "response is missing field {field}: {body}"
            );
        }

        assert_eq!(body["role"], json!("worker"));
        assert_eq!(body["node"], json!("node-1:9011"));
        assert_eq!(body["persistent"], json!(false));

        // Values carry the effective tree, sources a path->layer map.
        assert_eq!(body["values"]["worker"]["hostname"], json!("node-1"));
        assert_eq!(body["sources"]["worker.hostname"], json!("env"));

        // The tunable surface is a list of registry keys, and the runtime
        // entries show the override next to the file value it replaced.
        assert_eq!(
            body["writable"],
            json!([
                "transfer_inflight_budget_bytes",
                "worker_max_concurrent_tasks"
            ])
        );
        assert_eq!(
            body["runtime"][0]["key"],
            json!("worker_max_concurrent_tasks")
        );
        assert_eq!(body["runtime"][0]["file_value"], json!(64));
        assert_eq!(body["runtime"][0]["override_value"], json!(32));
        assert_eq!(body["runtime"][0]["effective_value"], json!(32));
        assert_eq!(body["unresolved"], json!([]));
    }

    #[test]
    fn config_view_response_reports_unresolved_keys() {
        let mut view = sample_view();
        view.unresolved = vec!["master.dir_space_enabled".to_string()];
        view.runtime.clear();

        let body = config_view_response(&view, "master", "node-1:9010");

        // A registered key whose configuration path does not resolve must be
        // reported, not silently dropped: an operator who pushed it would
        // otherwise see a success response and no effect.
        assert_eq!(
            body["unresolved"],
            json!(["master.dir_space_enabled".to_string()])
        );
        assert_eq!(body["runtime"], json!([]));
    }

    #[test]
    fn internal_error_is_a_server_error() {
        let error = RuntimeConfigHttpError::internal("boom");
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.message, "boom");
    }

    #[test]
    fn config_view_for_state_merges_extra_without_shadowing_view_fields() {
        let registry = RuntimeConfigRegistry::builder().build();
        let mut state = ConfigViewState::new(
            "worker",
            "node-1:9011".to_string(),
            Arc::new(ClusterConf::default()),
            ViewProfile::Full,
            ConfigSourceMap::new(),
            Arc::new(registry),
        );
        state.extra = json!({
            "scheduler_propagation": "next worker heartbeat",
            // A process-specific extra must not be able to replace a view field.
            "values": "clobbered",
        });

        let body = config_view_for_state(&state).expect("view body");

        assert_eq!(body["role"], json!("worker"));
        assert_eq!(
            body["scheduler_propagation"],
            json!("next worker heartbeat")
        );
        // `values` is written by the view; `extra` must not win that race.
        assert!(
            body["values"].is_object(),
            "extra clobbered the values tree: {body}"
        );
    }
}
