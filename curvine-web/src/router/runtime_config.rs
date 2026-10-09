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

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use curvine_config::RuntimeConfigRegistry;
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
}

impl IntoResponse for RuntimeConfigHttpError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"message": self.message}))).into_response()
    }
}

pub type RuntimeConfigHttpResult<T> = Result<T, RuntimeConfigHttpError>;

/// Builds the JSON body for `GET …/config` style endpoints.
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
