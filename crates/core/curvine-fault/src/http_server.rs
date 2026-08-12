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

//! Axum control plane for remote fault injection.
//!
//! [`fault_router`] is the low-level API for mounting a selected Runtime.
//! Normal hosts use [`crate::FaultHttpControl`] to mount the process Runtime.
//! Every route requires a bearer token; an empty token rejects all requests,
//! so a misconfigured host fails closed.

use crate::{
    ConfigureFaultRule, FaultRuleView, FaultRuntime, FaultRuntimeError, FaultRuntimeStatus,
};
use axum::extract::{Path, Request, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde_json::json;
use std::sync::Arc;

/// Bearer-token guard for the fault HTTP API.
#[derive(Clone)]
pub struct FaultHttpAuth {
    token: Arc<str>,
}

impl FaultHttpAuth {
    /// Creates a guard from a shared secret. An empty token never authorizes
    /// any request, so callers without a secret expose only 401 responses.
    pub fn new(token: impl AsRef<str>) -> Self {
        Self {
            token: Arc::from(token.as_ref()),
        }
    }
}

#[derive(Clone)]
struct FaultHttpState {
    runtime: FaultRuntime,
}

#[derive(Debug)]
struct FaultApiError {
    status: StatusCode,
    message: String,
}

impl From<FaultRuntimeError> for FaultApiError {
    fn from(value: FaultRuntimeError) -> Self {
        let status = match value {
            FaultRuntimeError::UnknownPoint(_) | FaultRuntimeError::InvalidRule(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
        };
        Self {
            status,
            message: value.to_string(),
        }
    }
}

impl IntoResponse for FaultApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"message": self.message}))).into_response()
    }
}

fn authorize(auth: &FaultHttpAuth, headers: &HeaderMap) -> Result<(), FaultApiError> {
    if auth.token.is_empty() {
        return Err(FaultApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "fault injection bearer token is not configured".to_string(),
        });
    }
    let provided = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided.is_some_and(|provided| constant_time_eq(auth.token.as_bytes(), provided.as_bytes()))
    {
        Ok(())
    } else {
        Err(FaultApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "fault injection bearer token is missing or invalid".to_string(),
        })
    }
}

async fn status(
    State(state): State<FaultHttpState>,
) -> Result<Json<FaultRuntimeStatus>, FaultApiError> {
    Ok(Json(state.runtime.status()))
}

async fn configure(
    State(state): State<FaultHttpState>,
    Path(rule_id): Path<String>,
    Json(rule): Json<ConfigureFaultRule>,
) -> Result<Json<FaultRuleView>, FaultApiError> {
    Ok(Json(state.runtime.configure(rule_id, rule)?))
}

async fn remove(
    State(state): State<FaultHttpState>,
    Path(rule_id): Path<String>,
) -> Result<Json<serde_json::Value>, FaultApiError> {
    let removed = state.runtime.remove(rule_id)?;
    Ok(Json(json!({"removed": removed})))
}

async fn get_rule(
    State(state): State<FaultHttpState>,
    Path(rule_id): Path<String>,
) -> Result<Json<FaultRuleView>, FaultApiError> {
    state
        .runtime
        .rule(&rule_id)?
        .map(Json)
        .ok_or_else(|| FaultApiError {
            status: StatusCode::NOT_FOUND,
            message: format!("unknown fault rule: {rule_id}"),
        })
}

async fn clear(
    State(state): State<FaultHttpState>,
) -> Result<Json<serde_json::Value>, FaultApiError> {
    let cleared = state.runtime.clear()?;
    Ok(Json(json!({"cleared": cleared})))
}

async fn require_auth(
    State(auth): State<FaultHttpAuth>,
    request: Request,
    next: Next,
) -> Result<Response, FaultApiError> {
    authorize(&auth, request.headers())?;
    Ok(next.run(request).await)
}

/// Builds the fault API router for one runtime.
pub fn fault_router(runtime: FaultRuntime, auth: FaultHttpAuth) -> Router {
    Router::new()
        .route("/api/v1/debug/faults", get(status))
        .route("/api/v1/debug/faults/rules", delete(clear))
        .route(
            "/api/v1/debug/faults/rules/:rule_id",
            get(get_rule).put(configure).delete(remove),
        )
        .route_layer(middleware::from_fn_with_state(auth, require_auth))
        .with_state(FaultHttpState { runtime })
}

/// Builds the fault API router for the lazily initialized process runtime.
pub(crate) fn process_fault_router(auth: FaultHttpAuth) -> Router {
    fault_router(FaultRuntime::process().clone(), auth)
}

fn constant_time_eq(expected: &[u8], provided: &[u8]) -> bool {
    if expected.len() != provided.len() {
        return false;
    }
    expected
        .iter()
        .zip(provided)
        .fold(0_u8, |diff, (left, right)| diff | (left ^ right))
        == 0
}

#[cfg(all(test, feature = "fault-injection", feature = "http-client"))]
mod tests {
    use super::*;
    use crate::{
        fault_point, FaultAction, FaultContext, FaultController, FaultHttpController,
        FaultTestSession, FAULT_POINT_REGISTRATIONS,
    };
    use std::time::Duration;

    const SERVER_POINT: &str = "worker.http_server.test";

    #[allow(dead_code)]
    fn declare_server_point(runtime: &FaultRuntime) {
        fault_point! {
            sync,
            name: "worker.http_server.test",
            description: "http server test point",
            runtime: runtime,
            context: {},
        }
    }

    fn test_runtime() -> FaultRuntime {
        FaultRuntime::isolated().unwrap()
    }

    #[tokio::test]
    async fn authenticated_http_lifecycle() {
        let runtime = test_runtime();
        let auth = FaultHttpAuth::new("secret");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let served_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, fault_router(served_runtime, auth))
                .await
                .unwrap();
        });

        let controller =
            Arc::new(FaultHttpController::new(format!("http://{address}"), "secret").unwrap());
        let mut session = FaultTestSession::new();
        session.add_target("worker", controller.clone());
        session.preflight().await.unwrap();

        let view = session
            .configure(
                "worker",
                "record",
                ConfigureFaultRule {
                    point: SERVER_POINT.to_string(),
                    action: FaultAction::Record,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(view.id, "record");

        assert_eq!(
            controller.rule("record").await.unwrap().unwrap().id,
            "record"
        );
        assert!(controller.rule("missing").await.unwrap().is_none());

        let point = FAULT_POINT_REGISTRATIONS
            .iter()
            .find(|registration| registration.point.name == SERVER_POINT)
            .unwrap()
            .point;
        runtime.evaluate(point, &FaultContext::default());
        assert_eq!(
            session
                .wait_for_executions("worker", "record", 1, Duration::from_secs(1))
                .await
                .unwrap()
                .executions,
            1
        );
        session.cleanup().await.unwrap();

        server.abort();
    }

    #[tokio::test]
    async fn authentication_precedes_json_body_extraction() {
        let runtime = test_runtime();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                fault_router(runtime, FaultHttpAuth::new("secret")),
            )
            .await
            .unwrap();
        });

        let response = reqwest::Client::new()
            .put(format!(
                "http://{address}/api/v1/debug/faults/rules/unauthorized"
            ))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);

        server.abort();
    }

    #[test]
    fn bearer_auth_requires_an_exact_nonempty_token() {
        let auth = FaultHttpAuth::new("secret");
        let mut headers = HeaderMap::new();
        assert_eq!(
            authorize(&auth, &headers).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );

        for value in ["Bearer wrong", "Basic secret"] {
            headers.insert(AUTHORIZATION, value.parse().unwrap());
            assert_eq!(
                authorize(&auth, &headers).unwrap_err().status,
                StatusCode::UNAUTHORIZED
            );
        }

        headers.insert(AUTHORIZATION, "Bearer secret".parse().unwrap());
        assert!(authorize(&auth, &headers).is_ok());

        headers.insert(AUTHORIZATION, "Bearer ".parse().unwrap());
        assert_eq!(
            authorize(&FaultHttpAuth::new(""), &headers)
                .unwrap_err()
                .status,
            StatusCode::UNAUTHORIZED
        );
    }
}
