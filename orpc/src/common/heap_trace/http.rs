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

use crate::common::heap_trace::{latest_summary, HeapProfileSummary, HeapTraceRuntime};
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct HeapTraceHttpResponse {
    pub content_type: String,
    pub body: Vec<u8>,
}

pub fn router(runtime: HeapTraceRuntime) -> Router {
    Router::new()
        .route("/debug/heap/profile", post(capture_profile))
        .route("/debug/heap/latest", get(latest_capture_summary))
        .route("/debug/heap/flamegraph.svg", get(capture_flamegraph_svg))
        .route("/debug/heap/flamegraph", get(latest_flamegraph))
        .route("/debug/heap/pprof", get(latest_pprof))
        .with_state(runtime)
}

pub async fn capture_profile_response(
    runtime: &HeapTraceRuntime,
) -> crate::CommonResult<HeapTraceHttpResponse> {
    runtime
        .profile_artifact()
        .await
        .map(|artifact| HeapTraceHttpResponse {
            content_type: artifact.media_type,
            body: artifact.payload,
        })
}

pub fn latest_capture_summary_response(runtime: &HeapTraceRuntime) -> HeapProfileSummary {
    latest_summary().unwrap_or_else(|| HeapProfileSummary {
        runtime_enabled: runtime.conf().runtime_enabled,
        sample_interval_bytes: runtime.conf().sample_interval_bytes,
        capture_count: 0,
        last_capture_epoch_ms: None,
    })
}

pub async fn capture_flamegraph_svg_response(
    runtime: &HeapTraceRuntime,
) -> crate::CommonResult<HeapTraceHttpResponse> {
    runtime.flamegraph_http_response().await
}

pub async fn latest_flamegraph_response(
    runtime: &HeapTraceRuntime,
) -> Option<HeapTraceHttpResponse> {
    runtime
        .latest_flamegraph_artifact()
        .await
        .map(|artifact| HeapTraceHttpResponse {
            content_type: artifact.media_type,
            body: artifact.payload,
        })
}

pub async fn latest_pprof_response(runtime: &HeapTraceRuntime) -> Option<HeapTraceHttpResponse> {
    runtime
        .latest_profile_artifact()
        .await
        .map(|artifact| HeapTraceHttpResponse {
            content_type: artifact.media_type,
            body: artifact.payload,
        })
}

async fn capture_profile(State(runtime): State<HeapTraceRuntime>) -> Response {
    match capture_profile_response(&runtime).await {
        Ok(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        Err(err) => error_response(err),
    }
}

async fn latest_capture_summary(State(runtime): State<HeapTraceRuntime>) -> Response {
    (
        StatusCode::OK,
        Json(latest_capture_summary_response(&runtime)),
    )
        .into_response()
}

async fn capture_flamegraph_svg(State(runtime): State<HeapTraceRuntime>) -> Response {
    match capture_flamegraph_svg_response(&runtime).await {
        Ok(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        Err(err) => error_response(err),
    }
}

async fn latest_flamegraph(State(runtime): State<HeapTraceRuntime>) -> Response {
    match latest_flamegraph_response(&runtime).await {
        Some(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        None => not_found_response("heap flamegraph artifact not found"),
    }
}

async fn latest_pprof(State(runtime): State<HeapTraceRuntime>) -> Response {
    match latest_pprof_response(&runtime).await {
        Some(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        None => not_found_response("heap profile artifact not found"),
    }
}

fn response_with_content_type(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response {
    let mut response = (status, body).into_response();
    if let Ok(value) = HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

fn error_response(err: crate::CommonError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"message": err.to_string()})),
    )
        .into_response()
}

fn not_found_response(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"message": message}))).into_response()
}
