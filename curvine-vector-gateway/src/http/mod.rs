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

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use curvine_common::state::{VectorExternalErrorCode, VectorJsonErrEnvelope};

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { "ok" }))
}

pub fn vector_action_router() -> Router {
    const STUB_ACTIONS: &[&str] = &[
        "/CreateVectorBucket",
        "/DeleteVectorBucket",
        "/GetVectorBucket",
        "/ListVectorBuckets",
        "/CreateIndex",
        "/DeleteIndex",
        "/GetIndex",
        "/ListIndexes",
        "/PutVectors",
        "/DeleteVectors",
        "/GetVectors",
        "/QueryVectors",
    ];
    let mut router = Router::new();
    for path in STUB_ACTIONS {
        router = router.route(path, post(vector_action_stub));
    }
    router
}

async fn vector_action_stub() -> impl IntoResponse {
    let body = VectorJsonErrEnvelope {
        request_id: uuid_placeholder(),
        code: VectorExternalErrorCode::NotImplemented,
        message: "vector control APIs are not wired in phase one".to_string(),
    };
    (StatusCode::NOT_IMPLEMENTED, Json(body))
}

fn uuid_placeholder() -> String {
    "00000000-0000-0000-0000-000000000000".to_string()
}
