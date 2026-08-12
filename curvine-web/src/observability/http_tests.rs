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

//! HTTP-level integration tests for observability routes (axum router + JSON envelope).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::Router;
use curvine_config::ClusterConf;
use curvine_runtime::runtime::Runtime;
use curvine_unified_fs::UnifiedFileSystem;
use serde_json::Value;
use tower::ServiceExt;

use super::api::router;
use super::model::{MetricSample, ACTIVE_MASTER_INSTANCE};
use super::ObservabilityService;

fn test_service() -> Arc<ObservabilityService> {
    let conf = ClusterConf::default();
    let unified_fs =
        UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
    Arc::new(ObservabilityService::new(conf, unified_fs))
}

fn test_app(service: Arc<ObservabilityService>) -> Router {
    router(service)
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("json body")
}

fn ingest_worker_read_bytes(service: &ObservabilityService, ts: i64, value: f64) {
    let mut labels = BTreeMap::new();
    labels.insert("component".to_string(), "worker".to_string());
    labels.insert("instance".to_string(), "w1:9001".to_string());
    labels.insert("worker_id".to_string(), "1".to_string());
    labels.insert("worker_state".to_string(), "live".to_string());
    service.builtin().tsdb().insert(
        ts,
        vec![MetricSample {
            name: "read_bytes".to_string(),
            labels,
            value,
        }],
    );
}

fn ingest_master_capacity(service: &ObservabilityService, ts: i64, value: f64) {
    let mut labels = BTreeMap::new();
    labels.insert("component".to_string(), "master".to_string());
    labels.insert("instance".to_string(), ACTIVE_MASTER_INSTANCE.to_string());
    service.builtin().tsdb().insert(
        ts,
        vec![MetricSample {
            name: "capacity".to_string(),
            labels,
            value,
        }],
    );
}

async fn require_test_auth(request: Request<Body>, next: Next) -> Response {
    if request
        .headers()
        .get("x-test-auth")
        .is_some_and(|value| value == "ok")
    {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        axum::Json(serde_json::json!({
            "success": false,
            "data": null,
            "error": { "code": "UNAUTHORIZED", "message": "login required" }
        })),
    )
        .into_response()
}

fn authed_app(service: Arc<ObservabilityService>) -> Router {
    Router::new()
        .nest("/api/v1/observability", router(service))
        .route_layer(middleware::from_fn(require_test_auth))
}

#[tokio::test]
async fn health_returns_success_envelope() {
    let app = test_app(test_service());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["success"], true);
    assert_eq!(json["data"]["active_provider"], "builtin");
    assert!(json["data"]["enabled"].as_bool().unwrap_or(false));
}

#[tokio::test]
async fn query_returns_series_from_builtin_tsdb() {
    let service = test_service();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    ingest_worker_read_bytes(&service, now - 30_000, 100.0);
    ingest_worker_read_bytes(&service, now, 200.0);

    let app = test_app(service);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/query?metric=read_bytes&component=worker&range=15m")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["success"], true);
    let series = json["data"]["series"].as_array().expect("series array");
    assert!(!series.is_empty());
    assert_eq!(series[0]["metric"], "read_bytes");
    assert!(series[0]["points"]
        .as_array()
        .map(|points| !points.is_empty())
        .unwrap_or(false));
}

#[tokio::test]
async fn query_rejects_invalid_metric_name() {
    let app = test_app(test_service());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/query?metric=rate(read_bytes[5m])&range=15m")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["success"], false);
    assert_eq!(json["error"]["code"], "INVALID_QUERY");
}

#[tokio::test]
async fn dashboard_returns_groups_and_panels() {
    let service = test_service();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    ingest_worker_read_bytes(&service, now - 30_000, 0.0);
    ingest_worker_read_bytes(&service, now, 15_000.0);
    ingest_master_capacity(&service, now, 1_000_000.0);
    service.builtin().set_last_collect_ms(now);

    let app = test_app(service);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/dashboard?range=15m&step=15s")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["success"], true);
    let groups = json["data"]["groups"].as_array().expect("groups");
    assert!(groups.iter().any(|group| group["id"] == "master"));
    assert!(groups.iter().any(|group| group["id"] == "workers"));
    let master_panels = groups
        .iter()
        .find(|group| group["id"] == "master")
        .and_then(|group| group["panels"].as_array())
        .expect("master panels");
    assert!(master_panels
        .iter()
        .any(|panel| panel["id"] == "cluster-capacity"));
}

#[tokio::test]
async fn nested_route_requires_auth_when_wrapped() {
    let app = authed_app(test_service());
    let unauth = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/observability/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

    let authed = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/observability/health")
                .header("x-test-auth", "ok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authed.status(), StatusCode::OK);
    let json = response_json(authed).await;
    assert_eq!(json["success"], true);
}

#[tokio::test]
async fn mode_post_rejects_invalid_mode() {
    let app = test_app(test_service());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mode")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"mode":"invalid"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["success"], false);
    assert_eq!(json["error"]["code"], "INVALID_MODE");
}

#[tokio::test]
async fn operations_returns_diagnostics_and_alerts_together() {
    let app = test_app(test_service());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/operations")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_json(response).await;
    assert_eq!(json["success"], true);
    let checks = json["data"]["diagnostics"]["checks"]
        .as_array()
        .expect("checks");
    assert!(checks.iter().any(|item| item["id"] == "master-targets"));
    assert!(checks
        .iter()
        .any(|item| item["id"] == "prometheus-coverage"));
    let alerts = json["data"]["alerts"].as_array().expect("alerts");
    assert!(alerts.iter().all(|item| item["suggested_fix"].is_string()));
    assert_eq!(
        json["data"]["generated_ms"],
        json["data"]["diagnostics"]["generated_ms"]
    );
}
