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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Query;
use axum::routing::{get, put};
use axum::{Extension, Json, Router};
use log::error;
use serde_json::{json, Value};

use super::dashboard::{build_dashboard, default_step_for_range};
use super::model::{
    parse_duration_ms, parse_observability_mode, DashboardQuery, InstantQuery,
    ObservabilityModeUpdate, ObservabilitySettingsUpdate, RangeQuery,
};
use super::operations::build_operations;
use super::prometheus_provider::validate_prometheus_metric_name;
use super::service::ObservabilityService;

/// Observability API for one cluster.
///
/// The service is *not* baked in: `curvine-web` may serve several clusters from
/// one process, so the per-request [`crate::router::admin_handler`] middleware
/// inserts the selected cluster's `Arc<ObservabilityService>` into the request
/// extensions and every handler here reads that extension.
pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/settings", get(settings).put(update_settings))
        .route("/mode", put(update_mode).post(update_mode))
        .route("/targets", get(targets))
        .route("/names", get(names))
        .route("/instant", get(instant))
        .route("/query", get(query))
        .route("/dashboard", get(dashboard))
        .route("/operations", get(operations))
}

async fn operations(Extension(service): Extension<Arc<ObservabilityService>>) -> Json<Value> {
    api_success(json!(build_operations(&service).await))
}

async fn health(Extension(service): Extension<Arc<ObservabilityService>>) -> Json<Value> {
    api_success(json!(service.health()))
}

async fn settings(Extension(service): Extension<Arc<ObservabilityService>>) -> Json<Value> {
    api_success(json!(service.settings()))
}

async fn update_settings(
    Extension(service): Extension<Arc<ObservabilityService>>,
    Json(body): Json<ObservabilitySettingsUpdate>,
) -> Json<Value> {
    match service.update_settings(body.scrape_interval_ms, body.retention_ms) {
        Ok(()) => api_success(json!(service.settings())),
        Err(err) => api_error("INVALID_SETTINGS", err),
    }
}

async fn update_mode(
    Extension(service): Extension<Arc<ObservabilityService>>,
    Json(body): Json<ObservabilityModeUpdate>,
) -> Json<Value> {
    let mode = match parse_observability_mode(&body.mode) {
        Ok(value) => value,
        Err(err) => return api_error("INVALID_MODE", err),
    };
    match service.update_mode(mode).await {
        Ok(health) => api_success(json!(health)),
        Err(err) => api_error("MODE_UPDATE_FAILED", err),
    }
}

async fn targets(Extension(service): Extension<Arc<ObservabilityService>>) -> Json<Value> {
    api_success(json!({ "targets": service.targets().await }))
}

async fn names(Extension(service): Extension<Arc<ObservabilityService>>) -> Json<Value> {
    match service.names().await {
        Ok(names) => api_success(json!({ "names": names })),
        Err(err) => api_error("PROMETHEUS_QUERY_FAILED", err),
    }
}

async fn instant(
    Extension(service): Extension<Arc<ObservabilityService>>,
    Query(query): Query<InstantQuery>,
) -> Json<Value> {
    if let Some(err) = invalid_metric_error(query.metric.as_deref()) {
        return err;
    }
    match service
        .instant(
            query.metric.as_deref(),
            query.component.as_deref(),
            query.instance.as_deref(),
        )
        .await
    {
        Ok(series) => api_success(json!({ "series": series })),
        Err(err) => api_error("PROMETHEUS_QUERY_FAILED", err),
    }
}

async fn query(
    Extension(service): Extension<Arc<ObservabilityService>>,
    Query(query): Query<RangeQuery>,
) -> Json<Value> {
    let range_ms = match parse_duration_ms(&query.range) {
        Ok(value) => value,
        Err(err) => return api_error("INVALID_QUERY", err),
    };
    let step_ms = match query
        .step
        .as_ref()
        .map(|value| parse_duration_ms(value))
        .transpose()
    {
        Ok(value) => value,
        Err(err) => return api_error("INVALID_QUERY", err),
    };
    if let Some(err) = invalid_metric_error(query.metric.as_deref()) {
        return err;
    }
    let end_ms = now_ms();
    let start_ms = end_ms.saturating_sub(range_ms);
    match service
        .range(
            query.metric.as_deref(),
            query.component.as_deref(),
            query.instance.as_deref(),
            start_ms,
            end_ms,
            step_ms,
        )
        .await
    {
        Ok(series) => api_success(json!({ "series": series })),
        Err(err) => api_error("PROMETHEUS_QUERY_FAILED", err),
    }
}

fn dashboard_range_or_default(range: &str) -> &str {
    let trimmed = range.trim();
    if trimmed.is_empty() {
        "15m"
    } else {
        trimmed
    }
}

fn invalid_metric_error(metric: Option<&str>) -> Option<Json<Value>> {
    validate_prometheus_metric_name(metric)
        .err()
        .map(|err| api_error("INVALID_QUERY", err))
}

async fn dashboard(
    Extension(service): Extension<Arc<ObservabilityService>>,
    Query(query): Query<DashboardQuery>,
) -> Json<Value> {
    let range = dashboard_range_or_default(&query.range);
    let range_ms = match parse_duration_ms(range) {
        Ok(value) => value,
        Err(err) => return api_error("INVALID_QUERY", err),
    };
    let step = query
        .step
        .as_deref()
        .unwrap_or_else(|| default_step_for_range(range));
    if let Err(err) = parse_duration_ms(step) {
        return api_error("INVALID_QUERY", err);
    }
    let end_ms = service
        .health()
        .last_collect_ms
        .filter(|value| *value > 0)
        .unwrap_or_else(now_ms);
    let payload = build_dashboard(&service, range, step, end_ms, range_ms).await;
    match serde_json::to_value(payload) {
        Ok(value) => api_success(value),
        Err(err) => {
            error!("failed to serialize observability dashboard: {err}");
            api_error("DASHBOARD_SERIALIZE_FAILED", err.to_string())
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn api_success(data: Value) -> Json<Value> {
    Json(json!({
        "success": true,
        "data": data,
        "error": null
    }))
}

fn api_error(code: &str, message: impl Into<String>) -> Json<Value> {
    Json(json!({
        "success": false,
        "data": null,
        "error": { "code": code, "message": message.into() }
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::observability::model::{MetricSample, RangePoint};
    use crate::observability::tsdb::InMemoryTsdb;

    #[test]
    fn dashboard_range_defaults_to_15m_when_empty() {
        assert_eq!(dashboard_range_or_default(""), "15m");
        assert_eq!(dashboard_range_or_default("   "), "15m");
        assert_eq!(dashboard_range_or_default("1h"), "1h");
    }

    #[test]
    fn parse_duration_supports_common_units() {
        assert_eq!(parse_duration_ms("15s").unwrap(), 15_000);
        assert_eq!(parse_duration_ms("1m").unwrap(), 60_000);
        assert_eq!(parse_duration_ms("15m").unwrap(), 900_000);
        assert_eq!(parse_duration_ms("1h").unwrap(), 3_600_000);
        assert_eq!(parse_duration_ms("3d").unwrap(), 259_200_000);
    }

    #[test]
    fn range_query_returns_points_from_tsdb() {
        let tsdb = InMemoryTsdb::new(3_600_000, 720, 100_000, 2_000_000);
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "worker".to_string());
        labels.insert("instance".to_string(), "w1:9001".to_string());
        labels.insert("worker_id".to_string(), "1".to_string());
        labels.insert("worker_state".to_string(), "live".to_string());
        let now = now_ms();
        tsdb.insert(
            now - 1_000,
            vec![MetricSample {
                name: "read_bytes".to_string(),
                labels: labels.clone(),
                value: 42.0,
            }],
        );
        tsdb.insert(
            now,
            vec![MetricSample {
                name: "read_bytes".to_string(),
                labels,
                value: 84.0,
            }],
        );
        let series = tsdb.range(
            Some("read_bytes"),
            Some("worker"),
            None,
            now - 60_000,
            now,
            Some(15_000),
        );
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points.len(), 2);
        assert_eq!(series[0].points.last(), Some(&RangePoint(now, 84.0)));
    }

    #[test]
    fn observability_query_path_is_protected_api_route() {
        let path = "/api/v1/observability/query";
        assert!(path.starts_with("/api/"));
        assert!(!path.starts_with("/api/v1/auth"));
    }

    #[test]
    fn api_error_matches_frontend_unwrap_envelope() {
        let payload = api_error("INVALID_QUERY", "unsupported duration unit in: 1d");
        let value = payload.0;
        assert_eq!(value["success"], false);
        assert_eq!(value["error"]["code"], "INVALID_QUERY");
        assert_eq!(
            value["error"]["message"],
            "unsupported duration unit in: 1d"
        );
    }

    #[test]
    fn invalid_metric_name_returns_invalid_query() {
        let payload = invalid_metric_error(Some("rate(read_bytes[5m])")).expect("invalid metric");
        let value = payload.0;
        assert_eq!(value["success"], false);
        assert_eq!(value["error"]["code"], "INVALID_QUERY");
    }

    #[test]
    fn dashboard_path_is_protected_api_route() {
        let path = "/api/v1/observability/dashboard";
        assert!(path.starts_with("/api/"));
        assert!(!path.starts_with("/api/v1/auth"));
    }

    #[test]
    fn mode_path_is_protected_api_route() {
        let path = "/api/v1/observability/mode";
        assert!(path.starts_with("/api/"));
        assert!(!path.starts_with("/api/v1/auth"));
    }

    #[test]
    fn invalid_dashboard_range_returns_invalid_query() {
        let payload = api_error("INVALID_QUERY", "unsupported duration unit in: 1d");
        let value = payload.0;
        assert_eq!(value["error"]["code"], "INVALID_QUERY");
    }
}
