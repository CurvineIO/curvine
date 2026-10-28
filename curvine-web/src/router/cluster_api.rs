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

//! Cluster management API: `/api/v1/clusters`.
//!
//! These routes are deliberately cluster-*independent*: they answer even when
//! `X-Curvine-Cluster` names a cluster that does not exist, and the payloads
//! never contain imported configuration text (a cluster file may hold S3
//! credentials), only the whitelisted [`ClusterSummary`] projection.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::to_bytes;
use axum::extract::{Path, Request};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::cluster::{
    configuration_errors, configuration_warnings, ClusterRegistry, ClusterSummary, ProbeResult,
    RegistryError,
};
use crate::router::admin_handler::{
    api_error_response, api_success, current_master_info_within, sanitize_error_text, snapshot,
    AdminRouterHandler,
};
use crate::util::addr::configured_master_addrs;
use curvine_config::{validation, ClusterConf};

/// Hard cap on an imported cluster file (it is a TOML config, not an upload).
const MAX_CLUSTER_CONFIG_BYTES: usize = 1024 * 1024;

/// The probe is a UX affordance, not a health check daemon: a slow cluster must
/// not hold the request open for the full `MASTER_INFO_TIMEOUT_SECS`.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Deserialize)]
struct ClusterConfigPayload {
    toml: String,
}

/// Routes for `/api/v1/clusters`.
///
/// Full paths rather than a nested router because the collection root has to
/// answer both `/api/v1/clusters` and `/api/v1/clusters/`.
pub fn router(registry: Arc<ClusterRegistry>) -> Router {
    Router::new()
        .route("/api/v1/clusters", get(list_clusters).post(create_cluster))
        .route("/api/v1/clusters/", get(list_clusters).post(create_cluster))
        .route("/api/v1/clusters/validate", post(validate_cluster))
        .route("/api/v1/clusters/reload", post(reload_clusters))
        .route(
            "/api/v1/clusters/:id",
            put(replace_cluster).delete(delete_cluster),
        )
        .route("/api/v1/clusters/:id/probe", post(probe_cluster))
        .route("/api/v1/clusters/:id/config", get(read_cluster_config))
        .layer(Extension(registry))
}

async fn list_clusters(Extension(registry): Extension<Arc<ClusterRegistry>>) -> Json<Value> {
    api_success(json!(registry.summary()))
}

async fn create_cluster(
    Extension(registry): Extension<Arc<ClusterRegistry>>,
    request: Request,
) -> Response {
    let text = match read_config_text(request).await {
        Ok(text) => text,
        Err(err) => return err.into_response(),
    };

    match registry.register_from_text(&text) {
        Ok(ctx) => {
            let id = ctx.id.clone();
            match summary_of(&registry, &id) {
                Some(summary) => (StatusCode::CREATED, api_success(json!(summary))).into_response(),
                None => api_success(json!({ "id": id })).into_response(),
            }
        }
        Err(err) => registry_error_response(err),
    }
}

async fn replace_cluster(
    Extension(registry): Extension<Arc<ClusterRegistry>>,
    Path(id): Path<String>,
    request: Request,
) -> Response {
    let text = match read_config_text(request).await {
        Ok(text) => text,
        Err(err) => return err.into_response(),
    };

    match registry.replace_from_text(&id, &text) {
        Ok(ctx) => {
            let id = ctx.id.clone();
            match summary_of(&registry, &id) {
                Some(summary) => api_success(json!(summary)).into_response(),
                None => api_success(json!({ "id": id })).into_response(),
            }
        }
        Err(err) => registry_error_response(err),
    }
}

async fn delete_cluster(
    Extension(registry): Extension<Arc<ClusterRegistry>>,
    Path(id): Path<String>,
) -> Response {
    match registry.remove(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => registry_error_response(err),
    }
}

async fn reload_clusters(Extension(registry): Extension<Arc<ClusterRegistry>>) -> Json<Value> {
    api_success(json!(registry.reload()))
}

/// Returns the raw TOML an imported cluster was loaded from, so the console can
/// pre-fill its edit form: `PUT` needs the whole document, and the summary
/// projection deliberately carries no configuration.
///
/// Deliberately not enveloped (`{success, data, error}`) and served as
/// `text/plain`: this is a document download, and the body must stay byte-for-byte
/// what the operator wrote (comments included) rather than a re-serialised
/// [`ClusterConf`]. `no-store` because it may contain credentials and must not be
/// cached by a proxy.
async fn read_cluster_config(
    Extension(registry): Extension<Arc<ClusterRegistry>>,
    Path(id): Path<String>,
) -> Response {
    match registry.config_text(&id) {
        Ok(text) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            text,
        )
            .into_response(),
        // Errors are already shaped for a path parameter: the built-in default has
        // no file (409 `not_removable`), and an unknown, malformed or vanished id
        // is 404 `unknown_cluster`.
        Err(err) => registry_error_response(err),
    }
}

/// Dry run: parse and validate without touching the clusters directory.
async fn validate_cluster(
    Extension(registry): Extension<Arc<ClusterRegistry>>,
    request: Request,
) -> Response {
    let text = match read_config_text(request).await {
        Ok(text) => text,
        Err(err) => return err.into_response(),
    };

    // Unknown keys are warnings, not errors (see `validation`), and the import
    // path already prints them; report them to the UI instead.
    let mut warnings = validation::audit_unknown_keys(&text).unwrap_or_default();

    match ClusterConf::from_text_import(&text) {
        Ok(conf) => {
            // These are the registry's own gates, so the dry run cannot promise
            // `ok: true` for something `create` would then refuse.
            let errors = configuration_errors(&conf);
            warnings.extend(configuration_warnings(&conf, &text));

            // An id that is already registered is *not* an error here: `validate`
            // answers "is this configuration itself usable", not "may it be
            // created right now". Failing it would make `PUT /clusters/:id`
            // impossible, because the UI only enables save once validation
            // passes. Existence conflicts stay at the method level, where they
            // belong: POST answers 409 cluster_exists, PUT replaces.
            if registry.contains(&conf.cluster_id) {
                warnings.push(format!(
                    "cluster '{}' is already registered: POST /clusters will reject it as \
                     cluster_exists, PUT /clusters/{} will replace it",
                    conf.cluster_id, conf.cluster_id
                ));
            }

            // The projected status has to agree with `errors` in the same body,
            // or the UI shows a green dot next to a rejected configuration.
            let status = if errors.is_empty() {
                "ready"
            } else {
                "invalid"
            };

            api_success(json!({
                "ok": errors.is_empty(),
                "cluster": summary_for_conf(&conf, status),
                // Every string in this body is rendered verbatim in the cluster
                // page, and `err_box!` decorates its messages with
                // `(path/to/file.rs:line)`; strip that before it reaches a browser.
                "errors": sanitized(&errors),
                "warnings": warnings,
            }))
            .into_response()
        }
        Err(err) => api_success(json!({
            "ok": false,
            "cluster": Value::Null,
            "errors": [sanitize_error_text(&err.to_string())],
            "warnings": warnings,
        }))
        .into_response(),
    }
}

/// Reuses the Overview master-info call against a *specific* cluster.
async fn probe_cluster(
    Extension(console): Extension<Arc<AdminRouterHandler>>,
    Extension(registry): Extension<Arc<ClusterRegistry>>,
    Path(id): Path<String>,
) -> Response {
    let ctx = match registry.resolve(Some(id.as_str())) {
        Ok(ctx) => ctx,
        Err(err) => {
            return api_error_response(StatusCode::NOT_FOUND, "unknown_cluster", err.to_string());
        }
    };

    let handler = snapshot(&console, &ctx);
    let started = Instant::now();
    let outcome = current_master_info_within(&handler, PROBE_TIMEOUT).await;

    let probe = match outcome {
        Ok(info) => ProbeResult {
            reachable: true,
            active_master: non_empty(info.active_master),
            // `FilesystemInfo` carries no build identity, and inventing one here
            // would require a second RPC.
            version: None,
            latency_ms: started.elapsed().as_millis() as u64,
            message: None,
        },
        Err(failure) => ProbeResult {
            reachable: false,
            active_master: None,
            version: None,
            latency_ms: started.elapsed().as_millis() as u64,
            // Always present when unreachable, and always a sentence: the UI
            // renders it verbatim in a tooltip.
            message: Some(failure.message()),
        },
    };

    if let Ok(mut slot) = ctx.last_probe.lock() {
        *slot = Some(probe.clone());
    }

    api_success(json!(probe)).into_response()
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn summary_of(registry: &ClusterRegistry, id: &str) -> Option<ClusterSummary> {
    registry.summary().into_iter().find(|entry| entry.id == id)
}

/// Projection for a configuration that is valid but not registered (validate).
///
/// `status` is the dry run's own verdict (`ready` | `invalid`); the registry
/// list keeps its separate meaning ("loaded" vs "failed to load").
fn summary_for_conf(conf: &ClusterConf, status: &'static str) -> ClusterSummary {
    let display_name = conf.web.display_name.trim().to_string();
    ClusterSummary {
        id: conf.cluster_id.clone(),
        display_name: if display_name.is_empty() {
            conf.cluster_id.clone()
        } else {
            display_name
        },
        source: "file",
        master_addrs: configured_master_addrs(conf)
            .iter()
            .map(|addr| addr.to_string())
            .collect(),
        // Validated, not probed: reachability is a per-request fact.
        status,
        error: None,
        editable: true,
        removable: true,
    }
}

/// Strips internal decorations from messages that are about to be rendered in
/// the console (see [`sanitize_error_text`]).
fn sanitized(messages: &[String]) -> Vec<String> {
    messages
        .iter()
        .map(|message| sanitize_error_text(message))
        .collect()
}

fn registry_error_response(err: RegistryError) -> Response {
    let status = match &err {
        RegistryError::InvalidConfig(_) | RegistryError::InvalidId(_) => StatusCode::BAD_REQUEST,
        RegistryError::Exists(_) | RegistryError::Limit(_) | RegistryError::NotRemovable(_) => {
            StatusCode::CONFLICT
        }
        RegistryError::Unknown(_) => StatusCode::NOT_FOUND,
        RegistryError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    // The message is shown verbatim by the cluster page: a parser or IO error
    // carries `(path/to/file.rs:line)` until it is sanitised.
    api_error_response(status, err.code(), sanitize_error_text(&err.to_string()))
}

/// Why a request body could not be read as cluster configuration.
///
/// A small enum rather than a `Response`: a `Response` is big, and this value is
/// only a carrier until the caller turns it into the error envelope.
enum ConfigBodyError {
    TooLarge,
    NotUtf8(String),
    BadJson(String),
}

impl ConfigBodyError {
    fn into_response(self) -> Response {
        match self {
            Self::TooLarge => api_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                format!("cluster configuration exceeds {MAX_CLUSTER_CONFIG_BYTES} bytes"),
            ),
            Self::NotUtf8(detail) => api_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                format!("cluster configuration must be UTF-8: {detail}"),
            ),
            Self::BadJson(detail) => api_error_response(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                format!("invalid JSON payload: {detail}"),
            ),
        }
    }
}

/// Accepts either a raw TOML body (`text/plain`, `application/toml`, no
/// content type) or `{"toml": "..."}` (`application/json`).
///
/// The body is read with an explicit bound instead of a `DefaultBodyLimit`
/// layer so the failure is reported in the usual error envelope.
async fn read_config_text(request: Request) -> Result<String, ConfigBodyError> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    let bytes = match to_bytes(request.into_body(), MAX_CLUSTER_CONFIG_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return Err(ConfigBodyError::TooLarge),
    };

    if content_type.contains("application/json") {
        let payload: ClusterConfigPayload = serde_json::from_slice(&bytes)
            .map_err(|err| ConfigBodyError::BadJson(err.to_string()))?;
        return Ok(payload.toml);
    }

    String::from_utf8(bytes.to_vec()).map_err(|err| ConfigBodyError::NotUtf8(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::OnceLock;

    use axum::body::Body;
    use tower::ServiceExt;

    use curvine_runtime::runtime::Runtime;

    /// `tempfile` is not a dev-dependency of this crate, so the tests own the
    /// directory lifecycle themselves.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "curvine-cluster-api-{tag}-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, AtomicOrdering::SeqCst)
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn test_rt() -> Arc<Runtime> {
        static RT: OnceLock<Arc<Runtime>> = OnceLock::new();
        Arc::clone(RT.get_or_init(|| Arc::new(Runtime::single())))
    }

    /// The cluster API is cluster-independent, so it only needs the registry.
    fn app(dir: &TempDir) -> Router {
        let mut conf = ClusterConf {
            cluster_id: "default-cluster".to_string(),
            ..Default::default()
        };
        conf.client.master_addrs = vec![curvine_net::net::InetAddr::new(
            "default-master.example",
            8995,
        )];
        // Keep the tests from spawning collectors that scrape nothing.
        conf.web.observability.enabled = false;
        let registry =
            ClusterRegistry::new(conf, test_rt(), dir.path().to_path_buf(), 4).expect("registry");
        router(Arc::new(registry))
    }

    /// A configuration that parses but can never reach a cluster: the exact
    /// shape the E2E run found `ok: true`.
    const PORT_ZERO: &str = r#"
    cluster_id = "broken"

    [master]
    rpc_port = 0

    [client]
    master_addrs = [{ hostname = "broken.example", port = 8995 }]
"#;

    /// No `[client] master_addrs`, no `[journal] journal_addrs`: the derived
    /// address is the built-in localhost:8995.
    const LOCALHOST_FALLBACK: &str = r#"
    cluster_id = "fallback"

    [master]
    hostname = "fallback.example"
"#;

    const USABLE: &str = r#"
    cluster_id = "cluster-b"

    [master]
    hostname = "cluster-b.example"

    [client]
    master_addrs = [{ hostname = "cluster-b.example", port = 8995 }]

    [web.observability]
    enabled = false
"#;

    async fn post(app: &Router, uri: &str, body: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(body.to_string()))
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        (status, serde_json::from_slice(&bytes).expect("json body"))
    }

    /// Responses are wrapped in the usual `{success, data}` envelope.
    fn data(body: &Value) -> &Value {
        body.get("data").unwrap_or(body)
    }

    #[tokio::test]
    async fn validate_accepts_a_usable_configuration() {
        let dir = TempDir::new("usable");
        let (status, body) = post(&app(&dir), "/api/v1/clusters/validate", USABLE).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(data(&body)["ok"], json!(true), "{body}");
        assert_eq!(data(&body)["errors"], json!([]), "{body}");
        assert_eq!(data(&body)["warnings"], json!([]), "{body}");
        assert_eq!(data(&body)["cluster"]["status"], json!("ready"), "{body}");
    }

    /// The edit flow (PUT) is only reachable if validating an *existing* id
    /// passes: the UI disables save until validation succeeds.
    #[tokio::test]
    async fn validate_passes_for_an_existing_id_and_warns_about_it() {
        let dir = TempDir::new("existing");
        let app = app(&dir);

        let (created, body) = post(&app, "/api/v1/clusters", USABLE).await;
        assert_eq!(created, StatusCode::CREATED, "{body}");

        let (status, body) = post(&app, "/api/v1/clusters/validate", USABLE).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(data(&body)["ok"], json!(true), "{body}");
        assert_eq!(data(&body)["errors"], json!([]), "{body}");
        assert_eq!(data(&body)["cluster"]["status"], json!("ready"), "{body}");
        let warnings = data(&body)["warnings"].to_string();
        assert!(warnings.contains("already registered"), "{warnings}");
        assert!(warnings.contains("PUT /clusters/cluster-b"), "{warnings}");
    }

    #[tokio::test]
    async fn validate_rejects_a_zero_rpc_port() {
        let dir = TempDir::new("port-zero");
        let (status, body) = post(&app(&dir), "/api/v1/clusters/validate", PORT_ZERO).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(data(&body)["ok"], json!(false), "{body}");
        let errors = data(&body)["errors"].to_string();
        assert!(errors.contains("master.rpc_port is 0"), "{errors}");
        // The projected status must not contradict the errors in the same body.
        assert_eq!(data(&body)["cluster"]["status"], json!("invalid"), "{body}");
    }

    #[tokio::test]
    async fn validate_rejects_an_out_of_range_port() {
        let dir = TempDir::new("port-range");
        let text = r#"
    cluster_id = "cluster-b"

    [client]
    master_addrs = [{ hostname = "cluster-b.example", port = 70000 }]
"#;
        let (status, body) = post(&app(&dir), "/api/v1/clusters/validate", text).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(data(&body)["ok"], json!(false), "{body}");
        assert_ne!(data(&body)["errors"], json!([]), "{body}");
    }

    #[tokio::test]
    async fn validate_warns_about_the_localhost_fallback_but_stays_ok() {
        let dir = TempDir::new("fallback");
        let (status, body) =
            post(&app(&dir), "/api/v1/clusters/validate", LOCALHOST_FALLBACK).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(data(&body)["ok"], json!(true), "{body}");
        let warnings = data(&body)["warnings"].to_string();
        assert!(warnings.contains("localhost:8995"), "{warnings}");
    }

    #[tokio::test]
    async fn create_still_saves_a_configuration_that_only_warns() {
        let dir = TempDir::new("fallback-create");
        let app = app(&dir);
        let (status, body) = post(&app, "/api/v1/clusters", LOCALHOST_FALLBACK).await;

        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(data(&body)["id"], json!("fallback"), "{body}");
        assert!(dir.path().join("fallback.toml").exists());
    }

    #[tokio::test]
    async fn create_refuses_what_validate_rejects() {
        let dir = TempDir::new("port-zero-create");
        let (status, body) = post(&app(&dir), "/api/v1/clusters", PORT_ZERO).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            !dir.path().join("broken.toml").exists(),
            "a rejected import must not be written"
        );
    }

    /// Same text, only the id changes: the dry run has to reach the same verdict
    /// as `create`, which refuses these ids (E2E found `ok: true` + 201 for `BETA`).
    #[tokio::test]
    async fn validate_rejects_an_unusable_cluster_id() {
        let long_id = "a".repeat(100);
        let cases = [
            ("BETA", "uppercase"),
            (long_id.as_str(), "too long"),
            ("a/b", "separator"),
            ("-beta", "leading dash"),
        ];

        let dir = TempDir::new("bad-ids");
        let app = app(&dir);

        for (id, why) in cases {
            let text = USABLE.replace(
                "cluster_id = \"cluster-b\"",
                &format!("cluster_id = \"{id}\""),
            );
            let (status, body) = post(&app, "/api/v1/clusters/validate", &text).await;

            assert_eq!(status, StatusCode::OK, "{why}: {body}");
            assert_eq!(data(&body)["ok"], json!(false), "{why}: {body}");
            assert_eq!(
                data(&body)["cluster"]["status"],
                json!("invalid"),
                "{why}: {body}"
            );
            let errors = data(&body)["errors"].to_string();
            assert!(errors.contains("invalid cluster id"), "{why}: {errors}");
            assert!(
                errors.contains("[a-z0-9_-]{1,63}"),
                "{why}: the rule must be visible: {errors}"
            );
        }
    }

    #[tokio::test]
    async fn create_refuses_an_uppercase_id_without_writing_a_file() {
        let dir = TempDir::new("uppercase-create");
        let text = USABLE.replace("cluster_id = \"cluster-b\"", "cluster_id = \"BETA\"");
        let (status, body) = post(&app(&dir), "/api/v1/clusters", &text).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        // A stray `BETA.toml` collides with `beta.toml` on a case-insensitive
        // filesystem (macOS, where the console runs).
        assert!(!dir.path().join("BETA.toml").exists());
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        assert!(entries.is_empty(), "nothing may be written: {entries:?}");
    }

    /// Everything the cluster page renders must be free of `err_box!` decorations.
    #[tokio::test]
    async fn validate_errors_carry_no_source_locations() {
        let dir = TempDir::new("sanitize-validate");
        let app = app(&dir);

        // Unparseable: `toml` reports where, and `err_box!` adds a source path.
        let (status, body) = post(&app, "/api/v1/clusters/validate", "cluster_id = alpha").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(data(&body)["ok"], json!(false), "{body}");
        let text = data(&body)["errors"].to_string();
        assert!(text.contains("TOML parse error"), "{text}");
        assert!(!text.contains(".rs:"), "source location leaked: {text}");
        // The rendered snippet (`1 | cluster_id = alpha`, `|     ^`) is noise in a
        // UI list; only the headline and the hint line are kept.
        assert!(!text.contains("1 |"), "diagnostic snippet leaked: {text}");
        assert!(text.contains("expected"), "{text}");

        // And the same for a text without `cluster_id` at all.
        let (_, body) = post(
            &app,
            "/api/v1/clusters/validate",
            "[master]\nrpc_port = 0\n",
        )
        .await;
        let text = data(&body)["errors"].to_string();
        assert!(text.contains("cluster_id"), "{text}");
        assert!(!text.contains(".rs:"), "source location leaked: {text}");
    }

    async fn get(app: &Router, uri: &str) -> (StatusCode, axum::http::HeaderMap, String) {
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        (
            status,
            headers,
            String::from_utf8(bytes.to_vec()).expect("utf8 body"),
        )
    }

    #[tokio::test]
    async fn cluster_config_returns_the_raw_toml_for_prefilling_the_editor() {
        let dir = TempDir::new("config-get");
        let app = app(&dir);
        // The comment is the point: the body must be the file, not a re-serialised
        // `ClusterConf`, or the edit round-trip would drop operator annotations.
        let text = format!("# imported by ops\n{USABLE}");
        let (status, _) = post(&app, "/api/v1/clusters", &text).await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, headers, body) = get(&app, "/api/v1/clusters/cluster-b/config").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            headers
                .get(header::CONTENT_TYPE)
                .map(|value| value.to_str().unwrap()),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .map(|value| value.to_str().unwrap()),
            Some("no-store"),
            "the document may carry credentials and must not be cached"
        );
        assert_eq!(body, text);
        assert!(body.starts_with("# imported by ops"), "{body}");
    }

    #[tokio::test]
    async fn cluster_config_refuses_the_default_cluster_and_unknown_ids() {
        let dir = TempDir::new("config-get-errors");
        let app = app(&dir);

        // 409, not 404: the default cluster exists but has no file to edit.
        let (status, _, body) = get(&app, "/api/v1/clusters/default-cluster/config").await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert!(body.contains("not_removable"), "{body}");

        for uri in [
            "/api/v1/clusters/cluster-b/config",
            "/api/v1/clusters/BETA/config",
        ] {
            let (status, _, body) = get(&app, uri).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body}");
            assert!(body.contains("unknown_cluster"), "{uri}: {body}");
        }
    }
}
