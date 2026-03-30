use axum::extract::Extension;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
#[cfg(feature = "heap-trace")]
use orpc::common::heap_trace::{
    capture_flamegraph_svg_response, capture_profile_response, latest_capture_summary_response,
    latest_flamegraph_response, latest_pprof_response, HeapTraceRuntime,
};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct WebServer;

impl WebServer {
    pub async fn start(
        port: u16,
        #[cfg(feature = "heap-trace")] heap_trace: Option<Arc<HeapTraceRuntime>>,
    ) -> orpc::CommonResult<()> {
        let mut app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(|| async { "ok" }));

        #[cfg(feature = "heap-trace")]
        if let Some(runtime) = heap_trace {
            app = app
                .route("/debug/heap/profile", axum::routing::post(capture_profile))
                .route("/debug/heap/latest", get(latest_capture_summary))
                .route("/debug/heap/flamegraph.svg", get(capture_flamegraph_svg))
                .route("/debug/heap/flamegraph", get(latest_flamegraph))
                .route("/debug/heap/pprof", get(latest_pprof))
                .layer(Extension((*runtime).clone()));
        }

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        tracing::info!("S3 gateway metrics server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

#[cfg(feature = "heap-trace")]
async fn capture_profile(Extension(runtime): Extension<HeapTraceRuntime>) -> Response {
    match capture_profile_response(&runtime).await {
        Ok(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        Err(err) => error_response(err),
    }
}

#[cfg(feature = "heap-trace")]
async fn latest_capture_summary(Extension(runtime): Extension<HeapTraceRuntime>) -> Response {
    (
        StatusCode::OK,
        Json(latest_capture_summary_response(&runtime)),
    )
        .into_response()
}

#[cfg(feature = "heap-trace")]
async fn capture_flamegraph_svg(Extension(runtime): Extension<HeapTraceRuntime>) -> Response {
    match capture_flamegraph_svg_response(&runtime).await {
        Ok(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        Err(err) => error_response(err),
    }
}

#[cfg(feature = "heap-trace")]
async fn latest_flamegraph(Extension(runtime): Extension<HeapTraceRuntime>) -> Response {
    match latest_flamegraph_response(&runtime).await {
        Some(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        None => not_found_response("heap flamegraph artifact not found"),
    }
}

#[cfg(feature = "heap-trace")]
async fn latest_pprof(Extension(runtime): Extension<HeapTraceRuntime>) -> Response {
    match latest_pprof_response(&runtime).await {
        Some(response) => {
            response_with_content_type(StatusCode::OK, &response.content_type, response.body)
        }
        None => not_found_response("heap profile artifact not found"),
    }
}

#[cfg(feature = "heap-trace")]
fn response_with_content_type(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response {
    let mut response = (status, body).into_response();
    if let Ok(value) = HeaderValue::from_str(content_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

#[cfg(feature = "heap-trace")]
fn error_response(err: orpc::CommonError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"message": err.to_string()})),
    )
        .into_response()
}

#[cfg(feature = "heap-trace")]
fn not_found_response(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"message": message}))).into_response()
}

async fn metrics_handler() -> String {
    orpc::common::Metrics::text_output().unwrap_or_else(|e| format!("Error: {}", e))
}
