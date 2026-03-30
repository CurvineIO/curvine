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

#![cfg(feature = "heap-trace")]

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use orpc::common::heap_trace::{
    clear_latest_summary, router, CapturedProfile, HeapProfileSummary, HeapProfiler,
    HeapTraceConfig, HeapTraceProfile, HeapTraceRuntime,
};
use orpc::CommonResult;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;
use tower::util::ServiceExt;

#[derive(Debug)]
struct FakeProfiler;

#[async_trait]
impl HeapProfiler for FakeProfiler {
    async fn capture(&self, _conf: &HeapTraceConfig) -> CommonResult<CapturedProfile> {
        Ok(CapturedProfile::new(
            HeapTraceProfile::new(vec![0, 1, 2]),
            "root;worker;alloc 5\nroot;worker;free 3\n",
        ))
    }
}

fn test_runtime(dir: &TempDir) -> HeapTraceRuntime {
    clear_latest_summary();
    HeapTraceRuntime::with_profiler(
        HeapTraceConfig::new(true, 4096),
        dir.path().to_path_buf(),
        Arc::new(FakeProfiler),
    )
}

#[tokio::test]
async fn manual_capture_route_returns_profile_payload() {
    let _guard = heap_trace_test_guard();
    let dir = TempDir::new().unwrap();
    let runtime = test_runtime(&dir);
    let app = router(runtime);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/debug/heap/profile")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "application/octet-stream"
    );
    let body = response_body(response).await;
    assert_eq!(body, vec![0, 1, 2]);
}

#[tokio::test]
async fn latest_summary_route_returns_expected_payload() {
    let _guard = heap_trace_test_guard();
    let dir = TempDir::new().unwrap();
    let runtime = test_runtime(&dir);
    runtime.trigger_now().await.unwrap();
    let app = router(runtime);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug/heap/latest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body(response).await;
    let summary: HeapProfileSummary = serde_json::from_slice(&body).unwrap();
    assert!(summary.runtime_enabled);
    assert_eq!(summary.sample_interval_bytes, 4096);
    assert_eq!(summary.capture_count, 1);
    assert!(summary.last_capture_epoch_ms.is_some());
}

#[tokio::test]
async fn flamegraph_svg_route_returns_svg_content() {
    let _guard = heap_trace_test_guard();
    let dir = TempDir::new().unwrap();
    let runtime = test_runtime(&dir);
    let app = router(runtime);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/debug/heap/flamegraph.svg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "image/svg+xml");
    let body = response_body(response).await;
    let svg = String::from_utf8(body).unwrap();
    assert!(svg.contains("<svg"));
}

#[tokio::test]
async fn latest_flamegraph_and_pprof_routes_return_latest_artifacts() {
    let _guard = heap_trace_test_guard();
    let dir = TempDir::new().unwrap();
    let runtime = test_runtime(&dir);
    runtime.trigger_now().await.unwrap();
    let app = router(runtime);

    let flamegraph = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/debug/heap/flamegraph")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(flamegraph.status(), StatusCode::OK);
    assert_eq!(flamegraph.headers()["content-type"], "image/svg+xml");
    let flamegraph_body = response_body(flamegraph).await;
    assert!(String::from_utf8(flamegraph_body).unwrap().contains("<svg"));

    let pprof = app
        .oneshot(
            Request::builder()
                .uri("/debug/heap/pprof")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pprof.status(), StatusCode::OK);
    assert_eq!(pprof.headers()["content-type"], "application/octet-stream");
    let pprof_body = response_body(pprof).await;
    assert_eq!(pprof_body, vec![0, 1, 2]);
}

async fn response_body(response: Response) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

fn heap_trace_test_guard() -> MutexGuard<'static, ()> {
    static HEAP_TRACE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    HEAP_TRACE_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap()
}
