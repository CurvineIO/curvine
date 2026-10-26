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

//! Optional CPU flame-graph route shared by Master and Worker.
//!
//! Transfer is not wired up yet: it would need `ProfilingConf` threaded
//! into `TransferRouterHandler`, and there is no request for it. Adding it
//! is a two-line change once that need is real.
//!
//! Registered only when both gates are open: `profiling.cpu_enabled = true`
//! and a binary built with `curvine-profiling/profiling`. Otherwise
//! [`mount`] returns the router untouched, so the route is absent rather
//! than present-and-forbidden.
//!
//! The sampler blocks an OS thread for the whole window, so the work runs on
//! the blocking pool: the sampling window must not occupy an async worker,
//! and an in-flight sample must not stall unrelated requests.
//!
//! Aborted clients: if the client disconnects mid-window, the handler future
//! is dropped and `in_flight` is released, but the `spawn_blocking` task keeps
//! sampling until its window ends and `curvine_profiling` still holds its own
//! session flag. A request arriving in that gap gets 409 with the sampler's
//! wording. This is bounded (at most the requested `seconds`, capped by
//! `max_duration_secs`) and self-healing — verified by aborting a 5s sample
//! and observing 409 immediately, then success once the window ended. Making
//! it cancellable would need the sampler to check a stop flag between
//! samples; not implemented because the window is bounded and the endpoint is
//! operator-triggered.

use axum::extract::Query;
use axum::http::{header, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use curvine_config::ProfilingConf;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Path of the flame-graph endpoint.
pub const CPU_PROFILE_PATH: &str = "/api/v1/profiling/cpu";
/// Path reporting what this build and config can actually serve.
///
/// The console reads it so its duration picker never offers a window the
/// server would silently clamp: a clamped request would still be described to
/// the operator as the duration they picked.
pub const CPU_PROFILE_STATUS_PATH: &str = "/api/v1/profiling/cpu/status";
/// Path of the heap (memory) flame graph.
///
/// Rendered from jemalloc's own allocation samples, so its widths are bytes
/// still held rather than CPU time. Unlike the CPU graph there is no
/// observation window: the samples already exist.
pub const MEMORY_PROFILE_PATH: &str = "/api/v1/profiling/memory";

/// Query parameters for a sampling request.
#[derive(Debug, Deserialize, Default)]
pub struct SampleQuery {
    /// Sampling window in seconds; clamped to `profiling.max_duration_secs`.
    #[serde(default)]
    pub seconds: Option<u64>,
    /// Sampling frequency in Hz; clamped to `profiling.cpu_frequency`.
    #[serde(default)]
    pub frequency: Option<u32>,
    /// Console theme (`light` | `dark`); the SVG cannot read the console's
    /// CSS, so the caller states its theme and the colours are baked to match.
    #[serde(default)]
    pub theme: Option<String>,
}

/// Mounts the flame-graph route when profiling is active.
pub fn mount(router: Router, conf: &ProfilingConf) -> Router {
    if !conf.cpu_enabled || !curvine_profiling::available() {
        return router;
    }
    let state = Arc::new(ProfileRouteState {
        conf: conf.clone(),
        in_flight: AtomicBool::new(false),
    });
    let status_state = state.clone();
    let memory_state = state.clone();
    router
        .route(
            CPU_PROFILE_PATH,
            get(move |query: Query<SampleQuery>| {
                let state = state.clone();
                async move { cpu_flamegraph(state, query.0).await }
            }),
        )
        .route(
            MEMORY_PROFILE_PATH,
            get(move |query: Query<SampleQuery>| {
                let state = memory_state.clone();
                async move { memory_flamegraph(state, query.0).await }
            }),
        )
        .route(
            CPU_PROFILE_STATUS_PATH,
            get(move || {
                let conf = status_state.conf.clone();
                async move {
                    axum::Json(serde_json::json!({
                        "enabled": true,
                        "available": curvine_profiling::available(),
                        "max_duration_secs": conf.max_duration_secs,
                        "cpu_frequency": conf.cpu_frequency,
                        "memory_enabled": conf.memory_enabled,
                        "memory_available": curvine_profiling::memory_available(),
                    }))
                }
            }),
        )
        // These endpoints are served by the *sampled* process, so the console
        // necessarily calls them cross-origin (e.g. console :19000 -> worker
        // :19002). The iframe loads without CORS, but the capability probe is
        // a `fetch`, so without this header the console cannot learn whether
        // the heap graph is available and leaves the Memory option disabled.
        //
        // `Any` origin rather than a fixed list: the console's address is not
        // known to the worker at startup, and these routes expose only a
        // capability record and a flame graph. They are gated by the same
        // record/replay-free access rules as `/metrics` on the same port.
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::any())
                .allow_methods([Method::GET]),
        )
}

/// Per-router state.
struct ProfileRouteState {
    conf: ProfilingConf,
    /// Claimed before dispatching to the blocking pool.
    ///
    /// The sampler rejects a concurrent session too, but only once its work
    /// item runs. Dispatching first would let a burst queue up on the
    /// blocking pool and then each queued request would start a *fresh*
    /// full-length window after the previous one finished. Claiming here
    /// makes the second request fail immediately instead.
    in_flight: AtomicBool,
}

async fn memory_flamegraph(state: Arc<ProfileRouteState>, query: SampleQuery) -> Response {
    let theme = curvine_profiling::ProfileTheme::parse(query.theme.as_deref());
    let conf = state.conf.clone();
    // No window to wait for: jemalloc has been sampling since startup, so this
    // only walks what already exists.
    let run =
        tokio::task::spawn_blocking(move || curvine_profiling::sample_memory(&conf, theme)).await;

    match run {
        Ok(Ok(output)) => {
            log::info!("served heap flame graph: bytes={}", output.svg.len());
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "image/svg+xml"),
                    (header::CACHE_CONTROL, "no-store"),
                ],
                output.svg,
            )
                .into_response()
        }
        Ok(Err(err)) => {
            let status = match err {
                curvine_profiling::MemoryProfileError::Busy => StatusCode::CONFLICT,
                // Compiled in but this process is not sampling: the operator
                // needs to know to rebuild/restart, not to retry.
                curvine_profiling::MemoryProfileError::NotActivated => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                // Normal early-life state, not a fault: with a 512 KiB sampling
                // interval a freshly started process has recorded nothing yet.
                // Retryable once traffic arrives.
                curvine_profiling::MemoryProfileError::NoSamples => StatusCode::SERVICE_UNAVAILABLE,
                curvine_profiling::MemoryProfileError::NotEnabled
                | curvine_profiling::MemoryProfileError::FeatureDisabled => StatusCode::NOT_FOUND,
                curvine_profiling::MemoryProfileError::Sampler(_) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            log::warn!("heap flame graph request failed: {err}");
            (status, err.to_string()).into_response()
        }
        Err(join_err) => {
            log::error!("heap flame graph task panicked: {join_err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "heap profiling task failed".to_string(),
            )
                .into_response()
        }
    }
}

async fn cpu_flamegraph(state: Arc<ProfileRouteState>, query: SampleQuery) -> Response {
    // Claim the slot before touching the blocking pool: a queued request must
    // fail fast rather than sample a window it never asked for.
    if state
        .in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        // Distinct wording from `curvine_profiling`'s Busy: both mean
        // "a session is running", but only this one proves the HTTP layer
        // claimed the slot, which matters when diagnosing a stuck slot.
        log::warn!("cpu flame graph rejected: route slot already claimed");
        return (
            StatusCode::CONFLICT,
            "a cpu profiling request is already in flight".to_string(),
        )
            .into_response();
    }
    // Released on every exit path below, including a panicking task.
    let release = InFlightGuard(&state.in_flight);

    let seconds = state.conf.clamp_duration(query.seconds);
    let theme = curvine_profiling::ProfileTheme::parse(query.theme.as_deref());
    let conf = state.conf.clone();
    // The request asked for `query.seconds`; `seconds` is what will actually
    // be sampled. Echo it in the logs so a clamped request is traceable.
    let run = tokio::task::spawn_blocking(move || {
        curvine_profiling::sample(&conf, Some(seconds), query.frequency, theme)
    })
    .await;
    drop(release);

    match run {
        Ok(Ok(output)) => {
            log::info!(
                "served cpu flame graph: seconds={} frequency={} bytes={}",
                output.seconds,
                output.frequency,
                output.svg.len()
            );
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "image/svg+xml"),
                    // A profile is a point-in-time artefact; never cache it.
                    (header::CACHE_CONTROL, "no-store"),
                ],
                output.svg,
            )
                .into_response()
        }
        Ok(Err(err)) => {
            let status = match err {
                // Another session owns the sampler: the caller may retry once
                // it finishes, so this is a conflict, not a server fault.
                curvine_profiling::ProfilingError::Busy => StatusCode::CONFLICT,
                // Reaching here with the route mounted means the gates were
                // closed after startup; report "not available" rather than a
                // server error.
                curvine_profiling::ProfilingError::NotEnabled
                | curvine_profiling::ProfilingError::FeatureDisabled => StatusCode::NOT_FOUND,
                // Nothing ran on CPU during the window. The request was valid
                // and the sampler healthy, so this is retryable.
                curvine_profiling::ProfilingError::NoSamples { .. } => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                curvine_profiling::ProfilingError::Sampler(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            log::warn!("cpu flame graph request failed: {err}");
            (status, err.to_string()).into_response()
        }
        Err(join_err) => {
            log::error!("cpu flame graph task panicked: {join_err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "cpu profiling task failed".to_string(),
            )
                .into_response()
        }
    }
}

/// Releases the in-flight claim on drop, so an early return or a panicking
/// task cannot leave the endpoint permanently busy.
struct InFlightGuard<'a>(&'a AtomicBool);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn duration_is_clamped_before_sampling() {
        let conf = ProfilingConf {
            max_duration_secs: 15,
            ..Default::default()
        };
        assert_eq!(conf.clamp_duration(Some(999)), 15);
        assert_eq!(conf.clamp_duration(None), 15);
        assert_eq!(conf.clamp_duration(Some(3)), 3);
    }

    #[tokio::test]
    async fn disabled_config_leaves_router_without_the_route() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let conf = ProfilingConf::default();
        let router = mount(Router::new(), &conf);
        let response = router
            .oneshot(Request::get(CPU_PROFILE_PATH).body(Body::empty()).unwrap())
            .await
            .expect("router call");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
