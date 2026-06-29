use crate::fs::state::NodeState;
use crate::fuse_metrics::mono_now;
use crate::FuseMetrics;
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use orpc::common::Metrics;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct WebServer;

impl WebServer {
    pub async fn start(port: u16, state: Arc<NodeState>) -> orpc::CommonResult<()> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(|| async { "ok" }))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        log::info!("FUSE metrics server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

// The `NodeState` is still injected into the router (`with_state`) so
// `WebServer::start`'s public signature is unchanged, but the handler no longer
// reads it: Phase 1b-2 made the legacy gauges (inode/handle) event-driven and
// removed the scrape-time `set_metrics()` refresh, so the scrape is now a pure
// `text_output()` with no map traversal under read locks.
async fn metrics_handler(State(_): State<Arc<NodeState>>) -> String {
    let fuse_metrics = FuseMetrics::get();

    // Scrape hygiene (unconditional, self-observation): time the render and
    // record the output size. Last-scrape semantics — the values in *this*
    // response reflect the previous scrape; the current scrape is visible next
    // time. This ordering (text_output THEN record_scrape) is what makes it
    // "last scrape"; do not reorder. Recorded on both success and the
    // error-body fallback.
    //
    // As of 1b-2 the scrape-time `set_metrics()` map traversal is gone, so this
    // timer covers the meaningful scrape work — the `text_output()` render —
    // rather than under-counting by that traversal. It deliberately does NOT
    // include Axum's `State` extraction or the `FuseMetrics::get()` before
    // `start`; those are tiny and out of scope for a render-regression signal.
    let start = mono_now();
    let output = Metrics::text_output().unwrap_or_else(|e| format!("Error: {}", e));
    fuse_metrics.record_scrape(start.elapsed().as_micros() as u64, output.len());
    output
}
