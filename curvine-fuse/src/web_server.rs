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

async fn metrics_handler(State(state): State<Arc<NodeState>>) -> String {
    let fuse_metrics = FuseMetrics::get();
    // Scrape-time refresh of the legacy gauges (event-driven migration is
    // Phase 1b-2; until then this stays so the legacy gauges remain correct).
    state.set_metrics(fuse_metrics);

    // Scrape hygiene (unconditional, self-observation): time the render and
    // record the output size. Last-scrape semantics — the values in *this*
    // response reflect the previous scrape; the current scrape is visible next
    // time. This ordering (text_output THEN record_scrape) is what makes it
    // "last scrape"; do not reorder. Recorded on both success and the
    // error-body fallback.
    //
    // NOTE (1b-1): `metrics_scrape_duration_us` times only `text_output()`, NOT
    // the `set_metrics()` lock/traversal above. Once 1b-2 makes the legacy
    // gauges event-driven and removes `set_metrics()`, this duration equals the
    // full handler cost; until then it under-counts by the set_metrics scan.
    let start = mono_now();
    let output = Metrics::text_output().unwrap_or_else(|e| format!("Error: {}", e));
    fuse_metrics.record_scrape(start.elapsed().as_micros() as u64, output.len());
    output
}
