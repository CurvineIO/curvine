use crate::fs::state::NodeState;
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
    state.set_metrics(fuse_metrics);
    Metrics::text_output().unwrap_or_else(|e| format!("Error: {}", e))
}
