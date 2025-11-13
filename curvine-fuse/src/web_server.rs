use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;

pub struct WebServer;

impl WebServer {
    pub async fn start(port: u16) -> orpc::CommonResult<()> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/healthz", get(|| async { "ok" }));

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        tracing::info!("FUSE metrics server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn metrics_handler() -> String {
    orpc::common::Metrics::text_output().unwrap_or_else(|e| format!("Error: {}", e))
}
