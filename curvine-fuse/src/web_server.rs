use axum::routing::get;
use axum::Router;
use curvine_client::file::FsContext;
#[cfg(feature = "heap-trace")]
use orpc::common::heap_trace::HeapTraceRuntime;
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
            app = app.merge(orpc::common::heap_trace::router((*runtime).clone()));
        }

        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        log::info!("FUSE metrics server listening on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn metrics_handler() -> String {
    FsContext::get_metrics()
        .text_output()
        .unwrap_or_else(|e| format!("Error: {}", e))
}
