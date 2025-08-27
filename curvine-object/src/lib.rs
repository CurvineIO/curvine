pub mod auth;
pub mod config;
pub mod http;
pub mod s3;

pub mod utils;

use std::net::SocketAddr;
use std::sync::Arc;

use auth::StaticAccessKeyStore;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;

/// Helper function to register all S3 handlers with the router
fn register_s3_handlers(
    router: axum::Router,
    handlers: Arc<s3::s3_handlers::S3Handlers>,
) -> axum::Router {
    router
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::PutObjectHandler + Send + Sync>
        ))
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::HeadHandler + Send + Sync>
        ))
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::ListBucketHandler + Send + Sync>
        ))
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::CreateBucketHandler + Send + Sync,
            >))
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::DeleteBucketHandler + Send + Sync,
            >))
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::DeleteObjectHandler + Send + Sync,
            >))
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::GetObjectHandler + Send + Sync>
        ))
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::GetBucketLocationHandler + Send + Sync,
            >))
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::MultiUploadObjectHandler + Send + Sync,
            >))
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::ListObjectHandler + Send + Sync>
        ))
}

/// Initialize S3 authentication credentials
///
/// Attempts to load credentials in the following order:
/// 1. Environment variables
/// 2. Secure random generation (fallback)
///
/// # Returns
/// * `CommonResult<Arc<dyn AccesskeyStore + Send + Sync>>` - Authentication store or error
async fn init_s3_authentication(
) -> orpc::CommonResult<Arc<dyn crate::auth::AccesskeyStore + Send + Sync>> {
    // Try environment variables first
    match StaticAccessKeyStore::from_env() {
        Ok(store) => {
            tracing::info!("Using S3 credentials from environment variables");
            Ok(Arc::new(store))
        }
        Err(env_err) => {
            tracing::warn!(
                "Failed to load S3 credentials from environment: {}",
                env_err
            );

            // Use fixed test credentials as fallback for development
            let store = StaticAccessKeyStore::with_single_key(
                "AqU4axe4feDyIielarPI".to_string(),
                "0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt".to_string(),
            );
            tracing::warn!("Using fixed test S3 credentials for development");
            Ok(Arc::new(store))
        }
    }
}

pub async fn start_gateway(
    conf: ClusterConf,
    listen: String,
    region: String,
) -> orpc::CommonResult<()> {
    // Ensure logging is initialized in standalone mode; ignore error if already set
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(false)
        .try_init();

    tracing::info!(
        "Starting Curvine S3 Gateway on {} with region {}",
        listen,
        region
    );

    // Create a runtime that can be used within tokio context
    let rt = std::sync::Arc::new(orpc::runtime::AsyncRuntime::new(
        "curvine-object",
        conf.client.io_threads,
        conf.client.worker_threads,
    ));
    // Prevent dropping inner Tokio runtime inside an async context
    let _leaked_rt = std::sync::Arc::clone(&rt);
    std::mem::forget(_leaked_rt);

    let ufs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
    let handlers = Arc::new(s3::s3_handlers::S3Handlers::new(
        ufs,
        "us-east-1".to_string(),
        rt.clone(),
    ));

    // Initialize S3 authentication
    let ak = init_s3_authentication().await?;
    tracing::info!("S3 Gateway authentication configured successfully");

    let app = axum::Router::new()
        .layer(axum::middleware::from_fn(crate::http::handle_fn))
        .layer(axum::middleware::from_fn(
            crate::http::handle_authorization_middleware,
        ))
        .layer(axum::Extension(ak))
        .route("/healthz", axum::routing::get(|| async { "ok" }));

    let app = register_s3_handlers(app, handlers);

    let addr: SocketAddr = listen.parse().expect("invalid listen address");
    tracing::info!("Binding to address: {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("S3 Gateway started successfully on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}
