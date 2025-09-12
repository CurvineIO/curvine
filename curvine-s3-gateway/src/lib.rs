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

//! # Curvine S3 Object Gateway
//!
//! A high-performance, S3-compatible object storage gateway built with Rust and Axum.
//! This crate provides a complete implementation of the AWS S3 API that integrates
//! seamlessly with the Curvine distributed file system.
//!
//! ## Key Features
//!
//! - **Full S3 API Compatibility**: Supports all major S3 operations including object CRUD,
//!   bucket management, and multipart uploads
//! - **High Performance**: Built on Axum and Tokio for asynchronous, high-throughput processing
//! - **Enterprise Security**: Complete AWS Signature V4 authentication implementation
//! - **Scalable Architecture**: Modular design supporting horizontal scaling
//! - **Range Request Support**: HTTP 206 partial content for efficient large file streaming
//! - **Multipart Upload**: Complete implementation of S3 multipart upload protocol
//!
//! ## Architecture Overview
//!
//! The gateway follows a layered architecture:
//!
//! 1. **HTTP Layer** (`http` module): Axum integration and request routing
//! 2. **S3 API Layer** (`s3` module): S3 protocol implementation and handlers
//! 3. **Authentication Layer** (`auth` module): AWS SigV4 signature verification
//! 4. **Storage Layer**: Integration with Curvine unified file system`

pub mod auth;
pub mod error;
pub mod http;
pub mod s3;
pub mod utils;

use std::net::SocketAddr;
use std::sync::Arc;

use auth::{
    AccessKeyStoreEnum, CredentialEntry, CredentialStore, CurvineAccessKeyStore,
    LocalAccessKeyStore,
};
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;

fn register_s3_handlers(
    router: axum::Router,
    handlers: Arc<s3::handlers::S3Handlers>,
) -> axum::Router {
    router
        // Expose concrete handlers for advanced streaming paths
        .layer(axum::Extension(handlers.clone()))
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
        // Object versions listing operations
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::ListObjectVersionsHandler + Send + Sync,
            >))
}

async fn init_s3_authentication(
    s3_conf: &curvine_common::conf::S3GatewayConf,
    ufs: &UnifiedFileSystem,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) -> orpc::CommonResult<AccessKeyStoreEnum> {
    let credentials_path = s3_conf.credentials_path.as_deref();
    let cache_refresh_interval =
        std::time::Duration::from_secs(s3_conf.cache_refresh_interval_secs);

    let initialize_result = if s3_conf.enable_distributed_auth {
        let store =
            CurvineAccessKeyStore::new(ufs.clone(), credentials_path, cache_refresh_interval)
                .map_err(|e| {
                    tracing::warn!("Failed to create Curvine credential store: {}", e);
                    format!("Failed to create distributed credential store: {}", e)
                })?;

        let store_enum = AccessKeyStoreEnum::Curvine(Arc::new(store));
        initialize_credential_store(store_enum, "distributed", rt.clone()).await
    } else {
        let store = LocalAccessKeyStore::new(credentials_path, Some(cache_refresh_interval))
            .map_err(|e| {
                tracing::warn!("Failed to create local credential store: {}", e);
                format!("Failed to create local credential store: {}", e)
            })?;

        let store_enum = AccessKeyStoreEnum::Local(Arc::new(store));
        initialize_credential_store(store_enum, "local", rt.clone()).await
    };

    match initialize_result {
        Ok(store) => Ok(store),
        Err(_) => {
            tracing::info!("Credential store is empty, trying environment variables...");
            match try_env_credentials(s3_conf, rt.clone()).await {
                Ok(store) => {
                    tracing::info!("Successfully loaded credentials from environment variables");
                    Ok(store)
                }
                Err(_) => {
                    tracing::warn!(
                        "No credentials found in primary store or environment variables"
                    );
                    log_credential_configuration_warning(s3_conf);
                    create_empty_store(s3_conf, rt.clone()).await
                }
            }
        }
    }
}

async fn try_env_credentials(
    s3_conf: &curvine_common::conf::S3GatewayConf,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) -> orpc::CommonResult<AccessKeyStoreEnum> {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .or_else(|_| std::env::var("CURVINE_ACCESS_KEY"))
        .map_err(|_| "No access key found in environment variables")?;

    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .or_else(|_| std::env::var("CURVINE_SECRET_KEY"))
        .map_err(|_| "No secret key found in environment variables")?;

    tracing::info!(
        "Found credentials in environment variables, access_key: {}",
        &access_key[..4.min(access_key.len())]
    );

    let cache_refresh_interval =
        std::time::Duration::from_secs(s3_conf.cache_refresh_interval_secs);
    let store = LocalAccessKeyStore::new(
        s3_conf.credentials_path.as_deref(),
        Some(cache_refresh_interval),
    )
    .map_err(|e| {
        format!(
            "Failed to create local credential store for env vars: {}",
            e
        )
    })?;

    let store_enum = AccessKeyStoreEnum::Local(Arc::new(store));

    store_enum
        .initialize()
        .await
        .map_err(|e| format!("Failed to initialize env credential store: {}", e))?;

    let credential = CredentialEntry::new(
        access_key,
        secret_key,
        Some("Environment Variable".to_string()),
    );
    store_enum
        .add_credential(credential)
        .await
        .map_err(|e| format!("Failed to add environment credential: {}", e))?;

    start_cache_refresh_task(&store_enum, rt);

    Ok(store_enum)
}

async fn create_empty_store(
    s3_conf: &curvine_common::conf::S3GatewayConf,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) -> orpc::CommonResult<AccessKeyStoreEnum> {
    tracing::info!("Creating empty credential store - credentials can be added later via CLI");

    let cache_refresh_interval =
        std::time::Duration::from_secs(s3_conf.cache_refresh_interval_secs);
    let store = LocalAccessKeyStore::new(
        s3_conf.credentials_path.as_deref(),
        Some(cache_refresh_interval),
    )
    .map_err(|e| format!("Failed to create empty credential store: {}", e))?;

    let store_enum = AccessKeyStoreEnum::Local(Arc::new(store));

    store_enum
        .initialize()
        .await
        .map_err(|e| format!("Failed to initialize empty credential store: {}", e))?;

    start_cache_refresh_task(&store_enum, rt);

    tracing::info!("Empty credential store created successfully");
    Ok(store_enum)
}

async fn initialize_credential_store(
    store_enum: AccessKeyStoreEnum,
    store_type_name: &str,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) -> orpc::CommonResult<AccessKeyStoreEnum> {
    store_enum.initialize().await.map_err(|e| {
        tracing::warn!(
            "Failed to initialize {} credential store: {}",
            store_type_name,
            e
        );
        format!(
            "Failed to initialize {} credential store: {}",
            store_type_name, e
        )
    })?;

    match store_enum.list_credentials().await {
        Ok(credentials) if !credentials.is_empty() => {
            tracing::info!(
                "Using {} credential store with {} credentials",
                store_type_name,
                credentials.len()
            );

            start_cache_refresh_task(&store_enum, rt);

            Ok(store_enum)
        }
        Ok(_) => {
            tracing::info!("{} credential store is empty", store_type_name);
            orpc::err_box!("Empty {} credential store", store_type_name)
        }
        Err(e) => {
            tracing::warn!(
                "Failed to list credentials from {} store: {}",
                store_type_name,
                e
            );
            orpc::err_box!(
                "Failed to validate {} credential store: {}",
                store_type_name,
                e
            )
        }
    }
}

fn start_cache_refresh_task(
    store_enum: &AccessKeyStoreEnum,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) {
    match store_enum.start_cache_refresh_task(rt) {
        Ok(()) => {
            tracing::info!(
                "Started cache refresh task for {} credential store",
                store_enum.store_type()
            );
        }
        Err(e) => {
            tracing::warn!(
                "Failed to start cache refresh task for {} store: {}",
                store_enum.store_type(),
                e
            );
        }
    }
}

fn log_credential_configuration_warning(s3_conf: &curvine_common::conf::S3GatewayConf) {
    tracing::warn!("No S3 credentials configured. Gateway started with empty credential store.");
    tracing::warn!("Please configure S3 credentials using one of the following methods:");

    if s3_conf.enable_distributed_auth {
        tracing::warn!("1. Add credentials to distributed store: curvine-s3-gateway credential add --access-key <key> --secret-key <secret>");
    } else {
        tracing::warn!("1. Add credentials to local store: curvine-s3-gateway credential add --access-key <key> --secret-key <secret>");
    }

    tracing::warn!("2. Generate random credentials: curvine-s3-gateway credential generate");
    tracing::warn!("3. Set environment variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY");
    tracing::warn!(
        "   Or use Curvine-specific variables: CURVINE_ACCESS_KEY and CURVINE_SECRET_KEY"
    );
    tracing::warn!("Note: S3 requests will fail until credentials are configured.");
}

pub async fn start_gateway(
    conf: ClusterConf,
    listen: String,
    region: String,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) -> orpc::CommonResult<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(false)
        .try_init();

    tracing::debug!(
        "Try to start Curvine S3 Gateway on {} with region {}",
        listen,
        region
    );

    let ufs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
    let ak_store = init_s3_authentication(&conf.s3_gateway, &ufs, rt.clone()).await?;
    let handlers = Arc::new(s3::handlers::S3Handlers::new(
        ufs,
        region.clone(),
        conf.s3_gateway.put_temp_dir.clone(),
        rt.clone(),
        conf.s3_gateway.get_mpsc_capacity,
        conf.s3_gateway.get_chunk_size_mb,
        conf.s3_gateway.get_prefetch_depth,
    ));

    tracing::info!("S3 Gateway authentication configured successfully");

    let app = axum::Router::new()
        .layer(axum::middleware::from_fn(crate::http::handle_fn))
        .layer(axum::middleware::from_fn(
            crate::http::handle_authorization_middleware,
        ))
        .layer(axum::middleware::from_fn(http_performance_middleware))
        .layer(axum::Extension(ak_store))
        .route("/healthz", axum::routing::get(|| async { "ok" }));

    let app = register_s3_handlers(app, handlers);

    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| format!("Invalid listen address '{}': {}", listen, e))?;
    tracing::debug!("Binding to address: {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    if let Ok(socket) = listener.local_addr() {
        tracing::info!("S3 Gateway started successfully on {}", socket);
    }

    axum::serve(listener, app).await?;
    Ok(())
}

async fn http_performance_middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut response = next.run(request).await;

    let headers = response.headers_mut();
    if !headers.contains_key("connection") {
        headers.insert(
            axum::http::header::CONNECTION,
            axum::http::HeaderValue::from_static("keep-alive"),
        );
    }

    headers.insert(
        "server",
        axum::http::HeaderValue::from_static("curvine-s3-gateway"),
    );

    response
}
