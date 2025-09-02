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

// Module declarations with brief descriptions
pub mod auth; // Authentication and authorization mechanisms
pub mod http; // HTTP layer with Axum integration and routing
pub mod s3; // S3 API implementation and protocol handlers
pub mod utils; // Utility functions and helper types

use std::net::SocketAddr;
use std::sync::Arc;

use auth::StaticAccessKeyStore;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;

/// Register all S3 handlers with the Axum router
///
/// This function sets up the complete S3 API handler chain by registering all
/// required handler implementations as Axum extensions. Each handler is wrapped
/// in an Arc for thread-safe sharing across request handlers.
///
/// # Arguments
///
/// * `router` - The base Axum router to extend with S3 handlers
/// * `handlers` - Shared S3 handlers implementation containing all S3 operations
///
/// # Returns
///
/// * `axum::Router` - Extended router with all S3 handlers registered
///
/// # Handler Types Registered
///
/// - `PutObjectHandler`: Object upload operations
/// - `HeadHandler`: Object metadata retrieval  
/// - `ListBucketHandler`: Bucket listing operations
/// - `CreateBucketHandler`: Bucket creation operations
/// - `DeleteBucketHandler`: Bucket deletion operations
/// - `DeleteObjectHandler`: Object deletion operations
/// - `GetObjectHandler`: Object download operations
/// - `GetBucketLocationHandler`: Bucket location retrieval
/// - `MultiUploadObjectHandler`: Multipart upload operations
/// - `ListObjectHandler`: Object listing operations
///
/// # Design Notes
///
/// Each handler is registered as a separate extension to enable fine-grained
/// feature control. This allows disabling specific operations by not registering
/// their handlers, improving security and resource usage.
fn register_s3_handlers(
    router: axum::Router,
    handlers: Arc<s3::handlers::S3Handlers>,
) -> axum::Router {
    router
        // Object upload operations
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::PutObjectHandler + Send + Sync>
        ))
        // Object metadata operations
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::HeadHandler + Send + Sync>
        ))
        // Bucket listing operations
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::ListBucketHandler + Send + Sync>
        ))
        // Bucket creation operations
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::CreateBucketHandler + Send + Sync,
            >))
        // Bucket deletion operations
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::DeleteBucketHandler + Send + Sync,
            >))
        // Object deletion operations
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::DeleteObjectHandler + Send + Sync,
            >))
        // Object download operations
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::GetObjectHandler + Send + Sync>
        ))
        // Bucket location operations
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::GetBucketLocationHandler + Send + Sync,
            >))
        // Multipart upload operations
        .layer(axum::Extension(handlers.clone()
            as Arc<
                dyn crate::s3::s3_api::MultiUploadObjectHandler + Send + Sync,
            >))
        // Object listing operations
        .layer(axum::Extension(
            handlers.clone() as Arc<dyn crate::s3::s3_api::ListObjectHandler + Send + Sync>
        ))
}

/// Initialize S3 authentication credentials with comprehensive fallback strategy
///
/// This function implements a robust credential loading strategy that attempts
/// multiple sources in order of preference:
///
/// 1. **Configuration File**: S3 gateway configuration (access_key, secret_key)
/// 2. **Environment Variables**: Standard AWS credential environment variables
/// 3. **Error**: No fallback credentials, gateway fails to start
///
/// # Arguments
///
/// * `s3_conf` - S3 gateway configuration containing optional credentials
///
/// # Returns
///
/// * `CommonResult<Arc<dyn AccesskeyStore + Send + Sync>>` - Thread-safe access key store
///
/// # Environment Variables
///
/// The function looks for these standard AWS environment variables:
/// - `AWS_ACCESS_KEY_ID` or `CURVINE_ACCESS_KEY`: Access key identifier
/// - `AWS_SECRET_ACCESS_KEY` or `CURVINE_SECRET_KEY`: Secret access key
///
/// # Error Handling
///
/// Returns an error if no credentials can be loaded from any source.
/// This ensures the gateway doesn't start with insecure default credentials.
///
/// # Security Notes
///
/// - Configuration-based credentials are preferred for production deployments
/// - Environment-based credentials provide compatibility with AWS tooling
/// - No default credentials are provided to prevent security issues
async fn init_s3_authentication(
    s3_conf: &curvine_common::conf::S3GatewayConf,
) -> orpc::CommonResult<Arc<dyn crate::auth::AccesskeyStore + Send + Sync>> {
    // First priority: Check configuration file credentials
    if let (Some(access_key), Some(secret_key)) = (&s3_conf.access_key, &s3_conf.secret_key) {
        if !access_key.trim().is_empty() && !secret_key.trim().is_empty() {
            tracing::info!("Using S3 credentials from configuration file");
            let store =
                StaticAccessKeyStore::with_single_key(access_key.clone(), secret_key.clone());
            return Ok(Arc::new(store));
        }
    }

    // Second priority: Attempt to load credentials from environment variables
    match StaticAccessKeyStore::from_env() {
        Ok(store) => {
            tracing::info!("Using S3 credentials from environment variables");
            Ok(Arc::new(store))
        }
        Err(env_err) => {
            tracing::error!(
                "Failed to load S3 credentials from both configuration and environment: {}",
                env_err
            );
            tracing::error!("Please configure S3 credentials in one of the following ways:");
            tracing::error!(
                "1. Set access_key and secret_key in [s3_gateway] section of config file"
            );
            tracing::error!(
                "2. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"
            );
            tracing::error!(
                "3. Set CURVINE_ACCESS_KEY and CURVINE_SECRET_KEY environment variables"
            );

            Err(format!(
                "No S3 credentials configured. Gateway cannot start without authentication. {env_err}"
            ).into())
        }
    }
}

/// Start the S3 gateway server with specified configuration
///
/// This is the main entry point for starting the Curvine S3 Object Gateway.
/// It initializes all necessary components and starts the HTTP server.
///
/// # Arguments
///
/// * `conf` - Cluster configuration containing file system and runtime settings
/// * `listen` - Network address and port to bind the HTTP server (e.g., "0.0.0.0:9900")
/// * `region` - S3 region identifier to report in responses (e.g., "us-east-1")
///
/// # Returns
///
/// * `orpc::CommonResult<()>` - Success or detailed error information
///
/// # Initialization Process
///
/// 1. **Logging Setup**: Initialize structured logging with environment-based filtering
/// 2. **Runtime Creation**: Set up shared async runtime for file system operations
/// 3. **File System Initialization**: Create unified file system interface
/// 4. **Handler Creation**: Initialize S3 operation handlers
/// 5. **Authentication Setup**: Configure S3 credential verification
/// 6. **Router Configuration**: Set up Axum router with middleware chain
/// 7. **Server Startup**: Bind to network address and start serving requests
///
/// # Server Configuration
///
/// The server is configured with the following middleware chain:
/// - S3 request handler (main routing logic)
/// - AWS Signature V4 authentication middleware
/// - Access key store for credential verification
/// - Health check endpoint (`/healthz`)
///
/// # Error Handling
///
/// The function handles various initialization errors:
/// - Invalid network address format
/// - File system initialization failures
/// - Authentication setup failures
/// - Network binding failures
///
/// # Performance Characteristics
///
/// - **Async I/O**: All operations use Tokio's async runtime
/// - **Thread Safety**: Shared state is protected with Arc for concurrent access
/// - **Resource Management**: Proper cleanup and resource management
/// - **Memory Efficiency**: Streaming operations for large objects
///
pub async fn start_gateway(
    conf: ClusterConf,
    listen: String,
    region: String,
    rt: std::sync::Arc<orpc::runtime::AsyncRuntime>,
) -> orpc::CommonResult<()> {
    // Initialize logging for standalone mode; ignore error if already set
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(false)
        .try_init();

    tracing::info!(
        "Try to start Curvine S3 Gateway on {} with region {}",
        listen,
        region
    );

    // Use the provided unified runtime (no need to create a new one)
    // This shared runtime is managed at the application level

    // Initialize the unified file system with the shared runtime
    let ufs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;

    // Create S3 handlers with file system, region, and multipart temp configuration
    let handlers = Arc::new(s3::handlers::S3Handlers::new(
        ufs,
        region.clone(),
        conf.s3_gateway.multipart_temp.clone(),
        rt.clone(),
    ));

    // Initialize S3 authentication with fallback strategy
    let ak_store = init_s3_authentication(&conf.s3_gateway).await?;
    tracing::info!("S3 Gateway authentication configured successfully");

    // Configure the Axum application with middleware chain
    let app = axum::Router::new()
        // Main S3 request handling middleware (routes all S3 operations)
        .layer(axum::middleware::from_fn(crate::http::handle_fn))
        // AWS Signature V4 authentication middleware (verifies all requests)
        .layer(axum::middleware::from_fn(
            crate::http::handle_authorization_middleware,
        ))
        // Access key store for credential verification
        .layer(axum::Extension(ak_store))
        // Health check endpoint for monitoring and load balancing
        .route("/healthz", axum::routing::get(|| async { "ok" }));

    // Register all S3 handlers with the router
    let app = register_s3_handlers(app, handlers);

    // Parse the listen address and bind the server
    let addr: SocketAddr = listen.parse().expect("invalid listen address");
    tracing::debug!("Binding to address: {}", addr);

    // Create TCP listener for incoming connections
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("S3 Gateway started successfully on {}", addr);

    // Start serving requests (this blocks until shutdown)
    axum::serve(listener, app).await?;
    Ok(())
}
