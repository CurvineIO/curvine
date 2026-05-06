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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::middleware;
use axum::Router;
use clap::Parser;
use curvine_common::conf::ClusterConf;
use curvine_common::version;
use curvine_vector_gateway::auth_store::{AccessKeyStoreEnum, LocalAccessKeyStore};
use curvine_vector_gateway::{auth, http};

#[derive(Debug, Parser, Clone)]
#[command(version = version::VERSION)]
struct VectorGatewayArgs {
    #[arg(long, default_value = "etc/curvine-cluster.toml")]
    conf: String,
    #[arg(long, default_value = "0.0.0.0:9951")]
    listen: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = VectorGatewayArgs::parse();

    let conf = ClusterConf::from(&args.conf).unwrap_or_else(|e| {
        tracing::warn!("falling back to default cluster conf ({e})");
        ClusterConf::default()
    });

    orpc::common::Logger::init(conf.master.log.clone());

    let store = init_local_store(&conf).await?;

    let app = Router::new()
        .merge(http::health_router())
        .merge(
            http::vector_action_router()
                .layer(middleware::from_fn(auth::vectors_sig_v4_middleware)),
        )
        .layer(axum::Extension(store));

    tracing::info!(
        "vector subsystem (gateway): default_shard_count={} segment_target_size_mb={} row_high_watermark={} size_mb_high_watermark={} (worker_private_root is worker-local: {})",
        conf.vector.default_shard_count,
        conf.vector.segment_target_size_mb,
        conf.vector.active_segment_row_high_watermark,
        conf.vector.active_segment_size_mb_high_watermark,
        conf.vector.worker_private_vector_root,
    );

    let addr: SocketAddr = args.listen.parse()?;
    tracing::info!(
        "curvine-vector-gateway listening on {addr} (SigV4 service namespace = s3vectors)"
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn init_local_store(
    conf: &ClusterConf,
) -> Result<AccessKeyStoreEnum, Box<dyn std::error::Error>> {
    let path = conf.s3_gateway.credentials_path.as_deref();
    let interval = Duration::from_secs(conf.s3_gateway.cache_refresh_interval_secs.max(1));
    let store = LocalAccessKeyStore::new(path, Some(interval))?;
    let enum_store = AccessKeyStoreEnum::Local(Arc::new(store));

    enum_store.initialize().await?;

    Ok(enum_store)
}
