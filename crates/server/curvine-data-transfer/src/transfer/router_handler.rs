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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::{Extension, Query};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use curvine_config::RuntimeConfigRegistry;
use curvine_metrics::Metrics;
use curvine_web::router::RouterHandler;
use curvine_web::router::{get_runtime_config, post_update_config, RuntimeConfigHttpResult};
use log::warn;
use serde_json::Value;

use crate::transfer::{ClusterMetadataCache, TransferService, TransferStoreBackend};

#[derive(Clone)]
pub struct TransferRouterHandler {
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    service: TransferService<TransferStoreBackend>,
    cluster_cache: ClusterMetadataCache,
    runtime_config: Arc<RuntimeConfigRegistry>,
}

impl TransferRouterHandler {
    pub fn new(
        ready: Arc<AtomicBool>,
        store_backend: &'static str,
        service: TransferService<TransferStoreBackend>,
        cluster_cache: ClusterMetadataCache,
        runtime_config: Arc<RuntimeConfigRegistry>,
    ) -> Self {
        Self {
            ready,
            store_backend,
            service,
            cluster_cache,
            runtime_config,
        }
    }
}

async fn metrics() -> String {
    Metrics::text_output().unwrap_or_else(|err| {
        warn!("failed to encode transfer metrics: {}", err);
        "metrics unavailable\n".to_string()
    })
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn readyz(
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    service: TransferService<TransferStoreBackend>,
    cluster_cache: ClusterMetadataCache,
) -> (StatusCode, String) {
    if !ready.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "not ready: starting\n".to_string(),
        );
    }

    if let Err(err) = service.check_store_available() {
        warn!(
            "transfer readiness check failed for {store_backend} store: {}",
            err
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {store_backend} transfer metadata store is unavailable\n"),
        );
    }

    if let Err(err) = cluster_cache.check_ready() {
        warn!(
            "transfer readiness check failed for cluster metadata: {}",
            err
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {err}\n"),
        );
    }

    (StatusCode::OK, "ok\n".to_string())
}

async fn runtime_config(
    Extension(handler): Extension<Arc<TransferRouterHandler>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    get_runtime_config(handler.runtime_config.clone(), Value::Null).await
}

async fn update_runtime_config(
    Extension(handler): Extension<Arc<TransferRouterHandler>>,
    query: Query<Vec<(String, String)>>,
) -> RuntimeConfigHttpResult<Json<Value>> {
    post_update_config(handler.runtime_config.clone(), query, Value::Null).await
}

impl RouterHandler for TransferRouterHandler {
    fn router(&self) -> Router {
        let ready = self.ready.clone();
        let store_backend = self.store_backend;
        let service = self.service.clone();
        let cluster_cache = self.cluster_cache.clone();
        let instance = Arc::new(self.clone());
        Router::new()
            .route("/healthz", get(healthz))
            .route(
                "/readyz",
                get(move || {
                    readyz(
                        ready.clone(),
                        store_backend,
                        service.clone(),
                        cluster_cache.clone(),
                    )
                }),
            )
            .route("/metrics", get(metrics))
            .route("/api/v1/transfer/config", get(runtime_config))
            .route("/api/update_config", post(update_runtime_config))
            .layer(Extension(instance))
    }
}
