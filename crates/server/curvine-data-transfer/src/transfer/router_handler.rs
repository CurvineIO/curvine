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

use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use curvine_web::router::RouterHandler;
use log::warn;
use orpc::common::Metrics;

use crate::transfer::{ClusterMetadataCache, TransferService, TransferStoreBackend};

pub struct TransferRouterHandler {
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    service: TransferService<TransferStoreBackend>,
    cluster_cache: ClusterMetadataCache,
}

impl TransferRouterHandler {
    pub fn new(
        ready: Arc<AtomicBool>,
        store_backend: &'static str,
        service: TransferService<TransferStoreBackend>,
        cluster_cache: ClusterMetadataCache,
    ) -> Self {
        Self {
            ready,
            store_backend,
            service,
            cluster_cache,
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

impl RouterHandler for TransferRouterHandler {
    fn router(&self) -> Router {
        let ready = self.ready.clone();
        let store_backend = self.store_backend;
        let service = self.service.clone();
        let cluster_cache = self.cluster_cache.clone();
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
    }
}
