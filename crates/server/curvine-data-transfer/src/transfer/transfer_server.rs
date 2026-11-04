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

use std::sync::Arc;
use std::time::Duration;

use crate::common::UfsFactory;
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, TransferStoreType};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_net::net::ConnState;
use curvine_net::net::NetUtils;
use curvine_proto::transfer_service_server::TransferServiceServer;
use curvine_rpc::handler::HandlerService;
use curvine_rpc::server::{ServerConf, ServerMonitor, ServerStateListener};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::transfer::grpc_handler::TransferRequestTracker;
use crate::transfer::{
    ClusterMetadataCache, MemoryTransferStore, MysqlTransferStore, PostgresTransferStore,
    SqliteTransferStore, TransferGrpcHandler, TransferHandler, TransferMetrics, TransferPlanner,
    TransferRouterHandler, TransferScheduler, TransferService, TransferStoreBackend,
};

#[derive(Clone)]
pub struct TransferRpcService {
    service: TransferService<TransferStoreBackend>,
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    cluster_cache: ClusterMetadataCache,
}

impl TransferRpcService {
    fn new(
        service: TransferService<TransferStoreBackend>,
        ready: Arc<AtomicBool>,
        store_backend: &'static str,
        cluster_cache: ClusterMetadataCache,
    ) -> Self {
        Self {
            service,
            ready,
            store_backend,
            cluster_cache,
        }
    }
}

impl HandlerService for TransferRpcService {
    type Item = TransferHandler<TransferStoreBackend>;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        TransferHandler::new(self.service.clone())
    }
}

impl WebHandlerService for TransferRpcService {
    type Item = TransferRouterHandler;

    fn get_handler(&self) -> Self::Item {
        TransferRouterHandler::new(
            self.ready.clone(),
            self.store_backend,
            self.service.clone(),
            self.cluster_cache.clone(),
        )
    }
}

pub struct TransferServer {
    rpc_rt: Arc<Runtime>,
    rpc_conf: ServerConf,
    rpc_service: TransferRpcService,
    web_server: WebServer<TransferRpcService>,
    cluster_cache: ClusterMetadataCache,
    scheduler: TransferScheduler<TransferStoreBackend>,
    ready: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
}

impl TransferServer {
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        if !conf.transfer.enabled {
            return Err(FsError::common("curvine-transfer requires transfer.enabled=true").into());
        }
        Logger::init(conf.transfer.log.clone());
        let _ = TransferMetrics::get()?;
        info!("allocator: {}", curvine_alloc::allocator_type_name());
        info!("git version: {}", curvine_sys::version::GIT_VERSION);

        let rt = Arc::new(conf.transfer_server_conf().create_runtime());
        let stop = Arc::new(AtomicBool::new(false));
        let store = Arc::new(match conf.transfer.effective_store_type() {
            TransferStoreType::Auto => unreachable!("resolved transfer store type cannot be auto"),
            TransferStoreType::Memory => TransferStoreBackend::Memory(MemoryTransferStore::new()),
            TransferStoreType::Sqlite => TransferStoreBackend::Sqlite(SqliteTransferStore::open(
                conf.transfer.sqlite_store_path(),
            )?),
            TransferStoreType::Mysql => TransferStoreBackend::Mysql(MysqlTransferStore::open(
                conf.transfer.mysql_store_url(),
            )?),
            TransferStoreType::Postgres => TransferStoreBackend::Postgres(
                PostgresTransferStore::open(conf.transfer.postgres_store_url())?,
            ),
        });
        let store_backend = store.backend_label();
        let report_endpoints = if conf.transfer.endpoints.is_empty() {
            vec![format!(
                "{}:{}",
                conf.transfer.hostname, conf.transfer.rpc_port
            )]
        } else {
            conf.transfer.endpoints.clone()
        };
        info!(
            "configured curvine-transfer store_backend={} rpc_addr={}:{} web_addr={}:{} report_endpoints={:?} max_running={} max_tasks={} task_stale_timeout_ms={}",
            store_backend,
            conf.transfer.hostname,
            conf.transfer.rpc_port,
            conf.transfer.hostname,
            conf.transfer.web_port,
            report_endpoints,
            conf.transfer.max_running_transfers,
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.task_stale_timeout.as_millis(),
        );
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone())?;
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));
        let cache = ClusterMetadataCache::with_snapshot_policy(
            fs.clone(),
            conf.transfer.cluster_snapshot_max_staleness,
            conf.transfer.allow_submit_with_stale_snapshot,
        );
        cache.clone().start_refresh_loop(
            Duration::from_millis(conf.client.mount_update_ttl_ms),
            stop.clone(),
        );
        let owner = if conf.transfer.instance_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            conf.transfer.instance_id.clone()
        };
        let planner = TransferPlanner::new(
            fs,
            factory.clone(),
            cache.clone(),
            conf.client.clone(),
            conf.transfer.max_tasks_per_transfer,
            conf.transfer.ufs_max_concurrency_per_endpoint,
        );
        let scheduler = TransferScheduler::new(
            store.clone(),
            planner,
            cache.clone(),
            factory,
            owner,
            report_endpoints,
            conf.transfer.clone(),
        );

        let ready = Arc::new(AtomicBool::new(false));
        let service = TransferRpcService::new(
            TransferService::with_cache_and_report_queue(
                store,
                cache.clone(),
                conf.transfer.task_stale_timeout,
                conf.transfer.task_report_queue_size(),
                conf.transfer.worker_threads,
            )?,
            ready.clone(),
            store_backend,
            cache.clone(),
        );
        let rpc_conf = conf.transfer_server_conf();
        let web_server = WebServer::with_rt(rt.clone(), conf.transfer_web_conf(), service.clone());
        Ok(Self {
            rpc_rt: rt,
            rpc_conf,
            rpc_service: service,
            web_server,
            cluster_cache: cache,
            scheduler,
            ready,
            stop,
        })
    }

    pub async fn start(self) -> CommonResult<ServerStateListener> {
        let ready = self.ready.clone();
        let web_name = self.web_server.server_name().to_string();
        let bind_addr = self.web_server.resolve_bind_addr();
        let mut web_status = self.web_server.start();
        WebServer::<TransferRpcService>::wait_bind(&mut web_status, &web_name, &bind_addr).await?;

        let result = async {
            self.cluster_cache.refresh().await?;
            let hostname = std::env::var("ORPC_BIND_HOSTNAME")
                .unwrap_or_else(|_| self.rpc_conf.hostname.clone());
            let address = format!("{}:{}", hostname, self.rpc_conf.port);
            let listener = match NetUtils::take_held_listener(self.rpc_conf.port) {
                Some(listener) => {
                    listener.set_nonblocking(true)?;
                    tokio::net::TcpListener::from_std(listener)?
                }
                None => tokio::net::TcpListener::bind(&address).await?,
            };
            self.scheduler.start(self.rpc_rt.clone(), self.stop.clone());
            let monitor = Arc::new(ServerMonitor::new());
            let rpc_status = monitor.new_listener();
            let task_monitor = monitor.clone();
            let stop = self.stop.clone();
            let task_ready = self.ready.clone();
            let tracker = TransferRequestTracker::default();
            let handler = TransferGrpcHandler::with_tracker(self.rpc_service.service.clone(), tracker.clone());
            let (health, health_service) = curvine_rpc::grpc::health_reporter();
            health
                .set_serving::<TransferServiceServer<TransferGrpcHandler<TransferStoreBackend>>>()
                .await;
            let tls = self.rpc_conf.grpc.server_tls()?;
            let reflection = if self.rpc_conf.grpc.enable_reflection {
                Some(curvine_rpc::grpc::reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(curvine_proto::FILE_DESCRIPTOR_SET)
                    .with_service_name("proto.TransferService")
                    .build_v1alpha()?)
            } else { None };
            let transport = curvine_rpc::grpc::TransportConfig::default();
            let mut grpc_server = transport.server(tls.as_ref())?;
            monitor.advance_running();
            ready.store(true, Ordering::Relaxed);
            self.rpc_rt.spawn(async move {
                let shutdown_tracker = tracker.clone();
                let shutdown_ready = task_ready.clone();
                let shutdown_stop = stop.clone();
                let shutdown_monitor = task_monitor.clone();
                let incoming = futures::stream::unfold(
                    (listener, transport),
                    |(listener, transport)| async move {
                        let accepted = listener.accept().await.and_then(|(stream, _)| {
                            transport.configure_socket(&stream)?;
                            Ok(stream)
                        });
                        Some((accepted, (listener, transport)))
                    },
                );
                let service = TransferServiceServer::new(handler)
                    .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
                let result = grpc_server
                    .add_optional_service(reflection)
                    .add_service(health_service)
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async move {
                        shutdown_signal().await;
                        shutdown_tracker.shutdown();
                        shutdown_ready.store(false, Ordering::Relaxed);
                        shutdown_stop.store(true, Ordering::Relaxed);
                        shutdown_monitor.advance_shutdown();
                        health
                            .set_not_serving::<TransferServiceServer<TransferGrpcHandler<TransferStoreBackend>>>()
                            .await;
                    })
                    .await;
                if let Err(error) = result {
                    log::error!("transfer gRPC server stopped: {}", error);
                }
                task_ready.store(false, Ordering::Relaxed);
                task_monitor.advance_shutdown();
                stop.store(true, Ordering::Relaxed);
                tracker.shutdown();
                tracker.wait_drained().await;
                task_monitor.advance_stop();
            });
            info!("Starting curvine-transfer gRPC server on {}", address);
            Ok(rpc_status)
        }
        .await;

        if result.is_err() {
            self.stop.store(true, Ordering::Relaxed);
        }
        result
    }

    pub fn block_on_start(self) -> CommonResult<()> {
        let rt = self.rpc_rt.clone();
        rt.block_on(async move {
            let mut status = self.start().await?;
            if let Err(err) = status.wait_stop().await {
                log::warn!("curvine-transfer wait stop failed: {}", err);
            }
            Ok(())
        })
    }
}

async fn shutdown_signal() {
    #[cfg(target_os = "linux")]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            Err(error) => {
                log::error!("failed to register SIGTERM handler: {}", error);
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = tokio::signal::ctrl_c().await;
}
