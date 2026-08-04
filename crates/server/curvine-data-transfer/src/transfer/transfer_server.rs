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
use curvine_error::FsError;
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use orpc::common::Logger;
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::RpcRuntime;
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::transfer::{
    ClusterMetadataCache, MemoryTransferStore, MysqlTransferStore, SqliteTransferStore,
    TransferHandler, TransferMetrics, TransferPlanner, TransferRouterHandler, TransferScheduler,
    TransferService, TransferStoreBackend,
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
    rpc_server: RpcServer<TransferRpcService>,
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
        Logger::init(conf.master.log.clone());
        let _ = TransferMetrics::get()?;
        info!("allocator: {}", curvine_alloc::allocator_type_name());
        info!("git version: {}", curvine_sys::version::GIT_VERSION);
        conf.print();

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
        });
        let store_backend = store.backend_label();
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
        let rpc_server =
            RpcServer::with_rt(rt.clone(), conf.transfer_server_conf(), service.clone());
        let web_server = WebServer::with_rt(rt, conf.transfer_web_conf(), service);
        let stop_on_rpc_stop = stop.clone();
        rpc_server.add_shutdown_hook(move || {
            stop_on_rpc_stop.store(true, Ordering::Relaxed);
        });
        Ok(Self {
            rpc_server,
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
            self.scheduler
                .start(self.rpc_server.clone_rt(), self.stop.clone());
            info!("Starting curvine-transfer rpc server");
            let mut rpc_status = self.rpc_server.start();
            rpc_status.wait_running().await?;
            ready.store(true, Ordering::Relaxed);
            Ok(rpc_status)
        }
        .await;

        if result.is_err() {
            self.stop.store(true, Ordering::Relaxed);
        }
        result
    }

    pub fn block_on_start(self) -> CommonResult<()> {
        let rt = self.rpc_server.clone_rt();
        rt.block_on(async move {
            let mut status = self.start().await?;
            if let Err(err) = status.wait_stop().await {
                log::warn!("curvine-transfer wait stop failed: {}", err);
            }
            Ok(())
        })
    }
}
