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
use curvine_config::{ClusterConf, ConfigSourceMap, RuntimeConfigRegistry, TransferStoreType};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_net::net::ConnState;
use curvine_rpc::handler::HandlerService;
use curvine_rpc::server::{RpcServer, ServerStateListener};
use curvine_runtime::common::Logger;
use curvine_runtime::runtime::RpcRuntime;
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::transfer::{
    build_transfer_runtime_registry, ClusterMetadataCache, Forwarder, MemoryTransferStore,
    MysqlTransferStore, PostgresTransferStore, SqliteTransferStore, TaskCache, TransferHandler,
    TransferMetrics, TransferPlanner, TransferRouterHandler, TransferRuntimeConfig,
    TransferScheduler, TransferService, TransferStoreBackend,
};

#[derive(Clone)]
pub struct TransferRpcService {
    service: TransferService<TransferStoreBackend>,
    forwarder: Option<Arc<Forwarder>>,
    ready: Arc<AtomicBool>,
    store_backend: &'static str,
    cluster_cache: ClusterMetadataCache,
    runtime_config: Arc<RuntimeConfigRegistry>,
    /// Layer attribution from the loader, for the config endpoint.
    config_sources: ConfigSourceMap,
    conf: Arc<ClusterConf>,
}

impl TransferRpcService {
    #[allow(clippy::too_many_arguments)]
    fn new(
        service: TransferService<TransferStoreBackend>,
        forwarder: Option<Arc<Forwarder>>,
        ready: Arc<AtomicBool>,
        store_backend: &'static str,
        cluster_cache: ClusterMetadataCache,
        runtime_config: Arc<RuntimeConfigRegistry>,
        conf: Arc<ClusterConf>,
        config_sources: ConfigSourceMap,
    ) -> Self {
        Self {
            service,
            forwarder,
            ready,
            store_backend,
            cluster_cache,
            runtime_config,
            config_sources,
            conf,
        }
    }
}

impl HandlerService for TransferRpcService {
    type Item = TransferHandler<TransferStoreBackend>;

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        let handler = TransferHandler::new(self.service.clone());
        match &self.forwarder {
            Some(forwarder) => handler.with_forwarder(forwarder.clone()),
            None => handler,
        }
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
            self.runtime_config.clone(),
            &self.conf,
            self.config_sources.clone(),
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
    /// Present in discovery=sql mode; dropped (or shut down) = deregistered.
    registration: Option<curvine_service_discovery::RegistrationGuard>,
}

impl TransferServer {
    /// Starts a transfer server without configuration-layer attribution.
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Self::with_conf_and_sources(conf, ConfigSourceMap::new())
    }

    /// Same as [`TransferServer::with_conf`] but keeps the loader's layer map so
    /// `GET /api/v1/transfer/config` can report where each value came from.
    pub fn with_conf_and_sources(
        conf: ClusterConf,
        config_sources: ConfigSourceMap,
    ) -> CommonResult<Self> {
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
        // TransferConf::init always materializes endpoints (explicit config or
        // hostname:rpc_port). These are advertised to workers as progress report
        // targets — never substitute a fresh localhost default here.
        let report_endpoints = conf.transfer.endpoints.clone();
        if report_endpoints.is_empty() {
            return Err(FsError::common(
                "transfer.endpoints is empty after config init; set transfer.hostname / \
                 transfer.endpoints to a worker-reachable host:port",
            )
            .into());
        }
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
        // Lease owners are process-unique. A configured static instance id
        // can be reused by two HA replicas and defeat owner-only fencing.
        let owner = uuid::Uuid::new_v4().to_string();
        // One state object, two consumers: the planner reads the task cap while
        // planning and the scheduler reads the worker ceiling on every dispatch
        // decision; the registry below is its only write path.
        let runtime_state = Arc::new(TransferRuntimeConfig::from_conf(&conf));
        let planner = TransferPlanner::new(
            fs.clone(),
            factory.clone(),
            cache.clone(),
            conf.client.clone(),
            runtime_state.clone(),
            conf.transfer.ufs_max_concurrency_per_endpoint,
        );
        // One cache shared by the scheduler (dispatch) and the service
        // (reports/reads): file-level tasks exist only here.
        let task_cache = Arc::new(TaskCache::new());
        // Discovery topology version shared with report acks. Refreshed by the
        // registration helper below and readable without a MySQL round trip.
        let topology_version = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let scheduler = TransferScheduler::new(
            store.clone(),
            planner,
            cache.clone(),
            factory,
            fs,
            owner,
            report_endpoints,
            conf.transfer.clone(),
            runtime_state.clone(),
            task_cache.clone(),
        );

        let ready = Arc::new(AtomicBool::new(false));
        let runtime_config = Arc::new(build_transfer_runtime_registry(runtime_state, &conf));
        let (registration, registered_version) = register_discovery(&conf, rt.clone());
        topology_version.store(registered_version, Ordering::Relaxed);
        let forwarder = build_forwarder(&conf, rt.clone());
        let mut transfer_service = TransferService::with_cache_and_report_queue(
            store,
            cache.clone(),
            conf.transfer.task_stale_timeout,
            conf.transfer.task_report_queue_size(),
            conf.transfer.worker_threads,
        )?
        .with_task_cache(task_cache.clone());
        transfer_service.with_topology_version(topology_version.clone());
        let service = TransferRpcService::new(
            transfer_service,
            forwarder,
            ready.clone(),
            store_backend,
            cache.clone(),
            runtime_config,
            Arc::new(conf.clone()),
            config_sources,
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
            registration,
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

    pub fn block_on_start(mut self) -> CommonResult<()> {
        let rt = self.rpc_server.clone_rt();
        let registration = self.registration.take();
        rt.block_on(async move {
            let mut status = self.start().await?;
            if let Err(err) = status.wait_stop().await {
                log::warn!("curvine-transfer wait stop failed: {}", err);
            }
            // Best-effort graceful deregistration; a missed one is covered by
            // lease expiry, so this never fails shutdown.
            if let Some(guard) = registration {
                let _ = guard.shutdown().await;
            }
            Ok(())
        })
    }
}

/// Builds the status/watch relay when discovery is on. Without discovery a
/// request is always answered by the instance that received it.
fn build_forwarder(
    conf: &ClusterConf,
    rt: Arc<curvine_runtime::runtime::Runtime>,
) -> Option<Arc<Forwarder>> {
    use curvine_config::TransferDiscovery;
    if conf.transfer.discovery != TransferDiscovery::Sql {
        return None;
    }
    let url = conf.transfer.mysql_store_url();
    if url.is_empty() {
        return None;
    }
    let discovery = curvine_service_discovery::SqlDiscovery::open(url).ok()?;
    let self_addr = format!("{}:{}", conf.transfer.hostname, conf.transfer.rpc_port);
    let factory = Arc::new(curvine_rpc::client::ClientFactory::with_rt(
        conf.client_rpc_conf(),
        rt,
    ));
    Some(Arc::new(Forwarder::new(discovery, self_addr, factory)))
}

/// Discovery-safe instance id for a host:port pair.
fn discovery_instance_id(host: &str, port: u16) -> String {
    let mut id: String = host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    id.push('-');
    id.push_str(&port.to_string());
    id
}

/// Registration guard plus the topology version it observed.
type Registration = (Option<curvine_service_discovery::RegistrationGuard>, i64);

/// Registers this instance in the SQL discovery registry when
/// `transfer.discovery = sql`. Returns none for the static mode.
fn register_discovery(
    conf: &ClusterConf,
    rt: Arc<curvine_runtime::runtime::Runtime>,
) -> Registration {
    use curvine_config::TransferDiscovery;
    if conf.transfer.discovery != TransferDiscovery::Sql {
        return (None, 0);
    }
    let url = conf.transfer.mysql_store_url();
    if url.is_empty() {
        info!("transfer.discovery=sql requires transfer.store_url=mysql://; skipping registration");
        return (None, 0);
    }
    let Some(discovery) = curvine_service_discovery::SqlDiscovery::open(url).ok() else {
        return (None, 0);
    };
    if let Err(err) = discovery.init_schema() {
        info!(
            "transfer discovery schema init failed, skipping registration: {}",
            err
        );
        return (None, 0);
    }
    let Some(kind) = curvine_service_discovery::ServiceKind::try_new("transfer").ok() else {
        return (None, 0);
    };
    let endpoint = curvine_service_discovery::ServiceEndpoint {
        kind: kind.clone(),
        // Discovery ids allow only [A-Za-z0-9_-], so a host:port identity must
        // be sanitized: `10.0.0.1:9010` -> `10-0-0-1-9010`. The registry keeps
        // host and port as their own columns, so nothing else depends on the
        // id's shape.
        id: discovery_instance_id(&conf.transfer.hostname, conf.transfer.rpc_port),
        host: conf.transfer.hostname.clone(),
        rpc_port: conf.transfer.rpc_port,
        web_port: Some(conf.transfer.web_port),
        component_info: curvine_proto::ComponentInfoProto {
            component: Some("transfer".to_string()),
            release_version: Some(curvine_sys::version::GIT_VERSION.to_string()),
            git_commit: None,
            git_tag: None,
            git_branch: None,
            protocol_version: None,
            min_protocol_version: None,
            capabilities: Vec::new(),
        },
        start_time_ms: curvine_runtime::common::LocalTime::mills(),
        status: curvine_service_discovery::ServiceStatus::Serving,
        metadata: None,
    };
    // Register on the server's own runtime: the provider spawns its heartbeat
    // task there, and a temporary runtime would be dropped here, silently
    // stopping the heartbeat so the lease expires seconds after registration.
    use curvine_service_discovery::ServiceRegistry;
    let registered = rt.block_on(async {
        discovery
            .register(
                endpoint,
                curvine_service_discovery::RegistrationOptions::default(),
            )
            .await
    });
    let version = discovery.topology_version().unwrap_or(0);
    match registered {
        Ok(guard) => {
            info!(
                "registered transfer instance in sql discovery: {}:{} topology_version={}",
                conf.transfer.hostname, conf.transfer.rpc_port, version
            );
            (Some(guard), version)
        }
        Err(err) => {
            info!(
                "transfer discovery registration failed, continuing unregistered: {}",
                err
            );
            (None, 0)
        }
    }
}

#[cfg(test)]
mod discovery_id_tests {
    use super::discovery_instance_id;

    /// Discovery ids are `[A-Za-z0-9_-]` only, so an IP or a DNS name with dots
    /// must be sanitized. A raw `host:port` used to fail registration outright.
    #[test]
    fn instance_id_is_discovery_safe() {
        assert_eq!(
            discovery_instance_id("10.119.43.210", 9012),
            "10-119-43-210-9012"
        );
        assert_eq!(
            discovery_instance_id("curvine-transfer-0.svc.cluster.local", 9010),
            "curvine-transfer-0-svc-cluster-local-9010"
        );
        assert_eq!(discovery_instance_id("localhost", 9012), "localhost-9012");
        for id in [
            discovery_instance_id("10.119.43.210", 9012),
            discovery_instance_id("cv_local", 1),
        ] {
            assert!(
                id.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "id {id} must satisfy the discovery id charset"
            );
        }
    }
}
