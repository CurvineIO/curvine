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

use once_cell::sync::OnceCell;

use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonError, CommonResult};
use curvine_data_transfer::transfer::EmbeddedTransfer;
use curvine_fault::FaultHttpControl;
use curvine_net::net::ConnState;
use curvine_raft::raft::storage::RocksLogStorage;
use curvine_rpc::handler::{HandlerService, LimitConf};
use curvine_rpc::server::{ServerMonitor, ServerStateListener};
use curvine_runtime::common::{LocalTime, Logger};
use curvine_runtime::runtime::{AsyncRuntime, GroupExecutor, RpcRuntime, Runtime};
use curvine_web::server::{WebHandlerService, WebServer};
use log::{error, info};

use crate::master::fs::{FsRetryCache, MasterActor, MasterFilesystem};
use crate::master::grpc_handler::{RequestTracker, TrackedStream};
use crate::master::journal::JournalSystem;
use crate::master::master_handler::MasterCompatibility;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::router_handler::MasterRouterHandler;
use crate::master::{MasterGrpcHandler, MasterHandler, MountManager};
use crate::master::{MasterMetrics, MasterMonitor, SyncWorkerManager};

pub static MASTER_METRICS: OnceCell<MasterMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct MasterService {
    conf: ClusterConf,
    fs: MasterFilesystem,
    retry_cache: Option<FsRetryCache>,
    mount_manager: Arc<MountManager>,
    rt: Arc<Runtime>,
    actor_rt: Arc<Runtime>,
    control_rpc_executor: Arc<GroupExecutor>,
    replication_manager: Arc<MasterReplicationManager>,
    limit: LimitConf,
    metrics: &'static MasterMetrics,
    fault_http: FaultHttpControl,
    compatibility: Arc<MasterCompatibility>,
    journal_store: RocksLogStorage,
    embedded_transfer: Option<Arc<EmbeddedTransfer>>,
}

impl MasterService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        conf: ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        mount_manager: Arc<MountManager>,
        rt: Arc<Runtime>,
        replication_manager: Arc<MasterReplicationManager>,
        metrics: &'static MasterMetrics,
        fault_http: FaultHttpControl,
        journal_store: RocksLogStorage,
        embedded_transfer: Option<Arc<EmbeddedTransfer>>,
    ) -> Self {
        let actor_rt = Arc::new(AsyncRuntime::new(
            "master-actor",
            1,
            conf.master.actor_threads,
        ));
        let control_rpc_executor = Arc::new(GroupExecutor::new("master-control-rpc", 2, 1024));
        let limit = LimitConf::new(conf.master.conn_limit, conf.master.global_limit);
        let compatibility = Arc::new(MasterCompatibility::new(&conf));
        Self {
            conf,
            fs,
            retry_cache,
            mount_manager,
            rt,
            actor_rt,
            control_rpc_executor,
            replication_manager,
            limit,
            metrics,
            fault_http,
            compatibility,
            journal_store,
            embedded_transfer,
        }
    }

    pub fn clone_worker_manager(&self) -> SyncWorkerManager {
        self.fs.worker_manager.clone()
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn master_monitor(&self) -> MasterMonitor {
        self.fs.master_monitor.clone()
    }

    pub fn embedded_transfer(&self) -> Option<Arc<EmbeddedTransfer>> {
        self.embedded_transfer.clone()
    }

    pub(crate) fn grpc_message_handler(&self, client_state: Option<ConnState>) -> MasterHandler {
        MasterHandler::new_with_shared(
            &self.conf,
            self.fs.clone(),
            self.retry_cache.clone(),
            client_state,
            self.mount_manager.clone(),
            self.control_rpc_executor.clone(),
            self.replication_manager.clone(),
            self.actor_rt.clone(),
            self.metrics,
            false,
            self.compatibility.clone(),
        )
        .with_embedded_transfer(self.embedded_transfer.clone())
    }
}

impl HandlerService for MasterService {
    type Item = MasterHandler;

    fn has_conn_state(&self) -> bool {
        true
    }

    fn get_message_handler(&self, client_state: Option<ConnState>) -> Self::Item {
        MasterHandler::new_with_shared(
            &self.conf,
            self.fs.clone(),
            self.retry_cache.clone(),
            client_state,
            self.mount_manager.clone(),
            self.control_rpc_executor.clone(),
            self.replication_manager.clone(),
            self.actor_rt.clone(),
            self.metrics,
            true,
            self.compatibility.clone(),
        )
        .with_embedded_transfer(self.embedded_transfer.clone())
    }

    fn is_stream(&self) -> bool {
        !self.conf.master.meta_request_concurrent
    }

    fn get_limit(&self) -> LimitConf {
        self.limit.clone()
    }
}

impl WebHandlerService for MasterService {
    type Item = MasterRouterHandler;

    fn get_handler(&self) -> Self::Item {
        MasterRouterHandler::new(
            self.conf.clone(),
            self.fs.clone(),
            self.metrics,
            self.fault_http.clone(),
            self.journal_store.clone(),
            self.rt.clone(),
        )
    }
}

pub struct Master {
    pub start_time: u64,
    rpc_service: Option<MasterService>,
    web_server: Option<WebServer<MasterService>>,
    journal_system: Option<JournalSystem>,
    actor: MasterActor,
    mount_manager: Arc<MountManager>,
    replication_manager: Arc<MasterReplicationManager>,
    embedded_transfer: Option<Arc<EmbeddedTransfer>>,
}

impl Master {
    fn new(conf: ClusterConf) -> CommonResult<Self> {
        let mut log = conf.master.log.clone();
        if conf.master.audit_logging_enabled {
            log.targets = vec!["audit".to_string()]
        }

        Logger::init(log);
        let fault_http = FaultHttpControl::from_env(&conf.fault_injection)
            .map_err(|error| CommonError::from(error.to_string()))?;
        let metrics = MASTER_METRICS.get_or_try_init(MasterMetrics::new)?;
        info!("allocator: {}", curvine_alloc::allocator_type_name());
        info!("git version: {}", curvine_sys::version::GIT_VERSION);
        conf.print();

        // step1: Create a journal system, the journal system determines how to create a fs dir.
        let journal_system = JournalSystem::from_conf(&conf)?;
        let fs = journal_system.fs();
        let worker_manager = journal_system.worker_manager();
        let mount_manager = journal_system.mount_manager();
        let quota_manager = journal_system.quota_manager();

        let rt = Arc::new(conf.master_server_conf().create_runtime());
        let replication_manager = MasterReplicationManager::new(&fs, &conf, &rt, &worker_manager)?;

        // The embedded transfer service is constructed here but only starts
        // after the master RPC server is listening and leadership is known.
        let embedded_transfer = Self::build_embedded_transfer(&conf, &journal_system)?;

        let actor = MasterActor::new(
            fs.clone(),
            journal_system.master_monitor(),
            conf.master.new_executor(),
            &replication_manager,
            quota_manager,
        );

        // step3: Create rpc server.
        let retry_cache = FsRetryCache::with_conf(&conf.master)?;
        let service = MasterService::new(
            conf.clone(),
            fs,
            retry_cache,
            mount_manager.clone(),
            rt.clone(),
            replication_manager.clone(),
            metrics,
            fault_http,
            journal_system.log_store(),
            embedded_transfer.clone(),
        );

        // step4: Create a web server
        let web_conf = conf.master_web_conf();
        let web_server = WebServer::new(web_conf, service.clone());

        Ok(Self {
            start_time: LocalTime::mills(),
            rpc_service: Some(service),
            web_server: Some(web_server),
            journal_system: Some(journal_system),
            actor,
            mount_manager,
            replication_manager,
            embedded_transfer,
        })
    }

    fn build_embedded_transfer(
        conf: &ClusterConf,
        journal_system: &JournalSystem,
    ) -> CommonResult<Option<Arc<EmbeddedTransfer>>> {
        if !conf.transfer.is_embedded() {
            return Ok(None);
        }
        let monitor = journal_system.master_monitor();
        // Gate the scheduler on the metadata write gate, not raw leadership: a
        // freshly elected leader that has not yet applied an entry of its own
        // term still has a partially replayed namespace, and transfer planning
        // reads that namespace.
        let transfer = EmbeddedTransfer::with_conf(conf, move || monitor.is_serving())?;
        Ok(Some(Arc::new(transfer)))
    }

    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Self::new(conf)
    }

    pub async fn start(&mut self) -> CommonResult<ServerStateListener> {
        // step 1: Start journal_system, raft server and raft node will be started internally
        let journal_system = match self.journal_system.take() {
            Some(journal_system) => journal_system,
            None => return err_box!("master journal system is not initialized"),
        };
        let mut listener = journal_system.start().await?;
        listener.wait_role().await?;

        // step 2: Start rpc server
        let service = match self.rpc_service.take() {
            Some(service) => service,
            None => return err_box!("master rpc server is not initialized"),
        };
        let port = service.conf.master.rpc_port;
        let hostname = std::env::var("ORPC_BIND_HOSTNAME")
            .unwrap_or_else(|_| service.conf.master.hostname.clone());
        let listener = match curvine_net::net::NetUtils::take_held_listener(port) {
            Some(listener) => {
                listener.set_nonblocking(true)?;
                tokio::net::TcpListener::from_std(listener)?
            }
            None => tokio::net::TcpListener::bind((hostname.as_str(), port)).await?,
        };
        let monitor = Arc::new(ServerMonitor::new());
        let rpc_status = monitor.new_listener();

        // step3: Start the web server
        let web_server = match self.web_server.take() {
            Some(web_server) => web_server,
            None => return err_box!("master web server is not initialized"),
        };
        let web_name = web_server.server_name().to_string();
        let bind_addr = web_server.resolve_bind_addr();
        let mut web_status = web_server.start();
        WebServer::<MasterService>::wait_bind(&mut web_status, &web_name, &bind_addr).await?;

        // step4: Start master actor
        self.actor.start()?;

        // reload mount info
        self.mount_manager.restore_best_effort();

        // Start the TTL scheduler after restoring mounts.
        if let Err(e) = self.actor.start_ttl_scheduler() {
            error!("Failed to start inode ttl scheduler: {}", e);
        }

        {
            use curvine_proto::master_service_server::MasterServiceServer;
            use curvine_proto::transfer_service_server::TransferServiceServer;
            let address = listener.local_addr()?;
            let rt = service.clone_rt();
            let metrics = service.metrics;
            let tracker = RequestTracker::default();
            let task_monitor = monitor.clone();
            let conn_limit = if service.conf.master.meta_request_concurrent {
                service.conf.master.conn_limit
            } else {
                1
            };
            let tls = service.conf.master.grpc.server_tls()?;
            let transport = curvine_rpc::grpc::TransportConfig::default();
            let mut grpc_server = transport
                .server(tls.as_ref())?
                .concurrency_limit_per_connection(conn_limit);
            let embedded = self.embedded_transfer.clone();
            let reflection = if service.conf.master.grpc.enable_reflection {
                let mut builder = curvine_rpc::grpc::reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(curvine_proto::FILE_DESCRIPTOR_SET)
                    .with_service_name("proto.MasterService");
                if embedded.is_some() {
                    builder = builder.with_service_name("proto.TransferService");
                }
                Some(builder.build_v1alpha()?)
            } else {
                None
            };
            let handler = Arc::new(MasterGrpcHandler::new(service, rt, tracker.clone()));
            let transfer_service = embedded.as_ref().map(|_| {
                TransferServiceServer::from_arc(handler.clone())
                    .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
            });
            let (health, health_service) = curvine_rpc::grpc::health_reporter();
            health
                .set_serving::<MasterServiceServer<MasterGrpcHandler>>()
                .await;
            if let Some(transfer) = &embedded {
                transfer.start();
                health
                    .set_serving::<TransferServiceServer<MasterGrpcHandler>>()
                    .await;
            }
            monitor.advance_running();
            tokio::spawn(async move {
                let shutdown_tracker = tracker.clone();
                let shutdown_monitor = task_monitor.clone();
                let incoming = futures::stream::unfold(
                    (listener, transport),
                    |(listener, transport)| async move {
                        let accepted = listener.accept().await.and_then(|(stream, _)| {
                            transport.configure_socket(&stream)?;
                            Ok(TrackedStream::new(stream, metrics))
                        });
                        Some((accepted, (listener, transport)))
                    },
                );
                let service = MasterServiceServer::from_arc(handler)
                    .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
                let shutting_down = embedded.clone();
                let result = grpc_server
                    .add_service(health_service)
                    .add_optional_service(reflection)
                    .add_optional_service(transfer_service)
                    .add_service(service)
                    .serve_with_incoming_shutdown(incoming, async move {
                        master_shutdown_signal().await;
                        shutdown_tracker.shutdown();
                        shutdown_monitor.advance_shutdown();
                        if let Some(transfer) = shutting_down {
                            transfer.shutdown();
                            health
                                .set_not_serving::<TransferServiceServer<MasterGrpcHandler>>()
                                .await;
                        }
                        health
                            .set_not_serving::<MasterServiceServer<MasterGrpcHandler>>()
                            .await;
                    })
                    .await;
                if let Err(error) = result {
                    error!("master gRPC server failed: {}", error);
                }
                if let Some(transfer) = embedded {
                    transfer.shutdown();
                }
                task_monitor.advance_shutdown();
                tracker.shutdown();
                tracker.wait_drained().await;
                task_monitor.advance_stop();
            });
            info!("master gRPC listener started on {}", address);
        }

        Ok(rpc_status)
    }

    pub fn block_on_start(mut self) -> CommonResult<()> {
        let rt = match self.rpc_service.as_ref() {
            Some(service) => service.clone_rt(),
            None => return err_box!("master rpc server is not initialized"),
        };
        let mut status = rt.block_on(async { self.start().await })?;
        rt.block_on(async { status.wait_stop().await })
    }

    pub fn get_metrics<'a>() -> CommonResult<&'a MasterMetrics> {
        MASTER_METRICS.get_or_try_init(MasterMetrics::new)
    }

    // Instantiate metrics during testing
    pub fn init_test_metrics() {
        let _ = Self::get_metrics();
    }

    /// Serialize the tests that publish through the process-global
    /// `MASTER_METRICS` (#1885 review).
    ///
    /// Session metrics are deliberately a singleton, so a test that asserts an
    /// exact counter delta (session reaper) can be blurred by any other test's
    /// heartbeat or reap landing between its reads. Every test that publishes
    /// (heartbeat handlers, reaper ticks) takes this guard; the session-reap
    /// tests take it for their whole body.
    #[cfg(test)]
    pub(crate) fn metric_test_serial() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};
        static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
        SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // for test
    pub fn get_fs(&self) -> MasterFilesystem {
        self.rpc_service
            .as_ref()
            .expect("master rpc server must be initialized")
            .fs
            .clone()
    }

    // for test
    pub fn get_replication_manager(&self) -> Arc<MasterReplicationManager> {
        self.replication_manager.clone()
    }

    pub fn service(&self) -> &MasterService {
        self.rpc_service
            .as_ref()
            .expect("master rpc server must be initialized")
    }

    /// Embedded transfer service, when this master is configured for it.
    pub fn embedded_transfer(&self) -> Option<Arc<EmbeddedTransfer>> {
        self.embedded_transfer.clone()
    }
}

async fn master_shutdown_signal() {
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
                error!("failed to register SIGTERM handler: {}", error);
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::{JournalConf, TransferMode};
    use curvine_runtime::common::Utils;

    fn test_conf(name: &str) -> ClusterConf {
        Master::init_test_metrics();
        let mut conf = ClusterConf {
            testing: true,
            format_master: true,
            ..Default::default()
        };
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "embedded-transfer-{name}/meta-{}",
            Utils::rand_str(6)
        ));
        conf.journal = JournalConf::with_test();
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "embedded-transfer-{name}/journal-{}",
            Utils::rand_str(6)
        ));
        conf
    }

    #[test]
    fn master_embeds_transfer_by_default() {
        let master = Master::with_conf(test_conf("default")).unwrap();

        let transfer = master
            .embedded_transfer()
            .expect("master must embed transfer when no endpoints are configured");
        assert_eq!(transfer.store_backend_label(), "memory");
    }

    #[test]
    fn master_does_not_embed_transfer_in_standalone_mode() {
        let mut conf = test_conf("standalone");
        conf.transfer.mode = Some(TransferMode::Standalone);
        conf.transfer.endpoints = vec!["transfer-01:9010".to_string()];

        let master = Master::with_conf(conf).unwrap();

        assert!(master.embedded_transfer().is_none());
    }

    #[test]
    fn embedded_transfer_rpcs_run_on_the_transfer_runtime() {
        use curvine_fs_api::RpcCode;
        use curvine_rpc::handler::MessageHandler;
        use curvine_rpc::message::Builder;

        let master = Master::with_conf(test_conf("transfer-rpc-rt")).unwrap();
        let handler = master.service().get_message_handler(None);
        let transfer = master
            .embedded_transfer()
            .expect("master must embed transfer by default");

        // Transfer RPCs loop back to this same master for metadata while they
        // hold a blocking thread, so they must not use the master pool.
        let submit = Builder::new_rpc(RpcCode::SubmitTransfer as i8).build();
        let rt = handler
            .get_rt(&submit)
            .expect("a transfer RPC must be routed to a runtime");
        assert!(std::ptr::eq(rt, transfer.runtime()));

        // Metadata RPCs stay on the master runtime: that is the pool that has
        // to be free to serve the loopback above.
        let status = Builder::new_rpc(RpcCode::FileStatus as i8).build();
        assert!(handler.get_rt(&status).is_none());
    }

    #[test]
    fn embedded_transfer_shutdown_is_idempotent() {
        let master = Master::with_conf(test_conf("shutdown-hook")).unwrap();
        let transfer = master
            .embedded_transfer()
            .expect("master must embed transfer by default");

        // The RPC server shutdown hook may run more than once when several
        // server states are torn down; stopping must stay harmless.
        transfer.shutdown();
        transfer.shutdown();
    }
}
