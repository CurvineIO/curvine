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

use crate::worker::block::{BlockActor, BlockStore};
use crate::worker::grpc_data::{GrpcBlockDataService, StreamTracker};
use crate::worker::handler::{ConnectionPeer, WorkerHandler, WorkerRouterHandler};
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::replication::worker_replication_manager::WorkerReplicationManager;
use crate::worker::task::TaskManager;
use crate::worker::{WorkerControlGrpcHandler, WorkerMetrics};
use curvine_config::ClusterConf;
use curvine_core_error::StringError;
use curvine_core_error::{CommonError, CommonResult};
use curvine_fault::FaultHttpControl;
use curvine_model::{CompatibilityPolicy, HeartbeatStatus, WorkerAddress};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::block_data_service_server::BlockDataServiceServer;
use curvine_proto::worker_control_service_server::WorkerControlServiceServer;
use curvine_rpc::handler::HandlerService;
use curvine_rpc::server::{ServerMonitor, ServerStateListener};
use curvine_runtime::common::Utils;
use curvine_runtime::common::{LocalTime, Logger};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastMutex;
#[cfg(feature = "spdk")]
use curvine_storage_spdk::SpdkEnv;
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static CLUSTER_CONF: OnceCell<ClusterConf> = OnceCell::new();

static WORKER_METRICS: OnceCell<WorkerMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct WorkerService {
    store: BlockStore,
    conf: ClusterConf,
    task_manager: Arc<TaskManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<WorkerReplicationManager>,
    fault_http: FaultHttpControl,
    /// Compatibility policy applied to client peers on data-plane connections.
    /// Built once from `worker.compatibility` config (diagnose by default) and
    /// shared by every per-connection `WorkerHandler` through an `Arc`, so
    /// handlers share the policy without deep-copying it per connection.
    compatibility_policy: Arc<CompatibilityPolicy>,
}

impl WorkerService {
    pub fn with_conf(
        conf: &ClusterConf,
        rt: Arc<Runtime>,
        worker_session_id: String,
    ) -> CommonResult<Self> {
        let fault_http = FaultHttpControl::from_env(&conf.fault_injection)
            .map_err(|error| CommonError::from(error.to_string()))?;
        let store: BlockStore = BlockStore::new(&conf.cluster_id, conf)?;

        let task_manager = TaskManager::with_rt(rt.clone(), conf, worker_session_id.clone())?;

        let replication_manager = WorkerReplicationManager::new(
            &store,
            &rt,
            conf,
            &task_manager.get_fs_context(),
            worker_session_id,
        );

        let ws = Self {
            store,
            conf: conf.clone(),
            task_manager: Arc::new(task_manager),
            rt,
            replication_manager,
            fault_http,
            compatibility_policy: Arc::new(conf.worker.compatibility.to_policy()),
        };
        Ok(ws)
    }

    pub fn clone_rt(&self) -> Arc<Runtime> {
        self.rt.clone()
    }

    pub fn conf(&self) -> &ClusterConf {
        &self.conf
    }
}

impl HandlerService for WorkerService {
    type Item = WorkerHandler;

    fn has_conn_state(&self) -> bool {
        true
    }

    fn get_message_handler(&self, conn_info: Option<ConnState>) -> Self::Item {
        let client_addr = conn_info
            .map(|state| state.remote_addr.to_string())
            .unwrap_or_default();
        WorkerHandler {
            store: self.store.clone(),
            handler: FastMutex::new(None),
            connection_peer: FastMutex::new(ConnectionPeer::Unknown),
            task_manager: self.task_manager.clone(),
            rt: self.rt.clone(),
            replication_handler: WorkerReplicationHandler::new(&self.replication_manager),
            compatibility_policy: self.compatibility_policy.clone(),
            client_addr,
            buffered_reads: false,
        }
    }
}

impl WebHandlerService for WorkerService {
    type Item = WorkerRouterHandler;

    fn get_handler(&self) -> Self::Item {
        WorkerRouterHandler {
            fault_http: self.fault_http.clone(),
        }
    }
}

// block data start service.
pub struct Worker {
    pub start_ms: u64,
    pub worker_id: u32,
    pub addr: WorkerAddress,
    rt: Arc<Runtime>,
    service: WorkerService,
    monitor: Arc<ServerMonitor>,
    tracker: StreamTracker,
    registered: Arc<AtomicBool>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    web_server: Option<WebServer<WorkerService>>,
    block_actor: BlockActor,
}

impl Worker {
    #[cfg_attr(not(feature = "spdk"), allow(unused_mut))]
    pub fn with_conf(mut conf: ClusterConf) -> CommonResult<Self> {
        Logger::init(conf.worker.log.clone());

        // Init SPDK before WorkerService - enables BlockMeta to open SPDK bdevs
        #[cfg(feature = "spdk")]
        if conf.worker.spdk_disk.enabled {
            conf.worker.spdk_disk.init()?;
            use curvine_config::WorkerDataDir;
            use curvine_model::StorageType;
            use log::warn;
            info!("SPDK enabled — initializing global SPDK environment");
            match SpdkEnv::init_global(conf.worker.spdk_disk.clone()) {
                Ok(env) => {
                    info!(
                        "SPDK environment ready: {} bdev(s), total capacity {}",
                        env.active_bdev_count(),
                        env.total_capacity()
                    );
                    // Validate: each data_dir needs one bdev (dir_id % num_bdevs)
                    let num_spdk_dirs = conf
                        .worker
                        .data_dir
                        .iter()
                        .filter(|d| {
                            WorkerDataDir::from_str(d)
                                .map(|dd| dd.storage_type == StorageType::SpdkDisk)
                                .unwrap_or(false)
                        })
                        .count();
                    let num_bdevs = env.active_bdev_count();
                    if num_spdk_dirs > num_bdevs {
                        return curvine_core_error::err_box!(
                            "Configuration has {} SPDK data_dir entries but only {} bdev(s) \
                             were discovered. Multiple dirs would map to the same NVMe \
                             namespace, causing data corruption. Either reduce SPDK data_dir \
                             entries or add more NVMe-oF targets.",
                            num_spdk_dirs,
                            num_bdevs
                        );
                    }
                    if num_spdk_dirs > 0 && num_spdk_dirs < num_bdevs {
                        warn!(
                            "{} SPDK data_dir(s) configured but {} bdev(s) discovered. \
                             {} bdev(s) will be unused.",
                            num_spdk_dirs,
                            num_bdevs,
                            num_bdevs - num_spdk_dirs
                        );
                    }
                }
                Err(e) => {
                    // SPDK init failure is fatal — blocks on SPDK dirs would be inaccessible.
                    return Err(e);
                }
            }
        }
        #[cfg(not(feature = "spdk"))]
        {
            if conf.worker.spdk_disk.enabled {
                return curvine_core_error::err_box!(
                    "SPDK is not enabled. Compile with --features spdk to use SPDK"
                );
            }
            info!("SPDK disabled (not compiled)");
        }

        let rt = Arc::new(conf.worker_server_conf().create_runtime());
        let worker_session_id = Utils::uuid();
        let service: WorkerService =
            WorkerService::with_conf(&conf, rt.clone(), worker_session_id.clone())?;
        let worker_id = service.store.worker_id()?;

        CLUSTER_CONF.get_or_init(|| conf.clone());
        WORKER_METRICS.get_or_try_init(|| WorkerMetrics::new(service.store.clone()))?;
        info!("allocator: {}", curvine_alloc::allocator_type_name());
        info!("git version: {}", curvine_sys::version::GIT_VERSION);
        conf.print();

        let block_store = service.store.clone();
        let monitor = Arc::new(ServerMonitor::new());

        let web_server = WebServer::with_rt(rt.clone(), conf.worker_web_conf(), service.clone());

        let addr = WorkerAddress {
            worker_id,
            hostname: conf.worker.hostname.clone(),
            ip_addr: conf.worker.hostname.clone(),
            rpc_port: conf.worker.rpc_port as u32,
            web_port: conf.worker.web_port as u32,
        };
        let start_ms = LocalTime::mills();
        let block_actor = BlockActor::new(
            rt.clone(),
            &conf,
            addr.clone(),
            worker_session_id,
            block_store.clone(),
            monitor.read_ctl(),
            start_ms,
        )?;

        let master_client = block_actor.client.clone();
        service
            .replication_manager
            .with_master_client(master_client.clone());

        let worker = Self {
            start_ms,
            worker_id,
            addr,
            rt,
            service,
            monitor,
            tracker: StreamTracker::default(),
            registered: Arc::new(AtomicBool::new(false)),
            shutdown: None,
            web_server: Some(web_server),
            block_actor,
        };

        Ok(worker)
    }

    /// Bind the advertised RPC port and start the tonic worker services.
    async fn start_tonic(&mut self, unregister: bool) -> CommonResult<ServerStateListener> {
        if self.shutdown.is_some() {
            return curvine_core_error::err_box!("worker server already started");
        }
        let grpc_config = &self.service.conf.worker.grpc;
        let tls = grpc_config.server_tls()?;
        let mut grpc_server = curvine_rpc::grpc::TransportConfig::default().server(tls.as_ref())?;
        let reflection = if grpc_config.enable_reflection {
            Some(
                curvine_rpc::grpc::reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(curvine_proto::FILE_DESCRIPTOR_SET)
                    .with_service_name("proto.WorkerControlService")
                    .with_service_name("proto.BlockDataService")
                    .build()?,
            )
        } else {
            None
        };
        let port = self.addr.rpc_port as u16;
        let hostname =
            std::env::var("ORPC_BIND_HOSTNAME").unwrap_or_else(|_| self.addr.hostname.clone());
        let held_listener = NetUtils::take_held_listener(port);
        let listener = self
            .rt
            .spawn(async move {
                if let Some(listener) = held_listener {
                    listener.set_nonblocking(true)?;
                    tokio::net::TcpListener::from_std(listener)
                } else {
                    tokio::net::TcpListener::bind((hostname.as_str(), port)).await
                }
            })
            .await
            .map_err(|error| CommonError::from(error.to_string()))??;
        let address = listener.local_addr()?;
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        self.shutdown = Some(stop_tx);
        let status = self.monitor.new_listener();
        let monitor = self.monitor.clone();
        let tracker = self.tracker.clone();
        let service = self.service.clone();
        let master_client = self.block_actor.client.clone();
        let registered = self.registered.clone();
        let spdk_enabled = self.service.conf.worker.spdk_disk.enabled;
        let (mut health, health_service) = curvine_rpc::grpc::health_reporter();
        health
            .set_serving::<BlockDataServiceServer<GrpcBlockDataService>>()
            .await;
        health
            .set_serving::<WorkerControlServiceServer<WorkerControlGrpcHandler>>()
            .await;
        let data_service = BlockDataServiceServer::new(GrpcBlockDataService::new(
            service.clone(),
            tracker.clone(),
        ))
        .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
        let control_service = WorkerControlServiceServer::new(WorkerControlGrpcHandler::new(
            service,
            tracker.clone(),
        ))
        .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
        let shutdown_monitor = monitor.clone();
        let shutdown_tracker = tracker.clone();
        self.rt.spawn(async move {
            let incoming = futures::stream::unfold(listener, |listener| async move {
                let accepted = listener.accept().await.and_then(|(stream, _)| {
                    stream.set_nodelay(true)?;
                    Ok(stream)
                });
                Some((accepted, listener))
            });
            let shutdown = async move {
                #[cfg(unix)]
                {
                    let mut terminate =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .expect("register SIGTERM");
                    tokio::select! {
                        _ = stop_rx => {},
                        _ = tokio::signal::ctrl_c() => {},
                        _ = terminate.recv() => {},
                    }
                }
                #[cfg(not(unix))]
                tokio::select! {
                    _ = stop_rx => {},
                    _ = tokio::signal::ctrl_c() => {},
                }
                shutdown_monitor.advance_shutdown();
                shutdown_tracker.shutdown();
                health
                    .set_not_serving::<BlockDataServiceServer<GrpcBlockDataService>>()
                    .await;
                health
                    .set_not_serving::<WorkerControlServiceServer<WorkerControlGrpcHandler>>()
                    .await;
            };
            let result = grpc_server
                .add_service(health_service)
                .add_optional_service(reflection)
                .add_service(data_service)
                .add_service(control_service)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await;
            if let Err(error) = result {
                log::error!("worker gRPC server failed: {}", error);
            }
            monitor.advance_shutdown();
            tracker.shutdown();
            tracker.wait_drained().await;
            // Keep synchronous heartbeat and storage shutdown off the I/O runtime.
            let _ = tokio::task::spawn_blocking(move || {
                if unregister && registered.load(Ordering::Acquire) {
                    if let Err(error) = master_client.heartbeat(HeartbeatStatus::End, vec![]) {
                        info!("error unregister {}", error);
                    }
                }
                if spdk_enabled {
                    info!("Shutting down SPDK environment");
                    #[cfg(feature = "spdk")]
                    curvine_storage_spdk::SpdkEnv::shutdown_global();
                }
            })
            .await;
            monitor.advance_stop();
        });
        self.monitor.advance_running();
        info!("worker gRPC listener started on {}", address);
        Ok(status)
    }

    pub async fn start(&mut self) -> CommonResult<ServerStateListener> {
        let mut status = self.start_tonic(true).await?;
        let result = async {
            let web_server = self
                .web_server
                .take()
                .expect("worker web server must be initialized before startup");
            let web_name = web_server.server_name().to_string();
            let bind_addr = web_server.resolve_bind_addr();
            let mut web_status = web_server.start();
            WebServer::<WorkerService>::wait_bind(&mut web_status, &web_name, &bind_addr).await?;

            let block_actor = self.block_actor.clone();
            tokio::task::spawn_blocking(move || block_actor.start())
                .await
                .map_err(|_| {
                    CommonError::from(StringError::from(
                        "worker block actor startup thread panicked",
                    ))
                })
                .and_then(|result| result)?;
            self.registered.store(true, Ordering::Release);
            Ok::<_, CommonError>(())
        }
        .await;
        if let Err(error) = result {
            self.shutdown.take();
            let _ = status.wait_stop().await;
            return Err(error);
        }
        Ok(status)
    }

    pub fn block_on_start(mut self) -> CommonResult<()> {
        let rt = self.rt.clone();
        let mut status = rt.block_on(async { self.start().await })?;
        rt.block_on(async { status.wait_stop().await })
    }

    /// Start only the worker RPC listener, without registration or the web server.
    pub fn start_standalone(mut self) {
        let rt = self.rt.clone();
        match rt.block_on(async { self.start_tonic(false).await }) {
            Ok(mut status) => {
                let _ = rt.block_on(async { status.wait_stop().await });
            }
            Err(error) => log::error!("worker gRPC startup failed: {}", error),
        }
    }

    pub fn get_conf<'a>() -> CommonResult<&'a ClusterConf> {
        CLUSTER_CONF.get().ok_or_else(|| {
            curvine_core_error::CommonError::from("worker cluster config is not initialized")
        })
    }

    pub fn get_metrics<'a>() -> CommonResult<&'a WorkerMetrics> {
        WORKER_METRICS.get().ok_or_else(|| {
            curvine_core_error::CommonError::from("worker metrics are not initialized")
        })
    }

    pub fn service(&self) -> &WorkerService {
        &self.service
    }
}
