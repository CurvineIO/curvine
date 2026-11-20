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

use crate::worker::block::{BlockActor, BlockActorConfig, BlockStore};
use crate::worker::control_grpc::ControlTracker;
use crate::worker::handler::{ConnectionPeer, WorkerHandler, WorkerRouterHandler};
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::replication::worker_replication_manager::WorkerReplicationManager;
use crate::worker::task::TaskManager;
use crate::worker::{WorkerControlGrpcHandler, WorkerMetrics};
use curvine_config::ClusterConf;
use curvine_core_error::StringError;
use curvine_core_error::{CommonError, CommonResult};
use curvine_fault::FaultHttpControl;
use curvine_model::{BlockReportInfo, CompatibilityPolicy, HeartbeatStatus, WorkerAddress};
use curvine_net::net::{ConnState, NetUtils};
use curvine_proto::worker_control_service_server::WorkerControlServiceServer;
use curvine_rpc::handler::HandlerService;
use curvine_rpc::server::{RpcServer, ServerStateListener};
use curvine_runtime::common::Utils;
use curvine_runtime::common::{LocalTime, Logger};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime, Runtime};
use curvine_runtime::sync::FastMutex;
#[cfg(feature = "spdk")]
use curvine_storage_spdk::SpdkEnv;
use curvine_web::server::{WebHandlerService, WebServer};
use dashmap::DashMap;
use log::info;
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

static CLUSTER_CONF: OnceCell<ClusterConf> = OnceCell::new();

static WORKER_METRICS: OnceCell<WorkerMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct WorkerService {
    store: BlockStore,
    conf: ClusterConf,
    task_manager: Arc<TaskManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<WorkerReplicationManager>,
    report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    fault_http: FaultHttpControl,
    /// Compatibility policy applied to client peers on data-plane connections.
    /// Built once from `worker.compatibility` config (diagnose by default) and
    /// shared by every per-connection `WorkerHandler` through an `Arc`, so
    /// handlers share the policy without deep-copying it per connection.
    compatibility_policy: Arc<CompatibilityPolicy>,
}

impl WorkerService {
    /// Runtime used by the data plane (load / export / replication tasks).
    ///
    /// `worker.transfer_io_threads = 0` keeps the legacy behaviour of running
    /// transfer tasks on the RPC runtime; any other value gives them a private
    /// runtime so blocking block I/O cannot stall request handling.
    fn transfer_runtime(conf: &ClusterConf, rpc_rt: &Arc<Runtime>) -> Arc<Runtime> {
        let io_threads = conf.worker.transfer_io_threads;
        if io_threads == 0 {
            info!("transfer tasks share the worker rpc runtime (transfer_io_threads = 0)");
            return rpc_rt.clone();
        }

        info!(
            "worker transfer runtime: {} async threads, {} blocking threads",
            io_threads, conf.worker.transfer_blocking_threads
        );
        Arc::new(AsyncRuntime::new(
            format!("{}-transfer", conf.cluster_id),
            io_threads,
            conf.worker.transfer_blocking_threads,
        ))
    }

    pub fn with_conf(
        conf: &ClusterConf,
        rt: Arc<Runtime>,
        worker_session_id: String,
    ) -> CommonResult<Self> {
        let fault_http = FaultHttpControl::from_env(&conf.fault_injection)
            .map_err(|error| CommonError::from(error.to_string()))?;
        let store: BlockStore = BlockStore::new(&conf.cluster_id, conf)?;
        let report_blocks = Arc::new(DashMap::new());

        // Data-plane runtime. Load / export tasks do blocking block I/O
        // (std::fs open / write / flush); when they share the RPC runtime they
        // occupy its async threads and the worker stops reading new requests,
        // so a submit only fails once the master's rpc timeout expires.
        let transfer_rt = Self::transfer_runtime(conf, &rt);

        let task_manager =
            TaskManager::with_rt(transfer_rt.clone(), conf, worker_session_id.clone())?;

        let replication_manager = WorkerReplicationManager::new(
            &store,
            &transfer_rt,
            conf,
            &task_manager.get_fs_context(),
            worker_session_id,
            report_blocks.clone(),
        );

        let ws = Self {
            store,
            conf: conf.clone(),
            task_manager: Arc::new(task_manager),
            rt,
            replication_manager,
            report_blocks,
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
    rpc_server: Option<RpcServer<WorkerService>>,
    web_server: Option<WebServer<WorkerService>>,
    block_actor: BlockActor,
}

impl Worker {
    #[cfg_attr(not(feature = "spdk"), allow(unused_mut))]
    pub fn with_conf(mut conf: ClusterConf) -> CommonResult<Self> {
        conf.worker.validate_follower_reports()?;
        if conf.worker.control_rpc_port == 0 {
            return curvine_core_error::err_box!("worker control_rpc_port must be nonzero");
        }
        if conf.worker.rpc_port == conf.worker.control_rpc_port {
            return curvine_core_error::err_box!(
                "worker rpc_port and control_rpc_port must be different"
            );
        }
        if conf.worker.web_port == conf.worker.control_rpc_port {
            return curvine_core_error::err_box!(
                "worker web_port and control_rpc_port must be different"
            );
        }

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
        let rpc_server = RpcServer::with_rt(rt.clone(), conf.worker_server_conf(), service.clone());

        let web_server = WebServer::with_rt(rt.clone(), conf.worker_web_conf(), service.clone());

        let net_addr = rpc_server.bind_addr();
        let addr = WorkerAddress {
            worker_id,
            hostname: net_addr.hostname.to_owned(),
            ip_addr: net_addr.hostname.to_owned(),
            rpc_port: net_addr.port as u32,
            control_rpc_port: Some(conf.worker.control_rpc_port as u32),
            web_port: conf.worker.web_port as u32,
        };
        let start_ms = LocalTime::mills();
        let registered = Arc::new(AtomicBool::new(false));
        let startup_guard = Arc::new(std::sync::Mutex::new(()));
        let block_actor = BlockActor::new(
            rt.clone(),
            &conf,
            block_store.clone(),
            BlockActorConfig {
                worker_addr: addr.clone(),
                worker_session_id: worker_session_id.clone(),
                worker_ctl: rpc_server.new_state_ctl(),
                startup_time_ms: start_ms,
                report_blocks: service.report_blocks.clone(),
                registered: registered.clone(),
                startup_guard: startup_guard.clone(),
            },
        )?;

        let master_client = block_actor.client.clone();
        service
            .replication_manager
            .with_master_client(master_client.clone());

        let registered_for_shutdown = registered.clone();
        let spdk_enabled = conf.worker.spdk_disk.enabled;
        rpc_server.add_shutdown_hook(move || {
            let _startup = startup_guard
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if registered_for_shutdown.load(Ordering::Acquire) {
                if let Err(e) = master_client.heartbeat(HeartbeatStatus::End, vec![]) {
                    info!("error unregister {}", e)
                }
            }
            if spdk_enabled {
                info!("Shutting down SPDK environment");
                #[cfg(feature = "spdk")]
                curvine_storage_spdk::SpdkEnv::shutdown_global();
            }
        });

        let worker = Self {
            start_ms,
            worker_id,
            addr,
            rpc_server: Some(rpc_server),
            web_server: Some(web_server),
            block_actor,
        };

        Ok(worker)
    }

    /// Bind control before the ORPC listener, and couple their shutdown paths.
    async fn start_rpc_only(&mut self) -> CommonResult<ServerStateListener> {
        let server = self
            .rpc_server
            .as_ref()
            .expect("worker rpc server must be initialized before startup");
        let rt = server.clone_rt();
        let service = server.service().clone();
        let stop_orpc = server.shutdown_handle();
        let setup = async {
            let grpc_config = &service.conf.worker.grpc;
            let tls = grpc_config.server_tls()?;
            let transport = curvine_rpc::grpc::TransportConfig::default();
            let grpc_server = transport.server(tls.as_ref())?;
            let reflection = if grpc_config.enable_reflection {
                Some(
                    curvine_rpc::grpc::reflection::server::Builder::configure()
                        .register_encoded_file_descriptor_set(curvine_proto::FILE_DESCRIPTOR_SET)
                        .with_service_name("proto.WorkerControlService")
                        .build_v1alpha()?,
                )
            } else {
                None
            };
            let port = service.conf.worker.control_rpc_port;
            let hostname =
                std::env::var("ORPC_BIND_HOSTNAME").unwrap_or_else(|_| self.addr.hostname.clone());
            let held_listener = NetUtils::take_held_listener(port);
            let listener = rt
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
            Ok::<_, CommonError>((transport, grpc_server, reflection, listener, address))
        }
        .await;
        let (transport, mut grpc_server, reflection, listener, address) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                // Run the normal cleanup hooks without ever binding the data
                // listener or starting the web server.
                stop_orpc.send_replace(true);
                let rpc_server = self
                    .rpc_server
                    .take()
                    .expect("worker rpc server initialized");
                let mut status = rpc_server.start();
                let _ = status.wait_stop().await;
                return Err(error);
            }
        };
        let mut stop_rx = stop_orpc.subscribe();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let done_rx = std::sync::Mutex::new(done_rx);
        let tracker = ControlTracker::default();
        let ready_tracker = tracker.clone();
        let (health, health_service) = curvine_rpc::grpc::health_reporter();
        health
            .set_not_serving::<WorkerControlServiceServer<WorkerControlGrpcHandler>>()
            .await;
        health
            .set_service_status("", curvine_rpc::grpc::ServingStatus::NotServing)
            .await;
        let control = WorkerControlServiceServer::new(WorkerControlGrpcHandler::new(
            service,
            tracker.clone(),
        ))
        .max_decoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(curvine_rpc::grpc::MAX_MESSAGE_BYTES);
        let stop_tracker = tracker.clone();
        rt.spawn(async move {
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
            let shutdown = async move {
                let became_ready = tokio::select! {
                    biased;
                    _ = async { let _ = stop_rx.wait_for(|requested| *requested).await; } => false,
                    ready = ready_rx => ready.is_ok(),
                };
                if became_ready {
                    health
                        .set_serving::<WorkerControlServiceServer<WorkerControlGrpcHandler>>()
                        .await;
                    health
                        .set_service_status("", curvine_rpc::grpc::ServingStatus::Serving)
                        .await;
                    let _ = stop_rx.wait_for(|requested| *requested).await;
                }
                stop_tracker.shutdown();
                health
                    .set_not_serving::<WorkerControlServiceServer<WorkerControlGrpcHandler>>()
                    .await;
                health
                    .set_service_status("", curvine_rpc::grpc::ServingStatus::NotServing)
                    .await;
            };
            let result = grpc_server
                .add_service(health_service)
                .add_optional_service(reflection)
                .add_service(control)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await;
            if let Err(error) = result {
                log::error!("worker control gRPC server failed: {}", error);
            }
            stop_orpc.send_replace(true);
            tracker.shutdown();
            tracker.wait_drained().await;
            let _ = done_tx.send(());
        });
        // The server signals gRPC shutdown before draining ORPC requests;
        // its hooks wait for the control drain before unregistering or closing storage.
        server.add_shutdown_hook(move || {
            let _ = done_rx.lock().expect("control shutdown poisoned").recv();
        });
        info!("worker control gRPC listener started on {}", address);
        let rpc_server = self
            .rpc_server
            .take()
            .expect("worker rpc server must be initialized before startup");
        let mut rpc_status = rpc_server.start();
        if let Err(error) = rpc_status.wait_startup().await {
            let _ = rpc_status.wait_stop().await;
            return Err(error);
        }
        ready_tracker.start_serving();
        let _ = ready_tx.send(());
        Ok(rpc_status)
    }

    pub async fn start(&mut self) -> CommonResult<ServerStateListener> {
        let stop_orpc = self
            .rpc_server
            .as_ref()
            .expect("worker rpc server must be initialized before startup")
            .shutdown_handle();
        let mut rpc_status = self.start_rpc_only().await?;
        let startup = async {
            // Register and report blocks before exposing the web listener.
            let block_actor = self.block_actor.clone();
            let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
            thread::spawn(move || {
                let _ = startup_tx.send(block_actor.start());
            });
            // The actor's synchronous client needs this same I/O runtime. Joining
            // its thread here blocks startup forever with a single I/O worker.
            startup_rx
                .await
                .map_err(|_| {
                    CommonError::from(StringError::from(
                        "worker block actor startup thread panicked",
                    ))
                })
                .and_then(|result| result)?;
            if *stop_orpc.borrow() {
                return curvine_core_error::err_box!("worker stopped during startup");
            }

            let web_server = self
                .web_server
                .take()
                .expect("worker web server must be initialized before startup");
            let web_name = web_server.server_name().to_string();
            let bind_addr = web_server.resolve_bind_addr();
            let mut web_status = web_server.start();
            WebServer::<WorkerService>::wait_bind(&mut web_status, &web_name, &bind_addr).await?;
            Ok::<(), CommonError>(())
        }
        .await;
        if let Err(error) = startup {
            stop_orpc.send_replace(true);
            let _ = rpc_status.wait_stop().await;
            return Err(error);
        }

        Ok(rpc_status)
    }

    pub fn block_on_start(mut self) -> CommonResult<()> {
        let rt = self
            .rpc_server
            .as_ref()
            .expect("worker rpc server must be initialized before startup")
            .clone_rt();

        let mut rpc_status = rt.block_on(async { self.start().await })?;
        rt.block_on(async { rpc_status.wait_stop().await })
    }

    // Start a standalone worker.
    pub fn start_standalone(mut self) {
        let rt = self
            .rpc_server
            .as_ref()
            .expect("worker rpc server must be initialized")
            .clone_rt();
        match rt.block_on(self.start_rpc_only()) {
            Ok(mut status) => {
                let _ = rt.block_on(status.wait_stop());
            }
            Err(error) => log::error!("worker RPC startup failed: {}", error),
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
        self.rpc_server
            .as_ref()
            .expect("worker rpc server must be initialized")
            .service()
    }
}

#[cfg(test)]
mod transfer_runtime_tests {
    use super::*;

    fn rpc_runtime(conf: &ClusterConf) -> Arc<Runtime> {
        Arc::new(conf.worker_server_conf().create_runtime())
    }

    #[test]
    fn zero_keeps_sharing_the_rpc_runtime() {
        let mut conf = ClusterConf::default();
        conf.worker.transfer_io_threads = 0;
        let rpc = rpc_runtime(&conf);

        let rt = WorkerService::transfer_runtime(&conf, &rpc);

        assert!(Arc::ptr_eq(&rt, &rpc));
    }

    #[test]
    fn configured_threads_build_a_private_runtime() {
        let mut conf = ClusterConf::default();
        conf.worker.transfer_io_threads = 4;
        conf.worker.transfer_blocking_threads = 16;
        let rpc = rpc_runtime(&conf);

        let rt = WorkerService::transfer_runtime(&conf, &rpc);

        assert!(!Arc::ptr_eq(&rt, &rpc));
        assert_eq!(rt.io_threads(), 4);
        assert_eq!(rt.worker_threads(), 16);
    }

    /// `io_threads()` / `worker_threads()` echo the stored config, so also pin
    /// the pool that was actually built: the blocking cap is the property the
    /// isolation depends on.
    #[test]
    fn blocking_pool_cap_matches_the_config() {
        let mut conf = ClusterConf::default();
        conf.worker.transfer_io_threads = 2;
        conf.worker.transfer_blocking_threads = 2;
        let rpc = rpc_runtime(&conf);

        let rt = WorkerService::transfer_runtime(&conf, &rpc);

        let live = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..6 {
            let live = live.clone();
            let peak = peak.clone();
            handles.push(rt.spawn_blocking(move || {
                use std::sync::atomic::Ordering;
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(100));
                live.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        rt.block_on(async {
            for handle in handles {
                let _ = handle.await;
            }
        });

        assert_eq!(
            peak.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "blocking pool must honour transfer_blocking_threads"
        );
    }
}

#[cfg(test)]
mod hybrid_listener_tests {
    use super::*;
    use std::net::TcpListener;
    use std::time::Duration;

    fn conf(data_port: u16, control_port: u16) -> ClusterConf {
        let mut conf = ClusterConf {
            format_worker: true,
            ..Default::default()
        };
        conf.worker.hostname = "127.0.0.1".into();
        conf.worker.rpc_port = data_port;
        conf.worker.control_rpc_port = control_port;
        conf.worker.io_threads = 1;
        conf.worker.worker_threads = 2;
        conf.worker.data_dir = vec![format!(
            "[MEM:10MB]{}/curvine-hybrid-worker-{}-{}",
            std::env::temp_dir().display(),
            std::process::id(),
            data_port
        )];
        conf.client.init().unwrap();
        conf
    }

    #[test]
    fn rejects_unadvertisable_or_conflicting_control_ports() {
        let mut config = ClusterConf::default();
        config.worker.control_rpc_port = 0;
        assert!(Worker::with_conf(config.clone()).is_err());
        config.worker.control_rpc_port = config.worker.rpc_port;
        assert!(Worker::with_conf(config.clone()).is_err());
        config.worker.control_rpc_port = config.worker.web_port;
        assert!(Worker::with_conf(config).is_err());
    }

    #[test]
    fn stopping_data_listener_stops_control_listener_too() {
        let data_port = NetUtils::hold_available_port();
        let control_port = NetUtils::hold_available_port();
        let mut worker = Worker::with_conf(conf(data_port, control_port)).unwrap();
        assert_eq!(worker.addr.rpc_port, data_port as u32);
        assert_eq!(worker.addr.control_rpc_port, Some(control_port as u32));
        let server = worker.rpc_server.as_ref().unwrap();
        let rt = server.clone_rt();
        let stop = server.shutdown_handle();
        rt.block_on(async {
            let mut status = worker.start_rpc_only().await.unwrap();
            assert!(tokio::net::TcpStream::connect(("127.0.0.1", data_port))
                .await
                .is_ok());
            assert!(tokio::net::TcpStream::connect(("127.0.0.1", control_port))
                .await
                .is_ok());
            stop.send_replace(true);
            tokio::time::timeout(Duration::from_secs(5), status.wait_stop())
                .await
                .unwrap()
                .unwrap();
            assert!(tokio::net::TcpStream::connect(("127.0.0.1", data_port))
                .await
                .is_err());
            assert!(tokio::net::TcpStream::connect(("127.0.0.1", control_port))
                .await
                .is_err());
        });
    }

    #[test]
    fn control_bind_failure_does_not_start_data_listener() {
        let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let control_port = occupied.local_addr().unwrap().port();
        let data_port = NetUtils::hold_available_port();
        let mut worker = Worker::with_conf(conf(data_port, control_port)).unwrap();
        let server = worker.rpc_server.as_ref().unwrap();
        let rt = server.clone_rt();
        let cleaned = Arc::new(AtomicBool::new(false));
        let cleaned_hook = cleaned.clone();
        server.add_shutdown_hook(move || cleaned_hook.store(true, Ordering::Release));
        assert!(rt.block_on(worker.start_rpc_only()).is_err());
        assert!(cleaned.load(Ordering::Acquire));
        assert!(worker.web_server.is_some());
        assert!(NetUtils::take_held_listener(data_port).is_some());
    }

    #[test]
    fn data_bind_failure_stops_control_listener() {
        let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let data_port = occupied.local_addr().unwrap().port();
        let control_port = NetUtils::hold_available_port();
        let mut worker = Worker::with_conf(conf(data_port, control_port)).unwrap();
        let rt = worker.rpc_server.as_ref().unwrap().clone_rt();
        assert!(rt.block_on(worker.start_rpc_only()).is_err());
        assert!(worker.web_server.is_some());
        assert!(TcpListener::bind(("127.0.0.1", control_port)).is_ok());
    }
}
