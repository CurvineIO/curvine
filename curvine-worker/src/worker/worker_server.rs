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
use crate::worker::handler::{ConnectionPeer, WorkerHandler, WorkerRouterHandler};
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::replication::worker_replication_manager::WorkerReplicationManager;
use crate::worker::task::TaskManager;
use crate::worker::WorkerMetrics;
use curvine_config::ClusterConf;
use curvine_core_error::StringError;
use curvine_core_error::{CommonError, CommonResult};
use curvine_fault::FaultHttpControl;
use curvine_model::{BlockReportInfo, CompatibilityPolicy, HeartbeatStatus, WorkerAddress};
use curvine_net::net::ConnState;
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
            web_port: conf.worker.web_port as u32,
        };
        let start_ms = LocalTime::mills();
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
            },
        )?;

        let master_client = block_actor.client.clone();
        service
            .replication_manager
            .with_master_client(master_client.clone());

        let spdk_enabled = conf.worker.spdk_disk.enabled;
        rpc_server.add_shutdown_hook(move || {
            if let Err(e) = master_client.heartbeat(HeartbeatStatus::End, vec![]) {
                info!("error unregister {}", e)
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

    pub async fn start(&mut self) -> CommonResult<ServerStateListener> {
        let rpc_server = self
            .rpc_server
            .take()
            .expect("worker rpc server must be initialized before startup");
        // step 3: Start rpc server
        let mut rpc_status = rpc_server.start();
        rpc_status.wait_running().await?;

        // step 4: Start the web server
        let web_server = self
            .web_server
            .take()
            .expect("worker web server must be initialized before startup");
        let web_name = web_server.server_name().to_string();
        let bind_addr = web_server.resolve_bind_addr();
        let mut web_status = web_server.start();
        WebServer::<WorkerService>::wait_bind(&mut web_status, &web_name, &bind_addr).await?;

        // step 5: Start block heartbeat check service
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
    pub fn start_standalone(&self) {
        self.rpc_server
            .as_ref()
            .expect("worker rpc server must be initialized")
            .block_on_start();
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
