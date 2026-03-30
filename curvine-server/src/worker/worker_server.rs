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
use crate::worker::handler::{WorkerHandler, WorkerRouterHandler};
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::replication::worker_replication_manager::WorkerReplicationManager;
use crate::worker::task::TaskManager;
use crate::worker::WorkerMetrics;
use curvine_common::conf::ClusterConf;
use curvine_common::state::{HeartbeatStatus, WorkerAddress};
use curvine_web::server::{WebHandlerService, WebServer};
use log::info;
#[cfg(feature = "heap-trace")]
use log::warn;
use once_cell::sync::OnceCell;
#[cfg(feature = "heap-trace")]
use orpc::common::heap_trace::{HeapTraceConfig, HeapTraceRuntime};
use orpc::common::{LocalTime, Logger};
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

static CLUSTER_CONF: OnceCell<ClusterConf> = OnceCell::new();

static WORKER_METRICS: OnceCell<WorkerMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct WorkerService {
    store: BlockStore,
    conf: ClusterConf,
    task_manager: Arc<TaskManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<WorkerReplicationManager>,
    #[cfg(feature = "heap-trace")]
    heap_trace: Option<Arc<HeapTraceRuntime>>,
}

impl WorkerService {
    pub fn with_conf(
        conf: &ClusterConf,
        rt: Arc<Runtime>,
        #[cfg(feature = "heap-trace")] heap_trace: Option<Arc<HeapTraceRuntime>>,
    ) -> CommonResult<Self> {
        let store: BlockStore = BlockStore::new(&conf.cluster_id, conf)?;

        let task_manager = TaskManager::with_rt(rt.clone(), conf)?;

        let replication_manager =
            WorkerReplicationManager::new(&store, &rt, conf, &task_manager.get_fs_context());

        let ws = Self {
            store,
            conf: conf.clone(),
            task_manager: Arc::new(task_manager),
            rt,
            replication_manager,
            #[cfg(feature = "heap-trace")]
            heap_trace,
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

    fn get_message_handler(&self, _: Option<ConnState>) -> Self::Item {
        WorkerHandler {
            store: self.store.clone(),
            handler: None,
            task_manager: self.task_manager.clone(),
            rt: self.rt.clone(),
            replication_handler: WorkerReplicationHandler::new(&self.replication_manager),
        }
    }
}

impl WebHandlerService for WorkerService {
    type Item = WorkerRouterHandler;

    fn get_handler(&self) -> Self::Item {
        WorkerRouterHandler::new(
            #[cfg(feature = "heap-trace")]
            self.heap_trace.clone(),
        )
    }
}

// block data start service.
pub struct Worker {
    pub start_ms: u64,
    pub worker_id: u32,
    pub addr: WorkerAddress,
    rpc_server: RpcServer<WorkerService>,
    web_server: WebServer<WorkerService>,
    block_actor: BlockActor,
    #[cfg(feature = "heap-trace")]
    heap_trace: Option<Arc<HeapTraceRuntime>>,
}

impl Worker {
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Logger::init(conf.worker.log.clone());

        let rt = Arc::new(conf.worker_server_conf().create_runtime());
        #[cfg(feature = "heap-trace")]
        let heap_trace = build_heap_trace_runtime(&conf);
        let service: WorkerService = WorkerService::with_conf(
            &conf,
            rt.clone(),
            #[cfg(feature = "heap-trace")]
            heap_trace.clone(),
        )?;
        let worker_id = service.store.worker_id();

        CLUSTER_CONF.get_or_init(|| conf.clone());
        WORKER_METRICS.get_or_init(|| WorkerMetrics::new(service.store.clone()).unwrap());
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
        let block_actor = BlockActor::new(
            rt.clone(),
            &conf,
            addr.clone(),
            block_store.clone(),
            rpc_server.new_state_ctl(),
        );

        let master_client = block_actor.client.clone();
        service
            .replication_manager
            .with_master_client(master_client.clone());

        rpc_server.add_shutdown_hook(move || {
            if let Err(e) = master_client.heartbeat(HeartbeatStatus::End, vec![]) {
                info!("error unregister {}", e)
            }
        });

        let worker = Self {
            start_ms: LocalTime::mills(),
            worker_id,
            addr,
            rpc_server,
            web_server,
            block_actor,
            #[cfg(feature = "heap-trace")]
            heap_trace,
        };

        Ok(worker)
    }

    pub async fn start(self) -> ServerStateListener {
        #[cfg(feature = "heap-trace")]
        if let Some(runtime) = &self.heap_trace {
            if let Err(err) = runtime.start_periodic(Duration::from_secs(60)).await {
                log::error!("Failed to start heap trace runtime: {}", err);
            }
        }

        let conf = CLUSTER_CONF.get().expect("Cluster conf not initialized");
        if conf.worker.enable_s3_gateway {
            #[cfg(target_os = "linux")]
            {
                info!("Starting S3 gateway alongside worker");
                let worker_rt = self.rpc_server.clone_rt();
                Self::start_s3_gateway(conf.clone(), worker_rt).await;
            }
        }

        // step 3: Start rpc server
        let mut rpc_status = self.rpc_server.start();
        rpc_status.wait_running().await.unwrap();

        // step 4: Start the web server
        self.web_server.start();

        // step 5: Start block heartbeat check service
        thread::spawn(move || self.block_actor.start())
            .join()
            .unwrap();

        rpc_status
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.clone_rt();

        rt.block_on(async move {
            let mut rpc_status = self.start().await;
            rpc_status.wait_stop().await.unwrap();
        })
    }

    // Start a standalone worker.
    pub fn start_standalone(&self) {
        self.rpc_server.block_on_start();
    }

    pub fn get_conf<'a>() -> &'a ClusterConf {
        CLUSTER_CONF.get().expect("Worker get conf error!")
    }

    pub fn get_metrics<'a>() -> &'a WorkerMetrics {
        WORKER_METRICS.get().expect("Worker get metrics error!")
    }

    pub fn service(&self) -> &WorkerService {
        self.rpc_server.service()
    }

    #[cfg(target_os = "linux")]
    async fn start_s3_gateway(mut conf: ClusterConf, worker_rt: Arc<Runtime>) {
        let listen_addr = conf.s3_gateway.listen.clone();
        let region = conf.s3_gateway.region.clone();
        conf.s3_gateway.enable_distributed_auth = true;

        info!(
            "Initializing S3 gateway on {} in region {}",
            listen_addr, region
        );

        let rt_clone = worker_rt.clone();
        #[cfg(feature = "heap-trace")]
        let heap_trace = build_heap_trace_runtime(&conf);
        worker_rt.spawn(async move {
            match curvine_s3_gateway::start_gateway(
                conf,
                listen_addr.clone(),
                region,
                rt_clone,
                #[cfg(feature = "heap-trace")]
                heap_trace,
            )
            .await
            {
                Ok(_) => {
                    info!("S3 gateway started successfully on {}", listen_addr);
                }
                Err(e) => {
                    log::error!("Failed to start S3 gateway on {}: {}", listen_addr, e);
                }
            }
        });
    }
}

#[cfg(feature = "heap-trace")]
fn build_heap_trace_runtime(conf: &ClusterConf) -> Option<Arc<HeapTraceRuntime>> {
    if !conf.heap_trace.runtime_enabled {
        return None;
    }

    let malloc_conf = std::env::var("MALLOC_CONF").unwrap_or_default();
    if !malloc_conf.contains("prof:true") || !malloc_conf.contains("prof_active:true") {
        warn!(
            "Heap trace requested for worker, but MALLOC_CONF did not enable jemalloc profiling at process start; runtime capture disabled"
        );
        return Some(Arc::new(HeapTraceRuntime::new(HeapTraceConfig::disabled())));
    }

    Some(Arc::new(HeapTraceRuntime::new(HeapTraceConfig::new(
        conf.heap_trace.runtime_enabled,
        0,
    ))))
}
