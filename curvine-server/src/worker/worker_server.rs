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
use once_cell::sync::OnceCell;
use orpc::common::{LocalTime, Logger};
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;
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
}

impl WorkerService {
    pub fn with_conf(conf: &ClusterConf, rt: Arc<Runtime>) -> CommonResult<Self> {
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
        WorkerRouterHandler {}
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
}

impl Worker {
    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Logger::init(conf.worker.log.clone());

        let rt = Arc::new(conf.worker_server_conf().create_runtime());
        let service: WorkerService = WorkerService::with_conf(&conf, rt.clone())?;
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
        };

        Ok(worker)
    }

    pub async fn start(self) -> ServerStateListener {
        // step 1: Start rpc server
        let mut rpc_status = self.rpc_server.start();
        rpc_status.wait_running().await.unwrap();

        // step 2: Start block heartbeat check service
        thread::spawn(move || self.block_actor.start())
            .join()
            .unwrap();

        // step 3: Start the web server
        self.web_server.start();

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
}
