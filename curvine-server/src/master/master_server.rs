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

use curvine_common::conf::ClusterConf;
use curvine_web::server::{WebHandlerService, WebServer};
use orpc::common::{LocalTime, Logger};
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::server::{RpcServer, ServerStateListener};
use orpc::CommonResult;

use crate::master::fs::{FsRetryCache, MasterActor, MasterFilesystem};
use crate::master::journal::JournalSystem;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::router_handler::MasterRouterHandler;
use crate::master::{JobHandler, MountManager};
use crate::master::{JobManager, MasterHandler};
use crate::master::{MasterMetrics, MasterMonitor, SyncWorkerManager};

pub static MASTER_METRICS: OnceCell<MasterMetrics> = OnceCell::new();

#[derive(Clone)]
pub struct MasterService {
    conf: ClusterConf,
    fs: MasterFilesystem,
    retry_cache: Option<FsRetryCache>,
    mount_manager: Arc<MountManager>,
    job_manager: Arc<JobManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<MasterReplicationManager>,
}

impl MasterService {
    pub fn new(
        conf: ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        mount_manager: Arc<MountManager>,
        job_manager: Arc<JobManager>,
        rt: Arc<Runtime>,
        replication_manager: Arc<MasterReplicationManager>,
    ) -> Self {
        Self {
            conf,
            fs,
            retry_cache,
            mount_manager,
            job_manager,
            rt,
            replication_manager,
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
}

impl HandlerService for MasterService {
    type Item = MasterHandler;

    fn has_conn_state(&self) -> bool {
        true
    }

    fn get_message_handler(&self, client_state: Option<ConnState>) -> Self::Item {
        MasterHandler::new(
            &self.conf,
            self.fs.clone(),
            self.retry_cache.clone(),
            client_state,
            self.mount_manager.clone(),
            JobHandler::new(self.job_manager.clone()),
            self.replication_manager.clone(),
        )
    }
}

impl WebHandlerService for MasterService {
    type Item = MasterRouterHandler;

    fn get_handler(&self) -> Self::Item {
        MasterRouterHandler::new(self.conf.clone(), self.fs.clone())
    }
}

pub struct Master {
    pub start_time: u64,
    rpc_server: RpcServer<MasterService>,
    web_server: WebServer<MasterService>,
    journal_system: JournalSystem,
    actor: MasterActor,
    mount_manager: Arc<MountManager>,
    job_manager: Arc<JobManager>,
    replication_manager: Arc<MasterReplicationManager>,
}

impl Master {
    fn new(conf: ClusterConf) -> CommonResult<Self> {
        let mut log = conf.master.log.clone();
        if conf.master.audit_logging_enabled {
            log.targets = vec!["audit".to_string()]
        }

        Logger::init(log);
        MASTER_METRICS.get_or_init(|| MasterMetrics::new().unwrap());
        conf.print();

        // step1: Create a journal system, the journal system determines how to create a fs dir.
        let journal_system = JournalSystem::from_conf(&conf)?;
        let fs = journal_system.fs();
        let worker_manager = journal_system.worker_manager();
        let mount_manager = journal_system.mount_manager();
        let quota_manager = journal_system.quota_manager();

        let rt = Arc::new(conf.master_server_conf().create_runtime());

        let replication_manager = MasterReplicationManager::new(&fs, &conf, &rt, &worker_manager);

        let actor = MasterActor::new(
            fs.clone(),
            journal_system.master_monitor(),
            conf.master.new_executor(),
            &replication_manager,
            quota_manager,
        );

        let job_manager = Arc::new(JobManager::from_cluster_conf(
            fs.clone(),
            mount_manager.clone(),
            rt.clone(),
            &conf,
        ));

        // step3: Create rpc server.
        let retry_cache = FsRetryCache::with_conf(&conf.master);
        let service = MasterService::new(
            conf.clone(),
            fs,
            retry_cache,
            mount_manager.clone(),
            job_manager.clone(),
            rt.clone(),
            replication_manager.clone(),
        );

        let rpc_conf = conf.master_server_conf();
        let rpc_server = RpcServer::with_rt(rt.clone(), rpc_conf, service.clone());

        // step4: Create a web server
        let web_conf = conf.master_web_conf();
        let web_server = WebServer::new(web_conf, service);

        Ok(Self {
            start_time: LocalTime::mills(),
            rpc_server,
            web_server,
            journal_system,
            actor,
            mount_manager,
            job_manager,
            replication_manager,
        })
    }

    pub fn with_conf(conf: ClusterConf) -> CommonResult<Self> {
        Self::new(conf)
    }

    pub async fn start(mut self) -> ServerStateListener {
        // step 1: Start journal_system, raft server and raft node will be started internally
        let mut listener = self.journal_system.start().await.unwrap();
        listener.wait_role().await.unwrap();

        // step 2: Start rpc server
        let mut rpc_status = self.rpc_server.start();
        rpc_status.wait_running().await.unwrap();

        // step3: Start the web server
        self.web_server.start();

        // step4: Start master actor
        self.actor.start();

        // reload mount info
        self.mount_manager.restore();

        // step5: Start job manager
        self.job_manager.start();

        rpc_status
    }

    pub fn block_on_start(self) {
        let rt = self.rpc_server.clone_rt();
        rt.block_on(async move {
            let mut status = self.start().await;
            status.wait_stop().await.unwrap();
        });
    }

    pub fn get_metrics<'a>() -> &'a MasterMetrics {
        MASTER_METRICS.get().expect("Master get metrics error!")
    }

    // Instantiate metrics during testing
    pub fn init_test_metrics() {
        let metrics = MasterMetrics::new().unwrap();
        MASTER_METRICS.get_or_init(|| metrics);
    }

    // for test
    pub fn get_fs(&self) -> MasterFilesystem {
        self.rpc_server.service().fs.clone()
    }

    // for test
    pub fn get_replication_manager(&self) -> Arc<MasterReplicationManager> {
        self.replication_manager.clone()
    }

    pub fn service(&self) -> &MasterService {
        self.rpc_server.service()
    }
}
