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
use curvine_fault::FaultHttpControl;
use curvine_web::server::{WebHandlerService, WebServer};
use log::error;
use orpc::common::{LocalTime, Logger};
use orpc::handler::HandlerService;
use orpc::io::net::ConnState;
use orpc::runtime::{GroupExecutor, RpcRuntime, Runtime};
use orpc::server::{RpcServer, ServerStateListener};
use orpc::{err_box, CommonError, CommonResult};

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
    heartbeat_rpc_executor: Arc<GroupExecutor>,
    block_report_rpc_executor: Arc<GroupExecutor>,
    control_rpc_executor: Arc<GroupExecutor>,
    list_rpc_executor: Arc<GroupExecutor>,
    get_block_locations_rpc_executor: Arc<GroupExecutor>,
    replication_manager: Arc<MasterReplicationManager>,
    metrics: &'static MasterMetrics,
    fault_http: FaultHttpControl,
}

impl MasterService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        conf: ClusterConf,
        fs: MasterFilesystem,
        retry_cache: Option<FsRetryCache>,
        mount_manager: Arc<MountManager>,
        job_manager: Arc<JobManager>,
        rt: Arc<Runtime>,
        heartbeat_rpc_executor: Arc<GroupExecutor>,
        block_report_rpc_executor: Arc<GroupExecutor>,
        control_rpc_executor: Arc<GroupExecutor>,
        list_rpc_executor: Arc<GroupExecutor>,
        get_block_locations_rpc_executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        metrics: &'static MasterMetrics,
        fault_http: FaultHttpControl,
    ) -> Self {
        Self {
            conf,
            fs,
            retry_cache,
            mount_manager,
            job_manager,
            rt,
            heartbeat_rpc_executor,
            block_report_rpc_executor,
            control_rpc_executor,
            list_rpc_executor,
            get_block_locations_rpc_executor,
            replication_manager,
            metrics,
            fault_http,
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
            self.heartbeat_rpc_executor.clone(),
            self.block_report_rpc_executor.clone(),
            self.control_rpc_executor.clone(),
            self.list_rpc_executor.clone(),
            self.get_block_locations_rpc_executor.clone(),
            self.replication_manager.clone(),
            self.metrics,
        )
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
        )
    }
}

pub struct Master {
    pub start_time: u64,
    rpc_server: Option<RpcServer<MasterService>>,
    web_server: Option<WebServer<MasterService>>,
    journal_system: Option<JournalSystem>,
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
        let fault_http = FaultHttpControl::from_env(&conf.fault_injection)
            .map_err(|error| CommonError::from(error.to_string()))?;
        let metrics = MASTER_METRICS.get_or_try_init(MasterMetrics::new)?;
        conf.print();

        // step1: Create a journal system, the journal system determines how to create a fs dir.
        let journal_system = JournalSystem::from_conf(&conf)?;
        let fs = journal_system.fs();
        let worker_manager = journal_system.worker_manager();
        let mount_manager = journal_system.mount_manager();
        let quota_manager = journal_system.quota_manager();
        let job_manager = journal_system.job_manager();

        let rt = Arc::new(conf.master_server_conf().create_runtime());
        let heartbeat_rpc_executor = Arc::new(GroupExecutor::new("master-heartbeat-rpc", 2, 1024));
        let block_report_rpc_executor =
            Arc::new(GroupExecutor::new("master-block-report-rpc", 2, 128));
        let control_rpc_executor = Arc::new(GroupExecutor::new("master-control-rpc", 2, 1024));
        let read_lane_threads = conf.master.worker_threads.saturating_sub(4).max(1);
        let read_lane_queue = conf.master.worker_threads.saturating_mul(2).max(1);
        let list_rpc_executor = Arc::new(GroupExecutor::new(
            "master-list-rpc",
            read_lane_threads,
            read_lane_queue,
        ));
        let get_block_locations_rpc_executor = Arc::new(GroupExecutor::new(
            "master-get-block-locations-rpc",
            read_lane_threads,
            read_lane_queue,
        ));

        let replication_manager = MasterReplicationManager::new(&fs, &conf, &rt, &worker_manager)?;

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
            job_manager.clone(),
            rt.clone(),
            heartbeat_rpc_executor,
            block_report_rpc_executor,
            control_rpc_executor,
            list_rpc_executor,
            get_block_locations_rpc_executor,
            replication_manager.clone(),
            metrics,
            fault_http,
        );

        let rpc_conf = conf.master_server_conf();
        let rpc_server = RpcServer::with_rt(rt.clone(), rpc_conf, service.clone());

        // step4: Create a web server
        let web_conf = conf.master_web_conf();
        let web_server = WebServer::new(web_conf, service);

        Ok(Self {
            start_time: LocalTime::mills(),
            rpc_server: Some(rpc_server),
            web_server: Some(web_server),
            journal_system: Some(journal_system),
            actor,
            mount_manager,
            job_manager,
            replication_manager,
        })
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
        let rpc_server = match self.rpc_server.take() {
            Some(rpc_server) => rpc_server,
            None => return err_box!("master rpc server is not initialized"),
        };
        let mut rpc_status = rpc_server.start();
        rpc_status.wait_running().await?;

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

        // step5: Start job manager
        self.job_manager.start()?;

        // step6: Start TTL scheduler (requires mount_manager and job_manager)
        if let Err(e) = self.actor.start_ttl_scheduler() {
            error!("Failed to start inode ttl scheduler: {}", e);
        }

        Ok(rpc_status)
    }

    pub fn block_on_start(mut self) -> CommonResult<()> {
        let rt = match self.rpc_server.as_ref() {
            Some(rpc_server) => rpc_server.clone_rt(),
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

    // for test
    pub fn get_fs(&self) -> MasterFilesystem {
        self.rpc_server
            .as_ref()
            .expect("master rpc server must be initialized")
            .service()
            .fs
            .clone()
    }

    // for test
    pub fn get_replication_manager(&self) -> Arc<MasterReplicationManager> {
        self.replication_manager.clone()
    }

    pub fn service(&self) -> &MasterService {
        self.rpc_server
            .as_ref()
            .expect("master rpc server must be initialized")
            .service()
    }
}
