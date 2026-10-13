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

use curvine_config::{ClusterConf, RuntimeConfigRegistry};
use curvine_core_error::{err_box, CommonError, CommonResult};
use curvine_fault::FaultHttpControl;
use curvine_net::net::ConnState;
use curvine_rpc::handler::{HandlerService, LimitConf};
use curvine_rpc::server::{RpcServer, ServerStateListener};
use curvine_runtime::common::{LocalTime, Logger};
use curvine_runtime::runtime::{AsyncRuntime, GroupExecutor, RpcRuntime, Runtime};
use log::{error, info};

use crate::master::build_master_runtime_registry;
use crate::master::fs::{FsRetryCache, MasterActor, MasterFilesystem};
use crate::master::journal::JournalSystem;
use crate::master::metrics_server::{wait_for_metrics_bind, MasterMetricsServer};
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::space::DirSpaceManager;
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
    actor_rt: Arc<Runtime>,
    control_rpc_executor: Arc<GroupExecutor>,
    replication_manager: Arc<MasterReplicationManager>,
    limit: LimitConf,
    metrics: &'static MasterMetrics,
    pub fault_http: FaultHttpControl,
    runtime_config: Arc<RuntimeConfigRegistry>,
    /// Shared dir-space query facade; `None` when the feature is disabled.
    pub dir_space_manager: Option<Arc<DirSpaceManager>>,
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
        replication_manager: Arc<MasterReplicationManager>,
        metrics: &'static MasterMetrics,
        fault_http: FaultHttpControl,
        runtime_config: Arc<RuntimeConfigRegistry>,
        dir_space_manager: Option<Arc<DirSpaceManager>>,
    ) -> Self {
        let actor_rt = Arc::new(AsyncRuntime::new(
            "master-actor",
            1,
            conf.master.actor_threads,
        ));
        let control_rpc_executor = Arc::new(GroupExecutor::new("master-control-rpc", 2, 1024));
        let limit = LimitConf::new(conf.master.conn_limit, conf.master.global_limit);
        Self {
            conf,
            fs,
            retry_cache,
            mount_manager,
            job_manager,
            rt,
            actor_rt,
            control_rpc_executor,
            replication_manager,
            limit,
            metrics,
            fault_http,
            runtime_config,
            dir_space_manager,
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
            self.control_rpc_executor.clone(),
            self.replication_manager.clone(),
            self.actor_rt.clone(),
            self.metrics,
            self.runtime_config.clone(),
        )
    }

    fn is_stream(&self) -> bool {
        !self.conf.master.meta_request_concurrent
    }

    fn get_limit(&self) -> LimitConf {
        self.limit.clone()
    }
}

pub struct Master {
    pub start_time: u64,
    rpc_server: Option<RpcServer<MasterService>>,
    metrics_server: Option<MasterMetricsServer>,
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
        info!("allocator: {}", curvine_alloc::allocator_type_name());
        info!("git version: {}", curvine_sys::version::GIT_VERSION);
        conf.print();

        // step 1: Create a journal system, the journal system determines how to create a fs dir.
        let journal_system = JournalSystem::from_conf(&conf)?;
        let fs = journal_system.fs();
        let worker_manager = journal_system.worker_manager();
        let mount_manager = journal_system.mount_manager();
        let quota_manager = journal_system.quota_manager();
        let job_manager = journal_system.job_manager();

        // step 2: Create runtime, replication manager, and master actor.
        let rt = Arc::new(conf.master_server_conf().create_runtime());
        let replication_manager = MasterReplicationManager::new(&fs, &conf, &rt, &worker_manager)?;

        // The dir-space manager owns the snapshot store and query facade; the
        // master actor drives its scans, the RPC handler (Task 8) serves its
        // queries. Built once here so both sides observe the same generations.
        let dir_space_manager = if conf.master.dir_space_enabled {
            Some(Arc::new(DirSpaceManager::new(
                fs.clone(),
                mount_manager.clone(),
                Arc::new(conf.master.clone()),
            )))
        } else {
            None
        };

        let actor = MasterActor::new(
            fs.clone(),
            journal_system.master_monitor(),
            conf.master.new_executor(),
            &replication_manager,
            quota_manager,
            dir_space_manager.clone(),
        );

        // step 3: Create rpc server.
        let retry_cache = FsRetryCache::with_conf(&conf.master)?;
        let metrics_fs = fs.clone();
        let runtime_config = Arc::new(build_master_runtime_registry(&conf));
        let start_time_ms = LocalTime::mills();
        let start_time = LocalTime::now_datetime();
        let service = MasterService::new(
            conf.clone(),
            fs,
            retry_cache,
            mount_manager.clone(),
            job_manager.clone(),
            rt.clone(),
            replication_manager.clone(),
            metrics,
            fault_http,
            runtime_config.clone(),
            dir_space_manager,
        );

        let rpc_conf = conf.master_server_conf();
        let rpc_server = RpcServer::with_rt(rt.clone(), rpc_conf, service);
        let metrics_server = MasterMetricsServer::with_rt(
            rt,
            conf.master_metrics_conf(),
            metrics_fs,
            runtime_config,
            start_time,
            start_time_ms,
        );

        Ok(Self {
            start_time: start_time_ms,
            rpc_server: Some(rpc_server),
            metrics_server: Some(metrics_server),
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
        let journal_system = match self.journal_system.take() {
            Some(journal_system) => journal_system,
            None => return err_box!("master journal system is not initialized"),
        };
        let mut listener = journal_system.start().await?;
        listener.wait_role().await?;

        let rpc_server = match self.rpc_server.take() {
            Some(rpc_server) => rpc_server,
            None => return err_box!("master rpc server is not initialized"),
        };
        let metrics_required = rpc_server.service().conf().master.metrics_required;
        let master_web_port = rpc_server.service().conf().master.web_port;
        let mut rpc_status = rpc_server.start();
        rpc_status.wait_running().await?;

        let metrics_server = match self.metrics_server.take() {
            Some(metrics_server) => metrics_server,
            None => return err_box!("master metrics server is not initialized"),
        };
        let bind_rx = metrics_server.start_with_bind_notify();
        if metrics_required {
            wait_for_metrics_bind(bind_rx, master_web_port).await?;
        } else if let Err(err) = wait_for_metrics_bind(bind_rx, master_web_port).await {
            error!(
                "Master metrics server failed to bind on port {master_web_port} \
                 (metrics_required=false, continuing): {err}"
            );
        }

        self.actor.start()?;
        self.mount_manager.restore_best_effort();
        self.actor.start_dir_space_scheduler()?;
        self.job_manager.start()?;

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
        let metrics = MasterMetrics::new().unwrap();
        MASTER_METRICS.get_or_init(|| metrics);
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
