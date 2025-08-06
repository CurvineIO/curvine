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

use crate::master::fs::heartbeat_checker::HeartbeatChecker;
use crate::master::fs::master_filesystem::MasterFilesystem;
use crate::master::meta::inode::ttl::ttl_manager::InodeTtlManager;
use crate::master::meta::inode::ttl::ttl_scheduler::TtlHeartbeatChecker;
use crate::master::meta::inode::ttl_scheduler::TtlHeartbeatConfig;
use crate::master::MasterMonitor;
use curvine_common::executor::ScheduledExecutor;
use log::{error, info};
use orpc::runtime::GroupExecutor;
use orpc::CommonResult;
use std::sync::Arc;

pub struct MasterActor {
    pub fs: MasterFilesystem,
    pub master_monitor: MasterMonitor,
    pub executor: Arc<GroupExecutor>,
}

impl MasterActor {
    pub fn new(
        fs: MasterFilesystem,
        master_monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
    ) -> Self {
        Self {
            fs,
            master_monitor,
            executor,
        }
    }

    pub fn start(&mut self) {
        info!("start master actor");
        Self::start_heartbeat_checker(
            self.fs.clone(),
            self.master_monitor.clone(),
            self.executor.clone(),
        )
        .unwrap();

        if let Err(e) = self.start_ttl_scheduler() {
            error!("Failed to start inode ttl scheduler: {}", e);
        }
    }

    pub fn start_ttl_scheduler(&mut self) -> CommonResult<()> {
        info!("Starting inode ttl scheduler.");

        let ttl_bucket_list = {
            let fs_dir_lock = self.fs.fs_dir();
            let fs_dir = fs_dir_lock.read();
            fs_dir.get_ttl_bucket_list()
        };

        let ttl_manager = InodeTtlManager::new(self.fs.clone(), ttl_bucket_list)?;
        let ttl_manager_arc = Arc::new(ttl_manager);

        // TTL manager is ready for use immediately after creation
        self.start_ttl_heartbeat_checker(ttl_manager_arc)?;

        info!("Inode ttl scheduler started successfully.");
        Ok(())
    }

    fn start_ttl_heartbeat_checker(
        &mut self,
        ttl_manager: Arc<InodeTtlManager>,
    ) -> CommonResult<()> {
        info!("Starting inode ttl checker");
        let ttl_config = TtlHeartbeatConfig {
            task_name: "inode-ttl-checker".to_string(),
            timeout_ms: self.fs.conf.ttl_checker_interval_ms() * 2,
        };
        let heartbeat_checker = TtlHeartbeatChecker::new(ttl_manager, ttl_config);
        let scheduler = ScheduledExecutor::new(
            "inode-ttl-checker".to_string(),
            self.fs.conf.ttl_checker_interval_ms(),
        );
        scheduler.start(heartbeat_checker)?;
        info!("Inode ttl checker started");
        Ok(())
    }

    fn start_heartbeat_checker(
        fs: MasterFilesystem,
        master_monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
    ) -> CommonResult<()> {
        let check_ms = fs.conf.worker_check_interval_ms();
        let scheduler = ScheduledExecutor::new("worker-heartbeat", check_ms);

        let task = HeartbeatChecker::new(fs, master_monitor, executor);

        scheduler.start(task)?;
        Ok(())
    }
}
