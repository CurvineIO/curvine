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

use crate::worker::block::{BlockStore, HeartbeatTask, MasterClient};
use curvine_client::file::FsContext;
use curvine_common::conf::ClusterConf;
use curvine_common::executor::ScheduledExecutor;
use curvine_common::state::{BlockReportInfo, HeartbeatStatus, WorkerAddress};
use dashmap::DashMap;
use log::info;
use orpc::common::TimeSpent;
use orpc::runtime::{GroupExecutor, Runtime};
use orpc::sync::StateCtl;
use orpc::CommonResult;
use std::sync::Arc;

/// Worker block management role.
/// 1. Register worker with master
/// 2. Report block information to the master
/// 3. Accept the master's instructions and delete the block data.
pub struct BlockActor {
    pub(crate) client: MasterClient,
    store: BlockStore,
    executor: Arc<GroupExecutor>,
    heartbeat_interval_ms: u64,
    worker_ctl: StateCtl,
    block_report_limit: usize,

    // Block that needs to be reported when the heartbeats.
    // Includes the following situations:
    // 1. Block file deletion report.
    // 2. Add a new block.
    report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
}

impl BlockActor {
    pub fn new(
        rt: Arc<Runtime>,
        conf: &ClusterConf,
        worker_addr: WorkerAddress,
        store: BlockStore,
        worker_ctl: StateCtl,
    ) -> BlockActor {
        let context = FsContext::with_rt(conf.clone(), rt).unwrap();
        let context = Arc::new(context);
        let client = MasterClient::new(
            context.clone(),
            store.cluster_id(),
            store.worker_id(),
            worker_addr,
        );
        let executor = GroupExecutor::new(
            "worker-block-executor",
            conf.worker.executor_threads,
            conf.worker.executor_channel_size,
        );
        let heartbeat_interval_ms = conf.master.heartbeat_interval_ms();
        let block_report_limit = conf.master.block_report_limit;
        Self {
            client,
            store,
            executor: Arc::new(executor),
            heartbeat_interval_ms,
            worker_ctl,
            block_report_limit,
            report_blocks: Arc::new(DashMap::new()),
        }
    }

    pub fn start(self) {
        info!("start block actor");

        self.register().unwrap();
        info!("worker register success");

        let spend = TimeSpent::new();
        let total_len = self.full_block_report().unwrap();
        info!(
            "worker block report success, total blocks {}, used {} ms",
            total_len,
            spend.used_ms()
        );

        Self::start_heartbeat(
            self.executor.clone(),
            self.worker_ctl.clone(),
            self.client.clone(),
            self.store.clone(),
            self.report_blocks.clone(),
            self.heartbeat_interval_ms,
        )
        .unwrap();
    }

    // Worker registration.
    pub fn register(&self) -> CommonResult<()> {
        let storages_info = self.store.get_and_check_storages();
        let result = self.client.heartbeat(HeartbeatStatus::Start, storages_info);
        result?;
        Ok(())
    }

    pub fn full_block_report(&self) -> CommonResult<usize> {
        let blocks = self.store.all_blocks();

        let mut off = 0;
        while off < blocks.len() {
            let end = (off + self.block_report_limit).min(blocks.len());
            let _ = self
                .client
                .full_block_report(blocks.len(), &blocks[off..end])?;
            off = end;
        }

        Ok(blocks.len())
    }

    pub fn start_heartbeat(
        executor: Arc<GroupExecutor>,
        worker_ctl: StateCtl,
        client: MasterClient,
        store: BlockStore,
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
        heartbeat_interval_ms: u64,
    ) -> CommonResult<()> {
        let scheduler = ScheduledExecutor::new("worker-heartbeat", heartbeat_interval_ms);

        let task = HeartbeatTask {
            executor,
            worker_ctl,
            client,
            store,
            report_blocks,
        };

        scheduler.start(task)?;
        Ok(())
    }
}
