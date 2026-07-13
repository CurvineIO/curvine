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

use crate::worker::block::{BlockStore, MasterClient};
use crate::worker::storage::Dataset;
use curvine_common::error::FsError;
use curvine_common::state::{BlockReportInfo, HeartbeatStatus, WorkerCommand};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use dashmap::DashMap;
use log::{error, warn};
use orpc::runtime::{GroupExecutor, LoopTask};
use orpc::server::ServerState;
use orpc::sync::StateCtl;
use orpc::try_log;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub struct HeartbeatTask {
    pub(crate) executor: Arc<GroupExecutor>,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) client: MasterClient,
    pub(crate) store: BlockStore,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    pub(crate) report_in_flight: Arc<AtomicBool>,
}

impl HeartbeatTask {
    // Asynchronously delete the block file.
    pub(crate) fn delete_block_task(
        executor: Arc<GroupExecutor>,
        store: BlockStore,
        cmds: Vec<WorkerCommand>,
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    ) {
        for cmd in cmds {
            match cmd {
                WorkerCommand::DeleteBlock(c) => {
                    for block in c.blocks {
                        if report_blocks.contains_key(&block) {
                            continue;
                        }

                        if let Err(e) = store
                            .write()
                            .map(|state| state.increment_blocks_to_delete())
                        {
                            error!("failed to mark block {} deleting: {}", block, e);
                            continue;
                        }

                        // Whether or not it is successfully deleted, it is marked as deleted
                        report_blocks.insert(block, BlockReportInfo::with_deleted(block, 0));

                        let store1 = store.clone();
                        let res = executor.spawn(move || {
                            match store1.async_remove_block(block) {
                                Ok(v) => v.len,
                                Err(e) => {
                                    warn!("async_remove_block {}: {}", block, e);
                                    0
                                }
                            };
                        });

                        let _ = try_log!(res);
                    }
                }
            }
        }
    }

    pub fn get_report_blocks(&self) -> Vec<BlockReportInfo> {
        let mut vec = vec![];
        let blocks = self
            .report_blocks
            .iter()
            .map(|x| *x.key())
            .collect::<Vec<_>>();

        for block in blocks {
            if let Some(v) = self.report_blocks.remove(&block) {
                vec.push(v.1);
            }
        }
        vec
    }

    pub fn put_missing_report(&self, blocks: Vec<BlockReportInfo>) {
        Self::put_report_blocks(self.report_blocks.clone(), blocks);
    }

    fn put_report_blocks(
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
        blocks: Vec<BlockReportInfo>,
    ) {
        for block in blocks {
            report_blocks.entry(block.id).or_insert(block);
        }
    }

    fn submit_block_report(&self, report_blocks: Vec<BlockReportInfo>) {
        let client = self.client.clone();
        let executor = self.executor.clone();
        let store = self.store.clone();
        let pending_reports = self.report_blocks.clone();
        let report_in_flight = self.report_in_flight.clone();
        let task_reports = Arc::new(Mutex::new(Some(report_blocks)));
        let task_reports_for_task = task_reports.clone();
        let res = self.executor.spawn(move || {
            let reports = task_reports_for_task
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .unwrap_or_default();
            match client.incr_block_report(&reports) {
                Ok(v) => {
                    let cmds = ProtoUtils::worker_cmd_from_pb(v.cmds);
                    Self::delete_block_task(executor, store, cmds, pending_reports);
                }
                Err(e) => {
                    error!("report blocks {}", e);
                    Self::put_report_blocks(pending_reports, reports);
                }
            }
            report_in_flight.store(false, Ordering::Release);
        });

        if let Err(e) = res {
            error!("submit block report task failed {}", e);
            let reports = task_reports
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .unwrap_or_default();
            self.put_missing_report(reports);
            self.report_in_flight.store(false, Ordering::Release);
        }
    }
}

impl LoopTask for HeartbeatTask {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        // Perform heartbeat sending.
        let info = match self.store.get_and_check_storages() {
            Ok(info) => info,
            Err(e) => {
                error!("collect worker storage info failed {}", e);
                return Ok(());
            }
        };
        let res = self.client.heartbeat(HeartbeatStatus::Running, info);
        match res {
            Ok(v) => {
                let cmds = ProtoUtils::worker_cmd_from_pb(v.cmds);
                Self::delete_block_task(
                    self.executor.clone(),
                    self.store.clone(),
                    cmds,
                    self.report_blocks.clone(),
                );
            }

            Err(e) => {
                // Wait for the next try again.
                error!("Send heartbeat failed {}", e);
                return Ok(());
            }
        };

        // Execute block report
        if self.report_in_flight.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let report_blocks = self.get_report_blocks();
        if report_blocks.is_empty() {
            self.report_in_flight.store(false, Ordering::Release);
            return Ok(());
        }

        self.submit_block_report(report_blocks);

        Ok(())
    }

    fn terminate(&self) -> bool {
        let state: ServerState = self.worker_ctl.state();
        state == ServerState::Stop
    }
}
