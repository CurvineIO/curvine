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
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::ProtoUtils;
use curvine_model::{BlockReportInfo, HeartbeatStatus, WorkerCommand};
use curvine_rpc::server::ServerState;
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use curvine_runtime::sync::StateCtl;
use dashmap::{DashMap, DashSet};
use log::{error, warn};
use std::sync::Arc;

pub struct HeartbeatTask {
    pub(crate) executor: Arc<GroupExecutor>,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) client: MasterClient,
    pub(crate) store: BlockStore,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    pub(crate) pending_deletes: Arc<DashSet<i64>>,
}

impl HeartbeatTask {
    // Asynchronously delete the block file.
    pub(crate) fn delete_block_task(
        executor: Arc<GroupExecutor>,
        store: BlockStore,
        cmds: Vec<WorkerCommand>,
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
        pending_deletes: Arc<DashSet<i64>>,
    ) {
        for cmd in cmds {
            match cmd {
                WorkerCommand::DeleteBlock(c) => {
                    for block in c.blocks {
                        if !pending_deletes.insert(block) {
                            continue;
                        }

                        if let Err(e) = store
                            .write()
                            .map(|state| state.increment_blocks_to_delete())
                        {
                            pending_deletes.remove(&block);
                            error!("failed to mark block {} deleting: {}", block, e);
                            continue;
                        }

                        let store1 = store.clone();
                        let report_blocks1 = report_blocks.clone();
                        let pending_deletes1 = pending_deletes.clone();
                        let res = executor.spawn(move || match store1.async_remove_block(block) {
                            Ok(Some(meta)) => {
                                report_blocks1
                                    .insert(block, BlockReportInfo::with_deleted(block, meta.len));
                            }
                            Ok(None) => {
                                report_blocks1
                                    .insert(block, BlockReportInfo::with_deleted(block, 0));
                            }
                            Err(e) => {
                                warn!("async_remove_block {}: {}", block, e);
                                pending_deletes1.remove(&block);
                            }
                        });

                        if let Err(e) = res {
                            pending_deletes.remove(&block);
                            match store.write() {
                                Ok(state) => state.decrement_blocks_to_delete(),
                                Err(err) => error!(
                                    "failed to clear deleting state for block {}: {}",
                                    block, err
                                ),
                            }
                            warn!("{}", e);
                        }
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
        for block in blocks {
            self.report_blocks.insert(block.id, block);
        }
    }

    fn acknowledge_reports(&self, blocks: &[BlockReportInfo]) {
        for block in blocks {
            if block.status == curvine_model::BlockReportStatus::Deleted {
                self.pending_deletes.remove(&block.id);
            }
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
                    self.pending_deletes.clone(),
                );
            }

            Err(e) => {
                // Wait for the next try again.
                error!("Send heartbeat failed {}", e);
                return Ok(());
            }
        };

        // Execute block report
        let report_blocks = self.get_report_blocks();
        if report_blocks.is_empty() {
            return Ok(());
        }

        let res = self.client.incr_block_report(&report_blocks);
        match res {
            Ok(v) => {
                self.acknowledge_reports(&report_blocks);
                let cmds = ProtoUtils::worker_cmd_from_pb(v.cmds);
                Self::delete_block_task(
                    self.executor.clone(),
                    self.store.clone(),
                    cmds,
                    self.report_blocks.clone(),
                    self.pending_deletes.clone(),
                );
            }

            Err(e) => {
                error!("report blocks {}", e);
                self.put_missing_report(report_blocks)
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        let state: ServerState = self.worker_ctl.state();
        state == ServerState::Stop
    }
}
