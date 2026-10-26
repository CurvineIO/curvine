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
use std::sync::{Arc, Mutex};

pub struct HeartbeatTask {
    pub(crate) executor: Arc<GroupExecutor>,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) client: MasterClient,
    pub(crate) store: BlockStore,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    pub(crate) pending_deletes: Arc<DashSet<i64>>,
    /// Quarantine release proofs that didn't yet match (quarantine not
    /// visible to this dataset generation, or a transient store error),
    /// retried every tick. See `Dataset::release_quarantined_block_with_proof`.
    #[cfg_attr(not(feature = "spdk"), allow(dead_code))]
    pub(crate) pending_quarantine_proofs:
        Arc<Mutex<Vec<crate::worker::storage::QuarantineReleaseProof>>>,
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

    /// Drain SPDK terminal-completion events and retry any proof that
    /// didn't match last tick (the quarantine may not have been visible yet
    /// when the event arrived — see `pending_quarantine_proofs`).
    #[cfg(feature = "spdk")]
    fn process_quarantine_terminal_events(&self) {
        use crate::worker::storage::QuarantineReleaseProof;

        let new_proofs: Vec<QuarantineReleaseProof> = curvine_storage_spdk::SpdkEnv::global()
            .map(|env| {
                env.drain_terminal_events()
                    .into_iter()
                    .map(|event| QuarantineReleaseProof {
                        block_id: event.block_id,
                        bdev_offset: event.bdev_offset,
                        size: event.size,
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut pending = match self.pending_quarantine_proofs.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        pending.extend(new_proofs);
        if pending.is_empty() {
            return;
        }

        pending.retain(|proof| {
            match self.store.release_quarantined_block_with_proof(proof) {
                Ok(true) => {
                    log::info!(
                        "released quarantined SPDK extent: block_id={} offset={} size={}",
                        proof.block_id,
                        proof.bdev_offset,
                        proof.size
                    );
                    false
                }
                // No exact match yet (or ever): keep retrying. A stale proof
                // whose block was since removed stays here harmlessly; see
                // the design doc's documented recovery-workflow limitation.
                Ok(false) => true,
                Err(e) => {
                    warn!(
                        "release_quarantined_block_with_proof failed for block {}: {}",
                        proof.block_id, e
                    );
                    true
                }
            }
        });
    }

    #[cfg(not(feature = "spdk"))]
    fn process_quarantine_terminal_events(&self) {}
}

impl LoopTask for HeartbeatTask {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        self.process_quarantine_terminal_events();

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
