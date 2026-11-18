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

use super::block_actor::{new_report_id, send_with_retry};
use crate::worker::block::{BlockMeta, BlockStore, CorruptCheck, FullReportSnapshot, MasterClient};
use crate::worker::storage::{is_quarantined_error, Dataset};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::ProtoUtils;
use curvine_model::{BlockReportInfo, HeartbeatStatus, WorkerCommand};
use curvine_proto::{BlockReportListResponse, WorkerHeartbeatResponse};
use curvine_rpc::server::ServerState;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use curvine_runtime::sync::StateCtl;
use dashmap::{DashMap, DashSet};
use log::{error, info, warn};
use std::mem::size_of_val;
use std::sync::{Arc, Mutex};

/// Re-checks of a corrupt block that may fail with an error before it is
/// dropped. A later failing read queues it again.
const MAX_CORRUPT_CHECK_ERRORS: u32 = 3;

#[derive(Default)]
pub(crate) struct CapacityHeartbeat {
    seq: u64,
    fence: Option<String>,
    registration_required: bool,
}

impl CapacityHeartbeat {
    fn collect<T>(&mut self, collect: impl FnOnce() -> T) -> (u64, Option<String>, T) {
        self.seq = self
            .seq
            .checked_add(1)
            .expect("capacity snapshot sequence exhausted");
        let fence = self.fence.clone();
        (self.seq, fence, collect())
    }

    fn received(&mut self, fence: Option<String>) {
        self.fence = fence;
    }

    fn receive_response(&mut self, response: &WorkerHeartbeatResponse) {
        self.registration_required = response.registration_required.unwrap_or(false);
        self.received(if self.registration_required {
            None
        } else {
            response.cache_reservation_fence.clone()
        });
    }

    // One recovery attempt per heartbeat tick. A lost Start/report response
    // keeps recovery pending; successful recovery never resets the sequence.
    fn recover(
        &mut self,
        register_and_report: impl FnOnce() -> CommonResult<()>,
    ) -> CommonResult<()> {
        if self.registration_required {
            register_and_report()?;
            self.registration_required = false;
        }
        Ok(())
    }
}

pub struct HeartbeatTask {
    pub(crate) executor: Arc<GroupExecutor>,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) client: MasterClient,
    pub(crate) store: BlockStore,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    /// Also parks quarantined ids (see `delete_block_task`). The release path
    /// must remove the id, or report Deleted itself, so master's pending
    /// delete can complete.
    pub(crate) pending_deletes: Arc<DashSet<i64>>,
    pub(crate) capacity_heartbeat: Mutex<CapacityHeartbeat>,
    pub(crate) block_report_limit: usize,
}

impl HeartbeatTask {
    pub(crate) fn full_block_report(&self) -> CommonResult<usize> {
        self.client.begin_initial_full_report();
        let result = self.full_block_report_inner();
        self.client.end_initial_full_report();
        result
    }

    fn full_block_report_inner(&self) -> CommonResult<usize> {
        // Capture the Start token BEFORE the inventory, and freeze it for all
        // chunks/retries even if a later registration replaces the client's token.
        let token = self.client.full_report_token();
        let blocks = self.store.all_blocks()?;
        let report_id = new_report_id();
        if blocks.is_empty() {
            let response = self.send_full_report(report_id, 0, &[], token.as_deref())?;
            let cmds = ProtoUtils::worker_cmd_from_pb(response.cmds);
            HeartbeatTask::delete_block_task(
                self.executor.clone(),
                self.store.clone(),
                cmds,
                self.report_blocks.clone(),
                self.pending_deletes.clone(),
            );
            return Ok(0);
        }

        let mut off = 0;
        while off < blocks.len() {
            let end = (off + self.block_report_limit).min(blocks.len());
            let batch = &blocks[off..end];
            let spend = TimeSpent::new();
            let response =
                self.send_full_report(report_id, blocks.len(), batch, token.as_deref())?;
            info!(
                "full block report, blocks={}, msg_bytes={}, used_ms={}",
                batch.len(),
                size_of_val(batch),
                spend.used_ms()
            );
            let cmds = ProtoUtils::worker_cmd_from_pb(response.cmds);
            HeartbeatTask::delete_block_task(
                self.executor.clone(),
                self.store.clone(),
                cmds,
                self.report_blocks.clone(),
                self.pending_deletes.clone(),
            );
            off = end;
        }

        Ok(blocks.len())
    }

    fn send_full_report(
        &self,
        report_id: u64,
        total_len: usize,
        batch: &[BlockMeta],
        token: Option<&str>,
    ) -> CommonResult<BlockReportListResponse> {
        send_with_retry(
            "full block report",
            || self.worker_ctl.state::<ServerState>() == ServerState::Stop,
            std::thread::sleep,
            || {
                self.client
                    .full_block_report(report_id, total_len, batch, token)
            },
        )
    }

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
                            // Permanent until released: keep the id so later
                            // heartbeats skip it instead of rescheduling.
                            Err(e) if is_quarantined_error(&e) => {
                                info!("block {} is quarantined; delete deferred", block);
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

    /// Re-check blocks whose reads failed a chunk CRC or found the file
    /// truncated. A block whose damage is confirmed is removed and reported as
    /// a corrupted deletion, so master drops the location and restores the
    /// replica count.
    pub(crate) fn invalidate_corrupt_task(
        executor: Arc<GroupExecutor>,
        store: BlockStore,
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    ) {
        let blocks = store.take_corrupt_blocks();
        if blocks.is_empty() {
            return;
        }
        let store1 = store.clone();
        let blocks_back = blocks.clone();
        let res = executor.spawn(move || {
            for (block, failed_checks) in blocks {
                match store1.invalidate_corrupt_block(block) {
                    Ok(CorruptCheck::Removed(meta)) => {
                        report_blocks
                            .insert(block, BlockReportInfo::with_corrupted(block, meta.len));
                    }
                    Ok(CorruptCheck::Kept(reason)) => {
                        info!("keep block {} reported corrupt: {}", block, reason);
                    }
                    Ok(CorruptCheck::Retry(reason)) => {
                        info!("defer re-check of corrupt block {}: {}", block, reason);
                        store1.requeue_corrupt(block, failed_checks);
                    }
                    Err(e) if failed_checks + 1 < MAX_CORRUPT_CHECK_ERRORS => {
                        warn!(
                            "re-check of corrupt block {} failed, will retry: {}",
                            block, e
                        );
                        store1.requeue_corrupt(block, failed_checks + 1);
                    }
                    Err(e) => error!(
                        "re-check of corrupt block {} failed {} times, giving up: {}",
                        block, MAX_CORRUPT_CHECK_ERRORS, e
                    ),
                }
            }
        });
        if let Err(e) = res {
            warn!("failed to schedule corrupt block re-check: {}", e);
            for (block, failed_checks) in blocks_back {
                store.requeue_corrupt(block, failed_checks);
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
        Self::acknowledge(&self.store, &self.pending_deletes, blocks);
    }

    /// Master applied `blocks`: forget their pending deletes and let corrupt
    /// block ids be recreated.
    pub(crate) fn acknowledge(
        store: &BlockStore,
        pending_deletes: &DashSet<i64>,
        blocks: &[BlockReportInfo],
    ) {
        for block in blocks {
            if block.status == curvine_model::BlockReportStatus::Deleted {
                pending_deletes.remove(&block.id);
                store.unfence(block.id);
            }
        }
    }
}

impl LoopTask for HeartbeatTask {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        // Perform heartbeat sending.
        let mut capacity = self.capacity_heartbeat.lock().unwrap();
        if let Err(e) = capacity.recover(|| {
            let storages = self.store.get_and_check_storages()?;
            self.client.heartbeat(HeartbeatStatus::Start, storages)?;
            self.full_block_report()?;
            Ok(())
        }) {
            warn!("worker re-registration failed; retry next heartbeat: {}", e);
            return Ok(());
        }
        let (seq, fence, info) = capacity.collect(|| self.store.get_and_check_storages());
        let info = match info {
            Ok(info) => info,
            Err(e) => {
                error!("collect worker storage info failed {}", e);
                return Ok(());
            }
        };
        let res = self.client.heartbeat_snapshot(info, seq, fence);
        if let Ok(response) = &res {
            capacity.receive_response(response);
            if capacity.registration_required {
                // Do not execute commands or send incremental reports until
                // Start + full report has restored this master's registration.
                warn!("master requested worker re-registration");
                return Ok(());
            }
        }
        drop(capacity);
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
                Self::invalidate_corrupt_task(
                    self.executor.clone(),
                    self.store.clone(),
                    self.report_blocks.clone(),
                );
            }

            Err(e) => {
                // Wait for the next try again.
                error!("Send heartbeat failed {}", e);
                return Ok(());
            }
        };

        if self.client.needs_follower_resync() {
            let client = self.client.clone();
            let store = self.store.clone();
            let block_report_limit = self.block_report_limit;
            if let Err(e) =
                self.client
                    .resync_follower_reports(&self.executor, move |max_inventory_bytes| {
                        let block_count = store.block_count()?;
                        if FullReportSnapshot::minimum_inventory_bytes(block_count)
                            > max_inventory_bytes
                        {
                            return Ok(None);
                        }
                        let blocks = store.all_blocks()?;
                        let snapshot = client.full_report_snapshot(blocks, block_report_limit);
                        Ok(Some(snapshot))
                    })
            {
                warn!("build follower worker full report failed: {}", e);
            }
        }

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

#[cfg(test)]
mod capacity_tests {
    use super::CapacityHeartbeat;

    #[test]
    fn cache_fence_recovery_retries_lost_start_and_report_without_resetting_sequence() {
        let mut state = CapacityHeartbeat::default();
        state.collect(|| ());
        state.received(Some("old-token".into()));
        state.receive_response(&curvine_proto::WorkerHeartbeatResponse {
            registration_required: Some(true),
            ..Default::default()
        });
        assert!(state.fence.is_none());
        // Both a lost Start response and a failed full report must keep recovery
        // pending. No Running snapshot is sent until the complete attempt succeeds.
        for stage in ["Start response lost", "full report failed"] {
            let result = state.recover(|| Err(std::io::Error::other(stage).into()));
            assert!(result.is_err());
            assert!(state.registration_required);
            assert_eq!(state.seq, 1);
            assert!(state.fence.is_none());
        }
        state.recover(|| Ok(())).unwrap();
        assert!(!state.registration_required);
        assert_eq!(state.collect(|| 100), (2, None, 100));
        state
            .recover(|| panic!("successful recovery must not repeat Start"))
            .unwrap();
        state.receive_response(&curvine_proto::WorkerHeartbeatResponse {
            cache_reservation_fence: Some("new-token".into()),
            ..Default::default()
        });
        assert_eq!(state.collect(|| 100), (3, Some("new-token".into()), 100));
    }

    #[test]
    fn inventory_recovery_repeated_requests_and_lost_reports_keep_sequence_monotonic() {
        let mut state = CapacityHeartbeat::default();
        for round in 1..=8 {
            let (seq, _, ()) = state.collect(|| ());
            assert_eq!(seq, round);
            state.receive_response(&curvine_proto::WorkerHeartbeatResponse {
                registration_required: Some(true),
                cache_reservation_fence: Some("must-not-echo".into()),
                ..Default::default()
            });
            for _ in 0..3 {
                assert!(state
                    .recover(|| Err(std::io::Error::other("lost Start/report response").into()))
                    .is_err());
                assert!(state.registration_required);
                assert_eq!(state.seq, round);
            }
            state.recover(|| Ok(())).unwrap();
            assert!(!state.registration_required);
            assert!(state.fence.is_none());
            state
                .recover(|| panic!("successful recovery must not repeat spontaneously"))
                .unwrap();
        }
        assert_eq!(state.collect(|| 4), (9, None, 4));
    }

    #[test]
    fn cache_fence_echo_requires_a_new_collection() {
        let mut state = CapacityHeartbeat::default();
        let first = state.collect(|| 100);
        state.received(Some("challenge".into()));
        assert_eq!(
            first,
            (1, None, 100),
            "already sampled payload cannot acquire a new fence"
        );
        let second = state.collect(|| 100); // write and eviction cancel out
        assert_eq!(second, (2, Some("challenge".into()), 100));
        // Failed RPC: retain challenge, but sample again with a fresh sequence.
        let retry = state.collect(|| 99);
        assert_eq!(retry, (3, Some("challenge".into()), 99));
        state.received(None); // old/new master has no outstanding batch
        assert_eq!(state.collect(|| 99), (4, None, 99));
    }
    #[test]
    fn cache_fence_collects_real_worker_accounting_after_write_and_delete() {
        use crate::worker::block::BlockStore;
        use curvine_config::{ClusterConf, WorkerConf};
        use curvine_model::{ExtendedBlock, FileType, StorageType};
        let conf = ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                dir_reserved: "0".into(),
                data_dir: vec![format!(
                    "[MEM:64MB]../testing/cache-fence-{}",
                    curvine_runtime::common::Utils::rand_str(8)
                )],
                ..Default::default()
            },
            ..Default::default()
        };
        let store = BlockStore::new("cache-fence", &conf).unwrap();
        let write = |id| {
            let mut block = ExtendedBlock::new(id, 16 * 4096, StorageType::Mem, FileType::File);
            let meta = store.open_block(&block).unwrap();
            let path = store.short_circuit(&meta).unwrap().unwrap();
            std::fs::write(path, vec![7; 4096]).unwrap();
            block.len = 4096;
            store.finalize_block(&block).unwrap();
        };
        write(1);
        let mut state = CapacityHeartbeat::default();
        let (_, _, before) = state.collect(|| store.get_and_check_storages().unwrap());
        write(2);
        store.remove_block(1).unwrap();
        state.received(Some("after-completion".into()));
        let (seq, fence, after) = state.collect(|| store.get_and_check_storages().unwrap());
        assert_eq!(seq, 2);
        assert_eq!(fence.as_deref(), Some("after-completion"));
        assert_eq!(after[0].fs_used, 4096);
        assert_eq!(
            after[0].available, before[0].available,
            "net-zero availability still carries the post-completion fence"
        );
    }
}
