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

use crate::worker::block::{
    BlockActor, BlockStore, CorruptCheck, FullReportSnapshot, MasterClient,
};
use crate::worker::storage::{is_quarantined_error, Dataset};
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::{BlockReportInfo, HeartbeatStatus, WorkerCommand};
use curvine_model::{ProtoUtils, NOT_REGISTERED_PREFIX};
use curvine_proto::WorkerHeartbeatResponse;
use curvine_rpc::server::ServerState;
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use curvine_runtime::sync::StateCtl;
use dashmap::{DashMap, DashSet};
use log::{error, info, warn};
use std::sync::Arc;

/// Re-checks of a corrupt block that may fail with an error before it is
/// dropped. A later failing read queues it again.
const MAX_CORRUPT_CHECK_ERRORS: u32 = 3;

/// Serialized with collection and response handling; retries never turn an old
/// capacity sample into an acknowledgement for a newly received token.
#[derive(Default)]
pub(crate) struct CapacityProof {
    sequence: u64,
    token: Option<String>,
    registration_required: bool,
}

impl CapacityProof {
    fn observe(&mut self, result: &CommonResult<WorkerHeartbeatResponse>) {
        match result {
            Ok(response) => self.token = response.cache_reservation_fence.clone(),
            Err(error)
                if error.downcast_ref::<FsError>().is_some_and(|error| {
                    matches!(error, FsError::InProgress(_))
                        && error.to_string().starts_with(NOT_REGISTERED_PREFIX)
                }) =>
            {
                // A former leader can retain our previous process's session
                // across failback when follower reporting is disabled.
                self.token = None;
                self.registration_required = true;
            }
            Err(_) => {}
        }
    }

    fn recover(
        &mut self,
        register_and_report: impl FnOnce() -> CommonResult<()>,
    ) -> CommonResult<()> {
        if self.registration_required {
            self.token = None;
            // Keep recovery pending on every failure, including a lost final
            // report response. Never return to Running with a partial report.
            register_and_report()?;
            self.registration_required = false;
        }
        // Keep the sequence monotonic: this is still the same worker process.
        Ok(())
    }

    fn collect<T>(&mut self, collect: impl FnOnce() -> T) -> (u64, Option<String>, T) {
        // Saturation makes later samples ineligible at the Master without
        // stopping ordinary heartbeat/liveness processing or wrapping to zero.
        self.sequence = self.sequence.saturating_add(1);
        (self.sequence, self.token.clone(), collect())
    }
}

pub struct HeartbeatTask {
    pub(crate) capacity_proof: std::sync::Mutex<CapacityProof>,
    /// Reuse startup's Start + full-report path, including batching and retries.
    pub(crate) registration: BlockActor,
    pub(crate) executor: Arc<GroupExecutor>,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) client: MasterClient,
    pub(crate) store: BlockStore,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    /// Also parks quarantined ids (see `delete_block_task`). The release path
    /// must remove the id, or report Deleted itself, so master's pending
    /// delete can complete.
    pub(crate) pending_deletes: Arc<DashSet<i64>>,
    pub(crate) block_report_limit: usize,
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
        let mut proof = self.capacity_proof.lock().unwrap();
        if let Err(error) = proof.recover(|| {
            self.registration.register()?;
            self.registration.full_block_report()?;
            Ok(())
        }) {
            error!(
                "worker re-registration failed, retry next heartbeat: {}",
                error
            );
            return Ok(());
        }
        let (sequence, token, sample) = proof.collect(|| self.store.get_and_check_storages());
        let info = match sample {
            Ok(info) => info,
            Err(e) => {
                error!("collect worker storage info failed {}", e);
                return Ok(());
            }
        };
        let res =
            self.client
                .heartbeat_snapshot(HeartbeatStatus::Running, info, Some(sequence), token);
        proof.observe(&res);
        drop(proof);
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
mod read_cache_tests {
    use super::*;
    use curvine_core_error::CommonError;
    use std::cell::Cell;

    #[test]
    fn read_cache_registration_accepts_master_errors_after_wire_round_trip() {
        use curvine_core_error::ErrorExt;
        use curvine_master::master::fs::WorkerManager;
        use curvine_model::WorkerAddress;

        // Pin the wire contract for older peers as well as both current producers.
        assert_eq!(NOT_REGISTERED_PREFIX, "NotRegistered:");
        for active in [true, false] {
            let mut conf = curvine_config::ClusterConf::default();
            // With the optimization off, exercise the independent passive producer.
            conf.master.read_cache_reservation_enabled = active;
            let mut manager = WorkerManager::new(&conf).unwrap();
            let send = |manager: &mut WorkerManager, status, session: &str| {
                let address = WorkerAddress {
                    worker_id: 1,
                    ..Default::default()
                };
                if active {
                    manager
                        .heartbeat(
                            "curvine",
                            status,
                            address,
                            1,
                            session.into(),
                            Default::default(),
                            String::new(),
                            0,
                            vec![],
                            None,
                        )
                        .map(|_| ())
                } else {
                    manager.heartbeat_passive(
                        "curvine",
                        status,
                        address,
                        1,
                        session.into(),
                        Default::default(),
                        String::new(),
                        0,
                        vec![],
                        None,
                    )
                }
            };
            send(&mut manager, HeartbeatStatus::Start, "current").unwrap();
            for status in [HeartbeatStatus::Running, HeartbeatStatus::End] {
                let error = send(&mut manager, status, "stale").unwrap_err();
                assert!(error.to_string().starts_with(NOT_REGISTERED_PREFIX));
                let decoded = FsError::decode(error.encode());
                let mut proof = CapacityProof {
                    sequence: 7,
                    token: Some("old-token".into()),
                    ..Default::default()
                };
                proof.observe(&Err(decoded.into()));
                assert!(proof.registration_required);
                assert!(proof.token.is_none());
                let recovered = Cell::new(false);
                proof
                    .recover(|| {
                        recovered.set(true);
                        Ok(())
                    })
                    .unwrap();
                assert!(recovered.get());
                assert!(!proof.registration_required);
                assert_eq!(proof.collect(|| ()), (8, None, ()));
            }
        }
    }

    #[test]
    fn read_cache_registration_requires_typed_not_registered_response() {
        let mut state = CapacityProof {
            token: Some("old-leader-token".into()),
            ..Default::default()
        };
        for error in [
            CommonError::from("NotRegistered: unrelated string"),
            FsError::common("NotRegistered: wrong error kind").into(),
            FsError::in_progress_msg("ResyncRequired: follower report").into(),
            FsError::in_progress_msg("Read-cache pending registration limit reached").into(),
            FsError::not_leader("leader changed").into(),
            std::io::Error::from(std::io::ErrorKind::TimedOut).into(),
        ] {
            state.observe(&Err(error));
            assert!(!state.registration_required);
            assert_eq!(state.token.as_deref(), Some("old-leader-token"));
            state
                .recover(|| panic!("ordinary errors must not re-register"))
                .unwrap();
        }
        state.observe(&Err(FsError::in_progress_msg(
            "NotRegistered: stale session",
        )
        .into()));
        assert!(state.registration_required);
        assert!(state.token.is_none());
        state.recover(|| Ok(())).unwrap();
        state.observe(&Ok(WorkerHeartbeatResponse::default()));
        assert!(!state.registration_required);
        assert!(
            state.token.is_none(),
            "legacy/disabled responses clear proof"
        );
    }

    #[test]
    fn read_cache_registration_retries_start_and_report_before_fresh_running() {
        let mut state = CapacityProof::default();
        let previous = state.collect(|| 100);
        state.token = Some("previous-leader-token".into());
        let starts = Cell::new(0);
        let reports = Cell::new(0);
        // Repeat the entire recovery, as in A -> B -> A -> B failback.
        for round in 0..2 {
            state.observe(&Err(FsError::in_progress_msg(
                "NotRegistered: stale session",
            )
            .into()));
            for failure in 0..3 {
                let result = state.recover(|| {
                    starts.set(starts.get() + 1);
                    if failure == 0 {
                        return Err(FsError::not_leader("lost Start reply / leader change").into());
                    }
                    reports.set(reports.get() + 1);
                    Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into())
                });
                assert!(result.is_err());
                assert!(
                    state.registration_required,
                    "partial/lost final report must retry"
                );
                assert!(state.token.is_none());
                assert_eq!(state.sequence, previous.0 + round);
            }
            state
                .recover(|| {
                    starts.set(starts.get() + 1);
                    reports.set(reports.get() + 1);
                    Ok(())
                })
                .unwrap();
            assert!(!state.registration_required);
            assert_eq!(state.collect(|| 96), (previous.0 + round + 1, None, 96));
            state
                .recover(|| panic!("completed recovery must not loop"))
                .unwrap();
        }
        assert_eq!((starts.get(), reports.get()), (8, 6));
    }

    #[test]
    fn read_cache_ack_only_accompanies_a_post_challenge_snapshot() {
        let mut state = CapacityProof::default();
        let first = state.collect(|| 100);
        assert_eq!(first, (1, None, 100));
        state.token = Some("challenge".into());
        // Retrying a saved sample cannot retroactively acknowledge the token.
        assert!(first.1.is_none());
        assert_eq!(state.collect(|| 96), (2, Some("challenge".into()), 96));
        // Lost response retains the token, but a new collection gets a new seq.
        assert_eq!(state.collect(|| 96), (3, Some("challenge".into()), 96));
        state.token = None; // legacy/disabled Master
        assert_eq!(state.collect(|| 96), (4, None, 96));
        assert_eq!(CapacityProof::default().collect(|| 96), (1, None, 96));
    }

    #[test]
    fn read_cache_sequence_exhaustion_does_not_stop_capacity_collection() {
        let mut state = CapacityProof {
            sequence: u64::MAX,
            token: Some("old".into()),
            ..Default::default()
        };
        assert_eq!(state.collect(|| 96), (u64::MAX, Some("old".into()), 96));
        assert_eq!(state.collect(|| 92), (u64::MAX, Some("old".into()), 92));
    }
}
