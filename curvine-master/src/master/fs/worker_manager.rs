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

use crate::master::fs::policy::{ChooseContext, WorkerPolicyAdapter};
use crate::master::fs::state::{BlockMap, WorkerMap};
use crate::master::fs::DeleteResult;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::{FsError, FsResult};
use curvine_model::{
    BlockLocation, CommitBlock, ExtendedBlock, HeartbeatStatus, LocatedBlock, StorageInfo,
    StorageType, TransferWorkerCapabilities, WorkerAddress, WorkerCommand, WorkerInfo,
    WorkerStatus, NOT_REGISTERED_PREFIX,
};
use curvine_proto::ComponentInfoProto;
use curvine_runtime::common::{ByteUnit, LocalTime};
use log::{info, warn};
use std::collections::{BTreeSet, HashMap};
use std::fmt::{Display, Formatter};

pub struct WorkerManager {
    pub(crate) worker_map: WorkerMap,
    pub(crate) block_map: BlockMap,
    pub(crate) worker_policy: WorkerPolicyAdapter,
    pub(crate) cluster_id: String,
    pub(crate) conf: ClusterConf,
    // Start removes the active registration until its first Running heartbeat.
    // Remember its session so an old process cannot occupy that empty slot.
    pending_read_cache_sessions: HashMap<u32, String>,
}

const READ_CACHE_PENDING_START_LIMIT: usize = 65_536;

/// A committed replica to settle: `(block_id, block_len, worker_id)`.
pub(crate) type SettleEntry = (i64, i64, u32);

impl WorkerManager {
    #[cfg(test)]
    pub(crate) fn fill_read_cache_pending_starts_for_test(&mut self, start_id: u32) {
        for id in start_id..start_id + READ_CACHE_PENDING_START_LIMIT as u32 {
            self.pending_read_cache_sessions
                .insert(id, "current".into());
        }
    }
    /// Validate a follower Start before its passive readiness state is
    /// invalidated. The heartbeat path repeats these checks before mutation.
    pub fn validate_passive_start(&self, cluster_id: &str, addr: &WorkerAddress) -> FsResult<()> {
        if cluster_id != self.cluster_id {
            return err_box!(
                "Registered cluster_id mismatch, expected {}, actual: {}",
                self.cluster_id,
                cluster_id
            );
        }
        self.worker_map.ensure_worker_id_addr(addr)
    }

    pub fn new(conf: &ClusterConf) -> FsResult<Self> {
        let worker_policy = WorkerPolicyAdapter::from_conf(conf)?;

        Ok(Self {
            worker_map: WorkerMap::new(),
            block_map: BlockMap::new(),
            worker_policy,
            cluster_id: conf.cluster_id.to_string(),
            conf: conf.clone(),
            pending_read_cache_sessions: HashMap::new(),
        })
    }

    /// The cluster id of a heartbeat or report must match, so a node or a
    /// report that belongs to another cluster cannot change this cluster's
    /// state.
    pub(crate) fn check_cluster_id(&self, cluster_id: &str) -> CommonResult<()> {
        if cluster_id != self.cluster_id {
            return err_box!(
                "cluster_id mismatch, expected {}, actual: {}",
                self.cluster_id,
                cluster_id
            );
        }
        Ok(())
    }

    /// Only the opt-in, sequenced capacity view is fenced here. Start remains
    /// the existing registration boundary; no retired-session history is kept.
    pub(crate) fn validate_read_cache_session(
        &self,
        status: HeartbeatStatus,
        worker_id: u32,
        session: &str,
    ) -> FsResult<()> {
        if !self.conf.master.read_cache_reservation_enabled
            || !matches!(status, HeartbeatStatus::Running | HeartbeatStatus::End)
        {
            return Ok(());
        }
        let expected = self
            .pending_read_cache_sessions
            .get(&worker_id)
            .or_else(|| {
                self.worker_map
                    .workers
                    .get(&worker_id)
                    .or_else(|| self.worker_map.lost_workers.get(&worker_id))
                    .filter(|worker| {
                        !worker.worker_session_id.is_empty()
                            && !worker.accepts_read_cache_capacity(None)
                    })
                    .map(|worker| &worker.worker_session_id)
            });
        if expected.is_some_and(|expected| expected != session) {
            return Err(FsError::in_progress_msg(format!(
                "{NOT_REGISTERED_PREFIX} worker {worker_id} heartbeat does not match the registered session"
            )));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn heartbeat(
        &mut self,
        cluster_id: &str,
        status: HeartbeatStatus,
        addr: WorkerAddress,
        weight: u32,
        worker_session_id: String,
        transfer_capabilities: TransferWorkerCapabilities,
        software_version: String,
        startup_time_ms: u64,
        storages: Vec<StorageInfo>,
        component_info: Option<ComponentInfoProto>,
    ) -> FsResult<Vec<WorkerCommand>> {
        self.heartbeat_impl(
            cluster_id,
            status,
            addr,
            weight,
            worker_session_id,
            transfer_capabilities,
            software_version,
            startup_time_ms,
            storages,
            component_info,
            true,
        )
    }

    /// Update the follower's local worker registration view without evaluating
    /// or returning leader-only block deletion commands.
    #[allow(clippy::too_many_arguments)]
    pub fn heartbeat_passive(
        &mut self,
        cluster_id: &str,
        status: HeartbeatStatus,
        addr: WorkerAddress,
        weight: u32,
        worker_session_id: String,
        transfer_capabilities: TransferWorkerCapabilities,
        software_version: String,
        startup_time_ms: u64,
        storages: Vec<StorageInfo>,
        component_info: Option<ComponentInfoProto>,
    ) -> FsResult<()> {
        self.heartbeat_impl(
            cluster_id,
            status,
            addr,
            weight,
            worker_session_id,
            transfer_capabilities,
            software_version,
            startup_time_ms,
            storages,
            component_info,
            false,
        )
        .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    fn heartbeat_impl(
        &mut self,
        cluster_id: &str,
        status: HeartbeatStatus,
        addr: WorkerAddress,
        weight: u32,
        worker_session_id: String,
        transfer_capabilities: TransferWorkerCapabilities,
        software_version: String,
        startup_time_ms: u64,
        storages: Vec<StorageInfo>,
        component_info: Option<ComponentInfoProto>,
        include_leader_commands: bool,
    ) -> FsResult<Vec<WorkerCommand>> {
        // The cluster id must match to prevent misregistration.
        self.check_cluster_id(cluster_id)?;
        self.validate_read_cache_session(status, addr.worker_id, &worker_session_id)?;

        if !include_leader_commands
            && matches!(status, HeartbeatStatus::Running | HeartbeatStatus::End)
        {
            self.worker_map.ensure_worker_id_addr(&addr)?;
            if self
                .worker_map
                .workers()
                .get(&addr.worker_id)
                .or_else(|| self.worker_map.lost_workers().get(&addr.worker_id))
                .is_some_and(|worker| {
                    worker.address != addr || worker.worker_session_id != worker_session_id
                })
            {
                return Err(FsError::in_progress_msg(format!(
                    "{NOT_REGISTERED_PREFIX} worker {} must register its current session before sending passive heartbeats",
                    addr.worker_id
                )));
            }
        }

        let cmds = match status {
            HeartbeatStatus::Start => {
                info!("Worker register: {}", addr);
                // Enforce the same worker_id ↔ address rule as insert() before remove(): a Start
                // from a conflicting address must not evict the live registration.
                self.worker_map.ensure_worker_id_addr(&addr)?;
                let same_session = !worker_session_id.is_empty()
                    && self
                        .worker_map
                        .workers()
                        .get(&addr.worker_id)
                        .is_some_and(|worker| {
                            worker.address == addr && worker.worker_session_id == worker_session_id
                        });
                if self.conf.master.read_cache_reservation_enabled && !same_session {
                    if !worker_session_id.is_empty() {
                        if self.pending_read_cache_sessions.len() >= READ_CACHE_PENDING_START_LIMIT
                            && !self
                                .pending_read_cache_sessions
                                .contains_key(&addr.worker_id)
                        {
                            return Err(FsError::in_progress_msg(
                                "Read-cache pending registration limit reached; retry Start",
                            ));
                        }
                        self.pending_read_cache_sessions
                            .insert(addr.worker_id, worker_session_id.clone());
                    } else {
                        self.pending_read_cache_sessions.remove(&addr.worker_id);
                    }
                }
                if !include_leader_commands {
                    // Workers emit their full report after Start but before
                    // the first Running heartbeat. Register this provisional
                    // follower view so those reports are admitted, without
                    // consulting or mutating the leader-only BlockMap.
                    self.worker_map.insert(
                        addr,
                        weight,
                        worker_session_id,
                        transfer_capabilities,
                        software_version,
                        startup_time_ms,
                        storages,
                        component_info,
                    )?;
                    return Ok(vec![]);
                }
                if same_session {
                    // A Start can arrive while a worker is in a promotion
                    // grace period after being marked Lost by a follower.
                    // Refresh the timeout origin so a slow initial full report
                    // cannot be cleaned up before its first Running heartbeat.
                    // Keep its status unchanged until that Running heartbeat
                    // confirms the worker is ready again.
                    if let Some(worker) = self.worker_map.workers.get_mut(&addr.worker_id) {
                        worker.last_update = LocalTime::mills();
                    }
                    return Ok(vec![]);
                }
                for stale in self.worker_map.remove_same_endpoint(&addr) {
                    warn!(
                        "Remove stale worker {} on restart endpoint {}",
                        stale.simple_debug(),
                        addr
                    );
                }
                // Same node restarting: clear the slot so we do not treat it as ready or run
                // Running heartbeat bookkeeping until insert() on the next Running beat.
                self.worker_map.remove(&addr);
                return Ok(vec![]);
            }

            HeartbeatStatus::Running if include_leader_commands => {
                self.block_map.handle_heartbeat(addr.worker_id)
            }

            HeartbeatStatus::Running => vec![],

            HeartbeatStatus::End => {
                info!("Worker unregister: {}", addr);
                let _ = self.worker_map.remove_offline(addr.worker_id);
                self.pending_read_cache_sessions.remove(&addr.worker_id);
                return Ok(vec![]);
            }
        };

        let worker_id = addr.worker_id;
        self.worker_map.insert(
            addr,
            weight,
            worker_session_id,
            transfer_capabilities,
            software_version,
            startup_time_ms,
            storages,
            component_info,
        )?;
        self.pending_read_cache_sessions.remove(&worker_id);
        if include_leader_commands {
            self.expire_scheduled_bytes();
        }
        Ok(cmds)
    }

    pub fn choose_worker(&mut self, ctx: ChooseContext) -> CommonResult<Vec<WorkerAddress>> {
        self.expire_scheduled_bytes();
        let replicas = ctx.replicas;
        let block_size = ctx.block_size;
        let workers = self.worker_policy.choose(self.worker_map.workers(), ctx)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > replicas as usize {
            err_box!("The number of workers exceeds the number of replicas")
        } else {
            self.schedule_chosen_workers(&workers, block_size);
            Ok(workers)
        }
    }

    pub(crate) fn unschedule_chosen_workers(&mut self, workers: &[WorkerAddress], block_size: i64) {
        self.adjust_chosen_workers(workers, block_size, false);
    }

    /// Binds the reservation taken by `choose_worker` to the allocated block
    /// so commit-time settlement releases at most this block's own credit.
    pub(crate) fn bind_scheduled_blocks(
        &mut self,
        workers: &[WorkerAddress],
        block_id: i64,
        block_size: i64,
    ) {
        if block_size <= 0 {
            return;
        }
        for addr in workers {
            if let Some(worker) = self.worker_map.workers.get_mut(&addr.worker_id) {
                worker.bind_scheduled_block(block_id, block_size);
            }
        }
    }

    /// Settles reservations for committed blocks.
    ///
    /// Allocation reserves a full `block_size` per replica and binds that
    /// credit to the block id (`bind_scheduled_blocks`). The remainder would
    /// otherwise stay reserved until the heartbeat reclaim (which only
    /// reclaims bytes actually written) or the lost-interval expiry, so a
    /// burst of small files could reserve the whole cluster and starve new
    /// allocations. Commit therefore releases the credit the block still owns
    /// above its final length. Credit a heartbeat already reclaimed, or
    /// absent because the commit never re-reserved (`FsWriterBase` reopening
    /// an existing block, repeated flushes), settles nothing, so other
    /// blocks' reservations stay intact.
    ///
    /// Releasing an unwritten pre-allocated block (`release_block_id`) goes
    /// through the same entries with `block_len = 0`: exactly the credit that
    /// block still owns comes back, and credit a heartbeat already reclaimed
    /// stays reclaimed.
    pub(crate) fn settle_committed_blocks(&mut self, settles: &[SettleEntry]) {
        for &(block_id, block_len, worker_id) in settles {
            if let Some(worker) = self.worker_map.workers.get_mut(&worker_id) {
                worker.settle_scheduled_block(block_id, block_len);
            }
        }
    }

    /// Builds the settlement snapshot for committed blocks: one flat
    /// allocation regardless of block count, so callers can snapshot under
    /// the namespace write lock before `commit_blocks` is moved into the
    /// journal. SPDK replicas are filtered out here and keep their credit:
    /// the bdev extent stays fully allocated regardless of `block_len`
    /// (`BlockMeta::with_final_spdk`), so the reservation is left for the
    /// heartbeat to reclaim once the worker reports the reduced available
    /// space.
    pub(crate) fn settle_snapshot(commit_blocks: &[CommitBlock]) -> Vec<SettleEntry> {
        commit_blocks
            .iter()
            .flat_map(|commit| {
                commit
                    .locations
                    .iter()
                    .filter(|loc| loc.storage_type != StorageType::SpdkDisk)
                    .map(move |loc| (commit.block_id, commit.block_len, loc.worker_id))
            })
            .collect()
    }

    /// Snapshots the chosen workers of a pre-allocated block, so a later
    /// release can settle that block's credit on exactly those workers.
    pub(crate) fn schedule_stamps(&self, workers: &[WorkerAddress]) -> Vec<(u32, u64)> {
        workers
            .iter()
            .filter_map(|addr| {
                let worker = self.worker_map.workers.get(&addr.worker_id)?;
                Some((addr.worker_id, worker.scheduled_since_ms))
            })
            .collect()
    }

    fn expire_scheduled_bytes(&mut self) {
        let timeout_ms = self.conf.master.worker_lost_interval_ms();
        if timeout_ms == 0 {
            return;
        }
        let now_ms = LocalTime::mills();
        for worker in self.worker_map.workers.values_mut() {
            worker.expire_scheduled_bytes(now_ms, timeout_ms);
        }
    }

    fn schedule_chosen_workers(&mut self, workers: &[WorkerAddress], block_size: i64) {
        self.adjust_chosen_workers(workers, block_size, true);
    }

    fn adjust_chosen_workers(
        &mut self,
        workers: &[WorkerAddress],
        block_size: i64,
        schedule: bool,
    ) {
        if block_size <= 0 {
            return;
        }
        for addr in workers {
            if let Some(worker) = self.worker_map.workers.get_mut(&addr.worker_id) {
                if schedule {
                    worker.schedule_bytes(block_size);
                } else {
                    worker.unschedule_bytes(block_size);
                }
            }
        }
    }

    /// Select the specified number of workers, do not rely on block information
    pub fn choose_workers(
        &self,
        count: usize,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let workers =
            self.worker_policy
                .choose_workers(self.worker_map.workers(), count, exclude_workers)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > count {
            err_box!("The number of workers exceeds the requested count")
        } else {
            Ok(workers)
        }
    }

    pub fn get_last_heartbeat(&self) -> Vec<(u32, u64)> {
        let mut res = vec![];
        for worker in self.worker_map.workers() {
            res.push((*worker.0, worker.1.last_update));
        }
        res
    }

    pub fn available_bytes(&self) -> i64 {
        self.worker_map
            .workers()
            .values()
            .filter(|worker| worker.is_live())
            .map(|worker| worker.allocatable_available().max(0))
            .fold(0, i64::saturating_add)
    }

    pub fn remove_expired_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        self.worker_map.remove_expired(id)
    }

    /// Mark an expired worker as unusable on a follower while retaining its
    /// heartbeat record. If this node is promoted before the next check, the
    /// active path still sees the expired worker and can run replicated
    /// location cleanup.
    pub fn mark_passive_worker_expired(&mut self, id: u32) -> Option<WorkerInfo> {
        let worker = self.worker_map.workers.get_mut(&id)?;
        if worker.status == WorkerStatus::Lost {
            return None;
        }
        worker.status = WorkerStatus::Lost;
        Some(worker.clone())
    }

    pub fn remove_expired_worker_if_stale(
        &mut self,
        id: u32,
        now: u64,
        baseline: u64,
        lost_ms: u64,
    ) -> Option<WorkerInfo> {
        match self.worker_map.workers.get(&id) {
            Some(worker) if now > worker.last_update.max(baseline) + lost_ms => {
                self.worker_map.remove_expired(id)
            }
            _ => None,
        }
    }

    pub fn add_blacklist_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        match self.worker_map.workers.get_mut(&id) {
            Some(v) if v.status == WorkerStatus::Live => {
                v.status = WorkerStatus::Blacklist;
                Some(v.clone())
            }

            _ => None,
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.remove_block(worker_id, block_id)
    }

    // Indicates the block that needs to be deleted.
    pub fn remove_blocks(&mut self, del_res: &DeleteResult) {
        self.block_map.remove_blocks(del_res)
    }

    pub fn deleted_block(&mut self, worker_id: u32, block_id: i64) {
        // Scheduling deletion is not proof that the worker has stopped using
        // space. Settle only after its Deleted report matches a pending request,
        // including the case where writer cancellation already removed the block.
        if self.block_map.deleted_block(worker_id, block_id) {
            if let Some(worker) = self.worker_map.workers.get_mut(&worker_id) {
                worker.settle_scheduled_block(block_id, 0);
            }
        }
    }

    pub fn get_worker(&self, id: u32) -> Option<&WorkerInfo> {
        self.worker_map.workers.get(&id)
    }

    pub fn get_known_worker(&self, id: u32) -> Option<&WorkerInfo> {
        self.worker_map
            .workers
            .get(&id)
            .or_else(|| self.worker_map.lost_workers().get(&id))
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        let mut addrs = Vec::with_capacity(locs.len());
        let mut live_storage_types = Vec::with_capacity(locs.len());
        for loc in locs {
            match self.get_worker(loc.worker_id) {
                Some(info) if info.is_live() => {
                    addrs.push(info.address.clone());
                    live_storage_types.push(loc.storage_type);
                }
                Some(info) => {
                    warn!(
                        "File {} block {}, worker {} is not alive (status={:?}), replica excluded",
                        path.as_ref(),
                        block.id,
                        loc.worker_id,
                        info.status
                    );
                }
                None => {
                    warn!(
                        "File {} block {}, worker {} replicas has been lost",
                        path.as_ref(),
                        block.id,
                        loc.worker_id
                    );
                }
            }
        }

        if addrs.is_empty() && !locs.is_empty() {
            return err_box!(
                "File {} block {}, all replicas has been lost",
                path.as_ref(),
                block.id
            );
        }

        let has_spdk = live_storage_types.contains(&StorageType::SpdkDisk);
        let lb = LocatedBlock {
            block,
            locs: addrs,
            has_spdk,
        };

        Ok(lb)
    }

    pub fn workers_have_spdk(&self, addrs: &[WorkerAddress]) -> bool {
        for addr in addrs {
            if let Some(info) = self.get_worker(addr.worker_id) {
                if info
                    .storage_map
                    .values()
                    .any(|s| s.storage_type == StorageType::SpdkDisk)
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn add_test_worker(&mut self, worker: WorkerInfo) {
        self.worker_map.workers.insert(worker.worker_id(), worker);
    }

    pub fn add_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let hosts: BTreeSet<String> = list.into_iter().collect();
        let mut res = vec![];
        for host in hosts {
            for worker in self.host_workers_mut(&host) {
                worker.status = WorkerStatus::Decommission;
            }
            res.extend(self.host_dcm_entries(&host, WorkerStatus::Decommission));
            self.worker_map.decommissioned_hosts.insert(host);
        }
        res
    }

    pub fn get_dcm(&self) -> Vec<String> {
        let hosts: BTreeSet<&String> = self.worker_map.decommissioned_hosts.iter().collect();
        hosts
            .into_iter()
            .flat_map(|host| self.host_dcm_entries(host, WorkerStatus::Decommission))
            .collect()
    }

    pub fn remove_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let hosts: BTreeSet<String> = list.into_iter().collect();
        let mut res = vec![];
        for host in hosts {
            if !self.worker_map.decommissioned_hosts.remove(&host) {
                continue;
            }
            for worker in self.host_workers_mut(&host) {
                if worker.status == WorkerStatus::Decommission {
                    worker.status = WorkerStatus::Live;
                }
            }
            res.extend(self.host_dcm_entries(&host, WorkerStatus::Live));
        }
        res
    }

    fn host_workers_mut<'a>(
        &'a mut self,
        host: &'a str,
    ) -> impl Iterator<Item = &'a mut WorkerInfo> + 'a {
        let wm = &mut self.worker_map;
        wm.workers
            .values_mut()
            .chain(wm.lost_workers.values_mut())
            .filter(move |worker| worker.address.hostname == host)
    }

    // Offline hosts fall back to lost entries, then to "-,<host>,<status>" in the same shape.
    fn host_dcm_entries(&self, host: &str, status: WorkerStatus) -> Vec<String> {
        let on_host = |worker: &&WorkerInfo| worker.address.hostname == host;
        let mut res: Vec<String> = self
            .worker_map
            .workers
            .values()
            .filter(on_host)
            .map(|worker| worker.simple_string())
            .collect();
        if res.is_empty() {
            res = self
                .worker_map
                .lost_workers
                .values()
                .filter(on_host)
                .map(|worker| worker.simple_string())
                .collect();
        }
        if res.is_empty() {
            res.push(format!("-,{},{:?}", host, status));
        }
        res
    }
}

impl Display for WorkerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut str = String::new();
        for (_, item) in self.worker_map.workers() {
            let s = format!(
                "worker_id={}, address={}, capacity={}, available={}\n",
                item.worker_id(),
                item.address,
                ByteUnit::byte_to_string(item.capacity as u64),
                ByteUnit::byte_to_string(item.available as u64),
            );
            str.push_str(&s)
        }

        write!(f, "{}", str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_error::ErrorKind;

    #[test]
    fn read_cache_pending_start_is_bounded_and_matching_completion_releases_slot() {
        let mut conf = ClusterConf::default();
        conf.master.read_cache_reservation_enabled = true;
        let mut wm = WorkerManager::new(&conf).unwrap();
        wm.fill_read_cache_pending_starts_for_test(0);
        let send = |wm: &mut WorkerManager, id, status, session: &str| {
            wm.heartbeat(
                "curvine",
                status,
                WorkerAddress {
                    worker_id: id,
                    hostname: id.to_string(),
                    ..Default::default()
                },
                1,
                session.into(),
                Default::default(),
                String::new(),
                0,
                vec![],
                None,
            )
        };
        let next = READ_CACHE_PENDING_START_LIMIT as u32;
        assert!(send(&mut wm, next, HeartbeatStatus::Start, "next").is_err());
        assert!(send(&mut wm, 0, HeartbeatStatus::Start, "current").is_ok());
        assert!(send(&mut wm, 0, HeartbeatStatus::Running, "old").is_err());
        assert!(send(&mut wm, 0, HeartbeatStatus::End, "old").is_err());
        assert_eq!(
            wm.pending_read_cache_sessions.len(),
            READ_CACHE_PENDING_START_LIMIT
        );
        send(&mut wm, 0, HeartbeatStatus::Running, "current").unwrap();
        assert!(!wm.pending_read_cache_sessions.contains_key(&0));
        send(&mut wm, next, HeartbeatStatus::Start, "next").unwrap();
        send(&mut wm, next, HeartbeatStatus::End, "next").unwrap();
        assert!(!wm.pending_read_cache_sessions.contains_key(&next));
    }

    fn worker_with_available(worker_id: u32, available: i64) -> WorkerInfo {
        WorkerInfo {
            address: WorkerAddress {
                worker_id,
                ..Default::default()
            },
            available,
            ..Default::default()
        }
    }

    #[test]
    fn available_bytes_clamps_negative_values_and_saturates() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        manager.add_test_worker(worker_with_available(1, -10));
        manager.add_test_worker(worker_with_available(2, 20));
        assert_eq!(manager.available_bytes(), 20);

        manager.add_test_worker(worker_with_available(3, i64::MAX));
        assert_eq!(manager.available_bytes(), i64::MAX);
    }

    #[test]
    fn available_bytes_subtracts_scheduled_bytes() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        let mut worker = worker_with_available(1, 20);
        worker.scheduled_bytes = 5;
        manager.add_test_worker(worker);
        assert_eq!(manager.available_bytes(), 15);
    }

    #[test]
    fn create_locate_block_excludes_non_live_workers() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        manager.add_test_worker(worker_with_available(1, 100));
        let mut blacklisted = worker_with_available(2, 100);
        blacklisted.status = WorkerStatus::Blacklist;
        manager.add_test_worker(blacklisted);
        let mut decommissioned = worker_with_available(3, 100);
        decommissioned.status = WorkerStatus::Decommission;
        manager.add_test_worker(decommissioned);

        let block = ExtendedBlock::new(7, 128, StorageType::Disk, curvine_model::FileType::File);
        let locs = vec![
            BlockLocation::with_id(1),
            BlockLocation::with_id(2),
            BlockLocation::with_id(3),
            BlockLocation::with_id(4),
        ];
        let located = manager.create_locate_block("/a", block, &locs).unwrap();
        assert_eq!(located.locs.len(), 1);
        assert_eq!(located.locs[0].worker_id, 1);
    }

    #[test]
    fn create_locate_block_errors_when_no_live_replica_remains() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        let mut blacklisted = worker_with_available(2, 100);
        blacklisted.status = WorkerStatus::Blacklist;
        manager.add_test_worker(blacklisted);

        let block = ExtendedBlock::new(7, 128, StorageType::Disk, curvine_model::FileType::File);
        let err = manager
            .create_locate_block("/a", block, &[BlockLocation::with_id(2)])
            .unwrap_err();
        assert!(err.to_string().contains("all replicas has been lost"));
    }

    #[test]
    fn available_bytes_ignores_non_live_workers() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        let mut blacklisted = worker_with_available(1, 100);
        blacklisted.status = WorkerStatus::Blacklist;
        manager.add_test_worker(blacklisted);
        manager.add_test_worker(worker_with_available(2, 20));
        assert_eq!(manager.available_bytes(), 20);
    }

    fn robin_manager() -> WorkerManager {
        let mut conf = ClusterConf::default();
        conf.master.worker_policy = "robin".to_string();
        WorkerManager::new(&conf).unwrap()
    }

    fn storage(capacity: i64, available: i64) -> StorageInfo {
        StorageInfo {
            storage_id: "disk-0".to_string(),
            capacity,
            available,
            ..Default::default()
        }
    }

    #[test]
    fn choose_worker_stops_after_scheduled_bytes_fill_available() {
        // 1 GiB remaining / 128 MiB blocks => 8 allocations then skip the node.
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        manager.add_test_worker(worker);

        for i in 0..8 {
            let chosen = manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
            assert_eq!(chosen[0].worker_id, 1, "allocation {i}");
        }

        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());
        assert_eq!(
            manager.get_worker(1).unwrap().scheduled_bytes,
            8 * block_size
        );
    }

    #[test]
    fn deleted_block_settles_only_the_confirmed_replica() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        for id in [1, 2] {
            manager.add_test_worker(worker_with_available(id, 1 << 30));
        }
        for block_id in [10, 20] {
            let workers = manager
                .choose_worker(ChooseContext::with_num(2, block_size, vec![]))
                .unwrap();
            manager.bind_scheduled_blocks(&workers, block_id, block_size);
        }
        manager.remove_blocks(&DeleteResult {
            blocks: [(
                10,
                vec![BlockLocation::with_id(1), BlockLocation::with_id(2)],
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        });

        // Queuing deletion and sending commands must not release capacity.
        for id in [1, 2] {
            assert!(!manager.block_map.handle_heartbeat(id).is_empty());
            assert_eq!(
                manager.get_worker(id).unwrap().scheduled_bytes,
                2 * block_size
            );
        }
        manager.deleted_block(1, 10);
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
        assert_eq!(
            manager.get_worker(2).unwrap().scheduled_bytes,
            2 * block_size
        );
        manager.deleted_block(1, 10);
        manager.deleted_block(99, 10);
        manager.deleted_block(1, 20); // Not requested: preserve this live block.
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
        manager.deleted_block(2, 10);
        assert_eq!(manager.get_worker(2).unwrap().scheduled_bytes, block_size);

        // Even if the same id acquires fresh credit, a duplicate old report
        // without a pending delete cannot consume it.
        let workers = manager
            .choose_worker(ChooseContext::with_num(2, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&workers, 10, block_size);
        manager.deleted_block(1, 10);
        assert_eq!(
            manager.get_worker(1).unwrap().scheduled_bytes,
            2 * block_size
        );
    }

    #[test]
    fn deleted_block_settles_remaining_credit_after_heartbeat() {
        let block_size = 128 << 20;
        for consumed in [0, block_size / 2, block_size] {
            let mut manager = robin_manager();
            let available = 1 << 30;
            manager.add_test_worker(worker_with_available(1, available));
            for block_id in [10, 20] {
                let workers = manager
                    .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                    .unwrap();
                manager.bind_scheduled_blocks(&workers, block_id, block_size);
            }
            let worker = manager.worker_map.workers.get_mut(&1).unwrap();
            worker.available -= consumed;
            worker.reclaim_scheduled_from_heartbeat(available);
            assert_eq!(worker.scheduled_bytes, 2 * block_size - consumed);

            manager.remove_block(1, 10);
            manager.deleted_block(1, 10);
            let worker = manager.get_worker(1).unwrap();
            assert_eq!(worker.scheduled_bytes, block_size);
            assert!(!worker.scheduled_blocks.contains_key(&10));
            assert_eq!(worker.scheduled_blocks.get(&20).unwrap().1, block_size);
        }
    }

    #[test]
    fn deleted_block_restores_allocation_without_waiting_for_expiry() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, block_size));
        let workers = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&workers, 10, block_size);
        manager.remove_block(1, 10);
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());

        manager.deleted_block(1, 10);
        assert_eq!(manager.available_bytes(), block_size);
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_ok());
    }

    #[test]
    fn deleted_block_without_credit_or_worker_preserves_other_budget() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, 1 << 30));
        let workers = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&workers, 20, block_size);

        manager.remove_block(1, 10); // Its reservation is already gone.
        manager.remove_block(99, 10); // Its worker is no longer known.
        manager.deleted_block(1, 10);
        manager.deleted_block(99, 10);
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
        assert!(!manager.block_map.deleted_block(99, 10));
    }

    #[test]
    fn settle_committed_blocks_releases_unused_reservation() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, 1 << 40));

        // A small file reserves a full block but only writes 4 KiB.
        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);

        let commit = CommitBlock {
            block_id: 1,
            block_len: 4096,
            locations: vec![BlockLocation::with_id(1)],
        };
        manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 4096);
    }

    #[test]
    fn settle_committed_blocks_keeps_full_block_reservation() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, 1 << 40));

        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);

        // A fully written block settles nothing; the heartbeat reclaims it.
        let commit = CommitBlock {
            block_id: 1,
            block_len: block_size,
            locations: vec![BlockLocation::with_id(1)],
        };
        manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
    }

    #[test]
    fn settle_without_bound_credit_is_noop() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, 1 << 40));

        // Block 1 holds the only reservation.
        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);

        // An existing-block rewrite commits block 2 without a new AddBlock
        // reservation; it must not consume block 1's credit. Unknown workers
        // are ignored.
        let commit = CommitBlock {
            block_id: 2,
            block_len: 0,
            locations: vec![BlockLocation::with_id(1), BlockLocation::with_id(99)],
        };
        manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
    }

    #[test]
    fn settle_after_heartbeat_reclaim_keeps_other_reservation() {
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        // Block 1 reserves, then a heartbeat observes the worker-side
        // temporary reservation and reclaims the whole credit before commit.
        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);

        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, available - block_size)],
                None,
            )
            .unwrap();
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);

        // Block 2 reserves afterwards; block 1's late small commit must not
        // consume block 2's credit.
        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 2, block_size);

        let commit = CommitBlock {
            block_id: 1,
            block_len: 4096,
            locations: vec![BlockLocation::with_id(1)],
        };
        manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
    }

    #[test]
    fn settle_repeated_flush_releases_credit_once() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, 1 << 40));

        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);

        // Each flush commits the same growing block; the credit converges to
        // the committed length instead of releasing `block_size - len` again.
        for block_len in [4096, 4096, 8192] {
            let commit = CommitBlock {
                block_id: 1,
                block_len,
                locations: vec![BlockLocation::with_id(1)],
            };
            manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        }
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 4096);
    }

    #[test]
    fn settle_skips_spdk_locations() {
        let mut manager = robin_manager();
        let block_size = 128 << 20;
        manager.add_test_worker(worker_with_available(1, 1 << 40));

        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);

        // An SPDK extent stays fully allocated after a small commit, so the
        // reservation must be left for the heartbeat to reclaim.
        let commit = CommitBlock {
            block_id: 1,
            block_len: 4096,
            locations: vec![BlockLocation::new(1, StorageType::SpdkDisk)],
        };
        manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);
    }

    #[test]
    fn choose_workers_without_block_size_does_not_schedule() {
        let mut manager = robin_manager();
        manager.add_test_worker(worker_with_available(1, 1 << 30));
        manager.choose_workers(1, vec![]).unwrap();
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
    }

    #[test]
    fn choose_worker_zero_block_size_does_not_schedule() {
        let mut manager = robin_manager();
        manager.add_test_worker(worker_with_available(1, 0));
        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, 0, vec![]))
            .unwrap();
        assert_eq!(chosen[0].worker_id, 1);
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
    }

    #[test]
    fn heartbeat_keeps_scheduled_bytes_when_available_unchanged() {
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        for _ in 0..8 {
            manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
        }

        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, available)],
                None,
            )
            .unwrap();

        assert_eq!(
            manager.get_worker(1).unwrap().scheduled_bytes,
            8 * block_size
        );
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());
    }

    #[test]
    fn passive_heartbeat_does_not_return_leader_delete_commands() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 1,
            ..Default::default()
        };
        manager.block_map.remove_block(1, 42);

        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(1).is_some());

        let leader = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(leader.len(), 1);
    }

    #[test]
    fn same_nonempty_session_start_is_idempotent_and_new_or_legacy_sessions_reset() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 1,
            ..Default::default()
        };
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                "session-1".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        manager
            .worker_map
            .workers
            .get_mut(&1)
            .unwrap()
            .scheduled_bytes = 64;

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "session-1".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(1).is_some());
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 64);

        {
            let worker = manager.worker_map.workers.get_mut(&1).unwrap();
            worker.status = WorkerStatus::Lost;
            worker.last_update = 1;
        }
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "session-1".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        let worker = manager.get_worker(1).unwrap();
        assert_eq!(worker.status, WorkerStatus::Lost);
        assert!(
            worker.last_update > 1,
            "same-session Start refreshes liveness"
        );

        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "session-2".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(1).is_some());
        assert_eq!(
            manager.get_worker(1).unwrap().worker_session_id,
            "session-2"
        );

        let new_worker_addr = WorkerAddress {
            worker_id: 2,
            ..Default::default()
        };
        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Start,
                new_worker_addr,
                1,
                "session-3".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(
            manager.get_worker(2).unwrap().worker_session_id,
            "session-3"
        );

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "session-2".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(1).is_none());

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(1).is_none());
    }

    #[test]
    fn passive_running_and_end_heartbeats_must_match_the_registered_session() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 1,
            ..Default::default()
        };
        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        for status in [HeartbeatStatus::Running, HeartbeatStatus::End] {
            let error = manager
                .heartbeat_passive(
                    &cluster_id,
                    status,
                    addr.clone(),
                    1,
                    "previous-session".to_string(),
                    TransferWorkerCapabilities::default(),
                    String::new(),
                    0,
                    vec![],
                    None,
                )
                .unwrap_err();
            assert!(matches!(error.kind(), ErrorKind::InProgress));
            assert!(error.to_string().starts_with(NOT_REGISTERED_PREFIX));
            assert_eq!(
                manager.get_worker(1).unwrap().worker_session_id,
                "current-session"
            );
        }

        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::End,
                addr,
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(1).is_none());
    }

    #[test]
    fn passive_expiry_keeps_the_worker_for_active_cleanup_after_promotion() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 1,
            ..Default::default()
        };
        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        assert!(manager.mark_passive_worker_expired(1).is_some());
        assert!(manager.mark_passive_worker_expired(1).is_none());
        assert_eq!(manager.get_worker(1).unwrap().status, WorkerStatus::Lost);
        assert!(!manager.get_worker(1).unwrap().is_live());
        assert_eq!(
            manager.get_last_heartbeat(),
            vec![(1, manager.get_worker(1).unwrap().last_update)]
        );

        let new_addr = WorkerAddress {
            worker_id: 1,
            hostname: "worker-1-new-endpoint".to_string(),
            ..Default::default()
        };
        manager
            .heartbeat_passive(
                &cluster_id,
                HeartbeatStatus::Start,
                new_addr.clone(),
                1,
                "new-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(manager.get_worker(1).unwrap().address, new_addr);
        assert!(manager.get_worker(1).unwrap().is_live());
    }

    #[test]
    fn bound_credit_survives_heartbeat_reinsert_and_settles() {
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager.bind_scheduled_blocks(&chosen, 1, block_size);

        // The heartbeat re-insert replaces the WorkerInfo; the credit map
        // must be carried over (carry_scheduled_from), not reset.
        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, available)],
                None,
            )
            .unwrap();
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, block_size);

        let commit = CommitBlock {
            block_id: 1,
            block_len: 4096,
            locations: vec![BlockLocation::with_id(1)],
        };
        manager.settle_committed_blocks(&WorkerManager::settle_snapshot(&[commit]));
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 4096);
    }

    #[test]
    fn heartbeat_reclaims_scheduled_bytes_by_available_delta() {
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        for _ in 0..4 {
            manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
        }

        let cluster_id = manager.cluster_id.clone();
        let remaining = available - 4 * block_size;
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, remaining)],
                None,
            )
            .unwrap();

        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
        assert_eq!(
            manager.get_worker(1).unwrap().allocatable_available(),
            remaining
        );

        for i in 0..4 {
            let chosen = manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
            assert_eq!(chosen[0].worker_id, 1, "post-heartbeat allocation {i}");
        }
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());
    }

    #[test]
    fn heartbeat_expires_scheduled_bytes_after_lost_interval() {
        let mut conf = ClusterConf::default();
        conf.master.worker_policy = "robin".to_string();
        conf.master.heartbeat_interval = "1ms".to_string();
        conf.master.worker_check_interval = "1ms".to_string();
        conf.master.worker_lost_interval = "10ms".to_string();
        conf.master.init().unwrap();
        let mut manager = WorkerManager::new(&conf).unwrap();

        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager
            .worker_map
            .workers
            .get_mut(&1)
            .unwrap()
            .scheduled_since_ms = 1;

        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, available)],
                None,
            )
            .unwrap();

        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
    }

    fn host_addr(worker_id: u32, hostname: &str) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            hostname: hostname.to_string(),
            ip_addr: "127.0.0.1".to_string(),
            rpc_port: 8997,
            ..Default::default()
        }
    }

    fn send_heartbeat(manager: &mut WorkerManager, status: HeartbeatStatus, addr: WorkerAddress) {
        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                status,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(1 << 30, 1 << 30)],
                None,
            )
            .unwrap();
    }

    fn restart(manager: &mut WorkerManager, addr: WorkerAddress) {
        send_heartbeat(manager, HeartbeatStatus::Start, addr.clone());
        send_heartbeat(manager, HeartbeatStatus::Running, addr);
    }

    fn status_of(manager: &WorkerManager, id: u32) -> WorkerStatus {
        manager.get_known_worker(id).unwrap().status
    }

    #[test]
    fn decommission_survives_blacklist_timeout() {
        let mut manager = robin_manager();
        send_heartbeat(&mut manager, HeartbeatStatus::Running, host_addr(1, "a"));
        assert_eq!(manager.add_dcm(vec!["a".to_string()]).len(), 1);

        assert!(manager.add_blacklist_worker(1).is_none());
        assert_eq!(status_of(&manager, 1), WorkerStatus::Decommission);
    }

    #[test]
    fn decommission_survives_lost_eviction_and_restart() {
        let mut manager = robin_manager();
        let addr = host_addr(1, "a");
        send_heartbeat(&mut manager, HeartbeatStatus::Running, addr.clone());
        manager.add_dcm(vec!["a".to_string()]);

        manager.add_blacklist_worker(1);
        manager.remove_expired_worker(1).unwrap();
        assert_eq!(status_of(&manager, 1), WorkerStatus::Decommission);

        restart(&mut manager, addr);
        assert_eq!(status_of(&manager, 1), WorkerStatus::Decommission);
        assert_eq!(manager.get_dcm().len(), 1);
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, 128 << 20, vec![]))
            .is_err());
    }

    #[test]
    fn decommission_survives_restart_with_new_worker_id() {
        let mut manager = robin_manager();
        send_heartbeat(&mut manager, HeartbeatStatus::Running, host_addr(1, "a"));
        manager.add_dcm(vec!["a".to_string()]);
        manager.remove_expired_worker(1).unwrap();

        restart(&mut manager, host_addr(2, "a"));
        assert_eq!(status_of(&manager, 2), WorkerStatus::Decommission);
    }

    #[test]
    fn decommission_applies_to_worker_registered_later() {
        let mut manager = robin_manager();
        assert_eq!(
            manager.add_dcm(vec!["a".to_string()]),
            vec!["-,a,Decommission"]
        );
        assert_eq!(manager.get_dcm(), vec!["-,a,Decommission"]);

        send_heartbeat(&mut manager, HeartbeatStatus::Running, host_addr(1, "a"));
        send_heartbeat(&mut manager, HeartbeatStatus::Running, host_addr(2, "b"));
        assert_eq!(status_of(&manager, 1), WorkerStatus::Decommission);
        assert_eq!(status_of(&manager, 2), WorkerStatus::Live);
        assert_eq!(manager.get_dcm(), vec!["1,a:8997,Decommission"]);
    }

    #[test]
    fn dcm_reports_lost_and_unregistered_hosts() {
        let mut manager = robin_manager();
        send_heartbeat(&mut manager, HeartbeatStatus::Running, host_addr(1, "a"));
        send_heartbeat(&mut manager, HeartbeatStatus::Running, host_addr(2, "b"));
        manager.remove_expired_worker(1).unwrap();

        let added = manager.add_dcm(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(
            added,
            vec![
                "1,a:8997,Decommission",
                "2,b:8997,Decommission",
                "-,c,Decommission"
            ]
        );
        assert_eq!(manager.get_dcm(), added);

        // The CLI matches requested hosts on the hostname in field 1.
        for (entry, host) in added.iter().zip(["a", "b", "c"]) {
            let addr = entry.split(',').nth(1).unwrap();
            assert_eq!(addr.split(':').next(), Some(host));
        }

        let removed = manager.remove_dcm(vec!["a".to_string(), "c".to_string(), "d".to_string()]);
        assert_eq!(removed, vec!["1,a:8997,Live", "-,c,Live"]);
        assert_eq!(manager.get_dcm(), vec!["2,b:8997,Decommission"]);
    }

    #[test]
    fn remove_dcm_while_lost_restores_live_on_register() {
        let mut manager = robin_manager();
        let addr = host_addr(1, "a");
        send_heartbeat(&mut manager, HeartbeatStatus::Running, addr.clone());
        manager.add_dcm(vec!["a".to_string()]);
        manager.remove_expired_worker(1).unwrap();

        assert_eq!(
            manager.remove_dcm(vec!["a".to_string()]),
            vec!["1,a:8997,Live"]
        );
        assert_eq!(status_of(&manager, 1), WorkerStatus::Live);

        restart(&mut manager, addr);
        assert_eq!(status_of(&manager, 1), WorkerStatus::Live);
        assert!(manager.get_dcm().is_empty());
        manager
            .choose_worker(ChooseContext::with_num(1, 128 << 20, vec![]))
            .unwrap();
    }

    #[test]
    fn blacklisted_worker_recovers_live_on_heartbeat() {
        let mut manager = robin_manager();
        let addr = host_addr(1, "a");
        send_heartbeat(&mut manager, HeartbeatStatus::Running, addr.clone());

        assert!(manager.add_blacklist_worker(1).is_some());
        assert!(manager.remove_dcm(vec!["a".to_string()]).is_empty());
        assert_eq!(status_of(&manager, 1), WorkerStatus::Blacklist);

        send_heartbeat(&mut manager, HeartbeatStatus::Running, addr);
        assert_eq!(status_of(&manager, 1), WorkerStatus::Live);
    }
}
