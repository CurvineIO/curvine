// Copyright 2025 Curvine
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

use super::MasterFilesystem;
use crate::master::replication::master_replication_manager::{
    DrainRequest, MasterReplicationManager,
};
use curvine_core_error::CommonResult;
use curvine_model::WorkerStatus;
use curvine_runtime::common::LocalTime;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DrainState {
    Draining,
    Paused,
    Drained,
    Blocked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DrainBlocker {
    AdmissionLimit,
    PendingRelease,
    Writing,
    StaleLocation,
    NoTargetCapacity,
    NoSafeWitness,
    InventoryUnconfirmed,
    WorkerUnavailable,
}

/// Leader-local progress per registration. Remaining counts are estimates from
/// the last completed pass (or the partial first pass); they are not a snapshot.
/// Only `Drained` is based on a fresh empty first page and heartbeat evidence.
#[derive(Clone, Debug)]
pub struct DrainProgress {
    pub hostname: String,
    pub worker_id: Option<u32>,
    pub state: DrainState,
    pub remaining_blocks: u64,
    pub remaining_bytes: u64,
    pub inflight: usize,
    pub blockers: BTreeMap<DrainBlocker, u64>,
    pub last_scan_ms: u64,
}

#[derive(Default)]
pub(super) struct DrainSnapshot {
    pub epoch: (u64, u64),
    pub rows: Vec<DrainProgress>,
}

#[derive(Default)]
struct ScanTotals {
    blocks: u64,
    bytes: u64,
    blockers: BTreeMap<DrainBlocker, u64>,
}

impl ScanTotals {
    fn blocked(&mut self, reason: DrainBlocker) {
        *self.blockers.entry(reason).or_default() += 1;
    }
}

#[derive(Default)]
struct WorkerScan {
    session: String,
    cursor: Option<i64>,
    pass: ScanTotals,
    last_pass: Option<ScanTotals>,
}

/// No per-block inventory is retained between ticks; the only per-block work
/// is owned by the existing replication tracker and guarded delete queue.
#[derive(Default)]
pub(crate) struct DecommissionMonitor {
    epoch: Option<(u64, u64)>,
    scans: HashMap<u32, WorkerScan>,
    first_worker: usize,
}

impl DecommissionMonitor {
    pub(crate) async fn run(
        fs: MasterFilesystem,
        replication: std::sync::Arc<MasterReplicationManager>,
        mut stopped: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(
            fs.conf.decommission_scan_interval_ms,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut monitor = Self::default();
        loop {
            tokio::select! {
                biased;
                _ = &mut stopped => break,
                _ = ticker.tick() => {
                    let fs = fs.clone();
                    let replication = replication.clone();
                    let result = tokio::task::spawn_blocking(move || {
                        let epoch = fs.drain_delete_epoch();
                        match monitor.tick(&fs, &replication) {
                            Ok(rows) if fs.can_dispatch_drain_deletes(epoch) => {
                                *fs.decommission_progress.lock() = DrainSnapshot { epoch, rows };
                            }
                            Ok(_) => (),
                            Err(error) => {
                                log::warn!("Decommission scan failed: {}", error);
                                fs.decommission_progress.lock().rows.clear();
                                monitor = Self::default();
                            }
                        }
                        monitor
                    }).await;
                    match result {
                        Ok(next) => monitor = next,
                        Err(error) => {
                            log::error!("Decommission scan task failed: {}", error);
                            break;
                        }
                    }
                }
            }
        }
        fs.decommission_progress.lock().rows.clear();
    }

    pub(crate) fn tick(
        &mut self,
        fs: &MasterFilesystem,
        replication: &MasterReplicationManager,
    ) -> CommonResult<Vec<DrainProgress>> {
        let epoch = fs.drain_delete_epoch();
        if self.epoch != Some(epoch) || !fs.can_dispatch_drain_deletes(epoch) {
            self.scans.clear();
            self.first_worker = 0;
        }
        self.epoch = Some(epoch);
        if !fs.conf.decommission_drain_enabled || !fs.can_dispatch_drain_deletes(epoch) {
            return Ok(vec![]);
        }

        let mut known_ids = HashSet::new();
        let mut hosts = BTreeMap::new();
        {
            let workers = fs.worker_manager.read();
            for host in &workers.worker_map.decommissioned_hosts {
                hosts.insert(host.clone(), Vec::new());
            }
            for worker in workers
                .worker_map
                .workers()
                .values()
                .chain(workers.worker_map.lost_workers().values())
            {
                if let Some(ids) = hosts.get_mut(&worker.address.hostname) {
                    ids.push(worker.worker_id());
                    known_ids.insert(worker.worker_id());
                }
            }
        }
        let mut work = Vec::new();
        for (host, ids) in &mut hosts {
            ids.sort_unstable();
            ids.dedup();
            if ids.is_empty() {
                work.push((host, None));
            } else {
                work.extend(ids.iter().map(|&id| (host, Some(id))));
            }
        }
        // Rotate the first registration so budget contention does not always
        // favor the same host; still inspect at most one page per Worker.
        if !work.is_empty() {
            let start = self.first_worker % work.len();
            work.rotate_left(start);
            self.first_worker = start + 1;
        }
        let mut progress = Vec::with_capacity(work.len());
        self.scans.retain(|id, _| known_ids.contains(id));
        for (host, worker_id) in work {
            if !fs.can_dispatch_drain_deletes(epoch) {
                return Ok(vec![]);
            }
            let dir = fs.fs_dir.read();
            let workers = fs.worker_manager.read();
            if !workers.worker_map.is_decommissioned(host) {
                continue;
            }
            let worker = worker_id.and_then(|id| workers.get_worker(id)).filter(|w| {
                w.status == WorkerStatus::Decommission
                    && w.address.hostname == *host
                    && !w.worker_session_id.is_empty()
                    && LocalTime::mills().saturating_sub(w.last_update)
                        <= fs.conf.worker_lost_interval_ms()
            });
            let inflight = worker_id.map_or(0, |id| replication.drain_inflight(id));
            let mut status = DrainProgress {
                hostname: host.clone(),
                worker_id,
                state: DrainState::Blocked,
                remaining_blocks: 0,
                remaining_bytes: 0,
                inflight,
                blockers: BTreeMap::new(),
                last_scan_ms: LocalTime::mills(),
            };
            let paused = dir.drain_paused(host)?;
            let Some(worker) = worker.filter(|_| !paused) else {
                if let Some(scan) = worker_id.and_then(|id| self.scans.get_mut(&id)) {
                    scan.cursor = None;
                    let totals = scan
                        .last_pass
                        .get_or_insert_with(|| std::mem::take(&mut scan.pass));
                    status.remaining_blocks = totals.blocks;
                    status.remaining_bytes = totals.bytes;
                    scan.pass = ScanTotals::default();
                }
                if paused {
                    status.state = DrainState::Paused;
                } else {
                    status.blockers.insert(DrainBlocker::WorkerUnavailable, 1);
                }
                progress.push(status);
                continue;
            };
            let id = worker.worker_id();
            let request = DrainRequest {
                worker_id: id,
                session: worker.worker_session_id.clone(),
                epoch,
            };
            let empty_inventory = workers.drain_inventory_epoch.get(&id) == Some(&epoch)
                && worker.block_num == 0
                && worker.total_scheduled_bytes() == 0
                && !worker.storage_map.is_empty()
                && worker.storage_map.values().all(|store| !store.failed);
            let pending_release = workers
                .drain_deletes
                .get(&id)
                .is_some_and(|entries| !entries.is_empty())
                || workers.block_map.has_pending(id);
            let scan = self.scans.entry(id).or_default();
            if scan.session != request.session {
                *scan = WorkerScan {
                    session: request.session.clone(),
                    ..Default::default()
                };
            }
            let first_page = scan.cursor.is_none();
            let (ids, next) =
                dir.scan_worker_block_ids(id, scan.cursor, fs.conf.decommission_blocks_per_scan)?;
            if first_page && ids.is_empty() {
                if empty_inventory && !pending_release && inflight == 0 {
                    status.state = DrainState::Drained;
                } else {
                    status
                        .blockers
                        .insert(DrainBlocker::InventoryUnconfirmed, 1);
                    if pending_release {
                        status.blockers.insert(DrainBlocker::PendingRelease, 1);
                    }
                }
            }
            drop(workers);
            for block_id in ids {
                scan.pass.blocks += 1;
                let Some(state) = dir.replication_block_state(block_id)? else {
                    scan.pass.blocked(DrainBlocker::StaleLocation);
                    continue;
                };
                if !state.locations.iter().any(|loc| loc.worker_id == id) {
                    scan.pass.blocked(DrainBlocker::StaleLocation);
                    continue;
                }
                scan.pass.bytes = scan.pass.bytes.saturating_add(state.len.max(0) as u64);
                if !state.complete {
                    scan.pass.blocked(DrainBlocker::Writing);
                    continue;
                }
                let mut workers = fs.worker_manager.write();
                if !fs.can_dispatch_drain_deletes(epoch)
                    || !workers.get_worker(id).is_some_and(|worker| {
                        worker.worker_session_id == request.session
                            && worker.status == WorkerStatus::Decommission
                    })
                {
                    scan.pass.blocked(DrainBlocker::WorkerUnavailable);
                    break;
                }
                if workers.block_map.is_pending(id, block_id) {
                    scan.pass.blocked(DrainBlocker::PendingRelease);
                    continue;
                }
                if !state.needs_replica(block_id, &workers) {
                    if workers.queue_existing_drain_replica(block_id, id, &state, epoch) {
                        scan.pass.blocked(DrainBlocker::PendingRelease);
                    } else {
                        scan.pass.blocked(DrainBlocker::NoSafeWitness);
                    }
                } else if !workers.worker_map.workers().values().any(|target| {
                    target.is_live()
                        && !target.worker_session_id.is_empty()
                        && target.can_allocate(state.len)
                        && !state
                            .locations
                            .iter()
                            .any(|loc| loc.worker_id == target.worker_id())
                }) {
                    scan.pass.blocked(DrainBlocker::NoTargetCapacity);
                } else if !replication.try_queue_drain(block_id, request.clone()) {
                    scan.pass.blocked(DrainBlocker::AdmissionLimit);
                }
            }
            scan.cursor = next;
            if next.is_none() {
                scan.last_pass = Some(std::mem::take(&mut scan.pass));
            }
            let totals = scan.last_pass.as_ref().unwrap_or(&scan.pass);
            status.remaining_blocks = totals.blocks;
            status.remaining_bytes = totals.bytes;
            status.blockers.extend(totals.blockers.clone());
            status.inflight = replication.drain_inflight(id);
            if status.state != DrainState::Drained
                && (status.inflight > 0
                    || status.remaining_blocks > 0
                        && (status.blockers.is_empty()
                            || status.blockers.contains_key(&DrainBlocker::PendingRelease)
                            || status.blockers.contains_key(&DrainBlocker::AdmissionLimit)))
            {
                status.state = DrainState::Draining;
            }
            progress.push(status);
        }
        progress
            .sort_unstable_by(|a, b| (&a.hostname, a.worker_id).cmp(&(&b.hostname, b.worker_id)));
        Ok(progress)
    }
}
