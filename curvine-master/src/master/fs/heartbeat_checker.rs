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

use crate::master::fs::MasterFilesystem;
use crate::master::quota::QuotaManager;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::MasterMonitor;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use log::{error, info, warn};
use parking_lot::Mutex;
use std::sync::Arc;

pub struct HeartbeatChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
    worker_blacklist_ms: u64,
    worker_lost_ms: u64,
    replication_manager: Arc<MasterReplicationManager>,
    quota_manager: Arc<QuotaManager>,
    last_active_epoch: Mutex<Option<u64>>,
}

impl HeartbeatChecker {
    pub fn new(
        fs: MasterFilesystem,
        monitor: MasterMonitor,
        executor: Arc<GroupExecutor>,
        replication_manager: Arc<MasterReplicationManager>,
        quota_manager: Arc<QuotaManager>,
    ) -> Self {
        let worker_blacklist_ms = fs.conf.worker_blacklist_interval_ms();
        let worker_lost_ms = fs.conf.worker_lost_interval_ms();
        Self {
            fs,
            monitor,
            executor,
            worker_blacklist_ms,
            worker_lost_ms,
            replication_manager,
            quota_manager,
            last_active_epoch: Mutex::new(None),
        }
    }
}

impl LoopTask for HeartbeatChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        if !self.monitor.is_active() {
            if self.fs.conf.follower_worker_state_enabled || self.fs.conf.follower_reads_enabled {
                if self.monitor.is_active()
                    || self.monitor.follower_readiness_snapshot().role
                        != curvine_raft::raft::RoleState::Follower
                {
                    return Ok(());
                }

                // A follower keeps only a local worker-liveness view. In
                // particular, it must not blacklist workers or remove their
                // block locations and trigger replication/quota work.
                let mut expired_workers = Vec::new();
                {
                    let mut wm = self.fs.worker_manager.write();
                    if self.monitor.is_active() {
                        return Ok(());
                    }
                    let now = LocalTime::mills();
                    let expired: Vec<u32> = wm
                        .get_last_heartbeat()
                        .into_iter()
                        .filter_map(|(id, last)| {
                            (now.saturating_sub(last) > self.worker_lost_ms).then_some(id)
                        })
                        .collect();
                    for id in expired {
                        if wm.mark_passive_worker_expired(id).is_some() {
                            expired_workers.push(id);
                        }
                    }
                }
                for id in expired_workers {
                    self.fs.forget_passive_worker(id);
                }
                if let Err(e) = self.fs.retry_passive_block_reports() {
                    warn!("follower block-report replay failed: {}", e);
                }
            }
            return Ok(());
        }

        let readiness = self.monitor.follower_readiness_snapshot();
        let promotion_epoch = {
            let mut last_epoch = self.last_active_epoch.lock();
            if *last_epoch == Some(readiness.leadership_epoch) {
                false
            } else {
                *last_epoch = Some(readiness.leadership_epoch);
                true
            }
        };
        if promotion_epoch
            && (self.fs.conf.follower_worker_state_enabled || self.fs.conf.follower_reads_enabled)
        {
            let now = LocalTime::mills();
            let refreshed = {
                let mut wm = self.fs.worker_manager.write();
                if !self.monitor.is_active()
                    || self.monitor.follower_readiness_snapshot().leadership_epoch
                        != readiness.leadership_epoch
                {
                    return Ok(());
                }
                wm.refresh_passive_lost_workers_for_promotion(now)
            };
            if !refreshed.is_empty() {
                info!(
                    "Master promotion granted {} passively expired workers a {} ms recovery window",
                    refreshed.len(),
                    self.worker_lost_ms
                );
            }
        }

        let mut blacklisted_workers = Vec::new();
        let mut removed_workers = Vec::new();
        {
            let mut wm = self.fs.worker_manager.write();
            let workers = wm.get_last_heartbeat();
            let now = LocalTime::mills();

            for (id, last_update) in workers {
                if now > last_update + self.worker_blacklist_ms {
                    // Worker blacklist timeout
                    if let Some(worker) = wm.add_blacklist_worker(id) {
                        blacklisted_workers.push((id, worker.address, worker.last_update));
                    }
                }

                if now > last_update + self.worker_lost_ms {
                    // Heartbeat timeout
                    if let Some(worker) = wm.remove_expired_worker(id) {
                        removed_workers.push((id, worker.address, worker.last_update));
                    }
                }
            }
        }

        for (id, address, last_update) in blacklisted_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded blacklist timeout {} ms",
                id, address, last_update, self.worker_blacklist_ms
            );
        }

        for (id, address, last_update) in removed_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded lost timeout {} ms and will be removed",
                id, address, last_update, self.worker_lost_ms
            );
            // Asynchronously delete all block location data.
            let fs = self.fs.clone();
            let rm = self.replication_manager.clone();
            let res = self.executor.spawn(move || {
                let spend = TimeSpent::new();
                let cleanup = match fs.delete_locations(id) {
                    Err(e) => {
                        warn!("{}", curvine_core_error::err_msg!(e));
                        Default::default()
                    }
                    Ok(res) => res,
                };
                let replication_block_num = cleanup.replication_block_ids.len();
                if let Err(e) = rm.report_under_replicated_blocks(id, cleanup.replication_block_ids)
                {
                    error!(
                        "Errors on reporting under-replicated {} blocks. err: {:?}",
                        replication_block_num, e
                    );
                }
                info!(
                    "Delete worker {} all locations used {} ms",
                    id,
                    spend.used_ms()
                );
            });
            if let Err(e) = &res {
                warn!("{}", e);
            }
        }

        if let Ok(info) = self.fs.filesystem_info() {
            self.quota_manager.detector(Some(info));
        };

        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use crate::master::quota::eviction::evictor::LRUEvictor;
    use crate::master::quota::eviction::types::EvictionConf;
    use crate::master::quota::QuotaManager;
    use crate::master::replication::master_replication_manager::MasterReplicationManager;
    use crate::master::{Master, MasterMonitor};
    use curvine_config::ClusterConf;
    use curvine_model::{TransferWorkerCapabilities, WorkerAddress, WorkerStatus};
    use curvine_raft::raft::{FollowerReadinessMonitor, RoleState};
    use curvine_runtime::common::Utils;
    use curvine_runtime::runtime::{AsyncRuntime, GroupExecutor};
    use curvine_runtime::sync::StateCtl;
    use std::sync::Arc;

    #[test]
    fn promotion_grants_passive_lost_worker_one_lost_interval_before_cleanup() {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.follower_worker_state_enabled = true;
        let test_id = Utils::rand_str(8);
        conf.master.meta_dir = Utils::test_sub_dir(format!("heartbeat-promotion/meta-{test_id}"));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("heartbeat-promotion/journal-{test_id}"));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();

        let readiness = FollowerReadinessMonitor::default();
        readiness.observe_raft_state(RoleState::Follower, 1, 2, 0);
        readiness.observe_applied(0);
        readiness.set_replay_ready(true);
        let monitor = MasterMonitor::with_follower_readiness(
            StateCtl::new(RoleState::Follower.into()),
            StateCtl::new(0),
            readiness.clone(),
        );
        fs.master_monitor = monitor.clone();

        let worker_id = 17;
        let addr = WorkerAddress {
            worker_id,
            ..Default::default()
        };
        {
            let mut wm = fs.worker_manager.write();
            let cluster_id = wm.cluster_id.clone();
            wm.heartbeat_passive(
                &cluster_id,
                curvine_model::HeartbeatStatus::Start,
                addr,
                1,
                "promoted-worker-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
            wm.worker_map
                .workers
                .get_mut(&worker_id)
                .unwrap()
                .last_update = 1;
            wm.mark_passive_worker_expired(worker_id).unwrap();
        }

        let runtime = Arc::new(AsyncRuntime::single());
        let replication_manager =
            MasterReplicationManager::new(&fs, &conf, &runtime, &fs.worker_manager).unwrap();
        let quota_manager = QuotaManager::new(
            EvictionConf::from_conf(&conf),
            fs.clone(),
            Arc::new(LRUEvictor::new(EvictionConf::from_conf(&conf))),
            runtime,
            true,
        );
        let executor = Arc::new(GroupExecutor::new("heartbeat-promotion-test", 1, 16));
        let checker = HeartbeatChecker::new(
            fs.clone(),
            monitor.clone(),
            executor,
            replication_manager,
            quota_manager,
        );

        checker.run().unwrap();
        readiness.observe_raft_state(RoleState::Leader, 2, 1, 0);
        monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();

        let promoted = fs.worker_manager.read();
        let worker = promoted.get_worker(worker_id).unwrap();
        assert_eq!(worker.status, WorkerStatus::Lost);
        assert!(worker.last_update > 1);
        drop(promoted);

        // The grace is bounded: after a full lost interval with no authoritative
        // heartbeat, the active checker performs its normal cleanup transition.
        {
            let mut wm = fs.worker_manager.write();
            let lost_interval_ms = wm.conf.master.worker_lost_interval_ms();
            wm.worker_map
                .workers
                .get_mut(&worker_id)
                .unwrap()
                .last_update = LocalTime::mills().saturating_sub(lost_interval_ms + 1);
        }
        checker.run().unwrap();
        let wm = fs.worker_manager.read();
        assert!(wm.get_worker(worker_id).is_none());
        assert!(wm.worker_map.lost_workers().contains_key(&worker_id));
    }
}
