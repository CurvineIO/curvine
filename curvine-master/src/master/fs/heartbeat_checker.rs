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
use curvine_runtime::sync::{AtomicBool, AtomicCounter};
use log::{error, info, warn};
use std::sync::Arc;

pub struct HeartbeatChecker {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    executor: Arc<GroupExecutor>,
    worker_blacklist_ms: u64,
    worker_lost_ms: u64,
    replication_manager: Arc<MasterReplicationManager>,
    quota_manager: Arc<QuotaManager>,
    last_active: AtomicBool,
    last_tick_ms: AtomicCounter,
    // Heartbeats are answered with NotLeader while this node is a follower, so worker
    // timestamps freeze at the demotion time; judgments compare max(last_update, this)
    // against the thresholds instead of rewriting the recorded heartbeats.
    heartbeat_baseline_ms: AtomicCounter,
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
            last_active: AtomicBool::new(false),
            last_tick_ms: AtomicCounter::new(0),
            heartbeat_baseline_ms: AtomicCounter::new(0),
        }
    }
}

impl LoopTask for HeartbeatChecker {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        let now = LocalTime::mills();
        let previous_tick = self.last_tick_ms.get();
        self.last_tick_ms.set(now);

        if !self.monitor.is_active() {
            self.last_active.set(false);
            return Ok(());
        }

        let mut blacklisted_workers = Vec::new();
        let mut expired_workers = Vec::new();
        let baseline = self.heartbeat_baseline_ms.get();
        {
            let mut wm = self.fs.worker_manager.write();
            if !self.monitor.is_active() {
                self.last_active.set(false);
                return Ok(());
            }

            let promoted = self.last_active.compare_and_set(false, true);
            let missed_role_change =
                previous_tick != 0 && now.saturating_sub(previous_tick) > self.worker_lost_ms;
            if promoted || missed_role_change {
                self.heartbeat_baseline_ms.set(now);
                let workers = wm.get_last_heartbeat();
                if !workers.is_empty() {
                    warn!(
                        "Promotion detected (promoted: {}, tick gap {} ms): re-based the \
                         heartbeat baseline of {} workers, expiration check resumes on the \
                         next tick",
                        promoted,
                        now.saturating_sub(previous_tick),
                        workers.len()
                    );
                }
                return Ok(());
            }

            let workers = wm.get_last_heartbeat();

            for (id, last_update) in workers {
                let heartbeat = last_update.max(baseline);
                if now > heartbeat + self.worker_blacklist_ms {
                    // Worker blacklist timeout
                    if let Some(worker) = wm.add_blacklist_worker(id) {
                        blacklisted_workers.push((id, worker.address, worker.last_update));
                    }
                }

                if now > heartbeat + self.worker_lost_ms {
                    // Heartbeat timeout
                    expired_workers.push(id);
                }
            }
        }

        for (id, address, last_update) in blacklisted_workers {
            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded blacklist timeout {} ms",
                id, address, last_update, self.worker_blacklist_ms
            );
        }

        for id in expired_workers {
            let removed = {
                let mut wm = self.fs.worker_manager.write();
                if self.monitor.is_active() {
                    wm.remove_expired_worker_if_stale(id, now, baseline, self.worker_lost_ms)
                } else {
                    None
                }
            };

            let worker = match removed {
                Some(worker) => worker,
                None => {
                    if !self.monitor.is_active() {
                        warn!(
                            "Skip lost-worker {} cleanup: node is no longer the active master",
                            id
                        );
                        break;
                    }
                    continue;
                }
            };

            warn!(
                "Worker {} ({}) last heartbeat {} has exceeded lost timeout {} ms and will be removed",
                id, worker.address, worker.last_update, self.worker_lost_ms
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
    use crate::master::quota::eviction::evictor::{Evictor, LRUEvictor};
    use crate::master::quota::eviction::EvictionConf;
    use crate::master::Master;
    use curvine_config::{ClusterConf, JournalConf, MasterConf};
    use curvine_model::{BlockLocation, StorageType, WorkerAddress, WorkerInfo};
    use curvine_raft::raft::RoleState;
    use curvine_runtime::common::Utils;
    use curvine_runtime::runtime::AsyncRuntime;

    const WORKER_ID: u32 = 7;
    const BLOCK_ID: i64 = 41;

    fn test_conf(name: &str) -> ClusterConf {
        ClusterConf {
            format_master: true,
            testing: true,
            master: MasterConf {
                meta_dir: Utils::test_sub_dir(format!("heartbeat-checker/meta-{}", name)),
                ..Default::default()
            },
            journal: JournalConf {
                enable: false,
                journal_dir: Utils::test_sub_dir(format!("heartbeat-checker/journal-{}", name)),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    // A master holding one registered worker which owns one block location.
    fn fixture(name: &str) -> (MasterFilesystem, HeartbeatChecker) {
        Master::init_test_metrics();
        let conf = test_conf(name);
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.add_test_worker(WorkerInfo::new(
            WorkerAddress {
                worker_id: WORKER_ID,
                ..Default::default()
            },
            1,
        ));
        fs.fs_dir()
            .write()
            .store
            .add_block_location(BLOCK_ID, BlockLocation::new(WORKER_ID, StorageType::Disk))
            .unwrap();

        let rt = Arc::new(AsyncRuntime::single());
        let eviction_conf = EvictionConf::from_conf(&conf);
        let evictor: Arc<dyn Evictor> = Arc::new(LRUEvictor::new(eviction_conf.clone()));
        let quota_manager = QuotaManager::new(eviction_conf, fs.clone(), evictor, rt.clone(), true);
        let replication_manager =
            MasterReplicationManager::new(&fs, &conf, &rt, &fs.worker_manager).unwrap();

        let checker = HeartbeatChecker::new(
            fs.clone(),
            fs.master_monitor.clone(),
            Arc::new(GroupExecutor::new("heartbeat-checker-test", 1, 16)),
            replication_manager,
            quota_manager,
        );
        (fs, checker)
    }

    fn set_heartbeat_age(fs: &MasterFilesystem, age_ms: u64) {
        let mut wm = fs.worker_manager.write();
        let worker = wm.worker_map.workers.get_mut(&WORKER_ID).unwrap();
        worker.last_update = LocalTime::mills().saturating_sub(age_ms);
    }

    fn worker_present(fs: &MasterFilesystem) -> bool {
        fs.worker_manager.read().get_worker(WORKER_ID).is_some()
    }

    fn worker_live(fs: &MasterFilesystem) -> bool {
        fs.worker_manager
            .read()
            .worker_map
            .workers
            .contains_key(&WORKER_ID)
    }

    fn location_count(fs: &MasterFilesystem) -> usize {
        fs.fs_dir()
            .read()
            .store
            .get_locations(BLOCK_ID)
            .unwrap()
            .len()
    }

    // SingleExecutor::drop pushes no-op tasks until try_send fails; a worker thread
    // that keeps draining the queue makes that loop spin forever, so the checker
    // (owner of the only GroupExecutor reference) is leaked instead of dropped.
    fn forget_checker(checker: HeartbeatChecker) {
        std::mem::forget(checker);
    }

    // Heartbeats stop arriving while the node is a follower, so the recorded
    // timestamps describe the demotion, not a worker failure: a standby tick must
    // leave the worker map untouched.
    #[test]
    fn demoted_tick_keeps_workers_and_their_locations() {
        let (fs, checker) = fixture("demoted-tick");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();
        assert!(worker_present(&fs));

        set_heartbeat_age(&fs, fs.conf.worker_lost_interval_ms() * 2);
        fs.master_monitor.journal_ctl.set_state(RoleState::Follower);
        checker.run().unwrap();

        assert!(
            worker_present(&fs),
            "a standby tick must not expire workers"
        );
        assert_eq!(
            location_count(&fs),
            1,
            "a standby tick must not delete block locations"
        );
        forget_checker(checker);
    }

    // The re-basing branches must not disable loss detection: a regular active tick
    // still expires a worker whose heartbeat aged past the lost timeout after the
    // re-based baseline.
    #[test]
    fn active_tick_expires_worker_past_lost_timeout() {
        let (fs, checker) = fixture("active-expiry");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();

        let lost_ms = fs.conf.worker_lost_interval_ms();
        set_heartbeat_age(&fs, lost_ms * 2);
        // The baseline only lifts timestamps that froze before the promotion, so it has
        // to age as well for this worker to stay past the lost timeout.
        checker
            .heartbeat_baseline_ms
            .set(LocalTime::mills().saturating_sub(lost_ms * 3));
        checker.run().unwrap();

        assert!(
            !worker_live(&fs),
            "a regular active tick must expire a worker past the lost timeout"
        );
        forget_checker(checker);
    }

    // A stalled checker can miss an entire follower period and then observe leadership
    // without any visible transition: the resumed tick must rebuild the baseline
    // instead of judging timestamps that froze while the node was a follower.
    #[test]
    fn resumed_tick_after_missed_promotion_rebases_instead_of_expiring() {
        let (fs, checker) = fixture("missed-promotion");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();

        set_heartbeat_age(&fs, fs.conf.worker_lost_interval_ms() * 2);
        let stalled_tick = LocalTime::mills().saturating_sub(fs.conf.worker_lost_interval_ms() * 2);
        checker.last_tick_ms.set(stalled_tick);

        checker.run().unwrap();
        assert!(
            worker_present(&fs),
            "a resumed tick must rebuild the baseline instead of expiring workers"
        );
        assert_eq!(location_count(&fs), 1);

        checker.run().unwrap();
        assert!(
            worker_present(&fs),
            "the rebased worker must survive the next judgment"
        );
        forget_checker(checker);
    }

    // The re-base must keep the recorded heartbeat: the worker listing and the expiry
    // log still report the real last heartbeat of a worker that died while this node
    // was a follower.
    #[test]
    fn promotion_rebase_keeps_recorded_heartbeat() {
        let (fs, checker) = fixture("rebase-keeps-heartbeat");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();

        fs.master_monitor.journal_ctl.set_state(RoleState::Follower);
        checker.run().unwrap();
        set_heartbeat_age(&fs, fs.conf.worker_lost_interval_ms() * 2);
        let frozen = fs
            .worker_manager
            .read()
            .get_worker(WORKER_ID)
            .unwrap()
            .last_update;

        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();

        let after = fs
            .worker_manager
            .read()
            .get_worker(WORKER_ID)
            .unwrap()
            .last_update;
        assert_eq!(
            after, frozen,
            "the re-base must not rewrite the recorded heartbeat"
        );
        assert!(worker_live(&fs));
        forget_checker(checker);
    }

    // After a promotion the frozen timestamps must be re-based, otherwise the
    // first active tick expires every worker that survived the demotion.
    #[test]
    fn promotion_rebases_heartbeats_before_expiring_workers() {
        let (fs, checker) = fixture("promotion-baseline");
        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();

        // Stay a follower for longer than the lost timeout.
        fs.master_monitor.journal_ctl.set_state(RoleState::Follower);
        checker.run().unwrap();
        set_heartbeat_age(&fs, fs.conf.worker_lost_interval_ms() * 2);

        fs.master_monitor.journal_ctl.set_state(RoleState::Leader);
        checker.run().unwrap();
        assert!(
            worker_present(&fs),
            "the first tick after promotion must only rebase the baseline"
        );

        checker.run().unwrap();
        assert!(
            worker_present(&fs),
            "the rebased worker must survive the next judgment"
        );
        assert_eq!(location_count(&fs), 1);
        forget_checker(checker);
    }
}
