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

use std::sync::Arc;

use crate::master::fs::{DeleteMode, MasterFilesystem};
use crate::master::meta::inode::ttl::InodeTtlExecutor;
use crate::master::meta::inode::InodeView;
use crate::master::quota::eviction::evictor::Evictor;
use crate::master::quota::eviction::types::EvictPlan;
use crate::master::quota::eviction::EvictionConf;
use crate::master::Master;
use curvine_error::FsError;
use curvine_model::FilesystemInfo;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use parking_lot::RwLock;
use std::collections::HashSet;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct QuotaManager {
    eviction_conf: EvictionConf,
    fs: MasterFilesystem,
    evictor: Arc<dyn Evictor>,
    ttl_executor: RwLock<Option<InodeTtlExecutor>>,
    tx: Sender<Option<FilesystemInfo>>,
}

impl QuotaManager {
    pub fn new(
        eviction_conf: EvictionConf,
        fs: MasterFilesystem,
        evictor: Arc<dyn Evictor>,
        rt: Arc<Runtime>,
        testing: bool,
    ) -> Arc<Self> {
        let (tx, mut rx): (
            Sender<Option<FilesystemInfo>>,
            Receiver<Option<FilesystemInfo>>,
        ) = mpsc::channel(1024);

        let manager = Arc::new(QuotaManager {
            evictor,
            eviction_conf,
            fs: fs.clone(),
            ttl_executor: RwLock::new(None),
            tx,
        });

        let mgr = manager.clone();

        if !testing {
            rt.spawn(async move {
                while let Some(cluster_info) = rx.recv().await {
                    mgr.handle_trigger(cluster_info);
                }
            });
        }

        manager
    }

    /// Set the TTL executor after initialization (called when MountManager, UfsFactory, JobManager are ready)
    pub fn set_ttl_executor(&self, executor: InodeTtlExecutor) {
        let mut guard = self.ttl_executor.write();
        *guard = Some(executor);
    }

    fn is_eviction_enabled(&self) -> bool {
        self.eviction_conf.enable_quota_eviction
    }

    pub fn detector(&self, info: Option<FilesystemInfo>) {
        let _ = self.tx.try_send(info);
    }

    fn handle_trigger(&self, cluster_info: Option<FilesystemInfo>) {
        if !self.is_eviction_enabled() {
            return;
        }

        // Update LRU cache size metric
        let metrics = match Master::get_metrics() {
            Ok(metrics) => metrics,
            Err(e) => {
                log::warn!("cluster-evict: master metrics unavailable: {}", e);
                return;
            }
        };
        metrics
            .eviction_lru_cache_size
            .set(self.evictor.cache_size() as i64);

        let Some(info) = cluster_info else {
            log::warn!("cluster-evict: failed to fetch filesystem_info");
            return;
        };

        let curvine_used = info.fs_used;
        let curvine_quota = info.available + info.fs_used;

        if curvine_quota <= 0 {
            return;
        }

        if curvine_used <= 0 {
            log::debug!("cluster-evict: curvine_used <= 0, stopping eviction");
            return;
        }

        let Some(mut plan) = self.create_evict_plan(curvine_used, curvine_quota) else {
            log::debug!(
                "cluster-evict: no eviction needed, curvine_used={}, curvine_quota={}, usage_ratio={:.2}%", 
                curvine_used, curvine_quota, (curvine_used as f64 / curvine_quota as f64) * 100.0
            );
            return;
        };

        // Increment eviction trigger counter
        metrics.eviction_trigger_count.inc();

        log::info!(
            "cluster-evict: starting eviction, curvine_used={}, curvine_quota={}, usage_ratio={:.2}%, target_free={}",
            curvine_used, curvine_quota, (curvine_used as f64 / curvine_quota as f64) * 100.0, plan.target_free_bytes
        );

        // Quota eviction is a scan, not a single page: keep moving the cursor
        // past candidates that cannot be reclaimed yet (live files, trash) and
        // stop only when the target is met, the candidate set is exhausted, or
        // the per-trigger scan budget is spent.
        let ttl_executor_guard = self.ttl_executor.read();
        let Some(ttl_executor) = ttl_executor_guard.as_ref() else {
            log::warn!("cluster-evict: ttl_executor not initialized, skipping eviction");
            return;
        };
        let (examined, reclaimed) = run_eviction_plan(
            &self.fs,
            self.evictor.as_ref(),
            ttl_executor,
            &self.eviction_conf,
            &mut plan,
        );
        if reclaimed > 0 {
            log::info!(
                "cluster-evict: trigger finished, examined={}, reclaimed={}, target_left={}",
                examined,
                reclaimed,
                plan.target_free_bytes
            );
        }
    }

    fn create_evict_plan(&self, used: i64, quota: i64) -> Option<EvictPlan> {
        if quota <= 0 {
            return None;
        }

        let usage_ratio = used as f64 / quota as f64;
        if usage_ratio < self.eviction_conf.high_watermark {
            return None;
        }

        let target_ratio = self
            .eviction_conf
            .low_watermark
            .min(self.eviction_conf.high_watermark)
            .min(1.0);

        let target_used = (target_ratio * quota as f64) as i64;
        let target_free_bytes = (used - target_used).max(0);

        Some(EvictPlan {
            trigger_used: used,
            quota_size: quota,
            target_free_bytes,
        })
    }
}

/// Runs one eviction trigger and returns `(examined, reclaimed_bytes)`.
///
/// `evictor.select_victims` always returns the same coldest prefix, so the loop
/// advances with an explicit skip set: candidates that are not reclaimable now
/// stay in the evictor but are not revisited within this trigger. Only bytes
/// that were actually released count toward the target.
fn run_eviction_plan(
    fs: &MasterFilesystem,
    evictor: &dyn Evictor,
    ttl_executor: &InodeTtlExecutor,
    conf: &EvictionConf,
    plan: &mut EvictPlan,
) -> (usize, i64) {
    let mut skipped: HashSet<i64> = HashSet::new();
    let mut examined = 0usize;
    let mut total_reclaimed = 0i64;

    loop {
        if plan.target_free_bytes <= 0 {
            log::debug!("cluster-evict: reached target_free_bytes, stopping eviction");
            break;
        }

        if conf.max_scan > 0 && examined >= conf.max_scan {
            log::info!(
                "cluster-evict: scan limit reached (examined={}, max_scan={}), target_left={}",
                examined,
                conf.max_scan,
                plan.target_free_bytes
            );
            break;
        }

        let inode_ids = evictor.select_victims_excluding(conf.candidate_scan_page, &skipped);

        if inode_ids.is_empty() {
            log::debug!("cluster-evict: candidate set exhausted, stopping eviction");
            break;
        }

        if conf.dry_run {
            log::debug!(
                "cluster-evict: dry_run=true, would process inode_ids_step={}",
                inode_ids.len()
            );
            break;
        }

        examined += inode_ids.len();
        let step = execute_eviction_step(fs, evictor, ttl_executor, &inode_ids);
        // Staged victims have already left the namespace and are reclaimed by
        // the purger within a cycle, so they reduce the remaining target too;
        // only `total_reclaimed` (and the freed-bytes metric) may not count them
        // before their blocks are actually released.
        let released = step.reclaimed_bytes.saturating_add(step.staged_bytes);
        plan.target_free_bytes = plan.target_free_bytes.saturating_sub(released);
        total_reclaimed += step.reclaimed_bytes;
        skipped.extend(step.skipped);
    }

    (examined, total_reclaimed)
}

/// Consumes one page of candidates.
///
/// Handled victims leave the evictor. A staged victim reports zero reclaimed
/// bytes — its blocks are released later by the purger — but the bytes its name
/// held are reported as `staged_bytes`, so the caller can still converge on the
/// byte target instead of scanning the whole candidate set. Candidates that are
/// not expirable yet, and directories, are reported as skipped so the caller can
/// advance past them without dropping them from the evictor.
fn execute_eviction_step(
    fs: &MasterFilesystem,
    evictor: &dyn Evictor,
    ttl_executor: &InodeTtlExecutor,
    inode_ids: &[i64],
) -> EvictionStep {
    let mut step = EvictionStep::default();
    let mut reclaimed_files = 0i64;

    // (inode id, bytes the name still holds). A hard-linked name keeps its
    // blocks after removal, so it contributes nothing to the byte target.
    let mut file_inode_ids: Vec<(i64, i64)> = Vec::with_capacity(inode_ids.len());
    {
        let fs_guard = fs.fs_dir.read();
        for &inode_id in inode_ids {
            match fs_guard.store.get_inode(inode_id, None) {
                Ok(Some(InodeView::File(f))) => {
                    let planned = if f.nlink() <= 1 { f.len.max(0) } else { 0 };
                    file_inode_ids.push((inode_id, planned));
                }
                // A missing inode is already gone; drop it from the evictor.
                Ok(None) => step.consumed.push(inode_id),
                Ok(Some(_)) | Err(_) => step.skipped.push(inode_id),
            }
        }
    }

    for (inode_id, planned_bytes) in file_inode_ids {
        // Evict into the async purge lane: the entry is renamed immediately and
        // reclaimed by the purger within a cycle, instead of waiting out
        // `trash_retention` while the cluster stays over quota.
        match ttl_executor.execute_by_id_with_mode(inode_id, DeleteMode::AsyncPurge) {
            Ok((true, _, reclaimed)) => {
                step.consumed.push(inode_id);
                if reclaimed > 0 {
                    reclaimed_files += 1;
                    step.reclaimed_bytes += reclaimed;
                } else {
                    // Staged for the purger: the bytes left the namespace but are
                    // not released yet, so they are planned rather than freed.
                    step.staged_bytes += planned_bytes;
                }
            }
            // Not expirable yet, or a hard-linked name that keeps its blocks:
            // leave it in the evictor for a later trigger.
            Ok((false, _, _)) => step.skipped.push(inode_id),
            Err(FsError::FileNotFound(_)) => step.consumed.push(inode_id),
            Err(e) => {
                log::warn!(
                    "cluster-evict: executor failed for inode_id={}, err={}",
                    inode_id,
                    e
                );
                step.skipped.push(inode_id);
            }
        }
    }

    if !step.consumed.is_empty() {
        evictor.remove_victims(&step.consumed);
    }

    if reclaimed_files > 0 {
        if let Ok(metrics) = Master::get_metrics() {
            metrics.eviction_files_deleted.inc_by(reclaimed_files);
            metrics.eviction_bytes_freed.inc_by(step.reclaimed_bytes);
        }

        log::info!(
            "cluster-evict: reclaimed {} files, freed {} bytes",
            reclaimed_files,
            step.reclaimed_bytes
        );
    }

    step
}

#[derive(Debug, Default)]
struct EvictionStep {
    /// Candidates removed from the evictor (purged, excluded, or gone).
    consumed: Vec<i64>,
    /// Candidates examined but still eligible on a later trigger.
    skipped: Vec<i64>,
    /// Bytes actually released by this step. Feeds the freed-bytes metric.
    reclaimed_bytes: i64,
    /// Bytes whose names left the namespace because the victim was staged for
    /// the async purger. Counted against the trigger target so the scan
    /// converges, but never reported as freed.
    staged_bytes: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::fs::DeleteMode;
    use crate::master::journal::JournalSystem;
    use crate::master::meta::inode::ttl::InodeTtlExecutor;
    use crate::master::quota::eviction::evictor::LRUEvictor;
    use crate::master::quota::eviction::{EvictionConf, EvictionMode, EvictionPolicy};
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_model::{CreateFileOptsBuilder, ListOptions, OpenFlags, TtlAction};
    use curvine_runtime::common::Utils;
    use std::time::Duration;

    fn test_fs(name: &str, trash_enabled: bool) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.trash_enabled = trash_enabled;
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "quota-evict-test/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "quota-evict-test/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        JournalSystem::fs_only_for_test(&conf).unwrap()
    }

    fn eviction_conf(page: usize, max_scan: usize) -> EvictionConf {
        EvictionConf {
            enable_quota_eviction: true,
            eviction_mode: EvictionMode::DeleteFile,
            policy: EvictionPolicy::Lru,
            high_watermark: 0.8,
            low_watermark: 0.6,
            candidate_scan_page: page,
            max_scan,
            dry_run: false,
            capacity: 1_000,
        }
    }

    fn plan_with_target(target: i64) -> EvictPlan {
        EvictPlan {
            trigger_used: 0,
            quota_size: 0,
            target_free_bytes: target,
        }
    }

    fn create_expired_file(fs: &MasterFilesystem, path: &str) -> i64 {
        create_expired_sized_file(fs, path, 0)
    }

    fn create_expired_sized_file(fs: &MasterFilesystem, path: &str, len: i64) -> i64 {
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_ms(1)
            .ttl_action(TtlAction::Delete)
            .build();
        let status = fs
            .create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))
            .unwrap();
        fs.complete_file(path, None, len, vec![], "", false, None)
            .unwrap();
        status.id
    }

    // A trigger must not stop at the first page: the default two-entry page
    // used to end the scan while the coldest inodes were live files, so an
    // expired file deeper in the LRU was never reclaimed.
    #[test]
    fn eviction_scan_continues_past_live_candidates() {
        let fs = test_fs("past-live", false);
        let live_a = fs.create("/scan/live-a.log", true).unwrap().id;
        let live_b = fs.create("/scan/live-b.log", true).unwrap().id;
        let expired = create_expired_file(&fs, "/scan/expired.log");
        std::thread::sleep(Duration::from_millis(10));

        let conf = eviction_conf(2, 0);
        let evictor = LRUEvictor::new(conf.clone());
        evictor.on_access(live_a);
        evictor.on_access(live_b);
        evictor.on_access(expired);

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let mut plan = plan_with_target(1);
        let (examined, reclaimed) = run_eviction_plan(&fs, &evictor, &executor, &conf, &mut plan);

        assert!(
            fs.file_status("/scan/expired.log").is_err(),
            "the expired file behind the live page must still be purged"
        );
        assert!(fs.file_status("/scan/live-a.log").is_ok());
        assert!(fs.file_status("/scan/live-b.log").is_ok());
        assert_eq!(examined, 3, "both live candidates plus the expired one");
        assert_eq!(reclaimed, 0, "an empty file releases no bytes");
    }

    // Trash entries are not eviction candidates: they are consumed (removed
    // from the evictor) so they cannot stall the scan, but they must not be
    // reported as reclaimed space.
    #[test]
    fn eviction_consumes_trash_candidates_without_counting_them() {
        let fs = test_fs("trash-consumed", true);
        let victim = fs.create("/trash/victim.log", true).unwrap().id;
        fs.delete_with_mode("/trash/victim.log", false, DeleteMode::Default)
            .unwrap();

        let mut entries = fs.list_options("/.Trash", ListOptions::default()).unwrap();
        assert_eq!(entries.len(), 1, "the delete must have been recycled");
        let entry_path = entries.remove(0).path;

        let conf = eviction_conf(1, 0);
        let evictor = LRUEvictor::new(conf);
        evictor.on_access(victim);

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let step = execute_eviction_step(&fs, &evictor, &executor, &[victim]);

        assert_eq!(step.reclaimed_bytes, 0, "a trashed file frees nothing");
        assert_eq!(
            step.consumed,
            vec![victim],
            "the trash entry must leave the eviction candidate set"
        );
        assert!(step.skipped.is_empty());
        assert!(
            fs.exists(entry_path).unwrap(),
            "eviction must not purge trash entries ahead of retention"
        );
    }

    // Live candidates are skipped, not consumed: they must stay in the evictor
    // for a later trigger while the scan advances past them.
    #[test]
    fn eviction_skips_live_candidates_without_dropping_them() {
        let fs = test_fs("live-skipped", false);
        let live = fs.create("/scan/live.log", true).unwrap().id;

        let conf = eviction_conf(1, 0);
        let evictor = LRUEvictor::new(conf);
        evictor.on_access(live);

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let step = execute_eviction_step(&fs, &evictor, &executor, &[live]);

        assert_eq!(step.reclaimed_bytes, 0);
        assert!(step.consumed.is_empty());
        assert_eq!(step.skipped, vec![live]);
        assert_eq!(evictor.cache_size(), 1, "the live file stays a candidate");
    }

    #[test]
    fn eviction_scan_is_bounded_by_max_scan() {
        let fs = test_fs("scan-cap", false);
        let conf = eviction_conf(1, 2);
        let evictor = LRUEvictor::new(conf.clone());
        for i in 0..5 {
            let status = fs.create(format!("/cap/f{}.log", i), true).unwrap();
            evictor.on_access(status.id);
        }

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let mut plan = plan_with_target(1_000_000);
        let (examined, reclaimed) = run_eviction_plan(&fs, &evictor, &executor, &conf, &mut plan);

        assert_eq!(
            examined, 2,
            "the scan budget must bound the per-trigger work"
        );
        assert_eq!(reclaimed, 0);
        assert_eq!(
            evictor.cache_size(),
            5,
            "skipped candidates remain for a later trigger"
        );
    }

    // Eviction must not park victims in the recycle bin for the whole retention
    // window, or eviction could never relieve pressure. A victim is staged for
    // the async purger instead, and the step still reports no reclaimed bytes
    // because nothing has been reclaimed yet.
    #[test]
    fn eviction_stages_victims_for_the_async_purger() {
        let fs = test_fs("evict-async-purge", true);
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);
        let victim = create_expired_sized_file(&fs, "/evict/victim.log", 17);
        std::thread::sleep(Duration::from_millis(10));

        let conf = eviction_conf(1, 0);
        let evictor = LRUEvictor::new(conf);
        evictor.on_access(victim);

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let step = execute_eviction_step(&fs, &evictor, &executor, &[victim]);

        assert_eq!(step.reclaimed_bytes, 0, "a staged victim frees nothing yet");
        assert_eq!(
            step.staged_bytes, 17,
            "the staged bytes must still be planned against the target"
        );
        assert_eq!(step.consumed, vec![victim]);
        assert!(
            fs.file_status("/evict/victim.log").is_err(),
            "the victim must leave the namespace immediately"
        );
        assert!(
            fs.list_trash().unwrap().is_empty(),
            "an evicted victim must not wait in the recoverable bin"
        );
        // `.pending` is hidden from client-facing listings, so inspect it
        // through the purger's own scan.
        assert_eq!(
            pending_entries(&fs).len(),
            1,
            "the victim must be staged for the purger"
        );

        // One purger cycle reclaims it, so the space comes back within a tick.
        fs.drain_pending_trash().unwrap().unwrap();
        assert!(pending_entries(&fs).is_empty());
    }

    fn pending_entries(fs: &MasterFilesystem) -> Vec<String> {
        fs.scan_pending_trash(usize::MAX, None)
            .map(|page| page.paths)
            .unwrap_or_default()
    }

    // The byte target must be reachable from staged victims alone. Counting only
    // `reclaimed_bytes` (always zero for an async purge) left `target_free_bytes`
    // untouched, so one trigger staged the whole candidate set instead of
    // stopping at the low watermark.
    #[test]
    fn eviction_target_converges_on_staged_bytes() {
        let fs = test_fs("evict-staged-target", true);
        fs.master_monitor
            .set_role_for_test(curvine_raft::raft::RoleState::Leader);

        let mut victims = Vec::new();
        for i in 0..4 {
            let id = create_expired_sized_file(&fs, &format!("/evict/f{}.log", i), 100);
            victims.push(id);
        }
        std::thread::sleep(Duration::from_millis(10));

        // Two candidates per page, 100 bytes each: a 150-byte target is met by
        // the first page, so the scan must stop there.
        let conf = eviction_conf(2, 0);
        let evictor = LRUEvictor::new(conf.clone());
        for &id in &victims {
            evictor.on_access(id);
        }

        let executor = InodeTtlExecutor::with_managers(fs.clone());
        let mut plan = plan_with_target(150);
        let (examined, reclaimed) = run_eviction_plan(&fs, &evictor, &executor, &conf, &mut plan);

        assert_eq!(
            examined, 2,
            "the target must stop the scan after the first page"
        );
        assert_eq!(reclaimed, 0, "a staged victim releases no bytes yet");
        assert!(
            plan.target_free_bytes <= 0,
            "staged bytes must count against the target, left {}",
            plan.target_free_bytes
        );
        assert_eq!(
            evictor.cache_size(),
            2,
            "the untouched candidates stay for a later trigger"
        );
        assert_eq!(pending_entries(&fs).len(), 2, "only one page is staged");
    }
}
