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

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::ttl_executor::InodeTtlExecutor;
use crate::master::meta::inode::InodeView;
use crate::master::quota::eviction::evictor::Evictor;
use crate::master::quota::eviction::types::EvictPlan;
use crate::master::quota::eviction::EvictionConf;
use crate::master::quota::eviction::EvictionMode;
use curvine_common::state::MasterInfo;
use orpc::runtime::{RpcRuntime, Runtime};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct QuotaManager {
    eviction_conf: EvictionConf,
    fs: MasterFilesystem,
    evictor: Arc<dyn Evictor>,
    ttl_executor: InodeTtlExecutor,
    tx: Sender<Option<MasterInfo>>,
}

impl QuotaManager {
    pub fn new(
        eviction_conf: EvictionConf,
        fs: MasterFilesystem,
        evictor: Arc<dyn Evictor>,
        rt: Arc<Runtime>,
    ) -> Arc<Self> {
        let (tx, mut rx): (Sender<Option<MasterInfo>>, Receiver<Option<MasterInfo>>) =
            mpsc::channel(1024);

        let manager = Arc::new(QuotaManager {
            evictor,
            ttl_executor: InodeTtlExecutor::new(fs.clone()),
            eviction_conf,
            fs: fs.clone(),
            tx,
        });

        let mgr = manager.clone();

        rt.spawn(async move {
            while let Some(cluster_info) = rx.recv().await {
                mgr.handle_trigger(cluster_info);
            }
        });

        manager
    }

    fn is_eviction_enabled(&self) -> bool {
        self.eviction_conf.enable_quota_eviction
    }

    pub fn detector(&self, info: Option<MasterInfo>) {
        let _ = self.tx.try_send(info);
    }

    fn handle_trigger(&self, cluster_info: Option<MasterInfo>) {
        if !self.is_eviction_enabled() {
            return;
        }

        let Some(info) = cluster_info else {
            log::warn!("cluster-evict: failed to fetch master_info");
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

        log::info!(
            "cluster-evict: starting eviction, curvine_used={}, curvine_quota={}, usage_ratio={:.2}%, target_free={}",
            curvine_used, curvine_quota, (curvine_used as f64 / curvine_quota as f64) * 100.0, plan.target_free_bytes
        );

        loop {
            let step_free = plan.target_free_bytes;

            if step_free <= 0 {
                log::debug!("cluster-evict: step_free <= 0, stopping eviction");
                break;
            }

            let inode_ids = self
                .evictor
                .select_victims(self.eviction_conf.candidate_scan_page);

            if inode_ids.is_empty() {
                log::debug!("cluster-evict: no more victims available, stopping eviction");
                break;
            }

            if self.eviction_conf.dry_run {
                log::debug!(
                    "cluster-evict: dry_run=true, would process inode_ids_step={}",
                    inode_ids.len()
                );
                break;
            }

            let total_freed = {
                let fs_guard = self.fs.fs_dir.read();
                let freed = inode_ids
                    .iter()
                    .filter_map(|&inode_id| fs_guard.store.get_inode(inode_id, None).ok().flatten())
                    .map(|inode_view| match &inode_view {
                        InodeView::File(_, f) => f.len.max(0),
                        _ => 0,
                    })
                    .sum::<i64>();
                freed
            };

            if total_freed <= 0 {
                log::debug!("cluster-evict: total_freed <= 0, stopping eviction");
                break;
            }

            self.execute_eviction(self.eviction_conf.eviction_mode, &inode_ids);

            plan.target_free_bytes = plan.target_free_bytes.saturating_sub(total_freed);

            if plan.target_free_bytes <= 0 {
                log::info!("cluster-evict: reached target_free_bytes, stopping eviction");
                break;
            }
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

    fn execute_eviction(&self, mode: EvictionMode, inode_ids: &[i64]) {
        let mut successfully_evicted = Vec::with_capacity(inode_ids.len());

        for inode_id_i64 in inode_ids {
            let inode_id = *inode_id_i64 as u64;
            let res = match mode {
                EvictionMode::FreeFile => self.ttl_executor.free_inode(inode_id),
                EvictionMode::DeleteFile => self.ttl_executor.delete_inode(inode_id),
            };

            match res {
                Ok(_) => {
                    successfully_evicted.push(*inode_id_i64);
                }
                Err(e) => {
                    log::warn!(
                        "prequota-evict: executor failed for inode_id={}, err={}",
                        inode_id,
                        e
                    );
                }
            }
        }

        if !successfully_evicted.is_empty() {
            self.evictor.remove_victims(&successfully_evicted);
        }
    }
}
