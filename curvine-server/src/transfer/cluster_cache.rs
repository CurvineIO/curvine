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

use curvine_client::file::CurvineFileSystem;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{MountInfo, TransferKind, WorkerInfo};
use curvine_common::FsResult;
use log::warn;
use orpc::common::LocalTime;
use orpc::runtime::RpcRuntime;
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::transfer::TransferMetrics;

#[derive(Clone, Default)]
pub struct ClusterSnapshot {
    pub version: u64,
    pub mounts: Vec<MountInfo>,
    pub workers: Vec<WorkerInfo>,
    pub updated_at: i64,
}

#[derive(Clone)]
pub struct ClusterMetadataCache {
    fs: CurvineFileSystem,
    snapshot: Arc<RwLock<ClusterSnapshot>>,
    rejected_worker_sessions: Arc<RwLock<HashSet<(u32, String)>>>,
    refresh_lock: Arc<Mutex<()>>,
    max_staleness: Duration,
    allow_stale_snapshot: bool,
}

impl ClusterMetadataCache {
    pub fn new(fs: CurvineFileSystem) -> Self {
        Self::with_snapshot_policy(fs, Duration::MAX, true)
    }

    pub fn with_snapshot_policy(
        fs: CurvineFileSystem,
        max_staleness: Duration,
        allow_stale_snapshot: bool,
    ) -> Self {
        Self {
            fs,
            snapshot: Arc::new(RwLock::new(ClusterSnapshot::default())),
            rejected_worker_sessions: Arc::new(RwLock::new(HashSet::new())),
            refresh_lock: Arc::new(Mutex::new(())),
            max_staleness,
            allow_stale_snapshot,
        }
    }

    pub fn snapshot(&self) -> ClusterSnapshot {
        self.snapshot.read().clone()
    }

    pub fn check_ready(&self) -> FsResult<()> {
        let snapshot = self.snapshot();
        if snapshot.updated_at <= 0 {
            return Err(FsError::common(
                "Transfer cluster metadata is unavailable; retry shortly",
            ));
        }
        self.check_snapshot_fresh(&snapshot)
    }

    pub fn remove_worker_session(&self, worker_id: u32, worker_session_id: &str) {
        self.rejected_worker_sessions
            .write()
            .insert((worker_id, worker_session_id.to_string()));
        let mut snapshot = self.snapshot.write();
        snapshot.workers.retain(|worker| {
            !(worker.worker_id() == worker_id && worker.worker_session_id == worker_session_id)
        });
    }

    pub async fn refresh(&self) -> FsResult<()> {
        let start = Instant::now();
        let result = self.refresh_inner().await;
        let snapshot = self.snapshot();
        let (live_workers, capable_workers) = worker_counts(&snapshot.workers);
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.observe_cluster_snapshot_refresh(
                if result.is_ok() { "success" } else { "failure" },
                start.elapsed().as_micros(),
                if result.is_ok() {
                    Some(snapshot.version)
                } else {
                    None
                },
                if snapshot.updated_at > 0 {
                    Some(snapshot.updated_at)
                } else {
                    None
                },
                Some(live_workers),
                Some(capable_workers),
            );
        }
        result
    }

    async fn refresh_inner(&self) -> FsResult<()> {
        let mounts = self.fs.get_mount_table().await?;
        let master = self.fs.get_master_info().await?;
        let mut workers = master.live_workers;
        {
            let mut rejected = self.rejected_worker_sessions.write();
            rejected.retain(|(worker_id, worker_session_id)| {
                workers.iter().any(|worker| {
                    worker.worker_id() == *worker_id
                        && worker.worker_session_id == *worker_session_id
                })
            });
            workers.retain(|worker| {
                !rejected.contains(&(worker.worker_id(), worker.worker_session_id.clone()))
            });
        }
        let mut snapshot = self.snapshot.write();
        snapshot.version = snapshot.version.saturating_add(1);
        snapshot.mounts = mounts;
        snapshot.workers = workers;
        snapshot.updated_at = LocalTime::mills() as i64;
        Ok(())
    }

    pub fn find_mount(
        &self,
        kind: TransferKind,
        source: &Path,
        target: &Path,
    ) -> FsResult<MountInfo> {
        self.find_mount_with_version(kind, source, target)
            .map(|snapshot| snapshot.mount)
    }

    pub fn find_mount_with_version(
        &self,
        kind: TransferKind,
        source: &Path,
        target: &Path,
    ) -> FsResult<MountSnapshot> {
        let snapshot = self.snapshot();
        if snapshot.mounts.is_empty() {
            return Err(FsError::common(
                "Transfer cluster mount snapshot is unavailable; retry shortly",
            ));
        }
        self.check_snapshot_fresh(&snapshot)?;
        self.matching_mount_snapshot(kind, source, target, &snapshot)
            .ok_or_else(|| {
                FsError::common(format!(
                    "No mount can serve {:?} from {} to {}",
                    kind,
                    source.full_path(),
                    target.full_path()
                ))
            })
    }

    pub fn find_mount_with_refresh(
        &self,
        kind: TransferKind,
        source: &Path,
        target: &Path,
    ) -> FsResult<MountSnapshot> {
        let snapshot = self.snapshot();
        self.check_snapshot_fresh(&snapshot)?;
        if let Some(snapshot) = self.matching_mount_snapshot(kind, source, target, &snapshot) {
            return Ok(snapshot);
        }

        let _guard = self.refresh_lock.lock();
        let snapshot = self.snapshot();
        self.check_snapshot_fresh(&snapshot)?;
        if let Some(snapshot) = self.matching_mount_snapshot(kind, source, target, &snapshot) {
            return Ok(snapshot);
        }

        // SubmitTransfer is synchronous and may run within a Tokio task.
        self.refresh_blocking()?;
        self.find_mount_with_version(kind, source, target)
    }

    fn matching_mount_snapshot(
        &self,
        kind: TransferKind,
        source: &Path,
        target: &Path,
        snapshot: &ClusterSnapshot,
    ) -> Option<MountSnapshot> {
        let source_text = if source.is_cv() {
            source.path().to_string()
        } else {
            source.full_path().to_string()
        };
        let target_text = if target.is_cv() {
            target.path().to_string()
        } else {
            target.full_path().to_string()
        };

        snapshot
            .mounts
            .iter()
            .find(|mount| match kind {
                TransferKind::Load => {
                    source_text.starts_with(&mount.ufs_path)
                        && target_text.starts_with(&mount.cv_path)
                }
                TransferKind::Export => {
                    source_text.starts_with(&mount.cv_path)
                        && target_text.starts_with(&mount.ufs_path)
                }
            })
            .cloned()
            .map(|mount| MountSnapshot {
                mount,
                version: snapshot.version,
            })
    }

    fn refresh_blocking(&self) -> FsResult<()> {
        let cache = self.clone();
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let rt = cache.fs.clone_runtime();
                    rt.block_on(cache.refresh())
                })
                .join()
                .map_err(|_| FsError::common("Transfer cluster metadata refresh thread panicked"))?
        })
    }

    pub fn live_workers(&self) -> FsResult<Vec<WorkerInfo>> {
        let snapshot = self.snapshot();
        self.check_snapshot_fresh(&snapshot)?;
        let (live_count, capable_count) = worker_counts(&snapshot.workers);
        if let Ok(metrics) = TransferMetrics::get() {
            metrics.set_cluster_snapshot(
                Some(snapshot.version),
                if snapshot.updated_at > 0 {
                    Some(snapshot.updated_at)
                } else {
                    None
                },
                Some(live_count),
                Some(capable_count),
            );
        }
        let workers: Vec<_> = snapshot
            .workers
            .into_iter()
            .filter(|worker| {
                worker.is_live()
                    && !worker.worker_session_id.is_empty()
                    && worker.transfer_capabilities.supports_transfer()
            })
            .collect();
        if workers.is_empty() {
            return Err(FsError::common(
                "No live worker with required transfer capabilities is available",
            ));
        }
        Ok(workers)
    }

    fn check_snapshot_fresh(&self, snapshot: &ClusterSnapshot) -> FsResult<()> {
        if self.allow_stale_snapshot {
            return Ok(());
        }
        let updated_at = snapshot.updated_at;
        if updated_at <= 0 {
            return Err(FsError::common(
                "Transfer cluster metadata is unavailable; retry shortly",
            ));
        }
        let max_staleness_ms = self.max_staleness.as_millis().min(i64::MAX as u128) as i64;
        let staleness_ms = (LocalTime::mills() as i64).saturating_sub(updated_at);
        if staleness_ms > max_staleness_ms {
            return Err(FsError::common(
                "Transfer cluster metadata is stale; retry shortly",
            ));
        }
        Ok(())
    }

    pub fn start_refresh_loop(self, interval: Duration, stop: Arc<AtomicBool>) {
        let rt = self.fs.clone_runtime();
        rt.spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Err(err) = self.refresh().await {
                    warn!("refresh transfer cluster metadata failed: {}", err);
                }
                if wait_or_stopped(interval, &stop).await {
                    break;
                }
            }
        });
    }
}

fn worker_counts(workers: &[WorkerInfo]) -> (usize, usize) {
    let live = workers.iter().filter(|worker| worker.is_live()).count();
    let capable = workers
        .iter()
        .filter(|worker| worker.is_live() && worker.transfer_capabilities.supports_transfer())
        .count();
    (live, capable)
}

async fn wait_or_stopped(interval: Duration, stop: &AtomicBool) -> bool {
    let deadline = Instant::now() + interval;
    while !stop.load(Ordering::Relaxed) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }
    true
}

#[derive(Clone)]
pub struct MountSnapshot {
    pub mount: MountInfo,
    pub version: u64,
}
