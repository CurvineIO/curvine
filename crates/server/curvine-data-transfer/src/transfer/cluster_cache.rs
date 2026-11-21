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

use curvine_client_core::file::CurvineFileSystem;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_model::{JobTaskType, MountInfo, TransferKind, WorkerInfo};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::RpcRuntime;
use log::warn;
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

    #[cfg(test)]
    pub(crate) fn set_test_snapshot(&self, snapshot: ClusterSnapshot) {
        *self.snapshot.write() = snapshot;
    }

    pub fn check_ready(&self) -> FsResult<()> {
        let snapshot = self.snapshot();
        if snapshot.updated_at <= 0 {
            return Err(FsError::common(
                "Cluster metadata is unavailable; retry shortly",
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
        let fs_info = self.fs.get_filesystem_info().await?;
        let mut workers = fs_info.live_workers;
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
        matches: impl Fn(&MountInfo) -> bool,
    ) -> FsResult<MountInfo> {
        self.find_mount_with_version(kind, source, target, &matches)
            .map(|snapshot| snapshot.mount)
    }

    fn find_mount_with_version(
        &self,
        kind: TransferKind,
        source: &Path,
        target: &Path,
        matches: &impl Fn(&MountInfo) -> bool,
    ) -> FsResult<MountSnapshot> {
        let snapshot = self.snapshot();
        if snapshot.mounts.is_empty() {
            return Err(FsError::common("Cluster mount snapshot is unavailable"));
        }
        self.check_snapshot_fresh(&snapshot)?;
        Self::matching_mount_snapshot(&snapshot, matches).ok_or_else(|| {
            FsError::common(format!(
                "No mount found for {:?}: {} -> {}",
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
        matches: impl Fn(&MountInfo) -> bool,
    ) -> FsResult<MountSnapshot> {
        let snapshot = self.snapshot();
        if self.check_snapshot_fresh(&snapshot).is_ok() {
            if let Some(snapshot) = Self::matching_mount_snapshot(&snapshot, &matches) {
                return Ok(snapshot);
            }
        }

        let _guard = self.refresh_lock.lock();
        let snapshot = self.snapshot();
        if self.check_snapshot_fresh(&snapshot).is_ok() {
            if let Some(snapshot) = Self::matching_mount_snapshot(&snapshot, &matches) {
                return Ok(snapshot);
            }
        }

        // SubmitTransfer is synchronous and may run within a Tokio task.
        self.refresh_blocking()?;
        self.find_mount_with_version(kind, source, target, &matches)
    }

    fn matching_mount_snapshot(
        snapshot: &ClusterSnapshot,
        matches: &impl Fn(&MountInfo) -> bool,
    ) -> Option<MountSnapshot> {
        snapshot
            .mounts
            .iter()
            .find(|mount| matches(mount))
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

    /// Blocking CV `get_status` for transfer submit/repair decisions.
    ///
    /// `TransferService::submit_transfer` is synchronous, so callers use this helper
    /// instead of awaiting `CurvineFileSystem::get_status` directly.
    pub fn get_status_blocking(&self, path: &Path) -> FsResult<curvine_model::FileStatus> {
        let fs = self.fs.clone();
        let path = path.clone();
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let rt = fs.clone_runtime();
                    rt.block_on(fs.get_status(&path))
                })
                .join()
                .map_err(|_| FsError::common("Transfer get_status thread panicked"))?
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

    /// Return live workers that can execute the requested transfer kind.
    ///
    /// Legacy workers are represented with Load/Export by `ProtoUtils` when they
    /// advertise transfer submission but omit the task-type field.
    pub fn live_workers_for_task(&self, task_type: JobTaskType) -> FsResult<Vec<WorkerInfo>> {
        let snapshot = self.snapshot();
        self.check_snapshot_fresh(&snapshot)?;
        let workers: Vec<_> = snapshot
            .workers
            .into_iter()
            .filter(|worker| {
                worker.is_live()
                    && !worker.worker_session_id.is_empty()
                    && worker.transfer_capabilities.supports_transfer()
                    && worker_supports_task(worker, task_type)
            })
            .collect();
        if workers.is_empty() {
            return Err(missing_task_capability(task_type));
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
                "Cluster metadata snapshot is stale; retry shortly",
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

fn worker_supports_task(worker: &WorkerInfo, task_type: JobTaskType) -> bool {
    worker.transfer_capabilities.task_types.contains(&task_type)
}

fn missing_task_capability(task_type: JobTaskType) -> FsError {
    FsError::common(format!(
        "No live worker supports transfer task type {}",
        task_type.as_str(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{JobTaskType, TransferWorkerCapabilities, WorkerAddress};

    fn worker(task_types: Vec<JobTaskType>) -> WorkerInfo {
        let mut worker = WorkerInfo::new(WorkerAddress::default(), 1);
        worker.transfer_capabilities = TransferWorkerCapabilities {
            task_submit: true,
            report_target: true,
            query_task: true,
            attempt_safe_output: true,
            source_read_plan: true,
            task_result: true,
            task_types,
        };
        worker
    }

    #[test]
    fn task_not_dispatched_to_incapable_worker() {
        let workers = [worker(vec![JobTaskType::Load])];
        assert!(workers
            .iter()
            .all(|worker| !worker_supports_task(worker, JobTaskType::Export)));
        assert!(workers
            .iter()
            .any(|worker| worker_supports_task(worker, JobTaskType::Load)));
    }

    #[test]
    fn pending_message_names_missing_capability() {
        assert!(missing_task_capability(JobTaskType::Export)
            .to_string()
            .contains("export"));
    }

    #[test]
    fn legacy_worker_still_receives_load() {
        let legacy = worker(vec![JobTaskType::Load, JobTaskType::Export]);
        assert!(worker_supports_task(&legacy, JobTaskType::Load));
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
