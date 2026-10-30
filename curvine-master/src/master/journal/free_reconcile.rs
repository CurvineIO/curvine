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

//! Free reconciliation for fs_mode files under transfer mode.
//!
//! Exports are asynchronous and best effort, so a file can reach Free while its
//! UFS copy is missing or holds another version. Releasing the Curvine copy
//! then loses data: fs_mode reads a released file from UFS by its current path.
//! A Curvine copy is therefore released only after the object at the file's
//! current UFS path is shown to hold the file's current version; otherwise the
//! file is re-exported and kept. Recursive Free also removes UFS entries with
//! no CV metadata under the directory.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, Path};
use curvine_metrics::{Counter, Metrics as m};
use curvine_model::{FileGen, FileStatus, StorageState, CV_GEN_METADATA_KEY};
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use log::{debug, info, warn};
use once_cell::sync::Lazy;
use tokio::sync::{mpsc, Semaphore};

use crate::master::fs::FsModeSnapshot;
use crate::master::{FsModeExport, JobManager};

struct FreeReconcileMetrics {
    deferred: Counter,
    queue_dropped: Counter,
    released: Counter,
    skipped_young: Counter,
    skipped_changed: Counter,
    reexported: Counter,
    verify_failed: Counter,
    failed: Counter,
}

static FREE_RECONCILE_METRICS: Lazy<FreeReconcileMetrics> = Lazy::new(|| FreeReconcileMetrics {
    deferred: m::new_counter(
        "fs_mode_free_reconcile_deferred_total",
        "fs_mode releases handed to reconciliation",
    )
    .expect("register fs_mode free deferred metric"),
    queue_dropped: m::new_counter(
        "fs_mode_free_reconcile_queue_dropped_total",
        "fs_mode releases dropped because the queue was full",
    )
    .expect("register fs_mode free queue dropped metric"),
    released: m::new_counter(
        "fs_mode_free_released_total",
        "fs_mode files released after UFS verification",
    )
    .expect("register fs_mode free released metric"),
    skipped_young: m::new_counter(
        "fs_mode_free_skipped_young_total",
        "fs_mode releases postponed because the UFS object is younger than the minimum age",
    )
    .expect("register fs_mode free skipped young metric"),
    skipped_changed: m::new_counter(
        "fs_mode_free_skipped_changed_total",
        "fs_mode releases dropped because the file changed during verification",
    )
    .expect("register fs_mode free skipped changed metric"),
    reexported: m::new_counter(
        "fs_mode_free_reexport_total",
        "fs_mode exports submitted by Free reconciliation",
    )
    .expect("register fs_mode free reexport metric"),
    verify_failed: m::new_counter(
        "fs_mode_free_verify_failed_total",
        "fs_mode objects that did not match the file's current version",
    )
    .expect("register fs_mode free verify failed metric"),
    failed: m::new_counter(
        "fs_mode_free_failed_total",
        "fs_mode reconciliation attempts that errored",
    )
    .expect("register fs_mode free failed metric"),
});

/// Minimum age of the UFS object before it is trusted for a release. A copy of
/// a superseded version still in flight finishes or dies well within two task
/// leases; until then it could still replace the verified object.
pub const MIN_OBJECT_AGE: Duration = Duration::from_secs(10 * 60);

/// One re-export per version per window. A copy takes time to land, and every
/// TTL round would otherwise submit the same file again.
pub const RESUBMIT_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Bounded so TTL pressure cannot grow memory. A full queue rejects a directory
/// cleanup; inode release requests are retried on later TTL rounds.
const RELEASE_QUEUE_DEPTH: usize = 4096;

/// Each check is one UFS `stat`; object stores cannot batch it because the
/// generation lives in user metadata that `LIST` does not return.
const VERIFY_CONCURRENCY: usize = 32;

/// Boundary between the release paths (TTL, `cv free`) and reconciliation.
pub trait FsModeReleaser: Send + Sync {
    /// True when `path` lies in an fs_mode mount. A lookup failure answers true:
    /// it must never turn into an unverified release.
    fn is_fs_mode(&self, path: &str) -> bool;

    /// True when an fs_mode mount point lies strictly under `path`.
    fn contains_fs_mode_mount(&self, path: &str) -> bool;

    /// Hand `inode_id` to reconciliation. Non-blocking.
    fn defer_release(&self, inode_id: i64);

    /// Scan a recursively freed directory for UFS entries without CV metadata.
    fn defer_cleanup(&self, path: &str) -> FsResult<()>;
}

enum ReconcileTask {
    Release(i64),
    Cleanup(String),
}

pub struct FreeReconciler {
    // MasterFilesystem owns the reconciler, so retaining JobManager here would
    // form a cycle through JobManager's MasterFilesystem clone.
    job_manager: Weak<JobManager>,
    pub(super) runtime: Arc<AsyncRuntime>,
    queue: mpsc::Sender<ReconcileTask>,
    /// `(inode, version) -> last re-export time`, for `RESUBMIT_INTERVAL`.
    exported: Mutex<HashMap<(i64, FileGen), i64>>,
}

impl FreeReconciler {
    /// Run Free reconciliation separately from the journal runtime.
    pub fn start(job_manager: Arc<JobManager>) -> Arc<Self> {
        let (queue, rx) = mpsc::channel(RELEASE_QUEUE_DEPTH);
        let reconciler = Arc::new(Self {
            job_manager: Arc::downgrade(&job_manager),
            runtime: Arc::new(AsyncRuntime::new("free-reconcile", 2, 2)),
            queue,
            exported: Mutex::new(HashMap::new()),
        });

        let worker = Arc::downgrade(&reconciler);
        drop(
            reconciler
                .runtime
                .spawn(async move { Self::run(worker, rx).await }),
        );
        reconciler
    }

    async fn run(reconciler: Weak<Self>, mut rx: mpsc::Receiver<ReconcileTask>) {
        let permits = Arc::new(Semaphore::new(VERIFY_CONCURRENCY));
        while let Some(task) = rx.recv().await {
            let Ok(permit) = permits.clone().acquire_owned().await else {
                break;
            };
            let Some(worker) = reconciler.upgrade() else {
                break;
            };
            let runtime = worker.runtime.clone();
            drop(runtime.spawn(async move {
                let _permit = permit;
                let result = match task {
                    ReconcileTask::Release(inode_id) => worker.reconcile_one(inode_id).await,
                    ReconcileTask::Cleanup(path) => worker.cleanup_ufs_only(&path).await,
                };
                if let Err(err) = result {
                    FREE_RECONCILE_METRICS.failed.inc();
                    warn!("free reconciliation failed: {}", err);
                }
            }));
        }
        info!("free reconcile worker stopped");
    }

    fn job_manager(&self) -> FsResult<Arc<JobManager>> {
        self.job_manager
            .upgrade()
            .ok_or_else(|| FsError::common("free reconcile: job manager stopped"))
    }

    /// Remove UFS entries only when their CV metadata is absent. Directories
    /// are visited after their children and removed non-recursively. The metadata
    /// lookup and UFS deletion are separate operations, so concurrent writes can
    /// race with cleanup.
    pub(crate) async fn cleanup_ufs_only(&self, root: &str) -> FsResult<()> {
        let job_manager = self.job_manager()?;
        let root = parse_path(root)?;
        let Some((ufs_root, mount)) = job_manager.get_mnt(&root)? else {
            return Ok(());
        };
        if !mount.info.is_fs_mode() {
            return Ok(());
        }
        let ufs = mount.ufs()?;
        let mut pending = vec![(ufs_root.clone(), false)];
        while let Some((dir, visited)) = pending.pop() {
            if visited {
                let cv_path = mount.info.get_cv_path(&dir)?;
                match job_manager.master_fs().file_status(cv_path.path()) {
                    Ok(_) => continue,
                    Err(FsError::FileNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
                match ufs.delete(&dir, false).await {
                    Ok(_) => info!("free reconcile: deleted UFS-only directory {}", dir),
                    Err(FsError::FileNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
                continue;
            }
            let entries = match ufs.list_status(&dir).await {
                Ok(entries) => entries,
                Err(FsError::FileNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if dir != ufs_root {
                pending.push((dir, true));
            }
            for entry in entries {
                let ufs_path = parse_path(&entry.path)?;
                let cv_path = mount.info.get_cv_path(&ufs_path)?;
                // Never let a parent mount's scan operate on a different mount.
                let Some((mapped, current_mount)) = job_manager.get_mnt(&cv_path)? else {
                    continue;
                };
                if current_mount.info.mount_id != mount.info.mount_id || mapped != ufs_path {
                    continue;
                }
                if entry.is_dir {
                    pending.push((ufs_path, false));
                    continue;
                }
                match job_manager.master_fs().file_status(cv_path.path()) {
                    Ok(_) => continue,
                    Err(FsError::FileNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
                match ufs.delete(&ufs_path, false).await {
                    Ok(_) => info!("free reconcile: deleted UFS-only file {}", ufs_path),
                    Err(FsError::FileNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    async fn reconcile_one(&self, inode_id: i64) -> FsResult<()> {
        let job_manager = self.job_manager()?;
        let master_fs = job_manager.master_fs();
        let Some(snapshot) = master_fs.fs_mode_snapshot(inode_id)? else {
            return Ok(());
        };
        // A file still being written exports on its next complete.
        if !snapshot.quiescent {
            return Ok(());
        }

        match snapshot.state {
            StorageState::Ufs => Ok(()),
            // Never confirmed on UFS.
            StorageState::Cv => self.export(inode_id, &snapshot).await,
            StorageState::Both => self.release_if_verified(inode_id, &snapshot).await,
        }
    }

    async fn release_if_verified(&self, inode_id: i64, snapshot: &FsModeSnapshot) -> FsResult<()> {
        let age_ms = (LocalTime::mills() as i64).saturating_sub(snapshot.ufs_mtime);
        if age_ms < MIN_OBJECT_AGE.as_millis() as i64 {
            FREE_RECONCILE_METRICS.skipped_young.inc();
            debug!("free reconcile: {} too young ({}ms)", snapshot.path, age_ms);
            return Ok(());
        }

        let path = parse_path(&snapshot.path)?;
        let job_manager = self.job_manager()?;
        let Some((ufs_path, mnt)) = job_manager.get_mnt(&path)? else {
            return Ok(());
        };
        if !mnt.info.is_fs_mode() {
            return Ok(());
        }
        let ufs = mnt.ufs()?;
        let object = match ufs.get_status(&ufs_path).await {
            Ok(object) => Some(object),
            Err(FsError::FileNotFound(_)) => None,
            Err(err) => return Err(err),
        };
        let has_user_metadata = !ufs.fs_kind().support_rename();
        let verified = object
            .as_ref()
            .is_some_and(|object| object_holds(object, snapshot, has_user_metadata));

        if !verified {
            FREE_RECONCILE_METRICS.verify_failed.inc();
            debug!(
                "free reconcile: {} not verified on UFS, re-exporting",
                snapshot.path
            );
            return self.export(inode_id, snapshot).await;
        }

        // The lock-held recheck by inode id is what makes this safe: a rewrite,
        // a newer mark or a rename onto the path since the snapshot releases
        // nothing.
        if job_manager.master_fs().release_verified(
            inode_id,
            &snapshot.path,
            snapshot.gen,
            snapshot.ufs_mtime,
        )? {
            FREE_RECONCILE_METRICS.released.inc();
            debug!("free reconcile: released {}", snapshot.path);
        } else {
            FREE_RECONCILE_METRICS.skipped_changed.inc();
            debug!(
                "free reconcile: {} changed during verification",
                snapshot.path
            );
        }
        Ok(())
    }

    async fn export(&self, inode_id: i64, snapshot: &FsModeSnapshot) -> FsResult<()> {
        if !self.claim_export(inode_id, snapshot.gen) {
            return Ok(());
        }
        let job_manager = self.job_manager()?;
        let Some(exporter) = job_manager.fs_mode_exporter() else {
            return Ok(());
        };
        let source = parse_path(&snapshot.path)?;
        let Some((target, mnt)) = job_manager.get_mnt(&source)? else {
            return Ok(());
        };
        if !mnt.info.is_fs_mode() {
            return Ok(());
        }
        let file = FsModeExport {
            mount_id: mnt.info.mount_id,
            source,
            target,
            gen: snapshot.gen,
        };
        if exporter.submit(vec![file]).await > 0 {
            FREE_RECONCILE_METRICS.reexported.inc();
        } else {
            // Let the next round try again instead of waiting out the window.
            self.release_claim(inode_id, snapshot.gen);
        }
        Ok(())
    }

    /// True when no export of this version was submitted inside the window.
    fn claim_export(&self, inode_id: i64, gen: FileGen) -> bool {
        let now = LocalTime::mills() as i64;
        let window = RESUBMIT_INTERVAL.as_millis() as i64;
        let mut exported = self.exported.lock().unwrap_or_else(|e| e.into_inner());
        if exported
            .get(&(inode_id, gen))
            .is_some_and(|at| now.saturating_sub(*at) < window)
        {
            return false;
        }
        exported.retain(|_, at| now.saturating_sub(*at) < window);
        exported.insert((inode_id, gen), now);
        true
    }

    fn release_claim(&self, inode_id: i64, gen: FileGen) {
        let mut exported = self.exported.lock().unwrap_or_else(|e| e.into_inner());
        exported.remove(&(inode_id, gen));
    }
}

/// Whether `object` is the file's current version.
///
/// Object stores carry the exact generation the export wrote, and keep it across
/// the server-side copy that implements rename; an object without it was not
/// written by a verified export and is never trusted. Backends without user
/// metadata (HDFS, local) keep the export's millisecond write time across
/// rename, which is what the mark recorded.
pub(crate) fn object_holds(
    object: &FileStatus,
    snapshot: &FsModeSnapshot,
    has_user_metadata: bool,
) -> bool {
    if object.is_dir || object.len != snapshot.gen.len {
        return false;
    }
    if has_user_metadata {
        let expected = snapshot.gen.to_string();
        object
            .x_attr
            .get(CV_GEN_METADATA_KEY)
            .is_some_and(|raw| raw.as_slice() == expected.as_bytes())
    } else {
        object.mtime == snapshot.ufs_mtime
    }
}

fn parse_path(path: &str) -> FsResult<Path> {
    Path::from_str(path)
        .map_err(|e| FsError::common(format!("free reconcile: invalid path {}: {}", path, e)))
}

impl FsModeReleaser for FreeReconciler {
    fn is_fs_mode(&self, path: &str) -> bool {
        let Ok(parsed) = parse_path(path) else {
            return true;
        };
        let Ok(job_manager) = self.job_manager() else {
            return true;
        };
        match job_manager.mount_manager().get_mount_info(&parsed) {
            Ok(Some(info)) => info.is_fs_mode(),
            Ok(None) => false,
            Err(err) => {
                warn!("free reconcile: mount lookup for {} failed: {}", path, err);
                true
            }
        }
    }

    fn contains_fs_mode_mount(&self, path: &str) -> bool {
        let prefix = format!("{}/", path.trim_end_matches('/'));
        let Ok(job_manager) = self.job_manager() else {
            return true;
        };
        match job_manager.mount_manager().get_mount_table() {
            Ok(mounts) => mounts
                .iter()
                .any(|m| m.is_fs_mode() && m.cv_path.starts_with(&prefix)),
            Err(err) => {
                warn!("free reconcile: mount table lookup failed: {}", err);
                true
            }
        }
    }

    fn defer_release(&self, inode_id: i64) {
        FREE_RECONCILE_METRICS.deferred.inc();
        if self
            .queue
            .try_send(ReconcileTask::Release(inode_id))
            .is_err()
        {
            FREE_RECONCILE_METRICS.queue_dropped.inc();
            debug!(
                "free reconcile: queue full, inode {} retries next round",
                inode_id
            );
        }
    }

    fn defer_cleanup(&self, path: &str) -> FsResult<()> {
        self.queue
            .try_send(ReconcileTask::Cleanup(path.to_owned()))
            .map_err(|_| {
                FREE_RECONCILE_METRICS.queue_dropped.inc();
                FsError::common(format!(
                    "free reconcile: could not schedule UFS cleanup for {}",
                    path
                ))
            })
    }
}
