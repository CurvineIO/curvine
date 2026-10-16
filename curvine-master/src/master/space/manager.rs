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

//! Dir-space manager: path resolution, scan orchestration and query API.
//!
//! The manager owns the immutable [`DirSpaceSnapshotStore`], enforces the
//! single-scan-permit invariant (at most one global or point round in flight,
//! Task 6 reuses the same permit for point rounds) and serves content-summary
//! queries against published generations.
//!
//! Invariants (design doc §9, §12):
//!
//! - Scans only run while [`MasterMonitor::is_active`] holds; a scan running
//!   when the role is lost aborts cooperatively inside the scanner and never
//!   publishes.
//! - Failed rounds never publish: the previous good generation stays
//!   queryable. The generation counter only advances on successful publish.
//! - Queries never fabricate zero values: a directory not covered by any
//!   published generation is a `SnapshotMiss`, mapped by Task 8 to the wire
//!   `SnapshotNotReady` error.
//! - Point selection resolves both the query path and the candidate point
//!   root inside one short `fs_dir` read lock; the store then compares by
//!   generation, never by wall clock.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use curvine_config::MasterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_fs_api::Path;
use curvine_runtime::common::LocalTime;
use log::{info, warn};

use crate::master::fs::MasterFilesystem;
use crate::master::space::scanner::{GlobalScanner, PointScanner, ScanContext, ScanLimits};
use crate::master::space::store::DirSpaceSnapshotStore;
use crate::master::space::types::{DirSpaceSnapshot, SnapshotMeta, SnapshotSource};
use crate::master::MountManager;
use std::collections::HashSet;

/// Outcome of one attempted global scan round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOutcome {
    /// A new generation was published.
    Success,
    /// Another scan already holds the permit.
    Busy,
    /// The feature is disabled by configuration.
    Disabled,
    /// This master is not the active master.
    RoleLost,
    /// The scan ran but failed; nothing was published.
    Failed,
    /// The scan hit its cooperative deadline.
    Timeout,
    /// The scan hit a resource limit.
    Limit,
}

impl ScanOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScanOutcome::Success => "success",
            ScanOutcome::Busy => "busy",
            ScanOutcome::Disabled => "disabled",
            ScanOutcome::RoleLost => "role_lost",
            ScanOutcome::Failed => "failed",
            ScanOutcome::Timeout => "timeout",
            ScanOutcome::Limit => "limit",
        }
    }
}

/// Errors surfaced by [`DirSpaceManager::query`]. Task 8 maps these onto the
/// public wire errors (`Unsupported`, `SnapshotNotReady`, `FileNotFound`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirSpaceQueryError {
    /// The dir-space feature is disabled.
    Disabled,
    /// No published generation contains the queried directory.
    SnapshotMiss(crate::master::space::store::SnapshotMiss),
    /// The queried path is invalid or does not resolve.
    Path(String),
}

/// Scan driver plus query facade over the published snapshot generations.
pub struct DirSpaceManager {
    fs: MasterFilesystem,
    /// Kept so callers can share the mount table. Mounts are not scanned
    /// unless their path is in the explicit tracked-path list.
    #[allow(dead_code)]
    mount_manager: Arc<MountManager>,
    conf: Arc<MasterConf>,
    store: DirSpaceSnapshotStore,
    pub(crate) scan_permit: AtomicBool,
    file_enabled: bool,
    enabled_override: Mutex<Option<bool>>,
    file_paths: Vec<String>,
    paths_override: Mutex<Option<Vec<String>>>,
    /// Bumped on every runtime override so the coordinator scans on its next tick.
    config_epoch: AtomicU64,
}

impl DirSpaceManager {
    /// `mount_manager` is consumed by Task 6 to auto-track fs-mode mounts; it
    /// is accepted here so the wiring is stable across the two tasks.
    pub fn new(
        fs: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        conf: Arc<MasterConf>,
    ) -> Self {
        Self {
            fs,
            mount_manager,
            file_enabled: conf.dir_space_enabled,
            enabled_override: Mutex::new(None),
            file_paths: conf.dir_space_tracked_paths_normalized.clone(),
            paths_override: Mutex::new(None),
            config_epoch: AtomicU64::new(0),
            conf,
            store: DirSpaceSnapshotStore::new(),
            scan_permit: AtomicBool::new(false),
        }
    }

    pub fn file_enabled(&self) -> bool {
        self.file_enabled
    }

    pub fn file_tracked_paths(&self) -> Vec<String> {
        self.file_paths.clone()
    }

    /// File value, or the runtime override when one is set.
    pub fn tracking_enabled(&self) -> bool {
        self.enabled_override
            .lock()
            .expect("dir-space enabled lock")
            .unwrap_or(self.file_enabled)
    }

    /// Generation of the runtime path/enabled overrides. Starts at 0.
    pub fn config_epoch(&self) -> u64 {
        self.config_epoch.load(Ordering::SeqCst)
    }

    fn bump_config_epoch(&self) {
        self.config_epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// Replace the enabled override. Returns the previous effective value.
    pub fn update_enabled(&self, enabled: bool) -> bool {
        let previous = self.tracking_enabled();
        *self
            .enabled_override
            .lock()
            .expect("dir-space enabled lock") = Some(enabled);
        self.bump_config_epoch();
        previous
    }

    /// Drop the enabled override and return the file-backed value.
    pub fn reset_enabled(&self) -> bool {
        *self
            .enabled_override
            .lock()
            .expect("dir-space enabled lock") = None;
        self.bump_config_epoch();
        self.file_enabled
    }

    /// Replace the explicit path list. `/` selects the global scan.
    /// Returns `(previous, applied)` normalized lists.
    pub fn update_tracked_paths(
        &self,
        raw_paths: &[String],
    ) -> CommonResult<(Vec<String>, Vec<String>)> {
        let normalized = MasterConf::normalize_tracked_path_list(
            raw_paths,
            self.conf.dir_space_max_tracked_points,
        )?;
        let previous = self.explicit_paths();
        *self
            .paths_override
            .lock()
            .expect("dir-space tracked paths lock") = Some(normalized.clone());
        self.bump_config_epoch();
        self.sync_tracked_point_catalog();
        Ok((previous, normalized))
    }

    /// Drop the path override and return the file-backed list.
    pub fn reset_tracked_paths(&self) -> Vec<String> {
        *self
            .paths_override
            .lock()
            .expect("dir-space tracked paths lock") = None;
        self.bump_config_epoch();
        self.sync_tracked_point_catalog();
        self.file_paths.clone()
    }

    /// Effective explicit list, including `/` when the global scan is selected.
    pub fn explicit_paths(&self) -> Vec<String> {
        self.paths_override
            .lock()
            .expect("dir-space tracked paths lock")
            .clone()
            .unwrap_or_else(|| self.file_paths.clone())
    }

    pub fn tracks_root(&self) -> bool {
        self.explicit_paths().iter().any(|path| path == "/")
    }

    pub fn store(&self) -> &DirSpaceSnapshotStore {
        &self.store
    }

    /// Configured global scan interval in milliseconds (coordinator cadence).
    pub fn interval_ms(&self) -> u64 {
        self.conf.dir_space_global_scan_interval_ms()
    }

    /// Configured tracked-point scan interval in milliseconds.
    pub fn point_interval_ms(&self) -> u64 {
        self.conf.dir_space_tracked_scan_interval_ms()
    }

    /// Refresh the global snapshot age metric from the published generation.
    pub fn refresh_global_age(&self) {
        if let Some(global) = self.store.global() {
            let age_ms = (now_unix_ms() as i64 - global.meta.finished_ms).max(0) as u64;
            if let Ok(metrics) = crate::master::Master::get_metrics() {
                metrics
                    .dir_space_global_age_seconds
                    .set((age_ms / 1000) as i64);
            }
        }
    }

    /// Refresh tracked-point aggregate gauges (no per-path labels).
    pub fn refresh_point_metrics(&self) {
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .dir_space_points_configured
                .set(self.effective_tracked_paths().len() as i64);
            let points = self.store.points();
            metrics.dir_space_points_ready.set(points.len() as i64);
            let total_entries: usize = points.values().map(|p| p.entries.len()).sum();
            metrics.dir_space_points_entries.set(total_entries as i64);
            let oldest_age_secs = points
                .values()
                .map(|p| (now_unix_ms() as i64 - p.meta.finished_ms).max(0) as u64 / 1000)
                .max();
            metrics
                .dir_space_points_oldest_age_seconds
                .set(oldest_age_secs.unwrap_or(0) as i64);
        }
    }

    fn global_limits(&self) -> ScanLimits {
        ScanLimits {
            max_inodes: self.conf.dir_space_max_inodes,
            max_edges: self.conf.dir_space_max_edges,
            max_dirs: self.conf.dir_space_max_dirs,
            max_depth: self.conf.dir_space_max_depth,
            memory_budget_bytes: self.conf.dir_space_memory_budget_bytes(),
            max_inode_record_bytes: self.conf.dir_space_max_inode_record_bytes(),
        }
    }

    fn point_limits(&self) -> ScanLimits {
        ScanLimits {
            max_inodes: self.conf.dir_space_track_max_inodes,
            max_edges: self.conf.dir_space_track_max_edges,
            max_dirs: self.conf.dir_space_max_dirs,
            max_depth: self.conf.dir_space_track_max_depth,
            memory_budget_bytes: self.conf.dir_space_track_memory_budget_bytes(),
            max_inode_record_bytes: self.conf.dir_space_max_inode_record_bytes(),
        }
    }

    /// Point-scan paths from the explicit list. `/` is omitted; it selects
    /// the global scan via [`Self::tracks_root`].
    pub fn effective_tracked_paths(&self) -> Vec<String> {
        self.explicit_paths()
            .into_iter()
            .filter(|path| path != "/")
            .collect()
    }

    /// Drop published snapshots that the current explicit list no longer covers.
    pub fn sync_tracked_point_catalog(&self) {
        if !self.tracks_root() && self.store.clear_global() {
            if let Ok(metrics) = crate::master::Master::get_metrics() {
                metrics.dir_space_global_ready.set(0);
            }
        }
        let effective: HashSet<String> = self.effective_tracked_paths().into_iter().collect();
        for path in self.store.points().keys().cloned().collect::<Vec<_>>() {
            if !effective.contains(&path) {
                self.store.remove_point(&path);
            }
        }
    }

    /// Resolve one tracked path to its directory inode id under a short read
    /// lock. Returns `None` when the path is missing or not a directory.
    fn resolve_tracked_root(&self, path: &str) -> Option<i64> {
        let normalized = normalize_tracked_path(path).ok()?;
        let fs_dir = self.fs.fs_dir.read();
        resolve_dir_id(&fs_dir, &normalized).ok()
    }

    /// Attempt one tracked-point refresh round starting at `start_index` in the
    /// effective path list. Returns the next round-robin cursor and whether the
    /// full list was completed within the round deadline.
    pub fn try_scan_tracked_points(&self, start_index: usize) -> (ScanOutcome, usize, bool) {
        if !self.tracking_enabled() {
            return (ScanOutcome::Disabled, start_index, false);
        }
        if !self.fs.master_monitor.is_active() {
            self.record_simple_outcome_for_scope("point", ScanOutcome::RoleLost);
            return (ScanOutcome::RoleLost, start_index, false);
        }
        if self
            .scan_permit
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.record_simple_outcome_for_scope("point", ScanOutcome::Busy);
            return (ScanOutcome::Busy, start_index, false);
        }
        let _permit = PermitGuard(&self.scan_permit);

        self.sync_tracked_point_catalog();
        let paths = self.effective_tracked_paths();
        if paths.is_empty() {
            return (ScanOutcome::Success, 0, true);
        }

        let timeout_ms = self.conf.dir_space_tracked_scan_timeout_ms();
        let monitor = self.fs.master_monitor.clone();
        let deadline = Instant::now() + std::time::Duration::from_millis(timeout_ms);
        let monitor_for_ctx = monitor.clone();
        let context = ScanContext {
            deadline,
            is_active: Box::new(move || monitor_for_ctx.is_active()),
            limits: self.point_limits(),
        };
        let mut index = start_index % paths.len();
        let mut scanned = 0usize;
        let mut any_success = false;

        loop {
            if Instant::now() >= deadline {
                break;
            }
            if !monitor.is_active() {
                self.record_simple_outcome_for_scope("point", ScanOutcome::RoleLost);
                return (ScanOutcome::RoleLost, index, false);
            }

            let path = paths[index].clone();
            let Some(root_id) = self.resolve_tracked_root(&path) else {
                warn!(
                    "dir-space tracked point {} skipped: path missing or not a directory",
                    path
                );
                index = (index + 1) % paths.len();
                scanned += 1;
                if scanned >= paths.len() {
                    return (ScanOutcome::Success, 0, true);
                }
                continue;
            };

            let started_ms = LocalTime::mills() as i64;
            match self.run_point_scan(&path, root_id, &context) {
                Ok(snapshot) => {
                    let duration_ms = (LocalTime::mills() as i64 - started_ms).max(0) as u64;
                    let inode_count = snapshot.meta.inode_count;
                    let edge_count = snapshot.meta.edge_count;
                    let anomaly_count = snapshot.meta.anomaly_count;
                    if !self
                        .explicit_paths()
                        .iter()
                        .any(|candidate| candidate == &path)
                    {
                        index = (index + 1) % paths.len();
                        scanned += 1;
                        if scanned >= paths.len() {
                            return (ScanOutcome::Success, 0, true);
                        }
                        continue;
                    }
                    let generation = self.store.publish_point(&path, snapshot).meta.generation;
                    info!(
                        "dir-space point scan published {} generation {} in {} ms",
                        path, generation, duration_ms
                    );
                    self.record_point_success(duration_ms, inode_count, edge_count, anomaly_count);
                    any_success = true;
                }
                Err(e) => {
                    let duration_ms = (LocalTime::mills() as i64 - started_ms).max(0) as u64;
                    let outcome = classify_scan_error(&e);
                    warn!(
                        "dir-space point scan {} {:?} after {} ms: {}",
                        path, outcome, duration_ms, e
                    );
                    self.record_point_failure(outcome, duration_ms, &path);
                }
            }

            index = (index + 1) % paths.len();
            scanned += 1;
            if scanned >= paths.len() {
                return (
                    if any_success {
                        ScanOutcome::Success
                    } else {
                        ScanOutcome::Failed
                    },
                    0,
                    true,
                );
            }
        }

        (
            if any_success {
                ScanOutcome::Success
            } else {
                ScanOutcome::Timeout
            },
            index,
            false,
        )
    }

    fn run_point_scan(
        &self,
        _path: &str,
        root_id: i64,
        context: &ScanContext,
    ) -> CommonResult<DirSpaceSnapshot> {
        let lease = {
            let fs_dir = self.fs.fs_dir.read();
            fs_dir.begin_snapshot_scan()?
        };
        let unpublished = PointScanner::scan(&lease, root_id, context)?;
        drop(lease);

        let mut entries = unpublished.entries;
        let root = unpublished.root;
        // `root` already includes this directory. Merging it into `empty_dir()`
        // would count the snapshot root twice.
        entries.insert(root_id, root);
        let stats = unpublished.stats;

        Ok(DirSpaceSnapshot {
            meta: SnapshotMeta {
                generation: 0,
                started_ms: unpublished.started_ms,
                finished_ms: unpublished.finished_ms,
                inode_count: unpublished.inode_count,
                edge_count: unpublished.edge_count,
                anomaly_count: stats.anomaly_count,
                degraded: stats.degraded,
            },
            root_id,
            source: SnapshotSource::TrackedPoint,
            entries,
        })
    }

    fn record_simple_outcome_for_scope(&self, scope: &str, outcome: ScanOutcome) {
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .dir_space_scan_total
                .with_label_values(&[scope, outcome.as_str()])
                .inc();
        }
    }

    fn record_point_success(
        &self,
        duration_ms: u64,
        inode_count: u64,
        edge_count: u64,
        anomaly_count: u64,
    ) {
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .dir_space_scan_total
                .with_label_values(&["point", "success"])
                .inc();
            metrics
                .dir_space_scan_duration_ms
                .with_label_values(&["point", "success"])
                .observe(duration_ms as f64);
            metrics
                .dir_space_scan_inodes
                .with_label_values(&["point"])
                .set(inode_count as i64);
            metrics
                .dir_space_scan_edges
                .with_label_values(&["point"])
                .set(edge_count as i64);
            metrics
                .dir_space_scan_anomalies
                .with_label_values(&["point"])
                .set(anomaly_count as i64);
        }
    }

    fn record_point_failure(&self, outcome: ScanOutcome, duration_ms: u64, _path: &str) {
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            let label = outcome.as_str();
            metrics
                .dir_space_scan_total
                .with_label_values(&["point", label])
                .inc();
            metrics
                .dir_space_scan_duration_ms
                .with_label_values(&["point", label])
                .observe(duration_ms as f64);
        }
    }

    /// Attempt one global scan round.
    ///
    /// A single `AtomicBool` permit serializes rounds; the guard releases it
    /// on every return path. Failures never publish, so the previous good
    /// generation survives.
    pub fn try_scan_global(&self) -> ScanOutcome {
        if !self.tracking_enabled() {
            return ScanOutcome::Disabled;
        }
        if !self.fs.master_monitor.is_active() {
            self.record_simple_outcome(ScanOutcome::RoleLost);
            return ScanOutcome::RoleLost;
        }
        if !self.tracks_root() {
            return ScanOutcome::Success;
        }
        if self
            .scan_permit
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            self.record_simple_outcome(ScanOutcome::Busy);
            return ScanOutcome::Busy;
        }
        let _permit = PermitGuard(&self.scan_permit);

        let timeout_ms = self.conf.dir_space_global_scan_timeout_ms();
        let monitor = self.fs.master_monitor.clone();
        let context = ScanContext {
            deadline: Instant::now() + std::time::Duration::from_millis(timeout_ms),
            is_active: Box::new(move || monitor.is_active()),
            limits: self.global_limits(),
        };

        let started_ms = LocalTime::mills() as i64;
        let result = self.run_scan(&context);
        let duration_ms = (LocalTime::mills() as i64 - started_ms).max(0) as u64;
        self.record_outcome(result, duration_ms)
    }

    fn run_scan(&self, context: &ScanContext) -> CommonResult<DirSpaceSnapshot> {
        // One short store clone; the lease (and its store Arc) is dropped
        // before returning so a pending restore can proceed.
        let lease = {
            let fs_dir = self.fs.fs_dir.read();
            fs_dir.begin_snapshot_scan()?
        };
        let unpublished = GlobalScanner::scan(&lease, context)?;
        drop(lease);

        let mut entries = unpublished.entries;
        let root_id = unpublished.root_id;
        let root = unpublished.root;
        // The scanner returns the root aggregate separately. Store it as-is:
        // it already counts this directory, so merging into `empty_dir()`
        // would add one extra directory.
        entries.insert(root_id, root);
        let stats = unpublished.stats;

        Ok(DirSpaceSnapshot {
            meta: SnapshotMeta {
                generation: 0, // assigned by the store on publish
                started_ms: unpublished.started_ms,
                finished_ms: unpublished.finished_ms,
                inode_count: unpublished.inode_count,
                edge_count: unpublished.edge_count,
                anomaly_count: stats.anomaly_count,
                degraded: stats.degraded,
            },
            root_id,
            source: SnapshotSource::Global,
            entries,
        })
    }

    /// Record a counter-only outcome (no scan ran) under the global scope.
    fn record_simple_outcome(&self, outcome: ScanOutcome) {
        if let Ok(metrics) = crate::master::Master::get_metrics() {
            metrics
                .dir_space_scan_total
                .with_label_values(&["global", outcome.as_str()])
                .inc();
        }
    }

    fn record_outcome(
        &self,
        result: CommonResult<DirSpaceSnapshot>,
        duration_ms: u64,
    ) -> ScanOutcome {
        let metrics = crate::master::Master::get_metrics();
        match result {
            Ok(snapshot) => {
                let inode_count = snapshot.meta.inode_count;
                let edge_count = snapshot.meta.edge_count;
                let anomaly_count = snapshot.meta.anomaly_count;
                if !self.tracks_root() {
                    return ScanOutcome::Success;
                }
                let generation = self.store.publish_global(snapshot).meta.generation;
                info!(
                    "dir-space global scan published generation {} in {} ms",
                    generation, duration_ms
                );
                if let Ok(metrics) = metrics {
                    metrics
                        .dir_space_scan_total
                        .with_label_values(&["global", "success"])
                        .inc();
                    metrics
                        .dir_space_scan_duration_ms
                        .with_label_values(&["global", "success"])
                        .observe(duration_ms as f64);
                    metrics
                        .dir_space_scan_inodes
                        .with_label_values(&["global"])
                        .set(inode_count as i64);
                    metrics
                        .dir_space_scan_edges
                        .with_label_values(&["global"])
                        .set(edge_count as i64);
                    metrics
                        .dir_space_scan_anomalies
                        .with_label_values(&["global"])
                        .set(anomaly_count as i64);
                    metrics.dir_space_global_ready.set(1);
                    metrics.dir_space_global_generation.set(generation as i64);
                    metrics.dir_space_global_entries.set(
                        self.store
                            .global()
                            .map(|g| g.entries.len() as i64)
                            .unwrap_or(0),
                    );
                }
                ScanOutcome::Success
            }
            Err(e) => {
                let outcome = classify_scan_error(&e);
                warn!(
                    "dir-space global scan {:?} after {} ms: {}",
                    outcome, duration_ms, e
                );
                if let Ok(metrics) = metrics {
                    let label = outcome.as_str();
                    metrics
                        .dir_space_scan_total
                        .with_label_values(&["global", label])
                        .inc();
                    metrics
                        .dir_space_scan_duration_ms
                        .with_label_values(&["global", label])
                        .observe(duration_ms as f64);
                }
                outcome
            }
        }
    }

    /// Resolve the longest tracked point that component-level-contains
    /// `path`, and validate it against the current namespace.
    ///
    /// Returns `Some((path, dir_id))` when the point root still resolves to
    /// the same inode id it was published with; a stale point (renamed or
    /// deleted root) is skipped. Locking contract: caller must NOT hold the
    /// `fs_dir` lock (this variant takes it itself).
    #[cfg(test)]
    pub fn select_point(
        &self,
        path: &str,
        points: &HashMap<String, Arc<DirSpaceSnapshot>>,
    ) -> Result<Option<(String, i64)>, DirSpaceQueryError> {
        if points.is_empty() {
            return Ok(None);
        }

        let fs_dir = self.fs.fs_dir.read();
        let mut candidates: Vec<&String> = points
            .keys()
            .filter(|p| Path::has_prefix(path, p.as_str()))
            .collect();
        candidates.sort_by_key(|p| std::cmp::Reverse(Path::get_components(p).len()));

        for candidate in candidates {
            let snapshot = &points[candidate.as_str()];
            match resolve_dir_id(&fs_dir, candidate) {
                Ok(id) if id == snapshot.root_id => {
                    return Ok(Some((candidate.clone(), id)));
                }
                Ok(_) | Err(_) => continue,
            }
        }
        Ok(None)
    }

    /// Serve a content-summary query for `path` from published generations.
    ///
    /// Files are not served here (Task 8 resolves them live); the caller is
    /// expected to have verified the path points at a directory.
    pub fn query(
        &self,
        path: &str,
    ) -> Result<crate::master::space::store::DirSpaceQueryResult, DirSpaceQueryError> {
        if !self.tracking_enabled() {
            return Err(DirSpaceQueryError::Disabled);
        }

        let normalized = normalize_dir_path(path)?;

        // One short fs_dir read lock covers both the query resolution and the
        // candidate point root validation.
        let fs_dir = self.fs.fs_dir.read();
        let dir_id = resolve_dir_id(&fs_dir, &normalized)
            .map_err(|_| DirSpaceQueryError::Path(path.to_string()))?;
        let points = self.store.points();
        let validated_point = self.select_point_locked(&fs_dir, &normalized, &points)?;
        drop(fs_dir);

        self.store
            .query(dir_id, validated_point.as_deref())
            .map_err(DirSpaceQueryError::SnapshotMiss)
    }

    fn select_point_locked(
        &self,
        fs_dir: &crate::master::meta::FsDir,
        path: &str,
        points: &HashMap<String, Arc<DirSpaceSnapshot>>,
    ) -> Result<Option<String>, DirSpaceQueryError> {
        if points.is_empty() {
            return Ok(None);
        }

        // Longest component-level prefix wins; `/a` never matches `/ab`.
        let mut candidates: Vec<&String> = points
            .keys()
            .filter(|p| Path::has_prefix(path, p.as_str()))
            .collect();
        candidates.sort_by_key(|p| std::cmp::Reverse(Path::get_components(p).len()));

        for candidate in candidates {
            let snapshot = &points[candidate.as_str()];
            match resolve_dir_id(fs_dir, candidate) {
                // Point root still resolves to the inode id it was published
                // with: the point generation is a valid candidate.
                Ok(id) if id == snapshot.root_id => return Ok(Some(candidate.clone())),
                // Root inode id changed (delete + recreate / rename): the old
                // generation must no longer be served.
                Ok(_) | Err(_) => continue,
            }
        }
        Ok(None)
    }
}

fn now_unix_ms() -> u64 {
    LocalTime::mills()
}

fn classify_scan_error(err: &impl std::fmt::Display) -> ScanOutcome {
    let message = err.to_string();
    if message.contains("timed out") {
        ScanOutcome::Timeout
    } else if message.contains("role lost") {
        ScanOutcome::RoleLost
    } else if message.contains("limit")
        || message.contains("budget")
        || message.contains("MaxInodes")
        || message.contains("MaxEdges")
        || message.contains("MaxDirs")
        || message.contains("MaxDepth")
    {
        ScanOutcome::Limit
    } else {
        ScanOutcome::Failed
    }
}

/// Releases the scan permit on every return path.
struct PermitGuard<'a>(&'a AtomicBool);

impl Drop for PermitGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Normalize a queried directory path to a component-level absolute form.
fn normalize_tracked_path(path: &str) -> CommonResult<String> {
    let path = path.trim();
    if !crate::master::meta::inode::InodeView::is_full_path(path) {
        return err_box!("tracked path must be absolute");
    }
    if path == "/" {
        return Ok("/".to_string());
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return err_box!("tracked path must not be empty");
    }
    Ok(trimmed.to_string())
}

fn normalize_dir_path(path: &str) -> Result<String, DirSpaceQueryError> {
    let path = path.trim();
    if !crate::master::meta::inode::InodeView::is_full_path(path) {
        return Err(DirSpaceQueryError::Path(path.to_string()));
    }
    if path == "/" {
        return Ok("/".to_string());
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(DirSpaceQueryError::Path(path.to_string()));
    }
    Ok(trimmed.to_string())
}

/// Resolve a normalized absolute directory path to its inode id under the
/// current in-memory tree. Caller must hold the `fs_dir` read lock.
fn resolve_dir_id(fs_dir: &crate::master::meta::FsDir, path: &str) -> CommonResult<i64> {
    let root = fs_dir.root_ptr();
    let inp = crate::master::meta::inode::InodePath::resolve(root, path, fs_dir.store_ref())?;
    let last = inp.get_last_inode();
    let last = match last {
        Some(last) => last,
        None => return err_box!("path {} does not exist", path),
    };
    let view = last.as_ref();
    if !view.is_dir() {
        return err_box!("path {} is not a directory", path);
    }
    Ok(view.id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use crate::master::space::store::SnapshotMiss;
    use crate::master::{Master, MasterMonitor, MountManager};
    use curvine_config::{ClusterConf, JournalConf, MasterConf};
    use curvine_model::RenameFlags;
    use curvine_raft::raft::RoleState;
    use curvine_runtime::common::Utils;
    use curvine_runtime::runtime::LoopTask;
    use std::sync::Arc;

    /// Real in-memory master filesystem backed by a temp RocksDB, with the
    /// dir-space feature enabled and a mutable role control for lifecycle
    /// tests. The monitor starts in Standby (state 0 = follower-equivalent
    /// for `is_active`).
    fn test_fs(name: &str, dir_space_enabled: bool) -> (MasterFilesystem, Arc<MasterConf>) {
        test_fs_with_options(name, dir_space_enabled, |_| {})
    }

    fn test_fs_with_options(
        name: &str,
        dir_space_enabled: bool,
        configure: impl FnOnce(&mut MasterConf),
    ) -> (MasterFilesystem, Arc<MasterConf>) {
        Master::init_test_metrics();
        let mut conf = ClusterConf {
            testing: true,
            master: MasterConf {
                meta_dir: Utils::test_sub_dir(format!("dir-space-manager/meta-{}", name)),
                ..Default::default()
            },
            journal: JournalConf {
                enable: false,
                journal_dir: Utils::test_sub_dir(format!("dir-space-manager/journal-{}", name)),
                ..Default::default()
            },
            ..Default::default()
        };
        conf.master.dir_space_enabled = dir_space_enabled;
        configure(&mut conf.master);
        if dir_space_enabled && conf.master.dir_space_tracked_paths.is_empty() {
            conf.master.dir_space_tracked_paths = vec!["/".to_string()];
        }
        conf.master.init().unwrap();
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        (fs, Arc::new(conf.master))
    }

    fn mount_manager(fs: &MasterFilesystem) -> Arc<MountManager> {
        Arc::new(MountManager::new(fs.clone()))
    }

    fn monitor_for(fs: &MasterFilesystem) -> MasterMonitor {
        fs.master_monitor.clone()
    }

    fn set_role(monitor: &MasterMonitor, leader: bool) {
        monitor.journal_ctl.set_state(if leader {
            RoleState::Leader
        } else {
            RoleState::Follower
        });
    }

    fn manager_for(fs: &MasterFilesystem, conf: &Arc<MasterConf>) -> Arc<DirSpaceManager> {
        Arc::new(DirSpaceManager::new(
            fs.clone(),
            mount_manager(fs),
            conf.clone(),
        ))
    }

    fn seed_tree(fs: &MasterFilesystem) {
        fs.mkdir("/a", true).unwrap();
        fs.mkdir("/ab", true).unwrap();
        fs.mkdir("/a/sub", true).unwrap();
    }

    // ------------------------------------------------------------------
    // Lifecycle: disabled / standby / role transitions
    // ------------------------------------------------------------------

    #[test]
    fn disabled_feature_never_scans_and_queries_are_unsupported() {
        let (fs, conf) = test_fs("disabled", false);
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        assert_eq!(manager.try_scan_global(), ScanOutcome::Disabled);
        assert!(manager.store().global().is_none());
        assert!(matches!(
            manager.query("/a"),
            Err(DirSpaceQueryError::Disabled)
        ));
    }

    #[test]
    fn standby_master_never_scans() {
        let (fs, conf) = test_fs("standby", true);
        // Role starts as Follower.
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        assert_eq!(manager.try_scan_global(), ScanOutcome::RoleLost);
        assert!(manager.store().global().is_none());
    }

    #[test]
    fn standby_to_active_scans_immediately_and_publishes() {
        let (fs, conf) = test_fs("promote", true);
        let monitor = monitor_for(&fs);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        // Coordinator: standby ticks do nothing, the standby→active transition
        // pulls the due time to now, and the tick following promotion scans.
        let task = crate::master::space::scheduler::DirSpaceCoordinatorTask::new(
            manager.clone(),
            monitor.clone(),
        );
        task.run().unwrap();
        assert!(manager.store().global().is_none(), "standby must not scan");

        set_role(&monitor, true);
        task.run().unwrap(); // transition tick: due = now
        task.run().unwrap(); // scan tick (still inside the transition window)

        let global = manager.store().global().expect("global published");
        assert_eq!(global.source, SnapshotSource::Global);
        assert_eq!(global.root_id, crate::master::meta::inode::ROOT_INODE_ID);
        assert!(
            global.entries.contains_key(&global.root_id),
            "root aggregate must be queryable"
        );

        // A published generation must serve queries for the seeded dirs.
        let res = manager.query("/a").expect("query must hit");
        assert_eq!(res.source, SnapshotSource::Global);
        // `/`, `/a`, `/ab`, `/a/sub`.
        assert_eq!(
            res.snapshot.entries[&res.snapshot.root_id].directory_count,
            4
        );
    }

    #[test]
    fn repeated_tick_while_scan_in_flight_is_busy_not_duplicate() {
        let (fs, conf) = test_fs("busy", true);
        let monitor = monitor_for(&fs);
        set_role(&monitor, true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        // Hold the permit manually to simulate an in-flight scan.
        manager
            .scan_permit
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .unwrap();
        assert_eq!(manager.try_scan_global(), ScanOutcome::Busy);

        // Release: the next attempt goes through.
        manager.scan_permit.store(false, Ordering::SeqCst);
        assert_eq!(manager.try_scan_global(), ScanOutcome::Success);
        assert_eq!(
            manager.try_scan_global(),
            ScanOutcome::Success,
            "permit must be released after each round"
        );
    }

    #[test]
    fn role_lost_mid_round_does_not_publish() {
        let (fs, conf) = test_fs("role-lost", true);
        let monitor = monitor_for(&fs);
        set_role(&monitor, true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        // Publish one good generation first.
        assert_eq!(manager.try_scan_global(), ScanOutcome::Success);
        let good = manager.store().global().unwrap();

        // Lose the role: the next round must abort without replacing it.
        set_role(&monitor, false);
        assert_eq!(manager.try_scan_global(), ScanOutcome::RoleLost);
        let still = manager.store().global().unwrap();
        assert!(Arc::ptr_eq(&good, &still), "failed round must not publish");
    }

    #[test]
    fn failed_round_keeps_previous_generation_queryable() {
        let (fs, conf) = test_fs("failed-round", true);
        let monitor = monitor_for(&fs);
        set_role(&monitor, true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);
        assert_eq!(manager.try_scan_global(), ScanOutcome::Success);
        let first = manager.store().global().unwrap();

        // Make the scan impossible: a pending restore rejects new leases, so
        // the round fails and the old generation stays.
        let guard = {
            let fs_dir = fs.fs_dir.read();
            fs_dir
                .store_ref()
                .begin_restore(std::time::Duration::from_secs(60))
                .unwrap()
        };
        assert_eq!(manager.try_scan_global(), ScanOutcome::Failed);
        drop(guard);

        let retained = manager.store().global().unwrap();
        assert!(Arc::ptr_eq(&first, &retained));
        assert!(manager.query("/a").is_ok());
    }

    // ------------------------------------------------------------------
    // Query selection: longest component-level point prefix
    // ------------------------------------------------------------------

    #[test]
    fn point_selection_component_boundaries_and_staleness() {
        let (fs, conf) = test_fs("points", true);
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        let id_a = {
            let fs_dir = fs.fs_dir.read();
            super::resolve_dir_id(&fs_dir, "/a").unwrap()
        };
        let id_ab = {
            let fs_dir = fs.fs_dir.read();
            super::resolve_dir_id(&fs_dir, "/ab").unwrap()
        };
        assert_ne!(id_a, id_ab);

        let mk_point = |root_id: i64| DirSpaceSnapshot {
            meta: SnapshotMeta {
                generation: 0,
                started_ms: 1,
                finished_ms: 2,
                inode_count: 1,
                edge_count: 0,
                anomaly_count: 0,
                degraded: false,
            },
            root_id,
            source: SnapshotSource::TrackedPoint,
            entries: HashMap::new(),
        };

        let mut points = HashMap::new();
        points.insert("/a".to_string(), Arc::new(mk_point(id_a)));
        // Component-level matching: "/a" is a prefix of "/a/sub", never "/ab".
        assert!(Path::has_prefix("/a/sub", "/a"));
        assert!(!Path::has_prefix("/ab", "/a"));

        // Valid point resolves.
        let selected = manager.select_point("/a/sub", &points).unwrap();
        assert_eq!(selected.as_ref().map(|(p, _)| p.as_str()), Some("/a"));

        // Stale point (root id changed): skipped.
        let mut stale = HashMap::new();
        stale.insert("/a".to_string(), Arc::new(mk_point(id_ab)));
        assert!(manager.select_point("/a/sub", &stale).unwrap().is_none());

        // Removed root: skipped.
        let mut gone = HashMap::new();
        gone.insert("/a".to_string(), Arc::new(mk_point(999_999)));
        assert!(manager.select_point("/a/sub", &gone).unwrap().is_none());

        // Longest prefix wins when nested points exist.
        let id_sub = {
            let fs_dir = fs.fs_dir.read();
            super::resolve_dir_id(&fs_dir, "/a/sub").unwrap()
        };
        let mut nested = points.clone();
        nested.insert("/a/sub".to_string(), Arc::new(mk_point(id_sub)));
        let selected = manager.select_point("/a/sub/x", &nested).unwrap();
        assert_eq!(selected.as_ref().map(|(p, _)| p.as_str()), Some("/a/sub"));
    }

    #[test]
    fn query_path_normalization_and_errors() {
        let (fs, conf) = test_fs("query-errors", true);
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        // Relative path is rejected as a path error.
        assert!(matches!(
            manager.query("relative/path"),
            Err(DirSpaceQueryError::Path(_))
        ));
        // Nonexistent path cannot resolve.
        assert!(matches!(
            manager.query("/does-not-exist"),
            Err(DirSpaceQueryError::Path(_))
        ));
        // Before any scan, existing dirs miss with NoSnapshot.
        assert!(matches!(
            manager.query("/a"),
            Err(DirSpaceQueryError::SnapshotMiss(SnapshotMiss::NoSnapshot))
        ));

        manager.try_scan_global();
        // Trailing slash normalizes to the same directory.
        let res = manager.query("/a/").expect("trailing slash must normalize");
        assert_eq!(res.source, SnapshotSource::Global);
    }

    // ------------------------------------------------------------------
    // Metrics (design doc §12)
    // ------------------------------------------------------------------

    #[test]
    fn dir_space_metrics_are_registered_and_labelled() {
        let (fs, conf) = test_fs("metrics", true);
        let monitor = monitor_for(&fs);
        set_role(&monitor, true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        // Registration happens with MasterMetrics::new (init_test_metrics);
        // all names below must be present exactly once in the registry.
        for name in [
            "dir_space_global_ready",
            "dir_space_global_generation",
            "dir_space_global_age_seconds",
            "dir_space_global_entries",
            "dir_space_points_configured",
            "dir_space_points_ready",
            "dir_space_points_oldest_age_seconds",
            "dir_space_points_entries",
            "dir_space_scan_duration_ms",
            "dir_space_scan_total",
            "dir_space_scan_inodes",
            "dir_space_scan_edges",
            "dir_space_scan_anomalies",
        ] {
            assert!(
                curvine_metrics::Metrics::get(name).is_some(),
                "metric {} missing from registry",
                name
            );
        }

        assert_eq!(manager.try_scan_global(), ScanOutcome::Success);
        manager.refresh_global_age();

        // The registry is process-global and parallel tests race text_output,
        // so assert through the same metric handles the manager wrote to.
        let metrics = Master::get_metrics().unwrap();
        let total = metrics
            .dir_space_scan_total
            .get_metric_with_label_values(&["global", "success"])
            .unwrap()
            .get() as u64;
        assert!(total >= 1, "success outcome must be counted, got {}", total);
        assert_eq!(metrics.dir_space_global_ready.get(), 1);
        assert!(metrics.dir_space_global_generation.get() >= 1);
        assert!(metrics.dir_space_global_entries.get() >= 1);

        // Busy outcome is labelled busy, not success.
        manager.scan_permit.store(true, Ordering::SeqCst);
        assert_eq!(manager.try_scan_global(), ScanOutcome::Busy);
        manager.scan_permit.store(false, Ordering::SeqCst);

        // Role lost maps to the role_lost label.
        set_role(&monitor, false);
        manager.try_scan_global();
        let role_lost = metrics
            .dir_space_scan_total
            .get_metric_with_label_values(&["global", "role_lost"])
            .unwrap()
            .get() as u64;
        assert!(
            role_lost >= 1,
            "role_lost outcome must be counted, got {}",
            role_lost
        );
    }

    #[test]
    fn tracked_point_effective_paths_and_publish() {
        let (fs, conf) = test_fs_with_options("tracked-publish", true, |master| {
            master.dir_space_tracked_paths = vec!["/a".to_string()];
        });
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        assert_eq!(manager.effective_tracked_paths(), vec!["/a".to_string()]);
        let (outcome, _, complete) = manager.try_scan_tracked_points(0);
        assert_eq!(outcome, ScanOutcome::Success);
        assert!(complete);
        let point = manager.store().point("/a").expect("point published");
        assert_eq!(point.source, SnapshotSource::TrackedPoint);
        assert!(point.entries.contains_key(&point.root_id));
        // `/a` and `/a/sub`; the point root must not be counted twice.
        assert_eq!(point.entries[&point.root_id].directory_count, 2);
        assert_eq!(point.entries[&point.root_id].file_count, 0);

        let res = manager.query("/a/sub").expect("point query");
        assert_eq!(res.source, SnapshotSource::TrackedPoint);
    }

    #[test]
    fn tracked_point_stale_root_not_selected_after_rename() {
        let (fs, conf) = test_fs_with_options("tracked-stale", true, |master| {
            master.dir_space_tracked_paths = vec!["/a".to_string()];
        });
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);
        manager.try_scan_tracked_points(0);

        fs.rename("/a", "/a-renamed", RenameFlags::empty()).unwrap();
        assert!(manager.query("/a-renamed/sub").is_err());
        assert!(manager
            .select_point("/a-renamed/sub", &manager.store().points())
            .unwrap()
            .is_none());
    }

    #[test]
    fn runtime_override_replaces_paths_and_disable_stops_scans() {
        let (fs, conf) = test_fs_with_options("runtime-paths", true, |master| {
            master.dir_space_tracked_paths = vec!["/a".to_string()];
        });
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);
        let registry = crate::master::build_master_runtime_registry(manager.clone());

        assert_eq!(manager.explicit_paths(), vec!["/a".to_string()]);
        let (outcome, _, complete) = manager.try_scan_tracked_points(0);
        assert_eq!(outcome, ScanOutcome::Success);
        assert!(complete);
        manager.query("/a").expect("point snapshot");

        registry
            .set("master.dir_space_tracked_paths", "[]", Some("test"))
            .unwrap();
        assert!(manager.explicit_paths().is_empty());
        assert!(manager.store().point("/a").is_none());
        assert!(matches!(
            manager.query("/a"),
            Err(DirSpaceQueryError::SnapshotMiss(_))
        ));

        registry
            .set("master.dir_space_tracked_paths", "[\"/a\"]", Some("test"))
            .unwrap();
        assert_eq!(manager.explicit_paths(), vec!["/a".to_string()]);
        manager.try_scan_tracked_points(0);
        manager.query("/a/sub").expect("republished point");

        registry
            .set("master.dir_space_enabled", "false", Some("test"))
            .unwrap();
        assert!(matches!(
            manager.query("/a"),
            Err(DirSpaceQueryError::Disabled)
        ));
        assert_eq!(manager.try_scan_global(), ScanOutcome::Disabled);
        assert_eq!(manager.try_scan_tracked_points(0).0, ScanOutcome::Disabled);

        registry
            .reset("master.dir_space_enabled", Some("test"))
            .unwrap();
        assert!(manager.tracking_enabled());
        registry
            .reset("master.dir_space_tracked_paths", Some("test"))
            .unwrap();
        assert_eq!(manager.explicit_paths(), vec!["/a".to_string()]);

        assert!(registry
            .set(
                "master.dir_space_tracked_paths",
                "[\"relative\"]",
                Some("test"),
            )
            .is_err());
        assert_eq!(manager.explicit_paths(), vec!["/a".to_string()]);
        assert!(manager.config_epoch() >= 1);
    }

    #[test]
    fn removing_root_drops_the_global_snapshot() {
        let (fs, conf) = test_fs("drop-root", true);
        set_role(&monitor_for(&fs), true);
        seed_tree(&fs);
        let manager = manager_for(&fs, &conf);

        assert!(manager.tracks_root());
        assert_eq!(manager.try_scan_global(), ScanOutcome::Success);
        assert!(manager.store().global().is_some());

        manager.update_tracked_paths(&[]).unwrap();
        assert!(!manager.tracks_root());
        assert!(manager.store().global().is_none());
        assert!(matches!(
            manager.query("/"),
            Err(DirSpaceQueryError::SnapshotMiss(_))
        ));
    }

    #[test]
    fn coordinator_terminate_follows_monitor_stop() {
        let (fs, conf) = test_fs("terminate", true);
        let monitor = monitor_for(&fs);
        let manager = manager_for(&fs, &conf);
        let task =
            crate::master::space::scheduler::DirSpaceCoordinatorTask::new(manager, monitor.clone());
        assert!(!task.terminate());
        monitor.journal_ctl.set_state(RoleState::Exit);
        assert!(task.terminate());
    }
}
