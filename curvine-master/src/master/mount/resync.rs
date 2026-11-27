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

//! In-memory tracking of Web-triggered mount resync tasks.
//!
//! A resync reconciles the Curvine metadata of an `fs_mode` mount point with
//! its UFS. Tasks are keyed by the mount's Curvine path: at most one resync
//! per mount point may run at a time, while different mount points resync
//! independently. The table is process-local (not journaled); a master
//! restart simply forgets finished tasks.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, MutexGuard};

use curvine_config::ClientConf;
use curvine_core_error::err_box;
use curvine_error::{ErrorKind, FsError, FsResult};
use curvine_fs_api::{FileSystem, Path};
use curvine_model::{MountInfo, OpenFlags};
use curvine_runtime::common::LocalTime;
use curvine_unified_fs::UfsFileSystem;
use serde::Serialize;

use crate::master::fs::MasterFilesystem;

/// Counters collected while walking the UFS tree of a mount point.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ResyncStats {
    /// UFS entries examined (files and directories).
    pub scanned: usize,
    /// Files whose cached mtime already matches the UFS mtime.
    pub skip_same_mtime: usize,
    /// Files whose UFS mtime is 0 and therefore cannot be compared.
    pub skip_ufs_time_zero: usize,
    /// Files deleted and re-created (or created when missing) in Curvine.
    pub recreated: usize,
    /// Entries that could not be processed; the walk continues past them.
    pub failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResyncState {
    Running,
    Done,
    Failed,
}

/// One resync task, as exposed by `GET /api/resync`.
#[derive(Debug, Clone, Serialize)]
pub struct ResyncTask {
    pub cv_path: String,
    /// What started the task: `manual` (Web API) or `auto` (first mount).
    pub trigger: String,
    pub state: ResyncState,
    pub stats: ResyncStats,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    pub error: Option<String>,
}

/// Tracks resync tasks per mount point. All methods are safe to call from
/// concurrent request handlers and background workers.
#[derive(Default)]
pub struct ResyncManager {
    tasks: Mutex<HashMap<String, ResyncTask>>,
}

impl ResyncManager {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, ResyncTask>> {
        self.tasks.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Register a new running task for `cv_path`. Fails when a resync is
    /// already running for the same mount point; a finished task is replaced.
    pub fn try_start(&self, cv_path: &str, trigger: &str) -> FsResult<()> {
        let mut tasks = self.lock();
        if let Some(task) = tasks.get(cv_path) {
            if task.state == ResyncState::Running {
                return err_box!("resync already running for mount point {}", cv_path);
            }
        }
        tasks.insert(
            cv_path.to_string(),
            ResyncTask {
                cv_path: cv_path.to_string(),
                trigger: trigger.to_string(),
                state: ResyncState::Running,
                stats: ResyncStats::default(),
                started_at_ms: LocalTime::mills() as i64,
                finished_at_ms: None,
                error: None,
            },
        );
        Ok(())
    }

    /// Mark the task for `cv_path` finished. `error` switches the state to
    /// `Failed`; the partial `stats` collected before the failure are kept.
    pub fn finish(&self, cv_path: &str, stats: ResyncStats, error: Option<String>) {
        let mut tasks = self.lock();
        if let Some(task) = tasks.get_mut(cv_path) {
            task.state = if error.is_some() {
                ResyncState::Failed
            } else {
                ResyncState::Done
            };
            task.stats = stats;
            task.finished_at_ms = Some(LocalTime::mills() as i64);
            task.error = error;
        }
    }

    pub fn get(&self, cv_path: &str) -> Option<ResyncTask> {
        self.lock().get(cv_path).cloned()
    }

    /// All tasks, oldest first, for the status API.
    pub fn tasks(&self) -> Vec<ResyncTask> {
        let tasks = self.lock();
        let mut list: Vec<ResyncTask> = tasks.values().cloned().collect();
        list.sort_by(|a, b| {
            a.started_at_ms
                .cmp(&b.started_at_ms)
                .then_with(|| a.cv_path.cmp(&b.cv_path))
        });
        list
    }
}

/// Whether a Curvine listing error means the directory does not exist (yet).
fn is_cv_dir_missing(err: &FsError) -> bool {
    matches!(err.kind(), ErrorKind::FileNotFound | ErrorKind::Expired)
}

/// Reconcile the Curvine metadata of an `fs_mode` mount point with its UFS.
///
/// Server-side port of the CLI `cv mount resync` walk: a breadth-first scan
/// of the UFS tree creates missing Curvine metadata and re-creates files
/// whose cached UFS mtime drifted. UFS access uses the stored mount
/// properties, so credentials never travel in a request payload. `dry_run`
/// counts what would change without creating or deleting files. Per-entry
/// failures are counted and the walk continues; losing leadership aborts
/// the whole task.
pub async fn run_resync(
    fs: &MasterFilesystem,
    mount: &MountInfo,
    defaults: &ClientConf,
    dry_run: bool,
    stats: &mut ResyncStats,
) -> FsResult<()> {
    if !mount.is_fs_mode() {
        return err_box!(
            "resync is only allowed for fs_mode mount; mount point {} has write_type {:?}",
            mount.cv_path,
            mount.write_type
        );
    }

    let ufs_root = Path::from_str(&mount.ufs_path)?;
    let ufs = UfsFileSystem::new(&ufs_root, mount.properties.clone(), mount.provider)?;

    let mut queue = VecDeque::from([ufs_root.clone()]);
    while let Some(ufs_dir) = queue.pop_front() {
        if !fs.master_monitor.is_active() {
            return err_box!("resync aborted: this master is no longer the active leader");
        }

        let ufs_entries = match ufs.list_status(&ufs_dir).await {
            Ok(v) => v,
            Err(e) => {
                stats.failed += 1;
                log::warn!("[resync] failed to list ufs dir {}: {}", ufs_dir, e);
                continue;
            }
        };

        let cv_dir = mount.get_cv_path(&ufs_dir)?;
        if let Err(e) = fs.mkdir(cv_dir.full_path(), true) {
            stats.failed += 1;
            log::warn!("[resync] failed to create cv dir {}: {}", cv_dir, e);
            continue;
        }

        let cv_entries = match fs.list_status(cv_dir.full_path()) {
            Ok(v) => v,
            Err(e) if is_cv_dir_missing(&e) => vec![],
            Err(e) => {
                stats.failed += 1;
                log::warn!("[resync] failed to list cv dir {}: {}", cv_dir, e);
                continue;
            }
        };

        let mut cv_map = HashMap::new();
        for entry in cv_entries {
            cv_map.insert(entry.path.to_string(), entry);
        }

        for ufs_entry in ufs_entries {
            let ufs_path = Path::from_str(ufs_entry.path)?;
            if ufs_entry.is_dir {
                queue.push_back(ufs_path);
                continue;
            }

            stats.scanned += 1;
            let ufs_mtime = ufs_entry.mtime;
            let ufs_len = ufs_entry.len;
            let cv_path = mount.get_cv_path(&ufs_path)?;
            let cv_key = cv_path.full_path().to_string();

            if let Some(cv_status) = cv_map.get(&cv_key) {
                let cv_ufs_mtime = cv_status.storage_policy.ufs_mtime;
                if cv_ufs_mtime == 0 {
                    stats.skip_ufs_time_zero += 1;
                    continue;
                }
                if cv_ufs_mtime == ufs_mtime {
                    stats.skip_same_mtime += 1;
                    continue;
                }

                if dry_run {
                    stats.recreated += 1;
                    continue;
                }

                // SkipTrash, non-recursive: only the stale metadata entry is
                // replaced, matching the CLI delete_with_opts(path, false, true).
                if let Err(e) = fs.delete(cv_path.full_path(), false) {
                    stats.failed += 1;
                    log::warn!("[resync] failed to delete {}: {}", cv_path, e);
                    continue;
                }
            }

            if dry_run {
                stats.recreated += 1;
                continue;
            }

            let create_opts = mount.get_sync_opts(defaults, ufs_mtime, ufs_len);
            if let Err(e) =
                fs.create_with_opts(cv_path.full_path(), create_opts, OpenFlags::new_create())
            {
                stats.failed += 1;
                log::warn!("[resync] failed to create {}: {}", cv_path, e);
                continue;
            }
            stats.recreated += 1;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_start_dedups_running_task_per_mount() {
        let manager = ResyncManager::new();
        manager.try_start("/mnt/a", "manual").unwrap();

        let err = manager.try_start("/mnt/a", "auto").unwrap_err();
        assert!(err.to_string().contains("already running"));

        // The rejected start must not overwrite the running task.
        let task = manager.get("/mnt/a").unwrap();
        assert_eq!(task.state, ResyncState::Running);
        assert_eq!(task.trigger, "manual");
        assert!(task.finished_at_ms.is_none());
    }

    #[test]
    fn different_mounts_run_in_parallel() {
        let manager = ResyncManager::new();
        manager.try_start("/mnt/a", "manual").unwrap();
        manager.try_start("/mnt/b", "auto").unwrap();

        let tasks = manager.tasks();
        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().all(|t| t.state == ResyncState::Running));
    }

    #[test]
    fn finish_records_stats_and_allows_restart() {
        let manager = ResyncManager::new();
        manager.try_start("/mnt/a", "manual").unwrap();

        let stats = ResyncStats {
            scanned: 10,
            skip_same_mtime: 7,
            recreated: 3,
            ..Default::default()
        };
        manager.finish("/mnt/a", stats.clone(), None);

        let task = manager.get("/mnt/a").unwrap();
        assert_eq!(task.state, ResyncState::Done);
        assert_eq!(task.stats.scanned, 10);
        assert_eq!(task.stats.skip_same_mtime, 7);
        assert_eq!(task.stats.recreated, 3);
        assert!(task.finished_at_ms.is_some());
        assert!(task.error.is_none());

        // A finished task no longer blocks a new resync.
        manager.try_start("/mnt/a", "manual").unwrap();
        let task = manager.get("/mnt/a").unwrap();
        assert_eq!(task.state, ResyncState::Running);
        assert_eq!(task.stats.scanned, 0);
        assert!(task.finished_at_ms.is_none());
    }

    #[test]
    fn finish_with_error_keeps_partial_stats() {
        let manager = ResyncManager::new();
        manager.try_start("/mnt/a", "auto").unwrap();

        let stats = ResyncStats {
            scanned: 4,
            failed: 1,
            ..Default::default()
        };
        manager.finish("/mnt/a", stats, Some("ufs unavailable".to_string()));

        let task = manager.get("/mnt/a").unwrap();
        assert_eq!(task.state, ResyncState::Failed);
        assert_eq!(task.stats.scanned, 4);
        assert_eq!(task.stats.failed, 1);
        assert_eq!(task.error.as_deref(), Some("ufs unavailable"));
        assert!(task.finished_at_ms.is_some());
    }

    #[test]
    fn get_unknown_mount_returns_none() {
        let manager = ResyncManager::new();
        assert!(manager.get("/mnt/missing").is_none());
        assert!(manager.tasks().is_empty());
    }

    #[test]
    fn state_serializes_lowercase() {
        let manager = ResyncManager::new();
        manager.try_start("/mnt/a", "manual").unwrap();
        let task = manager.get("/mnt/a").unwrap();
        let value = serde_json::to_value(&task).unwrap();
        assert_eq!(value["state"], "running");
        assert_eq!(value["cv_path"], "/mnt/a");
        assert_eq!(value["trigger"], "manual");

        manager.finish("/mnt/a", ResyncStats::default(), None);
        let value = serde_json::to_value(manager.get("/mnt/a").unwrap()).unwrap();
        assert_eq!(value["state"], "done");

        manager.try_start("/mnt/a", "manual").unwrap();
        manager.finish("/mnt/a", ResyncStats::default(), Some("boom".into()));
        let value = serde_json::to_value(manager.get("/mnt/a").unwrap()).unwrap();
        assert_eq!(value["state"], "failed");
        assert_eq!(value["error"], "boom");
    }

    use crate::master::journal::JournalSystem;
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_model::{MountOptions, SetAttrOptsBuilder, WriteType};
    use curvine_runtime::common::Utils;

    /// An in-memory master filesystem with leadership set explicitly, plus
    /// the cluster conf whose client defaults feed `get_sync_opts`.
    fn test_fs(active: bool) -> (MasterFilesystem, ClusterConf) {
        Master::init_test_metrics();
        let test_name = Utils::rand_str(6);
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir = Utils::test_sub_dir(format!("resync-test/meta-{test_name}"));
        conf.journal.journal_dir = Utils::test_sub_dir(format!("resync-test/journal-{test_name}"));

        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        if active {
            fs.master_monitor.set_term_for_test(1);
            fs.master_monitor.set_applied_term_for_test(1);
        } else {
            fs.master_monitor
                .set_role_for_test(curvine_raft::raft::RoleState::Follower);
        }
        (fs, conf)
    }

    /// A local UFS directory pre-populated with `files` (relative paths).
    fn ufs_dir(files: &[(&str, &str)]) -> String {
        let dir = Utils::test_sub_dir(format!("resync-test/ufs-{}", Utils::rand_str(6)));
        for (name, content) in files {
            let path = std::path::Path::new(&dir).join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        dir
    }

    fn fs_mode_mount(cv: &str, ufs_dir: &str) -> MountInfo {
        MountOptions::builder()
            .write_type(WriteType::FsMode)
            .build()
            .to_info(1, cv, &format!("file://{ufs_dir}"))
    }

    async fn run(
        fs: &MasterFilesystem,
        conf: &ClusterConf,
        mount: &MountInfo,
        dry_run: bool,
    ) -> FsResult<ResyncStats> {
        let mut stats = ResyncStats::default();
        run_resync(fs, mount, &conf.client, dry_run, &mut stats).await?;
        Ok(stats)
    }

    #[tokio::test]
    async fn resync_rejects_cache_mode_mounts() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[("a.txt", "hello")]);
        let mount = MountOptions::builder()
            .write_type(WriteType::CacheMode)
            .build()
            .to_info(1, "/mnt-cache", &format!("file://{dir}"));

        let mut stats = ResyncStats::default();
        let err = run_resync(&fs, &mount, &conf.client, false, &mut stats)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("fs_mode"), "{err}");
        assert_eq!(stats.scanned, 0);
    }

    #[tokio::test]
    async fn resync_creates_missing_metadata() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[("a.txt", "hello"), ("b.txt", "world!")]);
        let mount = fs_mode_mount("/mnt-create", &dir);

        let stats = run(&fs, &conf, &mount, false).await.unwrap();
        assert_eq!(stats.scanned, 2);
        assert_eq!(stats.recreated, 2);
        assert_eq!(stats.failed, 0);

        let status = fs.file_status("/mnt-create/a.txt").unwrap();
        assert_eq!(status.len, 5);
        assert!(status.storage_policy.ufs_mtime > 0);
        assert!(fs.file_status("/mnt-create/b.txt").is_ok());
    }

    #[tokio::test]
    async fn resync_second_run_skips_same_mtime() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[("a.txt", "hello"), ("b.txt", "world!")]);
        let mount = fs_mode_mount("/mnt-skip", &dir);

        run(&fs, &conf, &mount, false).await.unwrap();
        let stats = run(&fs, &conf, &mount, false).await.unwrap();
        assert_eq!(stats.scanned, 2);
        assert_eq!(stats.skip_same_mtime, 2);
        assert_eq!(stats.recreated, 0);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn resync_recreates_on_mtime_drift() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[("a.txt", "hello")]);
        let mount = fs_mode_mount("/mnt-drift", &dir);

        run(&fs, &conf, &mount, false).await.unwrap();
        // Simulate a stale cached mtime that no longer matches the UFS.
        fs.set_attr(
            "/mnt-drift/a.txt",
            SetAttrOptsBuilder::new().ufs_mtime(1).build(),
        )
        .unwrap();

        let stats = run(&fs, &conf, &mount, false).await.unwrap();
        assert_eq!(stats.recreated, 1);
        assert_eq!(stats.skip_same_mtime, 0);
        assert_eq!(stats.failed, 0);
        // The re-created entry carries the UFS mtime again, not the stale one.
        let status = fs.file_status("/mnt-drift/a.txt").unwrap();
        assert_ne!(status.storage_policy.ufs_mtime, 1);
    }

    #[tokio::test]
    async fn resync_skips_zero_ufs_mtime() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[("a.txt", "hello")]);
        let mount = fs_mode_mount("/mnt-zero", &dir);

        // A bare create stores ufs_mtime = 0: nothing to compare against.
        fs.create("/mnt-zero/a.txt", true).unwrap();
        let stats = run(&fs, &conf, &mount, false).await.unwrap();
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.skip_ufs_time_zero, 1);
        assert_eq!(stats.recreated, 0);
        assert!(fs.file_status("/mnt-zero/a.txt").is_ok());
    }

    #[tokio::test]
    async fn resync_dry_run_counts_without_writing_files() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[("a.txt", "hello"), ("b.txt", "world!")]);
        let mount = fs_mode_mount("/mnt-dry", &dir);

        let stats = run(&fs, &conf, &mount, true).await.unwrap();
        assert_eq!(stats.scanned, 2);
        assert_eq!(stats.recreated, 2);
        assert!(fs.file_status("/mnt-dry/a.txt").is_err());
        assert!(fs.file_status("/mnt-dry/b.txt").is_err());
    }

    #[tokio::test]
    async fn resync_walks_nested_directories() {
        let (fs, conf) = test_fs(true);
        let dir = ufs_dir(&[
            ("top.txt", "t"),
            ("sub/nested.txt", "n"),
            ("sub/deep/deep.txt", "d"),
        ]);
        let mount = fs_mode_mount("/mnt-nest", &dir);

        let stats = run(&fs, &conf, &mount, false).await.unwrap();
        assert_eq!(stats.scanned, 3);
        assert_eq!(stats.recreated, 3);
        assert_eq!(stats.failed, 0);
        assert!(fs.file_status("/mnt-nest/top.txt").is_ok());
        assert!(fs.file_status("/mnt-nest/sub/nested.txt").is_ok());
        assert!(fs.file_status("/mnt-nest/sub/deep/deep.txt").is_ok());
    }

    #[tokio::test]
    async fn resync_aborts_on_standby() {
        let (fs, conf) = test_fs(false);
        let dir = ufs_dir(&[("a.txt", "hello")]);
        let mount = fs_mode_mount("/mnt-standby", &dir);

        let mut stats = ResyncStats::default();
        let err = run_resync(&fs, &mount, &conf.client, false, &mut stats)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no longer the active leader"),
            "{err}"
        );
        assert!(fs.file_status("/mnt-standby/a.txt").is_err());
    }
}
