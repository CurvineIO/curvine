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

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_runtime::common::LocalTime;
use serde::Serialize;

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
}