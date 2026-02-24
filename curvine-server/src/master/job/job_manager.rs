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

use crate::common::UfsFactory;
use crate::master::fs::MasterFilesystem;
use crate::master::job::job_store::DeferredPublishIntent;
use crate::master::{JobContext, JobStore, LoadJobRunner, MountManager};
use core::time::Duration;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::executor::ScheduledExecutor;
use curvine_common::fs::Path;
use curvine_common::state::{
    JobStatus, JobTaskProgress, JobTaskState, LoadJobCommand, LoadJobResult,
    PersistedLoadJobSnapshot, WriteType,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::LocalTime;
use orpc::runtime::{LoopTask, RpcRuntime, Runtime};
use orpc::sync::channel::BlockingChannel;
use orpc::{err_box, err_ext};
use std::collections::HashMap;
use std::sync::Arc;

/// Load the Task Manager
pub struct JobManager {
    rt: Arc<Runtime>,
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    mount_manager: Arc<MountManager>,
    job_life_ttl: Duration,
    job_cleanup_ttl: Duration,
    job_max_files: usize,
}

impl JobManager {
    fn is_expected_deferred_publish_failure(e: &FsError) -> bool {
        let msg = e.to_string();
        msg.contains("source snapshot changed before submit")
            || msg.contains("still committing by lease")
    }

    pub fn from_cluster_conf(
        master_fs: MasterFilesystem,
        mount_manager: Arc<MountManager>,
        rt: Arc<Runtime>,
        conf: &ClusterConf,
    ) -> Self {
        let factory = Arc::new(UfsFactory::with_rt(&conf.client, rt.clone()));

        Self {
            rt,
            jobs: JobStore::new(),
            master_fs,
            factory,
            mount_manager,
            job_life_ttl: conf.job.job_life_ttl,
            job_cleanup_ttl: conf.job.job_cleanup_ttl,
            job_max_files: conf.job.job_max_files,
        }
    }

    /// Start the job manager
    pub fn start(&self) {
        self.restore_active_jobs_from_store();

        let cleanup_interval = self.job_cleanup_ttl.as_millis() as u64;
        let ttl_ms = self.job_life_ttl.as_millis() as i64;

        let executor = ScheduledExecutor::new("job_cleanup", cleanup_interval);
        executor
            .start(JobCleanupTask {
                jobs: self.jobs.clone(),
                master_fs: self.master_fs.clone(),
                ttl_ms,
            })
            .unwrap();

        info!("JobManager started");
    }

    fn update_state(&self, job_id: &str, state: JobTaskState, message: impl Into<String>) {
        self.jobs.update_state(job_id, state, message)
    }

    fn install_job_snapshot(&self, snapshot: PersistedLoadJobSnapshot) -> String {
        let canonical_job_id = snapshot.info.job_id.clone();
        if self.jobs.get(&canonical_job_id).is_none() {
            self.jobs.insert_job(
                canonical_job_id.clone(),
                JobContext::from_snapshot(snapshot),
            );
        }
        canonical_job_id
    }

    fn alias_state_rank(state: JobTaskState) -> u8 {
        match state {
            JobTaskState::Pending | JobTaskState::Loading => 3,
            JobTaskState::Completed => 2,
            JobTaskState::Failed => 1,
            JobTaskState::Canceled | JobTaskState::UNKNOWN => 0,
        }
    }

    fn prefer_alias_candidate(
        candidate_job_id: &str,
        candidate_create_time: i64,
        candidate_state: JobTaskState,
        current_job_id: &str,
        current_create_time: i64,
        current_state: JobTaskState,
    ) -> bool {
        let candidate_rank = Self::alias_state_rank(candidate_state);
        let current_rank = Self::alias_state_rank(current_state);
        if candidate_rank != current_rank {
            return candidate_rank > current_rank;
        }
        if candidate_create_time != current_create_time {
            return candidate_create_time > current_create_time;
        }
        candidate_job_id > current_job_id
    }

    fn rebuild_legacy_aliases(&self) {
        let mut preferred: HashMap<String, (String, i64, JobTaskState)> = HashMap::new();
        for entry in self.jobs.iter() {
            let job = entry.value();
            let legacy_job_id = CommonUtils::create_job_id(&job.info.source_path);
            let candidate = (
                job.info.job_id.clone(),
                job.info.create_time,
                job.state.state(),
            );
            match preferred.get(&legacy_job_id) {
                Some((current_job_id, current_create_time, current_state))
                    if !Self::prefer_alias_candidate(
                        &candidate.0,
                        candidate.1,
                        candidate.2,
                        current_job_id,
                        *current_create_time,
                        *current_state,
                    ) => {}
                _ => {
                    preferred.insert(legacy_job_id, candidate);
                }
            }
        }

        for (legacy_job_id, (canonical_job_id, _, _)) in preferred {
            self.jobs.add_alias(legacy_job_id, canonical_job_id);
        }
    }

    fn is_async_publish_job(job: &JobContext) -> bool {
        if job.info.mount_info.write_type != WriteType::AsyncThrough {
            return false;
        }
        let source = Path::from_str(&job.info.source_path);
        let target = Path::from_str(&job.info.target_path);
        match (source, target) {
            (Ok(source), Ok(target)) => source.is_cv() && !target.is_cv(),
            _ => false,
        }
    }

    fn deferred_publish_intent_from_job(job: &JobContext) -> Option<DeferredPublishIntent> {
        if !LoadJobRunner::is_deferred_publish_placeholder(job) || !Self::is_async_publish_job(job)
        {
            return None;
        }

        Some(DeferredPublishIntent {
            job_id: job.info.job_id.clone(),
            command: LoadJobCommand {
                source_path: job.info.source_path.clone(),
                target_path: Some(job.info.target_path.clone()),
                replicas: Some(job.info.replicas),
                block_size: Some(job.info.block_size),
                storage_type: Some(job.info.storage_type),
                ttl_ms: Some(job.info.ttl_ms),
                ttl_action: Some(job.info.ttl_action),
                overwrite: job.info.overwrite,
                source_generation: job.info.source_generation.clone(),
                expected_source_id: job.info.expected_source_id,
                expected_source_len: job.info.expected_source_len,
                expected_source_mtime: job.info.expected_source_mtime,
                expected_target_mtime: job.info.expected_target_mtime,
                expected_target_missing: job.info.expected_target_missing,
            },
            mount: job.info.mount_info.clone(),
        })
    }

    fn rebuild_deferred_publish_slots(&self) {
        for entry in self.jobs.iter() {
            let job = entry.value();
            if let Some(intent) = Self::deferred_publish_intent_from_job(job) {
                let _ = self
                    .jobs
                    .queue_deferred_publish(job.info.source_path.clone(), intent);
            }
        }
    }

    fn has_active_publish_job_for_source(&self, source_path: &str) -> bool {
        for entry in self.jobs.iter() {
            let job = entry.value();
            if job.info.source_path != source_path {
                continue;
            }
            if !Self::is_async_publish_job(job) {
                continue;
            }
            if LoadJobRunner::is_deferred_publish_placeholder(job) {
                continue;
            }
            if matches!(
                job.state.state(),
                JobTaskState::Pending | JobTaskState::Loading
            ) {
                return true;
            }
        }
        false
    }

    fn schedule_deferred_publish(&self, source_path: impl Into<String>) {
        let source_path = source_path.into();
        let job_runner = self.create_runner();
        self.rt.spawn(async move {
            if let Err(e) = job_runner
                .submit_deferred_publish_for_source(&source_path)
                .await
            {
                if Self::is_expected_deferred_publish_failure(&e) {
                    debug!(
                        "submit deferred publish skipped by expected race for source {}: {}",
                        source_path, e
                    );
                } else {
                    warn!(
                        "submit deferred publish failed for source {}: {}",
                        source_path, e
                    );
                }
            }
        });
    }

    fn recover_orphan_deferred_publish(&self) {
        let deferred_sources: Vec<String> = self
            .jobs
            .list_deferred_publish()
            .into_iter()
            .map(|(source, _)| source)
            .collect();
        for source_path in deferred_sources {
            if !self.has_active_publish_job_for_source(&source_path) {
                self.schedule_deferred_publish(source_path);
            }
        }
    }

    fn restore_active_jobs_from_store(&self) {
        let snapshots = match self.master_fs.fs_dir.read().get_job_snapshots() {
            Ok(v) => v,
            Err(e) => {
                warn!("failed to restore load jobs from store: {}", e);
                return;
            }
        };

        for snapshot in snapshots {
            let _ = self.install_job_snapshot(snapshot);
        }
        self.rebuild_legacy_aliases();
        self.rebuild_deferred_publish_slots();
        self.recover_orphan_deferred_publish();
    }

    fn recover_job_from_store(&self, job_id: &str) -> Option<String> {
        if let Ok(Some(snapshot)) = self.master_fs.fs_dir.read().get_job_snapshot(job_id) {
            let canonical_job_id = self.install_job_snapshot(snapshot);
            self.rebuild_legacy_aliases();
            self.rebuild_deferred_publish_slots();
            return Some(canonical_job_id);
        }

        let snapshots = self.master_fs.fs_dir.read().get_job_snapshots().ok()?;
        let mut matched = false;
        for snapshot in snapshots {
            let canonical = snapshot.info.job_id.clone();
            let legacy = CommonUtils::create_job_id(&snapshot.info.source_path);
            if canonical == job_id || legacy == job_id {
                matched = true;
            }
            let _ = self.install_job_snapshot(snapshot);
        }
        if !matched {
            return None;
        }

        self.rebuild_legacy_aliases();
        self.rebuild_deferred_publish_slots();
        self.jobs.resolve_job_id(job_id).or_else(|| {
            if self.jobs.get(job_id).is_some() {
                Some(job_id.to_string())
            } else {
                None
            }
        })
    }

    fn resolve_or_restore_job_id(&self, job_id: &str) -> Option<String> {
        if let Some(resolved) = self.jobs.resolve_job_id(job_id) {
            return Some(resolved);
        }

        let _ = self.recover_job_from_store(job_id);
        self.jobs.resolve_job_id(job_id).or_else(|| {
            if self.jobs.get(job_id).is_some() {
                Some(job_id.to_string())
            } else {
                None
            }
        })
    }

    fn persist_job_snapshot(&self, job_id: &str) -> FsResult<()> {
        let snapshot = if let Some(job) = self.jobs.get(job_id) {
            job.to_snapshot()
        } else {
            return Ok(());
        };
        let mut fs_dir = self.master_fs.fs_dir.write();
        fs_dir.store_job_snapshot(snapshot, true)?;
        Ok(())
    }

    fn persist_job_task_progress(&self, job_id: &str, task_id: &str) -> FsResult<()> {
        let (task_progress, job_state, job_progress) = if let Some(job) = self.jobs.get(job_id) {
            let task_progress = if let Some(task) = job.tasks.get(task_id) {
                task.progress.clone()
            } else {
                return err_box!(
                    "persist task progress failed: task {} not found in {}",
                    task_id,
                    job_id
                );
            };
            (task_progress, job.state.state(), job.progress.clone())
        } else {
            return Ok(());
        };

        let mut fs_dir = self.master_fs.fs_dir.write();
        fs_dir.update_job_task_progress(
            job_id,
            task_id,
            task_progress,
            job_state,
            job_progress,
            true,
        )?;
        Ok(())
    }

    pub fn get_job_status(&self, job_id: impl AsRef<str>) -> FsResult<JobStatus> {
        let job_id = job_id.as_ref();
        let resolved = self
            .resolve_or_restore_job_id(job_id)
            .unwrap_or(job_id.to_string());
        if let Some(job) = self.jobs.get(&resolved) {
            Ok(JobStatus {
                job_id: job.info.job_id.clone(),
                state: job.state.state(),
                source_path: job.info.source_path.clone(),
                target_path: job.info.target_path.clone(),
                progress: job.progress.clone(),
                source_generation: job.info.source_generation.clone(),
                expected_source_id: job.info.expected_source_id,
                expected_source_len: job.info.expected_source_len,
                expected_source_mtime: job.info.expected_source_mtime,
            })
        } else {
            err_ext!(FsError::job_not_found(job_id))
        }
    }

    pub fn create_runner(&self) -> LoadJobRunner {
        LoadJobRunner::new(
            self.jobs.clone(),
            self.master_fs.clone(),
            self.factory.clone(),
            self.job_max_files,
        )
    }

    pub fn submit_load_job(&self, command: LoadJobCommand) -> FsResult<LoadJobResult> {
        let source_path = Path::from_str(&command.source_path)?;

        // Check mount info for both UFS and CV paths
        // - For UFS path: Import (UFS → Curvine)
        // - For CV path: Export (Curvine → UFS), requires mount info to determine target UFS
        let mnt = if let Some(mnt) = self.mount_manager.get_mount_info(&source_path)? {
            mnt
        } else {
            return err_box!("Not found mount info for path: {}", source_path);
        };

        let job_runner = self.create_runner();

        let (tx, mut rx) = BlockingChannel::new(1).split();
        self.rt.spawn(async move {
            let res = job_runner.submit_load_task(command, mnt).await;
            if let Err(e) = tx.send(res) {
                warn!("send submit_load_job result: {}", e);
            }
        });

        rx.recv_check()?
    }

    /// Handle cancellation of tasks
    pub fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let resolved_job_id = if let Some(v) = self.resolve_or_restore_job_id(job_id) {
            v
        } else {
            return err_ext!(FsError::job_not_found(job_id));
        };
        let (assigned_workers, source_path) = {
            if let Some(job) = self.jobs.get(&resolved_job_id) {
                let state: JobTaskState = job.state.state();
                // Check whether it can be canceled
                if state == JobTaskState::Completed
                    || state == JobTaskState::Failed
                    || state == JobTaskState::Canceled
                {
                    info!(
                        "job {} is already in final state {:?}, source_path: {}, target_path: {}",
                        job_id, state, job.info.source_path, job.info.target_path
                    );
                    return Ok(());
                }

                (job.assigned_workers.clone(), job.info.source_path.clone())
            } else {
                return err_ext!(FsError::job_not_found(&resolved_job_id));
            }
        };

        self.update_state(
            &resolved_job_id,
            JobTaskState::Canceled,
            "Canceling job by user",
        );
        if let Err(e) = self.persist_job_snapshot(&resolved_job_id) {
            warn!(
                "failed to persist canceled job snapshot for {}: {}",
                resolved_job_id, e
            );
        }
        let cleared_deferred = self.jobs.clear_deferred_publish_by_job(&resolved_job_id);
        if cleared_deferred.is_none() {
            self.schedule_deferred_publish(source_path);
        }

        let job_runner = self.create_runner();
        let job_id = resolved_job_id.to_string();
        self.rt.spawn(async move {
            if let Err(e) = job_runner.cancel_job(&job_id, assigned_workers).await {
                warn!("Cancel job {} error: {}", job_id, e);
            }
        });

        Ok(())
    }

    pub fn update_progress(
        &self,
        job_id: impl AsRef<str>,
        task_id: impl AsRef<str>,
        progress: JobTaskProgress,
    ) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let task_id = task_id.as_ref();
        let resolved = self
            .resolve_or_restore_job_id(job_id)
            .unwrap_or(job_id.to_string());
        let updated = self.jobs.update_progress(&resolved, task_id, progress)?;
        let mut schedule_deferred = None;
        if updated.job_state_changed {
            if let Some(job) = self.jobs.get(&resolved) {
                if matches!(
                    job.state.state(),
                    JobTaskState::Completed | JobTaskState::Failed | JobTaskState::Canceled
                ) {
                    schedule_deferred = Some(job.info.source_path.clone());
                }
            }
        }
        if updated.task_state_changed || updated.job_state_changed {
            if let Err(delta_err) = self.persist_job_task_progress(&resolved, task_id) {
                warn!(
                    "failed to persist task-level progress for {}:{} ({}), fallback to full snapshot",
                    resolved, task_id, delta_err
                );
                if let Err(snapshot_err) = self.persist_job_snapshot(&resolved) {
                    warn!(
                        "failed to persist full job snapshot for {}: {}",
                        resolved, snapshot_err
                    );
                }
            }
        }
        if let Some(source_path) = schedule_deferred {
            self.schedule_deferred_publish(source_path);
        }
        Ok(())
    }

    pub fn jobs(&self) -> &JobStore {
        &self.jobs
    }

    pub fn factory(&self) -> &Arc<UfsFactory> {
        &self.factory
    }
}

struct JobCleanupTask {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    ttl_ms: i64,
}

impl LoopTask for JobCleanupTask {
    type Error = FsError;

    fn run(&self) -> Result<(), Self::Error> {
        // Collect tasks that need to be removed first
        let mut jobs_to_remove = vec![];
        let now = LocalTime::mills() as i64;
        for entry in self.jobs.iter() {
            let job = entry.value();
            if now > self.ttl_ms + job.info.create_time {
                jobs_to_remove.push(job.info.job_id.clone());
            }
        }

        for job_id in jobs_to_remove {
            if let Some(v) = self.jobs.remove_job(&job_id) {
                info!("Removing expired job: {:?}", v.1.info);
                if let Err(e) = self.master_fs.fs_dir.write().remove_job_snapshot(&job_id) {
                    warn!("failed to remove expired job snapshot {}: {}", job_id, e);
                }
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}
