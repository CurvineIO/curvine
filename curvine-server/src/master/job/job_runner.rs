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
use crate::master::fs::policy::ChooseContext;
use crate::master::fs::MasterFilesystem;
use crate::master::job::job_store::DeferredPublishIntent;
use crate::master::{JobContext, JobStore, TaskDetail};
use curvine_common::conf::ClientConf;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    FileStatus, JobTaskState, LoadJobCommand, LoadJobResult, LoadTaskInfo, MountInfo,
    SetAttrOptsBuilder, SyncLifecycleMarker, SyncLifecycleOwner, SyncLifecycleState, WorkerAddress,
    WriteType,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use futures::future;
use log::{debug, error, info, warn};
use orpc::common::{ByteUnit, FastHashMap, FastHashSet, LocalTime};
use orpc::err_box;
use std::collections::LinkedList;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub struct LoadJobRunner {
    jobs: JobStore,
    master_fs: MasterFilesystem,
    factory: Arc<UfsFactory>,
    job_max_files: usize,
}

enum ActiveJobDecision {
    Reuse {
        active_job_id: String,
    },
    Replace {
        replace_job_id: String,
        assigned_workers: FastHashSet<WorkerAddress>,
    },
    Submit,
}

const DEFERRED_PUBLISH_PENDING_PREFIX: &str = "DeferredPublishQueued";

impl LoadJobRunner {
    pub fn new(
        jobs: JobStore,
        master_fs: MasterFilesystem,
        factory: Arc<UfsFactory>,
        job_max_files: usize,
    ) -> Self {
        Self {
            jobs,
            master_fs,
            factory,
            job_max_files,
        }
    }

    pub fn choose_worker(&self, block_size: i64) -> FsResult<WorkerAddress> {
        let ctx = ChooseContext::with_num(1, block_size, vec![]);
        let worker_mgr = self.master_fs.worker_manager.read();
        let workers = worker_mgr.choose_worker(ctx)?;
        if let Some(worker) = workers.first() {
            Ok(worker.clone())
        } else {
            err_box!("No available worker found")
        }
    }

    fn check_active_job(
        &self,
        canonical_job_id: &str,
        legacy_job_id: &str,
        command: &LoadJobCommand,
        source_status: &FileStatus,
        target_path: &Path,
        allow_deferred_placeholder_submit: bool,
    ) -> ActiveJobDecision {
        let resolved_job_id = if self.jobs.get(canonical_job_id).is_some() {
            canonical_job_id.to_string()
        } else if let Some(job_id) = self.jobs.resolve_job_id(legacy_job_id) {
            job_id
        } else {
            return ActiveJobDecision::Submit;
        };
        let job = if let Some(job) = self.jobs.get(&resolved_job_id) {
            job
        } else {
            return ActiveJobDecision::Submit;
        };

        let state: JobTaskState = job.state.state();
        if state == JobTaskState::Pending || state == JobTaskState::Loading {
            if Self::is_deferred_publish_placeholder(&job) {
                return if allow_deferred_placeholder_submit {
                    ActiveJobDecision::Submit
                } else {
                    ActiveJobDecision::Reuse {
                        active_job_id: resolved_job_id,
                    }
                };
            }

            if let (Some(requested), Some(active)) = (
                Self::source_generation_from_command(command),
                Self::source_generation_from_active_job(&job),
            ) {
                if requested == active {
                    return ActiveJobDecision::Reuse {
                        active_job_id: resolved_job_id,
                    };
                }

                info!(
                    "job {} source generation changed while active, allow replacement: old_gen={}, new_gen={}",
                    resolved_job_id, active, requested
                );
                return ActiveJobDecision::Replace {
                    replace_job_id: resolved_job_id,
                    assigned_workers: job.assigned_workers.clone(),
                };
            }

            if let (Some(requested), Some(active)) = (
                Self::source_snapshot_from_command(command),
                Self::source_snapshot_from_active_job(&job),
            ) {
                if requested == active {
                    return ActiveJobDecision::Reuse {
                        active_job_id: resolved_job_id,
                    };
                }

                info!(
                    "job {} source generation changed while active, allow replacement: old(id={},len={},mtime={}), new(id={},len={},mtime={})",
                    resolved_job_id, active.0, active.1, active.2, requested.0, requested.1, requested.2
                );
                return ActiveJobDecision::Replace {
                    replace_job_id: resolved_job_id,
                    assigned_workers: job.assigned_workers.clone(),
                };
            }

            return ActiveJobDecision::Reuse {
                active_job_id: resolved_job_id,
            };
        }

        let should_reuse = if !source_status.is_dir {
            // Files are generally auto-loaded and executed in parallel.
            // Validate ufs_mtime to prevent distributing a large number of duplicate tasks.
            if let Ok(cv_status) = self.master_fs.file_status(target_path.path()) {
                if cv_status.is_expired() || !cv_status.is_complete {
                    false
                } else {
                    source_status.len == cv_status.len
                        && cv_status.storage_policy.ufs_mtime != 0
                        && cv_status.storage_policy.ufs_mtime == source_status.mtime
                }
            } else {
                false
            }
        } else {
            true
        };

        if should_reuse {
            ActiveJobDecision::Reuse {
                active_job_id: resolved_job_id,
            }
        } else {
            ActiveJobDecision::Submit
        }
    }

    async fn cancel_replaced_active_job(
        &self,
        replace_job_id: &str,
        assigned_workers: FastHashSet<WorkerAddress>,
    ) {
        self.jobs.update_state(
            replace_job_id,
            JobTaskState::Canceled,
            "Canceling stale generation before replacement",
        );
        if let Some(job) = self.jobs.get(replace_job_id) {
            if let Err(e) = self
                .master_fs
                .fs_dir
                .write()
                .store_job_snapshot(job.to_snapshot(), true)
            {
                warn!(
                    "failed to persist replaced job snapshot {}: {}",
                    replace_job_id, e
                );
            }
        }

        if !assigned_workers.is_empty() {
            if let Err(e) = self.cancel_job(replace_job_id, assigned_workers).await {
                warn!(
                    "Failed to cancel stale generation for job {}: {}",
                    replace_job_id, e
                );
            }
        }
    }

    fn is_hydrate_job(source_path: &Path, target_path: &Path) -> bool {
        !source_path.is_cv() && target_path.is_cv()
    }

    fn is_async_publish_job(source_path: &Path, target_path: &Path, write_type: WriteType) -> bool {
        source_path.is_cv() && !target_path.is_cv() && matches!(write_type, WriteType::AsyncThrough)
    }

    fn source_snapshot_matches_command(
        command: &LoadJobCommand,
        source_status: &FileStatus,
    ) -> bool {
        if let (Some(id), Some(len), Some(mtime)) = (
            command.expected_source_id,
            command.expected_source_len,
            command.expected_source_mtime,
        ) {
            return id == source_status.id
                && len == source_status.len
                && mtime == source_status.mtime;
        }
        true
    }

    fn refresh_deferred_publish_snapshot_if_needed(
        command: &mut LoadJobCommand,
        source_status: &FileStatus,
        allow_deferred_placeholder_submit: bool,
        is_async_publish: bool,
    ) -> bool {
        if Self::source_snapshot_matches_command(command, source_status) {
            return false;
        }
        if allow_deferred_placeholder_submit && is_async_publish {
            command.expected_source_id = Some(source_status.id);
            command.expected_source_len = Some(source_status.len);
            command.expected_source_mtime = Some(source_status.mtime);
            command.source_generation = Some(Self::source_generation_from_status(source_status));
            return true;
        }
        false
    }

    fn deferred_publish_pending_message(replace_job_id: &str) -> String {
        format!("{}:{}", DEFERRED_PUBLISH_PENDING_PREFIX, replace_job_id)
    }

    fn is_expected_deferred_publish_failure(error: &FsError) -> bool {
        let msg = error.to_string();
        msg.contains("source snapshot changed before submit")
            || msg.contains("still committing by lease")
    }

    pub fn is_deferred_publish_placeholder(job: &JobContext) -> bool {
        let state: JobTaskState = job.state.state();
        state == JobTaskState::Pending
            && job.tasks.is_empty()
            && job
                .progress
                .message
                .starts_with(DEFERRED_PUBLISH_PENDING_PREFIX)
    }

    fn persist_job_snapshot_if_exists(&self, job_id: &str) {
        if let Some(job) = self.jobs.get(job_id) {
            if let Err(e) = self
                .master_fs
                .fs_dir
                .write()
                .store_job_snapshot(job.to_snapshot(), true)
            {
                warn!("failed to persist job snapshot {}: {}", job_id, e);
            }
        }
    }

    fn queue_async_publish_replacement(
        &self,
        job_id: &str,
        source_path: &Path,
        target_path: &Path,
        replace_job_id: &str,
        command: &LoadJobCommand,
        mnt: &MountInfo,
    ) -> FsResult<()> {
        let source_key = source_path.full_path().to_string();
        let replaced = self.jobs.queue_deferred_publish(
            source_key.clone(),
            DeferredPublishIntent {
                job_id: job_id.to_string(),
                command: command.clone(),
                mount: mnt.clone(),
            },
        );

        if let Some(old) = replaced {
            if old.job_id != job_id {
                self.jobs.update_state(
                    &old.job_id,
                    JobTaskState::Canceled,
                    "Deferred publish replaced by newer generation",
                );
                self.persist_job_snapshot_if_exists(&old.job_id);
            }
        }

        let state_message = Self::deferred_publish_pending_message(replace_job_id);
        if self.jobs.get(job_id).is_some() {
            self.jobs
                .update_state(job_id, JobTaskState::Pending, state_message.clone());
        } else {
            let mut queued = JobContext::with_conf(
                command,
                job_id.to_string(),
                source_path.clone_uri(),
                target_path.clone_uri(),
                mnt,
                &ClientConf::default(),
            );
            queued.update_state(JobTaskState::Pending, state_message.clone());
            self.jobs.insert_job(job_id.to_string(), queued);
        }
        self.persist_job_snapshot_if_exists(job_id);
        Ok(())
    }

    fn source_snapshot_from_command(command: &LoadJobCommand) -> Option<(i64, i64, i64)> {
        match (
            command.expected_source_id,
            command.expected_source_len,
            command.expected_source_mtime,
        ) {
            (Some(id), Some(len), Some(mtime)) => Some((id, len, mtime)),
            _ => None,
        }
    }

    fn source_generation_from_command(command: &LoadJobCommand) -> Option<&str> {
        command
            .source_generation
            .as_deref()
            .filter(|generation| !generation.is_empty())
    }

    fn source_snapshot_from_active_job(job: &JobContext) -> Option<(i64, i64, i64)> {
        match (
            job.info.expected_source_id,
            job.info.expected_source_len,
            job.info.expected_source_mtime,
        ) {
            (Some(id), Some(len), Some(mtime)) => Some((id, len, mtime)),
            _ => None,
        }
    }

    fn source_generation_from_active_job(job: &JobContext) -> Option<&str> {
        job.info
            .source_generation
            .as_deref()
            .filter(|generation| !generation.is_empty())
    }

    fn source_generation_from_status(status: &FileStatus) -> String {
        CommonUtils::source_generation(status.id, status.len, status.mtime)
    }

    fn resolve_source_generation(command: &LoadJobCommand, source_status: &FileStatus) -> String {
        if let Some(source_generation) = Self::source_generation_from_command(command) {
            source_generation.to_string()
        } else if let Some((id, len, mtime)) = Self::source_snapshot_from_command(command) {
            CommonUtils::source_generation(id, len, mtime)
        } else {
            Self::source_generation_from_status(source_status)
        }
    }

    fn task_generation_tag(job: &JobContext) -> String {
        if let Some(source_generation) = Self::source_generation_from_active_job(job) {
            source_generation.to_string()
        } else if let Some((id, len, mtime)) = Self::source_snapshot_from_active_job(job) {
            CommonUtils::source_generation(id, len, mtime)
        } else {
            job.info.create_time.to_string()
        }
    }

    fn should_track_publish_source(source_path: &Path, target_path: &Path) -> bool {
        source_path.is_cv() && !target_path.is_cv()
    }

    fn publish_source_marker(job_id: &str, generation: &str) -> SyncLifecycleMarker {
        SyncLifecycleMarker::with_owner(
            SyncLifecycleState::Pending,
            generation,
            job_id,
            SyncLifecycleOwner::Publish,
        )
    }

    fn set_source_marker_opts(
        marker: &SyncLifecycleMarker,
        expect: Option<&SyncLifecycleMarker>,
    ) -> curvine_common::state::SetAttrOpts {
        let mut builder = SetAttrOptsBuilder::new();
        let mut attrs = std::collections::HashMap::new();
        marker.apply_attrs(&mut attrs);
        for (k, v) in attrs {
            builder = builder.add_x_attr(k, v);
        }

        if let Some(expect_marker) = expect {
            for (k, v) in expect_marker.as_expect_map() {
                builder = builder.expect_x_attr(k, v);
            }
        }

        builder.build()
    }

    async fn mark_publish_source_pending(
        &self,
        source_path: &Path,
        marker: &SyncLifecycleMarker,
    ) -> FsResult<()> {
        let max_attempts = 8;
        for attempt in 0..max_attempts {
            let status = self.master_fs.file_status(source_path.path())?;
            let current = status.sync_marker();

            if let Some(current_marker) = &current {
                if current_marker.state == SyncLifecycleState::Committing
                    && current_marker.lease != marker.lease
                {
                    if attempt + 1 == max_attempts {
                        return err_box!(
                            "source {} still committing by lease {} while setting pending lease {}",
                            source_path,
                            current_marker.lease,
                            marker.lease
                        );
                    }
                    sleep(Duration::from_millis(20)).await;
                    continue;
                }

                if current_marker == marker {
                    return Ok(());
                }
            }

            let opts = Self::set_source_marker_opts(marker, current.as_ref());
            match self.master_fs.set_attr(source_path.path(), opts) {
                Ok(_) => return Ok(()),
                Err(e) if attempt + 1 < max_attempts => {
                    warn!(
                        "retry set pending marker for {} lease {} (attempt {}/{}): {}",
                        source_path,
                        marker.lease,
                        attempt + 1,
                        max_attempts,
                        e
                    );
                    sleep(Duration::from_millis(20)).await;
                }
                Err(e) => return Err(e),
            }
        }

        err_box!(
            "failed to set pending marker for source {} lease {}",
            source_path,
            marker.lease
        )
    }

    fn reject_hydrate_target(target_status: &FileStatus) -> bool {
        !target_status.is_complete
    }

    fn has_active_job_touching_path(&self, cv_path: &str) -> bool {
        self.jobs.has_active_job_touching_path(cv_path)
    }

    fn matches_expected_snapshot(
        target_status: Option<&FileStatus>,
        expected_target_mtime: Option<i64>,
        expected_target_missing: bool,
    ) -> bool {
        if expected_target_missing && target_status.is_some() {
            return false;
        }

        if let Some(expected_mtime) = expected_target_mtime {
            return matches!(target_status, Some(status) if status.mtime == expected_mtime);
        }

        true
    }

    fn validate_hydrate_target(
        &self,
        target_path: &Path,
        command: &LoadJobCommand,
    ) -> FsResult<()> {
        let target_uri = target_path.clone_uri();
        if self.has_active_job_touching_path(&target_uri) {
            return err_box!(
                "Reject hydrate load for {} because an active load job is already touching this path",
                target_uri
            );
        }

        let target_status = match self.master_fs.file_status(target_path.path()) {
            Ok(status) => Some(status),
            Err(FsError::FileNotFound(_)) => None,
            Err(e) => return Err(e),
        };

        if !Self::matches_expected_snapshot(
            target_status.as_ref(),
            command.expected_target_mtime,
            command.expected_target_missing.unwrap_or(false),
        ) {
            return err_box!(
                "Reject hydrate load for {} because target metadata no longer matches expected snapshot",
                target_uri
            );
        }

        if let Some(target_status) = target_status {
            if Self::reject_hydrate_target(&target_status) {
                return err_box!(
                    "Reject hydrate load for {} because target cv metadata is incomplete",
                    target_uri
                );
            }
        }

        Ok(())
    }

    pub async fn submit_load_task(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
    ) -> FsResult<LoadJobResult> {
        self.submit_load_task_inner(command, mnt, false).await
    }

    async fn submit_load_task_inner(
        &self,
        command: LoadJobCommand,
        mnt: MountInfo,
        allow_deferred_placeholder_submit: bool,
    ) -> FsResult<LoadJobResult> {
        let mut command = command;
        let source_path = Path::from_str(&command.source_path)?;

        let target_path = if let Some(ref target) = command.target_path {
            Path::from_str(target)?
        } else if source_path.is_cv() {
            mnt.get_ufs_path(&source_path)?
        } else {
            mnt.get_cv_path(&source_path)?
        };

        let source_status = if source_path.is_cv() {
            self.master_fs.file_status(source_path.path())?
        } else {
            let ufs = self.factory.get_ufs(&mnt)?;
            ufs.get_status(&source_path).await?
        };
        let is_async_publish =
            Self::is_async_publish_job(&source_path, &target_path, mnt.write_type);

        if !Self::source_snapshot_matches_command(&command, &source_status) {
            if Self::refresh_deferred_publish_snapshot_if_needed(
                &mut command,
                &source_status,
                allow_deferred_placeholder_submit,
                is_async_publish,
            ) {
                debug!(
                    "deferred publish snapshot refreshed for {}, id={}, len={}, mtime={}",
                    source_path.full_path(),
                    source_status.id,
                    source_status.len,
                    source_status.mtime
                );
            } else {
                return err_box!(
                    "source snapshot changed before submit for {}: expected(id={:?},len={:?},mtime={:?}) actual(id={},len={},mtime={})",
                    source_path.full_path(),
                    command.expected_source_id,
                    command.expected_source_len,
                    command.expected_source_mtime,
                    source_status.id,
                    source_status.len,
                    source_status.mtime
                );
            }
        }

        let source_generation = Self::resolve_source_generation(&command, &source_status);
        command.source_generation = Some(source_generation.clone());

        let legacy_job_id = CommonUtils::create_job_id(source_path.full_path());
        let job_id =
            CommonUtils::create_job_id_with_generation(source_path.full_path(), &source_generation);
        let result = LoadJobResult {
            job_id: job_id.clone(),
            target_path: target_path.clone_uri(),
        };

        match self.check_active_job(
            &job_id,
            &legacy_job_id,
            &command,
            &source_status,
            &target_path,
            allow_deferred_placeholder_submit,
        ) {
            ActiveJobDecision::Reuse { active_job_id } => {
                self.jobs
                    .add_alias(legacy_job_id.clone(), active_job_id.clone());
                if active_job_id != job_id {
                    self.jobs.add_alias(job_id.clone(), active_job_id.clone());
                }
                info!(
                    "job {}, source_path {} already exists (reuse active {})",
                    job_id,
                    source_path.full_path(),
                    active_job_id
                );
                return Ok(LoadJobResult {
                    job_id: active_job_id,
                    target_path: target_path.clone_uri(),
                });
            }
            ActiveJobDecision::Replace {
                replace_job_id,
                assigned_workers,
            } => {
                if Self::is_async_publish_job(&source_path, &target_path, mnt.write_type) {
                    self.jobs.add_alias(legacy_job_id.clone(), job_id.clone());
                    self.queue_async_publish_replacement(
                        &job_id,
                        &source_path,
                        &target_path,
                        &replace_job_id,
                        &command,
                        &mnt,
                    )?;
                    info!(
                        "job {} deferred behind active publish {}, source_path {}",
                        job_id,
                        replace_job_id,
                        source_path.full_path()
                    );
                    return Ok(result);
                }
                info!(
                    "job {} source generation changed, cancel active tasks of {} before replacement",
                    job_id, replace_job_id
                );
                self.cancel_replaced_active_job(&replace_job_id, assigned_workers)
                    .await;
            }
            ActiveJobDecision::Submit => {}
        }

        if Self::is_hydrate_job(&source_path, &target_path) {
            self.validate_hydrate_target(&target_path, &command)?;
        }

        self.jobs.clear_deferred_publish(source_path.full_path());
        info!("Submitting load job {}", job_id);
        let mut job_context = JobContext::with_conf(
            &command,
            job_id.clone(),
            source_path.clone_uri(),
            target_path.clone_uri(),
            &mnt,
            &ClientConf::default(),
        );

        let res = self
            .create_all_tasks(&mut job_context, source_status, &mnt)
            .await;

        match res {
            Err(e) => {
                warn!("Create load job {} failed: {}", job_id, e);
                Err(e)
            }

            Ok(size) => {
                info!(
                    "Submit load job {} success, tasks {}, total_size {}",
                    job_id,
                    job_context.tasks.len(),
                    ByteUnit::byte_to_string(size as u64)
                );

                if Self::should_track_publish_source(&source_path, &target_path) {
                    let marker = Self::publish_source_marker(&job_id, &source_generation);
                    self.mark_publish_source_pending(&source_path, &marker)
                        .await?;
                }

                let tasks = job_context.tasks.clone();
                self.master_fs
                    .fs_dir
                    .write()
                    .store_job_snapshot(job_context.to_snapshot(), true)?;
                self.jobs.insert_job(job_id.clone(), job_context);
                self.jobs.add_alias(legacy_job_id, job_id.clone());
                // @todo Whether to cancel some tasks that may have been dispatched.
                self.submit_all_task(tasks).await?;

                Ok(result)
            }
        }
    }

    pub async fn submit_deferred_publish_for_source(
        &self,
        source_path: impl AsRef<str>,
    ) -> FsResult<()> {
        let source_path = source_path.as_ref();
        let source_path_key = source_path.to_string();
        let Some(intent) = self.jobs.take_deferred_publish(source_path) else {
            return Ok(());
        };
        let deferred_job_id = intent.job_id.clone();
        if let Some(job) = self.jobs.get(&deferred_job_id) {
            if matches!(
                job.state.state(),
                JobTaskState::Completed | JobTaskState::Failed | JobTaskState::Canceled
            ) {
                return Ok(());
            }
        }

        match self
            .submit_load_task_inner(intent.command.clone(), intent.mount.clone(), true)
            .await
        {
            Ok(result) => {
                if result.job_id != deferred_job_id {
                    self.jobs
                        .add_alias(deferred_job_id.clone(), result.job_id.clone());
                    if let Some(job) = self.jobs.get(&deferred_job_id) {
                        if Self::is_deferred_publish_placeholder(&job) {
                            self.jobs.update_state(
                                &deferred_job_id,
                                JobTaskState::Canceled,
                                format!(
                                    "Deferred publish rebased to newer generation {}",
                                    result.job_id
                                ),
                            );
                            self.persist_job_snapshot_if_exists(&deferred_job_id);
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                if Self::is_expected_deferred_publish_failure(&e) {
                    let replaced = self
                        .jobs
                        .queue_deferred_publish(source_path_key.clone(), intent);
                    if let Some(existing_intent) = replaced {
                        if existing_intent.job_id != deferred_job_id {
                            let superseded_job_id = existing_intent.job_id.clone();
                            let _ = self
                                .jobs
                                .queue_deferred_publish(source_path_key, existing_intent);
                            self.jobs.update_state(
                                &deferred_job_id,
                                JobTaskState::Canceled,
                                format!(
                                    "Deferred publish superseded by newer generation {}",
                                    superseded_job_id
                                ),
                            );
                            self.persist_job_snapshot_if_exists(&deferred_job_id);
                            return Err(e);
                        }
                    }
                    self.jobs.update_state(
                        &deferred_job_id,
                        JobTaskState::Pending,
                        format!("Deferred publish submit deferred by expected race: {}", e),
                    );
                    self.persist_job_snapshot_if_exists(&deferred_job_id);
                    return Err(e);
                }
                self.jobs.update_state(
                    &deferred_job_id,
                    JobTaskState::Failed,
                    format!("Deferred publish submit failed: {}", e),
                );
                self.persist_job_snapshot_if_exists(&deferred_job_id);
                Err(e)
            }
        }
    }

    async fn submit_all_task(&self, tasks: FastHashMap<String, TaskDetail>) -> FsResult<()> {
        let submit_futures: Vec<_> = tasks
            .take()
            .into_iter()
            .map(|(id, task)| async move {
                let client = self.factory.get_worker_client(&task.task.worker).await?;
                client.submit_load_task(task.task).await?;
                info!("Submit sub-task {}", id);
                Ok::<(), FsError>(())
            })
            .collect();

        future::try_join_all(submit_futures).await?;
        Ok(())
    }

    async fn create_all_tasks(
        &self,
        job: &mut JobContext,
        source_status: FileStatus,
        mnt: &MountInfo,
    ) -> FsResult<i64> {
        job.update_state(JobTaskState::Pending, "Assigning workers");
        let block_size = job.info.block_size;

        let mut total_size = 0;
        let mut stack = LinkedList::new();
        let mut task_index = 0;
        let generation_tag = Self::task_generation_tag(job);
        stack.push_back(source_status);

        // Get target base path for direction detection
        let target_base = Path::from_str(&job.info.target_path)?;

        while let Some(status) = stack.pop_front() {
            if status.is_dir {
                // List directory based on path type
                let dir_path = Path::from_str(status.path)?;
                let childs = if dir_path.is_cv() {
                    // Traverse Curvine directory
                    self.master_fs.list_status(dir_path.path())?
                } else {
                    // Traverse UFS directory
                    let ufs = self.factory.get_ufs(mnt)?;
                    ufs.list_status(&dir_path).await?
                };

                for child in childs {
                    stack.push_back(child);
                }
            } else {
                let worker = self.choose_worker(block_size)?;

                let source_path = Path::from_str(status.path)?;

                // Calculate target_path based on source and target types
                let target_path = if source_path.is_cv() && !target_base.is_cv() {
                    // Export: Curvine → UFS
                    mnt.get_ufs_path(&source_path)?
                } else if !source_path.is_cv() && target_base.is_cv() {
                    // Import: UFS → Curvine
                    mnt.get_cv_path(&source_path)?
                } else {
                    // Same type (Curvine→Curvine or UFS→UFS), not supported yet
                    return err_box!(
                        "Unsupported path combination: source={}, target={}",
                        source_path.full_path(),
                        target_base.full_path()
                    );
                };

                let task_id = format!(
                    "{}_{}_task_{}",
                    job.info.job_id,
                    generation_tag.as_str(),
                    task_index
                );
                task_index += 1;
                total_size += status.len;

                let task = LoadTaskInfo {
                    job: job.info.clone(),
                    task_id: task_id.clone(),
                    worker: worker.clone(),
                    source_path: source_path.clone_uri(),
                    target_path: target_path.clone_uri(),
                    create_time: LocalTime::mills() as i64,
                };
                job.add_task(task.clone());

                if job.tasks.len() > self.job_max_files {
                    return err_box!(
                        "Job {} files exceeds {}",
                        job.info.job_id,
                        self.job_max_files
                    );
                }
                info!("Added sub-task {}", task_id);
            }
        }

        Ok(total_size)
    }

    pub async fn cancel_job(
        &self,
        job_id: impl AsRef<str>,
        assigned_workers: FastHashSet<WorkerAddress>,
    ) -> FsResult<()> {
        let job_id = job_id.as_ref();
        for worker in assigned_workers.iter() {
            let client = self.factory.get_worker_client(worker).await?;
            let res = client.cancel_job(job_id).await;

            if let Err(e) = res {
                error!(
                    "Failed to send cancel load request to worker{}: {}",
                    worker, e
                );
                self.jobs.update_state(
                    job_id,
                    JobTaskState::Canceled,
                    format!(
                        "Failed to send cancel load request to worker {}: {}",
                        worker, e
                    ),
                );
            }
        }

        Ok(())
    }
}
