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
use crate::worker::task::TaskContext;
use curvine_client::file::CurvineFileSystem;
use curvine_client::rpc::JobMasterClient;
use curvine_client::unified::{CacheSyncReader, UfsFileSystem, UnifiedReader, UnifiedWriter};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{
    CreateFileOptsBuilder, FileStatus, JobTaskState, SetAttrOptsBuilder, SyncLifecycleMarker,
    SyncLifecycleOwner, SyncLifecycleState, WriteType,
};
use curvine_common::utils::CommonUtils;
use curvine_common::FsResult;
use log::{debug, error, info, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::err_box;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const PUBLISH_STAGE_SUFFIX: &str = ".__cv_stage__.";

pub struct LoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceGuardState {
    Match,
    Missing,
    Changed,
}

#[derive(Debug, Clone)]
struct OwnedSyncMarker {
    path: Path,
    marker: SyncLifecycleMarker,
}

impl LoadTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        let master_client = JobMasterClient::new(fs.fs_client());
        Self {
            task,
            fs,
            factory,
            master_client,
            progress_interval_ms,
            task_timeout_ms,
        }
    }

    pub fn get_ufs(&self) -> FsResult<UfsFileSystem> {
        self.factory.get_ufs(&self.task.info.job.mount_info)
    }

    fn should_guard_sync_source(&self, source: &Path, target: &Path) -> bool {
        if source.is_cv() && !target.is_cv() {
            return matches!(
                self.task.info.job.mount_info.write_type,
                WriteType::AsyncThrough | WriteType::CacheThrough
            );
        }

        !source.is_cv() && target.is_cv()
    }

    fn should_stage_publish_target(&self, source: &Path, target: &Path) -> bool {
        source.is_cv()
            && !target.is_cv()
            && matches!(
                self.task.info.job.mount_info.write_type,
                WriteType::AsyncThrough | WriteType::CacheThrough
            )
    }

    fn stage_publish_target(&self, source: &Path, target: &Path) -> FsResult<Option<Path>> {
        if !self.should_stage_publish_target(source, target) {
            return Ok(None);
        }
        let staged = format!(
            "{}{}{}",
            target.full_path(),
            PUBLISH_STAGE_SUFFIX,
            self.task.info.task_id
        );
        Ok(Some(Path::from_str(staged)?))
    }

    fn tracked_sync_cv_path(&self, source: &Path, target: &Path) -> Option<Path> {
        if source.is_cv() && !target.is_cv() {
            Some(source.clone())
        } else if !source.is_cv() && target.is_cv() {
            Some(target.clone())
        } else {
            None
        }
    }

    fn source_generation_for_marker(&self, fallback: &FileStatus) -> String {
        if let Some(generation) = self
            .task
            .info
            .job
            .source_generation
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            generation.clone()
        } else if let (Some(id), Some(len), Some(mtime)) = (
            self.task.info.job.expected_source_id,
            self.task.info.job.expected_source_len,
            self.task.info.job.expected_source_mtime,
        ) {
            CommonUtils::source_generation(id, len, mtime)
        } else {
            CommonUtils::source_generation(fallback.id, fallback.len, fallback.mtime)
        }
    }

    fn owned_sync_marker(
        &self,
        source: &Path,
        target: &Path,
        fallback: &FileStatus,
    ) -> Option<OwnedSyncMarker> {
        let path = self.tracked_sync_cv_path(source, target)?;
        let owner = if source.is_cv() && !target.is_cv() {
            SyncLifecycleOwner::Publish
        } else if !source.is_cv() && target.is_cv() {
            SyncLifecycleOwner::Hydrate
        } else {
            SyncLifecycleOwner::Unknown
        };

        Some(OwnedSyncMarker {
            path,
            marker: SyncLifecycleMarker::with_owner(
                SyncLifecycleState::Pending,
                self.source_generation_for_marker(fallback),
                self.task.info.job.job_id.clone(),
                owner,
            ),
        })
    }

    fn set_marker_opts(
        marker: &SyncLifecycleMarker,
        expect: Option<&SyncLifecycleMarker>,
        ufs_mtime: Option<i64>,
    ) -> curvine_common::state::SetAttrOpts {
        let mut builder = SetAttrOptsBuilder::new();
        for (k, v) in marker.as_expect_map() {
            builder = builder.add_x_attr(k, v);
        }
        if let Some(expect_marker) = expect {
            for (k, v) in expect_marker.as_expect_map() {
                builder = builder.expect_x_attr(k, v);
            }
        }
        if let Some(mtime) = ufs_mtime {
            builder = builder.ufs_mtime(mtime);
        }
        builder.build()
    }

    fn transition_opts(
        marker: &SyncLifecycleMarker,
        from_state: SyncLifecycleState,
        to_state: SyncLifecycleState,
        ufs_mtime: Option<i64>,
    ) -> curvine_common::state::SetAttrOpts {
        let next = SyncLifecycleMarker::with_owner(
            to_state,
            &marker.generation,
            &marker.lease,
            marker.owner,
        );
        let expected = SyncLifecycleMarker::with_owner(
            from_state,
            &marker.generation,
            &marker.lease,
            marker.owner,
        );
        Self::set_marker_opts(&next, Some(&expected), ufs_mtime)
    }

    async fn transition_source_state(
        &self,
        path: &Path,
        marker: &SyncLifecycleMarker,
        from: &[SyncLifecycleState],
        to: SyncLifecycleState,
        ufs_mtime: Option<i64>,
    ) -> FsResult<bool> {
        for from_state in from {
            let opts = Self::transition_opts(marker, *from_state, to, ufs_mtime);
            match self.fs.set_attr(path, opts).await {
                Ok(_) => return Ok(true),
                Err(e) if Self::is_compare_and_set_failed_error(&e) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }

    fn is_compare_and_set_failed_error(err: &FsError) -> bool {
        err.to_string().contains("compare-and-set failed")
    }

    fn marker_fallback_status(&self) -> FileStatus {
        FileStatus {
            id: self.task.info.job.expected_source_id.unwrap_or_default(),
            len: self.task.info.job.expected_source_len.unwrap_or_default(),
            mtime: self.task.info.job.expected_source_mtime.unwrap_or_default(),
            ..Default::default()
        }
    }

    async fn abort_marker_on_task_failure(&self) -> FsResult<()> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        let fallback = self.marker_fallback_status();
        let Some(owner) = self.owned_sync_marker(&source_path, &target_path, &fallback) else {
            return Ok(());
        };

        let switched = self
            .transition_source_state(
                &owner.path,
                &owner.marker,
                &[SyncLifecycleState::Pending, SyncLifecycleState::Committing],
                SyncLifecycleState::Aborted,
                None,
            )
            .await?;
        if !switched {
            debug!(
                "task {} failure cleanup skipped marker transition on {} because ownership changed (lease={})",
                self.task.info.task_id,
                owner.path,
                owner.marker.lease
            );
        }
        Ok(())
    }

    async fn ensure_pending_owner(&self, owner: &OwnedSyncMarker) -> FsResult<bool> {
        let max_attempts = 8;
        for attempt in 0..max_attempts {
            let status = match self.fs.get_status(&owner.path).await {
                Ok(status) => status,
                Err(FsError::FileNotFound(_) | FsError::Expired(_)) => return Ok(false),
                Err(e) => return Err(e),
            };
            let current = status.sync_marker();

            if current.as_ref() == Some(&owner.marker) {
                return Ok(true);
            }

            if let Some(current_marker) = &current {
                if matches!(
                    current_marker.state,
                    SyncLifecycleState::Pending | SyncLifecycleState::Committing
                ) && current_marker.lease != owner.marker.lease
                {
                    return Ok(false);
                }
            }

            let opts = Self::set_marker_opts(&owner.marker, current.as_ref(), None);
            match self.fs.set_attr(&owner.path, opts).await {
                Ok(_) => return Ok(true),
                Err(e) if attempt + 1 < max_attempts => {
                    warn!(
                        "task {} retry set pending marker on {} lease {} (attempt {}/{}): {}",
                        self.task.info.task_id,
                        owner.path,
                        owner.marker.lease,
                        attempt + 1,
                        max_attempts,
                        e
                    );
                    sleep(Duration::from_millis(20)).await;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }

    async fn abort_transition_if_owner(&self, owner: Option<&OwnedSyncMarker>) -> FsResult<bool> {
        let Some(owner) = owner else {
            return Ok(true);
        };
        self.transition_source_state(
            &owner.path,
            &owner.marker,
            &[SyncLifecycleState::Pending, SyncLifecycleState::Committing],
            SyncLifecycleState::Aborted,
            None,
        )
        .await
    }

    async fn ensure_committing_owner(&self, owner: Option<&OwnedSyncMarker>) -> FsResult<bool> {
        let Some(owner) = owner else {
            return Ok(true);
        };
        self.transition_source_state(
            &owner.path,
            &owner.marker,
            &[SyncLifecycleState::Committing],
            SyncLifecycleState::Committing,
            None,
        )
        .await
    }

    async fn current_source_status(&self, source: &Path) -> FsResult<FileStatus> {
        if source.is_cv() {
            self.fs.get_status(source).await
        } else {
            let ufs = self.get_ufs()?;
            ufs.get_status(source).await
        }
    }

    async fn source_guard_state(
        &self,
        source: &Path,
        target: &Path,
        expected: &FileStatus,
    ) -> FsResult<SourceGuardState> {
        if !self.should_guard_sync_source(source, target) {
            return Ok(SourceGuardState::Match);
        }

        let current = match self.current_source_status(source).await {
            Ok(status) => status,
            Err(FsError::FileNotFound(_) | FsError::Expired(_)) => {
                return Ok(SourceGuardState::Missing);
            }
            Err(e) => return Err(e),
        };

        let same_id = if expected.id > 0 && current.id > 0 {
            current.id == expected.id
        } else {
            true
        };
        let same_shape = current.len == expected.len && current.mtime == expected.mtime;
        let state = if same_id && same_shape && current.is_complete() {
            SourceGuardState::Match
        } else {
            SourceGuardState::Changed
        };

        if state != SourceGuardState::Match {
            warn!(
                "task {} source generation changed before commit: source={}, expected(id={},len={},mtime={}), current(id={},len={},mtime={},complete={})",
                self.task.info.task_id,
                source,
                expected.id,
                expected.len,
                expected.mtime,
                current.id,
                current.len,
                current.mtime,
                current.is_complete
            );
        }

        Ok(state)
    }

    async fn cleanup_target_after_abort(&self, target: &Path) {
        let res = if target.is_cv() {
            self.fs.delete(target, false).await
        } else {
            match self.get_ufs() {
                Ok(ufs) => ufs.delete(target, false).await,
                Err(e) => {
                    warn!(
                        "task {} failed to get ufs while cleanup target {}: {}",
                        self.task.info.task_id, target, e
                    );
                    return;
                }
            }
        };

        if let Err(e) = res {
            if !matches!(e, FsError::FileNotFound(_)) {
                warn!(
                    "task {} cleanup target {} failed: {}",
                    self.task.info.task_id, target, e
                );
            }
        }
    }

    fn should_cleanup_abort_target(
        owns_canonical_target: bool,
        write_target: &Path,
        canonical_target: &Path,
    ) -> bool {
        write_target.full_path() != canonical_target.full_path() || owns_canonical_target
    }

    async fn cleanup_abort_target_guarded(
        &self,
        owns_canonical_target: bool,
        write_target: &Path,
        canonical_target: &Path,
        stage: &str,
    ) {
        if Self::should_cleanup_abort_target(owns_canonical_target, write_target, canonical_target)
        {
            self.cleanup_target_after_abort(write_target).await;
            return;
        }

        warn!(
            "task {} skip {} cleanup on canonical target {} because ownership was lost",
            self.task.info.task_id, stage, canonical_target
        );
    }

    async fn cleanup_staged_target_if_needed(&self, write_target: &Path, canonical_target: &Path) {
        if write_target.full_path() != canonical_target.full_path() {
            self.cleanup_abort_target_guarded(false, write_target, canonical_target, "staged")
                .await;
        }
    }

    fn stage_write_plan(
        configured_overwrite: bool,
        write_target: &Path,
        canonical_target: &Path,
    ) -> (bool, Option<Path>) {
        let staged = write_target.full_path() != canonical_target.full_path();
        let write_overwrite = staged || configured_overwrite;
        let conflict_check_target = if configured_overwrite {
            None
        } else {
            Some(canonical_target.clone())
        };
        (write_overwrite, conflict_check_target)
    }

    fn is_destination_conflict_error(err: &FsError) -> bool {
        if matches!(err, FsError::FileAlreadyExists(_)) {
            return true;
        }
        let msg = err.to_string().to_ascii_lowercase();
        msg.contains("already exists")
            || msg.contains("file exists")
            || msg.contains("destination exists")
    }

    fn should_retry_promote_with_delete(
        configured_overwrite: bool,
        destination_exists: bool,
        rename_err: &FsError,
    ) -> bool {
        configured_overwrite
            && destination_exists
            && Self::is_destination_conflict_error(rename_err)
    }

    fn is_missing_target_status_error(err: &FsError) -> bool {
        matches!(err, FsError::FileNotFound(_) | FsError::Expired(_))
    }

    async fn promote_staged_publish_target(
        &self,
        staged_target: &Path,
        canonical_target: &Path,
        configured_overwrite: bool,
    ) -> FsResult<()> {
        if staged_target.full_path() == canonical_target.full_path() {
            return Ok(());
        }

        let ufs = self.get_ufs()?;
        if !configured_overwrite && ufs.exists(canonical_target).await? {
            return err_box!(
                "File exists and overwrite=false, reject staged promote to {}",
                canonical_target.full_path()
            );
        }
        match ufs.rename(staged_target, canonical_target).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let first_err = e.to_string();
                let destination_exists = match ufs.exists(canonical_target).await {
                    Ok(exists) => exists,
                    Err(status_err) => {
                        warn!(
                            "task {} failed to check staged promote destination {} after rename error: {}",
                            self.task.info.task_id, canonical_target, status_err
                        );
                        false
                    }
                };
                let staged_exists = match ufs.exists(staged_target).await {
                    Ok(exists) => exists,
                    Err(status_err) => {
                        warn!(
                            "task {} failed to check staged source {} after promote error: {}",
                            self.task.info.task_id, staged_target, status_err
                        );
                        true
                    }
                };

                if configured_overwrite && destination_exists && !staged_exists {
                    warn!(
                        "task {} promote staged target {} -> {} returned ambiguous error but destination exists and staged source is gone, treat as promoted: {}",
                        self.task.info.task_id,
                        staged_target,
                        canonical_target,
                        first_err
                    );
                    return Ok(());
                }

                if Self::should_retry_promote_with_delete(
                    configured_overwrite,
                    destination_exists,
                    &e,
                ) {
                    match ufs.delete(canonical_target, false).await {
                        Ok(()) => {}
                        Err(delete_err) if Self::is_missing_target_status_error(&delete_err) => {}
                        Err(delete_err) => {
                            return Err(FsError::common(format!(
                                "Failed to promote staged target {} -> {}: rename failed={}, and failed to remove existing destination: {}",
                                staged_target, canonical_target, first_err, delete_err
                            )));
                        }
                    }
                }
                warn!(
                    "task {} promote staged target {} -> {} failed, retrying once (overwrite={}, destination_exists={}, staged_exists={}, conflict_delete={}): {}",
                    self.task.info.task_id,
                    staged_target,
                    canonical_target,
                    configured_overwrite,
                    destination_exists,
                    staged_exists,
                    Self::should_retry_promote_with_delete(
                        configured_overwrite,
                        destination_exists,
                        &e
                    ),
                    first_err
                );
                sleep(Duration::from_millis(30)).await;
                ufs.rename(staged_target, canonical_target)
                    .await
                    .map(|_| ())
                    .map_err(|retry_err| {
                        FsError::common(format!(
                            "Failed to promote staged target {} -> {}: first={}, retry={}",
                            staged_target, canonical_target, first_err, retry_err
                        ))
                    })
            }
        }
    }

    async fn abort_streams(&self, reader: &mut UnifiedReader, writer: &mut UnifiedWriter) {
        if let Err(e) = writer.cancel().await {
            warn!(
                "task {} failed to cancel writer {}: {}",
                self.task.info.task_id,
                writer.path(),
                e
            );
        }

        if let Err(e) = reader.complete().await {
            warn!(
                "task {} failed to close reader {} on abort: {}",
                self.task.info.task_id,
                reader.path(),
                e
            );
        }
    }

    pub async fn run(&self) {
        if let Err(e) = self.run0().await {
            if let Err(transition_err) = self.abort_marker_on_task_failure().await {
                warn!(
                    "task {} failed to transition sync marker to aborted after error: {}, original_error={}",
                    self.task.info.task_id, transition_err, e
                );
            }
            if self.task.is_cancel() {
                info!(
                    "task {} canceled, skip failure report: {}",
                    self.task.info.task_id, e
                );
                return;
            }

            // The data replication process fails, set the status and report to the master
            error!("task {} execute failed: {}", self.task.info.task_id, e);
            let progress = self.task.set_failed(e.to_string());
            let res = self
                .master_client
                .report_task(
                    self.task.info.job.job_id.clone(),
                    self.task.info.task_id.clone(),
                    progress,
                )
                .await;

            if let Err(e) = res {
                warn!("report task {}", e)
            }
        }
    }

    async fn run0(&self) -> FsResult<()> {
        self.task
            .update_state(JobTaskState::Loading, "Task started");

        let (mut reader, mut writer) = self.create_stream().await?;
        let source_path = reader.path().clone();
        let canonical_target_path = Path::from_str(&self.task.info.target_path)?;
        let write_target_path = writer.path().clone();
        let source_snapshot = reader.status().clone();
        let owner_marker =
            self.owned_sync_marker(&source_path, &canonical_target_path, &source_snapshot);
        if let Some(owner) = owner_marker.as_ref() {
            if !self.ensure_pending_owner(owner).await? {
                self.cleanup_staged_target_if_needed(&write_target_path, &canonical_target_path)
                    .await;
                self.abort_streams(&mut reader, &mut writer).await;
                return err_box!(
                    "Task {} abort before copy: sync marker on {} is owned by another lease",
                    self.task.info.task_id,
                    owner.path
                );
            }
        }

        let mut last_progress_time = LocalTime::mills();
        let mut read_cost_ms = 0;
        let mut total_cost_ms = 0;

        loop {
            if self.task.is_cancel() {
                info!("task {} was cancelled", self.task.info.task_id);
                break;
            }

            let spend = TimeSpent::new();
            let chunk = reader.async_read(None).await?;
            read_cost_ms += spend.used_ms();

            if chunk.is_empty() {
                break;
            }

            writer.async_write(chunk).await?;
            total_cost_ms += spend.used_ms();

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(writer.pos(), reader.len()).await;
            }

            if total_cost_ms > self.task_timeout_ms {
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
        }

        if self.task.is_cancel() {
            match self
                .source_guard_state(&source_path, &canonical_target_path, &source_snapshot)
                .await?
            {
                SourceGuardState::Match | SourceGuardState::Missing => {
                    let owns_canonical = self
                        .abort_transition_if_owner(owner_marker.as_ref())
                        .await?;
                    if !owns_canonical
                        && write_target_path.full_path() == canonical_target_path.full_path()
                    {
                        warn!(
                            "task {} canceled but lease is no longer owner, skip cleanup for target {}",
                            self.task.info.task_id, write_target_path
                        );
                    }
                    self.cleanup_abort_target_guarded(
                        owns_canonical,
                        &write_target_path,
                        &canonical_target_path,
                        "cancel-before-commit",
                    )
                    .await;
                }
                SourceGuardState::Changed => {
                    let owns_canonical = self
                        .abort_transition_if_owner(owner_marker.as_ref())
                        .await?;
                    if !owns_canonical
                        && write_target_path.full_path() == canonical_target_path.full_path()
                    {
                        warn!(
                            "task {} canceled with source generation changed but lease is no longer owner, skip cleanup for canonical target {}",
                            self.task.info.task_id, write_target_path
                        );
                    }
                    self.cleanup_abort_target_guarded(
                        owns_canonical,
                        &write_target_path,
                        &canonical_target_path,
                        "cancel-source-changed",
                    )
                    .await;
                }
            }
            self.abort_streams(&mut reader, &mut writer).await;
            return err_box!("Task {} canceled before commit", self.task.info.task_id);
        }

        // Guard write-through replay: if source generation changed or vanished, never commit stale data.
        match self
            .source_guard_state(&source_path, &canonical_target_path, &source_snapshot)
            .await?
        {
            SourceGuardState::Match => {}
            SourceGuardState::Missing => {
                let owns_canonical = self
                    .abort_transition_if_owner(owner_marker.as_ref())
                    .await?;
                self.cleanup_abort_target_guarded(
                    owns_canonical,
                    &write_target_path,
                    &canonical_target_path,
                    "guard-missing",
                )
                .await;
                self.abort_streams(&mut reader, &mut writer).await;
                return err_box!(
                    "Task {} abort commit: source {} missing",
                    self.task.info.task_id,
                    source_path
                );
            }
            SourceGuardState::Changed => {
                let owns_canonical = self
                    .abort_transition_if_owner(owner_marker.as_ref())
                    .await?;
                if !owns_canonical
                    && write_target_path.full_path() == canonical_target_path.full_path()
                {
                    warn!(
                        "task {} source changed before commit but lease is no longer owner, skip cleanup for canonical target {}",
                        self.task.info.task_id, write_target_path
                    );
                }
                self.cleanup_abort_target_guarded(
                    owns_canonical,
                    &write_target_path,
                    &canonical_target_path,
                    "guard-changed",
                )
                .await;
                self.abort_streams(&mut reader, &mut writer).await;
                return err_box!(
                    "Task {} abort commit: source {} changed",
                    self.task.info.task_id,
                    source_path
                );
            }
        }

        if self.task.is_cancel() {
            match self
                .source_guard_state(&source_path, &canonical_target_path, &source_snapshot)
                .await?
            {
                SourceGuardState::Match | SourceGuardState::Missing => {
                    let owns_canonical = self
                        .abort_transition_if_owner(owner_marker.as_ref())
                        .await?;
                    if !owns_canonical
                        && write_target_path.full_path() == canonical_target_path.full_path()
                    {
                        warn!(
                            "task {} canceled after guard check but lease is no longer owner, skip cleanup for target {}",
                            self.task.info.task_id, write_target_path
                        );
                    }
                    self.cleanup_abort_target_guarded(
                        owns_canonical,
                        &write_target_path,
                        &canonical_target_path,
                        "cancel-after-guard",
                    )
                    .await;
                }
                SourceGuardState::Changed => {
                    let owns_canonical = self
                        .abort_transition_if_owner(owner_marker.as_ref())
                        .await?;
                    if !owns_canonical
                        && write_target_path.full_path() == canonical_target_path.full_path()
                    {
                        warn!(
                            "task {} canceled after guard check with source changed but lease is no longer owner, skip cleanup for canonical target {}",
                            self.task.info.task_id, write_target_path
                        );
                    }
                    self.cleanup_abort_target_guarded(
                        owns_canonical,
                        &write_target_path,
                        &canonical_target_path,
                        "cancel-after-guard-source-changed",
                    )
                    .await;
                }
            }
            self.abort_streams(&mut reader, &mut writer).await;
            return err_box!(
                "Task {} canceled after source guard and before commit",
                self.task.info.task_id
            );
        }

        let mut entered_committing = false;
        if let Some(owner) = owner_marker.as_ref() {
            let switched = self
                .transition_source_state(
                    &owner.path,
                    &owner.marker,
                    &[SyncLifecycleState::Pending],
                    SyncLifecycleState::Committing,
                    None,
                )
                .await?;
            if !switched {
                self.cleanup_staged_target_if_needed(&write_target_path, &canonical_target_path)
                    .await;
                self.abort_streams(&mut reader, &mut writer).await;
                return err_box!(
                    "Task {} abort commit: path {} lease {} is no longer pending owner",
                    self.task.info.task_id,
                    owner.path,
                    owner.marker.lease
                );
            }
            entered_committing = true;
        }

        let mut rollback_already_applied = false;
        let commit_result: FsResult<i64> = async {
            writer.complete().await?;
            reader.complete().await?;

            // Close the tiny race between pre-complete snapshot check and actual commit.
            match self
                .source_guard_state(&source_path, &canonical_target_path, &source_snapshot)
                .await?
            {
                SourceGuardState::Match => {}
                SourceGuardState::Missing => {
                    let owns_canonical = self.abort_transition_if_owner(owner_marker.as_ref()).await?;
                    if owns_canonical {
                        rollback_already_applied = true;
                    }
                    self.cleanup_abort_target_guarded(
                        owns_canonical,
                        &write_target_path,
                        &canonical_target_path,
                        "commit-source-missing",
                    )
                    .await;
                    return err_box!(
                        "Task {} rolled back commit: source {} missing during commit",
                        self.task.info.task_id,
                        source_path
                    );
                }
                SourceGuardState::Changed => {
                    let owns_canonical =
                        self.abort_transition_if_owner(owner_marker.as_ref()).await?;
                    rollback_already_applied = true;
                    if !owns_canonical
                        && write_target_path.full_path() == canonical_target_path.full_path()
                    {
                        warn!(
                            "task {} source changed during commit but lease is no longer owner, skip cleanup for canonical target {}",
                            self.task.info.task_id, write_target_path
                        );
                    }
                    self.cleanup_abort_target_guarded(
                        owns_canonical,
                        &write_target_path,
                        &canonical_target_path,
                        "commit-source-changed",
                    )
                    .await;
                    return err_box!(
                        "Task {} rolled back commit: source {} changed during commit",
                        self.task.info.task_id,
                        source_path
                    );
                }
            }

            if self.should_stage_publish_target(&source_path, &canonical_target_path)
                && write_target_path.full_path() != canonical_target_path.full_path()
            {
                if !self.ensure_committing_owner(owner_marker.as_ref()).await? {
                    self.cleanup_abort_target_guarded(
                        false,
                        &write_target_path,
                        &canonical_target_path,
                        "commit-promote-ownership-lost",
                    )
                    .await;
                    return err_box!(
                        "Task {} lost committing ownership before staged publish promote",
                        self.task.info.task_id
                    );
                }
                self.promote_staged_publish_target(
                    &write_target_path,
                    &canonical_target_path,
                    self.task.info.job.overwrite.unwrap_or(false),
                )
                .await?;
            }

            // cv -> ufs
            let ufs_mtime = if source_path.is_cv() && !canonical_target_path.is_cv() {
                let ufs_status = self.get_ufs()?.get_status(&canonical_target_path).await?;
                if let Some(owner) = owner_marker.as_ref() {
                    let committed = self
                        .transition_source_state(
                            &owner.path,
                            &owner.marker,
                            &[SyncLifecycleState::Committing],
                            SyncLifecycleState::Committed,
                            Some(ufs_status.mtime),
                        )
                        .await?;
                    if !committed {
                        return err_box!(
                            "Task {} completed write but lease {} lost ownership before commit finalize",
                            self.task.info.task_id,
                            owner.marker.lease
                        );
                    }
                } else {
                    let attr_opts = SetAttrOptsBuilder::new()
                        .ufs_mtime(ufs_status.mtime)
                        .build();
                    self.fs.set_attr(reader.path(), attr_opts).await?;
                }
                ufs_status.mtime
            } else {
                if let Some(owner) = owner_marker.as_ref() {
                    let committed = self
                        .transition_source_state(
                            &owner.path,
                            &owner.marker,
                            &[SyncLifecycleState::Committing],
                            SyncLifecycleState::Committed,
                            None,
                        )
                        .await?;
                    if !committed {
                        return err_box!(
                            "Task {} completed write but lease {} lost ownership before commit finalize",
                            self.task.info.task_id,
                            owner.marker.lease
                        );
                    }
                }
                reader.status().storage_policy.ufs_mtime
            };

            Ok(ufs_mtime)
        }
        .await;

        let ufs_mtime = match commit_result {
            Ok(ufs_mtime) => ufs_mtime,
            Err(e) => {
                if entered_committing && !rollback_already_applied {
                    if let Err(rollback_err) =
                        self.abort_transition_if_owner(owner_marker.as_ref()).await
                    {
                        warn!(
                            "task {} failed to mark aborted after commit error on {}: rollback_err={}, cause={}",
                            self.task.info.task_id, source_path, rollback_err, e
                        );
                    }
                }
                self.cleanup_staged_target_if_needed(&write_target_path, &canonical_target_path)
                    .await;
                return Err(e);
            }
        };

        self.update_progress(writer.pos(), reader.len()).await;

        info!(
            "task {} completed, source_path {}, target_path {}, ufs_mtime:{}, copy bytes {}, read cost {} ms, task cost {} ms",
            self.task.info.task_id,
            self.task.info.source_path,
            self.task.info.target_path,
            ufs_mtime,
            writer.pos(),
            read_cost_ms,
            total_cost_ms,
        );

        Ok(())
    }

    async fn create_stream(&self) -> FsResult<(UnifiedReader, UnifiedWriter)> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;

        // Create reader (automatically selects filesystem based on scheme)
        let reader = self.open_unified(&source_path).await?;

        // Create writer (automatically selects filesystem based on scheme)
        let writer = self.create_unified(&source_path, &target_path).await?;

        Ok((reader, writer))
    }

    async fn open_unified(&self, path: &Path) -> FsResult<UnifiedReader> {
        if path.is_cv() {
            // Curvine path
            let reader = CacheSyncReader::new(&self.fs, path).await?;
            Ok(UnifiedReader::CacheSync(reader))
        } else {
            // UFS path
            let ufs = self.get_ufs()?;
            ufs.open(path).await
        }
    }

    async fn create_unified(&self, source_path: &Path, path: &Path) -> FsResult<UnifiedWriter> {
        if path.is_cv() {
            let target_status = match self.fs.get_status(path).await {
                Ok(status) => Some(status),
                Err(e) => {
                    if Self::is_missing_target_status_error(&e) {
                        None
                    } else {
                        return Err(e);
                    }
                }
            };

            if !Self::matches_expected_snapshot(
                target_status.as_ref(),
                self.task.info.job.expected_target_mtime,
                self.task.info.job.expected_target_missing.unwrap_or(false),
            ) {
                return err_box!(
                    "Reject hydrate write to {} because target metadata no longer matches expected snapshot",
                    path.full_path()
                );
            }

            if let Some(status) = target_status {
                if !status.is_complete {
                    return err_box!(
                        "Reject hydrate write to {} because target cv metadata is incomplete",
                        path.full_path()
                    );
                }
            }

            // Curvine path - get source mtime for UFS→Curvine import
            let source_mtime = if !source_path.is_cv() {
                // Import from UFS, get source mtime
                let ufs = self.get_ufs()?;
                let source_status = ufs.get_status(source_path).await?;
                source_status.mtime
            } else {
                // Curvine→Curvine (not supported yet), use 0
                0
            };

            let opts = CreateFileOptsBuilder::new()
                .create_parent(true)
                .replicas(self.task.info.job.replicas)
                .block_size(self.task.info.job.block_size)
                .storage_type(self.task.info.job.storage_type)
                .ttl_ms(self.task.info.job.ttl_ms)
                .ttl_action(self.task.info.job.ttl_action)
                .ufs_mtime(source_mtime)
                .build();

            let overwrite = self.task.info.job.overwrite.unwrap_or(false);
            let writer = self.fs.create_with_opts(path, opts, overwrite).await?;
            Ok(UnifiedWriter::Cv(writer))
        } else {
            let ufs = self.get_ufs()?;
            let configured_overwrite = self.task.info.job.overwrite.unwrap_or(false);
            let write_target = self
                .stage_publish_target(source_path, path)?
                .unwrap_or_else(|| path.clone());
            let (write_overwrite, conflict_check_target) =
                Self::stage_write_plan(configured_overwrite, &write_target, path);

            if let Some(conflict_target) = conflict_check_target.as_ref() {
                if ufs.exists(conflict_target).await? {
                    warn!(
                        "UFS file already exists, skipping: {}",
                        conflict_target.full_path()
                    );
                    return err_box!("File exists and overwrite=false");
                }
            }

            if !write_overwrite && ufs.exists(&write_target).await? {
                warn!(
                    "UFS file already exists, skipping: {}",
                    write_target.full_path()
                );
                return err_box!("File exists and overwrite=false");
            }

            ufs.create(&write_target, write_overwrite).await
        }
    }

    fn matches_expected_snapshot(
        target_status: Option<&curvine_common::state::FileStatus>,
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

    pub async fn update_progress(&self, loaded_size: i64, total_size: i64) {
        if let Err(e) = self.update_progress0(loaded_size, total_size).await {
            warn!("update progress failed, err: {:?}", e);
        }
    }

    pub async fn update_progress0(&self, loaded_size: i64, total_size: i64) -> FsResult<()> {
        let progress = self.task.update_progress(loaded_size, total_size);
        let task = &self.task;

        self.master_client
            .report_task(&task.info.job.job_id, &task.info.task_id, progress)
            .await
    }
}
