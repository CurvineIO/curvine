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
use curvine_client_core::file::{CurvineFileSystem, FsReader};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, FsKind, Path, Reader, Writer};
use curvine_model::transfer_failure_message;
use curvine_model::{
    CreateFileOpts, CreateFileOptsBuilder, FileAllocOpts, FileBlocks, FileStatus, SetAttrOpts,
    SetAttrOptsBuilder, WorkerTaskProgress, WorkerTaskState, TRANSFER_TEMP_PATH_MARKER,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{JoinHandle, RpcRuntime};
use curvine_transfer_client::TransferClient;
use curvine_unified_fs::{UfsFileSystem, UnifiedReader, UnifiedWriter};
use log::{debug, error, info, warn};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const TRANSFER_REPORT_TIMEOUT_MS: u64 = 5_000;
const TRANSFER_COMMIT_JOB_ID_XATTR: &str = "curvine.transfer.job_id";
const TRANSFER_COMMIT_RUN_ID_XATTR: &str = "curvine.transfer.run_id";
const TRANSFER_COMMIT_TASK_ID_XATTR: &str = "curvine.transfer.task_id";
const TRANSFER_COMMIT_SOURCE_PATH_XATTR: &str = "curvine.transfer.source_path";
const TRANSFER_COMMIT_TARGET_PATH_XATTR: &str = "curvine.transfer.target_path";

pub struct TransferTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    transfer_client: Option<TransferClient>,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
    /// Test hook: pin the commit protocol instead of deriving it from the
    /// target backend, so the commit paths can be driven over `file://`.
    #[cfg(test)]
    commit_mode_override: Option<CommitMode>,
}

struct CopyStream {
    reader: UnifiedReader,
    writer: UnifiedWriter,
    final_path: Path,
    /// Temp output; only set for [`CommitMode::RenameTemp`].
    temp_path: Option<Path>,
    commit_mode: CommitMode,
}

enum CommitTarget {
    RenameTemp,
    AlreadyCommitted,
}

/// How a transfer task publishes its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitMode {
    /// No commit protocol: the task writes the final path and stops there.
    /// Legacy master-dispatched tasks carried no temp path, commit marker or
    /// retry protocol.
    NoCommit,
    /// Write a temp output and publish it with a rename.
    RenameTemp,
    /// Write the final path directly and let the backend's own upload commit
    /// it. Object stores implement `rename` as a whole-object server-side copy
    /// (see [`ufs_rename_is_whole_object_copy`]), which costs a second full
    /// copy per write-back and cannot publish an object larger than the
    /// backend's single-copy ceiling (5 GiB for `CopyObject` on S3).
    DirectWrite,
}

impl TransferTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        transfer_client: Option<TransferClient>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        let transfer_client = match task.info.transfer_report.as_ref() {
            Some(report)
                if report.report_endpoints.is_empty() && report.report_target.is_empty() =>
            {
                transfer_client
            }
            Some(report) => {
                let report_endpoints = if report.report_endpoints.is_empty() {
                    vec![report.report_target.clone()]
                } else {
                    report.report_endpoints.clone()
                };
                match TransferClient::with_report_endpoints(
                    &fs.fs_context(),
                    report_endpoints.clone(),
                ) {
                    Ok(client) => Some(client),
                    Err(err) => {
                        warn!(
                            "transfer task has invalid report endpoints {:?}: job={} run={} task={} attempt={} source={} target={} err={}",
                            report_endpoints,
                            task.info.job.job_id,
                            report.run_id,
                            task.info.task_id,
                            report.attempt_id,
                            task.info.source_path,
                            task.info.target_path,
                            err,
                        );
                        None
                    }
                }
            }
            None => transfer_client,
        };
        Self {
            task,
            fs,
            factory,
            transfer_client,
            progress_interval_ms,
            task_timeout_ms,
            #[cfg(test)]
            commit_mode_override: None,
        }
    }

    pub fn get_ufs(&self) -> FsResult<UfsFileSystem> {
        self.factory.get_ufs(&self.task.info.job.mount_info)
    }

    fn log_context(&self) -> String {
        let (run_id, attempt_id) = self
            .task
            .info
            .transfer_report
            .as_ref()
            .map(|report| (report.run_id, report.attempt_id))
            .unwrap_or_default();
        format!(
            "job={} run={} task={} attempt={} source={} target={}",
            self.task.info.job.job_id,
            run_id,
            self.task.info.task_id,
            attempt_id,
            self.task.info.source_path,
            self.task.info.target_path,
        )
    }

    pub async fn run(&self) -> bool {
        let remove_task = match self.run0().await {
            Ok(remove_task) => remove_task,
            Err(e) => {
                if self.task.is_cancel() {
                    info!(
                        "transfer task stopped after cancellation request: {} err={}",
                        self.log_context(),
                        e
                    );
                    return self.finish_canceled().await.unwrap_or_else(|err| {
                        error!(
                            "transfer task cancellation finalization failed: {} err={}",
                            self.log_context(),
                            err
                        );
                        true
                    });
                }
                // The data replication process fails, set the status and report to the master
                error!("transfer task failed: {} err={}", self.log_context(), e);
                let progress = self.task.set_failed(transfer_failure_message(
                    match Path::from_str(&self.task.info.source_path) {
                        Ok(source) if source.is_cv() => curvine_model::TransferKind::Export,
                        _ => curvine_model::TransferKind::Load,
                    },
                    &self.task.info.source_path,
                    &self.task.info.target_path,
                    &e,
                ));
                let res = self.report_progress(progress).await;

                if let Err(e) = res {
                    warn!(
                        "transfer task failure report failed: {} err={}",
                        self.log_context(),
                        e
                    );
                    return self.task.info.transfer_report.is_none();
                }
                true
            }
        };

        crate::fault_point! {
            async,
            name: "worker.load_task.after_run",
            description: "After a worker load task runner has fully exited",
            context: {
                "task_id" => self.task.info.task_id.clone(),
            },
        }

        remove_task
    }

    async fn run0(&self) -> FsResult<bool> {
        if self.task.is_cancel() {
            info!(
                "transfer task canceled before starting: {}",
                self.log_context()
            );
            return self.finish_canceled().await;
        }

        self.task
            .update_state(WorkerTaskState::Loading, "Task started");

        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        // UFS -> CV load: fan out into multiple reader/writer segments when the
        // mount asks for it. The transfer route writes into its temp output and
        // publishes that with a rename (`run_parallel`), so it fans out exactly
        // like the legacy route instead of collapsing to one stream.
        if !source_path.is_cv() && target_path.is_cv() && self.max_parallel_streams() > 1 {
            let initial_source = self.get_ufs()?.get_status(&source_path).await?;
            // Segments are block aligned, so the requested stream count is only
            // an upper bound: a file that fits in one block cannot be split. Take
            // the parallel path only when it really produces more than one
            // reader, otherwise the resize/reopen dance buys nothing.
            let (_, segments) = self.parallel_segments(initial_source.len);
            if segments > 1 {
                return self
                    .run_parallel(&source_path, &target_path, &initial_source)
                    .await;
            }
        }

        let commit_mode = self.commit_mode(&target_path);
        if commit_mode == CommitMode::DirectWrite {
            // The upload is the commit, so a published object has no rename left
            // to make it idempotent: recognise it here and mark the source
            // before the upload.
            if let Some((committed_len, ufs_mtime)) =
                self.prepare_direct_commit(&target_path).await?
            {
                return self
                    .finish_already_committed(&source_path, committed_len, ufs_mtime)
                    .await;
            }
        }

        let mut stream = self.create_stream(commit_mode).await?;
        if self.task.is_cancel() {
            info!("transfer task canceled: {}", self.log_context());
            return self.cancel_stream(&mut stream).await;
        }
        let Some((copied_bytes, reader_len, read_cost_ms, total_cost_ms)) =
            (match self.copy_and_commit_stream(&mut stream).await {
                Ok(result) => result,
                Err(err) => {
                    self.cleanup_failed_stream(&stream).await;
                    return Err(err);
                }
            })
        else {
            return Ok(true);
        };

        let (cv_path, ufs_mtime) = if stream.final_path.is_cv() {
            // ufs -> cv
            (&stream.final_path, stream.reader.status().mtime)
        } else {
            // cv -> ufs
            let ufs_status = self.get_ufs()?.get_status(&stream.final_path).await?;
            (stream.reader.path(), ufs_status.mtime)
        };

        let mark_output = stream.final_path.is_cv();
        let attr_opts = self.cv_commit_attr_opts(ufs_mtime, mark_output);
        self.fs.set_attr(cv_path, attr_opts).await?;

        if let Err(err) = self.update_progress0(copied_bytes, reader_len, true).await {
            if self.task.info.transfer_report.is_some() {
                warn!(
                    "final transfer task report failed, keep task for scheduler probe: {} err={}",
                    self.log_context(),
                    err
                );
                return Ok(false);
            }
            return Err(err);
        }

        info!(
            "transfer task completed: {} ufs_mtime={} copied_bytes={} read_cost_ms={} task_cost_ms={}",
            self.log_context(),
            ufs_mtime,
            copied_bytes,
            read_cost_ms,
            total_cost_ms,
        );

        Ok(true)
    }

    async fn copy_and_commit_stream(
        &self,
        stream: &mut CopyStream,
    ) -> FsResult<Option<(i64, i64, u64, u64)>> {
        let mut last_progress_time = LocalTime::mills();
        let mut read_cost_ms = 0;
        let mut total_cost_ms = 0;

        loop {
            if self.task.is_cancel() {
                info!(
                    "transfer task canceled while copying: {}",
                    self.log_context()
                );
                self.cancel_stream(stream).await?;
                return Ok(None);
            }

            let spend = TimeSpent::new();
            let chunk = stream.reader.async_read(None).await?;
            read_cost_ms += spend.used_ms();

            if chunk.is_empty() {
                break;
            }

            stream.writer.async_write(chunk).await?;
            total_cost_ms += spend.used_ms();

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(stream.writer.pos(), stream.reader.len(), false)
                    .await;
            }

            if total_cost_ms > self.task_timeout_ms {
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
        }

        stream.writer.complete().await?;
        stream.reader.complete().await?;
        if self.task.is_cancel() {
            info!(
                "transfer task canceled before committing output: {}",
                self.log_context()
            );
            self.cancel_stream(stream).await?;
            return Ok(None);
        }
        let copied_bytes = stream.writer.pos();
        let reader_len = stream.reader.len();
        self.commit_output(stream).await?;
        Ok(Some((
            copied_bytes,
            reader_len,
            read_cost_ms,
            total_cost_ms,
        )))
    }

    async fn cleanup_failed_stream(&self, stream: &CopyStream) {
        let Some(temp_path) = &stream.temp_path else {
            return;
        };
        if let Err(err) = self.delete_temp_output(temp_path).await {
            warn!(
                "delete failed transfer temp output {} failed: {} err={}",
                temp_path.full_path(),
                self.log_context(),
                err
            );
        }
    }

    async fn cancel_stream(&self, stream: &mut CopyStream) -> FsResult<bool> {
        if let Err(err) = stream.writer.cancel().await {
            warn!(
                "cancel transfer writer failed: {} temp={:?} err={}",
                self.log_context(),
                stream.temp_path,
                err
            );
        }

        if let Some(temp_path) = &stream.temp_path {
            if let Err(err) = self.delete_temp_output(temp_path).await {
                warn!(
                    "delete canceled transfer temp output {} failed: {} err={}",
                    temp_path.full_path(),
                    self.log_context(),
                    err
                );
            }
        }

        self.finish_canceled().await
    }

    async fn finish_canceled(&self) -> FsResult<bool> {
        let progress = self.task.set_canceled("task canceled");
        if let Err(err) = self.report_progress(progress).await {
            info!(
                "canceled transfer task report was not accepted, remove local task anyway: {} err={}",
                self.log_context(), err
            );
        }
        Ok(true)
    }

    async fn delete_temp_output(&self, temp_path: &Path) -> FsResult<()> {
        if temp_path.is_cv() {
            match self.fs.delete(temp_path, false).await {
                Ok(_) | Err(FsError::FileNotFound(_)) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            let ufs = self.get_ufs()?;
            match ufs.delete(temp_path, false).await {
                Ok(_) | Err(FsError::FileNotFound(_)) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    async fn create_stream(&self, commit_mode: CommitMode) -> FsResult<CopyStream> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        let temp_path = match commit_mode {
            CommitMode::RenameTemp => Some(self.transfer_temp_path(&target_path)?),
            CommitMode::NoCommit | CommitMode::DirectWrite => None,
        };
        let write_path = temp_path.clone().unwrap_or_else(|| target_path.clone());

        // Create reader (automatically selects filesystem based on scheme)
        let reader = self.open_unified(&source_path).await?;

        // Create writer (automatically selects filesystem based on scheme)
        let writer = self.create_unified(&write_path).await?;

        Ok(CopyStream {
            reader,
            writer,
            final_path: target_path,
            temp_path,
            commit_mode,
        })
    }

    /// The commit protocol for `target_path`; see [`CommitMode`].
    fn commit_mode(&self, target_path: &Path) -> CommitMode {
        #[cfg(test)]
        if let Some(mode) = self.commit_mode_override {
            return mode;
        }
        commit_mode_for(self.task.info.transfer_report.is_some(), target_path)
    }

    /// Commit protocol for a [`CommitMode::DirectWrite`] output.
    ///
    /// The upload itself publishes the object, so nothing is left to rename:
    /// the source is marked *before* the upload (so a replay recognises the
    /// object this run produced) and an object that is already in place is
    /// reported as complete instead of failing `overwrite=false` on every
    /// replay and pinning the journal.
    ///
    /// Returns the length and mtime of an already published object.
    async fn prepare_direct_commit(&self, target_path: &Path) -> FsResult<Option<(i64, i64)>> {
        let ufs = self.get_ufs()?;
        let overwrite = self.task.info.job.overwrite.unwrap_or(false);
        match self
            .validate_ufs_commit_target(&ufs, target_path, overwrite)
            .await?
        {
            CommitTarget::RenameTemp => {
                self.mark_ufs_commit_source(target_path).await?;
                Ok(None)
            }
            CommitTarget::AlreadyCommitted => {
                let status = ufs.get_status(target_path).await?;
                Ok(Some((status.len, status.mtime)))
            }
        }
    }

    /// Report a direct-commit output that an earlier attempt already published.
    async fn finish_already_committed(
        &self,
        source_path: &Path,
        committed_len: i64,
        ufs_mtime: i64,
    ) -> FsResult<bool> {
        let src_len = self.fs.get_status(source_path).await?.len;
        self.fs
            .set_attr(source_path, self.cv_commit_attr_opts(ufs_mtime, false))
            .await?;

        if let Err(err) = self.update_progress0(committed_len, src_len, true).await {
            if self.task.info.transfer_report.is_some() {
                warn!(
                    "final transfer task report failed, keep task for scheduler probe: {} err={}",
                    self.log_context(),
                    err
                );
                return Ok(false);
            }
            return Err(err);
        }

        info!(
            "transfer task completed, output was already committed: {} committed_bytes={}",
            self.log_context(),
            committed_len,
        );
        Ok(true)
    }

    fn transfer_temp_path(&self, target_path: &Path) -> FsResult<Path> {
        let Some(report) = &self.task.info.transfer_report else {
            return Ok(target_path.clone());
        };
        Ok(Path::from_str(format!(
            "{}{}{}-{}-{}-{}",
            target_path.full_path(),
            TRANSFER_TEMP_PATH_MARKER,
            self.task.info.job.job_id,
            report.run_id,
            self.task.info.task_id,
            report.attempt_id
        ))?)
    }

    async fn commit_output(&self, stream: &CopyStream) -> FsResult<()> {
        match stream.commit_mode {
            // `DirectWrite` was published by the upload itself; `NoCommit`
            // never had a temp output to publish.
            CommitMode::NoCommit | CommitMode::DirectWrite => return Ok(()),
            CommitMode::RenameTemp => {}
        }
        let Some(temp_path) = &stream.temp_path else {
            return Ok(());
        };
        let overwrite = self.task.info.job.overwrite.unwrap_or(false);
        if stream.final_path.is_cv() {
            match self
                .validate_cv_commit_target(&stream.final_path, stream.reader.status(), overwrite)
                .await?
            {
                CommitTarget::RenameTemp => {
                    self.ensure_not_canceled()?;
                    self.fs.rename(temp_path, &stream.final_path).await?;
                }
                CommitTarget::AlreadyCommitted => {
                    if let Err(err) = self.fs.delete(temp_path, false).await {
                        warn!(
                            "delete redundant transfer temp output {} failed after idempotent commit check: {}",
                            temp_path.full_path(),
                            err
                        );
                    }
                }
            }
        } else {
            let ufs = self.get_ufs()?;
            match self
                .validate_ufs_commit_target(&ufs, &stream.final_path, overwrite)
                .await?
            {
                CommitTarget::RenameTemp => {
                    self.ensure_not_canceled()?;
                    self.mark_ufs_commit_source(&stream.final_path).await?;
                    rename_ufs_output(&ufs, temp_path, &stream.final_path).await?;
                }
                CommitTarget::AlreadyCommitted => {
                    if let Err(err) = ufs.delete(temp_path, false).await {
                        warn!(
                            "delete redundant transfer temp output {} failed after idempotent commit check: {}",
                            temp_path.full_path(),
                            err
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn ensure_not_canceled(&self) -> FsResult<()> {
        if self.task.is_cancel() {
            return err_box!("Transfer task was canceled before committing output");
        }
        Ok(())
    }

    async fn validate_cv_commit_target(
        &self,
        path: &Path,
        source_status: &FileStatus,
        overwrite: bool,
    ) -> FsResult<CommitTarget> {
        match self.fs.get_status(path).await {
            Ok(status) if status.is_dir => err_box!(
                "Transfer target {} is a directory; refusing to overwrite directory with file",
                path.full_path()
            ),
            Ok(status) if !overwrite => {
                if self.is_committed_transfer_output(&status, source_status) {
                    Ok(CommitTarget::AlreadyCommitted)
                } else {
                    Err(FsError::file_exists(path.full_path()))
                }
            }
            Ok(_) => Ok(CommitTarget::RenameTemp),
            Err(FsError::FileNotFound(_)) => Ok(CommitTarget::RenameTemp),
            Err(err) => Err(err),
        }
    }

    fn is_committed_transfer_output(
        &self,
        target_status: &FileStatus,
        source_status: &FileStatus,
    ) -> bool {
        if target_status.len != source_status.len
            || target_status.storage_policy.ufs_mtime != source_status.mtime
        {
            return false;
        }
        self.has_transfer_commit_marker(target_status, None)
    }

    async fn validate_ufs_commit_target(
        &self,
        ufs: &UfsFileSystem,
        path: &Path,
        overwrite: bool,
    ) -> FsResult<CommitTarget> {
        match ufs.get_status(path).await {
            Ok(status) if status.is_dir => err_box!(
                "Transfer target {} is a directory; refusing to overwrite directory with file",
                path.full_path()
            ),
            Ok(status) if !overwrite => {
                let source_path = Path::from_str(&self.task.info.source_path)?;
                let source_status = self.fs.get_status(&source_path).await?;
                if self.is_committed_ufs_output(&status, source_status, path) {
                    Ok(CommitTarget::AlreadyCommitted)
                } else {
                    Err(FsError::file_exists(path.full_path()))
                }
            }
            Ok(_) => Ok(CommitTarget::RenameTemp),
            Err(FsError::FileNotFound(_)) => Ok(CommitTarget::RenameTemp),
            Err(err) => Err(err),
        }
    }

    fn is_committed_ufs_output(
        &self,
        target_status: &FileStatus,
        source_status: FileStatus,
        target_path: &Path,
    ) -> bool {
        target_status.len == source_status.len
            && self.has_transfer_commit_marker(&source_status, Some(target_path))
    }

    fn has_transfer_commit_marker(&self, status: &FileStatus, target_path: Option<&Path>) -> bool {
        let Some(report) = &self.task.info.transfer_report else {
            return false;
        };
        xattr_equals(
            status,
            TRANSFER_COMMIT_JOB_ID_XATTR,
            self.task.info.job.job_id.as_bytes(),
        ) && xattr_equals(
            status,
            TRANSFER_COMMIT_RUN_ID_XATTR,
            report.run_id.to_string().as_bytes(),
        ) && xattr_equals(
            status,
            TRANSFER_COMMIT_TASK_ID_XATTR,
            self.task.info.task_id.as_bytes(),
        ) && xattr_equals(
            status,
            TRANSFER_COMMIT_SOURCE_PATH_XATTR,
            self.task.info.source_path.as_bytes(),
        ) && target_path.is_none_or(|path| {
            xattr_equals(
                status,
                TRANSFER_COMMIT_TARGET_PATH_XATTR,
                path.full_path().as_bytes(),
            )
        })
    }

    async fn mark_ufs_commit_source(&self, target_path: &Path) -> FsResult<()> {
        if self.task.info.transfer_report.is_none() {
            return Ok(());
        }
        let source_path = Path::from_str(&self.task.info.source_path)?;
        if !source_path.is_cv() {
            return err_box!(
                "Transfer export source {} must be a Curvine path",
                source_path.full_path()
            );
        }
        let report = self.task.info.transfer_report.as_ref().unwrap();
        self.fs
            .set_attr(
                &source_path,
                SetAttrOptsBuilder::new()
                    .add_x_attr(
                        TRANSFER_COMMIT_JOB_ID_XATTR,
                        self.task.info.job.job_id.as_bytes().to_vec(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_RUN_ID_XATTR,
                        report.run_id.to_string().into_bytes(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_TASK_ID_XATTR,
                        self.task.info.task_id.as_bytes().to_vec(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_SOURCE_PATH_XATTR,
                        self.task.info.source_path.as_bytes().to_vec(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_TARGET_PATH_XATTR,
                        target_path.full_path().as_bytes().to_vec(),
                    )
                    .build(),
            )
            .await
            .map(|_| ())
    }

    /// Create options for a Curvine output (cache target or transfer temp).
    fn cv_create_opts(&self) -> CreateFileOpts {
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(self.task.info.job.replicas)
            .block_size(self.task.info.job.block_size)
            .storage_type(self.task.info.job.storage_type)
            .ttl_ms(self.task.info.job.ttl_ms)
            .ttl_action(self.task.info.job.ttl_action)
            .build()
    }

    /// Attributes applied to the committed Curvine output: the source UFS mtime
    /// and, when the output itself is a Curvine file, the transfer commit
    /// marker that makes a replayed task idempotent.
    fn cv_commit_attr_opts(&self, ufs_mtime: i64, mark_transfer_output: bool) -> SetAttrOpts {
        let mut attr_builder = SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime);
        if mark_transfer_output {
            if let Some(report) = &self.task.info.transfer_report {
                attr_builder = attr_builder
                    .add_x_attr(
                        TRANSFER_COMMIT_JOB_ID_XATTR,
                        self.task.info.job.job_id.as_bytes().to_vec(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_RUN_ID_XATTR,
                        report.run_id.to_string().into_bytes(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_TASK_ID_XATTR,
                        self.task.info.task_id.as_bytes().to_vec(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_SOURCE_PATH_XATTR,
                        self.task.info.source_path.as_bytes().to_vec(),
                    )
                    .add_x_attr(
                        TRANSFER_COMMIT_TARGET_PATH_XATTR,
                        self.task.info.target_path.as_bytes().to_vec(),
                    );
            }
        }
        attr_builder.build()
    }

    async fn open_unified(&self, path: &Path) -> FsResult<UnifiedReader> {
        if path.is_cv() {
            let reader = if self.task.info.source_read_plan_json.is_empty() {
                self.fs.open(path).await?
            } else {
                let file_blocks: FileBlocks =
                    serde_json::from_str(&self.task.info.source_read_plan_json).map_err(|err| {
                        FsError::common(format!(
                            "Invalid CV source read plan for task {} path {}: {}",
                            self.task.info.task_id,
                            path.full_path(),
                            err
                        ))
                    })?;
                FsReader::new(path.clone(), self.fs.fs_context(), file_blocks)?
            };
            Ok(UnifiedReader::Cv(reader))
        } else {
            // UFS path
            let ufs = self.get_ufs()?;
            ufs.open(path).await
        }
    }

    async fn create_unified(&self, path: &Path) -> FsResult<UnifiedWriter> {
        if path.is_cv() {
            let opts = self.cv_create_opts();

            let overwrite = self.task.info.job.overwrite.unwrap_or(false);
            let writer = self.fs.create_with_opts(path, opts, overwrite).await?;
            Ok(UnifiedWriter::Cv(writer))
        } else {
            let ufs = self.get_ufs()?;
            let overwrite = self.task.info.job.overwrite.unwrap_or(false);

            // The overwrite=false check is owned by the adapter's create().
            // No caller-side exists() pre-check here: it would duplicate the
            // adapter's metadata round trip on every exported object.
            match ufs.create(path, overwrite).await {
                Ok(writer) => Ok(writer),
                Err(FsError::FileAlreadyExists(_)) if !overwrite => {
                    warn!(
                        "transfer task rejected because target exists and overwrite=false: {}",
                        self.log_context()
                    );
                    err_box!("File exists and overwrite=false")
                }
                Err(FsError::FileNotFound(_)) => {
                    ensure_ufs_parent(&ufs, path).await?;
                    ufs.create(path, overwrite).await
                }
                Err(e) => Err(e),
            }
        }
    }

    pub async fn update_progress(&self, loaded_size: i64, total_size: i64, is_last: bool) {
        if let Err(e) = self
            .update_progress0(loaded_size, total_size, is_last)
            .await
        {
            debug!(
                "transfer task progress report failed: {} err={}",
                self.log_context(),
                e
            );
        }
    }

    pub async fn update_progress0(
        &self,
        loaded_size: i64,
        total_size: i64,
        is_last: bool,
    ) -> FsResult<()> {
        let progress = self.task.update_progress(loaded_size, total_size, is_last);
        self.report_progress(progress).await
    }

    async fn report_progress(&self, progress: WorkerTaskProgress) -> FsResult<()> {
        let task = &self.task.info;
        if let Some(report_info) = &task.transfer_report {
            let Some(client) = &self.transfer_client else {
                let report_endpoints = if report_info.report_endpoints.is_empty() {
                    vec![report_info.report_target.clone()]
                } else {
                    report_info.report_endpoints.clone()
                };
                return err_box!(
                    "Transfer task {} has no transfer client for report endpoints {:?}",
                    task.task_id,
                    report_endpoints
                );
            };
            let accepted = tokio::time::timeout(
                Duration::from_millis(TRANSFER_REPORT_TIMEOUT_MS),
                client.report_task(&task.job.job_id, &task.task_id, report_info, progress),
            )
            .await
            .map_err(|_| {
                FsError::common(format!(
                    "Transfer task report timed out after {} ms: job={}, task={}, attempt={}",
                    TRANSFER_REPORT_TIMEOUT_MS,
                    task.job.job_id,
                    task.task_id,
                    report_info.attempt_id
                ))
            })??;
            if !accepted {
                return err_box!(
                    "Transfer task report rejected: job={}, task={}, attempt={}",
                    task.job.job_id,
                    task.task_id,
                    report_info.attempt_id
                );
            }
            Ok(())
        } else {
            // Legacy master-dispatched tasks carried no report target and
            // reported progress to the master job engine, which is gone. A task
            // without a report target cannot be accounted for by anyone, so
            // fail loudly instead of silently dropping its progress.
            err_box!(
                "Legacy load task {} has no transfer report target; progress cannot be reported",
                task.task_id
            )
        }
    }

    /// Upper bound on the number of independent reader+writer streams a large
    /// UFS->CV load may fan out into.
    fn max_parallel_streams(&self) -> usize {
        self.task
            .info
            .job
            .mount_info
            .properties
            .get("load_task.parallel_streams")
            .and_then(|v| v.parse::<usize>().ok())
            // Keep the fallback equal to the `[job]` default so a task that
            // arrives without the property (old master, replayed task) does not
            // silently use a different policy.
            .unwrap_or(curvine_config::JobConf::DEFAULT_LOAD_PARALLEL_STREAMS)
            .max(1)
    }

    fn min_bytes_per_stream(&self) -> i64 {
        self.task
            .info
            .job
            .mount_info
            .properties
            .get("load_task.min_bytes_per_stream")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(curvine_config::JobConf::DEFAULT_LOAD_MIN_BYTES_PER_STREAM_BYTES)
            .max(1)
    }

    fn effective_streams(&self, src_len: i64) -> usize {
        Self::stream_count(
            src_len,
            self.min_bytes_per_stream(),
            self.max_parallel_streams(),
        )
    }

    fn stream_count(src_len: i64, min_bytes: i64, cap: usize) -> usize {
        let min_bytes = min_bytes.max(1);
        let cap = cap.max(1);
        let by_size = (src_len / min_bytes).max(0) as usize;
        by_size.clamp(1, cap)
    }

    fn parallel_segments(&self, src_len: i64) -> (i64, usize) {
        Self::parallel_segments_for(
            src_len,
            self.effective_streams(src_len),
            self.task.info.job.block_size,
        )
    }

    /// Segment length and the number of non-empty segments a fan-out of
    /// `src_len` bytes really produces. `segment_len` rounds up to a whole block,
    /// so a small file (or a large `block_size`) collapses to a single segment
    /// and must not take the parallel path.
    fn parallel_segments_for(src_len: i64, streams: usize, block_size: i64) -> (i64, usize) {
        let streams = streams.max(1);
        let block_size = block_size.max(1);
        let seg = Self::segment_len(src_len, streams, block_size);
        let segments = if src_len <= 0 {
            1
        } else {
            ((src_len + seg - 1) / seg).max(1) as usize
        };
        (seg, segments.min(streams))
    }

    fn segment_len(src_len: i64, streams: usize, block_size: i64) -> i64 {
        let streams = streams.max(1) as i64;
        let block_size = block_size.max(1);
        let raw = (src_len + streams - 1) / streams;
        let aligned = ((raw + block_size - 1) / block_size) * block_size;
        aligned.max(block_size)
    }

    /// Progress to report while the segments are still running.
    ///
    /// The counter only ever grows, but a segment can be ahead of the bytes that
    /// landed, so it is capped at the source length and never reported negative.
    fn parallel_progress(copied: i64, src_len: i64) -> i64 {
        copied.clamp(0, src_len.max(0))
    }

    /// Fan a UFS -> CV load out into concurrent segments.
    ///
    /// The segments only ever write the transfer temp output; the cache target
    /// is published when every segment has completed, the byte count matches
    /// the source length and the source has been re-validated.
    async fn run_parallel(
        &self,
        source_path: &Path,
        target_path: &Path,
        initial_source: &FileStatus,
    ) -> FsResult<bool> {
        let src_len = initial_source.len;
        let (_, streams) = self.parallel_segments(src_len);
        let spend = TimeSpent::new();
        // A legacy task (no commit protocol) writes the target itself, a
        // transfer task writes its temp output and publishes it below.
        let write_path = match self.commit_mode(target_path) {
            CommitMode::RenameTemp => self.transfer_temp_path(target_path)?,
            CommitMode::NoCommit | CommitMode::DirectWrite => target_path.clone(),
        };
        let sink = CvFanoutSink::new(
            self.fs.clone(),
            self.cv_create_opts(),
            self.task.info.job.overwrite.unwrap_or(false),
        );

        let written = match self
            .fanout_copy(&sink, source_path, &write_path, initial_source)
            .await
        {
            Ok(FanoutOutcome::Written(written)) => written,
            Ok(FanoutOutcome::Canceled) => {
                self.remove_fanout_output(&sink, &write_path, target_path)
                    .await;
                return self.finish_canceled().await;
            }
            Err(e) => {
                self.remove_fanout_output(&sink, &write_path, target_path)
                    .await;
                return Err(e);
            }
        };

        if write_path != *target_path {
            if self.task.is_cancel() {
                info!(
                    "transfer task canceled before publishing the parallel output: {}",
                    self.log_context()
                );
                self.remove_fanout_output(&sink, &write_path, target_path)
                    .await;
                return self.finish_canceled().await;
            }
            let overwrite = self.task.info.job.overwrite.unwrap_or(false);
            match self
                .validate_cv_commit_target(target_path, initial_source, overwrite)
                .await?
            {
                CommitTarget::RenameTemp => {
                    sink.publish(&write_path, target_path).await?;
                }
                CommitTarget::AlreadyCommitted => {
                    if let Err(err) = sink.remove(&write_path).await {
                        warn!(
                            "delete redundant parallel transfer temp output {} failed after idempotent commit check: {} err={}",
                            write_path.full_path(),
                            self.log_context(),
                            err
                        );
                    }
                }
            }
        }

        let ufs_mtime = initial_source.mtime;
        self.fs
            .set_attr(target_path, self.cv_commit_attr_opts(ufs_mtime, true))
            .await?;

        if let Err(err) = self.update_progress0(written, src_len, true).await {
            if self.task.info.transfer_report.is_some() {
                warn!(
                    "final transfer task report failed, keep task for scheduler probe: {} err={}",
                    self.log_context(),
                    err
                );
                return Ok(false);
            }
            return Err(err);
        }

        info!(
            "transfer task completed (parallel x{}): {} ufs_mtime={} copied_bytes={} commit={:?} task_cost_ms={}",
            streams,
            self.log_context(),
            ufs_mtime,
            written,
            self.commit_mode(target_path),
            spend.used_ms(),
        );

        Ok(true)
    }

    /// Copy the whole source into `write_path` with `streams` concurrent,
    /// block-aligned segments.
    ///
    /// Returns only after every segment completed, the byte count matches the
    /// source length and the source was re-validated against the status the plan
    /// was made from; nothing is published here.
    async fn fanout_copy<S: FanoutSink>(
        &self,
        sink: &S,
        source_path: &Path,
        write_path: &Path,
        initial_source: &FileStatus,
    ) -> FsResult<FanoutOutcome> {
        let src_len = initial_source.len;
        let (seg, streams) = self.parallel_segments(src_len);
        let spend = TimeSpent::new();
        let deadline_ms = LocalTime::mills() + self.task_timeout_ms;

        sink.prepare(write_path, src_len).await?;

        let task_timeout_ms = self.task_timeout_ms;
        // Bytes copied so far, shared with the segments: the parent reports it on
        // `progress_interval_ms` instead of only when a whole segment joins.
        // Segments are block aligned (>= one block), so waiting for a join leaves
        // a gap far longer than the scheduler's staleness window and a slow
        // stream gets re-dispatched while it is still writing.
        let copied = Arc::new(AtomicI64::new(0));
        let mut handles = Vec::with_capacity(streams);
        for i in 0..streams {
            let off = i as i64 * seg;
            if off >= src_len {
                break;
            }
            let len = seg.min(src_len - off);
            let ctx = SegmentContext {
                ufs: self.get_ufs()?,
                source_path: source_path.clone(),
                write_path: write_path.clone(),
                off,
                len,
                task: self.task.clone(),
                copied: copied.clone(),
                deadline_ms,
                task_timeout_ms,
            };
            let rt = self.fs.clone_runtime();
            let sink = sink.clone();
            handles.push(rt.spawn(async move { sink.segment(ctx).await }));
        }

        let mut written: i64 = 0;
        for idx in 0..handles.len() {
            if self.task.is_cancel() {
                info!(
                    "transfer task canceled during parallel load: {}",
                    self.log_context()
                );
                Self::abort_remaining(&handles, idx);
                return Ok(FanoutOutcome::Canceled);
            }
            crate::fault_point! {
                async,
                name: "worker.load_task.parallel.before_join_await",
                description: "After the parent cancellation check and before awaiting a parallel load stream",
                context: {
                    "task_id" => self.task.info.task_id.clone(),
                    "stream_index" => idx as i64,
                },
            }
            let joined = loop {
                tokio::select! {
                    joined = &mut handles[idx] => break joined,
                    _ = tokio::time::sleep(Duration::from_millis(self.progress_interval_ms)) => {
                        let done = Self::parallel_progress(copied.load(Ordering::Relaxed), src_len);
                        if done > 0 {
                            self.update_progress(done, src_len, false).await;
                        }
                    }
                }
            };
            match joined {
                Ok(Ok(n)) => {
                    written += n;
                    let done = Self::parallel_progress(copied.load(Ordering::Relaxed), src_len);
                    self.update_progress(done, src_len, false).await;
                }
                Ok(Err(e)) => {
                    Self::abort_remaining(&handles, idx + 1);
                    return Err(e);
                }
                Err(e) => {
                    Self::abort_remaining(&handles, idx + 1);
                    return err_box!("parallel load join error: {}", e);
                }
            }
            if spend.used_ms() > self.task_timeout_ms {
                Self::abort_remaining(&handles, idx + 1);
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
        }

        if self.task.is_cancel() {
            info!(
                "transfer task canceled after parallel load: {}",
                self.log_context()
            );
            return Ok(FanoutOutcome::Canceled);
        }
        if written != src_len {
            return err_box!(
                "Task {} parallel load incomplete: wrote {} of {} bytes; refusing to mark cache valid",
                self.task.info.task_id,
                written,
                src_len
            );
        }

        let final_source = self.get_ufs()?.get_status(source_path).await?;
        if final_source.len != initial_source.len || final_source.mtime != initial_source.mtime {
            return err_box!(format!(
                "Task {} parallel load source changed during transfer (initial len={}, initial mtime={}, final len={}, final mtime={}); refusing to mark cache valid",
                self.task.info.task_id,
                initial_source.len,
                initial_source.mtime,
                final_source.len,
                final_source.mtime,
            ));
        }

        Ok(FanoutOutcome::Written(written))
    }

    /// Drop an uncommitted fan-out output; the target is never touched.
    async fn remove_fanout_output<S: FanoutSink>(
        &self,
        sink: &S,
        write_path: &Path,
        target_path: &Path,
    ) {
        if write_path == target_path {
            return;
        }
        if let Err(err) = sink.remove(write_path).await {
            warn!(
                "delete failed parallel transfer temp output {} failed: {} err={}",
                write_path.full_path(),
                self.log_context(),
                err
            );
        }
    }

    fn abort_remaining(handles: &[JoinHandle<FsResult<i64>>], from: usize) {
        for h in handles.iter().skip(from) {
            h.abort();
        }
    }
}

/// Result of a completed [`TransferTaskRunner::fanout_copy`].
#[derive(Debug)]
enum FanoutOutcome {
    Written(i64),
    Canceled,
}

/// A boxed, `Send` operation future.
///
/// The fan-out sink runs its segments on the runtime, so every operation it
/// hands to the driver has to be `Send`. Boxing keeps the `Send` requirement on
/// the concrete implementation (a Curvine writer, a local file) instead of
/// pushing it into the driver as a generic bound it cannot express.
type BoxFut<T> = Pin<Box<dyn Future<Output = FsResult<T>> + Send>>;

/// Everything one fan-out segment needs.
struct SegmentContext {
    /// UFS handle to read the source from.
    ufs: UfsFileSystem,
    source_path: Path,
    write_path: Path,
    /// Block-aligned offset and length of this segment.
    off: i64,
    len: i64,
    task: Arc<TaskContext>,
    /// Bytes copied by all segments, for progress reporting.
    copied: Arc<AtomicI64>,
    deadline_ms: u64,
    task_timeout_ms: u64,
}

impl SegmentContext {
    const READ_CHUNK_BYTES: i64 = 16 * 1024 * 1024;
}

/// Positioned-write target of a fan-out copy.
///
/// Concurrent segments need positioned writes into an already-created file,
/// which is a Curvine block property: a UFS writer is a sequential stream. A
/// production task therefore always fans out into Curvine -- the cache target
/// itself, or the transfer temp file that is renamed onto it. The trait exists
/// so the offset math, the join/progress logic and the publish step can be
/// driven by a local-file sink in tests, without a cluster.
trait FanoutSink: Clone + Send + Sync + 'static {
    /// Create `path` and truncate it to `len` bytes.
    fn prepare(&self, path: &Path, len: i64) -> BoxFut<()>;

    /// Copy one segment and return its length.
    fn segment(&self, ctx: SegmentContext) -> BoxFut<i64>;

    /// Publish `from` as `to` (rename).
    fn publish(&self, from: &Path, to: &Path) -> BoxFut<()>;

    /// Best-effort removal of an uncommitted output.
    fn remove(&self, path: &Path) -> BoxFut<()>;
}

/// The Curvine fan-out sink used by a production load.
#[derive(Clone)]
struct CvFanoutSink {
    fs: CurvineFileSystem,
    opts: CreateFileOpts,
    overwrite: bool,
}

impl CvFanoutSink {
    fn new(fs: CurvineFileSystem, opts: CreateFileOpts, overwrite: bool) -> Self {
        Self {
            fs,
            opts,
            overwrite,
        }
    }
}

impl FanoutSink for CvFanoutSink {
    fn prepare(&self, path: &Path, len: i64) -> BoxFut<()> {
        let fs = self.fs.clone();
        let opts = self.opts.clone();
        let overwrite = self.overwrite;
        let path = path.clone();
        Box::pin(async move {
            let mut owner = fs.create_with_opts(&path, opts, overwrite).await?;
            owner.resize(FileAllocOpts::with_truncate(len)).await?;
            drop(owner);
            Ok(())
        })
    }

    fn segment(&self, ctx: SegmentContext) -> BoxFut<i64> {
        let fs = self.fs.clone();
        Box::pin(async move {
            let off = ctx.off;
            let len = ctx.len;
            let mut reader = ctx.ufs.open(&ctx.source_path).await?;
            reader.seek(off).await?;
            let mut writer = fs.open_for_write(&ctx.write_path, false).await?;
            writer.seek(off).await?;

            crate::fault_point! {
                async,
                name: "worker.load_task.parallel.before_segment_copy",
                description: "After a parallel load segment opens its streams and before it copies data",
                context: {
                    "task_id" => ctx.task.info.task_id.clone(),
                    "segment_offset" => off,
                    "segment_len" => len,
                },
            }

            let mut remaining = len;
            while remaining > 0 {
                if ctx.task.is_cancel() {
                    let _ = writer.cancel().await;
                    return Ok(0);
                }
                if LocalTime::mills() > ctx.deadline_ms {
                    let _ = writer.cancel().await;
                    return err_box!(
                        "Task {} exceed timeout {} ms (segment [{}, {}))",
                        ctx.task.info.task_id,
                        ctx.task_timeout_ms,
                        off,
                        off + len
                    );
                }
                let want = remaining.min(SegmentContext::READ_CHUNK_BYTES) as usize;
                let chunk = reader.async_read(Some(want)).await?;
                if chunk.is_empty() {
                    break;
                }
                let written_now = chunk.len() as i64;
                remaining -= written_now;
                writer.async_write(chunk).await?;
                ctx.copied.fetch_add(written_now, Ordering::Relaxed);
            }
            if remaining != 0 {
                let _ = writer.cancel().await;
                return err_box!(
                    "short read on segment [{}, {}): {} bytes missing (source shorter than stat len?)",
                    off,
                    off + len,
                    remaining
                );
            }
            writer.complete().await?;
            reader.complete().await?;
            Ok(len)
        })
    }

    fn publish(&self, from: &Path, to: &Path) -> BoxFut<()> {
        let fs = self.fs.clone();
        let from = from.clone();
        let to = to.clone();
        Box::pin(async move {
            if !fs.rename(&from, &to).await? {
                return err_box!(
                    "Transfer output rename did not commit {} to {}",
                    from.full_path(),
                    to.full_path()
                );
            }
            Ok(())
        })
    }

    fn remove(&self, path: &Path) -> BoxFut<()> {
        let fs = self.fs.clone();
        let path = path.clone();
        Box::pin(async move {
            match fs.delete(&path, false).await {
                Ok(_) | Err(FsError::FileNotFound(_)) => Ok(()),
                Err(err) => Err(err),
            }
        })
    }
}

/// Commit protocol for `target_path`; see [`CommitMode`].
fn commit_mode_for(has_transfer_report: bool, target_path: &Path) -> CommitMode {
    if !has_transfer_report {
        // A legacy master-dispatched task writes the final path and has no
        // commit protocol at all.
        return CommitMode::NoCommit;
    }
    if target_path.is_cv() {
        return CommitMode::RenameTemp;
    }
    if ufs_rename_is_whole_object_copy(target_path) {
        CommitMode::DirectWrite
    } else {
        CommitMode::RenameTemp
    }
}

/// Object-store backends whose UFS `rename` degrades to a whole-object
/// server-side copy (`Operator::rename` -> `copy` + delete in opendal).
///
/// Publishing a temp output through that rename costs one extra full-object copy
/// per write-back and cannot commit an object above the backend's single-copy
/// ceiling (5 GiB for `CopyObject` on S3), so a large export would fail at commit
/// and the journal would replay the same entry without advancing
/// `ufs_applied`. These backends write the final key directly and let the
/// (multipart) upload commit it.
///
/// Every other backend -- including any scheme this list does not know -- keeps
/// the temp+rename protocol: a backend that has a real atomic rename must keep
/// the isolation a temp output gives a failed write-back.
fn ufs_rename_is_whole_object_copy(target_path: &Path) -> bool {
    matches!(
        FsKind::from_scheme_opt(target_path.scheme()),
        FsKind::S3 | FsKind::Oss | FsKind::Gcs | FsKind::Azblob | FsKind::Cos
    )
}

async fn rename_ufs_output(
    ufs: &UfsFileSystem,
    temp_path: &Path,
    final_path: &Path,
) -> FsResult<()> {
    ensure_ufs_parent(ufs, final_path).await?;
    if !ufs.rename(temp_path, final_path).await? {
        return err_box!(
            "Transfer output rename did not commit {} to {}",
            temp_path.full_path(),
            final_path.full_path()
        );
    }
    Ok(())
}

async fn ensure_ufs_parent(ufs: &UfsFileSystem, path: &Path) -> FsResult<()> {
    if let Some(parent) = path.parent()? {
        if !parent.is_root() {
            ufs.mkdir(&parent, true).await?;
        }
    }
    Ok(())
}

fn xattr_equals(status: &FileStatus, key: &str, expected: &[u8]) -> bool {
    status
        .x_attr
        .get(key)
        .map(|actual| actual.as_slice() == expected)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        commit_mode_for, rename_ufs_output, BoxFut, CommitMode, FanoutOutcome, FanoutSink,
        SegmentContext, TransferTaskRunner,
    };
    use crate::common::UfsFactory;
    use crate::worker::task::TaskContext;
    use curvine_client_core::file::CurvineFileSystem;
    use curvine_config::ClusterConf;
    use curvine_fs_api::{FileSystem, Path, Reader, Writer};
    use curvine_model::{
        MountInfo, StorageType, TransferJobInfo, TransferTaskInfo, TransferTaskReportInfo,
        TtlAction, WorkerAddress,
    };
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
    use curvine_unified_fs::UfsFileSystem;
    use std::collections::HashMap;
    use std::fs;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Mutex};

    const MB: i64 = 1024 * 1024;

    // Reproduce the exact planning loop run_parallel uses, so tests exercise the
    // real offset/len math (block alignment + last-segment clamp), which is the
    // most error-prone part of the fan-out.
    fn plan(src_len: i64, streams: usize, block_size: i64) -> Vec<(i64, i64)> {
        let seg = TransferTaskRunner::segment_len(src_len, streams, block_size);
        let mut ranges = Vec::new();
        for i in 0..streams {
            let off = i as i64 * seg;
            if off >= src_len {
                break;
            }
            let len = seg.min(src_len - off);
            ranges.push((off, len));
        }
        ranges
    }

    #[test]
    fn segment_len_is_block_aligned() {
        let seg = TransferTaskRunner::segment_len(10 * 1024 * MB, 8, 4 * MB);
        assert_eq!(
            seg % (4 * MB),
            0,
            "segment must be a multiple of block_size"
        );
        assert!(seg >= 10 * 1024 * MB / 8);
    }

    #[test]
    fn segment_len_rounds_up_to_block_multiple() {
        let seg = TransferTaskRunner::segment_len(100 * MB, 3, 4 * MB);
        assert_eq!(seg, 36 * MB);
        assert_eq!(seg % (4 * MB), 0);
    }

    #[test]
    fn segment_len_never_below_block_size() {
        let seg = TransferTaskRunner::segment_len(1, 8, 4 * MB);
        assert_eq!(seg, 4 * MB);
    }

    #[test]
    fn segment_len_handles_zero_streams_and_block() {
        assert_eq!(TransferTaskRunner::segment_len(1000, 0, 0), 1000);
    }

    #[test]
    fn plan_ranges_are_contiguous_disjoint_and_cover_whole_file() {
        for &(src_len, streams, block_size) in &[
            (10 * 1024 * MB, 8, 4 * MB),
            (100 * MB, 3, 4 * MB),
            (7 * MB + 123, 4, 4 * MB),
            (4 * MB, 8, 4 * MB),
            (1, 8, 4 * MB),
        ] {
            let ranges = plan(src_len, streams, block_size);
            assert!(!ranges.is_empty(), "must produce at least one range");
            assert_eq!(ranges[0].0, 0);
            let mut expected_off = 0;
            for (off, len) in &ranges {
                assert_eq!(*off, expected_off, "gap/overlap at {}", off);
                assert!(*len > 0, "empty segment");
                expected_off += len;
            }
            assert_eq!(
                expected_off, src_len,
                "ranges must cover exactly [0,{})",
                src_len
            );
            for (off, _) in &ranges {
                assert_eq!(*off % block_size, 0, "segment start not block-aligned");
            }
        }
    }

    #[test]
    fn plan_does_not_over_allocate_streams_for_small_files() {
        let ranges = plan(4 * MB, 8, 4 * MB);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 4 * MB));
    }

    #[test]
    fn stream_count_grows_with_size_and_caps() {
        let min = 256 * MB;
        let cap = 8;
        assert_eq!(TransferTaskRunner::stream_count(100 * MB, min, cap), 1);
        assert_eq!(TransferTaskRunner::stream_count(min - 1, min, cap), 1);
        assert_eq!(TransferTaskRunner::stream_count(512 * MB, min, cap), 2);
        assert_eq!(TransferTaskRunner::stream_count(1024 * MB, min, cap), 4);
        assert_eq!(TransferTaskRunner::stream_count(2048 * MB, min, cap), 8);
        assert_eq!(
            TransferTaskRunner::stream_count(200 * 1024 * MB, min, cap),
            8
        );
    }

    #[test]
    fn stream_count_defensive_bounds() {
        assert_eq!(TransferTaskRunner::stream_count(0, 256 * MB, 8), 1);
        assert_eq!(TransferTaskRunner::stream_count(-5, 256 * MB, 8), 1);
        assert_eq!(TransferTaskRunner::stream_count(1024 * MB, 0, 8), 8);
        assert_eq!(TransferTaskRunner::stream_count(1024 * MB, 256 * MB, 0), 1);
    }

    /// A scratch directory for one test, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "transfer-{}-{}-{}",
                tag,
                std::process::id(),
                curvine_runtime::common::LocalTime::mills()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }

        fn uri(&self, name: &str) -> String {
            format!("file://{}", self.file(name).display())
        }

        fn path(&self, name: &str) -> Path {
            Path::from_str(self.uri(name)).unwrap()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Deterministic pseudo-random payload: a byte-identical copy is what every
    /// assertion below compares against.
    fn payload(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        for _ in 0..len {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            out.push((state >> 33) as u8);
        }
        out
    }

    fn mount_info(dir: &Scratch, streams: usize) -> MountInfo {
        MountInfo {
            ufs_path: format!("file://{}", dir.0.display()),
            properties: HashMap::from([
                (
                    "load_task.parallel_streams".to_string(),
                    streams.to_string(),
                ),
                ("load_task.min_bytes_per_stream".to_string(), MB.to_string()),
            ]),
            ..Default::default()
        }
    }

    /// A transfer-service task (report present) over `file://` on both ends.
    fn task_info(
        dir: &Scratch,
        source: &str,
        target: &str,
        streams: usize,
        report: bool,
    ) -> TransferTaskInfo {
        TransferTaskInfo {
            job: TransferJobInfo {
                job_id: "job-parallel".to_string(),
                source_path: source.to_string(),
                target_path: target.to_string(),
                block_size: 4 * MB,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: mount_info(dir, streams),
                create_time: 0,
                overwrite: Some(true),
            },
            task_id: "task-parallel".to_string(),
            worker: WorkerAddress::default(),
            source_path: source.to_string(),
            target_path: target.to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: report.then(|| TransferTaskReportInfo {
                run_id: 7,
                attempt_id: 1,
                worker_id: 1,
                worker_session_id: "session".to_string(),
                // Empty endpoints make the runner keep the (absent) client
                // instead of dialling a report target, so progress reports fail
                // locally and are only logged.
                report_target: String::new(),
                report_endpoints: Vec::new(),
            }),
        }
    }

    /// A runner whose Curvine client is never used: every path in these tests is
    /// `file://`.
    fn test_runner(info: TransferTaskInfo) -> (TransferTaskRunner, Arc<AsyncRuntime>) {
        let rt = Arc::new(AsyncRuntime::single());
        let conf = ClusterConf::default();
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone()).unwrap();
        let factory = UfsFactory::with_rt(&conf.client, rt.clone());
        let task = Arc::new(TaskContext::new(info));
        let runner = TransferTaskRunner::new(task, fs, Arc::new(factory), None, 60_000, 60_000);
        (runner, rt)
    }

    /// A fan-out sink over plain local files.
    ///
    /// It records every segment it is asked to write, so the test can assert the
    /// fan-out really produced disjoint positioned writers. Each segment writes
    /// at its own offset in the same file, exactly like the Curvine sink.
    #[derive(Clone, Default)]
    struct LocalFanoutSink {
        opened: Arc<Mutex<Vec<i64>>>,
    }

    impl LocalFanoutSink {
        fn opened_offsets(&self) -> Vec<i64> {
            self.opened.lock().unwrap().clone()
        }
    }

    impl FanoutSink for LocalFanoutSink {
        fn prepare(&self, path: &Path, len: i64) -> BoxFut<()> {
            let path = path.clone();
            Box::pin(async move {
                if let Some(parent) = path.parent()? {
                    fs::create_dir_all(parent.path())?;
                }
                fs::File::create(path.path())?.set_len(len as u64)?;
                Ok(())
            })
        }

        fn segment(&self, ctx: SegmentContext) -> BoxFut<i64> {
            self.opened.lock().unwrap().push(ctx.off);
            let off = ctx.off;
            let len = ctx.len;
            let ufs = ctx.ufs.clone();
            let source = ctx.source_path.clone();
            let write_path = ctx.write_path.clone();
            let copied: Arc<AtomicI64> = ctx.copied.clone();
            Box::pin(async move {
                let mut reader = ufs.open(&source).await?;
                reader.seek(off).await?;
                let mut file = fs::OpenOptions::new().write(true).open(write_path.path())?;
                file.seek(SeekFrom::Start(off as u64))?;

                let mut remaining = len;
                while remaining > 0 {
                    let want = remaining.min(3 * MB) as usize;
                    let chunk = reader.async_read(Some(want)).await?;
                    if chunk.is_empty() {
                        break;
                    }
                    let data = chunk.as_slice();
                    file.write_all(data)?;
                    remaining -= data.len() as i64;
                    copied.fetch_add(data.len() as i64, Ordering::Relaxed);
                }
                if remaining != 0 {
                    return curvine_core_error::err_box!("short read on a local fan-out segment");
                }
                file.flush()?;
                Ok(len)
            })
        }

        fn publish(&self, from: &Path, to: &Path) -> BoxFut<()> {
            let from = from.clone();
            let to = to.clone();
            Box::pin(async move {
                fs::rename(from.path(), to.path())?;
                Ok(())
            })
        }

        fn remove(&self, path: &Path) -> BoxFut<()> {
            let path = path.clone();
            Box::pin(async move {
                match fs::remove_file(path.path()) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(e.into()),
                }
            })
        }
    }

    #[test]
    fn commit_mode_is_backend_aware() {
        // Object stores whose `rename` is a whole-object server-side copy: the
        // upload commits, so there is no single `CopyObject` and no 5 GiB
        // single-copy ceiling.
        for scheme in ["s3", "s3a", "oss", "cos", "gcs", "gs", "azblob"] {
            let target = Path::from_str(format!("{scheme}://bucket/key")).unwrap();
            assert_eq!(
                commit_mode_for(true, &target),
                CommitMode::DirectWrite,
                "{scheme} must not commit with a copy-based rename"
            );
        }

        // Backends with a real rename (and every scheme this mapping does not
        // know) keep the temp output.
        for uri in [
            "file:///tmp/out.bin",
            "hdfs://nameservice/out.bin",
            "cosn://bucket/out.bin",
            "unknown://bucket/out.bin",
        ] {
            assert_eq!(
                commit_mode_for(true, &Path::from_str(uri).unwrap()),
                CommitMode::RenameTemp,
                "{uri} has a real rename and must keep the temp output"
            );
        }

        // A Curvine target is always renamed, and a legacy task has no commit
        // protocol at all.
        assert_eq!(
            commit_mode_for(true, &Path::from_str("cv://cluster/out.bin").unwrap()),
            CommitMode::RenameTemp
        );
        assert_eq!(
            commit_mode_for(false, &Path::from_str("s3://bucket/key").unwrap()),
            CommitMode::NoCommit
        );
    }

    #[test]
    fn transfer_fanout_writes_multiple_segments_and_publishes_identical_output() {
        let dir = Scratch::new("fanout");
        let len = 12 * MB as usize;
        let source_bytes = payload(len);
        fs::write(dir.file("source.bin"), &source_bytes).unwrap();

        let source = dir.path("source.bin");
        let temp = dir.path("target.bin.curvine-transfer-tmp");
        let target = dir.path("target.bin");
        let (runner, rt) = test_runner(task_info(
            &dir,
            &dir.uri("source.bin"),
            &dir.uri("target.bin"),
            3,
            true,
        ));

        let sink = LocalFanoutSink::default();
        let initial = rt
            .block_on(async { runner.get_ufs()?.get_status(&source).await })
            .unwrap();
        // 12 MiB, 4 MiB blocks, 3 requested streams: three block-aligned
        // segments on the *transfer* route (a report is set), which used to
        // collapse to a single stream.
        let outcome = rt
            .block_on(runner.fanout_copy(&sink, &source, &temp, &initial))
            .unwrap();

        match outcome {
            FanoutOutcome::Written(written) => assert_eq!(written, len as i64),
            FanoutOutcome::Canceled => panic!("fan-out was cancelled"),
        }
        assert_eq!(
            sink.opened_offsets(),
            vec![0, 4 * MB, 8 * MB],
            "each segment must open its own positioned writer"
        );

        // Nothing is published until the caller publishes it.
        assert!(!dir.file("target.bin").exists());
        assert_eq!(
            fs::read(dir.file("target.bin.curvine-transfer-tmp")).unwrap(),
            source_bytes
        );

        rt.block_on(sink.publish(&temp, &target)).unwrap();
        assert!(!dir.file("target.bin.curvine-transfer-tmp").exists());
        assert_eq!(
            fs::read(dir.file("target.bin")).unwrap(),
            source_bytes,
            "the published output must be byte-identical to the source"
        );
    }

    #[test]
    fn transfer_fanout_removes_its_temp_output_on_failure() {
        let dir = Scratch::new("fanout-fail");
        fs::write(dir.file("source.bin"), payload(MB as usize)).unwrap();
        let source = dir.path("source.bin");
        let temp = dir.path("target.bin.curvine-transfer-tmp");
        let target = dir.path("target.bin");
        let (runner, rt) = test_runner(task_info(
            &dir,
            &dir.uri("source.bin"),
            &dir.uri("target.bin"),
            3,
            true,
        ));
        let sink = LocalFanoutSink::default();

        // A source that shrank after the plan was made must fail the fan-out
        // instead of publishing a short output.
        let initial = rt
            .block_on(async { runner.get_ufs()?.get_status(&source).await })
            .unwrap();
        rt.block_on(sink.prepare(&temp, initial.len)).unwrap();
        fs::remove_file(dir.file("source.bin")).unwrap();
        let err = rt
            .block_on(runner.fanout_copy(&sink, &source, &temp, &initial))
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("no such file")
                || err.to_string().to_lowercase().contains("not found"),
            "unexpected error: {}",
            err
        );
        // The driver never publishes; cleanup stays with the caller.
        runner_cleanup(&runner, &sink, &temp, &target, &rt);
        assert!(!dir.file("target.bin.curvine-transfer-tmp").exists());
        assert!(!dir.file("target.bin").exists());
    }

    fn runner_cleanup(
        runner: &TransferTaskRunner,
        sink: &LocalFanoutSink,
        temp: &Path,
        target: &Path,
        rt: &AsyncRuntime,
    ) {
        rt.block_on(runner.remove_fanout_output(sink, temp, target));
    }

    #[test]
    fn direct_commit_writes_the_final_key_without_a_rename() {
        let dir = Scratch::new("direct-commit");
        let len = 3 * MB as usize;
        let source_bytes = payload(len);
        fs::write(dir.file("source.bin"), &source_bytes).unwrap();

        let target = dir.path("target.bin");
        let (mut runner, rt) = test_runner(task_info(
            &dir,
            &dir.uri("source.bin"),
            &dir.uri("target.bin"),
            8,
            true,
        ));
        // Force the protocol a copy-based object store gets: the upload is the
        // commit, there is no temp output and no rename.
        runner.commit_mode_override = Some(CommitMode::DirectWrite);
        assert_eq!(runner.commit_mode(&target), CommitMode::DirectWrite);

        let target_uri = dir.uri("target.bin");
        rt.block_on(async {
            let mut stream = runner.create_stream(CommitMode::DirectWrite).await.unwrap();
            assert!(
                stream.temp_path.is_none(),
                "direct commit must not use a temp output"
            );
            assert_eq!(stream.writer.path().full_path(), target_uri);

            let (copied, reader_len, _, _) = runner
                .copy_and_commit_stream(&mut stream)
                .await
                .unwrap()
                .expect("copy must complete");
            assert_eq!(copied, len as i64);
            assert_eq!(reader_len, len as i64);
        });

        assert_eq!(
            fs::read(dir.file("target.bin")).unwrap(),
            source_bytes,
            "the direct-commit output must be byte-identical to the source"
        );
        let leftovers: Vec<_> = fs::read_dir(&dir.0)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains("curvine-transfer-tmp"))
            .collect();
        assert!(leftovers.is_empty(), "no temp output: {:?}", leftovers);
    }

    #[test]
    fn failed_ufs_rename_keeps_existing_target() {
        let base_dir = std::env::temp_dir().join(format!(
            "transfer-ufs-commit-{}-{}",
            std::process::id(),
            curvine_runtime::common::LocalTime::mills()
        ));
        fs::create_dir_all(&base_dir).unwrap();
        let target = Path::from_str(format!("file://{}/target.txt", base_dir.display())).unwrap();
        let missing_temp =
            Path::from_str(format!("file://{}/missing-temp.txt", base_dir.display())).unwrap();
        fs::write(base_dir.join("target.txt"), "original-target").unwrap();

        let ufs = UfsFileSystem::new(&target, HashMap::new(), None).unwrap();
        let rt = AsyncRuntime::single();
        let err = rt
            .block_on(rename_ufs_output(&ufs, &missing_temp, &target))
            .unwrap_err();
        assert!(err.to_string().contains("No such file") || err.to_string().contains("not found"));
        assert_eq!(
            fs::read_to_string(base_dir.join("target.txt")).unwrap(),
            "original-target"
        );

        let _ = fs::remove_dir_all(base_dir);
    }

    #[test]
    fn parallel_segments_requires_more_than_one_block() {
        // 128 MiB object, 128 MiB block: four requested streams still produce a
        // single segment, so the parallel path must not run.
        assert_eq!(
            TransferTaskRunner::parallel_segments_for(128 * MB, 4, 128 * MB),
            (128 * MB, 1)
        );
        assert_eq!(
            TransferTaskRunner::parallel_segments_for(200 * MB, 8, 128 * MB),
            (128 * MB, 2)
        );
        assert_eq!(
            TransferTaskRunner::parallel_segments_for(MB, 8, 128 * MB).1,
            1
        );
    }

    #[test]
    fn parallel_segments_matches_the_plan_it_produces() {
        for &(src_len, streams, block_size) in &[
            (1024 * MB, 8, 128 * MB),
            (200 * MB, 6, 64 * MB),
            (3 * 1024 * MB + 7, 4, 128 * MB),
            (4 * MB, 8, 4 * MB),
        ] {
            let (seg, segments) =
                TransferTaskRunner::parallel_segments_for(src_len, streams, block_size);
            let ranges = plan(src_len, streams, block_size);
            assert_eq!(segments, ranges.len(), "segment count must match the plan");
            for (_, len) in &ranges {
                assert!(*len <= seg, "segment longer than the planned segment size");
            }
            assert!(segments <= streams.max(1));
        }
    }

    #[test]
    fn parallel_progress_is_capped_and_never_negative() {
        assert_eq!(TransferTaskRunner::parallel_progress(0, 1024), 0);
        assert_eq!(TransferTaskRunner::parallel_progress(512, 1024), 512);
        assert_eq!(TransferTaskRunner::parallel_progress(2048, 1024), 1024);
        assert_eq!(TransferTaskRunner::parallel_progress(-1, 1024), 0);
        assert_eq!(TransferTaskRunner::parallel_progress(7, 0), 0);
    }
}
