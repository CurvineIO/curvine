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
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_job_client::JobMasterClient;
use curvine_job_client::TransferClient;
use curvine_model::transfer_failure_message;
use curvine_model::{
    CreateFileOptsBuilder, FileAllocOpts, FileBlocks, FileStatus, JobTaskProgress, JobTaskState,
    SetAttrOptsBuilder, TRANSFER_TEMP_PATH_MARKER,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{JoinHandle, RpcRuntime};
use curvine_unified_fs::{UfsFileSystem, UnifiedReader, UnifiedWriter};
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::Duration;

const TRANSFER_REPORT_TIMEOUT_MS: u64 = 5_000;
const TRANSFER_COMMIT_JOB_ID_XATTR: &str = "curvine.transfer.job_id";
const TRANSFER_COMMIT_RUN_ID_XATTR: &str = "curvine.transfer.run_id";
const TRANSFER_COMMIT_TASK_ID_XATTR: &str = "curvine.transfer.task_id";
const TRANSFER_COMMIT_SOURCE_PATH_XATTR: &str = "curvine.transfer.source_path";
const TRANSFER_COMMIT_TARGET_PATH_XATTR: &str = "curvine.transfer.target_path";

pub struct LoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    master_client: JobMasterClient,
    transfer_client: Option<TransferClient>,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

struct CopyStream {
    reader: UnifiedReader,
    writer: UnifiedWriter,
    final_path: Path,
    temp_path: Option<Path>,
}

enum CommitTarget {
    RenameTemp,
    AlreadyCommitted,
}

impl LoadTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        transfer_client: Option<TransferClient>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        let master_client = JobMasterClient::new(fs.fs_client());
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
            master_client,
            transfer_client,
            progress_interval_ms,
            task_timeout_ms,
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
            .update_state(JobTaskState::Loading, "Task started");

        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        if self.task.info.transfer_report.is_none()
            && !source_path.is_cv()
            && target_path.is_cv()
            && self.max_parallel_streams() > 1
        {
            let initial_source = self.get_ufs()?.get_status(&source_path).await?;
            if self.effective_streams(initial_source.len) > 1 {
                return self
                    .run_parallel(&source_path, &target_path, &initial_source)
                    .await;
            }
        }

        let mut stream = self.create_stream().await?;
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

        let mut attr_builder = SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime);
        if stream.final_path.is_cv() {
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
        let attr_opts = attr_builder.build();
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

    async fn create_stream(&self) -> FsResult<CopyStream> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        let write_path = self.transfer_temp_path(&target_path)?;

        // Create reader (automatically selects filesystem based on scheme)
        let reader = self.open_unified(&source_path).await?;

        // Create writer (automatically selects filesystem based on scheme)
        let writer = self.create_unified(&write_path).await?;

        Ok(CopyStream {
            reader,
            writer,
            final_path: target_path,
            temp_path: self.task.info.transfer_report.as_ref().map(|_| write_path),
        })
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
            let opts = CreateFileOptsBuilder::new()
                .create_parent(true)
                .replicas(self.task.info.job.replicas)
                .block_size(self.task.info.job.block_size)
                .storage_type(self.task.info.job.storage_type)
                .ttl_ms(self.task.info.job.ttl_ms)
                .ttl_action(self.task.info.job.ttl_action)
                .build();

            let overwrite = self.task.info.job.overwrite.unwrap_or(false);
            let writer = self.fs.create_with_opts(path, opts, overwrite).await?;
            Ok(UnifiedWriter::Cv(writer))
        } else {
            let ufs = self.get_ufs()?;
            let overwrite = self.task.info.job.overwrite.unwrap_or(false);

            if !overwrite && ufs.exists(path).await? {
                warn!(
                    "transfer task rejected because target exists and overwrite=false: {}",
                    self.log_context()
                );
                return err_box!("File exists and overwrite=false");
            }

            match ufs.create(path, overwrite).await {
                Ok(writer) => Ok(writer),
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

    async fn report_progress(&self, progress: JobTaskProgress) -> FsResult<()> {
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
            self.master_client
                .report_task(&task.job.job_id, &task.task_id, progress)
                .await
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
            .unwrap_or(8)
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
            .unwrap_or(256 * 1024 * 1024)
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

    fn segment_len(src_len: i64, streams: usize, block_size: i64) -> i64 {
        let streams = streams.max(1) as i64;
        let block_size = block_size.max(1);
        let raw = (src_len + streams - 1) / streams;
        let aligned = ((raw + block_size - 1) / block_size) * block_size;
        aligned.max(block_size)
    }

    const READ_CHUNK_BYTES: i64 = 16 * 1024 * 1024;

    async fn run_parallel(
        &self,
        source_path: &Path,
        target_path: &Path,
        initial_source: &FileStatus,
    ) -> FsResult<bool> {
        let src_len = initial_source.len;
        let streams = self.effective_streams(src_len);
        let block_size = self.task.info.job.block_size.max(1);
        let seg = Self::segment_len(src_len, streams, block_size);
        let spend = TimeSpent::new();
        let deadline_ms = LocalTime::mills() + self.task_timeout_ms;

        {
            let mut owner = self.create_unified(target_path).await?;
            owner.resize(FileAllocOpts::with_truncate(src_len)).await?;
            drop(owner);
        }

        let task_timeout_ms = self.task_timeout_ms;
        let mut handles = Vec::with_capacity(streams);
        for i in 0..streams {
            let off = i as i64 * seg;
            if off >= src_len {
                break;
            }
            let len = seg.min(src_len - off);
            let ufs = self.get_ufs()?;
            let fs = self.fs.clone();
            let src = source_path.clone();
            let dst = target_path.clone();
            let rt = self.fs.clone_runtime();
            let task = self.task.clone();
            handles.push(rt.spawn(async move {
                let mut reader = ufs.open(&src).await?;
                reader.seek(off).await?;
                let mut writer = fs.open_for_write(&dst, false).await?;
                writer.seek(off).await?;

                crate::fault_point! {
                    async,
                    name: "worker.load_task.parallel.before_segment_copy",
                    description: "After a parallel load segment opens its streams and before it copies data",
                    context: {
                        "task_id" => task.info.task_id.clone(),
                        "stream_index" => i as i64,
                        "segment_offset" => off,
                        "segment_len" => len,
                    },
                }

                let mut remaining = len;
                while remaining > 0 {
                    if task.is_cancel() {
                        let _ = writer.cancel().await;
                        return FsResult::Ok(0);
                    }
                    if LocalTime::mills() > deadline_ms {
                        let _ = writer.cancel().await;
                        return err_box!(
                            "Task {} exceed timeout {} ms (segment [{}, {}))",
                            task.info.task_id,
                            task_timeout_ms,
                            off,
                            off + len
                        );
                    }
                    let want = remaining.min(Self::READ_CHUNK_BYTES) as usize;
                    let chunk = reader.async_read(Some(want)).await?;
                    if chunk.is_empty() {
                        break;
                    }
                    remaining -= chunk.len() as i64;
                    writer.async_write(chunk).await?;
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
                FsResult::Ok(len)
            }));
        }

        let mut written: i64 = 0;
        for idx in 0..handles.len() {
            if self.task.is_cancel() {
                info!(
                    "transfer task canceled during parallel load: {}",
                    self.log_context()
                );
                Self::abort_remaining(&handles, idx);
                return self.finish_canceled().await;
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
            match (&mut handles[idx]).await {
                Ok(Ok(n)) => {
                    written += n;
                    self.update_progress(written, src_len, false).await;
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
            return self.finish_canceled().await;
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
        let ufs_mtime = initial_source.mtime;
        let attr_opts = SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime).build();
        self.fs.set_attr(target_path, attr_opts).await?;

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
            "transfer task completed (parallel x{}): {} ufs_mtime={} copied_bytes={} task_cost_ms={}",
            streams,
            self.log_context(),
            ufs_mtime,
            written,
            spend.used_ms(),
        );

        Ok(true)
    }

    fn abort_remaining(handles: &[JoinHandle<FsResult<i64>>], from: usize) {
        for h in handles.iter().skip(from) {
            h.abort();
        }
    }
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
    use super::{rename_ufs_output, LoadTaskRunner};
    use curvine_fs_api::Path;
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
    use curvine_unified_fs::UfsFileSystem;
    use std::collections::HashMap;
    use std::fs;

    const MB: i64 = 1024 * 1024;

    // Reproduce the exact planning loop run_parallel uses, so tests exercise the
    // real offset/len math (block alignment + last-segment clamp), which is the
    // most error-prone part of the fan-out.
    fn plan(src_len: i64, streams: usize, block_size: i64) -> Vec<(i64, i64)> {
        let seg = LoadTaskRunner::segment_len(src_len, streams, block_size);
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
        let seg = LoadTaskRunner::segment_len(10 * 1024 * MB, 8, 4 * MB);
        assert_eq!(
            seg % (4 * MB),
            0,
            "segment must be a multiple of block_size"
        );
        assert!(seg >= 10 * 1024 * MB / 8);
    }

    #[test]
    fn segment_len_rounds_up_to_block_multiple() {
        let seg = LoadTaskRunner::segment_len(100 * MB, 3, 4 * MB);
        assert_eq!(seg, 36 * MB);
        assert_eq!(seg % (4 * MB), 0);
    }

    #[test]
    fn segment_len_never_below_block_size() {
        let seg = LoadTaskRunner::segment_len(1, 8, 4 * MB);
        assert_eq!(seg, 4 * MB);
    }

    #[test]
    fn segment_len_handles_zero_streams_and_block() {
        assert_eq!(LoadTaskRunner::segment_len(1000, 0, 0), 1000);
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
        assert_eq!(LoadTaskRunner::stream_count(100 * MB, min, cap), 1);
        assert_eq!(LoadTaskRunner::stream_count(min - 1, min, cap), 1);
        assert_eq!(LoadTaskRunner::stream_count(512 * MB, min, cap), 2);
        assert_eq!(LoadTaskRunner::stream_count(1024 * MB, min, cap), 4);
        assert_eq!(LoadTaskRunner::stream_count(2048 * MB, min, cap), 8);
        assert_eq!(LoadTaskRunner::stream_count(200 * 1024 * MB, min, cap), 8);
    }

    #[test]
    fn stream_count_defensive_bounds() {
        assert_eq!(LoadTaskRunner::stream_count(0, 256 * MB, 8), 1);
        assert_eq!(LoadTaskRunner::stream_count(-5, 256 * MB, 8), 1);
        assert_eq!(LoadTaskRunner::stream_count(1024 * MB, 0, 8), 8);
        assert_eq!(LoadTaskRunner::stream_count(1024 * MB, 256 * MB, 0), 1);
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
}
