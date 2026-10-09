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
use crate::worker::task::task_progress_reporter::TaskProgressReporter;
use crate::worker::task::{record_report_result, record_task_outcome, TaskContext};
use curvine_client_core::block::BlockWriterRemote;
use curvine_client_core::file::{CurvineFileSystem, FsReader};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_job_client::JobMasterClient;
use curvine_model::transfer_failure_message;
use curvine_model::{
    classify_transfer_plan, parse_segment_group, BatchManifest, CreateFileOptsBuilder,
    FileAllocOpts, FileBlocks, FileStatus, JobTaskProgress, JobTaskState, SegmentGroupPlan,
    SegmentPlan, SetAttrOptsBuilder, StorageType, TransferPlanKind, TRANSFER_TEMP_PATH_MARKER,
};
use curvine_runtime::common::{LocalTime, TimeSpent};
use curvine_runtime::runtime::{JoinHandle, RpcRuntime};
use curvine_unified_fs::{UfsFileSystem, UnifiedReader, UnifiedWriter};
use log::{debug, error, info, warn};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Single idempotent-commit marker (design §7.2, minimized from five keys):
/// value is `{task_id}` on committed targets, or `{task_id}\n{target_path}`
/// on a cv→ufs export source (one source fans out to many targets, so the
/// target must ride along). task_id embeds job_id ("{job_id}_task_{n}"),
/// and run_id/attempt_id equality is implied by a same-task re-report, so a
/// key/value pair is all the retry check needs. task_id is the whole value:
/// no separator ambiguity for plain targets.
const TRANSFER_COMMIT_TASK_XATTR: &str = "curvine.transfer.task";
/// Separator between task_id and target_path inside the export-source marker.
const TRANSFER_COMMIT_MARKER_SEP: char = '\n';
/// Legacy five-key markers (job_id/run_id/task_id/source_path/target_path).
/// Recognized read-only for one release cycle so in-flight files committed by
/// the previous image still dedupe; new writes only emit the single key.
const LEGACY_TRANSFER_COMMIT_JOB_ID_XATTR: &str = "curvine.transfer.job_id";
const LEGACY_TRANSFER_COMMIT_RUN_ID_XATTR: &str = "curvine.transfer.run_id";
const LEGACY_TRANSFER_COMMIT_TASK_ID_XATTR: &str = "curvine.transfer.task_id";
const LEGACY_TRANSFER_COMMIT_SOURCE_PATH_XATTR: &str = "curvine.transfer.source_path";
const LEGACY_TRANSFER_COMMIT_TARGET_PATH_XATTR: &str = "curvine.transfer.target_path";
const WRITER_CLEANUP_TIMEOUT_MS: u64 = 10_000;

pub struct LoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    master_client: JobMasterClient,
    progress_reporter: Option<TaskProgressReporter>,
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

/// Join a file name onto a directory URI. The directory may or may not carry
/// a trailing separator; the scheme prefix (`scheme://`) is preserved.
fn join_path(dir: &Path, name: &str) -> FsResult<Path> {
    let full = dir.full_path();
    let sep = Path::SEPARATOR;
    let joined = if full.ends_with(sep) {
        format!("{}{}", full, name)
    } else {
        format!("{}{}{}", full, sep, name)
    };
    Path::from_str(&joined).map_err(|err| FsError::common(format!("invalid joined path: {}", err)))
}

/// A missing target is normal for a first batch attempt; only an existing
/// target can carry the idempotency marker checked by `run_batch`.
fn optional_batch_target_status(status: FsResult<FileStatus>) -> FsResult<Option<FileStatus>> {
    match status {
        Ok(status) => Ok(Some(status)),
        Err(FsError::FileNotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

impl LoadTaskRunner {
    pub fn new(
        task: Arc<TaskContext>,
        fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        progress_reporter: Option<TaskProgressReporter>,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) -> Self {
        let master_client = JobMasterClient::new(fs.fs_client());
        Self {
            task,
            fs,
            factory,
            master_client,
            progress_reporter,
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
        let started = Instant::now();
        let source_path = self.task.info.source_path.clone();
        let remove_task = match self.run0().await {
            Ok(remove_task) => remove_task,
            Err(e) => {
                if self.task.is_cancel() {
                    info!(
                        "transfer task stopped after cancellation request: {} err={}",
                        self.log_context(),
                        e
                    );
                    let remove_task = self.finish_canceled().await.unwrap_or_else(|err| {
                        error!(
                            "transfer task cancellation finalization failed: {} err={}",
                            self.log_context(),
                            err
                        );
                        true
                    });
                    self.record_outcome(&source_path, started, None);
                    return remove_task;
                }
                // The data replication process fails, set the status and report to the master
                error!("transfer task failed: {} err={}", self.log_context(), e);
                let failed_reason = if e.to_string().contains("exceed timeout") {
                    "timeout"
                } else {
                    "copy_error"
                };
                let progress = self.task.set_failed(transfer_failure_message(
                    match Path::from_str(&self.task.info.source_path) {
                        Ok(source) if source.is_cv() => curvine_model::TransferKind::Export,
                        _ => curvine_model::TransferKind::Load,
                    },
                    "execution",
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
                    self.record_outcome(&source_path, started, Some(failed_reason));
                    return self.task.info.transfer_report.is_none();
                }
                self.record_outcome(&source_path, started, Some(failed_reason));
                return true;
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

        self.record_outcome(
            &source_path,
            started,
            outcome_failed_reason(&self.task, remove_task),
        );
        remove_task
    }

    fn record_outcome(&self, source_path: &str, started: Instant, failed_reason: Option<&str>) {
        let progress = self.task.progress();
        let (read_us, write_us) = self.task.copy_costs();
        record_task_outcome(
            source_path,
            progress.state,
            started.elapsed().as_micros(),
            read_us as u128,
            write_us as u128,
            progress.loaded_size,
            failed_reason,
        );
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

        // Batch-manifest task (design §7.2): one task copies many small files.
        // Plan identity comes from the shared JSON-tag classifier (§8 #23).
        match classify_transfer_plan(&self.task.info.source_read_plan_json) {
            TransferPlanKind::Batch => return self.run_batch().await,
            TransferPlanKind::Segment => return self.run_segment().await,
            TransferPlanKind::SegmentGroup => return self.run_segment_group().await,
            TransferPlanKind::Plain => {}
        }

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
        let Some((copied_bytes, reader_len, read_cost_us, task_cost_us)) =
            (match self.copy_and_commit_stream(&mut stream).await {
                Ok(result) => result,
                Err(err) => {
                    self.cleanup_failed_stream(&mut stream).await;
                    return Err(err);
                }
            })
        else {
            return Ok(true);
        };

        let ufs_mtime = self.mark_committed_output(&stream).await?;

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
            "transfer task completed: {} ufs_mtime={} copied_bytes={} read_cost_us={} task_cost_us={}",
            self.log_context(),
            ufs_mtime,
            copied_bytes,
            read_cost_us,
            task_cost_us,
        );

        Ok(true)
    }

    async fn copy_and_commit_stream(
        &self,
        stream: &mut CopyStream,
    ) -> FsResult<Option<(i64, i64, u64, u64)>> {
        let mut last_progress_time = LocalTime::mills();
        let mut read_cost_us: u64 = 0;
        let mut write_cost_us: u64 = 0;
        let mut total_cost_us: u64 = 0;

        loop {
            if self.task.is_cancel() {
                info!(
                    "transfer task canceled while copying: {}",
                    self.log_context()
                );
                self.cancel_stream(stream).await?;
                return Ok(None);
            }

            // Measure read and write phases separately so metrics can answer
            // "is the bottleneck the source read or the target write?".
            // Microsecond resolution: a single chunk write is routinely
            // sub-millisecond, so per-chunk millisecond truncation would
            // round every chunk to zero and report a write cost of ~0.
            let read_spend = TimeSpent::new();
            let chunk = stream.reader.async_read(None).await?;
            read_cost_us += read_spend.used_us();

            if chunk.is_empty() {
                break;
            }
            self.task.note_first_byte();

            let write_spend = TimeSpent::new();
            stream.writer.async_write(chunk).await?;
            let chunk_write_us = write_spend.used_us();
            write_cost_us += chunk_write_us;
            total_cost_us += chunk_write_us;

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(stream.writer.pos(), stream.reader.len(), false)
                    .await;
            }

            if total_cost_us > self.task_timeout_ms.saturating_mul(1000) {
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
        }

        // The final flush happens inside complete(); count it as write cost so
        // the trailing chunk is not silently dropped from the write phase total.
        let complete_spend = TimeSpent::new();
        let complete_result = stream.writer.complete().await;
        write_cost_us += complete_spend.used_us();

        // Attribute accumulated read/write phases to the task context for
        // per-task metrics attribution at completion.
        self.task.add_read_cost(read_cost_us);
        self.task.add_write_cost(write_cost_us);

        complete_result?;
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
            read_cost_us,
            total_cost_us,
        )))
    }

    async fn cleanup_failed_stream(&self, stream: &mut CopyStream) {
        if let Err(err) = self.await_cleanup(stream.writer.cancel()).await {
            warn!(
                "cancel failed transfer writer after copy error: {} err={}",
                self.log_context(),
                err
            );
        }
        let Some(temp_path) = &stream.temp_path else {
            return;
        };
        if let Err(err) = self.await_cleanup(self.delete_temp_output(temp_path)).await {
            warn!(
                "delete failed transfer temp output {} failed: {} err={}",
                temp_path.full_path(),
                self.log_context(),
                err
            );
        }
    }

    async fn cancel_stream(&self, stream: &mut CopyStream) -> FsResult<bool> {
        if let Err(err) = self.await_cleanup(stream.writer.cancel()).await {
            warn!(
                "cancel transfer writer failed: {} temp={:?} err={}",
                self.log_context(),
                stream.temp_path,
                err
            );
        }

        if let Some(temp_path) = &stream.temp_path {
            if let Err(err) = self.await_cleanup(self.delete_temp_output(temp_path)).await {
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

    /// Execute a multi-file batch task (design §7.2): iterate manifest entries,
    /// copy each file with the normal single-file path (idempotent commit),
    /// aggregate progress as loaded/total over the whole batch, and fail the
    /// task with the offending file named in the error message.
    async fn run_batch(&self) -> FsResult<bool> {
        let manifest: BatchManifest = serde_json::from_str(&self.task.info.source_read_plan_json)
            .map_err(|err| {
            FsError::common(format!(
                "Invalid batch manifest for task {}: {}",
                self.task.info.task_id, err
            ))
        })?;
        let source_dir = Path::from_str(&self.task.info.source_path)?;
        let target_dir = Path::from_str(&self.task.info.target_path)?;
        let total: i64 = manifest.batch.iter().map(|entry| entry.size).sum();
        let mut committed = 0i64;
        let mut copied_count = 0usize;

        for entry in &manifest.batch {
            if self.task.is_cancel() {
                info!("batch transfer task canceled: {}", self.log_context());
                return self.finish_canceled().await;
            }
            let source = join_path(&source_dir, &entry.rel_path)?;
            let target = join_path(&target_dir, &entry.rel_path)?;
            // Idempotent retry: a file already committed by a previous attempt
            // of this batch must not be copied again (review Required).
            // Commit markers live on CV targets only; UFS targets always copy.
            if target.is_cv() {
                if let Some(target_status) =
                    optional_batch_target_status(self.fs.get_status(&target).await)?
                {
                    let source_status = self.open_unified(&source).await?.status().clone();
                    if self.is_committed_transfer_output(&target_status, &source_status) {
                        committed += entry.size;
                        copied_count += 1;
                        continue;
                    }
                }
            }
            let mut stream = match self.create_stream_for(&source, &target).await {
                Ok(stream) => stream,
                Err(err) => {
                    return err_box!(
                        "batch task {} file {}: {}",
                        self.task.info.task_id,
                        entry.rel_path,
                        err
                    );
                }
            };
            if self.task.is_cancel() {
                info!("batch transfer task canceled: {}", self.log_context());
                return self.cancel_stream(&mut stream).await;
            }
            let Some((copied_bytes, reader_len, _, _)) =
                (match self.copy_and_commit_stream(&mut stream).await {
                    Ok(result) => result,
                    Err(err) => {
                        self.cleanup_failed_stream(&mut stream).await;
                        return err_box!(
                            "batch task {} file {}: {}",
                            self.task.info.task_id,
                            entry.rel_path,
                            err
                        );
                    }
                })
            else {
                return Ok(true);
            };
            // Commit markers are per-task (idempotent retry of the batch relies
            // on them, design §7.2). Mark before aggregating progress so a
            // marker failure fails this batch attempt like a copy failure.
            if let Err(err) = self.mark_committed_output(&stream).await {
                self.cleanup_failed_stream(&mut stream).await;
                return err_box!(
                    "batch task {} file {}: {}",
                    self.task.info.task_id,
                    entry.rel_path,
                    err
                );
            }
            committed += copied_bytes.max(reader_len).max(entry.size);
            copied_count += 1;
            // Report progress after each file so the batch keeps renewing its
            // stale deadline even when individual files are slow.
            self.update_progress(committed, total, false).await;
        }

        let progress = self.task.update_progress(committed, total, true);
        if let Err(err) = self.report_progress(progress).await {
            if self.task.info.transfer_report.is_some() {
                warn!(
                    "final batch transfer task report failed, keep task for scheduler probe: {} err={}",
                    self.log_context(),
                    err
                );
                return Ok(false);
            }
            return Err(err);
        }
        info!(
            "batch transfer task completed: {} files={} bytes={}",
            self.log_context(),
            copied_count,
            committed
        );
        Ok(true)
    }

    async fn create_stream(&self) -> FsResult<CopyStream> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        self.create_stream_for(&source_path, &target_path).await
    }

    /// Open source/target for an explicit pair (single-file task uses task
    /// fields; batch tasks override per manifest entry).
    async fn create_stream_for(
        &self,
        source_path: &Path,
        target_path: &Path,
    ) -> FsResult<CopyStream> {
        let write_path = self.transfer_temp_path(target_path)?;

        // Create reader (automatically selects filesystem based on scheme)
        let reader = self.open_unified(source_path).await?;

        // Create writer (automatically selects filesystem based on scheme)
        let writer = self.create_unified(&write_path).await?;

        Ok(CopyStream {
            reader,
            writer,
            final_path: target_path.clone(),
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

    /// Execute a single-file segment task (design §14): copy the byte range
    /// [offset, offset+length) of the source to the same byte range of the
    /// target. The source is read via a seeked range read; the target is
    /// opened for positional write (created on first segment) and seeked to
    /// the segment offset. No temp+rename commit here - segments write
    /// directly to the shared target file and the job's convergence protocol
    /// completes it once all segments are done (design §14, T4).
    async fn run_segment(&self) -> FsResult<bool> {
        // plan is wrapped as {"segment": {offset, length, total_len}} by the planner.
        let plan: SegmentPlan = serde_json::from_str(&self.task.info.source_read_plan_json)
            .and_then(|value: serde_json::Value| {
                serde_json::from_value(value.get("segment").cloned().unwrap_or_default())
            })
            .map_err(|err| {
                FsError::common(format!(
                    "Invalid segment plan for task {}: {}",
                    self.task.info.task_id, err
                ))
            })?;
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        // Progress throttle aligned to the configured report interval (the
        // previous hardcoded 10s window bypassed progress_interval_ms and could
        // let the scheduler judge a long segment stale - review Required).
        let deadline_ms = LocalTime::mills().saturating_add(self.task_timeout_ms);
        let mut last_progress_time = LocalTime::mills();

        // Ensure the target exists with the full block layout (first segment
        // creates and pre-allocates; concurrent segments reuse it). Reuse
        // the verified layout when ensure already queried it.
        let blocks = match self
            .ensure_segment_target(&target_path, plan.total_len, deadline_ms)
            .await?
        {
            Some(blocks) => blocks,
            None => {
                self.await_group_deadline(deadline_ms, self.fs.get_block_locations(&target_path))
                    .await?
            }
        };
        // Locate this segment's block in the target layout.
        let block_size = blocks.status.block_size.max(1);
        // Use the planner-computed block index (aligned to the job's block
        // size), not offset/block_size here: the live layout may report a
        // different block_size than the planner assumed during a rolling
        // upgrade or a non-default block size.
        let block_index = plan.block_index;

        // Open source and seek to the segment start.
        let mut reader = self
            .await_group_deadline(deadline_ms, self.open_unified(&source_path))
            .await?;

        self.await_group_deadline(deadline_ms, reader.seek(plan.offset))
            .await?;
        // Hold a writer record for the data-copy window so concurrent
        // readers see is_complete=false. Every path below closes it.
        let (mut writer, write_guard) = self
            .open_segment_writer(&target_path, &blocks, block_size, block_index, deadline_ms)
            .await?;
        let mut writer_completed = false;
        let copy_result: FsResult<Option<i64>> = async {
            let mut remaining = plan.length;
            let mut written = 0i64;
            while remaining > 0 {
                if self.task.is_cancel() {
                    info!("segment transfer task canceled: {}", self.log_context());
                    return Ok(None);
                }
                let chunk = self
                    .await_group_deadline(
                        deadline_ms,
                        reader.async_read(Some(remaining.min(Self::READ_CHUNK_BYTES) as usize)),
                    )
                    .await?;
                if chunk.is_empty() || chunk.len() as i64 > remaining {
                    return err_box!(
                        "segment task {} short read: {} bytes missing",
                        self.task.info.task_id,
                        remaining
                    );
                }
                self.task.note_first_byte();
                let chunk_len = chunk.len() as i64;
                self.await_group_deadline(deadline_ms, writer.write(chunk))
                    .await?;
                remaining -= chunk_len;
                written += chunk_len;
                if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                    last_progress_time = LocalTime::mills();
                    self.update_progress(written, plan.length, false).await;
                }
            }
            self.await_group_deadline(deadline_ms, writer.flush())
                .await?;
            self.await_group_deadline(deadline_ms, writer.complete())
                .await?;
            writer_completed = true;
            self.await_group_deadline(deadline_ms, reader.complete())
                .await?;
            Ok(Some(written))
        }
        .await;
        if !writer_completed {
            if let Err(err) = self.await_cleanup(writer.cancel()).await {
                warn!(
                    "cancel segment writer failed: {} err={}",
                    self.log_context(),
                    err
                );
            }
        }
        let close_result = self
            .await_cleanup(self.close_writer_record_as(&target_path, &write_guard))
            .await;
        let written = match copy_result {
            Ok(Some(written)) => {
                close_result?;
                written
            }
            Ok(None) => {
                if let Err(err) = close_result {
                    warn!(
                        "close canceled segment writer record failed: {} err={}",
                        self.log_context(),
                        err
                    );
                }
                return self.finish_canceled().await;
            }
            Err(err) => {
                if let Err(close_err) = close_result {
                    warn!(
                        "close failed segment writer record failed: {} err={}",
                        self.log_context(),
                        close_err
                    );
                }
                return Err(err);
            }
        };

        let progress = self.task.update_progress(written, plan.length, true);
        if let Err(err) = self.report_progress(progress).await {
            if self.task.info.transfer_report.is_some() {
                warn!(
                    "final segment transfer task report failed, keep task for scheduler probe: {} err={}",
                    self.log_context(),
                    err
                );
                return Ok(false);
            }
            return Err(err);
        }
        info!(
            "segment transfer task completed: {} offset={} length={}",
            self.log_context(),
            plan.offset,
            plan.length
        );
        Ok(true)
    }

    async fn run_segment_group(&self) -> FsResult<bool> {
        let plan = parse_segment_group(&self.task.info.source_read_plan_json)?;
        // A group task skips source bytes on retry, so it must be able to
        // re-identify the source it was planned against. Either an immutable
        // version or a HEAD `(len, mtime)` pair is accepted; a manifest with
        // neither is refused instead of resuming blind.
        if !plan.has_identity() {
            return err_box!(
                "segment group task {} is disabled: no source identity to validate",
                self.task.info.task_id
            );
        }
        if self.task.info.job.storage_type == StorageType::SpdkDisk {
            return err_box!(
                "segment group task {} unsupported on SPDK target",
                self.task.info.task_id
            );
        }
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        let deadline_ms = LocalTime::mills().saturating_add(self.task_timeout_ms);
        self.await_group_deadline(deadline_ms, self.check_group_source(&source_path, &plan))
            .await?;
        let blocks = match self
            .ensure_segment_target(&target_path, plan.total_len, deadline_ms)
            .await?
        {
            Some(blocks) => blocks,
            None => {
                self.await_group_deadline(deadline_ms, self.fs.get_block_locations(&target_path))
                    .await?
            }
        };
        let block_size = blocks.status.block_size.max(1);
        if block_size != plan.segment_bytes || blocks.status.len != plan.total_len {
            return err_box!(
                "segment group task {} target layout changed: block_size={} expected={} len={} expected={}",
                self.task.info.task_id,
                block_size,
                plan.segment_bytes,
                blocks.status.len,
                plan.total_len
            );
        }
        let resume = self
            .task
            .info
            .transfer_report
            .as_ref()
            .map(|report| report.resume_loaded_size)
            .unwrap_or(0);
        let start_resume = plan.resume_floor(resume.clamp(0, plan.group_len()));
        let segments = plan.iter_from(start_resume);
        let group_total = plan.group_len();
        self.task.update_progress(start_resume, group_total, false);
        let mut committed = start_resume;
        let mut last_progress_ms = LocalTime::mills();
        let mut reader = self
            .await_group_deadline(deadline_ms, self.open_unified(&source_path))
            .await?;
        for (offset, length, block_index) in segments {
            if !self
                .copy_one_segment(
                    &mut reader,
                    &target_path,
                    &blocks,
                    block_size,
                    offset,
                    length,
                    block_index,
                    committed,
                    group_total,
                    &mut last_progress_ms,
                    deadline_ms,
                )
                .await?
            {
                return Ok(true);
            }
            committed = committed.saturating_add(length);
            self.update_progress(committed, group_total, false).await;
            info!(
                "segment group progress: {} offset={} length={} block={} committed={} total={}",
                self.log_context(),
                offset,
                length,
                block_index,
                committed,
                group_total,
            );
        }
        self.await_group_deadline(deadline_ms, reader.complete())
            .await?;
        self.await_group_deadline(deadline_ms, self.check_group_source(&source_path, &plan))
            .await?;
        let progress = self.task.update_progress(committed, group_total, true);
        if let Err(err) = self.report_progress(progress).await {
            if self.task.info.transfer_report.is_some() {
                warn!(
                    "final segment group transfer task report failed, keep task for scheduler probe: {} err={}",
                    self.log_context(),
                    err
                );
                return Ok(false);
            }
            return Err(err);
        }
        info!(
            "segment group transfer task completed: {} segments_committed_bytes={} total={}",
            self.log_context(),
            committed,
            group_total,
        );
        Ok(true)
    }

    async fn check_group_source(
        &self,
        source_path: &Path,
        plan: &SegmentGroupPlan,
    ) -> FsResult<()> {
        // Immutable identity first (design D0.4): a manifest must pin either
        // a version or the HEAD stat pair before any byte may be skipped.
        if !plan.has_identity() {
            return err_box!(
                "segment group task {} has no source identity; refusing resume",
                self.task.info.task_id
            );
        }
        if source_path.is_cv() {
            // The planner only mints groups for UFS sources; a CV-sourced
            // group manifest cannot be identity-checked, so fail closed.
            return err_box!(
                "segment group task {} source identity is unverifiable for CV source",
                self.task.info.task_id
            );
        }
        let ufs = self.get_ufs()?;
        // One HEAD observation, the same call the planner fingerprinted with:
        // an immutable version must still match when the manifest pins one,
        // otherwise the exact `(len, mtime)` pair from that single observation
        // re-identifies the source. Both sides read HEAD, so an unchanged
        // object compares equal (measured on OSS-HDFS: `listDir` and
        // `getFileStatus` report the same millisecond mtime).
        let (len, mtime, live) = crate::common::ufs_object_fingerprint(&ufs, source_path).await;
        if let Err(mismatch) = plan.verify_observation(len, mtime, &live) {
            return err_box!(
                "segment group task {} source changed: {:?} len={} mtime={} version_present={}",
                self.task.info.task_id,
                mismatch,
                len,
                mtime,
                !live.is_empty()
            );
        }
        Ok(())
    }

    async fn open_segment_writer(
        &self,
        target_path: &Path,
        blocks: &FileBlocks,
        block_size: i64,
        block_index: i64,
        deadline_ms: u64,
    ) -> FsResult<(BlockWriterRemote, String)> {
        let located = blocks.block_locs.get(block_index as usize).ok_or_else(|| {
            FsError::common(format!(
                "segment task {} block {} not found in target layout (blocks={})",
                self.task.info.task_id,
                block_index,
                blocks.block_locs.len()
            ))
        })?;
        let located = if located.locs.is_empty() {
            self.await_group_deadline(
                deadline_ms,
                self.fs.fs_client().assign_worker_by_id(
                    target_path,
                    blocks.status.id,
                    located.block.clone(),
                ),
            )
            .await?
        } else {
            located.clone()
        };
        let worker = located.locs.first().ok_or_else(|| {
            FsError::common(format!(
                "segment task {} block {} has no assigned worker",
                self.task.info.task_id, block_index
            ))
        })?;
        let write_guard = self.next_writer_name();
        let _ = self
            .await_group_deadline(
                deadline_ms,
                self.fs
                    .open_existing_for_write_as(target_path, &write_guard),
            )
            .await?;
        let writer = match self
            .await_group_deadline(
                deadline_ms,
                BlockWriterRemote::new(
                    self.fs.fs_context().as_ref(),
                    located.block.clone(),
                    worker.clone(),
                    0,
                    block_size,
                ),
            )
            .await
        {
            Ok(writer) => writer,
            Err(err) => {
                if let Err(close_err) = self
                    .await_cleanup(self.close_writer_record_as(target_path, &write_guard))
                    .await
                {
                    warn!(
                        "close segment writer record after open failure failed: {} err={}",
                        self.log_context(),
                        close_err
                    );
                }
                return Err(err);
            }
        };
        Ok((writer, write_guard))
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_one_segment(
        &self,
        reader: &mut UnifiedReader,
        target_path: &Path,
        blocks: &FileBlocks,
        block_size: i64,
        offset: i64,
        length: i64,
        block_index: i64,
        committed_base: i64,
        progress_total: i64,
        last_progress_ms: &mut u64,
        deadline_ms: u64,
    ) -> FsResult<bool> {
        let (mut writer, write_guard) = self
            .open_segment_writer(target_path, blocks, block_size, block_index, deadline_ms)
            .await?;
        // Keep exactly one read and one write in flight. This chunk pipeline
        // overlaps the source and target RPCs while holding at most two chunks.
        let result = async {
            self.await_group_deadline(deadline_ms, reader.seek(offset))
                .await?;
            if self.task.is_cancel() {
                info!("segment transfer task canceled: {}", self.log_context());
                return Ok(false);
            }
            if LocalTime::mills() > deadline_ms {
                return err_box!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id,
                    self.task_timeout_ms
                );
            }
            let mut remaining = length;
            let mut chunk = self
                .await_group_deadline(
                    deadline_ms,
                    reader.async_read(Some(remaining.min(Self::READ_CHUNK_BYTES) as usize)),
                )
                .await?;
            while remaining > 0 {
                if self.task.is_cancel() {
                    info!("segment transfer task canceled: {}", self.log_context());
                    return Ok(false);
                }
                if LocalTime::mills() > deadline_ms {
                    return err_box!(
                        "Task {} exceed timeout {} ms",
                        self.task.info.task_id,
                        self.task_timeout_ms
                    );
                }
                if chunk.is_empty() || chunk.len() as i64 > remaining {
                    return err_box!(
                        "segment task {} short read: {} bytes missing",
                        self.task.info.task_id,
                        remaining
                    );
                }
                self.task.note_first_byte();
                let chunk_len = chunk.len() as i64;
                let next_remaining = remaining - chunk_len;
                if next_remaining == 0 {
                    self.await_group_deadline(deadline_ms, writer.write(chunk))
                        .await?;
                    break;
                } else {
                    let (next_chunk, _) = futures::try_join!(
                        self.await_group_deadline(
                            deadline_ms,
                            reader.async_read(Some(
                                next_remaining.min(Self::READ_CHUNK_BYTES) as usize
                            )),
                        ),
                        self.await_group_deadline(deadline_ms, writer.write(chunk)),
                    )?;
                    chunk = next_chunk;
                }
                remaining = next_remaining;
                if LocalTime::mills() > *last_progress_ms + self.progress_interval_ms {
                    *last_progress_ms = LocalTime::mills();
                    // A group heartbeat renews liveness but must not make a
                    // partially written segment resumable. Only the prefix
                    // before this segment has passed flush + complete.
                    self.update_progress(committed_base, progress_total, false)
                        .await;
                }
            }
            self.await_group_deadline(deadline_ms, writer.flush())
                .await?;
            self.await_group_deadline(deadline_ms, writer.complete())
                .await?;
            Ok(true)
        }
        .await;
        if !matches!(result, Ok(true)) {
            if let Err(err) = self.await_cleanup(writer.cancel()).await {
                warn!(
                    "cancel group segment writer failed: {} err={}",
                    self.log_context(),
                    err
                );
            }
        }
        let close_result = self
            .await_cleanup(self.close_writer_record_as(target_path, &write_guard))
            .await;
        match result {
            Ok(false) => {
                close_result?;
                self.finish_canceled().await.map(|_| false)
            }
            Ok(value) => {
                close_result?;
                Ok(value)
            }
            Err(err) => {
                if let Err(close_err) = close_result {
                    warn!(
                        "close failed group segment writer record failed: {} err={}",
                        self.log_context(),
                        close_err
                    );
                }
                Err(err)
            }
        }
    }

    /// Open an existing CV/UFS target for positional write, or create it on
    /// Ensure the segment target exists with the full block layout: the
    /// first segment creates and pre-allocates the whole file (resize), so
    /// every concurrent segment finds each block's layout (and its assigned
    /// worker) via get_block_locations. Concurrent creates race; a
    /// FileAlreadyExists on create means another segment already created it.
    async fn ensure_segment_target(
        &self,
        target_path: &Path,
        total_len: i64,
        deadline_ms: u64,
    ) -> FsResult<Option<FileBlocks>> {
        if target_path.is_cv() {
            // Existence check via get_status (no writer record); open is
            // only used where a handle is actually needed below.
            match self
                .await_group_deadline(deadline_ms, self.fs.get_status(target_path))
                .await
            {
                // The file may have been created by a concurrent segment
                // whose resize has not landed yet (create+resize are not
                // atomic). Verify the pre-allocation before treating the
                // target as ready instead of trusting existence alone.
                Ok(_) => self
                    .ensure_segment_size(target_path, total_len, deadline_ms)
                    .await
                    .map(Some),
                Err(FsError::FileNotFound(_)) => {
                    let opts = CreateFileOptsBuilder::new()
                        .create_parent(true)
                        .replicas(self.task.info.job.replicas)
                        .block_size(self.task.info.job.block_size)
                        .storage_type(self.task.info.job.storage_type)
                        .ttl_ms(self.task.info.job.ttl_ms)
                        .ttl_action(self.task.info.job.ttl_action)
                        .build();
                    match self
                        .await_group_deadline(
                            deadline_ms,
                            self.fs.create_with_opts(target_path, opts, false),
                        )
                        .await
                    {
                        // Keep the creating writer record open on success so
                        // the preallocated target remains incomplete until
                        // scheduler convergence performs the final complete.
                        Ok(mut writer) => {
                            let result = async {
                                if total_len > 0 {
                                    self.await_group_deadline(
                                        deadline_ms,
                                        writer.resize(FileAllocOpts::with_truncate(total_len)),
                                    )
                                    .await?;
                                }
                                self.await_group_deadline(deadline_ms, writer.flush()).await
                            }
                            .await;
                            match result {
                                Ok(()) => Ok(None),
                                Err(err) => {
                                    if let Err(cancel_err) =
                                        self.await_cleanup(writer.cancel()).await
                                    {
                                        warn!(
                                            "cancel newly created segment target after resize failure failed: {} err={}",
                                            self.log_context(),
                                            cancel_err
                                        );
                                    }
                                    // This target was created by this call;
                                    // remove it rather than leaving a partial
                                    // file that could appear complete after
                                    // closing its only writer record.
                                    if let Err(delete_err) =
                                        self.await_cleanup(self.fs.delete(target_path, false)).await
                                    {
                                        warn!(
                                            "delete newly created segment target after resize failure failed: {} err={}",
                                            self.log_context(),
                                            delete_err
                                        );
                                    }
                                    Err(err)
                                }
                            }
                        }
                        // Concurrent create race: another segment created the
                        // file. create+resize are not atomic, so verify the
                        // resize actually landed before treating the target as
                        // ready; otherwise re-apply it (review Required).
                        Err(FsError::FileAlreadyExists(_)) => self
                            .ensure_segment_size(target_path, total_len, deadline_ms)
                            .await
                            .map(Some),
                        Err(err) => Err(err),
                    }
                }
                Err(err) => Err(err),
            }
        } else {
            // Non-CV target (unusual for segment tasks, but keep parity).
            self.await_group_deadline(deadline_ms, self.create_unified(target_path))
                .await
                .map(|_| None)
        }
    }

    /// Release the writer record registered under an explicit paired name.
    async fn close_writer_record_as(&self, target_path: &Path, client_name: &str) -> FsResult<()> {
        self.fs.close_file_as(target_path, client_name).await
    }

    async fn await_cleanup<T>(&self, operation: impl Future<Output = FsResult<T>>) -> FsResult<T> {
        let timeout_ms = self.task_timeout_ms.clamp(1, WRITER_CLEANUP_TIMEOUT_MS);
        tokio::time::timeout(Duration::from_millis(timeout_ms), operation)
            .await
            .map_err(|_| {
                FsError::common(format!(
                    "Task {} writer cleanup exceeded {} ms",
                    self.task.info.task_id, timeout_ms
                ))
            })?
    }

    async fn await_group_deadline<T>(
        &self,
        deadline_ms: u64,
        operation: impl Future<Output = FsResult<T>>,
    ) -> FsResult<T> {
        let now_ms = LocalTime::mills();
        let remaining_ms = deadline_ms.saturating_sub(now_ms);
        if remaining_ms == 0 {
            return err_box!(
                "Task {} exceed timeout {} ms",
                self.task.info.task_id,
                self.task_timeout_ms
            );
        }
        tokio::time::timeout(Duration::from_millis(remaining_ms), operation)
            .await
            .map_err(|_| {
                FsError::common(format!(
                    "Task {} exceed timeout {} ms",
                    self.task.info.task_id, self.task_timeout_ms
                ))
            })?
    }

    /// Unique writer identity for one paired open/close. Sharing one name
    /// across concurrent opens breaks release accounting (the master tracks
    /// writers by name).
    fn next_writer_name(&self) -> String {
        static WRITER_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        format!(
            "{}-seg{}",
            self.fs.fs_context().clone_client_name(),
            WRITER_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    /// Verify the target pre-allocation landed and re-apply the resize when
    /// it has not. Both length and block layout must be ready: a leftover
    /// or concurrently-created target can have the right length but an
    /// empty layout (staged blocks discarded on cancel, or the creator's
    /// resize not yet visible). Truncate-to-length is idempotent, so
    /// concurrent segments racing here converge safely.
    /// Returns the verified layout so callers can reuse it instead of
    /// querying block locations a second time. (PR #214: layout reuse.)
    async fn ensure_segment_size(
        &self,
        target_path: &Path,
        total_len: i64,
        deadline_ms: u64,
    ) -> FsResult<FileBlocks> {
        let blocks = self
            .await_group_deadline(deadline_ms, self.fs.get_block_locations(target_path))
            .await?;
        if blocks.status.len >= total_len && !blocks.block_locs.is_empty() {
            return Ok(blocks);
        }
        let writer_name = self.next_writer_name();
        let mut writer = self
            .await_group_deadline(
                deadline_ms,
                self.fs
                    .open_existing_for_write_as(target_path, &writer_name),
            )
            .await?;
        if total_len > 0 {
            let result = async {
                self.await_group_deadline(
                    deadline_ms,
                    writer.resize(FileAllocOpts::with_truncate(total_len)),
                )
                .await?;
                self.await_group_deadline(deadline_ms, writer.flush()).await
            }
            .await;
            if result.is_err() {
                if let Err(cancel_err) = self.await_cleanup(writer.cancel()).await {
                    warn!(
                        "cancel failed segment resize writer after error: {} err={}",
                        self.log_context(),
                        cancel_err
                    );
                }
            }
            let close_result = self
                .await_cleanup(self.close_writer_record_as(target_path, &writer_name))
                .await;
            match result {
                Ok(()) => close_result?,
                Err(err) => {
                    if let Err(close_err) = close_result {
                        warn!(
                            "close failed segment resize writer record failed: {} err={}",
                            self.log_context(),
                            close_err
                        );
                    }
                    return Err(err);
                }
            }
        } else {
            self.await_cleanup(self.close_writer_record_as(target_path, &writer_name))
                .await?;
        }
        self.await_group_deadline(deadline_ms, self.fs.get_block_locations(target_path))
            .await
    }

    /// Stamp the single-key commit marker on the committed target (idempotent
    /// retry relies on it, design §7.2). Returns the target's UFS mtime. For
    /// batch tasks the marker is per-task (task_id is the batch-level value),
    /// which stays self-consistent across retries of the same batch.
    async fn mark_committed_output(&self, stream: &CopyStream) -> FsResult<i64> {
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
            if let Some(value) = self.commit_marker_value(None) {
                attr_builder = attr_builder.add_x_attr(TRANSFER_COMMIT_TASK_XATTR, value);
            }
        }
        let attr_opts = attr_builder.build();
        self.fs.set_attr(cv_path, attr_opts).await?;
        Ok(ufs_mtime)
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

    /// Marker value for this task: `{task_id}` for committed targets, or
    /// `{task_id}\n{target_path}` when stamping a cv→ufs export source that
    /// fans out to many targets. None when the task carries no transfer
    /// report (marker protocol disabled).
    fn commit_marker_value(&self, export_target: Option<&Path>) -> Option<Vec<u8>> {
        let has_report = self.task.info.transfer_report.is_some();
        commit_marker_value_for(&self.task.info.task_id, has_report, export_target)
    }

    /// True when the file carries this task's single-key commit marker, or
    /// (transition window) the legacy five-key set from the previous image.
    fn has_transfer_commit_marker(&self, status: &FileStatus, target_path: Option<&Path>) -> bool {
        if let Some(value) = self.commit_marker_value(target_path) {
            if xattr_equals(status, TRANSFER_COMMIT_TASK_XATTR, &value) {
                return true;
            }
        }
        self.has_legacy_transfer_commit_marker(status, target_path)
    }

    fn has_legacy_transfer_commit_marker(
        &self,
        status: &FileStatus,
        target_path: Option<&Path>,
    ) -> bool {
        let Some(report) = &self.task.info.transfer_report else {
            return false;
        };
        xattr_equals(
            status,
            LEGACY_TRANSFER_COMMIT_JOB_ID_XATTR,
            self.task.info.job.job_id.as_bytes(),
        ) && xattr_equals(
            status,
            LEGACY_TRANSFER_COMMIT_RUN_ID_XATTR,
            report.run_id.to_string().as_bytes(),
        ) && xattr_equals(
            status,
            LEGACY_TRANSFER_COMMIT_TASK_ID_XATTR,
            self.task.info.task_id.as_bytes(),
        ) && xattr_equals(
            status,
            LEGACY_TRANSFER_COMMIT_SOURCE_PATH_XATTR,
            self.task.info.source_path.as_bytes(),
        ) && target_path.is_none_or(|path| {
            xattr_equals(
                status,
                LEGACY_TRANSFER_COMMIT_TARGET_PATH_XATTR,
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
        let Some(value) = self.commit_marker_value(Some(target_path)) else {
            return Ok(());
        };
        self.fs
            .set_attr(
                &source_path,
                SetAttrOptsBuilder::new()
                    .add_x_attr(TRANSFER_COMMIT_TASK_XATTR, value)
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
            let Some(reporter) = &self.progress_reporter else {
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
            if progress.state.is_finish() {
                let result = reporter.report_terminal(progress).await;
                record_report_result(&result);
                return result;
            }
            reporter.enqueue_progress(progress);
            return Ok(());
        }

        let result = self
            .master_client
            .report_task(&task.job.job_id, &task.task_id, progress)
            .await;
        record_report_result(&result);
        result
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
                    // Per-chunk microsecond timing, matching the serial path:
                    // sub-millisecond chunks would round to zero in ms.
                    let read_spend = TimeSpent::new();
                    let chunk = reader.async_read(Some(want)).await?;
                    task.add_read_cost(read_spend.used_us());
                    if chunk.is_empty() {
                        break;
                    }
                    task.note_first_byte();
                    remaining -= chunk.len() as i64;
                    let write_spend = TimeSpent::new();
                    writer.async_write(chunk).await?;
                    task.add_write_cost(write_spend.used_us());
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
                let complete_spend = TimeSpent::new();
                writer.complete().await?;
                task.add_write_cost(complete_spend.used_us());
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

fn commit_marker_value_for(
    task_id: &str,
    has_report: bool,
    export_target: Option<&Path>,
) -> Option<Vec<u8>> {
    if !has_report {
        return None;
    }
    Some(match export_target {
        None => task_id.as_bytes().to_vec(),
        Some(path) => {
            let mut value = task_id.as_bytes().to_vec();
            value.push(TRANSFER_COMMIT_MARKER_SEP as u8);
            value.extend_from_slice(path.full_path().as_bytes());
            value
        }
    })
}

fn xattr_equals(status: &FileStatus, key: &str, expected: &[u8]) -> bool {
    status
        .x_attr
        .get(key)
        .map(|actual| actual.as_slice() == expected)
        .unwrap_or(false)
}

fn outcome_failed_reason(task: &TaskContext, _remove_task: bool) -> Option<&'static str> {
    let progress = task.progress();
    if progress.state == JobTaskState::Failed {
        if progress.message.contains("exceed timeout") {
            return Some("timeout");
        }
        return Some("copy_error");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        outcome_failed_reason, rename_ufs_output, LoadTaskRunner, TaskContext,
        TRANSFER_COMMIT_MARKER_SEP, TRANSFER_COMMIT_TASK_XATTR,
    };
    use curvine_fs_api::Path;
    use curvine_model::{
        JobSourceType, JobTaskState, LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TtlAction,
        WorkerAddress,
    };
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
    use curvine_unified_fs::UfsFileSystem;
    use std::collections::HashMap;
    use std::fs;

    const MB: i64 = 1024 * 1024;

    fn test_task_context() -> TaskContext {
        let job = LoadJobInfo {
            job_id: "job_a".to_string(),
            source_path: "ufs://bucket/file".to_string(),
            target_path: "/cv/file".to_string(),
            block_size: 4096,
            replicas: 1,
            storage_type: StorageType::default(),
            ttl_ms: 0,
            ttl_action: TtlAction::default(),
            mount_info: MountInfo::default(),
            create_time: 0,
            overwrite: None,
            source_type: JobSourceType::default(),
        };
        TaskContext::new(LoadTaskInfo {
            job,
            task_id: "task_a".to_string(),
            worker: WorkerAddress::default(),
            source_path: "ufs://bucket/file".to_string(),
            target_path: "/cv/file".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            estimated_size: 0,
            transfer_report: None,
        })
    }

    #[test]
    fn outcome_failed_reason_keeps_inflight_loading_unlabeled() {
        let task = test_task_context();
        task.update_state(JobTaskState::Loading, "copying");

        assert_eq!(outcome_failed_reason(&task, false), None);
    }

    #[test]
    fn outcome_failed_reason_classifies_copy_failures() {
        let task = test_task_context();
        task.set_failed("Task task_a exceed timeout 1000 ms");

        assert_eq!(outcome_failed_reason(&task, true), Some("timeout"));

        let task = test_task_context();
        task.set_failed("copy failed");

        assert_eq!(outcome_failed_reason(&task, true), Some("copy_error"));
    }

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

    #[test]
    fn commit_marker_is_single_compact_value() {
        use crate::worker::task::load_task_runner::commit_marker_value_for as marker;
        let target = Path::from_str("/cv/out").unwrap();

        // No transfer report: no marker at all.
        assert!(marker("job_a_task_0", false, None).is_none());

        // Committed target: bare task_id (job_id rides inside the prefix).
        assert_eq!(
            marker("job_a_task_0", true, None).unwrap(),
            b"job_a_task_0".to_vec()
        );

        // Export source: task_id + target path, one key instead of five.
        assert_eq!(
            marker("job_a_task_0", true, Some(&target)).unwrap(),
            format!("job_a_task_0{}/cv/out", TRANSFER_COMMIT_MARKER_SEP).into_bytes()
        );

        // Round-trip: the marker must be recognized by an exact-match check
        // against the same key (xattr_equals semantics).
        let value = marker("job_a_task_0", true, Some(&target)).unwrap();
        assert_eq!(
            value
                .split(|b| *b == TRANSFER_COMMIT_MARKER_SEP as u8)
                .count(),
            2
        );
        assert!(TRANSFER_COMMIT_TASK_XATTR.starts_with("curvine.transfer."));
    }
}

#[cfg(test)]
#[path = "tests/load_task_runner_test.rs"]
mod load_task_runner_test;
