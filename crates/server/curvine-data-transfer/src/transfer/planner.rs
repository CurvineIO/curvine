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
use crate::transfer::{job_mount_snapshot_async, ClusterMetadataCache, TransferMetrics};
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClientConf;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_model::ListOptions;
use curvine_model::{
    BatchManifest, BatchManifestEntry, FileStatus, LoadJobInfo, MountInfo, SegmentPlan,
    TransferCommand, TransferJobRecord, TransferKind, TransferProgress, TransferTaskRecord,
    TransferTaskState,
};
use curvine_runtime::common::LocalTime;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const CV_LIST_PAGE_SIZE: usize = 1_000;

pub struct PlannedTransfer {
    pub job_info: LoadJobInfo,
    pub tasks: Vec<TransferTaskRecord>,
    pub total_size: i64,
    pub skipped_files: usize,
    pub skipped_size: i64,
    pub cv_metadata_epoch: Option<u64>,
}

/// Small-file batching thresholds (design §7.1): a UFS directory-source task
/// carries at most this many files or this many bytes before splitting.
/// Batching is default-on; no configuration toggle (M/W/T ship together).
pub const BATCH_MAX_FILES: usize = 512;
pub const BATCH_TARGET_BYTES: i64 = 256 * 1024 * 1024;

/// Large-file segmentation (design §14): files at least this size are split
/// into per-block segment tasks dispatched to different workers. Equal to one
/// CV block (128 MiB) so each segment maps to exactly one target block.
pub const SEGMENT_BYTES: i64 = 128 * 1024 * 1024;

/// Compressed formats must not be split (design §14, Flink
/// `nonSplittableFileSuffixes`): segment-parallel reads of a compressed file
/// would decompress each segment independently and produce corrupt output.
/// Files with these suffixes stay a single task.
pub const NON_SPLITTABLE_SUFFIXES: [&str; 6] = [".gz", ".bz2", ".zip", ".xz", ".lz4", ".zst"];

/// True when the file must not be split into segments (compressed formats).
pub fn is_non_splittable(name: &str) -> bool {
    NON_SPLITTABLE_SUFFIXES
        .iter()
        .any(|suffix| name.to_ascii_lowercase().ends_with(suffix))
}

/// Pure batch split decision (design §7.1): flush when the file count or
/// accumulated bytes reach the thresholds.
pub fn batch_should_flush(file_count: usize, accumulated_bytes: i64) -> bool {
    file_count >= BATCH_MAX_FILES || accumulated_bytes >= BATCH_TARGET_BYTES
}

/// Pure segment split (design §14): returns (offset, length) ranges covering
/// the whole file, each at most SEGMENT_BYTES and aligned to the target's
/// CV block size so each segment maps to exactly one block. Testable without
/// a planner instance.
pub fn segment_offsets(file_len: i64, block_size: i64) -> Vec<(i64, i64)> {
    // One segment per CV block: never exceed SEGMENT_BYTES, but also align to
    // the job's block size so offset/block_size maps to an exact block index
    // (a larger configured block_size would otherwise misalign offsets).
    let seg = if block_size > 0 {
        SEGMENT_BYTES.min(block_size)
    } else {
        SEGMENT_BYTES
    };
    let mut ranges = Vec::new();
    let mut offset = 0i64;
    while offset < file_len {
        let length = seg.min(file_len - offset);
        ranges.push((offset, length));
        offset += length;
    }
    ranges
}

#[derive(Clone)]
pub struct TransferPlanner {
    master_fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    cache: ClusterMetadataCache,
    client_conf: ClientConf,
    max_tasks_per_transfer: usize,
    ufs_limiter: Arc<UfsEndpointLimiter>,
}

/// Max concurrent directory listings during planning collection.
/// Bounds fan-out; per-endpoint pressure is still gated by UfsEndpointLimiter.
const PLAN_LIST_CONCURRENCY: usize = 16;

/// Hard cap on collected filesystem entries (files + directories). Listing
/// memory is bounded independently of the task cap: batching collapses many
/// files into one task, so a task-derived cap would both over- and
/// under-count legal trees. Phase 2 still enforces max_tasks_per_transfer.
const PLAN_ENTRY_CAP: usize = 300_000;

/// One collected filesystem entry with its BFS position. Phase 2 sorts by
/// (depth, seq) to replay the original serial BFS order, keeping task ids
/// and batch boundaries deterministic.
struct CollectedEntry {
    depth: u32,
    dir_ord: u64,
    seq: u64,
    status: FileStatus,
    target_status: Option<FileStatus>,
}

/// Shared collection state for concurrent directory listing.
struct CollectShared {
    seq: AtomicU64,
    dir_ord: AtomicU64,
    count: AtomicUsize,
    entries: Mutex<Vec<CollectedEntry>>,
    sem: tokio::sync::Semaphore,
    collect_limit: usize,
    job_id: String,
}

/// One directory to list inside the collection JoinSet.
#[derive(Clone)]
struct CollectDirCtx {
    source: Path,
    target: Path,
    mount: MountInfo,
    dir: Path,
    incremental_load: bool,
    depth: u32,
    /// Discovery ordinal of this directory (assigned by its parent when it
    /// was listed). Children sort by (depth, dir_ord, child seq) so the
    /// final order is deterministic regardless of list completion timing.
    dir_ord: u64,
}

/// Record one entry, failing fast when the collection exceeds its cap.
fn push_entry(
    shared: &CollectShared,
    depth: u32,
    dir_ord: u64,
    status: FileStatus,
    target_status: Option<FileStatus>,
) -> FsResult<()> {
    let seq = shared.seq.fetch_add(1, Ordering::Relaxed);
    let count = shared
        .count
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    if count > shared.collect_limit {
        return Err(FsError::common(format!(
            "Transfer {} entries exceeds {} during listing",
            shared.job_id, shared.collect_limit,
        )));
    }
    shared.entries.lock().push(CollectedEntry {
        depth,
        dir_ord,
        seq,
        status,
        target_status,
    });
    Ok(())
}

fn root_is_dir(shared: &CollectShared) -> bool {
    shared
        .entries
        .lock()
        .first()
        .map(|e| e.status.is_dir)
        .unwrap_or(false)
}

impl TransferPlanner {
    pub fn new(
        master_fs: CurvineFileSystem,
        factory: Arc<UfsFactory>,
        cache: ClusterMetadataCache,
        client_conf: ClientConf,
        max_tasks_per_transfer: usize,
        ufs_max_concurrency_per_endpoint: usize,
    ) -> Self {
        Self {
            master_fs,
            factory,
            cache,
            client_conf,
            max_tasks_per_transfer,
            ufs_limiter: Arc::new(UfsEndpointLimiter::new(ufs_max_concurrency_per_endpoint)),
        }
    }

    pub async fn plan(&self, job: &TransferJobRecord) -> FsResult<PlannedTransfer> {
        let mount = job_mount_snapshot_async(job, &self.cache).await?;
        self.plan_once(job, mount).await
    }

    /// Phase 1 of planning: concurrently list the source tree (the slow
    /// network part) while task generation stays serial in phase 2.
    /// Returns entries in arbitrary completion order; callers sort by
    /// (depth, seq). Any listing failure aborts the whole collection, same
    /// as the old serial loop. Collection is capped so a runaway directory
    /// fails fast instead of buffering unbounded statuses.
    async fn collect_tree(
        &self,
        job_id: &str,
        root: CollectDirCtx,
        root_status: FileStatus,
        root_target: Option<FileStatus>,
    ) -> FsResult<Vec<CollectedEntry>> {
        let shared = Arc::new(CollectShared {
            seq: AtomicU64::new(0),
            dir_ord: AtomicU64::new(1),
            count: AtomicUsize::new(0),
            entries: Mutex::new(Vec::new()),
            sem: tokio::sync::Semaphore::new(PLAN_LIST_CONCURRENCY),
            collect_limit: PLAN_ENTRY_CAP,
            job_id: job_id.to_string(),
        });
        push_entry(&shared, 0, 0, root_status, root_target)?;
        let mut set = tokio::task::JoinSet::new();
        if root_is_dir(&shared) {
            let planner = self.clone();
            let shared = shared.clone();
            set.spawn(async move { planner.collect_dir(root, shared).await });
        }
        let mut first_err: Option<FsError> = None;
        while let Some(res) = set.join_next().await {
            let subdirs = match res {
                Ok(Ok(subdirs)) => subdirs,
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                    set.abort_all();
                    continue;
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(FsError::common(format!(
                            "planning list task failed: {}",
                            err
                        )));
                    }
                    set.abort_all();
                    continue;
                }
            };
            if first_err.is_none() {
                for ctx in subdirs {
                    let planner = self.clone();
                    let shared = shared.clone();
                    set.spawn(async move { planner.collect_dir(ctx, shared).await });
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        let entries = shared.entries.lock().drain(..).collect::<Vec<_>>();
        Ok(entries)
    }

    /// List one directory and return its subdirectories for fan-out. Runs
    /// inside the collection JoinSet with semaphore-bounded concurrency.
    async fn collect_dir(
        &self,
        ctx: CollectDirCtx,
        shared: Arc<CollectShared>,
    ) -> FsResult<Vec<CollectDirCtx>> {
        let _permit = shared
            .sem
            .acquire()
            .await
            .map_err(|_| FsError::common("Transfer planning is stopping; retry shortly"))?;
        // Target-side children for incremental loads (same lookup the old
        // serial loop did per directory). A missing target directory means
        // nothing to skip: treat it as empty instead of failing planning
        // (new trees and new subdirectories are legal in incremental loads).
        let target_children: HashMap<String, FileStatus> = if ctx.incremental_load {
            let target_path = append_relative_path(&ctx.source, &ctx.target, &ctx.dir)?;
            match self.list_status(&target_path, &ctx.mount).await {
                Ok(children) => children
                    .into_iter()
                    .map(|status| (status.name.clone(), status))
                    .collect(),
                Err(FsError::FileNotFound(_)) => HashMap::new(),
                Err(err) => return Err(err),
            }
        } else {
            HashMap::new()
        };
        let mut subdirs = Vec::new();
        for child in self.list_status(&ctx.dir, &ctx.mount).await? {
            let target_status = target_children.get(&child.name).cloned();
            if child.is_dir {
                let child_path = Path::from_str(&child.path)?;
                // Assign the child directory its discovery ordinal serially
                // inside the parent's listing, so sibling order is stable.
                let child_ord = shared.dir_ord.fetch_add(1, Ordering::Relaxed);
                push_entry(
                    &shared,
                    ctx.depth + 1,
                    child_ord,
                    child,
                    target_status.clone(),
                )?;
                subdirs.push(CollectDirCtx {
                    source: ctx.source.clone(),
                    target: ctx.target.clone(),
                    mount: ctx.mount.clone(),
                    dir: child_path,
                    incremental_load: ctx.incremental_load,
                    depth: ctx.depth + 1,
                    dir_ord: child_ord,
                });
            } else {
                push_entry(&shared, ctx.depth + 1, ctx.dir_ord, child, target_status)?;
            }
        }
        Ok(subdirs)
    }

    async fn plan_once(
        &self,
        job: &TransferJobRecord,
        mount: MountInfo,
    ) -> FsResult<PlannedTransfer> {
        let source = Path::from_str(&job.source_path)?;
        let target = Path::from_str(&job.target_path)?;
        let job_info = self.load_job_info(job, &mount)?;
        let source_status = self.get_status(&source, &mount).await?;
        let incremental_load = job.kind == TransferKind::Load && !source.is_cv() && target.is_cv();
        let target_status = if incremental_load {
            self.get_optional_status(&target, &mount).await?
        } else {
            None
        };
        validate_transfer_target(&source_status, target_status.as_ref(), &target)?;
        if source_status.is_dir && !incremental_load {
            self.validate_directory_target(job, &target, &mount).await?;
        }

        // Small-file batching state (design §7): files accumulate here and are
        // flushed as one batch task when the byte/file threshold is hit.
        // CV sources never batch - their per-file block-read plan occupies
        // `source_read_plan_json` and is the cheaper read path already.
        // Batching applies to directory sources on the non-CV (UFS) side only:
        // CV sources keep per-file block-read plans, and single-file transfers
        // keep their existing shape (design §7.1).
        let batching = source_status.is_dir && !source.is_cv();
        let overwrite = job_info.overwrite.unwrap_or(false);
        let mut batch: Vec<FileStatus> = Vec::new();
        let mut batch_bytes = 0i64;

        let mut tasks = Vec::new();
        let mut total_size = 0;
        let mut skipped_files = 0;
        let mut skipped_size = 0;
        // Phase 1 fans directory listing out to concurrent workers (the slow
        // network part); phase 2 below walks the collected statuses in BFS
        // order and generates tasks exactly as the old serial loop did, so
        // task ids and batch boundaries are unchanged.
        let mut collected = self
            .collect_tree(
                &job.job_id,
                CollectDirCtx {
                    source: source.clone(),
                    target: target.clone(),
                    mount: mount.clone(),
                    dir: source.clone(),
                    incremental_load,
                    depth: 0,
                    dir_ord: 0,
                },
                source_status,
                target_status,
            )
            .await?;
        collected.sort_by_key(|a| (a.depth, a.dir_ord, a.seq));

        for entry in collected {
            let (status, target_status) = (entry.status, entry.target_status);
            let path = Path::from_str(&status.path)?;
            if status.is_dir {
                // Directories only structure the walk; their children were
                // collected as separate entries in phase 1.
                continue;
            }

            let task_target = append_relative_path(&source, &target, &path)?;
            validate_transfer_target(&status, target_status.as_ref(), &task_target)?;
            let unchanged =
                if incremental_load && unchanged_load_target(&status, target_status.as_ref()) {
                    true
                } else if incremental_load
                    && needs_source_status_refresh(&status, target_status.as_ref())
                {
                    let current_source_status = self.get_status(&path, &mount).await?;
                    unchanged_load_target(&current_source_status, target_status.as_ref())
                } else {
                    false
                };
            if unchanged {
                skipped_files += 1;
                skipped_size += status.len;
                continue;
            }
            total_size += status.len;

            if batching {
                // Large files (>= batch target) are split into per-block
                // segment tasks dispatched to different workers (design §14).
                // A segment maps to exactly one CV block (128 MiB).
                if status.len >= BATCH_TARGET_BYTES && !is_non_splittable(path.name()) {
                    if !batch.is_empty() {
                        let files = std::mem::take(&mut batch);
                        batch_bytes = 0;
                        self.push_batch_task(job, &source, &target, files, &mut tasks)?;
                    }
                    self.push_segment_tasks(
                        job,
                        &path,
                        &task_target,
                        &status,
                        overwrite,
                        job_info.block_size,
                        &mut tasks,
                    )
                    .await?;
                    continue;
                }
                // Compressed large files or medium files stay a single task.
                if status.len >= BATCH_TARGET_BYTES {
                    if !batch.is_empty() {
                        let files = std::mem::take(&mut batch);
                        batch_bytes = 0;
                        self.push_batch_task(job, &source, &target, files, &mut tasks)?;
                    }
                    self.push_single_task(job, &path, &task_target, &status, &mut tasks)
                        .await?;
                    continue;
                }
                batch_bytes += status.len;
                batch.push(status);
                if batch_should_flush(batch.len(), batch_bytes) {
                    let files = std::mem::take(&mut batch);
                    batch_bytes = 0;
                    self.push_batch_task(job, &source, &target, files, &mut tasks)?;
                }
                continue;
            }

            self.push_single_task(job, &path, &task_target, &status, &mut tasks)
                .await?;
        }
        if !batch.is_empty() {
            let files = std::mem::take(&mut batch);
            self.push_batch_task(job, &source, &target, files, &mut tasks)?;
        }

        Ok(PlannedTransfer {
            job_info,
            tasks,
            total_size,
            skipped_files,
            skipped_size,
            cv_metadata_epoch: None,
        })
    }

    /// Flush one batch of files as a single task (design §7.1). The task's
    /// `source_path` is the job source root (a directory URI) so a legacy
    /// worker that receives it by mistake fails loudly (open on a directory)
    /// instead of silently copying only one file of the batch. Manifest
    /// entries carry paths relative to the source root; a batch may span
    /// subdirectories.
    #[allow(clippy::too_many_arguments)]
    async fn push_segment_tasks(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        task_target: &Path,
        status: &FileStatus,
        overwrite: bool,
        block_size: i64,
        tasks: &mut Vec<TransferTaskRecord>,
    ) -> FsResult<()> {
        // Segmentation writes each segment as one CV block on one worker. It
        // only applies to CV targets: a UFS target has no block layout to
        // write into, so fall back to a single whole-file task (design §14).
        if !task_target.is_cv() {
            return self
                .push_single_task(job, path, task_target, status, tasks)
                .await;
        }
        // Segment tasks write the target as a fresh full file (design §14). A
        // pre-existing target must be cleared before any segment lands,
        // otherwise a retry or an incremental re-copy mixes new segments with
        // old bytes (data corruption). Respect overwrite: without it a
        // pre-existing target is an error (same as single-file semantics).
        // The delete happens only after the segment ranges are computed (i.e.
        // we know at least one segment task will be emitted), so a planner run
        // that produces no tasks never destroys an existing target (review).
        let seg_block = if block_size > 0 {
            block_size
        } else {
            SEGMENT_BYTES
        };
        let ranges = segment_offsets(status.len, block_size);
        if task_target.is_cv() {
            match self.master_fs.get_status(task_target).await {
                Ok(_) => {
                    if !overwrite {
                        return err_box!(
                            "Transfer target {} already exists and overwrite=false",
                            task_target.full_path()
                        );
                    }
                    self.master_fs.delete(task_target, false).await?;
                }
                Err(FsError::FileNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        for (offset, length) in ranges {
            let plan = SegmentPlan {
                offset,
                length,
                total_len: status.len,
                block_index: offset / seg_block.max(1),
            };
            // Wrap so the runner can distinguish a segment task from a batch
            // manifest or a CV block plan: {"segment": {offset, length}}.
            let segment_json = serde_json::to_string(&serde_json::json!({
                "segment": plan
            }))
            .map_err(|_| FsError::common("Unable to serialize segment plan"))?;
            let task_id = format!("{}_task_{}", job.job_id, tasks.len());
            tasks.push(TransferTaskRecord {
                job_id: job.job_id.clone(),
                run_id: job.run_id,
                task_id,
                attempt_id: 0,
                source_path: path.clone_uri(),
                target_path: task_target.clone_uri(),
                worker_id: 0,
                worker_session_id: String::new(),
                source_read_plan_json: segment_json,
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
                progress: TransferProgress {
                    total_size: length,
                    ..Default::default()
                },
                estimated_size: length,
                retry_count: 0,
                attempt_started_at: 0,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: LocalTime::mills() as i64,
            });
        }
        if tasks.len() > self.max_tasks_per_transfer {
            return err_box!(
                "Transfer {} files exceeds {}",
                job.job_id,
                self.max_tasks_per_transfer
            );
        }
        Ok(())
    }

    /// Push one file as its own task (non-batching path, or a large file that
    /// exceeds the batch target but is below the segment threshold; design
    /// §7.1 keeps such files single-task).
    async fn push_single_task(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        task_target: &Path,
        status: &FileStatus,
        tasks: &mut Vec<TransferTaskRecord>,
    ) -> FsResult<()> {
        let task_id = format!("{}_task_{}", job.job_id, tasks.len());
        let source_read_plan_json = self.source_read_plan_json(path).await?;
        tasks.push(TransferTaskRecord {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id,
            attempt_id: 0,
            source_path: path.clone_uri(),
            target_path: task_target.clone_uri(),
            worker_id: 0,
            worker_session_id: String::new(),
            source_read_plan_json,
            report_target_json: String::new(),
            state: TransferTaskState::Pending,
            // Carries estimated bytes into record_json AND progress_total_size
            // (task_params maps progress into columns) so scheduler-side
            // in-flight aggregation has a non-zero baseline before first report.
            progress: TransferProgress {
                total_size: status.len,
                ..Default::default()
            },
            estimated_size: status.len,
            retry_count: 0,
            attempt_started_at: 0,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            last_report_at: 0,
            stale_deadline_at: 0,
            updated_at: LocalTime::mills() as i64,
        });
        if tasks.len() > self.max_tasks_per_transfer {
            return err_box!(
                "Transfer {} files exceeds {}",
                job.job_id,
                self.max_tasks_per_transfer
            );
        }
        Ok(())
    }

    fn push_batch_task(
        &self,
        job: &TransferJobRecord,
        source_root: &Path,
        target_root: &Path,
        files: Vec<FileStatus>,
        tasks: &mut Vec<TransferTaskRecord>,
    ) -> FsResult<()> {
        let entries: Vec<BatchManifestEntry> = files
            .iter()
            .map(|file| {
                let file_path = Path::from_str(&file.path)?;
                // Relative path from the source root to the file.
                Ok(BatchManifestEntry {
                    rel_path: relative_path_from(source_root, &file_path)?,
                    size: file.len,
                })
            })
            .collect::<FsResult<Vec<_>>>()?;
        let manifest = BatchManifest { batch: entries };
        let total: i64 = files.iter().map(|file| file.len).sum();
        let batch_source = source_root.clone();
        let batch_target = append_relative_path(source_root, target_root, &batch_source)?;
        let task_id = format!("{}_task_{}", job.job_id, tasks.len());
        tasks.push(TransferTaskRecord {
            job_id: job.job_id.clone(),
            run_id: job.run_id,
            task_id,
            attempt_id: 0,
            source_path: batch_source.clone_uri(),
            target_path: batch_target.clone_uri(),
            worker_id: 0,
            worker_session_id: String::new(),
            source_read_plan_json: serde_json::to_string(&manifest)
                .map_err(|_| FsError::common("Unable to serialize batch manifest"))?,
            report_target_json: String::new(),
            state: TransferTaskState::Pending,
            progress: TransferProgress {
                total_size: total,
                ..Default::default()
            },
            estimated_size: total,
            retry_count: 0,
            attempt_started_at: 0,
            accepted_at_ms: 0,
            first_byte_at_ms: 0,
            last_report_at: 0,
            stale_deadline_at: 0,
            updated_at: LocalTime::mills() as i64,
        });
        if tasks.len() > self.max_tasks_per_transfer {
            return err_box!(
                "Transfer {} files exceeds {}",
                job.job_id,
                self.max_tasks_per_transfer
            );
        }
        Ok(())
    }

    async fn validate_directory_target(
        &self,
        job: &TransferJobRecord,
        target: &Path,
        mount: &MountInfo,
    ) -> FsResult<()> {
        let target_status = if job.kind == TransferKind::Load && target.is_cv() {
            let start = Instant::now();
            let result = self.master_fs.get_status(target).await;
            record_metadata_operation("cv", "get_status", result.is_ok(), start);
            result
        } else {
            self.get_status(target, mount).await
        };
        match target_status {
            Ok(status) if status.is_dir => Ok(()),
            Ok(_) => err_box!(
                "Transfer target {} is a file; refusing to transfer directory into file",
                target.full_path()
            ),
            Err(FsError::FileNotFound(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn load_job_info(
        &self,
        job: &TransferJobRecord,
        mount: &MountInfo,
    ) -> FsResult<LoadJobInfo> {
        load_job_info(job, mount, &self.client_conf)
    }

    async fn get_status(&self, path: &Path, mount: &MountInfo) -> FsResult<FileStatus> {
        let start = Instant::now();
        if path.is_cv() {
            let result = self.master_fs.get_status(path).await;
            record_metadata_operation("cv", "get_status", result.is_ok(), start);
            result
        } else {
            let _permit = self.ufs_limiter.acquire(mount).await?;
            let result = match self.factory.get_ufs(mount) {
                Ok(ufs) => ufs.get_status(path).await,
                Err(err) => Err(err),
            };
            record_metadata_operation("ufs", "get_status", result.is_ok(), start);
            result
        }
    }

    async fn get_optional_status(
        &self,
        path: &Path,
        mount: &MountInfo,
    ) -> FsResult<Option<FileStatus>> {
        match self.get_status(path, mount).await {
            Ok(status) => Ok(Some(status)),
            Err(FsError::FileNotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn list_status(&self, path: &Path, mount: &MountInfo) -> FsResult<Vec<FileStatus>> {
        let start = Instant::now();
        if path.is_cv() {
            let result = self.list_cv_status(path).await;
            record_metadata_operation("cv", "list_status", result.is_ok(), start);
            result
        } else {
            let _permit = self.ufs_limiter.acquire(mount).await?;
            let result = match self.factory.get_ufs(mount) {
                Ok(ufs) => ufs.list_status(path).await,
                Err(err) => Err(err),
            };
            record_metadata_operation("ufs", "list_status", result.is_ok(), start);
            result
        }
    }

    async fn list_cv_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let mut statuses = Vec::new();
        let mut start_after = None;
        loop {
            let page = self
                .master_fs
                .list_options(
                    path,
                    ListOptions {
                        limit: Some(CV_LIST_PAGE_SIZE),
                        start_after: start_after.take(),
                    },
                )
                .await?;
            let count = page.len();
            start_after = page.last().map(|status| status.name.clone());
            statuses.extend(page);
            if count < CV_LIST_PAGE_SIZE {
                return Ok(statuses);
            }
        }
    }

    async fn source_read_plan_json(&self, path: &Path) -> FsResult<String> {
        if !path.is_cv() {
            return Ok(String::new());
        }
        let start = Instant::now();
        let blocks = self.master_fs.get_block_locations(path).await;
        record_metadata_operation("cv", "get_block_locations", blocks.is_ok(), start);
        let blocks = blocks?;
        serde_json::to_string(&blocks).map_err(|_| {
            FsError::common(format!(
                "Unable to prepare the CV read plan for {}",
                path.full_path()
            ))
        })
    }
}

pub(crate) fn unchanged_load_target(source: &FileStatus, target: Option<&FileStatus>) -> bool {
    let Some(target) = target else {
        return false;
    };
    !target.is_dir && target.is_complete() && target.cv_valid(Some(source))
}

pub(crate) fn needs_source_status_refresh(
    source: &FileStatus,
    target: Option<&FileStatus>,
) -> bool {
    let Some(target) = target else {
        return false;
    };
    !target.is_dir
        && target.is_complete()
        && target.cv_valid(None)
        && target.len == source.len
        && target.storage_policy.ufs_mtime != source.mtime
}

fn validate_transfer_target(
    source: &FileStatus,
    target: Option<&FileStatus>,
    target_path: &Path,
) -> FsResult<()> {
    let Some(target) = target else {
        return Ok(());
    };
    if source.is_dir && !target.is_dir {
        return err_box!(
            "Transfer target {} is a file; refusing to transfer directory into file",
            target_path.full_path()
        );
    }
    if !source.is_dir && target.is_dir {
        return err_box!(
            "Transfer target {} is a directory; refusing to overwrite directory with file",
            target_path.full_path()
        );
    }
    Ok(())
}

pub(crate) fn load_job_info(
    job: &TransferJobRecord,
    mount: &MountInfo,
    client_conf: &ClientConf,
) -> FsResult<LoadJobInfo> {
    let default_replicas = mount.replicas.unwrap_or(client_conf.replicas);
    let (replicas, overwrite) = match transfer_command(job) {
        Ok(command) => {
            let replicas = command
                .replicas()
                .ok()
                .flatten()
                .unwrap_or(default_replicas);
            (replicas, command.overwrite())
        }
        // Keep the pre-options behavior for old/corrupt records: they used the
        // default overwrite and replica settings while planning.
        Err(_) => (default_replicas, true),
    };
    Ok(LoadJobInfo {
        job_id: job.job_id.clone(),
        source_path: job.source_path.clone(),
        target_path: job.target_path.clone(),
        replicas,
        block_size: mount.block_size.unwrap_or(client_conf.block_size),
        storage_type: mount.storage_type.unwrap_or(client_conf.storage_type),
        ttl_ms: mount.ttl_ms,
        ttl_action: mount.ttl_action,
        mount_info: mount.clone(),
        create_time: job.created_at,
        overwrite: Some(overwrite),
        source_type: Default::default(),
    })
}

struct UfsEndpointLimiter {
    max_per_endpoint: usize,
    semaphores: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl UfsEndpointLimiter {
    fn new(max_per_endpoint: usize) -> Self {
        Self {
            max_per_endpoint,
            semaphores: Mutex::new(HashMap::new()),
        }
    }

    async fn acquire(&self, mount: &MountInfo) -> FsResult<OwnedSemaphorePermit> {
        let endpoint = ufs_endpoint_key(mount);
        let semaphore = {
            let mut semaphores = self.semaphores.lock();
            semaphores
                .entry(endpoint)
                .or_insert_with(|| Arc::new(Semaphore::new(self.max_per_endpoint)))
                .clone()
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| FsError::common("Transfer planning is stopping; retry shortly"))
    }
}

fn ufs_endpoint_key(mount: &MountInfo) -> String {
    let Ok(path) = Path::from_str(&mount.ufs_path) else {
        return mount.ufs_path.clone();
    };
    match path.scheme() {
        Some("file") => "file://".to_string(),
        Some(scheme) => format!("{}://{}", scheme, path.authority().unwrap_or("")),
        None => mount.ufs_path.clone(),
    }
}

fn record_metadata_operation(
    source: &'static str,
    operation: &'static str,
    success: bool,
    start: Instant,
) {
    if let Ok(metrics) = TransferMetrics::get() {
        metrics.observe_metadata_operation(
            source,
            operation,
            if success { "success" } else { "error" },
            start.elapsed().as_micros(),
        );
    }
}

/// Relative path of `source` under `source_root` ("" when equal). Shared by
/// batch manifests and target derivation (design §7.1).
fn relative_path_from(source_root: &Path, source: &Path) -> FsResult<String> {
    let source_root_text = normalized_path_text(source_root);
    let source_text = normalized_path_text(source);
    if source_text == source_root_text {
        return Ok(String::new());
    }
    Ok(source_text
        .strip_prefix(source_root_text.as_str())
        .ok_or_else(|| {
            FsError::common(format!(
                "Source path {} is not under transfer root {}",
                source.full_path(),
                source_root.full_path()
            ))
        })?
        .trim_start_matches('/')
        .to_string())
}

fn append_relative_path(source_root: &Path, target_root: &Path, source: &Path) -> FsResult<Path> {
    let relative = relative_path_from(source_root, source)?;
    let target_root_text = normalized_path_text(target_root);

    let target = if relative.is_empty() {
        target_root_text
    } else {
        format!("{}/{}", target_root_text.trim_end_matches('/'), relative)
    };
    Ok(Path::from_str(target)?)
}

fn transfer_command(job: &TransferJobRecord) -> FsResult<TransferCommand> {
    serde_json::from_str(&job.command_json).map_err(|_| {
        FsError::common(format!(
            "Stored transfer command for job {} is invalid",
            job.job_id
        ))
    })
}

fn normalized_path_text(path: &Path) -> String {
    if path.is_cv() {
        path.path().trim_end_matches('/').to_string()
    } else {
        path.full_path().trim_end_matches('/').to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::load_job_info;
    use curvine_config::ClientConf;
    use curvine_model::{
        MountInfo, TransferCommand, TransferJobRecord, TransferKind, TransferState,
    };

    fn job(command: TransferCommand) -> TransferJobRecord {
        TransferJobRecord {
            job_key: command.job_key(),
            job_id: "job-1".to_string(),
            run_id: 1,
            kind: TransferKind::Load,
            source_path: command.source_path.clone(),
            target_path: command.target_path.clone(),
            command_json: serde_json::to_string(&command).unwrap(),
            mount_snapshot_json: "{}".to_string(),
            secret_ref_json: "{}".to_string(),
            cluster_snapshot_version: 0,
            mount_snapshot_attempts: 0,
            planning_attempts: 0,
            cv_metadata_epoch: None,
            state: TransferState::Pending,
            owner: String::new(),
            lease_epoch: 0,
            lease_expire_at: 0,
            cancel_requested: false,
            summary: Default::default(),
            client_request_id: command.client_request_id.clone(),
            submitter: String::new(),
            tenant: String::new(),
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn load_job_info_uses_explicit_replicas_over_mount_default() {
        let mut command = TransferCommand {
            kind: TransferKind::Load,
            source_path: "s3://bucket/a".to_string(),
            target_path: "/mnt/a".to_string(),
            ..Default::default()
        };
        command.set_replicas(3);
        let mount = MountInfo {
            replicas: Some(2),
            ..Default::default()
        };
        let client_conf = ClientConf {
            replicas: 1,
            ..Default::default()
        };

        let info = load_job_info(&job(command), &mount, &client_conf).unwrap();
        assert_eq!(info.replicas, 3);
    }

    #[test]
    fn load_job_info_keeps_mount_default_without_replica_option() {
        let command = TransferCommand {
            kind: TransferKind::Load,
            source_path: "s3://bucket/a".to_string(),
            target_path: "/mnt/a".to_string(),
            ..Default::default()
        };
        let mount = MountInfo {
            replicas: Some(2),
            ..Default::default()
        };
        let info = load_job_info(&job(command), &mount, &ClientConf::default()).unwrap();
        assert_eq!(info.replicas, 2);
    }
}
