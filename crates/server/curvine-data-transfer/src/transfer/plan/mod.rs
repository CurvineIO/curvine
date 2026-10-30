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
use crate::transfer::{
    job_mount_snapshot_async, ClusterMetadataCache, TransferMetrics, TransferRuntimeConfig,
};
use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClientConf, TransferConf};
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path, GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS};
use curvine_model::ListOptions;
use curvine_model::{
    segment_group_json, BatchManifest, BatchManifestEntry, FileBlocks, FileGen, FileStatus,
    JobSourceType, LoadJobInfo, LocatedBlock, MountInfo, SegmentGroupPlan, SegmentPlan,
    TransferCommand, TransferJobRecord, TransferKind, TransferProgress, TransferTaskRecord,
    TransferTaskState, WorkerInfo,
};
use curvine_runtime::common::LocalTime;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const CV_LIST_PAGE_SIZE: usize = 1_000;

pub(crate) fn validate_fs_mode_mapping(
    mount: &MountInfo,
    mounts: &[MountInfo],
    source: &Path,
    target: &Path,
) -> FsResult<()> {
    let actual = mounts
        .iter()
        .filter(|candidate| Path::has_prefix(source.path(), &candidate.cv_path))
        .max_by_key(|candidate| candidate.cv_path.len());
    if !source.is_cv()
        || !mount.is_fs_mode()
        || actual.is_none_or(|candidate| candidate.mount_id != mount.mount_id)
        || mount.get_ufs_path(source)? != *target
    {
        return err_box!(
            "fs_mode source {} does not map to target {} in mount {}",
            source.full_path(),
            target.full_path(),
            mount.cv_path
        );
    }
    Ok(())
}

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

/// Default group size: half of the 10GiB worker in-flight budget.
pub const SEGMENT_GROUP_TARGET_BYTES: i64 = TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES / 2;

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

pub(crate) fn supports_segment_grouping(workers: &[WorkerInfo]) -> bool {
    !workers.is_empty()
        && workers
            .iter()
            .all(|worker| worker.transfer_capabilities.supports_segment_group())
}

/// True when a group manifest can carry an identity the worker re-checks:
/// an adapter-supplied immutable version, or the HEAD `(len, mtime)` pair from
/// the same observation (the strongest thing an adapter without a version can
/// offer, and the trust level `unchanged_load_target` already uses). The pair
/// is minted only from HEAD so the worker compares like for like; without any
/// identity the file falls back to single-segment tasks.
pub(crate) fn has_group_identity(version: &str, head: Option<(i64, i64)>) -> bool {
    !version.is_empty() || matches!(head, Some((len, mtime)) if len > 0 && mtime > 0)
}

/// Plan-time HEAD mtime to persist in a segment manifest, `0` when the planner
/// could not observe one. A zero keeps the target cache-invalid after
/// convergence rather than claiming a freshness nobody verified.
pub(crate) fn head_mtime(head: Option<(i64, i64)>) -> i64 {
    head.map(|(_, mtime)| mtime).unwrap_or(0)
}

/// Why a large file did or did not use segment-group tasks. Counted per
/// file in `plan_once` so one job emits one info line instead of one log
/// per file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentSplitOutcome {
    Grouped,
    /// No immutable version and no HEAD `(len, mtime)` observation.
    NoIdentity,
    NoCapability,
    Moved,
}

/// Pure segment split (design §14, §8 A1): returns (offset, length) ranges
/// covering the whole file with one segment per CV target block, so each
/// segment maps to exactly one block. Testable without a planner instance.
pub fn segment_offsets(file_len: i64, block_size: i64) -> Vec<(i64, i64)> {
    let seg = match block_size > 0 {
        true => block_size,
        false => SEGMENT_BYTES,
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

/// Pure segment-group partition (design §8 A2): packs consecutive whole
/// segments into at most `G_target = min(max(workers,
/// ceil(len / GROUP_BYTES)), segments)` groups, splitting any group that
/// still exceeds `GROUP_BYTES` (a single oversized block stands alone).
/// Cut points stay block-aligned; a one-segment group is emitted by the
/// caller as the legacy single-segment plan so small files are unchanged.
/// `source_version` is the adapter-supplied immutable identity (etag/version)
/// persisted into every group manifest; empty means unavailable (the caller
/// falls back to single segments per design D0.4).
pub fn plan_segment_groups(
    file_len: i64,
    block_size: i64,
    live_workers: usize,
    source_len: i64,
    source_mtime: i64,
    source_version: String,
) -> Vec<SegmentGroupPlan> {
    let ranges = segment_offsets(file_len, block_size);
    let total = ranges.len();
    if total == 0 {
        return Vec::new();
    }
    let seg = match block_size > 0 {
        true => block_size,
        false => SEGMENT_BYTES,
    }
    .max(1);
    let workers = live_workers.max(1);
    let by_bytes = file_len
        .div_euclid(SEGMENT_GROUP_TARGET_BYTES)
        .saturating_add(i64::from(file_len % SEGMENT_GROUP_TARGET_BYTES != 0))
        .max(1) as usize;
    let target = workers.max(by_bytes).min(total).max(1);
    let base = total / target;
    let rem = total % target;
    let mut groups = Vec::new();
    let mut start = 0usize;
    for i in 0..target {
        let count = base + usize::from(i < rem);
        if count == 0 {
            continue;
        }
        split_group_by_bytes(
            &ranges,
            start,
            count,
            seg,
            file_len,
            source_len,
            source_mtime,
            &source_version,
            SEGMENT_GROUP_TARGET_BYTES,
            &mut groups,
        );
        start += count;
    }
    groups
}

#[allow(clippy::too_many_arguments)]
fn split_group_by_bytes(
    ranges: &[(i64, i64)],
    start: usize,
    count: usize,
    seg: i64,
    file_len: i64,
    source_len: i64,
    source_mtime: i64,
    source_version: &str,
    target_bytes: i64,
    groups: &mut Vec<SegmentGroupPlan>,
) {
    let mut first = start;
    let mut bytes = 0i64;
    for index in start..start + count {
        let len = ranges[index].1;
        if bytes > 0 && bytes.saturating_add(len) > target_bytes {
            push_group_plan(
                ranges,
                first,
                index,
                seg,
                file_len,
                source_len,
                source_mtime,
                source_version,
                groups,
            );
            first = index;
            bytes = 0;
        }
        bytes = bytes.saturating_add(len);
    }
    push_group_plan(
        ranges,
        first,
        start + count,
        seg,
        file_len,
        source_len,
        source_mtime,
        source_version,
        groups,
    );
}

#[allow(clippy::too_many_arguments)]
fn push_group_plan(
    ranges: &[(i64, i64)],
    first: usize,
    end: usize,
    seg: i64,
    file_len: i64,
    source_len: i64,
    source_mtime: i64,
    source_version: &str,
    groups: &mut Vec<SegmentGroupPlan>,
) {
    if first >= end || end > ranges.len() {
        return;
    }
    let start_offset = ranges[first].0;
    let end_offset = ranges[end - 1].0.saturating_add(ranges[end - 1].1);
    groups.push(SegmentGroupPlan {
        start_offset,
        end_offset,
        total_len: file_len,
        first_block_index: start_offset / seg.max(1),
        segment_bytes: seg,
        source_len,
        source_mtime,
        source_version: source_version.to_owned(),
    });
}

fn build_single_segment_plans(
    status: &FileStatus,
    block_size: i64,
    source_mtime: i64,
) -> FsResult<Vec<(String, i64)>> {
    let seg_block = if block_size > 0 {
        block_size
    } else {
        SEGMENT_BYTES
    }
    .max(1);
    segment_offsets(status.len, block_size)
        .into_iter()
        .map(|(offset, length)| {
            let plan = SegmentPlan {
                offset,
                length,
                total_len: status.len,
                block_index: offset / seg_block,
                source_mtime,
            };
            let plan_json = serde_json::to_string(&serde_json::json!({
                "segment": plan
            }))
            .map_err(|_| FsError::common("Unable to serialize segment plan"))?;
            Ok((plan_json, length))
        })
        .collect()
}

pub(crate) fn build_group_plan_json(
    group: &SegmentGroupPlan,
    total_len: i64,
) -> FsResult<(String, i64)> {
    if group.group_len() <= group.segment_bytes.max(1) {
        let plan = SegmentPlan {
            offset: group.start_offset,
            length: group.group_len(),
            total_len,
            block_index: group.first_block_index,
            source_mtime: group.source_mtime,
        };
        let plan_json = serde_json::to_string(&serde_json::json!({
            "segment": plan
        }))
        .map_err(|_| FsError::common("Unable to serialize segment plan"))?;
        return Ok((plan_json, group.group_len()));
    }
    Ok((segment_group_json(group)?, group.group_len()))
}

#[derive(Clone)]
pub struct TransferPlanner {
    master_fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    cache: ClusterMetadataCache,
    client_conf: ClientConf,
    /// Shared runtime state. The cap is re-read on every check so a hot-updated
    /// value applies to the next planning pass without a restart.
    runtime: Arc<TransferRuntimeConfig>,
    ufs_limiter: Arc<UfsEndpointLimiter>,
}

impl TransferPlanner {
    /// Effective task cap for this planning pass.
    fn max_tasks(&self) -> usize {
        self.runtime.max_tasks_per_transfer()
    }
}

/// Max concurrent directory listings during planning collection.
/// Bounds fan-out; per-endpoint pressure is still gated by UfsEndpointLimiter.
const PLAN_LIST_CONCURRENCY: usize = 16;

// The collected-entry cap lives in the runtime state (`transfer.max_plan_entries`)
// rather than a constant: it bounds listing memory independently of the task cap
// (batching collapses many files into one task), and it has to be raisable for
// wide partitions without a rebuild. Phase 2 still enforces max_tasks_per_transfer.

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
        runtime: Arc<TransferRuntimeConfig>,
        ufs_max_concurrency_per_endpoint: usize,
    ) -> Self {
        Self {
            master_fs,
            factory,
            cache,
            client_conf,
            runtime,
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
            // Snapshot per pass: one planning run must use a single ceiling even
            // if the value is pushed while it is still listing.
            collect_limit: self.runtime.max_plan_entries(),
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
        // fs_mode export batches bypass the directory walk entirely: each entry
        // is an independent single-file copy with its own generation.
        let command = transfer_command(job)?;
        if command.is_fs_mode_batch() {
            return self.plan_fs_mode_batch(job, &command, &mount).await;
        }

        let source = Path::from_str(&job.source_path)?;
        let target = Path::from_str(&job.target_path)?;
        let job_info = self.load_job_info(job, &mount).await?;
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
        let mut batch: Vec<(FileStatus, bool)> = Vec::new();
        let mut batch_bytes = 0i64;

        let mut tasks = Vec::new();
        let mut total_size = 0;
        let mut skipped_files = 0;
        let mut skipped_size = 0;
        // Per-file segment-group decisions; logged once per job below so
        // large directory scans do not emit one line per file.
        let mut grouped_files = 0usize;
        let mut fallback_no_identity = 0usize;
        let mut fallback_no_capability = 0usize;
        let mut fallback_moved = 0usize;
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
            // Replica reconciliation: a cached copy whose replica count differs
            // from the mount's replica setting is re-copied even when its
            // content is unchanged, so the configured replicas are enforced.
            let replicas_mismatch = incremental_load
                && load_replica_mismatch(target_status.as_ref(), job_info.replicas);
            // Any re-copy of an existing target must set replace_target: the
            // scheduler deletes the old copy after planning, and a task that
            // writes an existing target with overwrite=false fails validation.
            let mut replace_target = replicas_mismatch;
            let mut unchanged = if replicas_mismatch {
                false
            } else if incremental_load && unchanged_load_target(&status, target_status.as_ref()) {
                true
            } else if incremental_load
                && needs_source_status_refresh(&status, target_status.as_ref())
            {
                let current_source_status = self.get_status(&path, &mount).await?;
                unchanged_load_target(&current_source_status, target_status.as_ref())
            } else {
                false
            };
            if unchanged && status.len > 0 {
                unchanged = self.target_cache_readable(&task_target).await;
                if !unchanged {
                    replace_target = true;
                }
            }
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
                    let outcome = self
                        .push_segment_tasks(
                            job,
                            &path,
                            &task_target,
                            &status,
                            overwrite,
                            replace_target,
                            job_info.block_size,
                            &mount,
                            &mut tasks,
                        )
                        .await?;
                    match outcome {
                        Some(SegmentSplitOutcome::Grouped) => grouped_files += 1,
                        Some(SegmentSplitOutcome::NoIdentity) => fallback_no_identity += 1,
                        Some(SegmentSplitOutcome::NoCapability) => fallback_no_capability += 1,
                        Some(SegmentSplitOutcome::Moved) => fallback_moved += 1,
                        None => {}
                    }
                    continue;
                }
                // Compressed large files or medium files stay a single task.
                if status.len >= BATCH_TARGET_BYTES {
                    if !batch.is_empty() {
                        let files = std::mem::take(&mut batch);
                        batch_bytes = 0;
                        self.push_batch_task(job, &source, &target, files, &mut tasks)?;
                    }
                    self.push_single_task(
                        job,
                        &path,
                        &task_target,
                        &status,
                        replace_target,
                        &mut tasks,
                    )
                    .await?;
                    continue;
                }
                batch_bytes += status.len;
                batch.push((status, replace_target));
                if batch_should_flush(batch.len(), batch_bytes) {
                    let files = std::mem::take(&mut batch);
                    batch_bytes = 0;
                    self.push_batch_task(job, &source, &target, files, &mut tasks)?;
                }
                continue;
            }

            self.push_single_task(
                job,
                &path,
                &task_target,
                &status,
                replace_target,
                &mut tasks,
            )
            .await?;
        }
        if !batch.is_empty() {
            let files = std::mem::take(&mut batch);
            self.push_batch_task(job, &source, &target, files, &mut tasks)?;
        }

        if grouped_files + fallback_no_identity + fallback_no_capability + fallback_moved > 0 {
            log::info!(
                "transfer {} segment split: grouped_files={} fallback_no_identity={} fallback_no_capability={} fallback_moved={}",
                job.job_id,
                grouped_files,
                fallback_no_identity,
                fallback_no_capability,
                fallback_moved
            );
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
        replace_target: bool,
        block_size: i64,
        mount: &MountInfo,
        tasks: &mut Vec<TransferTaskRecord>,
    ) -> FsResult<Option<SegmentSplitOutcome>> {
        if !task_target.is_cv() {
            self.push_single_task(job, path, task_target, status, replace_target, tasks)
                .await?;
            return Ok(None);
        }
        // One HEAD observation for len/mtime/version together (design D0.4).
        // A UFS-handle failure, or a length that moved since the walked LIST
        // status, both mean the file cannot be grouped and falls back to one
        // task per segment. mtime comes from the same HEAD the worker
        // re-reads: LIST and HEAD timestamps can disagree by truncation on
        // S3-style stores, so a stat identity is only minted from HEAD.
        let (head_len, head_mtime, source_version) = match self.factory.get_ufs(mount) {
            Ok(ufs) => crate::common::ufs_object_fingerprint(&ufs, path).await,
            Err(err) => {
                log::debug!(
                    "segment group fingerprint unavailable for {}: {}",
                    path.full_path(),
                    err
                );
                (0, 0, String::new())
            }
        };
        if head_len > 0 && head_len != status.len {
            log::debug!(
                "segment group source moved during planning for {}: list={} head={}",
                path.full_path(),
                status.len,
                head_len
            );
            self.push_single_task(job, path, task_target, status, replace_target, tasks)
                .await?;
            return Ok(Some(SegmentSplitOutcome::Moved));
        }
        let head = match head_len > 0 && head_mtime > 0 {
            true => Some((head_len, head_mtime)),
            false => None,
        };
        let (plans, outcome) =
            self.build_segment_plans(status, block_size, head, &source_version)?;
        let max_tasks = self.max_tasks();
        if tasks.len().saturating_add(plans.len()) > max_tasks {
            return err_box!("Transfer {} files exceeds {}", job.job_id, max_tasks);
        }
        if task_target.is_cv() {
            let start = Instant::now();
            let target_status = self.master_fs.get_status(task_target).await;
            record_metadata_operation("cv", "get_status", target_status.is_ok(), start);
            match target_status {
                Ok(_) if !overwrite && !replace_target => {
                    return err_box!(
                        "Transfer target {} already exists and overwrite=false",
                        task_target.full_path()
                    );
                }
                Ok(_) => {
                    // Scheduler removes an overwrite target only after the
                    // complete plan has been persisted while the job is still
                    // Planning. Deleting here could lose the old file when a
                    // planner crash leaves no dispatchable task rows.
                }
                Err(FsError::FileNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        let plan_count = plans.len();
        for (plan_json, total_size) in plans {
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
                source_read_plan_json: plan_json,
                report_target_json: String::new(),
                state: TransferTaskState::Pending,
                progress: TransferProgress {
                    total_size,
                    ..Default::default()
                },
                estimated_size: total_size,
                retry_count: 0,
                attempt_started_at: 0,
                accepted_at_ms: 0,
                first_byte_at_ms: 0,
                last_report_at: 0,
                stale_deadline_at: 0,
                updated_at: LocalTime::mills() as i64,
                replace_target,
                block_locations: Vec::new(),
            });
        }
        log::debug!(
            "segment split decision for {}: {:?} tasks={}",
            path.full_path(),
            outcome,
            plan_count
        );
        Ok(Some(outcome))
    }

    fn build_segment_plans(
        &self,
        status: &FileStatus,
        block_size: i64,
        head: Option<(i64, i64)>,
        source_version: &str,
    ) -> FsResult<(Vec<(String, i64)>, SegmentSplitOutcome)> {
        // Grouping needs an identity a worker can re-check at execution time
        // and a live capability proof (design §8#37). Without either, stay on
        // single segments rather than risk a resume that skips rewritten
        // source bytes or a task no worker can validate.
        if !has_group_identity(source_version, head) {
            return build_single_segment_plans(status, block_size, head_mtime(head))
                .map(|plans| (plans, SegmentSplitOutcome::NoIdentity));
        }
        let workers = match self.cache.live_workers() {
            Ok(workers) => workers,
            Err(_) => {
                return build_single_segment_plans(status, block_size, head_mtime(head))
                    .map(|plans| (plans, SegmentSplitOutcome::NoCapability));
            }
        };
        if !supports_segment_grouping(&workers) {
            return build_single_segment_plans(status, block_size, head_mtime(head))
                .map(|plans| (plans, SegmentSplitOutcome::NoCapability));
        }
        // Reaching here means the HEAD length matched the walked status, so
        // `status.len` is the observed source length. mtime is only written
        // when a HEAD observation produced it: a LIST-sourced mtime in the
        // manifest would make the worker's HEAD re-check false-fire on stores
        // where the two APIs disagree, and 0 tells the worker to skip it.
        let source_mtime = head.map(|(_, mtime)| mtime).unwrap_or(0);
        self.build_segment_groups(
            status,
            block_size,
            workers.len(),
            source_mtime,
            source_version,
        )
        .into_iter()
        .map(|group| build_group_plan_json(&group, status.len))
        .collect::<FsResult<Vec<(String, i64)>>>()
        .map(|plans| (plans, SegmentSplitOutcome::Grouped))
    }

    fn build_segment_groups(
        &self,
        status: &FileStatus,
        block_size: i64,
        live_workers: usize,
        source_mtime: i64,
        source_version: &str,
    ) -> Vec<SegmentGroupPlan> {
        plan_segment_groups(
            status.len,
            block_size,
            live_workers,
            status.len,
            source_mtime,
            source_version.to_owned(),
        )
    }

    /// Plans one fs_mode export batch: one single-file task per entry.
    ///
    /// The block lookup that produces a task's read plan is also the version
    /// check: the plan is used only when its status and last block match the
    /// generation the submitter recorded. The worker derives the generation from
    /// that same plan, so the version it stamps and marks is exactly the one
    /// checked here. A stale entry is skipped alone. Segment splitting is never
    /// used: a segment writes the shared target directly, so a failure would
    /// leave a partial object at the final path.
    async fn plan_fs_mode_batch(
        &self,
        job: &TransferJobRecord,
        command: &TransferCommand,
        mount: &MountInfo,
    ) -> FsResult<PlannedTransfer> {
        let job_info = self.load_job_info(job, mount).await?;
        let mounts = self.cache.snapshot().mounts;
        let mut tasks = Vec::new();
        let mut total_size = 0i64;
        let mut superseded = 0usize;

        for file in &command.fs_mode_files {
            let source = Path::from_str(&file.source)?;
            let target = Path::from_str(&file.target)?;
            validate_fs_mode_mapping(mount, &mounts, &source, &target)?;
        }

        for chunk in command
            .fs_mode_files
            .chunks(GET_BLOCK_LOCATIONS_BATCH_MAX_PATHS)
        {
            let mut sources = Vec::with_capacity(chunk.len());
            for file in chunk {
                sources.push(Path::from_str(&file.source)?);
            }
            let lookups = self.fs_mode_block_locations(&sources).await?;

            for ((file, source), lookup) in chunk.iter().zip(&sources).zip(lookups) {
                let blocks = match lookup {
                    Ok(blocks) => blocks,
                    Err(FsError::FileNotFound(_)) => {
                        superseded += 1;
                        continue;
                    }
                    Err(err) => return Err(err),
                };
                if blocks.status.is_dir || FileGen::of_blocks(&blocks) != file.gen {
                    superseded += 1;
                    continue;
                }

                let target = Path::from_str(&file.target)?;
                let plan = serde_json::to_string(&blocks).map_err(|_| {
                    FsError::common(format!(
                        "Unable to prepare the CV read plan for {}",
                        source.full_path()
                    ))
                })?;
                total_size = total_size.saturating_add(blocks.status.len);
                self.push_planned_task(job, source, &target, &blocks.status, plan, &mut tasks)?;
            }
        }

        if superseded > 0 {
            log::info!(
                "fs_mode batch {}: {} of {} entries superseded at planning",
                job.job_id,
                superseded,
                command.fs_mode_files.len()
            );
        }

        Ok(PlannedTransfer {
            job_info,
            tasks,
            total_size,
            skipped_files: superseded,
            skipped_size: 0,
            cv_metadata_epoch: None,
        })
    }

    /// Block layouts for `paths`, one bounded RPC per call when the master
    /// supports it, otherwise one lookup per path.
    async fn fs_mode_block_locations(&self, paths: &[Path]) -> FsResult<Vec<FsResult<FileBlocks>>> {
        let start = Instant::now();
        let batch = self.master_fs.get_block_locations_batch(paths).await;
        record_metadata_operation("cv", "get_block_locations_batch", batch.is_ok(), start);
        if let Some(results) = batch? {
            return Ok(results);
        }
        let mut results = Vec::with_capacity(paths.len());
        for path in paths {
            let start = Instant::now();
            let blocks = self.master_fs.get_block_locations(path).await;
            record_metadata_operation("cv", "get_block_locations", blocks.is_ok(), start);
            results.push(blocks);
        }
        Ok(results)
    }

    /// Push one file with an already prepared read plan.
    fn push_planned_task(
        &self,
        job: &TransferJobRecord,
        path: &Path,
        task_target: &Path,
        status: &FileStatus,
        source_read_plan_json: String,
        tasks: &mut Vec<TransferTaskRecord>,
    ) -> FsResult<()> {
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
            source_read_plan_json,
            report_target_json: String::new(),
            state: TransferTaskState::Pending,
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
            replace_target: false,
            block_locations: Vec::new(),
        });
        let max_tasks = self.max_tasks();
        if tasks.len() > max_tasks {
            return err_box!("Transfer {} files exceeds {}", job.job_id, max_tasks);
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
        replace_target: bool,
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
            replace_target,
            block_locations: Vec::new(),
        });
        let max_tasks = self.max_tasks();
        if tasks.len() > max_tasks {
            return err_box!("Transfer {} files exceeds {}", job.job_id, max_tasks);
        }
        Ok(())
    }

    fn push_batch_task(
        &self,
        job: &TransferJobRecord,
        source_root: &Path,
        target_root: &Path,
        files: Vec<(FileStatus, bool)>,
        tasks: &mut Vec<TransferTaskRecord>,
    ) -> FsResult<()> {
        let entries: Vec<BatchManifestEntry> = files
            .iter()
            .map(|(file, replace_target)| {
                let file_path = Path::from_str(&file.path)?;
                // Relative path from the source root to the file.
                Ok(BatchManifestEntry {
                    rel_path: relative_path_from(source_root, &file_path)?,
                    size: file.len,
                    replace_target: *replace_target,
                })
            })
            .collect::<FsResult<Vec<_>>>()?;
        let manifest = BatchManifest { batch: entries };
        let total: i64 = files.iter().map(|(file, _)| file.len).sum();
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
            replace_target: false,
            block_locations: Vec::new(),
        });
        let max_tasks = self.max_tasks();
        if tasks.len() > max_tasks {
            return err_box!("Transfer {} files exceeds {}", job.job_id, max_tasks);
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

    pub(crate) async fn load_job_info(
        &self,
        job: &TransferJobRecord,
        mount: &MountInfo,
    ) -> FsResult<LoadJobInfo> {
        let mount_dir_replicas = if job.kind == TransferKind::Load {
            self.mount_dir_replicas(mount).await?
        } else {
            None
        };
        load_job_info(job, mount, mount_dir_replicas, &self.client_conf)
    }

    /// Real replica count of the mount-point directory inode, read from master
    /// metadata. This is the ground truth the incremental-load replica check
    /// compares against — not the client-side default configuration. A
    /// transient master error propagates (planning retries) instead of
    /// silently switching the baseline to the client default, which would
    /// misjudge every file as mismatched and trigger a full re-copy.
    async fn mount_dir_replicas(&self, mount: &MountInfo) -> FsResult<Option<i32>> {
        let path = Path::from_str(&mount.cv_path)?;
        match self.get_status(&path, mount).await {
            Ok(status) => Ok((status.replicas > 0).then_some(status.replicas)),
            // The mount-point directory is gone; nothing authoritative left to
            // compare against, fall back to the client default.
            Err(FsError::FileNotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
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
            let start = Instant::now();
            let result = self
                .master_fs
                .list_options(
                    path,
                    ListOptions {
                        limit: Some(CV_LIST_PAGE_SIZE),
                        start_after: start_after.take(),
                    },
                )
                .await;
            record_metadata_operation("cv", "list_options", result.is_ok(), start);
            let page = result?;
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

    /// An incrementally-unchanged target is only skippable when its cached
    /// blocks still have live locations. A shell target (allocated blocks
    /// with empty locations, e.g. after a worker restart dropped the only
    /// replica without triggering lost-worker invalidation) must be
    /// re-copied; a failed location query also forces a re-copy.
    async fn target_cache_readable(&self, path: &Path) -> bool {
        let start = Instant::now();
        let blocks = self.master_fs.get_block_locations(path).await;
        record_metadata_operation("cv", "get_block_locations", blocks.is_ok(), start);
        match blocks {
            Ok(blocks) if located_blocks_readable(&blocks.block_locs) => true,
            Ok(_) => {
                log::warn!(
                    "incremental load re-copies {}: cached blocks have no live locations",
                    path.full_path()
                );
                false
            }
            Err(e) => {
                log::warn!(
                    "incremental load re-copies {}: get block locations failed: {}",
                    path.full_path(),
                    e
                );
                false
            }
        }
    }
}

/// A target with allocated blocks but no live locations is an unusable cache
/// shell: metadata looks complete while every cached byte is gone. Matches the
/// master's `cache_unreadable` semantics — partial replicas still count as
/// readable.
pub(crate) fn located_blocks_readable(block_locs: &[LocatedBlock]) -> bool {
    block_locs.iter().all(|l| !l.locs.is_empty())
}

pub(crate) fn unchanged_load_target(source: &FileStatus, target: Option<&FileStatus>) -> bool {
    let Some(target) = target else {
        return false;
    };
    !target.is_dir && target.is_complete() && target.cv_valid(Some(source))
}

/// A cached target whose replica count diverges from the job's replica setting
/// (the mount's replicas unless the command overrode it) must be re-copied even
/// when its content is unchanged, so the configured replicas are enforced.
pub(crate) fn load_replica_mismatch(target: Option<&FileStatus>, replicas: i32) -> bool {
    target.is_some_and(|target| !target.is_dir && target.replicas != replicas)
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
    mount_dir_replicas: Option<i32>,
    client_conf: &ClientConf,
) -> FsResult<LoadJobInfo> {
    // Replica precedence: explicit command option, mount config, then the real
    // replica count of the mount-point directory inode in master metadata. The
    // client default only covers the case where master metadata is unreachable.
    let default_replicas = mount
        .replicas
        .or(mount_dir_replicas)
        .unwrap_or(client_conf.replicas);
    let mut source_type = JobSourceType::default();
    let (replicas, overwrite) = match transfer_command(job) {
        Ok(command) => {
            let replicas = command
                .replicas()
                .ok()
                .flatten()
                .unwrap_or(default_replicas);
            // Tells the worker to derive the generation from the task's read
            // plan, stamp it on the object and mark conditionally.
            if command.is_fs_mode_batch() {
                source_type = JobSourceType::FsModeAuto;
            }
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
        source_type,
        // The transfer job this planned load task belongs to.
        transfer_job_id: Some(job.job_id.clone()),
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
        let elapsed_us = start.elapsed().as_micros();
        let result = if success { "success" } else { "error" };
        metrics.observe_metadata_operation(source, operation, result, elapsed_us);
        if source == "cv" && operation != "list_status" {
            metrics.observe_master_rpc("planning", operation, result, elapsed_us);
        }
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
            terminal_summary: None,
            terminal_counts: None,
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

        let info = load_job_info(&job(command), &mount, None, &client_conf).unwrap();
        assert_eq!(info.replicas, 3);
        // Planned load tasks stay associated with their transfer job so the
        // master job status can deep link back to it.
        assert_eq!(info.transfer_job_id.as_deref(), Some("job-1"));
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
        let info = load_job_info(&job(command), &mount, None, &ClientConf::default()).unwrap();
        assert_eq!(info.replicas, 2);
    }

    #[test]
    fn load_job_info_falls_back_to_mount_dir_inode_before_client_conf() {
        let command = TransferCommand {
            kind: TransferKind::Load,
            source_path: "s3://bucket/a".to_string(),
            target_path: "/mnt/a".to_string(),
            ..Default::default()
        };
        let mount = MountInfo {
            replicas: None,
            ..Default::default()
        };

        // Master metadata (mount-point directory inode) wins over client conf.
        let info = load_job_info(
            &job(command.clone()),
            &mount,
            Some(5),
            &ClientConf {
                replicas: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(info.replicas, 5);

        // Client conf only covers an unreachable master metadata lookup.
        let info = load_job_info(
            &job(command),
            &mount,
            None,
            &ClientConf {
                replicas: 1,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(info.replicas, 1);
    }
}
