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
use curvine_client::unified::{UfsFileSystem, UnifiedReader, UnifiedWriter};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{
    CreateFileOptsBuilder, FileAllocOpts, JobTaskState, SetAttrOptsBuilder,
};
use curvine_common::FsResult;
use log::{error, info, warn};
use orpc::common::{LocalTime, TimeSpent};
use orpc::err_box;
use orpc::runtime::RpcRuntime;
use orpc::sys::DataSlice;
use std::sync::Arc;

pub struct LoadTaskRunner {
    task: Arc<TaskContext>,
    fs: CurvineFileSystem,
    factory: Arc<UfsFactory>,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
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

    pub async fn run(&self) {
        if let Err(e) = self.run0().await {
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
        if self.task.is_cancel() {
            info!("task {} was cancelled", self.task.info.task_id);
            return Ok(());
        }
        self.task
            .update_state(JobTaskState::Loading, "Task started");

        // Fast path: UFS(e.g. S3) -> Curvine of a large file. A single OpenDAL
        // reader does not scale by cranking its internal `.concurrent(N)`
        // prefetch alone -- that knob only hides single-connection latency (a
        // small value like 2 is enough to saturate one connection); pushing it
        // higher plateaus because one reader still commits through one writer.
        // The way to scale is N *independent* streams, each an independent reader
        // pulling a disjoint byte range into its own writer, which scale
        // near-linearly (measured on BOS S3, single node: 8 streams ~1.36 GB/s
        // vs ~215 MB/s single stream, ~6x). So for a big UFS->CV load we fan out
        // into N independent readers writing to N contiguous regions, instead of
        // one serial stream.
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;
        if !source_path.is_cv() && target_path.is_cv() && self.max_parallel_streams() > 1 {
            // Probe source length cheaply, then let effective_streams() decide
            // the fan-out width from file size. A small file yields 1 stream, so
            // we fall through to the serial path below.
            let src_len = self.get_ufs()?.get_status(&source_path).await?.len;
            if self.effective_streams(src_len) > 1 {
                return self.run_parallel(&source_path, &target_path, src_len).await;
            }
        }

        let (mut reader, mut writer) = self.create_stream().await?;
        if self.task.is_cancel() {
            info!("task {} was cancelled", self.task.info.task_id);
            return Ok(());
        }
        let mut last_progress_time = LocalTime::mills();
        let mut read_cost_ms = 0;
        let mut total_cost_ms = 0;

        // Pipelined copy: overlap "read next chunk from source" with "write the
        // previously-read chunk to the target". The serial version read and wrote
        // strictly alternately, so a slow high-latency source (e.g. S3 over a
        // single connection) idled the writer and vice versa, capping throughput
        // at ~1/(1/read + 1/write). By prefetching the next chunk concurrently
        // with the current write via try_join!, effective throughput approaches
        // max(read, write) instead of their harmonic-style sum. This needs no
        // extra task/thread: both futures are polled on the same task.
        let mut pending = reader.async_read(None).await?;

        loop {
            if self.task.is_cancel() {
                info!("task {} was cancelled", self.task.info.task_id);
                return Ok(());
            }

            if pending.is_empty() {
                break;
            }

            let spend = TimeSpent::new();
            // Read the next chunk while writing the current one.
            let (next, ()) = tokio::try_join!(
                reader.async_read(None),
                writer.async_write(std::mem::replace(&mut pending, DataSlice::Empty)),
            )?;
            read_cost_ms += spend.used_ms();
            total_cost_ms += spend.used_ms();
            pending = next;

            if LocalTime::mills() > last_progress_time + self.progress_interval_ms {
                last_progress_time = LocalTime::mills();
                self.update_progress(writer.pos(), reader.len(), false)
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

        writer.complete().await?;
        reader.complete().await?;

        let (cv_path, ufs_mtime) = if writer.path().is_cv() {
            // ufs -> cv
            (writer.path(), reader.status().mtime)
        } else {
            // cv -> ufs
            let ufs_status = self.get_ufs()?.get_status(writer.path()).await?;
            (reader.path(), ufs_status.mtime)
        };

        let attr_opts = SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime).build();
        self.fs.set_attr(cv_path, attr_opts).await?;

        self.update_progress(writer.pos(), reader.len(), true).await;

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

    /// Upper bound on the number of independent reader+writer streams a large
    /// UFS->CV load may fan out into. Read from the mount property
    /// `load_task.parallel_streams` (per-bucket tunable), defaulting to 8.
    /// Clamped to >= 1. This is a CAP: the actual stream count is derived from
    /// file size by [`Self::effective_streams`].
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

    /// Minimum number of bytes a single stream must be responsible for before
    /// fanning out is worthwhile. Each extra stream carries fixed setup cost
    /// (UFS open, open_for_write RPC, per-block add_block, complete) that does
    /// NOT shrink with file size, so a stream only pays off once it moves at
    /// least this many bytes. Configurable via the mount property
    /// `load_task.min_bytes_per_stream` (per-bucket tunable), defaulting to
    /// 256 MiB. Clamped to >= 1.
    ///
    /// This subsumes the old boolean `load_task.parallel_threshold`: a file
    /// smaller than this value yields `effective_streams == 1`, i.e. no fan-out.
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

    /// Derive the stream count for a load of `src_len` bytes, growing the count
    /// with file size instead of always jumping to the cap. Fixed per-stream
    /// setup cost (opens/RPCs/complete) is independent of file size, so a
    /// mid-size file over-parallelized to the full cap can be slower than a
    /// smaller fan-out. We give each stream at least `min_bytes_per_stream` and
    /// clamp to `[1, max_parallel_streams]`:
    ///
    ///   effective = clamp(src_len / min_bytes_per_stream, 1, cap)
    ///
    /// Examples (min=256 MiB, cap=8): 100 MiB -> 1 stream (no fan-out),
    /// 512 MiB -> 2, 1 GiB -> 4, 2 GiB -> 8, 200 GiB -> 8 (capped).
    fn effective_streams(&self, src_len: i64) -> usize {
        Self::stream_count(
            src_len,
            self.min_bytes_per_stream(),
            self.max_parallel_streams(),
        )
    }

    /// Pure fan-out width math (extracted for unit testing): give each stream at
    /// least `min_bytes` bytes, clamp to `[1, cap]`. `min_bytes` and `cap` are
    /// treated as >= 1.
    fn stream_count(src_len: i64, min_bytes: i64, cap: usize) -> usize {
        let min_bytes = min_bytes.max(1);
        let cap = cap.max(1);
        let by_size = (src_len / min_bytes).max(0) as usize;
        by_size.clamp(1, cap)
    }

    /// Compute the per-stream segment length for a parallel load.
    ///
    /// The segment is `ceil(src_len / streams)` rounded UP to a whole number of
    /// `block_size` blocks. Block alignment is a correctness requirement: each
    /// stream gets its own writer, and the block is Curvine's unit of allocation
    /// and commit — if two streams' ranges shared a block they would race on that
    /// block. Rounding up guarantees stream `i` owns `[i*seg, (i+1)*seg)` with a
    /// block boundary at every `seg`, so block sets are disjoint.
    ///
    /// Returns a value that is always >= `block_size` and a multiple of it (so
    /// callers must still clamp the last segment to `src_len`). `streams` and
    /// `block_size` are treated as >= 1.
    fn segment_len(src_len: i64, streams: usize, block_size: i64) -> i64 {
        let streams = streams.max(1) as i64;
        let block_size = block_size.max(1);
        let raw = (src_len + streams - 1) / streams;
        let aligned = ((raw + block_size - 1) / block_size) * block_size;
        aligned.max(block_size)
    }

    /// Per-`async_read` request size within a stream. Capped so a single read
    /// doesn't try to buffer a whole (potentially multi-GiB) segment at once.
    const READ_CHUNK_BYTES: i64 = 16 * 1024 * 1024;

    /// Fan a large UFS->CV load into N independent streams, each of which opens
    /// its OWN reader AND its OWN writer for a disjoint, block-aligned byte range
    /// of the file, then reads-from-UFS and writes-to-Curvine that range end to
    /// end. This is "multi-read + multi-write":
    ///
    /// - Read side: N independent UFS readers scale near-linearly (a single
    ///   OpenDAL reader's internal `.concurrent()` prefetch does not).
    /// - Write side: N independent Curvine writers scale near-linearly too; a
    ///   single writer tops out around one block-writer's throughput.
    ///
    /// Correctness relies on three Curvine properties:
    /// 1. `resize(src_len)` up front allocates every block, giving each full
    ///    block length `block_size` and the final block the remainder, so
    ///    `compute_len()` already sums to exactly `src_len`. The master-side
    ///    `complete()` length check (`compute_len == file.len`) therefore holds
    ///    no matter which writer commits when.
    /// 2. Segments are rounded up to a whole number of `block_size` blocks, so no
    ///    two writers ever touch the same block (the unit of allocation/commit).
    /// 3. Curvine natively supports multiple concurrent writers on one file
    ///    (`add_block` is idempotent on already-allocated blocks).
    async fn run_parallel(
        &self,
        source_path: &Path,
        target_path: &Path,
        src_len: i64,
    ) -> FsResult<()> {
        let streams = self.effective_streams(src_len);
        let block_size = self.task.info.job.block_size.max(1);
        let seg = Self::segment_len(src_len, streams, block_size);
        let spend = TimeSpent::new();
        // Absolute deadline shared by every stream, so a single hung stream is
        // caught mid-segment instead of only after the whole (multi-GiB) segment
        // finishes. Same budget as the outer join-loop timeout check.
        let deadline_ms = LocalTime::mills() + self.task_timeout_ms;

        // Pre-create + resize the target to src_len so ALL blocks are allocated
        // up front (see property 1 above). We drop the owner writer WITHOUT
        // completing it: completing would clear the file's write feature; we only
        // needed the resize to allocate blocks (they persist in master meta).
        {
            let mut owner = self.create_unified(target_path).await?;
            owner.resize(FileAllocOpts::with_truncate(src_len)).await?;
            drop(owner);
        }

        // Fan out: each stream reads its range from UFS and writes it to the
        // (already allocated) target region with its own reader + writer.
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
                // open_for_write(overwrite=false): attach as an additional writer
                // to the existing (resized) file without truncating it.
                let mut writer = fs.open_for_write(&dst, false).await?;
                writer.seek(off).await?;

                let mut remaining = len;
                while remaining > 0 {
                    // Honor cancellation and the shared deadline INSIDE the
                    // segment loop: a segment can be multiple GiB, so checking
                    // only between segments would let a canceled/timed-out job
                    // keep reading from UFS and committing blocks. On early exit
                    // cancel the writer so it does not complete() a partial
                    // segment onto the pre-resized target.
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
                // Guard against silent short writes: if the source returned EOF
                // before its advertised length, the file would be left with a
                // hole in this segment. Fail loudly instead of committing partial
                // data that would later read back as corrupt.
                if remaining != 0 {
                    let _ = writer.cancel().await;
                    return err_box!(
                        "short read on segment [{}, {}): {} bytes missing (source shorter than stat len?)",
                        off,
                        off + len,
                        remaining
                    );
                }
                // All writers share one FsContext client_name, so whichever
                // stream completes first clears the file's write feature while
                // other streams may still be committing blocks. This is safe
                // ONLY because of correctness property (1) above: resize()
                // pre-allocated every block so compute_len == file.len already
                // holds regardless of commit order. Do NOT "fix" this into
                // per-stream client IDs -- it is intentional.
                writer.complete().await?;
                reader.complete().await?;
                FsResult::Ok(len)
            }));
        }

        // Join all streams. On ANY early exit (cancel, segment error, timeout)
        // abort the handles we have not yet awaited: a task blocked in a UFS read
        // never observes is_cancel() on its own, so without this it could keep
        // reading and complete() onto the target after the job is done/failed.
        let mut written: i64 = 0;
        for idx in 0..handles.len() {
            if self.task.is_cancel() {
                info!("task {} was cancelled", self.task.info.task_id);
                Self::abort_remaining(&handles, idx);
                return Ok(());
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

        // ufs -> cv: stamp the source mtime onto the cached file (cache validity).
        let ufs_mtime = self.get_ufs()?.get_status(source_path).await?.mtime;
        let attr_opts = SetAttrOptsBuilder::new().ufs_mtime(ufs_mtime).build();
        self.fs.set_attr(target_path, attr_opts).await?;

        self.update_progress(written, src_len, true).await;

        info!(
            "task {} completed (parallel x{}), source_path {}, target_path {}, ufs_mtime:{}, copy bytes {}, task cost {} ms",
            self.task.info.task_id,
            streams,
            self.task.info.source_path,
            self.task.info.target_path,
            ufs_mtime,
            written,
            spend.used_ms(),
        );

        Ok(())
    }

    /// Abort every not-yet-awaited stream handle in `handles[from..]`. Called on
    /// any early exit from the join loop so background streams stop reading UFS
    /// and never complete() onto the target after the job is done/failed. Tokio
    /// abort is best-effort (it fires at the next await point); combined with the
    /// in-loop is_cancel()/deadline checks this bounds how long a stream can run
    /// past the parent's decision to stop.
    fn abort_remaining(handles: &[orpc::runtime::JoinHandle<FsResult<i64>>], from: usize) {
        for h in handles.iter().skip(from) {
            h.abort();
        }
    }

    async fn create_stream(&self) -> FsResult<(UnifiedReader, UnifiedWriter)> {
        let source_path = Path::from_str(&self.task.info.source_path)?;
        let target_path = Path::from_str(&self.task.info.target_path)?;

        // Create reader (automatically selects filesystem based on scheme)
        let reader = self.open_unified(&source_path).await?;

        // Create writer (automatically selects filesystem based on scheme)
        let writer = self.create_unified(&target_path).await?;

        Ok((reader, writer))
    }

    async fn open_unified(&self, path: &Path) -> FsResult<UnifiedReader> {
        if path.is_cv() {
            let reader = self.fs.open(path).await?;
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
                warn!("UFS file already exists, skipping: {}", path.full_path());
                return err_box!("File exists and overwrite=false");
            }

            match ufs.create(path, overwrite).await {
                Ok(writer) => Ok(writer),
                Err(FsError::FileNotFound(_)) => {
                    if let Some(parent) = path.parent()? {
                        ufs.mkdir(&parent, true).await?;
                    }
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
            warn!("update progress failed, err: {:?}", e);
        }
    }

    pub async fn update_progress0(
        &self,
        loaded_size: i64,
        total_size: i64,
        is_last: bool,
    ) -> FsResult<()> {
        let progress = self.task.update_progress(loaded_size, total_size, is_last);
        let task = &self.task;

        self.master_client
            .report_task(&task.info.job.job_id, &task.info.task_id, progress)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::LoadTaskRunner;

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
        // 10 GiB, 8 streams, 4 MiB blocks: ceil(10Gi/8)=1280MiB, already aligned.
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
        // Non-block-multiple raw segment must round UP so writers never share a block.
        // src=100MiB, 3 streams => raw ceil ≈ 33.34MiB; rounded up to a 4MiB
        // multiple that is 36MiB (9 blocks).
        let seg = LoadTaskRunner::segment_len(100 * MB, 3, 4 * MB);
        assert_eq!(seg, 36 * MB);
        assert_eq!(seg % (4 * MB), 0);
    }

    #[test]
    fn segment_len_never_below_block_size() {
        // Tiny file, many streams: segment must not collapse to 0.
        let seg = LoadTaskRunner::segment_len(1, 8, 4 * MB);
        assert_eq!(seg, 4 * MB);
    }

    #[test]
    fn segment_len_handles_zero_streams_and_block() {
        // Defensive: streams/block_size are clamped to >= 1, no divide-by-zero.
        assert_eq!(LoadTaskRunner::segment_len(1000, 0, 0), 1000);
    }

    #[test]
    fn plan_ranges_are_contiguous_disjoint_and_cover_whole_file() {
        // The critical invariant: the union of all stream ranges must be exactly
        // [0, src_len) with no gaps and no overlaps (else data is lost/corrupted).
        for &(src_len, streams, block_size) in &[
            (10 * 1024 * MB, 8, 4 * MB),
            (100 * MB, 3, 4 * MB),
            (7 * MB + 123, 4, 4 * MB), // not block-aligned total
            (4 * MB, 8, 4 * MB),       // fewer effective streams than requested
            (1, 8, 4 * MB),            // tiny file -> single stream
        ] {
            let ranges = plan(src_len, streams, block_size);
            assert!(!ranges.is_empty(), "must produce at least one range");
            // First starts at 0.
            assert_eq!(ranges[0].0, 0);
            // Contiguous + disjoint.
            let mut expected_off = 0;
            for (off, len) in &ranges {
                assert_eq!(*off, expected_off, "gap/overlap at {}", off);
                assert!(*len > 0, "empty segment");
                expected_off += len;
            }
            // Covers exactly the whole file.
            assert_eq!(
                expected_off, src_len,
                "ranges must cover exactly [0,{})",
                src_len
            );
            // Every non-final segment start is block-aligned (disjoint block sets).
            for (off, _) in &ranges {
                assert_eq!(*off % block_size, 0, "segment start not block-aligned");
            }
        }
    }

    #[test]
    fn plan_does_not_over_allocate_streams_for_small_files() {
        // 4 MiB file with 8 requested streams and 4 MiB blocks: only 1 stream
        // should actually get a range (the rest would start at/after EOF).
        let ranges = plan(4 * MB, 8, 4 * MB);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 4 * MB));
    }

    #[test]
    fn stream_count_grows_with_size_and_caps() {
        let min = 256 * MB;
        let cap = 8;
        // Below min_bytes -> single stream (no fan-out), subsumes old threshold.
        assert_eq!(LoadTaskRunner::stream_count(100 * MB, min, cap), 1);
        assert_eq!(LoadTaskRunner::stream_count(min - 1, min, cap), 1);
        // Grows linearly with size.
        assert_eq!(LoadTaskRunner::stream_count(512 * MB, min, cap), 2);
        assert_eq!(LoadTaskRunner::stream_count(1024 * MB, min, cap), 4);
        assert_eq!(LoadTaskRunner::stream_count(2048 * MB, min, cap), 8);
        // Clamped at the cap for very large files.
        assert_eq!(LoadTaskRunner::stream_count(200 * 1024 * MB, min, cap), 8);
    }

    #[test]
    fn stream_count_defensive_bounds() {
        // Zero/negative length -> 1 (never 0), zero min_bytes/cap clamped to 1.
        assert_eq!(LoadTaskRunner::stream_count(0, 256 * MB, 8), 1);
        assert_eq!(LoadTaskRunner::stream_count(-5, 256 * MB, 8), 1);
        assert_eq!(LoadTaskRunner::stream_count(1024 * MB, 0, 8), 8);
        assert_eq!(LoadTaskRunner::stream_count(1024 * MB, 256 * MB, 0), 1);
    }
}
