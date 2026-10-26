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

use crate::file::{FsContext, FsReaderBase, FsReaderParallel, ReadDetector};
use crate::{FileChunk, FileSlice};
use curvine_core_error::err_box;
use curvine_error::FsResult;
use curvine_error::{ErrorKind, FsError};
use curvine_fs_api::Path;
use curvine_io::DataSlice;
use curvine_model::FileBlocks;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::channel::{
    AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender,
};
use curvine_runtime::sync::ErrorMonitor;
use log::{debug, error};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Permit;

// Control task type
enum ReadTask {
    Seek(i64, CallSender<i8>),
    Stop(CallSender<i8>),
    Pause((i64, bool)),
}

enum SelectTask<'a> {
    Control(ReadTask),
    Permit(Permit<'a, FileChunk>),
}

struct PrefetchArgs {
    rt: Arc<Runtime>,
    reader: FsReaderParallel,
    chunk_sender: AsyncSender<FileChunk>,
    task_receiver: AsyncReceiver<ReadTask>,
    produced_end: Arc<AtomicI64>,
}

// Take the first chunk that covers or passes `pos`, discarding any chunk whose
// tail is <= `pos` (residual data left after an absorbed seek). Used only after
// an absorb fired (BufferChannel::needs_drain): in steady state the plain
// `recv_check` path is kept, so a chunk arriving wholly below pos without a
// prior absorb still trips the "read data error" corruption guard upstream,
// byte-identical to the original behavior at window=0. Empty EOF sentinel
// chunks pass through unchanged.
async fn recv_skipping_below(recv: &mut AsyncReceiver<FileChunk>, pos: i64) -> FsResult<FileChunk> {
    loop {
        let chunk = recv.recv_check().await?;
        if chunk.is_empty() || chunk.off + chunk.len() as i64 > pos {
            return Ok(chunk);
        }
    }
}

// A parallel task description structure
// chunk_receiver: accept data
// task_sender: Send control command
struct BufferChannel {
    prefetch: Option<PrefetchArgs>,
    chunk_receiver: AsyncReceiver<FileChunk>,
    task_sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    produced_end: Arc<AtomicI64>,
    // True between a successful absorb and the next full seek: only then may
    // read() drain residual below-pos chunks. Outside absorbed seeks, chunks
    // wholly below pos must still trip the upstream "read data error" guard.
    needs_drain: bool,
    // Test-only failure injection for the resilient flip-seek loop: when set,
    // BufferChannel::seek fails before touching the lane, simulating a
    // mid-loop lane failure.
    #[cfg(test)]
    fail_seek: bool,
}

impl BufferChannel {
    fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    fn start_prefetch(&mut self) {
        let Some(args) = self.prefetch.take() else {
            return;
        };

        let monitor = self.err_monitor.clone();
        let parallel_id = args.reader.parallel_id();

        args.rt.spawn(async move {
            let res = FsReaderBuffer::read_future(
                args.chunk_sender,
                args.task_receiver,
                args.reader,
                args.produced_end,
            )
            .await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    if matches!(e.kind(), ErrorKind::FileNotFound) {
                        debug!("buffer read(parallel id {}) error: {:?}", parallel_id, e);
                    } else {
                        error!("buffer read(parallel id {}) error: {:?}", parallel_id, e);
                    }
                    monitor.set_error(e);
                }
            }
        });
    }

    async fn read(&mut self, want_pos: i64) -> FsResult<FileChunk> {
        self.start_prefetch();

        if self.needs_drain {
            // An absorbed seek may have left residual chunks wholly below pos;
            // skip them. The flag is cleared only once this drain cycle
            // COMPLETES (the returned chunk covers pos, or is the EOF
            // sentinel): clearing it before the receive would lose the drain
            // on a cancelled read future (residual chunks still queued, next
            // read bypasses draining and trips "read data error"); keeping it
            // armed until completion lets a cancelled read resume draining on
            // retry. From completion on, the lane is contiguous above pos, so
            // the fail-fast alignment guard is re-armed. Leaving the flag set
            // for the whole Sequential phase would let a lane that later
            // produces from a stale offset without a BufferChannel::seek
            // (error-retry, restart, refactor) be silently drained into a hang
            // instead of tripping "read data error". A subsequent absorb
            // re-arms the flag.
            let chunk = recv_skipping_below(&mut self.chunk_receiver, want_pos)
                .await
                .map_err(|e| self.check_error(e))?;
            self.needs_drain = false;
            Ok(chunk)
        } else {
            self.chunk_receiver
                .recv_check()
                .await
                .map_err(|e| self.check_error(e))
        }
    }

    fn can_absorb(&self, pos: i64) -> bool {
        pos <= self.produced_end.load(Ordering::Acquire)
    }

    // Record that a forward seek was absorbed against this lane: reads that
    // follow may need to drain residual chunks left below the new position.
    fn mark_absorbed(&mut self) {
        self.needs_drain = true;
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        #[cfg(test)]
        if self.fail_seek {
            return err_box!("injected lane seek failure for tests");
        }
        self.start_prefetch();

        let fun = async {
            // Notify seek and seek will pause data reading.
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(ReadTask::Seek(pos, tx)).await?;
            rx.receive().await?;

            // Clear the buffer data to remove old prefetched data before seek.
            // Both random and sequential reads need to clear buffer after seek,
            // because the prefetched data may be from the old position.
            while self.chunk_receiver.try_recv()?.is_some() {}

            // Reset the produced watermark: the task is now paused and a stale
            // high watermark must not let can_absorb false-accept (the covering
            // chunk would never arrive, deadlocking the read). This drain+reset
            // is also the hygiene point for the Sequential->Random flip seek:
            // with absorption enabled the random phase repositions only the
            // base reader, so this is the one place lane channels are emptied
            // and watermarks zeroed before lanes sit idle.
            self.produced_end.store(0, Ordering::Release);
            self.needs_drain = false;

            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn complete(&mut self) -> FsResult<()> {
        if self.prefetch.is_some() {
            // Prefetch task was never started, nothing to stop.
            return Ok(());
        }

        let fun = async {
            // Send a stop command and wait for the command to complete
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(ReadTask::Stop(tx)).await?;
            rx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn pause(&self, pos: i64, pause: bool) -> FsResult<()> {
        if self.prefetch.is_some() {
            return Ok(());
        }
        let fun = async { self.task_sender.send(ReadTask::Pause((pos, pause))).await };
        fun.await.map_err(|e| self.check_error(e))
    }
}

#[allow(clippy::large_enum_variant)]
enum ReaderAdapter {
    Buffer(BufferChannel),
    Base(FsReaderParallel),
}

impl ReaderAdapter {
    async fn read(&mut self, pos: i64) -> FsResult<FileChunk> {
        match self {
            ReaderAdapter::Buffer(r) => r.read(pos).await,
            ReaderAdapter::Base(r) => r.read().await,
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            ReaderAdapter::Buffer(r) => r.seek(pos).await,
            ReaderAdapter::Base(r) => r.seek(pos).await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            ReaderAdapter::Buffer(r) => r.complete().await,
            ReaderAdapter::Base(r) => r.complete().await,
        }
    }

    async fn pause(&mut self, pos: i64, pause: bool) -> FsResult<()> {
        match self {
            ReaderAdapter::Buffer(r) => r.pause(pos, pause).await,
            ReaderAdapter::Base(r) => r.seek(pos).await,
        }
    }
}

// Reader with buffer.
pub struct FsReaderBuffer {
    readers: Vec<ReaderAdapter>,
    base_reader_index: usize,
    path: Path,
    pos: i64,
    len: i64,

    slice_size: i64,
    absorb_window: i64,

    read_detector: ReadDetector,
}

impl FsReaderBuffer {
    pub fn new(
        path: Path,
        fs_context: Arc<FsContext>,
        file_blocks: FileBlocks,
        read_detector: ReadDetector,
    ) -> FsResult<Self> {
        let rt = fs_context.clone_runtime();
        let err_monitor = Arc::new(ErrorMonitor::new());

        let conf = &fs_context.conf.client;
        let chunk_num = conf.read_chunk_num;
        let chunk_size = conf.read_chunk_size;
        let slice_size = conf.read_slice_size;
        let absorb_window = conf.seek_absorb_window;

        let pos = 0;
        let len = file_blocks.status.len;

        let base = FsReaderParallel::from_base(
            FsReaderBase::new(path.clone(), fs_context.clone(), file_blocks.clone()),
            read_detector.read_parallel() as usize,
            slice_size,
            vec![FileSlice::new(0, len)],
            file_blocks.status.id,
        );

        let all = FsReaderParallel::create_all(
            path.clone(),
            fs_context,
            file_blocks,
            read_detector.read_parallel(),
            slice_size,
            chunk_size,
        )?;

        let mut readers = Vec::with_capacity(all.len() + 1);
        for reader in all {
            let reader = if chunk_num == 1 {
                ReaderAdapter::Base(reader)
            } else {
                let (chunk_sender, chunk_receiver) = AsyncChannel::new(chunk_num).split();
                let (task_sender, task_receiver) = AsyncChannel::new(2).split();
                let produced_end = Arc::new(AtomicI64::new(0));
                let channel = BufferChannel {
                    prefetch: Some(PrefetchArgs {
                        rt: rt.clone(),
                        reader,
                        chunk_sender,
                        task_receiver,
                        produced_end: produced_end.clone(),
                    }),
                    chunk_receiver,
                    task_sender,
                    err_monitor: err_monitor.clone(),
                    produced_end,
                    needs_drain: false,
                    #[cfg(test)]
                    fail_seek: false,
                };
                ReaderAdapter::Buffer(channel)
            };
            readers.push(reader);
        }

        let base_reader_index = readers.len();
        readers.push(ReaderAdapter::Base(base));

        let reader = Self {
            readers,
            base_reader_index,
            path,
            pos,
            len,
            slice_size,
            absorb_window,
            read_detector,
        };
        Ok(reader)
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn select_reader_index(
        is_random: bool,
        pos: i64,
        slice_size: i64,
        read_parallel: i64,
        base_reader_index: usize,
    ) -> Option<usize> {
        if is_random {
            return Some(base_reader_index);
        }

        if slice_size <= 0 || read_parallel <= 0 {
            return None;
        }

        Some((pos / slice_size % read_parallel) as usize)
    }

    fn get_reader(&mut self) -> FsResult<&mut ReaderAdapter> {
        let Some(id) = Self::select_reader_index(
            self.read_detector.is_random(),
            self.pos,
            self.slice_size,
            self.read_detector.read_parallel(),
            self.base_reader_index,
        ) else {
            return err_box!(
                "reader is not initialized: pos={}, slice_size={}, read_parallel={}, base_reader_index={}",
                self.pos,
                self.slice_size,
                self.read_detector.read_parallel(),
                self.base_reader_index
            );
        };

        match self.readers.get_mut(id) {
            Some(v) => Ok(v),
            None => err_box!("reader {} is not initialized", id),
        }
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::Empty);
        }

        let pos = self.pos;
        let reader = self.get_reader()?;
        let mut chunk = reader.read(pos).await?;

        // Handle data alignment issues.
        // The chunk read by the underlying reader may be aligned according to chunk_size,
        // so when returning data, you need to discard the excess data
        let diff = self.pos - chunk.off;
        let bytes = if diff == 0 {
            chunk.data
        } else if diff > 0 && diff <= chunk.len() as i64 {
            chunk.data.split_off(diff as usize)
        } else {
            return err_box!(
                "read data error: chunk offset {}, pos {}, diff {}",
                chunk.off,
                self.pos,
                diff
            );
        };

        let start_pos = self.pos;
        self.pos += bytes.len() as i64;

        let is_changed = self
            .read_detector
            .record_read(start_pos, self.pos, &self.path);

        if is_changed && self.read_detector.is_sequential() {
            for reader in &mut self.readers {
                reader.pause(self.pos, false).await?;
            }
        }

        Ok(bytes)
    }

    fn try_absorb(&mut self, pos: i64) -> bool {
        // Absorption requires smart prefetch: with the detector disabled its
        // bookkeeping no-ops and the absorb accounting would be meaningless.
        if !self.read_detector.enabled
            || self.absorb_window <= 0
            || pos - self.pos > self.absorb_window
        {
            return false;
        }
        // Absorption only marks the DESTINATION lane for draining. A seek
        // that crosses a slice boundary skips data in other lanes' queues
        // (e.g. the tail of the current slice), and those chunks would
        // surface later as "read data error" when the rotation returns to
        // that lane. Reject cross-slice absorption outright: within one slice
        // source and destination share a lane, so the single-lane drain
        // bookkeeping is sound.
        if self.slice_size <= 0 || pos / self.slice_size != self.pos / self.slice_size {
            return false;
        }
        // 随机模式选中的是 base reader(无缓冲 channel),不可吸收。
        let Some(id) = Self::select_reader_index(
            self.read_detector.is_random(),
            pos,
            self.slice_size,
            self.read_detector.read_parallel(),
            self.base_reader_index,
        ) else {
            return false;
        };
        let Some(ReaderAdapter::Buffer(ch)) = self.readers.get_mut(id) else {
            return false; // base reader 或未初始化
        };
        ch.start_prefetch(); // 保证预取任务已启动(否则水位必为 0,自然拒绝)
        let absorbed = ch.can_absorb(pos);
        if absorbed {
            ch.mark_absorbed(); // 后续 read 需要 drain 掉 pos 之下的残留 chunk
        }
        absorbed
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos == self.pos() {
            return Ok(());
        }

        // 前向小跳且落在目标 lane 的已生产数据内:本地吸收,零后端交互。
        if pos > self.pos && self.try_absorb(pos) {
            debug!(
                "seek absorbed: path={}, from={}, to={}, skipped={} bytes",
                self.path,
                self.pos,
                pos,
                pos - self.pos
            );
            self.read_detector.record_absorbed_seek(pos, &self.path);
            self.pos = pos;
            FsContext::get_metrics().seek_absorbed_total.inc();
            return Ok(());
        }

        // Capture the pattern BEFORE record_seek mutates it: the
        // Sequential->Random flip seek itself must still take the full loop so
        // every lane's BufferChannel::seek pauses the task, drains stale
        // chunks, and resets the watermark. Skipping that on the flip seek
        // leaves lanes holding stale data through the Random phase, and the
        // next read after flip-back pops a chunk with off > pos -> EIO
        // (reachable with the default window=0 config).
        let was_random = self.read_detector.is_random();
        self.read_detector.record_seek(&self.path);
        if self.read_detector.enabled && was_random && self.absorb_window > 0 {
            // 随机模式且吸收特性开启:只有 base reader 服务读;lane 在翻转 seek
            // 时已 pause/drain,由 Random→Sequential 的 pause(pos, false) 循环
            // 统一重定位。window=0 时走全量循环,与上游逐字节一致。
            if let Some(base) = self.readers.get_mut(self.base_reader_index) {
                base.seek(pos).await?;
            }
        } else {
            // Resilient loop: a failing lane must not abort the seek of the
            // remaining lanes. Lanes after a failure would otherwise never be
            // seeked/drained/watermark-reset, and since later Random-phase
            // seeks take the base-only branch nothing would ever repair them:
            // on flip-back a stale queued chunk would trip "read data error".
            // Every lane still gets its seek/drain/reset attempt; the first
            // error is returned afterwards, preserving error semantics.
            let mut first_err: Option<FsError> = None;
            for reader in &mut self.readers {
                if let Err(e) = reader.seek(pos).await {
                    first_err.get_or_insert(e);
                    continue;
                }
                if !self.read_detector.enabled {
                    if let Err(e) = reader.pause(pos, false).await {
                        first_err.get_or_insert(e);
                    }
                }
            }
            if let Some(e) = first_err {
                return Err(e);
            }
        }

        self.pos = pos;
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        for reader in &mut self.readers {
            reader.complete().await?;
        }
        Ok(())
    }

    async fn read_future(
        chunk_sender: AsyncSender<FileChunk>,
        mut task_receiver: AsyncReceiver<ReadTask>,
        mut reader: FsReaderParallel,
        produced_end: Arc<AtomicI64>,
    ) -> FsResult<()> {
        let mut paused = false;
        loop {
            let select_task = if paused {
                match task_receiver.recv().await {
                    Some(task) => SelectTask::Control(task),
                    None => return Ok(()), // control channel closed while paused
                }
            } else {
                tokio::select! {
                    biased;

                    task_opt = task_receiver.recv() => {
                        match task_opt {
                            Some(task) => SelectTask::Control(task),
                            None => return Ok(()), // control channel closed: normal shutdown
                        }
                    }

                    permit_res = chunk_sender.reserve() => {
                        match permit_res {
                            Ok(permit) => SelectTask::Permit(permit),
                            Err(_e) => return Ok(()), // data channel closed: normal shutdown
                        }
                    }
                }
            };

            match select_task {
                SelectTask::Control(task) => match task {
                    ReadTask::Seek(pos, tx) => {
                        // 1. reader executes seek
                        // 2. Set paused = true
                        // 3. The notification pause was successful
                        paused = true;
                        reader.seek(pos).await?;
                        tx.send(1)?;
                    }

                    ReadTask::Pause((pos, v)) => {
                        paused = v;
                        reader.seek(pos).await?;
                    }

                    ReadTask::Stop(tx) => {
                        reader.complete().await?;
                        tx.send(1)?;
                        return Ok(());
                    }
                },

                SelectTask::Permit(permit) => {
                    // paused is guaranteed false here (paused branch never yields a Permit).
                    let chunk = reader.read().await?;
                    if chunk.is_empty() {
                        paused = true;
                    } else {
                        // Update the produced watermark before the chunk becomes
                        // visible to consumers, so a seek target <= watermark is
                        // guaranteed to be (or become) queued data.
                        produced_end.store(chunk.off + chunk.len() as i64, Ordering::Release);
                    }
                    // Send the chunk (possibly empty) so the reader side does not block.
                    permit.send(chunk);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use curvine_config::ClusterConf;
    use curvine_model::{ExtendedBlock, FileStatus, FileType, LocatedBlock, StorageType};

    // The seek-absorb tests assert on the process-global
    // `ClientMetrics::seek_absorbed_total` counter, which is shared across all
    // tests in this binary. Rust runs tests on parallel threads, so concurrent
    // absorb tests would race on the counter; this mutex serializes them.
    static ABSORB_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn sparse_file_blocks(len: i64) -> FileBlocks {
        FileBlocks::new(
            FileStatus {
                id: 1,
                len,
                is_complete: true,
                ..Default::default()
            },
            vec![],
        )
    }

    // File blocks carrying synthetic block metadata for the whole range, so
    // reader seeks resolve through `get_read_block` without any backend: a
    // seek never instantiates a BlockReader (only reads do), which makes lane
    // seeks in fixtures deterministic metadata-only successes.
    fn blocked_file_blocks(len: i64) -> FileBlocks {
        FileBlocks::new(
            FileStatus {
                id: 1,
                len,
                is_complete: true,
                ..Default::default()
            },
            vec![LocatedBlock::new(
                ExtendedBlock::new(1, len, StorageType::Disk, FileType::File),
                vec![],
            )],
        )
    }

    fn absorb_reader_conf(
        file_len: i64,
        absorb_window_str: &str,
        slice_size_str: &str,
        with_block_meta: bool,
    ) -> (FsReaderBuffer, Arc<Runtime>) {
        let mut conf = ClusterConf::default();
        conf.client.read_parallel = 4;
        conf.client.read_chunk_num = 4;
        conf.client.read_slice_size_str = slice_size_str.to_string();
        conf.client.large_file_size_str = "1GB".to_string();
        conf.client.enable_smart_prefetch = true;
        conf.client.seek_absorb_window_str = absorb_window_str.to_string();
        conf.client.init().unwrap();

        let file_blocks = if with_block_meta {
            blocked_file_blocks(file_len)
        } else {
            sparse_file_blocks(file_len)
        };
        let path = Path::from_str("/absorb-test").unwrap();
        let read_detector = ReadDetector::with_conf(&conf.client, file_len);
        let fs_context = Arc::new(FsContext::new(conf).unwrap());
        let rt = fs_context.clone_runtime();
        let reader = FsReaderBuffer::new(path, fs_context, file_blocks, read_detector).unwrap();
        (reader, rt)
    }

    fn absorb_reader(file_len: i64, absorb_window_str: &str) -> (FsReaderBuffer, Arc<Runtime>) {
        absorb_reader_conf(file_len, absorb_window_str, "16MB", false)
    }

    // Park a buffered lane's producer BEFORE it can touch the backend: the
    // Pause command is queued ahead of the task start, and the producer's
    // `biased` select handles queued control messages before reserving a
    // data-channel permit, so the task deterministically pauses after a
    // metadata-only seek to its first slice (requires blocked_file_blocks).
    // Returns a cloned chunk sender for injecting fixture chunks.
    fn park_lane_producer(rt: &Runtime, channel: &mut BufferChannel) -> AsyncSender<FileChunk> {
        let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
        rt.block_on(channel.task_sender.send(ReadTask::Pause((0, true))))
            .unwrap();
        channel.start_prefetch();
        tx
    }

    #[test]
    fn test_random_read_uses_base_reader_for_sparse_parallel_slices() {
        let mut conf = ClusterConf::default();
        conf.client.read_parallel = 4;
        conf.client.read_chunk_num = 1;
        conf.client.read_slice_size_str = "16MB".to_string();
        conf.client.large_file_size_str = "1GB".to_string();
        conf.client.init().unwrap();

        let file_len = conf.client.read_slice_size / 2;
        let file_blocks = sparse_file_blocks(file_len);
        let path = Path::from_str("/small-file").unwrap();
        let read_detector = ReadDetector::with_conf(&conf.client, file_len);
        let fs_context = Arc::new(FsContext::new(conf).unwrap());
        let rt = fs_context.clone_runtime();
        let mut reader = FsReaderBuffer::new(path, fs_context, file_blocks, read_detector).unwrap();

        assert_eq!(reader.base_reader_index, 1);

        rt.block_on(reader.seek(file_len)).unwrap();
        assert!(reader.read_detector.is_random());
        assert!(reader.get_reader().is_ok());
    }

    #[tokio::test]
    async fn test_recv_skipping_below_discards_covered() {
        let (tx, mut rx) = AsyncChannel::<FileChunk>::new(8).split();
        tx.send(FileChunk::with_buf(0, BytesMut::from(&b"aaaa"[..])))
            .await
            .unwrap();
        tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..])))
            .await
            .unwrap();
        tx.send(FileChunk::with_buf(8, BytesMut::from(&b"cccc"[..])))
            .await
            .unwrap();
        drop(tx);

        let chunk = recv_skipping_below(&mut rx, 6).await.unwrap();
        assert_eq!(chunk.off, 4); // chunk[0,4) 整体低于 6 被丢弃;[4,8) 覆盖 6
    }

    #[tokio::test]
    async fn test_recv_skipping_below_boundary_and_eof() {
        let (tx, mut rx) = AsyncChannel::<FileChunk>::new(8).split();
        tx.send(FileChunk::with_buf(0, BytesMut::from(&b"aaaa"[..])))
            .await
            .unwrap();
        tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..])))
            .await
            .unwrap();
        tx.send(FileChunk::default()).await.unwrap(); // EOF 哨兵
        drop(tx);

        // pos 恰在 chunk 末尾边界:整条丢弃,取到 EOF 哨兵(直通,不参与位置比较)
        let chunk = recv_skipping_below(&mut rx, 8).await.unwrap();
        assert!(chunk.is_empty());
    }

    #[tokio::test]
    async fn test_recv_skipping_below_noop_when_head_covers() {
        let (tx, mut rx) = AsyncChannel::<FileChunk>::new(8).split();
        tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..])))
            .await
            .unwrap();
        drop(tx);

        let chunk = recv_skipping_below(&mut rx, 4).await.unwrap(); // 稳态:pos == head off
        assert_eq!(chunk.off, 4);
        assert_eq!(chunk.len(), 4);
    }

    #[test]
    fn test_seek_absorb_within_watermark() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "4MB");

        // Simulate the prefetch task of the selected lane having produced the
        // chunk covering the seek target [4,8) and queue it for consumption.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }
        let metrics = FsContext::get_metrics();
        let absorbed_before = metrics.seek_absorbed_total.get();

        // Forward seek from 0 to 6: inside the window and covered by produced data.
        rt.block_on(reader.seek(6)).unwrap();

        assert!(reader.read_detector.is_sequential());
        assert_eq!(reader.pos(), 6);
        assert_eq!(metrics.seek_absorbed_total.get(), absorbed_before + 1);

        // The next read aligns chunk [4,8) to pos 6 and returns its tail.
        let data = rt.block_on(reader.read()).unwrap();
        assert_eq!(data.as_slice(), b"bb");
        assert_eq!(reader.pos(), 8);
    }

    // Random 模式 seek 只重定位 base reader,lane 保持原样直到翻回 Sequential。
    // 用确定性 fixture(块元数据 + 停住的 producer)让两次 seek 必然成功,
    // 然后以 lane 状态的"存活性"区分两条路径:flip 之后向 lane 注入 chunk
    // 和水位,第二次 Random seek 若走 base-only 路径则 lane 原封不动;若退化
    // 回全量循环,lane 的 BufferChannel::seek 会 drain 掉 chunk 并清零水位,
    // 本测试即红。(负向对照:禁用 base-only 分支时本测试失败。)
    #[test]
    fn test_random_mode_seek_repositions_base_only() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader_conf(4096, "4MB", "16MB", true);
        let base_index = reader.base_reader_index;
        assert_eq!(base_index, 1, "fixture must create one buffered lane");

        // Park the lane producer before anything can race it: the flip seek
        // below must deterministically succeed.
        let lane_tx = if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            park_lane_producer(&rt, channel)
        } else {
            panic!("lane 0 must be a buffered channel");
        };

        // First seek flips Sequential -> Random via the full loop: every lane
        // is seeked/drained/watermark-reset, the detector flips.
        rt.block_on(reader.seek(1024)).unwrap();
        assert!(reader.read_detector.is_random());
        assert_eq!(reader.pos(), 1024);
        assert!(reader.get_reader().is_ok());

        // Seed the (already flipped) lane with a chunk and a watermark: the
        // sentinel state that a base-only seek must NOT touch.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            rt.block_on(lane_tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        // Second seek while already Random: must take the base-only branch.
        rt.block_on(reader.seek(3072)).unwrap();
        assert!(reader.read_detector.is_random());
        assert_eq!(reader.pos(), 3072);
        assert!(reader.get_reader().is_ok());
        assert_eq!(reader.base_reader_index, base_index);

        // The base reader really was repositioned...
        if let Some(ReaderAdapter::Base(base)) = reader.readers.get(base_index) {
            assert_eq!(base.pos(), 3072, "base reader must be repositioned");
        } else {
            panic!("reader {} must be the base reader", base_index);
        }

        // ...and the lane was NOT: its chunk and watermark survive. On a
        // regression to the full N+1 loop the lane seek drains the chunk and
        // resets the watermark, failing both assertions.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let leftover = channel
                .chunk_receiver
                .try_recv()
                .expect("try_recv on lane channel must not fail");
            assert_eq!(
                leftover.map(|c| (c.off, c.len())),
                Some((4, 4)),
                "Random-mode seek must leave lane chunks untouched"
            );
            assert_eq!(
                channel
                    .produced_end
                    .load(std::sync::atomic::Ordering::Acquire),
                8,
                "Random-mode seek must leave the lane watermark untouched"
            );
        } else {
            panic!("lane 0 must be a buffered channel");
        }
    }

    // Drive the buffered-lane seek/read sequence with block_on on the
    // reader's own runtime: the tests sharing the process-global
    // seek_absorbed_total counter hold ABSORB_TEST_LOCK (a std Mutex), and a
    // sync test keeps that guard out of async scopes (clippy:
    // await_holding_lock).
    #[test]
    fn test_seek_absorb_drains_covered_chunks() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "4MB");

        // Two already-produced chunks [0,4) and [4,8); the seek target 6 is
        // covered by the second one.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            rt.block_on(tx.send(FileChunk::with_buf(0, BytesMut::from(&b"aaaa"[..]))))
                .unwrap();
            rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        rt.block_on(reader.seek(6)).unwrap();
        assert!(reader.read_detector.is_sequential());
        assert_eq!(reader.pos(), 6);

        // The read drains chunk [0,4) (tail <= pos) and aligns [4,8) to pos 6.
        let data = rt.block_on(reader.read()).unwrap();
        assert_eq!(data.as_slice(), b"bb");
        assert_eq!(reader.pos(), 8);
    }

    #[test]
    fn test_seek_absorb_rejected_beyond_watermark() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "4MB");

        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        // Forward seek to 12: within the absorb window but beyond the watermark.
        // The seek must fall back to the full path, flipping the detector to
        // Random. The buffered lanes have no backend blocks, so their seek fails
        // inside the task and the error surfaces through the monitor.
        let absorbed_before = FsContext::get_metrics().seek_absorbed_total.get();
        let res = rt.block_on(reader.seek(12));
        if res.is_ok() {
            assert_eq!(reader.pos(), 12);
        } else {
            // Failed full-path seek: readers are left seeking, pos stays put.
            assert_eq!(reader.pos(), 0);
        }
        assert!(reader.read_detector.is_random());
        // The watermark gate must have refused absorption: no absorb happened,
        // so the counter is unchanged (a buggy absorb would increment it even
        // if a later record_seek flipped the pattern to Random).
        assert_eq!(
            FsContext::get_metrics().seek_absorbed_total.get(),
            absorbed_before
        );
    }

    #[test]
    fn test_seek_absorb_window_zero_disabled() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "0");

        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        // Window disabled: even a seek covered by produced data is not absorbed.
        let absorbed_before = FsContext::get_metrics().seek_absorbed_total.get();
        let res = rt.block_on(reader.seek(6));
        if res.is_ok() {
            assert_eq!(reader.pos(), 6);
        } else {
            assert_eq!(reader.pos(), 0);
        }
        assert!(reader.read_detector.is_random());
        // The window gate must have refused absorption: counter unchanged.
        assert_eq!(
            FsContext::get_metrics().seek_absorbed_total.get(),
            absorbed_before
        );
    }

    // Regression for Finding 1: the resilient flip-seek loop must process
    // EVERY lane even when one lane's seek fails. The fixture builds FOUR
    // buffered lanes (4MB file, 1MB slices) with block metadata so lane seeks
    // are deterministic metadata-only successes, and parks each producer via
    // a pre-queued Pause so no task ever touches the backend. Lane 0's seek
    // is injected to fail; lanes 1..3 must still be seeked, which for a
    // buffered lane means its channel is drained and its watermark reset.
    // The first error is still returned to the caller (error semantics
    // preserved) and self.pos is left unchanged.
    // On the unfixed code the `?` aborts the loop at lane 0, so lanes 1..3
    // keep their stale chunks and stale-high watermarks and this test is RED.
    #[test]
    fn test_flip_seek_partial_failure_still_drains_later_lanes() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader_conf(4 << 20, "4MB", "1MB", true);
        assert_eq!(
            reader.base_reader_index, 4,
            "fixture must create 4 buffered lanes"
        );
        reader.pos = 10;

        // Lane 0 fails its seek; lanes 1..3 carry a stale chunk each plus a
        // stale-high watermark, exactly the state that used to survive forever
        // once a mid-loop error aborted the full-seek loop.
        for (i, adapter) in reader.readers.iter_mut().enumerate() {
            if let ReaderAdapter::Buffer(channel) = adapter {
                if i == 0 {
                    channel.fail_seek = true;
                } else {
                    let tx = park_lane_producer(&rt, channel);
                    rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                        .unwrap();
                    channel
                        .produced_end
                        .store(8, std::sync::atomic::Ordering::Release);
                }
            }
        }

        // Backward seek from Sequential: full path, first lane errors.
        let res = rt.block_on(reader.seek(0));
        assert!(res.is_err(), "the first lane error must be returned");
        assert_eq!(reader.pos(), 10, "failed seek must not move pos");

        // Despite the error, every later lane must have been seeked: channel
        // drained and watermark reset. The producers are parked (paused), so
        // the channels stay open and a single try_recv returning None proves
        // the drain.
        let mut checked = 0;
        for (i, adapter) in reader.readers.iter_mut().enumerate() {
            if let ReaderAdapter::Buffer(channel) = adapter {
                if i == 0 {
                    assert!(
                        channel.fail_seek,
                        "lane 0 failure injection must still be armed"
                    );
                    continue;
                }
                let leftover = channel
                    .chunk_receiver
                    .try_recv()
                    .expect("try_recv on lane channel must not fail");
                assert!(
                    leftover.is_none(),
                    "lane {} must be drained despite lane 0's failure, got {:?}",
                    i,
                    leftover.map(|c| (c.off, c.len()))
                );
                assert_eq!(
                    channel
                        .produced_end
                        .load(std::sync::atomic::Ordering::Acquire),
                    0,
                    "lane {} watermark must be reset despite lane 0's failure",
                    i
                );
                checked += 1;
            }
        }
        assert_eq!(checked, 3, "the later-lane assertions must actually run");
    }

    // Regression for Finding 2: needs_drain must be bounded to exactly one
    // drain cycle. After the covering chunk is consumed the flag clears and
    // the fail-fast alignment guard is re-armed: a later chunk wholly below
    // pos on the same lane (a mispositioned production without a
    // BufferChannel::seek) must surface "read data error" instead of being
    // silently drained (which would convert the tripwire into a silent hang
    // waiting for the covering chunk that never comes).
    // On the unfixed code the flag stays set, recv_skipping_below silently
    // discards the stale chunk and returns the queued EOF sentinel, and the
    // read succeeds as Empty: this test is RED.
    #[test]
    fn test_needs_drain_cleared_after_one_drain_cycle() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "4MB");

        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            // Post-absorb lane state: a stale residual wholly below pos, the
            // covering chunk, then a stale low chunk queued without any seek
            // (the mispositioned production the tripwire exists to catch),
            // then an EOF sentinel so the unfixed silent-drain path terminates
            // instead of hanging the test.
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            rt.block_on(tx.send(FileChunk::with_buf(0, BytesMut::from(&b"aaaa"[..]))))
                .unwrap();
            rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            rt.block_on(tx.send(FileChunk::with_buf(0, BytesMut::from(&b"zzzz"[..]))))
                .unwrap();
            rt.block_on(tx.send(FileChunk::default())).unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        // Absorb the forward seek to 6; the first read drains the residual
        // [0,4) and consumes the covering chunk [4,8).
        rt.block_on(reader.seek(6)).unwrap();
        let data = rt.block_on(reader.read()).unwrap();
        assert_eq!(data.as_slice(), b"bb");
        assert_eq!(reader.pos(), 8);

        // The drain cycle is complete, so the guard is live again: the stale
        // chunk at off 0 must trip the alignment error, not be skipped.
        let err = rt.block_on(reader.read()).expect_err(
            "stale below-pos chunk after the drain cycle must trip the alignment guard",
        );
        assert!(
            err.to_string().contains("read data error"),
            "expected the read data error tripwire, got: {}",
            err
        );
    }

    // Regression for the flip-seek stale-lane EIO: the Sequential -> Random
    // flip seek must take the FULL lane loop so every lane's BufferChannel::seek
    // drains its channel and resets its watermark to 0. On the unfixed code the
    // base-only branch fired on the flip seek itself, leaving stale chunks and
    // a stale-high watermark in the lanes; on flip-back the next read popped a
    // chunk with off > pos -> "read data error" -> EIO (reachable at window=0
    // defaults).
    // The fixture uses block metadata and a parked producer so every lane seek
    // is a deterministic metadata-only success: the flip seek is REQUIRED to
    // succeed, and since a successful BufferChannel::seek drains before
    // returning, the lane state is asserted with a single NON-consuming
    // try_recv (a retry loop would consume the very stale chunk it is
    // supposed to detect, passing even when the seek skipped the drain).
    #[test]
    fn test_flip_seek_drains_lane_state() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader_conf(4096, "4MB", "16MB", true);
        // Match the flip-seek production scenario: lane prefetch has produced
        // into the channels but NO absorb has happened (this seek will flip
        // Sequential -> Random, which is exactly when stale lanes bite).
        reader.pos = 10;

        // Simulate a prefetched lane whose task already started: park the
        // producer deterministically, then inject a stale chunk and a
        // stale-high watermark.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = park_lane_producer(&rt, channel);
            rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        // Backward full-path seek from Sequential: not absorbable (pos moves
        // backward). Every lane seek is metadata-only, so the seek must
        // succeed — and a successful seek has drained and reset the lane
        // BEFORE returning.
        rt.block_on(reader.seek(0)).unwrap();
        assert_eq!(reader.pos(), 0);

        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            // Check the queue exactly once: the producer is parked, so the
            // channel is open and quiescent; any chunk still queued means the
            // flip seek skipped the drain.
            let leftover = channel
                .chunk_receiver
                .try_recv()
                .expect("try_recv on lane channel must not fail");
            assert!(
                leftover.is_none(),
                "flip seek must drain stale chunks from the lane channel, got {:?}",
                leftover.map(|c| (c.off, c.len()))
            );
            assert_eq!(
                channel
                    .produced_end
                    .load(std::sync::atomic::Ordering::Acquire),
                0,
                "flip seek must reset the produced watermark to 0"
            );
        } else {
            panic!("lane 0 must be a buffered channel");
        }
    }

    // Regression: needs_drain must survive a cancelled read. The drain cycle
    // clears the flag only when the receive COMPLETES; clearing it up front
    // would lose the drain when the read future is dropped mid-receive
    // (timeout, FUSE interrupt), and the retry would pop a residual below-pos
    // chunk straight into "read data error".
    #[test]
    fn test_absorbed_read_cancelled_mid_drain_resumes_on_retry() {
        use std::future::Future;

        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "4MB");

        // Arm the absorb with a produced watermark of 8 but an EMPTY channel:
        // the lane's chunks arrive only after the first read is cancelled.
        let tx = if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
            tx
        } else {
            panic!("lane 0 must be a buffered channel");
        };

        rt.block_on(reader.seek(6)).unwrap();
        assert_eq!(reader.pos(), 6);

        // First read: the drain cycle starts but the channel is empty, so the
        // future parks inside the receive — cancel it right there. The manual
        // poll needs no runtime: channel receives park without an ambient
        // executor.
        {
            let mut fut = Box::pin(reader.read());
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            assert!(
                matches!(fut.as_mut().poll(&mut cx), std::task::Poll::Pending),
                "the read must park on the empty channel"
            );
        } // dropped here: cancelled mid-receive

        // The lane now delivers the residual below-pos chunk and the covering
        // chunk.
        rt.block_on(tx.send(FileChunk::with_buf(0, BytesMut::from(&b"aaaa"[..]))))
            .unwrap();
        rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
            .unwrap();

        // Retry: the flag must still be armed, so the residual [0,4) is
        // drained and the covering [4,8) is aligned to pos 6. On the unfixed
        // code the cancelled read already cleared the flag and this read pops
        // [0,4) -> "read data error".
        let data = rt.block_on(reader.read()).unwrap();
        assert_eq!(data.as_slice(), b"bb");
        assert_eq!(reader.pos(), 8);
    }

    // Regression: absorption must not cross a slice boundary. Only the
    // destination lane is marked for draining, so a cross-slice absorb would
    // skip over data queued in OTHER lanes (the tail of the source slice),
    // and that stale chunk would trip "read data error" when the lane
    // rotation returns to it. The cross-slice gate must reject the absorb
    // even when the destination lane's watermark covers the target.
    #[test]
    fn test_seek_absorb_rejected_across_slice_boundary() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 4 buffered lanes: 4MB file, 1MB slices; slice k is lane k.
        let (mut reader, rt) = absorb_reader_conf(4 << 20, "4MB", "1MB", false);
        assert_eq!(reader.base_reader_index, 4);

        let slice = 1i64 << 20;
        reader.pos = slice - 4; // tail of slice 0 (lane 0)
        let target = slice + 4; // head of slice 1 (lane 1): 8 bytes forward

        // The destination lane's watermark covers the target; only the
        // cross-slice gate stands between this seek and a bad absorb.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(1) {
            channel
                .produced_end
                .store(2 * slice, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 1 must be a buffered channel");
        }

        let absorbed_before = FsContext::get_metrics().seek_absorbed_total.get();
        let res = rt.block_on(reader.seek(target));
        // The full-path seek may fail on the sparse fixture; either way the
        // absorb must have been refused.
        if res.is_ok() {
            assert_eq!(reader.pos(), target);
        } else {
            assert_eq!(reader.pos(), slice - 4);
        }
        assert!(reader.read_detector.is_random());
        assert_eq!(
            FsContext::get_metrics().seek_absorbed_total.get(),
            absorbed_before,
            "a cross-slice seek must not be absorbed"
        );
    }

    // Companion positive case: with multiple buffered lanes, a forward seek
    // WITHIN one slice still absorbs against that slice's lane.
    #[test]
    fn test_seek_absorb_same_slice_multi_lane() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader_conf(4 << 20, "4MB", "1MB", false);
        assert_eq!(reader.base_reader_index, 4);

        // Seek 0 -> 6 inside slice 0 (lane 0), covered by produced data.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        let absorbed_before = FsContext::get_metrics().seek_absorbed_total.get();
        rt.block_on(reader.seek(6)).unwrap();
        assert!(reader.read_detector.is_sequential());
        assert_eq!(reader.pos(), 6);
        assert_eq!(
            FsContext::get_metrics().seek_absorbed_total.get(),
            absorbed_before + 1
        );

        let data = rt.block_on(reader.read()).unwrap();
        assert_eq!(data.as_slice(), b"bb");
        assert_eq!(reader.pos(), 8);
    }
}
