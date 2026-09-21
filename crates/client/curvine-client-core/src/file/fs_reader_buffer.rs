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
            // skip them. Self-limiting: once the covering chunk is consumed,
            // the head matches pos again. The flag stays set until the next
            // full seek resets it, so later reads after another absorb on this
            // lane drain as well.
            recv_skipping_below(&mut self.chunk_receiver, want_pos)
                .await
                .map_err(|e| self.check_error(e))
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
        if self.absorb_window <= 0 || pos - self.pos > self.absorb_window {
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
            for reader in &mut self.readers {
                reader.seek(pos).await?;
                if !self.read_detector.enabled {
                    reader.pause(pos, false).await?;
                }
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
    use curvine_model::FileStatus;

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

    fn absorb_reader(file_len: i64, absorb_window_str: &str) -> (FsReaderBuffer, Arc<Runtime>) {
        let mut conf = ClusterConf::default();
        conf.client.read_parallel = 4;
        conf.client.read_chunk_num = 4;
        conf.client.read_slice_size_str = "16MB".to_string();
        conf.client.large_file_size_str = "1GB".to_string();
        conf.client.enable_smart_prefetch = true;
        conf.client.seek_absorb_window_str = absorb_window_str.to_string();
        conf.client.init().unwrap();

        let file_blocks = sparse_file_blocks(file_len);
        let path = Path::from_str("/absorb-test").unwrap();
        let read_detector = ReadDetector::with_conf(&conf.client, file_len);
        let fs_context = Arc::new(FsContext::new(conf).unwrap());
        let rt = fs_context.clone_runtime();
        let reader = FsReaderBuffer::new(path, fs_context, file_blocks, read_detector).unwrap();
        (reader, rt)
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

    // 随机模式 seek 只重定位 base reader:seek 到两个不同远端位置后,
    // get_reader() 仍指向 base、detector 保持 Random、pos 生效。
    // (行为优化而非语义变化;作为回归保护防止退化回 N+1 全量重定位。)
    #[test]
    fn test_random_mode_seek_repositions_base_only() {
        let (mut reader, rt) = absorb_reader(4096, "4MB");
        let base_index = reader.base_reader_index;

        // First full-path seek flips Sequential -> Random. The sparse file has
        // no backing blocks, so reader-side seeks may fail; the detector must
        // already be Random either way (record_seek runs before repositioning).
        let res = rt.block_on(reader.seek(1024));
        assert!(reader.read_detector.is_random());
        if res.is_ok() {
            assert_eq!(reader.pos(), 1024);
        } else {
            assert_eq!(reader.pos(), 0);
        }
        assert!(reader.get_reader().is_ok());
        assert_eq!(reader.base_reader_index, base_index);

        // Second seek while already Random: base-only path must keep semantics.
        let res = rt.block_on(reader.seek(3072));
        assert!(reader.read_detector.is_random());
        if res.is_ok() {
            assert_eq!(reader.pos(), 3072);
        } else {
            assert_eq!(reader.pos(), 0);
        }
        assert!(reader.get_reader().is_ok());
        assert_eq!(reader.base_reader_index, base_index);
    }

    // Convert a buffered-lane seek/read sequence into futures driven on the
    // reader's own runtime. All four tests share the process-global
    // seek_absorbed_total counter, so they are serialized via ABSORB_TEST_LOCK.
    #[tokio::test(flavor = "current_thread")]
    async fn test_seek_absorb_drains_covered_chunks() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, _rt) = absorb_reader(4096, "4MB");

        // Two already-produced chunks [0,4) and [4,8); the seek target 6 is
        // covered by the second one.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            tx.send(FileChunk::with_buf(0, BytesMut::from(&b"aaaa"[..])))
                .await
                .unwrap();
            tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..])))
                .await
                .unwrap();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
        } else {
            panic!("lane 0 must be a buffered channel");
        }

        reader.seek(6).await.unwrap();
        assert!(reader.read_detector.is_sequential());
        assert_eq!(reader.pos(), 6);

        // The read drains chunk [0,4) (tail <= pos) and aligns [4,8) to pos 6.
        let data = reader.read().await.unwrap();
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

    // Regression for the flip-seek stale-lane EIO: the Sequential -> Random
    // flip seek must take the FULL lane loop so every lane's BufferChannel::seek
    // drains its channel and resets its watermark to 0. On the unfixed code the
    // base-only branch fired on the flip seek itself, leaving stale chunks and
    // a stale-high watermark in the lanes; on flip-back the next read popped a
    // chunk with off > pos -> "read data error" -> EIO (reachable at window=0
    // defaults). On unfixed code neither the drain nor the reset happens, so
    // the stale chunk survives and this test is RED.
    #[test]
    fn test_flip_seek_drains_lane_state() {
        let _lock = ABSORB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut reader, rt) = absorb_reader(4096, "4MB");
        // Match the flip-seek production scenario: lane prefetch has produced
        // into the channels but NO absorb has happened (this seek will flip
        // Sequential -> Random, which is exactly when stale lanes bite).
        reader.pos = 10;

        // Simulate a prefetched lane whose task already started: inject a stale
        // chunk through a CLONED sender (so the lane's own PrefetchArgs sender
        // can be dropped below), start the task, and set a stale-high watermark.
        let lane_tx = if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
            let tx = channel.prefetch.as_ref().unwrap().chunk_sender.clone();
            rt.block_on(tx.send(FileChunk::with_buf(4, BytesMut::from(&b"bbbb"[..]))))
                .unwrap();
            channel.start_prefetch();
            channel
                .produced_end
                .store(8, std::sync::atomic::Ordering::Release);
            tx
        } else {
            panic!("lane 0 must be a buffered channel");
        };
        // Drop the test handle: the lane task's own sender is the only one
        // left, and the channel closes when the task consumes it.
        drop(lane_tx);

        // Backward full-path seek from Sequential: not absorbable (pos moves
        // backward). BufferChannel::seek notifies the started lane task (the
        // Seek task pauses it) and drains the channel; the lane task then
        // consumes its own sender, closing the channel so the drain loop can
        // terminate. Give the runtime a moment for that close to propagate
        // (test-only; production readers read from the channels, so queued
        // data never lingers like this).
        let res1 = rt.block_on(reader.seek(0));
        std::thread::sleep(std::time::Duration::from_millis(50));
        let res2 = rt.block_on(reader.seek(0));
        let _ = (res1, res2); // results intentionally unused; asserts below pin lane state

        // The flip seek must have drained lane state regardless of whether the
        // backend seek succeeded.
        if let Some(ReaderAdapter::Buffer(channel)) = reader.readers.get_mut(0) {
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
}
