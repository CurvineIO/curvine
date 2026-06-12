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
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::FileBlocks;
use curvine_common::FsResult;
use log::error;
use orpc::err_box;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
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
}

// A parallel task description structure
// chunk_receiver: accept data
// task_sender: Send control command
struct BufferChannel {
    prefetch: Option<PrefetchArgs>,
    chunk_receiver: AsyncReceiver<FileChunk>,
    task_sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
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
            let res =
                FsReaderBuffer::read_future(args.chunk_sender, args.task_receiver, args.reader)
                    .await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("buffer read(parallel id {}) error: {:?}", parallel_id, e);
                    monitor.set_error(e);
                }
            }
        });
    }

    async fn read(&mut self) -> FsResult<FileChunk> {
        self.start_prefetch();

        self.chunk_receiver
            .recv_check()
            .await
            .map_err(|e| self.check_error(e))
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
    async fn read(&mut self) -> FsResult<FileChunk> {
        match self {
            ReaderAdapter::Buffer(r) => r.read().await,
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
                let channel = BufferChannel {
                    prefetch: Some(PrefetchArgs {
                        rt: rt.clone(),
                        reader,
                        chunk_sender,
                        task_receiver,
                    }),
                    chunk_receiver,
                    task_sender,
                    err_monitor: err_monitor.clone(),
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

        let reader = self.get_reader()?;
        let mut chunk = reader.read().await?;

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

        FsContext::get_metrics()
            .read_bytes
            .inc_by(bytes.len() as i64);

        Ok(bytes)
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos == self.pos() {
            return Ok(());
        }

        self.read_detector.record_seek(&self.path);
        for reader in &mut self.readers {
            reader.seek(pos).await?;

            if !self.read_detector.enabled {
                reader.pause(pos, false).await?;
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
    use curvine_common::conf::ClusterConf;
    use curvine_common::state::FileStatus;

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
}
