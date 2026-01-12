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
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;
use tokio::sync::mpsc::Permit;
use tokio::task::yield_now;

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

// A parallel task description structure
// chunk_receiver: accept data
// task_sender: Send control command
struct BufferChannel {
    chunk_receiver: AsyncReceiver<FileChunk>,
    task_sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
}

impl BufferChannel {
    fn check_error(&self, e: impl Into<FsError>) -> FsError {
        self.err_monitor.take_error().unwrap_or(e.into())
    }

    async fn read(&mut self) -> FsResult<FileChunk> {
        self.chunk_receiver
            .recv_check()
            .await
            .map_err(|e| self.check_error(e))
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
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
                let monitor = err_monitor.clone();
                let parallel_id = reader.parallel_id();

                rt.spawn(async move {
                    let res = Self::read_future(chunk_sender, task_receiver, reader).await;
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            error!("buffer read(parallel id {})error: {:?}", parallel_id, e);
                            monitor.set_error(e);
                        }
                    }
                });
                let channel = BufferChannel {
                    chunk_receiver,
                    task_sender,
                    err_monitor: err_monitor.clone(),
                };
                ReaderAdapter::Buffer(channel)
            };
            readers.push(reader);
        }

        readers.push(ReaderAdapter::Base(base));

        let reader = Self {
            readers,
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

    fn get_reader(&mut self) -> FsResult<&mut ReaderAdapter> {
        let id = if self.read_detector.is_random() {
            self.read_detector.read_parallel()
        } else {
            self.pos / self.slice_size % self.read_detector.read_parallel()
        };

        match self.readers.get_mut(id as usize) {
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
        if is_changed {
            for reader in &mut self.readers {
                if self.read_detector.is_sequential() {
                    reader.pause(self.pos, false).await?;
                }
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
        // Mark whether the current task needs to be paused
        let mut paused = false;
        loop {
            // The queue can be written and controlled to complete any future.
            let select_task = tokio::select! {
                biased;

                task_opt = task_receiver.recv() => {
                    match task_opt {
                        Some(task) => SelectTask::Control(task),
                        None => return Ok(()), // control channel closed: normal shutdown
                    }
                }

                premit_res = chunk_sender.reserve() => {
                    if !paused {
                        match premit_res {
                            Ok(permit) => SelectTask::Permit(permit),
                            Err(_e) => return Ok(()), // data channel closed: normal shutdown
                        }
                    } else {
                        // Wait for the next command to prevent the CPU from idling.
                        match task_receiver.recv().await {
                            Some(task) => SelectTask::Control(task),
                            None => return Ok(()), // control channel closed while paused
                        }
                    }
                }
            };

            match select_task {
                SelectTask::Control(task) => {
                    match task {
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
                    }
                }

                SelectTask::Permit(permit) => {
                    if !paused {
                        let chunk = reader.read().await?;
                        if chunk.is_empty() {
                            paused = true;
                        }
                        // Send an empty chunk to prevent read from blocking.
                        permit.send(chunk);
                    } else {
                        yield_now().await;
                    }
                }
            }
        }
    }
}
