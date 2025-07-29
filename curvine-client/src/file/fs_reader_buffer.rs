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

use crate::file::{FsContext, FsReaderParallel};
use crate::FileChunk;
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
    Start,
    Stop(CallSender<i8>),
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
    fn check_error(&self, e: FsError) -> FsError {
        match self.err_monitor.take_error() {
            Some(e) => e,
            None => e,
        }
    }

    async fn read(&mut self) -> FsResult<FileChunk> {
        match self.chunk_receiver.recv_check().await {
            Ok(v) => Ok(v),
            Err(e) => Err(self.check_error(e.into())),
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        let res: FsResult<()> = {
            // Notify seek and seek will pause data reading.
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(ReadTask::Seek(pos, tx)).await?;
            rx.receive().await?;

            // Clear the buffer data.
            while (self.chunk_receiver.try_recv()?).is_some() {}

            // Restart the read task.
            self.task_sender.send(ReadTask::Start).await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            // Send a stop command and wait for the command to complete
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(ReadTask::Stop(tx)).await?;
            rx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }
}

// Sequential read and write, chunk_num > 1, use a buffered reader to read data in advance, and reduce network latency.
// Random read and write, chunk_num = 1, use a reader without buffer to read data directly from the remote end.
enum ReaderAdapter {
    Buffer(BufferChannel),
    Base(Box<FsReaderParallel>),
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
}

// Reader with buffer.
pub struct FsReaderBuffer {
    readers: Vec<ReaderAdapter>,
    path: Path,
    pos: i64,
    len: i64,

    read_parallel: i64,
    slice_size: i64,
}

impl FsReaderBuffer {
    pub fn new(path: Path, fs_context: Arc<FsContext>, file_blocks: FileBlocks) -> FsResult<Self> {
        let rt = fs_context.clone_runtime();
        let err_monitor = Arc::new(ErrorMonitor::new());

        let conf = &fs_context.conf.client;
        let chunk_num = conf.read_chunk_num;
        let chunk_size = conf.read_chunk_size;
        let read_parallel = conf.read_parallel;
        let slice_size = conf.read_slice_size;

        let pos = 0;
        let len = file_blocks.status.len;

        let all = FsReaderParallel::create_all(
            path.clone(),
            fs_context,
            file_blocks,
            read_parallel,
            slice_size,
            chunk_size,
        )?;

        let mut readers = Vec::with_capacity(all.len());
        for reader in all {
            let reader = if chunk_num == 1 {
                ReaderAdapter::Base(Box::new(reader))
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

        let reader = Self {
            readers,
            path,
            pos,
            len,
            read_parallel,
            slice_size,
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
        let id = self.pos / self.slice_size % self.read_parallel;
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
        // The chunk read by the underlying reader may be aligned according to chunk_size, so when returning data, you need to discard the excess data
        let diff = self.pos - chunk.off;
        let bytes = if diff == 0 {
            chunk.data
        } else if diff > 0 && diff <= chunk.len() as i64 {
            chunk.data.split_off(diff as usize)
        } else {
            return err_box!("Abnormal status");
        };

        self.pos += bytes.len() as i64;
        Ok(bytes)
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        for reader in &mut self.readers {
            reader.seek(pos).await?;
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

                task = task_receiver.recv_check() => {
                    SelectTask::Control(task?)
                }

                premit = chunk_sender.reserve() => {
                    if !paused {
                        SelectTask::Permit(premit?)
                    } else {
                        // Wait for the next command to prevent the CPU from idling.
                        SelectTask::Control(task_receiver.recv_check().await?)
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
                            reader.seek(pos).await?;
                            paused = true;
                            tx.send(1)?;
                        }

                        ReadTask::Start => paused = false,

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
