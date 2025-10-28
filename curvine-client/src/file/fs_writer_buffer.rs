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

use crate::file::fs_writer_buffer::WriterAdapter::{Base, Buffer};
use crate::file::FsWriterBase;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use log::error;
use orpc::runtime::RpcRuntime;
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;

// Control task type
enum WriterTask {
    Flush(CallSender<i8>),
    Complete((bool, CallSender<i8>)),
    Seek((i64, CallSender<i8>)),
}

enum SelectTask {
    Control(WriterTask),
    Data(DataSlice),
}

struct BufferChannel {
    chunk_sender: AsyncSender<DataSlice>,
    task_sender: AsyncSender<WriterTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
}

impl BufferChannel {
    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        match self.chunk_sender.send(data).await {
            Ok(_) => Ok(()),
            Err(e) => Err(self.check_error(e.into())),
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();
            self.task_sender
                .send(WriterTask::Complete((false, tx)))
                .await?;
            rx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();
            self.task_sender.send(WriterTask::Flush(tx)).await?;
            rx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        let res: FsResult<()> = {
            let (tx, rx) = CallChannel::channel();

            if let Err(e) = self.task_sender.send(WriterTask::Seek((pos, tx))).await {
                return Err(e.into());
            }

            if let Err(e) = rx.receive().await {
                return Err(e.into());
            }
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum WriterAdapter {
    Buffer(BufferChannel),
    Base(FsWriterBase),
}

impl WriterAdapter {
    async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        match self {
            Base(w) => w.write(data).await,
            Buffer(w) => w.write(data).await,
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            Base(w) => w.flush().await,
            Buffer(w) => w.flush().await,
        }
    }
    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Base(w) => w.complete().await,
            Buffer(w) => w.complete().await,
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            Base(w) => w.seek(pos).await,
            Buffer(w) => w.seek(pos).await,
        }
    }
}

// Reader with buffer.
pub struct FsWriterBuffer {
    path: Path,
    status: FileStatus,
    writer: WriterAdapter,
    pos: i64,
}

impl FsWriterBuffer {
    pub fn new(writer: FsWriterBase, chunk_num: usize) -> Self {
        let err_monitor = Arc::new(ErrorMonitor::new());
        let path = writer.path().clone();
        let status = writer.status().clone();
        let pos = writer.pos();

        let writer = if chunk_num == 1 {
            Base(writer)
        } else {
            let (chunk_sender, chunk_receiver) = AsyncChannel::new(chunk_num).split();
            let (task_sender, task_receiver) = AsyncChannel::new(2).split();
            let monitor = err_monitor.clone();

            let rt = writer.fs_context().clone_runtime();
            rt.spawn(async move {
                let res = Self::write_future(chunk_receiver, task_receiver, writer).await;
                match res {
                    Ok(_) => {}
                    Err(e) => {
                        error!("buffer writer error: {:?}", e);
                        monitor.set_error(e);
                    }
                }
            });

            Buffer(BufferChannel {
                chunk_sender,
                task_sender,
                err_monitor,
            })
        };

        Self {
            path,
            status,
            writer,
            pos,
        }
    }

    pub fn path_str(&self) -> &str {
        self.path.path()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
        let len = data.len();
        if len == 0 {
            return Ok(());
        }
        self.writer.write(data).await?;
        self.pos += len as i64;
        Ok(())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        self.writer.complete().await
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        self.writer.flush().await
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.writer.seek(pos).await?;
        self.pos = pos;
        Ok(())
    }

    async fn write_future(
        mut chunk_receiver: AsyncReceiver<DataSlice>,
        mut task_receiver: AsyncReceiver<WriterTask>,
        mut writer: FsWriterBase,
    ) -> FsResult<()> {
        loop {
            // The queue can be written and controlled to complete any future.
            let select_task = tokio::select! {
                biased;

                chunk = chunk_receiver.recv_check() => {
                   SelectTask::Data(chunk?)
                }

                task = task_receiver.recv_check() => {
                    SelectTask::Control(task?)
                }
            };

            match select_task {
                SelectTask::Control(task) => match task {
                    WriterTask::Flush(cx) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?;
                        }
                        writer.flush().await?;
                        cx.send(1)?;
                    }

                    WriterTask::Complete((_, tx)) => {
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?;
                        }
                        writer.complete().await?;
                        tx.send(1)?;
                        return Ok(());
                    }

                    WriterTask::Seek((pos, tx)) => {
                        // Process all buffered data first
                        while let Some(chunk) = chunk_receiver.try_recv()? {
                            writer.write(chunk).await?
                        }

                        // Execute seek operation
                        writer.seek(pos).await?;

                        if let Err(e) = tx.send(1) {
                            return Err(e.into());
                        }
                    }
                },

                SelectTask::Data(chunk) => {
                    writer.write(chunk).await?;
                }
            }
        }
    }
}
