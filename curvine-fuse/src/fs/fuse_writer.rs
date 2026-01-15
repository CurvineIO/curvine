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

use crate::fs::operator::Write;
use crate::raw::fuse_abi::fuse_write_out;
use crate::session::FuseResponse;
use curvine_client::unified::UnifiedWriter;
use curvine_common::conf::FuseConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::{FileAllocOpts, FileStatus};
use curvine_common::FsResult;
use log::error;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use orpc::sys::DataSlice;
use std::sync::Arc;
use tokio_util::bytes::Bytes;

enum WriteTask {
    Write(i64, Bytes, FuseResponse),
    Flush(CallSender<i8>, Option<FuseResponse>),
    Complete(CallSender<i8>, Option<FuseResponse>),
    Resize(CallSender<i8>, FileAllocOpts),
}

pub struct FuseWriter {
    path: Path,
    sender: AsyncSender<WriteTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    status: FileStatus,
    is_ufs: bool,
}

impl FuseWriter {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, writer: UnifiedWriter, ino: u64) -> Self {
        let is_ufs = !writer.path().is_cv();
        let path = writer.path().clone();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();

        let status = writer.status().clone();
        let monitor = err_monitor.clone();

        rt.spawn(async move {
            let res = Self::writer_future(writer, receiver, ino).await;
            match res {
                Ok(_) => (),

                Err(e) => {
                    error!("fuse writer error: {}", e);
                    monitor.set_error(e);
                }
            }
        });

        Self {
            path,
            sender,
            err_monitor,
            status,
            is_ufs,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    pub fn is_ufs(&self) -> bool {
        self.is_ufs
    }

    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    pub async fn write(&mut self, op: Write<'_>, reply: FuseResponse) -> FsResult<()> {
        self.sender
            .send(WriteTask::Write(op.arg.offset as i64, op.data, reply))
            .await
            .map_err(|e| self.check_error(e.into()))
    }

    pub async fn flush(&mut self, reply: Option<FuseResponse>) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Flush(rx, reply)).await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    pub async fn complete(&mut self, reply: Option<FuseResponse>) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Complete(rx, reply)).await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    pub async fn resize(&mut self, opts: FileAllocOpts) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Resize(rx, opts)).await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn writer_future(
        mut writer: UnifiedWriter,
        mut req_receiver: AsyncReceiver<WriteTask>,
        ino: u64,
    ) -> FsResult<()> {
        while let Some(task) = req_receiver.recv().await {
            match task {
                WriteTask::Write(off, data, reply) => {
                    let len = data.len();
                    let res: FsResult<fuse_write_out> = writer
                        .fuse_write(off, DataSlice::Bytes(data))
                        .await
                        .map(|_| fuse_write_out {
                            size: len as u32,
                            padding: 0,
                        });
                    reply.send_rep(res).await?;
                }

                WriteTask::Flush(tx, reply) => {
                    let res = writer.flush().await;
                    if let Some(reply) = reply {
                        reply.send_inode_out(ino, 0, -1).await?;
                        reply.send_rep(res).await?;
                    }
                    tx.send(1)?;
                }

                WriteTask::Complete(tx, reply) => {
                    let res = writer.complete().await;
                    if let Some(reply) = reply {
                        reply.send_inode_out(ino, 0, -1).await?;
                        reply.send_rep(res).await?;
                    }
                    tx.send(1)?;
                }

                WriteTask::Resize(tx, opts) => {
                    writer.resize(opts).await?;
                    tx.send(1)?;
                }
            }
        }

        Ok(())
    }
}
