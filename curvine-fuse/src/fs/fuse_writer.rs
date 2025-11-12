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
use curvine_common::state::FileStatus;
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
}

pub struct FuseWriter {
    path: Path,
    sender: AsyncSender<WriteTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    status: FileStatus,
}

impl FuseWriter {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, writer: UnifiedWriter) -> Self {
        let path = writer.path().clone();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();

        let status = writer.status().clone();
        let monitor = err_monitor.clone();

        rt.spawn(async move {
            let res = Self::writer_future(writer, receiver).await;
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
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn status(&self) -> &FileStatus {
        &self.status
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
        let res: FsResult<()> = {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Flush(rx, reply)).await?;
            tx.receive().await?;
            Ok(())
        };
        res.map_err(|e| self.check_error(e))
    }

    pub async fn complete(&mut self, reply: Option<FuseResponse>) -> FsResult<()> {
        let res: FsResult<()> = {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(WriteTask::Complete(rx, reply)).await?;
            tx.receive().await?;
            Ok(())
        };
        res.map_err(|e| self.check_error(e))
    }

    async fn writer_future(
        mut writer: UnifiedWriter,
        mut req_receiver: AsyncReceiver<WriteTask>,
    ) -> FsResult<()> {
        while let Some(task) = req_receiver.recv().await {
            match task {
                WriteTask::Write(off, data, reply) => {
                    let res: FsResult<fuse_write_out> = {
                        writer.seek(off).await?;

                        let len = data.len();
                        writer.fuse_write(DataSlice::Bytes(data)).await?;

                        Ok(fuse_write_out {
                            size: len as u32,
                            padding: 0,
                        })
                    };
                    reply.send_rep(res).await?;
                }

                WriteTask::Flush(tx, reply) => {
                    let res = writer.flush().await;
                    if let Some(reply) = reply {
                        reply.send_rep(res).await?;
                    }
                    tx.send(1)?;
                }

                WriteTask::Complete(tx, reply) => {
                    let res = writer.complete().await;
                    if let Some(reply) = reply {
                        reply.send_rep(res).await?;
                    }
                    tx.send(1)?;
                }
            }
        }

        Ok(())
    }
}
