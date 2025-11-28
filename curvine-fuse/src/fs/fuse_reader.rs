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

use crate::fs::operator::Read;
use crate::session::FuseResponse;
use curvine_client::unified::UnifiedReader;
use curvine_common::conf::FuseConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path, Reader};
use curvine_common::state::FileStatus;
use curvine_common::FsResult;
use log::error;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use std::sync::Arc;

enum ReadTask {
    Read(i64, usize, FuseResponse),
    Complete(CallSender<i8>, Option<FuseResponse>),
}

pub struct FuseReader {
    path: Path,
    len: i64,
    sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
    status: FileStatus,
}

impl FuseReader {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, reader: UnifiedReader) -> Self {
        let path = reader.path().clone();
        let len = reader.len();
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();
        let status = reader.status().clone();

        let monitor = err_monitor.clone();
        rt.spawn(async move {
            let res = Self::read_future(reader, receiver).await;
            match res {
                Ok(_) => (),

                Err(e) => {
                    error!("fuse reader error: {}", e);
                    monitor.set_error(e);
                }
            }
        });

        Self {
            path,
            len,
            sender,
            err_monitor,
            status,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn status(&self) -> &FileStatus {
        &self.status
    }

    fn check_error(&self, e: FsError) -> FsError {
        self.err_monitor.take_error().unwrap_or(e)
    }

    pub async fn read(&mut self, op: Read<'_>, reply: FuseResponse) -> FsResult<()> {
        let res = self
            .sender
            .send(ReadTask::Read(
                op.arg.offset as i64,
                op.arg.size as usize,
                reply,
            ))
            .await
            .map_err(|e| self.check_error(e.into()));
        res
    }

    pub async fn complete(&mut self, reply: Option<FuseResponse>) -> FsResult<()> {
        let fun = async {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(ReadTask::Complete(rx, reply)).await?;
            tx.receive().await?;
            Ok::<(), FsError>(())
        };
        fun.await.map_err(|e| self.check_error(e))
    }

    async fn read_future(
        mut reader: UnifiedReader,
        mut req_receiver: AsyncReceiver<ReadTask>,
    ) -> FsResult<()> {
        while let Some(task) = req_receiver.recv().await {
            match task {
                ReadTask::Read(off, len, reply) => {
                    let data = reader.fuse_read(off, len).await;
                    reply.send_data(data.map_err(|x| x.into())).await?;
                }

                ReadTask::Complete(tx, reply) => {
                    let res = reader.complete().await;
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
