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
use curvine_common::fs::Reader;
use curvine_common::FsResult;
use log::{error, info};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{AsyncChannel, AsyncReceiver, AsyncSender, CallChannel, CallSender};
use orpc::sync::ErrorMonitor;
use std::sync::Arc;
use clap::builder::TypedValueParser;

enum ReadTask {
    Read(i64, usize, FuseResponse),
    Complete(CallSender<i8>),
}

pub struct FuseReader {
    sender: AsyncSender<ReadTask>,
    err_monitor: Arc<ErrorMonitor<FsError>>,
}

impl FuseReader {
    pub fn new(conf: &FuseConf, rt: Arc<Runtime>, reader: UnifiedReader) -> Self {
        let err_monitor = Arc::new(ErrorMonitor::new());
        let (sender, receiver) = AsyncChannel::new(conf.stream_channel_size).split();

        let monitor = err_monitor.clone();
        rt.spawn(async move {
            let res = Self::read_future(reader, receiver).await;
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("fuse reader error: {}", e);
                    monitor.set_error(e);
                }
            }
        });

        Self {
            sender,
            err_monitor,
        }
    }

    fn check_error(&self, e: FsError) -> FsError {
        match self.err_monitor.take_error() {
            Some(e) => e,
            None => e,
        }
    }

    pub async fn read(&mut self, op: Read<'_>, rep: FuseResponse) -> FsResult<()> {
        let task = ReadTask::Read(op.arg.offset as i64, op.arg.size as usize, rep);

        // Check for errors before sending to avoid using invalid channel
        if let Some(error) = self.err_monitor.take_error() {
            return Err(error);
        }

        match self.sender.send(task).await {
            Err(e) => {
                // Channel may be closed - check for underlying error
                Err(self.check_error(e.into()))
            },
            Ok(_) => Ok(()),
        }
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        let res: FsResult<()> = {
            let (rx, tx) = CallChannel::channel();
            self.sender.send(ReadTask::Complete(rx)).await?;
            tx.receive().await?;
            Ok(())
        };

        match res {
            Err(e) => Err(self.check_error(e)),
            Ok(_) => Ok(()),
        }
    }

    async fn read_future(
        mut reader: UnifiedReader,
        mut req_receiver: AsyncReceiver<ReadTask>,
    ) -> FsResult<()> {
        while let Some(task) = req_receiver.recv().await {
            match task {
                ReadTask::Read(off, len, reply) => {
                    let data = reader.fuse_read(off, len).await;
                    let read_chunk: usize = match &data {
                        Err(e) => 0,
                        Ok(v) => v.iter().map(|x| x.len()).sum()
                    };
                    info!("read path {}, len {}, off {} chunk_len {}", reader.path(), len, off, read_chunk);
                    reply.send_data(data.map_err(|x| x.into())).await?;
                }

                ReadTask::Complete(tx) => {
                    reader.complete().await?;
                    tx.send(1)?;
                }
            }
        }
        Ok(())
    }
}
