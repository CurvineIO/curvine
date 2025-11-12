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

use crate::fs::FileSystem;
use crate::session::{FuseTask, ResponseData};
use crate::FuseResult;
use log::{error, info, warn};
use orpc::io::IOResult;
use orpc::runtime::Runtime;
use orpc::sync::channel::AsyncReceiver;
use orpc::sys::pipe::{AsyncFd, Pipe2, PipeFd};
use orpc::{err_box, sys};
use std::sync::Arc;

/// FuseSender
/// Reads data from queue and writes to fuse fd.
/// 1. For metadata requests, write response directly
/// 2. For read/write data requests, process then write response
pub struct FuseSender<T> {
    pub fs: Arc<T>,
    rt: Arc<Runtime>,
    kernel_fd: Arc<AsyncFd>,
    receiver: AsyncReceiver<FuseTask>,
    pipe2: Pipe2,
    debug: bool,
}

impl<T: FileSystem> FuseSender<T> {
    pub fn new(
        fs: Arc<T>,
        rt: Arc<Runtime>,
        kernel_fd: Arc<AsyncFd>,
        receiver: AsyncReceiver<FuseTask>,
        buf_size: usize,
        debug: bool,
    ) -> IOResult<Self> {
        let pipe2 = Pipe2::new(PipeFd::new(buf_size, false, false)?)?;
        let fuse_rx = Self {
            fs,
            rt,
            kernel_fd,
            receiver,
            pipe2,
            debug,
        };

        Ok(fuse_rx)
    }

    pub fn rt(&self) -> &Runtime {
        &self.rt
    }

    pub async fn start(mut self) -> FuseResult<()> {
        while let Some(task) = self.receiver.recv().await {
            match task {
                FuseTask::Reply(reply) => {
                    let id = reply.header.unique;
                    if let Err(e) = self.send(reply).await {
                        warn!("error send unique {}: {}", id, e);
                    }
                }

                FuseTask::Request(_) => {
                    error!("Not support")
                }
            }
        }
        Ok(())
    }

    // Send response data to fuse.
    pub async fn send(&mut self, rep: ResponseData) -> IOResult<()> {
        if self.debug {
            info!("reply {:?}", rep.header);
        }
        self.splice(rep).await
    }

    pub async fn write(&mut self, rep: ResponseData) -> IOResult<()> {
        let (_, iovec) = rep.as_iovec()?;
        self.kernel_fd
            .async_write(|fd| sys::writev(fd.fd(), &iovec))
            .await?;
        Ok(())
    }

    async fn splice(&mut self, rep: ResponseData) -> IOResult<()> {
        let (len, iovec) = rep.as_iovec()?;
        let write_len = self.pipe2.write_iov(&iovec).await?;
        if write_len != len {
            return err_box!("io return value error, res: {}, expect: {}", write_len, len);
        }

        let read_len = self.pipe2.read_io(&self.kernel_fd, len).await?;
        if read_len != len {
            err_box!("io return value error, res: {}, expect: {}", read_len, len)
        } else {
            Ok(())
        }
    }
}
