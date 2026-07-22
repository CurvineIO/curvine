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

use crate::err_box;
use crate::handler::RpcFrame;
use crate::io::IOResult;
use crate::sys::pipe::{AsyncFd, BorrowedFd};
use crate::sys::{self, RawIO};

// Write data into the pipeline.
pub struct PipeWriter {
    pub(crate) fd: BorrowedFd,
    pub(crate) async_fd: Option<AsyncFd>,
}

impl PipeWriter {
    pub fn new(fd: BorrowedFd) -> IOResult<Self> {
        let async_fd = AsyncFd::create(fd)?;
        Ok(Self { fd, async_fd })
    }

    pub async fn splice_in(&self, io: &RpcFrame, len: usize) -> IOResult<()> {
        sys::splice_in_full(io, None, self.raw_fd(), None, len).await
    }

    pub fn raw_fd(&self) -> RawIO {
        self.fd.fd()
    }

    pub fn is_async(&self) -> bool {
        self.async_fd.is_some()
    }

    pub fn async_fd(&self) -> Option<&AsyncFd> {
        self.async_fd.as_ref()
    }

    pub fn deregister(&mut self) -> Option<BorrowedFd> {
        self.async_fd.take().map(|x| x.deregister())
    }

    pub async fn async_write<R>(&self, f: impl FnMut(&BorrowedFd) -> IOResult<R>) -> IOResult<R> {
        if let Some(fd) = &self.async_fd {
            fd.async_write(f).await
        } else {
            err_box!("fd is not an asynchronous {}", self.raw_fd())
        }
    }
}
