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
use crate::io::IOResult;
use crate::sys::pipe::{AsyncFd, BorrowedFd};
use crate::sys::RawIO;

// Read data from the pipeline.
pub struct PipeReader {
    pub(crate) fd: BorrowedFd,
    pub(crate) async_fd: Option<AsyncFd>,
}

impl PipeReader {
    pub fn new(fd: BorrowedFd) -> IOResult<Self> {
        let async_fd = AsyncFd::create(fd)?;
        Ok(Self { fd, async_fd })
    }

    pub async fn async_read<R>(&self, f: impl FnMut(&BorrowedFd) -> IOResult<R>) -> IOResult<R> {
        if let Some(fd) = &self.async_fd {
            fd.async_read(f).await
        } else {
            err_box!("fd is not an asynchronous {}", self.raw_fd())
        }
    }

    pub fn raw_fd(&self) -> RawIO {
        self.fd.fd()
    }

    pub fn async_fd(&self) -> Option<&AsyncFd> {
        self.async_fd.as_ref()
    }

    pub fn deregister(&mut self) -> Option<BorrowedFd> {
        self.async_fd.take().map(|x| x.deregister())
    }
}
