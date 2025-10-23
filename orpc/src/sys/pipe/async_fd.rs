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

#![allow(unused)]

use crate::err_box;
use crate::io::IOResult;
use crate::sys::pipe::BorrowedFd;
use crate::sys::{CInt, RawIO};
use crate::{io_loop_check, sys};
use std::io::IoSlice;
use tokio::io::Interest;

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};

#[cfg(target_os = "linux")]
use tokio::io::unix::AsyncFd as UnixAsyncFd;

#[cfg(not(target_os = "linux"))]
type AsyncInner = BorrowedFd;

#[cfg(target_os = "linux")]
type AsyncInner = tokio::io::unix::AsyncFd<BorrowedFd>;

// Asynchronous read and write pipeline, it is encapsulated on tokio AsyncFd.
pub struct AsyncFd(AsyncInner);

impl AsyncFd {
    pub fn new(fd: BorrowedFd) -> IOResult<AsyncFd> {
        #[cfg(not(target_os = "linux"))]
        {
            Ok(AsyncFd(fd))
        }

        #[cfg(target_os = "linux")]
        {
            if fd.is_blocking() {
                err_box!("Blocking pipes cannot use async io")
            } else {
                let async_fd = UnixAsyncFd::new(fd)?;
                Ok(AsyncFd(async_fd))
            }
        }
    }

    pub fn create(fd: BorrowedFd) -> IOResult<Option<AsyncFd>> {
        if fd.is_blocking() {
            Ok(None)
        } else {
            let res = Self::new(fd)?;
            Ok(Some(res))
        }
    }

    pub fn raw_fd(&self) -> RawIO {
        #[cfg(not(target_os = "linux"))]
        {
            panic!("unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            self.0.as_raw_fd()
        }
    }

    pub async fn writable(&self) -> IOResult<()> {
        #[cfg(not(target_os = "linux"))]
        {
            err_box!("unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            let _ = self.0.writable().await?;
            Ok(())
        }
    }

    pub async fn readable(&self) -> IOResult<()> {
        #[cfg(not(target_os = "linux"))]
        {
            err_box!("unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            let _ = self.0.readable().await?;
            Ok(())
        }
    }

    pub async fn async_io<R>(
        &self,
        interest: Interest,
        mut f: impl FnMut(&BorrowedFd) -> IOResult<R>,
    ) -> IOResult<R> {
        #[cfg(not(target_os = "linux"))]
        {
            err_box!("unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            let res = self
                .0
                .async_io(interest, |inner| match f(inner) {
                    Ok(res) => Ok(res),
                    Err(e) => Err(e.into_raw()),
                })
                .await?;
            Ok(res)
        }
    }

    pub async fn async_write<R>(&self, f: impl FnMut(&BorrowedFd) -> IOResult<R>) -> IOResult<R> {
        self.async_io(Interest::WRITABLE, f).await
    }

    pub async fn async_read<R>(&self, f: impl FnMut(&BorrowedFd) -> IOResult<R>) -> IOResult<R> {
        self.async_io(Interest::READABLE, f).await
    }

    // The task_inner method functions: remove the pipeline read and write events from the poller
    pub fn deregister(self) -> BorrowedFd {
        #[cfg(not(target_os = "linux"))]
        {
            self.0
        }

        #[cfg(target_os = "linux")]
        {
            self.0.into_inner()
        }
    }

    // Write data into fd.
    pub async fn write_iov(&self, len: usize, iov: &[IoSlice<'_>]) -> IOResult<()> {
        let len = len as CInt;
        loop {
            let res = self.async_write(|fd| sys::writev(fd.fd(), iov)).await;

            io_loop_check!(res, len)?;
        }

        Ok(())
    }

    pub fn into_inner(self) -> AsyncInner {
        self.0
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for AsyncFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}
