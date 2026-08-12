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

use crate as sys;
use crate::pipe::BorrowedFd;
use crate::{CInt, RawIO, SysResult};
use std::io::{ErrorKind, IoSlice};
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
    pub fn new(fd: BorrowedFd) -> SysResult<AsyncFd> {
        #[cfg(not(target_os = "linux"))]
        {
            Ok(AsyncFd(fd))
        }

        #[cfg(target_os = "linux")]
        {
            if fd.is_blocking() {
                sys_error!(
                    ErrorKind::InvalidInput,
                    "Blocking pipes cannot use async io"
                )
            } else {
                let async_fd = UnixAsyncFd::new(fd)?;
                Ok(AsyncFd(async_fd))
            }
        }
    }

    pub fn create(fd: BorrowedFd) -> SysResult<Option<AsyncFd>> {
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

    pub async fn writable(&self) -> SysResult<()> {
        #[cfg(not(target_os = "linux"))]
        {
            sys_error!(ErrorKind::Unsupported, "unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            let _ = self.0.writable().await?;
            Ok(())
        }
    }

    pub async fn readable(&self) -> SysResult<()> {
        #[cfg(not(target_os = "linux"))]
        {
            sys_error!(ErrorKind::Unsupported, "unsupported operation")
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
        mut f: impl FnMut(&BorrowedFd) -> SysResult<R>,
    ) -> SysResult<R> {
        #[cfg(not(target_os = "linux"))]
        {
            sys_error!(ErrorKind::Unsupported, "unsupported operation")
        }

        #[cfg(target_os = "linux")]
        {
            let res = self.0.async_io(interest, |inner| f(inner)).await?;
            Ok(res)
        }
    }

    pub async fn async_write<R>(&self, f: impl FnMut(&BorrowedFd) -> SysResult<R>) -> SysResult<R> {
        self.async_io(Interest::WRITABLE, f).await
    }

    pub async fn async_read<R>(&self, f: impl FnMut(&BorrowedFd) -> SysResult<R>) -> SysResult<R> {
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

    // Write data into fd. A short writev is normal on non-blocking fds, so the
    // remaining iovec view is advanced and the write resumed until `len` bytes land.
    pub async fn write_iov(&self, len: usize, iov: &[IoSlice<'_>]) -> SysResult<()> {
        let mut written = 0usize;
        while written < len {
            let res = if written == 0 {
                self.async_write(|fd| sys::writev(fd.fd(), iov)).await
            } else {
                let remaining = sys::skip_iov_bytes(iov, written);
                self.async_write(|fd| sys::writev(fd.fd(), &remaining))
                    .await
            };

            match res {
                Ok(0) => {
                    return sys_error!(ErrorKind::WriteZero, "writev returned 0");
                }
                Ok(transferred) => written += transferred as usize,
                Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error),
            }
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
