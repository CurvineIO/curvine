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

use crate::io::IOResult;
use crate::sys;
use crate::sys::RawIO;

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, RawFd};

// Encapsulated file descriptor describing the pipeline, which holds ownership of fd.
#[derive(Debug)]
pub struct OwnedFd(RawIO);

impl OwnedFd {
    pub fn new(fd: RawIO) -> Self {
        OwnedFd(fd)
    }

    pub fn raw_fd(&self) -> RawIO {
        self.0
    }

    pub fn is_blocking(&self) -> bool {
        sys::pipe_is_blocking(self.0)
    }

    pub fn as_borrowed(&self) -> BorrowedFd {
        BorrowedFd::new(self.0)
    }

    pub fn set_blocking(&self, blocking: bool) -> IOResult<()> {
        sys::set_pipe_blocking(self.0, blocking)
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for OwnedFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for OwnedFd {
    fn drop(&mut self) {
        if let Err(e) = sys::close_raw_io(self.0) {
            log::warn!("drop fd {}:{}", self.0, e)
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct BorrowedFd(RawIO);

impl BorrowedFd {
    pub fn new(fd: RawIO) -> Self {
        BorrowedFd(fd)
    }

    pub fn fd(&self) -> RawIO {
        self.0
    }

    pub fn is_blocking(&self) -> bool {
        sys::pipe_is_blocking(self.0)
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for BorrowedFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}
