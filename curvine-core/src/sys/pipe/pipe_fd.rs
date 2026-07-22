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
use crate::sys::pipe::OwnedFd;

#[derive(Debug)]
pub struct PipeFd {
    pub(crate) buf_size: usize,
    pub(crate) read: OwnedFd,
    pub(crate) write: OwnedFd,
}

impl PipeFd {
    pub fn new(buf_size: usize, read_blocking: bool, write_blocking: bool) -> IOResult<Self> {
        let [read_fd, write_fd] = sys::pipe2(buf_size)?;
        sys::set_pipe_blocking(read_fd, read_blocking)?;
        sys::set_pipe_blocking(write_fd, write_blocking)?;

        let fd = Self {
            buf_size,
            read: OwnedFd::new(read_fd),
            write: OwnedFd::new(write_fd),
        };

        Ok(fd)
    }
}
