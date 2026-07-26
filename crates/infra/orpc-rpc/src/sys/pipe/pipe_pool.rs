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
use crate::sync::AtomicLen;
use crate::sys::pipe::{Pipe2, PipeFd};
use crate::sys::PIPE_BUF;
use log::warn;
use std::collections::LinkedList;
use std::sync::Mutex;

/// A simple implementation of the pipe resource pool.
/// In a multithreaded environment, closing pipe is an unwise choice and can lead to unexpected problems.
/// @see https://www.man7.org/linux/man-pages/man2/close.2.html
/// Therefore, after pipe is created, it will not be destroyed
/// By copying data through splice, we use blocking pipes.
/// Reading data from the network and processing data is completely separate, and the business processing is performed in blocking threads, which makes it very difficult to use non-blocking pipes.
/// The prerequisite for using blocking pipe is that the size of the pipe is greater than or equal to the size of the network data block, so that there will be no blockage when reading and writing pipe data.
pub struct PipePool {
    cur_cap: AtomicLen,
    max_cap: usize,
    pipe_buf_size: usize,
    state: Mutex<LinkedList<PipeFd>>,
}

impl PipePool {
    pub fn new(init_cap: usize, max_cap: usize, pipe_buf_size: usize) -> Self {
        let pipe_buf_size = if pipe_buf_size == 0 {
            PIPE_BUF
        } else {
            pipe_buf_size
        };

        let mut list = LinkedList::new();
        let cur_cap = AtomicLen::new(0);
        for _ in 0..init_cap {
            let res = PipeFd::new(pipe_buf_size, false, false);
            match res {
                Ok(fd) => {
                    cur_cap.incr();
                    list.push_back(fd);
                }
                Err(e) => warn!("create pipe {}", e),
            };
        }

        Self {
            state: Mutex::new(list),
            cur_cap,
            max_cap,
            pipe_buf_size,
        }
    }

    pub fn acquire(&self) -> IOResult<Option<Pipe2>> {
        if cfg!(not(target_os = "linux")) {
            return Ok(None);
        }

        let mut state = self.state.lock().unwrap();

        // There are resources.
        if let Some(fd) = state.pop_front() {
            // Bind fd and tokio poller.
            let pipe = Pipe2::new(fd)?;
            return Ok(Some(pipe));
        }

        if self.cur_cap.get() >= self.max_cap {
            // Resource cap is reached.
            return Ok(None);
        }

        let fd = PipeFd::new(self.pipe_buf_size, false, false)?;
        let pipe = Pipe2::new(fd)?;
        self.cur_cap.incr();
        Ok(Some(pipe))
    }

    pub fn release(&self, pipe: &mut Pipe2) {
        let mut state = self.state.lock().unwrap();
        state.push_back(pipe.take_fd())
    }
}

impl Default for PipePool {
    fn default() -> Self {
        Self::new(100, 100, PIPE_BUF)
    }
}
