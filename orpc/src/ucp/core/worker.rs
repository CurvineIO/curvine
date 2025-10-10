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
use crate::sync::StateCtl;
use crate::sys::pipe::{AsyncFd, BorrowedFd};
use crate::sys::{RawIO, RawPtr};
use crate::ucp::bindings::*;
use crate::ucp::stderr;
use crate::{err_box, err_ucs, sys};
use num_enum::{FromPrimitive, IntoPrimitive};
use std::mem;
use std::mem::MaybeUninit;
use std::sync::Arc;
use tokio::task::yield_now;
use crate::ucp::core::Context;

#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
enum State {
    #[num_enum(default)]
    Init,
    Polling,
    Stopped,
}

pub struct Worker {
    inner: RawPtr<ucp_worker>,
    context: Arc<Context>,
    state: StateCtl,
}

impl Worker {
    pub fn new(context: Arc<Context>) -> IOResult<Self> {
        let params = ucp_worker_params_t {
            field_mask: ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as _,
            thread_mode: ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE,
            ..unsafe { mem::zeroed() }
        };

        let mut inner = MaybeUninit::<*mut ucp_worker>::uninit();
        let status =
            unsafe { ucp_worker_create(context.as_mut_ptr(), &params, inner.as_mut_ptr()) };
        err_ucs!(status)?;

        Ok(Self {
            inner: RawPtr::from_uninit(inner),
            context,
            state: StateCtl::new(State::Init.into()),
        })
    }

    pub fn as_ptr(&self) -> *const ucp_worker {
        self.inner.as_ptr()
    }

    pub fn as_mut_ptr(&self) -> *mut ucp_worker {
        self.inner.as_mut_ptr()
    }

    pub fn progress(&self) -> u32 {
        unsafe { ucp_worker_progress(self.inner.as_mut_ptr()) }
    }

    pub fn print(&self) {
        unsafe { ucp_worker_print_info(self.inner.as_mut_ptr(), stderr) }
    }

    /// Waits (blocking) until an event has happened.
    pub fn wait(&self) -> IOResult<()> {
        let status = unsafe { ucp_worker_wait(self.inner.as_mut_ptr()) };
        err_ucs!(status)
    }

    pub fn raw_fd(&self) -> IOResult<RawIO> {
        let mut fd: RawIO = 0;
        let status = unsafe { ucp_worker_get_efd(self.inner.as_mut_ptr(), &mut fd) };
        err_ucs!(status)?;
        Ok(fd)
    }

    pub fn arm(&self) -> IOResult<bool> {
        let status = unsafe { ucp_worker_arm(self.as_mut_ptr()) };

        match status {
            ucs_status_t::UCS_OK => Ok(true),
            ucs_status_t::UCS_ERR_BUSY => Ok(false),
            status => {
                err_ucs!(status)?;
                err_box!("ucp_worker_arm failed: {:?}", status)
            }
        }
    }

    pub async fn event_poll(&self) -> IOResult<()> {
        if !self
            .state
            .compare_and_set(State::Init.into(), State::Polling.into())
        {
            return err_box!("worker polling already started");
        }

        let fd = self.raw_fd()?;
        sys::set_pipe_blocking(fd, false)?;
        let fd = AsyncFd::new(BorrowedFd::new(fd))?.into_inner();

        let mut total_events = 0;
        while self.state.state::<State>() == State::Polling {
            while self.progress() != 0 {
                total_events += 1;
                if total_events > 100 {
                    yield_now().await;
                    total_events = 0;
                }
            }

            if self.arm()? {
                let mut ready = fd.readable().await?;
                ready.clear_ready();
            }
        }

        Ok(())
    }

    pub fn context(&self) -> &Arc<Context> {
        &self.context
    }

    pub fn stop(&self) {
        self.state.set_state(State::Stopped);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe { ucp_worker_destroy(self.inner.as_mut_ptr()) }
    }
}

impl Default for Worker {
    fn default() -> Self {
        let context = Arc::new(Context::default());
        Self::new(context).expect("Default worker")
    }
}
