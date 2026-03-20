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

use crate::raft::RoleState;
use log::error;
use orpc::runtime::MutEvent;
use orpc::server::ServerState;
use orpc::sync::StateCtl;
use orpc::{err_box, CommonResult};
use std::error::Error;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

// An independent thread of execution of an event loop
pub struct EventLoop<T> {
    thread_name: String,
    dependency_ctl: Option<StateCtl>,
    receiver: mpsc::Receiver<T>,
    timeout: Duration,
}

impl<T> EventLoop<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(
        thread_name: impl Into<String>,
        dependency_ctl: StateCtl,
        receiver: mpsc::Receiver<T>,
        poll_timeout_ms: u64,
    ) -> Self {
        Self {
            thread_name: thread_name.into(),
            dependency_ctl: Some(dependency_ctl),
            receiver,
            timeout: Duration::from_millis(poll_timeout_ms),
        }
    }

    pub fn start<F, E>(self, task: F) -> CommonResult<StateCtl>
    where
        F: MutEvent<T, E> + Send + 'static,
        E: Error + From<String>,
    {
        let name = self.thread_name.to_string();
        let builder = thread::Builder::new().name(name.clone());

        let ctl = self
            .dependency_ctl
            .unwrap_or(StateCtl::new(ServerState::Running.into()));
        let thread_ctl = StateCtl::new(ServerState::Init.into());
        let return_ctl = thread_ctl.read_only();

        let receiver = self.receiver;
        let dur = self.timeout;
        builder.spawn(move || {
            thread_ctl.advance_state(ServerState::Running);
            let res = Self::loop0(ctl, receiver, dur, task);
            thread_ctl.advance_state(ServerState::Stop);
            error!("thread stop: {:?}", res);
        })?;

        Ok(return_ctl)
    }

    fn loop0<F, E>(
        ctl: StateCtl,
        receiver: mpsc::Receiver<T>,
        dur: Duration,
        mut task: F,
    ) -> Result<(), E>
    where
        F: MutEvent<T, E> + Send + 'static,
        E: Error + From<String>,
    {
        // @todo Modify.
        while ctl.value() < RoleState::Exit.into() {
            let event = match receiver.recv_timeout(dur) {
                Ok(v) => Some(v),
                Err(RecvTimeoutError::Timeout) => None,
                Err(e) => return err_box!("event loop: {}", e),
            };

            task.run(event)?;
        }

        Ok(())
    }
}
