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

use crate::sync::{StateCtl, StateListener, StateMonitor};
use crate::CommonResult;
use num_enum::{FromPrimitive, IntoPrimitive};

#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum ServerState {
    #[num_enum(default)]
    Init = 0,

    // The service is running, such as tcp bind has been completed.
    Running = 1,

    // The service is in shutdown state, not accepting new requests, is cleaning up resources and processing unfinished tasks.
    Shutdown = 2,

    // The service has been executed to the last line and the service has stopped.
    Stop = 3,
}

// Asynchronous task status monitor.
#[derive(Clone)]
pub struct ServerMonitor(StateMonitor);

impl ServerMonitor {
    pub fn new() -> Self {
        Self(StateMonitor::new(ServerState::Init.into()))
    }

    pub fn advance_running(&self) {
        self.0.advance_state(ServerState::Running, true);
    }

    pub fn advance_shutdown(&self) {
        self.0.advance_state(ServerState::Shutdown, true);
    }

    pub fn advance_stop(&self) {
        self.0.advance_state(ServerState::Stop, true);
    }

    pub fn new_listener(&self) -> ServerStateListener {
        ServerStateListener(self.0.new_listener())
    }

    pub fn read_ctl(&self) -> StateCtl {
        self.0.read_ctl()
    }

    pub fn state(&self) -> ServerState {
        self.0.state()
    }
}

impl Default for ServerMonitor {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ServerStateListener(StateListener);

impl ServerStateListener {
    pub async fn wait_running(&mut self) -> CommonResult<()> {
        self.0.wait_state(ServerState::Running).await
    }

    pub async fn wait_shutdown(&mut self) -> CommonResult<()> {
        self.0.wait_state(ServerState::Shutdown).await
    }

    pub async fn wait_stop(&mut self) -> CommonResult<()> {
        self.0.wait_state(ServerState::Stop).await
    }
}
