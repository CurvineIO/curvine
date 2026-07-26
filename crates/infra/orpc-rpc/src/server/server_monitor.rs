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

    /// Wait until the server reaches Running, or fail fast if it shuts down before binding.
    pub async fn wait_startup(&mut self) -> CommonResult<()> {
        use crate::err_box;

        let running = ServerState::Running.into();
        let shutdown = ServerState::Shutdown.into();

        if self.0.current() == running {
            return Ok(());
        }
        if self.0.current() >= shutdown {
            return err_box!("server failed to start");
        }

        loop {
            let state = self.0.next_state().await?;
            if state == running {
                return Ok(());
            }
            if state >= shutdown {
                return err_box!("server failed to start");
            }
            if self.0.current() == running {
                return Ok(());
            }
            if self.0.current() >= shutdown {
                return err_box!("server failed to start");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{AsyncRuntime, RpcRuntime};

    #[test]
    fn wait_startup_fails_when_shutdown_before_running() {
        let monitor = ServerMonitor::new();
        let mut listener = monitor.new_listener();
        let rt = AsyncRuntime::single();

        rt.block_on(async {
            monitor.advance_shutdown();
            monitor.advance_stop();
            assert!(listener.wait_startup().await.is_err());
        });
    }

    #[test]
    fn wait_startup_succeeds_when_running() {
        let monitor = ServerMonitor::new();
        let mut listener = monitor.new_listener();
        let rt = AsyncRuntime::single();

        rt.block_on(async {
            monitor.advance_running();
            listener.wait_startup().await.unwrap();
        });
    }
}
