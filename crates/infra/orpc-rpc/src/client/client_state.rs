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

use crate::io::io_error::IOError;
use crate::io::net::InetAddr;
use crate::sync::StateCtl;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::sync::Mutex;

// Connection status
// Normal Normal state.
// Closed is closed normally, such as timeout, set to timeout error.
// Error A closed caused by an error occurred, setting the current error.
#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum InnerState {
    #[num_enum(default)]
    Normal,
    Closed,
    Error,
}

// The data that the customer service needs to share.
pub struct ClientState {
    pub remote_addr: InetAddr,
    pub local_addr: InetAddr,
    pub conn_info: String,
    pub state: StateCtl,
    pub error: Mutex<Option<IOError>>,
}

impl ClientState {
    pub fn new(remote_addr: InetAddr, local_addr: InetAddr) -> Self {
        let conn_info = format!("[{}] -> [{}]", local_addr, remote_addr);
        Self {
            remote_addr,
            local_addr,
            conn_info,
            state: StateCtl::new(InnerState::Normal.into()),
            error: Mutex::new(None),
        }
    }

    pub fn conn_info(&self) -> &str {
        &self.conn_info
    }

    pub fn is_closed(&self) -> bool {
        self.state.state::<InnerState>() != InnerState::Normal
    }

    pub fn set_closed(&self) {
        self.state.set_state(InnerState::Closed)
    }

    pub fn has_error(&self) -> bool {
        self.state.state::<InnerState>() == InnerState::Error
    }

    pub fn set_error(&self, error: IOError) {
        if self.has_error() {
            return;
        }

        let mut e = self.error.lock().unwrap();
        self.state.set_state(InnerState::Error);
        let _ = (*e).replace(error);
    }

    pub fn take_error(&self) -> Option<IOError> {
        if !self.has_error() {
            return None;
        }
        let mut e = self.error.lock().unwrap();
        e.take()
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            remote_addr: Default::default(),
            local_addr: Default::default(),
            conn_info: "".to_string(),
            state: StateCtl::new(0),
            error: Mutex::new(None),
        }
    }
}
