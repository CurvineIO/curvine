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

use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::sync::{StateCtl, StateListener, StateMonitor};
use orpc::CommonResult;
use raft::{SoftState, StateRole};

// raft node status.
#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum RoleState {
    // The raft node has not joined the cluster yet.
    #[num_enum(default)]
    Init = 0,
    Leader = 1,
    Follower = 2,
    Exit = 3,
}

// Asynchronous Task Status Monitor.
pub struct RoleMonitor(StateMonitor);

impl RoleMonitor {
    pub fn new() -> Self {
        Self(StateMonitor::new(RoleState::Init.into()))
    }

    // Node role conversion.
    pub fn advance_role(&self, ss: &SoftState) {
        if ss.raft_state == StateRole::Leader || ss.raft_state == StateRole::Follower {
            let now = if ss.raft_state == StateRole::Leader {
                RoleState::Leader
            } else {
                RoleState::Follower
            };

            let cur: RoleState = self.0.state();
            if cur == RoleState::Init {
                self.0.advance_state(now, true);
            } else {
                self.0.advance_state(now, false);
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        self.state() == RoleState::Leader
    }

    pub fn advance_exit(&self) {
        self.0.advance_state(RoleState::Exit, true);
    }

    pub fn new_listener(&self) -> RoleStateListener {
        RoleStateListener(self.0.new_listener())
    }

    pub fn read_ctl(&self) -> StateCtl {
        self.0.read_ctl()
    }

    pub fn state(&self) -> RoleState {
        self.0.state()
    }

    pub fn is_running(&self) -> bool {
        self.state() != RoleState::Exit
    }
}

pub struct RoleStateListener(StateListener);

impl RoleStateListener {
    pub async fn wait_leader(&mut self) -> CommonResult<()> {
        self.0.wait_state(RoleState::Leader).await
    }

    pub async fn wait_follower(&mut self) -> CommonResult<()> {
        self.0.wait_state(RoleState::Follower).await
    }

    // Wait for the node to become a leader or follower.
    pub async fn wait_role(&mut self) -> CommonResult<()> {
        loop {
            let cur = RoleState::from(self.0.next_state().await?);
            if cur == RoleState::Leader || cur == RoleState::Follower {
                return Ok(());
            } else {
                continue;
            }
        }
    }
}

impl Default for RoleMonitor {
    fn default() -> Self {
        Self::new()
    }
}
