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

use curvine_common::raft::RoleState;
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::sync::StateCtl;

// master state controller
#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum MasterState {
    // Active master node, only this state can provide a metadata lake.
    Active = 1,
    // Slave node, only copy logs.
    #[num_enum(default)]
    Standby = 2,

    // The node is in safe mode.
    SafeMode = 3,

    // The node has exited.
    Exit = 4,
}

#[derive(Clone)]
pub struct MasterMonitor {
    pub(crate) journal_ctl: StateCtl,
    pub fs_ctl: StateCtl,
}

impl MasterMonitor {
    pub fn new(journal_ctl: StateCtl, fs_ctl: StateCtl) -> Self {
        Self {
            journal_ctl,
            fs_ctl,
        }
    }

    // Determine whether the current node is an active node.
    // The journal is at the active node of the leader, which is the master.
    pub fn is_active(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Leader
    }

    pub fn journal_state(&self) -> MasterState {
        let s: RoleState = self.journal_ctl.state();
        match s {
            RoleState::Leader => MasterState::Active,
            RoleState::Follower => MasterState::Standby,
            _ => MasterState::Exit,
        }
    }

    pub fn is_stop(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Exit
    }
}
