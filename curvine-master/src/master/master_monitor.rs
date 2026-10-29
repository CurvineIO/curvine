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

use curvine_raft::raft::RoleState;
use curvine_runtime::sync::StateCtl;
use num_enum::{FromPrimitive, IntoPrimitive};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    /// Raft term of the last persisted hard state, published by the journal
    /// loader. Client-session lease
    /// epochs are keyed on it: a promotion always carries a higher term than the
    /// previous leadership, while a standby can still observe it (#1883 review).
    term: Arc<AtomicU64>,
}

impl MasterMonitor {
    pub fn new(journal_ctl: StateCtl, fs_ctl: StateCtl) -> Self {
        Self::with_term(journal_ctl, fs_ctl, Arc::new(AtomicU64::new(0)))
    }

    /// Build a monitor that shares its term handle with the journal loader.
    pub fn with_term(journal_ctl: StateCtl, fs_ctl: StateCtl, term: Arc<AtomicU64>) -> Self {
        Self {
            journal_ctl,
            fs_ctl,
            term,
        }
    }

    /// Raft term of the last persisted hard state; 0 before the first one.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }

    /// Test-only term bump, mirroring [`MasterMonitor::set_role_for_test`].
    #[doc(hidden)]
    pub fn set_term_for_test(&self, term: u64) {
        self.term.store(term, Ordering::Release);
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

    /// Test-only role switch, mirroring `JournalSystem::fs_only_for_test`.
    #[doc(hidden)]
    pub fn set_role_for_test(&self, state: curvine_raft::raft::RoleState) {
        self.journal_ctl.set_state(state);
    }

    pub fn is_stop(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Exit
    }
}
