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

use curvine_core_error::err_ext;
use curvine_error::{FsError, FsResult};
use curvine_raft::raft::RoleState;
use curvine_raft::raft::{FollowerReadinessMonitor, FollowerReadinessSnapshot};
use curvine_runtime::sync::{AtomicCounter, StateCtl};
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
    follower_readiness: FollowerReadinessMonitor,
    /// Raft term of the last persisted hard state, published by the journal
    /// loader. Client-session lease
    /// epochs are keyed on it: a promotion always carries a higher term than the
    /// previous leadership, while a standby can still observe it (#1883 review).
    term: Arc<AtomicU64>,
    applied_term: Arc<AtomicCounter>,
}

impl MasterMonitor {
    pub fn new(journal_ctl: StateCtl, fs_ctl: StateCtl) -> Self {
        Self::with_term_and_follower_readiness(
            journal_ctl,
            fs_ctl,
            Arc::new(AtomicU64::new(0)),
            FollowerReadinessMonitor::default(),
        )
    }

    pub fn with_follower_readiness(
        journal_ctl: StateCtl,
        fs_ctl: StateCtl,
        follower_readiness: FollowerReadinessMonitor,
    ) -> Self {
        Self::with_term_and_follower_readiness(
            journal_ctl,
            fs_ctl,
            Arc::new(AtomicU64::new(0)),
            follower_readiness,
        )
    }

    pub fn with_term(journal_ctl: StateCtl, fs_ctl: StateCtl, term: Arc<AtomicU64>) -> Self {
        Self::with_term_and_follower_readiness(
            journal_ctl,
            fs_ctl,
            term,
            FollowerReadinessMonitor::default(),
        )
    }

    pub fn with_term_and_follower_readiness(
        journal_ctl: StateCtl,
        fs_ctl: StateCtl,
        term: Arc<AtomicU64>,
        follower_readiness: FollowerReadinessMonitor,
    ) -> Self {
        Self {
            journal_ctl,
            fs_ctl,
            term,
            applied_term: Arc::new(AtomicCounter::new(0)),
            follower_readiness,
        }
    }

    pub fn with_applied_term(mut self, applied_term: Arc<AtomicCounter>) -> Self {
        self.applied_term = applied_term;
        self
    }

    /// Returns current Raft contact and actual journal replay observations.
    pub fn follower_readiness_snapshot(&self) -> FollowerReadinessSnapshot {
        self.follower_readiness.snapshot()
    }

    /// Raft term of the last persisted hard state; 0 before the first one.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }

    pub fn applied_term(&self) -> u64 {
        self.applied_term.get()
    }

    #[doc(hidden)]
    pub fn set_term_for_test(&self, term: u64) {
        self.term.store(term, Ordering::Release);
    }

    #[doc(hidden)]
    pub fn set_applied_term_for_test(&self, term: u64) {
        self.applied_term.set(term);
    }

    // Determine whether the current node is an active node.
    // The journal is at the active node of the leader, which is the master.
    pub fn is_active(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Leader
    }

    /// True while this node serves writes: it holds leadership and the state
    /// machine has applied an entry of the current term, i.e. the replay of the
    /// inherited journal tail is done (#2297).
    ///
    /// The raft term-start no-op is the first entry of a new term, and apply is
    /// index ordered, so an applied entry of the current term is the witness that
    /// the inherited, previously uncommitted log tail reached the namespace. Until
    /// then the in-memory tree is missing whatever that tail created, and a
    /// leader-first write would mutate the namespace against a stale view.
    pub fn is_serving(&self) -> bool {
        self.is_active() && self.applied_term() == self.term()
    }

    /// Rejects a namespace mutation unless this node is serving writes (#2297).
    ///
    /// Callers re-check it under their write lock: a request admitted while this
    /// node was a serving leader can block on that lock across a leadership
    /// change and must not mutate a tree this node no longer publishes.
    pub fn check_serving(&self) -> FsResult<()> {
        if self.is_serving() {
            return Ok(());
        }
        err_ext!(FsError::not_leader(format!(
            "master is not serving writes: term={}, applied_term={} (not the leader, or still replaying the inherited journal tail)",
            self.term(),
            self.applied_term()
        )))
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
