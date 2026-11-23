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

use std::future::Future;

use raft::{eraftpb::HardState, StateRole};

use crate::proto::raft::{FsmState, SnapshotData};
use crate::raft::storage::ApplyMsg;
use crate::raft::{RaftError, RaftResult};

/// Application layer storage.
/// Replay raft log
pub trait AppStorage: Clone + Send + Sync + 'static {
    fn apply(&self, wait: bool, msg: ApplyMsg) -> impl Future<Output = RaftResult<()>> + Send;

    /// Discard locally derived state so it can be rebuilt from the journal.
    ///
    /// A node without a snapshot must not trust its application store: it can
    /// contain leader-first writes that never reached the committed log. The
    /// caller replays the journal from its first index after this returns.
    ///
    /// Stateful stores must implement this explicitly. A store that cannot
    /// discard and rebuild its data must fail closed: silently returning `Ok`
    /// lets stale, never-committed state survive a reinstall. Pure in-memory
    /// test doubles opt in with an explicit no-op implementation.
    fn reset(&self) -> impl Future<Output = RaftResult<()>> + Send {
        async {
            Err(RaftError::other(
                "application storage cannot discard its derived state; \
                 recover this member from a snapshot instead"
                    .into(),
            ))
        }
    }

    fn get_fsm_state(&self) -> FsmState;

    /// Observe durable Raft state without replaying a business operation.
    fn hard_state_changed(&self, _state: &HardState) {}

    fn role_change(&self, role: StateRole) -> impl Future<Output = RaftResult<()>> + Send;

    fn create_snapshot(&self) -> impl Future<Output = RaftResult<SnapshotData>> + Send;

    fn apply_snapshot(&self, snapshot: SnapshotData)
        -> impl Future<Output = RaftResult<()>> + Send;

    // Get the snapshot to save the directory.
    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String>;
}
