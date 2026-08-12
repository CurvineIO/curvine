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

use raft::StateRole;

use crate::proto::raft::{FsmState, SnapshotData};
use crate::raft::storage::ApplyMsg;
use crate::raft::RaftResult;

/// Application layer storage.
/// Replay raft log
pub trait AppStorage: Clone + Send + Sync + 'static {
    fn apply(&self, wait: bool, msg: ApplyMsg) -> impl Future<Output = RaftResult<()>> + Send;

    fn get_fsm_state(&self) -> FsmState;

    fn role_change(&self, role: StateRole) -> impl Future<Output = RaftResult<()>> + Send;

    fn create_snapshot(&self) -> impl Future<Output = RaftResult<SnapshotData>> + Send;

    fn apply_snapshot(&self, snapshot: SnapshotData)
        -> impl Future<Output = RaftResult<()>> + Send;

    // Get the snapshot to save the directory.
    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String>;
}
