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

use crate::proto::raft::SnapshotData;
use crate::raft::{FsmStateMap, RaftResult};
use std::future::Future;
use crate::raft::storage::ApplyMsg;


/// Application layer storage.
/// Replay raft log
pub trait AppStorage: Clone + Send + Sync + 'static {
    fn apply(&self, wait_for_apply: bool, msg: ApplyMsg)
        -> impl Future<Output = RaftResult<()>> + Send;

    fn get_applied(&self) -> u64;

    fn create_snapshot(&self, node_id: u64, last_applied: u64, fsm_state_map: FsmStateMap) -> RaftResult<SnapshotData>;

    fn apply_snapshot(&self, snapshot: &SnapshotData) -> RaftResult<()>;

    // Get the snapshot to save the directory.
    fn snapshot_dir(&self, snapshot_id: u64) -> RaftResult<String>;
}
