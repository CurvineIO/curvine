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
use crate::raft::RaftResult;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{Error, Storage};

// raft log storage interface
// The Storage interface of raft-rs has certain limitations and is not universal enough.
pub trait LogStorage: Storage + Clone + Send + Sync + 'static {
    ///Write raft log
    fn append(&self, entries: &[Entry]) -> RaftResult<()>;

    // Directly set the current raft log for testing.
    // This method requires clearing historical data.
    fn set_entries(&self, entries: &[Entry]) -> RaftResult<()> {
        self.append(entries)
    }

    fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>>;

    /// Set the hard state of raft node, which contains the following information:
    /// 1. current_term, raft record the last recorded term
    /// 2. voted_for, which node is voting for the current term.
    fn set_hard_state(&self, hard_state: &HardState) -> RaftResult<()>;

    /// Set the submission index of HardState
    /// When a node receives a committed log index from another node, it updates its own Commit field to ensure that the state of the node is consistent with that of the other nodes
    /// Usually, this is called when processing committed log entries to ensure that the node does not skip uncommitted parts when applying the log
    fn set_hard_state_commit(&self, commit: u64) -> RaftResult<()>;

    /// Set the configuration status of the node
    /// When nodes in the cluster change (such as adding or deleting nodes), the configuration status needs to be updated.
    fn set_conf_state(&self, conf_state: &ConfState) -> RaftResult<()>;

    /// Create a new snapshot.
    fn create_snapshot(&self, data: SnapshotData) -> RaftResult<()>;

    /// The state machine used to apply snapshots to nodes.
    /// When the node needs to restore state from the snapshot, it calls this method.
    fn apply_snapshot(&self, snapshot: Snapshot) -> RaftResult<()>;

    /// Clean up the submitted log entries to reduce storage usage and accelerate state machine replay
    /// When a snapshot is applied by the node, it can use the compact operation to delete old log items that are already included in the snapshot
    fn compact(&self, index: u64) -> RaftResult<()>;

    fn trigger_snap_unavailable(&mut self) {
        //empty
    }

    fn latest_snapshot(&self) -> RaftResult<Option<Snapshot>> {
        match self.snapshot(0, 0) {
            Ok(v) => Ok(Some(v)),
            Err(e) => match e {
                Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable) => Ok(None),
                _ => Err(e.into()),
            },
        }
    }

    // Determine whether there is a snapshot in the current system.
    fn has_snapshot(&self) -> bool {
        match self.snapshot(0, 0) {
            Ok(v) => v != Snapshot::default(),
            Err(_) => false,
        }
    }
}
