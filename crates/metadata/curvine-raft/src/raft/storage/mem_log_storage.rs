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
use crate::raft::storage::LogStorage;
use crate::raft::{LibRaftResult, RaftResult};
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::storage::MemStorage;
use raft::{GetEntriesContext, RaftState, Storage};
use std::sync::{Arc, Mutex};

/// Save raft logs using memory.
#[derive(Clone)]
pub struct MemLogStorage {
    core: MemStorage,
    snapshot: Arc<Mutex<Snapshot>>,
}

impl Default for MemLogStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemLogStorage {
    pub fn new() -> Self {
        Self {
            core: MemStorage::default(),
            snapshot: Arc::new(Mutex::new(Snapshot::default())),
        }
    }
}

impl LogStorage for MemLogStorage {
    fn append(&self, entries: &[Entry]) -> RaftResult<()> {
        let mut store = self.core.wl();
        store.append(entries)?;
        Ok(())
    }

    fn scan_entries(&self, _low: u64, _high: u64) -> RaftResult<Vec<Entry>> {
        panic!()
    }

    fn set_hard_state(&self, hard_state: &HardState) -> RaftResult<()> {
        let mut store = self.core.wl();
        store.set_hardstate(hard_state.clone());
        Ok(())
    }

    fn set_hard_state_commit(&self, commit: u64) -> RaftResult<()> {
        let mut store = self.core.wl();
        store.mut_hard_state().set_commit(commit);
        Ok(())
    }

    fn set_conf_state(&self, conf_state: &ConfState) -> RaftResult<()> {
        let mut store = self.core.wl();
        store.set_conf_state(conf_state.clone());
        Ok(())
    }

    fn create_snapshot(&self, data: SnapshotData) -> RaftResult<()> {
        let mut snapshot = self.core.snapshot(data.fsm_state.applied.index, 0)?;
        snapshot.set_data(data.encode_to_vec());

        let mut sn = self.snapshot.lock().unwrap();
        *sn = snapshot;

        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> RaftResult<()> {
        let mut store = self.core.wl();
        store.apply_snapshot(snapshot)?;
        Ok(())
    }

    fn compact(&self, index: u64) -> RaftResult<()> {
        let mut store = self.core.wl();
        store.compact(index)?;
        Ok(())
    }

    fn trigger_snap_unavailable(&mut self) {
        let mut store = self.core.wl();
        store.trigger_snap_unavailable();
    }
}

impl Storage for MemLogStorage {
    fn initial_state(&self) -> LibRaftResult<RaftState> {
        let raft_state = self.core.initial_state()?;
        Ok(raft_state)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> LibRaftResult<Vec<Entry>> {
        let entries = self.core.entries(low, high, max_size, context)?;
        Ok(entries)
    }

    fn term(&self, idx: u64) -> LibRaftResult<u64> {
        self.core.term(idx)
    }

    fn first_index(&self) -> LibRaftResult<u64> {
        self.core.first_index()
    }

    fn last_index(&self) -> LibRaftResult<u64> {
        self.core.last_index()
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> LibRaftResult<Snapshot> {
        Ok(self.snapshot.lock().unwrap().clone())
    }
}
