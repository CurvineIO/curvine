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

use crate::conf::JournalConf;
use crate::proto::raft::SnapshotData;
use crate::raft::storage::{LogStorage, RocksStorageCore};
use crate::raft::{LibRaftResult, RaftError, RaftResult};
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::util::limit_size;
use raft::{GetEntriesContext, RaftState, Storage};
use std::io;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct RocksLogStorage {
    core: Arc<RwLock<RocksStorageCore>>,
}

impl RocksLogStorage {
    pub fn new(core: RocksStorageCore) -> Self {
        Self {
            core: Arc::new(RwLock::new(core)),
        }
    }

    pub fn from_conf(conf: &JournalConf, format: bool) -> Self {
        let core = RocksStorageCore::new(&conf.journal_dir, format);
        Self::new(core)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, RocksStorageCore> {
        self.core.read().unwrap()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, RocksStorageCore> {
        self.core.write().unwrap()
    }

    pub fn lib_error(error: RaftError) -> raft::Error {
        match error {
            RaftError::Raft(e) => e.source,
            e => raft::Error::Io(io::Error::other(e)),
        }
    }

    pub fn clone_store(&self) -> Arc<RwLock<RocksStorageCore>> {
        self.core.clone()
    }
}

impl LogStorage for RocksLogStorage {
    fn append(&self, entries: &[Entry]) -> RaftResult<()> {
        let mut store = self.write();
        store.append(entries)
    }

    fn set_entries(&self, entries: &[Entry]) -> RaftResult<()> {
        let mut store = self.write();
        store.set_entries(entries)
    }

    fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>> {
        let store = self.read();
        store.scan_entries(low, high)
    }

    fn set_hard_state(&self, hard_state: &HardState) -> RaftResult<()> {
        let mut store = self.write();
        store.set_hard_state(hard_state.clone())
    }

    fn set_hard_state_commit(&self, commit: u64) -> RaftResult<()> {
        let mut store = self.write();
        store.set_hard_state_commit(commit)
    }

    fn set_conf_state(&self, conf_state: &ConfState) -> RaftResult<()> {
        let mut store = self.write();
        store.set_conf_state(conf_state.clone())
    }

    fn create_snapshot(&self, data: SnapshotData, request_index: u64) -> RaftResult<()> {
        let store = self.read();
        let _ = store.create_snapshot(data.encode_to_vec(), request_index)?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> RaftResult<()> {
        let mut store = self.write();
        store.apply_snapshot(snapshot)?;
        Ok(())
    }

    fn compact(&self, index: u64) -> RaftResult<()> {
        let mut store = self.write();
        store.compact(index)?;
        Ok(())
    }

    fn trigger_snap_unavailable(&mut self) {
        self.write().trigger_snap_unavailable();
    }
}

impl Storage for RocksLogStorage {
    fn initial_state(&self) -> LibRaftResult<RaftState> {
        let mut store = self.write();
        store.init_state().map_err(Self::lib_error)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> LibRaftResult<Vec<Entry>> {
        let mut core = self.write();
        if core.trigger_log_unavailable && context.can_async() {
            core.get_entries_context = Some(context);
            return Err(raft::Error::Store(
                raft::StorageError::LogTemporarilyUnavailable,
            ));
        }

        let max_size = max_size.into();
        let mut entries = core.get_entries(low, high).map_err(Self::lib_error)?;

        // @todo I don't understand the meaning of calling this method.
        limit_size(&mut entries, max_size);
        Ok(entries)
    }

    fn term(&self, idx: u64) -> LibRaftResult<u64> {
        let core = self.read();
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }

        let offset = core.first_index();
        if idx < offset {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if idx > core.last_index() {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        // @todo This interface will query rocksdb every time, and then consider optimization.
        let entry = core.get_check(idx).map_err(Self::lib_error)?;
        Ok(entry.term)
    }

    fn first_index(&self) -> LibRaftResult<u64> {
        Ok(self.read().first_index())
    }

    fn last_index(&self) -> LibRaftResult<u64> {
        Ok(self.read().last_index())
    }

    fn snapshot(&self, request_index: u64, _: u64) -> LibRaftResult<Snapshot> {
        let mut core = self.write();
        if core.trigger_snap_unavailable {
            core.trigger_snap_unavailable = false;
            Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ))
        } else {
            let mut snap = core.last_snapshot().map_err(Self::lib_error)?;

            if snap.get_metadata().index < request_index {
                snap.mut_metadata().index = request_index;
            }

            Ok(snap)
        }
    }
}

/*impl Drop for RocksLogStorage {
    fn drop(&mut self) {
        let store = self.write();
        let _ = try_log!(store.store_meta());
    }
}*/
