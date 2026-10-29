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
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::util::limit_size;
use raft::{GetEntriesContext, RaftState, Storage};
use std::io;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct RocksLogStorage {
    core: Arc<RwLock<RocksStorageCore>>,
    compact_retain_entries: u64,
}

impl RocksLogStorage {
    pub fn new(core: RocksStorageCore) -> Self {
        Self {
            core: Arc::new(RwLock::new(core)),
            compact_retain_entries: 0,
        }
    }

    pub fn from_conf(conf: &JournalConf, format: bool) -> Self {
        let core = RocksStorageCore::new(conf.db_conf(), format);
        Self {
            core: Arc::new(RwLock::new(core)),
            compact_retain_entries: (conf.retain_checkpoint_num as u64)
                .saturating_mul(conf.snapshot_entries),
        }
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

    pub fn hard_state(&self) -> HardState {
        self.core.read().unwrap().hard_state().clone()
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

    fn set_hard_state_commit(&self, commit: u64) -> RaftResult<HardState> {
        let mut store = self.write();
        store.set_hard_state_commit(commit)
    }

    fn set_conf_state(&self, conf_state: &ConfState) -> RaftResult<()> {
        let mut store = self.write();
        store.set_conf_state(conf_state.clone())
    }

    fn create_snapshot(&self, data: SnapshotData) -> RaftResult<()> {
        let store = self.read();
        let _ = store.create_snapshot(data)?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> RaftResult<()> {
        let mut store = self.write();
        store.apply_snapshot(snapshot)?;
        Ok(())
    }

    fn compact(&self, index: u64) -> RaftResult<()> {
        let index = index.saturating_sub(self.compact_retain_entries);
        let mut store = self.write();
        store.compact(index)?;
        Ok(())
    }

    fn recovery_state(&self) -> RaftResult<Option<u64>> {
        let store = self.read();
        Ok(store.recovery_state())
    }

    fn set_recovery_state(&self, state: Option<u64>) -> RaftResult<()> {
        let mut store = self.write();
        store.set_recovery_state(state)
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
            let snap = core.last_snapshot().map_err(Self::lib_error)?;

            // A snapshot must describe the state machine at the index it
            // advertises. The persisted payload still carries the state at
            // `snapshot_metadata.index`, so relabelling it to a higher index
            // would hand the receiver a (term, index) pair it trusts for commit
            // and log matching while the entries in between were never applied.
            // Report it as temporarily unavailable and let raft retry once a
            // newer snapshot exists; raft-rs tolerates only
            // SnapshotTemporarilyUnavailable here and treats anything else as
            // fatal.
            if snap.get_metadata().index < request_index {
                return Err(raft::Error::Store(
                    raft::StorageError::SnapshotTemporarilyUnavailable,
                ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_runtime::common::{FileUtils, Utils};

    fn test_store(
        name: &str,
        snapshot_entries: u64,
        retain_checkpoint_num: usize,
    ) -> RocksLogStorage {
        let conf = JournalConf {
            journal_dir: Utils::test_sub_dir(format!(
                "rocks-compact-{}-{}",
                name,
                Utils::rand_id()
            )),
            snapshot_entries,
            retain_checkpoint_num,
            ..Default::default()
        };
        FileUtils::delete_path(&conf.journal_dir, true).unwrap();
        RocksLogStorage::from_conf(&conf, true)
    }

    fn indexed_entries(low: u64, high: u64) -> Vec<Entry> {
        (low..=high)
            .map(|index| Entry {
                index,
                term: 1,
                ..Default::default()
            })
            .collect()
    }

    #[test]
    fn compact_keeps_the_retention_window() {
        // Window = retain_checkpoint_num x snapshot_entries = 3 x 10.
        let store = test_store("window", 10, 3);
        store.append(&indexed_entries(1, 100)).unwrap();

        store.compact(70).unwrap();
        assert_eq!(store.first_index().unwrap(), 40);
        assert_eq!(store.last_index().unwrap(), 100);

        // Compacting inside the window must not trim anything further.
        store.compact(70).unwrap();
        assert_eq!(store.first_index().unwrap(), 40);
    }

    #[test]
    fn compact_without_a_retention_window_trims_up_to_the_target() {
        // snapshot_entries = 0 leaves the retention window empty.
        let store = test_store("no-window", 0, 3);
        store.append(&indexed_entries(1, 100)).unwrap();

        store.compact(70).unwrap();
        assert_eq!(store.first_index().unwrap(), 70);
        assert_eq!(store.last_index().unwrap(), 100);
    }

    fn snapshot_at(index: u64, term: u64) -> Snapshot {
        let mut snap = Snapshot::default();
        snap.mut_metadata().index = index;
        snap.mut_metadata().term = term;
        snap.mut_metadata().mut_conf_state().voters = vec![1, 2, 3];
        snap
    }

    // A snapshot installed from a newer term invalidates any vote cast in the
    // old term. Keeping the stale vote could let a crash-restart appear to vote
    // twice in the higher term and break election safety.
    #[test]
    fn apply_snapshot_from_a_newer_term_clears_the_prior_vote() {
        let store = test_store("vote-clear", 10, 1);
        store.append(&indexed_entries(1, 5)).unwrap();
        store
            .set_hard_state(&HardState {
                term: 5,
                vote: 3,
                commit: 5,
            })
            .unwrap();
        assert_eq!(store.hard_state().vote, 3);

        store.apply_snapshot(snapshot_at(4, 9)).unwrap();

        let hard_state = store.hard_state();
        assert_eq!(hard_state.term, 9);
        assert_eq!(
            hard_state.vote, 0,
            "a newer-term snapshot must clear the stale vote"
        );
        assert_eq!(hard_state.commit, 5);
    }

    // The inverse guard: a snapshot at the same term must not discard a vote
    // legitimately cast in that term.
    #[test]
    fn apply_snapshot_at_the_same_term_preserves_the_vote() {
        let store = test_store("vote-keep", 10, 1);
        store.append(&indexed_entries(1, 5)).unwrap();
        store
            .set_hard_state(&HardState {
                term: 9,
                vote: 3,
                commit: 5,
            })
            .unwrap();

        store.apply_snapshot(snapshot_at(4, 9)).unwrap();

        let hard_state = store.hard_state();
        assert_eq!(hard_state.term, 9);
        assert_eq!(
            hard_state.vote, 3,
            "a same-term snapshot must preserve the vote"
        );
    }

    // A snapshot must not be relabelled to a higher index: the persisted payload
    // still carries the base index state, so advertising a newer (term, index)
    // would let a peer commit past entries it never applied.
    #[test]
    fn snapshot_past_the_persisted_index_is_temporarily_unavailable() {
        let store = test_store("relabel", 10, 1);
        let entries: Vec<Entry> = (1..=10u64)
            .map(|index| Entry {
                index,
                term: index,
                ..Default::default()
            })
            .collect();
        store.append(&entries).unwrap();
        store
            .set_hard_state(&HardState {
                term: 10,
                commit: 10,
                ..Default::default()
            })
            .unwrap();
        // Seed a base snapshot at index 3 (term 3); entries 4..=10 stay.
        store.apply_snapshot(snapshot_at(3, 3)).unwrap();

        // Requests at or below the persisted snapshot index are served as-is.
        let base = store.snapshot(3, 0).unwrap();
        assert_eq!(base.get_metadata().index, 3);
        assert_eq!(base.get_metadata().term, 3);

        let below = store.snapshot(0, 0).unwrap();
        assert_eq!(below.get_metadata().index, 3);

        // A higher request index cannot be satisfied without fabricating state.
        for request_index in [7, 20] {
            let err = store.snapshot(request_index, 0).unwrap_err();
            assert!(
                matches!(
                    err,
                    raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable)
                ),
                "request {request_index} past the persisted snapshot must be temporarily \
                 unavailable, got {err:?}"
            );
        }
    }
}
