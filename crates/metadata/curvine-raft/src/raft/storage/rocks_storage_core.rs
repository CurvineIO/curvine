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
use crate::raft::{RaftError, RaftResult, LOG_START_INDEX};
use crate::rocksdb::{DBConf, DBEngine, RocksUtils, WriteBatch};
use curvine_core_error::{err_box, err_ext, CommonResult, ErrorExt};
use log::warn;
use prost::Message;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::{GetEntriesContext, RaftState};
use std::{cmp, mem};

pub struct RocksStorageCore {
    pub(crate) raft_state: RaftState,
    pub(crate) snapshot_metadata: SnapshotMetadata,
    db: DBEngine,
    first_index: Option<u64>,
    last_index: Option<u64>,
    init_error: Option<String>,

    pub(crate) trigger_snap_unavailable: bool,
    pub(crate) trigger_log_unavailable: bool,
    pub(crate) get_entries_context: Option<GetEntriesContext>,
}

impl RocksStorageCore {
    pub const CF_ENTRIES: &'static str = "default";
    pub const CF_META: &'static str = "meta";
    // Save the current snapshot
    pub const SNAP_KEY: &'static [u8] = &[0x01u8];
    // Save index range。
    pub const INDEX_KEY: &'static [u8] = &[0x02u8];
    pub const STATE_KEY: &'static [u8] = &[0x03u8];
    pub const CONF_STATE_KEY: &'static [u8] = &[0x04u8];

    pub fn new(conf: DBConf, format: bool) -> Self {
        let conf = conf
            .add_cf(Self::CF_ENTRIES)
            .add_cf(Self::CF_META)
            .set_disable_wal(false);

        let db = DBEngine::new(conf, format).unwrap();
        let mut core = Self {
            raft_state: Default::default(),
            snapshot_metadata: Default::default(),
            db,
            first_index: None,
            last_index: None,
            init_error: None,
            trigger_snap_unavailable: false,
            trigger_log_unavailable: false,
            get_entries_context: None,
        };

        if let Err(error) = core.load_persisted_state() {
            core.init_error = Some(error.to_string());
        }

        core
    }

    pub fn init_state(&mut self) -> RaftResult<RaftState> {
        if let Some(error) = &self.init_error {
            return err_box!(
                "invalid local raft storage: {}. Restore a consistent master meta+journal pair from a healthy voter; do not restart this voter from an empty, partial, or corrupted local directory.",
                error
            );
        }
        self.validate_hard_state_commit(self.raft_state.hard_state.commit)?;
        Ok(self.raft_state.clone())
    }

    fn load_persisted_state(&mut self) -> RaftResult<()> {
        if let Some((first, last)) = self
            .get_index_range()
            .map_err(|error| error.ctx("failed to decode raft index range"))?
        {
            self.first_index = Some(first);
            self.last_index = Some(last);
        }

        if let Some(data) = self
            .db
            .get_cf(Self::CF_META, Self::STATE_KEY)
            .map_err(|error| RaftError::from(error).ctx("failed to read raft hard state"))?
        {
            self.raft_state.hard_state = HardState::decode(data.as_ref())
                .map_err(|error| RaftError::from(error).ctx("failed to decode raft hard state"))?;
        }

        if let Some(data) = self
            .db
            .get_cf(Self::CF_META, Self::SNAP_KEY)
            .map_err(|error| RaftError::from(error).ctx("failed to read raft snapshot"))?
        {
            let snapshot = Snapshot::decode(data.as_ref())
                .map_err(|error| RaftError::from(error).ctx("failed to decode raft snapshot"))?;
            self.snapshot_metadata = snapshot.get_metadata().clone();
            self.raft_state.conf_state = snapshot.get_metadata().get_conf_state().clone();
        }

        if let Some(data) = self
            .db
            .get_cf(Self::CF_META, Self::CONF_STATE_KEY)
            .map_err(|error| RaftError::from(error).ctx("failed to read raft conf state"))?
        {
            self.raft_state.conf_state = ConfState::decode(data.as_ref())
                .map_err(|error| RaftError::from(error).ctx("failed to decode raft conf state"))?;
        }

        Ok(())
    }

    // Get the entry of the specified index
    pub fn get(&self, index: u64) -> RaftResult<Option<Entry>> {
        let value = self
            .db
            .get_cf(Self::CF_ENTRIES, RocksUtils::u64_to_bytes(index))?;

        match value {
            None => Ok(None),
            Some(v) => {
                let entry = Entry::decode(&v[..])?;
                Ok(Some(entry))
            }
        }
    }

    pub fn get_check(&self, index: u64) -> RaftResult<Entry> {
        match self.get(index)? {
            None => err_box!("entry {} not exists", index),
            Some(v) => Ok(v),
        }
    }

    pub fn has_entry_at(&self, index: u64) -> bool {
        index >= self.first_index() && index <= self.last_index()
    }

    pub fn set_hard_state(&mut self, hs: HardState) -> RaftResult<()> {
        self.validate_hard_state_commit(hs.commit)?;

        let mut batch = StoreWriteBatch::new(&self.db);
        batch.set_state(&hs)?;
        batch.commit()?;

        self.raft_state.hard_state = hs;
        Ok(())
    }

    pub fn set_hard_state_commit(&mut self, commit: u64) -> RaftResult<()> {
        self.validate_hard_state_commit(commit)?;

        let mut hard_state = self.raft_state.hard_state.clone();
        hard_state.set_commit(commit);

        let mut batch = StoreWriteBatch::new(&self.db);
        batch.set_state(&hard_state)?;
        batch.commit()?;

        self.raft_state.hard_state = hard_state;
        Ok(())
    }

    fn validate_hard_state_commit(&self, commit: u64) -> RaftResult<()> {
        let snapshot_index = self.snapshot_metadata.index;
        let first_index = self.first_index();
        let lower_bound = first_index.saturating_sub(1);
        let last_index = self.last_index();
        if commit < lower_bound || commit > last_index {
            return err_box!(
                "invalid local raft storage: hard_state.commit={} is outside durable range [{}, {}], first_index={}, snapshot_index={}. Restore a consistent master meta+journal pair from a healthy voter; do not restart this voter from an empty or partial local directory.",
                commit,
                lower_bound,
                last_index,
                first_index,
                snapshot_index
            );
        }
        Ok(())
    }

    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    pub fn mut_hard_state(&mut self) -> &mut HardState {
        &mut self.raft_state.hard_state
    }

    pub fn set_conf_state(&mut self, cs: ConfState) -> RaftResult<()> {
        let mut batch = StoreWriteBatch::new(&self.db);
        batch.set_conf_state(&cs)?;
        batch.commit()?;

        self.raft_state.conf_state = cs;
        Ok(())
    }

    pub fn first_index(&self) -> u64 {
        match self.first_index {
            Some(index) => index,
            None => self.snapshot_metadata.index + 1,
        }
    }

    pub fn last_index(&self) -> u64 {
        match self.last_index {
            Some(index) => index,
            None => self.snapshot_metadata.index,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> RaftResult<()> {
        let meta = snapshot.get_metadata();
        let index = meta.index;

        // Index is 0, indicating that there is no snapshot, but there may be log entry.
        if index > LOG_START_INDEX && self.first_index() > index {
            warn!(
                "snapshot out of date: snapshot_index={}, first_index={}, skip apply",
                index,
                self.first_index()
            );
            return err_ext!(RaftError::raft(raft::Error::Store(
                raft::StorageError::SnapshotOutOfDate
            )));
        }

        let mut hard_state = self.raft_state.hard_state.clone();
        hard_state.term = cmp::max(hard_state.term, meta.term);
        hard_state.commit = cmp::max(hard_state.commit, index);

        let mut batch = StoreWriteBatch::new(&self.db);
        let mut new_range = None;

        // Non-initialized snapshots need to be saved.
        if index > LOG_START_INDEX {
            let next_index = index.saturating_add(1);
            let old_first = self.first_index();
            let old_last = self.last_index();
            let new_last = cmp::max(old_last, index);

            batch.delete_entry(old_first, next_index)?;
            batch.append_index_range(next_index, new_last)?;
            batch.append_snapshot(&snapshot)?;
            batch.set_state(&hard_state)?;
            new_range = Some((next_index, new_last));
        }
        batch.set_conf_state(meta.get_conf_state())?;
        batch.commit()?;

        if let Some((first_index, last_index)) = new_range {
            self.first_index = Some(first_index);
            self.last_index = Some(last_index);
        }
        self.snapshot_metadata = meta.clone();
        self.raft_state.hard_state = hard_state;
        self.raft_state.conf_state = meta.get_conf_state().clone();

        Ok(())
    }

    pub fn create_snapshot(&self, data: SnapshotData) -> RaftResult<Snapshot> {
        let request_index = data.fsm_state.applied.index;

        let mut snapshot = Snapshot::default();
        snapshot.set_data(data.encode_to_vec());
        let meta = snapshot.mut_metadata();

        if request_index > self.raft_state.hard_state.commit {
            return err_box!(
                "snapshot temporarily unavailable: request_index {}, hard_state {:?}",
                request_index,
                self.raft_state.hard_state
            );
        }

        meta.index = request_index;
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            cmp::Ordering::Equal => self.snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let entry = self.get_check(meta.index)?;
                entry.term
            }
            cmp::Ordering::Less => {
                return err_box!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index,
                    self.snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(self.raft_state.conf_state.clone());

        let mut batch = StoreWriteBatch::new(&self.db);
        batch.append_index_range(self.first_index(), self.last_index())?;
        batch.append_snapshot(&snapshot)?;
        batch.commit()?;

        Ok(snapshot)
    }

    // Get the latest snapshot.
    pub fn last_snapshot(&self) -> RaftResult<Snapshot> {
        let kv = self.db.get_cf(Self::CF_META, Self::SNAP_KEY)?;
        match kv {
            None => {
                let err = raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable);
                Err(RaftError::raft(err))
            }

            Some(v) => {
                let mut snapshot = Snapshot::decode(&v[..])?;

                // Solve the problem that the newly added node is not in conf_state, resulting in the snapshot not being referenced correctly.
                if let Some(old_meta) = &snapshot.metadata {
                    let meta = SnapshotMetadata {
                        conf_state: Some(self.raft_state.conf_state.clone()),
                        index: old_meta.index,
                        term: old_meta.term,
                    };
                    snapshot.metadata = Some(meta);
                }

                Ok(snapshot)
            }
        }
    }

    pub fn compact(&mut self, compact_index: u64) -> RaftResult<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() {
            return err_box!(
                "compact index {} exceeds last index {}, cannot compact past last log entry",
                compact_index,
                self.last_index()
            );
        }

        let start = self.first_index();

        let mut batch = StoreWriteBatch::new(&self.db);
        // delete_entry(start, end) deletes [start, end); remove all index < compact_index
        batch.delete_entry(start, compact_index)?;
        batch.append_index_range(compact_index, self.last_index())?;
        batch.commit()?;

        let _ = self.first_index.replace(compact_index);

        Ok(())
    }

    pub fn set_entries(&mut self, entries: &[Entry]) -> RaftResult<()> {
        let mut batch = StoreWriteBatch::new(&self.db);
        // Delete historical data.
        batch.delete_entry(self.first_index(), self.last_index())?;
        // Append new data.
        batch.append_entry(entries)?;
        batch.commit()?;

        let _ = mem::replace(&mut self.first_index, entries.first().map(|x| x.index));
        let _ = mem::replace(&mut self.last_index, entries.last().map(|x| x.index));

        Ok(())
    }

    pub fn append(&mut self, entries: &[Entry]) -> RaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if self.first_index() > entries[0].index {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.first_index() - 1,
                entries[0].index,
            );
        }
        if self.last_index() + 1 < entries[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                entries[0].index,
            );
        }

        let mut batch = StoreWriteBatch::new(&self.db);
        batch.append_entry(entries)?;

        if self.first_index.is_none() {
            let _ = mem::replace(&mut self.first_index, entries.first().map(|x| x.index));
        }
        let _ = mem::replace(&mut self.last_index, entries.last().map(|x| x.index));

        batch.append_index_range(self.first_index(), self.last_index())?;
        batch.commit()?;

        Ok(())
    }

    pub fn trigger_snap_unavailable(&mut self) {
        self.trigger_snap_unavailable = true;
    }

    pub fn trigger_log_unavailable(&mut self, v: bool) {
        self.trigger_log_unavailable = v;
    }

    pub fn take_get_entries_context(&mut self) -> Option<GetEntriesContext> {
        self.get_entries_context.take()
    }

    pub fn get_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>> {
        if low < self.first_index() {
            return err_ext!(RaftError::raft(raft::Error::Store(
                raft::StorageError::Compacted
            )));
        }

        if high > self.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                self.last_index() + 1,
                high
            );
        }
        self.scan_entries(low, high)
    }

    pub fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>> {
        let iter = self.db.range_scan(
            Self::CF_ENTRIES,
            RocksUtils::u64_to_bytes(low),
            RocksUtils::u64_to_bytes(high),
        )?;

        let mut vec = Vec::with_capacity((high - low) as usize);
        for item in iter {
            let kv = item?;
            let entry: Entry = Entry::decode(&kv.1[..])?;
            vec.push(entry);
        }

        Ok(vec)
    }

    fn get_index_range(&self) -> RaftResult<Option<(u64, u64)>> {
        if let Some(value) = self.db.get_cf(Self::CF_META, Self::INDEX_KEY)? {
            let range = RocksUtils::u64_u64_from_bytes(&value)?;
            Ok(Some(range))
        } else {
            Ok(None)
        }
    }
}

struct StoreWriteBatch<'a>(WriteBatch<'a>);

impl<'a> StoreWriteBatch<'a> {
    fn new(db: &'a DBEngine) -> Self {
        Self(WriteBatch::new(db))
    }

    fn delete_entry(&mut self, start: u64, end: u64) -> CommonResult<()> {
        if start >= end {
            return Ok(());
        }
        self.0.delete_range_cf(
            RocksStorageCore::CF_ENTRIES,
            RocksUtils::u64_to_bytes(start),
            RocksUtils::u64_to_bytes(end),
        )
    }

    fn append_entry(&mut self, entries: &[Entry]) -> CommonResult<()> {
        for entry in entries {
            let key = RocksUtils::u64_to_bytes(entry.index);
            let value = entry.encode_to_vec();

            self.0.put_cf(RocksStorageCore::CF_ENTRIES, key, value)?;
        }
        Ok(())
    }

    fn append_index_range(&mut self, start: u64, end: u64) -> CommonResult<()> {
        let bytes = RocksUtils::u64_u64_to_bytes(start, end);
        self.0.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::INDEX_KEY,
            bytes,
        )?;
        Ok(())
    }

    fn append_snapshot(&mut self, snapshot: &Snapshot) -> CommonResult<()> {
        self.0.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::SNAP_KEY,
            snapshot.encode_to_vec(),
        )?;
        Ok(())
    }

    fn set_conf_state(&mut self, state: &ConfState) -> CommonResult<()> {
        self.0.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::CONF_STATE_KEY,
            state.encode_to_vec(),
        )?;
        Ok(())
    }

    fn set_state(&mut self, state: &HardState) -> CommonResult<()> {
        self.0.put_cf(
            RocksStorageCore::CF_META,
            RocksStorageCore::STATE_KEY,
            state.encode_to_vec(),
        )?;
        Ok(())
    }

    fn commit(self) -> CommonResult<()> {
        self.0.commit_and_flush_wal(true)
    }
}
