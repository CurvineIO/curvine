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

use crate::proto::raft::RaftStateStoreProto;
use crate::raft::{RaftError, RaftResult, LOG_START_INDEX};
use crate::rocksdb::{DBConf, DBEngine, RocksUtils, WriteBatch};
use orpc::{err_box, err_ext, CommonResult};
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

    pub fn new<T: AsRef<str>>(dir: T, format: bool) -> Self {
        let conf = DBConf::new(dir)
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
            trigger_snap_unavailable: false,
            trigger_log_unavailable: false,
            get_entries_context: None,
        };

        // Readfirst_index，last_index
        if let Some((first, last)) = core.get_index_range().unwrap() {
            core.first_index = Some(first);
            core.last_index = Some(last);
        }

        core
    }

    pub fn init_state(&mut self) -> RaftResult<RaftState> {
        Ok(self.raft_state.clone())
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
        self.raft_state.hard_state = hs;
        Ok(())
    }

    pub fn set_hard_state_commit(&mut self, commit: u64) -> RaftResult<()> {
        self.mut_hard_state().set_commit(commit);
        Ok(())
    }

    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    pub fn create_proto_state(&self) -> RaftStateStoreProto {
        RaftStateStoreProto {
            hard_state: self.raft_state.hard_state.clone(),
            conf_state: self.raft_state.conf_state.clone(),
            snapshot: self.snapshot_metadata.clone(),
        }
    }

    pub fn mut_hard_state(&mut self) -> &mut HardState {
        &mut self.raft_state.hard_state
    }

    pub fn set_conf_state(&mut self, cs: ConfState) -> RaftResult<()> {
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

        // Index is 0, indicating that there is no snapshot, but there may be log entry
        if index > LOG_START_INDEX && self.first_index() > index {
            return err_ext!(RaftError::raft(raft::Error::Store(
                raft::StorageError::SnapshotOutOfDate
            )));
        }

        // Non-initialized snapshots need to be saved.
        if index > LOG_START_INDEX {
            self.db
                .put_cf(Self::CF_META, Self::SNAP_KEY, snapshot.encode_to_vec())?;
        }

        self.snapshot_metadata = meta.clone();
        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = index;
        self.raft_state.conf_state = meta.get_conf_state().clone();

        Ok(())
    }

    pub fn create_snapshot(&self, data: Vec<u8>, request_index: u64) -> RaftResult<Snapshot> {
        let mut snapshot = Snapshot::default();
        snapshot.set_data(data);
        let meta = snapshot.mut_metadata();
        meta.index = self.raft_state.hard_state.commit.min(request_index);
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            cmp::Ordering::Equal => self.snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let entry = self.get_check(meta.index).unwrap();
                entry.term
            }
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, self.snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(self.raft_state.conf_state.clone());
        self.db
            .put_cf(Self::CF_META, Self::SNAP_KEY, snapshot.encode_to_vec())?;

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

        if compact_index > self.last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }

        let mut batch = StoreWriteBatch::new(&self.db);
        if let Some(v) = self.first_index {
            batch.delete_entry(v, compact_index - 1)?;
        }
        batch.append_index_range(self.first_index(), self.last_index())?;

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
        for index in start..end {
            let key = RocksUtils::u64_to_bytes(index);
            self.0.delete_cf(RocksStorageCore::CF_ENTRIES, key)?;
        }
        Ok(())
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

    fn commit(self) -> CommonResult<()> {
        self.0.commit()
    }
}
