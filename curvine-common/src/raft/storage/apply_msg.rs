//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.


use std::ops::Deref;
use raft::eraftpb::Entry;
use orpc::err_box;
use crate::raft::RaftResult;
use crate::raft::storage::LogStorage;

/// Object-safe subset of LogStorage: only scan_entries. Used by ApplyScan so we can store
/// `Box<dyn ScanEntries>` (LogStorage is not dyn compatible due to Storage/Clone).
pub trait ScanEntries: Send + Sync + 'static {
    fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>>;
}

impl<T: LogStorage> ScanEntries for T {
    fn scan_entries(&self, low: u64, high: u64) -> RaftResult<Vec<Entry>> {
        LogStorage::scan_entries(self, low, high)
    }
}

pub struct ApplyEntry {
    pub is_leader: bool,
    pub entry: Entry,
}

impl Deref for ApplyEntry {
    type Target = Entry;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}
pub struct ApplyScan {
    pub is_leader: bool,
    pub last_applied: u64,
    pub log_store: Box<dyn ScanEntries>,
}

impl ApplyScan {
    pub fn get_entries(&self, start: u64, end: u64) -> RaftResult<Vec<Entry>> {
        if start > end {
            return err_box!("invalid scan range: start={} > end={}", start, end);
        }

        if start <= self.last_applied {
            return err_box!(
                "scan start={} must be greater than last_applied={}",
                start,
                self.last_applied
            );
        }

        self.log_store.scan_entries(start, end)
    }
}

pub enum ApplyMsg {
    Entry(ApplyEntry),
    Scan(ApplyScan),
}

impl ApplyMsg {
    pub fn new_entry(is_leader: bool, entry: Entry) -> Self {
        ApplyMsg::Entry(ApplyEntry {
            is_leader,
            entry,
        })
    }

    pub fn new_scan(is_leader: bool, last_applied: u64, log_store: impl LogStorage) -> ApplyMsg {
        ApplyMsg::Scan(ApplyScan {
            is_leader,
            last_applied,
            log_store: Box::new(log_store), // coerces to Box<dyn ScanEntries> via blanket impl
        })
    }

    pub fn is_leader(&self) -> bool {
        match self {
            ApplyMsg::Entry(entry) => entry.is_leader,
            ApplyMsg::Scan(scan) => scan.is_leader,
        }
    }

    pub fn take_entry(self) -> Entry {
        match self {
            ApplyMsg::Entry(entry) => entry.entry,
            ApplyMsg::Scan(_) => panic!("invalid entry"),
        }
    }
}