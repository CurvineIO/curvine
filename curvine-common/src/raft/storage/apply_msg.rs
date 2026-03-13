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

use crate::proto::raft::{AppliedIndex, SnapshotData};
use crate::raft::RaftResult;
use orpc::sync::channel::CallSender;
use raft::eraftpb::Entry;
use raft::StateRole;

pub enum ApplyMsg {
    Entry(Entry),
    Scan(AppliedIndex),
    Snapshot(CallSender<RaftResult<SnapshotData>>),
    RoleChange(StateRole),
}

impl ApplyMsg {
    pub fn new_entry(entry: Entry) -> Self {
        ApplyMsg::Entry(entry)
    }

    pub fn new_scan(applied_index: AppliedIndex) -> ApplyMsg {
        ApplyMsg::Scan(applied_index)
    }

    pub fn take_entry(self) -> Entry {
        match self {
            ApplyMsg::Entry(entry) => entry,
            _ => panic!("invalid entry"),
        }
    }
}
