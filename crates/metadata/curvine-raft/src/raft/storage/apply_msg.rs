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
use curvine_runtime::sync::channel::CallSender;
use raft::eraftpb::Entry;
use raft::StateRole;

pub enum ApplyMsg {
    Entry(Entry),
    EntryWithAck((Entry, CallSender<RaftResult<()>>)),
    Scan(AppliedIndex),
    CreateSnapshot(CallSender<RaftResult<SnapshotData>>),
    ApplySnapshot((CallSender<RaftResult<()>>, SnapshotData)),
    RoleChange((StateRole, CallSender<RaftResult<()>>)),
    Shutdown(CallSender<()>),
}

impl ApplyMsg {
    pub fn new_entry(entry: Entry) -> Self {
        ApplyMsg::Entry(entry)
    }

    pub fn new_entry_with_ack(entry: Entry, ack: CallSender<RaftResult<()>>) -> Self {
        ApplyMsg::EntryWithAck((entry, ack))
    }

    pub fn new_scan(applied_index: AppliedIndex) -> ApplyMsg {
        ApplyMsg::Scan(applied_index)
    }

    pub fn into_entry_with_ack(self) -> RaftResult<(Entry, Option<CallSender<RaftResult<()>>>)> {
        match self {
            ApplyMsg::Entry(entry) => Ok((entry, None)),
            ApplyMsg::EntryWithAck((entry, ack)) => Ok((entry, Some(ack))),
            _ => Err("expected raft entry apply message".to_string().into()),
        }
    }
}
