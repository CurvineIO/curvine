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
    /// A committed propose whose sender is parked on the acknowledgement.
    /// `proposer_epoch` is `Some` only when this process's handler already
    /// mutated the FSM for the entry, and it records the metadata-restore
    /// epoch at that moment. The apply worker skips the replay only while the
    /// epoch is still current: a snapshot restore in between discards the
    /// mutation, so the entry must be replayed then.
    EntryWithAck((Entry, CallSender<RaftResult<()>>, Option<u64>)),
    Scan(AppliedIndex),
    /// Discard the local derived state before a full journal replay.
    Reset(CallSender<RaftResult<()>>),
    CreateSnapshot(CallSender<RaftResult<SnapshotData>>),
    ApplySnapshot((CallSender<RaftResult<()>>, SnapshotData)),
    RoleChange((StateRole, CallSender<RaftResult<()>>)),
    Shutdown(CallSender<()>),
}

impl ApplyMsg {
    pub fn new_entry(entry: Entry) -> Self {
        ApplyMsg::Entry(entry)
    }

    pub fn new_entry_with_ack(
        entry: Entry,
        ack: CallSender<RaftResult<()>>,
        proposer_epoch: Option<u64>,
    ) -> Self {
        ApplyMsg::EntryWithAck((entry, ack, proposer_epoch))
    }

    pub fn new_scan(applied_index: AppliedIndex) -> ApplyMsg {
        ApplyMsg::Scan(applied_index)
    }

    pub fn into_entry_with_ack(self) -> RaftResult<(Entry, Option<CallSender<RaftResult<()>>>)> {
        match self {
            ApplyMsg::Entry(entry) => Ok((entry, None)),
            ApplyMsg::EntryWithAck((entry, ack, _)) => Ok((entry, Some(ack))),
            _ => Err("expected raft entry apply message".to_string().into()),
        }
    }
}
