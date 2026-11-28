// Copyright 2026 OPPO.
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

//! The Raft runtime uses prost 0.11, while gRPC uses prost 0.14. Keep the
//! runtime's types in storage and Raft itself, and convert at the RPC edge.
//! These conversions move payload buffers rather than serializing them again.

use crate::proto::{eraftpb as wire, raft as rpc};
use raft::eraftpb as runtime;

// Destructuring every field intentionally makes a future schema drift a
// compile error instead of silently dropping data during a conversion.
macro_rules! mirror {
    ($name:ident { $($field:ident: $kind:ident),* $(,)? }) => {
        impl From<runtime::$name> for wire::$name {
            fn from(value: runtime::$name) -> Self {
                let runtime::$name { $($field),* } = value;
                Self { $($field: mirror!(@$kind $field)),* }
            }
        }

        impl From<wire::$name> for runtime::$name {
            fn from(value: wire::$name) -> Self {
                let wire::$name { $($field),* } = value;
                Self { $($field: mirror!(@$kind $field)),* }
            }
        }
    };
    (@plain $value:ident) => { $value };
    (@option $value:ident) => { $value.map(Into::into) };
    (@vec $value:ident) => { $value.into_iter().map(Into::into).collect() };
}

mirror!(Entry {
    entry_type: plain,
    term: plain,
    index: plain,
    data: plain,
    context: plain,
    sync_log: plain,
});
mirror!(SnapshotMetadata {
    conf_state: option,
    index: plain,
    term: plain
});
mirror!(Snapshot {
    data: plain,
    metadata: option
});
mirror!(HardState {
    term: plain,
    vote: plain,
    commit: plain
});
mirror!(ConfState {
    voters: plain,
    learners: plain,
    voters_outgoing: plain,
    learners_next: plain,
    auto_leave: plain,
});
mirror!(ConfChange {
    change_type: plain,
    node_id: plain,
    context: plain,
    id: plain
});
mirror!(ConfChangeSingle {
    change_type: plain,
    node_id: plain
});
mirror!(ConfChangeV2 {
    transition: plain,
    changes: vec,
    context: plain
});

// Tag 14 was already published as `priority` in Curvine's descriptor. The
// runtime calls that field `deprecated_priority` and uses signed tag 16 for
// its current priority; keep both names and both values at their original tags.
impl From<runtime::Message> for wire::Message {
    fn from(value: runtime::Message) -> Self {
        let runtime::Message {
            msg_type,
            to,
            from,
            term,
            log_term,
            index,
            entries,
            commit,
            commit_term,
            snapshot,
            request_snapshot,
            reject,
            reject_hint,
            context,
            deprecated_priority,
            priority,
        } = value;
        Self {
            msg_type,
            to,
            from,
            term,
            log_term,
            index,
            entries: entries.into_iter().map(Into::into).collect(),
            commit,
            commit_term,
            snapshot: snapshot.map(Into::into),
            request_snapshot,
            reject,
            reject_hint,
            context,
            priority: deprecated_priority,
            signed_priority: priority,
        }
    }
}

impl From<wire::Message> for runtime::Message {
    fn from(value: wire::Message) -> Self {
        let wire::Message {
            msg_type,
            to,
            from,
            term,
            log_term,
            index,
            entries,
            commit,
            commit_term,
            snapshot,
            request_snapshot,
            reject,
            reject_hint,
            context,
            priority,
            signed_priority,
        } = value;
        Self {
            msg_type,
            to,
            from,
            term,
            log_term,
            index,
            entries: entries.into_iter().map(Into::into).collect(),
            commit,
            commit_term,
            snapshot: snapshot.map(Into::into),
            request_snapshot,
            reject,
            reject_hint,
            context,
            deprecated_priority: priority,
            priority: signed_priority,
        }
    }
}

#[derive(Clone)]
pub(super) struct RuntimeRaftRequest {
    pub message: runtime::Message,
    pub sender_session: Option<u64>,
    pub receiver_session: Option<u64>,
    pub leader_commit: Option<u64>,
}

impl From<rpc::RaftRequest> for RuntimeRaftRequest {
    fn from(request: rpc::RaftRequest) -> Self {
        Self {
            message: request.message.into(),
            sender_session: request.sender_session,
            receiver_session: request.receiver_session,
            leader_commit: request.leader_commit,
        }
    }
}

impl From<RuntimeRaftRequest> for rpc::RaftRequest {
    fn from(request: RuntimeRaftRequest) -> Self {
        Self {
            message: request.message.into(),
            sender_session: request.sender_session,
            receiver_session: request.receiver_session,
            leader_commit: request.leader_commit,
        }
    }
}

#[cfg(test)]
mod tests;
