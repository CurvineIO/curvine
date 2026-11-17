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

use self::RaftError::*;
use crate::raft::{NodeId, RaftGroup};
use curvine_core_error::CommonError;
use curvine_core_error::{ErrorDecoder, ErrorExt, ErrorImpl, StringError};
use curvine_io::IOError;
use curvine_rocksdb::Error;
use num_enum::{FromPrimitive, IntoPrimitive};
use prost::bytes::BytesMut;
use prost::DecodeError;
use std::io;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::task::JoinError;

#[repr(i32)]
#[derive(Debug, Copy, Clone, IntoPrimitive, FromPrimitive)]
enum ErrorKind {
    Raft = 1,
    NotLeader = 2,
    IO = 3,
    LeaderNotReady = 4,

    #[num_enum(default)]
    Other = 5,

    Transport = 6,
}

#[derive(Debug, Error)]
pub enum RaftError {
    // raft-rs internal error.
    #[error("{0}")]
    Raft(ErrorImpl<raft::Error>),

    // The current node is not a leader.
    #[error("{0}")]
    NotLeader(ErrorImpl<StringError, (NodeId, Box<RaftGroup>)>),

    // The current cluster does not have a leader or the leader is not equipped.
    #[error("{0}")]
    LeaderNotReady(ErrorImpl<StringError>),

    #[error("{0}")]
    IO(ErrorImpl<io::Error>),

    // Transport-level failure reported by the rpc connector, e.g. its bounded
    // retry budget was exhausted or no node was reachable. This is how the
    // connector reports "could not determine the outcome", which a propose
    // must retry; `Other` is the catch-all for server-side failures (journal
    // apply, decode, channel teardown) and stays fatal for a retrying propose.
    #[error("{0}")]
    Transport(ErrorImpl<StringError>),

    #[error("{0}")]
    Other(ErrorImpl<StringError>),
}

impl RaftError {
    pub fn raft(error: raft::Error) -> Self {
        Raft(ErrorImpl::with_source(error))
    }

    pub fn io(error: io::Error) -> Self {
        IO(ErrorImpl::with_source(error))
    }

    pub fn not_leader(leader: NodeId, group: &RaftGroup) -> Self {
        let data = (leader, Box::new(group.clone()));
        NotLeader(ErrorImpl::with_data("Not leader".into(), data))
    }

    pub fn other(error: CommonError) -> Self {
        Other(ErrorImpl::with_source(error.into()))
    }

    pub fn leader_not_ready() -> Self {
        LeaderNotReady(ErrorImpl::with_source("leader not ready".into()))
    }

    pub fn retry_leader(&self) -> bool {
        matches!(self, NotLeader(_) | LeaderNotReady(_))
    }

    /// Whether a propose that failed with this error can be retried with the
    /// same req id.
    ///
    /// Leader-availability failures (`NotLeader`, `LeaderNotReady`) and
    /// transport failures (`Transport`, which is how the connector reports an
    /// exhausted retry budget or an unreachable cluster) are transient: the
    /// server dedups proposes by req id, so retrying the same request can
    /// never commit the payload twice. `Other` is the catch-all variant
    /// (`From<CommonError>`, `From<DecodeError>`, `From<SendError>`,
    /// `From<RecvError>`) and includes journal apply failures, which can never
    /// succeed on retry and stay fatal. Raft-internal errors (`Raft`) may
    /// indicate a local storage or logic failure and also stay fatal.
    pub fn retry_propose(&self) -> bool {
        matches!(
            self,
            NotLeader(_) | LeaderNotReady(_) | IO(_) | Transport(_)
        )
    }

    pub fn with_opt_msg(error: Option<Self>, msg: impl Into<String>) -> Self {
        match error {
            None => Self::other("No errors found".into()),
            Some(e) => e.ctx(msg),
        }
    }

    pub fn is_snapshot_out_of_date(&self) -> bool {
        match self {
            Raft(e) => matches!(
                &e.source,
                raft::Error::Store(raft::StorageError::SnapshotOutOfDate)
            ),
            _ => false,
        }
    }
}

impl ErrorExt for RaftError {
    fn ctx(self, ctx: impl Into<String>) -> Self {
        match self {
            Raft(e) => Raft(e.ctx(ctx)),
            NotLeader(e) => NotLeader(e.ctx(ctx)),
            IO(e) => IO(e.ctx(ctx)),
            Other(e) => Other(e.ctx(ctx)),
            Transport(e) => Transport(e.ctx(ctx)),
            LeaderNotReady(e) => LeaderNotReady(e.ctx(ctx)),
        }
    }

    fn encode(&self) -> BytesMut {
        match self {
            Raft(e) => e.encode(ErrorKind::Raft),
            NotLeader(e) => e.encode(ErrorKind::NotLeader),
            IO(e) => e.encode(ErrorKind::IO),
            Other(e) => e.encode(ErrorKind::Other),
            Transport(e) => e.encode(ErrorKind::Transport),
            LeaderNotReady(e) => e.encode(ErrorKind::LeaderNotReady),
        }
    }

    fn decode(bytes: BytesMut) -> Self {
        let de = ErrorDecoder::new(bytes);
        let kind = de.kind();
        match kind {
            ErrorKind::Raft => {
                let io = de.into_io();
                let raft_error = raft::Error::Io(io.source);
                Raft(ErrorImpl::with_source(raft_error))
            }

            ErrorKind::NotLeader => NotLeader(de.into_string()),
            ErrorKind::IO => IO(de.into_io()),
            ErrorKind::Other => Other(de.into_string()),
            ErrorKind::Transport => Transport(de.into_string()),
            ErrorKind::LeaderNotReady => LeaderNotReady(de.into_string()),
        }
    }

    fn should_retry(&self) -> bool {
        self.retry_leader()
    }
}

impl From<CommonError> for RaftError {
    fn from(value: CommonError) -> Self {
        Other(ErrorImpl::with_source(value.into()))
    }
}

impl From<raft::Error> for RaftError {
    fn from(value: raft::Error) -> Self {
        Self::raft(value)
    }
}

impl From<DecodeError> for RaftError {
    fn from(value: DecodeError) -> Self {
        Self::other(value.into())
    }
}

impl From<String> for RaftError {
    fn from(value: String) -> Self {
        Self::other(value.into())
    }
}

impl From<Error> for RaftError {
    fn from(value: Error) -> Self {
        let io = io::Error::other(value);
        IO(ErrorImpl::with_source(io))
    }
}

impl From<io::Error> for RaftError {
    fn from(value: io::Error) -> Self {
        IO(ErrorImpl::with_source(value))
    }
}

impl From<JoinError> for RaftError {
    fn from(value: JoinError) -> Self {
        let io = io::Error::other(value);
        IO(ErrorImpl::with_source(io))
    }
}

impl From<IOError> for RaftError {
    fn from(value: IOError) -> Self {
        // The connector reports an exhausted retry budget or an unreachable
        // cluster as an IOError; a propose must retry those, so they get their
        // own retryable kind instead of the catch-all `Other`.
        Transport(ErrorImpl::with_source(format!("{}", value).into()))
    }
}

impl<T> From<SendError<T>> for RaftError {
    fn from(value: SendError<T>) -> Self {
        Other(ErrorImpl::with_source(format!("{}", value).into()))
    }
}

impl From<RecvError> for RaftError {
    fn from(value: RecvError) -> Self {
        Other(ErrorImpl::with_source(format!("{}", value).into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Only raft-internal errors and `Other` (the catch-all: journal apply
    // failures, decode failures, driver channel teardown) stay fatal for a
    // retrying propose: leader availability, transport failures (the
    // connector's exhausted budget) and decoded IO errors are transient, and
    // the server-side req id dedup makes retrying the same request safe.
    #[test]
    fn retry_propose_classifies_transient_errors() {
        assert!(RaftError::leader_not_ready().retry_propose());

        let group = RaftGroup::new("test", HashMap::new());
        assert!(RaftError::not_leader(2, &group).retry_propose());

        assert!(RaftError::io(io::Error::other("connection refused")).retry_propose());

        // The connector's exhausted retry budget arrives as an IOError and
        // must classify as retryable Transport, not the fatal catch-all.
        let transport: RaftError = IOError::create("failed to determine after retries").into();
        assert!(matches!(transport, RaftError::Transport(_)));
        assert!(transport.retry_propose());

        assert!(!RaftError::raft(raft::Error::ProposalDropped).retry_propose());
    }

    // `Other` is the catch-all variant, not a transport-only one: journal
    // apply failures arrive as `CommonError` and must stay fatal, otherwise a
    // batch that can never apply would be retried forever (issue #2191).
    #[test]
    fn retry_propose_keeps_catch_all_fatal() {
        let apply_error: CommonError = "failed to apply journal".into();
        let err: RaftError = apply_error.into();
        assert!(matches!(err, RaftError::Other(_)));
        assert!(!err.retry_propose());
    }
}
