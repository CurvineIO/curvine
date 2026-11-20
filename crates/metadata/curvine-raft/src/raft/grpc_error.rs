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

use crate::proto::raft::RaftErrorDetail;
use crate::raft::{RaftError, RaftGroup};
use prost::Message;
use tonic::{Code, Status};

const DETAIL_TYPE: &str = "curvine.raft-error.v1";

/// Preserve leader redirection and group membership in language-neutral details.
pub fn to_status(error: &RaftError) -> Status {
    let (kind, code, leader_id, group, group_name) = match error {
        RaftError::NotLeader(inner) => match &inner.data {
            Some((leader, group)) => (
                2,
                Code::FailedPrecondition,
                Some(*leader),
                group.to_proto(),
                Some(group.name.clone()),
            ),
            None => (2, Code::FailedPrecondition, None, Vec::new(), None),
        },
        RaftError::LeaderNotReady(_) => (4, Code::Unavailable, None, Vec::new(), None),
        RaftError::IO(_) => (3, Code::Unavailable, None, Vec::new(), None),
        RaftError::Raft(_) => (1, Code::Internal, None, Vec::new(), None),
        RaftError::Other(_) => (5, Code::Internal, None, Vec::new(), None),
    };
    let detail = RaftErrorDetail {
        r#type: DETAIL_TYPE.to_owned(),
        kind,
        message: error.to_string(),
        leader_id,
        group,
        group_name,
    };
    Status::with_details(code, detail.message.clone(), detail.encode_to_vec().into())
}

/// Untyped transport errors must not be treated as leader redirects.
pub fn from_status(status: Status) -> RaftError {
    if let Ok(detail) = RaftErrorDetail::decode(status.details()) {
        if detail.r#type == DETAIL_TYPE {
            return match detail.kind {
                2 if detail.leader_id.is_some()
                    && detail.group.iter().all(|p| p.port <= u16::MAX as u32) =>
                {
                    let group =
                        RaftGroup::from_proto(detail.group_name.unwrap_or_default(), detail.group);
                    RaftError::not_leader(detail.leader_id.unwrap(), &group)
                }
                4 => RaftError::leader_not_ready(),
                1 => RaftError::raft(raft::Error::Io(std::io::Error::other(detail.message))),
                3 => RaftError::io(std::io::Error::other(detail.message)),
                _ => RaftError::other(detail.message.into()),
            };
        }
    }
    let kind = if status.code() == Code::DeadlineExceeded {
        std::io::ErrorKind::TimedOut
    } else {
        std::io::ErrorKind::Other
    };
    RaftError::io(std::io::Error::new(kind, status.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftPeer;

    #[test]
    fn preserves_leader_redirect_and_group() {
        let group = RaftGroup::new(
            "group",
            [(7, RaftPeer::new(7, "node", 8996))].into_iter().collect(),
        );
        let error = from_status(to_status(&RaftError::not_leader(7, &group)));
        match error {
            RaftError::NotLeader(inner) => {
                let (leader, group) = inner.data.unwrap();
                assert_eq!(leader, 7);
                assert_eq!(group.get_addr(&7).unwrap().port, 8996);
            }
            other => panic!("expected leader redirect, got {other}"),
        }
    }

    #[test]
    fn plain_transport_unavailable_is_not_redirect() {
        assert!(!from_status(Status::unavailable("disconnected")).retry_leader());
    }
}
