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

//! Conversion between typed filesystem errors and gRPC status details.

use crate::{ErrorKind, FsError};
use curvine_core_error::ErrorExt;
use curvine_proto::FsErrorDetail;
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use tonic::{Code, Status};

const DETAIL_TYPE: &str = "curvine.fs-error.v1";

/// Encodes the stable filesystem error kind and its existing error payload.
pub fn to_status(error: &FsError) -> Status {
    let code = match error.kind() {
        ErrorKind::NotLeaderMaster | ErrorKind::TransferStoreUnavailable => Code::Unavailable,
        ErrorKind::TransferOverloaded | ErrorKind::DiskOutOfSpace => Code::ResourceExhausted,
        ErrorKind::FileNotFound | ErrorKind::BlockNotFound | ErrorKind::JobNotFound => {
            Code::NotFound
        }
        ErrorKind::FileAlreadyExists => Code::AlreadyExists,
        ErrorKind::InvalidArgument | ErrorKind::InvalidPath => Code::InvalidArgument,
        ErrorKind::Unsupported | ErrorKind::UnsupportedUfsRead => Code::Unimplemented,
        ErrorKind::Timeout => Code::DeadlineExceeded,
        _ => Code::FailedPrecondition,
    };
    let detail = to_detail(error);
    Status::with_details(
        code,
        detail.message.clone(),
        Bytes::from(detail.encode_to_vec()),
    )
}

/// Encode a typed error as a protobuf message, including for streaming replies.
pub fn to_detail(error: &FsError) -> FsErrorDetail {
    FsErrorDetail {
        r#type: DETAIL_TYPE.to_string(),
        kind: error.kind() as i32,
        message: error.to_string(),
        legacy_error: Some(error.encode().to_vec()),
    }
}

/// Decode a recognized detail without trusting a malformed legacy payload.
pub fn from_detail(detail: &FsErrorDetail) -> Option<FsError> {
    if detail.r#type != DETAIL_TYPE {
        return None;
    }
    if let Some(bytes) = detail.legacy_error.as_deref() {
        return valid_legacy_error(bytes, detail.kind)
            .then(|| FsError::decode(BytesMut::from(bytes)));
    }
    // Other language implementations need only send kind and message.
    if !(1..=36).contains(&detail.kind) && detail.kind != ErrorKind::Common as i32 {
        return None;
    }
    let message = detail.message.as_bytes();
    let len = u32::try_from(message.len()).ok()?;
    let mut bytes = BytesMut::with_capacity(12 + message.len());
    bytes.extend_from_slice(&detail.kind.to_be_bytes());
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(message);
    bytes.extend_from_slice(&0u32.to_be_bytes());
    Some(FsError::decode(bytes))
}

/// Restores domain errors only from recognizable, well-formed details.
/// Untyped transport failures remain IO errors and never impersonate a leader
/// change. In particular an Unavailable response does not imply NotLeader.
pub fn from_status(status: Status) -> FsError {
    if let Ok(detail) = FsErrorDetail::decode(status.details()) {
        if let Some(error) = from_detail(&detail) {
            return error;
        }
    }
    let kind = match status.code() {
        Code::DeadlineExceeded => std::io::ErrorKind::TimedOut,
        Code::Cancelled => std::io::ErrorKind::Interrupted,
        _ => std::io::ErrorKind::Other,
    };
    FsError::io(std::io::Error::new(kind, status.to_string()))
}

fn valid_legacy_error(bytes: &[u8], expected_kind: i32) -> bool {
    if bytes.len() < 8 {
        return false;
    }
    let kind = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let message_len = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    kind == expected_kind && message_len <= bytes.len() - 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_error_survives_status_round_trip() {
        let status = to_status(&FsError::not_leader("new leader"));
        assert_eq!(status.code(), Code::Unavailable);
        let error = from_status(status);
        assert!(error.retry_master());
        assert!(error.to_string().contains("new leader"));
    }

    #[test]
    fn transport_unavailable_does_not_impersonate_not_leader() {
        assert!(!from_status(Status::unavailable("connection lost")).retry_master());
    }

    #[test]
    fn language_neutral_detail_restores_domain_kind() {
        let detail = FsErrorDetail {
            r#type: DETAIL_TYPE.into(),
            kind: ErrorKind::NotLeaderMaster as i32,
            message: "leader moved".into(),
            legacy_error: None,
        };
        assert!(from_detail(&detail).unwrap().retry_master());
    }

    #[test]
    fn malformed_details_do_not_panic() {
        let detail = FsErrorDetail {
            r#type: DETAIL_TYPE.into(),
            kind: ErrorKind::NotLeaderMaster as i32,
            message: String::new(),
            legacy_error: Some(vec![0, 1]),
        };
        let status = Status::with_details(Code::Unavailable, "bad", detail.encode_to_vec().into());
        assert!(!from_status(status).retry_master());
    }
}
