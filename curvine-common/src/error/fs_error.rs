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

use crate::fs::RpcCode;
use crate::raft::RaftError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use num_enum::{FromPrimitive, IntoPrimitive};
use orpc::error::{ErrorDecoder, ErrorExt, ErrorImpl, StringError};
use orpc::io::IOError;
use orpc::CommonError;
use prost::bytes::BytesMut;
use prost::{DecodeError, EncodeError};
use serde_json::json;
use std::io;
use std::num::ParseIntError;
use std::sync::mpsc::{RecvError, SendError};
use thiserror::Error;
use tokio::task::JoinError;
use tokio::time::error::Elapsed;

// Uniformly defined file system error codes.
#[repr(i32)]
#[derive(Debug, Copy, Clone, IntoPrimitive, FromPrimitive)]
pub enum ErrorKind {
    IO = 1,
    NotLeaderMaster = 2,
    Raft = 3,
    Timeout = 4,
    PBDecode = 5,
    PBEncode = 6,
    FileAlreadyExists = 7,
    FileNotFound = 8,
    InvalidFileSize = 9,
    ParentNotDir = 10,
    DirNotEmpty = 11,
    AbnormalData = 12,
    BlockIsWriting = 13,
    BlockInfo = 14,
    Lease = 15,
    InvalidPath = 16,
    DiskOutOfSpace = 17,
    InProgress = 18,
    Unsupported = 19,
    Ufs = 20,
    Expired = 21,
    UnsupportedUfsRead = 22,

    #[num_enum(default)]
    Common = 10000,
}

#[derive(Debug, Error)]
pub enum FsError {
    // io error.
    #[error("{0}")]
    IO(ErrorImpl<io::Error>),

    // Not a master node.
    #[error("{0}")]
    NotLeaderMaster(ErrorImpl<StringError>),

    // raft internal error
    #[error("{0}")]
    Raft(ErrorImpl<StringError>),

    // tokio task timeout
    #[error("{0}")]
    Timeout(ErrorImpl<Elapsed>),

    // Protobuf decoding error
    #[error("{0}")]
    PBDecode(ErrorImpl<DecodeError>),

    // Protobuf encoding error.
    #[error("{0}")]
    PBEncode(ErrorImpl<EncodeError>),

    // File system related errors
    // The file already exists
    #[error("{0}")]
    FileAlreadyExists(ErrorImpl<StringError>),

    // The file does not exist
    #[error("{0}")]
    FileNotFound(ErrorImpl<StringError>),

    // The submitted file length is abnormal.
    #[error("{0}")]
    InvalidFileSize(ErrorImpl<StringError>),

    // The upper file is not a directory.
    #[error("{0}")]
    ParentNotDir(ErrorImpl<StringError>),

    // The directory is not empty
    #[error("{0}")]
    DirNotEmpty(ErrorImpl<StringError>),

    // The data state is abnormal.
    #[error("{0}")]
    AbnormalData(ErrorImpl<StringError>),

    // block is already in write state
    #[error("{0}")]
    BlockIsWriting(ErrorImpl<StringError>),

    // Block exception
    #[error("{0}")]
    BlockInfo(ErrorImpl<StringError>),

    // The lease written to the client service is incorrect.
    #[error("{0}")]
    Lease(ErrorImpl<StringError>),

    // The path is incorrect.
    #[error("{0}")]
    InvalidPath(ErrorImpl<StringError>),

    // Insufficient disk space.
    #[error("{0}")]
    DiskOutOfSpace(ErrorImpl<StringError>),

    #[error("{0}")]
    InProgress(ErrorImpl<StringError>),

    #[error("{0}")]
    Unsupported(ErrorImpl<StringError>),

    #[error("{0}")]
    Ufs(ErrorImpl<StringError>),

    // Data expiration
    #[error("{0}")]
    Expired(ErrorImpl<StringError>),

    // Unsupported UFS read operation
    #[error("{0}")]
    UnsupportedUfsRead(ErrorImpl<StringError>),

    // Other errors that are not defined.
    #[error("{0}")]
    Common(ErrorImpl<StringError>),
}

impl FsError {
    pub fn common<T: AsRef<str>>(error: T) -> Self {
        let str = error.as_ref();
        Self::Common(ErrorImpl::with_source(str.into()))
    }

    pub fn not_leader_master(code: RpcCode, client_ip: &str) -> Self {
        let error = format!(
            "Not a leader master, code={:?}, client_ip={}",
            code, client_ip
        );
        Self::NotLeaderMaster(ErrorImpl::with_source(error.into()))
    }

    pub fn not_leader(msg: impl Into<String>) -> Self {
        Self::NotLeaderMaster(ErrorImpl::with_source(msg.into().into()))
    }

    pub fn in_progress(req_id: i64) -> Self {
        let msg = format!("Request {} in progress", req_id);
        Self::InProgress(ErrorImpl::with_source(msg.into()))
    }

    pub fn file_not_found(path: impl AsRef<str>) -> Self {
        let msg = format!("File {} not found", path.as_ref());
        Self::FileNotFound(ErrorImpl::with_source(msg.into()))
    }

    pub fn file_expired(path: impl AsRef<str>) -> Self {
        let msg = format!("File {} expired", path.as_ref());
        Self::Expired(ErrorImpl::with_source(msg.into()))
    }

    pub fn unsupported_ufs_read(path: impl AsRef<str>) -> Self {
        let msg = format!("File {} unsupported ufs read", path.as_ref());
        Self::UnsupportedUfsRead(ErrorImpl::with_source(msg.into()))
    }

    pub fn file_exists(path: impl AsRef<str>) -> Self {
        let msg = format!("{}  already exists", path.as_ref());
        Self::FileAlreadyExists(ErrorImpl::with_source(msg.into()))
    }

    pub fn io(error: io::Error) -> Self {
        Self::IO(ErrorImpl::with_source(error))
    }

    pub fn invalid_path(path: impl AsRef<str>, ext_msg: impl AsRef<str>) -> Self {
        let msg = format!("Path {} is invalid, {}", path.as_ref(), ext_msg.as_ref());
        Self::InvalidPath(ErrorImpl::with_source(msg.into()))
    }

    pub fn unsupported<T: Into<String>>(feature: T) -> Self {
        let msg = format!("{} is not implemented", feature.into());
        Self::Unsupported(ErrorImpl::with_source(msg.into()))
    }

    // Determine whether the current error allows retry.
    // NotLeaderMaster error indicates that a master switch has occurred and you need to retry access to the next master
    pub fn retry_master(&self) -> bool {
        matches!(self, FsError::NotLeaderMaster(_))
    }

    pub fn with_opt_msg(error: Option<Self>, msg: impl Into<String>) -> Self {
        match error {
            None => Self::common("No errors found"),
            Some(e) => e.ctx(msg),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        match self {
            FsError::IO(_) => ErrorKind::IO,
            FsError::NotLeaderMaster(_) => ErrorKind::NotLeaderMaster,
            FsError::Raft(_) => ErrorKind::Raft,
            FsError::Timeout(_) => ErrorKind::Timeout,
            FsError::PBDecode(_) => ErrorKind::PBDecode,
            FsError::PBEncode(_) => ErrorKind::PBEncode,
            FsError::FileAlreadyExists(_) => ErrorKind::FileAlreadyExists,
            FsError::FileNotFound(_) => ErrorKind::FileNotFound,
            FsError::InvalidFileSize(_) => ErrorKind::InvalidFileSize,
            FsError::ParentNotDir(_) => ErrorKind::ParentNotDir,
            FsError::DirNotEmpty(_) => ErrorKind::DirNotEmpty,
            FsError::AbnormalData(_) => ErrorKind::AbnormalData,
            FsError::BlockIsWriting(_) => ErrorKind::BlockIsWriting,
            FsError::BlockInfo(_) => ErrorKind::BlockInfo,
            FsError::Lease(_) => ErrorKind::Lease,
            FsError::InvalidPath(_) => ErrorKind::InvalidPath,
            FsError::DiskOutOfSpace(_) => ErrorKind::DiskOutOfSpace,
            FsError::InProgress(_) => ErrorKind::InProgress,
            FsError::Unsupported(_) => ErrorKind::Unsupported,
            FsError::Ufs(_) => ErrorKind::Ufs,
            FsError::Expired(_) => ErrorKind::Expired,
            FsError::UnsupportedUfsRead(_) => ErrorKind::UnsupportedUfsRead,
            FsError::Common(_) => ErrorKind::Common,
        }
    }

    pub fn libc_kind(&self) -> i64 {
        -(self.kind() as i64)
    }
}

impl From<String> for FsError {
    fn from(value: String) -> Self {
        FsError::Common(ErrorImpl::with_source(value.into()))
    }
}

impl From<CommonError> for FsError {
    fn from(value: CommonError) -> Self {
        FsError::Common(ErrorImpl::with_source(value.into()))
    }
}

impl From<RaftError> for FsError {
    fn from(value: RaftError) -> Self {
        Self::Raft(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl From<std::io::Error> for FsError {
    fn from(value: io::Error) -> Self {
        Self::IO(ErrorImpl::with_source(value))
    }
}

impl<T> From<SendError<T>> for FsError {
    fn from(value: SendError<T>) -> Self {
        Self::Common(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl From<RecvError> for FsError {
    fn from(value: RecvError) -> Self {
        Self::Common(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl From<JoinError> for FsError {
    fn from(value: JoinError) -> Self {
        Self::Common(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl From<IOError> for FsError {
    fn from(value: IOError) -> Self {
        Self::IO(ErrorImpl::with_source(value.into_raw()))
    }
}

impl From<ParseIntError> for FsError {
    fn from(value: ParseIntError) -> Self {
        Self::Common(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl IntoResponse for FsError {
    fn into_response(self) -> Response {
        let body = Json(json!({"message": self.to_string()}));
        (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for FsError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::Common(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for FsError {
    fn from(value: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::Common(ErrorImpl::with_source(value.to_string().into()))
    }
}

impl ErrorExt for FsError {
    fn ctx(self, ctx: impl Into<String>) -> Self {
        match self {
            FsError::IO(e) => FsError::IO(e.ctx(ctx)),
            FsError::NotLeaderMaster(e) => FsError::NotLeaderMaster(e.ctx(ctx)),
            FsError::Raft(e) => FsError::Raft(e.ctx(ctx)),
            FsError::Timeout(e) => FsError::Timeout(e.ctx(ctx)),
            FsError::PBDecode(e) => FsError::PBDecode(e.ctx(ctx)),
            FsError::PBEncode(e) => FsError::PBEncode(e.ctx(ctx)),
            FsError::FileAlreadyExists(e) => FsError::FileAlreadyExists(e.ctx(ctx)),
            FsError::FileNotFound(e) => FsError::FileNotFound(e.ctx(ctx)),
            FsError::InvalidFileSize(e) => FsError::InvalidFileSize(e.ctx(ctx)),
            FsError::ParentNotDir(e) => FsError::ParentNotDir(e.ctx(ctx)),
            FsError::DirNotEmpty(e) => FsError::DirNotEmpty(e.ctx(ctx)),
            FsError::AbnormalData(e) => FsError::AbnormalData(e.ctx(ctx)),
            FsError::BlockIsWriting(e) => FsError::BlockIsWriting(e.ctx(ctx)),
            FsError::BlockInfo(e) => FsError::BlockInfo(e.ctx(ctx)),
            FsError::Lease(e) => FsError::Lease(e.ctx(ctx)),
            FsError::InvalidPath(e) => FsError::InvalidPath(e.ctx(ctx)),
            FsError::DiskOutOfSpace(e) => FsError::DiskOutOfSpace(e.ctx(ctx)),
            FsError::InProgress(e) => FsError::InProgress(e.ctx(ctx)),
            FsError::Unsupported(e) => FsError::Unsupported(e.ctx(ctx)),
            FsError::Ufs(e) => FsError::Ufs(e.ctx(ctx)),
            FsError::Expired(e) => FsError::Expired(e.ctx(ctx)),
            FsError::UnsupportedUfsRead(e) => FsError::UnsupportedUfsRead(e.ctx(ctx)),
            FsError::Common(e) => FsError::Common(e.ctx(ctx)),
        }
    }

    fn encode(&self) -> BytesMut {
        match self {
            FsError::IO(e) => e.encode(ErrorKind::IO),
            FsError::NotLeaderMaster(e) => e.encode(ErrorKind::NotLeaderMaster),
            FsError::Raft(e) => e.encode(ErrorKind::Raft),
            FsError::Timeout(e) => e.encode(ErrorKind::Timeout),
            FsError::PBDecode(e) => e.encode(ErrorKind::PBDecode),
            FsError::PBEncode(e) => e.encode(ErrorKind::PBEncode),
            FsError::FileAlreadyExists(e) => e.encode(ErrorKind::FileAlreadyExists),
            FsError::FileNotFound(e) => e.encode(ErrorKind::FileNotFound),
            FsError::InvalidFileSize(e) => e.encode(ErrorKind::InvalidFileSize),
            FsError::ParentNotDir(e) => e.encode(ErrorKind::ParentNotDir),
            FsError::DirNotEmpty(e) => e.encode(ErrorKind::DirNotEmpty),
            FsError::AbnormalData(e) => e.encode(ErrorKind::AbnormalData),
            FsError::BlockIsWriting(e) => e.encode(ErrorKind::BlockIsWriting),
            FsError::BlockInfo(e) => e.encode(ErrorKind::BlockInfo),
            FsError::Lease(e) => e.encode(ErrorKind::Lease),
            FsError::InvalidPath(e) => e.encode(ErrorKind::InvalidPath),
            FsError::DiskOutOfSpace(e) => e.encode(ErrorKind::DiskOutOfSpace),
            FsError::InProgress(e) => e.encode(ErrorKind::InProgress),
            FsError::Unsupported(e) => e.encode(ErrorKind::Unsupported),
            FsError::Ufs(e) => e.encode(ErrorKind::Ufs),
            FsError::Expired(e) => e.encode(ErrorKind::Expired),
            FsError::UnsupportedUfsRead(e) => e.encode(ErrorKind::UnsupportedUfsRead),
            FsError::Common(e) => e.encode(ErrorKind::Common),
        }
    }

    // @todo Consider using macro instead in the future.There are a lot of repetitions of code.
    fn decode(byte: BytesMut) -> Self {
        let de = ErrorDecoder::new(byte);

        match de.kind() {
            ErrorKind::IO => FsError::IO(de.into_io()),
            ErrorKind::NotLeaderMaster => FsError::NotLeaderMaster(de.into_string()),
            ErrorKind::Raft => FsError::Raft(de.into_string()),
            ErrorKind::Timeout => FsError::IO(de.into_io()),
            ErrorKind::PBDecode => FsError::IO(de.into_io()),
            ErrorKind::PBEncode => FsError::IO(de.into_io()),
            ErrorKind::FileAlreadyExists => FsError::FileAlreadyExists(de.into_string()),
            ErrorKind::FileNotFound => FsError::FileNotFound(de.into_string()),
            ErrorKind::InvalidFileSize => FsError::InvalidFileSize(de.into_string()),
            ErrorKind::ParentNotDir => FsError::ParentNotDir(de.into_string()),
            ErrorKind::DirNotEmpty => FsError::DirNotEmpty(de.into_string()),
            ErrorKind::AbnormalData => FsError::AbnormalData(de.into_string()),
            ErrorKind::BlockIsWriting => FsError::BlockIsWriting(de.into_string()),
            ErrorKind::BlockInfo => FsError::BlockInfo(de.into_string()),
            ErrorKind::Lease => FsError::Lease(de.into_string()),
            ErrorKind::InvalidPath => FsError::InvalidPath(de.into_string()),
            ErrorKind::DiskOutOfSpace => FsError::DiskOutOfSpace(de.into_string()),
            ErrorKind::InProgress => FsError::InProgress(de.into_string()),
            ErrorKind::Unsupported => FsError::Unsupported(de.into_string()),
            ErrorKind::Ufs => FsError::Ufs(de.into_string()),
            ErrorKind::Expired => FsError::Expired(de.into_string()),
            ErrorKind::UnsupportedUfsRead => FsError::UnsupportedUfsRead(de.into_string()),
            ErrorKind::Common => FsError::Common(de.into_string()),
        }
    }

    fn should_retry(&self) -> bool {
        self.retry_master()
    }
}

#[cfg(test)]
mod tests {
    use crate::error::fs_error::FsError;
    use orpc::error::{ErrorExt, ErrorImpl};

    #[test]
    pub fn error_test() {
        let error: FsError = FsError::DiskOutOfSpace(ErrorImpl::with_source("disk full".into()));
        println!("error {:?}", error);

        let bytes = error.encode();
        let error: FsError = FsError::decode(bytes);
        let check = matches!(error, FsError::DiskOutOfSpace(_));

        assert!(check)
    }
}
