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

use curvine_common::error::FsError;
use orpc::io::IOError;
use orpc::CommonError;
use std::fmt;
use std::fmt::Debug;
use tokio::time::error::Elapsed;

#[derive(Debug)]
pub struct FuseError {
    pub(crate) errno: i32,
    pub(crate) error: CommonError,
}

impl FuseError {
    pub fn new(errno: i32, error: CommonError) -> Self {
        Self { errno, error }
    }
}

impl From<String> for FuseError {
    fn from(value: String) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl std::error::Error for FuseError {}

impl fmt::Display for FuseError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "errno {}: {}", self.error, self.errno)
    }
}

impl From<&str> for FuseError {
    fn from(value: &str) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl From<CommonError> for FuseError {
    fn from(value: CommonError) -> Self {
        Self::new(libc::EIO, value)
    }
}

impl From<IOError> for FuseError {
    fn from(value: IOError) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl From<FsError> for FuseError {
    fn from(value: FsError) -> Self {
        match &value {
            FsError::IO(_) => Self::new(libc::EIO, value.into()),
            FsError::NotLeaderMaster(_) => Self::new(libc::EAGAIN, value.into()),
            FsError::Raft(_) => Self::new(libc::EAGAIN, value.into()),
            FsError::Timeout(_) => Self::new(libc::ETIMEDOUT, value.into()),
            FsError::PBDecode(_) | FsError::PBEncode(_) => Self::new(libc::EPROTO, value.into()),
            FsError::FileAlreadyExists(_) => Self::new(libc::EEXIST, value.into()),
            FsError::FileNotFound(_) => Self::new(libc::ENOENT, value.into()),
            FsError::InvalidFileSize(_) | FsError::InvalidPath(_) => {
                Self::new(libc::EINVAL, value.into())
            }
            FsError::ParentNotDir(_) => Self::new(libc::ENOTDIR, value.into()),
            FsError::DirNotEmpty(_) => Self::new(libc::ENOTEMPTY, value.into()),
            FsError::AbnormalData(_) | FsError::BlockInfo(_) | FsError::Ufs(_) => {
                Self::new(libc::EIO, value.into())
            }
            FsError::BlockIsWriting(_) => Self::new(libc::EBUSY, value.into()),
            FsError::Lease(_) | FsError::InProgress(_) => Self::new(libc::EAGAIN, value.into()),
            FsError::DiskOutOfSpace(_) => Self::new(libc::ENOSPC, value.into()),
            FsError::Unsupported(_) | FsError::UnsupportedUfsRead(_) => {
                Self::new(libc::EOPNOTSUPP, value.into())
            }
            FsError::Expired(_) => Self::new(libc::ESTALE, value.into()),
            FsError::Common(_) => Self::new(libc::EIO, value.into()),
        }
    }
}

impl From<Elapsed> for FuseError {
    fn from(value: Elapsed) -> Self {
        Self::new(libc::EIO, value.into())
    }
}
