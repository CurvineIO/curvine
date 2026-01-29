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
        // Map well-known FsError kinds directly to POSIX errno
        let mapped = match &value {
            FsError::FileAlreadyExists(_) => Some(libc::EEXIST),
            FsError::FileNotFound(_) => Some(libc::ENOENT),
            FsError::DirNotEmpty(_) => Some(libc::ENOTEMPTY),
            FsError::ParentNotDir(_) => Some(libc::ENOTDIR),
            FsError::InvalidPath(_) => Some(libc::EINVAL),
            FsError::DiskOutOfSpace(_) => Some(libc::ENOSPC),
            FsError::Timeout(_) => Some(libc::ETIMEDOUT),
            FsError::Unsupported(_) => Some(libc::ENOSYS),
            FsError::InProgress(_) => Some(libc::EBUSY),
            FsError::UnsupportedUfsRead(_) => Some(libc::EOPNOTSUPP),
            _ => None,
        };

        if let Some(errno) = mapped {
            return Self::new(errno, value.into());
        }

        // Fallback: infer from message content for UFS/common errors (e.g., opendal PermissionDenied)
        let msg = value.to_string().to_lowercase();
        if msg.contains("permission denied") || msg.contains("os error 13") {
            return Self::new(libc::EACCES, value.into());
        }
        if msg.contains("not implemented") || msg.contains("unsupported") {
            return Self::new(libc::ENOSYS, value.into());
        }

        // Default to EIO
        Self::new(libc::EIO, value.into())
    }
}

impl From<Elapsed> for FuseError {
    fn from(value: Elapsed) -> Self {
        Self::new(libc::EIO, value.into())
    }
}

impl From<std::io::Error> for FuseError {
    fn from(value: std::io::Error) -> Self {
        Self::new(libc::EIO, value.into())
    }
}
