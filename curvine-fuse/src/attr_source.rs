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

use crate::fs::dcache::CachedStatus;
use crate::*;
use curvine_model::{FileStatus, FileType};

/// The handful of fields `fuse_attr` is built from.
///
/// Both the backend `FileStatus` and the dcache's compact `CachedStatus` feed
/// the same layout code, so the two representations cannot drift apart.
pub(crate) trait AttrSource {
    fn ino(&self) -> u64;
    fn name(&self) -> &str;
    fn file_type(&self) -> FileType;
    fn size(&self) -> FuseResult<u64>;
    fn mtime(&self) -> i64;
    fn atime(&self) -> i64;
    fn ctime(&self) -> i64;
    fn mode(&self) -> u32;
    fn owner(&self) -> &str;
    fn group(&self) -> &str;
    fn nlink(&self) -> u32;
    fn xattr(&self, name: &str) -> Option<&[u8]>;
}

impl AttrSource for FileStatus {
    fn ino(&self) -> u64 {
        self.id as u64
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn file_type(&self) -> FileType {
        self.file_type
    }

    fn size(&self) -> FuseResult<u64> {
        FuseUtils::fuse_st_size(self)
    }

    fn mtime(&self) -> i64 {
        self.mtime
    }

    fn atime(&self) -> i64 {
        self.atime
    }

    fn ctime(&self) -> i64 {
        FileStatus::ctime(self)
    }

    fn mode(&self) -> u32 {
        self.mode
    }

    fn owner(&self) -> &str {
        &self.owner
    }

    fn group(&self) -> &str {
        &self.group
    }

    fn nlink(&self) -> u32 {
        self.nlink
    }

    fn xattr(&self, name: &str) -> Option<&[u8]> {
        self.x_attr.get(name).map(Vec::as_slice)
    }
}

/// `CachedStatus` stores neither the ino (it lives on `Inode`) nor the name (it
/// lives in the parent's dentry map), so both are borrowed alongside it.
pub(crate) struct CachedStatusView<'a> {
    pub(crate) status: &'a CachedStatus,
    pub(crate) ino: u64,
    pub(crate) name: &'a str,
}

impl AttrSource for CachedStatusView<'_> {
    fn ino(&self) -> u64 {
        self.ino
    }

    fn name(&self) -> &str {
        self.name
    }

    fn file_type(&self) -> FileType {
        self.status.file_type
    }

    fn size(&self) -> FuseResult<u64> {
        FuseUtils::st_size(
            self.status.file_type,
            self.status.target.as_deref(),
            self.status.len,
            self.ino,
        )
    }

    fn mtime(&self) -> i64 {
        self.status.mtime
    }

    fn atime(&self) -> i64 {
        self.status.atime
    }

    fn ctime(&self) -> i64 {
        self.status.ctime()
    }

    fn mode(&self) -> u32 {
        self.status.mode
    }

    fn owner(&self) -> &str {
        &self.status.owner
    }

    fn group(&self) -> &str {
        &self.status.group
    }

    fn nlink(&self) -> u32 {
        self.status.nlink
    }

    fn xattr(&self, name: &str) -> Option<&[u8]> {
        self.status.xattr(name)
    }
}
