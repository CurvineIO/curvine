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

use crate::state::{FileType, StoragePolicy, TtlAction};
use crate::UFS_INODE_ID;
use orpc::common::LocalTime;
use orpc::ternary;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileStatus {
    pub id: i64,
    pub path: String,
    pub name: String,
    pub is_dir: bool,
    pub mtime: i64,
    pub atime: i64,
    pub children_num: i32,
    pub is_complete: bool,
    pub len: i64,
    pub replicas: i32,
    pub block_size: i64,
    pub file_type: FileType,
    pub x_attr: HashMap<String, Vec<u8>>,
    pub storage_policy: StoragePolicy,

    // ACL permission control
    pub mode: u32,
    pub owner: String,
    pub group: String,

    // Number of hard links to this file
    pub nlink: u32,

    pub target: Option<String>,
}

impl FileStatus {
    pub fn with_name(id: i64, name: String, is_dir: bool) -> Self {
        FileStatus {
            id,
            name,
            is_dir,
            file_type: ternary!(is_dir, FileType::Dir, FileType::File),
            ..Default::default()
        }
    }

    // Determine whether the file is readable.
    pub fn readable(&self) -> bool {
        if self.is_dir {
            false
        } else {
            // Streaming file or file has been completed
            self.is_complete || self.file_type == FileType::Stream
        }
    }

    // Whether it is a streaming file.
    pub fn is_stream(&self) -> bool {
        self.file_type == FileType::Stream
    }

    pub fn is_expired(&self) -> bool {
        let sp = &self.storage_policy;
        if sp.ttl_action != TtlAction::None && sp.ttl_ms > 0 {
            LocalTime::mills() as i64 > self.mtime.saturating_add(sp.ttl_ms)
        } else {
            false
        }
    }

    // Check if this file has hard links (nlink > 1 and is a regular file)
    pub fn exists_links(&self) -> bool {
        !self.is_dir && self.nlink > 1 && self.id > 0
    }

    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    pub fn ufs_exists(&self) -> bool {
        self.storage_policy.ufs_exists()
    }

    pub fn cv_exists(&self) -> bool {
        self.storage_policy.cv_exists()
    }

    /// Returns true if CV data is valid and usable: CV exists, not expired, UFS exists;
    /// when `ufs_status` is provided, also checks len and mtime match UFS.
    pub fn cv_valid(&self, ufs_status: Option<&FileStatus>) -> bool {
        if !self.cv_exists() {
            return false;
        }
        if self.is_expired() || !self.ufs_exists() {
            return false;
        }
        if let Some(ufs_status) = ufs_status {
            self.len == ufs_status.len && self.storage_policy.ufs_mtime == ufs_status.mtime
        } else {
            true
        }
    }

    pub fn ufs_valid(&self, ufs_status: &FileStatus) -> bool {
        if self.ufs_exists() {
            self.len == ufs_status.len && self.storage_policy.ufs_mtime == ufs_status.mtime
        } else {
            false
        }
    }

    /// After cache validity: set visible `mtime` from `storage_policy.ufs_mtime` and clear `id`
    /// for the UFS-backed read path.
    pub fn apply_ufs_fields(&mut self) {
        self.mtime = self.storage_policy.ufs_mtime;
        self.id = UFS_INODE_ID;
    }
}
