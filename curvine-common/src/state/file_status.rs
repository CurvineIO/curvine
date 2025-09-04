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
        self.storage_policy.ttl_action == TtlAction::Delete
            && LocalTime::mills() as i64 > self.atime + self.storage_policy.ttl_ms
    }
}
