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

use crate::state::StorageType;
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

pub struct DeleteBlockCmd {
    pub blocks: Vec<i64>,
}

pub enum WorkerCommand {
    DeleteBlock(DeleteBlockCmd),
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, IntoPrimitive, FromPrimitive)]
pub enum BlockReportStatus {
    Finalized = 1,
    #[num_enum(default)]
    Writing = 2,
    Deleted = 3,
}

#[derive(Debug)]
pub struct BlockReportInfo {
    pub id: i64,
    pub status: BlockReportStatus,
    pub storage_type: StorageType,
    pub block_size: i64,
}

impl BlockReportInfo {
    pub fn new(
        id: i64,
        status: BlockReportStatus,
        storage_type: StorageType,
        block_size: i64,
    ) -> Self {
        Self {
            id,
            status,
            storage_type,
            block_size,
        }
    }

    pub fn with_deleted(id: i64, len: i64) -> Self {
        Self::new(id, BlockReportStatus::Deleted, StorageType::Disk, len)
    }
}

pub struct BlockReportList {
    pub cluster_id: String,
    pub worker_id: u32,
    pub full_report: bool,
    pub total_len: u64,
    pub blocks: Vec<BlockReportInfo>,
}
