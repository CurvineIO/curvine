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

use curvine_common::proto::BlockReadResponse;
use curvine_common::state::StorageType;

pub struct CreateBlockContext {
    pub id: i64,
    pub off: i64,
    pub block_size: i64,
    pub path: Option<String>,
    pub storage_type: StorageType,
}

pub struct BlockReadContext {
    pub id: i64,
    pub len: i64,
    pub path: Option<String>,
    pub storage_type: StorageType,
}

impl BlockReadContext {
    pub fn from_req(req: BlockReadResponse) -> Self {
        Self {
            id: req.id,
            len: req.len,
            path: req.path,
            storage_type: StorageType::from(req.storage_type),
        }
    }
}
