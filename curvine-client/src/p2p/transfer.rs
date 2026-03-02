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

use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId {
    pub file_id: i64,
    pub version_epoch: i64,
    pub block_id: i64,
    pub off: i64,
}

impl ChunkId {
    pub fn new(block_id: i64, off: i64) -> Self {
        Self::with_version(0, 0, block_id, off)
    }

    pub fn with_version(file_id: i64, version_epoch: i64, block_id: i64, off: i64) -> Self {
        Self {
            file_id,
            version_epoch,
            block_id,
            off,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransferRequest {
    pub chunk_id: ChunkId,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub struct TransferResponse {
    pub chunk_id: ChunkId,
    pub data: Bytes,
}
