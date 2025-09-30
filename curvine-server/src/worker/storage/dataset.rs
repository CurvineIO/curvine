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

use crate::worker::block::BlockMeta;
use curvine_common::state::ExtendedBlock;
use orpc::common::ByteUnit;
use orpc::{err_box, CommonResult};

// block data collection.
pub trait Dataset {
    // Total capacity
    fn capacity(&self) -> i64;

    fn capacity_str(&self) -> String {
        ByteUnit::byte_to_string(self.capacity() as u64)
    }

    // Total available capacity
    fn available(&self) -> i64;

    fn fs_used(&self) -> i64;

    fn num_blocks(&self) -> usize;

    fn available_str(&self) -> String {
        ByteUnit::byte_to_string(self.available() as u64)
    }

    fn get_block(&self, id: i64) -> Option<&BlockMeta>;

    // Create a new block.
    fn create_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

    // Append a block
    fn append_block(&mut self, expected_len: i64, block: &ExtendedBlock)
        -> CommonResult<BlockMeta>;

    // 🔧 Reopen a finalized block with copy-on-write for random writes
    fn reopen_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

    // Submit block
    fn finalize_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

    // Cancel a block
    fn abort_block(&mut self, block: &ExtendedBlock) -> CommonResult<()>;

    // Delete a block.
    fn remove_block(&mut self, block: &ExtendedBlock) -> CommonResult<()>;

    fn get_block_check(&self, id: i64) -> CommonResult<&BlockMeta> {
        match self.get_block(id) {
            None => err_box!("block {} not exists", id),
            Some(v) => Ok(v),
        }
    }
}
