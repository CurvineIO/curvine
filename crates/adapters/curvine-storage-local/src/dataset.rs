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

use crate::BlockMeta;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{ExtendedBlock, StorageType};
use curvine_runtime::common::ByteUnit;

/// Snapshot of a single data directory's free-space ratio, for the
/// `disk_free_ratio` worker metric.
#[derive(Debug, Clone)]
pub struct DirFreeRatio {
    pub dir_id: u32,
    pub dir_path: String,
    pub storage_type: StorageType,
    /// `available_space / total_space` (FS) or `(capacity - used) / capacity`
    /// (SPDK), in `[0.0, 1.0]`. Pre-reserved, pre-guard.
    pub free_ratio: f64,
}

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

    fn num_blocks_to_delete(&self) -> usize;
    fn increment_blocks_to_delete(&self);
    fn decrement_blocks_to_delete(&self);

    fn available_str(&self) -> String {
        ByteUnit::byte_to_string(self.available() as u64)
    }

    /// Per-directory raw free-space ratios for the `disk_free_ratio` metric.
    fn dir_free_ratios(&self) -> Vec<DirFreeRatio>;

    fn get_block(&self, id: i64) -> Option<&BlockMeta>;

    // Create a new block.
    fn open_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

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
