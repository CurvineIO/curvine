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

use crate::worker::block::{BlockMeta, BlockState};
use crate::worker::storage::layout::BlockLayout;
use crate::worker::storage::{SpdkMetaStore, VfsDir};
use curvine_common::state::ExtendedBlock;
use log::{info, warn};
use orpc::CommonResult;
use std::sync::Arc;

#[derive(Clone)]
pub struct BdevLayout {
    spdk_meta: Option<Arc<SpdkMetaStore>>,
}

impl BdevLayout {
    pub fn new(spdk_meta: Option<Arc<SpdkMetaStore>>) -> Self {
        Self { spdk_meta }
    }
}

impl BlockLayout for BdevLayout {
    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let mut meta = BlockMeta::with_tmp(block, dir);
        let offset = dir
            .state
            .offset_alloc
            .allocate(block.id, block.len)
            .map_err(|e| {
                orpc::err_msg!(format!(
                    "Failed to allocate bdev offset for block {}: {}",
                    block.id, e
                ))
            })?;
        meta.bdev_offset = offset;
        Ok(meta)
    }

    fn finalize(&self, meta: &BlockMeta, committed_len: i64) -> CommonResult<BlockMeta> {
        Ok(BlockMeta::with_final_spdk(meta, committed_len))
    }

    fn scan(&self, dir: &VfsDir) -> CommonResult<Vec<BlockMeta>> {
        let Some(store) = self.spdk_meta.as_ref() else {
            warn!("SPDK dir {} has no meta store - starting empty", dir.id());
            return Ok(vec![]);
        };

        // TODO: pre-filter by dir_id in the meta store to avoid scan_all() per SPDK dir.
        let all_records = store.scan_all()?;
        let all_records_len = all_records.len();
        let mut alloc_entries = Vec::new();
        let mut blocks = Vec::new();
        for record in all_records {
            if record.dir_id != dir.id() {
                continue;
            }

            alloc_entries.push((record.block_id, record.offset, record.size));
            let state = if record.finalized {
                BlockState::Finalized
            } else {
                BlockState::Recovering
            };
            blocks.push(BlockMeta {
                id: record.block_id,
                len: record.len,
                state,
                dir: dir.state.clone(),
                actual_len: record.len,
                bdev_offset: record.offset,
            });
        }
        dir.state.offset_alloc.restore(&alloc_entries);

        info!(
            "Restored {} SPDK blocks for dir {} from RocksDB (skipped {} from other dirs)",
            blocks.len(),
            dir.id(),
            all_records_len - blocks.len(),
        );
        Ok(blocks)
    }

    fn release(&self, meta: &BlockMeta) -> CommonResult<()> {
        meta.dir.offset_alloc.free(meta.id());
        Ok(())
    }

    // SPDK blocks have no filesystem file; offset release happens in release().
    fn deallocate(&self, _meta: &BlockMeta) -> CommonResult<()> {
        Ok(())
    }
}
