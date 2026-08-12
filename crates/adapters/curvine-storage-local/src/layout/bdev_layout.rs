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

use crate::layout::{validate_open_offset, BlockLayout};
use crate::{BlockMeta, BlockState};
use crate::{BlockReadContext, BlockWriteContext, SpdkMetaStore, VfsDir};
use curvine_core_error::{err_box, CommonResult};
use curvine_io::IOResult;
use curvine_model::ExtendedBlock;
use log::{info, warn};
use std::sync::Arc;

#[cfg(feature = "spdk")]
use curvine_io::IOError;
#[cfg(feature = "spdk")]
use curvine_storage_spdk::SpdkBdev;

#[derive(Clone)]
pub struct BdevLayout {
    spdk_meta: Option<Arc<SpdkMetaStore>>,
}

impl BdevLayout {
    pub fn new(spdk_meta: Option<Arc<SpdkMetaStore>>) -> Self {
        Self { spdk_meta }
    }

    #[cfg(feature = "spdk")]
    fn bdev_name(dir: &VfsDir) -> IOResult<&str> {
        dir.state.bdev_name.as_deref().ok_or_else(|| {
            IOError::from(format!("SPDK dir {} has no bdev name assigned", dir.id()))
        })
    }
}

impl BlockLayout for BdevLayout {
    fn preserves_committed_on_write(&self) -> bool {
        false
    }

    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let allocated_bytes = match dir.state.offset_alloc.allocation_size(block.len) {
            Some(size) => size,
            None => return err_box!("Invalid bdev allocation size: {}", block.len),
        };
        let mut meta = BlockMeta::with_tmp(block, dir);
        let offset = dir
            .state
            .offset_alloc
            .allocate(block.id, block.len)
            .map_err(|e| {
                curvine_core_error::err_msg!(format!(
                    "Failed to allocate bdev offset for block {}: {}",
                    block.id, e
                ))
            })?;
        meta.bdev_offset = offset;
        meta.actual_len = allocated_bytes;
        Ok(meta)
    }

    fn prepare_write(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        block: &ExtendedBlock,
    ) -> CommonResult<BlockMeta> {
        let (offset, allocated_bytes) =
            dir.state.offset_alloc.get_entry(meta.id()).ok_or_else(|| {
                curvine_core_error::err_msg!(format!(
                    "No bdev allocation found for block {} in dir {}",
                    meta.id(),
                    dir.id()
                ))
            })?;
        if block.len < 0 || block.len > allocated_bytes {
            return err_box!(
                "Block {} write size {} exceeds bdev extent {}",
                meta.id(),
                block.len,
                allocated_bytes
            );
        }

        let mut prepared = BlockMeta::new(meta.id(), block.len, dir);
        prepared.bdev_offset = offset;
        prepared.actual_len = allocated_bytes;
        Ok(prepared)
    }

    fn finalize(
        &self,
        _dir: &VfsDir,
        meta: &BlockMeta,
        committed_len: i64,
    ) -> CommonResult<BlockMeta> {
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
                dir_id: dir.id(),
                storage_type: dir.storage_type(),
                actual_len: record.size,
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

    fn release(&self, dir: &VfsDir, meta: &BlockMeta) {
        dir.state.offset_alloc.free(meta.id());
    }

    // SPDK blocks have no filesystem file; offset release happens in release().
    fn deallocate(&self, _dir: &VfsDir, _meta: &BlockMeta) -> CommonResult<()> {
        Ok(())
    }

    fn open_writer(&self, dir: &VfsDir, meta: &BlockMeta, off: i64) -> IOResult<BlockWriteContext> {
        validate_open_offset(meta, off)?;
        #[cfg(feature = "spdk")]
        {
            let bdev_name = Self::bdev_name(dir)?;
            let base_offset = meta.bdev_offset;
            let abs_offset = base_offset.checked_add(off).ok_or_else(|| {
                IOError::from(format!(
                    "Block write offset overflow: base={}, offset={}",
                    base_offset, off
                ))
            })?;
            let max_len = 0.max(meta.len - off);
            info!(
                "Opening SPDK bdev '{}' for writing at offset {} (block {} base={}, max_len={})",
                bdev_name, abs_offset, meta.id, base_offset, max_len
            );
            if max_len == 0 {
                return err_box!("Cannot open SPDK writer: no space remaining");
            }
            let bdev = SpdkBdev::open_write(bdev_name, abs_offset, max_len)?;
            BlockWriteContext::new(bdev, base_offset, meta.len, off)
        }
        #[cfg(not(feature = "spdk"))]
        {
            let _ = (dir, meta, off);
            err_box!("SPDK support not compiled in")
        }
    }

    fn open_reader(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        off: i64,
        logical_len: i64,
    ) -> IOResult<BlockReadContext> {
        let physical_len = meta.len;
        let logical_len = logical_len.max(physical_len);
        if off < 0 || off > logical_len {
            return err_box!(
                "Invalid block offset: {}, block length: {}",
                off,
                logical_len
            );
        }
        #[cfg(feature = "spdk")]
        {
            let bdev_name = Self::bdev_name(dir)?;
            let base_offset = meta.bdev_offset;
            // Seek within physical bytes; sparse logical tail is synthesized.
            let device_off = off.min(physical_len);
            let abs_offset = base_offset.checked_add(device_off).ok_or_else(|| {
                IOError::from(format!(
                    "Block read offset overflow: base={}, offset={}",
                    base_offset, device_off
                ))
            })?;
            let abs_offset = u64::try_from(abs_offset).map_err(|_| {
                IOError::from(format!(
                    "Invalid absolute block read offset: {}",
                    abs_offset
                ))
            })?;
            let max_len = physical_len - device_off;
            info!(
                "Opening SPDK bdev '{}' for reading at offset {} (block {} base={}, physical_len={}, logical_len={}, max_len={})",
                bdev_name, abs_offset, meta.id, base_offset, physical_len, logical_len, max_len
            );
            let bdev = SpdkBdev::open_read(bdev_name, abs_offset, max_len)?;
            BlockReadContext::with_physical(bdev, base_offset, logical_len, physical_len, off)
        }
        #[cfg(not(feature = "spdk"))]
        {
            let _ = (dir, meta, off, logical_len, physical_len);
            err_box!("SPDK support not compiled in")
        }
    }

    fn short_circuit(&self, _dir: &VfsDir, _meta: &BlockMeta) -> CommonResult<Option<String>> {
        Ok(None)
    }
}
