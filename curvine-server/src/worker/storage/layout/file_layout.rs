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
use crate::worker::storage::layout::{validate_open_offset, BlockLayout};
use crate::worker::storage::{BlockReadContext, BlockWriteContext, VfsDir};
use curvine_common::state::ExtendedBlock;
use orpc::common::FileUtils;
use orpc::io::{BlockDevice, IOError, IOResult, LocalFile};
use orpc::CommonResult;
use std::fs::File;

#[derive(Clone, Copy)]
pub struct FileLayout;

impl BlockLayout for FileLayout {
    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let meta = BlockMeta::with_tmp(block, dir);
        let file = meta.get_block_path()?;
        File::create(file)?;
        Ok(meta)
    }

    fn finalize(&self, meta: &BlockMeta, _committed_len: i64) -> CommonResult<BlockMeta> {
        BlockMeta::with_final(meta)
    }

    fn scan(&self, dir: &VfsDir) -> CommonResult<Vec<BlockMeta>> {
        let active_dir = FileUtils::list_files(&dir.active_dir, true)?;
        let staging_dir = FileUtils::list_files(&dir.staging_dir, true)?;

        let mut blocks = vec![];
        for file in active_dir {
            if let Ok(meta) = BlockMeta::from_file(&file, BlockState::Finalized, dir) {
                blocks.push(meta);
            }
        }

        for file in staging_dir {
            if let Ok(meta) = BlockMeta::from_file(&file, BlockState::Recovering, dir) {
                blocks.push(meta);
            }
        }

        Ok(blocks)
    }

    // Filesystem blocks own no under-lock state; offset/file teardown is in deallocate().
    fn release(&self, _meta: &BlockMeta) -> CommonResult<()> {
        Ok(())
    }

    fn deallocate(&self, meta: &BlockMeta) -> CommonResult<()> {
        FileUtils::delete_path(meta.get_block_path()?, false)?;
        Ok(())
    }

    fn open_writer(&self, meta: &BlockMeta, off: i64) -> IOResult<BlockWriteContext> {
        validate_open_offset(meta, off)?;
        let file = meta.get_block_file()?;
        let device = BlockDevice::Local(LocalFile::with_write_offset(file, false, off)?);
        BlockWriteContext::new(device, 0, meta.len, off)
    }

    fn open_reader(&self, meta: &BlockMeta, off: i64) -> IOResult<BlockReadContext> {
        validate_open_offset(meta, off)?;
        let read_off = u64::try_from(off)
            .map_err(|_| IOError::from(format!("Invalid read offset: {}", off)))?;
        let file = meta.get_block_file()?;
        let device = BlockDevice::Local(LocalFile::with_read(file, read_off)?);
        BlockReadContext::new(device, 0, meta.len, off)
    }

    fn short_circuit(&self, meta: &BlockMeta) -> CommonResult<Option<String>> {
        Ok(Some(meta.get_block_file()?))
    }
}
