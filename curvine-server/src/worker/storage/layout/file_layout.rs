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
#[cfg(test)]
use orpc::common::ByteUnit;
use orpc::common::FileUtils;
use orpc::io::{BlockDevice, IOError, IOResult, LocalFile};
use orpc::{err_box, try_err, CommonResult};
use std::fs::{self, File};
use std::path::PathBuf;

#[derive(Clone, Copy)]
pub struct FileLayout;

const ACTIVE_DIR: &str = "active";
const STAGING_DIR: &str = "staging";

impl FileLayout {
    fn active_dir(dir: &VfsDir) -> PathBuf {
        dir.base_path().join(ACTIVE_DIR)
    }

    fn staging_dir(dir: &VfsDir) -> PathBuf {
        dir.base_path().join(STAGING_DIR)
    }

    fn ensure_layout_dirs(dir: &VfsDir) -> CommonResult<()> {
        FileUtils::create_dir(Self::active_dir(dir), true)?;
        FileUtils::create_dir(Self::staging_dir(dir), true)?;
        Ok(())
    }

    fn block_dir(dir: &VfsDir, meta: &BlockMeta) -> CommonResult<PathBuf> {
        let path = match meta.state() {
            BlockState::Finalized | BlockState::Writing => {
                let uid = meta.id() as u64;
                let d1 = (uid >> 48) & 0x1F;
                let d2 = (uid >> 32) & 0x1F;
                Self::active_dir(dir)
                    .join(format!("b{}", d1))
                    .join(format!("b{}", d2))
            }
            BlockState::Recovering => Self::staging_dir(dir),
        };

        if path.exists() {
            if !path.is_dir() {
                return err_box!("Path {} not a dir", path.to_string_lossy());
            }
        } else {
            try_err!(fs::create_dir_all(&path));
        }
        Ok(path)
    }

    pub(crate) fn block_path(dir: &VfsDir, meta: &BlockMeta) -> CommonResult<PathBuf> {
        Ok(Self::block_dir(dir, meta)?.join(meta.state().get_name(meta.id())))
    }

    fn block_file(dir: &VfsDir, meta: &BlockMeta) -> CommonResult<String> {
        Ok(Self::block_path(dir, meta)?.to_string_lossy().to_string())
    }

    #[cfg(test)]
    pub(crate) fn write_test_data(dir: &VfsDir, meta: &BlockMeta, size: &str) -> CommonResult<()> {
        let bytes = ByteUnit::from_str(size)?.as_byte();
        LocalFile::write_string(
            Self::block_path(dir, meta)?,
            &"A".repeat(bytes as usize),
            true,
        )?;
        Ok(())
    }
}

impl BlockLayout for FileLayout {
    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        let meta = BlockMeta::with_tmp(block, dir);
        let file = Self::block_path(dir, &meta)?;
        File::create(file)?;
        Ok(meta)
    }

    fn prepare_write(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        block: &ExtendedBlock,
    ) -> CommonResult<BlockMeta> {
        if block.len < 0 {
            return err_box!("Invalid file block size: {}", block.len);
        }
        let mut prepared = BlockMeta::new(meta.id(), block.len, dir);
        // The old file still occupies space until resize/finalize. Keep the
        // larger physical charge so preparing a smaller write cannot temporarily
        // overstate available capacity or under-release on abort.
        prepared.actual_len = meta.actual_len.max(block.len);
        Ok(prepared)
    }

    fn finalize(
        &self,
        dir: &VfsDir,
        meta: &BlockMeta,
        _committed_len: i64,
    ) -> CommonResult<BlockMeta> {
        let path = Self::block_path(dir, meta)?;
        BlockMeta::with_final(meta, &path)
    }

    fn scan(&self, dir: &VfsDir) -> CommonResult<Vec<BlockMeta>> {
        Self::ensure_layout_dirs(dir)?;
        let active_dir = FileUtils::list_files(Self::active_dir(dir), true)?;
        let staging_dir = FileUtils::list_files(Self::staging_dir(dir), true)?;

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
    fn release(&self, _dir: &VfsDir, _meta: &BlockMeta) {}

    fn deallocate(&self, dir: &VfsDir, meta: &BlockMeta) -> CommonResult<()> {
        FileUtils::delete_path(Self::block_path(dir, meta)?, false)?;
        Ok(())
    }

    fn open_writer(&self, dir: &VfsDir, meta: &BlockMeta, off: i64) -> IOResult<BlockWriteContext> {
        validate_open_offset(meta, off)?;
        let file = Self::block_file(dir, meta)?;
        let device = BlockDevice::Local(LocalFile::with_write_offset(file, false, off)?);
        BlockWriteContext::new(device, 0, meta.len, off)
    }

    fn open_reader(&self, dir: &VfsDir, meta: &BlockMeta, off: i64) -> IOResult<BlockReadContext> {
        validate_open_offset(meta, off)?;
        let read_off = u64::try_from(off)
            .map_err(|_| IOError::from(format!("Invalid read offset: {}", off)))?;
        let file = Self::block_file(dir, meta)?;
        let device = BlockDevice::Local(LocalFile::with_read(file, read_off)?);
        BlockReadContext::new(device, 0, meta.len, off)
    }

    fn short_circuit(&self, dir: &VfsDir, meta: &BlockMeta) -> CommonResult<Option<String>> {
        Ok(Some(Self::block_file(dir, meta)?))
    }
}
