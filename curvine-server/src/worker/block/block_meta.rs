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

use crate::worker::storage::{DirState, VfsDir, ACTIVE_DIR, STAGING_DIR};
use curvine_common::state::{ExtendedBlock, StorageType};
use once_cell::sync::Lazy;
use orpc::common::{ByteUnit, FileUtils};
use orpc::io::{BlockDevice, IOResult, LocalFile};

#[cfg(feature = "spdk")]
use log::info;
#[cfg(feature = "spdk")]
use orpc::io::SpdkBdev;

use orpc::{err_box, sys, try_err, CommonResult};
use regex::Regex;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs};

static FILE_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^blk_(\w+)$").unwrap());

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i8)]
pub enum BlockState {
    Finalized = 0,
    Writing = 1,
    Recovering = 2,
}

impl BlockState {
    pub fn get_name(&self, id: i64) -> String {
        format!("blk_{}", id)
    }

    pub fn check_file(file: &str) -> Option<i64> {
        match FILE_REGEX.captures(file) {
            None => None,
            Some(v) => {
                let id = match v.get(1) {
                    None => return None,
                    Some(v) => v.as_str().parse::<i64>().unwrap(),
                };
                Some(id)
            }
        }
    }
}

/// Metadata information of the worker block.
/// The len field has 3 meanings:
/// 1. The value of block in writing, len is block_size.
/// 2. The final block has been executed, and len is the current file length.
/// 3. Worker restarts the loading block, len is the file length.
#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub(crate) id: i64,
    pub(crate) len: i64,
    pub(crate) state: BlockState,
    pub(crate) dir: Arc<DirState>,
    pub(crate) actual_len: i64,
    /// SPDK bdev byte offset
    pub(crate) bdev_offset: i64,
}

impl BlockMeta {
    pub fn new(id: i64, block_size: i64, dir: &VfsDir) -> Self {
        Self {
            id,
            len: block_size,
            state: BlockState::Writing,
            dir: dir.state.clone(),
            actual_len: block_size,
            bdev_offset: 0,
        }
    }

    fn get_len<P: AsRef<Path>>(path: P) -> CommonResult<(i64, i64)> {
        let path = path.as_ref();
        let metadata = path.metadata()?;
        let len = metadata.len() as i64;
        let actual_len = sys::file_actual_size(metadata)? as i64;

        Ok((len, actual_len))
    }

    pub fn from_file(file: &str, state: BlockState, dir: &VfsDir) -> CommonResult<Self> {
        let path = Path::new(file);
        let filename = match FileUtils::filename(path) {
            None => return err_box!("Not found filename {}", file),
            Some(v) => v,
        };

        let (len, actual_len) = Self::get_len(path)?;
        match BlockState::check_file(&filename) {
            None => err_box!("Not a block file {}", file),
            Some(id) => {
                let meta = Self {
                    id,
                    len,
                    state,
                    dir: dir.state.clone(),
                    actual_len,
                    bdev_offset: 0,
                };

                Ok(meta)
            }
        }
    }

    pub fn with_tmp(block: &ExtendedBlock, dir: &VfsDir) -> Self {
        Self::new(block.id, block.len, dir)
    }

    pub fn with_final(meta: &BlockMeta) -> CommonResult<Self> {
        let path = meta.get_block_path()?;
        let (len, actual_len) = Self::get_len(&path)?;

        let meta = Self {
            id: meta.id,
            len,
            state: BlockState::Finalized,
            dir: meta.dir.clone(),
            actual_len,
            bdev_offset: meta.bdev_offset,
        };

        Ok(meta)
    }

    pub fn with_id(id: i64) -> Self {
        Self {
            id,
            len: 0,
            state: BlockState::Finalized,
            dir: Arc::new(DirState::default()),
            actual_len: 0,
            bdev_offset: 0,
        }
    }

    /// Finalize SPDK block — uses in-memory BlockMeta length (no filesystem file).
    pub fn with_final_spdk(meta: &BlockMeta, committed_len: i64) -> Self {
        Self {
            id: meta.id,
            len: committed_len,
            state: BlockState::Finalized,
            dir: meta.dir.clone(),
            actual_len: committed_len,
            bdev_offset: meta.bdev_offset,
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn state(&self) -> &BlockState {
        &self.state
    }

    pub fn is_final(&self) -> bool {
        self.state == BlockState::Finalized
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, BlockState::Finalized | BlockState::Writing)
    }

    // Write some test data.
    pub fn write_test_data(&self, size: &str) -> CommonResult<()> {
        let bytes = ByteUnit::from_str(size)?.as_byte();
        let str = "A".repeat(bytes as usize);
        LocalFile::write_string(self.get_block_path()?, str.as_str(), true)?;
        Ok(())
    }

    fn get_block_dir(&self) -> CommonResult<PathBuf> {
        let dir = match self.state {
            BlockState::Finalized | BlockState::Writing => {
                let uid = self.id as u64;
                let d1 = (uid >> 48) & 0x1F;
                let d2 = (uid >> 32) & 0x1F;

                let mut path = PathBuf::from(&self.dir.base_path);
                path.push(ACTIVE_DIR);
                path.push(format!("b{}", d1));
                path.push(format!("b{}", d2));
                path
            }

            BlockState::Recovering => {
                let mut path = PathBuf::from(&self.dir.base_path);
                path.push(STAGING_DIR);
                path
            }
        };

        if dir.exists() {
            if dir.is_dir() {
                Ok(dir)
            } else {
                err_box!("Path {} not a dir", dir.to_string_lossy())
            }
        } else {
            try_err!(fs::create_dir_all(&dir));
            Ok(dir)
        }
    }

    // 1/2/blk_blockid
    pub fn get_block_path(&self) -> CommonResult<PathBuf> {
        let mut path = self.get_block_dir()?;
        path.push(self.state.get_name(self.id));
        Ok(path)
    }

    pub fn get_block_file(&self) -> CommonResult<String> {
        let file = self.get_block_path()?.to_string_lossy().to_string();
        Ok(file)
    }

    pub fn create_writer(&self, off: i64, overwrite: bool) -> IOResult<BlockDevice> {
        match self.storage_type() {
            #[cfg(feature = "spdk")]
            StorageType::SpdkDisk => {
                let bdev_name = self.get_bdev_name()?;
                let abs_offset = self.bdev_offset + off;
                let max_len = 0.max(self.len - off);
                info!(
                    "Opening SPDK bdev '{}' for writing at offset {} (block {} base={}, max_len={})",
                    bdev_name, abs_offset, self.id, self.bdev_offset, max_len
                );
                if max_len == 0 {
                    return err_box!("Cannot open SPDK writer: no space remaining");
                }
                let bdev = SpdkBdev::open_write(&bdev_name, abs_offset, max_len)?;
                Ok(BlockDevice::Spdk(bdev))
            }
            _ => {
                let file = self.get_block_file()?;
                let local = LocalFile::with_write_offset(file, overwrite, off)?;
                Ok(BlockDevice::Local(local))
            }
        }
    }

    pub fn create_reader(&self, offset: u64) -> IOResult<BlockDevice> {
        match self.storage_type() {
            #[cfg(feature = "spdk")]
            StorageType::SpdkDisk => {
                let bdev_name = self.get_bdev_name()?;
                let abs_offset = self.bdev_offset as u64 + offset;
                let max_len = 0.max(self.len - offset as i64);
                info!(
                    "Opening SPDK bdev '{}' for reading at offset {} (block {} base={}, max_len={})",
                    bdev_name, abs_offset, self.id, self.bdev_offset, max_len
                );
                if max_len == 0 {
                    return err_box!("Cannot open SPDK reader: no space remaining");
                }
                let bdev = SpdkBdev::open_read(&bdev_name, abs_offset, max_len)?;
                Ok(BlockDevice::Spdk(bdev))
            }
            _ => {
                let local = LocalFile::with_read(self.get_block_file()?, offset)?;
                Ok(BlockDevice::Local(local))
            }
        }
    }

    /// Get the SPDK bdev name for this block.
    #[cfg(feature = "spdk")]
    fn get_bdev_name(&self) -> IOResult<String> {
        self.dir.bdev_name.clone().ok_or_else(|| {
            orpc::io::IOError::from(format!(
                "block {} has StorageType::SpdkDisk but dir (dir_id={}) has no bdev_name assigned",
                self.id, self.dir.dir_id
            ))
        })
    }

    pub fn dir_id(&self) -> u32 {
        self.dir.dir_id
    }

    pub fn storage_type(&self) -> StorageType {
        self.dir.storage_type
    }

    pub fn base_path(&self) -> &Path {
        self.dir.base_path.as_path()
    }
}

impl fmt::Display for BlockMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BlockMeta id = {}, len = {}, state = {:?}",
            self.id, self.len, self.state
        )
    }
}

#[cfg(all(test, feature = "spdk"))]
mod test {
    use super::*;
    use crate::worker::storage::DirState;
    #[cfg(feature = "spdk")]
    fn spdk_writing_meta(id: i64, block_size: i64) -> BlockMeta {
        let dir = Arc::new(DirState {
            dir_id: 1,
            base_path: PathBuf::from("/nonexistent/spdk/path"),
            storage_type: StorageType::SpdkDisk,
            bdev_name: Some("NVMe_test_n1".to_string()),
            bdev_capacity: 1024 * 1024 * 1024,
            offset_alloc: DirState::new_offset_alloc(
                StorageType::SpdkDisk,
                1024 * 1024 * 1024,
                4096,
            ),
        });
        BlockMeta {
            id,
            len: block_size,
            state: BlockState::Writing,
            dir,
            actual_len: block_size,
            bdev_offset: 0,
        }
    }
    #[test]
    fn with_final_spdk_succeeds() {
        let meta = spdk_writing_meta(1, 4096);
        let final_meta = BlockMeta::with_final_spdk(&meta, 2048);
        assert_eq!(final_meta.state, BlockState::Finalized);
        assert_eq!(final_meta.len, 2048);
    }
    #[test]
    fn with_final_local_succeeds() -> CommonResult<()> {
        let test_dir = "../testing/block_meta_test";
        let _ = std::fs::remove_dir_all(test_dir);
        let dir = Arc::new(DirState {
            dir_id: 0,
            base_path: PathBuf::from(test_dir),
            storage_type: StorageType::Ssd,
            bdev_name: None,
            bdev_capacity: 0,
            offset_alloc: DirState::new_offset_alloc(StorageType::Ssd, 0, 512),
        });
        let meta = BlockMeta {
            id: 1,
            len: 100,
            state: BlockState::Writing,
            dir,
            actual_len: 100,
            bdev_offset: 0,
        };
        let path = meta.get_block_path()?;
        LocalFile::write_string(&path, &"data", true)?;
        let final_meta = BlockMeta::with_final(&meta)?;
        assert_eq!(final_meta.state, BlockState::Finalized);
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
