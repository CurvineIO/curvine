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

use crate::worker::storage::VfsDir;
use curvine_common::state::{ExtendedBlock, StorageType};
use orpc::common::FileUtils;
use orpc::{err_box, sys, CommonResult};
use std::fmt;
use std::fmt::Formatter;
use std::path::Path;

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
        let id = file.strip_prefix("blk_")?;
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        id.parse::<i64>().ok()
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
    pub(crate) dir_id: u32,
    pub(crate) storage_type: StorageType,
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
            dir_id: dir.id(),
            storage_type: dir.storage_type(),
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
                    dir_id: dir.id(),
                    storage_type: dir.storage_type(),
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

    pub fn with_final(meta: &BlockMeta, path: &Path) -> CommonResult<Self> {
        let (len, actual_len) = Self::get_len(path)?;
        let meta = Self {
            id: meta.id,
            len,
            state: BlockState::Finalized,
            dir_id: meta.dir_id,
            storage_type: meta.storage_type,
            actual_len,
            bdev_offset: 0,
        };

        Ok(meta)
    }

    /// Finalize SPDK block — uses in-memory BlockMeta length (no filesystem file).
    pub fn with_final_spdk(meta: &BlockMeta, committed_len: i64) -> Self {
        Self {
            id: meta.id,
            len: committed_len,
            state: BlockState::Finalized,
            dir_id: meta.dir_id,
            storage_type: meta.storage_type,
            actual_len: meta.actual_len,
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

    pub fn dir_id(&self) -> u32 {
        self.dir_id
    }

    pub fn storage_type(&self) -> StorageType {
        self.storage_type
    }

    pub fn physical_bytes(&self) -> i64 {
        self.actual_len
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
    use orpc::io::LocalFile;
    use std::path::PathBuf;

    #[cfg(feature = "spdk")]
    fn spdk_writing_meta(id: i64, block_size: i64) -> BlockMeta {
        BlockMeta {
            id,
            len: block_size,
            state: BlockState::Writing,
            dir_id: 1,
            storage_type: StorageType::SpdkDisk,
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
        assert_eq!(final_meta.bdev_offset, 0);
        assert_eq!(final_meta.physical_bytes(), 4096);
    }

    #[test]
    fn with_final_local_succeeds() -> CommonResult<()> {
        let test_dir = "../testing/block_meta_test";
        let _ = std::fs::remove_dir_all(test_dir);
        std::fs::create_dir_all(test_dir)?;
        let path = PathBuf::from(test_dir).join("blk_1");
        LocalFile::write_string(&path, "data", true)?;
        let meta = BlockMeta {
            id: 1,
            len: 100,
            state: BlockState::Writing,
            dir_id: 0,
            storage_type: StorageType::Ssd,
            actual_len: 100,
            bdev_offset: 0,
        };
        let final_meta = BlockMeta::with_final(&meta, &path)?;
        assert_eq!(final_meta.state, BlockState::Finalized);
        let _ = std::fs::remove_dir_all(test_dir);
        Ok(())
    }
}
