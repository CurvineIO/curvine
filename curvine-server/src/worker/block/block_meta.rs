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

use crate::worker::storage::{DirState, VfsDir, FINALIZED_DIR, RBW_DIR};
use curvine_common::state::{ExtendedBlock, StorageType};
use once_cell::sync::Lazy;
use orpc::common::{ByteUnit, FileUtils};
use orpc::io::{IOResult, LocalFile};
use orpc::{err_box, try_err, CommonResult};
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
}

impl BlockMeta {
    pub fn new(id: i64, block_size: i64, dir: &VfsDir) -> Self {
        Self {
            id,
            len: block_size,
            state: BlockState::Writing,
            dir: dir.state.clone(),
        }
    }

    pub fn from_file(file: &str, state: BlockState, dir: &VfsDir) -> CommonResult<Self> {
        let path = Path::new(file);
        let len = path.metadata()?.len();
        let filename = match FileUtils::filename(path) {
            None => return err_box!("Not found filename {}", file),
            Some(v) => v,
        };

        match BlockState::check_file(&filename) {
            None => err_box!("Not a block file {}", file),
            Some(id) => {
                let meta = Self {
                    id,
                    len: len as i64,
                    state,
                    dir: dir.state.clone(),
                };

                Ok(meta)
            }
        }
    }

    pub fn with_tmp(block: &ExtendedBlock, dir: &VfsDir) -> Self {
        Self::new(block.id, block.len, dir)
    }

    pub fn with_final(meta: &BlockMeta, file_size: i64) -> Self {
        Self {
            id: meta.id,
            len: file_size,
            state: BlockState::Finalized,
            dir: meta.dir.clone(),
        }
    }

    pub fn with_id(id: i64) -> Self {
        Self {
            id,
            len: 0,
            state: BlockState::Finalized,
            dir: Arc::new(DirState::default()),
        }
    }

    pub fn with_cow(finalized_meta: &BlockMeta, new_block_size: i64, dir: &VfsDir) -> Self {
        Self {
            id: finalized_meta.id,
            len: new_block_size,
            state: BlockState::Writing,
            dir: dir.state.clone(),
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

    // Write some test data.
    pub fn write_test_data(&self, size: &str) -> CommonResult<()> {
        let bytes = ByteUnit::from_str(size)?.as_byte();
        let str = "A".repeat(bytes as usize);
        LocalFile::write_string(self.get_block_path()?, str.as_str(), true)?;
        Ok(())
    }

    pub fn support_append(&self) -> bool {
        self.state != BlockState::Recovering
    }

    fn get_block_dir(&self) -> CommonResult<PathBuf> {
        let dir = match self.state {
            BlockState::Finalized => {
                let d1 = (self.id >> 16) & 0x1F;
                let d2 = (self.id >> 8) & 0x1F;

                let mut path = PathBuf::from(&self.dir.base_path);
                path.push(FINALIZED_DIR);
                path.push(format!("b{}", d1));
                path.push(format!("b{}", d2));
                path
            }

            BlockState::Writing | BlockState::Recovering => {
                let mut path = PathBuf::from(&self.dir.base_path);
                path.push(RBW_DIR);
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

    pub fn create_writer(&self, is_append: bool) -> IOResult<LocalFile> {
        let file = self.get_block_file()?;
        if is_append {
            LocalFile::with_append(file)
        } else {
            // truncate=false keeps existing content while allowing seek + write for random writes
            LocalFile::with_write(file, false)
        }
    }

    pub fn create_writer_with_offset(&self, is_append: bool, offset: i64) -> IOResult<LocalFile> {
        let file = self.get_block_file()?;
        if is_append {
            LocalFile::with_append(file)
        } else {
            // For random writes, use method that supports offset
            LocalFile::with_write_offset(file, false, offset)
        }
    }

    pub fn create_reader(&self, offset: u64) -> IOResult<LocalFile> {
        LocalFile::with_read(self.get_block_file()?, offset)
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
