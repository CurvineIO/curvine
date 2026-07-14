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

mod bdev_layout;
mod file_layout;

pub use self::bdev_layout::BdevLayout;
pub use self::file_layout::FileLayout;

use crate::worker::block::BlockMeta;
use crate::worker::storage::{BlockReadContext, BlockWriteContext, SpdkMetaStore, VfsDir};
use curvine_common::state::{ExtendedBlock, StorageType};
use orpc::io::IOResult;
use orpc::{err_box, CommonResult};
use std::sync::Arc;

fn validate_open_offset(meta: &BlockMeta, off: i64) -> IOResult<()> {
    if off < 0 || off > meta.len {
        return err_box!("Invalid block offset: {}, block length: {}", off, meta.len);
    }
    Ok(())
}

pub trait BlockLayout {
    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

    fn finalize(&self, meta: &BlockMeta, committed_len: i64) -> CommonResult<BlockMeta>;

    fn scan(&self, dir: &VfsDir) -> CommonResult<Vec<BlockMeta>>;

    /// Release layout-owned state while the dataset lock is still held.
    fn release(&self, meta: &BlockMeta) -> CommonResult<()>;

    /// Remove backing resources. Callers may run this outside the dataset lock.
    fn deallocate(&self, meta: &BlockMeta) -> CommonResult<()>;

    fn open_writer(&self, meta: &BlockMeta, off: i64) -> IOResult<BlockWriteContext>;

    fn open_reader(&self, meta: &BlockMeta, off: i64) -> IOResult<BlockReadContext>;

    /// Local path a co-located client can open directly, `Ok(None)` if the
    /// layout cannot expose one (e.g. raw bdev). Errors are propagated so
    /// callers can distinguish "not eligible" from "path resolution failed".
    fn short_circuit(&self, meta: &BlockMeta) -> CommonResult<Option<String>>;
}

#[derive(Clone)]
pub enum BlockLayoutKind {
    File(FileLayout),
    Bdev(BdevLayout),
}

impl BlockLayoutKind {
    fn file() -> Self {
        Self::File(FileLayout)
    }

    fn bdev(spdk_meta: Option<Arc<SpdkMetaStore>>) -> Self {
        Self::Bdev(BdevLayout::new(spdk_meta))
    }
}

#[derive(Clone)]
pub struct BlockLayouts {
    file: BlockLayoutKind,
    bdev: BlockLayoutKind,
}

impl BlockLayouts {
    pub fn new(spdk_meta: Option<Arc<SpdkMetaStore>>) -> Self {
        Self {
            file: BlockLayoutKind::file(),
            bdev: BlockLayoutKind::bdev(spdk_meta),
        }
    }

    pub fn get(&self, storage_type: StorageType) -> &BlockLayoutKind {
        match storage_type {
            StorageType::SpdkDisk => &self.bdev,
            _ => &self.file,
        }
    }
}

impl BlockLayout for BlockLayoutKind {
    fn allocate(&self, dir: &VfsDir, block: &ExtendedBlock) -> CommonResult<BlockMeta> {
        match self {
            Self::File(layout) => layout.allocate(dir, block),
            Self::Bdev(layout) => layout.allocate(dir, block),
        }
    }

    fn finalize(&self, meta: &BlockMeta, committed_len: i64) -> CommonResult<BlockMeta> {
        match self {
            Self::File(layout) => layout.finalize(meta, committed_len),
            Self::Bdev(layout) => layout.finalize(meta, committed_len),
        }
    }

    fn scan(&self, dir: &VfsDir) -> CommonResult<Vec<BlockMeta>> {
        match self {
            Self::File(layout) => layout.scan(dir),
            Self::Bdev(layout) => layout.scan(dir),
        }
    }

    fn release(&self, meta: &BlockMeta) -> CommonResult<()> {
        match self {
            Self::File(layout) => layout.release(meta),
            Self::Bdev(layout) => layout.release(meta),
        }
    }

    fn deallocate(&self, meta: &BlockMeta) -> CommonResult<()> {
        match self {
            Self::File(layout) => layout.deallocate(meta),
            Self::Bdev(layout) => layout.deallocate(meta),
        }
    }

    fn open_writer(&self, meta: &BlockMeta, off: i64) -> IOResult<BlockWriteContext> {
        match self {
            Self::File(layout) => layout.open_writer(meta, off),
            Self::Bdev(layout) => layout.open_writer(meta, off),
        }
    }

    fn open_reader(&self, meta: &BlockMeta, off: i64) -> IOResult<BlockReadContext> {
        match self {
            Self::File(layout) => layout.open_reader(meta, off),
            Self::Bdev(layout) => layout.open_reader(meta, off),
        }
    }

    fn short_circuit(&self, meta: &BlockMeta) -> CommonResult<Option<String>> {
        match self {
            Self::File(layout) => layout.short_circuit(meta),
            Self::Bdev(layout) => layout.short_circuit(meta),
        }
    }
}
