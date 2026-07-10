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

mod mem_meta_store;
mod vfs_meta_store;

pub use self::mem_meta_store::MemMetaStore;
pub use self::vfs_meta_store::VfsMetaStore;

use crate::worker::block::BlockMeta;
use orpc::CommonResult;

/// Persistent side-car store for block metadata records.
///
/// The in-memory index remains owned by `VfsMetaStore`; this trait is the narrow
/// adapter used for stores such as the current SPDK RocksDB records.
pub trait BlockMetaStore: Send + Sync {
    fn put_block_meta(&self, meta: &BlockMeta) -> CommonResult<()>;

    fn remove_block_meta(&self, meta: &BlockMeta) -> CommonResult<()>;
}
