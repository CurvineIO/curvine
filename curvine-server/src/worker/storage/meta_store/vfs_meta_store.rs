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

use crate::worker::block::BlockMeta;
use crate::worker::storage::meta_store::{BlockMetaStore, MemMetaStore};
use crate::worker::storage::SpdkMetaStore;
use curvine_common::state::StorageType;
use log::warn;
use std::sync::Arc;

pub struct VfsMetaStore {
    mem: MemMetaStore,
    spdk: Option<Arc<SpdkMetaStore>>,
}

impl VfsMetaStore {
    pub fn new(spdk: Option<Arc<SpdkMetaStore>>) -> Self {
        Self {
            mem: MemMetaStore::new(),
            spdk,
        }
    }

    pub fn get(&self, id: i64) -> Option<&BlockMeta> {
        self.mem.get(id)
    }

    pub fn put(&mut self, meta: BlockMeta) -> Option<BlockMeta> {
        self.persist_spdk_put(&meta);
        self.mem.put(meta)
    }

    pub fn remove(&mut self, id: i64) -> Option<BlockMeta> {
        let meta = self.mem.remove(id);
        if let Some(meta) = meta.as_ref() {
            self.persist_spdk_remove(meta);
        }
        meta
    }

    pub fn block_count(&self) -> usize {
        self.mem.block_count()
    }

    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        self.mem.values().cloned().collect()
    }

    #[cfg(test)]
    pub fn contains_key(&self, id: i64) -> bool {
        self.mem.contains_key(id)
    }

    fn persist_spdk_put(&self, meta: &BlockMeta) {
        if meta.storage_type() != StorageType::SpdkDisk {
            return;
        }

        if let Some(store) = self.spdk.as_ref() {
            if let Err(e) = store.put_block_meta(meta) {
                warn!("SpdkMetaStore put failed for block {}: {}", meta.id(), e);
            }
        }
    }

    fn persist_spdk_remove(&self, meta: &BlockMeta) {
        if meta.storage_type() != StorageType::SpdkDisk {
            return;
        }

        if let Some(store) = self.spdk.as_ref() {
            if let Err(e) = store.remove_block_meta(meta) {
                warn!("SpdkMetaStore delete failed for block {}: {}", meta.id(), e);
            }
        }
    }
}
