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
use std::collections::HashMap;

#[derive(Default)]
pub struct MemMetaStore {
    blocks: HashMap<i64, BlockMeta>,
}

impl MemMetaStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, id: i64) -> Option<&BlockMeta> {
        self.blocks.get(&id)
    }

    pub fn put(&mut self, meta: BlockMeta) -> Option<BlockMeta> {
        self.blocks.insert(meta.id(), meta)
    }

    pub fn remove(&mut self, id: i64) -> Option<BlockMeta> {
        self.blocks.remove(&id)
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn values(&self) -> impl Iterator<Item = &BlockMeta> {
        self.blocks.values()
    }

    #[cfg(test)]
    pub fn contains_key(&self, id: i64) -> bool {
        self.blocks.contains_key(&id)
    }
}
