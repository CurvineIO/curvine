// Copyright 2026 OPPO
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

//! Slot table for the directory handles of a packed directory.
//!
//! Slots live in fixed-size blocks behind `Arc`. Cloning the table copies one
//! pointer per block; a write copies only the block it touches
//! (`Arc::make_mut`). That keeps the cost of publishing a read snapshot after a
//! write independent of how many subdirectories the parent holds, which a flat
//! `Vec<Option<Arc<DirNode>>>` cannot: it cloned and later dropped every slot.

use super::DirNode;
use std::sync::Arc;

/// Slots per block. One block copy is `BLOCK` reference-count increments, so it
/// bounds the per-write copy; larger blocks make clones cheaper but writes
/// dearer. 64 keeps a block within a cache line pair of pointers.
const BLOCK: usize = 64;

type Block = Vec<Option<Arc<DirNode>>>;

#[derive(Clone, Default)]
pub(super) struct DirSlots {
    blocks: Vec<Arc<Block>>,
    len: usize,
}

impl DirSlots {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn get(&self, slot: usize) -> Option<&Arc<DirNode>> {
        self.blocks
            .get(slot / BLOCK)
            .and_then(|block| block.get(slot % BLOCK))
            .and_then(Option::as_ref)
    }

    /// Reserves the next slot index; the caller reads `len` before calling.
    pub(super) fn push_empty(&mut self) {
        self.slot_mut(self.len).take();
    }

    pub(super) fn push(&mut self, node: Arc<DirNode>) {
        *self.slot_mut(self.len) = Some(node);
    }

    pub(super) fn set(&mut self, slot: usize, node: Option<Arc<DirNode>>) {
        *self.slot_mut(slot) = node;
    }

    pub(super) fn take(&mut self, slot: usize) -> Option<Arc<DirNode>> {
        if slot >= self.len {
            return None;
        }
        self.slot_mut(slot).take()
    }

    fn slot_mut(&mut self, slot: usize) -> &mut Option<Arc<DirNode>> {
        if slot >= self.len {
            self.len = slot + 1;
        }
        let block = slot / BLOCK;
        while self.blocks.len() <= block {
            self.blocks.push(Arc::new(Vec::with_capacity(BLOCK)));
        }
        let entries = Arc::make_mut(&mut self.blocks[block]);
        if entries.len() <= slot % BLOCK {
            entries.resize(slot % BLOCK + 1, None);
        }
        &mut entries[slot % BLOCK]
    }
}
