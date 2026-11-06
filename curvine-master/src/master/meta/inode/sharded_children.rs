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

//! Per-name sharded child index for large or contended directories.
//!
//! Promotion follows the Astra protocol: a mutation that observes lock
//! contention with enough children migrates the single index into shards
//! once, inside the write lock, and every later reader re-checks
//! [`ShardedChildren::ready`] after taking its fallback lock, so a reader
//! that raced the promotion either sees the old single map or the finished
//! shard table — never a half-filled one.

use super::{InodeChildren, InodeView};
use fxhash::FxHasher;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

const MIN_SHARDED_CHILDREN: usize = 256;

pub(crate) struct DirectoryShard {
    pub(crate) children: RwLock<InodeChildren>,
}

pub(crate) struct ShardedChildren {
    pub(crate) ready: AtomicBool,
    pub(crate) shards: Box<[OnceLock<DirectoryShard>]>,
}

impl ShardedChildren {
    pub(crate) fn new() -> Self {
        let count = shard_count();
        let shards = (0..count).map(|_| OnceLock::new()).collect();
        Self {
            ready: AtomicBool::new(false),
            shards,
        }
    }

    /// Migrates every child of `children` into the shards. Called while the
    /// single index write lock is held, so the snapshot is complete.
    pub(crate) fn insert_children(&self, children: &mut InodeChildren) {
        let mut shard_buffers: Vec<BTreeMap<String, Box<InodeView>>> =
            (0..self.shards.len()).map(|_| BTreeMap::new()).collect();
        let owned = std::mem::replace(children, InodeChildren::new_map());
        let mut insert = |name: String, view: Box<InodeView>| {
            let index = self.shard_index(&name);
            shard_buffers[index].insert(name, view);
        };
        match owned {
            InodeChildren::List(list) => {
                for view in list {
                    insert(view.name().to_owned(), view);
                }
            }
            InodeChildren::Map(map) => {
                for (name, view) in map {
                    insert(name, view);
                }
            }
            InodeChildren::Packed(packed) => {
                for (name, view) in packed.into_entries() {
                    insert(name, view);
                }
            }
        }
        for (buffer, shard) in shard_buffers.into_iter().zip(&self.shards) {
            let container = if buffer.is_empty() {
                super::InodeChildren::new_map()
            } else {
                super::InodeChildren::Map(buffer)
            };
            let _ = shard.set(DirectoryShard {
                children: RwLock::new(container),
            });
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub(crate) fn shard(&self, name: &str) -> &DirectoryShard {
        self.shard_at(self.shard_index(name))
    }

    pub(crate) fn shard_index(&self, name: &str) -> usize {
        shard_index(name, self.shards.len())
    }

    pub(crate) fn shard_at(&self, index: usize) -> &DirectoryShard {
        self.shards[index].get_or_init(|| DirectoryShard {
            children: RwLock::new(super::InodeChildren::new_map()),
        })
    }

    pub(crate) fn read_all(&self) -> Vec<parking_lot::RwLockReadGuard<'_, InodeChildren>> {
        self.shards
            .iter()
            .enumerate()
            .map(|(index, _)| self.shard_at(index).children.read())
            .collect()
    }
}

fn shard_count() -> usize {
    static SHARD_COUNT: OnceLock<usize> = OnceLock::new();
    *SHARD_COUNT.get_or_init(|| {
        // Eight shards per core keeps per-shard packed areas small on very
        // large directories: each area grow is an O(entries) memcpy, and a
        // finer grid cuts that spike while leaving small directories
        // unchanged (they stay in the pre-shard container).
        std::thread::available_parallelism()
            .map(|count| count.get().saturating_mul(8))
            .unwrap_or(32)
            .next_power_of_two()
            .clamp(32, 256)
    })
}

pub(crate) fn min_sharded_children() -> usize {
    static THRESHOLD: OnceLock<usize> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::thread::available_parallelism()
            .map(|count| count.get().saturating_mul(32))
            .unwrap_or(MIN_SHARDED_CHILDREN)
            .clamp(MIN_SHARDED_CHILDREN, 2048)
    })
}

/// Shard selection is process-local and never persisted; the existing
/// non-cryptographic hasher keeps the routing path cheap.
pub(crate) fn shard_index(name: &str, count: usize) -> usize {
    let mut hasher = FxHasher::default();
    name.hash(&mut hasher);
    (hasher.finish() as usize) & (count - 1)
}
