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

use super::object_lock_pool::ObjectLockPool;
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard};

const DEFAULT_BLOCK_LOCK_SHARDS: usize = 4096;
const DEFAULT_WORKER_LOCK_SHARDS: usize = 1024;

/// Sharded locks for block-location and Worker metadata updates.
pub struct BlockLocationLockManager {
    block_locks: ObjectLockPool,
    worker_locks: ObjectLockPool,
}

impl Default for BlockLocationLockManager {
    fn default() -> Self {
        Self::new(DEFAULT_BLOCK_LOCK_SHARDS, DEFAULT_WORKER_LOCK_SHARDS)
    }
}

impl BlockLocationLockManager {
    pub fn new(block_shards: usize, worker_shards: usize) -> Self {
        Self {
            block_locks: ObjectLockPool::new(block_shards),
            worker_locks: ObjectLockPool::new(worker_shards),
        }
    }

    pub fn read_blocks<'a>(&'a self, block_ids: &[i64]) -> BlockLocationLockSet<'a> {
        let block_keys = sorted_unique_keys(block_ids.iter().map(|block_id| *block_id as u64));
        let guards = block_keys
            .iter()
            .map(|key| BlockLocationGuard::Read {
                _guard: self.block_locks.get_or_create_lock(*key).read_arc(),
            })
            .collect();

        BlockLocationLockSet {
            manager: self,
            guards,
            block_keys,
            worker_keys: Vec::new(),
        }
    }

    pub fn write_blocks<'a>(&'a self, block_ids: &[i64]) -> BlockLocationLockSet<'a> {
        let block_keys = sorted_unique_keys(block_ids.iter().map(|block_id| *block_id as u64));
        let guards = block_keys
            .iter()
            .map(|key| BlockLocationGuard::Write {
                _guard: self.block_locks.get_or_create_lock(*key).write_arc(),
            })
            .collect();

        BlockLocationLockSet {
            manager: self,
            guards,
            block_keys,
            worker_keys: Vec::new(),
        }
    }

    pub fn write_worker<'a>(&'a self, worker_id: u32) -> BlockLocationLockSet<'a> {
        let worker_keys = vec![worker_id as u64];
        let guards = vec![BlockLocationGuard::Write {
            _guard: self
                .worker_locks
                .get_or_create_lock(worker_id as u64)
                .write_arc(),
        }];

        BlockLocationLockSet {
            manager: self,
            guards,
            block_keys: Vec::new(),
            worker_keys,
        }
    }

    pub fn write_worker_blocks<'a>(
        &'a self,
        worker_id: u32,
        block_ids: &[i64],
    ) -> BlockLocationLockSet<'a> {
        self.write_workers_blocks(&[worker_id], block_ids)
    }

    pub fn write_workers_blocks<'a>(
        &'a self,
        worker_ids: &[u32],
        block_ids: &[i64],
    ) -> BlockLocationLockSet<'a> {
        let worker_keys = sorted_unique_keys(worker_ids.iter().map(|worker_id| *worker_id as u64));
        let block_keys = sorted_unique_keys(block_ids.iter().map(|block_id| *block_id as u64));
        let mut guards = Vec::with_capacity(block_keys.len() + worker_keys.len());

        for key in &worker_keys {
            guards.push(BlockLocationGuard::Write {
                _guard: self.worker_locks.get_or_create_lock(*key).write_arc(),
            });
        }

        for key in &block_keys {
            guards.push(BlockLocationGuard::Write {
                _guard: self.block_locks.get_or_create_lock(*key).write_arc(),
            });
        }

        BlockLocationLockSet {
            manager: self,
            guards,
            block_keys,
            worker_keys,
        }
    }
}

fn sorted_unique_keys(keys: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut keys = keys.collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

pub struct BlockLocationLockSet<'a> {
    manager: &'a BlockLocationLockManager,
    guards: Vec<BlockLocationGuard>,
    block_keys: Vec<u64>,
    worker_keys: Vec<u64>,
}

enum BlockLocationGuard {
    Read {
        _guard: ArcRwLockReadGuard<parking_lot::RawRwLock, ()>,
    },
    Write {
        _guard: ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>,
    },
}

impl Drop for BlockLocationLockSet<'_> {
    fn drop(&mut self) {
        self.guards.clear();
        self.manager.block_locks.cleanup_locks(&self.block_keys);
        self.manager.worker_locks.cleanup_locks(&self.worker_keys);
    }
}
