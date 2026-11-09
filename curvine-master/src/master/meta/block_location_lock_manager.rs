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
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

const DEFAULT_BLOCK_LOCK_SHARDS: usize = 4096;
const DEFAULT_WORKER_LOCK_SHARDS: usize = 1024;

/// Sharded locks for block-location and Worker metadata updates.
///
/// Lock-order contract: every acquisition sorts by stripe index and takes
/// each stripe once in the strongest mode. Cross-pool callers must acquire
/// worker stripes before block stripes (`write_workers_blocks`), and callers
/// holding inode locks must acquire this manager after them, matching the
/// global inode → location order.
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
    /// Best-effort lock-acquire duration in microseconds (design doc §11.2);
    /// metric registration failure is ignored on the hot path.
    fn record_acquire_us(mode: &'static str, started: std::time::Instant) {
        use std::sync::OnceLock;
        static READ: OnceLock<curvine_metrics::Histogram> = OnceLock::new();
        static WRITE: OnceLock<curvine_metrics::Histogram> = OnceLock::new();
        let cache = match mode {
            "read" => &READ,
            "write" => &WRITE,
            _ => unreachable!("unknown block lock mode"),
        };
        let histogram = match cache.get() {
            Some(histogram) => histogram,
            None => match crate::master::Master::get_metrics() {
                Ok(metrics) => {
                    cache.get_or_init(|| metrics.block_lock_acquire_us.with_label_values(&[mode]))
                }
                Err(_) => return,
            },
        };
        histogram.observe(started.elapsed().as_micros() as f64);
    }

    pub fn new(block_shards: usize, worker_shards: usize) -> Self {
        Self {
            block_locks: ObjectLockPool::new(block_shards),
            worker_locks: ObjectLockPool::new(worker_shards),
        }
    }

    pub fn read_blocks(&self, block_ids: &[i64]) -> BlockLocationLockSet<'_> {
        let started = std::time::Instant::now();
        let keys = sorted_unique_keys(block_ids.iter().map(|block_id| *block_id as u64));
        let guards = acquire_stripes(&self.block_locks, &keys, BlockMode::Read);
        Self::record_acquire_us("read", started);
        BlockLocationLockSet { guards }
    }

    pub fn write_blocks(&self, block_ids: &[i64]) -> BlockLocationLockSet<'_> {
        let started = std::time::Instant::now();
        let keys = sorted_unique_keys(block_ids.iter().map(|block_id| *block_id as u64));
        let guards = acquire_stripes(&self.block_locks, &keys, BlockMode::Write);
        Self::record_acquire_us("write", started);
        BlockLocationLockSet { guards }
    }

    pub fn write_worker(&self, worker_id: u32) -> BlockLocationLockSet<'_> {
        let started = std::time::Instant::now();
        let keys = vec![worker_id as u64];
        let guards = acquire_stripes(&self.worker_locks, &keys, BlockMode::Write);
        Self::record_acquire_us("write", started);
        BlockLocationLockSet { guards }
    }

    pub fn write_worker_blocks(
        &self,
        worker_id: u32,
        block_ids: &[i64],
    ) -> BlockLocationLockSet<'_> {
        self.write_workers_blocks(&[worker_id], block_ids)
    }

    pub fn write_workers_blocks(
        &self,
        worker_ids: &[u32],
        block_ids: &[i64],
    ) -> BlockLocationLockSet<'_> {
        let started = std::time::Instant::now();
        let worker_keys = sorted_unique_keys(worker_ids.iter().map(|worker_id| *worker_id as u64));
        let block_keys = sorted_unique_keys(block_ids.iter().map(|block_id| *block_id as u64));
        // Lock order: worker stripes before block stripes, each pool ascending
        // by stripe index.
        let worker_guards = acquire_stripes(&self.worker_locks, &worker_keys, BlockMode::Write);
        let block_guards = acquire_stripes(&self.block_locks, &block_keys, BlockMode::Write);
        Self::record_acquire_us("write", started);

        BlockLocationLockSet {
            guards: worker_guards.into_iter().chain(block_guards).collect(),
        }
    }
}

#[derive(Clone, Copy)]
enum BlockMode {
    Read,
    Write,
}

/// Group keys by stripe and acquire each stripe once in the strongest
/// requested mode, ascending by stripe index. Dedup happens on the stripe
/// index, not the key pair: two keys sharing a stripe must yield one guard,
/// otherwise the same thread would take one RwLock twice and self-deadlock.
fn acquire_stripes<'a>(
    pool: &'a ObjectLockPool,
    keys: &[u64],
    mode: BlockMode,
) -> Vec<BlockLocationGuard<'a>> {
    let mut stripes = keys.iter().map(|key| pool.index(*key)).collect::<Vec<_>>();
    stripes.sort_unstable();
    stripes.dedup();

    stripes
        .into_iter()
        .map(|stripe| match mode {
            BlockMode::Read => BlockLocationGuard::Read {
                _guard: pool.lock_shared_at(stripe),
            },
            BlockMode::Write => BlockLocationGuard::Write {
                _guard: pool.lock_exclusive_at(stripe),
            },
        })
        .collect()
}

fn sorted_unique_keys(keys: impl Iterator<Item = u64>) -> Vec<u64> {
    let mut keys = keys.collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

pub struct BlockLocationLockSet<'a> {
    guards: Vec<BlockLocationGuard<'a>>,
}

enum BlockLocationGuard<'a> {
    Read { _guard: RwLockReadGuard<'a, ()> },
    Write { _guard: RwLockWriteGuard<'a, ()> },
}

#[cfg(test)]
#[path = "tests/block_location_lock_manager_test.rs"]
mod tests;
