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

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) const DEFAULT_IDLE_LOCKS_PER_SHARD: usize = 64;

pub(crate) type ObjectLock = Arc<RwLock<()>>;

pub(crate) struct ObjectLockPool {
    shards: Vec<RwLock<ObjectLockShard>>,
    max_idle_locks_per_shard: usize,
}

#[derive(Default)]
struct ObjectLockShard {
    locks: HashMap<u64, ObjectLock>,
}

impl ObjectLockPool {
    pub(crate) fn new(shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(ObjectLockShard::default()));
        }
        Self {
            shards,
            max_idle_locks_per_shard: DEFAULT_IDLE_LOCKS_PER_SHARD,
        }
    }

    pub(crate) fn get_or_create_lock(&self, key: u64) -> ObjectLock {
        let shard_index = self.shard_index(key);
        {
            let shard = self.shards[shard_index].read();
            if let Some(lock) = shard.locks.get(&key) {
                return lock.clone();
            }
        }

        let mut shard = self.shards[shard_index].write();
        let lock = shard
            .locks
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();

        if shard.locks.len() > self.max_idle_locks_per_shard {
            Self::cleanup_shard(&mut shard, self.max_idle_locks_per_shard);
        }

        lock
    }

    pub(crate) fn cleanup_lock(&self, key: u64) {
        let shard = &self.shards[self.shard_index(key)];
        if shard.read().locks.len() <= self.max_idle_locks_per_shard {
            return;
        }
        let mut shard = shard.write();
        if shard.locks.len() > self.max_idle_locks_per_shard
            && shard
                .locks
                .get(&key)
                .is_some_and(|lock| Arc::strong_count(lock) == 1)
        {
            shard.locks.remove(&key);
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_lock_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().locks.len())
            .sum()
    }

    pub(crate) fn cleanup_locks(&self, keys: &[u64]) {
        if keys.is_empty() {
            return;
        }

        let mut shard_indexes = keys
            .iter()
            .map(|key| self.shard_index(*key))
            .collect::<Vec<_>>();
        shard_indexes.sort_unstable();
        shard_indexes.dedup();

        for shard_index in shard_indexes {
            {
                let shard = self.shards[shard_index].read();
                if shard.locks.len() <= self.max_idle_locks_per_shard {
                    continue;
                }
            }
            let mut shard = self.shards[shard_index].write();
            Self::cleanup_shard(&mut shard, self.max_idle_locks_per_shard);
        }
    }

    fn shard_index(&self, key: u64) -> usize {
        key as usize % self.shards.len()
    }

    fn cleanup_shard(shard: &mut ObjectLockShard, max_idle_locks: usize) {
        if shard.locks.len() <= max_idle_locks {
            return;
        }

        let remove_count = shard.locks.len().saturating_sub(max_idle_locks);
        let removable = shard
            .locks
            .iter()
            .filter_map(|(key, lock)| {
                if Arc::strong_count(lock) == 1 {
                    Some(*key)
                } else {
                    None
                }
            })
            .take(remove_count)
            .collect::<Vec<_>>();

        for key in removable {
            shard.locks.remove(&key);
        }
    }
}
