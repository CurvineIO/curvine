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

use std::num::NonZeroUsize;
use std::sync::Mutex;

use crate::master::quota::eviction::lfu;
use crate::master::quota::eviction::types::EvictionConf;

const DEFAULT_EVICTION_CACHE_CAPACITY: usize = 5_000_000;

fn eviction_cache_capacity(configured_capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(configured_capacity).unwrap_or_else(|| {
        NonZeroUsize::new(DEFAULT_EVICTION_CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN)
    })
}

use std::collections::HashSet;

pub trait Evictor: Send + Sync {
    fn on_access(&self, inode_id: i64);
    fn select_victims(&self, limit: usize) -> Vec<i64>;
    /// Coldest candidates first, skipping ids already examined in this
    /// eviction round. `select_victims` alone cannot make progress: it always
    /// returns the same prefix until those entries are removed, so a trigger
    /// that finds nothing expirable would stop after one page.
    fn select_victims_excluding(&self, limit: usize, skip: &HashSet<i64>) -> Vec<i64>;
    fn remove_victims(&self, inode_ids: &[i64]);
    fn cache_size(&self) -> usize;
}

pub struct LRUEvictor {
    caches: Mutex<lru::LruCache<i64, ()>>,
    conf: EvictionConf,
}

impl LRUEvictor {
    pub fn new(conf: EvictionConf) -> Self {
        let capacity = eviction_cache_capacity(conf.capacity);

        Self {
            caches: Mutex::new(lru::LruCache::new(capacity)),
            conf,
        }
    }

    fn peek_victims(&self, limit: usize) -> Vec<i64> {
        self.peek_victims_excluding(limit, &HashSet::new())
    }

    fn peek_victims_excluding(&self, limit: usize, skip: &HashSet<i64>) -> Vec<i64> {
        if let Ok(caches) = self.caches.lock() {
            caches
                .iter()
                .rev()
                .map(|(&inode_id, _)| inode_id)
                .filter(|inode_id| !skip.contains(inode_id))
                .take(limit)
                .collect()
        } else {
            Vec::new()
        }
    }

    fn remove_victims(&self, inode_ids: &[i64]) {
        if let Ok(mut caches) = self.caches.lock() {
            for &inode_id in inode_ids {
                caches.pop(&inode_id);
            }
        }
    }
}

impl Evictor for LRUEvictor {
    fn on_access(&self, inode_id: i64) {
        if !self.conf.enable_quota_eviction {
            return;
        }

        if let Ok(mut caches) = self.caches.lock() {
            caches.put(inode_id, ());
        }
    }

    fn select_victims(&self, limit: usize) -> Vec<i64> {
        self.peek_victims(limit)
    }

    fn select_victims_excluding(&self, limit: usize, skip: &HashSet<i64>) -> Vec<i64> {
        self.peek_victims_excluding(limit, skip)
    }

    fn remove_victims(&self, inode_ids: &[i64]) {
        self.remove_victims(inode_ids)
    }

    fn cache_size(&self) -> usize {
        if let Ok(caches) = self.caches.lock() {
            caches.len()
        } else {
            0
        }
    }
}

pub struct LFUEvictor {
    caches: Mutex<lfu::LFUCache<i64>>,
    conf: EvictionConf,
}

impl LFUEvictor {
    pub fn new(conf: EvictionConf) -> Self {
        let capacity = eviction_cache_capacity(conf.capacity);

        Self {
            caches: Mutex::new(lfu::LFUCache::new(capacity.get())),
            conf,
        }
    }

    fn peek_victims(&self, limit: usize) -> Vec<i64> {
        self.peek_victims_excluding(limit, &HashSet::new())
    }

    fn peek_victims_excluding(&self, limit: usize, skip: &HashSet<i64>) -> Vec<i64> {
        if let Ok(caches) = self.caches.lock() {
            caches
                .iter()
                .filter(|inode_id| !skip.contains(inode_id))
                .take(limit)
                .copied()
                .collect()
        } else {
            Vec::new()
        }
    }

    fn remove_victims(&self, inode_ids: &[i64]) {
        if let Ok(mut caches) = self.caches.lock() {
            for &inode_id in inode_ids {
                caches.remove(inode_id);
            }
        }
    }
}

impl Evictor for LFUEvictor {
    fn on_access(&self, inode_id: i64) {
        if !self.conf.enable_quota_eviction {
            return;
        }

        if let Ok(mut caches) = self.caches.lock() {
            caches.put(inode_id);
        }
    }

    fn select_victims(&self, limit: usize) -> Vec<i64> {
        self.peek_victims(limit)
    }

    fn select_victims_excluding(&self, limit: usize, skip: &HashSet<i64>) -> Vec<i64> {
        self.peek_victims_excluding(limit, skip)
    }

    fn remove_victims(&self, inode_ids: &[i64]) {
        self.remove_victims(inode_ids)
    }

    fn cache_size(&self) -> usize {
        if let Ok(caches) = self.caches.lock() {
            caches.len()
        } else {
            0
        }
    }
}
