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

use std::sync::Mutex;

use crate::master::quota::eviction::types::EvictionConf;

pub trait Evictor: Send + Sync {
    fn on_access(&self, inode_id: i64);
    fn select_victims(&self, limit: usize) -> Vec<i64>;
    fn remove_victims(&self, inode_ids: &[i64]);
}

pub struct LRUEvictor {
    caches: Mutex<lru::LruCache<i64, ()>>,
    conf: EvictionConf,
}

impl LRUEvictor {
    pub fn new(conf: EvictionConf) -> Self {
        Self {
            caches: Mutex::new(lru::LruCache::unbounded()),
            conf,
        }
    }

    fn peek_victims(&self, limit: usize) -> Vec<i64> {
        if let Ok(caches) = self.caches.lock() {
            caches
                .iter()
                .rev()
                .take(limit)
                .map(|(&inode_id, _)| inode_id)
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

    fn remove_victims(&self, inode_ids: &[i64]) {
        self.remove_victims(inode_ids)
    }
}
