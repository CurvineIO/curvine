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

use crate::p2p::sha256_bytes;
use crate::p2p::ChunkId;
use bytes::Bytes;
use curvine_common::conf::ClientP2pConf;
use orpc::common::LocalTime;
use orpc::sync::FastDashMap;
use std::cmp::min;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct CachedChunk {
    pub data: Bytes,
    pub mtime: i64,
    pub checksum: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheGetResultTag {
    Hit,
    Miss,
    MtimeMismatch,
    ChecksumFailure,
    Corruption,
}

#[derive(Debug, Clone)]
pub struct CacheGetResult {
    pub tag: CacheGetResultTag,
    pub chunk: Option<CachedChunk>,
}

#[derive(Debug, Clone, Default)]
pub struct CacheSnapshot {
    pub usage_bytes: u64,
    pub capacity_bytes: u64,
    pub usage_ratio: f64,
    pub cached_chunks_count: usize,
    pub expired_chunks: u64,
    pub invalidations: u64,
    pub mtime_mismatches: u64,
    pub checksum_failures: u64,
    pub corruption_count: u64,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    path: PathBuf,
    len: u64,
    mtime: i64,
    checksum: Bytes,
    expire_at_ms: i64,
    last_access_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EvictionToken {
    last_access_ms: i64,
    file_id: i64,
    version_epoch: i64,
    block_id: i64,
    off: i64,
}

impl EvictionToken {
    fn new(chunk_id: ChunkId, last_access_ms: i64) -> Self {
        Self {
            last_access_ms,
            file_id: chunk_id.file_id,
            version_epoch: chunk_id.version_epoch,
            block_id: chunk_id.block_id,
            off: chunk_id.off,
        }
    }

    fn chunk_id(self) -> ChunkId {
        ChunkId::with_version(self.file_id, self.version_epoch, self.block_id, self.off)
    }
}

pub struct CacheManager {
    root_dir: PathBuf,
    capacity_bytes: u64,
    ttl_ms: i64,
    shard_count: usize,
    enable_checksum: bool,
    usage_bytes: AtomicU64,
    expired_chunks: AtomicU64,
    invalidations: AtomicU64,
    mtime_mismatches: AtomicU64,
    checksum_failures: AtomicU64,
    corruption_count: AtomicU64,
    entries: FastDashMap<ChunkId, CacheEntry>,
    eviction_queue: Mutex<BinaryHeap<Reverse<EvictionToken>>>,
}

impl CacheManager {
    pub fn new(conf: &ClientP2pConf) -> Self {
        let root_dir = PathBuf::from(conf.cache_dir.clone());
        let _ = fs::create_dir_all(&root_dir);
        let shard_count = conf.cache_shards.max(1);
        for idx in 0..shard_count {
            let _ = fs::create_dir_all(root_dir.join(format!("{:03}", idx)));
        }

        Self {
            root_dir,
            capacity_bytes: conf.cache_capacity.max(1),
            ttl_ms: conf.cache_ttl.as_millis().max(1) as i64,
            shard_count,
            enable_checksum: conf.enable_checksum,
            usage_bytes: AtomicU64::new(0),
            expired_chunks: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            mtime_mismatches: AtomicU64::new(0),
            checksum_failures: AtomicU64::new(0),
            corruption_count: AtomicU64::new(0),
            entries: FastDashMap::default(),
            eviction_queue: Mutex::new(BinaryHeap::new()),
        }
    }

    pub fn put(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> bool {
        if data.is_empty() {
            return false;
        }

        let now_ms = LocalTime::mills() as i64;
        let path = self.chunk_path(chunk_id);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if fs::write(&path, data.as_ref()).is_err() {
            self.corruption_count.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        if let Some((_, old)) = self.entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(old.len, Ordering::Relaxed);
        }

        let checksum = self.calc_checksum(data.as_ref());
        let len = data.len() as u64;
        self.entries.insert(
            chunk_id,
            CacheEntry {
                path,
                len,
                mtime,
                checksum,
                expire_at_ms: now_ms + self.ttl_ms,
                last_access_ms: now_ms,
            },
        );
        self.usage_bytes.fetch_add(len, Ordering::Relaxed);
        self.record_access(chunk_id, now_ms);
        self.evict_until_fit();
        true
    }

    pub fn get(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> CacheGetResult {
        let now_ms = LocalTime::mills() as i64;
        let (path, stored_len, mtime, entry_checksum) = match self.entries.get_mut(&chunk_id) {
            Some(mut entry) => {
                if entry.expire_at_ms <= now_ms {
                    drop(entry);
                    if self.invalidate(chunk_id) {
                        self.expired_chunks.fetch_add(1, Ordering::Relaxed);
                    }
                    return CacheGetResult {
                        tag: CacheGetResultTag::Miss,
                        chunk: None,
                    };
                }
                if expected_mtime.is_some_and(|expected| expected > 0 && entry.mtime != expected) {
                    self.mtime_mismatches.fetch_add(1, Ordering::Relaxed);
                    drop(entry);
                    self.invalidate(chunk_id);
                    return CacheGetResult {
                        tag: CacheGetResultTag::MtimeMismatch,
                        chunk: None,
                    };
                }
                entry.last_access_ms = now_ms;
                (
                    entry.path.clone(),
                    entry.len,
                    entry.mtime,
                    entry.checksum.clone(),
                )
            }
            None => {
                return CacheGetResult {
                    tag: CacheGetResultTag::Miss,
                    chunk: None,
                };
            }
        };

        let bytes = match fs::read(&path) {
            Ok(v) => v,
            Err(_) => {
                self.corruption_count.fetch_add(1, Ordering::Relaxed);
                self.invalidate(chunk_id);
                return CacheGetResult {
                    tag: CacheGetResultTag::Corruption,
                    chunk: None,
                };
            }
        };
        self.record_access(chunk_id, now_ms);

        if bytes.len() as u64 != stored_len {
            self.corruption_count.fetch_add(1, Ordering::Relaxed);
            self.invalidate(chunk_id);
            return CacheGetResult {
                tag: CacheGetResultTag::Corruption,
                chunk: None,
            };
        }

        if self.enable_checksum {
            let checksum = self.calc_checksum(&bytes);
            if checksum != entry_checksum {
                self.checksum_failures.fetch_add(1, Ordering::Relaxed);
                self.corruption_count.fetch_add(1, Ordering::Relaxed);
                self.invalidate(chunk_id);
                return CacheGetResult {
                    tag: CacheGetResultTag::ChecksumFailure,
                    chunk: None,
                };
            }
        }

        let len = min(bytes.len(), max_len.max(1));
        let data = Bytes::from(bytes).slice(0..len);
        let checksum = if self.enable_checksum && len < stored_len as usize {
            self.calc_checksum(data.as_ref())
        } else {
            entry_checksum
        };
        let chunk = CachedChunk {
            data,
            mtime,
            checksum,
        };
        CacheGetResult {
            tag: CacheGetResultTag::Hit,
            chunk: Some(chunk),
        }
    }

    pub fn cleanup_expired(&self) {
        let now_ms = LocalTime::mills() as i64;
        let expired: Vec<ChunkId> = self
            .entries
            .iter()
            .filter_map(|v| (v.value().expire_at_ms <= now_ms).then_some(*v.key()))
            .collect();
        if !expired.is_empty() {
            let mut newly_expired = 0u64;
            for chunk_id in expired {
                if self.invalidate(chunk_id) {
                    newly_expired += 1;
                }
            }
            if newly_expired > 0 {
                self.expired_chunks
                    .fetch_add(newly_expired, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> CacheSnapshot {
        let usage_bytes = self.usage_bytes.load(Ordering::Relaxed);
        let capacity_bytes = self.capacity_bytes;
        let usage_ratio = if capacity_bytes == 0 {
            0.0
        } else {
            usage_bytes as f64 * 100.0 / capacity_bytes as f64
        };

        CacheSnapshot {
            usage_bytes,
            capacity_bytes,
            usage_ratio,
            cached_chunks_count: self.entries.len(),
            expired_chunks: self.expired_chunks.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            mtime_mismatches: self.mtime_mismatches.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
            corruption_count: self.corruption_count.load(Ordering::Relaxed),
        }
    }

    fn invalidate(&self, chunk_id: ChunkId) -> bool {
        if let Some((_, entry)) = self.entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(entry.len, Ordering::Relaxed);
            self.invalidations.fetch_add(1, Ordering::Relaxed);
            let _ = fs::remove_file(entry.path);
            return true;
        }
        false
    }

    fn evict_until_fit(&self) {
        if self.usage_bytes.load(Ordering::Relaxed) <= self.capacity_bytes {
            return;
        }
        self.compact_eviction_queue_if_needed();
        while self.usage_bytes.load(Ordering::Relaxed) > self.capacity_bytes {
            let Some(token) = self.pop_oldest_access() else {
                break;
            };
            let chunk_id = token.chunk_id();
            let Some(entry) = self.entries.get(&chunk_id) else {
                continue;
            };
            if entry.last_access_ms != token.last_access_ms {
                continue;
            }
            drop(entry);
            self.invalidate(chunk_id);
        }
    }

    fn record_access(&self, chunk_id: ChunkId, last_access_ms: i64) {
        let mut queue = self
            .eviction_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queue.push(Reverse(EvictionToken::new(chunk_id, last_access_ms)));
    }

    fn pop_oldest_access(&self) -> Option<EvictionToken> {
        let mut queue = self
            .eviction_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queue.pop().map(|Reverse(token)| token)
    }

    fn compact_eviction_queue_if_needed(&self) {
        let entries_len = self.entries.len();
        if entries_len == 0 {
            return;
        }
        let queue_len = self
            .eviction_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len();
        if queue_len <= entries_len.saturating_mul(8) {
            return;
        }
        let mut rebuilt = BinaryHeap::with_capacity(entries_len);
        for entry in self.entries.iter() {
            rebuilt.push(Reverse(EvictionToken::new(
                *entry.key(),
                entry.value().last_access_ms,
            )));
        }
        let mut queue = self
            .eviction_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *queue = rebuilt;
    }

    fn calc_checksum(&self, data: &[u8]) -> Bytes {
        if !self.enable_checksum {
            return Bytes::new();
        }
        sha256_bytes(data)
    }

    fn chunk_path(&self, chunk_id: ChunkId) -> PathBuf {
        let hash = (chunk_id.file_id as u64)
            ^ (chunk_id.version_epoch as u64).rotate_left(5)
            ^ (chunk_id.block_id as u64).rotate_left(11)
            ^ (chunk_id.off as u64).rotate_left(17)
            ^ ((chunk_id.off as u64) << 7);
        let shard = (hash % self.shard_count as u64) as usize;
        self.root_dir.join(format!("{:03}", shard)).join(format!(
            "{}_{}_{}_{}.bin",
            chunk_id.file_id, chunk_id.version_epoch, chunk_id.block_id, chunk_id.off
        ))
    }
}
