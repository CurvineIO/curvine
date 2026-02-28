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
                    self.expired_chunks.fetch_add(1, Ordering::Relaxed);
                    drop(entry);
                    self.invalidate(chunk_id);
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
            self.expired_chunks
                .fetch_add(expired.len() as u64, Ordering::Relaxed);
            for chunk_id in expired {
                self.invalidate(chunk_id);
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

    fn invalidate(&self, chunk_id: ChunkId) {
        if let Some((_, entry)) = self.entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(entry.len, Ordering::Relaxed);
            self.invalidations.fetch_add(1, Ordering::Relaxed);
            let _ = fs::remove_file(entry.path);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::conf::ClientP2pConf;
    use std::fs;
    use std::thread::sleep;
    use std::time::Duration;

    fn test_conf(dir: String) -> ClientP2pConf {
        let mut conf = ClientP2pConf {
            cache_dir: dir,
            cache_capacity_str: "32MB".to_string(),
            cache_ttl_str: "5m".to_string(),
            provider_ttl_str: "10m".to_string(),
            provider_publish_interval_str: "30s".to_string(),
            enable_checksum: true,
            ..ClientP2pConf::default()
        };
        conf.cache_ttl = Duration::from_secs(300);
        conf.cache_capacity = 32 * 1024 * 1024;
        conf.provider_ttl = Duration::from_secs(600);
        conf.provider_publish_interval = Duration::from_secs(30);
        conf
    }

    fn temp_cache_dir(case: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "curvine-p2p-cache-manager-{}-{}",
            case,
            orpc::common::Utils::uuid()
        ));
        dir.to_string_lossy().to_string()
    }

    #[test]
    fn tracks_mtime_mismatch_and_invalidation() {
        let dir = temp_cache_dir("mtime");
        let conf = test_conf(dir.clone());
        let manager = CacheManager::new(&conf);
        let chunk = ChunkId::new(1, 0);
        assert!(manager.put(chunk, Bytes::from_static(b"hello"), 100));

        let res = manager.get(chunk, 64, Some(101));
        assert_eq!(res.tag, CacheGetResultTag::MtimeMismatch);
        let snap = manager.snapshot();
        assert_eq!(snap.mtime_mismatches, 1);
        assert_eq!(snap.invalidations, 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tracks_checksum_failure_and_corruption() {
        let dir = temp_cache_dir("checksum");
        let conf = test_conf(dir.clone());
        let manager = CacheManager::new(&conf);
        let chunk = ChunkId::new(2, 128);
        assert!(manager.put(chunk, Bytes::from_static(b"original"), 200));

        let path = manager.chunk_path(chunk);
        fs::write(path, b"tampered").unwrap();
        let res = manager.get(chunk, 64, Some(200));
        assert_eq!(res.tag, CacheGetResultTag::ChecksumFailure);
        let snap = manager.snapshot();
        assert_eq!(snap.checksum_failures, 1);
        assert_eq!(snap.corruption_count, 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn keeps_multiple_versions_for_same_offset() {
        let dir = temp_cache_dir("versions");
        let conf = test_conf(dir.clone());
        let manager = CacheManager::new(&conf);
        let chunk_v1 = ChunkId::with_version(10, 100, 3, 0);
        let chunk_v2 = ChunkId::with_version(10, 101, 3, 0);

        assert!(manager.put(chunk_v1, Bytes::from_static(b"v1"), 100));
        assert!(manager.put(chunk_v2, Bytes::from_static(b"v2"), 101));

        let data_v1 = manager.get(chunk_v1, 16, Some(100)).chunk.unwrap().data;
        let data_v2 = manager.get(chunk_v2, 16, Some(101)).chunk.unwrap().data;

        assert_eq!(data_v1, Bytes::from_static(b"v1"));
        assert_eq!(data_v2, Bytes::from_static(b"v2"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn truncated_read_returns_checksum_for_returned_slice() {
        let dir = temp_cache_dir("truncate-checksum");
        let conf = test_conf(dir.clone());
        let manager = CacheManager::new(&conf);
        let chunk = ChunkId::with_version(20, 1, 4, 0);
        let payload = Bytes::from_static(b"abcdefghij");
        assert!(manager.put(chunk, payload, 9));

        let res = manager.get(chunk, 4, Some(9));
        assert_eq!(res.tag, CacheGetResultTag::Hit);
        let chunk = res.chunk.unwrap();
        assert_eq!(chunk.data, Bytes::from_static(b"abcd"));
        assert_eq!(chunk.checksum, manager.calc_checksum(chunk.data.as_ref()));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn evict_prefers_older_access_entries() {
        let dir = temp_cache_dir("evict-lru");
        let mut conf = test_conf(dir.clone());
        conf.cache_capacity = 9;
        conf.enable_checksum = false;
        let manager = CacheManager::new(&conf);
        let chunk_a = ChunkId::with_version(30, 1, 1, 0);
        let chunk_b = ChunkId::with_version(30, 1, 2, 0);
        let chunk_c = ChunkId::with_version(30, 1, 3, 0);

        assert!(manager.put(chunk_a, Bytes::from_static(b"aaaa"), 1));
        assert!(manager.put(chunk_b, Bytes::from_static(b"bbbb"), 1));
        sleep(Duration::from_millis(2));
        assert_eq!(
            manager.get(chunk_a, 16, Some(1)).chunk.unwrap().data,
            Bytes::from_static(b"aaaa")
        );
        sleep(Duration::from_millis(2));
        assert!(manager.put(chunk_c, Bytes::from_static(b"cccc"), 1));

        assert!(manager.get(chunk_a, 16, Some(1)).chunk.is_some());
        assert!(manager.get(chunk_b, 16, Some(1)).chunk.is_none());
        assert!(manager.get(chunk_c, 16, Some(1)).chunk.is_some());
        let _ = fs::remove_dir_all(dir);
    }
}
