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

use crate::p2p::ChunkId;
use bytes::Bytes;
use curvine_common::conf::ClientP2pConf;
use orpc::common::LocalTime;
use orpc::sync::FastDashMap;
use ring::digest;
use std::cmp::min;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{IoSlice, Read, Seek, SeekFrom, Write};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePutResultTag {
    Stored,
    Refreshed,
    Failed,
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

#[derive(Debug, Clone)]
struct FetchedCacheEntry {
    off: u64,
    len: u64,
    mtime: i64,
    checksum: Bytes,
    expire_at_ms: i64,
    last_access_ms: i64,
}

struct FetchedLogWriterState {
    file: fs::File,
    next_off: u64,
}

#[derive(Debug, Clone)]
pub struct FetchedCacheBatchItem {
    pub chunk_id: ChunkId,
    pub data: Bytes,
    pub mtime: i64,
    pub checksum: Bytes,
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
    fetched_log_path: PathBuf,
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
    fetched_entries: FastDashMap<ChunkId, FetchedCacheEntry>,
    eviction_queue: Mutex<BinaryHeap<Reverse<EvictionToken>>>,
    fetched_log_writer: Mutex<Option<FetchedLogWriterState>>,
    fetched_log_reader: Mutex<Option<fs::File>>,
}

impl CacheManager {
    pub fn new(conf: &ClientP2pConf) -> Self {
        let root_dir = PathBuf::from(conf.cache_dir.clone());
        let _ = fs::create_dir_all(&root_dir);
        let fetched_log_path = root_dir.join("fetched_chunks.log");
        let shard_count = conf.cache_shards.max(1);
        for idx in 0..shard_count {
            let _ = fs::create_dir_all(root_dir.join(format!("{:03}", idx)));
        }

        Self {
            root_dir,
            fetched_log_path,
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
            fetched_entries: FastDashMap::default(),
            eviction_queue: Mutex::new(BinaryHeap::new()),
            fetched_log_writer: Mutex::new(None),
            fetched_log_reader: Mutex::new(None),
        }
    }

    pub fn put(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> bool {
        !matches!(
            self.put_with_result(chunk_id, data, mtime),
            CachePutResultTag::Failed
        )
    }

    pub fn put_with_result(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> CachePutResultTag {
        self.put_with_result_and_checksum(chunk_id, data, mtime, None)
    }

    pub fn put_with_result_and_checksum(
        &self,
        chunk_id: ChunkId,
        data: Bytes,
        mtime: i64,
        checksum_hint: Option<Bytes>,
    ) -> CachePutResultTag {
        if data.is_empty() {
            return CachePutResultTag::Failed;
        }

        let checksum = if self.enable_checksum {
            checksum_hint
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| self.calc_checksum(data.as_ref()))
        } else {
            Bytes::new()
        };
        let now_ms = LocalTime::mills() as i64;
        if let Some(mut entry) = self.entries.get_mut(&chunk_id) {
            if entry.mtime == mtime
                && entry.len == data.len() as u64
                && entry.expire_at_ms > now_ms
                && (!self.enable_checksum || entry.checksum == checksum)
            {
                entry.last_access_ms = now_ms;
                entry.expire_at_ms = now_ms + self.ttl_ms;
                drop(entry);
                self.record_access(chunk_id, now_ms);
                return CachePutResultTag::Refreshed;
            }
        }

        let path = self.chunk_path(chunk_id);
        if fs::write(&path, data.as_ref()).is_err() {
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            if fs::write(&path, data.as_ref()).is_err() {
                self.corruption_count.fetch_add(1, Ordering::Relaxed);
                return CachePutResultTag::Failed;
            }
        }

        if let Some((_, old)) = self.entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(old.len, Ordering::Relaxed);
        }
        if let Some((_, old)) = self.fetched_entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(old.len, Ordering::Relaxed);
        }

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
        CachePutResultTag::Stored
    }

    pub fn put_fetched_with_result_and_checksum(
        &self,
        chunk_id: ChunkId,
        data: Bytes,
        mtime: i64,
        checksum_hint: Option<Bytes>,
    ) -> CachePutResultTag {
        let checksum = if self.enable_checksum {
            checksum_hint.unwrap_or_default()
        } else {
            Bytes::new()
        };
        self.put_fetched_batch_with_checksum(vec![FetchedCacheBatchItem {
            chunk_id,
            data,
            mtime,
            checksum,
        }])
        .into_iter()
        .next()
        .map(|(_, tag)| tag)
        .unwrap_or(CachePutResultTag::Failed)
    }

    pub fn put_fetched_batch_with_checksum(
        &self,
        batch: Vec<FetchedCacheBatchItem>,
    ) -> Vec<(ChunkId, CachePutResultTag)> {
        if batch.is_empty() {
            return Vec::new();
        }
        let now_ms = LocalTime::mills() as i64;
        let mut results = Vec::with_capacity(batch.len());
        let mut to_write = Vec::with_capacity(batch.len());

        for mut item in batch {
            if item.data.is_empty() {
                results.push((item.chunk_id, CachePutResultTag::Failed));
                continue;
            }
            if self.enable_checksum {
                if item.checksum.is_empty() {
                    item.checksum = self.calc_checksum(item.data.as_ref());
                }
            } else {
                item.checksum = Bytes::new();
            }
            if let Some(mut entry) = self.fetched_entries.get_mut(&item.chunk_id) {
                if entry.mtime == item.mtime
                    && entry.len == item.data.len() as u64
                    && entry.expire_at_ms > now_ms
                    && (!self.enable_checksum || entry.checksum == item.checksum)
                {
                    entry.last_access_ms = now_ms;
                    entry.expire_at_ms = now_ms + self.ttl_ms;
                    drop(entry);
                    self.record_access(item.chunk_id, now_ms);
                    results.push((item.chunk_id, CachePutResultTag::Refreshed));
                    continue;
                }
            }
            to_write.push(item);
        }

        if to_write.is_empty() {
            return results;
        }

        let written = self.append_fetched_log_batch(&to_write);
        let Ok(written) = written else {
            self.corruption_count
                .fetch_add(to_write.len() as u64, Ordering::Relaxed);
            results.extend(
                to_write
                    .into_iter()
                    .map(|item| (item.chunk_id, CachePutResultTag::Failed)),
            );
            return results;
        };

        for (item, (off, len)) in to_write.into_iter().zip(written.into_iter()) {
            if let Some((_, old)) = self.entries.remove(&item.chunk_id) {
                self.usage_bytes.fetch_sub(old.len, Ordering::Relaxed);
                let _ = fs::remove_file(old.path);
            }
            if let Some((_, old)) = self.fetched_entries.remove(&item.chunk_id) {
                self.usage_bytes.fetch_sub(old.len, Ordering::Relaxed);
            }
            self.fetched_entries.insert(
                item.chunk_id,
                FetchedCacheEntry {
                    off,
                    len,
                    mtime: item.mtime,
                    checksum: item.checksum,
                    expire_at_ms: now_ms + self.ttl_ms,
                    last_access_ms: now_ms,
                },
            );
            self.usage_bytes.fetch_add(len, Ordering::Relaxed);
            self.record_access(item.chunk_id, now_ms);
            results.push((item.chunk_id, CachePutResultTag::Stored));
        }
        self.evict_until_fit();
        results
    }

    pub fn get(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> CacheGetResult {
        self.get_internal(chunk_id, max_len, expected_mtime, true, true)
    }

    pub fn get_for_remote(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> CacheGetResult {
        self.get_internal(chunk_id, max_len, expected_mtime, false, false)
    }

    fn get_internal(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        invalidate_on_mtime_mismatch: bool,
        verify_checksum: bool,
    ) -> CacheGetResult {
        enum StoredLocation {
            File(PathBuf),
            FetchedLog(u64),
        }
        let now_ms = LocalTime::mills() as i64;
        let (location, stored_len, mtime, entry_checksum) =
            if let Some(mut entry) = self.entries.get_mut(&chunk_id) {
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
                    if invalidate_on_mtime_mismatch {
                        drop(entry);
                        self.invalidate(chunk_id);
                    }
                    return CacheGetResult {
                        tag: CacheGetResultTag::MtimeMismatch,
                        chunk: None,
                    };
                }
                entry.last_access_ms = now_ms;
                (
                    StoredLocation::File(entry.path.clone()),
                    entry.len,
                    entry.mtime,
                    entry.checksum.clone(),
                )
            } else if let Some(mut entry) = self.fetched_entries.get_mut(&chunk_id) {
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
                    if invalidate_on_mtime_mismatch {
                        drop(entry);
                        self.invalidate(chunk_id);
                    }
                    return CacheGetResult {
                        tag: CacheGetResultTag::MtimeMismatch,
                        chunk: None,
                    };
                }
                entry.last_access_ms = now_ms;
                (
                    StoredLocation::FetchedLog(entry.off),
                    entry.len,
                    entry.mtime,
                    entry.checksum.clone(),
                )
            } else {
                return CacheGetResult {
                    tag: CacheGetResultTag::Miss,
                    chunk: None,
                };
            };

        let bytes = match location {
            StoredLocation::File(path) => match fs::read(&path) {
                Ok(v) => v,
                Err(_) => {
                    self.corruption_count.fetch_add(1, Ordering::Relaxed);
                    self.invalidate(chunk_id);
                    return CacheGetResult {
                        tag: CacheGetResultTag::Corruption,
                        chunk: None,
                    };
                }
            },
            StoredLocation::FetchedLog(off) => {
                match self.read_fetched_log(off, stored_len as usize) {
                    Ok(v) => v,
                    Err(_) => {
                        self.corruption_count.fetch_add(1, Ordering::Relaxed);
                        self.invalidate(chunk_id);
                        return CacheGetResult {
                            tag: CacheGetResultTag::Corruption,
                            chunk: None,
                        };
                    }
                }
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

        if self.enable_checksum && verify_checksum {
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
        let mut expired: Vec<ChunkId> = self
            .entries
            .iter()
            .filter_map(|v| (v.value().expire_at_ms <= now_ms).then_some(*v.key()))
            .collect();
        expired.extend(
            self.fetched_entries
                .iter()
                .filter_map(|v| (v.value().expire_at_ms <= now_ms).then_some(*v.key())),
        );
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
            cached_chunks_count: self.entries.len() + self.fetched_entries.len(),
            expired_chunks: self.expired_chunks.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            mtime_mismatches: self.mtime_mismatches.load(Ordering::Relaxed),
            checksum_failures: self.checksum_failures.load(Ordering::Relaxed),
            corruption_count: self.corruption_count.load(Ordering::Relaxed),
        }
    }

    fn invalidate(&self, chunk_id: ChunkId) -> bool {
        let mut removed = false;
        if let Some((_, entry)) = self.entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(entry.len, Ordering::Relaxed);
            let _ = fs::remove_file(entry.path);
            removed = true;
        }
        if let Some((_, entry)) = self.fetched_entries.remove(&chunk_id) {
            self.usage_bytes.fetch_sub(entry.len, Ordering::Relaxed);
            removed = true;
        }
        if removed {
            self.invalidations.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    fn evict_until_fit(&self) {
        self.compact_eviction_queue_if_needed();
        if self.usage_bytes.load(Ordering::Relaxed) <= self.capacity_bytes {
            return;
        }
        while self.usage_bytes.load(Ordering::Relaxed) > self.capacity_bytes {
            let Some(token) = self.pop_oldest_access() else {
                break;
            };
            let chunk_id = token.chunk_id();
            let should_evict = if let Some(entry) = self.entries.get(&chunk_id) {
                let stale = entry.last_access_ms != token.last_access_ms;
                drop(entry);
                !stale
            } else if let Some(entry) = self.fetched_entries.get(&chunk_id) {
                let stale = entry.last_access_ms != token.last_access_ms;
                drop(entry);
                !stale
            } else {
                false
            };
            if should_evict {
                self.invalidate(chunk_id);
            }
        }
    }

    fn record_access(&self, chunk_id: ChunkId, last_access_ms: i64) {
        let should_compact = {
            let mut queue = self
                .eviction_queue
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            queue.push(Reverse(EvictionToken::new(chunk_id, last_access_ms)));
            let entries_len = self.entries.len() + self.fetched_entries.len();
            entries_len > 0 && queue.len() > entries_len.saturating_mul(8)
        };
        if should_compact {
            self.compact_eviction_queue_if_needed();
        }
    }

    fn pop_oldest_access(&self) -> Option<EvictionToken> {
        let mut queue = self
            .eviction_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queue.pop().map(|Reverse(token)| token)
    }

    fn compact_eviction_queue_if_needed(&self) {
        let entries_len = self.entries.len() + self.fetched_entries.len();
        if entries_len == 0 {
            let mut queue = self
                .eviction_queue
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            queue.clear();
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
        for entry in self.fetched_entries.iter() {
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

    fn append_fetched_log_batch(
        &self,
        batch: &[FetchedCacheBatchItem],
    ) -> std::io::Result<Vec<(u64, u64)>> {
        let mut writer_guard = self
            .fetched_log_writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if writer_guard.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&self.fetched_log_path)?;
            let next_off = file.metadata()?.len();
            *writer_guard = Some(FetchedLogWriterState { file, next_off });
        }
        let writer = writer_guard
            .as_mut()
            .expect("fetched log writer must be initialized");
        let mut off = writer.next_off;
        let mut offsets = Vec::with_capacity(batch.len());
        for item in batch {
            let len = item.data.len() as u64;
            offsets.push((off, len));
            off = off.saturating_add(len);
        }
        let mut slices: Vec<IoSlice<'_>> = batch
            .iter()
            .map(|item| IoSlice::new(item.data.as_ref()))
            .collect();
        let mut slice_view: &mut [IoSlice<'_>] = &mut slices;
        while !slice_view.is_empty() {
            let written = match writer.file.write_vectored(slice_view) {
                Ok(written) => written,
                Err(e) => {
                    writer.next_off = writer.file.metadata()?.len();
                    return Err(e);
                }
            };
            if written == 0 {
                writer.next_off = writer.file.metadata()?.len();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to append fetched log batch",
                ));
            }
            IoSlice::advance_slices(&mut slice_view, written);
        }
        writer.next_off = off;
        Ok(offsets)
    }

    fn read_fetched_log(&self, off: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let mut reader = {
            let mut reader_guard = self
                .fetched_log_reader
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if reader_guard.is_none() {
                *reader_guard = Some(OpenOptions::new().read(true).open(&self.fetched_log_path)?);
            }
            reader_guard
                .as_ref()
                .expect("fetched log reader must be initialized")
                .try_clone()?
        };
        reader.seek(SeekFrom::Start(off))?;
        let mut bytes = vec![0u8; len];
        reader.read_exact(&mut bytes)?;
        Ok(bytes)
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

fn sha256_bytes(data: &[u8]) -> Bytes {
    let hashed = digest::digest(&digest::SHA256, data);
    Bytes::copy_from_slice(hashed.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn new_cache_manager() -> CacheManager {
        let mut conf = ClientP2pConf::default();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        conf.cache_dir = std::env::temp_dir()
            .join(format!("curvine-p2p-cache-{}-{}", std::process::id(), ts))
            .to_string_lossy()
            .into_owned();
        CacheManager::new(&conf)
    }

    #[test]
    fn duplicate_put_refreshes_without_rewrite() {
        let cache_manager = new_cache_manager();
        let chunk_id = ChunkId::with_version(9, 1, 1, 0);
        let data = Bytes::from_static(b"p2p-refresh");

        let first = cache_manager.put_with_result(chunk_id, data.clone(), 100);
        let second = cache_manager.put_with_result(chunk_id, data, 100);
        let snapshot = cache_manager.snapshot();

        assert_eq!(first, CachePutResultTag::Stored);
        assert_eq!(second, CachePutResultTag::Refreshed);
        assert_eq!(snapshot.cached_chunks_count, 1);

        let _ = fs::remove_dir_all(cache_manager.root_dir.clone());
    }

    #[test]
    fn fetched_batch_put_writes_and_reads_back() {
        let cache_manager = new_cache_manager();
        let chunk_a = ChunkId::with_version(9, 1, 1, 66);
        let chunk_b = ChunkId::with_version(9, 1, 1, 67);
        let data_a = Bytes::from_static(b"batch-a");
        let data_b = Bytes::from_static(b"batch-b");
        let results = cache_manager.put_fetched_batch_with_checksum(vec![
            FetchedCacheBatchItem {
                chunk_id: chunk_a,
                data: data_a.clone(),
                mtime: 100,
                checksum: Bytes::new(),
            },
            FetchedCacheBatchItem {
                chunk_id: chunk_b,
                data: data_b.clone(),
                mtime: 100,
                checksum: Bytes::new(),
            },
        ]);

        assert_eq!(results.len(), 2);
        assert!(results
            .iter()
            .all(|(_, tag)| *tag == CachePutResultTag::Stored));
        let get_a = cache_manager.get(chunk_a, data_a.len(), Some(100));
        let get_b = cache_manager.get(chunk_b, data_b.len(), Some(100));
        assert_eq!(get_a.tag, CacheGetResultTag::Hit);
        assert_eq!(get_b.tag, CacheGetResultTag::Hit);
        assert_eq!(get_a.chunk.expect("chunk a").data, data_a);
        assert_eq!(get_b.chunk.expect("chunk b").data, data_b);

        let _ = fs::remove_dir_all(cache_manager.root_dir.clone());
    }

    #[test]
    fn fetched_log_append_resumes_from_existing_file_len() {
        let mut conf = ClientP2pConf::default();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        conf.cache_dir = std::env::temp_dir()
            .join(format!("curvine-p2p-cache-{}-{}", std::process::id(), ts))
            .to_string_lossy()
            .into_owned();
        let fetched_log_path = PathBuf::from(conf.cache_dir.clone()).join("fetched_chunks.log");
        fs::create_dir_all(PathBuf::from(conf.cache_dir.clone())).unwrap();
        fs::write(&fetched_log_path, b"prefix").unwrap();

        let cache_manager = CacheManager::new(&conf);
        let chunk_id = ChunkId::with_version(9, 1, 1, 68);
        let data = Bytes::from_static(b"after-restart");

        let put_tag =
            cache_manager.put_fetched_with_result_and_checksum(chunk_id, data.clone(), 100, None);
        let get_result = cache_manager.get(chunk_id, data.len(), Some(100));

        assert_eq!(put_tag, CachePutResultTag::Stored);
        assert_eq!(get_result.tag, CacheGetResultTag::Hit);
        assert_eq!(get_result.chunk.expect("chunk should exist").data, data);

        let _ = fs::remove_dir_all(cache_manager.root_dir.clone());
    }

    #[test]
    fn wrong_checksum_hint_is_detected_on_read() {
        let cache_manager = new_cache_manager();
        let chunk_id = ChunkId::with_version(9, 1, 1, 128);
        let data = Bytes::from_static(b"wrong-checksum");

        let put_tag = cache_manager.put_with_result_and_checksum(
            chunk_id,
            data,
            100,
            Some(Bytes::from_static(b"bad-sum")),
        );
        let first_get = cache_manager.get(chunk_id, 1024, Some(100));
        let second_get = cache_manager.get(chunk_id, 1024, Some(100));

        assert_eq!(put_tag, CachePutResultTag::Stored);
        assert_eq!(first_get.tag, CacheGetResultTag::ChecksumFailure);
        assert_eq!(second_get.tag, CacheGetResultTag::Miss);

        let _ = fs::remove_dir_all(cache_manager.root_dir.clone());
    }

    #[test]
    fn remote_mtime_mismatch_keeps_provider_cache_entry() {
        let cache_manager = new_cache_manager();
        let chunk_id = ChunkId::with_version(1, 1, 1, 0);
        let data = Bytes::from_static(b"p2p-test-data");
        assert!(cache_manager.put(chunk_id, data.clone(), 100));

        let mismatch = cache_manager.get_for_remote(chunk_id, data.len(), Some(101));
        assert_eq!(mismatch.tag, CacheGetResultTag::MtimeMismatch);
        assert!(mismatch.chunk.is_none());

        let hit = cache_manager.get(chunk_id, data.len(), Some(100));
        assert_eq!(hit.tag, CacheGetResultTag::Hit);
        let hit_chunk = hit.chunk.expect("cache hit expected");
        assert_eq!(hit_chunk.data, data);

        let _ = fs::remove_dir_all(cache_manager.root_dir.clone());
    }

    #[test]
    fn local_mtime_mismatch_invalidates_cache_entry() {
        let cache_manager = new_cache_manager();
        let chunk_id = ChunkId::with_version(2, 1, 1, 0);
        let data = Bytes::from_static(b"p2p-test-data");
        assert!(cache_manager.put(chunk_id, data.clone(), 100));

        let mismatch = cache_manager.get(chunk_id, data.len(), Some(101));
        assert_eq!(mismatch.tag, CacheGetResultTag::MtimeMismatch);
        assert!(mismatch.chunk.is_none());

        let miss = cache_manager.get(chunk_id, data.len(), Some(100));
        assert_eq!(miss.tag, CacheGetResultTag::Miss);
        assert!(miss.chunk.is_none());

        let _ = fs::remove_dir_all(cache_manager.root_dir.clone());
    }
}
