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

use crate::p2p::{CacheSnapshot, ChunkId, DiscoverySnapshot};
use bytes::Bytes;
use curvine_common::conf::ClientP2pConf;
use once_cell::sync::Lazy;
use orpc::sync::FastDashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone)]
struct PublishedChunk {
    owner_id: u64,
    data: Bytes,
    mtime: i64,
}

static NEXT_SERVICE_ID: AtomicU64 = AtomicU64::new(1);
static PUBLISHED_CHUNKS: Lazy<FastDashMap<ChunkId, PublishedChunk>> =
    Lazy::new(FastDashMap::default);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pState {
    Disabled = 0,
    Stopped = 1,
    Running = 2,
}

impl P2pState {
    fn from_u8(value: u8) -> Self {
        match value {
            2 => Self::Running,
            1 => Self::Stopped,
            _ => Self::Disabled,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct P2pStatsSnapshot {
    pub discovery: DiscoverySnapshot,
    pub cache: CacheSnapshot,
}

pub struct P2pService {
    conf: ClientP2pConf,
    state: AtomicU8,
    stats: Mutex<P2pStatsSnapshot>,
    service_id: u64,
    published_bytes: AtomicU64,
    published_chunks: FastDashMap<ChunkId, usize>,
}

impl P2pService {
    pub fn new(conf: ClientP2pConf) -> Self {
        let state = if conf.enable {
            P2pState::Stopped
        } else {
            P2pState::Disabled
        };
        Self {
            conf,
            state: AtomicU8::new(state as u8),
            stats: Mutex::new(P2pStatsSnapshot::default()),
            service_id: NEXT_SERVICE_ID.fetch_add(1, Ordering::Relaxed),
            published_bytes: AtomicU64::new(0),
            published_chunks: FastDashMap::default(),
        }
    }

    pub fn conf(&self) -> &ClientP2pConf {
        &self.conf
    }

    pub fn state(&self) -> P2pState {
        P2pState::from_u8(self.state.load(Ordering::Relaxed))
    }

    pub fn is_running(&self) -> bool {
        self.state() == P2pState::Running
    }

    pub fn is_enabled(&self) -> bool {
        self.conf.enable
    }

    pub fn start(&self) -> bool {
        if !self.conf.enable {
            return false;
        }
        self.state.store(P2pState::Running as u8, Ordering::Relaxed);
        true
    }

    pub fn stop(&self) {
        if !self.conf.enable {
            return;
        }
        self.clear_published_chunks();
        self.state.store(P2pState::Stopped as u8, Ordering::Relaxed);
    }

    pub fn stats_snapshot(&self) -> P2pStatsSnapshot {
        let mut snapshot = self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let usage = self.published_bytes.load(Ordering::Relaxed);
        let capacity = self.conf.cache_capacity.max(1);
        snapshot.cache.usage_bytes = usage;
        snapshot.cache.capacity_bytes = capacity;
        snapshot.cache.usage_ratio = usage as f64 / capacity as f64;
        snapshot.cache.cached_chunks_count = self.published_chunks.len();
        snapshot
    }

    pub fn update_stats(&self, snapshot: P2pStatsSnapshot) {
        *self
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
    }

    pub fn publish_chunk(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> bool {
        if !self.is_running() || data.is_empty() {
            return false;
        }

        let len = data.len();
        let previous_len = self.published_chunks.insert(chunk_id, len);
        if let Some(previous_len) = previous_len {
            self.published_bytes
                .fetch_sub(previous_len as u64, Ordering::Relaxed);
        }
        self.published_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        PUBLISHED_CHUNKS.insert(
            chunk_id,
            PublishedChunk {
                owner_id: self.service_id,
                data,
                mtime,
            },
        );
        true
    }

    pub async fn fetch_chunk(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> Option<Bytes> {
        if !self.is_running() || max_len == 0 {
            return None;
        }

        let chunk = PUBLISHED_CHUNKS.get(&chunk_id)?;
        if expected_mtime.is_some_and(|mtime| mtime > 0 && chunk.mtime != mtime) {
            return None;
        }

        Some(chunk.data.slice(0..chunk.data.len().min(max_len)))
    }

    fn clear_published_chunks(&self) {
        let chunk_ids: Vec<ChunkId> = self
            .published_chunks
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for chunk_id in chunk_ids {
            if let Some(entry) = PUBLISHED_CHUNKS.get(&chunk_id) {
                let owned = entry.owner_id == self.service_id;
                drop(entry);
                if owned {
                    PUBLISHED_CHUNKS.remove(&chunk_id);
                }
            }
        }
        self.published_chunks.clear();
        self.published_bytes.store(0, Ordering::Relaxed);
    }
}

impl Drop for P2pService {
    fn drop(&mut self) {
        self.clear_published_chunks();
    }
}

#[cfg(test)]
mod tests {
    use super::{P2pService, P2pState};
    use crate::p2p::ChunkId;
    use bytes::Bytes;
    use curvine_common::conf::ClientP2pConf;
    use std::sync::atomic::{AtomicI64, Ordering};

    static TEST_CHUNK_SEQ: AtomicI64 = AtomicI64::new(1);

    fn next_chunk_id() -> ChunkId {
        let seq = TEST_CHUNK_SEQ.fetch_add(1, Ordering::Relaxed);
        ChunkId::with_version(seq, seq, seq, 0)
    }

    #[test]
    fn disabled_service_does_not_start() {
        let service = P2pService::new(ClientP2pConf::default());
        assert_eq!(service.state(), P2pState::Disabled);
        assert!(!service.start());
        assert_eq!(service.state(), P2pState::Disabled);
    }

    #[test]
    fn enabled_service_transitions_between_stopped_and_running() {
        let service = P2pService::new(ClientP2pConf {
            enable: true,
            ..ClientP2pConf::default()
        });
        assert_eq!(service.state(), P2pState::Stopped);
        assert!(service.start());
        assert_eq!(service.state(), P2pState::Running);
        service.stop();
        assert_eq!(service.state(), P2pState::Stopped);
    }

    #[test]
    fn running_services_share_published_chunks_and_cleanup_on_stop() {
        let conf = ClientP2pConf {
            enable: true,
            ..ClientP2pConf::default()
        };
        let publisher = P2pService::new(conf.clone());
        let consumer = P2pService::new(conf);
        assert!(publisher.start());
        assert!(consumer.start());

        let chunk_id = next_chunk_id();
        let data = Bytes::from_static(b"peer-data");
        assert!(publisher.publish_chunk(chunk_id, data.clone(), 7));

        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let fetched = rt.block_on(consumer.fetch_chunk(chunk_id, 4, Some(7)));
        assert_eq!(fetched, Some(Bytes::from_static(b"peer")));

        publisher.stop();

        let after_stop = rt.block_on(consumer.fetch_chunk(chunk_id, 4, Some(7)));
        assert!(after_stop.is_none());
    }
}
