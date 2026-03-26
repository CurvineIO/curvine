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
use curvine_common::utils::CommonUtils;
use once_cell::sync::Lazy;
use orpc::sync::FastDashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;

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
    peer_whitelist: Mutex<Vec<String>>,
    tenant_whitelist: Mutex<HashSet<String>>,
    runtime_policy_version: AtomicU64,
    runtime_policy_lock: AsyncMutex<()>,
}

impl P2pService {
    pub fn new(conf: ClientP2pConf) -> Self {
        let state = if conf.enable {
            P2pState::Stopped
        } else {
            P2pState::Disabled
        };
        let peer_whitelist = conf.peer_whitelist.clone();
        let tenant_whitelist: HashSet<String> = conf.tenant_whitelist.iter().cloned().collect();
        Self {
            conf,
            state: AtomicU8::new(state as u8),
            stats: Mutex::new(P2pStatsSnapshot::default()),
            service_id: NEXT_SERVICE_ID.fetch_add(1, Ordering::Relaxed),
            published_bytes: AtomicU64::new(0),
            published_chunks: FastDashMap::default(),
            peer_whitelist: Mutex::new(peer_whitelist),
            tenant_whitelist: Mutex::new(tenant_whitelist),
            runtime_policy_version: AtomicU64::new(0),
            runtime_policy_lock: AsyncMutex::new(()),
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
        self.publish_chunk_for_tenant(chunk_id, data, mtime, None)
    }

    pub fn publish_chunk_for_tenant(
        &self,
        chunk_id: ChunkId,
        data: Bytes,
        mtime: i64,
        tenant_id: Option<&str>,
    ) -> bool {
        if !self.is_running() || data.is_empty() {
            return false;
        }
        if !self.is_tenant_allowed(tenant_id) {
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
        self.fetch_chunk_for_tenant(chunk_id, max_len, expected_mtime, None)
            .await
    }

    pub async fn fetch_chunk_for_tenant(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        tenant_id: Option<&str>,
    ) -> Option<Bytes> {
        if !self.is_running() || max_len == 0 {
            return None;
        }
        if !self.is_tenant_allowed(tenant_id) {
            return None;
        }

        let chunk = PUBLISHED_CHUNKS.get(&chunk_id)?;
        if expected_mtime.is_some_and(|mtime| mtime > 0 && chunk.mtime != mtime) {
            return None;
        }

        Some(chunk.data.slice(0..chunk.data.len().min(max_len)))
    }

    pub fn runtime_policy_version(&self) -> u64 {
        self.runtime_policy_version.load(Ordering::Relaxed)
    }

    pub fn is_tenant_allowed(&self, tenant_id: Option<&str>) -> bool {
        let tenant_whitelist = self
            .tenant_whitelist
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if tenant_whitelist.is_empty() {
            return true;
        }
        tenant_id.is_some_and(|tenant_id| tenant_whitelist.contains(tenant_id))
    }

    pub async fn sync_runtime_policy_from_master(
        &self,
        policy_version: u64,
        peer_whitelist: Vec<String>,
        tenant_whitelist: Vec<String>,
        policy_signature: String,
    ) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let _guard = self.runtime_policy_lock.lock().await;
        if policy_version == 0 {
            self.apply_runtime_policy(
                0,
                self.conf.peer_whitelist.clone(),
                self.conf.tenant_whitelist.clone(),
            );
            return true;
        }
        if policy_version <= self.runtime_policy_version() {
            return true;
        }
        if !CommonUtils::verify_p2p_policy_signatures(
            &self.conf.policy_hmac_key,
            policy_version,
            &peer_whitelist,
            &tenant_whitelist,
            &policy_signature,
        ) {
            return false;
        }
        self.apply_runtime_policy(policy_version, peer_whitelist, tenant_whitelist);
        true
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

    fn apply_runtime_policy(
        &self,
        version: u64,
        peer_whitelist: Vec<String>,
        tenant_whitelist: Vec<String>,
    ) {
        *self
            .peer_whitelist
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = peer_whitelist;
        *self
            .tenant_whitelist
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            tenant_whitelist.into_iter().collect();
        self.runtime_policy_version
            .store(version, Ordering::Relaxed);
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
    use curvine_common::utils::CommonUtils;
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

    #[test]
    fn signed_runtime_policy_updates_effective_tenant_whitelist() {
        let mut conf = ClientP2pConf::default();
        conf.enable = true;
        conf.policy_hmac_key = "secret".to_string();
        conf.tenant_whitelist = vec!["local-tenant".to_string()];
        let service = P2pService::new(conf);
        assert!(service.start());

        let peers = vec!["peer-a".to_string()];
        let tenants = vec!["runtime-tenant".to_string()];
        let signature = CommonUtils::sign_p2p_policy("secret", 7, &peers, &tenants);
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        assert!(rt.block_on(service.sync_runtime_policy_from_master(7, peers, tenants, signature,)));

        assert_eq!(service.runtime_policy_version(), 7);
        assert!(service.is_tenant_allowed(Some("runtime-tenant")));
        assert!(!service.is_tenant_allowed(Some("local-tenant")));
    }

    #[test]
    fn runtime_policy_version_zero_restores_local_whitelist() {
        let mut conf = ClientP2pConf::default();
        conf.enable = true;
        conf.policy_hmac_key = "secret".to_string();
        conf.tenant_whitelist = vec!["local-tenant".to_string()];
        let service = P2pService::new(conf);
        assert!(service.start());

        let peers = vec!["peer-a".to_string()];
        let tenants = vec!["runtime-tenant".to_string()];
        let signature = CommonUtils::sign_p2p_policy("secret", 3, &peers, &tenants);
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        assert!(rt.block_on(service.sync_runtime_policy_from_master(3, peers, tenants, signature,)));
        assert!(rt.block_on(service.sync_runtime_policy_from_master(
            0,
            vec![],
            vec![],
            String::new(),
        )));

        assert_eq!(service.runtime_policy_version(), 0);
        assert!(service.is_tenant_allowed(Some("local-tenant")));
        assert!(!service.is_tenant_allowed(Some("runtime-tenant")));
    }
}
