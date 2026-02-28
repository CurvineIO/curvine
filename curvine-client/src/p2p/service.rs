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
use crate::p2p::{CacheManager, ChunkId};
use bytes::Bytes;
use curvine_common::conf::ClientP2pConf;
use curvine_common::utils::CommonUtils;
use futures::StreamExt;
use libp2p::identity;
use libp2p::kad;
use libp2p::mdns;
use libp2p::multiaddr::Protocol;
use libp2p::noise;
use libp2p::request_response;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::{NetworkBehaviour, StreamProtocol, SwarmEvent};
use libp2p::tcp;
use libp2p::yamux;
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use log::{debug, warn};
use once_cell::sync::{Lazy, OnceCell};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::FastDashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex, Semaphore};
use tokio::time::{sleep, timeout};

static PEER_STATS: Lazy<FastDashMap<String, Arc<PeerStats>>> = Lazy::new(FastDashMap::default);

#[derive(Debug, Default)]
struct PeerStats {
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
    latency_us_total: AtomicU64,
    latency_samples: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct PeerEwma {
    latency_ms: f64,
    failure_ratio: f64,
    throughput_mb_s: f64,
}

impl Default for PeerEwma {
    fn default() -> Self {
        Self {
            latency_ms: 200.0,
            failure_ratio: 0.0,
            throughput_mb_s: 4.0,
        }
    }
}

impl PeerEwma {
    fn observe_success(&mut self, latency_ms: f64, bytes: usize) {
        self.latency_ms = ewma(self.latency_ms, latency_ms.max(0.0), 0.2);
        self.failure_ratio = ewma(self.failure_ratio, 0.0, 0.3);
        if latency_ms > 0.0 && bytes > 0 {
            let throughput_mb_s = bytes as f64 / latency_ms / 1024.0;
            self.throughput_mb_s = ewma(self.throughput_mb_s, throughput_mb_s.max(0.01), 0.2);
        }
    }

    fn observe_failure(&mut self) {
        self.failure_ratio = ewma(self.failure_ratio, 1.0, 0.3);
    }
}

#[derive(Debug, Default)]
struct NetworkState {
    active_peers: AtomicU64,
    mdns_peers: AtomicU64,
    dht_peers: AtomicU64,
    bootstrap_connected: AtomicU64,
    local_peer_id: OnceCell<String>,
    listen_addr: OnceCell<String>,
}

#[derive(Debug)]
struct QpsLimiter {
    rate_per_sec: u64,
    state: AsyncMutex<QpsBucketState>,
}

#[derive(Debug)]
struct QpsBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl QpsLimiter {
    fn new(rate_per_sec: u64) -> Self {
        Self {
            rate_per_sec,
            state: AsyncMutex::new(QpsBucketState {
                tokens: rate_per_sec as f64,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn acquire(&self, timeout: Duration) -> bool {
        if self.rate_per_sec == 0 {
            return true;
        }
        let deadline = Instant::now() + timeout;
        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed_secs = now.duration_since(state.last_refill).as_secs_f64();
                state.tokens = (state.tokens + elapsed_secs * self.rate_per_sec as f64)
                    .min(self.rate_per_sec as f64);
                state.last_refill = now;
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return true;
                }
                Duration::from_secs_f64((1.0 - state.tokens) / self.rate_per_sec as f64)
                    .max(Duration::from_millis(1))
            };

            if Instant::now() + wait > deadline {
                return false;
            }
            sleep(wait).await;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct P2pStatsSnapshot {
    pub active_peers: usize,
    pub mdns_peers: usize,
    pub dht_peers: usize,
    pub bootstrap_connected: usize,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub avg_peer_latency_ms: f64,
    pub cache_usage_bytes: u64,
    pub cache_capacity_bytes: u64,
    pub cache_usage_ratio: f64,
    pub cached_chunks_count: usize,
    pub expired_chunks: u64,
    pub invalidations: u64,
    pub mtime_mismatches: u64,
    pub checksum_failures: u64,
    pub corruption_count: u64,
    pub policy_rejects: u64,
    pub policy_rollback_ignored: u64,
}

#[derive(Debug, Clone, Default)]
pub struct P2pReadTraceContext {
    pub trace_id: Option<String>,
    pub tenant_id: Option<String>,
    pub job_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum P2pState {
    Disabled = 0,
    Init = 1,
    Running = 2,
    Stopped = 3,
}

#[derive(Debug)]
enum NetworkCommand {
    Publish {
        chunk_id: ChunkId,
    },
    Fetch {
        fetch_token: u64,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        trace: TraceLabels,
        response: oneshot::Sender<Option<FetchedChunk>>,
    },
    CancelFetch {
        fetch_token: u64,
    },
    UpdateRuntimePolicy {
        peer_whitelist: Option<Vec<String>>,
        tenant_whitelist: Option<Vec<String>>,
        response: oneshot::Sender<bool>,
    },
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkFetchRequest {
    file_id: i64,
    version_epoch: i64,
    block_id: i64,
    off: i64,
    len: usize,
    expected_mtime: i64,
    tenant_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkFetchResponse {
    found: bool,
    mtime: i64,
    checksum: Bytes,
    data: Bytes,
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "P2pNetworkEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
struct P2pNetworkBehaviour {
    request_response: request_response::cbor::Behaviour<ChunkFetchRequest, ChunkFetchResponse>,
    mdns: Toggle<mdns::tokio::Behaviour>,
    kad: kad::Behaviour<kad::store::MemoryStore>,
}

#[derive(Debug)]
enum P2pNetworkEvent {
    RequestResponse(request_response::Event<ChunkFetchRequest, ChunkFetchResponse>),
    Mdns(mdns::Event),
    Kad(kad::Event),
}

impl From<request_response::Event<ChunkFetchRequest, ChunkFetchResponse>> for P2pNetworkEvent {
    fn from(value: request_response::Event<ChunkFetchRequest, ChunkFetchResponse>) -> Self {
        Self::RequestResponse(value)
    }
}

impl From<mdns::Event> for P2pNetworkEvent {
    fn from(value: mdns::Event) -> Self {
        Self::Mdns(value)
    }
}

impl From<kad::Event> for P2pNetworkEvent {
    fn from(value: kad::Event) -> Self {
        Self::Kad(value)
    }
}

struct PendingProviderQuery {
    fetch_token: u64,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
    trace: TraceLabels,
    response: oneshot::Sender<Option<FetchedChunk>>,
    deadline: Instant,
}

struct PendingFetchRequest {
    fetch_token: u64,
    chunk_id: ChunkId,
    max_len: usize,
    expected_mtime: Option<i64>,
    trace: TraceLabels,
    response: oneshot::Sender<Option<FetchedChunk>>,
    candidates: VecDeque<PeerId>,
    active_peer: Option<PeerId>,
    started_at: Instant,
    deadline: Instant,
}

struct PendingIncomingResponse {
    channel: request_response::ResponseChannel<ChunkFetchResponse>,
    deadline: Instant,
}

struct PreparedIncomingResponse {
    id: u64,
    response: ChunkFetchResponse,
    sent: u64,
}

#[derive(Debug)]
struct FetchedChunk {
    data: Bytes,
    mtime: i64,
    checksum: Bytes,
}

#[derive(Clone)]
struct CachedProviders {
    peers: HashSet<PeerId>,
    expire_at: Instant,
}

#[derive(Debug, Clone)]
struct TraceLabels {
    trace_id: String,
    tenant_id: String,
    job_id: String,
    sampled: bool,
}

pub struct P2pService {
    conf: ClientP2pConf,
    state: AtomicU8,
    peer_id: String,
    inflight: Arc<Semaphore>,
    runtime: Option<Arc<Runtime>>,
    cache_manager: Arc<CacheManager>,
    qps_limiter: Option<Arc<QpsLimiter>>,
    fetch_flights: FastDashMap<ChunkId, Arc<AsyncMutex<()>>>,
    network_mtime_mismatches: AtomicU64,
    network_checksum_failures: AtomicU64,
    network_state: Arc<NetworkState>,
    network_tx: Mutex<Option<mpsc::Sender<NetworkCommand>>>,
    fetch_tokens: AtomicU64,
    runtime_policy_version: AtomicU64,
    runtime_policy_applied: AtomicBool,
    runtime_policy_lock: AsyncMutex<()>,
    runtime_policy_rejects: AtomicU64,
    runtime_policy_rollback_ignored: AtomicU64,
    rejected_policy_version: AtomicU64,
    rejected_policy_signature: AsyncMutex<String>,
}

impl P2pService {
    pub fn new(conf: ClientP2pConf) -> Self {
        Self::new_with_runtime(conf, None)
    }

    pub fn new_with_runtime(conf: ClientP2pConf, runtime: Option<Arc<Runtime>>) -> Self {
        let state = if conf.enable && runtime.is_some() {
            P2pState::Init as u8
        } else {
            P2pState::Disabled as u8
        };
        let runtime_policy_version = load_runtime_policy_version(&conf);
        let cache_manager = Arc::new(CacheManager::new(&conf));

        Self {
            inflight: Arc::new(Semaphore::new(conf.max_inflight_requests.max(1))),
            qps_limiter: (conf.p2p_qps_limit > 0)
                .then(|| Arc::new(QpsLimiter::new(conf.p2p_qps_limit))),
            conf,
            state: AtomicU8::new(state),
            peer_id: format!("client-{}", orpc::common::Utils::uuid()),
            runtime,
            cache_manager,
            fetch_flights: FastDashMap::default(),
            network_mtime_mismatches: AtomicU64::new(0),
            network_checksum_failures: AtomicU64::new(0),
            network_state: Arc::new(NetworkState::default()),
            network_tx: Mutex::new(None),
            fetch_tokens: AtomicU64::new(1),
            runtime_policy_version: AtomicU64::new(runtime_policy_version),
            runtime_policy_applied: AtomicBool::new(runtime_policy_version == 0),
            runtime_policy_lock: AsyncMutex::new(()),
            runtime_policy_rejects: AtomicU64::new(0),
            runtime_policy_rollback_ignored: AtomicU64::new(0),
            rejected_policy_version: AtomicU64::new(0),
            rejected_policy_signature: AsyncMutex::new(String::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.conf.enable && self.runtime.is_some()
    }

    pub fn state(&self) -> P2pState {
        match self.state.load(Ordering::Relaxed) {
            0 => P2pState::Disabled,
            1 => P2pState::Init,
            2 => P2pState::Running,
            _ => P2pState::Stopped,
        }
    }

    pub fn start(&self) {
        if self.is_enabled() {
            self.stats();
            self.ensure_network_started();
            self.state.store(P2pState::Running as u8, Ordering::Relaxed);
        }
    }

    pub fn stop(&self) {
        if self.is_enabled() {
            self.state.store(P2pState::Stopped as u8, Ordering::Relaxed);
            PEER_STATS.remove(&self.peer_id);
            self.stop_network();
        }
    }

    pub fn conf(&self) -> &ClientP2pConf {
        &self.conf
    }

    pub async fn update_runtime_policy(
        &self,
        peer_whitelist: Option<Vec<String>>,
        tenant_whitelist: Option<Vec<String>>,
    ) -> bool {
        if !self.is_enabled() {
            return false;
        }
        self.ensure_network_started();
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|network_tx| network_tx.clone());
        let Some(tx) = tx else {
            return false;
        };
        let (response_tx, response_rx) = oneshot::channel();
        if tx
            .send(NetworkCommand::UpdateRuntimePolicy {
                peer_whitelist,
                tenant_whitelist,
                response: response_tx,
            })
            .await
            .is_err()
        {
            return false;
        }
        timeout(self.transfer_timeout(), response_rx)
            .await
            .ok()
            .and_then(|v| v.ok())
            .unwrap_or(false)
    }

    pub fn runtime_policy_version(&self) -> u64 {
        self.runtime_policy_version.load(Ordering::Relaxed)
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
            return true;
        }
        let current_version = self.runtime_policy_version.load(Ordering::Relaxed);
        if policy_version < current_version {
            self.runtime_policy_rollback_ignored
                .fetch_add(1, Ordering::Relaxed);
            return true;
        }
        if policy_version == current_version && self.runtime_policy_applied.load(Ordering::Relaxed)
        {
            return true;
        }
        if self
            .is_repeated_rejected_policy(policy_version, &policy_signature)
            .await
        {
            return true;
        }
        if !self.verify_master_policy_signature(
            policy_version,
            &peer_whitelist,
            &tenant_whitelist,
            &policy_signature,
        ) {
            self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
            self.remember_rejected_policy(policy_version, &policy_signature)
                .await;
            warn!(
                "reject p2p policy update due to signature mismatch, version={}",
                policy_version
            );
            return false;
        }
        if !self
            .update_runtime_policy(Some(peer_whitelist), Some(tenant_whitelist))
            .await
        {
            self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        if !persist_runtime_policy_version(&self.conf, policy_version) {
            self.runtime_policy_rejects.fetch_add(1, Ordering::Relaxed);
            warn!(
                "failed to persist runtime policy version={}, retry on next sync",
                policy_version
            );
            return false;
        }
        self.runtime_policy_version
            .store(policy_version, Ordering::Relaxed);
        self.runtime_policy_applied.store(true, Ordering::Relaxed);
        self.clear_rejected_policy().await;
        true
    }

    fn verify_master_policy_signature(
        &self,
        policy_version: u64,
        peer_whitelist: &[String],
        tenant_whitelist: &[String],
        policy_signature: &str,
    ) -> bool {
        CommonUtils::verify_p2p_policy_signatures(
            &self.conf.policy_hmac_key,
            policy_version,
            peer_whitelist,
            tenant_whitelist,
            policy_signature,
        )
    }

    async fn is_repeated_rejected_policy(
        &self,
        policy_version: u64,
        policy_signature: &str,
    ) -> bool {
        if self.rejected_policy_version.load(Ordering::Relaxed) != policy_version {
            return false;
        }
        let rejected_signature = self.rejected_policy_signature.lock().await;
        rejected_signature.as_str() == policy_signature
    }

    async fn remember_rejected_policy(&self, policy_version: u64, policy_signature: &str) {
        self.rejected_policy_version
            .store(policy_version, Ordering::Relaxed);
        let mut rejected_signature = self.rejected_policy_signature.lock().await;
        rejected_signature.clear();
        rejected_signature.push_str(policy_signature);
    }

    async fn clear_rejected_policy(&self) {
        self.rejected_policy_version.store(0, Ordering::Relaxed);
        self.rejected_policy_signature.lock().await.clear();
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub fn bootstrap_peer_addr(&self) -> Option<String> {
        let peer_id = PeerId::from_str(self.network_state.local_peer_id.get()?).ok()?;
        let mut multiaddr = self
            .network_state
            .listen_addr
            .get()
            .and_then(|addr| Multiaddr::from_str(addr).ok())
            .or_else(|| {
                self.conf
                    .listen_addrs
                    .iter()
                    .find_map(|addr| Multiaddr::from_str(addr).ok())
            })?;
        if multiaddr.iter().any(|protocol| match protocol {
            Protocol::Tcp(port) => port == 0,
            Protocol::Udp(port) => port == 0,
            _ => false,
        }) {
            return None;
        }
        if multiaddr
            .iter()
            .any(|protocol| matches!(protocol, Protocol::P2p(_)))
        {
            return Some(multiaddr.to_string());
        }
        multiaddr.push(Protocol::P2p(peer_id));
        Some(multiaddr.to_string())
    }

    pub fn snapshot(&self) -> P2pStatsSnapshot {
        let active_peers = self.network_state.active_peers.load(Ordering::Relaxed) as usize;
        let stats = self.stats();
        let cache_snapshot = self.cache_manager.snapshot();
        let bytes_sent = stats.bytes_sent.load(Ordering::Relaxed);
        let bytes_recv = stats.bytes_recv.load(Ordering::Relaxed);
        let latency_us_total = stats.latency_us_total.load(Ordering::Relaxed);
        let latency_samples = stats.latency_samples.load(Ordering::Relaxed);
        let avg_peer_latency_ms = if latency_samples == 0 {
            0.0
        } else {
            latency_us_total as f64 / latency_samples as f64 / 1000.0
        };

        let mdns_peers = self.network_state.mdns_peers.load(Ordering::Relaxed) as usize;
        let dht_peers = self.network_state.dht_peers.load(Ordering::Relaxed) as usize;
        let bootstrap_connected = self
            .network_state
            .bootstrap_connected
            .load(Ordering::Relaxed) as usize;

        P2pStatsSnapshot {
            active_peers,
            mdns_peers,
            dht_peers,
            bootstrap_connected,
            bytes_sent,
            bytes_recv,
            avg_peer_latency_ms,
            cache_usage_bytes: cache_snapshot.usage_bytes,
            cache_capacity_bytes: cache_snapshot.capacity_bytes,
            cache_usage_ratio: cache_snapshot.usage_ratio,
            cached_chunks_count: cache_snapshot.cached_chunks_count,
            expired_chunks: cache_snapshot.expired_chunks,
            invalidations: cache_snapshot.invalidations,
            mtime_mismatches: cache_snapshot
                .mtime_mismatches
                .saturating_add(self.network_mtime_mismatches.load(Ordering::Relaxed)),
            checksum_failures: cache_snapshot
                .checksum_failures
                .saturating_add(self.network_checksum_failures.load(Ordering::Relaxed)),
            corruption_count: cache_snapshot.corruption_count,
            policy_rejects: self.runtime_policy_rejects.load(Ordering::Relaxed),
            policy_rollback_ignored: self.runtime_policy_rollback_ignored.load(Ordering::Relaxed),
        }
    }

    pub fn publish_chunk(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> bool {
        if !self.is_enabled() || self.state() != P2pState::Running || data.is_empty() {
            return false;
        }

        if !self.cache_manager.put(chunk_id, data, mtime) {
            return false;
        }
        self.send_network_command(NetworkCommand::Publish { chunk_id })
    }

    pub async fn fetch_chunk(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> Option<Bytes> {
        self.fetch_chunk_with_context(chunk_id, max_len, expected_mtime, None)
            .await
    }

    pub async fn fetch_chunk_with_context(
        &self,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        context: Option<&P2pReadTraceContext>,
    ) -> Option<Bytes> {
        if !self.is_enabled() || self.state() != P2pState::Running || max_len == 0 {
            return None;
        }

        if let Some(chunk) = self
            .cache_manager
            .get(chunk_id, max_len, expected_mtime)
            .chunk
        {
            return Some(chunk.data);
        }

        let flight_lock = self.fetch_flight_lock(chunk_id);
        let flight_guard = timeout(self.transfer_timeout(), flight_lock.clone().lock_owned())
            .await
            .ok()?;

        if let Some(chunk) = self
            .cache_manager
            .get(chunk_id, max_len, expected_mtime)
            .chunk
        {
            drop(flight_guard);
            self.cleanup_fetch_flight(&chunk_id, &flight_lock);
            return Some(chunk.data);
        }

        if let Some(limiter) = &self.qps_limiter {
            if !limiter.acquire(self.transfer_timeout()).await {
                drop(flight_guard);
                self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                return None;
            }
        }
        let permit = timeout(
            self.transfer_timeout(),
            self.inflight.clone().acquire_owned(),
        )
        .await
        .ok()?
        .ok()?;
        let fetch_token = self.next_fetch_token();
        let trace = TraceLabels::from_context(fetch_token, context, &self.conf);
        emit_p2p_trace(
            "discover", "begin", &trace, chunk_id, None, None, None, None,
        );
        if let Some(chunk) = self
            .fetch_chunk_from_network_with_hedge(
                fetch_token,
                chunk_id,
                max_len,
                expected_mtime,
                trace.clone(),
            )
            .await
        {
            if let Some(mtime) = expected_mtime {
                if mtime > 0 && chunk.mtime > 0 && chunk.mtime != mtime {
                    self.network_mtime_mismatches
                        .fetch_add(1, Ordering::Relaxed);
                    emit_p2p_trace(
                        "validate",
                        "mtime_mismatch",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        None,
                        Some("mtime_mismatch"),
                    );
                    drop(flight_guard);
                    self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                    drop(permit);
                    return None;
                }
            }

            if self.conf.enable_checksum
                && (chunk.checksum.is_empty() || sha256_bytes(&chunk.data) != chunk.checksum)
            {
                self.network_checksum_failures
                    .fetch_add(1, Ordering::Relaxed);
                emit_p2p_trace(
                    "validate",
                    "checksum_mismatch",
                    &trace,
                    chunk_id,
                    None,
                    None,
                    Some(chunk.data.len()),
                    Some("checksum_mismatch"),
                );
                drop(flight_guard);
                self.cleanup_fetch_flight(&chunk_id, &flight_lock);
                drop(permit);
                return None;
            }
            emit_p2p_trace(
                "validate",
                "ok",
                &trace,
                chunk_id,
                None,
                None,
                Some(chunk.data.len()),
                None,
            );

            let mtime = if chunk.mtime > 0 {
                chunk.mtime
            } else {
                expected_mtime.unwrap_or(0)
            };
            let _ = self.cache_manager.put(chunk_id, chunk.data.clone(), mtime);
            emit_p2p_trace(
                "cache_put",
                "ok",
                &trace,
                chunk_id,
                None,
                None,
                Some(chunk.data.len()),
                None,
            );
            drop(flight_guard);
            self.cleanup_fetch_flight(&chunk_id, &flight_lock);
            drop(permit);
            return Some(chunk.data);
        }
        emit_p2p_trace(
            "transfer",
            "miss",
            &trace,
            chunk_id,
            None,
            None,
            None,
            Some("network_miss"),
        );
        drop(flight_guard);
        self.cleanup_fetch_flight(&chunk_id, &flight_lock);
        drop(permit);
        None
    }

    async fn fetch_chunk_from_network_with_hedge(
        &self,
        fetch_token: u64,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        trace: TraceLabels,
    ) -> Option<FetchedChunk> {
        let Some(delay) = hedge_delay(&self.conf) else {
            return self
                .fetch_chunk_from_network(fetch_token, chunk_id, max_len, expected_mtime, trace)
                .await;
        };

        let primary = self.fetch_chunk_from_network(
            fetch_token,
            chunk_id,
            max_len,
            expected_mtime,
            trace.clone(),
        );
        tokio::pin!(primary);
        let timer = sleep(delay);
        tokio::pin!(timer);
        tokio::select! {
            result = &mut primary => result,
            _ = &mut timer => {
                if let Some(limiter) = &self.qps_limiter {
                    if !limiter.acquire(self.transfer_timeout()).await {
                        return primary.await;
                    }
                }
                let secondary = self.fetch_chunk_from_network(
                    fetch_token,
                    chunk_id,
                    max_len,
                    expected_mtime,
                    trace,
                );
                tokio::pin!(secondary);
                tokio::select! {
                    primary_result = &mut primary => {
                        if primary_result.is_some() {
                            self.cancel_network_fetch(fetch_token);
                            return primary_result;
                        }
                        let secondary_result = secondary.await;
                        self.cancel_network_fetch(fetch_token);
                        secondary_result
                    },
                    secondary_result = &mut secondary => {
                        if secondary_result.is_some() {
                            self.cancel_network_fetch(fetch_token);
                            return secondary_result;
                        }
                        let primary_result = primary.await;
                        self.cancel_network_fetch(fetch_token);
                        primary_result
                    },
                }
            }
        }
    }

    async fn fetch_chunk_from_network(
        &self,
        fetch_token: u64,
        chunk_id: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        trace: TraceLabels,
    ) -> Option<FetchedChunk> {
        self.ensure_network_started();
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|network_tx| network_tx.clone())?;
        let timeout_budget = self.transfer_timeout();
        let deadline = Instant::now() + timeout_budget;
        let (response_tx, response_rx) = oneshot::channel();
        timeout(
            timeout_budget,
            tx.send(NetworkCommand::Fetch {
                fetch_token,
                chunk_id,
                max_len,
                expected_mtime,
                trace,
                response: response_tx,
            }),
        )
        .await
        .ok()?
        .ok()?;
        let wait_budget = deadline.saturating_duration_since(Instant::now());
        if wait_budget.is_zero() {
            return None;
        }
        timeout(wait_budget, response_rx).await.ok()?.ok()?
    }

    fn next_fetch_token(&self) -> u64 {
        self.fetch_tokens.fetch_add(1, Ordering::Relaxed)
    }

    fn cancel_network_fetch(&self, fetch_token: u64) {
        let _ = self.send_network_command(NetworkCommand::CancelFetch { fetch_token });
    }

    fn ensure_network_started(&self) {
        if self.runtime.is_none() || !self.is_enabled() {
            return;
        }
        let mut network_tx = match self.network_tx.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        if network_tx
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
        {
            return;
        }

        let (tx, rx) = mpsc::channel(self.conf.max_inflight_requests.max(64));
        *network_tx = Some(tx);
        drop(network_tx);

        let conf = self.conf.clone();
        let cache_manager = self.cache_manager.clone();
        let network_state = self.network_state.clone();
        let stats = self.stats();
        let future = async move {
            run_network_loop(conf, cache_manager, rx, network_state, stats).await;
        };

        if let Some(runtime) = &self.runtime {
            runtime.spawn(future);
        }
    }

    fn send_network_command(&self, cmd: NetworkCommand) -> bool {
        self.ensure_network_started();
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|network_tx| network_tx.clone());
        let Some(tx) = tx else {
            return false;
        };
        match tx.try_send(cmd) {
            Ok(_) => true,
            Err(TrySendError::Closed(_)) => false,
            Err(TrySendError::Full(_)) => false,
        }
    }

    fn stop_network(&self) {
        let tx = self
            .network_tx
            .lock()
            .ok()
            .and_then(|mut network_tx| network_tx.take());
        if let Some(tx) = tx {
            match tx.try_send(NetworkCommand::Stop) {
                Ok(_) => {}
                Err(TrySendError::Closed(_)) => {}
                Err(TrySendError::Full(_)) => {}
            }
        }
        self.network_state.active_peers.store(0, Ordering::Relaxed);
        self.network_state.mdns_peers.store(0, Ordering::Relaxed);
        self.network_state.dht_peers.store(0, Ordering::Relaxed);
        self.network_state
            .bootstrap_connected
            .store(0, Ordering::Relaxed);
    }

    fn transfer_timeout(&self) -> Duration {
        transfer_timeout(&self.conf)
    }

    fn stats(&self) -> Arc<PeerStats> {
        PEER_STATS
            .entry(self.peer_id.clone())
            .or_insert_with(|| Arc::new(PeerStats::default()))
            .clone()
    }

    fn fetch_flight_lock(&self, chunk_id: ChunkId) -> Arc<AsyncMutex<()>> {
        self.fetch_flights
            .entry(chunk_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    fn cleanup_fetch_flight(&self, chunk_id: &ChunkId, lock: &Arc<AsyncMutex<()>>) {
        if Arc::strong_count(lock) != 2 {
            return;
        }
        if let Some((_, current)) = self.fetch_flights.remove(chunk_id) {
            if !Arc::ptr_eq(&current, lock) {
                self.fetch_flights.insert(*chunk_id, current);
            }
        }
    }
}

impl Drop for P2pService {
    fn drop(&mut self) {
        if !self.conf.enable {
            return;
        }
        PEER_STATS.remove(&self.peer_id);
        self.stop_network();
    }
}

fn normalize_trace_label(value: Option<&str>) -> String {
    match value.map(str::trim) {
        Some(v) if !v.is_empty() => v.to_string(),
        _ => "unknown".to_string(),
    }
}

fn should_sample_trace(fetch_token: u64, conf: &ClientP2pConf) -> bool {
    if !conf.trace_enable {
        return false;
    }
    let rate = conf.trace_sample_rate;
    if rate <= 0.0 {
        return false;
    }
    if rate >= 1.0 {
        return true;
    }
    let hash = fetch_token.wrapping_mul(0x9e3779b97f4a7c15);
    let threshold = (rate * u64::MAX as f64) as u64;
    hash <= threshold
}

impl TraceLabels {
    fn from_context(
        fetch_token: u64,
        context: Option<&P2pReadTraceContext>,
        conf: &ClientP2pConf,
    ) -> Self {
        let sampled = should_sample_trace(fetch_token, conf);
        let tenant_id = normalize_trace_label(context.and_then(|ctx| ctx.tenant_id.as_deref()));
        let job_id = normalize_trace_label(context.and_then(|ctx| ctx.job_id.as_deref()));
        if !sampled {
            return Self {
                trace_id: String::new(),
                tenant_id,
                job_id,
                sampled,
            };
        }
        let trace_id = context
            .and_then(|ctx| ctx.trace_id.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("fetch-{}", fetch_token));
        Self {
            trace_id,
            tenant_id,
            job_id,
            sampled,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_p2p_trace(
    stage: &'static str,
    event: &'static str,
    labels: &TraceLabels,
    chunk_id: ChunkId,
    peer: Option<&PeerId>,
    elapsed_ms: Option<f64>,
    bytes: Option<usize>,
    reason: Option<&str>,
) {
    if !labels.sampled {
        return;
    }
    tracing::debug!(
        target: "curvine_client::p2p_trace",
        stage = stage,
        event = event,
        trace_id = %labels.trace_id,
        tenant_id = %labels.tenant_id,
        job_id = %labels.job_id,
        file_id = chunk_id.file_id,
        version_epoch = chunk_id.version_epoch,
        block_id = chunk_id.block_id,
        off = chunk_id.off,
        peer_id = ?peer,
        elapsed_ms = elapsed_ms.unwrap_or(0.0),
        bytes = bytes.unwrap_or(0),
        reason = reason.unwrap_or(""),
    );
}

fn handle_network_command(
    command: NetworkCommand,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &mut NetworkInitState,
    loop_state: &mut NetworkLoopState,
) -> bool {
    match command {
        NetworkCommand::Publish { chunk_id } => {
            if conf.enable_dht
                && swarm
                    .behaviour_mut()
                    .kad
                    .start_providing(chunk_record_key(chunk_id))
                    .is_ok()
            {
                loop_state.published_chunks.insert(chunk_id);
            }
        }
        NetworkCommand::Fetch {
            fetch_token,
            chunk_id,
            max_len,
            expected_mtime,
            trace,
            response,
        } => {
            let now = Instant::now();
            let deadline = now + loop_state.transfer_timeout;
            let cached = loop_state
                .provider_cache
                .get(&chunk_id)
                .and_then(|providers| (providers.expire_at > now).then_some(providers.clone()));
            if let Some(cached) = cached {
                emit_p2p_trace(
                    "discover",
                    "provider_cache",
                    &trace,
                    chunk_id,
                    None,
                    None,
                    Some(cached.peers.len()),
                    None,
                );
                if cached.peers.is_empty() {
                    emit_p2p_trace(
                        "discover",
                        "provider_empty",
                        &trace,
                        chunk_id,
                        None,
                        None,
                        None,
                        Some("provider_empty"),
                    );
                    let _ = response.send(None);
                    return true;
                }
                let candidates = build_candidates(
                    cached.peers,
                    &loop_state.connected_peers,
                    &loop_state.bootstrap_connected,
                    init.local_peer_id,
                    &loop_state.peer_ewma,
                    &loop_state.inflight_per_peer,
                );
                dispatch_fetch_request(
                    swarm,
                    PendingFetchRequest {
                        fetch_token,
                        chunk_id,
                        max_len,
                        expected_mtime,
                        trace,
                        response,
                        candidates,
                        active_peer: None,
                        started_at: now,
                        deadline,
                    },
                    init,
                    loop_state,
                    conf,
                );
                return true;
            }
            loop_state.provider_cache.remove(&chunk_id);

            if conf.enable_dht {
                emit_p2p_trace(
                    "discover",
                    "dht_query",
                    &trace,
                    chunk_id,
                    None,
                    None,
                    None,
                    None,
                );
                let query_id = swarm
                    .behaviour_mut()
                    .kad
                    .get_providers(chunk_record_key(chunk_id));
                loop_state.pending_queries.insert(
                    query_id,
                    PendingProviderQuery {
                        fetch_token,
                        chunk_id,
                        max_len,
                        expected_mtime,
                        trace,
                        response,
                        deadline: now + loop_state.discovery_timeout,
                    },
                );
            } else {
                emit_p2p_trace(
                    "discover",
                    "skip_dht",
                    &trace,
                    chunk_id,
                    None,
                    None,
                    None,
                    Some("dht_disabled"),
                );
                let candidates = build_candidates(
                    std::iter::empty::<PeerId>(),
                    &loop_state.connected_peers,
                    &loop_state.bootstrap_connected,
                    init.local_peer_id,
                    &loop_state.peer_ewma,
                    &loop_state.inflight_per_peer,
                );
                dispatch_fetch_request(
                    swarm,
                    PendingFetchRequest {
                        fetch_token,
                        chunk_id,
                        max_len,
                        expected_mtime,
                        trace,
                        response,
                        candidates,
                        active_peer: None,
                        started_at: now,
                        deadline,
                    },
                    init,
                    loop_state,
                    conf,
                );
            }
        }
        NetworkCommand::CancelFetch { fetch_token } => {
            cancel_pending_by_token(
                fetch_token,
                &mut loop_state.pending_queries,
                &mut loop_state.pending_requests,
                &mut loop_state.inflight_per_peer,
            );
        }
        NetworkCommand::UpdateRuntimePolicy {
            peer_whitelist: updated_peer_whitelist,
            tenant_whitelist: updated_tenant_whitelist,
            response,
        } => {
            if let Some(updated_peer_whitelist) = updated_peer_whitelist {
                let mut next_peer_whitelist = parse_peer_whitelist(&updated_peer_whitelist);
                next_peer_whitelist.extend(init.bootstrap_peer_ids.iter().copied());
                init.peer_whitelist = next_peer_whitelist;

                let to_disconnect: Vec<PeerId> = loop_state
                    .connected_peers
                    .iter()
                    .copied()
                    .filter(|peer| {
                        !is_peer_allowed(*peer, init.local_peer_id, &init.peer_whitelist)
                    })
                    .collect();
                for peer in to_disconnect {
                    loop_state.connected_peers.remove(&peer);
                    loop_state.mdns_peers.remove(&peer);
                    loop_state.dht_peers.remove(&peer);
                    loop_state.bootstrap_connected.remove(&peer);
                    loop_state.inflight_per_peer.remove(&peer);
                    loop_state.peer_ewma.remove(&peer);
                    for providers in loop_state.provider_cache.values_mut() {
                        providers.peers.remove(&peer);
                    }
                    let _ = swarm.disconnect_peer_id(peer);
                }
            }
            if let Some(updated_tenant_whitelist) = updated_tenant_whitelist {
                init.tenant_whitelist = Arc::new(parse_tenant_whitelist(&updated_tenant_whitelist));
            }
            let _ = response.send(true);
        }
        NetworkCommand::Stop => {
            if conf.enable_dht {
                for chunk_id in loop_state.published_chunks.drain() {
                    swarm
                        .behaviour_mut()
                        .kad
                        .stop_providing(&chunk_record_key(chunk_id));
                }
            }
            return false;
        }
    }
    true
}

struct SwarmEventCtx<'a> {
    swarm: &'a mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &'a ClientP2pConf,
    cache_manager: &'a Arc<CacheManager>,
    init: &'a NetworkInitState,
    loop_state: &'a mut NetworkLoopState,
    incoming_tx: &'a mpsc::Sender<PreparedIncomingResponse>,
    stats: &'a Arc<PeerStats>,
}

fn handle_swarm_event(event: SwarmEvent<P2pNetworkEvent>, ctx: &mut SwarmEventCtx<'_>) -> bool {
    match event {
        SwarmEvent::Behaviour(P2pNetworkEvent::RequestResponse(event)) => {
            let mut rr_ctx = RequestResponseEventCtx {
                swarm: ctx.swarm,
                cache_manager: ctx.cache_manager,
                conf: ctx.conf,
                init: ctx.init,
                loop_state: ctx.loop_state,
                incoming_tx: ctx.incoming_tx,
                stats: ctx.stats,
            };
            handle_request_response_event(event, &mut rr_ctx);
        }
        SwarmEvent::Behaviour(P2pNetworkEvent::Mdns(event)) => {
            if ctx.conf.enable_mdns {
                handle_mdns_event(
                    event,
                    ctx.swarm,
                    ctx.conf.enable_dht,
                    ctx.init.local_peer_id,
                    &ctx.init.peer_whitelist,
                    &mut ctx.loop_state.mdns_peers,
                );
            }
        }
        SwarmEvent::Behaviour(P2pNetworkEvent::Kad(event)) => {
            if ctx.conf.enable_dht {
                handle_kad_event(event, ctx.swarm, ctx.conf, ctx.init, ctx.loop_state);
            }
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            ..
        } => {
            if !handle_connection_established(
                ctx.swarm,
                peer_id,
                connection_id,
                ctx.init,
                ctx.loop_state,
                ctx.conf.max_active_peers,
            ) {
                return false;
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            handle_connection_closed(peer_id, ctx.loop_state);
        }
        SwarmEvent::NewListenAddr { .. } => {}
        _ => {}
    }
    true
}

fn handle_mdns_event(
    event: mdns::Event,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    enable_dht: bool,
    local_peer_id: PeerId,
    peer_whitelist: &HashSet<PeerId>,
    mdns_peers: &mut HashSet<PeerId>,
) {
    match event {
        mdns::Event::Discovered(entries) => {
            for (peer, addr) in entries {
                if !is_peer_allowed(peer, local_peer_id, peer_whitelist) || peer == local_peer_id {
                    continue;
                }
                mdns_peers.insert(peer);
                if enable_dht {
                    swarm
                        .behaviour_mut()
                        .kad
                        .add_address(&peer, strip_peer_id(&addr));
                }
                let _ = swarm.dial(addr);
            }
        }
        mdns::Event::Expired(entries) => {
            for (peer, _) in entries {
                mdns_peers.remove(&peer);
            }
        }
    }
}

fn handle_kad_event(
    event: kad::Event,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
) {
    match event {
        kad::Event::RoutingUpdated { peer, .. } => {
            loop_state.dht_peers.insert(peer);
        }
        kad::Event::OutboundQueryProgressed {
            id, result, step, ..
        } => {
            if let Some(pending) = loop_state.pending_queries.remove(&id) {
                let (providers, keep_waiting) = parse_provider_query_outcome(
                    result,
                    step.last,
                    init.local_peer_id,
                    &init.peer_whitelist,
                );

                if keep_waiting {
                    loop_state.pending_queries.insert(id, pending);
                } else {
                    finalize_provider_query(swarm, pending, providers, init, loop_state, conf);
                }
            }
        }
        _ => {}
    }
}

fn parse_provider_query_outcome(
    result: kad::QueryResult,
    step_last: bool,
    local_peer_id: PeerId,
    peer_whitelist: &HashSet<PeerId>,
) -> (HashSet<PeerId>, bool) {
    let mut providers = HashSet::new();
    let mut keep_waiting = !step_last;
    match result {
        kad::QueryResult::GetProviders(Ok(ok)) => match ok {
            kad::GetProvidersOk::FoundProviders {
                providers: found, ..
            } => {
                providers.extend(found);
                providers.retain(|peer| is_peer_allowed(*peer, local_peer_id, peer_whitelist));
                keep_waiting = false;
            }
            kad::GetProvidersOk::FinishedWithNoAdditionalRecord { .. } => {}
        },
        kad::QueryResult::GetProviders(Err(_)) => {
            keep_waiting = false;
        }
        _ => {
            keep_waiting = false;
        }
    }
    (providers, keep_waiting)
}

fn finalize_provider_query(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    pending: PendingProviderQuery,
    providers: HashSet<PeerId>,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    conf: &ClientP2pConf,
) {
    emit_p2p_trace(
        "discover",
        "dht_result",
        &pending.trace,
        pending.chunk_id,
        None,
        None,
        Some(providers.len()),
        None,
    );
    loop_state.provider_cache.insert(
        pending.chunk_id,
        CachedProviders {
            peers: providers.clone(),
            expire_at: Instant::now()
                + if providers.is_empty() {
                    negative_provider_cache_ttl_from_discovery(loop_state.discovery_timeout)
                } else {
                    loop_state.provider_cache_ttl
                },
        },
    );
    let candidates = build_candidates(
        providers,
        &loop_state.connected_peers,
        &loop_state.bootstrap_connected,
        init.local_peer_id,
        &loop_state.peer_ewma,
        &loop_state.inflight_per_peer,
    );
    dispatch_fetch_request(
        swarm,
        PendingFetchRequest {
            fetch_token: pending.fetch_token,
            chunk_id: pending.chunk_id,
            max_len: pending.max_len,
            expected_mtime: pending.expected_mtime,
            trace: pending.trace.clone(),
            response: pending.response,
            candidates,
            active_peer: None,
            started_at: Instant::now(),
            deadline: Instant::now() + loop_state.transfer_timeout,
        },
        init,
        loop_state,
        conf,
    );
}

fn negative_provider_cache_ttl_from_discovery(discovery_timeout: Duration) -> Duration {
    discovery_timeout
}

fn handle_connection_established(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    peer_id: PeerId,
    connection_id: libp2p::swarm::ConnectionId,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    max_active_peers: usize,
) -> bool {
    if !is_peer_allowed(peer_id, init.local_peer_id, &init.peer_whitelist)
        || !should_accept_peer(
            peer_id,
            &loop_state.connected_peers,
            &init.bootstrap_peer_ids,
            max_active_peers,
        )
    {
        let _ = swarm.close_connection(connection_id);
        return false;
    }
    loop_state.connected_peers.insert(peer_id);
    if init.bootstrap_peer_ids.contains(&peer_id) {
        loop_state.bootstrap_connected.insert(peer_id);
    }
    true
}

fn handle_connection_closed(peer_id: PeerId, loop_state: &mut NetworkLoopState) {
    loop_state.connected_peers.remove(&peer_id);
    loop_state.bootstrap_connected.remove(&peer_id);
    loop_state.inflight_per_peer.remove(&peer_id);
    loop_state.peer_ewma.remove(&peer_id);
    for providers in loop_state.provider_cache.values_mut() {
        providers.peers.remove(&peer_id);
    }
}

fn sync_network_state_metrics(network_state: &Arc<NetworkState>, loop_state: &NetworkLoopState) {
    network_state.active_peers.store(
        loop_state.connected_peers.len().saturating_add(1) as u64,
        Ordering::Relaxed,
    );
    network_state
        .mdns_peers
        .store(loop_state.mdns_peers.len() as u64, Ordering::Relaxed);
    network_state
        .dht_peers
        .store(loop_state.dht_peers.len() as u64, Ordering::Relaxed);
    network_state.bootstrap_connected.store(
        loop_state.bootstrap_connected.len() as u64,
        Ordering::Relaxed,
    );
}

struct NetworkInitState {
    local_peer_id: PeerId,
    bootstrap_peer_ids: HashSet<PeerId>,
    peer_whitelist: HashSet<PeerId>,
    tenant_whitelist: Arc<HashSet<String>>,
}

struct NetworkLoopState {
    connected_peers: HashSet<PeerId>,
    mdns_peers: HashSet<PeerId>,
    dht_peers: HashSet<PeerId>,
    bootstrap_connected: HashSet<PeerId>,
    provider_cache: HashMap<ChunkId, CachedProviders>,
    provider_cache_ttl: Duration,
    discovery_timeout: Duration,
    transfer_timeout: Duration,
    published_chunks: HashSet<ChunkId>,
    pending_queries: HashMap<kad::QueryId, PendingProviderQuery>,
    pending_requests: HashMap<request_response::OutboundRequestId, PendingFetchRequest>,
    pending_incoming: HashMap<u64, PendingIncomingResponse>,
    next_incoming_id: u64,
    inflight_per_peer: HashMap<PeerId, usize>,
    peer_ewma: HashMap<PeerId, PeerEwma>,
}

impl NetworkLoopState {
    fn new(conf: &ClientP2pConf) -> Self {
        Self {
            connected_peers: HashSet::new(),
            mdns_peers: HashSet::new(),
            dht_peers: HashSet::new(),
            bootstrap_connected: HashSet::new(),
            provider_cache: HashMap::new(),
            provider_cache_ttl: provider_cache_ttl(conf),
            discovery_timeout: discovery_timeout(conf),
            transfer_timeout: transfer_timeout(conf),
            published_chunks: HashSet::new(),
            pending_queries: HashMap::new(),
            pending_requests: HashMap::new(),
            pending_incoming: HashMap::new(),
            next_incoming_id: 1,
            inflight_per_peer: HashMap::new(),
            peer_ewma: HashMap::new(),
        }
    }
}

fn init_network_identity_and_policy(
    conf: &ClientP2pConf,
    swarm: &libp2p::Swarm<P2pNetworkBehaviour>,
    network_state: &Arc<NetworkState>,
) -> NetworkInitState {
    let local_peer_id = *swarm.local_peer_id();
    let _ = network_state.local_peer_id.set(local_peer_id.to_string());
    let bootstrap_peers = parse_bootstrap_peers(&conf.bootstrap_peers);
    let bootstrap_peer_ids: HashSet<PeerId> = bootstrap_peers.iter().map(|v| v.0).collect();
    let mut peer_whitelist = parse_peer_whitelist(&conf.peer_whitelist);
    peer_whitelist.extend(bootstrap_peer_ids.iter().copied());
    let tenant_whitelist = Arc::new(parse_tenant_whitelist(&conf.tenant_whitelist));
    NetworkInitState {
        local_peer_id,
        bootstrap_peer_ids,
        peer_whitelist,
        tenant_whitelist,
    }
}

fn bootstrap_and_listen_swarm(
    conf: &ClientP2pConf,
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
) {
    for entry in &conf.bootstrap_peers {
        let Ok(addr) = Multiaddr::from_str(entry) else {
            continue;
        };
        let Some(peer_id) = addr.iter().find_map(|protocol| match protocol {
            Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        }) else {
            continue;
        };
        if conf.enable_dht {
            swarm
                .behaviour_mut()
                .kad
                .add_address(&peer_id, strip_peer_id(&addr));
        }
        if let Err(e) = swarm.dial(addr.clone()) {
            debug!("failed to dial bootstrap {} {}: {}", peer_id, addr, e);
        }
    }

    let mut has_listen = false;
    for addr in &conf.listen_addrs {
        match Multiaddr::from_str(addr) {
            Ok(addr) => {
                if swarm.listen_on(addr).is_ok() {
                    has_listen = true;
                }
            }
            Err(e) => warn!("invalid p2p listen addr {}: {}", addr, e),
        }
    }
    if !has_listen {
        if let Ok(addr) = Multiaddr::from_str("/ip4/0.0.0.0/tcp/0") {
            let _ = swarm.listen_on(addr);
        }
    }
}

async fn run_network_loop(
    conf: ClientP2pConf,
    cache_manager: Arc<CacheManager>,
    mut command_rx: mpsc::Receiver<NetworkCommand>,
    network_state: Arc<NetworkState>,
    stats: Arc<PeerStats>,
) {
    let (incoming_tx, mut incoming_rx) =
        mpsc::channel::<PreparedIncomingResponse>(conf.max_inflight_requests.max(64));
    let mut swarm = match build_swarm(&conf) {
        Ok(v) => v,
        Err(e) => {
            warn!("failed to start libp2p swarm: {}", e);
            return;
        }
    };

    let mut init = init_network_identity_and_policy(&conf, &swarm, &network_state);
    bootstrap_and_listen_swarm(&conf, &mut swarm);
    let mut loop_state = NetworkLoopState::new(&conf);
    let mut ticker = tokio::time::interval(maintenance_tick_interval(&conf));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                expire_pending_queries(&mut loop_state.pending_queries);
                expire_pending_requests(&mut loop_state.pending_requests, &mut loop_state.inflight_per_peer);
                expire_pending_incoming(&mut loop_state.pending_incoming);
                expire_provider_cache(&mut loop_state.provider_cache, Instant::now());
            }
            incoming = incoming_rx.recv() => {
                let Some(incoming) = incoming else { continue; };
                let Some(pending) = loop_state.pending_incoming.remove(&incoming.id) else {
                    continue;
                };
                if swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(pending.channel, incoming.response)
                    .is_ok()
                    && incoming.sent > 0
                {
                    stats.bytes_sent.fetch_add(incoming.sent, Ordering::Relaxed);
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else { break; };
                if !handle_network_command(
                    command,
                    &mut swarm,
                    &conf,
                    &mut init,
                    &mut loop_state,
                ) {
                    break;
                }
            }
            event = swarm.select_next_some() => {
                if let SwarmEvent::NewListenAddr { address, .. } = &event {
                    let _ = network_state.listen_addr.set(address.to_string());
                }
                let mut ctx = SwarmEventCtx {
                    swarm: &mut swarm,
                    conf: &conf,
                    cache_manager: &cache_manager,
                    init: &init,
                    loop_state: &mut loop_state,
                    incoming_tx: &incoming_tx,
                    stats: &stats,
                };
                if !handle_swarm_event(event, &mut ctx) {
                    continue;
                }
                sync_network_state_metrics(&network_state, &loop_state);
            }
        }
    }
}

fn build_swarm(
    conf: &ClientP2pConf,
) -> Result<libp2p::Swarm<P2pNetworkBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    let req_config = request_response::Config::default()
        .with_request_timeout(transfer_timeout(conf))
        .with_max_concurrent_streams(conf.max_inflight_requests.max(1));
    let identity = load_or_generate_identity(conf)?;

    let swarm = SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();
            let mut kad_config = kad::Config::new(kad::PROTOCOL_NAME);
            kad_config
                .set_query_timeout(discovery_timeout(conf))
                .set_provider_record_ttl(Some(conf.provider_ttl))
                .set_provider_publication_interval(Some(conf.provider_publish_interval));
            let mut kad_behaviour = kad::Behaviour::with_config(
                local_peer_id,
                kad::store::MemoryStore::new(local_peer_id),
                kad_config,
            );
            kad_behaviour.set_mode(Some(kad::Mode::Server));

            let request_response_behaviour = request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new("/curvine/p2p/chunk/1"),
                    request_response::ProtocolSupport::Full,
                )],
                req_config.clone(),
            );
            let mdns_behaviour = if conf.enable_mdns {
                Toggle::from(Some(mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    local_peer_id,
                )?))
            } else {
                Toggle::from(None)
            };

            Ok(P2pNetworkBehaviour {
                request_response: request_response_behaviour,
                mdns: mdns_behaviour,
                kad: kad_behaviour,
            })
        })?
        .with_swarm_config(std::convert::identity)
        .with_connection_timeout(connect_timeout(conf))
        .build();
    Ok(swarm)
}

fn identity_key_path(conf: &ClientP2pConf) -> PathBuf {
    if conf.identity_key_path.is_empty() {
        Path::new(&conf.cache_dir).join("peer_identity.key")
    } else {
        PathBuf::from(&conf.identity_key_path)
    }
}

fn load_or_generate_identity(
    conf: &ClientP2pConf,
) -> Result<identity::Keypair, Box<dyn std::error::Error + Send + Sync>> {
    let key_path = identity_key_path(conf);
    if let Ok(encoded) = fs::read(&key_path) {
        if let Ok(identity) = identity::Keypair::from_protobuf_encoding(&encoded) {
            return Ok(identity);
        }
    }
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let identity = identity::Keypair::generate_ed25519();
    let encoded = identity.to_protobuf_encoding()?;
    fs::write(key_path, encoded)?;
    Ok(identity)
}

fn runtime_policy_version_path(conf: &ClientP2pConf) -> PathBuf {
    Path::new(&conf.cache_dir).join("runtime_policy.version")
}

fn load_runtime_policy_version(conf: &ClientP2pConf) -> u64 {
    fs::read_to_string(runtime_policy_version_path(conf))
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or_default()
}

fn persist_runtime_policy_version(conf: &ClientP2pConf, policy_version: u64) -> bool {
    let path = runtime_policy_version_path(conf);
    path.parent()
        .is_none_or(|parent| fs::create_dir_all(parent).is_ok())
        && fs::write(path, policy_version.to_string()).is_ok()
}

fn parse_bootstrap_peers(peers: &[String]) -> Vec<(PeerId, Multiaddr)> {
    peers
        .iter()
        .filter_map(|entry| {
            let addr = Multiaddr::from_str(entry).ok()?;
            let peer_id = addr.iter().find_map(|protocol| match protocol {
                Protocol::P2p(peer_id) => Some(peer_id),
                _ => None,
            })?;
            Some((peer_id, addr))
        })
        .collect()
}

fn chunk_record_key(chunk_id: ChunkId) -> kad::RecordKey {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&chunk_id.file_id.to_be_bytes());
    key[8..16].copy_from_slice(&chunk_id.version_epoch.to_be_bytes());
    key[16..24].copy_from_slice(&chunk_id.block_id.to_be_bytes());
    key[24..32].copy_from_slice(&chunk_id.off.to_be_bytes());
    kad::RecordKey::new(&key)
}

fn strip_peer_id(addr: &Multiaddr) -> Multiaddr {
    let mut result = Multiaddr::empty();
    for protocol in addr.iter() {
        if matches!(protocol, Protocol::P2p(_)) {
            continue;
        }
        result.push(protocol);
    }
    result
}

fn should_accept_peer(
    peer_id: PeerId,
    connected_peers: &HashSet<PeerId>,
    bootstrap_peer_ids: &HashSet<PeerId>,
    max_active_peers: usize,
) -> bool {
    if connected_peers.contains(&peer_id) || bootstrap_peer_ids.contains(&peer_id) {
        return true;
    }
    max_active_peers == 0 || connected_peers.len() < max_active_peers
}

fn parse_peer_whitelist(peers: &[String]) -> HashSet<PeerId> {
    peers
        .iter()
        .filter_map(|entry| PeerId::from_str(entry).ok())
        .collect()
}

fn parse_tenant_whitelist(tenants: &[String]) -> HashSet<String> {
    tenants
        .iter()
        .map(|tenant| tenant.trim())
        .filter(|tenant| !tenant.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn is_peer_allowed(
    peer_id: PeerId,
    local_peer_id: PeerId,
    peer_whitelist: &HashSet<PeerId>,
) -> bool {
    peer_id == local_peer_id || peer_whitelist.is_empty() || peer_whitelist.contains(&peer_id)
}

fn is_tenant_allowed(tenant_id: &str, tenant_whitelist: &HashSet<String>) -> bool {
    if tenant_whitelist.is_empty() {
        return true;
    }
    let tenant = tenant_id.trim();
    !tenant.is_empty() && tenant_whitelist.contains(tenant)
}

fn peer_dispatch_window(
    peer_id: PeerId,
    max_inflight_per_peer: usize,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
) -> usize {
    if max_inflight_per_peer == 0 {
        return 0;
    }
    let Some(stats) = peer_ewma.get(&peer_id).copied() else {
        return max_inflight_per_peer;
    };
    let factor = if stats.failure_ratio >= 0.5 {
        0.2
    } else if stats.failure_ratio >= 0.2 {
        0.5
    } else if stats.latency_ms >= 500.0 {
        0.3
    } else if stats.latency_ms >= 250.0 {
        0.6
    } else if stats.throughput_mb_s < 1.0 {
        0.7
    } else {
        1.0
    };
    (max_inflight_per_peer as f64 * factor)
        .ceil()
        .max(1.0)
        .min(max_inflight_per_peer as f64) as usize
}

fn can_dispatch_peer(
    peer_id: PeerId,
    inflight_per_peer: &HashMap<PeerId, usize>,
    max_inflight_per_peer: usize,
    adaptive_window: bool,
    min_inflight_per_peer: usize,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
) -> bool {
    if max_inflight_per_peer == 0 {
        return true;
    }
    let min_window = min_inflight_per_peer.max(1).min(max_inflight_per_peer);
    let adaptive = if adaptive_window {
        peer_dispatch_window(peer_id, max_inflight_per_peer, peer_ewma)
    } else {
        max_inflight_per_peer
    };
    let limit = adaptive.max(min_window);
    inflight_per_peer.get(&peer_id).copied().unwrap_or(0) < limit
}

fn release_peer_inflight(
    pending: &PendingFetchRequest,
    inflight_per_peer: &mut HashMap<PeerId, usize>,
) {
    let Some(peer_id) = pending.active_peer else {
        return;
    };
    let Some(count) = inflight_per_peer.get_mut(&peer_id) else {
        return;
    };
    if *count <= 1 {
        inflight_per_peer.remove(&peer_id);
    } else {
        *count -= 1;
    }
}

fn build_candidates<I>(
    providers: I,
    connected_peers: &HashSet<PeerId>,
    bootstrap_connected: &HashSet<PeerId>,
    local_peer_id: PeerId,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
    inflight_per_peer: &HashMap<PeerId, usize>,
) -> VecDeque<PeerId>
where
    I: IntoIterator<Item = PeerId>,
{
    let mut seen = HashSet::new();
    let mut ordered = VecDeque::new();

    for peer in providers {
        if peer != local_peer_id && seen.insert(peer) {
            ordered.push_back(peer);
        }
    }

    for peer in connected_peers {
        if *peer != local_peer_id && seen.insert(*peer) {
            ordered.push_back(*peer);
        }
    }

    for peer in bootstrap_connected {
        if *peer != local_peer_id && seen.insert(*peer) {
            ordered.push_back(*peer);
        }
    }
    let mut ranked: Vec<(PeerId, f64)> = ordered
        .into_iter()
        .map(|peer_id| {
            (
                peer_id,
                candidate_score(peer_id, peer_ewma, inflight_per_peer),
            )
        })
        .collect();
    ranked.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
    ranked.into_iter().map(|(peer_id, _)| peer_id).collect()
}

fn candidate_score(
    peer_id: PeerId,
    peer_ewma: &HashMap<PeerId, PeerEwma>,
    inflight_per_peer: &HashMap<PeerId, usize>,
) -> f64 {
    let stats = peer_ewma.get(&peer_id).copied().unwrap_or_default();
    let inflight = inflight_per_peer.get(&peer_id).copied().unwrap_or(0) as f64;
    let throughput_penalty = 200.0 / stats.throughput_mb_s.max(0.1);
    stats.latency_ms + stats.failure_ratio * 1000.0 + throughput_penalty + inflight * 10.0
}

fn dispatch_fetch_request(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    mut pending: PendingFetchRequest,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    conf: &ClientP2pConf,
) {
    while let Some(peer_id) = pending.candidates.pop_front() {
        if !is_peer_allowed(peer_id, init.local_peer_id, &init.peer_whitelist)
            || peer_id == init.local_peer_id
        {
            continue;
        }
        if !can_dispatch_peer(
            peer_id,
            &loop_state.inflight_per_peer,
            conf.max_inflight_per_peer,
            conf.adaptive_window,
            conf.min_inflight_per_peer,
            &loop_state.peer_ewma,
        ) {
            continue;
        }
        emit_p2p_trace(
            "connect",
            "dispatch",
            &pending.trace,
            pending.chunk_id,
            Some(&peer_id),
            None,
            None,
            None,
        );
        let request_id = swarm.behaviour_mut().request_response.send_request(
            &peer_id,
            ChunkFetchRequest {
                file_id: pending.chunk_id.file_id,
                version_epoch: pending.chunk_id.version_epoch,
                block_id: pending.chunk_id.block_id,
                off: pending.chunk_id.off,
                len: pending.max_len,
                expected_mtime: pending.expected_mtime.unwrap_or(0),
                tenant_id: pending.trace.tenant_id.clone(),
            },
        );
        pending.active_peer = Some(peer_id);
        *loop_state.inflight_per_peer.entry(peer_id).or_insert(0) += 1;
        loop_state.pending_requests.insert(request_id, pending);
        return;
    }
    emit_p2p_trace(
        "connect",
        "no_candidate",
        &pending.trace,
        pending.chunk_id,
        None,
        None,
        None,
        Some("no_candidate"),
    );
    let _ = pending.response.send(None);
}

struct RequestResponseEventCtx<'a> {
    swarm: &'a mut libp2p::Swarm<P2pNetworkBehaviour>,
    cache_manager: &'a Arc<CacheManager>,
    conf: &'a ClientP2pConf,
    init: &'a NetworkInitState,
    loop_state: &'a mut NetworkLoopState,
    incoming_tx: &'a mpsc::Sender<PreparedIncomingResponse>,
    stats: &'a Arc<PeerStats>,
}

fn handle_request_response_event(
    event: request_response::Event<ChunkFetchRequest, ChunkFetchResponse>,
    ctx: &mut RequestResponseEventCtx<'_>,
) {
    match event {
        request_response::Event::Message { peer, message, .. } => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let incoming_id = ctx.loop_state.next_incoming_id;
                ctx.loop_state.next_incoming_id = ctx.loop_state.next_incoming_id.wrapping_add(1);
                ctx.loop_state.pending_incoming.insert(
                    incoming_id,
                    PendingIncomingResponse {
                        channel,
                        deadline: Instant::now() + ctx.loop_state.transfer_timeout,
                    },
                );
                handle_incoming_fetch_request(
                    ctx.cache_manager.clone(),
                    Arc::clone(&ctx.init.tenant_whitelist),
                    request,
                    incoming_id,
                    ctx.incoming_tx.clone(),
                );
            }
            request_response::Message::Response {
                request_id,
                response,
            } => {
                let mut response_ctx = IncomingFetchResponseCtx {
                    swarm: ctx.swarm,
                    conf: ctx.conf,
                    init: ctx.init,
                    loop_state: ctx.loop_state,
                    stats: ctx.stats,
                };
                handle_incoming_fetch_response(&mut response_ctx, peer, request_id, response);
            }
        },
        request_response::Event::OutboundFailure {
            request_id, peer, ..
        } => handle_outbound_fetch_failure(
            ctx.swarm,
            ctx.conf,
            ctx.init,
            ctx.loop_state,
            peer,
            request_id,
        ),
        _ => {}
    }
}

fn handle_incoming_fetch_request(
    cache_manager: Arc<CacheManager>,
    tenant_whitelist: Arc<HashSet<String>>,
    request: ChunkFetchRequest,
    incoming_id: u64,
    incoming_tx: mpsc::Sender<PreparedIncomingResponse>,
) {
    tokio::spawn(async move {
        let prepared = tokio::task::spawn_blocking(move || {
            prepare_incoming_fetch_response(
                &cache_manager,
                tenant_whitelist.as_ref(),
                request,
                incoming_id,
            )
        })
        .await
        .ok();
        if let Some(prepared) = prepared {
            let _ = incoming_tx.send(prepared).await;
        }
    });
}

fn prepare_incoming_fetch_response(
    cache_manager: &CacheManager,
    tenant_whitelist: &HashSet<String>,
    request: ChunkFetchRequest,
    incoming_id: u64,
) -> PreparedIncomingResponse {
    let chunk_id = ChunkId::with_version(
        request.file_id,
        request.version_epoch,
        request.block_id,
        request.off,
    );
    let expected_mtime = (request.expected_mtime > 0).then_some(request.expected_mtime);
    let (found, data, mtime, checksum) = if is_tenant_allowed(&request.tenant_id, tenant_whitelist)
    {
        let cache = cache_manager.get(chunk_id, request.len, expected_mtime);
        match cache.chunk {
            Some(chunk) => (true, chunk.data, chunk.mtime, chunk.checksum),
            None => (false, Bytes::new(), 0, Bytes::new()),
        }
    } else {
        (false, Bytes::new(), 0, Bytes::new())
    };
    let sent = data.len() as u64;
    PreparedIncomingResponse {
        id: incoming_id,
        response: ChunkFetchResponse {
            found,
            mtime,
            checksum,
            data,
        },
        sent,
    }
}

struct IncomingFetchResponseCtx<'a> {
    swarm: &'a mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &'a ClientP2pConf,
    init: &'a NetworkInitState,
    loop_state: &'a mut NetworkLoopState,
    stats: &'a Arc<PeerStats>,
}

fn handle_incoming_fetch_response(
    ctx: &mut IncomingFetchResponseCtx<'_>,
    peer: PeerId,
    request_id: request_response::OutboundRequestId,
    response: ChunkFetchResponse,
) {
    if let Some(mut pending) = ctx.loop_state.pending_requests.remove(&request_id) {
        release_peer_inflight(&pending, &mut ctx.loop_state.inflight_per_peer);
        pending.active_peer = None;
        let response_len = response.data.len();
        if response.found && !response.data.is_empty() {
            let mut bytes = response.data;
            if bytes.len() > pending.max_len {
                bytes = bytes.slice(0..pending.max_len);
            }
            if !bytes.is_empty() {
                let elapsed_ms = pending.started_at.elapsed().as_secs_f64() * 1000.0;
                ctx.stats
                    .bytes_recv
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                ctx.stats.latency_us_total.fetch_add(
                    pending
                        .started_at
                        .elapsed()
                        .as_micros()
                        .min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                ctx.stats.latency_samples.fetch_add(1, Ordering::Relaxed);
                ctx.loop_state
                    .peer_ewma
                    .entry(peer)
                    .or_default()
                    .observe_success(elapsed_ms, bytes.len());
                emit_p2p_trace(
                    "transfer",
                    "response_ok",
                    &pending.trace,
                    pending.chunk_id,
                    Some(&peer),
                    Some(elapsed_ms),
                    Some(bytes.len()),
                    None,
                );
                let _ = pending.response.send(Some(FetchedChunk {
                    data: bytes,
                    mtime: response.mtime,
                    checksum: response.checksum,
                }));
                return;
            }
        }
        emit_p2p_trace(
            "transfer",
            "response_retry",
            &pending.trace,
            pending.chunk_id,
            Some(&peer),
            Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
            Some(response_len),
            Some("response_not_found_or_empty"),
        );
        ctx.loop_state
            .peer_ewma
            .entry(peer)
            .or_default()
            .observe_failure();
        pending.started_at = Instant::now();
        dispatch_fetch_request(ctx.swarm, pending, ctx.init, ctx.loop_state, ctx.conf);
    }
}

fn handle_outbound_fetch_failure(
    swarm: &mut libp2p::Swarm<P2pNetworkBehaviour>,
    conf: &ClientP2pConf,
    init: &NetworkInitState,
    loop_state: &mut NetworkLoopState,
    peer: PeerId,
    request_id: request_response::OutboundRequestId,
) {
    if let Some(mut pending) = loop_state.pending_requests.remove(&request_id) {
        release_peer_inflight(&pending, &mut loop_state.inflight_per_peer);
        pending.active_peer = None;
        emit_p2p_trace(
            "transfer",
            "outbound_failure",
            &pending.trace,
            pending.chunk_id,
            Some(&peer),
            Some(pending.started_at.elapsed().as_secs_f64() * 1000.0),
            None,
            Some("outbound_failure"),
        );
        loop_state
            .peer_ewma
            .entry(peer)
            .or_default()
            .observe_failure();
        pending.started_at = Instant::now();
        dispatch_fetch_request(swarm, pending, init, loop_state, conf);
    }
}

fn cancel_pending_by_token(
    fetch_token: u64,
    pending_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>,
    pending_requests: &mut HashMap<request_response::OutboundRequestId, PendingFetchRequest>,
    inflight_per_peer: &mut HashMap<PeerId, usize>,
) {
    let query_ids: Vec<kad::QueryId> = pending_queries
        .iter()
        .filter_map(|(id, pending)| (pending.fetch_token == fetch_token).then_some(*id))
        .collect();
    for query_id in query_ids {
        if let Some(pending) = pending_queries.remove(&query_id) {
            emit_p2p_trace(
                "discover",
                "cancelled",
                &pending.trace,
                pending.chunk_id,
                None,
                None,
                None,
                Some("cancelled"),
            );
            let _ = pending.response.send(None);
        }
    }

    let request_ids: Vec<request_response::OutboundRequestId> = pending_requests
        .iter()
        .filter_map(|(id, pending)| (pending.fetch_token == fetch_token).then_some(*id))
        .collect();
    for request_id in request_ids {
        if let Some(pending) = pending_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "cancelled",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("cancelled"),
            );
            let _ = pending.response.send(None);
        }
    }
}

fn expire_pending_queries(pending_queries: &mut HashMap<kad::QueryId, PendingProviderQuery>) {
    let now = Instant::now();
    let expired_ids: Vec<kad::QueryId> = pending_queries
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for query_id in expired_ids {
        if let Some(pending) = pending_queries.remove(&query_id) {
            emit_p2p_trace(
                "discover",
                "timeout",
                &pending.trace,
                pending.chunk_id,
                None,
                None,
                None,
                Some("discover_timeout"),
            );
            let _ = pending.response.send(None);
        }
    }
}

fn expire_pending_requests(
    pending_requests: &mut HashMap<request_response::OutboundRequestId, PendingFetchRequest>,
    inflight_per_peer: &mut HashMap<PeerId, usize>,
) {
    let now = Instant::now();
    let expired_ids: Vec<request_response::OutboundRequestId> = pending_requests
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for request_id in expired_ids {
        if let Some(pending) = pending_requests.remove(&request_id) {
            release_peer_inflight(&pending, inflight_per_peer);
            emit_p2p_trace(
                "transfer",
                "timeout",
                &pending.trace,
                pending.chunk_id,
                pending.active_peer.as_ref(),
                None,
                None,
                Some("transfer_timeout"),
            );
            let _ = pending.response.send(None);
        }
    }
}

fn expire_pending_incoming(pending_incoming: &mut HashMap<u64, PendingIncomingResponse>) {
    let now = Instant::now();
    let expired_ids: Vec<u64> = pending_incoming
        .iter()
        .filter_map(|(id, pending)| (pending.deadline <= now).then_some(*id))
        .collect();
    for pending_id in expired_ids {
        pending_incoming.remove(&pending_id);
    }
}

fn ewma(current: f64, observed: f64, alpha: f64) -> f64 {
    current * (1.0 - alpha) + observed * alpha
}

fn provider_cache_ttl(conf: &ClientP2pConf) -> Duration {
    let min_ttl = discovery_timeout(conf);
    std::cmp::max(
        min_ttl,
        std::cmp::min(conf.provider_publish_interval, conf.provider_ttl),
    )
}

#[cfg(test)]
fn negative_provider_cache_ttl(conf: &ClientP2pConf) -> Duration {
    discovery_timeout(conf)
}

fn discovery_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.discovery_timeout_ms.max(1))
}

fn connect_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.connect_timeout_ms.max(1))
}

fn transfer_timeout(conf: &ClientP2pConf) -> Duration {
    Duration::from_millis(conf.transfer_timeout_ms.max(1))
}

fn hedge_delay(conf: &ClientP2pConf) -> Option<Duration> {
    (conf.hedge_delay_ms > 0).then(|| Duration::from_millis(conf.hedge_delay_ms))
}

fn maintenance_tick_interval(conf: &ClientP2pConf) -> Duration {
    let min_timeout_ms = discovery_timeout(conf)
        .as_millis()
        .min(transfer_timeout(conf).as_millis());
    Duration::from_millis(min_timeout_ms.clamp(100, 1000) as u64)
}

fn expire_provider_cache(provider_cache: &mut HashMap<ChunkId, CachedProviders>, now: Instant) {
    let expired_ids: Vec<ChunkId> = provider_cache
        .iter()
        .filter_map(|(chunk_id, providers)| (providers.expire_at <= now).then_some(*chunk_id))
        .collect();
    for chunk_id in expired_ids {
        provider_cache.remove(&chunk_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use orpc::runtime::Runtime;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    fn enabled_conf() -> ClientP2pConf {
        ClientP2pConf {
            enable: true,
            request_timeout_ms: 200,
            discovery_timeout_ms: 200,
            connect_timeout_ms: 200,
            transfer_timeout_ms: 200,
            max_inflight_requests: 8,
            cache_dir: std::env::temp_dir()
                .join(format!(
                    "curvine-p2p-unit-cache-{}",
                    orpc::common::Utils::uuid()
                ))
                .to_string_lossy()
                .to_string(),
            ..ClientP2pConf::default()
        }
    }

    fn test_runtime() -> Arc<Runtime> {
        static TEST_RT: Lazy<Arc<Runtime>> = Lazy::new(|| Arc::new(Runtime::new("p2p-test", 2, 2)));
        TEST_RT.clone()
    }

    async fn wait_bootstrap_addr(service: &P2pService) -> Option<String> {
        for _ in 0..400 {
            if let Some(addr) = service.bootstrap_peer_addr() {
                return Some(addr);
            }
            sleep(Duration::from_millis(25)).await;
        }
        None
    }

    async fn wait_fetch(
        service: &P2pService,
        chunk: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
    ) -> Option<Bytes> {
        for _ in 0..200 {
            if let Some(data) = service.fetch_chunk(chunk, max_len, expected_mtime).await {
                return Some(data);
            }
            sleep(Duration::from_millis(25)).await;
        }
        None
    }

    async fn wait_fetch_with_context(
        service: &P2pService,
        chunk: ChunkId,
        max_len: usize,
        expected_mtime: Option<i64>,
        context: &P2pReadTraceContext,
    ) -> Option<Bytes> {
        for _ in 0..200 {
            if let Some(data) = service
                .fetch_chunk_with_context(chunk, max_len, expected_mtime, Some(context))
                .await
            {
                return Some(data);
            }
            sleep(Duration::from_millis(25)).await;
        }
        None
    }

    async fn wait_snapshot<F>(service: &P2pService, mut predicate: F) -> P2pStatsSnapshot
    where
        F: FnMut(&P2pStatsSnapshot) -> bool,
    {
        for _ in 0..80 {
            let snapshot = service.snapshot();
            if predicate(&snapshot) {
                return snapshot;
            }
            sleep(Duration::from_millis(25)).await;
        }
        service.snapshot()
    }

    #[test]
    fn provider_cache_ttl_is_bounded_by_publish_interval_and_ttl() {
        let mut conf = enabled_conf();
        conf.discovery_timeout_ms = 500;
        conf.provider_ttl = Duration::from_secs(120);
        conf.provider_publish_interval = Duration::from_secs(30);
        assert_eq!(provider_cache_ttl(&conf), Duration::from_secs(30));
        conf.provider_publish_interval = Duration::from_millis(200);
        assert_eq!(provider_cache_ttl(&conf), Duration::from_millis(500));
    }

    #[test]
    fn negative_provider_cache_ttl_tracks_discovery_timeout() {
        let mut conf = enabled_conf();
        conf.discovery_timeout_ms = 321;
        assert_eq!(
            negative_provider_cache_ttl(&conf),
            Duration::from_millis(321)
        );
    }

    #[test]
    fn layered_timeout_helpers_use_config_values() {
        let mut conf = enabled_conf();
        conf.discovery_timeout_ms = 123;
        conf.connect_timeout_ms = 456;
        conf.transfer_timeout_ms = 789;
        assert_eq!(discovery_timeout(&conf), Duration::from_millis(123));
        assert_eq!(connect_timeout(&conf), Duration::from_millis(456));
        assert_eq!(transfer_timeout(&conf), Duration::from_millis(789));
    }

    #[test]
    fn hedge_delay_helper_uses_config() {
        let mut conf = enabled_conf();
        conf.hedge_delay_ms = 0;
        assert!(hedge_delay(&conf).is_none());
        conf.hedge_delay_ms = 120;
        assert_eq!(hedge_delay(&conf), Some(Duration::from_millis(120)));
    }

    #[tokio::test]
    async fn qps_limiter_denies_when_deadline_exceeded() {
        let limiter = QpsLimiter::new(1);
        assert!(limiter.acquire(Duration::from_millis(10)).await);
        assert!(!limiter.acquire(Duration::from_millis(50)).await);
    }

    #[test]
    fn chunk_record_key_is_version_sensitive() {
        let base = ChunkId::with_version(7, 1000, 12, 64);
        let changed = ChunkId::with_version(7, 1001, 12, 64);
        assert_ne!(chunk_record_key(base), chunk_record_key(changed));
    }

    #[test]
    fn build_candidates_prefers_lower_score_peers() {
        let peer_fast = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let peer_slow = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let local = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let connected = HashSet::from([peer_fast, peer_slow]);
        let bootstrap = HashSet::new();
        let mut ewma_map = HashMap::new();
        ewma_map.insert(
            peer_fast,
            PeerEwma {
                latency_ms: 10.0,
                failure_ratio: 0.0,
                throughput_mb_s: 12.0,
            },
        );
        ewma_map.insert(
            peer_slow,
            PeerEwma {
                latency_ms: 400.0,
                failure_ratio: 0.8,
                throughput_mb_s: 0.5,
            },
        );
        let inflight = HashMap::new();

        let candidates = build_candidates(
            std::iter::empty::<PeerId>(),
            &connected,
            &bootstrap,
            local,
            &ewma_map,
            &inflight,
        );
        let ordered: Vec<PeerId> = candidates.into_iter().collect();
        assert_eq!(ordered.first(), Some(&peer_fast));
    }

    #[test]
    fn should_accept_peer_respects_max_active_peers() {
        let peer1 = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let peer2 = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let peer3 = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let mut connected = HashSet::new();
        connected.insert(peer1);
        connected.insert(peer2);
        let bootstrap = HashSet::new();

        assert!(!should_accept_peer(peer3, &connected, &bootstrap, 2));
        assert!(should_accept_peer(peer3, &connected, &bootstrap, 3));
        assert!(should_accept_peer(peer3, &connected, &bootstrap, 0));
    }

    #[test]
    fn should_accept_peer_allows_bootstrap_and_existing_peer() {
        let peer1 = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let peer2 = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let mut connected = HashSet::new();
        connected.insert(peer1);
        let mut bootstrap = HashSet::new();
        bootstrap.insert(peer2);

        assert!(should_accept_peer(peer1, &connected, &bootstrap, 1));
        assert!(should_accept_peer(peer2, &connected, &bootstrap, 1));
    }

    #[test]
    fn peer_whitelist_allows_only_listed_peers_when_present() {
        let local = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let allowed = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let denied = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let whitelist = HashSet::from([allowed]);
        assert!(is_peer_allowed(local, local, &whitelist));
        assert!(is_peer_allowed(allowed, local, &whitelist));
        assert!(!is_peer_allowed(denied, local, &whitelist));
    }

    #[test]
    fn peer_dispatch_window_degrades_for_slow_peer() {
        let peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let mut ewma = HashMap::new();
        ewma.insert(
            peer,
            PeerEwma {
                latency_ms: 800.0,
                failure_ratio: 0.6,
                throughput_mb_s: 0.3,
            },
        );
        let healthy = peer_dispatch_window(peer, 16, &HashMap::new());
        let degraded = peer_dispatch_window(peer, 16, &ewma);
        assert_eq!(healthy, 16);
        assert!(degraded < healthy);
        assert!(degraded >= 1);
    }

    #[test]
    fn identity_key_is_stable_when_reloaded() {
        let mut conf = enabled_conf();
        conf.cache_dir = std::env::temp_dir()
            .join(format!(
                "curvine-p2p-identity-{}",
                orpc::common::Utils::uuid()
            ))
            .to_string_lossy()
            .to_string();
        let first = load_or_generate_identity(&conf).expect("first identity should load");
        let second = load_or_generate_identity(&conf).expect("second identity should load");
        assert_eq!(first.public().to_peer_id(), second.public().to_peer_id());
    }

    #[test]
    fn runtime_policy_version_is_persisted_and_loaded() {
        let mut conf = enabled_conf();
        conf.cache_dir = std::env::temp_dir()
            .join(format!(
                "curvine-p2p-policy-version-{}",
                orpc::common::Utils::uuid()
            ))
            .to_string_lossy()
            .to_string();
        assert_eq!(load_runtime_policy_version(&conf), 0);
        assert!(persist_runtime_policy_version(&conf, 9));
        assert_eq!(load_runtime_policy_version(&conf), 9);
    }

    #[tokio::test]
    async fn persisted_policy_version_rejects_stale_master_version_after_restart() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        conf.cache_dir = std::env::temp_dir()
            .join(format!(
                "curvine-p2p-policy-restart-{}",
                orpc::common::Utils::uuid()
            ))
            .to_string_lossy()
            .to_string();
        assert!(persist_runtime_policy_version(&conf, 5));
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();
        wait_bootstrap_addr(&service)
            .await
            .expect("service bootstrap address should be ready");

        assert_eq!(service.runtime_policy_version(), 5);
        assert!(
            service
                .sync_runtime_policy_from_master(
                    5,
                    vec![],
                    vec!["tenant-a".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert!(
            service
                .sync_runtime_policy_from_master(
                    4,
                    vec![],
                    vec!["tenant-b".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(service.snapshot().policy_rollback_ignored, 1);
    }

    #[test]
    fn can_dispatch_peer_respects_per_peer_limit() {
        let peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let mut inflight = HashMap::new();
        inflight.insert(peer, 2usize);

        assert!(!can_dispatch_peer(
            peer,
            &inflight,
            2,
            false,
            1,
            &HashMap::new()
        ));
        assert!(can_dispatch_peer(
            peer,
            &inflight,
            3,
            false,
            1,
            &HashMap::new()
        ));
        assert!(can_dispatch_peer(
            peer,
            &inflight,
            0,
            false,
            1,
            &HashMap::new()
        ));
    }

    #[test]
    fn release_peer_inflight_decrements_and_removes_peer() {
        let peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        let mut inflight = HashMap::new();
        inflight.insert(peer, 2usize);

        let pending = PendingFetchRequest {
            fetch_token: 0,
            chunk_id: ChunkId::new(1, 0),
            max_len: 1,
            expected_mtime: None,
            trace: TraceLabels::from_context(0, None, &enabled_conf()),
            response: tokio::sync::oneshot::channel().0,
            candidates: VecDeque::new(),
            active_peer: Some(peer),
            started_at: Instant::now(),
            deadline: Instant::now(),
        };

        release_peer_inflight(&pending, &mut inflight);
        assert_eq!(inflight.get(&peer).copied(), Some(1));
        release_peer_inflight(&pending, &mut inflight);
        assert!(!inflight.contains_key(&peer));
    }

    #[test]
    fn expire_provider_cache_removes_only_expired_entries() {
        let mut provider_cache: HashMap<ChunkId, CachedProviders> = HashMap::new();
        let now = Instant::now();
        let peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        provider_cache.insert(
            ChunkId::new(1, 0),
            CachedProviders {
                peers: HashSet::from([peer]),
                expire_at: now - Duration::from_millis(1),
            },
        );
        let peer2 = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        provider_cache.insert(
            ChunkId::new(2, 0),
            CachedProviders {
                peers: HashSet::from([peer2]),
                expire_at: now + Duration::from_secs(5),
            },
        );
        expire_provider_cache(&mut provider_cache, now);
        assert!(!provider_cache.contains_key(&ChunkId::new(1, 0)));
        assert!(provider_cache.contains_key(&ChunkId::new(2, 0)));
    }

    #[tokio::test]
    async fn cancel_pending_by_token_clears_queries_and_requests() {
        let token = 77u64;
        let mut pending_queries = HashMap::new();
        let mut pending_requests = HashMap::new();
        let mut inflight_per_peer = HashMap::new();

        cancel_pending_by_token(
            token,
            &mut pending_queries,
            &mut pending_requests,
            &mut inflight_per_peer,
        );

        assert!(pending_queries.is_empty());
        assert!(pending_requests.is_empty());
        assert!(inflight_per_peer.is_empty());
    }

    #[test]
    fn build_swarm_succeeds_with_tcp_listen() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        if let Err(e) = build_swarm(&conf) {
            panic!("build_swarm should succeed: {}", e);
        }
    }

    #[tokio::test]
    async fn published_chunk_is_fetchable_from_other_peer() {
        let mut provider_conf = enabled_conf();
        provider_conf.enable_mdns = false;
        provider_conf.enable_dht = false;
        provider_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let provider = P2pService::new_with_runtime(provider_conf.clone(), Some(test_runtime()));
        provider.start();

        let bootstrap = wait_bootstrap_addr(&provider)
            .await
            .expect("provider bootstrap address should be ready");

        let mut consumer_conf = enabled_conf();
        consumer_conf.enable_mdns = false;
        consumer_conf.enable_dht = false;
        consumer_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        consumer_conf.bootstrap_peers = vec![bootstrap];
        let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
        consumer.start();

        let chunk = ChunkId::new(7, 128);
        provider.publish_chunk(chunk, Bytes::from_static(b"p2p-data"), 1000);
        let result = wait_fetch(&consumer, chunk, 1024, Some(1000)).await;
        let provider_snapshot = provider.snapshot();
        let consumer_snapshot = consumer.snapshot();

        assert_eq!(result, Some(Bytes::from_static(b"p2p-data")));
        assert!(provider_snapshot.active_peers >= 2);
        assert!(provider_snapshot.bytes_sent >= 8);
        assert!(consumer_snapshot.bytes_recv >= 8);
        assert!(consumer_snapshot.avg_peer_latency_ms >= 0.0);
    }

    #[tokio::test]
    async fn service_without_runtime_is_disabled_even_if_enable_is_true() {
        let conf = enabled_conf();
        let provider = P2pService::new(conf.clone());
        let consumer = P2pService::new(conf);
        provider.start();
        consumer.start();

        let chunk = ChunkId::new(8, 0);
        assert!(!provider.publish_chunk(chunk, Bytes::from_static(b"x"), 1));
        let result = consumer.fetch_chunk(chunk, 1024, Some(1)).await;

        assert_eq!(provider.state(), P2pState::Disabled);
        assert_eq!(consumer.state(), P2pState::Disabled);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn disabled_service_cannot_fetch() {
        let conf = ClientP2pConf {
            enable: false,
            ..ClientP2pConf::default()
        };
        let provider = P2pService::new_with_runtime(
            ClientP2pConf {
                enable: true,
                ..enabled_conf()
            },
            Some(test_runtime()),
        );
        let consumer = P2pService::new_with_runtime(conf, Some(test_runtime()));
        provider.start();
        consumer.start();

        let chunk = ChunkId::new(9, 0);
        provider.publish_chunk(chunk, Bytes::from_static(b"data"), 1);
        let result = consumer.fetch_chunk(chunk, 64, Some(1)).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn snapshot_contains_discovery_counts() {
        let mut first_conf = enabled_conf();
        first_conf.enable_mdns = false;
        first_conf.enable_dht = false;
        first_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let first = P2pService::new_with_runtime(first_conf.clone(), Some(test_runtime()));
        first.start();

        let bootstrap = wait_bootstrap_addr(&first)
            .await
            .expect("provider bootstrap address should be ready");

        let mut second_conf = enabled_conf();
        second_conf.enable_mdns = false;
        second_conf.enable_dht = false;
        second_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        second_conf.bootstrap_peers = vec![bootstrap];
        let second = P2pService::new_with_runtime(second_conf, Some(test_runtime()));
        second.start();

        let snapshot = wait_snapshot(&second, |s| {
            s.active_peers >= 2 && s.bootstrap_connected >= 1
        })
        .await;

        assert!(snapshot.active_peers >= 2);
        assert!(snapshot.bootstrap_connected >= 1);
        assert_eq!(snapshot.mdns_peers, 0);
        assert_eq!(snapshot.dht_peers, 0);
    }

    #[tokio::test]
    async fn runtime_backend_uses_network_instead_of_inprocess_registry() {
        let rt = test_runtime();

        let mut provider_conf = enabled_conf();
        provider_conf.enable_mdns = false;
        provider_conf.enable_dht = false;
        provider_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let provider = P2pService::new_with_runtime(provider_conf.clone(), Some(rt.clone()));
        provider.start();

        let bootstrap = wait_bootstrap_addr(&provider)
            .await
            .expect("provider bootstrap address should be ready");

        let mut consumer_conf = enabled_conf();
        consumer_conf.enable_mdns = false;
        consumer_conf.enable_dht = false;
        consumer_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        consumer_conf.bootstrap_peers = vec![bootstrap];
        let consumer = P2pService::new_with_runtime(consumer_conf, Some(rt));
        consumer.start();

        let chunk = ChunkId::new(19, 0);
        provider.publish_chunk(chunk, Bytes::from_static(b"network-only"), 42);

        let result = wait_fetch(&consumer, chunk, 1024, Some(42)).await;
        assert_eq!(result, Some(Bytes::from_static(b"network-only")));
    }

    #[tokio::test]
    async fn enable_false_disables_p2p() {
        let mut conf = enabled_conf();
        conf.enable = false;
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();
        assert_eq!(service.state(), P2pState::Disabled);
    }

    #[test]
    fn publish_chunk_returns_false_when_publish_queue_is_full() {
        let mut conf = enabled_conf();
        conf.max_inflight_requests = 1;
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service
            .state
            .store(P2pState::Running as u8, Ordering::Relaxed);
        let (tx, _rx) = mpsc::channel(1);
        *service.network_tx.lock().unwrap() = Some(tx);

        let first = ChunkId::with_version(30, 1, 1, 0);
        let second = ChunkId::with_version(30, 1, 1, 1);
        assert!(service.publish_chunk(first, Bytes::from_static(b"first"), 10));
        assert!(!service.publish_chunk(second, Bytes::from_static(b"second"), 10));
    }

    #[tokio::test]
    async fn local_cache_hit_works_without_remote_peer() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();

        let chunk = ChunkId::new(31, 0);
        assert!(service.publish_chunk(chunk, Bytes::from_static(b"local-cache"), 777));
        let data = service.fetch_chunk(chunk, 1024, Some(777)).await;
        assert_eq!(data, Some(Bytes::from_static(b"local-cache")));
    }

    #[tokio::test]
    async fn different_version_chunk_isolated_in_cache_and_lookup() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();

        let chunk_v1 = ChunkId::with_version(88, 1000, 33, 0);
        let chunk_v2 = ChunkId::with_version(88, 1001, 33, 0);
        assert!(service.publish_chunk(chunk_v1, Bytes::from_static(b"v1000"), 1000));

        let miss = service.fetch_chunk(chunk_v2, 1024, Some(1001)).await;
        let hit = service.fetch_chunk(chunk_v1, 1024, Some(1000)).await;

        assert!(miss.is_none());
        assert_eq!(hit, Some(Bytes::from_static(b"v1000")));
    }

    #[tokio::test]
    async fn mtime_mismatch_returns_none_and_updates_snapshot() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();

        let chunk = ChunkId::new(32, 0);
        assert!(service.publish_chunk(chunk, Bytes::from_static(b"x"), 10));
        let data = service.fetch_chunk(chunk, 1024, Some(11)).await;
        let snapshot = service.snapshot();

        assert!(data.is_none());
        assert!(snapshot.mtime_mismatches >= 1);
    }

    #[tokio::test]
    async fn concurrent_fetches_share_single_network_fetch() {
        let mut provider_conf = enabled_conf();
        provider_conf.enable_mdns = false;
        provider_conf.enable_dht = false;
        provider_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let provider = Arc::new(P2pService::new_with_runtime(
            provider_conf.clone(),
            Some(test_runtime()),
        ));
        provider.start();

        let bootstrap = wait_bootstrap_addr(&provider)
            .await
            .expect("provider bootstrap address should be ready");

        let mut consumer_conf = enabled_conf();
        consumer_conf.enable_mdns = false;
        consumer_conf.enable_dht = false;
        consumer_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        consumer_conf.bootstrap_peers = vec![bootstrap];
        let consumer = Arc::new(P2pService::new_with_runtime(
            consumer_conf,
            Some(test_runtime()),
        ));
        consumer.start();
        let _ = wait_snapshot(&consumer, |snapshot| snapshot.active_peers >= 2).await;

        let chunk = ChunkId::new(44, 0);
        assert!(provider.publish_chunk(chunk, Bytes::from_static(b"singleflight"), 2024));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let c = consumer.clone();
            tasks.push(tokio::spawn(async move {
                c.fetch_chunk(chunk, 1024, Some(2024)).await
            }));
        }
        for task in tasks {
            assert_eq!(
                task.await.unwrap(),
                Some(Bytes::from_static(b"singleflight"))
            );
        }

        let snapshot = consumer.snapshot();
        assert!(snapshot.bytes_recv > 0);
        assert!(snapshot.bytes_recv <= 24);
    }

    #[test]
    fn trace_labels_default_to_unknown_when_context_absent() {
        let mut conf = enabled_conf();
        conf.trace_enable = true;
        conf.trace_sample_rate = 1.0;
        let labels = TraceLabels::from_context(7, None, &conf);
        assert_eq!(labels.trace_id, "fetch-7");
        assert_eq!(labels.tenant_id, "unknown");
        assert_eq!(labels.job_id, "unknown");
    }

    #[test]
    fn trace_labels_prefer_context_values() {
        let mut conf = enabled_conf();
        conf.trace_enable = true;
        conf.trace_sample_rate = 1.0;
        let ctx = P2pReadTraceContext {
            trace_id: Some("trace-xyz".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            job_id: Some("job-9".to_string()),
        };
        let labels = TraceLabels::from_context(9, Some(&ctx), &conf);
        assert_eq!(labels.trace_id, "trace-xyz");
        assert_eq!(labels.tenant_id, "tenant-a");
        assert_eq!(labels.job_id, "job-9");
    }

    #[test]
    fn trace_labels_respect_sampling_policy() {
        let mut conf = enabled_conf();
        conf.trace_enable = true;
        conf.trace_sample_rate = 0.0;
        let labels = TraceLabels::from_context(3, None, &conf);
        assert!(!labels.sampled);
        assert!(labels.trace_id.is_empty());
        conf.trace_sample_rate = 1.0;
        let labels = TraceLabels::from_context(3, None, &conf);
        assert!(labels.sampled);
        assert!(!labels.trace_id.is_empty());
    }

    #[test]
    fn trace_labels_keep_tenant_and_job_when_unsampled() {
        let mut conf = enabled_conf();
        conf.trace_enable = true;
        conf.trace_sample_rate = 0.0;
        let ctx = P2pReadTraceContext {
            trace_id: Some("trace-xyz".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            job_id: Some("job-9".to_string()),
        };
        let labels = TraceLabels::from_context(9, Some(&ctx), &conf);
        assert!(!labels.sampled);
        assert!(labels.trace_id.is_empty());
        assert_eq!(labels.tenant_id, "tenant-a");
        assert_eq!(labels.job_id, "job-9");
    }

    #[test]
    fn tenant_whitelist_blocks_non_member_tenants() {
        let whitelist = HashSet::from(["tenant-a".to_string()]);
        assert!(is_tenant_allowed("tenant-a", &whitelist));
        assert!(!is_tenant_allowed("tenant-b", &whitelist));
        assert!(!is_tenant_allowed("", &whitelist));
    }

    #[tokio::test]
    async fn tenant_whitelist_denies_cross_tenant_fetch() {
        let mut provider_conf = enabled_conf();
        provider_conf.enable_mdns = false;
        provider_conf.enable_dht = false;
        provider_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        provider_conf.tenant_whitelist = vec!["tenant-a".to_string()];
        let provider = P2pService::new_with_runtime(provider_conf.clone(), Some(test_runtime()));
        provider.start();

        let bootstrap = wait_bootstrap_addr(&provider)
            .await
            .expect("provider bootstrap address should be ready");

        let mut consumer_conf = enabled_conf();
        consumer_conf.enable_mdns = false;
        consumer_conf.enable_dht = false;
        consumer_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        consumer_conf.bootstrap_peers = vec![bootstrap];
        let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
        consumer.start();

        let chunk = ChunkId::new(203, 0);
        provider.publish_chunk(chunk, Bytes::from_static(b"tenant-data"), 1);
        let denied_ctx = P2pReadTraceContext {
            trace_id: Some("trace-denied".to_string()),
            tenant_id: Some("tenant-b".to_string()),
            job_id: Some("job-x".to_string()),
        };
        let allow_ctx = P2pReadTraceContext {
            trace_id: Some("trace-allow".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            job_id: Some("job-y".to_string()),
        };
        let denied = consumer
            .fetch_chunk_with_context(chunk, 1024, Some(1), Some(&denied_ctx))
            .await;
        let allowed = wait_fetch_with_context(&consumer, chunk, 1024, Some(1), &allow_ctx).await;

        assert!(denied.is_none());
        assert_eq!(allowed, Some(Bytes::from_static(b"tenant-data")));
    }

    #[tokio::test]
    async fn tenant_whitelist_updates_take_effect_without_restart() {
        let mut provider_conf = enabled_conf();
        provider_conf.enable_mdns = false;
        provider_conf.enable_dht = false;
        provider_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let provider = P2pService::new_with_runtime(provider_conf.clone(), Some(test_runtime()));
        provider.start();

        let bootstrap = wait_bootstrap_addr(&provider)
            .await
            .expect("provider bootstrap address should be ready");

        let mut consumer_conf = enabled_conf();
        consumer_conf.enable_mdns = false;
        consumer_conf.enable_dht = false;
        consumer_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        consumer_conf.bootstrap_peers = vec![bootstrap];
        let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
        consumer.start();

        let tenant_a_ctx = P2pReadTraceContext {
            trace_id: Some("trace-a".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            job_id: Some("job-a".to_string()),
        };
        let tenant_b_ctx = P2pReadTraceContext {
            trace_id: Some("trace-b".to_string()),
            tenant_id: Some("tenant-b".to_string()),
            job_id: Some("job-b".to_string()),
        };

        let chunk_before = ChunkId::new(401, 0);
        provider.publish_chunk(chunk_before, Bytes::from_static(b"before"), 11);
        let before =
            wait_fetch_with_context(&consumer, chunk_before, 1024, Some(11), &tenant_a_ctx).await;
        assert_eq!(before, Some(Bytes::from_static(b"before")));

        assert!(
            provider
                .update_runtime_policy(None, Some(vec!["tenant-b".to_string()]))
                .await
        );

        let chunk_after = ChunkId::new(402, 0);
        provider.publish_chunk(chunk_after, Bytes::from_static(b"after"), 22);
        let denied = consumer
            .fetch_chunk_with_context(chunk_after, 1024, Some(22), Some(&tenant_a_ctx))
            .await;
        let allowed =
            wait_fetch_with_context(&consumer, chunk_after, 1024, Some(22), &tenant_b_ctx).await;

        assert!(denied.is_none());
        assert_eq!(allowed, Some(Bytes::from_static(b"after")));
    }

    #[tokio::test]
    async fn stale_master_policy_version_is_ignored() {
        let mut provider_conf = enabled_conf();
        provider_conf.enable_mdns = false;
        provider_conf.enable_dht = false;
        provider_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let provider = P2pService::new_with_runtime(provider_conf.clone(), Some(test_runtime()));
        provider.start();

        let bootstrap = wait_bootstrap_addr(&provider)
            .await
            .expect("provider bootstrap address should be ready");

        let mut consumer_conf = enabled_conf();
        consumer_conf.enable_mdns = false;
        consumer_conf.enable_dht = false;
        consumer_conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        consumer_conf.bootstrap_peers = vec![bootstrap];
        let consumer = P2pService::new_with_runtime(consumer_conf, Some(test_runtime()));
        consumer.start();

        assert!(
            provider
                .sync_runtime_policy_from_master(
                    2,
                    vec![],
                    vec!["tenant-a".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(provider.runtime_policy_version(), 2);
        assert!(
            provider
                .sync_runtime_policy_from_master(
                    1,
                    vec![],
                    vec!["tenant-b".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(provider.runtime_policy_version(), 2);

        let tenant_a_ctx = P2pReadTraceContext {
            trace_id: Some("trace-a".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            job_id: Some("job-a".to_string()),
        };
        let tenant_b_ctx = P2pReadTraceContext {
            trace_id: Some("trace-b".to_string()),
            tenant_id: Some("tenant-b".to_string()),
            job_id: Some("job-b".to_string()),
        };
        let chunk = ChunkId::new(601, 0);
        provider.publish_chunk(chunk, Bytes::from_static(b"master-policy"), 66);
        let denied = consumer
            .fetch_chunk_with_context(chunk, 1024, Some(66), Some(&tenant_b_ctx))
            .await;
        let allowed =
            wait_fetch_with_context(&consumer, chunk, 1024, Some(66), &tenant_a_ctx).await;
        assert!(denied.is_none());
        assert_eq!(allowed, Some(Bytes::from_static(b"master-policy")));
    }

    #[tokio::test]
    async fn network_loop_can_restart_after_stop_and_start() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();
        assert!(
            service
                .sync_runtime_policy_from_master(
                    1,
                    vec![],
                    vec!["tenant-a".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(service.runtime_policy_version(), 1);

        service.stop();
        sleep(Duration::from_millis(100)).await;
        service.start();

        assert!(
            service
                .sync_runtime_policy_from_master(
                    2,
                    vec![],
                    vec!["tenant-b".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(service.runtime_policy_version(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn transient_policy_persist_failure_can_retry_same_version() {
        use std::os::unix::fs::PermissionsExt;

        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        let cache_dir = std::env::temp_dir().join(format!(
            "curvine-p2p-policy-permission-{}",
            orpc::common::Utils::uuid()
        ));
        std::fs::create_dir_all(&cache_dir).unwrap();
        conf.cache_dir = cache_dir.to_string_lossy().to_string();
        let service = P2pService::new_with_runtime(conf.clone(), Some(test_runtime()));
        service.start();
        assert!(wait_bootstrap_addr(&service).await.is_some());

        let mut perms = std::fs::metadata(&cache_dir).unwrap().permissions();
        perms.set_mode(0o500);
        std::fs::set_permissions(&cache_dir, perms).unwrap();

        assert!(
            !service
                .sync_runtime_policy_from_master(
                    1,
                    vec![],
                    vec!["tenant-a".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(service.runtime_policy_version(), 0);

        let mut perms = std::fs::metadata(&cache_dir).unwrap().permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(&cache_dir, perms).unwrap();

        assert!(
            service
                .sync_runtime_policy_from_master(
                    1,
                    vec![],
                    vec!["tenant-a".to_string()],
                    "".to_string(),
                )
                .await
        );
        assert_eq!(service.runtime_policy_version(), 1);
    }

    #[tokio::test]
    async fn master_policy_signature_is_mandatory_when_key_configured() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        conf.policy_hmac_key = "policy-secret".to_string();
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();

        let peers = vec!["12D3KooWxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()];
        let tenants = vec!["tenant-a".to_string()];
        assert!(
            !service
                .sync_runtime_policy_from_master(1, peers.clone(), tenants.clone(), "".to_string())
                .await
        );

        let signature = CommonUtils::sign_p2p_policy("policy-secret", 1, &peers, &tenants);
        assert!(
            service
                .sync_runtime_policy_from_master(1, peers, tenants, signature)
                .await
        );
        assert_eq!(service.runtime_policy_version(), 1);
    }

    #[tokio::test]
    async fn repeated_rejected_master_policy_is_deduplicated() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        conf.policy_hmac_key = "policy-secret".to_string();
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();
        let peers = vec!["12D3KooWxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()];
        let tenants = vec!["tenant-a".to_string()];

        assert!(
            !service
                .sync_runtime_policy_from_master(1, peers.clone(), tenants.clone(), "".to_string())
                .await
        );
        let rejected = service.snapshot().policy_rejects;
        assert_eq!(rejected, 1);
        assert!(
            service
                .sync_runtime_policy_from_master(1, peers.clone(), tenants.clone(), "".to_string())
                .await
        );
        assert_eq!(service.snapshot().policy_rejects, rejected);

        let signature = CommonUtils::sign_p2p_policy("policy-secret", 1, &peers, &tenants);
        assert!(
            service
                .sync_runtime_policy_from_master(1, peers, tenants, signature)
                .await
        );
        assert_eq!(service.runtime_policy_version(), 1);
    }

    #[tokio::test]
    async fn master_policy_accepts_any_signature_in_rotation_window() {
        let mut conf = enabled_conf();
        conf.enable_mdns = false;
        conf.enable_dht = false;
        conf.listen_addrs = vec!["/ip4/127.0.0.1/tcp/0".to_string()];
        conf.policy_hmac_key = "old-secret".to_string();
        let service = P2pService::new_with_runtime(conf, Some(test_runtime()));
        service.start();

        let peers = vec!["12D3KooWxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".to_string()];
        let tenants = vec!["tenant-a".to_string()];
        let old_signature = CommonUtils::sign_p2p_policy("old-secret", 1, &peers, &tenants);
        let new_signature = CommonUtils::sign_p2p_policy("new-secret", 1, &peers, &tenants);
        let rotation_signatures = format!("{},{}", new_signature, old_signature);

        assert!(
            service
                .sync_runtime_policy_from_master(1, peers, tenants, rotation_signatures)
                .await
        );
        assert_eq!(service.runtime_policy_version(), 1);
    }
}
