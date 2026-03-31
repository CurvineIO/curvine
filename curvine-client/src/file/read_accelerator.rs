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

use crate::client_metrics::ReadSource;
use crate::p2p::{
    ChunkId, FetchChunkOrigin, P2pReadTraceContext, P2pService, P2pState, P2pStatsSnapshot,
};
use bytes::Bytes;
use curvine_common::conf::ClusterConf;
use curvine_common::FsResult;
use fxhash::FxHasher;
use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use orpc::common::LocalTime;
use orpc::err_box;
use orpc::sync::FastDashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

const ADAPTIVE_EWMA_ALPHA_DEN: u64 = 8;
const ADAPTIVE_MIN_SAMPLES: u64 = 8;
const ADAPTIVE_BYPASS_RATIO_NUM: u64 = 9;
const ADAPTIVE_BYPASS_RATIO_DEN: u64 = 10;
const ADAPTIVE_PROBE_INTERVAL: u64 = 1024;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct ReadChunkKey {
    file_id: i64,
    version_epoch: i64,
    block_id: i64,
    off: i64,
}

impl ReadChunkKey {
    fn new(file_id: i64, version_epoch: i64, block_id: i64, off: i64) -> Self {
        Self {
            file_id,
            version_epoch,
            block_id,
            off,
        }
    }
}

pub(crate) type ReadChunkFlight = (Arc<AsyncMutex<()>>, OwnedMutexGuard<()>);

pub(crate) struct AcceleratedReadHit {
    data: Bytes,
    adaptive_source: Option<ReadSource>,
}

impl AcceleratedReadHit {
    pub(crate) fn into_parts(self) -> (Bytes, Option<ReadSource>) {
        (self.data, self.adaptive_source)
    }
}

pub(crate) struct AcceleratedReadRequest {
    source: ReadSource,
    read_key: ReadChunkKey,
    chunk_id: ChunkId,
    expect_len: usize,
    expected_mtime: Option<i64>,
    trace_context: P2pReadTraceContext,
}

impl AcceleratedReadRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_id: i64,
        version_epoch: i64,
        block_id: i64,
        read_off: i64,
        expect_len: usize,
        file_mtime: i64,
        source: ReadSource,
        tenant_id: Option<String>,
        job_id: Option<String>,
    ) -> Self {
        let version_epoch = version_epoch.max(0);
        Self {
            source,
            read_key: ReadChunkKey::new(file_id, version_epoch, block_id, read_off),
            chunk_id: ChunkId::with_version(file_id, version_epoch, block_id, read_off),
            expect_len,
            expected_mtime: (file_mtime > 0).then_some(file_mtime),
            trace_context: P2pReadTraceContext {
                trace_id: Some(format!(
                    "{}:{}:{}:{}",
                    file_id, version_epoch, block_id, read_off
                )),
                tenant_id,
                job_id,
            },
        }
    }

    pub(crate) fn source(&self) -> ReadSource {
        self.source
    }
}

pub(crate) struct ReadAccelerator {
    p2p_service: Option<Arc<P2pService>>,
    read_chunk_cache: Cache<ReadChunkKey, Bytes, BuildHasherDefault<FxHasher>>,
    read_chunk_flights: FastDashMap<ReadChunkKey, Arc<AsyncMutex<()>>>,
    adaptive_worker_latency_us: AtomicU64,
    adaptive_worker_samples: AtomicU64,
    adaptive_p2p_latency_us: AtomicU64,
    adaptive_p2p_samples: AtomicU64,
    adaptive_probe_seq: AtomicU64,
}

impl ReadAccelerator {
    pub(crate) fn new(conf: &ClusterConf, p2p_service: Option<Arc<P2pService>>) -> Self {
        let read_chunk_cache_capacity = (conf.client.p2p.cache_capacity
            / conf.client.read_chunk_size.max(1) as u64)
            .clamp(1, 65_536);
        let read_chunk_cache = CacheBuilder::default()
            .max_capacity(read_chunk_cache_capacity)
            .time_to_live(conf.client.p2p.cache_ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());
        Self {
            p2p_service,
            read_chunk_cache,
            read_chunk_flights: FastDashMap::default(),
            adaptive_worker_latency_us: AtomicU64::new(0),
            adaptive_worker_samples: AtomicU64::new(0),
            adaptive_p2p_latency_us: AtomicU64::new(0),
            adaptive_p2p_samples: AtomicU64::new(0),
            adaptive_probe_seq: AtomicU64::new(0),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn p2p_state(&self) -> Option<P2pState> {
        self.p2p_service.as_ref().map(|service| service.state())
    }

    #[allow(dead_code)]
    pub(crate) fn p2p_peer_id(&self) -> Option<String> {
        self.p2p_service
            .as_ref()
            .map(|service| service.peer_id().to_string())
    }

    #[allow(dead_code)]
    pub(crate) fn p2p_bootstrap_peer_addr(&self) -> Option<String> {
        self.p2p_service
            .as_ref()
            .and_then(|service| service.bootstrap_peer_addr())
    }

    #[allow(dead_code)]
    pub(crate) fn p2p_stats_snapshot(&self) -> Option<P2pStatsSnapshot> {
        self.p2p_service.as_ref().map(|service| service.snapshot())
    }

    #[allow(dead_code)]
    pub(crate) fn p2p_runtime_policy_version(&self) -> Option<u64> {
        self.p2p_service
            .as_ref()
            .map(|service| service.runtime_policy_version())
    }

    pub(crate) fn publish_p2p_chunk(&self, chunk_id: ChunkId, data: Bytes, mtime: i64) -> bool {
        self.p2p_service
            .as_ref()
            .is_some_and(|service| service.publish_chunk(chunk_id, data, mtime))
    }

    pub(crate) fn stop_p2p(&self) {
        if let Some(service) = &self.p2p_service {
            service.stop();
        }
    }

    pub(crate) fn p2p_snapshot(&self) -> (String, P2pStatsSnapshot) {
        if let Some(service) = &self.p2p_service {
            (service.peer_id().to_string(), service.snapshot())
        } else {
            ("disabled".to_string(), P2pStatsSnapshot::default())
        }
    }

    pub(crate) async fn apply_master_p2p_runtime_policy(
        &self,
        version: u64,
        peer_whitelist: Vec<String>,
        tenant_whitelist: Vec<String>,
        signature: String,
    ) -> FsResult<()> {
        let Some(service) = &self.p2p_service else {
            return Ok(());
        };
        if service
            .sync_runtime_policy_from_master(version, peer_whitelist, tenant_whitelist, signature)
            .await
        {
            Ok(())
        } else {
            err_box!("failed to apply p2p runtime policy version {}", version)
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.p2p_service.is_some()
    }

    pub(crate) fn try_read_local_chunk(&self, request: &AcceleratedReadRequest) -> Option<Bytes> {
        if !self.enabled() {
            return None;
        }
        self.read_chunk_cache.get(&request.read_key)
    }

    pub(crate) fn cache_local_chunk(&self, request: &AcceleratedReadRequest, data: Bytes) {
        if self.enabled() && !data.is_empty() {
            self.read_chunk_cache.insert(request.read_key.clone(), data);
        }
    }

    pub(crate) async fn acquire_flight(
        &self,
        request: &AcceleratedReadRequest,
    ) -> Option<ReadChunkFlight> {
        if !self.enabled() {
            return None;
        }
        let lock = if let Some(lock) = self.read_chunk_flights.get(&request.read_key) {
            lock.clone()
        } else {
            let lock = Arc::new(AsyncMutex::new(()));
            self.read_chunk_flights
                .entry(request.read_key.clone())
                .or_insert_with(|| lock.clone())
                .clone()
        };
        let guard = lock.clone().lock_owned().await;
        Some((lock, guard))
    }

    pub(crate) fn release_flight(
        &self,
        request: &AcceleratedReadRequest,
        flight: &mut Option<ReadChunkFlight>,
    ) {
        if let Some((lock, guard)) = flight.take() {
            drop(guard);
            if let Some(existing) = self.read_chunk_flights.get(&request.read_key) {
                let should_remove = Arc::ptr_eq(existing.value(), &lock);
                drop(existing);
                if should_remove {
                    self.read_chunk_flights.remove(&request.read_key);
                }
            }
        }
    }

    pub(crate) fn observe_adaptive_read_latency(&self, source: ReadSource, start_nanos: u128) {
        let elapsed_us = ((LocalTime::nanos() - start_nanos) / 1000).min(u64::MAX as u128) as u64;
        match source {
            ReadSource::WorkerLocal | ReadSource::WorkerRemote => {
                self.adaptive_worker_samples.fetch_add(1, Ordering::Relaxed);
                update_latency_ewma(&self.adaptive_worker_latency_us, elapsed_us);
            }
            ReadSource::P2p => {
                self.adaptive_p2p_samples.fetch_add(1, Ordering::Relaxed);
                update_latency_ewma(&self.adaptive_p2p_latency_us, elapsed_us);
            }
            _ => {}
        }
    }

    pub(crate) fn should_bypass_p2p(&self, source: ReadSource) -> bool {
        let worker_latency_us = self.adaptive_worker_latency_us.load(Ordering::Relaxed);
        let worker_samples = self.adaptive_worker_samples.load(Ordering::Relaxed);
        let p2p_latency_us = self.adaptive_p2p_latency_us.load(Ordering::Relaxed);
        let p2p_samples = self.adaptive_p2p_samples.load(Ordering::Relaxed);
        let probe_seq = self
            .adaptive_probe_seq
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        should_bypass_p2p_adaptive(
            source,
            worker_latency_us,
            worker_samples,
            p2p_latency_us,
            p2p_samples,
            probe_seq,
        )
    }

    pub(crate) async fn try_read_cached_p2p(
        &self,
        request: &AcceleratedReadRequest,
    ) -> Option<Bytes> {
        if request.source == ReadSource::Hole || request.expect_len == 0 {
            return None;
        }
        let Some(service) = &self.p2p_service else {
            return None;
        };
        let data = service
            .fetch_cached_chunk_with_context(
                request.chunk_id,
                request.expect_len,
                request.expected_mtime,
                Some(&request.trace_context),
            )
            .await?;
        self.cache_local_chunk(request, data.clone());
        Some(data)
    }

    pub(crate) async fn try_read_network_p2p(
        &self,
        request: &AcceleratedReadRequest,
    ) -> FsResult<Option<AcceleratedReadHit>> {
        if request.source == ReadSource::Hole || request.expect_len == 0 {
            return Ok(None);
        }
        if self.should_bypass_p2p(request.source) {
            return Ok(None);
        }
        let Some(service) = &self.p2p_service else {
            return Ok(None);
        };
        if let Some((data, origin)) = service
            .fetch_chunk_with_origin_context(
                request.chunk_id,
                request.expect_len,
                request.expected_mtime,
                Some(&request.trace_context),
            )
            .await
        {
            self.cache_local_chunk(request, data.clone());
            let adaptive_source = match origin {
                FetchChunkOrigin::Network => Some(ReadSource::P2p),
                FetchChunkOrigin::Cached => None,
            };
            return Ok(Some(AcceleratedReadHit {
                data,
                adaptive_source,
            }));
        }
        if service.conf().fallback_worker_on_fail {
            Ok(None)
        } else {
            err_box!("p2p read miss and worker fallback is disabled")
        }
    }

    pub(crate) fn on_worker_chunk_read(&self, request: &AcceleratedReadRequest, data: Bytes) {
        if data.is_empty() {
            return;
        }
        self.cache_local_chunk(request, data.clone());
        let _ = self.publish_p2p_chunk(
            request.chunk_id,
            data,
            request.expected_mtime.unwrap_or_default(),
        );
    }
}

fn update_latency_ewma(target: &AtomicU64, observed_us: u64) {
    loop {
        let current = target.load(Ordering::Relaxed);
        let next = if current == 0 {
            observed_us
        } else {
            let retained = current.saturating_mul(ADAPTIVE_EWMA_ALPHA_DEN.saturating_sub(1));
            retained
                .saturating_add(observed_us)
                .saturating_div(ADAPTIVE_EWMA_ALPHA_DEN)
        };
        if target
            .compare_exchange(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
    }
}

pub(crate) fn should_bypass_p2p_adaptive(
    source: ReadSource,
    worker_latency_us: u64,
    worker_samples: u64,
    p2p_latency_us: u64,
    p2p_samples: u64,
    probe_seq: u64,
) -> bool {
    if !matches!(source, ReadSource::WorkerLocal | ReadSource::WorkerRemote) {
        return false;
    }
    if worker_samples < ADAPTIVE_MIN_SAMPLES {
        return false;
    }
    if p2p_samples == 0 {
        return false;
    }
    if worker_latency_us == 0 || p2p_latency_us == 0 {
        return false;
    }
    if worker_latency_us.saturating_mul(ADAPTIVE_BYPASS_RATIO_DEN)
        > p2p_latency_us.saturating_mul(ADAPTIVE_BYPASS_RATIO_NUM)
    {
        return false;
    }
    !probe_seq.is_multiple_of(ADAPTIVE_PROBE_INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::{
        should_bypass_p2p_adaptive, ReadAccelerator, ADAPTIVE_MIN_SAMPLES, ADAPTIVE_PROBE_INTERVAL,
    };
    use crate::client_metrics::ReadSource;
    use curvine_common::conf::ClusterConf;

    #[test]
    fn adaptive_bypass_only_for_worker_source() {
        assert!(!should_bypass_p2p_adaptive(
            ReadSource::P2p,
            100,
            ADAPTIVE_MIN_SAMPLES,
            400,
            ADAPTIVE_MIN_SAMPLES,
            1,
        ));
    }

    #[test]
    fn adaptive_bypass_uses_periodic_probe() {
        assert!(should_bypass_p2p_adaptive(
            ReadSource::WorkerRemote,
            100,
            ADAPTIVE_MIN_SAMPLES,
            400,
            ADAPTIVE_MIN_SAMPLES,
            1,
        ));
        assert!(!should_bypass_p2p_adaptive(
            ReadSource::WorkerRemote,
            100,
            ADAPTIVE_MIN_SAMPLES,
            400,
            ADAPTIVE_MIN_SAMPLES,
            ADAPTIVE_PROBE_INTERVAL,
        ));
    }

    #[test]
    fn read_accelerator_disabled_without_p2p() {
        let conf = ClusterConf::default();
        let accelerator = ReadAccelerator::new(&conf, None);
        assert!(!accelerator.enabled());
    }
}
