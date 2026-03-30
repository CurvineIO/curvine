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

use crate::block::{BlockClient, BlockClientPool};
use crate::client_metrics::ReadSource;
use crate::file::{CurvineFileSystem, FsClient};
use crate::p2p::ChunkId;
use crate::p2p::{P2pService, P2pStatsSnapshot};
use crate::ClientMetrics;
use bytes::Bytes;
use curvine_common::conf::ClusterConf;
use curvine_common::proto::ClientAddressProto;
use curvine_common::state::{ClientAddress, WorkerAddress};
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use fxhash::FxHasher;
use log::warn;
use moka::policy::EvictionPolicy;
use moka::sync::{Cache, CacheBuilder};
use once_cell::sync::OnceCell;
use orpc::client::{ClientConf, ClusterConnector};
use orpc::common::{LocalTime, Utils};
use orpc::io::net::NetUtils;
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::FastDashMap;
use orpc::sys::CacheManager;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

static CLIENT_METRICS: OnceCell<ClientMetrics> = OnceCell::new();
const ADAPTIVE_EWMA_ALPHA_DEN: u64 = 8;
const ADAPTIVE_MIN_SAMPLES: u64 = 8;
const ADAPTIVE_BYPASS_RATIO_NUM: u64 = 9;
const ADAPTIVE_BYPASS_RATIO_DEN: u64 = 10;
const ADAPTIVE_PROBE_INTERVAL: u64 = 1024;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub(crate) struct ReadChunkKey {
    pub(crate) file_id: i64,
    pub(crate) version_epoch: i64,
    pub(crate) block_id: i64,
    pub(crate) off: i64,
}

impl ReadChunkKey {
    pub(crate) fn new(file_id: i64, version_epoch: i64, block_id: i64, off: i64) -> Self {
        Self {
            file_id,
            version_epoch,
            block_id,
            off,
        }
    }
}

// The core feature of the file system is thread-safe, which can be shared between multiple threads through Arc.
// 1. The cluster configuration file is saved.
// 2. Create client.
// 3. Perceive master switching.
pub struct FsContext {
    pub(crate) conf: ClusterConf,
    pub(crate) connector: Arc<ClusterConnector>,
    pub(crate) client_addr: ClientAddress,
    pub(crate) os_cache: CacheManager,
    pub(crate) failed_workers: Cache<u32, WorkerAddress, BuildHasherDefault<FxHasher>>,
    pub(crate) read_chunk_cache: Cache<ReadChunkKey, Bytes, BuildHasherDefault<FxHasher>>,
    pub(crate) read_chunk_flights: FastDashMap<ReadChunkKey, Arc<AsyncMutex<()>>>,
    pub(crate) block_pool: Arc<BlockClientPool>,
    pub(crate) p2p_service: Option<Arc<P2pService>>,
    pub(crate) adaptive_worker_latency_us: AtomicU64,
    pub(crate) adaptive_worker_samples: AtomicU64,
    pub(crate) adaptive_p2p_latency_us: AtomicU64,
    pub(crate) adaptive_p2p_samples: AtomicU64,
    pub(crate) adaptive_probe_seq: AtomicU64,
}

impl FsContext {
    pub fn new(conf: ClusterConf) -> FsResult<Self> {
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        Self::with_rt(conf, rt)
    }

    pub fn with_rt(conf: ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let hostname = conf.client.hostname.to_owned();
        let ip = NetUtils::local_ip(&hostname);
        let client_addr = ClientAddress {
            client_name: Utils::uuid(),
            hostname,
            ip_addr: ip,
            port: 0,
        };

        CLIENT_METRICS
            .get_or_init(|| ClientMetrics::new(&conf.client.metadata_operation_buckets).unwrap());
        Self::get_metrics().set_read_label_policy(
            conf.client.p2p.metrics_label_series_cap,
            conf.client.p2p.metrics_hash_job_id,
        );

        let connector = ClusterConnector::with_rt(conf.client_rpc_conf(), rt.clone());
        for node in conf.master_nodes() {
            connector.add_node(node)?;
        }

        let os_cache = CacheManager::new(
            conf.client.enable_read_ahead,
            conf.client.read_ahead_len,
            conf.client.drop_cache_len,
            conf.client.read_chunk_size as i64,
        );

        let exclude_workers = CacheBuilder::default()
            .time_to_live(conf.client.failed_worker_ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());
        let read_chunk_cache_capacity = (conf.client.p2p.cache_capacity
            / conf.client.read_chunk_size.max(1) as u64)
            .clamp(1, 65_536);
        let read_chunk_cache = CacheBuilder::default()
            .max_capacity(read_chunk_cache_capacity)
            .time_to_live(conf.client.p2p.cache_ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());
        let read_chunk_flights = FastDashMap::default();

        let block_pool = Arc::new(BlockClientPool::new(
            conf.client.enable_block_conn_pool,
            conf.client.block_conn_idle_size,
            conf.client.block_conn_idle_time.as_millis() as u64,
        ));
        let p2p_service = if conf.client.p2p.enable {
            let service = Arc::new(P2pService::new_with_runtime(
                conf.client.p2p.clone(),
                Some(rt.clone()),
            ));
            if service.is_enabled() {
                service.start();
            }
            Some(service)
        } else {
            None
        };

        let context = Self {
            conf,
            connector: Arc::new(connector),
            client_addr,
            os_cache,
            failed_workers: exclude_workers,
            read_chunk_cache,
            read_chunk_flights,
            block_pool,
            p2p_service,
            adaptive_worker_latency_us: AtomicU64::new(0),
            adaptive_worker_samples: AtomicU64::new(0),
            adaptive_p2p_latency_us: AtomicU64::new(0),
            adaptive_p2p_samples: AtomicU64::new(0),
            adaptive_probe_seq: AtomicU64::new(0),
        };
        Ok(context)
    }

    pub fn clone_client_name(&self) -> String {
        self.client_addr.client_name.clone()
    }

    pub fn clone_runtime(&self) -> Arc<Runtime> {
        self.connector.clone_runtime()
    }

    pub fn rt(&self) -> &Runtime {
        self.connector.rt()
    }

    pub fn is_local_worker(&self, addr: &WorkerAddress) -> bool {
        addr.is_local(&self.client_addr.hostname)
    }

    pub async fn block_client(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        let client = self
            .connector
            .create_client(&addr.inet_addr(), false)
            .await?;
        Ok(BlockClient::new(client, addr.clone(), self))
    }

    pub async fn acquire_write(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        self.block_pool.acquire_write(self, addr).await
    }

    pub async fn acquire_read(&self, addr: &WorkerAddress) -> IOResult<BlockClient> {
        self.block_pool.acquire_read(self, addr).await
    }

    pub fn read_chunk_size(&self) -> usize {
        self.conf.client.read_chunk_size
    }

    pub fn read_chunk_num(&self) -> usize {
        self.conf.client.read_chunk_num
    }

    pub fn read_parallel(&self) -> i64 {
        self.conf.client.read_parallel
    }

    pub fn read_since_size(&self) -> i64 {
        self.conf.client.read_slice_size
    }

    pub fn write_chunk_size(&self) -> usize {
        self.conf.client.write_chunk_size
    }

    pub fn write_chunk_num(&self) -> usize {
        self.conf.client.write_chunk_num
    }

    pub fn block_size(&self) -> i64 {
        self.conf.client.block_size
    }

    pub fn cluster_conf(&self) -> ClusterConf {
        self.conf.clone()
    }

    pub fn rpc_conf(&self) -> &ClientConf {
        self.connector.factory().conf()
    }

    pub fn clone_os_cache(&self) -> CacheManager {
        self.os_cache.clone()
    }

    pub fn p2p_service(&self) -> Option<Arc<P2pService>> {
        self.p2p_service.clone()
    }

    pub(crate) fn read_chunk_cache_enabled(&self) -> bool {
        self.p2p_service.is_some()
    }

    pub(crate) fn get_read_chunk_cache(&self, key: &ReadChunkKey) -> Option<Bytes> {
        if !self.read_chunk_cache_enabled() {
            return None;
        }
        self.read_chunk_cache.get(key)
    }

    pub(crate) fn put_read_chunk_cache(&self, key: ReadChunkKey, data: Bytes) {
        if self.read_chunk_cache_enabled() && !data.is_empty() {
            self.read_chunk_cache.insert(key, data);
        }
    }

    pub(crate) fn read_chunk_flight_lock(&self, key: ReadChunkKey) -> Arc<AsyncMutex<()>> {
        if let Some(lock) = self.read_chunk_flights.get(&key) {
            return lock.clone();
        }
        let lock = Arc::new(AsyncMutex::new(()));
        self.read_chunk_flights
            .entry(key)
            .or_insert_with(|| lock.clone())
            .clone()
    }

    pub(crate) fn cleanup_read_chunk_flight(&self, key: &ReadChunkKey, lock: &Arc<AsyncMutex<()>>) {
        if let Some(existing) = self.read_chunk_flights.get(key) {
            let should_remove = Arc::ptr_eq(existing.value(), lock);
            drop(existing);
            if should_remove {
                self.read_chunk_flights.remove(key);
            }
        }
    }

    pub(crate) fn on_worker_chunk_read(
        &self,
        read_key: ReadChunkKey,
        chunk_id: ChunkId,
        data: Bytes,
        mtime: i64,
    ) {
        if data.is_empty() {
            return;
        }
        self.put_read_chunk_cache(read_key, data.clone());
        if let Some(service) = self.p2p_service() {
            let _ = service.publish_chunk(chunk_id, data, mtime);
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

    pub fn get_metrics<'a>() -> &'a ClientMetrics {
        CLIENT_METRICS.get().expect("client get metrics error!")
    }

    // Exclude a worker
    pub fn add_failed_worker(&self, addr: &WorkerAddress) {
        self.failed_workers.insert(addr.worker_id, addr.clone())
    }

    pub fn is_failed_worker(&self, addr: &WorkerAddress) -> bool {
        self.failed_workers.contains_key(&addr.worker_id)
    }

    pub fn get_failed_workers(&self) -> Vec<u32> {
        let mut res = vec![];
        for item in self.failed_workers.iter() {
            res.push(item.1.worker_id);
        }

        res
    }

    pub fn client_addr_pb(&self) -> ClientAddressProto {
        ProtoUtils::client_address_to_pb(self.client_addr.clone())
    }

    pub fn exclude_workers(&self) -> Vec<u32> {
        self.failed_workers.iter().map(|x| x.1.worker_id).collect()
    }

    pub fn start_clean_task(fs: CurvineFileSystem, pool: Arc<BlockClientPool>) {
        let metric_report_enable = fs.conf().client.metric_report_enable;
        let interval = fs.conf().client.clean_task_interval;
        let rt = fs.clone_runtime();
        let fs_context = Arc::downgrade(&fs.fs_context);
        let snapshot_context = fs_context.clone();
        let metrics_context = fs_context.clone();
        let pool = Arc::downgrade(&pool);

        rt.spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;

                let Some(snapshot_fs_context) = snapshot_context.upgrade() else {
                    break;
                };
                let Some(pool) = pool.upgrade() else {
                    break;
                };
                pool.clear_idle_conn();
                sync_p2p_metrics(&snapshot_fs_context);

                if metric_report_enable {
                    let Some(fs_context) = metrics_context.upgrade() else {
                        break;
                    };
                    if let Err(e) = metrics_report(&fs_context).await {
                        warn!("metrics report: {}", e)
                    }
                }
            }
        });

        rt.spawn(async move {
            let Some(startup_fs_context) = fs_context.upgrade() else {
                return;
            };
            if let Err(e) = sync_p2p_runtime_policy(&startup_fs_context).await {
                warn!("sync p2p runtime policy: {}", e)
            }
            drop(startup_fs_context);

            let mut interval = tokio::time::interval(interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(runtime_fs_context) = fs_context.upgrade() else {
                    break;
                };
                if let Err(e) = sync_p2p_runtime_policy(&runtime_fs_context).await {
                    warn!("sync p2p runtime policy: {}", e)
                }
            }
        });
    }
}

fn sync_p2p_metrics(fs_context: &Arc<FsContext>) {
    let (service_id, snapshot) = if let Some(p2p_service) = fs_context.p2p_service() {
        (p2p_service.peer_id().to_string(), p2p_service.snapshot())
    } else {
        ("disabled".to_string(), P2pStatsSnapshot::default())
    };
    FsContext::get_metrics().sync_p2p_snapshot(&service_id, &snapshot);
}

async fn sync_p2p_runtime_policy(fs_context: &Arc<FsContext>) -> FsResult<()> {
    let Some(service) = fs_context.p2p_service() else {
        return Ok(());
    };
    let client = FsClient::new(fs_context.clone());
    let response = client.get_p2p_policy().await?;
    if service
        .sync_runtime_policy_from_master(
            response.p2p_policy_version,
            response.p2p_peer_whitelist,
            response.p2p_tenant_whitelist,
            response.p2p_policy_signature.unwrap_or_default(),
        )
        .await
    {
        Ok(())
    } else {
        orpc::err_box!(
            "failed to apply p2p runtime policy version {}",
            response.p2p_policy_version
        )
    }
}

async fn metrics_report(fs_context: &Arc<FsContext>) -> FsResult<()> {
    let metrics = ClientMetrics::encode()?;
    FsClient::new(fs_context.clone())
        .metrics_report(metrics)
        .await
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

fn should_bypass_p2p_adaptive(
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

impl Drop for FsContext {
    fn drop(&mut self) {
        if let Some(service) = self.p2p_service.as_ref() {
            service.stop();
        }
        self.block_pool.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        should_bypass_p2p_adaptive, FsContext, ADAPTIVE_MIN_SAMPLES, ADAPTIVE_PROBE_INTERVAL,
    };
    use crate::block::BlockClient;
    use crate::client_metrics::ReadSource;
    use crate::file::CurvineFileSystem;
    use crate::p2p::P2pState;
    use curvine_common::conf::ClusterConf;
    use curvine_common::state::WorkerAddress;
    use orpc::runtime::RpcRuntime;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

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
    fn fs_context_skips_p2p_service_when_disabled() {
        let conf = ClusterConf::default();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let ctx = FsContext::with_rt(conf, rt).expect("fs context should build");
        assert!(ctx.p2p_service().is_none());
    }

    #[test]
    fn fs_context_creates_p2p_service_when_enabled() {
        let mut conf = ClusterConf::default();
        conf.client.p2p.enable = true;
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let ctx = FsContext::with_rt(conf, rt).expect("fs context should build");
        let service = ctx.p2p_service().expect("p2p service should exist");
        assert_eq!(service.state(), P2pState::Running);
    }

    #[test]
    fn clean_tasks_do_not_keep_fs_context_alive() {
        let conf = ClusterConf::default();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = CurvineFileSystem::with_rt(conf, rt.clone()).expect("fs should build");
        let weak = Arc::downgrade(&fs.fs_context);

        drop(fs);
        rt.block_on(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
        });

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn drop_shutdown_clears_idle_block_pool_cycles() {
        let conf = ClusterConf::default();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let ctx = Arc::new(FsContext::with_rt(conf, rt).expect("fs context should build"));
        let weak_pool: Weak<_> = Arc::downgrade(&ctx.block_pool);

        let mut client = BlockClient::new_for_test(WorkerAddress {
            worker_id: 1,
            ..WorkerAddress::default()
        });
        client.set_pool(ctx.block_pool.clone());
        ctx.block_pool.release(client);
        assert_eq!(ctx.block_pool.idle_conn(), 1);

        drop(ctx);

        assert!(weak_pool.upgrade().is_none());
    }
}
