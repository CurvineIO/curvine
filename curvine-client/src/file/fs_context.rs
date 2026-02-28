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
use crate::file::CurvineFileSystem;
use crate::p2p::P2pService;
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
use orpc::common::Utils;
use orpc::io::net::NetUtils;
use orpc::io::IOResult;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::FastDashMap;
use orpc::sys::CacheManager;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

static CLIENT_METRICS: OnceCell<ClientMetrics> = OnceCell::new();

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
    pub(crate) p2p_service: Option<Arc<P2pService>>,
    pub(crate) block_pool: Arc<BlockClientPool>,
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

        let read_chunk_cache = CacheBuilder::default()
            .max_capacity(conf.client.read_chunk_cache_capacity.max(1))
            .time_to_live(conf.client.read_chunk_cache_ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());

        let read_chunk_flights =
            FastDashMap::with_capacity(conf.client.read_chunk_cache_capacity.min(65536) as usize);

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

        let block_pool = Arc::new(BlockClientPool::new(
            conf.client.enable_block_conn_pool,
            conf.client.block_conn_idle_size,
            conf.client.block_conn_idle_time.as_millis() as u64,
        ));

        let context = Self {
            conf,
            connector: Arc::new(connector),
            client_addr,
            os_cache,
            failed_workers: exclude_workers,
            read_chunk_cache,
            read_chunk_flights,
            p2p_service,
            block_pool,
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

    pub fn read_chunk_cache_enabled(&self) -> bool {
        self.conf.client.enable_read_chunk_cache
    }

    pub(crate) fn get_read_chunk_cache(&self, key: &ReadChunkKey) -> Option<Bytes> {
        if !self.read_chunk_cache_enabled() {
            return None;
        }
        self.read_chunk_cache.get(key)
    }

    pub(crate) fn put_read_chunk_cache(&self, key: ReadChunkKey, data: Bytes) {
        if !self.read_chunk_cache_enabled() || data.is_empty() {
            return;
        }
        self.read_chunk_cache.insert(key, data);
    }

    pub(crate) fn read_chunk_flight_lock(&self, key: ReadChunkKey) -> Arc<AsyncMutex<()>> {
        self.read_chunk_flights
            .entry(key)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    pub(crate) fn cleanup_read_chunk_flight(&self, key: &ReadChunkKey, lock: &Arc<AsyncMutex<()>>) {
        let _ = self.read_chunk_flights.remove_if(key, |_, current| {
            Arc::ptr_eq(current, lock) && Arc::strong_count(current) == 2
        });
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

        fs.clone_runtime().spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;

                pool.clear_idle_conn();
                fs.sync_p2p_metrics();
                if let Some(p2p_service) = fs.fs_context.p2p_service() {
                    if p2p_service.is_enabled() {
                        if let Err(e) = fs.sync_p2p_runtime_policy().await {
                            warn!("sync p2p runtime policy: {}", e)
                        }
                    }
                }

                if metric_report_enable {
                    if let Err(e) = fs.metrics_report().await {
                        warn!("metrics report: {}", e)
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_read_chunk_flight_removes_entry_when_only_map_and_caller_ref_remain() {
        let conf = ClusterConf::default();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let context = FsContext::with_rt(conf, rt).unwrap();
        let key = ReadChunkKey::new(1, 1, 1, 0);

        let lock = context.read_chunk_flight_lock(key.clone());
        assert!(context.read_chunk_flights.contains_key(&key));
        assert_eq!(Arc::strong_count(&lock), 2);

        context.cleanup_read_chunk_flight(&key, &lock);

        assert!(!context.read_chunk_flights.contains_key(&key));
    }

    #[test]
    fn cleanup_read_chunk_flight_keeps_entry_when_other_readers_still_hold_refs() {
        let conf = ClusterConf::default();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let context = FsContext::with_rt(conf, rt).unwrap();
        let key = ReadChunkKey::new(2, 1, 1, 0);

        let lock = context.read_chunk_flight_lock(key.clone());
        let sibling_reader_ref = lock.clone();
        assert!(Arc::strong_count(&lock) >= 3);

        context.cleanup_read_chunk_flight(&key, &lock);
        assert!(context.read_chunk_flights.contains_key(&key));

        drop(sibling_reader_ref);
        context.cleanup_read_chunk_flight(&key, &lock);
        assert!(!context.read_chunk_flights.contains_key(&key));
    }

    #[test]
    fn disabled_p2p_should_not_create_cache_dir() {
        let mut conf = ClusterConf::default();
        conf.client.p2p.enable = false;
        let cache_dir =
            std::env::temp_dir().join(format!("curvine-p2p-disabled-cache-{}", Utils::uuid()));
        let _ = std::fs::remove_dir_all(&cache_dir);
        let _ = std::fs::remove_file(&cache_dir);
        conf.client.p2p.cache_dir = cache_dir.to_string_lossy().to_string();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let context = FsContext::with_rt(conf, rt).unwrap();
        assert!(context.p2p_service().is_none());
        assert!(!cache_dir.exists());
    }
}
