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
use crate::file::{CurvineFileSystem, FsClient, ReadAccelerator};
use crate::p2p::{P2pService, P2pState, P2pStatsSnapshot};
use crate::ClientMetrics;
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
use orpc::sys::CacheManager;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

static CLIENT_METRICS: OnceCell<ClientMetrics> = OnceCell::new();

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
    pub(crate) block_pool: Arc<BlockClientPool>,
    pub(crate) read_accelerator: ReadAccelerator,
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
        let read_accelerator = ReadAccelerator::new(&conf, p2p_service.clone());

        let context = Self {
            conf,
            connector: Arc::new(connector),
            client_addr,
            os_cache,
            failed_workers: exclude_workers,
            block_pool,
            read_accelerator,
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

    pub(crate) fn read_accelerator(&self) -> &ReadAccelerator {
        &self.read_accelerator
    }

    pub(crate) fn p2p_state(&self) -> Option<P2pState> {
        self.read_accelerator.p2p_state()
    }

    pub(crate) fn p2p_peer_id(&self) -> Option<String> {
        self.read_accelerator.p2p_peer_id()
    }

    pub(crate) fn p2p_bootstrap_peer_addr(&self) -> Option<String> {
        self.read_accelerator.p2p_bootstrap_peer_addr()
    }

    pub(crate) fn p2p_stats_snapshot(&self) -> Option<P2pStatsSnapshot> {
        self.read_accelerator.p2p_stats_snapshot()
    }

    pub(crate) fn p2p_runtime_policy_version(&self) -> Option<u64> {
        self.read_accelerator.p2p_runtime_policy_version()
    }

    pub(crate) fn stop_p2p(&self) {
        self.read_accelerator.stop_p2p();
    }

    pub(crate) fn p2p_snapshot(&self) -> (String, P2pStatsSnapshot) {
        self.read_accelerator.p2p_snapshot()
    }

    pub(crate) async fn apply_master_p2p_runtime_policy(
        &self,
        version: u64,
        peer_whitelist: Vec<String>,
        tenant_whitelist: Vec<String>,
        signature: String,
    ) -> FsResult<()> {
        self.read_accelerator
            .apply_master_p2p_runtime_policy(version, peer_whitelist, tenant_whitelist, signature)
            .await
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
    let (service_id, snapshot) = fs_context.p2p_snapshot();
    FsContext::get_metrics().sync_p2p_snapshot(&service_id, &snapshot);
}

async fn sync_p2p_runtime_policy(fs_context: &Arc<FsContext>) -> FsResult<()> {
    let client = FsClient::new(fs_context.clone());
    let response = client.get_p2p_policy().await?;
    fs_context
        .apply_master_p2p_runtime_policy(
            response.p2p_policy_version,
            response.p2p_peer_whitelist,
            response.p2p_tenant_whitelist,
            response.p2p_policy_signature.unwrap_or_default(),
        )
        .await
}

async fn metrics_report(fs_context: &Arc<FsContext>) -> FsResult<()> {
    let metrics = ClientMetrics::encode()?;
    FsClient::new(fs_context.clone())
        .metrics_report(metrics)
        .await
}

impl Drop for FsContext {
    fn drop(&mut self) {
        self.stop_p2p();
        self.block_pool.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::FsContext;
    use crate::block::BlockClient;
    use crate::file::CurvineFileSystem;
    use crate::p2p::P2pState;
    use curvine_common::conf::ClusterConf;
    use curvine_common::state::WorkerAddress;
    use orpc::runtime::RpcRuntime;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    #[test]
    fn fs_context_skips_p2p_service_when_disabled() {
        let conf = ClusterConf::default();
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let ctx = FsContext::with_rt(conf, rt).expect("fs context should build");
        assert_eq!(ctx.p2p_state(), None);
    }

    #[test]
    fn fs_context_creates_p2p_service_when_enabled() {
        let mut conf = ClusterConf::default();
        conf.client.p2p.enable = true;
        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let ctx = FsContext::with_rt(conf, rt).expect("fs context should build");
        assert_eq!(ctx.p2p_state(), Some(P2pState::Running));
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
