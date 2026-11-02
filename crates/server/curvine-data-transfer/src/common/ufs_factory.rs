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

use crate::JobWorkerClient;
use curvine_config::ClientConf;
use curvine_error::FsResult;
use curvine_model::{MountInfo, WorkerAddress};
use curvine_net::net::InetAddr;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_runtime::sync::FastSyncCache;
use curvine_unified_fs::{MountValue, UfsFileSystem};
use std::sync::Arc;
use std::time::Duration;

pub struct UfsFactory {
    rpc_conf: curvine_rpc::client::ClientConf,
    rt: Arc<Runtime>,
    grpc_channels: dashmap::DashMap<InetAddr, tonic::transport::Channel>,
    ufs_cache: FastSyncCache<u32, Arc<MountValue>>,
}

impl UfsFactory {
    pub fn with_rt(conf: &ClientConf, rt: Arc<Runtime>) -> Self {
        Self {
            rpc_conf: conf.client_rpc_conf(),
            rt,
            grpc_channels: dashmap::DashMap::new(),
            ufs_cache: FastSyncCache::with_ttl(Duration::from_millis(conf.mount_update_ttl_ms)),
        }
    }

    pub async fn get_worker_client(&self, worker: &WorkerAddress) -> FsResult<JobWorkerClient> {
        let timeout = Duration::from_millis(self.rpc_conf.rpc_timeout_ms);
        let address = InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);
        if let Some(channel) = self.grpc_channels.get(&address) {
            return Ok(JobWorkerClient::with_grpc(channel.clone(), timeout));
        }
        let uri = self
            .rpc_conf
            .grpc
            .uri(&worker.ip_addr, worker.rpc_port as u16);
        let tls = self
            .rpc_conf
            .grpc
            .client_tls(&worker.ip_addr)
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        let transport = curvine_rpc::grpc::TransportConfig {
            connect_timeout: Duration::from_millis(self.rpc_conf.conn_timeout_ms),
            request_timeout: Some(timeout),
            ..Default::default()
        };
        let endpoint = transport
            .endpoint(uri, tls.as_ref())
            .map_err(|e| curvine_error::FsError::common(e.to_string()))?;
        let channel = self
            .rt
            .spawn(async move { endpoint.connect_lazy() })
            .await
            .map_err(|e| {
                curvine_error::FsError::common(format!("worker channel setup failed: {e}"))
            })?;
        let channel = self.grpc_channels.entry(address).or_insert(channel).clone();
        Ok(JobWorkerClient::with_grpc(channel, timeout))
    }

    pub fn get_mnt(&self, mnt: &MountInfo) -> FsResult<Arc<MountValue>> {
        if let Some(v) = self.ufs_cache.get(&mnt.mount_id) {
            return Ok(v);
        };

        let mnt_value = Arc::new(MountValue::new(mnt.clone())?);
        self.ufs_cache.insert(mnt.mount_id, mnt_value.clone());
        Ok(mnt_value)
    }

    pub fn get_ufs(&self, mnt: &MountInfo) -> FsResult<UfsFileSystem> {
        self.get_mnt(mnt)?.ufs()
    }
}
