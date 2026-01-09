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

use crate::master::JobWorkerClient;
use curvine_client::unified::UfsFileSystem;
use curvine_common::conf::ClientConf;
use curvine_common::fs::Path;
use curvine_common::state::{MountInfo, WorkerAddress};
use curvine_common::FsResult;
use orpc::client::ClientFactory;
use orpc::io::net::InetAddr;
use orpc::runtime::Runtime;
use orpc::sync::FastSyncCache;
use std::sync::Arc;
use std::time::Duration;

pub struct UfsFactory {
    client_factory: ClientFactory,
    ufs_cache: FastSyncCache<u32, UfsFileSystem>,
}

impl UfsFactory {
    pub fn with_rt(conf: &ClientConf, rt: Arc<Runtime>) -> Self {
        let client_factory = ClientFactory::with_rt(conf.client_rpc_conf(), rt);
        Self {
            client_factory,
            ufs_cache: FastSyncCache::with_ttl(conf.mount_update_ttl),
        }
    }

    pub async fn get_worker_client(&self, worker: &WorkerAddress) -> FsResult<JobWorkerClient> {
        let worker_addr = InetAddr::new(worker.ip_addr.clone(), worker.rpc_port as u16);

        let client = self.client_factory.get(&worker_addr).await?;
        let timeout = Duration::from_millis(self.client_factory.conf().rpc_timeout_ms);
        let client = JobWorkerClient::new(client, timeout);
        Ok(client)
    }

    pub fn get_ufs(&self, mnt: &MountInfo) -> FsResult<UfsFileSystem> {
        if let Some(v) = self.ufs_cache.get(&mnt.mount_id) {
            return Ok(v);
        };
        let path = Path::from_str(&mnt.ufs_path)?;
        let ufs = UfsFileSystem::new(&path, mnt.properties.clone(), None)?;
        self.ufs_cache.insert(mnt.mount_id, ufs.clone());
        Ok(ufs)
    }
}
