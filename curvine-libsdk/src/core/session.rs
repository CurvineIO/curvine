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

use crate::FilesystemConf;
use curvine_client::file::FsClient;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::FsResult;
use orpc::common::Logger;
use orpc::runtime::Runtime;
use std::sync::Arc;

#[derive(Clone)]
pub struct Session {
    rt: Arc<Runtime>,
    unified: UnifiedFileSystem,
}

impl Session {
    pub fn from_cluster_conf(conf: ClusterConf) -> FsResult<Self> {
        Logger::init(conf.log.clone());

        let rpc_conf = conf.client_rpc_conf();
        let rt = Arc::new(rpc_conf.create_runtime());
        let unified = UnifiedFileSystem::with_rt(conf, rt.clone())?;
        Ok(Self { rt, unified })
    }

    pub fn from_conf_path(path: impl AsRef<str>) -> FsResult<Self> {
        let conf = ClusterConf::from(path.as_ref())?;
        Self::from_cluster_conf(conf)
    }

    pub fn from_master_addrs(addrs: &[String]) -> FsResult<Self> {
        Self::from_cluster_conf(FilesystemConf::with_master_addrs(addrs)?.into_cluster_conf()?)
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.rt
    }

    pub fn unified(&self) -> &UnifiedFileSystem {
        &self.unified
    }

    pub fn fs_client(&self) -> Arc<FsClient> {
        self.unified.fs_client()
    }
}
