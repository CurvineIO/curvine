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

use crate::core::Session;
use crate::filesystem::FileSystemClient;
use crate::job::JobClient;
use crate::master::MasterClient;
use crate::FilesystemConf;
use curvine_common::conf::ClusterConf;
use curvine_common::FsResult;
use std::sync::Arc;

/// How to establish a Curvine libsdk session.
#[derive(Debug, Clone)]
pub enum ConnectOptions {
    /// Full `curvine-cluster.toml` path.
    ConfigFile(String),
    /// Master RPC endpoints only; remaining client settings use defaults.
    MasterAddrs(Vec<String>),
}

#[derive(Debug, Clone, Default)]
pub struct LibCurvineBuilder {
    masters: Vec<String>,
    conf_path: Option<String>,
    rpc_timeout_ms: Option<u64>,
}

impl LibCurvineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn conf_file(mut self, path: impl Into<String>) -> Self {
        self.conf_path = Some(path.into());
        self
    }

    pub fn masters(mut self, addrs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.masters = addrs.into_iter().map(Into::into).collect();
        self
    }

    pub fn rpc_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.rpc_timeout_ms = Some(timeout_ms);
        self
    }

    pub fn connect(self) -> FsResult<LibCurvine> {
        let has_conf = self.conf_path.is_some();
        let has_masters = !self.masters.is_empty();
        if has_conf && has_masters {
            return orpc::err_box!("LibCurvineBuilder: set conf_file or masters, not both");
        }
        let opts = if let Some(path) = self.conf_path {
            ConnectOptions::ConfigFile(path)
        } else if has_masters {
            ConnectOptions::MasterAddrs(self.masters)
        } else {
            return orpc::err_box!("LibCurvineBuilder requires conf_file or masters");
        };
        LibCurvine::connect_with_timeout(opts, self.rpc_timeout_ms)
    }
}

pub struct LibCurvine {
    session: Arc<Session>,
    master: MasterClient,
    job: JobClient,
    filesystem: FileSystemClient,
}

impl LibCurvine {
    pub fn connect(opts: ConnectOptions) -> FsResult<Self> {
        Self::connect_with_timeout(opts, None)
    }

    pub fn connect_file(conf_path: impl AsRef<str>) -> FsResult<Self> {
        Self::connect(ConnectOptions::ConfigFile(conf_path.as_ref().to_string()))
    }

    pub fn connect_masters(addrs: impl Into<Vec<String>>) -> FsResult<Self> {
        Self::connect(ConnectOptions::MasterAddrs(addrs.into()))
    }

    pub fn builder() -> LibCurvineBuilder {
        LibCurvineBuilder::new()
    }

    fn connect_with_timeout(opts: ConnectOptions, rpc_timeout_ms: Option<u64>) -> FsResult<Self> {
        let conf = match opts {
            ConnectOptions::ConfigFile(path) => ClusterConf::from(path)?,
            ConnectOptions::MasterAddrs(addrs) => {
                FilesystemConf::with_master_addrs(addrs)?.into_cluster_conf()?
            }
        };
        let conf = apply_rpc_timeout(conf, rpc_timeout_ms);
        let session = Arc::new(Session::from_cluster_conf(conf)?);
        Ok(Self {
            master: MasterClient::new(session.clone()),
            job: JobClient::new(session.clone()),
            filesystem: FileSystemClient::new(session.clone()),
            session,
        })
    }

    pub fn master(&self) -> &MasterClient {
        &self.master
    }

    pub fn job(&self) -> &JobClient {
        &self.job
    }

    pub fn filesystem(&self) -> &FileSystemClient {
        &self.filesystem
    }

    pub fn session(&self) -> &Session {
        &self.session
    }
}

fn apply_rpc_timeout(mut conf: ClusterConf, rpc_timeout_ms: Option<u64>) -> ClusterConf {
    if let Some(timeout_ms) = rpc_timeout_ms {
        conf.client.rpc_timeout_ms = timeout_ms;
    }
    conf
}
