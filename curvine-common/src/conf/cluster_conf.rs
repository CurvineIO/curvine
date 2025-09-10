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

use crate::conf::{ClientConf, FuseConf, JobConf, JournalConf, MasterConf, WorkerConf};
use crate::rocksdb::DBConf;
use crate::version;
use log::info;
use orpc::client::{ClientConf as RpcConf, ClientFactory, SyncClient};
use orpc::common::{LogConf, Utils};
use orpc::io::net::{InetAddr, NodeAddr};
use orpc::io::retry::TimeBondedRetryBuilder;
use orpc::server::ServerConf;
use orpc::{try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::{Display, Formatter};
use std::fs::read_to_string;
use std::time::Duration;

// Cluster configuration files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConf {
    pub format_master: bool,

    pub format_worker: bool,

    // Whether it is in unit test state.In this state, the data will not flow normally, which facilitates unit tests to obtain data.
    pub testing: bool,

    pub cluster_id: String,

    pub master: MasterConf,

    // Log synchronization configuration.
    pub journal: JournalConf,

    pub worker: WorkerConf,

    pub log: LogConf,

    pub client: ClientConf,

    pub fuse: FuseConf,

    pub s3_gateway: S3GatewayConf,

    pub job: JobConf,
}

impl ClusterConf {
    pub const DEFAULT_HOSTNAME: &'static str = "localhost";
    pub const DEFAULT_MASTER_PORT: u16 = 8995;
    pub const DEFAULT_RAFT_PORT: u16 = 8996;
    pub const DEFAULT_WORKER_PORT: u16 = 8997;
    pub const DEFAULT_MASTER_WEB_PORT: u16 = 9000;
    pub const DEFAULT_WORKER_WEB_PORT: u16 = 9001;

    pub const ENV_MASTER_HOSTNAME: &'static str = "CURVINE_MASTER_HOSTNAME";
    pub const ENV_WORKER_HOSTNAME: &'static str = "CURVINE_WORKER_HOSTNAME";
    pub const ENV_CLIENT_HOSTNAME: &'static str = "CURVINE_CLIENT_HOSTNAME";
    pub const ENV_CONF_FILE: &'static str = "CURVINE_CONF_FILE";

    pub fn from<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let str = try_err!(read_to_string(path.as_ref()));
        let mut conf = try_err!(toml::from_str::<Self>(&str));

        // Check the environment variable configuration.
        if let Ok(v) = env::var(Self::ENV_MASTER_HOSTNAME) {
            conf.master.hostname = v.to_owned();
            conf.journal.hostname = v;
        }
        if let Ok(v) = env::var(Self::ENV_WORKER_HOSTNAME) {
            conf.worker.hostname = v;
        }
        if let Ok(v) = env::var(Self::ENV_CLIENT_HOSTNAME) {
            conf.client.hostname = v;
        }

        conf.master.init()?;
        conf.client.init()?;
        conf.fuse.init()?;
        conf.job.init()?;

        Ok(conf)
    }

    // Master service starts configuration.
    pub fn master_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.master.hostname, self.master.rpc_port);
        conf.name = format!("{}-master", self.cluster_id);
        conf.io_threads = self.master.io_threads;
        conf.worker_threads = self.master.worker_threads;
        // master will automatically close the idle connection, and the customer service will automatically maintain a heartbeat.
        conf.close_idle = self.master.io_close_idle;
        conf.timout_ms = self.master.io_timeout_ms();
        conf
    }

    pub fn master_web_conf(&self) -> ServerConf {
        let mut web_conf = ServerConf::with_hostname(&self.master.hostname, self.master.web_port);
        web_conf.name = format!("{}-master", self.cluster_id);
        web_conf.io_threads = self.master.io_threads;
        web_conf.worker_threads = self.master.worker_threads;
        web_conf
    }

    pub fn worker_addr(&self) -> InetAddr {
        InetAddr::new(self.worker.hostname.clone(), self.worker.rpc_port)
    }

    pub fn master_addr(&self) -> InetAddr {
        InetAddr::new(&self.master.hostname, self.master.rpc_port)
    }

    // Get all master nodes
    pub fn master_nodes(&self) -> Vec<NodeAddr> {
        let mut map = vec![];

        let start = 100;
        if self.client.master_addrs.is_empty() {
            map.push(NodeAddr::from_addr(start, self.master_addr()));
        } else {
            for (index, addr) in self.client.master_addrs.iter().enumerate() {
                let id = start + index as u64;
                map.push(NodeAddr::from_addr(id, addr.clone()));
            }
        }
        map
    }

    pub fn masters_string(&self) -> String {
        let res: Vec<String> = self
            .master_nodes()
            .iter()
            .map(|x| format!("{}", x.addr))
            .collect();
        res.join(",")
    }

    pub fn worker_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.worker.hostname, self.worker.rpc_port);
        conf.name = format!("{}-worker", self.cluster_id);
        conf.io_threads = self.worker.io_threads;
        conf.worker_threads = self.worker.worker_threads;

        // The raw client used by the worker does not currently implement heartbeat checks, so the default server does not actively close the connection.
        conf.close_idle = self.worker.io_close_idle;
        conf.timout_ms = self.worker.io_timeout_ms();

        conf.enable_splice = self.worker.enable_splice;
        conf.pipe_buf_size = self.worker.pipe_buf_size;
        conf.pipe_pool_init_cap = self.worker.pipe_pool_init_cap;
        conf.pipe_pool_max_cap = self.worker.pipe_pool_max_cap;
        conf.pipe_pool_idle_time = self.worker.pipe_pool_idle_time;

        conf.enable_send_file = self.worker.enable_send_file;
        conf
    }

    pub fn worker_web_conf(&self) -> ServerConf {
        let mut web_conf = ServerConf::with_hostname(&self.worker.hostname, self.worker.web_port);
        web_conf.name = format!("{}-web", self.cluster_id);
        web_conf.io_threads = self.worker.io_threads;
        web_conf.worker_threads = self.worker.worker_threads;
        web_conf
    }

    pub fn client_rpc_conf(&self) -> RpcConf {
        self.client.client_rpc_conf()
    }

    // Test use
    pub fn worker_sync_client(&self) -> CommonResult<SyncClient> {
        let factory = ClientFactory::new(self.client_rpc_conf());
        Ok(factory.create_sync(&self.worker_addr())?)
    }

    pub fn format() -> Self {
        Self {
            format_master: true,
            ..Default::default()
        }
    }

    // Test and modify the metadata-related path.
    pub fn change_test_meta_dir<T: AsRef<str>>(&mut self, name: T) {
        let base = Utils::cur_dir_sub(format!("../testing/{}", name.as_ref()));
        self.master.meta_dir = format!("{}/meta", base);
        self.journal.journal_dir = format!("{}/journal", base);
    }

    // Get the rocksdb configuration used to obtain metadata
    pub fn meta_rocks_conf(&self) -> DBConf {
        DBConf::new(&self.master.meta_dir)
            .set_compress_type(&self.master.meta_compression_type)
            .set_disable_wal(self.master.meta_disable_wal)
            .set_db_write_buffer_size(&self.master.meta_db_write_buffer_size)
            .set_write_buffer_size(&self.master.meta_write_buffer_size)
    }

    pub fn io_retry_policy_builder(&self) -> TimeBondedRetryBuilder {
        TimeBondedRetryBuilder::new(
            Duration::from_millis(self.client.rpc_retry_max_duration_ms),
            Duration::from_millis(self.client.rpc_retry_min_sleep_ms),
            Duration::from_millis(self.client.rpc_retry_max_sleep_ms),
        )
    }

    pub fn print(&self) {
        let conf = self.to_pretty_toml().unwrap();
        info!("git version: {}", version::GIT_VERSION);
        info!("cluster conf start: \n{}\n", conf);
    }

    pub fn to_pretty_toml(&self) -> CommonResult<String> {
        Ok(toml::to_string_pretty(self)?)
    }
}

impl Default for ClusterConf {
    fn default() -> Self {
        Self {
            format_master: true,
            format_worker: true,
            testing: false,
            cluster_id: "curvine".to_string(),
            master: Default::default(),
            journal: Default::default(),
            worker: Default::default(),
            log: Default::default(),
            client: Default::default(),
            fuse: FuseConf::default(),
            s3_gateway: Default::default(),
            job: Default::default(),
        }
    }
}

impl Display for ClusterConf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

/// S3 Object Gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3GatewayConf {
    pub listen: String,
    pub region: String,
    pub put_temp_dir: String,
    pub put_memory_buffer_threshold: usize,
    pub put_max_memory_buffer: usize,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub enable_distributed_auth: bool,
    pub credentials_path: Option<String>,
    pub cache_refresh_interval_secs: u64,
}

impl Default for S3GatewayConf {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:9900".to_string(),
            region: "us-east-1".to_string(),
            put_temp_dir: "/tmp/curvine-temp".to_string(),
            put_memory_buffer_threshold: 1048576, // 1MB
            put_max_memory_buffer: 16777216,      // 16MB
            access_key: None,
            secret_key: None,
            enable_distributed_auth: false,
            credentials_path: None,
            cache_refresh_interval_secs: 30,
        }
    }
}
