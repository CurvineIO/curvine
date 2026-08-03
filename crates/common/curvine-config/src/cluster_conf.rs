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

use crate::{CliConf, ClientConf, FuseConf, JobConf, TransferConf};
use log::info;
use nix::ifaddrs::getifaddrs;
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_net::net::{InetAddr, NodeAddr};
use curvine_net::retry::TimeBondedRetryBuilder;
use curvine_rpc::client::ClientConf as RpcConf;
use curvine_runtime::common::LogConf;
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::{Display, Formatter};
use std::fs::read_to_string;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MasterConf {
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,
}

impl Default for MasterConf {
    fn default() -> Self {
        Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: ClusterConf::DEFAULT_MASTER_PORT,
            web_port: ClusterConf::DEFAULT_MASTER_WEB_PORT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JournalPeer {
    pub id: u64,
    pub hostname: String,
    pub port: u16,
}

impl Default for JournalPeer {
    fn default() -> Self {
        Self {
            id: 1,
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            port: ClusterConf::DEFAULT_RAFT_PORT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JournalConf {
    pub hostname: String,
    pub rpc_port: u16,
    pub journal_addrs: Vec<JournalPeer>,
}

impl Default for JournalConf {
    fn default() -> Self {
        Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: ClusterConf::DEFAULT_RAFT_PORT,
            journal_addrs: vec![JournalPeer::default()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConf {
    pub format_master: bool,
    pub format_worker: bool,
    pub testing: bool,
    pub cluster_id: String,
    pub net_interface: String,
    pub master: MasterConf,
    pub journal: JournalConf,
    pub log: LogConf,
    pub client: ClientConf,
    pub fuse: FuseConf,
    pub job: JobConf,
    pub transfer: TransferConf,
    pub cli: CliConf,
}

impl ClusterConf {
    pub const DEFAULT_HOSTNAME: &'static str = "localhost";
    pub const DEFAULT_MASTER_PORT: u16 = 8995;
    pub const DEFAULT_RAFT_PORT: u16 = 8996;
    pub const DEFAULT_MASTER_WEB_PORT: u16 = 9000;

    pub const ENV_MASTER_HOSTNAME: &'static str = "CURVINE_MASTER_HOSTNAME";
    pub const ENV_WORKER_HOSTNAME: &'static str = "CURVINE_WORKER_HOSTNAME";
    pub const ENV_CLIENT_HOSTNAME: &'static str = "CURVINE_CLIENT_HOSTNAME";
    pub const ENV_CONF_FILE: &'static str = "CURVINE_CONF_FILE";

    pub fn from<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let raw = try_err!(read_to_string(path.as_ref()));
        let mut conf = try_err!(toml::from_str::<Self>(&raw));

        if !conf.net_interface.is_empty() {
            let ip = Self::interface_ipv4(&conf.net_interface)?;
            for env_key in [
                Self::ENV_MASTER_HOSTNAME,
                Self::ENV_WORKER_HOSTNAME,
                Self::ENV_CLIENT_HOSTNAME,
            ] {
                if let Ok(v) = env::var(env_key) {
                    eprintln!(
                        "[WARN] net_interface '{}' is set (resolved to {}); ignoring {}='{}'.",
                        conf.net_interface, ip, env_key, v
                    );
                }
            }

            conf.master.hostname = ip.clone();
            conf.journal.hostname = ip.clone();
            conf.client.hostname = ip;
        } else {
            if let Ok(v) = env::var(Self::ENV_MASTER_HOSTNAME) {
                conf.master.hostname = v.clone();
                conf.journal.hostname = v;
            }

            if let Ok(v) = env::var(Self::ENV_CLIENT_HOSTNAME) {
                conf.client.hostname = v;
            }
        }

        conf.client.init()?;
        conf.fuse.init()?;
        conf.job.init()?;
        conf.transfer.init()?;

        if conf.client.master_addrs.is_empty() {
            if conf.journal.journal_addrs.is_empty() {
                conf.client
                    .master_addrs
                    .push(InetAddr::new(&conf.master.hostname, conf.master.rpc_port));
            } else {
                for peer in &conf.journal.journal_addrs {
                    conf.client
                        .master_addrs
                        .push(InetAddr::new(&peer.hostname, conf.master.rpc_port));
                }
            }
        }

        Ok(conf)
    }

    pub fn master_addr(&self) -> InetAddr {
        InetAddr::new(&self.master.hostname, self.master.rpc_port)
    }

    pub fn master_nodes(&self) -> Vec<NodeAddr> {
        let mut map = Vec::new();
        let start = 100;

        if self.client.master_addrs.is_empty() {
            map.push(NodeAddr::from_addr(start, self.master_addr()));
        } else {
            for (index, addr) in self.client.master_addrs.iter().enumerate() {
                map.push(NodeAddr::from_addr(start + index as u64, addr.clone()));
            }
        }

        map
    }

    pub fn masters_string(&self) -> String {
        self.master_nodes()
            .iter()
            .map(|x| format!("{}", x.addr))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn client_rpc_conf(&self) -> RpcConf {
        self.client.client_rpc_conf()
    }

    pub fn io_retry_policy_builder(&self) -> TimeBondedRetryBuilder {
        TimeBondedRetryBuilder::new(
            Duration::from_millis(self.client.rpc_retry_max_duration_ms),
            Duration::from_millis(self.client.rpc_retry_min_sleep_ms),
            Duration::from_millis(self.client.rpc_retry_max_sleep_ms),
        )
    }

    pub fn print(&self) {
        let conf = self
            .to_pretty_toml()
            .unwrap_or_else(|_| "<invalid>".to_string());
        info!("cluster conf start: \n{}\n", conf);
    }

    pub fn to_pretty_toml(&self) -> CommonResult<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    pub fn interface_ipv4<T: AsRef<str>>(interface: T) -> CommonResult<String> {
        let interface = interface.as_ref();
        let addrs = try_err!(getifaddrs());
        for ifaddr in addrs {
            if ifaddr.interface_name != interface {
                continue;
            }
            if let Some(address) = ifaddr.address {
                if let Some(sin) = address.as_sockaddr_in() {
                    return Ok(sin.ip().to_string());
                }
            }
        }
        err_box!("no IPv4 address found on network interface '{}'", interface)
    }
}

impl Default for ClusterConf {
    fn default() -> Self {
        Self {
            format_master: true,
            format_worker: true,
            testing: false,
            cluster_id: "curvine".to_string(),
            net_interface: String::new(),
            master: Default::default(),
            journal: Default::default(),
            log: Default::default(),
            client: Default::default(),
            fuse: Default::default(),
            job: Default::default(),
            transfer: Default::default(),
            cli: Default::default(),
        }
    }
}

impl Display for ClusterConf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_workspace_cluster_config() {
        let path = format!(
            "{}/../../../etc/curvine-cluster.toml",
            env!("CARGO_MANIFEST_DIR")
        );
        let conf = ClusterConf::from(path).unwrap();

        assert_eq!(conf.master.rpc_port, ClusterConf::DEFAULT_MASTER_PORT);
        assert!(!conf.client.master_addrs.is_empty());
    }
}
