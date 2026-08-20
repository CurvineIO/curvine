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

use crate::{
    CliConf, ClientConf, DBConf, FuseConf, JobConf, JournalConf, MasterConf, TransferConf,
    WorkerConf,
};
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_fault::FaultHttpConfig;
use curvine_net::net::{InetAddr, NodeAddr};
use curvine_net::retry::TimeBondedRetryBuilder;
use curvine_rpc::client::{ClientConf as RpcConf, ClientFactory, SyncClient};
use curvine_rpc::ServerConf;
use curvine_runtime::common::{LogConf, Utils};
use log::info;
use nix::ifaddrs::getifaddrs;
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

    /// Network interface (e.g. `eth0`) used to resolve the local IPv4 address
    /// that is applied to the master/journal/worker/client hostnames. Empty by
    /// default: when empty, the original hostname configuration is kept as-is.
    pub net_interface: String,

    pub master: MasterConf,

    // Log synchronization configuration.
    pub journal: JournalConf,

    pub worker: WorkerConf,

    /// Test-only fault HTTP control plane.
    ///
    /// Fault point instrumentation is controlled by Cargo features. This
    /// setting only controls HTTP route exposure; enabling it without the
    /// corresponding Cargo feature fails server startup.
    pub fault_injection: FaultHttpConfig,

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
    pub const DEFAULT_WORKER_PORT: u16 = 8997;
    pub const DEFAULT_MASTER_WEB_PORT: u16 = 9001;
    pub const DEFAULT_WORKER_WEB_PORT: u16 = 9002;
    pub const DEFAULT_FUSE_WEB_PORT: u16 = 9003;

    pub const ENV_MASTER_HOSTNAME: &'static str = "CURVINE_MASTER_HOSTNAME";
    pub const ENV_WORKER_HOSTNAME: &'static str = "CURVINE_WORKER_HOSTNAME";
    pub const ENV_CLIENT_HOSTNAME: &'static str = "CURVINE_CLIENT_HOSTNAME";
    pub const ENV_TRANSFER_HOSTNAME: &'static str = "CURVINE_TRANSFER_HOSTNAME";
    pub const ENV_CONF_FILE: &'static str = "CURVINE_CONF_FILE";

    pub fn from<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let mut conf = Self::read(path)?;
        conf.normalize_whitespace();
        conf.apply_hostname_overrides()?;
        conf.master.init()?;
        conf.client.init()?;
        conf.fuse.init()?;
        conf.job.init()?;
        conf.transfer.init()?;
        conf.resolve_master_addrs();
        Ok(conf)
    }

    /// Load only the configuration needed by the standalone Transfer service.
    pub fn from_transfer<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let mut conf = Self::read(path)?;
        conf.normalize_whitespace();
        conf.apply_transfer_hostname_overrides()?;
        conf.client.init()?;
        conf.transfer.init()?;
        conf.resolve_master_addrs();
        Ok(conf)
    }

    fn read<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let str = try_err!(read_to_string(path.as_ref()));
        Ok(try_err!(toml::from_str::<Self>(&str)))
    }

    fn normalize_whitespace(&mut self) {
        self.net_interface = self.net_interface.trim().to_string();
        self.master.hostname = self.master.hostname.trim().to_string();
        self.journal.hostname = self.journal.hostname.trim().to_string();
        self.worker.hostname = self.worker.hostname.trim().to_string();
        self.client.hostname = self.client.hostname.trim().to_string();
        self.transfer.hostname = self.transfer.hostname.trim().to_string();
        for addr in &mut self.client.master_addrs {
            addr.hostname = addr.hostname.trim().to_string();
        }
        for peer in &mut self.journal.journal_addrs {
            peer.hostname = peer.hostname.trim().to_string();
        }
        self.worker.data_dir = self
            .worker
            .data_dir
            .iter()
            .map(|dir| dir.trim().to_string())
            .filter(|dir| !dir.is_empty())
            .collect();
        self.transfer.endpoints = self
            .transfer
            .endpoints
            .iter()
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect();
    }

    fn apply_hostname_overrides(&mut self) -> CommonResult<()> {
        // Hostname resolution is either/or. A configured network interface
        // (non-empty, e.g. `eth0`) wins: resolve its local IPv4 and apply it to
        // every role hostname, ignoring the hostname env vars. An empty
        // interface honors the per-role hostname env-var overrides instead.
        if !self.net_interface.is_empty() {
            let ip = Self::interface_ipv4(&self.net_interface)?;

            // net_interface takes precedence over the CURVINE_*_HOSTNAME env
            // vars. Warn (rather than silently ignore) when any of them is also
            // set, so an operator who exported a hostname override but sees it
            // have no effect can tell why. `from()` runs during config loading,
            // before `Logger::init`, so the log macros would be dropped — use
            // eprintln! to make the warning visible on stderr.
            for env_key in [
                Self::ENV_MASTER_HOSTNAME,
                Self::ENV_WORKER_HOSTNAME,
                Self::ENV_CLIENT_HOSTNAME,
                Self::ENV_TRANSFER_HOSTNAME,
            ] {
                if let Ok(v) = env::var(env_key) {
                    eprintln!(
                        "[WARN] net_interface '{}' is set (resolved to {}); ignoring {}='{}'. \
                         net_interface overrides the CURVINE_*_HOSTNAME env vars.",
                        self.net_interface, ip, env_key, v
                    );
                }
            }

            self.master.hostname = ip.clone();
            self.journal.hostname = ip.clone();
            self.worker.hostname = ip.clone();
            self.client.hostname = ip.clone();
            self.transfer.hostname = ip;
        } else {
            if let Ok(v) = env::var(Self::ENV_MASTER_HOSTNAME) {
                let hostname = v.trim().to_string();
                self.master.hostname = hostname.clone();
                self.journal.hostname = hostname;
            }

            // Apply worker hostname from environment variable (used by worker process)
            if let Ok(v) = env::var(Self::ENV_WORKER_HOSTNAME) {
                self.worker.hostname = v.trim().to_string();
            }

            // Apply client hostname from environment variable
            if let Ok(v) = env::var(Self::ENV_CLIENT_HOSTNAME) {
                self.client.hostname = v.trim().to_string();
            }

            if let Ok(v) = env::var(Self::ENV_TRANSFER_HOSTNAME) {
                self.transfer.hostname = v.trim().to_string();
            }
        }

        Ok(())
    }

    fn apply_transfer_hostname_overrides(&mut self) -> CommonResult<()> {
        if !self.net_interface.is_empty() {
            let ip = Self::interface_ipv4(&self.net_interface)?;
            for env_key in [Self::ENV_CLIENT_HOSTNAME, Self::ENV_TRANSFER_HOSTNAME] {
                if let Ok(v) = env::var(env_key) {
                    eprintln!(
                        "[WARN] net_interface '{}' is set (resolved to {}); ignoring {}='{}'. \\
                         net_interface overrides the CURVINE_*_HOSTNAME env vars.",
                        self.net_interface, ip, env_key, v
                    );
                }
            }
            self.client.hostname = ip.clone();
            self.transfer.hostname = ip;
        } else {
            if let Ok(v) = env::var(Self::ENV_CLIENT_HOSTNAME) {
                self.client.hostname = v.trim().to_string();
            }
            if let Ok(v) = env::var(Self::ENV_TRANSFER_HOSTNAME) {
                self.transfer.hostname = v.trim().to_string();
            }
        }
        Ok(())
    }

    fn resolve_master_addrs(&mut self) {
        if self.client.master_addrs.is_empty() {
            if self.journal.journal_addrs.is_empty() {
                self.client
                    .master_addrs
                    .push(InetAddr::new(&self.master.hostname, self.master.rpc_port));
            } else {
                for peer in &self.journal.journal_addrs {
                    self.client
                        .master_addrs
                        .push(InetAddr::new(&peer.hostname, self.master.rpc_port));
                }
            }
        }
    }

    pub fn check_master_hostname(&mut self) -> CommonResult<()> {
        // With `net_interface`, `master.hostname` is the NIC-resolved IPv4, so
        // validate the property raft actually depends on: the local journal
        // address must resolve to a node id in `journal_addrs`. Checking it here
        // turns the otherwise opaque, deep-in-raft "Not a master role" failure
        // into a clear, actionable startup error.
        //
        // Note: `JournalConfExt::node_id` lives in curvine-raft; to avoid a
        // config→raft dependency cycle we inline the same peer lookup here.
        if !self.net_interface.is_empty() {
            let local = self.journal.local_addr();
            let found = self
                .journal
                .journal_addrs
                .iter()
                .any(|peer| peer.hostname == local.hostname && peer.port == local.port);
            if !found {
                return err_box!(
                    "net_interface '{}' resolved journal address to '{}', which is not found in \
                     journal_addrs [{}]. When using net_interface, each node's entry in \
                     journal_addrs must use the exact IPv4 that the interface resolves to.",
                    self.net_interface,
                    local,
                    self.journal
                        .journal_addrs
                        .iter()
                        .map(|peer| format!("{}:{}", peer.hostname, peer.port))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            return Ok(());
        }

        let hostname_exists = self
            .journal
            .journal_addrs
            .iter()
            .any(|peer| peer.hostname == self.master.hostname);

        if !hostname_exists {
            return err_box!(
                "hostname '{}' from {} is not found in journal_addrs. Available hostnames: [{}]",
                self.master.hostname,
                Self::ENV_MASTER_HOSTNAME,
                self.journal
                    .journal_addrs
                    .iter()
                    .map(|peer| peer.hostname.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        Ok(())
    }

    // Master service starts configuration.
    pub fn master_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.master.hostname, self.master.rpc_port);
        conf.name = format!("{}-master", self.cluster_id);
        conf.io_threads = self.master.io_threads;
        conf.worker_threads = self.master.worker_threads;
        // master will automatically close the idle connection, and the customer service will automatically maintain a heartbeat.
        conf.close_idle = self.master.io_close_idle;
        conf.timeout_ms = self.master.io_timeout_ms();
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
        conf.timeout_ms = self.worker.io_timeout_ms();

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

    pub fn transfer_server_conf(&self) -> ServerConf {
        self.transfer.server_conf(&self.cluster_id)
    }

    pub fn transfer_web_conf(&self) -> ServerConf {
        let mut web_conf =
            ServerConf::with_hostname(&self.transfer.hostname, self.transfer.web_port);
        web_conf.name = format!("{}-transfer-web", self.cluster_id);
        web_conf.io_threads = self.transfer.io_threads;
        web_conf.worker_threads = self.transfer.worker_threads;
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
        let pid = std::process::id();
        let rand = Utils::rand_str(6);
        let base = Utils::cur_dir_sub(format!(
            "../target/testing/{}_{}_{}",
            name.as_ref(),
            pid,
            rand
        ));
        self.master.meta_dir = format!("{}/meta", base);
        self.journal.journal_dir = format!("{}/journal", base);
    }

    // Get the rocksdb configuration used to obtain metadata
    pub fn db_conf(&self) -> DBConf {
        self.master.rocksdb.clone().set_dir(&self.master.meta_dir)
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
        // Allocator / git version logging stays in server entrypoints so
        // curvine-config does not depend on curvine-common alloc/version helpers.
        info!("cluster conf start: \n{}\n", conf);
    }

    pub fn to_pretty_toml(&self) -> CommonResult<String> {
        Ok(toml::to_string_pretty(self)?)
    }

    /// Resolve the local IPv4 address bound to the named network interface
    /// (e.g. `eth0`).
    ///
    /// Enumerates the host's interface addresses via `getifaddrs(3)` and returns
    /// the first IPv4 address whose interface name matches `interface`. Returns
    /// an error if the interface does not exist or has no IPv4 address assigned
    /// (an IPv6-only interface yields no match).
    pub fn interface_ipv4<T: AsRef<str>>(interface: T) -> CommonResult<String> {
        let interface = interface.as_ref();
        let addrs = try_err!(getifaddrs());
        for ifaddr in addrs {
            if ifaddr.interface_name != interface {
                continue;
            }
            // Only entries carrying an address are relevant; an interface can
            // also surface broadcast/netmask-only rows we must skip.
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
            worker: Default::default(),
            fault_injection: Default::default(),
            log: Default::default(),
            client: Default::default(),
            fuse: FuseConf::default(),
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
    use super::ClusterConf;
    use crate::RaftPeer;
    use curvine_runtime::common::Utils;

    // The loopback interface is present on every supported host and always
    // carries 127.0.0.1, so it is a stable target for the happy path.
    #[test]
    fn interface_ipv4_resolves_loopback() {
        #[cfg(target_os = "macos")]
        let loopback = "lo0";
        #[cfg(not(target_os = "macos"))]
        let loopback = "lo";

        let ip = ClusterConf::interface_ipv4(loopback)
            .expect("loopback interface must resolve to an IPv4 address");
        assert_eq!(ip, "127.0.0.1");
    }

    // A non-existent interface has no matching IPv4 entry and must error rather
    // than silently returning a bogus address.
    #[test]
    fn interface_ipv4_unknown_interface_errors() {
        let res = ClusterConf::interface_ipv4("curvine_no_such_if0");
        assert!(res.is_err(), "unknown interface must return an error");
    }

    // With net_interface set, check_master_hostname validates that the local
    // journal address (journal.hostname:rpc_port) resolves to a node id in
    // journal_addrs. When journal_addrs contains a matching entry, it passes.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn check_master_hostname_net_interface_matching_journal_addr_ok() {
        let mut conf = ClusterConf::default();
        conf.net_interface = "eth0".to_string();
        conf.journal.hostname = "10.0.0.5".to_string();
        conf.journal.rpc_port = 8996;
        conf.journal.journal_addrs = vec![RaftPeer::new(1, "10.0.0.5", 8996)];

        assert!(
            conf.check_master_hostname().is_ok(),
            "local journal address present in journal_addrs must pass"
        );
    }

    // With net_interface set, if the local journal address is absent from
    // journal_addrs, node_id resolution fails and check_master_hostname must
    // surface a clear error instead of deferring to an opaque raft failure.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn check_master_hostname_net_interface_missing_journal_addr_errors() {
        let mut conf = ClusterConf::default();
        conf.net_interface = "eth0".to_string();
        conf.journal.hostname = "10.0.0.5".to_string();
        conf.journal.rpc_port = 8996;
        // journal_addrs lists a different address, so the local node is not found.
        conf.journal.journal_addrs = vec![RaftPeer::new(1, "10.0.0.9", 8996)];

        assert!(
            conf.check_master_hostname().is_err(),
            "local journal address absent from journal_addrs must error"
        );
    }

    #[test]
    fn trims_whitespace_in_hostname_config() {
        let path = std::env::temp_dir().join(format!(
            "curvine-trim-conf-{}-{}.toml",
            std::process::id(),
            Utils::rand_str(6)
        ));
        std::fs::write(
            &path,
            r#"
                net_interface = " "

                [master]
                hostname = " master-01.example "

                [journal]
                hostname = " journal-01.example "
                journal_addrs = [
                    { id = 1, hostname = " journal-01.example ", port = 8996 },
                    { id = 2, hostname = "journal-02.example", port = 8996 },
                ]

                [worker]
                hostname = " worker-01.example "
                data_dir = [" /data/curvine ", "/data/curvine2"]

                [client]
                hostname = " client-01.example "
                master_addrs = [
                    { hostname = " curvine-master-01.oppo.local", port = 8995 },
                    { hostname = " curvine-master-02.oppo.local", port = 8995 },
                    { hostname = "curvine-master-03.oppo.local", port = 8995 },
                ]

                [transfer]
                hostname = " transfer-01.example "
                endpoints = [" transfer-01.example:9010 ", "transfer-02.example:9010"]
            "#,
        )
        .unwrap();

        let conf = ClusterConf::from(path.to_str().unwrap()).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(conf.net_interface, "");
        assert_eq!(conf.master.hostname, "master-01.example");
        assert_eq!(conf.journal.hostname, "journal-01.example");
        assert_eq!(conf.journal.journal_addrs[0].hostname, "journal-01.example");
        assert_eq!(conf.worker.hostname, "worker-01.example");
        assert_eq!(
            conf.worker.data_dir,
            vec!["/data/curvine", "/data/curvine2"]
        );
        assert_eq!(conf.client.hostname, "client-01.example");
        assert_eq!(
            conf.client.master_addrs[1].hostname,
            "curvine-master-02.oppo.local"
        );
        assert_eq!(conf.transfer.hostname, "transfer-01.example");
        assert_eq!(
            conf.transfer.endpoints,
            vec!["transfer-01.example:9010", "transfer-02.example:9010"]
        );
    }

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

    #[test]
    fn transfer_init_skips_unused_master_validation() {
        let path = std::env::temp_dir().join(format!(
            "curvine-transfer-conf-{}-{}.toml",
            std::process::id(),
            Utils::rand_str(6)
        ));
        std::fs::write(
            &path,
            r#"
                [master]
                conn_limit = 0

                [transfer]
                enabled = true
            "#,
        )
        .unwrap();

        let conf = ClusterConf::from_transfer(path.to_str().unwrap()).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(!conf.client.master_addrs.is_empty());
    }

    #[test]
    fn transfer_net_interface_keeps_master_address() {
        #[cfg(target_os = "macos")]
        let loopback = "lo0";
        #[cfg(not(target_os = "macos"))]
        let loopback = "lo";

        let mut conf = ClusterConf {
            net_interface: loopback.to_string(),
            ..Default::default()
        };
        conf.master.hostname = "cv-master".to_string();
        conf.journal.journal_addrs.clear();

        conf.apply_transfer_hostname_overrides().unwrap();
        conf.resolve_master_addrs();

        assert_eq!(conf.master.hostname, "cv-master");
        assert_eq!(conf.client.master_addrs[0].hostname, "cv-master");
    }
}
