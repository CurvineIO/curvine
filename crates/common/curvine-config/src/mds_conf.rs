use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::LogConf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum KvBackendType {
    Memory,
    #[default]
    Fdb,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MdsConf {
    pub enabled: bool,
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,
    pub io_threads: usize,
    pub worker_threads: usize,
    pub kv_backend: KvBackendType,
    /// Path to the FoundationDB cluster file (the same file `fdbcli -C <path>`
    /// accepts). Required when `kv_backend = "fdb"`; the MDS opens FDB directly
    /// from this path.
    pub fdb_cluster_file: String,
    /// Per-transaction timeout (ms) applied to every FDB transaction.
    ///
    /// Leave it at the default unless you really know FoundationDB: it bounds
    /// how long an operation waits on a stuck/unreachable cluster before
    /// failing fast. Too small causes spurious timeouts under normal load; too
    /// large lets a wedged cluster stall requests. Do NOT tune it by hand
    /// without a clear reason.
    pub fdb_txn_timeout_ms: i32,
    pub log: LogConf,
}

impl MdsConf {
    pub const DEFAULT_RPC_PORT: u16 = 8998;
    pub const DEFAULT_WEB_PORT: u16 = 9004;
    pub const DEFAULT_FDB_TXN_TIMEOUT_MS: i32 = 5_000;

    pub fn init(&mut self) -> CommonResult<()> {
        self.hostname = self.hostname.trim().to_string();
        self.fdb_cluster_file = self.fdb_cluster_file.trim().to_string();
        if self.hostname.is_empty() {
            return err_box!("mds.hostname must not be empty");
        }
        if self.rpc_port == 0 {
            return err_box!("mds.rpc_port must be greater than zero");
        }
        if self.web_port == 0 {
            return err_box!("mds.web_port must be greater than zero");
        }
        if self.rpc_port == self.web_port {
            return err_box!("mds.rpc_port and mds.web_port must be different");
        }
        if self.io_threads == 0 {
            return err_box!("mds.io_threads must be greater than zero");
        }
        if self.worker_threads == 0 {
            return err_box!("mds.worker_threads must be greater than zero");
        }
        if self.fdb_txn_timeout_ms <= 0 {
            return err_box!("mds.fdb_txn_timeout_ms must be greater than zero");
        }
        if self.kv_backend == KvBackendType::Fdb && self.fdb_cluster_file.is_empty() {
            return err_box!("mds.kv_backend=fdb requires a non-empty mds.fdb_cluster_file");
        }
        Ok(())
    }
}

impl Default for MdsConf {
    fn default() -> Self {
        Self {
            enabled: false,
            hostname: "localhost".to_string(),
            rpc_port: Self::DEFAULT_RPC_PORT,
            web_port: Self::DEFAULT_WEB_PORT,
            io_threads: 4,
            worker_threads: 8,
            kv_backend: KvBackendType::default(),
            fdb_cluster_file: "/etc/foundationdb/fdb.cluster".to_string(),
            fdb_txn_timeout_ms: Self::DEFAULT_FDB_TXN_TIMEOUT_MS,
            log: LogConf {
                file_name: "mds.log".to_string(),
                ..Default::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_disabled_and_valid() {
        // Default kv_backend is fdb with the standard cluster-file path, so the
        // bare default is valid.
        let mut conf = MdsConf::default();
        conf.init().unwrap();

        assert!(!conf.enabled);
        assert_eq!(conf.rpc_port, MdsConf::DEFAULT_RPC_PORT);
        assert_eq!(conf.web_port, MdsConf::DEFAULT_WEB_PORT);
        assert_eq!(conf.kv_backend, KvBackendType::Fdb);
        assert_eq!(conf.fdb_cluster_file, "/etc/foundationdb/fdb.cluster");
        assert_eq!(conf.fdb_txn_timeout_ms, MdsConf::DEFAULT_FDB_TXN_TIMEOUT_MS);
        assert_eq!(conf.log.file_name, "mds.log");
    }

    #[test]
    fn rejects_invalid_ports() {
        let mut conf = MdsConf {
            rpc_port: 0,
            ..Default::default()
        };
        assert!(conf.init().is_err());

        let mut conf = MdsConf {
            web_port: MdsConf::DEFAULT_RPC_PORT,
            ..Default::default()
        };
        assert!(conf.init().is_err());
    }

    #[test]
    fn rejects_invalid_fdb_timeout() {
        let mut conf = MdsConf {
            fdb_txn_timeout_ms: 0,
            ..Default::default()
        };
        assert!(conf.init().is_err());
    }

    #[test]
    fn missing_fdb_timeout_uses_default() {
        let conf: MdsConf = toml::from_str(
            r#"
                enabled = true
                fdb_cluster_file = "/tmp/fdb.cluster"
            "#,
        )
        .unwrap();

        assert_eq!(conf.fdb_txn_timeout_ms, 5_000);
    }

    #[test]
    fn parses_kv_backend() {
        let memory: MdsConf = toml::from_str(r#"kv_backend = "memory""#).unwrap();
        assert_eq!(memory.kv_backend, KvBackendType::Memory);

        let fdb: MdsConf = toml::from_str(r#"kv_backend = "fdb""#).unwrap();
        assert_eq!(fdb.kv_backend, KvBackendType::Fdb);
    }

    #[test]
    fn fdb_accepts_cluster_file() {
        let mut conf: MdsConf = toml::from_str(
            r#"
                enabled = true
                kv_backend = "fdb"
                fdb_cluster_file = "/etc/foundationdb/fdb.cluster"
            "#,
        )
        .unwrap();
        conf.init().unwrap();
        assert_eq!(conf.fdb_cluster_file, "/etc/foundationdb/fdb.cluster");
    }

    #[test]
    fn fdb_requires_cluster_file() {
        let mut conf: MdsConf = toml::from_str(
            r#"
                kv_backend = "fdb"
                fdb_cluster_file = ""
            "#,
        )
        .unwrap();
        assert!(conf.init().is_err());
    }
}
