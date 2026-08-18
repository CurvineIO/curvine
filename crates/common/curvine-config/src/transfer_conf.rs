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

use crate::{FsError, FsResult};
use curvine_rpc::server::ServerConf;
use curvine_runtime::common::{DurationUnit, LogConf};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferStoreType {
    Auto,
    Memory,
    Sqlite,
    Mysql,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TransferConf {
    pub enabled: bool,
    // Deprecated compatibility inputs normalize into store_url during init.
    #[serde(skip_serializing)]
    pub store_type: TransferStoreType,
    #[serde(skip_serializing)]
    pub store_url: String,
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,
    pub log: LogConf,
    #[serde(skip, default = "TransferConf::default_io_threads")]
    pub io_threads: usize,
    #[serde(skip, default = "TransferConf::default_worker_threads")]
    pub worker_threads: usize,
    pub instance_id: String,
    pub endpoints: Vec<String>,
    #[serde(skip_serializing)]
    pub sqlite_path: String,
    #[serde(skip_serializing)]
    pub mysql_url: String,
    #[serde(skip)]
    pub allow_submit_with_stale_snapshot: bool,
    pub max_running_transfers: usize,
    #[serde(skip, default = "TransferConf::default_max_tasks_per_transfer")]
    pub max_tasks_per_transfer: usize,
    #[serde(
        skip,
        default = "TransferConf::default_ufs_max_concurrency_per_endpoint"
    )]
    pub ufs_max_concurrency_per_endpoint: usize,
    #[serde(skip, default = "TransferConf::default_task_max_retries")]
    pub task_max_retries: usize,

    #[serde(skip)]
    pub task_report_interval: Duration,
    #[serde(skip, default = "TransferConf::default_task_report_interval_str")]
    pub task_report_interval_str: String,

    #[serde(skip)]
    pub task_stale_timeout: Duration,
    #[serde(skip, default = "TransferConf::default_task_stale_timeout_str")]
    pub task_stale_timeout_str: String,

    #[serde(skip)]
    pub lease_timeout: Duration,
    #[serde(alias = "lease_timeout")]
    pub lease_timeout_str: String,

    #[serde(skip)]
    pub cluster_snapshot_max_staleness: Duration,
    #[serde(
        skip,
        default = "TransferConf::default_cluster_snapshot_max_staleness_str"
    )]
    pub cluster_snapshot_max_staleness_str: String,

    #[serde(skip)]
    pub terminal_retention: Duration,
    #[serde(alias = "terminal_retention")]
    pub terminal_retention_str: String,
}

impl TransferConf {
    pub const DEFAULT_TASK_REPORT_INTERVAL: &'static str = "10s";
    pub const DEFAULT_TASK_STALE_TIMEOUT: &'static str = "60s";
    pub const DEFAULT_LEASE_TIMEOUT: &'static str = "120s";
    pub const DEFAULT_CLUSTER_SNAPSHOT_MAX_STALENESS: &'static str = "60s";
    pub const DEFAULT_TERMINAL_RETENTION: &'static str = "168h";
    pub const DEFAULT_MAX_PLANNING_TRANSFERS: usize = 8;
    pub const DEFAULT_PLANNING_BATCH_SIZE: usize = 1000;
    pub const DEFAULT_WORKER_DISPATCH_CONCURRENCY: usize = 256;
    pub const DEFAULT_TASK_REPORT_QUEUE_SIZE: usize = 10_000;
    pub const DEFAULT_TASK_PROBE_CONCURRENCY: usize = 64;
    pub const DEFAULT_CLIENT_PENDING_QUEUE_SIZE: usize = 1024;
    pub const DEFAULT_CLEANUP_BATCH_SIZE: usize = 1000;
    pub const DEFAULT_RPC_PORT: u16 = 9010;
    pub const DEFAULT_WEB_PORT: u16 = 9011;
    pub const DEFAULT_IO_THREADS: usize = 4;
    pub const DEFAULT_WORKER_THREADS: usize = 8;
    pub const DEFAULT_MAX_TASKS_PER_TRANSFER: usize = 100_000;
    pub const DEFAULT_UFS_MAX_CONCURRENCY_PER_ENDPOINT: usize = 32;
    pub const DEFAULT_TASK_MAX_RETRIES: usize = 3;

    pub fn init(&mut self) -> FsResult<()> {
        self.infer_store_url();
        if self.endpoints.is_empty() {
            self.endpoints
                .push(format!("{}:{}", self.hostname, self.rpc_port));
        }
        self.lease_timeout = DurationUnit::from_str(&self.lease_timeout_str)?.as_duration();
        self.task_stale_timeout = Self::derive_task_stale_timeout(self.lease_timeout);
        self.task_report_interval = Self::derive_task_report_interval(self.task_stale_timeout);
        self.cluster_snapshot_max_staleness =
            DurationUnit::from_str(&self.cluster_snapshot_max_staleness_str)?.as_duration();
        self.terminal_retention =
            DurationUnit::from_str(&self.terminal_retention_str)?.as_duration();
        if !self.store_url.is_empty() && self.effective_store_type() == TransferStoreType::Auto {
            return Err(FsError::common(
                "transfer.store_url must use memory://, sqlite://, or mysql://",
            ));
        }
        match self.effective_store_type() {
            TransferStoreType::Sqlite if self.sqlite_store_path().is_empty() => {
                return Err(FsError::common(
                    "transfer.store_url=sqlite:// requires a database path",
                ));
            }
            TransferStoreType::Mysql if self.mysql_store_url().is_empty() => {
                return Err(FsError::common(
                    "transfer.store_url=mysql:// requires a database URL",
                ));
            }
            _ => {}
        }
        if self.enabled && self.effective_store_type() == TransferStoreType::Mysql {
            self.validate_mysql_store()?;
        }
        if self.max_running_transfers == 0 {
            return Err(FsError::common(
                "transfer.max_running_transfers must be greater than 0",
            ));
        }
        if self.ufs_max_concurrency_per_endpoint == 0 {
            return Err(FsError::common(
                "transfer.ufs_max_concurrency_per_endpoint must be greater than 0",
            ));
        }
        if self.cluster_snapshot_max_staleness.is_zero() {
            return Err(FsError::common(
                "transfer.cluster_snapshot_max_staleness must be greater than 0",
            ));
        }
        if self.lease_timeout.is_zero() {
            return Err(FsError::common(
                "transfer.lease_timeout must be greater than 0",
            ));
        }
        if self.max_tasks_per_transfer == 0 {
            return Err(FsError::common(
                "transfer.max_tasks_per_transfer must be greater than 0",
            ));
        }
        Ok(())
    }

    fn infer_store_url(&mut self) {
        if !self.store_url.is_empty() {
            return;
        }

        self.store_url = match self.effective_store_type() {
            TransferStoreType::Memory => "memory://".to_string(),
            TransferStoreType::Sqlite => format!("sqlite://{}", self.sqlite_path),
            TransferStoreType::Mysql => self.mysql_url.clone(),
            TransferStoreType::Auto => String::new(),
        };
    }

    pub fn effective_store_type(&self) -> TransferStoreType {
        if !self.store_url.is_empty() {
            return if self.store_url == "memory://" {
                TransferStoreType::Memory
            } else if self.store_url.starts_with("sqlite://") {
                TransferStoreType::Sqlite
            } else if self.store_url.starts_with("mysql://") {
                TransferStoreType::Mysql
            } else {
                TransferStoreType::Auto
            };
        }
        match self.store_type {
            TransferStoreType::Auto if self.mysql_url.is_empty() => TransferStoreType::Sqlite,
            TransferStoreType::Auto => TransferStoreType::Mysql,
            store_type => store_type,
        }
    }

    pub fn sqlite_store_path(&self) -> &str {
        self.store_url
            .strip_prefix("sqlite://")
            .unwrap_or(&self.sqlite_path)
    }

    pub fn mysql_store_url(&self) -> &str {
        if self.store_url.starts_with("mysql://") {
            &self.store_url
        } else {
            &self.mysql_url
        }
    }

    fn validate_mysql_store(&self) -> FsResult<()> {
        if self.allow_submit_with_stale_snapshot {
            return Err(FsError::common(
                "production transfer forbids transfer.allow_submit_with_stale_snapshot=true",
            ));
        }
        Ok(())
    }

    pub fn max_executing_transfers(&self) -> u64 {
        self.max_running_transfers as u64
    }

    fn default_io_threads() -> usize {
        Self::DEFAULT_IO_THREADS
    }

    fn default_worker_threads() -> usize {
        Self::DEFAULT_WORKER_THREADS
    }

    fn default_max_tasks_per_transfer() -> usize {
        Self::DEFAULT_MAX_TASKS_PER_TRANSFER
    }

    fn default_ufs_max_concurrency_per_endpoint() -> usize {
        Self::DEFAULT_UFS_MAX_CONCURRENCY_PER_ENDPOINT
    }

    fn default_task_max_retries() -> usize {
        Self::DEFAULT_TASK_MAX_RETRIES
    }

    fn default_task_report_interval_str() -> String {
        Self::DEFAULT_TASK_REPORT_INTERVAL.to_string()
    }

    fn default_task_stale_timeout_str() -> String {
        Self::DEFAULT_TASK_STALE_TIMEOUT.to_string()
    }

    fn default_cluster_snapshot_max_staleness_str() -> String {
        Self::DEFAULT_CLUSTER_SNAPSHOT_MAX_STALENESS.to_string()
    }

    pub fn scheduler_workers(&self) -> usize {
        Self::DEFAULT_MAX_PLANNING_TRANSFERS
            .min(self.max_running_transfers.max(1))
            .max(1)
    }

    pub fn planning_batch_size(&self) -> usize {
        Self::DEFAULT_PLANNING_BATCH_SIZE
    }

    pub fn worker_dispatch_concurrency(&self) -> usize {
        Self::DEFAULT_WORKER_DISPATCH_CONCURRENCY
    }

    pub fn task_report_queue_size(&self) -> usize {
        Self::DEFAULT_TASK_REPORT_QUEUE_SIZE
    }

    pub fn task_probe_concurrency(&self) -> usize {
        Self::DEFAULT_TASK_PROBE_CONCURRENCY
    }

    pub fn client_pending_queue_size(&self) -> usize {
        Self::DEFAULT_CLIENT_PENDING_QUEUE_SIZE
    }

    pub fn cleanup_batch_size(&self) -> usize {
        Self::DEFAULT_CLEANUP_BATCH_SIZE
    }

    fn derive_task_stale_timeout(lease_timeout: Duration) -> Duration {
        lease_timeout.div_f64(2.0)
    }

    fn derive_task_report_interval(task_stale_timeout: Duration) -> Duration {
        let derived = task_stale_timeout.div_f64(6.0);
        let default = DurationUnit::from_str(Self::DEFAULT_TASK_REPORT_INTERVAL)
            .expect("default transfer task report interval must be valid")
            .as_duration();
        derived.min(default).max(Duration::from_millis(1))
    }

    pub fn server_conf(&self, cluster_id: &str) -> ServerConf {
        let mut conf = ServerConf::with_hostname(&self.hostname, self.rpc_port);
        conf.name = format!("{}-transfer", cluster_id);
        conf.io_threads = self.io_threads;
        conf.worker_threads = self.worker_threads;
        conf
    }
}

impl Default for TransferConf {
    fn default() -> Self {
        Self {
            enabled: false,
            store_type: TransferStoreType::Auto,
            store_url: String::new(),
            hostname: "localhost".to_string(),
            rpc_port: Self::DEFAULT_RPC_PORT,
            web_port: Self::DEFAULT_WEB_PORT,
            log: LogConf {
                file_name: "transfer.log".to_string(),
                ..Default::default()
            },
            io_threads: Self::DEFAULT_IO_THREADS,
            worker_threads: Self::DEFAULT_WORKER_THREADS,
            instance_id: String::new(),
            endpoints: vec![],
            sqlite_path: "data/transfer/transfer.db".to_string(),
            mysql_url: String::new(),
            allow_submit_with_stale_snapshot: false,
            max_running_transfers: 64,
            max_tasks_per_transfer: Self::DEFAULT_MAX_TASKS_PER_TRANSFER,
            ufs_max_concurrency_per_endpoint: Self::DEFAULT_UFS_MAX_CONCURRENCY_PER_ENDPOINT,
            task_max_retries: Self::DEFAULT_TASK_MAX_RETRIES,
            task_report_interval: Duration::default(),
            task_report_interval_str: Self::DEFAULT_TASK_REPORT_INTERVAL.to_string(),
            task_stale_timeout: Duration::default(),
            task_stale_timeout_str: Self::DEFAULT_TASK_STALE_TIMEOUT.to_string(),
            lease_timeout: Duration::default(),
            lease_timeout_str: Self::DEFAULT_LEASE_TIMEOUT.to_string(),
            cluster_snapshot_max_staleness: Duration::default(),
            cluster_snapshot_max_staleness_str: Self::DEFAULT_CLUSTER_SNAPSHOT_MAX_STALENESS
                .to_string(),
            terminal_retention: Duration::default(),
            terminal_retention_str: Self::DEFAULT_TERMINAL_RETENTION.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TransferConf, TransferStoreType};

    #[test]
    fn defaults_to_local_sqlite_with_a_local_endpoint() {
        let mut conf = TransferConf {
            enabled: true,
            ..Default::default()
        };

        conf.init().unwrap();

        assert_eq!(conf.effective_store_type(), TransferStoreType::Sqlite);
        assert_eq!(conf.store_url, "sqlite://data/transfer/transfer.db");
        assert_eq!(conf.endpoints, vec!["localhost:9010"]);
        assert_eq!(conf.rpc_port, TransferConf::DEFAULT_RPC_PORT);
        assert_eq!(conf.web_port, TransferConf::DEFAULT_WEB_PORT);
        assert_eq!(conf.log.file_name, "transfer.log");
    }

    #[test]
    fn parses_dedicated_log_configuration() {
        let conf: TransferConf = toml::from_str(
            r#"
                log = { level = "debug", log_dir = "stdout", file_name = "transfer-test.log" }
            "#,
        )
        .unwrap();

        assert_eq!(conf.log.file_name, "transfer-test.log");
    }

    #[test]
    fn mysql_url_selects_production_safe_defaults() {
        let mut conf: TransferConf = toml::from_str(
            r#"
                enabled = true
                store_url = "mysql://root:curvine@127.0.0.1:3306/curvine_transfer"
            "#,
        )
        .unwrap();

        conf.init().unwrap();

        assert_eq!(conf.effective_store_type(), TransferStoreType::Mysql);
        assert_eq!(
            conf.store_url,
            "mysql://root:curvine@127.0.0.1:3306/curvine_transfer"
        );
    }

    #[test]
    fn accepts_memory_store_url_and_rejects_unknown_scheme() {
        let mut memory = TransferConf {
            enabled: true,
            store_url: "memory://".to_string(),
            ..Default::default()
        };
        memory.init().unwrap();
        assert_eq!(memory.effective_store_type(), TransferStoreType::Memory);

        let mut invalid = TransferConf {
            enabled: true,
            store_url: "postgres://transfer".to_string(),
            ..Default::default()
        };
        let err = invalid.init().unwrap_err().to_string();
        assert!(err.contains("transfer.store_url must use"));
    }

    #[test]
    fn store_url_is_not_serialized_into_printed_configuration() {
        let conf = TransferConf {
            store_url: "mysql://transfer:secret@db.example:3306/curvine_transfer".to_string(),
            ..Default::default()
        };

        let text = toml::to_string(&conf).unwrap();

        assert!(!text.contains("secret"));
        assert!(!text.contains("store_url"));
    }
}
