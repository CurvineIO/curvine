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

use crate::{ClusterConf, CompatibilityConf, DBConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::{ByteUnit, DurationUnit, LogConf, Utils};
use curvine_runtime::runtime::GroupExecutor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// master Configuration file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MasterConf {
    pub hostname: String,
    pub rpc_port: u16,
    pub web_port: u16,
    pub io_threads: usize,
    pub worker_threads: usize,
    pub actor_threads: usize,

    // Master network read and write data timeout time, and whether to close idle connection; the default timeout is 10 minutes, close connections with timeout without data.
    pub io_timeout: String,
    pub io_close_idle: bool,

    // Whether metadata requests can be processed concurrently within a connection.
    pub meta_request_concurrent: bool,

    // Metadata configuration, currently only supports rocksdb.
    // rocksdb configuration.
    pub meta_dir: String,

    pub min_block_size: i64,
    pub max_block_size: i64,

    pub min_replication: u16,
    pub max_replication: u16,

    pub max_path_len: usize,
    pub max_path_depth: usize,

    // fs request to retry the configuration
    pub retry_cache_enable: bool,
    pub retry_cache_size: u64,
    pub retry_cache_ttl: String,

    pub block_report_limit: usize,

    // Worker selects strategy
    pub worker_policy: String,

    pub executor_threads: usize,

    pub executor_channel_size: usize,

    pub heartbeat_interval: String,
    #[serde(skip)]
    pub heartbeat_interval_unit: DurationUnit,

    pub worker_check_interval: String,
    #[serde(skip)]
    pub worker_check_interval_unit: DurationUnit,

    pub worker_blacklist_interval: String,
    #[serde(skip)]
    pub worker_blacklist_interval_unit: DurationUnit,

    pub worker_lost_interval: String,
    #[serde(skip)]
    pub worker_lost_interval_unit: DurationUnit,

    // Audit log configuration.
    pub audit_logging_enabled: bool,
    pub audit_log: LogConf,

    // Block replication
    pub block_replication_enabled: bool,
    pub block_replication_concurrency_limit: usize,
    pub block_replication_retry_interval: String,
    #[serde(skip)]
    pub block_replication_retry_interval_unit: DurationUnit,
    /// Deadline for connecting to a source worker and receiving its submit acknowledgement.
    pub block_replication_submit_timeout: String,
    #[serde(skip)]
    pub block_replication_submit_timeout_unit: DurationUnit,
    /// Maximum time to wait for a submitted replication attempt to report a result.
    pub block_replication_job_timeout: String,
    #[serde(skip)]
    pub block_replication_job_timeout_unit: DurationUnit,

    pub log: LogConf,

    pub ttl_checker_retry_attempts: u32,

    pub ttl_checker_interval: String,
    #[serde(skip)]
    pub ttl_checker_interval_unit: DurationUnit,

    pub ttl_bucket_interval: String,
    #[serde(skip)]
    pub ttl_bucket_interval_unit: DurationUnit,

    pub ttl_max_retry_duration: String,
    #[serde(skip)]
    pub ttl_max_retry_duration_unit: DurationUnit,

    pub ttl_retry_interval: String,
    #[serde(skip)]
    pub ttl_retry_interval_unit: DurationUnit,

    // Recycle bin configuration.
    // Soft-deleted entries are moved to /.Trash and purged after trash_retention.
    // A retention of "0" keeps entries until they are purged manually.
    pub trash_enabled: bool,
    pub trash_retention: String,
    #[serde(skip)]
    pub trash_retention_unit: DurationUnit,

    // How often the master scans /.Trash for expired entries.
    pub trash_check_interval: String,
    #[serde(skip)]
    pub trash_check_interval_unit: DurationUnit,

    // Maximum number of inodes a single trash purge batch removes before the
    // namespace lock is released. Every path in a batch removes exactly one
    // inode, so this also bounds the batch's journal record.
    pub trash_purge_batch_inodes: usize,

    // Milliseconds the purger yields between batches so foreground metadata
    // operations can take the namespace lock.
    pub trash_purge_yield_ms: u64,

    // How often the master drains /.Trash/.pending, i.e. the entries staged by
    // async deletes. Runs regardless of trash_enabled.
    pub trash_purge_interval: String,
    #[serde(skip)]
    pub trash_purge_interval_unit: DurationUnit,

    // Eviction configuration
    pub enable_quota_eviction: bool,
    pub quota_eviction_mode: String,
    pub quota_eviction_policy: String,
    pub quota_eviction_high_rate: f64,
    pub quota_eviction_low_rate: f64,
    pub quota_eviction_scan_page: i32,
    /// Maximum number of eviction candidates examined per trigger. 0 means
    /// unlimited (a full scan of the candidate set). Bounds the work done when
    /// a large part of the cache is not reclaimable (live files, trash).
    pub quota_eviction_max_scan: i32,
    pub quota_eviction_dry_run: bool,
    pub quota_eviction_capacity: usize,

    // File lock configuration
    // Lock expiration time (applies to both POSIX and BSD locks)
    // If a lock is held longer than this duration, it will be considered stale and can be removed
    // This prevents locks from being held indefinitely if a process crashes
    // Default: 5 minutes - longer than typical distributed lock timeout
    // to accommodate file operations that may take time
    pub lock_expire_time: String,
    #[serde(skip)]
    pub lock_expire_time_unit: DurationUnit,

    // Client session lease: a client session that has not sent a ClientHeartbeat
    // within this window is considered expired. Should be several times the
    // client heartbeat interval (default 12s).
    pub client_session_timeout: String,
    #[serde(skip)]
    pub client_session_timeout_unit: DurationUnit,

    pub buffer_size: usize,

    pub conn_limit: usize,
    pub global_limit: usize,

    /// Version compatibility policy (diagnose by default; enforce only when
    /// explicitly configured).
    pub compatibility: CompatibilityConf,

    #[serde(default = "MasterConf::rocksdb_default")]
    pub rocksdb: DBConf,
}

impl MasterConf {
    pub const DEFAULT_BLOCK_REPORT_LIMIT: usize = 100_000;

    pub fn init(&mut self) -> CommonResult<()> {
        self.heartbeat_interval_unit = DurationUnit::from_str(&self.heartbeat_interval)?;

        self.worker_check_interval_unit = DurationUnit::from_str(&self.worker_check_interval)?;

        self.worker_blacklist_interval_unit =
            DurationUnit::from_str(&self.worker_blacklist_interval)?;

        self.worker_lost_interval_unit = DurationUnit::from_str(&self.worker_lost_interval)?;

        // Initialize TTL duration units
        self.ttl_checker_interval_unit = DurationUnit::from_str(&self.ttl_checker_interval)?;
        self.ttl_bucket_interval_unit = DurationUnit::from_str(&self.ttl_bucket_interval)?;
        self.ttl_max_retry_duration_unit = DurationUnit::from_str(&self.ttl_max_retry_duration)?;
        self.ttl_retry_interval_unit = DurationUnit::from_str(&self.ttl_retry_interval)?;
        self.trash_retention_unit = DurationUnit::from_str(&self.trash_retention)?;
        self.trash_check_interval_unit = DurationUnit::from_str(&self.trash_check_interval)?;
        self.trash_purge_interval_unit = DurationUnit::from_str(&self.trash_purge_interval)?;
        self.block_replication_retry_interval_unit =
            DurationUnit::from_str(&self.block_replication_retry_interval)?;
        self.block_replication_submit_timeout_unit =
            DurationUnit::from_str(&self.block_replication_submit_timeout)?;
        self.block_replication_job_timeout_unit =
            DurationUnit::from_str(&self.block_replication_job_timeout)?;

        // Initialize lock expiration time
        self.lock_expire_time_unit = DurationUnit::from_str(&self.lock_expire_time)?;

        self.client_session_timeout_unit = DurationUnit::from_str(&self.client_session_timeout)?;
        if self.client_session_timeout_unit.as_millis() == 0 {
            return err_box!("client_session_timeout must be greater than zero");
        }

        if self.heartbeat_interval_unit > self.worker_blacklist_interval_unit {
            return err_box!("Worker_blacklist_interval must be greater than heartbeat_interval");
        };

        if self.heartbeat_interval_unit > self.worker_lost_interval_unit {
            return err_box!("Worker_lost_interval must be greater than heartbeat_interval");
        }

        if self.worker_check_interval_unit >= self.worker_lost_interval_unit {
            return err_box!("Worker_lost_interval must be greater than worker_check_interval");
        }

        if self.conn_limit == 0 {
            return err_box!("master.conn_limit must be greater than zero");
        }

        if self.global_limit == 0 {
            return err_box!("master.global_limit must be greater than zero");
        }

        if self.block_replication_submit_timeout_ms() == 0 {
            return err_box!("master.block_replication_submit_timeout must be greater than zero");
        }

        if self.block_replication_job_timeout_ms() == 0 {
            return err_box!("master.block_replication_job_timeout must be greater than zero");
        }

        if self.trash_check_interval_ms() == 0 {
            return err_box!("master.trash_check_interval must be greater than zero");
        }

        if self.trash_purge_interval_ms() == 0 {
            return err_box!("master.trash_purge_interval must be greater than zero");
        }

        Ok(())
    }

    pub fn rocksdb_default() -> DBConf {
        DBConf {
            block_size: ByteUnit::kb(4),
            disable_wal: true,
            use_bloom_filter: true,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks_in_cache: true,
            ..Default::default()
        }
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval_unit.as_millis()
    }

    pub fn worker_check_interval_ms(&self) -> u64 {
        self.worker_check_interval_unit.as_millis()
    }

    pub fn worker_blacklist_interval_ms(&self) -> u64 {
        self.worker_blacklist_interval_unit.as_millis()
    }

    pub fn worker_lost_interval_ms(&self) -> u64 {
        self.worker_lost_interval_unit.as_millis()
    }

    pub fn ttl_checker_interval_ms(&self) -> u64 {
        self.ttl_checker_interval_unit.as_millis()
    }

    pub fn ttl_bucket_interval_ms(&self) -> u64 {
        self.ttl_bucket_interval_unit.as_millis()
    }

    pub fn ttl_max_retry_duration_ms(&self) -> u64 {
        self.ttl_max_retry_duration_unit.as_millis()
    }

    pub fn ttl_retry_interval_ms(&self) -> u64 {
        self.ttl_retry_interval_unit.as_millis()
    }

    pub fn block_replication_retry_interval_ms(&self) -> u64 {
        self.block_replication_retry_interval_unit.as_millis()
    }

    pub fn block_replication_submit_timeout_ms(&self) -> u64 {
        self.block_replication_submit_timeout_unit.as_millis()
    }

    pub fn block_replication_job_timeout_ms(&self) -> u64 {
        self.block_replication_job_timeout_unit.as_millis()
    }

    pub fn lock_expire_time_ms(&self) -> u64 {
        self.lock_expire_time_unit.as_millis()
    }

    pub fn client_session_timeout_ms(&self) -> u64 {
        self.client_session_timeout_unit.as_millis()
    }

    pub fn trash_retention_ms(&self) -> u64 {
        self.trash_retention_unit.as_millis()
    }

    pub fn trash_purge_interval_ms(&self) -> u64 {
        self.trash_purge_interval_unit.as_millis()
    }

    pub fn trash_check_interval_ms(&self) -> u64 {
        self.trash_check_interval_unit.as_millis()
    }

    pub fn io_timeout_ms(&self) -> u64 {
        let dur = DurationUnit::from_str(&self.io_timeout).unwrap();
        dur.as_millis()
    }

    pub fn new_executor(&self) -> Arc<GroupExecutor> {
        let executor = GroupExecutor::new(
            "master-executor",
            self.executor_threads,
            self.executor_channel_size,
        );
        Arc::new(executor)
    }
}

impl Default for MasterConf {
    fn default() -> Self {
        let dir = Utils::cur_dir_sub("fs-meta");

        let rocksdb = Self::rocksdb_default().set_dir(&dir);

        let mut conf = Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: ClusterConf::DEFAULT_MASTER_PORT,
            web_port: ClusterConf::DEFAULT_MASTER_WEB_PORT,
            io_threads: 32,
            worker_threads: Utils::worker_threads(32),
            actor_threads: 4,
            io_timeout: "10m".to_string(),
            io_close_idle: true,
            meta_request_concurrent: true,

            meta_dir: dir,

            min_block_size: 1024 * 1024,
            max_block_size: 100 * 1024 * 1024 * 1024,
            min_replication: 1,
            max_replication: 100,
            max_path_len: 8000,
            max_path_depth: 1000,

            retry_cache_enable: true,
            retry_cache_size: 100_000,
            retry_cache_ttl: "10m".to_string(),

            block_report_limit: Self::DEFAULT_BLOCK_REPORT_LIMIT,

            worker_policy: "local".to_string(),

            executor_threads: 10,
            executor_channel_size: 1000,

            heartbeat_interval: "3s".to_string(),
            heartbeat_interval_unit: Default::default(),

            worker_check_interval: "10s".to_string(),
            worker_check_interval_unit: Default::default(),

            worker_blacklist_interval: "30s".to_string(),
            worker_blacklist_interval_unit: Default::default(),

            worker_lost_interval: "10m".to_string(),
            worker_lost_interval_unit: Default::default(),

            audit_logging_enabled: true,
            audit_log: Default::default(),

            block_replication_enabled: false,
            block_replication_concurrency_limit: 1000,
            block_replication_retry_interval: "5s".to_string(),
            block_replication_retry_interval_unit: Default::default(),
            block_replication_submit_timeout: "30s".to_string(),
            block_replication_submit_timeout_unit: Default::default(),
            block_replication_job_timeout: "30m".to_string(),
            block_replication_job_timeout_unit: Default::default(),
            log: LogConf {
                file_name: "master.log".to_string(),
                ..Default::default()
            },

            ttl_checker_retry_attempts: 3,

            ttl_checker_interval: "1h".to_string(),
            ttl_checker_interval_unit: Default::default(),

            ttl_bucket_interval: "1h".to_string(),
            ttl_bucket_interval_unit: Default::default(),

            ttl_max_retry_duration: "10m".to_string(),
            ttl_max_retry_duration_unit: Default::default(),

            ttl_retry_interval: "1s".to_string(),
            ttl_retry_interval_unit: Default::default(),

            trash_enabled: false,
            trash_retention: "7d".to_string(),
            trash_retention_unit: Default::default(),

            trash_check_interval: "1h".to_string(),
            trash_check_interval_unit: Default::default(),

            trash_purge_batch_inodes: 4096,
            trash_purge_yield_ms: 5,

            trash_purge_interval: "1s".to_string(),
            trash_purge_interval_unit: Default::default(),

            // Eviction configuration defaults
            enable_quota_eviction: false,
            quota_eviction_mode: "free".to_string(),
            quota_eviction_policy: "lru".to_string(),
            quota_eviction_high_rate: 0.8,
            quota_eviction_low_rate: 0.6,
            quota_eviction_scan_page: 2,
            quota_eviction_max_scan: 10_000,
            quota_eviction_dry_run: false,
            quota_eviction_capacity: 5_000_000, // Default: 5 million entries (~250MB)

            lock_expire_time: "5m".to_string(),
            lock_expire_time_unit: Default::default(),

            client_session_timeout: "60s".to_string(),
            client_session_timeout_unit: Default::default(),

            buffer_size: 128 * 1024,

            conn_limit: 8,
            global_limit: 4096,

            compatibility: Default::default(),

            rocksdb,
        };

        conf.init().unwrap();
        conf
    }
}

#[cfg(test)]
mod tests {
    use super::MasterConf;

    #[test]
    fn replication_submit_timeout_must_be_positive() {
        let mut conf = MasterConf {
            block_replication_submit_timeout: "0ms".to_string(),
            ..Default::default()
        };

        let error = conf
            .init()
            .expect_err("zero submit timeout must be rejected");
        assert!(error
            .to_string()
            .contains("block_replication_submit_timeout must be greater than zero"));
    }

    #[test]
    fn replication_job_timeout_must_be_positive() {
        let mut conf = MasterConf {
            block_replication_job_timeout: "0ms".to_string(),
            ..Default::default()
        };

        let error = conf.init().expect_err("zero job timeout must be rejected");
        assert!(error
            .to_string()
            .contains("block_replication_job_timeout must be greater than zero"));
    }

    #[test]
    fn worker_check_interval_must_be_shorter_than_lost_interval() {
        let mut conf = MasterConf {
            worker_check_interval: "15m".to_string(),
            ..Default::default()
        };

        let error = conf
            .init()
            .expect_err("a check cadence slower than the lost interval must be rejected");
        assert!(error
            .to_string()
            .contains("Worker_lost_interval must be greater than worker_check_interval"));
    }

    #[test]
    fn trash_defaults_are_disabled_with_one_week_retention() {
        let conf = MasterConf::default();

        assert!(!conf.trash_enabled);
        assert_eq!(conf.trash_retention_ms(), 7 * 24 * 60 * 60 * 1000);
        assert_eq!(conf.trash_check_interval_ms(), 60 * 60 * 1000);
    }

    #[test]
    fn zero_trash_retention_disables_auto_purge() {
        let mut conf = MasterConf {
            trash_retention: "0".to_string(),
            ..Default::default()
        };
        conf.init().unwrap();

        assert_eq!(conf.trash_retention_ms(), 0);
    }

    #[test]
    fn trash_check_interval_must_be_positive() {
        let mut conf = MasterConf {
            trash_check_interval: "0ms".to_string(),
            ..Default::default()
        };

        let error = conf
            .init()
            .expect_err("zero trash check interval must be rejected");
        assert!(error
            .to_string()
            .contains("trash_check_interval must be greater than zero"));
    }

    #[test]
    fn client_session_timeout_defaults_to_sixty_seconds() {
        let mut conf = MasterConf::default();
        conf.init().unwrap();

        assert_eq!(conf.client_session_timeout_ms(), 60_000);
    }

    #[test]
    fn client_session_timeout_must_be_positive() {
        let mut conf = MasterConf {
            client_session_timeout: "0s".to_string(),
            ..Default::default()
        };

        let error = conf
            .init()
            .expect_err("zero client session timeout must be rejected");
        assert!(error
            .to_string()
            .contains("client_session_timeout must be greater than zero"));
    }
}
