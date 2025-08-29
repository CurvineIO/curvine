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

use crate::conf::{ClusterConf, JobConf};
use orpc::common::{DurationUnit, LogConf, Utils};
use orpc::runtime::GroupExecutor;
use orpc::{err_box, CommonResult};
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

    // Master network read and write data timeout time, and whether to close idle connection; the default timeout is 10 minutes, close connections with timeout without data.
    pub io_timeout: String,
    pub io_close_idle: bool,

    // Metadata configuration, currently only supports rocksdb.
    // rocksdb configuration.
    pub meta_dir: String,
    pub meta_disable_wal: bool,
    pub meta_compression_type: String,
    pub meta_db_write_buffer_size: String,
    pub meta_write_buffer_size: String,

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

    pub log: LogConf,
    // Master loading function configuration
    pub load: JobConf,

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
}

impl MasterConf {
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

        if self.heartbeat_interval_unit > self.worker_blacklist_interval_unit {
            return err_box!("Worker_blacklist_interval must be greater than heartbeat_interval");
        };

        if self.heartbeat_interval_unit > self.worker_lost_interval_unit {
            return err_box!("Worker_lost_interval must be greater than heartbeat_interval");
        }

        Ok(())
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
        let mut conf = Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: ClusterConf::DEFAULT_MASTER_PORT,
            web_port: ClusterConf::DEFAULT_MASTER_WEB_PORT,
            io_threads: 32,
            worker_threads: Utils::worker_threads(32),
            io_timeout: "10m".to_string(),
            io_close_idle: true,

            meta_dir: dir,
            meta_disable_wal: true,
            meta_compression_type: "none".to_string(),
            meta_db_write_buffer_size: "0".to_string(),
            meta_write_buffer_size: "64MB".to_string(),

            min_block_size: 1024 * 1024,
            max_block_size: 100 * 1024 * 1024 * 1024,
            min_replication: 1,
            max_replication: 100,
            max_path_len: 8000,
            max_path_depth: 1000,

            retry_cache_enable: true,
            retry_cache_size: 100_000,
            retry_cache_ttl: "10m".to_string(),

            block_report_limit: 1000,

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

            log: Default::default(),
            load: Default::default(),

            ttl_checker_retry_attempts: 3,

            ttl_checker_interval: "1h".to_string(),
            ttl_checker_interval_unit: Default::default(),

            ttl_bucket_interval: "1h".to_string(),
            ttl_bucket_interval_unit: Default::default(),

            ttl_max_retry_duration: "10m".to_string(),
            ttl_max_retry_duration_unit: Default::default(),

            ttl_retry_interval: "1s".to_string(),
            ttl_retry_interval_unit: Default::default(),
        };

        conf.init().unwrap();
        conf
    }
}
