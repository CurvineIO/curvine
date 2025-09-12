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

use crate::conf::ClusterConf;
use crate::state::{StorageType, TtlAction};
use orpc::client::ClientConf as RpcConf;
use orpc::common::{ByteUnit, DurationUnit, Utils};
use orpc::io::net::InetAddr;
use orpc::CommonResult;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientConf {
    // List of master addresses
    pub master_addrs: Vec<InetAddr>,

    // The hostname of the machine where the customer service is located.
    // In some cases, this value needs to be set to identify that the client and worker are on the same machine.
    pub hostname: String,

    pub io_threads: usize,
    pub worker_threads: usize,

    pub replicas: i32,
    #[serde(skip)]
    pub block_size: i64,
    #[serde(alias = "block_size")]
    pub block_size_str: String,

    #[serde(skip)]
    pub write_chunk_size: usize,
    #[serde(alias = "write_chunk_size")]
    pub write_chunk_size_str: String,
    pub write_chunk_num: usize,

    #[serde(skip)]
    pub read_chunk_size: usize,
    #[serde(alias = "read_chunk_size")]
    pub read_chunk_size_str: String,
    pub read_chunk_num: usize,

    // These 2 parameters are used to improve the speed of reading a single file.
    // Read the parallelism of a file, default is 1
    pub read_parallel: i64,
    // The file is divided into blocks of different sizes according to this size, and read non-duplicate blocks at parallel task intervals.
    // Assume read_parallel = 2, file blocks 0,1,2,3
    // Parallel task 1 reads 0 and 2; Parallel task 2 reads 1 and 3
    // Default is 0, the value is read_chunk_size * read_chunk_num
    #[serde(skip)]
    pub read_slice_size: i64,
    #[serde(alias = "read_slice_size")]
    pub read_slice_size_str: String,

    // In random read scenarios, the seek operation may frequently switch blocks, and the connection will be closed by default.
    // This will have huge overhead.
    // If seek is detected that causes block switching to exceed this value, all block readers will be cached.
    pub close_reader_limit: u32,

    pub short_circuit: bool,

    #[serde(skip)]
    pub storage_type: StorageType,
    #[serde(alias = "storage_type")]
    pub storage_type_str: String,

    #[serde(skip)]
    pub ttl_ms: i64,
    #[serde(alias = "ttl_ms")]
    pub ttl_ms_str: String,

    #[serde(skip)]
    pub ttl_action: TtlAction,
    #[serde(alias = "ttl_action")]
    pub ttl_action_str: String,

    /// Whether to enable automatic caching function
    /// When enabled, when the client reads files from external file systems (such as S3, OSS, etc.),
    /// will automatically submit a load request to the master and cache the file into curvine
    pub auto_cache_enabled: bool,

    /// Default TTL for automatic cache (living time)
    /// Format: Number + units, such as "10d" (10 days), "24h" (24 hours)
    /// Or pure number (seconds), such as "86400" (1 day)
    pub auto_cache_ttl: String,

    /// Default cache TTL
    /// Same as auto_cache_ttl, but as Option<String> type, it is convenient to use in the API
    pub default_cache_ttl: Option<String>,

    // Set up the customer service retry policy
    pub conn_retry_max_duration_ms: u64,
    pub conn_retry_min_sleep_ms: u64,
    pub conn_retry_max_sleep_ms: u64,

    // rpc requests retry policy.
    pub rpc_retry_max_duration_ms: u64,
    pub rpc_retry_min_sleep_ms: u64,
    pub rpc_retry_max_sleep_ms: u64,

    // Whether to close the idle rpc connection.
    pub rpc_close_idle: bool,

    //Configuration of timeout for a request.
    pub conn_timeout_ms: u64,
    pub rpc_timeout_ms: u64,
    pub data_timeout_ms: u64,

    // Number of fs master connections.
    // After testing 3 connections, the best performance can be achieved, so the default value is 3.
    pub master_conn_pool_size: usize,

    // Whether to enable pre-reading
    pub enable_read_ahead: bool,
    // Default is 0, the value is read_chunk_size * read_chunk_num
    #[serde(skip)]
    pub read_ahead_len: i64,
    #[serde(alias = "read_ahead_len")]
    pub read_ahead_len_str: String,
    #[serde(skip)]
    pub drop_cache_len: i64,
    #[serde(alias = "drop_cache_len")]
    pub drop_cache_len_str: String,

    // Worker blacklist survival time
    #[serde(skip)]
    pub failed_worker_ttl: Duration,
    #[serde(alias = "failed_worker_ttl")]
    pub failed_worker_ttl_str: String,

    // Whether to enable the unified file system
    pub enable_unified_fs: bool,
    // If the cache hits, read data from Curvine.
    // If the cache misses, determine whether to allow Curvine to directly read data from the unified file system (UFS).
    pub enable_rust_read_ufs: bool,

    // Mount information update interval
    #[serde(skip)]
    pub mount_update_ttl: Duration,
    #[serde(alias = "mount_update_ttl")]
    pub mount_update_ttl_str: String,

    pub umask: u32,

    pub metric_report_enable: bool,

    #[serde(skip)]
    pub metric_report_interval: Duration,
    #[serde(alias = "metric_report_interval")]
    pub metric_report_interval_str: String,

    pub close_timeout_secs: u64,
}

impl ClientConf {
    pub const LONG_READ_THRESHOLD_LEN: i64 = 256 * 1024;

    pub const DEFAULT_FILE_SYSTEM_UMASK: u32 = 0o22;

    pub const DEFAULT_FILE_SYSTEM_MODE: u32 = 0o777;

    pub const DEFAULT_METRIC_REPORT_INTERVAL_STR: &'static str = "10s";

    pub const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;

    pub fn init(&mut self) -> CommonResult<()> {
        self.block_size = ByteUnit::from_str(&self.block_size_str)?.as_byte() as i64;

        self.write_chunk_size = ByteUnit::from_str(&self.write_chunk_size_str)?.as_byte() as usize;
        self.read_chunk_size = ByteUnit::from_str(&self.read_chunk_size_str)?.as_byte() as usize;

        // Handle read_slice
        let read_slice_size = ByteUnit::from_str(&self.read_slice_size_str)?.as_byte() as i64;
        self.read_slice_size = if read_slice_size <= 0 {
            (self.read_chunk_num * self.read_chunk_size) as i64
        } else {
            read_slice_size
        };

        // Process pre-reading
        self.drop_cache_len = ByteUnit::from_str(&self.drop_cache_len_str)?.as_byte() as i64;
        let read_ahead_len = ByteUnit::from_str(&self.read_ahead_len_str)?.as_byte() as i64;
        self.read_ahead_len = if read_ahead_len <= 0 {
            (self.read_chunk_num * self.read_chunk_size) as i64
        } else {
            read_ahead_len
        };
        if self.read_chunk_num <= 1 || self.read_ahead_len < Self::LONG_READ_THRESHOLD_LEN {
            self.enable_read_ahead = false
        }

        self.failed_worker_ttl = DurationUnit::from_str(&self.failed_worker_ttl_str)?.as_duration();
        self.mount_update_ttl = DurationUnit::from_str(&self.mount_update_ttl_str)?.as_duration();

        self.ttl_ms = DurationUnit::from_str(&self.ttl_ms_str)?.as_millis() as i64;
        self.ttl_action = TtlAction::try_from(self.ttl_action_str.as_str())?;
        self.storage_type = StorageType::try_from(self.storage_type_str.as_str())?;

        self.metric_report_interval =
            DurationUnit::from_str(&self.metric_report_interval_str)?.as_duration();

        Ok(())
    }

    pub fn client_rpc_conf(&self) -> RpcConf {
        let conf = self;
        RpcConf {
            io_threads: conf.io_threads,
            worker_threads: conf.worker_threads,

            conn_retry_max_duration_ms: conf.conn_retry_max_duration_ms,
            conn_retry_min_sleep_ms: conf.conn_retry_min_sleep_ms,
            conn_retry_max_sleep_ms: conf.conn_retry_max_sleep_ms,

            io_retry_max_duration_ms: conf.rpc_retry_max_duration_ms,
            io_retry_min_sleep_ms: conf.rpc_retry_min_sleep_ms,
            io_retry_max_sleep_ms: conf.rpc_retry_max_sleep_ms,

            close_idle: conf.rpc_close_idle,

            conn_timeout_ms: conf.conn_timeout_ms,
            rpc_timeout_ms: conf.rpc_timeout_ms,
            data_timeout_ms: conf.data_timeout_ms,

            conn_size: conf.master_conn_pool_size,

            buffer_size: 128 * 1024,
            ..Default::default()
        }
    }

    pub fn get_mode(&self) -> u32 {
        Self::DEFAULT_FILE_SYSTEM_MODE & !self.umask
    }
}

impl Default for ClientConf {
    fn default() -> Self {
        let master_addrs = vec![InetAddr::new(
            ClusterConf::DEFAULT_HOSTNAME,
            ClusterConf::DEFAULT_MASTER_PORT,
        )];
        let mut conf = Self {
            master_addrs,
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            io_threads: 16,
            worker_threads: Utils::worker_threads(16),

            replicas: 1,
            block_size: 0,
            block_size_str: "128MB".to_owned(),

            write_chunk_size: 0,
            write_chunk_size_str: "128KB".to_owned(),
            write_chunk_num: 8,

            read_chunk_size: 0,
            read_chunk_size_str: "128KB".to_owned(),
            read_chunk_num: 8,
            read_parallel: 1,
            read_slice_size: 0,
            read_slice_size_str: "0".to_owned(),
            close_reader_limit: 20,

            short_circuit: true,

            storage_type: StorageType::Disk,
            storage_type_str: "disk".to_string(),
            ttl_ms: 0,
            ttl_ms_str: "0".to_string(),
            ttl_action: TtlAction::None,
            ttl_action_str: "none".to_string(),

            auto_cache_enabled: false,
            auto_cache_ttl: "7d".to_string(),
            default_cache_ttl: Some("7d".to_string()),

            conn_retry_max_duration_ms: 2 * 60 * 1000,
            conn_retry_min_sleep_ms: 300,
            conn_retry_max_sleep_ms: 10 * 1000,

            rpc_retry_max_duration_ms: 5 * 60 * 1000,
            rpc_retry_min_sleep_ms: 300,
            rpc_retry_max_sleep_ms: 30 * 1000,

            rpc_close_idle: true,
            conn_timeout_ms: 30 * 1000,
            rpc_timeout_ms: 120 * 1000,
            data_timeout_ms: 5 * 60 * 1000,
            master_conn_pool_size: 1,

            enable_read_ahead: true,
            read_ahead_len: 0,
            read_ahead_len_str: "0".to_string(),
            drop_cache_len: 0,
            drop_cache_len_str: "1MB".to_string(),

            failed_worker_ttl: Duration::default(),
            failed_worker_ttl_str: "10m".to_owned(),

            enable_unified_fs: true,
            enable_rust_read_ufs: true,

            mount_update_ttl: Default::default(),
            mount_update_ttl_str: "10m".to_string(),

            umask: Self::DEFAULT_FILE_SYSTEM_UMASK,

            metric_report_enable: true,
            metric_report_interval: Default::default(),
            metric_report_interval_str: Self::DEFAULT_METRIC_REPORT_INTERVAL_STR.to_string(),
            close_timeout_secs: Self::DEFAULT_CLOSE_TIMEOUT_SECS,
        };

        conf.init().unwrap();
        conf
    }
}
