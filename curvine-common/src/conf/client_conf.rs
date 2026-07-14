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
use curvine_common_macros::ClientCliArgs;
use orpc::client::ClientConf as RpcConf;
use orpc::common::{ByteUnit, DurationUnit, Utils};
use orpc::io::net::InetAddr;
use orpc::CommonResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, ClientCliArgs)]
#[client_cli(prefix = "client", strip_suffix = "_str", opt_in)]
#[serde(default)]
pub struct ClientConf {
    // List of master addresses
    #[client_cli(skip)]
    pub master_addrs: Vec<InetAddr>,

    // The hostname of the machine where the customer service is located.
    // In some cases, this value needs to be set to identify that the client and worker are on the same machine.
    #[client_cli]
    pub hostname: String,

    #[client_cli]
    pub io_threads: usize,
    #[client_cli]
    pub worker_threads: usize,

    #[client_cli]
    pub replicas: i32,
    #[serde(skip)]
    pub block_size: i64,
    #[serde(alias = "block_size")]
    #[client_cli]
    pub block_size_str: String,

    #[serde(skip)]
    pub write_chunk_size: usize,
    #[serde(alias = "write_chunk_size")]
    #[client_cli]
    pub write_chunk_size_str: String,
    #[client_cli]
    pub write_chunk_num: usize,

    #[serde(skip)]
    pub read_chunk_size: usize,
    #[serde(alias = "read_chunk_size")]
    #[client_cli]
    pub read_chunk_size_str: String,
    #[client_cli]
    pub read_chunk_num: usize,

    // These 2 parameters are used to improve the speed of reading a single file.
    // Read the parallelism of a file, default is 1
    #[client_cli]
    pub read_parallel: i64,
    // The file is divided into blocks of different sizes according to this size, and read non-duplicate blocks at parallel task intervals.
    // Assume read_parallel = 2, file blocks 0,1,2,3
    // Parallel task 1 reads 0 and 2; Parallel task 2 reads 1 and 3
    // Default is 0, the value is read_chunk_size * read_chunk_num
    #[serde(skip)]
    pub read_slice_size: i64,
    #[serde(alias = "read_slice_size")]
    #[client_cli]
    pub read_slice_size_str: String,

    // Maximum number of open block handles (readers and writers).
    // When the limit is reached, FIFO eviction will close the oldest (first opened) handle.
    // This limits memory usage and connection count in random read/write scenarios.
    #[client_cli]
    pub max_cache_block_handles: usize,

    #[client_cli]
    pub short_circuit: bool,

    #[serde(skip)]
    pub storage_type: StorageType,
    #[serde(alias = "storage_type")]
    #[client_cli]
    pub storage_type_str: String,

    #[serde(skip)]
    pub ttl_ms: i64,
    #[serde(alias = "ttl_ms")]
    #[client_cli]
    pub ttl_ms_str: String,

    #[serde(skip)]
    pub ttl_action: TtlAction,
    #[serde(alias = "ttl_action")]
    #[client_cli]
    pub ttl_action_str: String,

    /// Whether to enable automatic caching function
    /// When enabled, when the client reads files from external file systems (such as S3, OSS, etc.),
    /// will automatically submit a load request to the master and cache the file into curvine
    #[client_cli]
    pub auto_cache_enabled: bool,

    /// Default TTL for automatic cache (living time)
    /// Format: Number + units, such as "10d" (10 days), "24h" (24 hours)
    /// Or pure number (seconds), such as "86400" (1 day)
    #[client_cli]
    pub auto_cache_ttl: String,

    /// Default cache TTL
    /// Same as auto_cache_ttl, but as Option<String> type, it is convenient to use in the API
    #[client_cli(skip)]
    pub default_cache_ttl: Option<String>,

    // Set up the customer service retry policy
    #[client_cli]
    pub conn_retry_max_duration_ms: u64,
    #[client_cli]
    pub conn_retry_min_sleep_ms: u64,
    #[client_cli]
    pub conn_retry_max_sleep_ms: u64,

    // rpc requests retry policy.
    #[client_cli]
    pub rpc_retry_max_duration_ms: u64,
    #[client_cli]
    pub rpc_retry_min_sleep_ms: u64,
    #[client_cli]
    pub rpc_retry_max_sleep_ms: u64,

    // Whether to close the idle rpc connection.
    #[client_cli]
    pub rpc_close_idle: bool,

    //Configuration of timeout for a request.
    #[client_cli]
    pub conn_timeout_ms: u64,
    #[client_cli]
    pub rpc_timeout_ms: u64,
    #[client_cli]
    pub data_timeout_ms: u64,
    pub pipeline_timeout_ms: u64,

    // After testing 3 connections, the best performance can be achieved, so the default value is 3.
    #[client_cli]
    pub master_conn_pool_size: usize,

    // Whether to enable pre-reading
    #[client_cli]
    pub enable_read_ahead: bool,
    // Default is 0, the value is read_chunk_size * read_chunk_num
    #[serde(skip)]
    pub read_ahead_len: i64,
    #[serde(alias = "read_ahead_len")]
    #[client_cli]
    pub read_ahead_len_str: String,
    #[serde(skip)]
    pub drop_cache_len: i64,
    #[serde(alias = "drop_cache_len")]
    #[client_cli]
    pub drop_cache_len_str: String,

    // Worker blacklist survival time, in milliseconds.
    #[serde(alias = "failed_worker_ttl")]
    #[client_cli]
    pub failed_worker_ttl_ms: u64,

    // Whether to enable the unified file system
    #[client_cli]
    pub enable_unified_fs: bool,
    // If the cache hits, read data from Curvine.
    // If the cache misses, determine whether to allow Curvine to directly read data from the unified file system (UFS).
    #[client_cli]
    pub enable_rust_read_ufs: bool,

    // Whether to enable client-side audit logging for UnifiedFileSystem operations.
    // The log target is "audit" and records: cmd, ok, src, dst, usedUs.
    #[client_cli]
    pub audit_logging_enabled: bool,

    // Mount information update interval, in milliseconds.
    #[serde(alias = "mount_update_ttl")]
    #[client_cli]
    pub mount_update_ttl_ms: u64,

    // File creation umask in octal notation (e.g. 022 or 0o22).
    #[client_cli(octal)]
    pub umask: u32,

    #[client_cli]
    pub metric_report_enable: bool,

    // Cleanup task interval, in milliseconds.
    #[serde(alias = "clean_task_interval")]
    #[client_cli]
    pub clean_task_interval_ms: u64,

    #[client_cli]
    pub close_timeout_secs: u64,

    #[client_cli(skip)]
    pub metadata_operation_buckets: Vec<f64>,

    // Minimum interval for checking if ufs sync task is complete / checking if curvine file data has updates, in milliseconds
    #[serde(alias = "sync_check_interval_min")]
    #[client_cli]
    pub sync_check_interval_min_ms: u64,

    // Maximum interval for checking if ufs sync task is complete / checking if curvine file data has updates, in milliseconds
    #[serde(alias = "sync_check_interval_max")]
    #[client_cli]
    pub sync_check_interval_max_ms: u64,

    // Maximum timeout for waiting for sync job to complete, in milliseconds
    #[serde(alias = "max_sync_wait_timeout")]
    #[client_cli]
    pub max_sync_wait_timeout_ms: u64,

    // Number of sync_check_interval cycles before logging
    #[client_cli]
    pub sync_check_log_tick: u32,

    #[client_cli]
    pub enable_block_conn_pool: bool,
    #[client_cli]
    pub block_conn_idle_size: usize,

    // Block connection idle time, in milliseconds.
    #[serde(alias = "block_conn_idle_time")]
    #[client_cli]
    pub block_conn_idle_time_ms: u64,

    #[serde(skip)]
    pub small_file_size: i64,
    #[serde(alias = "small_file_size")]
    #[client_cli]
    pub small_file_size_str: String,

    // Smart prefetch configuration
    // Whether to enable smart prefetch, default is true
    #[client_cli]
    pub enable_smart_prefetch: bool,

    #[serde(skip)]
    pub large_file_size: i64,
    #[serde(alias = "large_file_size")]
    #[client_cli]
    pub large_file_size_str: String,

    #[client_cli]
    pub max_read_parallel: i64,

    // Sequential read check threshold
    #[client_cli]
    pub sequential_read_threshold: u64,
}

impl ClientConf {
    pub const LONG_READ_THRESHOLD_LEN: i64 = 256 * 1024;

    pub const DEFAULT_FILE_SYSTEM_UMASK: u32 = 0o22;

    pub const DEFAULT_FILE_SYSTEM_MODE: u32 = 0o777;

    pub const DEFAULT_CLEAN_TASK_INTERVAL_MS: u64 = 60 * 1000;

    pub const DEFAULT_CLOSE_TIMEOUT_SECS: u64 = 5;

    pub const DEFAULT_READ_PARALLEL: i64 = 1;

    pub const DEFAULT_MAX_READ_PARALLEL: i64 = 8;

    pub fn init(&mut self) -> CommonResult<()> {
        if self.read_parallel <= 0 {
            self.read_parallel = Self::DEFAULT_READ_PARALLEL;
        }
        if self.max_read_parallel <= 0 {
            self.max_read_parallel = Self::DEFAULT_MAX_READ_PARALLEL;
        }

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

        self.small_file_size = ByteUnit::from_str(&self.small_file_size_str)?.as_byte() as i64;

        self.ttl_ms = DurationUnit::from_str(&self.ttl_ms_str)?.as_millis() as i64;
        self.ttl_action = TtlAction::try_from(self.ttl_action_str.as_str())?;
        self.storage_type = StorageType::try_from(self.storage_type_str.as_str())?;

        // Process smart prefetch configuration
        self.large_file_size = ByteUnit::from_str(&self.large_file_size_str)?.as_byte() as i64;

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

            buffer_size: self.read_chunk_size.max(self.write_chunk_size),
            ..Default::default()
        }
    }

    pub fn get_mode(&self) -> u32 {
        Self::DEFAULT_FILE_SYSTEM_MODE & !self.umask
    }
}

impl Default for ClientConf {
    fn default() -> Self {
        let mut conf = Self {
            master_addrs: vec![],
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
            read_parallel: Self::DEFAULT_READ_PARALLEL,
            read_slice_size: 0,
            read_slice_size_str: "0".to_owned(),
            max_cache_block_handles: 10,

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

            conn_retry_max_duration_ms: 60 * 1000,
            conn_retry_min_sleep_ms: 100,
            conn_retry_max_sleep_ms: 2 * 1000,

            rpc_retry_max_duration_ms: 120 * 1000,
            rpc_retry_min_sleep_ms: 100,
            rpc_retry_max_sleep_ms: 10 * 1000,

            rpc_close_idle: true,
            conn_timeout_ms: 30 * 1000,
            rpc_timeout_ms: 120 * 1000,
            data_timeout_ms: 120 * 1000,
            pipeline_timeout_ms: 120 * 1000,
            master_conn_pool_size: 3,

            enable_read_ahead: true,
            read_ahead_len: 0,
            read_ahead_len_str: "0".to_string(),
            drop_cache_len: 0,
            drop_cache_len_str: "1MB".to_string(),

            failed_worker_ttl_ms: 10 * 60 * 1000,

            enable_unified_fs: true,
            enable_rust_read_ufs: true,

            audit_logging_enabled: true,

            mount_update_ttl_ms: 10 * 1000,

            umask: Self::DEFAULT_FILE_SYSTEM_UMASK,

            metric_report_enable: true,

            clean_task_interval_ms: Self::DEFAULT_CLEAN_TASK_INTERVAL_MS,

            close_timeout_secs: Self::DEFAULT_CLOSE_TIMEOUT_SECS,

            metadata_operation_buckets: vec![
                10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0,
            ],

            sync_check_interval_min_ms: 100,

            sync_check_interval_max_ms: 1000,

            max_sync_wait_timeout_ms: 5 * 60 * 1000,

            sync_check_log_tick: 3,

            enable_block_conn_pool: true,
            block_conn_idle_size: 128,

            small_file_size: 0,
            small_file_size_str: "4MB".to_string(),

            block_conn_idle_time_ms: 60 * 1000,

            enable_smart_prefetch: true,
            large_file_size: 0,
            large_file_size_str: "10GB".to_string(),
            max_read_parallel: Self::DEFAULT_MAX_READ_PARALLEL,
            sequential_read_threshold: 7,
        };

        conf.init().unwrap();
        conf
    }
}
