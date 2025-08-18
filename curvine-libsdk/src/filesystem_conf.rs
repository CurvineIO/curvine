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

#![allow(clippy::should_implement_trait)]

use curvine_common::conf::{ClientConf, ClusterConf};
use curvine_common::FsResult;
use orpc::common::LogConf;
use orpc::io::net::InetAddr;
use orpc::{err_box, try_err};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FilesystemConf {
    pub master_addrs: String,
    pub client_hostname: String,
    pub io_threads: usize,
    pub worker_threads: usize,
    pub replicas: i32,
    pub block_size: String,
    pub short_circuit: bool,

    pub write_chunk_size: String,
    pub write_chunk_num: usize,

    pub read_chunk_size: String,
    pub read_chunk_num: usize,
    pub read_parallel: i64,
    pub read_slice_size: String,
    pub close_reader_limit: u32,

    pub storage_type: String,
    pub ttl_ms: String,
    pub ttl_action: String,

    pub auto_cache_enabled: bool,
    pub auto_cache_ttl: String,
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

    // Whether to enable pre-reading, it only controls whether short-circuit read and write, and whether it is turned on.
    // The worker terminal is currently open forever.
    pub enable_read_ahead: bool,

    // Read preview size
    pub read_ahead_len: String,

    pub drop_cache_len: String,

    // Log configuration
    pub log_level: String,
    pub log_dir: String,
    pub log_file_name: String,
    pub max_log_files: usize,
    pub display_thread: bool,
    pub display_position: bool,

    pub failed_worker_ttl: String,

    pub enable_unified_fs: bool,
    pub enable_read_ufs: bool,

    pub mount_update_ttl: String,
}

impl FilesystemConf {
    pub fn from_str<T: AsRef<str>>(conf: T) -> FsResult<Self> {
        let conf = try_err!(toml::from_str::<Self>(conf.as_ref()));
        Ok(conf)
    }

    pub fn into_cluster_conf(self) -> FsResult<ClusterConf> {
        let mut master_addrs = vec![];
        if self.master_addrs.is_empty() {
            return err_box!("fs.cv.master_addrs can not be empty");
        }

        for node in self.master_addrs.split(",") {
            let vec: Vec<&str> = node.split(":").collect();
            if vec.len() != 2 {
                return err_box!("wrong format fs.cv.master_addrs {}", self.master_addrs);
            }
            let hostname = vec[0].to_string();
            let port: u16 = vec[1].parse()?;
            master_addrs.push(InetAddr::new(hostname, port));
        }

        let client = ClientConf {
            master_addrs,
            hostname: self.client_hostname,
            io_threads: self.io_threads,
            worker_threads: self.worker_threads,
            replicas: self.replicas,
            block_size: 0,
            block_size_str: self.block_size,
            short_circuit: self.short_circuit,

            write_chunk_size: 0,
            write_chunk_size_str: self.write_chunk_size,
            write_chunk_num: self.write_chunk_num,

            read_chunk_size: 0,
            read_chunk_size_str: self.read_chunk_size,
            read_chunk_num: self.read_chunk_num,

            read_parallel: self.read_parallel,
            read_slice_size: 0,
            read_slice_size_str: self.read_slice_size,
            close_reader_limit: self.close_reader_limit,

            conn_retry_max_duration_ms: self.conn_retry_max_duration_ms,
            conn_retry_min_sleep_ms: self.conn_retry_min_sleep_ms,
            conn_retry_max_sleep_ms: self.conn_retry_max_sleep_ms,

            rpc_retry_max_duration_ms: self.rpc_retry_max_duration_ms,
            rpc_retry_min_sleep_ms: self.rpc_retry_min_sleep_ms,
            rpc_retry_max_sleep_ms: self.rpc_retry_max_sleep_ms,

            conn_timeout_ms: self.conn_timeout_ms,
            rpc_timeout_ms: self.rpc_timeout_ms,
            data_timeout_ms: self.data_timeout_ms,
            master_conn_pool_size: self.master_conn_pool_size,
            rpc_close_idle: self.rpc_close_idle,

            enable_read_ahead: self.enable_read_ahead,
            read_ahead_len: 0,
            read_ahead_len_str: self.read_ahead_len,
            drop_cache_len: 0,
            drop_cache_len_str: self.drop_cache_len,

            storage_type_str: self.storage_type,
            ttl_ms_str: self.ttl_ms,
            ttl_action_str: self.ttl_action,

            auto_cache_enabled: self.auto_cache_enabled,
            auto_cache_ttl: self.auto_cache_ttl,
            default_cache_ttl: self.default_cache_ttl,

            failed_worker_ttl: Duration::default(),
            failed_worker_ttl_str: self.failed_worker_ttl,

            enable_unified_fs: self.enable_unified_fs,
            enable_read_ufs: self.enable_read_ufs,
            mount_update_ttl_str: self.mount_update_ttl,

            ..Default::default()
        };

        let log = LogConf {
            level: self.log_level,
            log_dir: self.log_dir,
            file_name: self.log_file_name,
            max_log_files: self.max_log_files,
            display_thread: self.display_thread,
            display_position: self.display_position,
            ..Default::default()
        };

        let mut cluster = ClusterConf {
            client,
            log,
            ..Default::default()
        };
        cluster.client.init()?;
        Ok(cluster)
    }
}
