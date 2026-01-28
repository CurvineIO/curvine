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

use crate::conf::ClusterConf;
use crate::state::StorageType;
use orpc::common::{ByteUnit, DurationUnit, FileUtils, LogConf, Utils};
use orpc::{err_box, CommonResult};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq)]
#[serde(default)]
pub struct WorkerDataDir {
    pub storage_type: StorageType,
    pub capacity: u64,
    pub path: String,
}

impl WorkerDataDir {
    pub fn new(storage_type: StorageType, capacity: u64, path: &str) -> Self {
        let path = FileUtils::absolute_path_string(path).unwrap();
        Self {
            storage_type,
            capacity,
            path,
        }
    }

    fn with_path(path: &str) -> Self {
        Self::new(StorageType::Disk, 0, path)
    }

    fn is_alphabetic(str: &str) -> bool {
        for c in str.chars() {
            if !c.is_alphabetic() {
                return false;
            }
        }
        true
    }

    fn parse_stg_type(str: &str) -> StorageType {
        StorageType::from_str_name(str)
    }

    pub fn from_str(str: &str) -> CommonResult<Self> {
        let re = Regex::new(r"^\[([\w:]*)\](.+)$")?;
        let caps = match re.captures(str) {
            None => return Ok(Self::with_path(str)),
            Some(v) => v,
        };

        let prefix = caps.get(1).map_or("", |m| m.as_str());
        let path = caps.get(2).map_or("", |m| m.as_str());
        let arr = prefix.split(":").collect::<Vec<&str>>();

        if prefix.is_empty() || arr.is_empty() {
            // /dir
            return Ok(Self::with_path(str));
        };

        let (stg_type, capacity) = if arr.len() == 1 {
            if Self::is_alphabetic(arr[0]) {
                //[HDD]/dir
                (arr[0], "0")
            } else {
                //[20GB]/dir
                ("disk", arr[0])
            }
        } else if arr.len() == 2 {
            //[HDD:20GB]/dir
            (arr[0], arr[1])
        } else {
            return err_box!("Incorrect data format {}", str);
        };

        Ok(Self::new(
            Self::parse_stg_type(stg_type),
            ByteUnit::from_str(capacity)?.as_byte(),
            path,
        ))
    }

    pub fn storage_path<T: AsRef<str>>(&self, cluster_id: T) -> String {
        format!(
            "{}{}{}",
            self.path,
            std::path::MAIN_SEPARATOR_STR,
            cluster_id.as_ref()
        )
    }

    pub fn path_str(&self) -> &str {
        &self.path
    }
}

// Worker configuration file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerConf {
    pub hostname: String,

    pub rpc_port: u16,
    pub web_port: u16,

    pub dir_reserved: String,

    pub data_dir: Vec<String>,

    pub io_slow_threshold: String,

    pub io_threads: usize,

    pub worker_threads: usize,

    // Worker network read and write data timeout time, and whether to close idle connection; the default timeout is 10 minutes, and the timeout connection is not closed.
    // When writing to the worker, there may be no data for a long time; the client service does not implement a timeout sending heartbeat, so the server does not close the connection.
    pub io_timeout: String,
    pub io_close_idle: bool,

    pub scheduler_threads: usize,

    pub log: LogConf,

    // Number of asynchronous task threads
    pub executor_threads: usize,

    // Asynchronous task queue size.
    pub executor_channel_size: usize,

    pub enable_splice: bool,
    pub enable_send_file: bool,

    // Pipe size
    pub pipe_buf_size: usize,
    // Number of cores of pipeline resource pool
    pub pipe_pool_init_cap: usize,
    // Maximum number of pipeline resource pools
    pub pipe_pool_max_cap: usize,
    // In the pipeline resource pool, the pipeline idle recycling time.
    pub pipe_pool_idle_time: usize,

    pub block_replication_concurrency_limit: usize,
    pub block_replication_chunk_size: usize,

    // Enable S3 gateway alongside worker
    pub enable_s3_gateway: bool,
}

impl WorkerConf {
    pub fn init(&mut self) -> CommonResult<()> {
        Ok(())
    }

    pub fn io_slow_us(&self) -> u64 {
        let dur = DurationUnit::from_str(&self.io_slow_threshold).unwrap();
        dur.as_millis() * 1000
    }

    pub fn io_timeout_ms(&self) -> u64 {
        let dur = DurationUnit::from_str(&self.io_timeout).unwrap();
        dur.as_millis()
    }
}

impl Default for WorkerConf {
    fn default() -> Self {
        Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            rpc_port: ClusterConf::DEFAULT_WORKER_PORT,
            web_port: ClusterConf::DEFAULT_WORKER_WEB_PORT,
            dir_reserved: "0".to_string(),
            data_dir: vec![],
            io_slow_threshold: "300ms".to_string(),
            io_threads: 32,
            worker_threads: Utils::worker_threads(32),
            io_timeout: "10m".to_string(),
            io_close_idle: false,

            scheduler_threads: 2,
            log: Default::default(),

            executor_threads: 10,
            executor_channel_size: 1000,

            enable_splice: false,
            enable_send_file: true,

            pipe_buf_size: 64 * 1024,
            pipe_pool_init_cap: 0,
            pipe_pool_max_cap: 2000, // 100 mb
            pipe_pool_idle_time: 0,
            block_replication_concurrency_limit: 100,
            block_replication_chunk_size: 1024 * 1024,
            enable_s3_gateway: false,
        }
    }
}
