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

use crate::state::StorageType;
use crate::{ClusterConf, CompatibilityConf};
use curvine_core_error::{err_box, CommonResult};
use curvine_io::SpdkConf;
use curvine_runtime::common::{ByteUnit, DurationUnit, FileUtils, LogConf, Utils};
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

    fn is_valid_storage_type(str: &str) -> bool {
        for c in str.chars() {
            if !c.is_alphabetic() && c != '_' {
                return false;
            }
        }
        true
    }

    fn parse_stg_type(str: &str) -> StorageType {
        StorageType::from_str_name(str)
    }

    pub fn from_str(str: &str) -> CommonResult<Self> {
        let str = str.trim();
        let re = Regex::new(r"^\[([\w:]*)\](.+)$")?;
        let caps = match re.captures(str) {
            None => return Ok(Self::with_path(str)),
            Some(v) => v,
        };

        let prefix = caps.get(1).map_or("", |m| m.as_str());
        let path = caps.get(2).map_or("", |m| m.as_str()).trim();
        let arr = prefix.split(':').map(str::trim).collect::<Vec<&str>>();

        if prefix.is_empty() || arr.is_empty() {
            // /dir
            return Ok(Self::with_path(str));
        };

        let (stg_type, capacity) = if arr.len() == 1 {
            if Self::is_valid_storage_type(arr[0]) {
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

    /// Relative allocation weight used by the master's `weighted` worker policy.
    /// A value of 0 keeps the worker registered but excludes it from new allocations.
    /// This is trusted administrator configuration; use values that reflect the
    /// intended relative capacity of workers in the cluster.
    pub weight: u32,

    pub rpc_port: u16,
    pub web_port: u16,

    pub dir_reserved: String,

    /// Strict physical free-space floor for Curvine write admission. The worker
    /// reports only allocatable bytes above this floor, so both new blocks and
    /// FileLayout rewrite staging copies are rejected if their full reservation
    /// would cross it. Existing committed data remains intact and readable when
    /// a rewrite is rejected. `0.0` disables the floor (default). Range
    /// `[0.0, 1.0)`. Example: `0.1` protects 10% of the underlying device.
    ///
    /// This favors shared-device safety over write availability: an existing
    /// file on a low-space worker can become temporarily non-writable. Rewrites
    /// are not exempt because FileLayout keeps the committed copy and a staging
    /// copy concurrently, and an expanding rewrite reserves the full target
    /// block size rather than only the growth delta. Allowing such rewrites to
    /// bypass the floor could consume space reserved for other applications.
    ///
    /// Unlike `dir_reserved` (a fixed byte reservation), this scales with disk
    /// size and reacts to other applications consuming the shared device. Both
    /// filesystem-backed and SPDK raw-device directories apply it. The floor is
    /// enforced against Curvine admissions; another application can still
    /// consume space after the worker samples the device.
    pub free_ratio: f64,

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

    // SPDK over NVMe-oF/RDMA configuration.
    pub spdk_disk: SpdkConf,

    /// Version compatibility policy applied to client peers on data-plane
    /// connections (diagnose by default; enforce only when explicitly
    /// configured). `min_worker_version` is ignored for a worker; the client
    /// bound `min_client_version` and `blocked_versions` are what apply.
    pub compatibility: CompatibilityConf,

    /// Mirror worker lifecycle and block reports to follower masters. The
    /// leader remains authoritative for commands and the default is disabled.
    pub follower_reports_enabled: bool,

    /// Maximum FIFO block-report operations per follower peer. Lifecycle
    /// heartbeats use fixed coalesced slots, and every slot counts toward the
    /// global byte budget.
    pub follower_report_queue_max_ops: usize,

    /// Maximum bytes held by follower queues and the shared full-resync
    /// inventory on this worker. RPCs count by encoded size; the shared
    /// `BlockMeta` vector counts by allocated bytes and is charged once no
    /// matter how many peers use it. The limit must fit the worker's full
    /// inventory plus per-peer Start and queue descriptors. If it does not,
    /// follower state remains stale and clients must fall back to the leader.
    /// Increase this limit when a worker's inventory grows beyond it.
    pub follower_report_queue_max_bytes: usize,

    /// Maximum number of follower peer queues created from a live master
    /// snapshot. Peers are selected in stable node-ID order.
    pub follower_report_max_peer_queues: usize,

    /// Maximum number of follower peer RPCs allowed in flight across this
    /// worker. Each peer itself is serialized to preserve Start/report order.
    pub follower_report_max_concurrency: usize,

    /// Maximum time to retry one queued follower request before it is dropped.
    pub follower_report_retry_timeout_ms: u64,

    /// Minimum and maximum retry delays for transient follower errors. The
    /// maximum delay also rate-limits retries when a full inventory exceeds the
    /// configured byte budget.
    pub follower_report_retry_min_ms: u64,
    pub follower_report_retry_max_ms: u64,
}

impl WorkerConf {
    const MAX_FOLLOWER_REPORT_QUEUE_OPS: usize = 4096;
    const MAX_FOLLOWER_REPORT_QUEUE_BYTES: usize = 1024 * 1024 * 1024;
    const MAX_FOLLOWER_REPORT_PEER_QUEUES: usize = 32;
    const MAX_FOLLOWER_REPORT_CONCURRENCY: usize = 32;
    const MAX_FOLLOWER_REPORT_RETRY_TIMEOUT_MS: u64 = 300_000;
    const MAX_FOLLOWER_REPORT_RETRY_DELAY_MS: u64 = 60_000;

    pub fn io_slow_us(&self) -> u64 {
        let dur = DurationUnit::from_str(&self.io_slow_threshold).unwrap();
        dur.as_millis() * 1000
    }

    pub fn io_timeout_ms(&self) -> u64 {
        let dur = DurationUnit::from_str(&self.io_timeout).unwrap();
        dur.as_millis()
    }

    /// Validate settings used by opt-in passive worker report fanout.
    pub fn validate_follower_reports(&self) -> CommonResult<()> {
        if !self.follower_reports_enabled {
            return Ok(());
        }
        if self.follower_report_queue_max_ops == 0 {
            return err_box!("worker.follower_report_queue_max_ops must be > 0");
        }
        if self.follower_report_queue_max_ops > Self::MAX_FOLLOWER_REPORT_QUEUE_OPS {
            return err_box!(
                "worker.follower_report_queue_max_ops must be <= {}",
                Self::MAX_FOLLOWER_REPORT_QUEUE_OPS
            );
        }
        if self.follower_report_queue_max_bytes == 0 {
            return err_box!("worker.follower_report_queue_max_bytes must be > 0");
        }
        if self.follower_report_queue_max_bytes > Self::MAX_FOLLOWER_REPORT_QUEUE_BYTES {
            return err_box!(
                "worker.follower_report_queue_max_bytes must be <= {}",
                Self::MAX_FOLLOWER_REPORT_QUEUE_BYTES
            );
        }
        if self.follower_report_max_peer_queues == 0 {
            return err_box!("worker.follower_report_max_peer_queues must be > 0");
        }
        if self.follower_report_max_peer_queues > Self::MAX_FOLLOWER_REPORT_PEER_QUEUES {
            return err_box!(
                "worker.follower_report_max_peer_queues must be <= {}",
                Self::MAX_FOLLOWER_REPORT_PEER_QUEUES
            );
        }
        if self.follower_report_max_concurrency == 0 {
            return err_box!("worker.follower_report_max_concurrency must be > 0");
        }
        if self.follower_report_max_concurrency > Self::MAX_FOLLOWER_REPORT_CONCURRENCY {
            return err_box!(
                "worker.follower_report_max_concurrency must be <= {}",
                Self::MAX_FOLLOWER_REPORT_CONCURRENCY
            );
        }
        if self.follower_report_retry_timeout_ms == 0 {
            return err_box!("worker.follower_report_retry_timeout_ms must be > 0");
        }
        if self.follower_report_retry_timeout_ms > Self::MAX_FOLLOWER_REPORT_RETRY_TIMEOUT_MS {
            return err_box!(
                "worker.follower_report_retry_timeout_ms must be <= {}",
                Self::MAX_FOLLOWER_REPORT_RETRY_TIMEOUT_MS
            );
        }
        if self.follower_report_retry_min_ms == 0 {
            return err_box!("worker.follower_report_retry_min_ms must be > 0");
        }
        if self.follower_report_retry_min_ms > self.follower_report_retry_timeout_ms {
            return err_box!(
                "worker.follower_report_retry_min_ms must be <= worker.follower_report_retry_timeout_ms"
            );
        }
        if self.follower_report_retry_max_ms > Self::MAX_FOLLOWER_REPORT_RETRY_DELAY_MS {
            return err_box!(
                "worker.follower_report_retry_max_ms must be <= {}",
                Self::MAX_FOLLOWER_REPORT_RETRY_DELAY_MS
            );
        }
        if self.follower_report_retry_max_ms < self.follower_report_retry_min_ms {
            return err_box!(
                "worker.follower_report_retry_max_ms must be >= worker.follower_report_retry_min_ms"
            );
        }
        Ok(())
    }
}

impl Default for WorkerConf {
    fn default() -> Self {
        Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            weight: 1,
            rpc_port: ClusterConf::DEFAULT_WORKER_PORT,
            web_port: ClusterConf::DEFAULT_WORKER_WEB_PORT,
            dir_reserved: "0".to_string(),
            free_ratio: 0.0,
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
            spdk_disk: SpdkConf::default(),
            compatibility: CompatibilityConf::default(),
            follower_reports_enabled: false,
            follower_report_queue_max_ops: 256,
            follower_report_queue_max_bytes: 64 * 1024 * 1024,
            follower_report_max_peer_queues: 32,
            follower_report_max_concurrency: 16,
            follower_report_retry_timeout_ms: 30_000,
            follower_report_retry_min_ms: 100,
            follower_report_retry_max_ms: 5_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WorkerConf, WorkerDataDir};

    #[test]
    fn from_str_trims_path_and_prefix() {
        let dir = WorkerDataDir::from_str("  [SSD:100MB] /data/curvine  ").unwrap();
        assert!(dir.path_str().ends_with("/data/curvine"));
        assert_eq!(dir.capacity, 100 * 1024 * 1024);
    }

    #[test]
    fn free_ratio_defaults_to_zero() {
        assert_eq!(WorkerConf::default().free_ratio, 0.0);
    }

    #[test]
    fn free_ratio_parses_from_toml() {
        let toml = r#"
dir_reserved = "0"
free_ratio = 0.1
data_dir = ["[SSD]/data/data1"]
"#;
        let conf: WorkerConf = toml::from_str(toml).unwrap();
        assert_eq!(conf.free_ratio, 0.1);
    }

    #[test]
    fn free_ratio_absent_defaults_to_zero() {
        // Backward compat: old configs without free_ratio must still parse.
        let toml = r#"
dir_reserved = "10GB"
data_dir = ["[SSD]/data/data1"]
"#;
        let conf: WorkerConf = toml::from_str(toml).unwrap();
        assert_eq!(conf.free_ratio, 0.0);
    }

    #[test]
    fn follower_report_fanout_is_disabled_by_default_with_bounded_limits() {
        let conf = WorkerConf::default();

        assert!(!conf.follower_reports_enabled);
        assert!(conf.follower_report_queue_max_ops > 0);
        assert!(conf.follower_report_queue_max_bytes > 0);
        assert!(conf.follower_report_max_peer_queues > 0);
        assert!(conf.follower_report_max_concurrency > 0);
        assert!(conf.validate_follower_reports().is_ok());
    }

    #[test]
    fn follower_report_fanout_validates_enabled_queue_limits() {
        let mut conf = WorkerConf {
            follower_reports_enabled: true,
            follower_report_queue_max_ops: 0,
            ..WorkerConf::default()
        };
        assert!(conf
            .validate_follower_reports()
            .unwrap_err()
            .to_string()
            .contains("follower_report_queue_max_ops"));

        conf.follower_report_queue_max_ops = 8;
        conf.follower_report_retry_min_ms = 10;
        conf.follower_report_retry_max_ms = 5;
        assert!(conf
            .validate_follower_reports()
            .unwrap_err()
            .to_string()
            .contains("follower_report_retry_max_ms"));

        conf.follower_report_retry_min_ms = 1;
        conf.follower_report_retry_max_ms = 10;
        conf.follower_report_max_peer_queues = 33;
        assert!(conf
            .validate_follower_reports()
            .unwrap_err()
            .to_string()
            .contains("follower_report_max_peer_queues"));
    }
}
