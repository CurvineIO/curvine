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

#![allow(unused)]

use crate::master::fs::MasterFilesystem;
use crate::master::meta::inode::ttl::TtlBucket;
use crate::master::Master;
use curvine_core_error::CommonResult;
use curvine_metrics::{Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Metrics, Metrics as m};
use curvine_model::MetricValue;
use curvine_runtime::sync::FastDashMap;
use curvine_sys::SysUtils;
use log::{debug, info, warn};
use std::fmt::{Debug, Formatter};

pub struct MasterMetrics {
    pub(crate) rpc_request_total_count: Counter,
    pub(crate) rpc_request_total_time: Counter,
    pub(crate) active_connections: Gauge,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) block_num: Gauge,
    pub(crate) blocks_size_avg: Gauge,
    pub(crate) allocatable_capacity: Gauge,
    pub(crate) allocatable_available: Gauge,

    pub(crate) worker_num: GaugeVec,

    pub(crate) journal_queue_len: Gauge,
    pub(crate) journal_flush_count: Counter,
    pub(crate) journal_flush_time: Counter,
    pub(crate) journal_committed: Gauge,
    pub(crate) journal_term: Gauge,
    pub(crate) journal_applied: Gauge,
    pub(crate) journal_ufs_applied: Gauge,

    pub(crate) used_memory_bytes: Gauge,
    pub(crate) rocksdb_metrics: GaugeVec,

    pub(crate) inode_dir_num: Gauge,
    pub(crate) inode_file_num: Gauge,

    // for the replication manager
    pub(crate) replication_staging_number: Gauge,
    pub(crate) replication_inflight_number: Gauge,
    pub(crate) replication_failure_count: Counter,

    pub(crate) operation_duration: HistogramVec,

    // for quota eviction (LRU)
    pub(crate) eviction_lru_cache_size: Gauge,
    pub(crate) eviction_trigger_count: Counter,
    pub(crate) eviction_files_deleted: Counter,
    pub(crate) eviction_bytes_freed: Counter,

    pub(crate) ttl_bucket_len: Gauge,
    pub(crate) ttl_total_inodes: Gauge,
    pub(crate) ttl_skipped_inodes: Counter,

    // fs_dir global lock health. The metadata lock cannot be sharded yet, so a
    // stalled or wedged lock freezes the whole control plane. These give the
    // stall an observable signal instead of a silent multi-minute freeze.
    pub(crate) fs_dir_stalled: Gauge,
    pub(crate) fs_dir_stall_total: Counter,
    pub(crate) fs_dir_probe_acquire_us: Gauge,

    // Compatibility metrics.
    // Per-verdict gauge: 1 for the current verdict of each worker/client
    // (compatible, missing_info, blocked, protocol_mismatch, version_too_old,
    // version_unknown). Only one label per peer is active at a time.
    pub(crate) compat_worker_verdict: GaugeVec,
    pub(crate) compat_client_verdict: GaugeVec,
    // Total number of enforce-mode rejections, labelled by component and verdict.
    pub(crate) compat_enforce_rejected_total: CounterVec,
}

impl MasterMetrics {
    pub fn new() -> CommonResult<Self> {
        let buckets = vec![
            10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0,
        ];
        let wm = Self {
            rpc_request_total_count: m::new_counter(
                "rpc_request_total_count",
                "Number of rpc request",
            )?,
            rpc_request_total_time: m::new_counter(
                "rpc_request_time",
                "Rpc request time duration(ms)",
            )?,
            active_connections: m::new_gauge(
                "master_active_connections",
                "Number of active RPC connections to master",
            )?,

            capacity: m::new_gauge("capacity", "Total storage capacity")?,
            available: m::new_gauge("available", "Storage directory available space")?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            block_num: m::new_gauge("num_blocks", "Total block number")?,
            blocks_size_avg: m::new_gauge("blocks_size_avg", "Average block size")?,
            allocatable_capacity: m::new_gauge(
                "allocatable_capacity",
                "Storage capacity eligible for new writes (Live workers only)",
            )?,
            allocatable_available: m::new_gauge(
                "allocatable_available",
                "Available space eligible for new writes (Live workers only)",
            )?,
            worker_num: m::new_gauge_vec("worker_num", "The number of lived workers", &["tag"])?,

            journal_queue_len: m::new_gauge("journal_queue_len", "Journal queue length")?,
            journal_flush_count: m::new_counter(
                "journal_flush_count",
                "Number of flushes of log entries",
            )?,
            journal_flush_time: m::new_counter("journal_flush_time", "Log entry flush time")?,
            journal_committed: m::new_gauge(
                "journal_committed",
                "Raft commit index of the journal",
            )?,
            journal_term: m::new_gauge("journal_term", "Current Raft term of the journal")?,
            journal_applied: m::new_gauge(
                "journal_applied",
                "Last applied index of the journal state machine",
            )?,
            journal_ufs_applied: m::new_gauge(
                "journal_ufs_applied",
                "Last UFS-applied index of the journal state machine",
            )?,

            used_memory_bytes: m::new_gauge("used_memory_bytes", "Total memory used")?,
            rocksdb_metrics: m::new_gauge_vec("rocksdb_metrics", "RocksDB metrics", &["tag"])?,

            inode_dir_num: m::new_gauge("inode_dir_num", "Total dir")?,

            inode_file_num: m::new_gauge("inode_file_num", "Total file")?,

            replication_staging_number: m::new_gauge(
                "replication_staging_number",
                "Replication stage number",
            )?,
            replication_inflight_number: m::new_gauge(
                "replication_inflight_number",
                "Replication stage number",
            )?,
            replication_failure_count: m::new_counter(
                "replication_failure_count",
                "Total failure count",
            )?,

            operation_duration: m::new_histogram_vec_with_buckets(
                "operation_duration",
                "Operation duration except WorkerHeartbeat",
                &["operation"],
                &buckets,
            )?,

            // Quota eviction metrics
            eviction_lru_cache_size: m::new_gauge(
                "eviction_lru_cache_size",
                "Number of files tracked in LRU cache for eviction",
            )?,
            eviction_trigger_count: m::new_counter(
                "eviction_trigger_count",
                "Number of times eviction was triggered",
            )?,
            eviction_files_deleted: m::new_counter(
                "eviction_files_deleted",
                "Total number of files deleted by eviction",
            )?,
            eviction_bytes_freed: m::new_counter(
                "eviction_bytes_freed",
                "Total bytes freed by eviction",
            )?,
            ttl_bucket_len: m::new_gauge("ttl_bucket_len", "Number of TTL buckets")?,
            ttl_total_inodes: m::new_gauge("ttl_total_inodes", "Total number of inodes")?,
            ttl_skipped_inodes: m::new_counter(
                "ttl_skipped_inodes",
                "Total number of inodes skipped while updating TTL buckets",
            )?,
            fs_dir_stalled: m::new_gauge(
                "fs_dir_stalled",
                "1 when the fs_dir metadata lock has been unacquirable beyond the stall threshold",
            )?,
            fs_dir_stall_total: m::new_counter(
                "fs_dir_stall_total",
                "Total number of fs_dir metadata lock stall episodes detected",
            )?,
            fs_dir_probe_acquire_us: m::new_gauge(
                "fs_dir_probe_acquire_us",
                "Latency in microseconds of the last fs_dir watchdog read-lock probe",
            )?,

            compat_worker_verdict: m::new_gauge_vec(
                "compat_worker_verdict",
                "Current compatibility verdict per worker, labelled by worker_id and verdict",
                &["worker_id", "verdict"],
            )?,
            compat_client_verdict: m::new_gauge_vec(
                "compat_client_verdict",
                "Current compatibility verdict per client, labelled by client_addr and verdict",
                &["client_addr", "verdict"],
            )?,
            compat_enforce_rejected_total: m::new_counter_vec(
                "compat_enforce_rejected_total",
                "Total number of enforce-mode compatibility rejections, labelled by component and verdict",
                &["component", "verdict"],
            )?,
        };

        Ok(wm)
    }

    pub fn text_output(&self, fs: MasterFilesystem) -> CommonResult<String> {
        let filesystem_info = fs.filesystem_info()?;
        self.capacity.set(filesystem_info.capacity);
        self.available.set(filesystem_info.available);
        self.fs_used.set(filesystem_info.fs_used);
        self.block_num.set(filesystem_info.block_num);
        self.allocatable_capacity
            .set(filesystem_info.allocatable_capacity);
        self.allocatable_available
            .set(filesystem_info.allocatable_available);
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

        if filesystem_info.block_num > 0 {
            let avg_size = filesystem_info.fs_used / filesystem_info.block_num;
            self.blocks_size_avg.set(avg_size);
        }

        let fs_dir = fs.fs_dir.read();
        let rocksdb_metrics = fs_dir.get_rocks_store().get_rocksdb_metrics()?;
        for (key, value) in rocksdb_metrics {
            self.rocksdb_metrics
                .with_label_values(&[&key])
                .set(value as i64);
        }

        let ttl_list = fs_dir.get_ttl_bucket_list();
        drop(fs_dir);
        self.ttl_bucket_len.set(ttl_list.buckets_len() as i64);
        self.ttl_total_inodes.set(ttl_list.total_inodes() as i64);

        self.worker_num
            .with_label_values(&["live"])
            .set(filesystem_info.live_workers.len() as i64);
        self.worker_num
            .with_label_values(&["blacklist"])
            .set(filesystem_info.blacklist_workers.len() as i64);
        self.worker_num
            .with_label_values(&["decommission"])
            .set(filesystem_info.decommission_workers.len() as i64);
        self.worker_num
            .with_label_values(&["lost"])
            .set(filesystem_info.lost_workers.len() as i64);

        Metrics::text_output()
    }

    pub fn get_or_register(&self, value: &MetricValue) -> CommonResult<CounterVec> {
        if let Some(v) = Metrics::get(&value.name) {
            return v.try_into_counter_vec();
        }

        let label_values: Vec<&str> = value.tags.keys().map(|v| v.as_str()).collect();
        let metric = m::new_counter_vec(&value.name, &value.name, &label_values)?;
        Ok(metric)
    }

    pub fn metrics_report(&self, metrics: Vec<MetricValue>) -> CommonResult<()> {
        for value in metrics {
            let counter = match self.get_or_register(&value) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Metric not found: {}: {}", value.name, e);
                    continue;
                }
            };

            let label_values: Vec<&str> = value.tags.values().map(|v| v.as_str()).collect();
            counter
                .with_label_values(&label_values)
                .inc_by(value.value as i64)
        }

        Ok(())
    }
}

impl Debug for MasterMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MasterMetrics")
    }
}
