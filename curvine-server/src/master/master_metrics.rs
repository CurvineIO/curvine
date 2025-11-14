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
use crate::master::Master;
use curvine_common::state::MetricValue;
use log::{debug, info, warn};
use orpc::common::{Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Metrics as m, Metrics};
use orpc::sync::FastDashMap;
use orpc::sys::SysUtils;
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};

pub struct MasterMetrics {
    pub(crate) rpc_request_total_count: Counter,
    pub(crate) rpc_request_total_time: Counter,

    pub(crate) cluster_capacity: Gauge,
    pub(crate) cluster_available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) blocks_total_num: Gauge,
    pub(crate) blocks_size_avg: Gauge,

    pub(crate) worker_num: GaugeVec,

    pub(crate) journal_queue_len: Gauge,
    pub(crate) journal_flush_count: Counter,
    pub(crate) journal_flush_time: Counter,

    pub(crate) used_memory_bytes: Gauge,
    pub(crate) rocksdb_used_memory_bytes: GaugeVec,

    pub(crate) inode_dir_num: Gauge,
    pub(crate) inode_file_num: Gauge,

    // for the replication manager
    pub(crate) replication_staging_number: Gauge,
    pub(crate) replication_inflight_number: Gauge,
    pub(crate) replication_failure_count: Counter,

    pub(crate) operation_duration: HistogramVec,
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

            cluster_capacity: m::new_gauge("cluster_capacity", "Total storage capacity")?,
            cluster_available: m::new_gauge(
                "cluster_available",
                "Storage directory available space",
            )?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            blocks_total_num: m::new_gauge("blocks_total_num", "Total block number")?,
            blocks_size_avg: m::new_gauge("blocks_size_avg", "Average block size")?,
            worker_num: m::new_gauge_vec("worker_num", "The number of lived workers", &["tag"])?,

            journal_queue_len: m::new_gauge("journal_queue_len", "Journal queue length")?,
            journal_flush_count: m::new_counter(
                "journal_flush_count",
                "Number of flushes of log entries",
            )?,
            journal_flush_time: m::new_counter("journal_flush_time", "Log entry flush time")?,

            used_memory_bytes: m::new_gauge("used_memory_bytes", "Total memory used")?,
            rocksdb_used_memory_bytes: m::new_gauge_vec(
                "rocksdb_used_memory_bytes",
                "RocksDB memory usage in bytes",
                &["tag"],
            )?,

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
        };

        Ok(wm)
    }

    pub fn text_output(&self, fs: MasterFilesystem) -> CommonResult<String> {
        let master_info = fs.master_info()?;
        self.cluster_capacity.set(master_info.capacity);
        self.cluster_available.set(master_info.available);
        self.fs_used.set(master_info.fs_used);
        self.blocks_total_num.set(master_info.block_num);
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

        if master_info.block_num > 0 {
            let avg_size = master_info.fs_used / master_info.block_num;
            self.blocks_size_avg.set(avg_size);
        }

        let fs_dir = fs.fs_dir.read();
        if let Ok(memory_info) = fs_dir.get_rocks_store().get_rocksdb_memory() {
            for (component, size) in memory_info {
                self.rocksdb_used_memory_bytes
                    .with_label_values(&[&component])
                    .set(size as i64);
            }
        }

        self.worker_num
            .with_label_values(&["live"])
            .set(master_info.live_workers.len() as i64);
        self.worker_num
            .with_label_values(&["blacklist"])
            .set(master_info.blacklist_workers.len() as i64);
        self.worker_num
            .with_label_values(&["decommission"])
            .set(master_info.decommission_workers.len() as i64);
        self.worker_num
            .with_label_values(&["lost"])
            .set(master_info.lost_workers.len() as i64);

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
                    warn!("Not fond metrics {}: {}", value.name, e);
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
