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
use orpc::common::{Counter, CounterVec, Gauge, GaugeVec, Metrics as m, Metrics};
use orpc::sync::FastDashMap;
use orpc::sys::SysUtils;
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};

pub struct MasterMetrics {
    pub(crate) files_total: Gauge,
    pub(crate) dir_total: Gauge,

    pub(crate) rpc_request_count: CounterVec,
    pub(crate) rpc_request_time: CounterVec,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,

    pub(crate) worker_num: GaugeVec,

    pub(crate) journal_queue_len: Gauge,
    pub(crate) journal_flush_count: Counter,
    pub(crate) journal_flush_time: Counter,

    pub(crate) used_memory_bytes: Gauge,

    pub(crate) inode_dir_num: Gauge,
    pub(crate) inode_file_num: Gauge,
}

impl MasterMetrics {
    pub fn new() -> CommonResult<Self> {
        let wm = Self {
            files_total: m::new_gauge("files_total", "Total number of files")?,
            dir_total: m::new_gauge("dir_total", "Total number of directory")?,

            rpc_request_count: m::new_counter_vec(
                "rpc_request_count",
                "Numbers of rpc request",
                &["method"],
            )?,
            rpc_request_time: m::new_counter_vec(
                "rpc_request_time",
                "Rpc request time duration(ms)",
                &["method"],
            )?,

            capacity: m::new_gauge("capacity", "Total storage capacity")?,
            available: m::new_gauge("available", "Storage directory available space")?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            worker_num: m::new_gauge_vec("worker_num", "The number of lived workers", &["tag"])?,

            journal_queue_len: m::new_gauge("journal_queue_len", "Journal queue length")?,
            journal_flush_count: m::new_counter(
                "journal_flush_count",
                "Number of flushes of log entries",
            )?,
            journal_flush_time: m::new_counter("journal_flush_time", "Log entry flush time")?,

            used_memory_bytes: m::new_gauge("used_memory_bytes", "Total memory used")?,

            inode_dir_num: m::new_gauge("inode_dir_num", "Total dir")?,

            inode_file_num: m::new_gauge("inode_file_num", "Total file")?,
        };

        Ok(wm)
    }

    pub fn text_output(&self, fs: MasterFilesystem) -> CommonResult<String> {
        let master_info = fs.master_info()?;
        self.capacity.set(master_info.capacity);
        self.capacity.set(master_info.capacity);
        self.available.set(master_info.available);
        self.fs_used.set(master_info.fs_used);
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

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
