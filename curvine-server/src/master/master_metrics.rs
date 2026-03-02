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
use curvine_common::state::{MetricType, MetricValue};
use log::{debug, info, warn};
use orpc::common::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Metrics as m, Metrics,
};
use orpc::sync::FastDashMap;
use orpc::sys::SysUtils;
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};

pub struct MasterMetrics {
    pub(crate) rpc_request_total_count: Counter,
    pub(crate) rpc_request_total_time: Counter,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) block_num: Gauge,
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

    // for quota eviction (LRU)
    pub(crate) eviction_lru_cache_size: Gauge,
    pub(crate) eviction_trigger_count: Counter,
    pub(crate) eviction_files_deleted: Counter,
    pub(crate) eviction_bytes_freed: Counter,
}

enum RegisteredMetric {
    Counter(Counter),
    CounterVec(CounterVec),
    Gauge(Gauge),
    GaugeVec(GaugeVec),
    Skip,
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

            capacity: m::new_gauge("capacity", "Total storage capacity")?,
            available: m::new_gauge("available", "Storage directory available space")?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            block_num: m::new_gauge("num_blocks", "Total block number")?,
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
        };

        Ok(wm)
    }

    pub fn text_output(&self, fs: MasterFilesystem) -> CommonResult<String> {
        let master_info = fs.master_info()?;
        self.capacity.set(master_info.capacity);
        self.available.set(master_info.available);
        self.fs_used.set(master_info.fs_used);
        self.block_num.set(master_info.block_num);
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

    fn sorted_label_keys(value: &MetricValue) -> Vec<&str> {
        let mut keys: Vec<&str> = value.tags.keys().map(|v| v.as_str()).collect();
        keys.sort_unstable();
        keys
    }

    fn sorted_label_values<'a>(value: &'a MetricValue, keys: &[&str]) -> Vec<&'a str> {
        keys.iter()
            .filter_map(|k| value.tags.get(*k).map(|v| v.as_str()))
            .collect()
    }

    fn get_or_register(
        &self,
        value: &MetricValue,
        label_keys: &[&str],
    ) -> CommonResult<RegisteredMetric> {
        if let Some(v) = Metrics::get(&value.name) {
            return match value.metric_type {
                MetricType::Gauge => {
                    if label_keys.is_empty() {
                        if let Ok(metric) = v.clone().try_into_gauge() {
                            return Ok(RegisteredMetric::Gauge(metric));
                        }
                    }
                    v.try_into_gauge_vec().map(RegisteredMetric::GaugeVec)
                }
                MetricType::Counter => {
                    if label_keys.is_empty() {
                        if let Ok(metric) = v.clone().try_into_counter() {
                            return Ok(RegisteredMetric::Counter(metric));
                        }
                    }
                    v.try_into_counter_vec().map(RegisteredMetric::CounterVec)
                }
                MetricType::Histogram => {
                    if label_keys.is_empty() {
                        if let Ok(metric) = v.clone().try_into_counter() {
                            return Ok(RegisteredMetric::Counter(metric));
                        }
                        if v.clone().try_into_histogram().is_ok() {
                            return Ok(RegisteredMetric::Skip);
                        }
                    }
                    if let Ok(metric) = v.clone().try_into_counter_vec() {
                        return Ok(RegisteredMetric::CounterVec(metric));
                    }
                    if v.clone().try_into_histogram_vec().is_ok() {
                        return Ok(RegisteredMetric::Skip);
                    }
                    v.try_into_counter_vec().map(RegisteredMetric::CounterVec)
                }
            };
        }

        match value.metric_type {
            MetricType::Gauge => {
                if label_keys.is_empty() {
                    let metric = m::new_gauge(&value.name, &value.name)?;
                    Ok(RegisteredMetric::Gauge(metric))
                } else {
                    let metric = m::new_gauge_vec(&value.name, &value.name, label_keys)?;
                    Ok(RegisteredMetric::GaugeVec(metric))
                }
            }
            MetricType::Counter => {
                if label_keys.is_empty() {
                    let metric = m::new_counter(&value.name, &value.name)?;
                    Ok(RegisteredMetric::Counter(metric))
                } else {
                    let metric = m::new_counter_vec(&value.name, &value.name, label_keys)?;
                    Ok(RegisteredMetric::CounterVec(metric))
                }
            }
            MetricType::Histogram => {
                if label_keys.is_empty() {
                    let metric = m::new_counter(&value.name, &value.name)?;
                    Ok(RegisteredMetric::Counter(metric))
                } else {
                    let metric = m::new_counter_vec(&value.name, &value.name, label_keys)?;
                    Ok(RegisteredMetric::CounterVec(metric))
                }
            }
        }
    }

    pub fn metrics_report(&self, metrics: Vec<MetricValue>) -> CommonResult<()> {
        for value in metrics {
            let label_keys = Self::sorted_label_keys(&value);
            let metric = match self.get_or_register(&value, &label_keys) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Not found metrics {}: {}", value.name, e);
                    continue;
                }
            };

            let label_values = Self::sorted_label_values(&value, &label_keys);
            match metric {
                RegisteredMetric::Counter(counter) => {
                    if value.value > 0f64 {
                        counter.inc_by(value.value as i64);
                    }
                }
                RegisteredMetric::CounterVec(counter) => {
                    if value.value > 0f64 {
                        counter
                            .with_label_values(&label_values)
                            .inc_by(value.value as i64);
                    }
                }
                RegisteredMetric::Gauge(gauge) => {
                    gauge.set(value.value as i64);
                }
                RegisteredMetric::GaugeVec(gauge) => {
                    gauge
                        .with_label_values(&label_values)
                        .set(value.value as i64);
                }
                RegisteredMetric::Skip => {}
            };
        }

        Ok(())
    }
}

impl Debug for MasterMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MasterMetrics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{MetricType, MetricValue};
    use orpc::common::Utils;
    use std::collections::HashMap;

    fn metric(
        name: String,
        metric_type: MetricType,
        value: f64,
        tags: HashMap<String, String>,
    ) -> MetricValue {
        MetricValue {
            metric_type,
            name,
            value,
            tags,
        }
    }

    #[test]
    fn gauge_metric_should_overwrite_value() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!("test_client_gauge_{}", Utils::uuid().replace('-', "_"));
        let mut tags = HashMap::new();
        tags.insert("a".to_string(), "v1".to_string());
        tags.insert("b".to_string(), "v2".to_string());
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Gauge,
                10.0,
                tags.clone(),
            )])
            .unwrap();
        metrics
            .metrics_report(vec![metric(name.clone(), MetricType::Gauge, 3.0, tags)])
            .unwrap();

        let gauge = Metrics::get(&name).unwrap().try_into_gauge_vec().unwrap();
        assert_eq!(gauge.with_label_values(&["v1", "v2"]).get(), 3);
    }

    #[test]
    fn counter_metric_should_accumulate_value() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!("test_client_counter_{}", Utils::uuid().replace('-', "_"));
        let mut tags = HashMap::new();
        tags.insert("a".to_string(), "v1".to_string());
        tags.insert("b".to_string(), "v2".to_string());
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Counter,
                2.0,
                tags.clone(),
            )])
            .unwrap();
        metrics
            .metrics_report(vec![metric(name.clone(), MetricType::Counter, 5.0, tags)])
            .unwrap();

        let counter = Metrics::get(&name).unwrap().try_into_counter_vec().unwrap();
        assert_eq!(counter.with_label_values(&["v1", "v2"]).get(), 7);
    }

    #[test]
    fn gauge_metric_without_labels_should_overwrite_value() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!(
            "test_client_gauge_plain_{}",
            Utils::uuid().replace('-', "_")
        );
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Gauge,
                10.0,
                HashMap::new(),
            )])
            .unwrap();
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Gauge,
                3.0,
                HashMap::new(),
            )])
            .unwrap();

        let gauge = Metrics::get(&name).unwrap().try_into_gauge().unwrap();
        assert_eq!(gauge.get(), 3);
    }

    #[test]
    fn counter_metric_without_labels_should_accumulate_value() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!(
            "test_client_counter_plain_{}",
            Utils::uuid().replace('-', "_")
        );
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Counter,
                2.0,
                HashMap::new(),
            )])
            .unwrap();
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Counter,
                5.0,
                HashMap::new(),
            )])
            .unwrap();

        let counter = Metrics::get(&name).unwrap().try_into_counter().unwrap();
        assert_eq!(counter.get(), 7);
    }

    #[test]
    fn histogram_metric_should_reuse_existing_counter_vec_as_count_delta() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!(
            "test_client_histogram_existing_{}",
            Utils::uuid().replace('-', "_")
        );
        let counter = Metrics::new_counter_vec(&name, &name, &["a", "b"]).unwrap();
        let mut tags = HashMap::new();
        tags.insert("a".to_string(), "v1".to_string());
        tags.insert("b".to_string(), "v2".to_string());

        metrics
            .metrics_report(vec![metric(name.clone(), MetricType::Histogram, 5.0, tags)])
            .unwrap();

        assert_eq!(counter.with_label_values(&["v1", "v2"]).get(), 5);
    }

    #[test]
    fn histogram_metric_should_skip_existing_histogram_vec() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!(
            "test_client_histogram_skip_{}",
            Utils::uuid().replace('-', "_")
        );
        let histogram = Metrics::new_histogram_vec(&name, &name, &["a", "b"]).unwrap();
        let mut tags = HashMap::new();
        tags.insert("a".to_string(), "v1".to_string());
        tags.insert("b".to_string(), "v2".to_string());

        metrics
            .metrics_report(vec![metric(name.clone(), MetricType::Histogram, 5.0, tags)])
            .unwrap();

        let sample = histogram.with_label_values(&["v1", "v2"]);
        assert_eq!(sample.get_sample_count(), 0);
        assert_eq!(sample.get_sample_sum(), 0.0);
    }

    #[test]
    fn histogram_metric_should_register_counter_vec_on_first_report() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!(
            "test_client_histogram_register_{}",
            Utils::uuid().replace('-', "_")
        );
        let mut tags = HashMap::new();
        tags.insert("a".to_string(), "v1".to_string());
        tags.insert("b".to_string(), "v2".to_string());

        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Histogram,
                7.0,
                tags.clone(),
            )])
            .unwrap();
        metrics
            .metrics_report(vec![metric(name.clone(), MetricType::Histogram, 3.0, tags)])
            .unwrap();

        let counter = Metrics::get(&name).unwrap().try_into_counter_vec().unwrap();
        assert_eq!(counter.with_label_values(&["v1", "v2"]).get(), 10);
    }

    #[test]
    fn histogram_metric_should_register_counter_on_first_report_without_labels() {
        let metrics = MasterMetrics::new().unwrap();
        let name = format!(
            "test_client_histogram_plain_{}",
            Utils::uuid().replace('-', "_")
        );

        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Histogram,
                4.0,
                HashMap::new(),
            )])
            .unwrap();
        metrics
            .metrics_report(vec![metric(
                name.clone(),
                MetricType::Histogram,
                6.0,
                HashMap::new(),
            )])
            .unwrap();

        let counter = Metrics::get(&name).unwrap().try_into_counter().unwrap();
        assert_eq!(counter.get(), 10);
    }
}
