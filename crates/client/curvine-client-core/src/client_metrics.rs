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

use crate::file::FsContext;
use curvine_core_error::CommonResult;
use curvine_error::FsResult;
use curvine_io::DataSlice;
use curvine_metrics::{
    Counter, CounterVec, Gauge, Histogram, HistogramVec, MetricFamily, MetricFamilyType, Metrics,
    Metrics as m,
};
use curvine_model::{MetricType, MetricValue};
use curvine_runtime::common::TimeSpent;
use curvine_runtime::sync::FastDashMap;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;

/// Counter watermarks computed by [`ClientMetrics::encode`]. They stay
/// invisible until [`ClientMetrics::commit`] runs after the report was
/// delivered, so a failed report resends the same deltas instead of losing
/// them and leaving the master's counters permanently behind.
pub type PendingWatermarks = HashMap<String, f64>;

pub struct ClientMetrics {
    pub mount_cache_hits: CounterVec,
    pub mount_cache_misses: CounterVec,
    pub async_cache_admission_skipped: CounterVec,
    pub last_value_map: FastDashMap<String, f64>,

    pub metadata_operation_duration: HistogramVec,
    pub write_bytes: Counter,
    pub write_time_us: Counter,
    pub read_bytes: Counter,
    pub read_time_us: Counter,
    pub read_discarded_bytes: Counter,
    pub seek_absorbed_total: Counter,
    pub block_idle_conn: Gauge,
    /// Chunks currently being written to their replicas.
    pub write_inflight: Gauge,
    /// Bytes accepted by a writer but not yet handed to the block layer.
    pub write_queued_bytes: Gauge,
    /// Time one chunk takes from dispatch to every replica acknowledging it.
    /// Microseconds keep the reported sum integral: the master stores counters
    /// as `i64`, so a sum in seconds would truncate sub-second chunks to zero.
    pub write_backend_us: Histogram,
}

impl ClientMetrics {
    pub const PREFIX: &'static str = "client";

    pub fn new(buckets: &[f64]) -> CommonResult<Self> {
        let cm = Self {
            mount_cache_hits: m::new_counter_vec(
                "client_mount_cache_hits",
                "mount cache miss count",
                &["id"],
            )?,
            mount_cache_misses: m::new_counter_vec(
                "client_mount_cache_misses",
                "mount cache miss count",
                &["id"],
            )?,
            async_cache_admission_skipped: m::new_counter_vec(
                "client_async_cache_admission_skipped",
                "auto-cache admission requests skipped by reason",
                &["reason"],
            )?,

            last_value_map: FastDashMap::default(),

            metadata_operation_duration: m::new_histogram_vec_with_buckets(
                "client_metadata_operation_duration",
                "metadata operation duration",
                &["operation"],
                buckets,
            )?,
            write_bytes: m::new_counter("client_write_bytes", "write bytes total")?,
            write_time_us: m::new_counter("client_write_time_us", "write time us total")?,
            read_bytes: m::new_counter("client_read_bytes", "read bytes total")?,
            read_time_us: m::new_counter("client_read_time_us", "read time us total")?,
            read_discarded_bytes: m::new_counter(
                "client_read_discarded_bytes",
                "pipelined read bytes received but dropped after a seek or close",
            )?,
            seek_absorbed_total: m::new_counter(
                "client_seek_absorbed_total",
                "forward seeks absorbed by prefetch buffer",
            )?,
            block_idle_conn: m::new_gauge("client_block_idle_conn", "block idle conn total")?,
            write_inflight: m::new_gauge(
                "client_write_inflight_requests",
                "chunk writes currently sent to the block layer",
            )?,
            write_queued_bytes: m::new_gauge(
                "client_write_queued_bytes",
                "bytes queued by writers and not yet sent to the block layer",
            )?,
            write_backend_us: m::new_histogram_with_buckets(
                "client_write_backend_us",
                "duration of one chunk write across all replicas, in microseconds",
                &[
                    500.0,
                    1_000.0,
                    2_000.0,
                    5_000.0,
                    10_000.0,
                    20_000.0,
                    50_000.0,
                    100_000.0,
                    250_000.0,
                    500_000.0,
                    1_000_000.0,
                    2_500_000.0,
                    5_000_000.0,
                ],
            )?,
        };

        Ok(cm)
    }

    /// Run a write future and record elapsed time; bytes are counted only on success.
    pub async fn track_write<F>(&self, len: i64, fut: F) -> FsResult<i64>
    where
        F: Future<Output = FsResult<()>>,
    {
        let spent = TimeSpent::new();
        let res = fut.await;
        self.write_time_us.inc_by(spent.used_us() as i64);

        res?;
        self.write_bytes.inc_by(len);
        Ok(len)
    }

    /// Run a read future and record elapsed time; returned bytes are counted on success.
    pub async fn track_read<F>(&self, fut: F) -> FsResult<DataSlice>
    where
        F: Future<Output = FsResult<DataSlice>>,
    {
        let spent = TimeSpent::new();
        let res = fut.await;
        self.read_time_us.inc_by(spent.used_us() as i64);

        let slice = res?;
        self.read_bytes.inc_by(slice.len() as i64);
        Ok(slice)
    }

    pub fn text_output(&self) -> CommonResult<String> {
        Metrics::text_output()
    }

    /// Encode the current metrics and the counter watermarks the deltas were
    /// computed against. The watermarks take effect only through
    /// [`Self::commit`] once the report has been delivered.
    pub fn encode() -> CommonResult<(Vec<MetricValue>, PendingWatermarks)> {
        let cm = FsContext::get_metrics();
        let mut watermarks: PendingWatermarks = cm
            .last_value_map
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        let values = Self::encode_families(Metrics::registry().gather(), &mut watermarks);
        Ok((values, watermarks))
    }

    /// Commit the watermarks returned by [`Self::encode`] after the master
    /// accepted the report.
    pub fn commit(watermarks: PendingWatermarks) {
        Self::commit_into(FsContext::get_metrics(), watermarks);
    }

    fn commit_into(cm: &ClientMetrics, watermarks: PendingWatermarks) {
        for (key, value) in watermarks {
            // Series are monotonic: a concurrent encode may have committed a
            // newer total already, so never move a watermark backwards.
            cm.last_value_map
                .entry(key)
                .and_modify(|current| *current = current.max(value))
                .or_insert(value);
        }
    }

    /// Encode metric families for the master.
    ///
    /// Counters travel as positive deltas. Gauges travel as absolute snapshots
    /// on every report, including zero and decreases, so the master can keep
    /// them as levels. Histograms are decomposed into the counter series the
    /// master can store: `_count`, `_sum` and `_bucket{le}`.
    fn encode_families(
        families: Vec<MetricFamily>,
        watermarks: &mut PendingWatermarks,
    ) -> Vec<MetricValue> {
        let mut metric_values = Vec::new();
        for mf in families {
            let name = mf.get_name().to_string();
            if !name.starts_with(Self::PREFIX) {
                continue;
            }

            for metric in mf.get_metric() {
                let mut tags = HashMap::new();
                for label_pair in metric.get_label() {
                    tags.insert(
                        label_pair.get_name().to_string(),
                        label_pair.get_value().to_string(),
                    );
                }

                match mf.get_field_type() {
                    MetricFamilyType::COUNTER if metric.has_counter() => {
                        let value = metric.get_counter().get_value();
                        Self::push_counter(&mut metric_values, watermarks, &name, &tags, value);
                    }
                    MetricFamilyType::GAUGE if metric.has_gauge() => {
                        metric_values.push(MetricValue {
                            metric_type: MetricType::Gauge,
                            name: name.clone(),
                            value: metric.get_gauge().get_value(),
                            tags,
                        });
                    }
                    MetricFamilyType::HISTOGRAM if metric.has_histogram() => {
                        let histogram = metric.get_histogram();
                        Self::push_counter(
                            &mut metric_values,
                            watermarks,
                            &format!("{}_count", name),
                            &tags,
                            histogram.get_sample_count() as f64,
                        );
                        Self::push_counter(
                            &mut metric_values,
                            watermarks,
                            &format!("{}_sum", name),
                            &tags,
                            histogram.get_sample_sum(),
                        );
                        for bucket in histogram.get_bucket() {
                            let mut bucket_tags = tags.clone();
                            bucket_tags.insert(
                                "le".to_string(),
                                Self::bucket_bound(bucket.get_upper_bound()),
                            );
                            Self::push_counter(
                                &mut metric_values,
                                watermarks,
                                &format!("{}_bucket", name),
                                &bucket_tags,
                                bucket.get_cumulative_count() as f64,
                            );
                        }
                        // The prometheus proto omits the +Inf bucket. Emit it
                        // explicitly as the sample count so the master exposes
                        // a complete histogram for histogram_quantile.
                        let mut inf_tags = tags.clone();
                        inf_tags.insert("le".to_string(), "+Inf".to_string());
                        Self::push_counter(
                            &mut metric_values,
                            watermarks,
                            &format!("{}_bucket", name),
                            &inf_tags,
                            histogram.get_sample_count() as f64,
                        );
                    }
                    _ => {}
                }
            }
        }

        metric_values
    }

    /// Emit the positive delta of a monotonic series. Counters and histogram
    /// aggregates only grow, so the delta is what the master increments by.
    /// The new total goes to `watermarks`, not to the live map: it only counts
    /// once the report carrying this delta is delivered.
    fn push_counter(
        out: &mut Vec<MetricValue>,
        watermarks: &mut PendingWatermarks,
        name: &str,
        tags: &HashMap<String, String>,
        value: f64,
    ) {
        let key = Self::counter_key(name, tags);
        let last_value = watermarks.get(&key).copied().unwrap_or(0.0);
        watermarks.insert(key, value);

        if value > last_value {
            out.push(MetricValue {
                metric_type: MetricType::Counter,
                name: name.to_string(),
                value: value - last_value,
                tags: tags.clone(),
            });
        }
    }

    /// Stable identity of a counter series. Labels are sorted by name because
    /// HashMap iteration order is randomized: a Debug-printed key would differ
    /// between reports and turn the cumulative value into a fresh delta.
    fn counter_key(name: &str, tags: &HashMap<String, String>) -> String {
        let mut labels: Vec<_> = tags.iter().collect();
        labels.sort_by_key(|(key, _)| key.as_str());

        let mut out = name.to_string();
        for (key, value) in labels {
            out.push('|');
            out.push_str(key);
            out.push('=');
            out.push_str(value);
        }
        out
    }

    fn bucket_bound(bound: f64) -> String {
        if bound.is_infinite() {
            "+Inf".to_string()
        } else {
            bound.to_string()
        }
    }
}

/// Queued-bytes accounting for one chunk.
///
/// The guard is decremented when it is dropped, and it travels with the chunk
/// through the writer channel. A cancelled enqueue, a dropped channel and an
/// aborted buffer task therefore all release the bytes without explicit
/// cleanup; when the budget is granted the owner releases the guard before
/// dispatch so the chunk stops counting as queued exactly once.
pub struct QueuedBytes {
    gauge: Gauge,
    len: usize,
}

impl QueuedBytes {
    pub fn new(gauge: Gauge, len: usize) -> Self {
        gauge.add(len as i64);
        Self { gauge, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Stop counting the bytes as queued. Idempotent, so the drop that follows
    /// cannot subtract them twice.
    pub fn release(&mut self) {
        let len = std::mem::take(&mut self.len);
        if len > 0 {
            self.gauge.sub(len as i64);
        }
    }
}

impl Drop for QueuedBytes {
    fn drop(&mut self) {
        self.release();
    }
}

impl Debug for ClientMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientMetrics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_bytes_release_on_drop() {
        let metrics = ClientMetrics::new(&[]).unwrap();
        let before = metrics.write_queued_bytes.get();

        let mut queued = QueuedBytes::new(metrics.write_queued_bytes.clone(), 10);
        assert_eq!(metrics.write_queued_bytes.get(), before + 10);

        // Releasing before dispatch and dropping afterwards must subtract once.
        queued.release();
        assert_eq!(metrics.write_queued_bytes.get(), before);
        queued.release();
        drop(queued);
        assert_eq!(metrics.write_queued_bytes.get(), before);
    }

    #[test]
    fn queued_bytes_release_when_the_chunks_are_discarded() {
        // Dropped channels and aborted buffer tasks drop the chunks they hold.
        let metrics = ClientMetrics::new(&[]).unwrap();
        let before = metrics.write_queued_bytes.get();
        {
            let _first = QueuedBytes::new(metrics.write_queued_bytes.clone(), 4);
            let _second = QueuedBytes::new(metrics.write_queued_bytes.clone(), 6);
            assert_eq!(metrics.write_queued_bytes.get(), before + 10);
        }
        assert_eq!(metrics.write_queued_bytes.get(), before);
    }

    #[test]
    fn encode_reports_histograms_as_count_sum_and_buckets() {
        let histogram = m::new_histogram_with_buckets(
            "client_test_backend_us",
            "test histogram",
            &[1000.0, 2000.0],
        )
        .unwrap();
        histogram.observe(1500.0);
        histogram.observe(2500.0);

        let values =
            ClientMetrics::encode_families(Metrics::registry().gather(), &mut HashMap::new());
        let series = |name: &str| -> Vec<MetricValue> {
            values.iter().filter(|v| v.name == name).cloned().collect()
        };

        let count = series("client_test_backend_us_count");
        assert_eq!(count.len(), 1);
        assert_eq!(count[0].metric_type, MetricType::Counter);
        assert_eq!(count[0].value, 2.0);

        let sum = series("client_test_backend_us_sum");
        assert_eq!(sum.len(), 1);
        assert_eq!(sum[0].value, 4000.0);

        // Bucket series are cumulative and only emitted when they grow.
        let buckets = series("client_test_backend_us_bucket");
        let bucket = |le: &str| {
            buckets
                .iter()
                .find(|v| v.tags.get("le").map(String::as_str) == Some(le))
                .unwrap_or_else(|| panic!("missing bucket le={le}"))
                .value
        };
        assert!(buckets
            .iter()
            .all(|v| v.tags.get("le") != Some(&"1000".to_string())));
        assert_eq!(bucket("2000"), 1.0);
        assert_eq!(bucket("+Inf"), 2.0);
    }

    #[test]
    fn encode_reports_gauges_as_absolute_levels() {
        let gauge = m::new_gauge("client_test_queued_bytes", "test gauge").unwrap();

        let encode =
            || ClientMetrics::encode_families(Metrics::registry().gather(), &mut HashMap::new());
        let reported = |values: &[MetricValue]| {
            values
                .iter()
                .find(|v| v.name == "client_test_queued_bytes")
                .cloned()
                .expect("gauge must be reported")
        };

        gauge.set(10);
        let value = reported(&encode());
        assert_eq!(value.metric_type, MetricType::Gauge);
        assert_eq!(value.value, 10.0);

        // A drain to zero must be transmitted so the master overwrites the
        // previous level instead of keeping the stale one.
        gauge.set(0);
        assert_eq!(reported(&encode()).value, 0.0);

        gauge.set(3);
        assert_eq!(reported(&encode()).value, 3.0);
    }

    #[test]
    fn failed_reports_resend_the_counter_delta() {
        let cm = ClientMetrics::new(&[]).unwrap();
        let counter = m::new_counter("client_test_report_retry_total", "test counter").unwrap();

        let encode = || {
            let mut watermarks: PendingWatermarks = cm
                .last_value_map
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect();
            let values =
                ClientMetrics::encode_families(Metrics::registry().gather(), &mut watermarks);
            (values, watermarks)
        };
        let delta = |values: &[MetricValue]| {
            values
                .iter()
                .find(|v| v.name == "client_test_report_retry_total")
                .map(|v| v.value)
                .unwrap_or(0.0)
        };

        counter.inc_by(5);
        let (values, watermarks) = encode();
        assert_eq!(delta(&values), 5.0);

        // The report was not delivered, so nothing is committed: the next
        // encode must resend the same delta instead of swallowing it.
        drop(watermarks);
        let (values, watermarks) = encode();
        assert_eq!(delta(&values), 5.0);

        // Once delivered, only growth beyond the committed total is sent.
        ClientMetrics::commit_into(&cm, watermarks);
        counter.inc_by(2);
        let (values, _) = encode();
        assert_eq!(delta(&values), 2.0);
    }

    #[test]
    fn counter_key_is_stable_across_label_insertion_orders() {
        // Every report rebuilds the label map, and HashMap order is random.
        // The key must not depend on it, or a steady counter would be
        // re-reported at its cumulative value as if it were a fresh delta.
        let first: HashMap<String, String> = [
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]
        .into_iter()
        .collect();
        let second: HashMap<String, String> = [
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ]
        .into_iter()
        .collect();

        assert_eq!(
            ClientMetrics::counter_key("client_test_counter", &first),
            ClientMetrics::counter_key("client_test_counter", &second),
        );
    }

    #[test]
    fn test_seek_absorbed_total_counter() {
        let metrics = ClientMetrics::new(&[]).unwrap();
        assert_eq!(metrics.seek_absorbed_total.get(), 0);

        metrics.seek_absorbed_total.inc();
        assert_eq!(metrics.seek_absorbed_total.get(), 1);

        metrics.seek_absorbed_total.inc_by(4);
        assert_eq!(metrics.seek_absorbed_total.get(), 5);
    }
}
