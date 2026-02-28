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
use crate::p2p::P2pStatsSnapshot;
use curvine_common::state::{MetricType, MetricValue};
use orpc::common::{Counter, CounterVec, Histogram, HistogramVec, LocalTime, Metrics as m};
use orpc::common::{Gauge, Metrics};
use orpc::sync::FastDashMap;
use orpc::CommonResult;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

const UNKNOWN_LABEL_VALUE: &str = "unknown";
const OTHER_LABEL_VALUE: &str = "other";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReadSource {
    LocalChunkCache,
    P2p,
    WorkerLocal,
    WorkerRemote,
    Hole,
}

impl ReadSource {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            ReadSource::LocalChunkCache => "local_chunk_cache",
            ReadSource::P2p => "p2p",
            ReadSource::WorkerLocal => "worker_local",
            ReadSource::WorkerRemote => "worker_remote",
            ReadSource::Hole => "hole",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReadFallbackReason {
    OpenReaderError,
    SwitchReplica,
    AllWorkersFailed,
    HoleReadError,
}

impl ReadFallbackReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            ReadFallbackReason::OpenReaderError => "open_reader_error",
            ReadFallbackReason::SwitchReplica => "switch_replica",
            ReadFallbackReason::AllWorkersFailed => "all_workers_failed",
            ReadFallbackReason::HoleReadError => "hole_read_error",
        }
    }
}

fn normalize_label(value: Option<&str>) -> &str {
    match value.map(str::trim) {
        Some(v) if !v.is_empty() => v,
        _ => UNKNOWN_LABEL_VALUE,
    }
}

fn hash_label_value(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[derive(Default)]
struct P2pCounterBaseline {
    bytes_sent: i64,
    bytes_recv: i64,
    mtime_mismatches: i64,
    checksum_failures: i64,
    policy_rejects: i64,
    policy_rollback_ignored: i64,
}

pub struct ClientMetrics {
    pub mount_cache_hits: CounterVec,
    pub mount_cache_misses: CounterVec,
    pub last_value_map: FastDashMap<String, f64>,

    pub metadata_operation_duration: HistogramVec,
    pub write_bytes: Counter,
    pub write_time_us: Counter,
    pub read_bytes: Counter,
    pub read_time_us: Counter,
    pub block_idle_conn: Gauge,
    pub read_source_ops: CounterVec,
    pub read_source_bytes: CounterVec,
    pub read_source_latency_us_total: CounterVec,
    pub read_source_latency_ms: HistogramVec,
    pub read_source_ops_labeled: CounterVec,
    pub read_source_bytes_labeled: CounterVec,
    pub read_source_latency_us_total_labeled: CounterVec,
    pub read_source_latency_ms_labeled: HistogramVec,
    pub read_label_overflow_total: Counter,
    pub cache_hit_rate: Gauge,
    pub p2p_hit_rate: Gauge,
    pub total_hit_rate: Gauge,
    pub avg_read_latency_ms: Gauge,
    pub latency_percentiles: Histogram,
    pub read_fallback_total: CounterVec,
    pub read_fallback_total_labeled: CounterVec,
    pub p2p_active_peers: Gauge,
    pub p2p_mdns_peers: Gauge,
    pub p2p_dht_peers: Gauge,
    pub p2p_bootstrap_connected: Gauge,
    pub p2p_avg_peer_latency_ms: Gauge,
    pub p2p_bytes_sent: Counter,
    pub p2p_bytes_recv: Counter,
    pub p2p_policy_rejects_total: Counter,
    pub p2p_policy_rollback_ignored_total: Counter,
    pub cache_usage_bytes: Gauge,
    pub cache_capacity_bytes: Gauge,
    pub cache_usage_ratio: Gauge,
    pub cached_chunks_count: Gauge,
    pub expired_chunks: Gauge,
    pub mtime_mismatches: Counter,
    pub checksum_failures: Counter,
    pub corruption_count: Gauge,
    pub invalidations: Gauge,
    p2p_counter_baselines: FastDashMap<String, P2pCounterBaseline>,
    pub read_ops_total: AtomicI64,
    pub read_local_hits: AtomicI64,
    pub read_p2p_hits: AtomicI64,
    pub read_latency_us_total_acc: AtomicI64,
    pub read_label_series_cap: AtomicUsize,
    pub read_label_hash_job_id: AtomicBool,
    pub read_label_series: FastDashMap<String, ()>,
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
            block_idle_conn: m::new_gauge("block_idle_conn", "block idle conn total")?,
            read_source_ops: m::new_counter_vec(
                "client_read_source_ops",
                "read operation count by source",
                &["source"],
            )?,
            read_source_bytes: m::new_counter_vec(
                "client_read_source_bytes",
                "read bytes by source",
                &["source"],
            )?,
            read_source_latency_us_total: m::new_counter_vec(
                "client_read_source_latency_us_total",
                "read latency total in microseconds by source",
                &["source"],
            )?,
            read_source_latency_ms: m::new_histogram_vec_with_buckets(
                "client_read_source_latency_ms",
                "read latency by source in milliseconds",
                &["source"],
                &[
                    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                ],
            )?,
            read_source_ops_labeled: m::new_counter_vec(
                "client_read_source_ops_labeled",
                "read operation count by source and tenant/job labels",
                &["source", "tenant_id", "job_id"],
            )?,
            read_source_bytes_labeled: m::new_counter_vec(
                "client_read_source_bytes_labeled",
                "read bytes by source and tenant/job labels",
                &["source", "tenant_id", "job_id"],
            )?,
            read_source_latency_us_total_labeled: m::new_counter_vec(
                "client_read_source_latency_us_total_labeled",
                "read latency total in microseconds by source and tenant/job labels",
                &["source", "tenant_id", "job_id"],
            )?,
            read_source_latency_ms_labeled: m::new_histogram_vec_with_buckets(
                "client_read_source_latency_ms_labeled",
                "read latency by source and tenant/job labels in milliseconds",
                &["source", "tenant_id", "job_id"],
                &[
                    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                ],
            )?,
            read_label_overflow_total: m::new_counter(
                "client_read_label_overflow_total",
                "times tenant/job labels fallback to other due series cap",
            )?,
            cache_hit_rate: m::new_gauge("client_cache_hit_rate", "local cache hit rate percent")?,
            p2p_hit_rate: m::new_gauge("client_p2p_hit_rate", "p2p hit rate percent")?,
            total_hit_rate: m::new_gauge(
                "client_total_hit_rate",
                "combined local and p2p hit rate percent",
            )?,
            avg_read_latency_ms: m::new_gauge(
                "client_avg_read_latency_ms",
                "average read latency in milliseconds",
            )?,
            latency_percentiles: m::new_histogram_with_buckets(
                "client_latency_percentiles_ms",
                "read latency distribution in milliseconds",
                &[
                    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                ],
            )?,
            read_fallback_total: m::new_counter_vec(
                "client_read_fallback_total",
                "read fallback count by reason",
                &["reason"],
            )?,
            read_fallback_total_labeled: m::new_counter_vec(
                "client_read_fallback_total_labeled",
                "read fallback count by reason and tenant/job labels",
                &["reason", "tenant_id", "job_id"],
            )?,
            p2p_active_peers: m::new_gauge("client_p2p_active_peers", "active p2p peers")?,
            p2p_mdns_peers: m::new_gauge("client_p2p_mdns_peers", "mdns discovered peers")?,
            p2p_dht_peers: m::new_gauge("client_p2p_dht_peers", "dht discovered peers")?,
            p2p_bootstrap_connected: m::new_gauge(
                "client_p2p_bootstrap_connected",
                "connected bootstrap peers",
            )?,
            p2p_avg_peer_latency_ms: m::new_gauge(
                "client_p2p_avg_peer_latency_ms",
                "avg p2p peer latency in ms",
            )?,
            p2p_bytes_sent: m::new_counter("client_p2p_bytes_sent", "p2p bytes sent total")?,
            p2p_bytes_recv: m::new_counter("client_p2p_bytes_recv", "p2p bytes recv total")?,
            p2p_policy_rejects_total: m::new_counter(
                "client_p2p_policy_rejects_total",
                "rejected p2p policy update count",
            )?,
            p2p_policy_rollback_ignored_total: m::new_counter(
                "client_p2p_policy_rollback_ignored_total",
                "ignored stale p2p policy update count",
            )?,
            cache_usage_bytes: m::new_gauge(
                "client_cache_usage_bytes",
                "local p2p cache usage in bytes",
            )?,
            cache_capacity_bytes: m::new_gauge(
                "client_cache_capacity_bytes",
                "local p2p cache capacity in bytes",
            )?,
            cache_usage_ratio: m::new_gauge(
                "client_cache_usage_ratio",
                "local p2p cache usage ratio in percent",
            )?,
            cached_chunks_count: m::new_gauge(
                "client_cached_chunks_count",
                "cached chunk count in local p2p cache",
            )?,
            expired_chunks: m::new_gauge(
                "client_expired_chunks",
                "expired chunk count in local p2p cache",
            )?,
            mtime_mismatches: m::new_counter(
                "client_mtime_mismatches",
                "mtime mismatch count when reading cached chunks",
            )?,
            checksum_failures: m::new_counter(
                "client_checksum_failures",
                "checksum mismatch count when reading cached chunks",
            )?,
            corruption_count: m::new_gauge(
                "client_corruption_count",
                "corrupted chunk count detected in local p2p cache",
            )?,
            invalidations: m::new_gauge(
                "client_invalidations",
                "invalidated chunk count in local p2p cache",
            )?,
            p2p_counter_baselines: FastDashMap::default(),
            read_ops_total: AtomicI64::new(0),
            read_local_hits: AtomicI64::new(0),
            read_p2p_hits: AtomicI64::new(0),
            read_latency_us_total_acc: AtomicI64::new(0),
            read_label_series_cap: AtomicUsize::new(1024),
            read_label_hash_job_id: AtomicBool::new(true),
            read_label_series: FastDashMap::default(),
        };

        Ok(cm)
    }

    pub(crate) fn set_read_label_policy(&self, series_cap: usize, hash_job_id: bool) {
        self.read_label_series_cap
            .store(series_cap.max(1), Ordering::Relaxed);
        self.read_label_hash_job_id
            .store(hash_job_id, Ordering::Relaxed);
    }

    fn normalize_label_pair(
        &self,
        tenant_id: Option<&str>,
        job_id: Option<&str>,
    ) -> (String, String) {
        let tenant = normalize_label(tenant_id).to_string();
        let mut job = normalize_label(job_id).to_string();
        if self.read_label_hash_job_id.load(Ordering::Relaxed) && job != UNKNOWN_LABEL_VALUE {
            job = hash_label_value(&job);
        }
        let cap = self.read_label_series_cap.load(Ordering::Relaxed).max(1);
        let key = format!("{}|{}", tenant, job);
        if self.read_label_series.contains_key(&key) {
            return (tenant, job);
        }
        if self.read_label_series.len() >= cap {
            self.read_label_overflow_total.inc();
            return (OTHER_LABEL_VALUE.to_string(), OTHER_LABEL_VALUE.to_string());
        }
        self.read_label_series.insert(key, ());
        (tenant, job)
    }

    pub(crate) fn observe_read_source(
        &self,
        source: ReadSource,
        bytes: usize,
        start_nanos: u128,
        tenant_id: Option<&str>,
        job_id: Option<&str>,
    ) {
        let elapsed_nanos = LocalTime::nanos() - start_nanos;
        let latency_ms = elapsed_nanos as f64 / LocalTime::NANOSECONDS_PER_MILLISECOND as f64;
        let latency_us = (elapsed_nanos / 1000) as i64;
        let source_str = source.as_str();
        let (tenant_label, job_label) = self.normalize_label_pair(tenant_id, job_id);

        self.read_source_ops.with_label_values(&[source_str]).inc();
        self.read_source_bytes
            .with_label_values(&[source_str])
            .inc_by(bytes as i64);
        self.read_source_latency_us_total
            .with_label_values(&[source_str])
            .inc_by(latency_us);
        self.read_source_latency_ms
            .with_label_values(&[source_str])
            .observe(latency_ms);
        self.read_source_ops_labeled
            .with_label_values(&[source_str, tenant_label.as_str(), job_label.as_str()])
            .inc();
        self.read_source_bytes_labeled
            .with_label_values(&[source_str, tenant_label.as_str(), job_label.as_str()])
            .inc_by(bytes as i64);
        self.read_source_latency_us_total_labeled
            .with_label_values(&[source_str, tenant_label.as_str(), job_label.as_str()])
            .inc_by(latency_us);
        self.read_source_latency_ms_labeled
            .with_label_values(&[source_str, tenant_label.as_str(), job_label.as_str()])
            .observe(latency_ms);
        self.latency_percentiles.observe(latency_ms);

        self.read_ops_total.fetch_add(1, Ordering::Relaxed);
        self.read_latency_us_total_acc
            .fetch_add(latency_us, Ordering::Relaxed);
        if source == ReadSource::LocalChunkCache {
            self.read_local_hits.fetch_add(1, Ordering::Relaxed);
        }
        if source == ReadSource::P2p {
            self.read_p2p_hits.fetch_add(1, Ordering::Relaxed);
        }
        self.sync_read_derived_metrics();
    }

    pub(crate) fn observe_read_fallback(
        &self,
        reason: ReadFallbackReason,
        tenant_id: Option<&str>,
        job_id: Option<&str>,
    ) {
        let (tenant_label, job_label) = self.normalize_label_pair(tenant_id, job_id);
        self.read_fallback_total
            .with_label_values(&[reason.as_str()])
            .inc();
        self.read_fallback_total_labeled
            .with_label_values(&[reason.as_str(), tenant_label.as_str(), job_label.as_str()])
            .inc();
    }

    fn sync_read_derived_metrics(&self) {
        let total = self.read_ops_total.load(Ordering::Relaxed);
        if total <= 0 {
            return;
        }
        let local_hits = self.read_local_hits.load(Ordering::Relaxed).max(0);
        let p2p_hits = self.read_p2p_hits.load(Ordering::Relaxed).max(0);
        let latency_us = self
            .read_latency_us_total_acc
            .load(Ordering::Relaxed)
            .max(0);
        let total_hits = local_hits.saturating_add(p2p_hits);
        self.cache_hit_rate
            .set(local_hits.saturating_mul(100) / total);
        self.p2p_hit_rate.set(p2p_hits.saturating_mul(100) / total);
        self.total_hit_rate
            .set(total_hits.saturating_mul(100) / total);
        self.avg_read_latency_ms
            .set(latency_us.saturating_div(total).saturating_div(1000));
    }

    pub(crate) fn sync_p2p_snapshot(&self, service_id: &str, snapshot: &P2pStatsSnapshot) {
        self.p2p_active_peers.set(snapshot.active_peers as i64);
        self.p2p_mdns_peers.set(snapshot.mdns_peers as i64);
        self.p2p_dht_peers.set(snapshot.dht_peers as i64);
        self.p2p_bootstrap_connected
            .set(snapshot.bootstrap_connected as i64);
        self.p2p_avg_peer_latency_ms
            .set(snapshot.avg_peer_latency_ms as i64);
        self.cache_usage_bytes
            .set(snapshot.cache_usage_bytes.min(i64::MAX as u64) as i64);
        self.cache_capacity_bytes
            .set(snapshot.cache_capacity_bytes.min(i64::MAX as u64) as i64);
        self.cache_usage_ratio
            .set(snapshot.cache_usage_ratio as i64);
        self.cached_chunks_count
            .set(snapshot.cached_chunks_count.min(i64::MAX as usize) as i64);
        self.expired_chunks
            .set(snapshot.expired_chunks.min(i64::MAX as u64) as i64);
        self.corruption_count
            .set(snapshot.corruption_count.min(i64::MAX as u64) as i64);
        self.invalidations
            .set(snapshot.invalidations.min(i64::MAX as u64) as i64);

        let sent_total = snapshot.bytes_sent.min(i64::MAX as u64) as i64;
        let recv_total = snapshot.bytes_recv.min(i64::MAX as u64) as i64;
        let mtime_total = snapshot.mtime_mismatches.min(i64::MAX as u64) as i64;
        let checksum_total = snapshot.checksum_failures.min(i64::MAX as u64) as i64;
        let policy_rejects_total = snapshot.policy_rejects.min(i64::MAX as u64) as i64;
        let policy_rollback_total = snapshot.policy_rollback_ignored.min(i64::MAX as u64) as i64;

        let service_key = normalize_label(Some(service_id));
        let mut baseline = self
            .p2p_counter_baselines
            .entry(service_key.to_string())
            .or_default();
        let sent_prev = baseline.bytes_sent;
        let recv_prev = baseline.bytes_recv;
        let mtime_prev = baseline.mtime_mismatches;
        let checksum_prev = baseline.checksum_failures;
        let policy_rejects_prev = baseline.policy_rejects;
        let policy_rollback_prev = baseline.policy_rollback_ignored;

        baseline.bytes_sent = sent_total;
        baseline.bytes_recv = recv_total;
        baseline.mtime_mismatches = mtime_total;
        baseline.checksum_failures = checksum_total;
        baseline.policy_rejects = policy_rejects_total;
        baseline.policy_rollback_ignored = policy_rollback_total;

        if sent_total > sent_prev {
            self.p2p_bytes_sent.inc_by(sent_total - sent_prev);
        }
        if recv_total > recv_prev {
            self.p2p_bytes_recv.inc_by(recv_total - recv_prev);
        }
        if mtime_total > mtime_prev {
            self.mtime_mismatches.inc_by(mtime_total - mtime_prev);
        }
        if checksum_total > checksum_prev {
            self.checksum_failures
                .inc_by(checksum_total - checksum_prev);
        }
        if policy_rejects_total > policy_rejects_prev {
            self.p2p_policy_rejects_total
                .inc_by(policy_rejects_total - policy_rejects_prev);
        }
        if policy_rollback_total > policy_rollback_prev {
            self.p2p_policy_rollback_ignored_total
                .inc_by(policy_rollback_total - policy_rollback_prev);
        }
    }

    pub fn text_output(&self) -> CommonResult<String> {
        Metrics::text_output()
    }

    fn compute_report_value(
        metric_type: MetricType,
        last_value: f64,
        current_value: f64,
    ) -> Option<f64> {
        if last_value.is_nan() {
            return match metric_type {
                MetricType::Gauge => Some(current_value),
                MetricType::Counter | MetricType::Histogram => {
                    (current_value > 0f64).then_some(current_value)
                }
            };
        }

        match metric_type {
            MetricType::Gauge => {
                ((current_value - last_value).abs() > f64::EPSILON).then_some(current_value)
            }
            MetricType::Counter | MetricType::Histogram => {
                let delta = current_value - last_value;
                (delta > 0f64).then_some(delta)
            }
        }
    }

    pub fn encode() -> CommonResult<Vec<MetricValue>> {
        let cm = FsContext::get_metrics();
        let mut metric_values = Vec::new();
        let metric_families = Metrics::registry().gather();
        for mf in metric_families {
            let name = mf.get_name().to_string();
            if !name.starts_with(Self::PREFIX) {
                continue;
            }

            let metric_type = match mf.get_field_type() {
                prometheus::proto::MetricType::COUNTER => MetricType::Counter,
                prometheus::proto::MetricType::GAUGE => MetricType::Gauge,
                prometheus::proto::MetricType::HISTOGRAM => MetricType::Histogram,
                _ => MetricType::Gauge,
            };

            for metric in mf.get_metric() {
                let mut tags = HashMap::new();
                for label_pair in metric.get_label() {
                    tags.insert(
                        label_pair.get_name().to_string(),
                        label_pair.get_value().to_string(),
                    );
                }

                let value = match metric_type {
                    MetricType::Counter => {
                        if metric.has_counter() {
                            metric.get_counter().get_value()
                        } else {
                            0.0
                        }
                    }
                    MetricType::Gauge => {
                        if metric.has_gauge() {
                            metric.get_gauge().get_value()
                        } else {
                            0.0
                        }
                    }
                    MetricType::Histogram => {
                        if metric.has_histogram() {
                            metric.get_histogram().get_sample_count() as f64
                        } else {
                            0.0
                        }
                    }
                };

                let report_value = {
                    let key = format!("{}:{:?}", name, tags);
                    let mut last_value = cm.last_value_map.entry(key).or_insert(f64::NAN);
                    let report_value = Self::compute_report_value(metric_type, *last_value, value);
                    *last_value = value;
                    report_value
                };

                if let Some(report_value) = report_value {
                    metric_values.push(MetricValue {
                        metric_type,
                        name: name.clone(),
                        value: report_value,
                        tags,
                    });
                }
            }
        }

        Ok(metric_values)
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
    use orpc::common::LocalTime;

    #[test]
    fn gauge_should_report_absolute_value_on_decrease() {
        let value = ClientMetrics::compute_report_value(MetricType::Gauge, 10.0, 3.0);
        assert_eq!(value, Some(3.0));
    }

    #[test]
    fn counter_should_only_report_positive_delta() {
        let value = ClientMetrics::compute_report_value(MetricType::Counter, 10.0, 3.0);
        assert_eq!(value, None);
    }

    #[test]
    fn read_source_metrics_include_tenant_and_job_labels() {
        let cm = ClientMetrics::new(&[1.0, 5.0, 10.0]).unwrap();
        cm.set_read_label_policy(1024, false);
        let start = LocalTime::nanos();
        cm.observe_read_source(ReadSource::P2p, 128, start, Some("tenant-a"), Some("job-1"));
        assert_eq!(
            cm.read_source_ops_labeled
                .with_label_values(&["p2p", "tenant-a", "job-1"])
                .get(),
            1
        );
        assert_eq!(
            cm.read_source_bytes_labeled
                .with_label_values(&["p2p", "tenant-a", "job-1"])
                .get(),
            128
        );
    }

    #[test]
    fn fallback_metrics_use_unknown_labels_when_context_missing() {
        let cm = ClientMetrics::new(&[1.0, 5.0, 10.0]).unwrap();
        cm.observe_read_fallback(ReadFallbackReason::SwitchReplica, None, None);
        assert_eq!(
            cm.read_fallback_total_labeled
                .with_label_values(&["switch_replica", "unknown", "unknown"])
                .get(),
            1
        );
    }

    #[test]
    fn labeled_read_metrics_fallback_to_other_when_series_exceeds_cap() {
        let cm = ClientMetrics::new(&[1.0, 5.0, 10.0]).unwrap();
        cm.set_read_label_policy(1, false);
        let start = LocalTime::nanos();
        cm.observe_read_source(ReadSource::P2p, 64, start, Some("tenant-a"), Some("job-1"));
        cm.observe_read_source(ReadSource::P2p, 64, start, Some("tenant-b"), Some("job-2"));
        assert_eq!(
            cm.read_source_ops_labeled
                .with_label_values(&["p2p", "tenant-a", "job-1"])
                .get(),
            1
        );
        assert_eq!(
            cm.read_source_ops_labeled
                .with_label_values(&["p2p", "other", "other"])
                .get(),
            1
        );
    }

    #[test]
    fn labeled_read_metrics_hash_job_id_when_policy_enabled() {
        let cm = ClientMetrics::new(&[1.0, 5.0, 10.0]).unwrap();
        cm.set_read_label_policy(8, true);
        let start = LocalTime::nanos();
        let tenant = "tenant-h";
        let job = "job-super-long-123";
        let hashed = hash_label_value(job);
        cm.observe_read_source(ReadSource::P2p, 32, start, Some(tenant), Some(job));
        assert_eq!(
            cm.read_source_ops_labeled
                .with_label_values(&["p2p", tenant, hashed.as_str()])
                .get(),
            1
        );
    }

    #[test]
    fn p2p_counter_deltas_should_not_cross_talk_between_services() {
        let cm = ClientMetrics::new(&[1.0, 5.0, 10.0]).unwrap();
        let sent_before = cm.p2p_bytes_sent.get();
        let recv_before = cm.p2p_bytes_recv.get();

        let a1 = P2pStatsSnapshot {
            bytes_sent: 100,
            bytes_recv: 60,
            ..Default::default()
        };
        cm.sync_p2p_snapshot("client-a", &a1);

        let b1 = P2pStatsSnapshot {
            bytes_sent: 200,
            bytes_recv: 90,
            ..Default::default()
        };
        cm.sync_p2p_snapshot("client-b", &b1);

        let sent_delta = cm.p2p_bytes_sent.get() - sent_before;
        let recv_delta = cm.p2p_bytes_recv.get() - recv_before;
        assert_eq!(sent_delta, 300);
        assert_eq!(recv_delta, 150);
    }
}
