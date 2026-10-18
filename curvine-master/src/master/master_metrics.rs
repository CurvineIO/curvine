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
use curvine_model::{MetricValue, WorkerInfo};
use curvine_runtime::sync::FastDashMap;
use curvine_sys::version::ReleaseVersion;
use dashmap::DashMap;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::{Duration, Instant};

/// Release version label of a client that sent no component info yet.
const CLIENT_VERSION_NONE: &str = "none";
/// Release version label of a client whose reported version cannot be parsed.
/// Parsing keeps the label set bounded by real builds, not by client input.
const CLIENT_VERSION_UNKNOWN: &str = "unknown";

struct ClientCompat {
    release_version: String,
    verdict: &'static str,
    last_seen: Instant,
}

/// Last compatibility verdict of each client address, aggregated into
/// `(release_version, verdict)` counts when metrics are scraped.
///
/// The client address is state here, never a metric label, so the number of
/// series depends on the number of client builds, not on the number of
/// addresses. Addresses that stop calling are dropped after `TTL`.
#[derive(Default)]
pub(crate) struct ClientCompatTracker {
    clients: DashMap<String, ClientCompat>,
}

impl ClientCompatTracker {
    pub(crate) const TTL: Duration = Duration::from_secs(60 * 60);

    /// Record the verdict of one request. `release_version` is `None` when the
    /// request carried no component info, which a client sends only on the
    /// first call of a session; its later calls keep the version and verdict
    /// recorded for the address. Returns true when the verdict of the address
    /// changed, including its first observation.
    pub(crate) fn record(
        &self,
        client_ip: &str,
        release_version: Option<&str>,
        verdict: &'static str,
        now: Instant,
    ) -> bool {
        if let Some(mut known) = self.clients.get_mut(client_ip) {
            known.last_seen = now;
            let Some(version) = release_version else {
                return false;
            };
            let changed = known.verdict != verdict;
            known.release_version = Self::version_label(version);
            known.verdict = verdict;
            return changed;
        }

        self.clients.insert(
            client_ip.to_string(),
            ClientCompat {
                release_version: release_version
                    .map(Self::version_label)
                    .unwrap_or_else(|| CLIENT_VERSION_NONE.to_string()),
                verdict,
                last_seen: now,
            },
        );
        true
    }

    /// Drop addresses not seen within the TTL and count the rest by
    /// `(release_version, verdict)`.
    pub(crate) fn counts(&self, now: Instant) -> HashMap<(String, &'static str), i64> {
        self.clients
            .retain(|_, c| now.saturating_duration_since(c.last_seen) <= Self::TTL);
        let mut counts = HashMap::new();
        for entry in self.clients.iter() {
            *counts
                .entry((entry.release_version.clone(), entry.verdict))
                .or_insert(0) += 1;
        }
        counts
    }

    fn version_label(version: &str) -> String {
        ReleaseVersion::parse(version)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| CLIENT_VERSION_UNKNOWN.to_string())
    }
}

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
    pub(crate) scheduled_bytes: GaugeVec,

    pub(crate) worker_num: GaugeVec,

    pub(crate) journal_queue_len: Gauge,
    pub(crate) journal_flush_count: Counter,
    pub(crate) journal_flush_time: Counter,
    pub(crate) journal_committed: Gauge,
    pub(crate) journal_term: Gauge,
    pub(crate) journal_applied: Gauge,
    pub(crate) journal_ufs_applied: Gauge,

    pub(crate) rocksdb_metrics: GaugeVec,

    pub(crate) inode_dir_num: Gauge,
    pub(crate) inode_file_num: Gauge,

    // for the replication manager
    pub(crate) replication_staging_number: Gauge,
    pub(crate) replication_inflight_number: Gauge,
    pub(crate) replication_uncertain_number: Gauge,
    pub(crate) replication_failure_count: Counter,
    pub(crate) replication_timeout_count: Counter,

    pub(crate) operation_duration: HistogramVec,

    // Delay from request receipt to the executor starting to run the handler,
    // i.e. how long a request waited in the RPC runtime's queue. operation_duration
    // above only covers processing time, so this is what exposes executor
    // saturation (e.g. a read burst occupying the blocking pool) before clients
    // start timing out. The executor label separates the two blocking pools:
    // worker heartbeats/block reports wait on actor_rt, all other sync RPCs wait
    // on the main runtime (see MasterHandler::uses_actor_rt).
    pub(crate) rpc_queue_duration: HistogramVec,

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
    // Per-verdict gauge: 1 for the current verdict of each worker
    // (compatible, missing_info, blocked, protocol_mismatch, version_too_old,
    // version_unknown). Only one label per worker is active at a time.
    pub(crate) compat_worker_verdict: GaugeVec,
    // Number of clients per release version and verdict. Client addresses are
    // not a label because they are not bounded by cluster size.
    pub(crate) compat_client_verdict: GaugeVec,
    // Number of times a client address changed to a verdict, by verdict.
    pub(crate) compat_client_verdict_total: CounterVec,
    compat_clients: ClientCompatTracker,
    // Total number of enforce-mode rejections, labelled by component and verdict.
    pub(crate) compat_enforce_rejected_total: CounterVec,
}

impl MasterMetrics {
    pub fn new() -> CommonResult<Self> {
        let buckets = vec![
            10.0,
            50.0,
            100.0,
            500.0,
            1000.0,
            5000.0,
            10000.0,
            50000.0,
            100000.0,
            500_000.0,
            1_000_000.0,
            5_000_000.0,
            10_000_000.0,
            30_000_000.0,
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
            scheduled_bytes: m::new_gauge_vec(
                "scheduled_bytes",
                "In-flight allocations not yet reflected in heartbeat available space, labelled by live worker connect address",
                &["worker_addr"],
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

            rocksdb_metrics: m::new_gauge_vec("rocksdb_metrics", "RocksDB metrics", &["tag"])?,

            inode_dir_num: m::new_gauge("inode_dir_num", "Total dir")?,

            inode_file_num: m::new_gauge("inode_file_num", "Total file")?,

            replication_staging_number: m::new_gauge(
                "replication_staging_number",
                "Replication stage number",
            )?,
            replication_inflight_number: m::new_gauge(
                "replication_inflight_number",
                "Replication jobs awaiting a result",
            )?,
            replication_uncertain_number: m::new_gauge(
                "replication_uncertain_number",
                "Replication jobs whose remote execution state is uncertain",
            )?,
            replication_failure_count: m::new_counter(
                "replication_failure_count",
                "Total failure count",
            )?,
            replication_timeout_count: m::new_counter(
                "replication_timeout_count",
                "Total replication submit and result timeouts",
            )?,

            operation_duration: m::new_histogram_vec_with_buckets(
                "operation_duration",
                "Operation duration including WorkerHeartbeat",
                &["operation"],
                &buckets,
            )?,

            rpc_queue_duration: m::new_histogram_vec_with_buckets(
                "rpc_queue_duration",
                "Latency(us) from request receipt to handler execution start, by executor pool (main/actor)",
                &["executor"],
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
                "Number of clients per release version and compatibility verdict",
                &["release_version", "verdict"],
            )?,
            compat_client_verdict_total: m::new_counter_vec(
                "compat_client_verdict_total",
                "Total number of times a client address changed to a compatibility verdict, labelled by verdict",
                &["verdict"],
            )?,
            compat_clients: ClientCompatTracker::default(),
            compat_enforce_rejected_total: m::new_counter_vec(
                "compat_enforce_rejected_total",
                "Total number of enforce-mode compatibility rejections, labelled by component and verdict",
                &["component", "verdict"],
            )?,
        };

        Ok(wm)
    }

    fn refresh_scheduled_bytes(metric: &GaugeVec, workers: &[WorkerInfo]) {
        metric.reset();
        for worker in workers {
            let worker_addr = worker.address.connect_addr();
            metric
                .with_label_values(&[&worker_addr])
                .set(worker.scheduled_bytes.max(0));
        }
    }

    /// Record the compatibility verdict of a client request.
    /// `release_version` is `None` when the request carried no component info.
    pub(crate) fn record_client_compat(
        &self,
        client_ip: &str,
        release_version: Option<&str>,
        verdict: &'static str,
    ) {
        if self
            .compat_clients
            .record(client_ip, release_version, verdict, Instant::now())
        {
            self.compat_client_verdict_total
                .with_label_values(&[verdict])
                .inc();
        }
    }

    /// Rebuild the client gauge from the tracker, so versions and verdicts no
    /// client holds any more disappear from the output.
    fn refresh_client_verdicts(&self) {
        self.compat_client_verdict.reset();
        for ((version, verdict), count) in self.compat_clients.counts(Instant::now()) {
            self.compat_client_verdict
                .with_label_values(&[&version, verdict])
                .set(count);
        }
    }

    pub fn text_output(&self, fs: MasterFilesystem) -> CommonResult<String> {
        let filesystem_info = fs.filesystem_info()?;
        self.refresh_client_verdicts();
        self.capacity.set(filesystem_info.capacity);
        self.available.set(filesystem_info.available);
        self.fs_used.set(filesystem_info.fs_used);
        self.block_num.set(filesystem_info.block_num);
        self.allocatable_capacity
            .set(filesystem_info.allocatable_capacity);
        self.allocatable_available
            .set(filesystem_info.allocatable_available);
        Self::refresh_scheduled_bytes(&self.scheduled_bytes, &filesystem_info.live_workers);

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

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::WorkerAddress;

    fn count(
        counts: &HashMap<(String, &'static str), i64>,
        version: &str,
        verdict: &'static str,
    ) -> i64 {
        counts
            .get(&(version.to_string(), verdict))
            .copied()
            .unwrap_or(0)
    }

    #[test]
    fn client_verdicts_are_aggregated_by_release_version() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        // Many addresses on one build produce one entry, not one each.
        for i in 0..50 {
            tracker.record(
                &format!("10.0.0.{i}"),
                Some("0.1.0"),
                "version_too_old",
                now,
            );
        }
        tracker.record("10.0.1.1", Some("0.3.0"), "compatible", now);

        let counts = tracker.counts(now);
        assert_eq!(counts.len(), 2);
        assert_eq!(count(&counts, "0.1.0", "version_too_old"), 50);
        assert_eq!(count(&counts, "0.3.0", "compatible"), 1);
    }

    #[test]
    fn later_statfs_without_component_info_keeps_the_recorded_version() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        assert!(tracker.record("10.0.0.1", Some("0.3.0"), "compatible", now));

        // The following calls of the session carry no component_info.
        assert!(!tracker.record("10.0.0.1", None, "missing_info", now));
        assert!(!tracker.record("10.0.0.1", None, "missing_info", now));

        let counts = tracker.counts(now);
        assert_eq!(counts.len(), 1);
        assert_eq!(count(&counts, "0.3.0", "compatible"), 1);
    }

    #[test]
    fn first_call_without_component_info_is_a_legacy_client() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        assert!(tracker.record("10.0.0.1", None, "missing_info", now));
        assert_eq!(count(&tracker.counts(now), "none", "missing_info"), 1);
    }

    #[test]
    fn record_reports_verdict_transitions_only() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        assert!(tracker.record("10.0.0.1", Some("0.2.0"), "blocked", now));
        assert!(!tracker.record("10.0.0.1", Some("0.2.0"), "blocked", now));
        assert!(tracker.record("10.0.0.1", Some("0.3.0"), "compatible", now));
        assert!(tracker.record("10.0.0.1", Some("0.2.0"), "blocked", now));
    }

    #[test]
    fn unparseable_versions_share_one_label() {
        let tracker = ClientCompatTracker::default();
        let now = Instant::now();
        for i in 0..20 {
            tracker.record(
                &format!("10.0.0.{i}"),
                Some(&format!("garbage-{i}")),
                "version_unknown",
                now,
            );
        }
        let counts = tracker.counts(now);
        assert_eq!(counts.len(), 1);
        assert_eq!(count(&counts, "unknown", "version_unknown"), 20);
    }

    #[test]
    fn idle_clients_expire() {
        let tracker = ClientCompatTracker::default();
        let start = Instant::now();
        tracker.record("10.0.0.1", Some("0.3.0"), "compatible", start);
        tracker.record(
            "10.0.0.2",
            Some("0.3.0"),
            "compatible",
            start + ClientCompatTracker::TTL,
        );

        let later = start + ClientCompatTracker::TTL + Duration::from_secs(1);
        assert_eq!(count(&tracker.counts(later), "0.3.0", "compatible"), 1);
        assert!(tracker.counts(later + ClientCompatTracker::TTL).is_empty());
    }

    fn live_worker(ip: &str, rpc_port: u32, scheduled_bytes: i64) -> WorkerInfo {
        let mut worker = WorkerInfo::new(
            WorkerAddress {
                worker_id: rpc_port,
                hostname: "worker".to_string(),
                ip_addr: ip.to_string(),
                rpc_port,
                web_port: 0,
            },
            1,
        );
        worker.scheduled_bytes = scheduled_bytes;
        worker
    }

    #[test]
    fn scheduled_bytes_drops_lost_worker_labels() {
        let metric_name = "test_scheduled_bytes_drops_lost_worker_labels";
        let metric = m::new_gauge_vec(metric_name, metric_name, &["worker_addr"]).unwrap();
        let lost = "10.0.0.1:8995";

        MasterMetrics::refresh_scheduled_bytes(&metric, &[live_worker("10.0.0.1", 8995, 128)]);
        let output = Metrics::text_output().unwrap();
        assert!(
            output.contains(&format!("{metric_name}{{worker_addr=\"{lost}\"}} 128")),
            "{output}"
        );

        MasterMetrics::refresh_scheduled_bytes(&metric, &[]);
        let output = Metrics::text_output().unwrap();
        assert!(
            !output.contains(&format!("{metric_name}{{worker_addr=\"{lost}\"}}")),
            "{output}"
        );
    }

    #[test]
    fn scheduled_bytes_keeps_same_ip_workers_distinct() {
        let metric_name = "test_scheduled_bytes_keeps_same_ip_workers_distinct";
        let metric = m::new_gauge_vec(metric_name, metric_name, &["worker_addr"]).unwrap();
        let workers = [
            live_worker("10.0.0.1", 8995, 128),
            live_worker("10.0.0.1", 8996, 256),
        ];

        MasterMetrics::refresh_scheduled_bytes(&metric, &workers);
        let output = Metrics::text_output().unwrap();
        assert!(
            output.contains(&format!(
                "{metric_name}{{worker_addr=\"10.0.0.1:8995\"}} 128"
            )),
            "{output}"
        );
        assert!(
            output.contains(&format!(
                "{metric_name}{{worker_addr=\"10.0.0.1:8996\"}} 256"
            )),
            "{output}"
        );
    }
}
