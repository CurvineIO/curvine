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

use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_metrics::{Counter, CounterVec, Gauge, GaugeVec, HistogramVec, Metrics as m};
use curvine_model::TransferTaskCounts;
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Mutex;
use std::time::Instant;

static TRANSFER_METRICS: OnceCell<TransferMetrics> = OnceCell::new();

pub struct TransferMetrics {
    submit_total: CounterVec,
    report_total: CounterVec,
    report_rpc_total: CounterVec,
    report_batch_tasks_total: Counter,
    report_queue_len: Gauge,
    report_queue_len_by_lane: GaugeVec,
    job_summary_dirty_len: Gauge,
    job_summary_flush_total: CounterVec,
    job_summary_flush_duration_us: HistogramVec,
    active_jobs: Gauge,
    executing_jobs: Gauge,
    pending_jobs: Gauge,
    tasks_pending: Gauge,
    tasks_running: Gauge,
    tasks_completed: Gauge,
    tasks_failed: Gauge,
    tasks_stale: Gauge,
    cleanup_purged_total: Counter,
    acquire_total: CounterVec,
    planning_total: CounterVec,
    dispatch_total: CounterVec,
    lease_renew_total: CounterVec,
    stale_retry_total: CounterVec,
    terminal_total: CounterVec,
    worker_inflight_count: GaugeVec,
    worker_inflight_bytes: GaugeVec,
    reject_total: CounterVec,
    requeue_total: CounterVec,
    completed_bytes_total: CounterVec,
    store_operation_duration_us: HistogramVec,
    store_operation_total: CounterVec,
    store_unavailable: GaugeVec,
    store_unavailable_total: CounterVec,
    store_unavailable_duration_us_total: CounterVec,
    store_unavailable_since: Mutex<HashMap<String, Instant>>,
    metadata_operation_duration_us: HistogramVec,
    metadata_operation_total: CounterVec,
    cluster_snapshot_version: Gauge,
    cluster_snapshot_staleness_ms: Gauge,
    cluster_snapshot_live_workers: Gauge,
    cluster_snapshot_capable_workers: Gauge,
    cluster_snapshot_refresh_total: CounterVec,
    cluster_snapshot_refresh_duration_us: HistogramVec,
}

impl TransferMetrics {
    pub fn get() -> FsResult<&'static Self> {
        TRANSFER_METRICS.get_or_try_init(Self::new)
    }

    fn new() -> FsResult<Self> {
        let metrics = Self {
            submit_total: m::new_counter_vec(
                "transfer_submit_total",
                "Transfer submit requests by kind and result",
                &["kind", "result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            report_total: m::new_counter_vec(
                "transfer_task_report_total",
                "Transfer task report requests by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            report_rpc_total: m::new_counter_vec(
                "transfer_task_report_rpc_total",
                "Transfer task report RPCs by envelope type",
                &["type"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            report_batch_tasks_total: m::new_counter(
                "transfer_task_report_batch_tasks_total",
                "Total task progress reports received in batch RPC envelopes",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            report_queue_len: m::new_gauge(
                "transfer_task_report_queue_len",
                "Current transfer task report queue length",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            report_queue_len_by_lane: m::new_gauge_vec(
                "transfer_task_report_queue_len_by_lane",
                "Current transfer task report queue length by lane",
                &["lane"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            job_summary_dirty_len: m::new_gauge(
                "transfer_job_summary_dirty_len",
                "Current number of jobs waiting for progress summary flush",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            job_summary_flush_total: m::new_counter_vec(
                "transfer_job_summary_flush_total",
                "Transfer job progress summary flush attempts by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            job_summary_flush_duration_us: m::new_histogram_vec_with_buckets(
                "transfer_job_summary_flush_duration_us",
                "Transfer job progress summary flush latency in microseconds",
                &["result"],
                &[
                    100.0,
                    500.0,
                    1_000.0,
                    5_000.0,
                    10_000.0,
                    50_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            active_jobs: m::new_gauge(
                "transfer_active_jobs",
                "Current non-terminal transfer jobs in TransferStore",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            executing_jobs: m::new_gauge(
                "transfer_executing_jobs",
                "Current transfer jobs being planned, dispatched, running, or canceling",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            pending_jobs: m::new_gauge(
                "transfer_pending_jobs",
                "Current pending transfer jobs waiting in TransferStore backlog",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_pending: m::new_gauge(
                "transfer_tasks_pending",
                "Transfer tasks in Pending state across store (global observability)",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_running: m::new_gauge(
                "transfer_tasks_running",
                "Transfer tasks in Running state across store (global observability)",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_completed: m::new_gauge(
                "transfer_tasks_completed",
                "Transfer tasks in Completed state across store (global observability)",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_failed: m::new_gauge(
                "transfer_tasks_failed",
                "Transfer tasks in Failed state across store (global observability)",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_stale: m::new_gauge(
                "transfer_tasks_stale",
                "Transfer tasks in Stale state across store (global observability)",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cleanup_purged_total: m::new_counter(
                "transfer_cleanup_purged_total",
                "Total terminal transfer jobs purged by cleanup",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            acquire_total: m::new_counter_vec(
                "transfer_acquire_total",
                "Transfer scheduler acquire attempts by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            planning_total: m::new_counter_vec(
                "transfer_planning_total",
                "Transfer planning results by kind and result",
                &["kind", "result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            dispatch_total: m::new_counter_vec(
                "transfer_dispatch_total",
                "Transfer task dispatch results by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            lease_renew_total: m::new_counter_vec(
                "transfer_lease_renew_total",
                "Transfer lease renew attempts by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            stale_retry_total: m::new_counter_vec(
                "transfer_stale_retry_total",
                "Transfer stale task retry decisions by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            terminal_total: m::new_counter_vec(
                "transfer_terminal_total",
                "Transfer terminal state transitions by state and reason",
                &["state", "reason"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            worker_inflight_count: m::new_gauge_vec(
                "transfer_worker_inflight_count",
                "Per-worker in-flight task count (state=2, scheduler ledger)",
                &["worker_id"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            worker_inflight_bytes: m::new_gauge_vec(
                "transfer_worker_inflight_bytes",
                "Per-worker in-flight estimated bytes (state=2, scheduler ledger)",
                &["worker_id"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            reject_total: m::new_counter_vec(
                "transfer_worker_reject_total",
                "Worker submit rejections by class (capacity/session)",
                &["class"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            requeue_total: m::new_counter_vec(
                "transfer_requeue_total",
                "Transfer task requeues by reason (reject/stale)",
                &["reason"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            completed_bytes_total: m::new_counter_vec(
                "transfer_completed_bytes_total",
                "Bytes completed by terminal transfer state",
                &["state"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            store_operation_duration_us: m::new_histogram_vec_with_buckets(
                "transfer_store_operation_duration_us",
                "TransferStore operation latency in microseconds",
                &["backend", "operation", "result"],
                &[
                    100.0,
                    500.0,
                    1_000.0,
                    5_000.0,
                    10_000.0,
                    50_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            store_operation_total: m::new_counter_vec(
                "transfer_store_operation_total",
                "TransferStore operations by backend, operation, and result",
                &["backend", "operation", "result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            store_unavailable: m::new_gauge_vec(
                "transfer_store_unavailable",
                "Whether TransferStore backend is currently unavailable",
                &["backend"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            store_unavailable_total: m::new_counter_vec(
                "transfer_store_unavailable_total",
                "TransferStore unavailable events by backend and operation",
                &["backend", "operation"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            store_unavailable_duration_us_total: m::new_counter_vec(
                "transfer_store_unavailable_duration_us_total",
                "Total observed TransferStore unavailable duration in microseconds",
                &["backend"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            store_unavailable_since: Mutex::new(HashMap::new()),
            metadata_operation_duration_us: m::new_histogram_vec_with_buckets(
                "transfer_metadata_operation_duration_us",
                "Transfer planning metadata operation latency in microseconds",
                &["source", "operation", "result"],
                &[
                    100.0,
                    500.0,
                    1_000.0,
                    5_000.0,
                    10_000.0,
                    50_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                    5_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            metadata_operation_total: m::new_counter_vec(
                "transfer_metadata_operation_total",
                "Transfer planning metadata operations by source, operation, and result",
                &["source", "operation", "result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cluster_snapshot_version: m::new_gauge(
                "transfer_cluster_snapshot_version",
                "Current Transfer cluster metadata snapshot version",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cluster_snapshot_staleness_ms: m::new_gauge(
                "transfer_cluster_snapshot_staleness_ms",
                "Current Transfer cluster metadata snapshot staleness in milliseconds",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cluster_snapshot_live_workers: m::new_gauge(
                "transfer_cluster_snapshot_live_workers",
                "Current live workers in the Transfer cluster snapshot",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cluster_snapshot_capable_workers: m::new_gauge(
                "transfer_cluster_snapshot_capable_workers",
                "Current live workers with Transfer capabilities in the Transfer cluster snapshot",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cluster_snapshot_refresh_total: m::new_counter_vec(
                "transfer_cluster_snapshot_refresh_total",
                "Transfer cluster metadata snapshot refresh attempts by result",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            cluster_snapshot_refresh_duration_us: m::new_histogram_vec_with_buckets(
                "transfer_cluster_snapshot_refresh_duration_us",
                "Transfer cluster metadata snapshot refresh latency in microseconds",
                &["result"],
                &[
                    100.0,
                    500.0,
                    1_000.0,
                    5_000.0,
                    10_000.0,
                    50_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                    5_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
        };
        for backend in ["memory", "sqlite", "mysql", "postgres"] {
            metrics
                .store_unavailable
                .with_label_values(&[backend])
                .set(0);
            metrics
                .store_unavailable_duration_us_total
                .with_label_values(&[backend])
                .inc_by(0);
        }
        Ok(metrics)
    }

    pub fn inc_submit(&self, kind: &str, result: &str) {
        self.submit_total.with_label_values(&[kind, result]).inc();
    }

    pub fn inc_report(&self, result: &str) {
        self.report_total.with_label_values(&[result]).inc();
    }

    pub fn inc_report_rpc(&self, report_type: &str) {
        self.report_rpc_total
            .with_label_values(&[report_type])
            .inc();
    }

    pub fn inc_report_batch_tasks(&self, count: usize) {
        if count > 0 {
            self.report_batch_tasks_total.inc_by(count as i64);
        }
    }

    pub fn set_report_queue_len(&self, value: usize) {
        self.report_queue_len.set(value as i64);
    }

    pub fn set_report_queue_len_by_lane(&self, progress: usize, terminal: usize) {
        self.report_queue_len_by_lane
            .with_label_values(&["progress"])
            .set(progress as i64);
        self.report_queue_len_by_lane
            .with_label_values(&["terminal"])
            .set(terminal as i64);
    }

    pub fn set_job_summary_dirty_len(&self, value: usize) {
        self.job_summary_dirty_len.set(value as i64);
    }

    pub fn observe_job_summary_flush(&self, result: &str, elapsed_us: u128) {
        self.job_summary_flush_total
            .with_label_values(&[result])
            .inc();
        self.job_summary_flush_duration_us
            .with_label_values(&[result])
            .observe(elapsed_us as f64);
    }

    pub fn set_job_counts(&self, active: u64, executing: u64) {
        let pending = active.saturating_sub(executing);
        self.active_jobs.set(active as i64);
        self.executing_jobs.set(executing as i64);
        self.pending_jobs.set(pending as i64);
    }

    pub fn set_task_counts(&self, counts: &TransferTaskCounts) {
        self.tasks_pending.set(counts.pending as i64);
        self.tasks_running.set(counts.running as i64);
        self.tasks_completed.set(counts.completed as i64);
        self.tasks_failed.set(counts.failed as i64);
        self.tasks_stale.set(counts.stale as i64);
    }

    pub fn inc_cleanup_purged(&self, value: usize) {
        if value > 0 {
            self.cleanup_purged_total.inc_by(value as i64);
        }
    }

    pub fn inc_acquire(&self, result: &str) {
        self.acquire_total.with_label_values(&[result]).inc();
    }

    pub fn inc_planning(&self, kind: &str, result: &str) {
        self.planning_total.with_label_values(&[kind, result]).inc();
    }

    pub fn inc_dispatch(&self, result: &str, value: usize) {
        if value > 0 {
            self.dispatch_total
                .with_label_values(&[result])
                .inc_by(value as i64);
        }
    }

    pub fn inc_lease_renew(&self, result: &str) {
        self.lease_renew_total.with_label_values(&[result]).inc();
    }

    pub fn inc_stale_retry(&self, result: &str) {
        self.stale_retry_total.with_label_values(&[result]).inc();
    }

    pub fn inc_terminal(&self, state: &str, reason: &str) {
        self.terminal_total
            .with_label_values(&[state, reason])
            .inc();
    }

    /// Per-worker in-flight ledger snapshot (design §10 invariant 8).
    pub fn set_worker_inflight(&self, worker_id: u32, count: u64, bytes: i64) {
        let label = worker_id.to_string();
        self.worker_inflight_count
            .with_label_values(&[&label])
            .set(i64::try_from(count).unwrap_or(i64::MAX));
        self.worker_inflight_bytes
            .with_label_values(&[&label])
            .set(bytes);
    }

    /// Worker submit rejections by class (capacity / session / unknown).
    pub fn inc_reject(&self, class: &str) {
        self.reject_total.with_label_values(&[class]).inc();
    }

    /// Task requeues to Pending by reason (capacity / stale / other).
    pub fn inc_requeue(&self, reason: &str) {
        self.requeue_total.with_label_values(&[reason]).inc();
    }

    pub fn inc_completed_bytes(&self, state: &str, bytes: i64) {
        if bytes > 0 {
            self.completed_bytes_total
                .with_label_values(&[state])
                .inc_by(bytes);
        }
    }

    pub fn observe_store_operation(
        &self,
        backend: &str,
        operation: &str,
        result: &str,
        elapsed_us: u128,
    ) {
        let labels = &[backend, operation, result];
        self.store_operation_duration_us
            .with_label_values(labels)
            .observe(elapsed_us as f64);
        self.store_operation_total.with_label_values(labels).inc();
    }

    pub fn record_store_unavailable(&self, backend: &str, operation: &str) {
        self.store_unavailable.with_label_values(&[backend]).set(1);
        self.store_unavailable_total
            .with_label_values(&[backend, operation])
            .inc();
        if let Ok(mut unavailable_since) = self.store_unavailable_since.lock() {
            unavailable_since
                .entry(backend.to_string())
                .or_insert_with(Instant::now);
        }
    }

    pub fn record_store_available(&self, backend: &str) {
        self.store_unavailable.with_label_values(&[backend]).set(0);
        if let Ok(mut unavailable_since) = self.store_unavailable_since.lock() {
            if let Some(start) = unavailable_since.remove(backend) {
                let elapsed_us = start.elapsed().as_micros();
                if elapsed_us > 0 {
                    self.store_unavailable_duration_us_total
                        .with_label_values(&[backend])
                        .inc_by(elapsed_us.min(i64::MAX as u128) as i64);
                }
            }
        }
    }

    pub fn is_store_unavailable(&self, backend: &str) -> bool {
        self.store_unavailable_since
            .lock()
            .map(|unavailable_since| unavailable_since.contains_key(backend))
            .unwrap_or(true)
    }

    pub fn observe_metadata_operation(
        &self,
        source: &str,
        operation: &str,
        result: &str,
        elapsed_us: u128,
    ) {
        let labels = &[source, operation, result];
        self.metadata_operation_duration_us
            .with_label_values(labels)
            .observe(elapsed_us as f64);
        self.metadata_operation_total
            .with_label_values(labels)
            .inc();
    }

    pub fn observe_cluster_snapshot_refresh(
        &self,
        result: &str,
        elapsed_us: u128,
        version: Option<u64>,
        updated_at_ms: Option<i64>,
        live_workers: Option<usize>,
        capable_workers: Option<usize>,
    ) {
        self.cluster_snapshot_refresh_total
            .with_label_values(&[result])
            .inc();
        self.cluster_snapshot_refresh_duration_us
            .with_label_values(&[result])
            .observe(elapsed_us as f64);
        self.set_cluster_snapshot(version, updated_at_ms, live_workers, capable_workers);
    }

    pub fn set_cluster_snapshot(
        &self,
        version: Option<u64>,
        updated_at_ms: Option<i64>,
        live_workers: Option<usize>,
        capable_workers: Option<usize>,
    ) {
        if let Some(version) = version {
            self.cluster_snapshot_version.set(version as i64);
        }
        if let Some(updated_at_ms) = updated_at_ms {
            let staleness_ms = curvine_runtime::common::LocalTime::mills()
                .saturating_sub(updated_at_ms.max(0) as u64);
            self.cluster_snapshot_staleness_ms.set(staleness_ms as i64);
        }
        if let Some(live_workers) = live_workers {
            self.cluster_snapshot_live_workers.set(live_workers as i64);
        }
        if let Some(capable_workers) = capable_workers {
            self.cluster_snapshot_capable_workers
                .set(capable_workers as i64);
        }
    }
}

impl Debug for TransferMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TransferMetrics")
    }
}
