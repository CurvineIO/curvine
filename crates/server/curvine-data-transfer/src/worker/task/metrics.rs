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
use curvine_fs_api::Path;
use curvine_metrics::{CounterVec, Gauge, HistogramVec, Metrics as m};
use curvine_model::JobTaskState;
use once_cell::sync::OnceCell;
use std::fmt::{Debug, Formatter};

use crate::worker::task::task_progress_reporter::ReportOutcome;

static WORKER_TRANSFER_METRICS: OnceCell<WorkerTransferMetrics> = OnceCell::new();

pub struct WorkerTransferMetrics {
    submit_total: CounterVec,
    submit_rejected_total: CounterVec,
    terminal_total: CounterVec,
    failed_reason_total: CounterVec,
    report_total: CounterVec,
    copy_bytes_total: CounterVec,
    execution_duration_us: HistogramVec,
    read_duration_us: HistogramVec,
    write_duration_us: HistogramVec,
    tasks_active: Gauge,
    tasks_running: Gauge,
    tasks_queued: Gauge,
    terminal_retained: Gauge,
    terminal_retention_age_seconds: Gauge,
    evicted_total: CounterVec,
    permits_available: Gauge,
    permits_max: Gauge,
    inflight_budget_bytes: Gauge,
    queue_duration_us: HistogramVec,
}

impl WorkerTransferMetrics {
    pub fn get() -> FsResult<&'static Self> {
        WORKER_TRANSFER_METRICS.get_or_try_init(Self::new)
    }

    fn new() -> FsResult<Self> {
        Ok(Self {
            submit_total: m::new_counter_vec(
                "worker_transfer_task_submit_total",
                "Transfer task submit decisions on this worker",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            submit_rejected_total: m::new_counter_vec(
                "worker_transfer_task_submit_rejected_total",
                "Transfer task submit rejections by reason",
                &["reason"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            terminal_total: m::new_counter_vec(
                "worker_transfer_task_terminal_total",
                "Transfer task terminal outcomes on this worker",
                &["state", "kind"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            failed_reason_total: m::new_counter_vec(
                "worker_transfer_task_failed_total",
                "Transfer task failures by reason on this worker",
                &["reason"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            report_total: m::new_counter_vec(
                "worker_transfer_task_report_total",
                "Transfer task progress reports sent from this worker",
                &["result"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            copy_bytes_total: m::new_counter_vec(
                "worker_transfer_task_copy_bytes_total",
                "Bytes copied by transfer tasks on this worker",
                &["state"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            execution_duration_us: m::new_histogram_vec_with_buckets(
                "worker_transfer_task_execution_duration_us",
                "Transfer task execution latency on this worker in microseconds",
                &["state"],
                &[
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    30_000_000.0,
                    60_000_000.0,
                    300_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            read_duration_us: m::new_histogram_vec_with_buckets(
                "worker_transfer_task_read_duration_us",
                "Source-read phase latency per transfer task in microseconds",
                &["state"],
                &[
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    30_000_000.0,
                    60_000_000.0,
                    300_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            write_duration_us: m::new_histogram_vec_with_buckets(
                "worker_transfer_task_write_duration_us",
                "Target-write phase latency per transfer task in microseconds",
                &["state"],
                &[
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    30_000_000.0,
                    60_000_000.0,
                    300_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_active: m::new_gauge(
                "worker_transfer_tasks_active",
                "Transfer tasks tracked in this worker task store",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_running: m::new_gauge(
                "worker_transfer_tasks_running",
                "Transfer tasks currently executing copy/load on this worker",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            tasks_queued: m::new_gauge(
                "worker_transfer_tasks_queued",
                "Transfer tasks waiting for a worker concurrency permit",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            terminal_retained: m::new_gauge(
                "worker_transfer_terminal_retained_tasks",
                "Finished transfer task contexts kept for a scheduler stale probe",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            terminal_retention_age_seconds: m::new_gauge(
                "worker_transfer_terminal_retention_age_seconds",
                "Age of the oldest finished transfer task context kept for a scheduler stale probe, -1 when none is retained",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            evicted_total: m::new_counter_vec(
                "worker_transfer_task_evicted_total",
                "Transfer task contexts evicted from the worker task store, by reason",
                &["reason"],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            permits_available: m::new_gauge(
                "worker_transfer_task_permits_available",
                "Available transfer task concurrency permits on this worker",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            permits_max: m::new_gauge(
                "worker_transfer_task_permits_max",
                "Configured maximum concurrent transfer tasks on this worker",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            inflight_budget_bytes: m::new_gauge(
                "worker_transfer_task_inflight_budget_bytes",
                "Current runtime Transfer in-flight byte admission budget on this worker",
            )
            .map_err(|err| FsError::common(err.to_string()))?,
            queue_duration_us: m::new_histogram_vec_with_buckets(
                "worker_transfer_task_queue_duration_us",
                "Time a transfer task waits for a concurrency permit before running, in microseconds",
                &["result"],
                &[
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    30_000_000.0,
                    60_000_000.0,
                    300_000_000.0,
                ],
            )
            .map_err(|err| FsError::common(err.to_string()))?,
        })
    }

    pub fn set_permits_max(&self, value: usize) {
        self.permits_max.set(value as i64);
    }

    pub fn set_permits_available(&self, value: usize) {
        self.permits_available.set(value as i64);
    }

    pub fn set_inflight_budget_bytes(&self, value: i64) {
        self.inflight_budget_bytes.set(value);
    }

    pub fn inc_submit_accepted(&self) {
        self.submit_total.with_label_values(&["accepted"]).inc();
    }

    pub fn inc_submit_duplicate(&self) {
        self.submit_total.with_label_values(&["duplicate"]).inc();
    }

    pub fn inc_submit_rejected(&self, reason: &str) {
        self.submit_total.with_label_values(&["rejected"]).inc();
        self.submit_rejected_total
            .with_label_values(&[reason])
            .inc();
    }

    pub fn inc_active(&self) {
        self.tasks_active.inc();
    }

    pub fn dec_active(&self) {
        self.tasks_active.dec();
    }

    /// Publishes the authoritative number of tracked task contexts.
    ///
    /// The gauge is reconciled from the store size rather than incremented in
    /// place: a superseded submit replaces an entry without adding one, so
    /// pairing `inc`/`dec` with entry mutations drifts over time.
    pub fn set_active(&self, count: usize) {
        self.tasks_active.set(count as i64);
    }

    pub fn set_terminal_retained(&self, count: usize, oldest_age_seconds: Option<i64>) {
        self.terminal_retained.set(count as i64);
        self.terminal_retention_age_seconds
            .set(oldest_age_seconds.unwrap_or(-1));
    }

    pub fn inc_evicted(&self, reason: &str) {
        self.evicted_total.with_label_values(&[reason]).inc();
    }

    pub fn inc_running(&self) {
        self.tasks_running.inc();
    }

    pub fn dec_running(&self) {
        self.tasks_running.dec();
    }

    pub fn inc_queued(&self) {
        self.tasks_queued.inc();
    }

    pub fn observe_queued(&self, result: &str, duration_us: u128) {
        self.queue_duration_us
            .with_label_values(&[result])
            .observe(duration_us as f64);
    }

    pub fn dec_queued(&self) {
        self.tasks_queued.dec();
    }

    pub fn observe_terminal(
        &self,
        state: &str,
        kind: &str,
        duration_us: u128,
        read_us: u128,
        write_us: u128,
        copied_bytes: i64,
    ) {
        self.terminal_total.with_label_values(&[state, kind]).inc();
        self.execution_duration_us
            .with_label_values(&[state])
            .observe(duration_us as f64);
        self.read_duration_us
            .with_label_values(&[state])
            .observe(read_us as f64);
        self.write_duration_us
            .with_label_values(&[state])
            .observe(write_us as f64);
        if copied_bytes > 0 {
            self.copy_bytes_total
                .with_label_values(&[state])
                .inc_by(copied_bytes);
        }
    }

    pub fn inc_failed_reason(&self, reason: &str) {
        self.failed_reason_total.with_label_values(&[reason]).inc();
    }

    pub fn inc_report(&self, result: &str) {
        self.report_total.with_label_values(&[result]).inc();
    }
}

impl Debug for WorkerTransferMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerTransferMetrics")
    }
}

pub fn transfer_kind_label(source_path: &str) -> &'static str {
    match Path::from_str(source_path) {
        Ok(source) if source.is_cv() => "export",
        _ => "load",
    }
}

pub fn job_task_state_name(state: JobTaskState) -> &'static str {
    match state {
        JobTaskState::Completed => "completed",
        JobTaskState::Failed => "failed",
        JobTaskState::Canceled => "canceled",
        JobTaskState::PartialSuccess => "partial_success",
        JobTaskState::Loading => "loading",
        JobTaskState::Pending => "pending",
        JobTaskState::UNKNOWN => "unknown",
    }
}

pub fn record_task_outcome(
    source_path: &str,
    state: JobTaskState,
    duration_us: u128,
    read_us: u128,
    write_us: u128,
    copied_bytes: i64,
    failed_reason: Option<&str>,
) {
    if !state.is_finish() {
        return;
    }
    let kind = transfer_kind_label(source_path);
    let state = job_task_state_name(state);
    record_metric(|metrics| {
        metrics.observe_terminal(state, kind, duration_us, read_us, write_us, copied_bytes);
        if let Some(reason) = failed_reason {
            metrics.inc_failed_reason(reason);
        }
    });
}

/// Records the outcome of an awaited progress report RPC.
pub fn record_report_outcome(outcome: &FsResult<ReportOutcome>) {
    record_metric(|metrics| match outcome {
        Ok(ReportOutcome::Accepted) => metrics.inc_report("success"),
        Ok(ReportOutcome::Rejected) => metrics.inc_report("rejected"),
        Err(_) => metrics.inc_report("failure"),
    });
}

/// Records one reaped task context, by why it was evicted.
pub fn record_task_evicted(reason: &str) {
    record_metric(|metrics| metrics.inc_evicted(reason));
}

pub fn record_metric(f: impl FnOnce(&WorkerTransferMetrics)) {
    if let Ok(metrics) = WorkerTransferMetrics::get() {
        f(metrics);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_core_error::err_box;
    use curvine_metrics::Metrics;

    fn report_count(result: &str) -> i64 {
        let metric =
            Metrics::get("worker_transfer_task_report_total").expect("report counter registered");
        let counter_vec = metric.try_into_counter_vec().expect("counter vec");
        counter_vec.with_label_values(&[result]).get()
    }

    fn terminal_count(state: &str, kind: &str) -> i64 {
        let metric = Metrics::get("worker_transfer_task_terminal_total")
            .expect("terminal counter registered");
        let counter_vec = metric.try_into_counter_vec().expect("counter vec");
        counter_vec.with_label_values(&[state, kind]).get()
    }

    fn failed_reason_count(reason: &str) -> i64 {
        let metric =
            Metrics::get("worker_transfer_task_failed_total").expect("failed counter registered");
        let counter_vec = metric.try_into_counter_vec().expect("counter vec");
        counter_vec.with_label_values(&[reason]).get()
    }

    #[test]
    fn worker_transfer_metrics_register_once() {
        let first = WorkerTransferMetrics::get().expect("register worker transfer metrics");
        let second = WorkerTransferMetrics::get().expect("reuse worker transfer metrics");
        assert!(std::ptr::eq(first, second));
    }

    #[test]
    fn record_report_outcome_separates_fencing_from_transport_failure() {
        WorkerTransferMetrics::get().expect("register worker transfer metrics");
        let before_success = report_count("success");
        let before_rejected = report_count("rejected");
        let before_failure = report_count("failure");

        record_report_outcome(&Ok(ReportOutcome::Accepted));
        assert_eq!(report_count("success"), before_success + 1);
        assert_eq!(report_count("rejected"), before_rejected);
        assert_eq!(report_count("failure"), before_failure);

        record_report_outcome(&Ok(ReportOutcome::Rejected));
        assert_eq!(report_count("rejected"), before_rejected + 1);
        assert_eq!(report_count("failure"), before_failure);

        record_report_outcome(&err_box!("Transfer task report timed out after 5000 ms"));
        assert_eq!(report_count("failure"), before_failure + 1);
    }

    #[test]
    fn record_task_outcome_skips_non_terminal_state() {
        WorkerTransferMetrics::get().expect("register worker transfer metrics");
        let before = terminal_count("loading", "load");

        record_task_outcome(
            "ufs://bucket/file",
            JobTaskState::Loading,
            1_000,
            600,
            400,
            1024,
            Some("report_failed"),
        );

        assert_eq!(terminal_count("loading", "load"), before);
        assert_eq!(failed_reason_count("report_failed"), 0);
    }

    #[test]
    fn record_task_outcome_records_terminal_failed_outcome() {
        WorkerTransferMetrics::get().expect("register worker transfer metrics");
        let before = terminal_count("failed", "load");
        let before_timeout = failed_reason_count("timeout");

        record_task_outcome(
            "ufs://bucket/file",
            JobTaskState::Failed,
            2_000,
            1_200,
            800,
            2048,
            Some("timeout"),
        );

        assert_eq!(terminal_count("failed", "load"), before + 1);
        assert_eq!(failed_reason_count("timeout"), before_timeout + 1);
    }
}
