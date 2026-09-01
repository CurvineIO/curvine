//! Observability for the KV layer.
//!
//! Metrics follow the project convention (see `MasterMetrics`,
//! `curvine-client-core`): request counts, success/failure split, error kind,
//! latency histograms and in-flight concurrency. Labels are strictly
//! low-cardinality: `backend` (memory/fdb), `op` (get/put/...) and `kind`
//! (a bounded [`crate::kv::KvError::kind`] value). Keys, values and request
//! identifiers are NEVER used as labels.

use crate::kv::error::KvError;
use curvine_core_error::CommonResult;
use curvine_metrics::{CounterVec, Gauge, HistogramVec, Metrics};
use once_cell::sync::Lazy;
use std::time::Instant;

/// Operation label values. Kept as constants so call sites cannot introduce
/// unbounded label cardinality by typo.
pub mod op {
    pub const GET: &str = "get";
    pub const MULTI_GET: &str = "multi_get";
    pub const SNAPSHOT_GET: &str = "snapshot_get";
    pub const PUT: &str = "put";
    pub const DELETE: &str = "delete";
    pub const BATCH_DELETE: &str = "batch_delete";
    pub const CAS: &str = "compare_and_set";
    pub const BEGIN: &str = "begin";
    pub const COMMIT: &str = "commit";
}

/// Latency buckets in microseconds, from sub-millisecond point reads to
/// multi-hundred-millisecond contended commits.
const LATENCY_BUCKETS_US: &[f64] = &[
    50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0, 100_000.0,
    250_000.0,
];

/// Process-wide KV metric handles. Registered once on first access.
pub struct KvMetrics {
    /// Total operations attempted, by backend + op.
    pub ops_total: CounterVec,
    /// Operations that returned `Ok`, by backend + op.
    pub ops_ok_total: CounterVec,
    /// Operations that returned `Err`, by backend + op + error kind.
    pub ops_err_total: CounterVec,
    /// Operation latency (microseconds), by backend + op.
    pub op_latency_us: HistogramVec,
    /// Transaction commit conflicts, by backend.
    pub txn_conflicts_total: CounterVec,
    /// Transaction retries performed by `run_txn`, by backend.
    pub txn_retries_total: CounterVec,
    /// Currently in-flight transactions.
    pub txn_in_flight: Gauge,
}

impl KvMetrics {
    fn register() -> CommonResult<Self> {
        Ok(Self {
            ops_total: Metrics::new_counter_vec(
                "mds_kv_ops_total",
                "Total KV operations attempted",
                &["backend", "op"],
            )?,
            ops_ok_total: Metrics::new_counter_vec(
                "mds_kv_ops_ok_total",
                "KV operations that succeeded",
                &["backend", "op"],
            )?,
            ops_err_total: Metrics::new_counter_vec(
                "mds_kv_ops_err_total",
                "KV operations that failed",
                &["backend", "op", "kind"],
            )?,
            op_latency_us: Metrics::new_histogram_vec_with_buckets(
                "mds_kv_op_latency_us",
                "KV operation latency in microseconds",
                &["backend", "op"],
                LATENCY_BUCKETS_US,
            )?,
            txn_conflicts_total: Metrics::new_counter_vec(
                "mds_kv_txn_conflicts_total",
                "KV transaction commit conflicts",
                &["backend"],
            )?,
            txn_retries_total: Metrics::new_counter_vec(
                "mds_kv_txn_retries_total",
                "KV transaction retries performed by run_txn",
                &["backend"],
            )?,
            txn_in_flight: Metrics::new_gauge(
                "mds_kv_txn_in_flight",
                "Currently in-flight KV transactions",
            )?,
        })
    }

    /// Records the outcome and latency of a single operation.
    pub fn observe(&self, backend: &str, op: &str, start: Instant, result: &Result<(), KvError>) {
        self.ops_total.with_label_values(&[backend, op]).inc();
        self.op_latency_us
            .with_label_values(&[backend, op])
            .observe(start.elapsed().as_micros() as f64);
        match result {
            Ok(()) => self.ops_ok_total.with_label_values(&[backend, op]).inc(),
            Err(error) => {
                self.ops_err_total
                    .with_label_values(&[backend, op, error.kind()])
                    .inc();
                if matches!(error, KvError::Conflict) {
                    self.txn_conflicts_total.with_label_values(&[backend]).inc();
                }
            }
        }
    }

    /// Records a retry of a transaction closure.
    pub fn record_retry(&self, backend: &str) {
        self.txn_retries_total.with_label_values(&[backend]).inc();
    }
}

static KV_METRICS: Lazy<KvMetrics> =
    Lazy::new(|| KvMetrics::register().expect("failed to register KV metrics"));

/// Returns the process-wide KV metrics, registering them on first use.
pub fn metrics() -> &'static KvMetrics {
    &KV_METRICS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_updates_counters() {
        let m = metrics();
        let backend = "metrics_unit_test";
        let before = m.ops_total.with_label_values(&[backend, op::GET]).get();
        m.observe(backend, op::GET, Instant::now(), &Ok(()));
        let after = m.ops_total.with_label_values(&[backend, op::GET]).get();
        assert_eq!(after, before + 1);

        let before = m
            .ops_err_total
            .with_label_values(&[backend, op::COMMIT, "conflict"])
            .get();
        m.observe(backend, op::COMMIT, Instant::now(), &Err(KvError::Conflict));
        let after = m
            .ops_err_total
            .with_label_values(&[backend, op::COMMIT, "conflict"])
            .get();
        assert_eq!(after, before + 1);
    }
}
