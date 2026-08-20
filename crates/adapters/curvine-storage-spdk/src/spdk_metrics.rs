#![cfg(feature = "spdk")]

use curvine_core_error::CommonResult;
use curvine_metrics::{Counter, Gauge, Histogram, Metrics as m};
use log::warn;
use std::sync::OnceLock;
use std::time::Duration;

pub(crate) struct SpdkMetrics {
    qpair_acquire_cached: Counter,
    qpair_acquire_allocated: Counter,
    qpair_acquire_timeout: Counter,
    qpair_acquire_shutdown: Counter,
    qpair_acquire_alloc_failed: Counter,
    qpair_acquire_wait_us: Histogram,
    qpair_contention_total: Counter,
    qpair_active: Gauge,
    qpair_limit: Gauge,
    qpair_cached: Gauge,
    qpair_release_cached: Counter,
    qpair_release_freed_pool_full: Counter,
    qpair_shutdown_total: Counter,
}

static SPDK_METRICS: OnceLock<Option<SpdkMetrics>> = OnceLock::new();

impl SpdkMetrics {
    fn new() -> CommonResult<Self> {
        let qpair_acquire_total = m::new_counter_vec(
            "spdk_qpair_acquire_total",
            "Total SPDK qpair acquire attempts by result",
            &["result"],
        )?;
        let qpair_release_total = m::new_counter_vec(
            "spdk_qpair_release_total",
            "Total SPDK qpair releases by result",
            &["result"],
        )?;

        Ok(Self {
            qpair_acquire_cached: qpair_acquire_total.with_label_values(&["cached"]),
            qpair_acquire_allocated: qpair_acquire_total.with_label_values(&["allocated"]),
            qpair_acquire_timeout: qpair_acquire_total.with_label_values(&["timeout"]),
            qpair_acquire_shutdown: qpair_acquire_total.with_label_values(&["shutdown"]),
            qpair_acquire_alloc_failed: qpair_acquire_total.with_label_values(&["alloc_failed"]),
            qpair_acquire_wait_us: m::new_histogram_with_buckets(
                "spdk_qpair_acquire_wait_us",
                "Microseconds spent waiting for an SPDK qpair slot",
                &[
                    100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0,
                    5000000.0, 30000000.0,
                ],
            )?,
            qpair_contention_total: m::new_counter(
                "spdk_qpair_contention_total",
                "Number of SPDK qpair acquire attempts blocked by controller qpair capacity",
            )?,
            qpair_active: m::new_gauge(
                "spdk_qpair_active",
                "Current active SPDK qpair reservations across controllers",
            )?,
            qpair_limit: m::new_gauge(
                "spdk_qpair_limit",
                "Configured SPDK qpair reservation limit across controllers",
            )?,
            qpair_cached: m::new_gauge(
                "spdk_qpair_cached",
                "Current cached idle SPDK qpairs across controllers",
            )?,
            qpair_release_cached: qpair_release_total.with_label_values(&["cached"]),
            qpair_release_freed_pool_full: qpair_release_total
                .with_label_values(&["freed_pool_full"]),
            qpair_shutdown_total: m::new_counter(
                "spdk_qpair_shutdown_total",
                "Total SPDK qpair pool shutdown drains",
            )?,
        })
    }
}

fn with_metrics<F>(f: F)
where
    F: FnOnce(&SpdkMetrics),
{
    let metrics = SPDK_METRICS.get_or_init(|| match SpdkMetrics::new() {
        Ok(metrics) => Some(metrics),
        Err(err) => {
            warn!("failed to register SPDK metrics: {}", err);
            None
        }
    });
    if let Some(metrics) = metrics.as_ref() {
        f(metrics);
    }
}

pub(crate) fn record_qpair_acquire(result: &'static str) {
    with_metrics(|m| match result {
        "cached" => m.qpair_acquire_cached.inc(),
        "allocated" => m.qpair_acquire_allocated.inc(),
        "timeout" => m.qpair_acquire_timeout.inc(),
        "shutdown" => m.qpair_acquire_shutdown.inc(),
        "alloc_failed" => m.qpair_acquire_alloc_failed.inc(),
        _ => warn!("unknown spdk qpair acquire result label: {}", result),
    });
}

pub(crate) fn record_qpair_wait(duration: Duration) {
    with_metrics(|m| m.qpair_acquire_wait_us.observe(duration.as_micros() as f64));
}

pub(crate) fn record_qpair_contention() {
    with_metrics(|m| m.qpair_contention_total.inc());
}

pub(crate) fn set_qpair_active(value: usize) {
    with_metrics(|m| m.qpair_active.set(value as i64));
}

pub(crate) fn set_qpair_limit(value: usize) {
    with_metrics(|m| m.qpair_limit.set(value as i64));
}

pub(crate) fn set_qpair_cached(value: usize) {
    with_metrics(|m| m.qpair_cached.set(value as i64));
}

pub(crate) fn record_qpair_release(result: &'static str) {
    with_metrics(|m| match result {
        "cached" => m.qpair_release_cached.inc(),
        "freed_pool_full" => m.qpair_release_freed_pool_full.inc(),
        _ => warn!("unknown spdk qpair release result label: {}", result),
    });
}

pub(crate) fn record_qpair_shutdown() {
    with_metrics(|m| m.qpair_shutdown_total.inc());
}
