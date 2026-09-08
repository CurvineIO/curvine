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

#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::future::join_all;
use futures::stream::{self, StreamExt};
use tokio::sync::Semaphore;

use super::model::{
    DashboardGroup, DashboardPanel, DashboardPayload, DashboardSeries, DashboardTableRow,
    DashboardWindow, PanelKind, RangePoint, RangeSeries, SeriesSummary, TargetHealth,
};
use super::service::ObservabilityService;

const US_TO_MS: f64 = 0.001;
const DEFAULT_MAX_PANEL_BUILD_CONCURRENCY: usize = 8;

#[derive(Clone, Copy)]
enum GaugeRatioMode {
    Used,
    Watermark,
}

/// Heavy metric families queried only with explicit tag whitelists (design doc Phase 4).
const ROCKSDB_METRIC: &str = "rocksdb_metrics";
const ROCKSDB_DB_SCOPE_TAGS: &[&str] = &[
    "db_rocksdb_block-cache-usage",
    "db_rocksdb_block-cache-pinned-usage",
    "db_rocksdb_size-all-mem-tables",
    "db_rocksdb_estimate-table-readers-mem",
];
const ROCKSDB_CF_PREFIXES: &[&str] = &["cf_default_", "cf_inodes_", "cf_location_"];

/// Master RPC / FS operation / journal flush counters are accumulated in microseconds
/// on curvine-server; dashboard panels display milliseconds.
pub fn default_step_for_range(range: &str) -> &'static str {
    match range {
        "1h" => "1m",
        "6h" => "5m",
        "24h" => "15m",
        "3d" => "1h",
        _ => "15s",
    }
}

/// Counter rate between adjacent points. Reset (negative delta) yields rate 0, never negative.
pub fn compute_counter_rate(points: &[RangePoint]) -> Vec<RangePoint> {
    if points.len() < 2 {
        return Vec::new();
    }
    let mut rates = Vec::with_capacity(points.len() - 1);
    for window in points.windows(2) {
        let prev = &window[0];
        let curr = &window[1];
        let delta_ms = curr.0.saturating_sub(prev.0);
        if delta_ms <= 0 {
            continue;
        }
        let delta_val = curr.1 - prev.1;
        let rate = if delta_val < 0.0 {
            0.0
        } else {
            delta_val / (delta_ms as f64 / 1000.0)
        };
        rates.push(RangePoint(curr.0, rate));
    }
    rates
}

pub fn compute_series_summary(points: &[RangePoint]) -> Option<SeriesSummary> {
    if points.is_empty() {
        return None;
    }
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut sum = 0.0;
    for point in points {
        min = min.min(point.1);
        max = max.max(point.1);
        sum += point.1;
    }
    let count = points.len() as f64;
    Some(SeriesSummary {
        current: points.last().map(|point| point.1),
        min: Some(min),
        max: Some(max),
        avg: Some(sum / count),
    })
}

#[derive(Clone)]
struct DashboardBuildCtx {
    service: Arc<ObservabilityService>,
    query_limiter: Arc<Semaphore>,
    panel_concurrency: usize,
    start_ms: i64,
    end_ms: i64,
    step_ms: Option<i64>,
}

async fn query_range(
    ctx: &DashboardBuildCtx,
    metric: Option<&str>,
    component: Option<&str>,
    instance: Option<&str>,
) -> Result<Vec<RangeSeries>, String> {
    query_range_with_step(ctx, metric, component, instance, ctx.step_ms).await
}

/// Query TSDB without dashboard step downsampling (used for histogram bucket deltas).
async fn query_range_raw(
    ctx: &DashboardBuildCtx,
    metric: Option<&str>,
    component: Option<&str>,
    instance: Option<&str>,
) -> Result<Vec<RangeSeries>, String> {
    query_range_with_step(ctx, metric, component, instance, None).await
}

async fn query_range_with_step(
    ctx: &DashboardBuildCtx,
    metric: Option<&str>,
    component: Option<&str>,
    instance: Option<&str>,
    step_ms: Option<i64>,
) -> Result<Vec<RangeSeries>, String> {
    let _permit = ctx
        .query_limiter
        .acquire()
        .await
        .map_err(|err| err.to_string())?;
    ctx.service
        .range(
            metric,
            component,
            instance,
            ctx.start_ms,
            ctx.end_ms,
            step_ms,
        )
        .await
}

fn merge_errors(errors: Vec<String>) -> Option<String> {
    if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
    }
}

fn degraded_panel(
    id: &str,
    title: &str,
    subtitle: &str,
    kind: PanelKind,
    section: Option<&str>,
    error: String,
) -> DashboardPanel {
    DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind,
        series: Vec::new(),
        rows: Vec::new(),
        section: section.map(str::to_string),
        error: Some(error),
    }
}

fn resolve_panel_result(
    id: &str,
    title: &str,
    subtitle: &str,
    kind: PanelKind,
    section: Option<&str>,
    result: Result<DashboardPanel, String>,
) -> DashboardPanel {
    match result {
        Ok(mut panel) => {
            if panel.section.is_none() {
                if let Some(section) = section {
                    panel = panel.with_section(section);
                }
            }
            panel
        }
        Err(err) => degraded_panel(id, title, subtitle, kind, section, err),
    }
}

fn panel_task<F, Fut>(
    ctx: DashboardBuildCtx,
    id: &'static str,
    title: &'static str,
    subtitle: &'static str,
    kind: PanelKind,
    section: Option<&'static str>,
    build: F,
) -> Pin<Box<dyn Future<Output = DashboardPanel> + Send>>
where
    F: FnOnce(DashboardBuildCtx) -> Fut + Send + 'static,
    Fut: Future<Output = Result<DashboardPanel, String>> + Send + 'static,
{
    Box::pin(
        async move { resolve_panel_result(id, title, subtitle, kind, section, build(ctx).await) },
    )
}

async fn collect_limited_panels(
    ctx: &DashboardBuildCtx,
    tasks: Vec<Pin<Box<dyn Future<Output = DashboardPanel> + Send>>>,
) -> Vec<DashboardPanel> {
    stream::iter(tasks)
        .buffer_unordered(ctx.panel_concurrency.max(1))
        .collect()
        .await
}

pub async fn build_dashboard(
    service: &Arc<ObservabilityService>,
    range: &str,
    step: &str,
    end_ms: i64,
    range_ms: i64,
) -> DashboardPayload {
    let start_ms = end_ms.saturating_sub(range_ms);
    let step_ms = super::model::parse_duration_ms(step).ok();
    let last_collect_ms = service.health().last_collect_ms;
    let query_limit = service
        .observability_conf()
        .max_concurrent_dashboard_queries
        .max(1);
    let panel_limit = query_limit.min(DEFAULT_MAX_PANEL_BUILD_CONCURRENCY);
    let ctx = DashboardBuildCtx {
        service: Arc::clone(service),
        query_limiter: Arc::new(Semaphore::new(query_limit)),
        panel_concurrency: panel_limit,
        start_ms,
        end_ms,
        step_ms,
    };

    let window = DashboardWindow {
        range: range.to_string(),
        step: step.to_string(),
        start_ms,
        end_ms,
        last_collect_ms,
    };

    DashboardPayload {
        window,
        cards: Vec::new(),
        groups: build_groups(&ctx).await,
    }
}

async fn build_groups(ctx: &DashboardBuildCtx) -> Vec<DashboardGroup> {
    let (overview_panels, master_panels, worker_panels, transfer_panels, fuse_panels) = tokio::join!(
        collect_limited_panels(ctx, build_overview_panel_tasks(ctx)),
        collect_limited_panels(ctx, build_master_panel_tasks(ctx)),
        collect_limited_panels(ctx, build_worker_panel_tasks(ctx)),
        collect_limited_panels(ctx, build_transfer_panel_tasks(ctx)),
        collect_limited_panels(ctx, build_fuse_panel_tasks(ctx)),
    );
    vec![
        DashboardGroup {
            id: "overview".to_string(),
            title: "Overview".to_string(),
            panels: overview_panels,
        },
        DashboardGroup {
            id: "master".to_string(),
            title: "Master".to_string(),
            panels: master_panels,
        },
        DashboardGroup {
            id: "workers".to_string(),
            title: "Workers".to_string(),
            panels: worker_panels,
        },
        DashboardGroup {
            id: "transfer".to_string(),
            title: "Transfer".to_string(),
            panels: transfer_panels,
        },
        DashboardGroup {
            id: "fuse".to_string(),
            title: "FUSE".to_string(),
            panels: fuse_panels,
        },
    ]
}

fn build_overview_panel_tasks(
    ctx: &DashboardBuildCtx,
) -> Vec<Pin<Box<dyn Future<Output = DashboardPanel> + Send>>> {
    vec![
        panel_task(
            ctx.clone(),
            "cluster-worker-capacity",
            "Worker Capacity",
            "Cluster sum of worker capacity, fs_used and available (bytes)",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                sum_worker_metrics_panel(
                    ctx,
                    "cluster-worker-capacity",
                    "Worker Capacity",
                    "Cluster sum of worker capacity, fs_used and available (bytes)",
                    &["capacity", "fs_used", "available"],
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-storage-watermark",
            "Storage Pool Watermark",
            "100 * (1 - sum(available) / sum(capacity))",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                aggregate_gauge_ratio_panel(
                    ctx,
                    "cluster-storage-watermark",
                    "Storage Pool Watermark",
                    "100 * (1 - sum(available) / sum(capacity))",
                    "available",
                    "capacity",
                    "worker",
                    GaugeRatioMode::Watermark,
                    "cluster",
                    "cluster",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-iops",
            "Cluster IOPS",
            "Sum of worker read_count and write_count rates",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                sum_counter_rate_panel(
                    ctx,
                    "cluster-iops",
                    "Cluster IOPS",
                    "Sum of worker read_count and write_count rates",
                    &["read_count", "write_count"],
                    "worker",
                    "cluster",
                    "cluster",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-risks",
            "Cluster Risks",
            "Lost workers, failed disks, stalled fs_dir and replication failures",
            PanelKind::Timeseries,
            Some("risks"),
            |ctx| {
                cluster_risk_panel(
                    ctx,
                    "cluster-risks",
                    "Cluster Risks",
                    "Lost workers, failed disks, stalled fs_dir and replication failures",
                )
            },
        ),
    ]
}

fn build_transfer_panel_tasks(
    ctx: &DashboardBuildCtx,
) -> Vec<Pin<Box<dyn Future<Output = DashboardPanel> + Send>>> {
    const TRANSFER: &str = "transfer";
    vec![
        // --- Status ---
        panel_task(
            ctx.clone(),
            "transfer-job-water",
            "Transfer Job Water",
            "active, executing and pending transfer jobs",
            PanelKind::Timeseries,
            Some("status"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "transfer-job-water",
                    "Transfer Job Water",
                    "active, executing and pending transfer jobs",
                    &[
                        "transfer_active_jobs",
                        "transfer_executing_jobs",
                        "transfer_pending_jobs",
                    ],
                    TRANSFER,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-report-queue",
            "Transfer Report Queue",
            "transfer_task_report_queue_len and queue length by lane",
            PanelKind::Timeseries,
            Some("status"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "transfer-report-queue",
                    "Transfer Report Queue",
                    "transfer_task_report_queue_len and queue length by lane",
                    &[
                        "transfer_task_report_queue_len",
                        "transfer_task_report_queue_len_by_lane",
                    ],
                    TRANSFER,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-store-unavailable",
            "Transfer Store Unavailable",
            "transfer_store_unavailable by backend",
            PanelKind::Timeseries,
            Some("status"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "transfer-store-unavailable",
                    "Transfer Store Unavailable",
                    "transfer_store_unavailable by backend",
                    &["transfer_store_unavailable"],
                    TRANSFER,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-snapshot-health",
            "Transfer Snapshot Health",
            "snapshot version, staleness and available worker counts",
            PanelKind::Timeseries,
            Some("status"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "transfer-snapshot-health",
                    "Transfer Snapshot Health",
                    "snapshot version, staleness and available worker counts",
                    &[
                        "transfer_cluster_snapshot_version",
                        "transfer_cluster_snapshot_staleness_ms",
                        "transfer_cluster_snapshot_live_workers",
                        "transfer_cluster_snapshot_capable_workers",
                    ],
                    TRANSFER,
                    false,
                )
            },
        ),
        // --- Flow ---
        panel_task(
            ctx.clone(),
            "transfer-job-flow-rate",
            "Transfer Job Flow",
            "submit, terminal and cleanup rates",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                multi_counter_rate_panel(
                    ctx,
                    "transfer-job-flow-rate",
                    "Transfer Job Flow",
                    "submit, terminal and cleanup rates",
                    &[
                        "transfer_submit_total",
                        "transfer_terminal_total",
                        "transfer_cleanup_purged_total",
                    ],
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-scheduler-rate",
            "Transfer Scheduler Events",
            "acquire, planning, dispatch, lease renew and stale retry rates",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                multi_counter_rate_panel(
                    ctx,
                    "transfer-scheduler-rate",
                    "Transfer Scheduler Events",
                    "acquire, planning, dispatch, lease renew and stale retry rates",
                    &[
                        "transfer_acquire_total",
                        "transfer_planning_total",
                        "transfer_dispatch_total",
                        "transfer_lease_renew_total",
                        "transfer_stale_retry_total",
                    ],
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-report-rate",
            "Transfer Task Reports",
            "transfer_task_report_total rate by result",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-report-rate",
                    "Transfer Task Reports",
                    "transfer_task_report_total rate by result",
                    "transfer_task_report_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-completed-bytes-rate",
            "Transfer Completed Bytes",
            "transfer_completed_bytes_total rate by state",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-completed-bytes-rate",
                    "Transfer Completed Bytes",
                    "transfer_completed_bytes_total rate by state",
                    "transfer_completed_bytes_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-anomaly-rate",
            "Transfer Exception Signals",
            "failed terminal, rejected task report and failed planning rates",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                transfer_anomaly_panel(
                    ctx,
                    "transfer-anomaly-rate",
                    "Transfer Exception Signals",
                    "failed terminal, rejected task report and failed planning rates",
                )
            },
        ),
        // --- Store & metadata ---
        panel_task(
            ctx.clone(),
            "transfer-store-latency-p99",
            "Transfer Store P99",
            "transfer_store_operation_duration_us P99 by backend and operation",
            PanelKind::Timeseries,
            Some("store"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "transfer-store-latency-p99",
                    "Transfer Store P99",
                    "transfer_store_operation_duration_us P99 by backend and operation",
                    "transfer_store_operation_duration_us_bucket",
                    TRANSFER,
                    0.99,
                    "transfer_store_p99_us",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-store-op-rate",
            "Transfer Store Operations",
            "transfer_store_operation_total rate",
            PanelKind::Timeseries,
            Some("store"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-store-op-rate",
                    "Transfer Store Operations",
                    "transfer_store_operation_total rate",
                    "transfer_store_operation_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-store-unavailable-rate",
            "Transfer Store Unavailable Events",
            "transfer_store_unavailable_total rate by backend and operation",
            PanelKind::Timeseries,
            Some("store"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-store-unavailable-rate",
                    "Transfer Store Unavailable Events",
                    "transfer_store_unavailable_total rate by backend and operation",
                    "transfer_store_unavailable_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-metadata-latency-p99",
            "Transfer Metadata P99",
            "transfer_metadata_operation_duration_us P99 by source and operation",
            PanelKind::Timeseries,
            Some("store"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "transfer-metadata-latency-p99",
                    "Transfer Metadata P99",
                    "transfer_metadata_operation_duration_us P99 by source and operation",
                    "transfer_metadata_operation_duration_us_bucket",
                    TRANSFER,
                    0.99,
                    "transfer_metadata_p99_us",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-metadata-op-rate",
            "Transfer Metadata Operations",
            "transfer_metadata_operation_total rate",
            PanelKind::Timeseries,
            Some("store"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-metadata-op-rate",
                    "Transfer Metadata Operations",
                    "transfer_metadata_operation_total rate",
                    "transfer_metadata_operation_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-store-unavailable-duration-rate",
            "Transfer Store Unavailable Duration",
            "transfer_store_unavailable_duration_us_total rate by backend",
            PanelKind::Timeseries,
            Some("store"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-store-unavailable-duration-rate",
                    "Transfer Store Unavailable Duration",
                    "transfer_store_unavailable_duration_us_total rate by backend",
                    "transfer_store_unavailable_duration_us_total",
                    TRANSFER,
                )
            },
        ),
        // --- Snapshot ---
        panel_task(
            ctx.clone(),
            "transfer-snapshot-refresh-rate",
            "Transfer Snapshot Refresh",
            "transfer_cluster_snapshot_refresh_total rate by result",
            PanelKind::Timeseries,
            Some("snapshot"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-snapshot-refresh-rate",
                    "Transfer Snapshot Refresh",
                    "transfer_cluster_snapshot_refresh_total rate by result",
                    "transfer_cluster_snapshot_refresh_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-snapshot-refresh-p99",
            "Transfer Snapshot Refresh P99",
            "transfer_cluster_snapshot_refresh_duration_us P99 by result",
            PanelKind::Timeseries,
            Some("snapshot"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "transfer-snapshot-refresh-p99",
                    "Transfer Snapshot Refresh P99",
                    "transfer_cluster_snapshot_refresh_duration_us P99 by result",
                    "transfer_cluster_snapshot_refresh_duration_us_bucket",
                    TRANSFER,
                    0.99,
                    "transfer_snapshot_refresh_p99_us",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-job-summary-water",
            "Transfer Job Summary Queue",
            "transfer_job_summary_dirty_len waiting for progress summary flush",
            PanelKind::Timeseries,
            Some("status"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "transfer-job-summary-water",
                    "Transfer Job Summary Queue",
                    "transfer_job_summary_dirty_len waiting for progress summary flush",
                    &["transfer_job_summary_dirty_len"],
                    TRANSFER,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-job-summary-flush-rate",
            "Transfer Job Summary Flush",
            "transfer_job_summary_flush_total rate by result",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-job-summary-flush-rate",
                    "Transfer Job Summary Flush",
                    "transfer_job_summary_flush_total rate by result",
                    "transfer_job_summary_flush_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-job-summary-flush-p99",
            "Transfer Job Summary Flush P99",
            "transfer_job_summary_flush_duration_us P99 by result",
            PanelKind::Timeseries,
            Some("flow"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "transfer-job-summary-flush-p99",
                    "Transfer Job Summary Flush P99",
                    "transfer_job_summary_flush_duration_us P99 by result",
                    "transfer_job_summary_flush_duration_us_bucket",
                    TRANSFER,
                    0.99,
                    "transfer_job_summary_flush_p99_us",
                    1.0,
                )
            },
        ),
        // --- Process ---
        panel_task(
            ctx.clone(),
            "transfer-process-cpu",
            "Transfer Process CPU",
            "process_cpu_seconds_total rate (cores)",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "transfer-process-cpu",
                    "Transfer Process CPU",
                    "process_cpu_seconds_total rate (cores)",
                    "process_cpu_seconds_total",
                    TRANSFER,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-process-memory",
            "Transfer Process Memory",
            "process_resident_memory_bytes",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "transfer-process-memory",
                    "Transfer Process Memory",
                    "process_resident_memory_bytes",
                    &["process_resident_memory_bytes"],
                    TRANSFER,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "transfer-process-fd",
            "Transfer Process FD Usage",
            "100 * process_open_fds / process_max_fds",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                gauge_ratio_panel(
                    ctx,
                    "transfer-process-fd",
                    "Transfer Process FD Usage",
                    "100 * process_open_fds / process_max_fds",
                    "process_open_fds",
                    "process_max_fds",
                    TRANSFER,
                    GaugeRatioMode::Used,
                    "transfer_fd_usage_pct",
                )
            },
        ),
    ]
}

fn build_master_panel_tasks(
    ctx: &DashboardBuildCtx,
) -> Vec<Pin<Box<dyn Future<Output = DashboardPanel> + Send>>> {
    vec![
        panel_task(
            ctx.clone(),
            "cluster-capacity",
            "Cluster Capacity",
            "capacity, available and fs_used (bytes)",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "cluster-capacity",
                    "Cluster Capacity",
                    "capacity, available and fs_used (bytes)",
                    &["capacity", "fs_used", "available"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-memory",
            "Master Memory",
            "Master used_memory_bytes",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "master-memory",
                    "Master Memory",
                    "Master used_memory_bytes",
                    &["used_memory_bytes"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "metadata-scale",
            "Dir & File Count",
            "inode_dir_num and inode_file_num",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "metadata-scale",
                    "Dir & File Count",
                    "inode_dir_num and inode_file_num",
                    &["inode_file_num", "inode_dir_num"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-process-cpu",
            "Master Process CPU",
            "process_cpu_seconds_total rate (cores)",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "master-process-cpu",
                    "Master Process CPU",
                    "process_cpu_seconds_total rate (cores)",
                    "process_cpu_seconds_total",
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-process-memory",
            "Master Process Memory",
            "process_resident_memory_bytes",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "master-process-memory",
                    "Master Process Memory",
                    "process_resident_memory_bytes",
                    &["process_resident_memory_bytes"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-active-connections",
            "Master Active Connections",
            "master_active_connections",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "master-active-connections",
                    "Master Active Connections",
                    "master_active_connections",
                    &["master_active_connections"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-process-fd",
            "Master Process FD Usage",
            "100 * process_open_fds / process_max_fds",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                gauge_ratio_panel(
                    ctx,
                    "master-process-fd",
                    "Master Process FD Usage",
                    "100 * process_open_fds / process_max_fds",
                    "process_open_fds",
                    "process_max_fds",
                    "master",
                    GaugeRatioMode::Used,
                    "master_fd_usage_pct",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-state-counts",
            "Worker Count",
            "worker_num by state",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-state-counts",
                    "Worker Count",
                    "worker_num by state",
                    &["worker_num"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-read-throughput",
            "Read Throughput",
            "Sum of worker read_bytes rate (bytes per second)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                sum_worker_rate_panel(
                    ctx,
                    "cluster-read-throughput",
                    "Read Throughput",
                    "Sum of worker read_bytes rate (bytes per second)",
                    "read_bytes",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-write-throughput",
            "Write Throughput",
            "Sum of worker write_bytes rate (bytes per second)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                sum_worker_rate_panel(
                    ctx,
                    "cluster-write-throughput",
                    "Write Throughput",
                    "Sum of worker write_bytes rate (bytes per second)",
                    "write_bytes",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-rpc-latency",
            "RPC Avg Latency",
            "rpc_request_time / rpc_request_total_count (ms)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "master-rpc-latency",
                    "RPC Avg Latency",
                    "rpc_request_time / rpc_request_total_count (ms)",
                    "rpc_request_time",
                    "rpc_request_total_count",
                    "master",
                    "rpc_avg_latency_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-rpc-qps",
            "Master RPC QPS",
            "rpc_request_total_count rate",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "master-rpc-qps",
                    "Master RPC QPS",
                    "rpc_request_total_count rate",
                    "rpc_request_total_count",
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "operation-rate",
            "FS Operation QPS",
            "operation_duration_count rate by operation",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "operation-rate",
                    "FS Operation QPS",
                    "operation_duration_count rate by operation",
                    "operation_duration_count",
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "operation-latency-p99",
            "FS Operation P99 Latency",
            "operation_duration_bucket P99 by operation (ms)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "operation-latency-p99",
                    "FS Operation P99 Latency",
                    "operation_duration_bucket P99 by operation (ms)",
                    "operation_duration_bucket",
                    "master",
                    0.99,
                    "operation_p99_latency_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "operation-latency",
            "FS Operation Avg Latency",
            "operation_duration_sum / operation_duration_count (ms)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "operation-latency",
                    "FS Operation Avg Latency",
                    "operation_duration_sum / operation_duration_count (ms)",
                    "operation_duration_sum",
                    "operation_duration_count",
                    "master",
                    "operation_avg_latency_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "journal-queue",
            "Journal Queue Length",
            "Master journal_queue_len",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "journal-queue",
                    "Journal Queue Length",
                    "Master journal_queue_len",
                    &["journal_queue_len"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "journal-term",
            "Journal Term",
            "journal_term",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "journal-term",
                    "Journal Term",
                    "journal_term",
                    &["journal_term"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "journal-lag",
            "Journal Lag",
            "journal_committed minus journal_applied and journal_ufs_applied",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                gauge_difference_multi_panel(
                    ctx,
                    "journal-lag",
                    "Journal Lag",
                    "journal_committed minus journal_applied and journal_ufs_applied",
                    "journal_committed",
                    &["journal_applied", "journal_ufs_applied"],
                    "master",
                    "journal_lag",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "replication-inflight",
            "Replication Inflight",
            "replication_inflight_number",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "replication-inflight",
                    "Replication Inflight",
                    "replication_inflight_number",
                    &["replication_inflight_number"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "replication-staging",
            "Replication Staging",
            "replication_staging_number",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "replication-staging",
                    "Replication Staging",
                    "replication_staging_number",
                    &["replication_staging_number"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "replication-failure-rate",
            "Replication Failures",
            "replication_failure_count rate",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "replication-failure-rate",
                    "Replication Failures",
                    "replication_failure_count rate",
                    "replication_failure_count",
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fs-dir-stalled",
            "FS Dir Stalled",
            "fs_dir_stalled (1=stalled, 0=healthy)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fs-dir-stalled",
                    "FS Dir Stalled",
                    "fs_dir_stalled (1=stalled, 0=healthy)",
                    &["fs_dir_stalled"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fs-dir-probe-acquire",
            "FS Dir Probe Acquire",
            "fs_dir_probe_acquire_us",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fs-dir-probe-acquire",
                    "FS Dir Probe Acquire",
                    "fs_dir_probe_acquire_us",
                    &["fs_dir_probe_acquire_us"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "journal-flush-latency",
            "Journal Flush Avg Time",
            "journal_flush_time / journal_flush_count (ms)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "journal-flush-latency",
                    "Journal Flush Avg Time",
                    "journal_flush_time / journal_flush_count (ms)",
                    "journal_flush_time",
                    "journal_flush_count",
                    "master",
                    "journal_flush_latency_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "namespace-scale",
            "Namespace Scale",
            "inode_dir_num, inode_file_num and num_blocks",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "namespace-scale",
                    "Namespace Scale",
                    "inode_dir_num, inode_file_num and num_blocks",
                    &["inode_dir_num", "inode_file_num", "num_blocks"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "maintenance-signals",
            "Maintenance Signals",
            "replication staging/inflight, eviction LRU and TTL inodes",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "maintenance-signals",
                    "Maintenance Signals",
                    "replication staging/inflight, eviction LRU and TTL inodes",
                    &[
                        "replication_staging_number",
                        "replication_inflight_number",
                        "eviction_lru_cache_size",
                        "ttl_total_inodes",
                    ],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "eviction-events-rate",
            "Eviction Events",
            "eviction_trigger_count and eviction_files_deleted rates",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                multi_counter_rate_panel(
                    ctx,
                    "eviction-events-rate",
                    "Eviction Events",
                    "eviction_trigger_count and eviction_files_deleted rates",
                    &["eviction_trigger_count", "eviction_files_deleted"],
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "eviction-bytes-rate",
            "Eviction Freed Bytes",
            "eviction_bytes_freed rate (bytes per second)",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "eviction-bytes-rate",
                    "Eviction Freed Bytes",
                    "eviction_bytes_freed rate (bytes per second)",
                    "eviction_bytes_freed",
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "ttl-status",
            "TTL Status",
            "ttl_bucket_len and ttl_total_inodes",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "ttl-status",
                    "TTL Status",
                    "ttl_bucket_len and ttl_total_inodes",
                    &["ttl_bucket_len", "ttl_total_inodes"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "ttl-skipped-rate",
            "TTL Skipped Inodes",
            "ttl_skipped_inodes rate",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "ttl-skipped-rate",
                    "TTL Skipped Inodes",
                    "ttl_skipped_inodes rate",
                    "ttl_skipped_inodes",
                    "master",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-blocks",
            "Block Count",
            "Master num_blocks",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "master-blocks",
                    "Block Count",
                    "Master num_blocks",
                    &["num_blocks"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-blocks-to-delete",
            "Blocks To Delete",
            "Cluster sum of worker num_blocks_to_delete",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                sum_worker_metrics_panel(
                    ctx,
                    "cluster-blocks-to-delete",
                    "Blocks To Delete",
                    "Cluster sum of worker num_blocks_to_delete",
                    &["num_blocks_to_delete"],
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "master-block-size",
            "Avg Block Size",
            "Master blocks_size_avg",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "master-block-size",
                    "Avg Block Size",
                    "Master blocks_size_avg",
                    &["blocks_size_avg"],
                    "master",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "cluster-disks",
            "Cluster Disks",
            "Cluster sum of worker total_disks and failed_disks",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                sum_worker_metrics_panel(
                    ctx,
                    "cluster-disks",
                    "Cluster Disks",
                    "Cluster sum of worker total_disks and failed_disks",
                    &["total_disks", "failed_disks"],
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "rocksdb-memory",
            "RocksDB Memory",
            "Whitelisted RocksDB memory gauges",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "rocksdb-memory",
                    "RocksDB Memory",
                    "Whitelisted RocksDB memory gauges",
                    &[ROCKSDB_METRIC],
                    "master",
                    true,
                )
            },
        ),
    ]
}

fn build_worker_panel_tasks(
    ctx: &DashboardBuildCtx,
) -> Vec<Pin<Box<dyn Future<Output = DashboardPanel> + Send>>> {
    vec![
        panel_task(
            ctx.clone(),
            "worker-storage",
            "Storage",
            "Per-worker capacity, fs_used and available (bytes)",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-storage",
                    "Storage",
                    "Per-worker capacity, fs_used and available (bytes)",
                    &["capacity", "fs_used", "available"],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-memory",
            "Memory Usage",
            "Per-worker used_memory_bytes",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-memory",
                    "Memory Usage",
                    "Per-worker used_memory_bytes",
                    &["used_memory_bytes"],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-process-cpu",
            "Worker Process CPU",
            "process_cpu_seconds_total rate (cores)",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-process-cpu",
                    "Worker Process CPU",
                    "process_cpu_seconds_total rate (cores)",
                    "process_cpu_seconds_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-process-memory",
            "Worker Process Memory",
            "process_resident_memory_bytes",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-process-memory",
                    "Worker Process Memory",
                    "process_resident_memory_bytes",
                    &["process_resident_memory_bytes"],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-process-fd",
            "Worker Process FD Usage",
            "100 * process_open_fds / process_max_fds",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                gauge_ratio_panel(
                    ctx,
                    "worker-process-fd",
                    "Worker Process FD Usage",
                    "100 * process_open_fds / process_max_fds",
                    "process_open_fds",
                    "process_max_fds",
                    "worker",
                    GaugeRatioMode::Used,
                    "worker_fd_usage_pct",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-storage-watermark",
            "Storage Watermark",
            "100 * (1 - available / capacity) by worker",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                gauge_ratio_panel(
                    ctx,
                    "worker-storage-watermark",
                    "Storage Watermark",
                    "100 * (1 - available / capacity) by worker",
                    "available",
                    "capacity",
                    "worker",
                    GaugeRatioMode::Watermark,
                    "worker_storage_watermark_pct",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-storage-used-pct",
            "Storage Used",
            "100 * fs_used / capacity by worker",
            PanelKind::Timeseries,
            Some("capacity"),
            |ctx| {
                gauge_ratio_panel(
                    ctx,
                    "worker-storage-used-pct",
                    "Storage Used",
                    "100 * fs_used / capacity by worker",
                    "fs_used",
                    "capacity",
                    "worker",
                    GaugeRatioMode::Used,
                    "worker_storage_used_pct",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-disks",
            "Disks",
            "total_disks and failed_disks by worker",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-disks",
                    "Disks",
                    "total_disks and failed_disks by worker",
                    &["total_disks", "failed_disks"],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-blocks",
            "Blocks",
            "num_blocks and num_blocks_to_delete by worker",
            PanelKind::Timeseries,
            Some("trends"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-blocks",
                    "Blocks",
                    "num_blocks and num_blocks_to_delete by worker",
                    &["num_blocks", "num_blocks_to_delete"],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-read-throughput",
            "Read Throughput",
            "Per-worker read throughput (bytes per second)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                rate_panel(
                    ctx,
                    "worker-read-throughput",
                    "Read Throughput",
                    "Per-worker read throughput (bytes per second)",
                    "read_bytes",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-write-throughput",
            "Write Throughput",
            "Per-worker write throughput (bytes per second)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                rate_panel(
                    ctx,
                    "worker-write-throughput",
                    "Write Throughput",
                    "Per-worker write throughput (bytes per second)",
                    "write_bytes",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-read-iops",
            "Read IOPS",
            "read_count rate (data read operations per second)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-read-iops",
                    "Read IOPS",
                    "read_count rate (data read operations per second)",
                    "read_count",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-write-iops",
            "Write IOPS",
            "write_count rate (data write operations per second)",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-write-iops",
                    "Write IOPS",
                    "write_count rate (data write operations per second)",
                    "write_count",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-write-latency",
            "Avg Write Time",
            "write_time_us / write_count",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "worker-write-latency",
                    "Avg Write Time",
                    "write_time_us / write_count",
                    "write_time_us",
                    "write_count",
                    "worker",
                    "write_avg_latency_us",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-read-latency",
            "Avg Read Time",
            "read_time_us / read_count",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "worker-read-latency",
                    "Avg Read Time",
                    "read_time_us / read_count",
                    "read_time_us",
                    "read_count",
                    "worker",
                    "read_avg_latency_us",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-write-block-rate",
            "Write Block Open Rate",
            "write_blocks open rate by local/remote path",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-write-block-rate",
                    "Write Block Open Rate",
                    "write_blocks open rate by local/remote path",
                    "write_blocks",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-read-block-rate",
            "Read Block Open Rate",
            "read_blocks open rate by local/remote path",
            PanelKind::Timeseries,
            Some("performance"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-read-block-rate",
                    "Read Block Open Rate",
                    "read_blocks open rate by local/remote path",
                    "read_blocks",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-task-water",
            "Transfer Task Water",
            "active, queued and running transfer tasks on each worker",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-transfer-task-water",
                    "Transfer Task Water",
                    "active, queued and running transfer tasks on each worker",
                    &[
                        "worker_transfer_tasks_active",
                        "worker_transfer_tasks_queued",
                        "worker_transfer_tasks_running",
                    ],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-concurrency",
            "Transfer Task Concurrency",
            "available and max transfer task permits per worker",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "worker-transfer-concurrency",
                    "Transfer Task Concurrency",
                    "available and max transfer task permits per worker",
                    &[
                        "worker_transfer_task_permits_available",
                        "worker_transfer_task_permits_max",
                    ],
                    "worker",
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-submit-rate",
            "Transfer Task Submit",
            "worker_transfer_task_submit_total rate by result",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-transfer-submit-rate",
                    "Transfer Task Submit",
                    "worker_transfer_task_submit_total rate by result",
                    "worker_transfer_task_submit_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-submit-rejected-rate",
            "Transfer Task Submit Rejected",
            "worker_transfer_task_submit_rejected_total rate by reason",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-transfer-submit-rejected-rate",
                    "Transfer Task Submit Rejected",
                    "worker_transfer_task_submit_rejected_total rate by reason",
                    "worker_transfer_task_submit_rejected_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-terminal-rate",
            "Transfer Task Terminal",
            "worker_transfer_task_terminal_total rate by state and kind",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-transfer-terminal-rate",
                    "Transfer Task Terminal",
                    "worker_transfer_task_terminal_total rate by state and kind",
                    "worker_transfer_task_terminal_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-failed-rate",
            "Transfer Task Failures",
            "worker_transfer_task_failed_total rate by reason",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-transfer-failed-rate",
                    "Transfer Task Failures",
                    "worker_transfer_task_failed_total rate by reason",
                    "worker_transfer_task_failed_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-report-rate",
            "Transfer Task Progress Reports",
            "worker_transfer_task_report_total rate by result",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-transfer-report-rate",
                    "Transfer Task Progress Reports",
                    "worker_transfer_task_report_total rate by result",
                    "worker_transfer_task_report_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-copy-bytes-rate",
            "Transfer Task Copy Bytes",
            "worker_transfer_task_copy_bytes_total rate by state",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "worker-transfer-copy-bytes-rate",
                    "Transfer Task Copy Bytes",
                    "worker_transfer_task_copy_bytes_total rate by state",
                    "worker_transfer_task_copy_bytes_total",
                    "worker",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "worker-transfer-execution-p99",
            "Transfer Task Execution P99",
            "worker_transfer_task_execution_duration_us P99 by state",
            PanelKind::Timeseries,
            Some("transfer-tasks"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "worker-transfer-execution-p99",
                    "Transfer Task Execution P99",
                    "worker_transfer_task_execution_duration_us P99 by state",
                    "worker_transfer_task_execution_duration_us_bucket",
                    "worker",
                    0.99,
                    "worker_transfer_execution_p99_us",
                    1.0,
                )
            },
        ),
    ]
}

fn build_fuse_panel_tasks(
    ctx: &DashboardBuildCtx,
) -> Vec<Pin<Box<dyn Future<Output = DashboardPanel> + Send>>> {
    const FUSE: &str = "fuse";
    vec![
        // --- Runtime ---
        panel_task(
            ctx.clone(),
            "fuse-runtime-handles",
            "Handles & Inodes",
            "inode_num, file_handle_num and dir_handle_num",
            PanelKind::Timeseries,
            Some("runtime"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-runtime-handles",
                    "Handles & Inodes",
                    "inode_num, file_handle_num and dir_handle_num",
                    &["inode_num", "file_handle_num", "dir_handle_num"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-memory",
            "Memory Usage",
            "fuse_used_memory_bytes and write-back cache usage",
            PanelKind::Timeseries,
            Some("runtime"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-memory",
                    "Memory Usage",
                    "fuse_used_memory_bytes and write-back cache usage",
                    &[
                        "fuse_used_memory_bytes",
                        "write_back_cache_usage",
                        "write_back_cache_limit",
                    ],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-process-cpu",
            "FUSE Process CPU",
            "process_cpu_seconds_total rate (cores)",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-process-cpu",
                    "FUSE Process CPU",
                    "process_cpu_seconds_total rate (cores)",
                    "process_cpu_seconds_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-process-memory",
            "FUSE Process Memory",
            "process_resident_memory_bytes",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-process-memory",
                    "FUSE Process Memory",
                    "process_resident_memory_bytes",
                    &["process_resident_memory_bytes"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-process-fd",
            "FUSE Process FD Usage",
            "100 * process_open_fds / process_max_fds",
            PanelKind::Timeseries,
            Some("process"),
            |ctx| {
                gauge_ratio_panel(
                    ctx,
                    "fuse-process-fd",
                    "FUSE Process FD Usage",
                    "100 * process_open_fds / process_max_fds",
                    "process_open_fds",
                    "process_max_fds",
                    FUSE,
                    GaugeRatioMode::Used,
                    "fuse_fd_usage_pct",
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-writeback-inodes",
            "Write-back Cache Inodes",
            "write_back_cache_inode_num",
            PanelKind::Timeseries,
            Some("runtime"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-writeback-inodes",
                    "Write-back Cache Inodes",
                    "write_back_cache_inode_num",
                    &["write_back_cache_inode_num"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-kernel-fd-health",
            "Kernel FD Health",
            "curvine_fuse_kernel_fd_health (1=healthy, 0=degraded)",
            PanelKind::Timeseries,
            Some("runtime"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-kernel-fd-health",
                    "Kernel FD Health",
                    "curvine_fuse_kernel_fd_health (1=healthy, 0=degraded)",
                    &["curvine_fuse_kernel_fd_health"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-scrape-bytes",
            "Metrics Scrape Size",
            "curvine_fuse_metrics_scrape_bytes",
            PanelKind::Timeseries,
            Some("runtime"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-scrape-bytes",
                    "Metrics Scrape Size",
                    "curvine_fuse_metrics_scrape_bytes",
                    &["curvine_fuse_metrics_scrape_bytes"],
                    FUSE,
                    false,
                )
            },
        ),
        // --- IO ---
        panel_task(
            ctx.clone(),
            "fuse-io-bytes-rate",
            "IO Throughput",
            "curvine_fuse_io_bytes_total rate by io_type and path_type",
            PanelKind::Timeseries,
            Some("io"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-io-bytes-rate",
                    "IO Throughput",
                    "curvine_fuse_io_bytes_total rate by io_type and path_type",
                    "curvine_fuse_io_bytes_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-io-requests-rate",
            "IO Request Rate",
            "curvine_fuse_io_requests_total rate by io_type and path_type",
            PanelKind::Timeseries,
            Some("io"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-io-requests-rate",
                    "IO Request Rate",
                    "curvine_fuse_io_requests_total rate by io_type and path_type",
                    "curvine_fuse_io_requests_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-io-size",
            "IO Request Size P99",
            "curvine_fuse_io_size_bytes P99 by io_type and path_type",
            PanelKind::Timeseries,
            Some("io"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-io-size",
                    "IO Request Size P99",
                    "curvine_fuse_io_size_bytes P99 by io_type and path_type",
                    "curvine_fuse_io_size_bytes_bucket",
                    FUSE,
                    0.99,
                    "fuse_io_size_p99",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-response-bytes-rate",
            "Response Bytes Rate",
            "curvine_fuse_response_bytes_total rate by opcode",
            PanelKind::Timeseries,
            Some("io"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-response-bytes-rate",
                    "Response Bytes Rate",
                    "curvine_fuse_response_bytes_total rate by opcode",
                    "curvine_fuse_response_bytes_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-stream-lifecycle-qps",
            "Stream Lifecycle QPS",
            "curvine_fuse_stream_lifecycle_requests_total rate (flush/fsync/release)",
            PanelKind::Timeseries,
            Some("io"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-stream-lifecycle-qps",
                    "Stream Lifecycle QPS",
                    "curvine_fuse_stream_lifecycle_requests_total rate (flush/fsync/release)",
                    "curvine_fuse_stream_lifecycle_requests_total",
                    FUSE,
                )
            },
        ),
        // --- Latency ---
        panel_task(
            ctx.clone(),
            "fuse-request-qps",
            "Request QPS",
            "curvine_fuse_requests_total rate",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-request-qps",
                    "Request QPS",
                    "curvine_fuse_requests_total rate",
                    "curvine_fuse_requests_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-request-p99",
            "Request P99 Latency",
            "curvine_fuse_request_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-request-p99",
                    "Request P99 Latency",
                    "curvine_fuse_request_duration_us P99 (ms)",
                    "curvine_fuse_request_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_request_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-request-avg",
            "Request Avg Latency",
            "curvine_fuse_request_duration_us sum/count (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "fuse-request-avg",
                    "Request Avg Latency",
                    "curvine_fuse_request_duration_us sum/count (ms)",
                    "curvine_fuse_request_duration_us_sum",
                    "curvine_fuse_request_duration_us_count",
                    FUSE,
                    "fuse_request_avg_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-operation-p99",
            "Metadata Operation P99",
            "curvine_fuse_operation_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-operation-p99",
                    "Metadata Operation P99",
                    "curvine_fuse_operation_duration_us P99 (ms)",
                    "curvine_fuse_operation_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_operation_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-io-p99",
            "IO P99 Latency",
            "curvine_fuse_io_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-io-p99",
                    "IO P99 Latency",
                    "curvine_fuse_io_duration_us P99 (ms)",
                    "curvine_fuse_io_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_io_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-io-dispatch-p99",
            "IO Dispatch P99",
            "curvine_fuse_io_dispatch_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-io-dispatch-p99",
                    "IO Dispatch P99",
                    "curvine_fuse_io_dispatch_duration_us P99 (ms)",
                    "curvine_fuse_io_dispatch_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_io_dispatch_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-stream-lifecycle-p99",
            "Stream Lifecycle P99",
            "curvine_fuse_stream_lifecycle_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-stream-lifecycle-p99",
                    "Stream Lifecycle P99",
                    "curvine_fuse_stream_lifecycle_duration_us P99 (ms)",
                    "curvine_fuse_stream_lifecycle_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_stream_lifecycle_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-readdir-p99",
            "Readdir P99 Latency",
            "curvine_fuse_readdir_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-readdir-p99",
                    "Readdir P99 Latency",
                    "curvine_fuse_readdir_duration_us P99 (ms)",
                    "curvine_fuse_readdir_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_readdir_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-readdir-entries",
            "Readdir Entries P99",
            "curvine_fuse_readdir_entries P99 (entries per batch)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-readdir-entries",
                    "Readdir Entries P99",
                    "curvine_fuse_readdir_entries P99 (entries per batch)",
                    "curvine_fuse_readdir_entries_bucket",
                    FUSE,
                    0.99,
                    "fuse_readdir_entries_p99",
                    1.0,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-response-write-p99",
            "Response Write P99",
            "curvine_fuse_response_write_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-response-write-p99",
                    "Response Write P99",
                    "curvine_fuse_response_write_duration_us P99 (ms)",
                    "curvine_fuse_response_write_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_response_write_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-setlkw-wait-p99",
            "SETLKW Wait P99",
            "curvine_fuse_setlkw_wait_duration_us P99 (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-setlkw-wait-p99",
                    "SETLKW Wait P99",
                    "curvine_fuse_setlkw_wait_duration_us P99 (ms)",
                    "curvine_fuse_setlkw_wait_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_setlkw_wait_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-stage-p99",
            "Stage P99 Latency",
            "curvine_fuse_stage_duration_us P99 by stage (ms)",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                histogram_quantile_panel(
                    ctx,
                    "fuse-stage-p99",
                    "Stage P99 Latency",
                    "curvine_fuse_stage_duration_us P99 by stage (ms)",
                    "curvine_fuse_stage_duration_us_bucket",
                    FUSE,
                    0.99,
                    "fuse_stage_p99_ms",
                    US_TO_MS,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-receive-loop-wait",
            "Receive Loop Wait Avg",
            "curvine_fuse_receive_loop_wait_duration_us avg (ms); saturation metric",
            PanelKind::Timeseries,
            Some("latency"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "fuse-receive-loop-wait",
                    "Receive Loop Wait Avg",
                    "curvine_fuse_receive_loop_wait_duration_us avg (ms); saturation metric",
                    "curvine_fuse_receive_loop_wait_duration_us_sum",
                    "curvine_fuse_receive_loop_wait_duration_us_count",
                    FUSE,
                    "fuse_receive_loop_wait_avg_ms",
                    US_TO_MS,
                )
            },
        ),
        // --- Queue ---
        panel_task(
            ctx.clone(),
            "fuse-active-requests",
            "Active Requests",
            "curvine_fuse_active_requests by kind",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-active-requests",
                    "Active Requests",
                    "curvine_fuse_active_requests by kind",
                    &["curvine_fuse_active_requests"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-reply-queue-depth",
            "Reply Queue Depth",
            "curvine_fuse_reply_queue_depth",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-reply-queue-depth",
                    "Reply Queue Depth",
                    "curvine_fuse_reply_queue_depth",
                    &["curvine_fuse_reply_queue_depth"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-stream-write-queue-depth",
            "Stream Write Queue Depth",
            "curvine_fuse_stream_write_queue_depth",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-stream-write-queue-depth",
                    "Stream Write Queue Depth",
                    "curvine_fuse_stream_write_queue_depth",
                    &["curvine_fuse_stream_write_queue_depth"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-meta-task-inflight",
            "Metadata Tasks In Flight",
            "curvine_fuse_meta_task_inflight",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-meta-task-inflight",
                    "Metadata Tasks In Flight",
                    "curvine_fuse_meta_task_inflight",
                    &["curvine_fuse_meta_task_inflight"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-stream-io-inflight",
            "Stream IO In Flight",
            "curvine_fuse_stream_io_inflight by io_type",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-stream-io-inflight",
                    "Stream IO In Flight",
                    "curvine_fuse_stream_io_inflight by io_type",
                    &["curvine_fuse_stream_io_inflight"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-stream-lifecycle-inflight",
            "Stream Lifecycle In Flight",
            "curvine_fuse_stream_lifecycle_inflight by io_type",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-stream-lifecycle-inflight",
                    "Stream Lifecycle In Flight",
                    "curvine_fuse_stream_lifecycle_inflight by io_type",
                    &["curvine_fuse_stream_lifecycle_inflight"],
                    FUSE,
                    false,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-setlkw-inflight",
            "SETLKW In Flight",
            "curvine_fuse_setlkw_inflight",
            PanelKind::Timeseries,
            Some("queue"),
            |ctx| {
                timeseries_panel(
                    ctx,
                    "fuse-setlkw-inflight",
                    "SETLKW In Flight",
                    "curvine_fuse_setlkw_inflight",
                    &["curvine_fuse_setlkw_inflight"],
                    FUSE,
                    false,
                )
            },
        ),
        // --- Cache ---
        panel_task(
            ctx.clone(),
            "fuse-user-meta-cache",
            "User Meta Cache",
            "curvine_fuse_user_meta_cache_total rate by cache and status",
            PanelKind::Timeseries,
            Some("cache"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-user-meta-cache",
                    "User Meta Cache",
                    "curvine_fuse_user_meta_cache_total rate by cache and status",
                    "curvine_fuse_user_meta_cache_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-meta-cache-invalidations",
            "Meta Cache Invalidations",
            "curvine_fuse_user_meta_cache_invalidations_total rate",
            PanelKind::Timeseries,
            Some("cache"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-meta-cache-invalidations",
                    "Meta Cache Invalidations",
                    "curvine_fuse_user_meta_cache_invalidations_total rate",
                    "curvine_fuse_user_meta_cache_invalidations_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-node-cache",
            "Node Cache Lookups",
            "curvine_fuse_node_cache_total rate by operation and status",
            PanelKind::Timeseries,
            Some("cache"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-node-cache",
                    "Node Cache Lookups",
                    "curvine_fuse_node_cache_total rate by operation and status",
                    "curvine_fuse_node_cache_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-negative-entries",
            "Negative Dentry Returns",
            "curvine_fuse_negative_entry_returned_total rate",
            PanelKind::Timeseries,
            Some("cache"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-negative-entries",
                    "Negative Dentry Returns",
                    "curvine_fuse_negative_entry_returned_total rate",
                    "curvine_fuse_negative_entry_returned_total",
                    FUSE,
                )
            },
        ),
        // --- Health ---
        panel_task(
            ctx.clone(),
            "fuse-error-rate",
            "Error Rate",
            "curvine_fuse_errors_total rate by errno",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-error-rate",
                    "Error Rate",
                    "curvine_fuse_errors_total rate by errno",
                    "curvine_fuse_errors_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-decode-errors",
            "Decode Errors",
            "curvine_fuse_decode_errors_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-decode-errors",
                    "Decode Errors",
                    "curvine_fuse_decode_errors_total rate",
                    "curvine_fuse_decode_errors_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-interrupted-rate",
            "Interrupted Requests",
            "curvine_fuse_interrupted_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-interrupted-rate",
                    "Interrupted Requests",
                    "curvine_fuse_interrupted_total rate",
                    "curvine_fuse_interrupted_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-unsupported-rate",
            "Unsupported Requests",
            "curvine_fuse_unsupported_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-unsupported-rate",
                    "Unsupported Requests",
                    "curvine_fuse_unsupported_total rate",
                    "curvine_fuse_unsupported_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-notify-events",
            "Kernel Notify Events",
            "curvine_fuse_notify_total rate by code and status",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-notify-events",
                    "Kernel Notify Events",
                    "curvine_fuse_notify_total rate by code and status",
                    "curvine_fuse_notify_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-receive-errors",
            "Receive Errors",
            "curvine_fuse_receive_errors_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-receive-errors",
                    "Receive Errors",
                    "curvine_fuse_receive_errors_total rate",
                    "curvine_fuse_receive_errors_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-reply-enqueue-errors",
            "Reply Enqueue Errors",
            "curvine_fuse_reply_enqueue_errors_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-reply-enqueue-errors",
                    "Reply Enqueue Errors",
                    "curvine_fuse_reply_enqueue_errors_total rate",
                    "curvine_fuse_reply_enqueue_errors_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-response-write-errors",
            "Response Write Errors",
            "curvine_fuse_response_write_errors_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-response-write-errors",
                    "Response Write Errors",
                    "curvine_fuse_response_write_errors_total rate",
                    "curvine_fuse_response_write_errors_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-session-init",
            "Session Init Events",
            "curvine_fuse_session_init_total rate",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-session-init",
                    "Session Init Events",
                    "curvine_fuse_session_init_total rate",
                    "curvine_fuse_session_init_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-session-shutdown",
            "Session Shutdown Events",
            "curvine_fuse_session_shutdown_total rate by reason",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_rate_panel(
                    ctx,
                    "fuse-session-shutdown",
                    "Session Shutdown Events",
                    "curvine_fuse_session_shutdown_total rate by reason",
                    "curvine_fuse_session_shutdown_total",
                    FUSE,
                )
            },
        ),
        panel_task(
            ctx.clone(),
            "fuse-scrape-duration",
            "Metrics Scrape Duration",
            "curvine_fuse_metrics_scrape_duration_us avg (ms)",
            PanelKind::Timeseries,
            Some("health"),
            |ctx| {
                counter_ratio_panel(
                    ctx,
                    "fuse-scrape-duration",
                    "Metrics Scrape Duration",
                    "curvine_fuse_metrics_scrape_duration_us avg (ms)",
                    "curvine_fuse_metrics_scrape_duration_us_sum",
                    "curvine_fuse_metrics_scrape_duration_us_count",
                    FUSE,
                    "fuse_scrape_avg_ms",
                    US_TO_MS,
                )
            },
        ),
    ]
}

async fn sum_worker_rate_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metric: &str,
) -> Result<DashboardPanel, String> {
    let raw = query_range(&ctx, Some(metric), Some("worker"), None).await?;
    let series = sum_worker_rate_series(metric, &raw).into_iter().collect();
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: None,
    })
}

fn sum_worker_rate_series(metric: &str, raw: &[RangeSeries]) -> Option<DashboardSeries> {
    let mut by_ts: BTreeMap<i64, f64> = BTreeMap::new();
    for item in raw {
        for point in compute_counter_rate(&item.points) {
            *by_ts.entry(point.0).or_insert(0.0) += point.1;
        }
    }
    if by_ts.is_empty() {
        return None;
    }
    let points: Vec<RangePoint> = by_ts
        .into_iter()
        .map(|(timestamp_ms, value)| RangePoint(timestamp_ms, value))
        .collect();
    let mut labels = BTreeMap::new();
    labels.insert("component".to_string(), "cluster".to_string());
    labels.insert("metric".to_string(), metric.to_string());
    Some(dashboard_series(
        format!("{metric} (cluster)"),
        labels,
        points,
    ))
}

async fn timeseries_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metrics: &[&str],
    component: &str,
    apply_rocksdb_whitelist: bool,
) -> Result<DashboardPanel, String> {
    let query_futures: Vec<_> = metrics
        .iter()
        .map(|metric| {
            let ctx = ctx.clone();
            let metric = *metric;
            async move {
                query_range(&ctx, Some(metric), Some(component), None)
                    .await
                    .map(|raw| (metric, raw))
            }
        })
        .collect();
    let results = join_all(query_futures).await;
    let mut series = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok((metric, raw)) => {
                for item in raw {
                    if (apply_rocksdb_whitelist || metric == ROCKSDB_METRIC)
                        && !rocksdb_tag_allowed(&item.labels)
                    {
                        continue;
                    }
                    series.push(to_dashboard_series(&item, false));
                }
            }
            Err(err) => errors.push(err),
        }
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

async fn rate_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metric: &str,
) -> Result<DashboardPanel, String> {
    let raw = query_range(&ctx, Some(metric), Some("worker"), None).await?;
    let series = raw
        .into_iter()
        .map(|item| to_dashboard_series(&item, true))
        .collect();
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: None,
    })
}

fn dashboard_series(
    name: String,
    labels: BTreeMap<String, String>,
    points: Vec<RangePoint>,
) -> DashboardSeries {
    let summary = compute_series_summary(&points);
    DashboardSeries {
        name,
        labels,
        points,
        summary,
    }
}

async fn counter_rate_series(
    ctx: DashboardBuildCtx,
    metric: &str,
    component: &str,
) -> Result<Vec<DashboardSeries>, String> {
    let raw = query_range(&ctx, Some(metric), Some(component), None).await?;
    Ok(raw
        .into_iter()
        .filter_map(|item| {
            let points = compute_counter_rate(&item.points);
            if points.is_empty() {
                return None;
            }
            let mut labels = item.labels.clone();
            enrich_series_labels(&item.metric, &mut labels);
            Some(dashboard_series(
                series_display_name(&item.metric, &labels, true),
                labels,
                points,
            ))
        })
        .collect())
}

fn enrich_series_labels(metric: &str, labels: &mut BTreeMap<String, String>) {
    labels
        .entry("metric".to_string())
        .or_insert_with(|| metric.to_string());
}

async fn counter_rate_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metric: &str,
    component: &str,
) -> Result<DashboardPanel, String> {
    let series = counter_rate_series(ctx, metric, component).await?;
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: None,
    })
}

async fn counter_ratio_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    numerator_metric: &str,
    denominator_metric: &str,
    component: &str,
    derived_metric: &str,
    value_scale: f64,
) -> Result<DashboardPanel, String> {
    let (series, error) = counter_ratio_series(
        ctx,
        numerator_metric,
        denominator_metric,
        component,
        derived_metric,
        value_scale,
    )
    .await?;
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error,
    })
}

fn scale_points(points: Vec<RangePoint>, value_scale: f64) -> Vec<RangePoint> {
    if value_scale == 1.0 {
        return points;
    }
    points
        .into_iter()
        .map(|point| RangePoint(point.0, point.1 * value_scale))
        .collect()
}

async fn counter_ratio_series(
    ctx: DashboardBuildCtx,
    numerator_metric: &str,
    denominator_metric: &str,
    component: &str,
    derived_metric: &str,
    value_scale: f64,
) -> Result<(Vec<DashboardSeries>, Option<String>), String> {
    let (numerators, denominators) = futures::join!(
        query_range(&ctx, Some(numerator_metric), Some(component), None),
        query_range(&ctx, Some(denominator_metric), Some(component), None),
    );
    let mut errors = Vec::new();
    let numerators = match numerators {
        Ok(value) => value,
        Err(err) => {
            errors.push(err);
            Vec::new()
        }
    };
    let denominators = match denominators {
        Ok(value) => value,
        Err(err) => {
            errors.push(err);
            Vec::new()
        }
    };
    let series = numerators
        .into_iter()
        .filter_map(|numerator| {
            let denominator = denominators
                .iter()
                .find(|candidate| labels_match_for_ratio(&numerator.labels, &candidate.labels))?;
            let points = scale_points(
                counter_delta_ratio_points(&numerator.points, &denominator.points, ctx.step_ms),
                value_scale,
            );
            if points.is_empty() {
                return None;
            }
            let mut labels = numerator.labels;
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            Some(dashboard_series(
                series_display_name(derived_metric, &labels, false),
                labels,
                points,
            ))
        })
        .collect();
    Ok((series, merge_errors(errors)))
}

fn labels_match_for_ratio(
    left: &BTreeMap<String, String>,
    right: &BTreeMap<String, String>,
) -> bool {
    for key in [
        "component",
        "instance",
        "operation",
        "worker_id",
        "worker_state",
        "type",
        "tag",
    ] {
        if left.get(key) != right.get(key) {
            return false;
        }
    }
    true
}

fn align_counter_buckets(points: &[RangePoint], step_ms: i64) -> BTreeMap<i64, f64> {
    let step = step_ms.max(1);
    let mut buckets = BTreeMap::new();
    for point in points {
        let bucket = point.0.div_euclid(step) * step;
        buckets.insert(bucket, point.1);
    }
    buckets
}

/// Average per event from cumulative counters: delta(numerator) / delta(denominator).
fn counter_delta_ratio_points(
    numerator: &[RangePoint],
    denominator: &[RangePoint],
    step_ms: Option<i64>,
) -> Vec<RangePoint> {
    if numerator.len() < 2 || denominator.len() < 2 {
        return Vec::new();
    }
    let step = step_ms.unwrap_or(15_000);
    let numerator_by_ts = align_counter_buckets(numerator, step);
    let denominator_by_ts = align_counter_buckets(denominator, step);
    let common_timestamps: Vec<i64> = numerator_by_ts
        .keys()
        .filter(|timestamp| denominator_by_ts.contains_key(timestamp))
        .copied()
        .collect();
    if common_timestamps.len() < 2 {
        return Vec::new();
    }
    common_timestamps
        .windows(2)
        .filter_map(|window| {
            let prev_ts = window[0];
            let curr_ts = window[1];
            let num_prev = *numerator_by_ts.get(&prev_ts)?;
            let num_curr = *numerator_by_ts.get(&curr_ts)?;
            let denom_prev = *denominator_by_ts.get(&prev_ts)?;
            let denom_curr = *denominator_by_ts.get(&curr_ts)?;
            let delta_num = num_curr - num_prev;
            let delta_den = denom_curr - denom_prev;
            if delta_num < 0.0 {
                return None;
            }
            let value = if delta_den <= 0.0 {
                0.0
            } else {
                delta_num / delta_den
            };
            Some(RangePoint(curr_ts, value))
        })
        .collect()
}

async fn histogram_quantile_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    bucket_metric: &str,
    component: &str,
    quantile: f64,
    derived_metric: &str,
    value_scale: f64,
) -> Result<DashboardPanel, String> {
    let (series, error) = histogram_quantile_series(
        ctx,
        bucket_metric,
        component,
        quantile,
        derived_metric,
        value_scale,
    )
    .await?;
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error,
    })
}

async fn histogram_quantile_series(
    ctx: DashboardBuildCtx,
    bucket_metric: &str,
    component: &str,
    quantile: f64,
    derived_metric: &str,
    value_scale: f64,
) -> Result<(Vec<DashboardSeries>, Option<String>), String> {
    if ctx.service.uses_prometheus_provider() {
        return histogram_quantile_series_promql(
            ctx,
            bucket_metric,
            component,
            quantile,
            derived_metric,
            value_scale,
        )
        .await;
    }

    let buckets = match query_range_raw(&ctx, Some(bucket_metric), Some(component), None).await {
        Ok(value) => value,
        Err(err) => return Ok((Vec::new(), Some(err))),
    };
    let mut groups: BTreeMap<BTreeMap<String, String>, Vec<RangeSeries>> = BTreeMap::new();
    for series in buckets {
        if !series.labels.contains_key("le") {
            continue;
        }
        let mut group_labels = series.labels.clone();
        group_labels.remove("le");
        groups.entry(group_labels).or_default().push(series);
    }

    let series = groups
        .into_iter()
        .filter_map(|(group_labels, bucket_series)| {
            let points = scale_points(
                histogram_quantile_points(&bucket_series, quantile, ctx.step_ms),
                value_scale,
            );
            if points.is_empty() {
                return None;
            }
            let mut labels = group_labels;
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            Some(dashboard_series(
                series_display_name(derived_metric, &labels, false),
                labels,
                points,
            ))
        })
        .collect();
    Ok((series, None))
}

fn prom_rate_window(step_ms: Option<i64>) -> String {
    let step = step_ms.unwrap_or(15_000).max(15_000);
    let window_ms = step.saturating_mul(4).max(120_000);
    if window_ms % 3_600_000 == 0 {
        format!("{}h", window_ms / 3_600_000)
    } else if window_ms % 60_000 == 0 {
        format!("{}m", window_ms / 60_000)
    } else {
        format!("{}s", window_ms / 1_000)
    }
}

async fn histogram_quantile_series_promql(
    ctx: DashboardBuildCtx,
    bucket_metric: &str,
    component: &str,
    quantile: f64,
    derived_metric: &str,
    value_scale: f64,
) -> Result<(Vec<DashboardSeries>, Option<String>), String> {
    let rate_window = prom_rate_window(ctx.step_ms);
    let query = format!(
        "histogram_quantile({quantile}, sum by (le, instance, job, component, kind, opcode, status, errno, stage, io_type, path_type, backend, operation, source, result, reason, state, lane) (rate({bucket_metric}{{component=\"{component}\"}}[{rate_window}])))",
        quantile = quantile,
        bucket_metric = bucket_metric,
        component = component,
        rate_window = rate_window,
    );
    let raw = match ctx
        .service
        .range_expr(&query, ctx.start_ms, ctx.end_ms, ctx.step_ms)
        .await
    {
        Ok(value) => value,
        Err(err) => return Ok((Vec::new(), Some(err))),
    };
    let series = raw
        .into_iter()
        .filter_map(|item| {
            let points = scale_points(item.points, value_scale);
            if points.is_empty() {
                return None;
            }
            let mut labels = item.labels;
            labels.remove("le");
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            enrich_series_labels(derived_metric, &mut labels);
            Some(dashboard_series(
                series_display_name(derived_metric, &labels, false),
                labels,
                points,
            ))
        })
        .collect();
    Ok((series, None))
}

fn histogram_quantile_points(
    bucket_series: &[RangeSeries],
    quantile: f64,
    step_ms: Option<i64>,
) -> Vec<RangePoint> {
    if bucket_series.is_empty() {
        return Vec::new();
    }
    let step = step_ms.unwrap_or(15_000).max(1);
    let mut by_le: BTreeMap<String, BTreeMap<i64, f64>> = BTreeMap::new();
    for series in bucket_series {
        let Some(le) = series.labels.get("le") else {
            continue;
        };
        by_le.insert(le.clone(), align_counter_buckets(&series.points, step));
    }
    if by_le.len() < 2 {
        return Vec::new();
    }
    let ordered_ts = common_aligned_timestamps(&by_le);
    if ordered_ts.len() < 2 {
        return Vec::new();
    }

    let mut quantile_points = Vec::new();
    for window in ordered_ts.windows(2) {
        let prev_ts = window[0];
        let curr_ts = window[1];
        let mut bucket_deltas = Vec::new();
        for (le, points_map) in &by_le {
            let (Some(prev), Some(curr)) = (points_map.get(&prev_ts), points_map.get(&curr_ts))
            else {
                bucket_deltas.clear();
                break;
            };
            let delta = curr - prev;
            if delta < 0.0 {
                bucket_deltas.clear();
                break;
            }
            let Some(upper) = parse_histogram_le(le) else {
                bucket_deltas.clear();
                break;
            };
            bucket_deltas.push((upper, delta));
        }
        if bucket_deltas.len() != by_le.len() {
            continue;
        }
        bucket_deltas.sort_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let bucket_counts = cumulative_bucket_deltas_to_counts(&bucket_deltas);
        if bucket_counts.is_empty() {
            continue;
        }
        if let Some(value) = compute_histogram_quantile(quantile, &bucket_counts) {
            quantile_points.push(RangePoint(curr_ts, value));
        }
    }
    quantile_points
}

fn common_aligned_timestamps(by_le: &BTreeMap<String, BTreeMap<i64, f64>>) -> Vec<i64> {
    let mut iter = by_le.values();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut common: BTreeSet<i64> = first.keys().copied().collect();
    for rest in iter {
        common.retain(|timestamp| rest.contains_key(timestamp));
    }
    common.into_iter().collect()
}

/// Convert Prometheus cumulative `_bucket` counter deltas into per-bucket counts.
fn cumulative_bucket_deltas_to_counts(cumulative_deltas: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let mut prev = 0.0;
    let mut counts = Vec::new();
    for (upper, cumulative_delta) in cumulative_deltas {
        let count = cumulative_delta - prev;
        prev = *cumulative_delta;
        if count > 0.0 {
            counts.push((*upper, count));
        }
    }
    counts
}

fn parse_histogram_le(le: &str) -> Option<f64> {
    if le == "+Inf" {
        return Some(f64::INFINITY);
    }
    le.parse::<f64>().ok()
}

/// Prometheus-style histogram quantile from per-bucket count deltas in one window.
fn compute_histogram_quantile(quantile: f64, buckets: &[(f64, f64)]) -> Option<f64> {
    if !(0.0..=1.0).contains(&quantile) || buckets.is_empty() {
        return None;
    }
    if buckets.len() == 1 {
        let (bucket_end, count) = buckets[0];
        if count <= 0.0 {
            return None;
        }
        if !bucket_end.is_finite() {
            return Some(0.0);
        }
        return Some(bucket_end * quantile);
    }
    let mut cumulative = 0.0;
    let mut coalesced = Vec::with_capacity(buckets.len());
    for (upper, count) in buckets {
        if *count <= 0.0 {
            continue;
        }
        cumulative += count;
        if coalesced
            .last()
            .map(|last: &(f64, f64)| last.0 == *upper)
            .unwrap_or(false)
        {
            coalesced.last_mut().expect("coalesced").1 = cumulative;
        } else {
            coalesced.push((*upper, cumulative));
        }
    }
    if coalesced.len() < 2 {
        return None;
    }
    let total = coalesced.last().expect("coalesced").1;
    if total <= 0.0 {
        return None;
    }
    let rank = quantile * total;
    let bucket_idx = coalesced
        .iter()
        .position(|(_, count)| *count >= rank)
        .unwrap_or(coalesced.len() - 1);
    let (bucket_end, bucket_count) = coalesced[bucket_idx];
    let (bucket_start, prev_count) = if bucket_idx == 0 {
        (0.0, 0.0)
    } else {
        let (prev_upper, prev_cumulative) = coalesced[bucket_idx - 1];
        (prev_upper, prev_cumulative)
    };
    let count_in_bucket = bucket_count - prev_count;
    if count_in_bucket <= 0.0 {
        return Some(bucket_start);
    }
    let rank_in_bucket = rank - prev_count;
    if !bucket_end.is_finite() {
        return Some(bucket_start);
    }
    Some(bucket_start + (bucket_end - bucket_start) * (rank_in_bucket / count_in_bucket))
}

async fn sum_worker_metrics_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metrics: &[&str],
) -> Result<DashboardPanel, String> {
    let query_futures: Vec<_> = metrics
        .iter()
        .map(|metric| {
            let ctx = ctx.clone();
            let metric = *metric;
            async move {
                query_range(&ctx, Some(metric), Some("worker"), None)
                    .await
                    .map(|raw| (metric, raw))
            }
        })
        .collect();
    let results = join_all(query_futures).await;
    let mut series = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok((metric, raw)) => {
                if let Some(aggregated) = sum_worker_metric_series(metric, &raw) {
                    series.push(aggregated);
                }
            }
            Err(err) => errors.push(err),
        }
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

fn sum_worker_metric_series(metric: &str, raw: &[RangeSeries]) -> Option<DashboardSeries> {
    if raw.is_empty() {
        return None;
    }
    let mut by_ts: BTreeMap<i64, f64> = BTreeMap::new();
    for item in raw {
        for point in &item.points {
            *by_ts.entry(point.0).or_insert(0.0) += point.1;
        }
    }
    if by_ts.is_empty() {
        return None;
    }
    let points: Vec<RangePoint> = by_ts
        .into_iter()
        .map(|(timestamp_ms, value)| RangePoint(timestamp_ms, value))
        .collect();
    let mut labels = BTreeMap::new();
    labels.insert("component".to_string(), "cluster".to_string());
    labels.insert("metric".to_string(), metric.to_string());
    Some(dashboard_series(
        format!("{metric} (cluster)"),
        labels,
        points,
    ))
}

async fn multi_counter_rate_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metrics: &[&str],
    component: &str,
) -> Result<DashboardPanel, String> {
    let query_futures: Vec<_> = metrics
        .iter()
        .map(|metric| {
            let ctx = ctx.clone();
            let metric = *metric;
            async move {
                counter_rate_series(ctx, metric, component)
                    .await
                    .map(|series| (metric, series))
            }
        })
        .collect();
    let results = join_all(query_futures).await;
    let mut series = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok((_metric, mut item_series)) => series.append(&mut item_series),
            Err(err) => errors.push(err),
        }
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

async fn transfer_anomaly_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
) -> Result<DashboardPanel, String> {
    let mut series = Vec::new();
    let mut errors = Vec::new();
    if ctx.service.uses_prometheus_provider() {
        let rate_window = prom_rate_window(ctx.step_ms);
        let queries = [
            (
                "transfer_terminal_total",
                format!(
                    "sum by (reason, instance) (rate(transfer_terminal_total{{component=\"transfer\",state=\"failed\"}}[{rate_window}]))"
                ),
            ),
            (
                "transfer_task_report_total",
                format!(
                    "sum by (result, instance) (rate(transfer_task_report_total{{component=\"transfer\",result!~\"accepted|accepted_async|queued\"}}[{rate_window}]))"
                ),
            ),
            (
                "transfer_planning_total",
                format!(
                    "sum by (kind, result, instance) (rate(transfer_planning_total{{component=\"transfer\",result!=\"success\"}}[{rate_window}]))"
                ),
            ),
        ];
        for (metric, query) in queries {
            match ctx
                .service
                .range_expr(&query, ctx.start_ms, ctx.end_ms, ctx.step_ms)
                .await
            {
                Ok(raw) => {
                    for mut item in raw {
                        enrich_series_labels(metric, &mut item.labels);
                        item.labels
                            .entry("component".to_string())
                            .or_insert_with(|| "transfer".to_string());
                        if item.points.is_empty() {
                            continue;
                        }
                        series.push(dashboard_series(
                            series_display_name(metric, &item.labels, true),
                            item.labels,
                            item.points,
                        ));
                    }
                }
                Err(err) => errors.push(err),
            }
        }
    } else {
        let query_futures = [
            "transfer_terminal_total",
            "transfer_task_report_total",
            "transfer_planning_total",
        ]
        .into_iter()
        .map(|metric| {
            let ctx = ctx.clone();
            async move { query_range(&ctx, Some(metric), Some("transfer"), None).await }
        })
        .collect::<Vec<_>>();
        let results = join_all(query_futures).await;
        for (metric, result) in [
            "transfer_terminal_total",
            "transfer_task_report_total",
            "transfer_planning_total",
        ]
        .into_iter()
        .zip(results)
        {
            match result {
                Ok(raw) => series.extend(filter_transfer_anomaly_series(metric, &raw)),
                Err(err) => errors.push(err),
            }
        }
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

fn filter_transfer_anomaly_series(metric: &str, raw: &[RangeSeries]) -> Vec<DashboardSeries> {
    raw.iter()
        .filter(|item| match metric {
            "transfer_terminal_total" => {
                item.labels.get("state").map(String::as_str) == Some("failed")
            }
            "transfer_task_report_total" => item.labels.get("result").is_none_or(|result| {
                !matches!(result.as_str(), "accepted" | "accepted_async" | "queued")
            }),
            "transfer_planning_total" => {
                item.labels.get("result").map(String::as_str) != Some("success")
            }
            _ => false,
        })
        .map(|item| to_dashboard_series(item, true))
        .collect()
}

fn sum_counter_rate_series(
    metric: &str,
    raw: &[RangeSeries],
    aggregate_component: &str,
    aggregate_instance: &str,
) -> Option<DashboardSeries> {
    let mut by_ts: BTreeMap<i64, f64> = BTreeMap::new();
    for item in raw {
        for point in compute_counter_rate(&item.points) {
            *by_ts.entry(point.0).or_insert(0.0) += point.1;
        }
    }
    if by_ts.is_empty() {
        return None;
    }
    let points: Vec<RangePoint> = by_ts
        .into_iter()
        .map(|(timestamp_ms, value)| RangePoint(timestamp_ms, value))
        .collect();
    let mut labels = BTreeMap::new();
    labels.insert("component".to_string(), aggregate_component.to_string());
    labels.insert("instance".to_string(), aggregate_instance.to_string());
    labels.insert("metric".to_string(), metric.to_string());
    Some(dashboard_series(
        format!("{metric} ({aggregate_component})"),
        labels,
        points,
    ))
}

async fn sum_counter_rate_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    metrics: &[&str],
    component: &str,
    aggregate_component: &str,
    aggregate_instance: &str,
) -> Result<DashboardPanel, String> {
    let query_futures: Vec<_> = metrics
        .iter()
        .map(|metric| {
            let ctx = ctx.clone();
            let metric = *metric;
            async move {
                query_range(&ctx, Some(metric), Some(component), None)
                    .await
                    .map(|raw| (metric, raw))
            }
        })
        .collect();
    let results = join_all(query_futures).await;
    let mut series = Vec::new();
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok((metric, raw)) => {
                if let Some(aggregated) =
                    sum_counter_rate_series(metric, &raw, aggregate_component, aggregate_instance)
                {
                    series.push(aggregated);
                }
            }
            Err(err) => errors.push(err),
        }
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

fn gauge_ratio_value(mode: GaugeRatioMode, numerator: f64, denominator: f64) -> f64 {
    if denominator <= 0.0 {
        return 0.0;
    }
    match mode {
        GaugeRatioMode::Used => (100.0 * numerator / denominator).max(0.0),
        GaugeRatioMode::Watermark => (100.0 * (1.0 - numerator / denominator)).max(0.0),
    }
}

fn sum_series_by_ts(raw: &[RangeSeries]) -> BTreeMap<i64, f64> {
    let mut by_ts = BTreeMap::new();
    for item in raw {
        for point in &item.points {
            *by_ts.entry(point.0).or_insert(0.0) += point.1;
        }
    }
    by_ts
}

fn gauge_ratio_points(
    numerator: &[RangePoint],
    denominator: &[RangePoint],
    mode: GaugeRatioMode,
) -> Vec<RangePoint> {
    let denominator_by_ts: BTreeMap<i64, f64> =
        denominator.iter().map(|point| (point.0, point.1)).collect();
    numerator
        .iter()
        .filter_map(|point| {
            let denominator = denominator_by_ts.get(&point.0)?;
            Some(RangePoint(
                point.0,
                gauge_ratio_value(mode, point.1, *denominator),
            ))
        })
        .collect()
}

fn aggregate_gauge_ratio_series(
    numerator_metric: &str,
    _denominator_metric: &str,
    derived_metric: &str,
    mode: GaugeRatioMode,
    aggregate_component: &str,
    aggregate_instance: &str,
    numerators: &[RangeSeries],
    denominators: &[RangeSeries],
) -> Option<DashboardSeries> {
    let numerator_by_ts = sum_series_by_ts(numerators);
    let denominator_by_ts = sum_series_by_ts(denominators);
    if numerator_by_ts.is_empty() || denominator_by_ts.is_empty() {
        return None;
    }
    let points: Vec<RangePoint> = numerator_by_ts
        .iter()
        .filter_map(|(timestamp_ms, numerator)| {
            let denominator = denominator_by_ts.get(timestamp_ms)?;
            Some(RangePoint(
                *timestamp_ms,
                gauge_ratio_value(mode, *numerator, *denominator),
            ))
        })
        .collect();
    if points.is_empty() {
        return None;
    }
    let mut labels = BTreeMap::new();
    labels.insert("component".to_string(), aggregate_component.to_string());
    labels.insert("instance".to_string(), aggregate_instance.to_string());
    labels.insert("metric".to_string(), numerator_metric.to_string());
    labels.insert("derived_metric".to_string(), derived_metric.to_string());
    Some(dashboard_series(
        format!("{derived_metric} ({aggregate_instance})"),
        labels,
        points,
    ))
}

fn per_instance_gauge_ratio_series(
    numerator_metric: &str,
    derived_metric: &str,
    mode: GaugeRatioMode,
    numerators: &[RangeSeries],
    denominators: &[RangeSeries],
) -> Vec<DashboardSeries> {
    numerators
        .iter()
        .filter_map(|numerator| {
            let denominator = denominators
                .iter()
                .find(|candidate| labels_match_for_ratio(&numerator.labels, &candidate.labels))?;
            let points = gauge_ratio_points(&numerator.points, &denominator.points, mode);
            if points.is_empty() {
                return None;
            }
            let mut labels = numerator.labels.clone();
            labels.insert("metric".to_string(), numerator_metric.to_string());
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            Some(dashboard_series(
                series_display_name(derived_metric, &labels, false),
                labels,
                points,
            ))
        })
        .collect()
}

fn gauge_difference_series(
    base_metric: &str,
    compare_metric: &str,
    base_raw: &[RangeSeries],
    compare_raw: &[RangeSeries],
    component: &str,
    derived_metric: &str,
) -> Vec<DashboardSeries> {
    base_raw
        .iter()
        .filter_map(|base| {
            let compare = compare_raw
                .iter()
                .find(|candidate| labels_match_for_ratio(&base.labels, &candidate.labels))?;
            let compare_by_ts: BTreeMap<i64, f64> = compare
                .points
                .iter()
                .map(|point| (point.0, point.1))
                .collect();
            let points: Vec<RangePoint> = base
                .points
                .iter()
                .filter_map(|point| {
                    let value = compare_by_ts.get(&point.0)?;
                    Some(RangePoint(point.0, (point.1 - *value).max(0.0)))
                })
                .collect();
            if points.is_empty() {
                return None;
            }
            let mut labels = base.labels.clone();
            labels.insert("metric".to_string(), base_metric.to_string());
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            let instance = labels
                .get("instance")
                .map(String::as_str)
                .unwrap_or(component);
            Some(dashboard_series(
                format!("{compare_metric} lag ({instance})"),
                labels,
                points,
            ))
        })
        .collect()
}

async fn gauge_difference_multi_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    base_metric: &str,
    compare_metrics: &[&str],
    component: &str,
    derived_metric: &str,
) -> Result<DashboardPanel, String> {
    let mut series = Vec::new();
    let mut errors = Vec::new();
    if ctx.service.uses_prometheus_provider() {
        for compare_metric in compare_metrics {
            let query = format!(
                "{base_metric}{{component=\"{component}\"}} - {compare_metric}{{component=\"{component}\"}}"
            );
            match ctx
                .service
                .range_expr(&query, ctx.start_ms, ctx.end_ms, ctx.step_ms)
                .await
            {
                Ok(raw) => series.extend(raw.into_iter().filter_map(|item| {
                    if item.points.is_empty() {
                        return None;
                    }
                    let mut labels = item.labels;
                    labels.insert("metric".to_string(), base_metric.to_string());
                    labels.insert("derived_metric".to_string(), derived_metric.to_string());
                    Some(dashboard_series(
                        series_display_name(derived_metric, &labels, false),
                        labels,
                        item.points,
                    ))
                })),
                Err(err) => errors.push(err),
            }
        }
    } else {
        let base_raw = query_range(&ctx, Some(base_metric), Some(component), None).await?;
        for compare_metric in compare_metrics {
            let compare_raw =
                query_range(&ctx, Some(compare_metric), Some(component), None).await?;
            series.extend(gauge_difference_series(
                base_metric,
                compare_metric,
                &base_raw,
                &compare_raw,
                component,
                derived_metric,
            ));
        }
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

async fn range_expr_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    query: &str,
    component: &str,
    instance: &str,
    metric: &str,
    derived_metric: &str,
    value_scale: f64,
) -> Result<DashboardPanel, String> {
    let raw = ctx
        .service
        .range_expr(query, ctx.start_ms, ctx.end_ms, ctx.step_ms)
        .await?;
    let series = raw
        .into_iter()
        .filter_map(|item| {
            let points = scale_points(item.points, value_scale);
            if points.is_empty() {
                return None;
            }
            let mut labels = item.labels;
            labels.insert("component".to_string(), component.to_string());
            if !instance.is_empty() {
                labels.insert("instance".to_string(), instance.to_string());
            }
            labels.insert("metric".to_string(), metric.to_string());
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            Some(dashboard_series(
                series_display_name(derived_metric, &labels, false),
                labels,
                points,
            ))
        })
        .collect();
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: None,
    })
}

async fn aggregate_gauge_ratio_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    numerator_metric: &str,
    denominator_metric: &str,
    component: &str,
    mode: GaugeRatioMode,
    aggregate_component: &str,
    aggregate_instance: &str,
) -> Result<DashboardPanel, String> {
    if ctx.service.uses_prometheus_provider() {
        let expr = match mode {
            GaugeRatioMode::Used => format!(
                "100 * sum({numerator_metric}{{component=\"{component}\"}}) / clamp_min(sum({denominator_metric}{{component=\"{component}\"}}), 1)"
            ),
            GaugeRatioMode::Watermark => format!(
                "100 * (1 - sum({numerator_metric}{{component=\"{component}\"}}) / clamp_min(sum({denominator_metric}{{component=\"{component}\"}}), 1))"
            ),
        };
        return range_expr_panel(
            ctx,
            id,
            title,
            subtitle,
            &expr,
            aggregate_component,
            aggregate_instance,
            numerator_metric,
            "aggregate_gauge_ratio",
            1.0,
        )
        .await;
    }
    let (numerators, denominators) = futures::join!(
        query_range(&ctx, Some(numerator_metric), Some(component), None,),
        query_range(&ctx, Some(denominator_metric), Some(component), None,),
    );
    let numerators = numerators?;
    let denominators = denominators?;
    let series = aggregate_gauge_ratio_series(
        numerator_metric,
        denominator_metric,
        "aggregate_gauge_ratio",
        mode,
        aggregate_component,
        aggregate_instance,
        &numerators,
        &denominators,
    )
    .into_iter()
    .collect();
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: None,
    })
}

async fn gauge_ratio_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
    numerator_metric: &str,
    denominator_metric: &str,
    component: &str,
    mode: GaugeRatioMode,
    derived_metric: &str,
) -> Result<DashboardPanel, String> {
    if ctx.service.uses_prometheus_provider() {
        let expr = match mode {
            GaugeRatioMode::Used => format!(
                "100 * {numerator_metric}{{component=\"{component}\"}} / clamp_min({denominator_metric}{{component=\"{component}\"}}, 1)"
            ),
            GaugeRatioMode::Watermark => format!(
                "100 * (1 - {numerator_metric}{{component=\"{component}\"}} / clamp_min({denominator_metric}{{component=\"{component}\"}}, 1))"
            ),
        };
        return range_expr_panel(
            ctx,
            id,
            title,
            subtitle,
            &expr,
            component,
            "",
            numerator_metric,
            derived_metric,
            1.0,
        )
        .await;
    }
    let (numerators, denominators) = futures::join!(
        query_range(&ctx, Some(numerator_metric), Some(component), None,),
        query_range(&ctx, Some(denominator_metric), Some(component), None,),
    );
    let numerators = numerators?;
    let denominators = denominators?;
    let series = per_instance_gauge_ratio_series(
        numerator_metric,
        derived_metric,
        mode,
        &numerators,
        &denominators,
    );
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: None,
    })
}

async fn cluster_risk_panel(
    ctx: DashboardBuildCtx,
    id: &str,
    title: &str,
    subtitle: &str,
) -> Result<DashboardPanel, String> {
    let replication_ctx = ctx.clone();
    let (lost, failed, stalled, replication) = futures::join!(
        query_range(&ctx, Some("worker_num"), Some("master"), None),
        query_range(&ctx, Some("failed_disks"), Some("worker"), None),
        query_range(&ctx, Some("fs_dir_stalled"), Some("master"), None),
        counter_rate_series(replication_ctx, "replication_failure_count", "master"),
    );
    let mut series = Vec::new();
    let mut errors = Vec::new();
    match lost {
        Ok(raw) => series.extend(
            raw.into_iter()
                .filter(|item| item.labels.get("tag").map(String::as_str) == Some("lost"))
                .map(|item| to_dashboard_series(&item, false)),
        ),
        Err(err) => errors.push(err),
    }
    match failed {
        Ok(raw) => {
            if let Some(aggregated) = sum_worker_metric_series("failed_disks", &raw) {
                series.push(aggregated);
            }
        }
        Err(err) => errors.push(err),
    }
    match stalled {
        Ok(raw) => series.extend(
            raw.into_iter()
                .map(|item| to_dashboard_series(&item, false)),
        ),
        Err(err) => errors.push(err),
    }
    match replication {
        Ok(mut item_series) => series.append(&mut item_series),
        Err(err) => errors.push(err),
    }
    Ok(DashboardPanel {
        id: id.to_string(),
        title: title.to_string(),
        subtitle: subtitle.to_string(),
        kind: PanelKind::Timeseries,
        series,
        rows: Vec::new(),
        section: None,
        error: merge_errors(errors),
    })
}

#[allow(dead_code)]
fn data_sources_panel(targets: &[TargetHealth]) -> DashboardPanel {
    let up = targets.iter().filter(|target| target.up).count();
    let total = targets.len();
    let master = targets.iter().filter(|t| t.component == "master").count();
    let workers = targets.iter().filter(|t| t.component == "worker").count();
    let mut rows: Vec<DashboardTableRow> = targets.iter().map(target_table_row).collect();
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    DashboardPanel {
        id: "data-sources".to_string(),
        title: "Data Sources".to_string(),
        subtitle: format!("{up}/{total} targets up ({master} master, {workers} workers)"),
        kind: PanelKind::Table,
        series: Vec::new(),
        rows,
        section: None,
        error: None,
    }
}

fn target_table_row(target: &TargetHealth) -> DashboardTableRow {
    let status = if target.up { "up" } else { "down" }.to_string();
    let mut detail = String::new();
    if !target.last_warning.is_empty() {
        detail.push_str(&target.last_warning);
    }
    if !target.last_error.is_empty() {
        if !detail.is_empty() {
            detail.push_str("; ");
        }
        detail.push_str(&target.last_error);
    }
    if target.component == "worker" && target.worker_state.as_deref() == Some("lost") && !target.up
    {
        if !detail.is_empty() {
            detail.push_str("; ");
        }
        detail.push_str("lost in Curvine state (scrape down expected)");
    }
    DashboardTableRow {
        id: target.id.clone(),
        component: target.component.clone(),
        instance: target.instance.clone(),
        scrape_up: target.up,
        worker_id: target.worker_id,
        worker_state: target.worker_state.clone(),
        status,
        detail,
    }
}

fn to_dashboard_series(item: &RangeSeries, as_rate: bool) -> DashboardSeries {
    let points = if as_rate {
        compute_counter_rate(&item.points)
    } else {
        item.points.clone()
    };
    let summary = compute_series_summary(&points);
    let mut labels = item.labels.clone();
    enrich_series_labels(&item.metric, &mut labels);
    DashboardSeries {
        name: series_display_name(&item.metric, &labels, as_rate),
        labels,
        points,
        summary,
    }
}

fn series_display_name(metric: &str, labels: &BTreeMap<String, String>, as_rate: bool) -> String {
    if let Some(operation) = labels.get("operation") {
        let mut name = operation.clone();
        if let Some(instance) = labels.get("instance") {
            name = format!("{name} ({instance})");
        }
        return name;
    }
    if let Some(derived_metric) = labels.get("derived_metric") {
        if let Some(operation) = labels.get("operation") {
            let mut name = operation.clone();
            if let Some(instance) = labels.get("instance") {
                name = format!("{name} ({instance})");
            }
            return name;
        }
        let mut name = derived_metric.clone();
        if let Some(instance) = labels.get("instance") {
            name = format!("{name} ({instance})");
        }
        return name;
    }
    if let Some(tag) = labels.get("tag") {
        let mut name = format!("{metric}{{tag=\"{tag}\"}}");
        if let Some(instance) = labels.get("instance") {
            name = format!("{name} ({instance})");
        }
        return name;
    }
    if as_rate {
        if let Some(worker_id) = labels.get("worker_id") {
            let mut name = format!("worker-{worker_id}");
            if let Some(block_type) = labels.get("type") {
                name = format!("{name} ({block_type})");
            }
            return name;
        }
    }
    if let Some(instance) = labels.get("instance") {
        return format!("{metric} ({instance})");
    }
    metric.to_string()
}

fn rocksdb_tag_allowed(labels: &BTreeMap<String, String>) -> bool {
    let Some(tag) = labels.get("tag") else {
        return false;
    };
    if ROCKSDB_DB_SCOPE_TAGS.iter().any(|allowed| allowed == tag) {
        return true;
    }
    tag.ends_with("_rocksdb_size-all-mem-tables")
        && ROCKSDB_CF_PREFIXES
            .iter()
            .any(|prefix| tag.starts_with(prefix))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::observability::model::MetricSample;
    use crate::observability::tsdb::InMemoryTsdb;

    fn master_target(targets: &[TargetHealth]) -> Option<&TargetHealth> {
        targets.iter().find(|target| target.component == "master")
    }

    fn master_discovery_ok(master: Option<&TargetHealth>) -> bool {
        let Some(master) = master else {
            return false;
        };
        let warning = master.last_warning.trim();
        if warning.is_empty() {
            return true;
        }
        !(warning.contains("master info") || warning.contains("worker discovery stale"))
    }

    fn master_metrics_scrape_up(master: Option<&TargetHealth>) -> bool {
        master.map(|target| target.up).unwrap_or(false)
    }

    fn active_master_display(targets: &[TargetHealth]) -> String {
        let master = master_target(targets);
        if master_discovery_ok(master) {
            master
                .and_then(|target| target.active_master.clone())
                .filter(|value| !value.is_empty())
                .or_else(|| master.map(|target| target.instance.clone()))
                .unwrap_or_else(|| "-".to_string())
        } else {
            "-".to_string()
        }
    }

    fn active_master_card_meta(master: Option<&TargetHealth>) -> String {
        let Some(master) = master else {
            return "Active master unknown".to_string();
        };
        if !master_discovery_ok(Some(master)) {
            if !master.last_warning.is_empty() {
                return master.last_warning.clone();
            }
            return "Active master discovery failed".to_string();
        }
        if master.up {
            return "Active master discovered".to_string();
        }
        let detail = if master.last_error.is_empty() {
            "metrics scrape target is down".to_string()
        } else {
            format!("metrics scrape target is down: {}", master.last_error)
        };
        format!("Active master discovered; {detail}")
    }

    fn assess_cluster_health(
        targets: &[TargetHealth],
        lost_workers: u32,
        failed_disks: f64,
    ) -> (&'static str, &'static str, String) {
        let master = master_target(targets);
        let discovery_ok = master_discovery_ok(master);
        let master_scrape_up = master_metrics_scrape_up(master);
        let live_worker_scrape_down = targets
            .iter()
            .filter(|target| {
                target.component == "worker"
                    && target.worker_state.as_deref() == Some("live")
                    && !target.up
            })
            .count();

        let mut cluster_issues = Vec::new();
        let mut observability_issues = Vec::new();

        if !discovery_ok {
            cluster_issues.push("Active master discovery failed".to_string());
        }
        if lost_workers > 0 {
            cluster_issues.push(format!("{lost_workers} lost worker(s)"));
        }
        if failed_disks > 0.0 {
            cluster_issues.push(format!("{failed_disks:.0} failed disk(s)"));
        }
        if discovery_ok && !master_scrape_up {
            observability_issues.push("Master metrics scrape target is down".to_string());
        }
        if live_worker_scrape_down > 0 {
            observability_issues.push(format!(
                "{live_worker_scrape_down} live worker metrics scrape target(s) down"
            ));
        }

        if !cluster_issues.is_empty() {
            return ("Unhealthy", "bad", cluster_issues.join("; "));
        }
        if !observability_issues.is_empty() {
            return ("Degraded", "warn", observability_issues.join("; "));
        }

        (
            "Healthy",
            "good",
            "No Curvine cluster issues detected".to_string(),
        )
    }

    fn worker_sample(name: &str, instance: &str, value: f64, ts: i64) -> (i64, MetricSample) {
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "worker".to_string());
        labels.insert("instance".to_string(), instance.to_string());
        labels.insert("worker_id".to_string(), "1".to_string());
        labels.insert("worker_state".to_string(), "live".to_string());
        (
            ts,
            MetricSample {
                name: name.to_string(),
                labels,
                value,
            },
        )
    }

    fn range_series(
        metric: &str,
        labels: BTreeMap<String, String>,
        points: Vec<RangePoint>,
    ) -> RangeSeries {
        RangeSeries {
            metric: metric.to_string(),
            labels,
            points,
        }
    }

    #[test]
    fn counter_rate_normal_increase() {
        let points = vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1_500.0)];
        let rates = compute_counter_rate(&points);
        assert_eq!(rates.len(), 1);
        assert!((rates[0].1 - 100.0).abs() < 0.001);
        assert!(rates[0].1 >= 0.0);
    }

    #[test]
    fn counter_rate_missing_point_returns_empty() {
        let points = vec![RangePoint(1_000, 42.0)];
        assert!(compute_counter_rate(&points).is_empty());
    }

    #[test]
    fn counter_rate_reset_is_zero_not_negative() {
        let points = vec![RangePoint(1_000, 1_000.0), RangePoint(16_000, 100.0)];
        let rates = compute_counter_rate(&points);
        assert_eq!(rates.len(), 1);
        assert_eq!(rates[0].1, 0.0);
    }

    #[test]
    fn series_summary_computes_min_max_avg_current() {
        let points = vec![
            RangePoint(1_000, 10.0),
            RangePoint(2_000, 30.0),
            RangePoint(3_000, 20.0),
        ];
        let summary = compute_series_summary(&points).expect("summary");
        assert_eq!(summary.current, Some(20.0));
        assert_eq!(summary.min, Some(10.0));
        assert_eq!(summary.max, Some(30.0));
        assert!((summary.avg.unwrap() - 20.0).abs() < 0.001);
    }

    #[test]
    fn sum_counter_rate_series_aggregates_instances_by_timestamp() {
        let mut labels_a = BTreeMap::new();
        labels_a.insert("component".to_string(), "worker".to_string());
        labels_a.insert("instance".to_string(), "w1:9001".to_string());
        labels_a.insert("worker_id".to_string(), "1".to_string());
        let mut labels_b = labels_a.clone();
        labels_b.insert("instance".to_string(), "w2:9001".to_string());
        labels_b.insert("worker_id".to_string(), "2".to_string());

        let raw = vec![
            range_series(
                "read_count",
                labels_a,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1_000.0)],
            ),
            range_series(
                "read_count",
                labels_b,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 500.0)],
            ),
        ];

        let aggregated =
            sum_counter_rate_series("read_count", &raw, "cluster", "cluster").expect("series");
        assert_eq!(
            aggregated.labels.get("component").map(String::as_str),
            Some("cluster")
        );
        assert_eq!(aggregated.points.len(), 1);
        assert!((aggregated.points[0].1 - 100.0).abs() < 0.001);
    }

    #[test]
    fn aggregate_gauge_ratio_series_uses_cluster_totals() {
        let mut labels_a = BTreeMap::new();
        labels_a.insert("component".to_string(), "worker".to_string());
        labels_a.insert("instance".to_string(), "w1:9001".to_string());
        labels_a.insert("worker_id".to_string(), "1".to_string());
        let mut labels_b = labels_a.clone();
        labels_b.insert("instance".to_string(), "w2:9001".to_string());
        labels_b.insert("worker_id".to_string(), "2".to_string());

        let numerators = vec![
            range_series(
                "available",
                labels_a.clone(),
                vec![RangePoint(1_000, 1_000.0), RangePoint(16_000, 1_000.0)],
            ),
            range_series(
                "available",
                labels_b.clone(),
                vec![RangePoint(1_000, 1_500.0), RangePoint(16_000, 1_500.0)],
            ),
        ];
        let denominators = vec![
            range_series(
                "capacity",
                labels_a,
                vec![RangePoint(1_000, 2_000.0), RangePoint(16_000, 2_000.0)],
            ),
            range_series(
                "capacity",
                labels_b,
                vec![RangePoint(1_000, 3_000.0), RangePoint(16_000, 3_000.0)],
            ),
        ];

        let series = aggregate_gauge_ratio_series(
            "available",
            "capacity",
            "aggregate_gauge_ratio",
            GaugeRatioMode::Watermark,
            "cluster",
            "cluster",
            &numerators,
            &denominators,
        )
        .expect("series");
        assert_eq!(series.points.len(), 2);
        assert!(series
            .points
            .iter()
            .all(|point| (point.1 - 50.0).abs() < 0.001));
    }

    #[test]
    fn gauge_difference_series_clamps_to_zero_when_compare_is_ahead() {
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "master".to_string());
        labels.insert("instance".to_string(), "m1:9000".to_string());
        let base = vec![range_series(
            "journal_committed",
            labels.clone(),
            vec![RangePoint(1_000, 100.0), RangePoint(16_000, 120.0)],
        )];
        let compare = vec![range_series(
            "journal_applied",
            labels,
            vec![RangePoint(1_000, 120.0), RangePoint(16_000, 115.0)],
        )];

        let series = gauge_difference_series(
            "journal_committed",
            "journal_applied",
            &base,
            &compare,
            "master",
            "journal_lag",
        );
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0]
                .points
                .iter()
                .map(|point| point.1)
                .collect::<Vec<_>>(),
            vec![0.0, 5.0]
        );
        assert_eq!(
            series[0].labels.get("derived_metric").map(String::as_str),
            Some("journal_lag")
        );
    }

    #[test]
    fn transfer_anomaly_series_keeps_only_failed_signals() {
        let mut base = BTreeMap::new();
        base.insert("component".to_string(), "transfer".to_string());
        base.insert("instance".to_string(), "cv-transfer-0:9011".to_string());

        let mut terminal_failed = base.clone();
        terminal_failed.insert("state".to_string(), "failed".to_string());
        terminal_failed.insert("reason".to_string(), "task_failed".to_string());
        let mut terminal_completed = base.clone();
        terminal_completed.insert("state".to_string(), "completed".to_string());
        let terminal = vec![
            range_series(
                "transfer_terminal_total",
                terminal_failed,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1.0)],
            ),
            range_series(
                "transfer_terminal_total",
                terminal_completed,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1.0)],
            ),
        ];

        let mut report_rejected = base.clone();
        report_rejected.insert("result".to_string(), "error".to_string());
        let mut report_accepted = base.clone();
        report_accepted.insert("result".to_string(), "accepted".to_string());
        let report = vec![
            range_series(
                "transfer_task_report_total",
                report_rejected,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1.0)],
            ),
            range_series(
                "transfer_task_report_total",
                report_accepted,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1.0)],
            ),
        ];

        let mut planning_failed = base.clone();
        planning_failed.insert("result".to_string(), "failed".to_string());
        let mut planning_success = base;
        planning_success.insert("result".to_string(), "success".to_string());
        let planning = vec![
            range_series(
                "transfer_planning_total",
                planning_failed,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1.0)],
            ),
            range_series(
                "transfer_planning_total",
                planning_success,
                vec![RangePoint(1_000, 0.0), RangePoint(16_000, 1.0)],
            ),
        ];

        let mut series = filter_transfer_anomaly_series("transfer_terminal_total", &terminal);
        series.extend(filter_transfer_anomaly_series(
            "transfer_task_report_total",
            &report,
        ));
        series.extend(filter_transfer_anomaly_series(
            "transfer_planning_total",
            &planning,
        ));
        assert_eq!(series.len(), 3);
        assert!(series.iter().all(|item| !item.points.is_empty()));
    }

    #[test]
    fn cluster_health_degrades_on_live_worker_scrape_down() {
        let targets = vec![
            TargetHealth {
                id: "master:m1:9000".to_string(),
                component: "master".to_string(),
                instance: "m1:9000".to_string(),
                url: "http://m1:9000/metrics".to_string(),
                worker_id: None,
                worker_state: None,
                active_master: None,
                up: true,
                last_scrape_ms: None,
                last_success_ms: None,
                last_scrape_duration_ms: None,
                last_error: String::new(),
                last_warning: String::new(),
                sample_count: 1,
                parse_error_count: 0,
            },
            TargetHealth {
                id: "worker:1".to_string(),
                component: "worker".to_string(),
                instance: "w1:9001".to_string(),
                url: "http://w1:9001/metrics".to_string(),
                worker_id: Some(1),
                worker_state: Some("live".to_string()),
                active_master: None,
                up: false,
                last_scrape_ms: None,
                last_success_ms: None,
                last_scrape_duration_ms: None,
                last_error: "timeout".to_string(),
                last_warning: String::new(),
                sample_count: 0,
                parse_error_count: 0,
            },
        ];
        let (label, tone, meta) = assess_cluster_health(&targets, 0, 0.0);
        assert_eq!(label, "Degraded");
        assert_eq!(tone, "warn");
        assert!(meta.contains("live worker metrics scrape"));
    }

    #[test]
    fn cluster_health_degraded_when_master_metrics_down_but_discovery_ok() {
        let targets = vec![TargetHealth {
            id: "master:m1:9000".to_string(),
            component: "master".to_string(),
            instance: "m1:9000".to_string(),
            url: "http://m1:9000/metrics".to_string(),
            worker_id: None,
            worker_state: None,
            active_master: None,
            up: false,
            last_scrape_ms: None,
            last_success_ms: None,
            last_scrape_duration_ms: None,
            last_error: "connection refused".to_string(),
            last_warning: String::new(),
            sample_count: 0,
            parse_error_count: 0,
        }];
        let (label, tone, meta) = assess_cluster_health(&targets, 0, 0.0);
        assert_eq!(label, "Degraded");
        assert_eq!(tone, "warn");
        assert!(meta.contains("Master metrics scrape target is down"));
    }

    #[test]
    fn cluster_health_unhealthy_when_master_discovery_fails() {
        let targets = vec![TargetHealth {
            id: "master:m1:9000".to_string(),
            component: "master".to_string(),
            instance: "m1:9000".to_string(),
            url: "http://m1:9000/metrics".to_string(),
            worker_id: None,
            worker_state: None,
            active_master: None,
            up: false,
            last_scrape_ms: None,
            last_success_ms: None,
            last_scrape_duration_ms: None,
            last_error: "connection refused".to_string(),
            last_warning: "master info request timed out after 5s".to_string(),
            sample_count: 0,
            parse_error_count: 0,
        }];
        let (label, tone, meta) = assess_cluster_health(&targets, 0, 0.0);
        assert_eq!(label, "Unhealthy");
        assert_eq!(tone, "bad");
        assert!(meta.contains("Active master discovery failed"));
    }

    #[test]
    fn active_master_card_shows_rpc_endpoint_when_metrics_scrape_down() {
        let targets = vec![TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: "active-master".to_string(),
            url: "http://127.0.0.1:29000/metrics".to_string(),
            worker_id: None,
            worker_state: None,
            active_master: Some("cv-master-2:8995".to_string()),
            up: false,
            last_scrape_ms: None,
            last_success_ms: None,
            last_scrape_duration_ms: None,
            last_error: "timeout".to_string(),
            last_warning: String::new(),
            sample_count: 0,
            parse_error_count: 0,
        }];
        let master = master_target(&targets);
        assert_eq!(active_master_display(&targets), "cv-master-2:8995");
        assert!(master_discovery_ok(master));
        assert!(active_master_card_meta(master).contains("metrics scrape target is down"));
    }

    #[test]
    fn data_sources_panel_includes_target_rows() {
        let targets = vec![TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "w1:9001".to_string(),
            url: "http://w1:9001/metrics".to_string(),
            worker_id: Some(1),
            worker_state: Some("lost".to_string()),
            active_master: None,
            up: false,
            last_scrape_ms: None,
            last_success_ms: None,
            last_scrape_duration_ms: None,
            last_error: "connection refused".to_string(),
            last_warning: String::new(),
            sample_count: 0,
            parse_error_count: 0,
        }];
        let panel = data_sources_panel(&targets);
        assert_eq!(panel.kind, PanelKind::Table);
        assert_eq!(panel.rows.len(), 1);
        assert_eq!(panel.rows[0].worker_state.as_deref(), Some("lost"));
        assert!(panel.rows[0].detail.contains("lost in Curvine state"));
    }

    #[tokio::test]
    async fn dashboard_returns_cards_groups_and_panels() {
        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;

        let conf = ClusterConf::default();
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = Arc::new(ObservabilityService::new(conf, unified_fs));
        let tsdb = service.builtin().tsdb();
        let (ts, sample) = worker_sample("read_bytes", "w1:9001", 0.0, 1_000);
        tsdb.insert(ts, vec![sample]);
        let (ts, sample) = worker_sample("read_bytes", "w1:9001", 15_000.0, 16_000);
        tsdb.insert(ts, vec![sample]);

        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "master".to_string());
        labels.insert("instance".to_string(), "m1:9000".to_string());
        tsdb.insert(
            16_000,
            vec![
                MetricSample {
                    name: "capacity".to_string(),
                    labels: labels.clone(),
                    value: 1_000_000.0,
                },
                MetricSample {
                    name: "fs_used".to_string(),
                    labels,
                    value: 250_000.0,
                },
            ],
        );

        let payload = build_dashboard(&service, "15m", "15s", 20_000, 900_000).await;
        assert!(payload.cards.is_empty());
        assert_eq!(payload.groups.len(), 5);
        assert!(payload.groups.iter().any(|group| group.id == "overview"));
        assert!(payload.groups.iter().any(|group| group.id == "master"));
        assert!(payload.groups.iter().any(|group| group.id == "workers"));
        assert!(payload.groups.iter().any(|group| group.id == "transfer"));
        assert!(payload.groups.iter().any(|group| group.id == "fuse"));
        assert!(!payload
            .groups
            .iter()
            .any(|group| group.id == "data_sources"));
        let master = payload
            .groups
            .iter()
            .find(|group| group.id == "master")
            .expect("master group");
        let cluster_read_panel = master
            .panels
            .iter()
            .find(|panel| panel.id == "cluster-read-throughput")
            .expect("cluster read throughput panel");
        assert_eq!(cluster_read_panel.title, "Read Throughput");
        assert_eq!(cluster_read_panel.section.as_deref(), Some("performance"));
        assert!(!cluster_read_panel.series.is_empty());
        let workers = payload
            .groups
            .iter()
            .find(|group| group.id == "workers")
            .expect("workers group");
        let read_panel = workers
            .panels
            .iter()
            .find(|panel| panel.id == "worker-read-throughput")
            .expect("read throughput panel");
        assert_eq!(read_panel.title, "Read Throughput");
        assert_eq!(read_panel.section.as_deref(), Some("performance"));
        assert!(!read_panel.series.is_empty());
        let rate_points = &read_panel.series[0].points;
        assert!(!rate_points.is_empty());
        assert!(rate_points.iter().all(|point| point.1 >= 0.0));

        let overview = payload
            .groups
            .iter()
            .find(|group| group.id == "overview")
            .expect("overview group");
        assert!(overview
            .panels
            .iter()
            .any(|panel| panel.id == "cluster-worker-capacity"
                && panel.section.as_deref() == Some("capacity")));

        let transfer = payload
            .groups
            .iter()
            .find(|group| group.id == "transfer")
            .expect("transfer group");
        assert!(transfer
            .panels
            .iter()
            .any(|panel| panel.id == "transfer-job-water"
                && panel.section.as_deref() == Some("status")));
        assert!(transfer
            .panels
            .iter()
            .any(|panel| panel.id == "transfer-completed-bytes-rate"
                && panel.section.as_deref() == Some("flow")));
        assert!(transfer
            .panels
            .iter()
            .any(|panel| panel.id == "transfer-job-summary-water"
                && panel.section.as_deref() == Some("status")));
        assert!(workers
            .panels
            .iter()
            .any(|panel| panel.id == "worker-transfer-task-water"
                && panel.section.as_deref() == Some("transfer-tasks")));
        assert!(workers
            .panels
            .iter()
            .any(|panel| panel.id == "worker-transfer-terminal-rate"
                && panel.section.as_deref() == Some("transfer-tasks")));
    }

    #[test]
    fn rocksdb_whitelist_filters_unlisted_tags() {
        let mut allowed = BTreeMap::new();
        allowed.insert("component".to_string(), "master".to_string());
        allowed.insert(
            "tag".to_string(),
            "db_rocksdb_block-cache-usage".to_string(),
        );
        let mut denied = allowed.clone();
        denied.insert(
            "tag".to_string(),
            "cf_block_rocksdb_compaction-pending".to_string(),
        );
        let mut cf_allowed = BTreeMap::new();
        cf_allowed.insert("component".to_string(), "master".to_string());
        cf_allowed.insert(
            "tag".to_string(),
            "cf_default_rocksdb_size-all-mem-tables".to_string(),
        );
        assert!(rocksdb_tag_allowed(&allowed));
        assert!(!rocksdb_tag_allowed(&denied));
        assert!(rocksdb_tag_allowed(&cf_allowed));
    }

    #[test]
    fn journal_lag_matches_series_by_instance() {
        fn difference_series(base: &RangeSeries, compare: &RangeSeries) -> Option<DashboardSeries> {
            let compare_by_ts: BTreeMap<i64, f64> = compare
                .points
                .iter()
                .map(|point| (point.0, point.1))
                .collect();
            let points: Vec<RangePoint> = base
                .points
                .iter()
                .filter_map(|point| {
                    let value = compare_by_ts.get(&point.0)?;
                    Some(RangePoint(point.0, (point.1 - value).max(0.0)))
                })
                .collect();
            if points.is_empty() {
                return None;
            }
            let mut labels = base.labels.clone();
            labels.insert("tag".to_string(), format!("{} lag", compare.metric));
            let instance = labels
                .get("instance")
                .map(String::as_str)
                .unwrap_or("master");
            Some(dashboard_series(
                format!("{} lag ({instance})", compare.metric),
                labels,
                points,
            ))
        }

        let mut labels_a = BTreeMap::new();
        labels_a.insert("component".to_string(), "master".to_string());
        labels_a.insert("instance".to_string(), "m1:9000".to_string());
        let mut labels_b = labels_a.clone();
        labels_b.insert("instance".to_string(), "m2:9000".to_string());
        let committed = [
            RangeSeries {
                metric: "journal_committed".to_string(),
                labels: labels_a.clone(),
                points: vec![RangePoint(1_000, 100.0), RangePoint(16_000, 120.0)],
            },
            RangeSeries {
                metric: "journal_committed".to_string(),
                labels: labels_b.clone(),
                points: vec![RangePoint(1_000, 200.0), RangePoint(16_000, 210.0)],
            },
        ];
        let applied = [
            RangeSeries {
                metric: "journal_applied".to_string(),
                labels: labels_a,
                points: vec![RangePoint(1_000, 90.0), RangePoint(16_000, 115.0)],
            },
            RangeSeries {
                metric: "journal_applied".to_string(),
                labels: labels_b,
                points: vec![RangePoint(1_000, 200.0), RangePoint(16_000, 210.0)],
            },
        ];
        let lag_a = difference_series(&committed[0], &applied[0]).expect("lag a");
        let lag_b = difference_series(&committed[1], &applied[1]).expect("lag b");
        assert_eq!(lag_a.points.last().map(|point| point.1), Some(5.0));
        assert_eq!(lag_b.points.last().map(|point| point.1), Some(0.0));
    }

    #[test]
    fn histogram_quantile_interpolates_within_bucket() {
        let buckets = vec![(100.0, 50.0), (1000.0, 50.0)];
        let p99 = compute_histogram_quantile(0.99, &buckets).expect("p99");
        assert!((p99 - 982.0).abs() < 0.01);
    }

    #[test]
    fn histogram_quantile_supports_single_bucket_window() {
        let p99 = compute_histogram_quantile(0.99, &[(50.0, 10.0)]).expect("p99");
        assert!((p99 - 49.5).abs() < 0.01);
    }

    #[test]
    fn histogram_quantile_points_aligns_fuse_bucket_series_to_common_step() {
        let mut base = BTreeMap::new();
        base.insert("component".to_string(), "fuse".to_string());
        base.insert("instance".to_string(), "127.0.0.1:9002".to_string());
        base.insert("kind".to_string(), "meta".to_string());
        base.insert("opcode".to_string(), "read".to_string());
        base.insert("status".to_string(), "success".to_string());

        let mut le_100 = base.clone();
        le_100.insert("le".to_string(), "100".to_string());
        let mut le_1000 = base.clone();
        le_1000.insert("le".to_string(), "1000".to_string());
        let mut le_inf = base.clone();
        le_inf.insert("le".to_string(), "+Inf".to_string());

        let bucket_series = vec![
            RangeSeries {
                metric: "curvine_fuse_request_duration_us_bucket".to_string(),
                labels: le_100,
                points: vec![RangePoint(1_000, 0.0), RangePoint(61_000, 50.0)],
            },
            RangeSeries {
                metric: "curvine_fuse_request_duration_us_bucket".to_string(),
                labels: le_1000,
                points: vec![RangePoint(1_000, 0.0), RangePoint(61_000, 100.0)],
            },
            RangeSeries {
                metric: "curvine_fuse_request_duration_us_bucket".to_string(),
                labels: le_inf,
                points: vec![RangePoint(1_000, 0.0), RangePoint(61_000, 100.0)],
            },
        ];

        let points = histogram_quantile_points(&bucket_series, 0.99, Some(15_000));
        assert_eq!(points.len(), 1);
        assert!((points[0].1 - 982.0).abs() < 0.01);
    }

    #[test]
    fn histogram_quantile_points_skips_windows_without_all_bucket_labels() {
        let mut base = BTreeMap::new();
        base.insert("component".to_string(), "fuse".to_string());
        base.insert("instance".to_string(), "127.0.0.1:9002".to_string());

        let mut le_100 = base.clone();
        le_100.insert("le".to_string(), "100".to_string());
        let mut le_inf = base.clone();
        le_inf.insert("le".to_string(), "+Inf".to_string());

        let bucket_series = vec![
            RangeSeries {
                metric: "curvine_fuse_io_size_bytes_bucket".to_string(),
                labels: le_100,
                points: vec![RangePoint(1_000, 0.0), RangePoint(61_000, 10.0)],
            },
            RangeSeries {
                metric: "curvine_fuse_io_size_bytes_bucket".to_string(),
                labels: le_inf,
                points: vec![RangePoint(61_000, 10.0)],
            },
        ];

        assert!(histogram_quantile_points(&bucket_series, 0.99, Some(15_000)).is_empty());
    }

    #[test]
    fn histogram_quantile_series_groups_by_operation() {
        let tsdb = InMemoryTsdb::new(3_600_000, 720, 100_000, 2_000_000);
        let mut base = BTreeMap::new();
        base.insert("component".to_string(), "master".to_string());
        base.insert("instance".to_string(), "m1:9010".to_string());
        base.insert("operation".to_string(), "read".to_string());

        let mut le_100 = base.clone();
        le_100.insert("le".to_string(), "100".to_string());
        let mut le_1000 = base.clone();
        le_1000.insert("le".to_string(), "1000".to_string());
        let mut le_inf = base.clone();
        le_inf.insert("le".to_string(), "+Inf".to_string());

        tsdb.insert(
            1_000,
            vec![
                MetricSample {
                    name: "operation_duration_bucket".to_string(),
                    labels: le_100.clone(),
                    value: 0.0,
                },
                MetricSample {
                    name: "operation_duration_bucket".to_string(),
                    labels: le_1000.clone(),
                    value: 0.0,
                },
                MetricSample {
                    name: "operation_duration_bucket".to_string(),
                    labels: le_inf.clone(),
                    value: 0.0,
                },
            ],
        );
        tsdb.insert(
            16_000,
            vec![
                MetricSample {
                    name: "operation_duration_bucket".to_string(),
                    labels: le_100,
                    value: 50.0,
                },
                MetricSample {
                    name: "operation_duration_bucket".to_string(),
                    labels: le_1000,
                    value: 100.0,
                },
                MetricSample {
                    name: "operation_duration_bucket".to_string(),
                    labels: le_inf,
                    value: 100.0,
                },
            ],
        );

        let series = histogram_quantile_series_from_tsdb(
            &tsdb,
            "operation_duration_bucket",
            "master",
            0.99,
            "operation_p99_latency_ms",
            0,
            20_000,
            None,
            1.0,
        );
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points.len(), 1);
        assert!((series[0].points[0].1 - 982.0).abs() < 0.01);

        let scaled = histogram_quantile_series_from_tsdb(
            &tsdb,
            "operation_duration_bucket",
            "master",
            0.99,
            "operation_p99_latency_ms",
            0,
            20_000,
            None,
            US_TO_MS,
        );
        assert_eq!(scaled.len(), 1);
        assert!((scaled[0].points[0].1 - 0.982).abs() < 0.001);
    }

    #[test]
    fn sum_worker_metric_series_aggregates_across_workers() {
        let tsdb = InMemoryTsdb::new(3_600_000, 720, 100_000, 2_000_000);
        let (ts, sample) = worker_sample("num_blocks_to_delete", "w1:9001", 3.0, 1_000);
        tsdb.insert(ts, vec![sample]);
        let (ts, sample) = worker_sample("num_blocks_to_delete", "w2:9001", 5.0, 1_000);
        tsdb.insert(ts, vec![sample]);
        let raw = tsdb.range(
            Some("num_blocks_to_delete"),
            Some("worker"),
            None,
            0,
            20_000,
            None,
        );
        let aggregated = sum_worker_metric_series("num_blocks_to_delete", &raw).expect("series");
        assert_eq!(aggregated.points.len(), 1);
        assert_eq!(aggregated.points[0].1, 8.0);
    }

    #[test]
    fn series_display_name_prefers_operation_over_instance() {
        let mut labels = BTreeMap::new();
        labels.insert("operation".to_string(), "create".to_string());
        labels.insert("instance".to_string(), "m1:9010".to_string());
        assert_eq!(
            series_display_name("operation_count", &labels, true),
            "create (m1:9010)"
        );
    }

    #[test]
    fn series_display_name_distinguishes_metrics_on_same_instance() {
        let mut labels = BTreeMap::new();
        labels.insert("instance".to_string(), "m1:9010".to_string());
        assert_eq!(
            series_display_name("eviction_trigger_count", &labels, true),
            "eviction_trigger_count (m1:9010)"
        );
        assert_eq!(
            series_display_name("eviction_files_deleted", &labels, true),
            "eviction_files_deleted (m1:9010)"
        );
        assert_ne!(
            series_display_name("eviction_trigger_count", &labels, true),
            series_display_name("eviction_files_deleted", &labels, true)
        );
    }

    #[test]
    fn counter_ratio_matches_operation_label() {
        let mut labels_a = BTreeMap::new();
        labels_a.insert("component".to_string(), "master".to_string());
        labels_a.insert("instance".to_string(), "m1:9010".to_string());
        labels_a.insert("operation".to_string(), "GetMasterInfo".to_string());
        let mut labels_b = labels_a.clone();
        labels_b.insert("operation".to_string(), "ListStatus".to_string());
        assert!(labels_match_for_ratio(&labels_a, &labels_a));
        assert!(!labels_match_for_ratio(&labels_a, &labels_b));
    }

    #[test]
    fn counter_ratio_aligns_misaligned_scrape_timestamps() {
        let tsdb = InMemoryTsdb::new(3_600_000, 720, 100_000, 2_000_000);
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "worker".to_string());
        labels.insert("instance".to_string(), "w1:9001".to_string());
        let timestamps = [
            (1_000_i64, 0.0, 0.0),
            (16_000, 1_000.0, 10.0),
            (31_000, 2_500.0, 20.0),
        ];
        let mut samples = Vec::new();
        for (ts, time_us, count) in timestamps {
            samples.push(MetricSample {
                name: "read_time_us".to_string(),
                labels: labels.clone(),
                value: time_us,
            });
            tsdb.insert(ts + 27, std::mem::take(&mut samples));
            samples.push(MetricSample {
                name: "read_count".to_string(),
                labels: labels.clone(),
                value: count,
            });
            tsdb.insert(ts + 54, std::mem::take(&mut samples));
        }
        let series = counter_ratio_series_from_tsdb(
            &tsdb,
            "read_time_us",
            "read_count",
            "worker",
            "read_avg_latency_us",
            0,
            40_000,
            Some(15_000),
            1.0,
        );
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points.len(), 2);
        assert!((series[0].points[0].1 - 100.0).abs() < 0.001);
    }

    #[test]
    fn counter_ratio_pairs_operation_duration_sum_and_count() {
        let tsdb = InMemoryTsdb::new(3_600_000, 720, 100_000, 2_000_000);
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "master".to_string());
        labels.insert("instance".to_string(), "m1:9010".to_string());
        labels.insert("operation".to_string(), "GetMasterInfo".to_string());
        let timestamps = [1_000_i64, 16_000, 31_000];
        let mut samples = Vec::new();
        for (idx, ts) in timestamps.iter().enumerate() {
            let count = (idx as f64 + 1.0) * 10.0;
            let sum = count * 25.0;
            samples.push(MetricSample {
                name: "operation_duration_count".to_string(),
                labels: labels.clone(),
                value: count,
            });
            samples.push(MetricSample {
                name: "operation_duration_sum".to_string(),
                labels: labels.clone(),
                value: sum,
            });
            tsdb.insert(*ts, std::mem::take(&mut samples));
        }
        let series = counter_ratio_series_from_tsdb(
            &tsdb,
            "operation_duration_sum",
            "operation_duration_count",
            "master",
            "operation_avg_latency_ms",
            0,
            40_000,
            None,
            1.0,
        );
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points.len(), 2);
        assert!((series[0].points[0].1 - 25.0).abs() < 0.001);

        let scaled = counter_ratio_series_from_tsdb(
            &tsdb,
            "operation_duration_sum",
            "operation_duration_count",
            "master",
            "operation_avg_latency_ms",
            0,
            40_000,
            None,
            US_TO_MS,
        );
        assert_eq!(scaled.len(), 1);
        assert!((scaled[0].points[0].1 - 0.025).abs() < 0.001);
    }

    fn counter_ratio_series_from_tsdb(
        tsdb: &InMemoryTsdb,
        numerator_metric: &str,
        denominator_metric: &str,
        component: &str,
        derived_metric: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
        value_scale: f64,
    ) -> Vec<DashboardSeries> {
        let denominators = tsdb.range(
            Some(denominator_metric),
            Some(component),
            None,
            start_ms,
            end_ms,
            step_ms,
        );
        tsdb.range(
            Some(numerator_metric),
            Some(component),
            None,
            start_ms,
            end_ms,
            step_ms,
        )
        .into_iter()
        .filter_map(|numerator| {
            let denominator = denominators
                .iter()
                .find(|candidate| labels_match_for_ratio(&numerator.labels, &candidate.labels))?;
            let points = scale_points(
                counter_delta_ratio_points(&numerator.points, &denominator.points, step_ms),
                value_scale,
            );
            if points.is_empty() {
                return None;
            }
            let mut labels = numerator.labels;
            labels.insert("derived_metric".to_string(), derived_metric.to_string());
            Some(dashboard_series(
                series_display_name(derived_metric, &labels, false),
                labels,
                points,
            ))
        })
        .collect()
    }

    fn histogram_quantile_series_from_tsdb(
        tsdb: &InMemoryTsdb,
        bucket_metric: &str,
        component: &str,
        quantile: f64,
        derived_metric: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
        value_scale: f64,
    ) -> Vec<DashboardSeries> {
        let buckets = tsdb.range(
            Some(bucket_metric),
            Some(component),
            None,
            start_ms,
            end_ms,
            step_ms,
        );
        let mut groups: BTreeMap<BTreeMap<String, String>, Vec<RangeSeries>> = BTreeMap::new();
        for series in buckets {
            if !series.labels.contains_key("le") {
                continue;
            }
            let mut group_labels = series.labels.clone();
            group_labels.remove("le");
            groups.entry(group_labels).or_default().push(series);
        }

        groups
            .into_iter()
            .filter_map(|(group_labels, bucket_series)| {
                let points = scale_points(
                    histogram_quantile_points(&bucket_series, quantile, step_ms),
                    value_scale,
                );
                if points.is_empty() {
                    return None;
                }
                let mut labels = group_labels;
                labels.insert("derived_metric".to_string(), derived_metric.to_string());
                Some(dashboard_series(
                    series_display_name(derived_metric, &labels, false),
                    labels,
                    points,
                ))
            })
            .collect()
    }
}
