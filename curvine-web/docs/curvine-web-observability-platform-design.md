# Curvine Web Observability and Operations Platform Design Proposal

Related work: standalone `curvine-web` admin console, master/worker/Transfer/FUSE Prometheus `/metrics` endpoints, Curvine operations dashboard.

## Summary

This document proposes an incremental design for turning `curvine-web` into a one-stop Curvine operations and observability platform.

The first milestone adds a built-in metrics collector to `curvine-web`: it discovers Curvine masters and workers, scrapes their existing Prometheus text `/metrics` endpoints, stores recent samples in an in-memory ring-buffer TSDB, and exposes authenticated JSON APIs for a Vue/ECharts monitoring page.

The production direction keeps the same `curvine-web` UI and API surface, but adds a `PrometheusProvider` later so production deployments can use Prometheus as the long-term metrics and alerting backend. The built-in collector remains the zero-dependency, out-of-box, and fallback path.

In short:

```text
Phase 1:
Curvine master/worker /metrics -> curvine-web built-in collector -> curvine-web dashboard

Production extension:
Curvine master/worker /metrics -> Prometheus -> curvine-web dashboard
```

This avoids making `curvine-web` a full Grafana replacement. Instead, `curvine-web` provides Curvine-specific operational views, diagnosis, and management actions on top of metrics data.

## Motivation

Curvine already has an admin UI and several control APIs, but operators still need a unified place to answer operational questions:

- Is the cluster healthy right now?
- Which master is active, and are standby masters reachable?
- How many workers are live, lost, blacklisted, or decommissioned?
- Which worker is close to capacity or has failed disks?
- Are read/write throughput, block count, memory usage, and pending deletion blocks abnormal?
- Is journal apply/commit/queue behavior healthy?
- Are Curvine metrics targets reachable and being scraped?
- When a metric is abnormal, where should the operator click next: worker detail, master HA, jobs, mounts, or file system?

Today, these workflows are split across raw `/metrics`, master APIs, worker pages, shell commands, and potentially external dashboards. That does not provide a one-stop operations experience.

The design goal is to make `curvine-web` the Curvine-native operations console:

```text
observe -> diagnose -> navigate to Curvine object -> operate
```

## Current State

### `curvine-web` Architecture

`curvine-web` is already an independent service:

- Binary entrypoint: `curvine-web/src/bin/curvine-web.rs`
- Service wrapper: `curvine-web/src/server/admin_web_service.rs`
- HTTP server: `curvine-web/src/server/web_server.rs`
- Main admin router: `curvine-web/src/router/admin_handler.rs`
- Frontend: `curvine-web/webui/`

> **Note (later change):** the frontend has since been migrated to
> `curvine-web/webui-next/` (React 18 + Vite + Tailwind). The Vue paths quoted
> throughout this document describe the original implementation and are kept as
> the design record; the React module is the one that ships.

The backend constructs:

```text
AdminWebService -> AdminRouterHandler -> authenticated /api/v1 routes
```

`AdminRouterHandler` uses Curvine client/RPC paths such as `UnifiedFileSystem` and master info calls to communicate with the active master.

The frontend is Vue 3 + Vite:

- Axios wrapper: `curvine-web/webui/src/utils/request.ts`
- API wrapper: `curvine-web/webui/src/api/client.ts`
- Router: `curvine-web/webui/src/router/index.ts`
- Shell layout/navigation: `curvine-web/webui/src/layouts/AdminLayout.vue`
- Existing pages: `Overview.vue`, `Workers.vue`, `WorkerDetail.vue`, `Jobs.vue`, `Mounts.vue`, `Config.vue`
- Existing summary card: `components/admin/MetricCard.vue`

### Existing Metrics Endpoints

Curvine already exposes Prometheus text format metrics:

- Master `/metrics`: `curvine-server/src/master/router_handler.rs`
- Worker `/metrics`: `curvine-server/src/worker/handler/router_handler.rs`
- Transfer `/metrics`: `crates/server/curvine-data-transfer/src/transfer/router_handler.rs`
- FUSE `/metrics`: exposed through the FUSE metrics web endpoint

The master metrics are registered in:

```text
curvine-server/src/master/master_metrics.rs
```

The worker metrics are registered in:

```text
curvine-server/src/worker/worker_metrics.rs
```

### Existing Master Metrics

Useful first-release master metrics include:

```text
capacity
available
fs_used
num_blocks
blocks_size_avg
worker_num{tag="live|blacklist|decommission|lost"}
journal_queue_len
journal_flush_count
journal_flush_time
journal_committed
journal_term
journal_applied
journal_ufs_applied
used_memory_bytes
rocksdb_metrics{tag}
inode_dir_num
inode_file_num
operation_duration{operation}    # histogram family: _bucket/_sum/_count in exposition
replication_staging_number
replication_inflight_number
replication_failure_count
eviction_lru_cache_size
eviction_trigger_count
eviction_files_deleted
eviction_bytes_freed
ttl_bucket_len
ttl_total_inodes
```

### Existing Worker Metrics

Useful first-release worker metrics include:

```text
write_bytes
write_time_us
write_count
write_blocks{type}
read_bytes
read_time_us
read_count
read_blocks{type}
capacity
available
fs_used
failed_disks
num_blocks
total_disks
num_blocks_to_delete
used_memory_bytes
```

### Existing Transfer Metrics

Transfer exposes its own Prometheus text `/metrics` endpoint on `transfer.web_port`
(`9011` by default). Useful metrics include:

```text
transfer_active_jobs
transfer_executing_jobs
transfer_pending_jobs
transfer_tasks_pending
transfer_tasks_running
transfer_tasks_completed
transfer_tasks_failed
transfer_tasks_stale
transfer_submit_total{kind,result}
transfer_terminal_total{state,reason}
transfer_task_report_queue_len
transfer_task_report_queue_len_by_lane{lane}
transfer_task_report_total{result}
transfer_task_report_rpc_total{type}
transfer_task_report_batch_tasks_total
transfer_completed_bytes_total{state}
transfer_acquire_total{result}
transfer_planning_total{kind,result}
transfer_dispatch_total{result}
transfer_lease_renew_total{result}
transfer_stale_retry_total{result}
transfer_requeue_total{reason}
transfer_worker_reject_total{class}
transfer_worker_inflight_count{worker_id}
transfer_worker_inflight_bytes{worker_id}
transfer_store_operation_duration_us{backend,operation,result}
transfer_store_operation_total{backend,operation,result}
transfer_store_unavailable{backend}
transfer_store_unavailable_total{backend,operation}
transfer_store_unavailable_duration_us_total{backend}
transfer_metadata_operation_duration_us{source,operation,result}
transfer_metadata_operation_total{source,operation,result}
transfer_cluster_snapshot_version
transfer_cluster_snapshot_staleness_ms
transfer_cluster_snapshot_live_workers
transfer_cluster_snapshot_capable_workers
transfer_cluster_snapshot_refresh_total{result}
transfer_cluster_snapshot_refresh_duration_us{result}
```

The `transfer_tasks_*` gauges are store-wide state counts; `transfer_task_report_rpc_total{type}`
uses `type=single|batch` to show how many progress reports arrive as single versus batch
envelopes, `transfer_task_report_batch_tasks_total` counts the tasks carried inside batch
envelopes, and `transfer_requeue_total{reason}` / `transfer_worker_reject_total{class}` /
`transfer_worker_inflight_*{worker_id}` expose the backpressure control plane.

### Worker Transfer Task Metrics

Each `curvine-worker` exposes its transfer-task metrics on `worker.web_port`
(`9002` by default):

```text
worker_transfer_tasks_active
worker_transfer_tasks_queued
worker_transfer_tasks_running
worker_transfer_terminal_retained_tasks
worker_transfer_terminal_retention_age_seconds
worker_transfer_task_evicted_total{reason}
worker_transfer_task_permits_available
worker_transfer_task_permits_max
worker_transfer_task_submit_total{result}
worker_transfer_task_submit_rejected_total{reason}
worker_transfer_task_terminal_total{state,kind}
worker_transfer_task_failed_total{reason}
worker_transfer_task_report_total{result}
worker_transfer_task_copy_bytes_total{state}
worker_transfer_task_execution_duration_us{state}
worker_transfer_task_read_duration_us{state}
worker_transfer_task_write_duration_us{state}
worker_transfer_task_queue_duration_us{result}
```

`worker_transfer_tasks_active` is reconciled from the task-store size, not
incremented per submission, so a superseded submit cannot make it drift.

A finished task stays in the store only while it can still answer a scheduler
stale probe. A report the Store refused (fencing) released its attempt's
ownership, so the probe will never ask about that context: it is dropped
immediately and counted in `worker_transfer_task_evicted_total{reason="fenced"}`.
A report that could not be delivered is retained for
`transfer.worker_terminal_retention` (default `10m`, floor `2m`) and then reaped,
counted as `reason="expired"`. `worker_transfer_terminal_retained_tasks` and
`worker_transfer_terminal_retention_age_seconds` are the gauges that show that
window: the age is `-1` when nothing is retained. Before this bound existed an
undelivered or fenced terminal report kept a whole task context, including the
encoded source block layout, for the life of the worker process.

### Transfer Discovery Feasibility

Transfer is a single logical service in the default deployment. When
`transfer.enabled` is true, `curvine-web` discovers its scrape target from
`transfer.hostname` and `transfer.web_port`. The observability collector treats
Transfer as a first-class target: it is included in target health, builtin
scrape coverage, Prometheus coverage checks, the Transfer chart group, and the
instance filter on the Observability page.

### Worker Discovery Feasibility

Worker auto-discovery is feasible because the master already reports workers through `MasterInfo`:

```text
live_workers
blacklist_workers
decommission_workers
lost_workers
```

Each `WorkerInfo` contains a `WorkerAddress` with fields such as:

```text
worker_id
hostname
ip_addr
rpc_port
web_port
```

Important note: `WorkerInfo.address` already contains `web_port`, and the worker registers its real web port when it joins the cluster. The worker metrics endpoint must therefore prefer:

```text
WorkerInfo.address.web_port when > 0
```

and only fall back to:

```text
conf.worker.web_port
```

when the worker-reported port is missing or zero. This matters because `curvine-web` may be running with a config file whose default worker web port does not match the actual worker process. Using only `conf.worker.web_port` would make the collector systematically scrape the wrong port in that case.

The endpoint host construction must prefer a routable hostname/IP and fall back carefully when only loopback addresses are present.

`curvine-web` already has address-normalization helpers in `admin_handler.rs`, including `is_loopback_hostname`, `resolve_display_hostname`, and `resolve_display_ip`. Phase 0 should extract the reusable parts into a shared helper module, for example `curvine-web/src/util/addr.rs` or `curvine-web/src/observability/discovery.rs`, rather than implementing a third copy. The Observability page should resolve master/worker hosts consistently with the existing Overview and Workers pages.

### Current Gap

`curvine-web` currently has useful admin pages and a master overview, but it does not have:

- a time-series collector,
- a metrics target registry,
- worker metrics auto-discovery,
- a metrics query API,
- a charting frontend,
- target health diagnosis,
- Prometheus compatibility abstraction.

## Goals

1. Add a first-class `Observability` area to `curvine-web`.
2. Provide an out-of-box monitoring experience without requiring external Prometheus.
3. Auto-discover workers through master state.
4. Scrape existing master/worker `/metrics` endpoints with configurable interval and timeout.
5. Parse Prometheus text format into low-cardinality time series.
6. Store recent samples in memory using bounded ring buffers.
7. Expose authenticated JSON APIs for targets, instant values, range queries, and metric names.
8. Add Vue/ECharts dashboards for cluster, master, worker, Transfer, FUSE, and target health.
9. Keep the backend extensible so a future `PrometheusProvider` can reuse the same frontend/API.
10. Preserve existing admin functionality and ensure scrape failures never break core `curvine-web` routes.

## Non-Goals

- Do not replace Prometheus, Grafana, Alertmanager, Thanos, Mimir, or VictoriaMetrics.
- Do not implement long-term durable metrics storage in `curvine-web`.
- Do not implement production alert rule evaluation in `curvine-web` in the first milestone.
- Do not require every user to manually deploy Prometheus for the first observability release.
- Do not expose arbitrary PromQL editing in the first UI.
- Do not add high-cardinality labels such as path, block id, request id, or file name.
- Do not provide histogram quantiles such as P50/P95/P99 in the first UI. Histogram buckets are stored as ordinary series in the first milestone; dashboards may show bucket/count/rate views, but quantile calculation is deferred.
- Do not redesign the existing `orpc::common::Metrics` registry.
- Do not change the master/worker metrics payloads in the first PR unless a small compatibility fix is required.

## Product Positioning

The platform has two modes:

```text
builtin mode:
  curvine-web discovers and scrapes Curvine targets itself.
  Good for development, local clusters, demos, small deployments, and fallback.

prometheus mode:
  Prometheus scrapes Curvine targets.
  curvine-web queries Prometheus and renders Curvine-specific operations pages.
  Good for production.
```

The first implementation targets `builtin mode`. The API and module names should still use `observability` rather than `metrics` so the production provider can be added later without renaming the frontend.

## Target Architecture

```text
                 curvine-web
┌────────────────────────────────────────────────────────────────┐
│                                                                │
│  AdminRouterHandler                                            │
│    ├─ existing /api/v1/overview, /workers, /jobs, ...           │
│    └─ new /api/v1/observability/*                              │
│                                                                │
│  ObservabilityService                                          │
│    ├─ Discovery                                                │
│    │   ├─ configured masters                                   │
│    │   └─ workers from MasterInfo                              │
│    │                                                           │
│    ├─ MetricsProvider                                          │
│    │   ├─ BuiltinProvider                                      │
│    │   │   ├─ ScrapePool                                       │
│    │   │   ├─ Prometheus text parser                           │
│    │   │   └─ InMemoryTsdb                                     │
│    │   └─ PrometheusProvider (future)                          │
│    │                                                           │
│    ├─ DashboardService                                         │
│    └─ TargetHealth                                             │
│                                                                │
└────────────────────────────────────────────────────────────────┘
             │                                   ▲
             │ JSON                              │ HTTP GET /metrics
             ▼                                   │
┌─────────────────────────┐       ┌──────────────────────────────┐
│ Vue Observability pages │       │ master:conf.master.web_port  │
│ ECharts panels          │       │ worker-1..N:worker_web_port  │
└─────────────────────────┘       └──────────────────────────────┘
```

## Module Layout

Add a new backend module:

```text
curvine-web/src/util/
  addr.rs

curvine-web/src/observability/
  mod.rs
  api.rs
  model.rs
  discovery.rs
  prom_parse.rs
  scraper.rs
  tsdb.rs
  collector.rs
  service.rs
```

Suggested responsibilities:

### `model.rs`

Shared structs:

```rust
MetricTarget
MetricTargetKind      // Master | Worker
MetricTargetStatus    // Up | Down | Unknown
MetricSample
SeriesKey
SeriesPoint
InstantMetric
RangeQuery
RangeSeries
ProviderHealth
```

Important target labels:

```text
component = master | worker
instance = host:web_port
worker_id = <id>       # only for workers when known
worker_state = live | blacklist | decommission | lost
```

These labels are mandatory ingestion labels, not just query-time filters. They must be injected into every parsed sample before writing to the TSDB. Master and worker metrics currently share names such as:

```text
capacity
available
fs_used
num_blocks
used_memory_bytes
```

Without injected `component` and `instance` labels, samples from master and worker targets would collide into the same `SeriesKey`, corrupting charts and instant queries. The collector must therefore normalize each scrape result as:

```text
raw Prometheus labels + component + instance + optional worker_id + optional worker_state
```

before constructing the `SeriesKey`.

### `discovery.rs`

Discovers scrape targets.

Discovery should reuse the existing Curvine client path to fetch master state. Concretely, `ObservabilityService` should use `UnifiedFileSystem::get_master_info()` or an equivalent existing helper/callback to get `MasterInfo`; it should not reimplement the master RPC protocol inside the observability module.

Master targets:

- derive from `conf.client.master_addrs`,
- include current active master when known,
- later include HA standby information from existing master HA helper.

Master metrics port rule:

- the host comes from the active/configured master endpoint,
- the metrics port comes from `conf.master.web_port`,
- never use the RPC port embedded in `MasterInfo.active_master` as the metrics port.

The standalone curvine-web admin console uses `[web].port`; `conf.master.web_port` is reserved for the master process metrics-only HTTP listener.

Worker targets:

- call existing master info logic,
- read `MasterInfo.live_workers`, `blacklist_workers`, `decommission_workers`, `lost_workers`,
- construct `http://host:worker_web_port/metrics`.

Worker metrics port rule:

- use `WorkerInfo.address.web_port` when it is greater than zero,
- otherwise fall back to `conf.worker.web_port`,
- record which source was used in target metadata for debugging if practical.

Host selection rules:

1. Prefer a routable address from the perspective of the `curvine-web` process, not merely the address that looks nicest in the UI.
2. In bare-metal deployments, prefer non-loopback `WorkerInfo.address.hostname` when it is resolvable/reachable from `curvine-web`.
3. In container or Kubernetes deployments, prefer `WorkerInfo.address.ip_addr` when the hostname is a pod name, short hostname, or otherwise not routable from `curvine-web`.
4. Add a future config knob if needed, for example `web.observability.prefer_worker_ip = true`, so operators can force IP-based scrape targets.
5. If both hostname and IP are loopback, still expose the target but mark it with a warning reason.
6. Never fail the whole discovery pass because one worker address is malformed.

### `prom_parse.rs`

Parses Prometheus text format.

First release can support:

```text
metric_name value
metric_name{label="value"} value
```

Ignore:

```text
# HELP
# TYPE
comments
blank lines
```

Treat histogram series as ordinary metric samples:

```text
operation_duration_bucket{...}
operation_duration_sum{...}
operation_duration_count{...}
```

This is enough for storage and charting. Histogram-aware quantile views can be added later.

Implementation options:

- Prefer a mature lightweight parser if dependency policy allows it.
- Otherwise implement a minimal parser locally to avoid pulling in extra dependencies.

Parser requirements:

- stable label sorting,
- no panic on malformed lines,
- line-level parse errors counted and exposed in target health,
- metric values parsed as `f64`,
- timestamps in Prometheus exposition lines can be ignored initially.

Parser decision for Phase 1:

- Use a local minimal parser first unless an existing dependency exposes a suitable Prometheus text parser with a small API surface.
- Do not leave parser strategy open when Phase 1 starts.
- The Phase 1 parser only needs to support the Curvine master/worker exposition shape listed above; full Prometheus exposition compatibility can be added later if real payloads require it.

### `scraper.rs`

Fetches one target:

```text
GET http://target/metrics
timeout: default 2s or 3s
```

Returns:

```text
samples
duration_ms
status
error
payload_bytes
parse_error_count
```

Scrape requirements:

- timeout each target,
- bound response size if practical,
- do not follow arbitrary redirects in the first release,
- failure updates target health but does not break the collector loop,
- use concurrent scraping with a max concurrency limit.

HTTP client decision:

- Add `reqwest` to `curvine-web` with minimal features, matching the existing `curvine-cli` pattern:

```toml
reqwest = { version = "0.11", features = ["json", "rustls-tls"], default-features = false }
```

`curvine-web` currently has no HTTP client dependency. The workspace already uses `hyper`, and `curvine-cli` already uses `reqwest`; for this collector, `reqwest` gives simpler timeout and HTTP response handling with a small, explicit dependency. If the workspace later standardizes on `reqwest 0.12`, this dependency should follow that workspace decision, but Phase 1 should not leave the HTTP client choice open.

### `tsdb.rs`

In-memory bounded storage:

```text
HashMap<SeriesKey, VecDeque<SeriesPoint>>
```

`SeriesKey`:

```text
metric_name
sorted_labels
```

`SeriesPoint`:

```text
timestamp_ms
value
```

Default retention:

```text
scrape_interval = 15s
retention = 1h
points_per_series ~= 240
```

The store should prune old points on write. It should also expose:

```text
instant(metric selectors)
range(metric selectors, start, end, step)
names()
series_count()
sample_count()
```

First release can return raw stored points without server-side downsampling. If `step` is provided, the query can pick the closest point per step bucket or simply return raw points and let the frontend handle dense data. Keep the behavior documented.

### `collector.rs`

Background loop:

```text
loop:
  discover targets
  scrape targets concurrently
  write samples to TSDB
  update target health
  sleep scrape_interval
```

The collector is spawned through the existing `Runtime` held by `AdminWebService`/`AdminRouterHandler`.

The collector should start when `AdminRouterHandler` is created, but it must not block router initialization.

The routing integration must remain thin. `admin_handler.rs` is already large, so observability must not add collector, scrape, parser, or TSDB logic there. `AdminRouterHandler` should only:

- hold `Arc<ObservabilityService>`,
- call a helper that returns an observability `Router`,
- merge/nest that router under `/api/v1/observability`.

The route handlers themselves should live in the observability module, for example:

```text
curvine-web/src/observability/api.rs
```

or inside `service.rs` if the handler surface stays small.

Shutdown can be best-effort in the first release; no persistent state is required.

### `service.rs`

Owns shared observability state:

```rust
pub struct ObservabilityService {
    conf: ClusterConf,
    // Or an equivalent callback/helper that returns MasterInfo.
    // The service should reuse UnifiedFileSystem::get_master_info()
    // instead of reimplementing master RPC.
    unified_fs: UnifiedFileSystem,
    tsdb: Arc<InMemoryTsdb>,
    targets: Arc<RwLock<TargetRegistry>>,
    collector_handle: ...,
}
```

Later this should become:

```rust
pub struct ObservabilityService {
    provider: Arc<dyn MetricsProvider>,
}
```

This makes the future `PrometheusProvider` a provider swap rather than a frontend/API rewrite.

## Configuration

First release can use defaults and environment variables, then formalize TOML config.

Recommended final config:

```toml
[web.observability]
enabled = true              # dev/single-node default; production packaging may default to false
mode = "builtin"             # builtin | prometheus | auto
scrape_interval = "15s"
scrape_timeout = "3s"
retention = "1h"
max_concurrent_scrapes = 32
max_series = 100000
max_samples_per_series = 720

[web.observability.prometheus]
url = ""
query_timeout = "5s"
fallback_builtin = true
```

Default for the first release:

```text
enabled = true for development/single-node distributions
enabled = configurable for production packaging
mode = builtin
scrape_interval = 15s
scrape_timeout = 3s
retention = 1h
```

Production packages should allow `web.observability.enabled=false`. Environments that use `curvine-web` only as an admin console, or where worker metrics ports are intentionally isolated, should be able to disable the scrape loop to avoid repeated failed requests and noisy logs. The safest production default can be revisited per packaging target.

The `[web.observability]` structure does not exist in `ClusterConf` yet. Phase 1 may start with hard-coded defaults and/or environment variables, but the implementation plan must decide when to add the formal config structs to `curvine-common`. The final TOML shape above is the target, not a statement that the config already exists.

## Backend API

Add authenticated routes under `/api/v1/observability/*`.

Do not use `/api/v1/metrics/*`; that name can be confused with the raw Prometheus `/metrics` endpoint.

All new APIs must use the same response envelope as existing `/api/v1` handlers. The frontend `api/client.ts` `unwrap()` helper expects:

```json
{
  "success": true,
  "data": {},
  "error": null
}
```

The examples below show the `data` payload for readability. Implementations should return `api_success(data)` or the equivalent existing helper, not bare JSON.

### `GET /api/v1/observability/health`

Returns provider and collector health.

Example:

```json
{
  "enabled": true,
  "mode": "builtin",
  "active_provider": "builtin",
  "scrape_interval_ms": 15000,
  "retention_ms": 3600000,
  "series_count": 1280,
  "sample_count": 204800,
  "last_discovery_ms": 1760000000000,
  "last_collect_ms": 1760000001000
}
```

### `GET /api/v1/observability/targets`

Returns discovered targets and health.

Lost workers should be listed as targets by default, with `worker_state=lost`. Their scrape state may be down or unreachable; that is not the same as the Curvine state. This is the adopted Phase 2 default rather than an unresolved behavior choice.

Example:

```json
{
  "targets": [
    {
      "id": "master:cv-master-0:8997",
      "component": "master",
      "instance": "cv-master-0:8997",
      "url": "http://cv-master-0:8997/metrics",
      "up": true,
      "last_scrape_ms": 1760000000000,
      "last_scrape_duration_ms": 34,
      "last_error": "",
      "sample_count": 42
    },
    {
      "id": "worker:12",
      "component": "worker",
      "instance": "cv-worker-12:8998",
      "worker_id": 12,
      "worker_state": "live",
      "url": "http://cv-worker-12:8998/metrics",
      "up": true,
      "last_scrape_ms": 1760000000000,
      "last_scrape_duration_ms": 28,
      "last_error": "",
      "sample_count": 24
    }
  ]
}
```

### `GET /api/v1/observability/names`

Returns known metric names and optionally label keys.

Example:

```json
{
  "names": [
    "capacity",
    "available",
    "fs_used",
    "read_bytes",
    "write_bytes",
    "journal_queue_len"
  ]
}
```

### `GET /api/v1/observability/instant`

Returns latest values for one or more metrics.

Query options:

```text
metric=capacity
component=worker
instance=...
```

Example response:

```json
{
  "series": [
    {
      "metric": "fs_used",
      "labels": {
        "component": "worker",
        "instance": "worker-1:8998"
      },
      "timestamp_ms": 1760000000000,
      "value": 123456789
    }
  ]
}
```

### `GET /api/v1/observability/query`

Returns range series for charts.

Query options:

```text
metric=fs_used
component=worker
instance=worker-1:8998
range=1h
step=15s
```

Example response:

```json
{
  "series": [
    {
      "metric": "fs_used",
      "labels": {
        "component": "worker",
        "instance": "worker-1:8998"
      },
      "points": [
        [1760000000000, 123456789],
        [1760000015000, 123556789]
      ]
    }
  ]
}
```

### `GET /api/v1/observability/dashboard`

Optional but recommended after the primitive APIs are in place.

Returns a curated Curvine dashboard payload so the frontend does not need to know every metric mapping.

This is also the best seam for future Prometheus integration:

```text
frontend -> dashboard API -> BuiltinProvider or PrometheusProvider
```

## Frontend Design

Add an `Observability` route:

```text
curvine-web/webui/src/views/Observability.vue
```

Router:

```text
/observability
```

Navigation:

```text
Observability
```

API client additions:

```ts
fetchObservabilityHealth()
fetchObservabilityTargets()
fetchObservabilityNames()
fetchInstantMetrics(params)
queryMetrics(params)
fetchObservabilityDashboard(params)
```

Chart library:

- Add `echarts`.
- `vue-echarts` is optional. A small local wrapper component around ECharts is also acceptable.

Reusable components:

```text
TimeSeriesChart.vue
TargetHealthTable.vue
MetricSelector.vue
```

### First Dashboard Layout

The first release should be dense and operational, not marketing-style.

To avoid hard-coding too many raw metric names before the curated dashboard API exists, Phase 3 should stay intentionally narrow:

```text
Phase 3 frontend:
  - Health and Targets table
  - metric names list
  - 1 or 2 raw query charts for smoke testing

Phase 4:
  - /api/v1/observability/dashboard
  - complete curated panels
  - remove temporary frontend metric maps
```

If Phase 3 needs more panels for demo purposes, keep metric-name mappings in a single temporary constant file and explicitly delete or replace it in Phase 4.

For the Phase 3 raw charts:

```text
Cluster Capacity:
  query component=master series such as capacity and fs_used.

Worker Read Throughput:
  either show per-worker read_bytes rate as multiple series,
  or compute a simple frontend sum across worker instances.
```

Do not overbuild aggregation in Phase 3; backend-curated aggregation belongs in the Phase 4 dashboard API.

Top controls:

```text
time range: 15m | 1h | 6h | 24h
target filter: all | master | worker | specific instance
refresh
auto refresh
provider status
```

Top metric cards:

The following is the target-state full layout after the Phase 4 dashboard API lands. Phase 3 should implement only the narrow subset described above: health, targets, names, and one or two raw query charts.

```text
Cluster Health
Active Master
Live Workers
Lost Workers
Used Capacity
Read Throughput
Write Throughput
Failed Disks
```

Panels:

```text
Cluster Capacity
Worker Read Throughput
Worker Write Throughput
Journal Queue
Worker Capacity by Node
Failed Disks
Memory Usage
Data Sources
```

### Future Dashboard Layout

After the first release, split into tabs or collapsible sections:

```text
Overview
Master
Workers
Jobs
Alerts
Data Sources
Diagnostics
```

The long-term page should connect observations to operations:

```text
worker capacity high -> Worker Detail
worker down -> Worker Detail + target health
master unhealthy -> Master HA
job failure -> Jobs
mount issue -> Mounts
scrape missing -> Data Sources diagnostics
```

## Metric Semantics and Derived Values

Some metrics are gauges:

```text
capacity
available
fs_used
num_blocks
failed_disks
used_memory_bytes
journal_queue_len
```

Some metrics are counters:

```text
read_bytes
read_count
write_bytes
write_count
journal_flush_count
replication_failure_count
```

The first release can store all raw values. The UI can compute simple rates from adjacent points:

```text
rate = (current - previous) / time_delta_seconds
```

Counter reset handling:

- If delta is negative, treat the rate as unknown or zero for that point.
- Do not show negative throughput.

Later, the backend can provide derived rate series:

```text
read_bytes_per_second
write_bytes_per_second
read_ops_per_second
write_ops_per_second
```

## Target Health Semantics

Each target keeps:

```text
up
last_scrape_ms
last_success_ms
last_scrape_duration_ms
last_error
sample_count
parse_error_count
```

Rules:

- HTTP success and parse success with at least one sample -> `up = true`
- HTTP failure -> `up = false`
- timeout -> `up = false`, error message includes timeout
- parse errors on some lines -> keep `up = true` if valid samples exist, record `parse_error_count`
- no valid samples -> `up = false`

Lost workers:

- If a worker is marked lost by master, include it in targets with `worker_state=lost`.
- Scraping may fail. This is expected and should be shown as target down.
- The UI should distinguish "lost in Curvine state" from "scrape target unreachable".

## Security and Exposure

The new JSON APIs are under the existing authenticated `/api/v1` route group.

The raw master/worker `/metrics` endpoints may still be unauthenticated depending on deployment. This proposal does not change that exposure model.

Operational guidance:

- production deployments should restrict raw `/metrics` ports to Prometheus/curvine-web networks,
- do not expose metrics ports publicly,
- consider a later proposal for bind address, auth, or TLS on metrics endpoints.

`curvine-web` should not proxy arbitrary URLs supplied by the browser. Targets must come from Curvine discovery/config, not from user-provided URL parameters.

## Performance and Resource Bounds

The collector must be bounded.

Recommended defaults:

```text
scrape_interval = 15s
scrape_timeout = 3s
retention = 1h
max_concurrent_scrapes = 32
max_response_bytes = 4 MiB per target
```

Memory estimate:

```text
series_count * points_per_series * point_size
```

For example:

```text
100 workers
30 series per worker
240 points per series
~720,000 points
```

This is acceptable for a lightweight in-memory store if labels and points are stored carefully. Hard caps should still exist:

```text
max_series
max_samples_per_series
max_total_samples
```

When caps are exceeded:

- drop oldest points first,
- avoid panics,
- expose a warning in `/observability/health`.

`rocksdb_metrics{tag}` may create more series than the simple capacity/IO gauges, especially as RocksDB exposes more internal counters. The first release can ingest it like any other metric, but `max_series` pressure must be visible in `/observability/health`. If large clusters hit the cap, Phase 4 dashboard APIs should use an explicit metric/tag whitelist for heavy metric families.

## Failure Modes

The collector is best-effort. It must not break the admin console.

Failure examples and behavior:

```text
master metrics unreachable:
  mark target down, keep existing samples, continue loop

one worker metrics unreachable:
  mark that worker target down, continue scraping others

worker metrics port blocked by firewall or NetworkPolicy:
  mark scrape target down, show the network error in Data Sources, and keep Curvine worker state separate from scrape reachability

master info discovery fails:
  keep previous targets for a grace period, mark discovery error

parser fails on one line:
  count parse error, continue parsing next line

TSDB full:
  evict old samples, expose health warning

frontend chart query too broad:
  backend should cap result size or return a clear error
```

The built-in collector requires HTTP reachability from the `curvine-web` process to each master/worker web metrics port. Master RPC reachability alone is not enough for built-in scraping. The Data Sources page must distinguish:

```text
Curvine state:
  live | blacklist | decommission | lost

Metrics scrape state:
  up | down | timeout | connection refused | network policy blocked
```

## Production Prometheus Compatibility

The first release is built-in collector only. The design should still prepare for:

```text
PrometheusProvider
```

Provider behavior:

```text
BuiltinProvider:
  query local InMemoryTsdb

PrometheusProvider:
  call Prometheus HTTP API /api/v1/query and /api/v1/query_range
```

The frontend must not care which provider is active.

Recommended final modes:

```text
builtin:
  only built-in collector

prometheus:
  only Prometheus query provider

auto:
  prefer Prometheus, fallback to built-in collector
```

`auto` mode must avoid doubling scrape load. When Prometheus is healthy and selected as the active provider, the built-in collector should not continuously scrape the same master/worker targets. Acceptable implementations:

```text
Option A:
  do not start the built-in scrape loop while Prometheus is healthy;
  start it only after Prometheus health checks fail.

Option B:
  keep BuiltinProvider initialized for fallback queries, but set its scrape loop disabled/scrape_interval=0 until fallback is active.
```

Do not run full-rate Prometheus scraping and full-rate built-in scraping at the same time unless an operator explicitly enables that for debugging.

Production default later:

```text
mode = prometheus
fallback_builtin = true
```

The same `ObservabilityService` should expose:

```text
active_provider = builtin | prometheus
provider_health
fallback_reason
```

## Deployment Experience

To keep the platform "one-stop", users should not be forced to hand-craft monitoring from scratch.

First release:

- no Prometheus required,
- `curvine-web` observes Curvine targets itself.

Production release:

- provide `prometheus.yml` examples,
- provide Kubernetes `ServiceMonitor`/`PodMonitor` templates,
- provide `PrometheusRule` examples,
- add a `Data Sources` page that verifies whether Prometheus is scraping all Curvine targets.

Future Helm/compose options:

```text
observability.enabled=true
observability.mode=builtin|prometheus|auto
observability.prometheus.enabled=true
observability.prometheus.existingUrl=http://...
```

This keeps the user experience one-stop while still following production monitoring best practices.

## Implementation Plan

### Phase 0 - Design and dependencies

Scope:

- Add this design document.
- Add `reqwest` to `curvine-web` with minimal features, following the existing `curvine-cli` dependency style unless the workspace has standardized on a newer version.
- Decide and record the Prometheus text parser strategy before Phase 1 starts; default recommendation is a local minimal parser.
- Confirm worker web endpoint construction from `WorkerInfo.address.web_port` with fallback to `conf.worker.web_port`.
- Confirm master metrics endpoint construction from master host + `conf.master.web_port`.
- Extract or centralize reusable address helpers so discovery stays aligned with `admin_handler.rs` behavior (`is_loopback_hostname`, `resolve_display_hostname`, `resolve_display_ip`).
- Confirm where to spawn the collector from `AdminRouterHandler`/`AdminWebService`.
- Confirm observability route registration lives in the observability module, not inline in `admin_handler.rs`.

Expected outcome:

- no runtime behavior change,
- review agreement on module boundaries and API names.

### Phase 1 - Backend minimal collector, master first

Scope:

- Add `observability` module skeleton.
- Add `MetricTarget`, `MetricSample`, `SeriesKey`, `InMemoryTsdb`.
- Add minimal Prometheus text parser.
- Add single-target scraper.
- Discover and scrape the active/configured master only.
- Store samples in memory.
- Inject mandatory scrape labels before TSDB writes:

```text
component=master
instance=<master-host>:<conf.master.web_port>
```

- Add:

```text
GET /api/v1/observability/health
GET /api/v1/observability/targets
GET /api/v1/observability/instant
GET /api/v1/observability/names
```

Expected outcome:

- `curvine-web` can scrape master `/metrics`,
- operator can see target health and latest master metric values through JSON APIs.

### Phase 2 - Worker auto-discovery and concurrent scraping

Scope:

- Implement worker discovery from `MasterInfo`.
- Construct worker `/metrics` endpoints from worker host/IP + `worker.address.web_port`, falling back to `conf.worker.web_port` only when the reported port is missing or zero.
- Inject mandatory scrape labels before TSDB writes:

```text
component=worker
instance=<worker-host>:<worker-web-port>
worker_id=<worker-id>
worker_state=<live|blacklist|decommission|lost>
```

- Add concurrent scrape pool with max concurrency.
- Add target health registry.
- Add range query:

```text
GET /api/v1/observability/query
```

Expected outcome:

- live/blacklist/decommission/lost workers are discovered automatically,
- worker metrics are scraped without manual configuration,
- API can return time-series points for charts.

### Phase 3 - Frontend observability page

Scope:

- Add ECharts dependency.
- Add `Observability.vue`.
- Add route and sidebar navigation item.
- Add API client functions.
- Add reusable chart component.
- Add target health table.
- Add only the first narrow panels:

```text
Data Sources
Collector Health
Metric Names
1-2 raw query charts, for example Cluster Capacity and Worker Read Throughput
```

Expected outcome:

- users can open `curvine-web` and view a working monitoring page,
- no Prometheus setup required.

### Phase 4 - Curated dashboard API and derived metrics

Scope:

- Add `/api/v1/observability/dashboard`.
- Add backend mappings for key Curvine panels.
- Add basic counter rate derivation.
- Add min/max/avg/current summary per series for chart tables.
- Add panel grouping:

```text
Master
Workers
Data Sources
```

Expected outcome:

- frontend becomes less coupled to raw metric names,
- dashboard payload can later come from PrometheusProvider.

### Phase 5 - Prometheus provider compatibility

Scope:

- Introduce `MetricsProvider` trait.
- Rename current implementation conceptually to `BuiltinProvider`.
- Add `PrometheusProvider`.
- Implement Prometheus health check.
- Implement Prometheus query and range query via HTTP API.
- Add provider mode config.

Expected outcome:

- production deployments can use Prometheus as source of truth,
- same frontend works for both providers.

### Phase 6 - Production operations polish

Scope:

- Add Prometheus scrape config examples.
- Add Kubernetes ServiceMonitor/PodMonitor examples.
- Add PrometheusRule examples.
- Add alert display page.
- Add diagnostics page:

```text
Prometheus reachable
Curvine master targets discovered
Curvine worker targets discovered
Prometheus target coverage
missing targets
suggested fixes
```

Expected outcome:

- production users can onboard with less manual work,
- `curvine-web` becomes an operations entry point, not just a chart page.

## Required Tests

### Parser tests

1. Parses simple gauge/counter line:

```text
capacity 123
```

2. Parses labels and sorts labels stably:

```text
worker_num{tag="live"} 3
```

3. Ignores comments:

```text
# HELP capacity ...
# TYPE capacity gauge
```

4. Handles malformed lines without panic.
5. Parses histogram bucket/sum/count as ordinary series.
6. Handles escaped label values if supported by parser; otherwise documents unsupported escape cases.

### TSDB tests

1. Inserts points for a new series.
2. Appends points for an existing series.
3. Prunes points older than retention.
4. Enforces max samples per series.
5. Queries instant latest value.
6. Queries range points by metric name and labels.
7. Does not mix series with different labels.

### Discovery tests

1. Builds master target from config.
2. Builds active/configured master metrics target from master host + `conf.master.web_port`, not the RPC port embedded in `active_master`.
3. Builds worker target from `WorkerInfo.address.hostname + WorkerInfo.address.web_port` when the worker-reported web port is greater than zero.
4. Uses `WorkerInfo.address.web_port` even when it differs from `conf.worker.web_port`; the worker-reported port is the real registered endpoint and must win.
5. Falls back from `WorkerInfo.address.web_port` to `conf.worker.web_port` only when the reported port is zero or missing.
6. Falls back to `ip_addr` when hostname is loopback or empty, aligned with the existing display/loopback handling in `admin_handler.rs`.
7. Preserves worker state label.
8. Does not fail the entire discovery result when one worker address is invalid.

### Ingestion label tests

1. Master samples with metric name `capacity` are stored with `component=master` and `instance=<master-host>:<web-port>`.
2. Worker samples with metric name `capacity` are stored with `component=worker` and `instance=<worker-host>:<web-port>`.
3. Master and worker samples with the same raw metric name do not share a `SeriesKey`.
4. Worker samples include `worker_id` and `worker_state` labels when available.
5. Existing raw Prometheus labels are preserved while injected labels are added.

### Scraper tests

1. Successful scrape returns samples and duration.
2. Timeout marks target down.
3. HTTP non-2xx marks target down.
4. Partial parse errors are recorded.
5. Empty payload marks target down.

### API tests

1. `/observability/health` returns enabled mode and TSDB stats.
2. `/observability/targets` returns target health.
3. `/observability/instant` filters by metric/component/instance.
4. `/observability/query` returns range points.
5. Existing auth middleware protects all observability APIs.

### Frontend tests/checks

1. Route guard works for `/observability`.
2. Empty state renders when no metrics are available.
3. Target down state renders clearly.
4. Charts do not overflow on small screens.
5. Long worker hostnames do not break layout.
6. Counter rate calculation handles reset: when a counter series drops from a larger value to a smaller value, the rendered rate for that interval is zero or unknown, never negative.
7. Golden chart case covers normal counter increase, missing point, and counter reset for read/write throughput panels.

## Open Questions

1. Should the local minimal parser be replaced by a mature parser later if Curvine metrics payloads need broader Prometheus exposition support?
2. Should lost workers be actively scraped forever, or should `curvine-web` stop scraping them after a configurable grace period? Phase 2 default is to list them as targets and allow scrape failure while clearly separating `worker_state=lost` from scrape reachability.
3. What is the default worker endpoint host preference per packaging target? The implementation should support IP preference for container/Kubernetes environments, even if the default differs for bare metal.
4. Should the first release expose raw metric names to users, or only curated dashboard panels?
5. How many hours of built-in retention are acceptable by default?
6. Should the built-in collector be enabled by default in each production package, or should production packages default to `web.observability.enabled=false` unless explicitly enabled?
7. Should master/worker metrics eventually add a `curvine_` prefix to avoid collisions? Injected `component`/`instance` labels solve TSDB collisions, but a prefix would still improve ecosystem hygiene.
8. Should the dashboard compute rates in frontend first, or backend first?
9. Should Prometheus integration be Phase 5, or is production onboarding important enough to bring it earlier?
10. Should alert display read directly from Prometheus/Alertmanager later, or only show Curvine-derived health warnings?

## Minimal First Pull Request

The first PR should be small and shippable:

1. Add `curvine-web/src/observability/` module skeleton.
2. Add model structs.
3. Add minimal Prometheus text parser and tests.
4. Add in-memory TSDB and tests.
5. Add `reqwest` to `curvine-web` with minimal features.
6. Add master-only scrape path using master host + `conf.master.web_port`.
7. Inject mandatory `component=master` and `instance=<master-host>:<web-port>` labels before TSDB writes.
8. Add `/api/v1/observability/health`, `/targets`, `/instant`, `/names`.
9. Register observability routes through a small observability router helper; keep `admin_handler.rs` limited to holding `Arc<ObservabilityService>` and merging/nesting the router.
10. Add no frontend yet, or only a hidden/debug route if desired.

Why stop there:

- It validates the hardest backend primitives without UI complexity.
- It avoids worker discovery edge cases until the storage/parser/query path is proven.
- It keeps the diff reviewable and reduces risk in the already large `admin_handler.rs`.

After this PR, the next PR can add worker auto-discovery and range query. The frontend can follow once backend API behavior is stable.

## Final Direction

The final platform should be described as:

```text
curvine-web is the Curvine operations center.

It provides built-in observability for out-of-box use,
and Prometheus-backed observability for production.

It combines metrics, target health, master HA, worker management,
jobs, mounts, file system browsing, alerts, and diagnostics into one UI.
```

The first release should focus on making the built-in observability loop real and reliable. The architecture must keep a clear provider boundary so production Prometheus support can be added without replacing the UI.
