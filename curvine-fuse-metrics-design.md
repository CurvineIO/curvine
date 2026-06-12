# Curvine FUSE End-to-End Metrics Enhancement Proposal

Related issue: [#897](https://github.com/CurvineIO/curvine/issues/897)

## Summary

This document proposes an incremental enhancement to `curvine-fuse` metrics so that operators can observe the full FUSE request lifecycle, including request rate, error rate, end-to-end latency, key internal stages, read/write throughput, and the overhead introduced by metrics collection itself.

The proposal keeps the current Prometheus `/metrics` model, reuses the existing `orpc::common::Metrics` wrapper, and focuses on low-cardinality metrics that are safe for high-frequency FUSE workloads.

## Motivation

`curvine-fuse` is often on the critical path of application read/write and metadata operations. When users see high latency or low throughput from a mounted filesystem, the current metrics are not enough to answer questions such as:

- Which FUSE opcode is slow or failing?
- Is the latency introduced before request dispatch, inside filesystem operations, or while sending replies back to the kernel?
- Are read/write operations slow because of enqueueing, actual IO, flush/release/fsync, or backend fallback paths?
- Are metadata operations dominated by cache misses, client calls, or FUSE framework overhead?
- Is Prometheus scraping or metrics collection itself adding measurable overhead?

Improving this visibility is especially important for production environments, performance regression testing, and future optimization work around FUSE IO paths.

## Current State

### Existing Metrics Infrastructure

Curvine already has a Prometheus-based metrics infrastructure:

- `orpc/src/common/metrics.rs` wraps Prometheus `Counter`, `Gauge`, `Histogram`, `CounterVec`, `GaugeVec`, and `HistogramVec`.
- Metrics are registered in the Prometheus default registry.
- `Metrics::text_output()` exports Prometheus text format.
- Duplicate metric registration is handled through the shared metrics map.

### Existing FUSE Metrics

`curvine-fuse` currently exposes `/metrics` through its own web server. The FUSE-specific metrics are mostly runtime state gauges:

```text
inode_num
file_handle_num
dir_handle_num
```

These values are refreshed from `NodeState::set_metrics()` when `/metrics` is scraped.

### Existing Client Metrics Visible from FUSE

Because `curvine-fuse` initializes `curvine-client` in the same process, the FUSE `/metrics` output can also include client-side metrics such as:

```text
client_metadata_operation_duration{operation}
client_read_bytes
client_read_time_us
client_write_bytes
client_write_time_us
client_block_idle_conn
client_mount_cache_hits{id}
client_mount_cache_misses{id}
```

These metrics are useful, but they do not provide FUSE-level end-to-end visibility. In particular, they do not capture kernel receive/send cost, FUSE opcode latency, reply queueing, errno distribution, or per-operation FUSE framework overhead.

## Goals

1. Add end-to-end FUSE request metrics by opcode.
2. Add latency histograms for FUSE requests and major internal stages.
3. Add read/write/flush/release/fsync metrics at the FUSE layer.
4. Preserve the existing `/metrics` endpoint and Prometheus text format.
5. Keep labels low-cardinality and safe for production.
6. Provide enough metrics to build Grafana dashboards for FUSE overview, IO, metadata, cache, and runtime health.
7. Measure and control the overhead of metrics collection and Prometheus scraping.

## Non-Goals

- Do not redesign the global Curvine metrics framework in the first step.
- Do not change Master or Worker metrics as part of the initial FUSE proposal.
- Do not export path, inode, file handle, request unique ID, mount path, or user-controlled values as metric labels.
- Do not report FUSE metrics to Master through `ClientMetrics::encode()` in the first step.
- Do not introduce distributed tracing in this proposal. Tracing can be designed separately later.

## Full Lifecycle Model

A FUSE request can be observed as the following logical stages:

```text
kernel
  -> FuseReceiver::splice() / receive
  -> request parse and classification
  -> dispatch_meta() or send_stream()
  -> FileSystem operation
  -> Curvine client / local cache / UFS fallback when applicable
  -> response creation and enqueue
  -> FuseSender::splice() / reply writeback
  -> kernel
```

The first implementation should focus on stages that can be measured with minimal code changes:

- `receive`: reading requests from the FUSE device.
- `operation`: executing the FUSE filesystem operation.
- `reply_enqueue`: creating and enqueueing the response.
- `reply_write`: writing the response back to the FUSE device.
- `request`: end-to-end latency from request arrival to response completion when feasible.

Some stages may be approximated in the first phase. For example, `dispatch_meta()` currently mixes operation execution and response enqueueing for metadata requests. The implementation should document this boundary clearly and refine it later if needed.

## Metric Naming and Label Policy

### Naming

New FUSE-specific metrics should use the `curvine_fuse_` prefix.

This avoids name collisions with existing generic metrics such as `capacity`, `available`, or client-side metrics without a namespace prefix.

### Label Rules

Allowed labels should be bounded enums:

- `opcode`: FUSE opcode name, for example `Lookup`, `GetAttr`, `Read`, `Write`, `Release`.
- `kind`: coarse request category, for example `metadata`, `stream`, `framework`.
- `status`: `success` or `error`.
- `errno`: normalized errno name or number for failed requests.
- `stage`: bounded internal stage name, for example `receive`, `operation`, `reply_write`.
- `io_type`: bounded IO operation type, for example `read`, `write`, `flush`, `fsync`, `release`.
- `path_type`: bounded data path category, for example `curvine`, `ufs`, `fallback`, `unknown`.

Labels that must not be used:

- file path
- inode
- file handle
- request unique ID
- user name
- process ID
- mount path
- storage worker address
- arbitrary error message

High-cardinality labels can make Prometheus expensive and may expose sensitive information.

## Latency Metrics and Prometheus Semantics

Latency metrics should use Prometheus histograms instead of gauges.

A Prometheus scrape does not receive a precomputed average latency or a single representative latency value. The scrape receives the current value of each exported time series. For histograms, that means cumulative bucket counters plus cumulative `sum` and `count` values.

Example output:

```text
curvine_fuse_request_duration_us_bucket{opcode="Read",le="100"} 123
curvine_fuse_request_duration_us_bucket{opcode="Read",le="500"} 456
curvine_fuse_request_duration_us_bucket{opcode="Read",le="1000"} 789
curvine_fuse_request_duration_us_bucket{opcode="Read",le="+Inf"} 1000
curvine_fuse_request_duration_us_sum{opcode="Read"} 850000
curvine_fuse_request_duration_us_count{opcode="Read"} 1000
```

Average latency over a time window should be calculated in PromQL:

```promql
rate(curvine_fuse_request_duration_us_sum[5m])
/
rate(curvine_fuse_request_duration_us_count[5m])
```

Percentiles should be calculated with `histogram_quantile()`:

```promql
histogram_quantile(
  0.99,
  sum by (le, opcode) (
    rate(curvine_fuse_request_duration_us_bucket[5m])
  )
)
```

Design implications:

- Use `HistogramVec` for request, operation, stage, and IO latency.
- Do not add `average_latency` metrics. PromQL can compute averages from histogram `sum` and `count`.
- Do not use a gauge for `last_latency_us` except for temporary debugging.
- Use counters for requests, errors, and bytes.
- Use gauges only for current state, such as in-flight requests, handle count, and queue depth.

## Proposed Metrics

### Request and Error Metrics

```text
curvine_fuse_requests_total{opcode,kind,status}
curvine_fuse_errors_total{opcode,kind,errno}
curvine_fuse_interrupted_total{opcode}
curvine_fuse_unsupported_total{opcode}
```

Purpose:

- Measure QPS by opcode.
- Measure success and error rates by request category.
- Identify high-error opcodes and errno distributions.
- Track unsupported and interrupted requests separately.

Notes:

- `status` should be derived from the final result sent to the kernel.
- `errno` should only be attached to error metrics, not to all request metrics.
- Unsupported opcodes should be counted even if they return quickly.

### Request and Stage Latency Metrics

```text
curvine_fuse_request_duration_us{opcode,kind,status}
curvine_fuse_operation_duration_us{opcode,kind,status}
curvine_fuse_stage_duration_us{stage,kind,status}
```

Purpose:

- `request_duration_us`: end-to-end FUSE request latency.
- `operation_duration_us`: filesystem operation latency after dispatch.
- `stage_duration_us`: framework stages such as receive, reply enqueue, and reply write.

Recommended initial buckets for request and operation latency:

```text
10, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
25000, 50000, 100000, 250000, 500000, 1000000,
2500000, 5000000, 10000000
```

Recommended initial buckets for short framework stages:

```text
5, 10, 25, 50, 100, 250, 500, 1000, 2500,
5000, 10000, 25000, 50000, 100000
```

All duration metrics use microseconds because the existing Curvine metrics already use microsecond-based names such as `client_read_time_us`.

### In-Flight and Queue Metrics

```text
curvine_fuse_inflight_requests{kind}
curvine_fuse_pending_interruptible_requests
curvine_fuse_reply_queue_depth
curvine_fuse_stream_write_queue_depth
curvine_fuse_receiver_tasks{state}
curvine_fuse_sender_tasks{state}
```

Purpose:

- Observe request concurrency.
- Detect stuck or overloaded reply paths.
- Identify whether latency is caused by queue buildup.

Notes:

- If the underlying async channel does not expose a cheap `len()`, queue depth can be deferred or maintained with atomic counters around enqueue/dequeue.
- Gauges should be event-driven where possible instead of recomputing by traversing maps during scrape.

### IO Metrics

```text
curvine_fuse_io_bytes_total{io_type,path_type,status}
curvine_fuse_io_requests_total{io_type,path_type,status}
curvine_fuse_io_duration_us{io_type,path_type,status}
curvine_fuse_io_size_bytes{io_type,path_type}
```

Suggested `io_type` values:

```text
read
write
flush
fsync
release
```

Suggested `path_type` values:

```text
curvine
ufs
fallback
unknown
```

Purpose:

- Measure read/write throughput at the FUSE layer.
- Measure IO latency distribution independently from client-layer counters.
- Distinguish actual IO latency from FUSE enqueue latency.
- Compare Curvine, UFS, and fallback paths when the implementation can classify them reliably.

Recommended initial buckets for IO size:

```text
4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864
```

Notes:

- `FuseReader::read()` is a good place to observe actual read duration and bytes returned.
- `FuseWriter::write()` currently enqueues writes; actual write work happens later. The design should distinguish enqueue duration from actual write duration where practical.
- `path_type` can start as `unknown` and be refined later.

### Cache and State Metrics

```text
curvine_fuse_node_cache_hits_total{cache}
curvine_fuse_node_cache_misses_total{cache,reason}
curvine_fuse_node_cache_invalidations_total{reason}
curvine_fuse_readdir_entries_total{status}
curvine_fuse_readdir_duration_us{status}
curvine_fuse_state_persist_total{status}
curvine_fuse_state_persist_duration_us{status}
curvine_fuse_state_restore_total{status}
curvine_fuse_state_restore_duration_us{status}
```

Purpose:

- Understand metadata cache effectiveness.
- Observe readdir cost and directory listing sizes.
- Detect expensive or failing state persist/restore operations.

Notes:

- `reason` must be a bounded enum, such as `not_found`, `expired`, `invalidated`, or `unknown`.
- Do not add per-path or per-inode cache labels.

### Existing Gauge Compatibility

Existing FUSE gauges should be preserved for compatibility:

```text
inode_num
file_handle_num
dir_handle_num
```

New namespaced equivalents should be added:

```text
curvine_fuse_inode_count
curvine_fuse_file_handle_count
curvine_fuse_dir_handle_count
```

The old gauges can be deprecated later after dashboards migrate to the namespaced metrics.

### Metrics Self-Observation

The metrics endpoint should expose lightweight self-observation metrics:

```text
curvine_fuse_metrics_scrape_duration_us
curvine_fuse_metrics_scrape_bytes
curvine_fuse_metrics_series_estimate
```

Purpose:

- Measure `/metrics` handler latency.
- Track response size growth after adding histograms.
- Help operators detect when monitoring overhead becomes significant.

`curvine_fuse_metrics_series_estimate` can be optional. It may be approximated from known metric definitions rather than computed by expensive registry traversal.

## Performance and Overhead Control

FUSE is a high-frequency syscall path. Metrics collection must be treated as part of the performance design, not as an afterthought.

### Hot Path Costs

Metrics can add overhead through:

- `inc()` and `observe()` calls on every request.
- Label lookup for `*Vec` metrics.
- Histogram bucket updates.
- Extra time measurement calls.
- Additional allocations when labels or error strings are built dynamically.
- Additional lock contention if metrics are updated while holding business locks.

Design rules:

- Use bounded static string labels.
- Avoid formatting labels on the hot path.
- Pre-initialize metric children for common label combinations when practical.
- Do not use path, inode, request ID, or worker address as labels.
- Do not perform map traversal or expensive state inspection during request handling.
- Avoid updating detailed histograms for every small chunk unless the metric is clearly useful.
- Keep metrics updates outside long-held business locks where possible.

### Scrape Costs

Prometheus scraping adds overhead because `/metrics` must gather all registered metrics and encode them as text.

Potential costs:

- `Metrics::text_output()` calls Prometheus gather and text encoding.
- Histograms produce one time series per bucket, plus `sum` and `count`.
- High-cardinality labels multiply the number of time series.
- `NodeState::set_metrics()` currently refreshes state gauges during scrape.
- Short scrape intervals amplify gather and encode cost.

Design rules:

- Keep scrape-time gauge refresh lightweight.
- Do not issue network calls or client RPCs from the metrics handler.
- Do not traverse large inode, file handle, or directory maps during scrape.
- Maintain queue depth and in-flight gauges event-by-event when possible.
- Use a default scrape interval of at least 10 seconds. Production deployments should usually start with 15 or 30 seconds.
- Consider splitting detailed metrics into a separate endpoint later if the default `/metrics` output becomes too large.

### Optional Detail Levels

A future configuration could allow operators to trade visibility for overhead:

```toml
[fuse]
metrics_enabled = true
metrics_detail_level = "basic" # off | basic | detailed
metrics_io_size_histogram = true
metrics_stage_histogram = true
```

Initial behavior can keep metrics enabled by default while reserving a clean internal API for future switches, for example `FuseMetrics::enabled()` or `FuseMetrics::detail_level()`.

### Cardinality Budget

Every new metric should include an estimated time-series budget during review.

Example estimates:

```text
curvine_fuse_request_duration_us:
  opcode ~= 35
  kind <= 2
  status <= 2
  buckets = 18 + sum + count
  series ~= 35 * 2 * 2 * 20 = 2800

curvine_fuse_stage_duration_us:
  stage ~= 6
  kind <= 3
  status <= 2
  buckets = 14 + sum + count
  series ~= 6 * 3 * 2 * 16 = 576

curvine_fuse_io_duration_us:
  io_type ~= 5
  path_type <= 4
  status <= 2
  buckets = 18 + sum + count
  series ~= 5 * 4 * 2 * 20 = 800
```

The first implementation should stay within a few thousand additional time series per FUSE process. Future cache, readdir, and state metrics should continue to include cardinality estimates.

### Benchmark and Acceptance Criteria

Metrics overhead should be validated with benchmarks before enabling detailed metrics broadly.

Suggested comparison scenarios:

1. Current metrics only.
2. Basic FUSE metrics enabled.
3. Detailed FUSE metrics enabled.
4. Prometheus scraping every 15 seconds.
5. Prometheus scraping every 1 second as a stress scenario.

Suggested measurements:

- FUSE read/write throughput.
- Metadata QPS.
- p95 and p99 latency.
- CPU usage.
- RSS memory.
- `/metrics` response size.
- `/metrics` handler latency.
- Estimated Prometheus time-series count.

A reasonable initial acceptance target is that basic metrics should not introduce a measurable regression under normal scrape intervals. Detailed metrics can have a documented overhead budget and should be optional if benchmarks show a visible cost.

## Implementation Plan

### Phase 1: Request-Level FUSE Metrics

Scope:

- Extend `curvine-fuse/src/fuse_metrics.rs` with namespaced FUSE request metrics.
- Count requests and errors by opcode and kind.
- Add request and operation latency histograms.
- Add in-flight request gauges.
- Add receive and reply write stage latency where easy to measure.
- Preserve existing `inode_num`, `file_handle_num`, and `dir_handle_num` gauges.
- Add namespaced equivalents for those gauges.

Likely code touch points:

- `curvine-fuse/src/fuse_metrics.rs`
- `curvine-fuse/src/web_server.rs`
- `curvine-fuse/src/session/channel/fuse_receiver.rs`
- `curvine-fuse/src/session/channel/fuse_sender.rs`
- `curvine-fuse/src/session/fuse_response.rs`
- `curvine-fuse/src/fs/curvine_file_system.rs`

Expected outcome:

- Operators can view FUSE QPS, error rate, and p95/p99 latency by opcode.
- The first implementation remains low-risk and does not require client or server changes.

### Phase 2: Read/Write IO Metrics

Scope:

- Add FUSE-layer read/write bytes, request counts, latency, and size histograms.
- Add flush, fsync, and release latency metrics.
- Separate write enqueue latency from actual write latency when practical.
- Start with `path_type="unknown"` if reliable classification is not available.

Likely code touch points:

- `curvine-fuse/src/fs/fuse_reader.rs`
- `curvine-fuse/src/fs/fuse_writer.rs`
- `curvine-fuse/src/fs/curvine_file_system.rs`

Expected outcome:

- Operators can compare application-visible FUSE IO performance against client-layer IO counters.
- Slow read/write paths become visible without adding labels that expose file paths.

### Phase 3: Cache, Readdir, and State Metrics

Scope:

- Add bounded cache hit/miss/invalidation counters.
- Add readdir entry count and duration metrics.
- Add state persist/restore success, failure, and duration metrics.
- Keep existing scrape-time state gauge refresh lightweight.

Likely code touch points:

- `curvine-fuse/src/fs/state/node_state.rs`
- `curvine-fuse/src/fs/state/node_map.rs`
- `curvine-fuse/src/fs/state/file_handle.rs`
- `curvine-fuse/src/fs/state/dir_handle.rs`
- `curvine-fuse/src/fs/curvine_file_system.rs`

Expected outcome:

- Metadata cache behavior and readdir cost become observable.
- State recovery and persist issues can be diagnosed from metrics.

### Phase 4: Client Path Enhancements

Scope:

- Add client RPC latency histograms if needed.
- Add block read/write latency histograms if counters are not enough.
- Improve Curvine/UFS/fallback path classification for FUSE IO metrics.
- Evaluate whether selected FUSE metrics should be reported to Master in a later design.

Likely code touch points:

- `curvine-client/src/client_metrics.rs`
- `curvine-client/src/file/fs_client.rs`
- `curvine-client/src/file/curvine_filesystem.rs`
- `curvine-client/src/unified/*`

Expected outcome:

- FUSE-level and client-level metrics can be correlated to identify whether bottlenecks are in the FUSE framework, client logic, RPC, cache, or backend path.

## Grafana Dashboard Suggestions

### FUSE Overview

- Request QPS by opcode.
- Error rate by opcode and errno.
- p50, p95, and p99 request latency by opcode.
- In-flight requests by kind.
- Receive and reply write latency.

### FUSE IO

- Read and write throughput.
- Read and write request size distribution.
- Read and write p95/p99 latency.
- Flush, fsync, and release latency.
- Curvine, UFS, fallback, and unknown path latency when classification is available.

### Metadata and Cache

- Lookup, getattr, setattr, create, unlink, rename, and readdir latency.
- Metadata error rate by opcode.
- Cache hit and miss rate.
- Readdir entries per request.
- Inode, file handle, and directory handle counts.

### Runtime Health

- Process CPU, RSS, thread count, and open file descriptors from default Prometheus collectors.
- Reply queue depth.
- Pending interruptible requests.
- Metrics scrape duration and response size.

## Review Questions

1. Is `curvine_fuse_` an acceptable prefix for new FUSE metrics?
2. Should existing unprefixed gauges be kept permanently, or deprecated after namespaced replacements are available?
3. Are the proposed histogram buckets appropriate for FUSE metadata and IO workloads?
4. Should `dispatch_meta()` latency initially include response enqueueing, or should operation and response creation be separated first?
5. Should detailed stage and IO size histograms be configurable from the first implementation?
6. Is `path_type="unknown"` acceptable in Phase 2 until Curvine/UFS/fallback classification is reliable?
7. Should FUSE metrics remain local to the FUSE `/metrics` endpoint, or should selected metrics be reported to Master in a future phase?

## Minimal First Pull Request

A minimal first implementation after this design is accepted could include:

- `curvine_fuse_requests_total{opcode,kind,status}`
- `curvine_fuse_errors_total{opcode,kind,errno}`
- `curvine_fuse_request_duration_us{opcode,kind,status}`
- `curvine_fuse_operation_duration_us{opcode,kind,status}`
- `curvine_fuse_stage_duration_us{stage,kind,status}` for `receive` and `reply_write`
- `curvine_fuse_inflight_requests{kind}`
- `curvine_fuse_inode_count`
- `curvine_fuse_file_handle_count`
- `curvine_fuse_dir_handle_count`
- `curvine_fuse_metrics_scrape_duration_us`
- `curvine_fuse_metrics_scrape_bytes`

This keeps the first code change focused while establishing the naming, latency, label, and overhead-control patterns for later phases.
