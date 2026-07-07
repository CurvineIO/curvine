# Curvine FUSE Metrics — Grafana Dashboard

A DevOps troubleshooting dashboard for the `curvine-fuse` client, covering all
`curvine_fuse_*` Prometheus metrics. The dashboard is organized as a **triage
path** (not by metric family): first locate impact and the bottleneck stage,
then drill into the domain rows.

- Dashboard model: [`curvine-fuse-dashboard.json`](./curvine-fuse-dashboard.json)
- Pure JSON — importing it changes nothing on the Rust side.

## Import

1. In Grafana: **Dashboards → New → Import** → upload `curvine-fuse-dashboard.json`.
2. Select your Prometheus datasource for the `Datasource` variable.
3. **First step after import: confirm the `Job` variable.** It defaults to a
   regex `/.*curvine.*fuse.*/` over `label_values(up, job)`. If your Prometheus
   job name for the fuse target differs, edit the `job` variable's regex (or
   value) so it matches only curvine-fuse targets — otherwise the availability
   panels will show unrelated targets while the business panels show no data.

### Prometheus scrape setup

Point Prometheus at each fuse process's `/metrics` endpoint. There is **no
per-mount label** — multiple fuse instances/mounts are distinguished only by the
Prometheus `instance` label. The dashboard uses `$__rate_interval`, so it adapts
to your scrape interval automatically; a scrape interval of 10–30s pairs well
with the default `30s` refresh and `now-1h` time range.

## Template variables

| Variable | Type | Notes |
|---|---|---|
| `$datasource` | datasource | Prometheus |
| `$job` | query `label_values(up, job)` | regex-constrained to curvine-fuse; sourced from `up` (not a business metric) so **down targets stay visible** |
| `$instance` | query `label_values(up{job=~"$job"}, instance)` | multi + All (All value `.*`) |
| `$opcode` | custom, **single-select** | 34 handled request ops; query uses `opcode="$opcode"` |
| `$kind` | custom `metadata,stream` | multi + All; query uses `kind=~"$kind"` |
| `$io_type` | custom `read,write` | Stream IO only; multi + All; `io_type=~"$io_type"` |
| `$lifecycle_io_type` | custom `flush,fsync,release` | Lifecycle only — **separate from `$io_type`**; `io_type=~"$lifecycle_io_type"` |
| `$path_type` | custom `curvine,ufs,fallback,local,unknown` | multi + All; `path_type=~"$path_type"` |

`$opcode` is a **static** list: it must be updated by hand when
`curvine-fuse/src/session/channel/fuse_receiver.rs` gains or loses a dispatch
arm. It covers only the 34 handled data/metadata/stream request ops. **`Init`
and `Interrupt` are handled control ops** (they appear in `requests_total` but
are excluded from the drilldown — see the *Control requests* panel in the
Framework row). Unsupported ops (`Ioctl`/`Lseek`/`Rename2`/`BMap`/`Poll`/…) are
observed via the *Unsupported top-N* panel in the Framework row.

## Reading the dashboard

### Availability: three states, don't conflate

- `up==0` and business panels show No data → **process/scrape target
  unreachable** (`kernel_fd_health` is absent). This is **not** QPS=0.
- `up==1 && QPS==0` → reachable but currently **idle** (normal).
- `up==1 && kernel_fd_health==0` → reachable but the **FUSE fd/session is
  unhealthy**.

The two Overview stats show these as **fleet counts, not per-instance tiles**, so
their size stays fixed no matter how many instances `$instance` selects:

- **Availability (up)** → `UP` (targets with `up==1`) + `DOWN` (`up==0`) counts;
  `DOWN>0` turns red. `count(...) or vector(0)` keeps the all-healthy case at `0`,
  not No data.
- **Kernel fd health** → `HEALTHY` + `UNHEALTHY` counts over **reachable**
  instances; `UNHEALTHY>0` turns red. A down process (`up==0`) exports no
  `kernel_fd_health`, so it is in **neither** count — read this together with the
  Availability `DOWN` count.

Which specific instance is down/unhealthy is in the **Per-instance overview**
table (it lists `up` and `kernel_fd_health` per instance).

### No data ≠ broken

Top-N error/event tables show **No data** when there were no such events in the
range — that is expected, not a broken panel. The Overview error-total stats are
zero-filled so "no errors" reads as `0`, not No data. Ratio panels (non-success,
cache hit rate) are zero-filled too.

### Histograms lag — use gauges as leading signals

**All `*_duration_us` histograms are completion-based**: a request/stage is only
observed *after* it finishes. Work currently stuck in a queue, a backend call,
the reply channel, or a kernel write is **not yet counted**, so p99 can stay flat
(or samples drop) during a stall. Under async/backpressure scenarios the leading
signals are the inflight/queue gauges: `active_requests`, `reply_queue_depth`,
`meta_task_inflight`, `stream_io_inflight`, `stream_lifecycle_inflight`,
`stream_write_queue_depth`, `setlkw_inflight`.

### Two errno conventions (do not mix)

- `errors_total{errno}` and `response_write_errors_total{errno}` use **uppercase**
  POSIX names (`ENOENT`, `EIO`, …, `OTHER`).
- `receive_errors_total{errno}` uses **lowercase** splice names
  (`enoent`, `eintr`, `eagain`, `enodev`, `other`).

### Other easy-to-misread semantics

- `path_type=fallback` means a **fallback-capable handle**, not a read that
  actually fell back to UFS. Don't treat fallback latency/bytes as real
  UFS-fallback IO.
- `receive_loop_wait_duration_us` **includes idle wait** — a high p99 under low
  traffic is normal, not slow requests.
- `operation_duration_us` is the **metadata dispatch scope** (includes reply
  enqueue, excludes interrupted SETLKW); it is not the end-to-end
  `request_duration_us`. **Do not subtract the two** to localize latency.
- `stream_lifecycle_requests_total` has **no status** label → no lifecycle error
  rate (its result mixes backend and reply-enqueue errors).
- `readdir_entries` observes only the **success** child; it is a per-syscall
  batch size, not total directory size.
- `decode_errors_total{phase="decode"}` is **terminal** (receiver died → restart
  signal), not a rate to threshold; `phase="parse"` is recurring/recoverable.
- `stage_duration_us` has **no `opcode` label** (framework horizontal overview,
  by kind); per-op latency is in the Operation drilldown row.

### Aggregation rules

- `kernel_fd_health` → `min by(instance)` (never sum).
- Queues / inflight → `sum by(instance)` (plus `kind`/`io_type` where the metric
  carries it, in the Bottleneck Locator).
- `{inode,file_handle,dir_handle}_count` → kept **per-instance**, not summed
  across instances.

## DevOps Triage Matrix

Enter from a **symptom**; the row tells you where to look and what to do next.
(Remember: p99 histograms lag — check inflight/queue gauges first under
backpressure.)

| Symptom | Look at | Likely cause | Next step |
|---|---|---|---|
| `up==0` | Overview Availability | scrape/process down | check process/port/deployment (business No data ≠ QPS=0) |
| `up==1 && kernel_fd_health==0` | Overview Availability | FUSE fd/session unhealthy | check mount/kernel fd/session log + recent shutdown reason |
| request p99 high & `meta_spawn` stage high | Locator + Framework | metadata task scheduling / runtime pressure | check `meta_task_inflight` |
| request p99 high & `operation` stage high | Metadata + Cache | metadata backend/cache/readdir | op drilldown + cache/readdir + backend side |
| request p99 high & `stream_io` stage high | Stream IO | backend read/write slow | io_type/path_type/status/throughput |
| `active_requests` high but stage p99 flat | Locator + queues | concurrency accumulation / downstream wait (p99 lag) | inspect queues/inflight + op mix |
| write latency high & `stream_write_queue_depth` high | Stream IO | writer queue backlog | write QPS/bytes/lifecycle flush/release |
| `reply_write` p99 high & `response_write_errors_total` present | Reply | kernel fd write slow/failing | write errors by errno + kernel fd health |
| `reply_queue_depth` high & `reply_write` rate flat | Reply | sender not draining / downstream write stuck | reply_queue_depth trend + downstream write |
| `reply_enqueue_errors_total` present | Reply | request never reached the sender | enqueue failures by reason |
| `reply_write` p99 high but `reply_queue_depth` low | Reply | single slow kernel write, no backlog | write latency distribution |
| non-success ratio high & errno top-N present | Overview / Operation | FS operation error | errno/op/status (`errors_total`) |
| `unsupported_total` high | Framework → Unsupported top-N | kernel sent an unimplemented op | opcode/reason, confirm compatibility |
| `setlkw_wait`/`setlkw_inflight` high | Metadata | lock wait or SETLKW reply backpressure | setlkw duration/inflight + interrupted |
| cache hit rate low | Cache & readdir | metadata cache miss/invalidation | invalidation reason/top op |

## Scope boundary

This dashboard localizes bottlenecks to the **fuse-local链路 only**. When it
shows a domain is slow, the next hop is a different system:

- `operation_duration_us` high → fuse metadata dispatch/backend call is slow
  overall → look at Curvine metadata/backend side.
- `io_duration_us` high → fuse read/write backend call is slow → look at Curvine
  client/server/storage/network metrics.
- `path_type=fallback` is not a real UFS-fallback outcome (see above).

The dashboard does not decompose Curvine master/worker/server internals.

## Tables and implementation limits

Multi-query tables (`Per-instance overview`, `Top opcode p99`, `Metadata op
hotspots`, `read/write comparison`) use Grafana transforms (`joinByField` on a
single key, or `merge` for the `(io_type,path_type)` comparison) to render one
row per key. `io_dispatch_duration_us` and `stream_io_inflight` carry only
`io_type` (no `path_type`), so they are shown in a companion panel next to the
`read/write comparison` table rather than force-joined into it. If a table ever
renders as multiple frames in your Grafana version, fall back to the individual
per-metric panels in the same row to continue troubleshooting.

## Enhancements (optional, not required for v1)

Data links / variable interlinking (click an instance → set `$instance`, click
an opcode → set `$opcode`) are enhancements; panel descriptions already state the
next variable to switch. They can be added after the core dashboard is in use.
