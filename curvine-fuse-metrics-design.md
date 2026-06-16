# Curvine FUSE End-to-End Metrics Enhancement Proposal

Related issue: [#897](https://github.com/CurvineIO/curvine/issues/897)

## Summary

This document proposes an incremental enhancement to `curvine-fuse` metrics so that operators can observe the full FUSE request lifecycle: request rate, error rate, end-to-end latency, key internal stages, read/write throughput, cache effectiveness, and the overhead introduced by metrics collection itself.

The proposal keeps the current Prometheus `/metrics` model, reuses `orpc::common::Metrics`, and focuses on low-cardinality, statically-labeled metrics that are safe for high-frequency FUSE workloads. The phasing is designed so each step is a small, independently reviewable change that does not touch the existing `dispatch_meta()` opcode matrix in a single large diff.

## Motivation

`curvine-fuse` is on the critical path of application read/write and metadata operations. When users see high latency or low throughput from a mounted filesystem, the current metrics (three runtime gauges: `inode_num`, `file_handle_num`, `dir_handle_num`) are not enough to answer questions such as:

- Which FUSE opcode is slow or failing?
- Is the latency introduced before request dispatch, inside filesystem operations, or while sending replies back to the kernel?
- Are read/write operations slow because of enqueueing, actual IO, flush/release/fsync, or backend fallback paths?
- Are metadata operations dominated by cache misses, client calls, or FUSE framework overhead?
- Is Prometheus scraping or metrics collection itself adding measurable overhead?

These questions matter for production diagnosis, regression testing, and future FUSE IO path optimization.

## Current State

### Existing Metrics Infrastructure

Curvine has a process-global Prometheus registry behind `orpc::common::Metrics`:

- Wraps Prometheus `Counter`, `Gauge`, `Histogram`, `CounterVec`, `GaugeVec`, `HistogramVec`.
- Registers into `prometheus::default_registry()` with a `METRICS_MAP` that swallows duplicate registrations (concurrent test safety).
- `Metrics::text_output()` exports Prometheus text format.
- A `TimeSpent` RAII helper (`orpc/src/common/time_spent.rs`) provides drop-guard timers for counters and histogram-vecs.

### Existing FUSE Metrics

`curvine-fuse` exposes `/metrics` via its own axum server (`curvine-fuse/src/web_server.rs`) on `fuse.web_port` (default 9002). Today it registers only three runtime gauges (`curvine-fuse/src/fuse_metrics.rs`):

```text
inode_num
file_handle_num
dir_handle_num
```

These are recomputed at scrape time by `NodeState::set_metrics()`, which traverses inode and handle maps under read locks. This is a known overhead source under frequent scraping.

### Existing Client Metrics Visible from FUSE

Because `curvine-fuse` initializes `curvine-client` in the same process, the FUSE `/metrics` output also includes client-side metrics:

```text
client_metadata_operation_duration{operation}    # HistogramVec
client_read_bytes / client_read_time_us          # Counter
client_write_bytes / client_write_time_us        # Counter
client_block_idle_conn                           # Gauge
client_mount_cache_hits / client_mount_cache_misses{id}
```

These metrics are useful, but they do not capture kernel↔FUSE delivery cost, FUSE opcode latency, reply queue depth, errno distribution, or per-opcode framework overhead.

### Code Reality Worth Noting

Several details of the actual code base shape the design and were misrepresented in earlier drafts:

1. **Metadata vs stream dispatch are different.**
   - Metadata requests are spawned per-request (`fuse_receiver.rs` `dispatch_meta_interrupt` → `rt.spawn`), and within `dispatch_meta()` each opcode arm awaits the operation and the reply enqueue **in the same expression**: `reply.send_xxx(fs.<op>(op).await).await`. Splitting "operation" from "reply enqueue" cleanly requires touching every arm.
   - Stream requests (`Read`, `Write`, `Flush`, `Release`, `Fsync`) are *not* executed inside `dispatch_meta`. `fs.read/write/...` only enqueues a task into a per-handle internal channel (`FuseReader` / `FuseWriter`). Real IO and the FUSE reply happen on a separate tokio task. Measuring "operation duration" in the receiver task only captures enqueue cost.

2. **Opcode names are not statically available.** `FuseOpCode` (`session/fuse_op_code.rs`) only derives `Debug`. Today the only way to get a string label is `format!("{:?}", op)`, which allocates per request.

3. **Errno values are not centrally enumerated.** Errors come from `FuseError { errno: i32, ... }` (`fuse_error.rs`). Conversion helpers map known cases to ENOENT/EEXIST/ENOTDIR/etc., but there is no helper that maps an errno integer to a stable, low-cardinality string label. Without one, an `errno` label would either be a number or a libc-locale string.

4. **Only `SETLKW` is interruptible.** `FuseRequest::is_interrupt()` returns `true` only for `FUSE_SETLKW`. The `pending_requests` map (used by the `FUSE_INTERRUPT` opcode handler) therefore only tracks blocking-lock waits.

5. **Tokio mpsc has no `len()`.** Reply queue depth and stream-write queue depth must be maintained as event-driven atomics; they cannot be cheaply read from the channel itself.

6. **Persist is event-triggered, not periodic.** Snapshotting state to disk is only triggered by `SIGUSR1` (`fuse_session.rs`). Restore only happens on startup when `CURVINE_FUSE_STATE_PATH` is set (live-upgrade child inheritance).

7. **`Forget` / `BatchForget` send no reply** (they go through `send_none`). They must be excluded from reply-side metrics.

8. **Notifications share the reply channel.** `send_notify` (`unique == FUSE_NOTIFY_UNIQUE`) goes through the same `AsyncSender<FuseTask>` as request replies and is *not* a request response. Counting it as a request would skew QPS.

9. **`Rename2` is silently ENOSYS.** It is parsed by `FuseRequest` but has no `dispatch_meta` arm, so it falls through to the wildcard. Same for several other parsed-but-unhandled opcodes.

10. **Path classification is mostly available.** `FuseWriter` already has `is_ufs: bool`. `FuseReader` is constructed from a `UnifiedReader` enum whose variant is observable at construction time but currently discarded. A small change can capture it as a `&'static str` field.

These facts drive several specific choices in the proposal below.

## Goals

1. End-to-end FUSE request metrics by opcode (QPS, error rate, latency).
2. Latency histograms for daemon-internal end-to-end FUSE requests (finished in the sender) and the major code-locatable stages (`parse_operator`, `meta_spawn`, `stream_enqueue`, `stream_io`, `reply_enqueue`, `reply_write`, plus a per-op `operation_duration`), with splice/receive cost tracked as a standalone saturation metric rather than a per-request stage.
3. Read/write/flush/release/fsync metrics at the FUSE layer.
4. Preserve the existing `/metrics` endpoint and Prometheus text format.
5. Keep all labels low-cardinality and statically allocated; no per-request heap formatting.
6. Provide enough metrics to build Grafana dashboards for FUSE overview, IO, metadata, cache, and runtime health.
7. Quantify and bound the overhead of metrics collection and Prometheus scraping.
8. Sequence the work so each phase is a small, reviewable PR that does not modify existing dispatch logic in bulk.

## Non-Goals

- Do not redesign the global Curvine metrics framework.
- Do not change Master or Worker metrics in this proposal.
- Do not export path, inode, file handle, request unique ID, mount path, user name, process ID, or worker address as metric labels.
- Do not report FUSE metrics to Master through `ClientMetrics::encode()` in this proposal.
- Do not introduce distributed tracing. A lightweight cross-layer correlation strategy (sampling-based audit log) is described in the "Cross-layer Correlation" section.
- Do not change the exposure model of the `/metrics` endpoint. It is currently served unauthenticated on `0.0.0.0:<fuse.web_port>` (`web_server.rs:19`). This proposal enriches the payload but does not add authentication, TLS, or bind-address restriction. Operators who run on shared hosts should firewall the port or bind it to localhost out of band; tightening the exposure surface is deliberately left to a separate change so it can be reviewed on its own merits. This is called out explicitly because adding richer metrics increases the value of what an unauthenticated scraper can observe.

## Request Lifecycle Model (Task-Based, Not Linear)

The earlier draft modelled a FUSE request as a linear stage chain. The actual code has two execution profiles depending on opcode kind:

### Metadata path

```text
[ receiver task A ]
  splice from kernel_fd (blocks until kernel sends a request)  -- receive_loop_wait_duration_us (standalone, NOT a stage)
  parse header (cheap)
  from_bytes() ok? -> create FuseReqCtx{labels, active guard}  -- request_duration_us START
  is_stream() ? no
  rt.spawn(async {                                       -- "meta_spawn" stage
    [ spawned task M ]   (meta_task_inflight +1; active_requests +1 via guard on ctx)
      parse_operator()                                  -- "parse_operator" stage
      dispatch_meta() match
        reply.send_rep(fs.<op>(op).await).await ------+  -- "operation" stage (incl. reply_enqueue)
      })                                               |  -- "reply_enqueue" stage (the send().await)
                                                       v
[ sender task S ] <- FuseTask::RequestReply{data, labels, active, status, errno}
  recv -> splice writev to kernel_fd                   -- "reply_write" stage
                                                       -- request_duration_us FINISH + drop active guard (here, not at send_rep)
```

### Stream path (Read/Write/Flush/Release/Fsync)

```text
[ receiver task A ]
  splice from kernel_fd                                -- receive_loop_wait_duration_us (standalone)
  parse header
  from_bytes() ok? -> create FuseReqCtx (rides on FuseResponse)  -- request_duration_us START
  is_stream() ? yes
  send_stream():
    parse_operator()                                   -- "parse_operator" stage
    fs.read(op, reply)  -- enqueues ReadTask{..., reply} to channel; returns immediately
                          -- "stream_enqueue" stage, NOT operation cost
[ FuseReader / FuseWriter task R ]                     -- spawned per handle (stream_io_inflight +1; active guard rides on reply)
  recv -> reader.fuse_read(off, len).await             -- "stream_io" stage: real IO (curvine/ufs/fallback)
  reply.send_data(...)                                 -- "reply_enqueue" stage (still only an enqueue!)
[ sender task S ] <- FuseTask::RequestReply{data, labels, active, status, errno}
  splice writev to kernel_fd                           -- "reply_write" stage
                                                       -- request_duration_us FINISH + drop active guard (same sender path as metadata)
```

### What this implies for instrumentation

The single most important consequence, and the one earlier drafts got wrong: **the daemon-internal end-to-end finish point is in the sender task, not at `reply.send_*()`.** `FuseResponse::send_rep/send_buf/send_data` only build a `ResponseData` and `sender.send(FuseTask::RequestReply{…}).await` it onto the shared reply channel (`fuse_response.rs:132,159,178`). The bytes are not written to the kernel fd until `FuseSender::splice()` runs in the sender task (`fuse_sender.rs:101`). Therefore:

- **`request_duration_us` starts** when the receiver has a parsed request (after `receive()` returns `Ok(buf)` and `from_bytes` succeeds) and **finishes** after `FuseSender::splice()` for that reply succeeds or fails. Observing it at `reply.send_*()` would silently exclude both the reply-channel queue wait and the kernel-fd write — i.e. it would not be end-to-end at all. (This is true for both metadata and stream; the stream reply is sent from the reader/writer task but still only *enqueues*, so finishing there has the same defect.)
- Making the finish point work requires the sender to know the request's `opcode`, `kind`, and start instant. The sender today sees only `unique`. The "Metrics Context Propagation" section below defines the minimal context that travels with the reply to close this gap. This is the central structural change of this revision; everything else is inc/observe.

The stages measurable without restructuring the dispatch matrix are:

- `parse_operator` — `req.parse_operator()` cost (cheap; metadata and stream).
- `meta_spawn` — `rt.spawn` submission to first poll inside the spawned future (metadata only).
- `stream_enqueue` — `send_stream()` putting work on the reader/writer channel (stream only).
- `operation` — for metadata, `fs.<op>` **plus** the awaited `reply_enqueue` (see double-count note below); not defined for stream.
- `stream_io` — the real backend IO inside the reader/writer task (stream only).
- `reply_enqueue` — `AsyncSender::send().await` onto the reply channel.
- `reply_write` — `splice` to the kernel fd in the sender task.

Notes:

- **`operation` double-counts `reply_enqueue` for metadata, by construction.** Each `dispatch_meta` arm is `reply.send_rep(fs.<op>(op).await).await` (`fuse_receiver.rs:260-342`) — `fs.<op>` and the awaited enqueue are one expression. A single timer around the `match` therefore measures operation + enqueue together. This is acceptable for an early low-churn phase, but the metric's help string must say `operation_duration_us` includes reply enqueue, and dashboards must not subtract `reply_enqueue` and present the result as "pure operation latency" — the subtraction is only an approximation and breaks on error/no-reply/notify paths. Clean splitting of `fs.<op>.await` from `reply.send_*().await` is a later refactor.
- **`receive` is deliberately NOT in this stage list.** Earlier drafts modelled splice + header parse as a `receive` stage and noted it was "dominated by idle wait at low load". Idle wait before a request exists cannot be attributed to any specific request's latency, so folding it into `request_duration_us` or comparing it against the other stages is misleading. The splice itself is still observed, but as a **standalone** `curvine_fuse_receive_loop_wait_duration_us` health/saturation histogram (see Self-observation metrics), explicitly documented as not part of the per-request stage breakdown. Receive *errors* remain a first-class metric (`receive_errors_total`).

## Metrics Context Propagation

Because the finish point is in the sender and the sender only sees `unique`, the design needs an explicit, cheap context that travels with each request from decode to finish. This is the one structural addition of the proposal; without it, `request_duration_us{opcode,kind,status}`, `response_write_duration_us{opcode}`, and `response_write_errors_total{opcode,errno}` cannot be completed in the sender.

### Context shape: copyable labels vs move-only context

A RAII in-flight guard must **not** be `Copy` — a copied guard would double-decrement its gauge. So the design separates the cheap copyable label set from the move-only context that owns the guard:

```rust
#[derive(Clone, Copy)]
struct FuseReqLabels {
    opcode: &'static str,     // FuseOpCode::as_str()
    kind: FuseReqKind,        // Metadata | Stream   (NOT a NoReply variant — see below)
    start: Instant,           // monotonic; see "Time source"
    request_bytes: u32,       // from the parsed header
}

// Move-only. Owns the active-request guard; dropping it decrements active_requests exactly once.
struct FuseReqCtx {
    labels: FuseReqLabels,
    active: Option<ActiveGuard>,   // named `active` everywhere; *_inflight is reserved for task-local gauges
}
```

- **`FuseReqLabels`** is what gets copied onto the reply task so the sender can finish the per-request series. The reply channel carries **distinct task variants** rather than one `Reply` with `Option` fields — this is what lets the move-only `ActiveGuard` live on the request path while notifications stay separate, and gives disabled mode a zero-cost path:

```rust
enum FuseTask {
    // A reply to a real FUSE request, metrics enabled. Owns the active guard;
    // the sender finishes request metrics and drops the guard after the kernel-fd write.
    RequestReply {
        data: ResponseData,
        labels: FuseReqLabels,
        active: ActiveGuard,        // move-only; dropped exactly once at sender finish
        status: FuseReqStatus,
        errno: i32,
    },
    // A kernel notification (cache invalidation). No originating request,
    // so no labels/guard; carries its own code for sender-side attribution.
    NotifyReply {
        data: ResponseData,
        code: &'static str,         // FuseNotifyCode::as_str()
    },
    // Legacy fast path, used when metrics are disabled: no context, no guard,
    // sender does no request-finish work. Keeps metrics_enabled=false byte-close
    // to current behaviour (see "Disabled-mode task variant").
    Reply(ResponseData),
    // ... existing Request variant
}
```

  Splitting `RequestReply` from `NotifyReply` (rather than a single `Reply { labels: Option<…> }`) means: (a) the `ActiveGuard` has an unambiguous owner and is moved — not cloned — onto the task, so it decrements exactly once; (b) notifications carry their `code` to the sender, so notify write failures are attributable (see Notify metrics) instead of vanishing behind `labels=None`; (c) the sender's finish branch is selected by variant, not by an `Option` check, so a context-less request reply is structurally impossible.
- **`FuseReqCtx`** (with the guard) is stored on the `FuseResponse`. The guard is what keeps `active_requests{kind}` accurate for the *full* request lifetime — see In-flight guards. When the reply is built, the guard is **moved** out of `FuseReqCtx` into `FuseTask::RequestReply`, so the count is released at sender finish, not at reply production.

### Time source

Durations use a **monotonic** clock. `LocalTime::nanos()` is wall-clock (`SystemTime::now().duration_since(UNIX_EPOCH)`, `local_time.rs:31-33`) and must not be used for `request_duration_us` / stage latency: NTP steps or suspend/resume can produce skewed or negative deltas. `FuseReqLabels::start` is a `std::time::Instant` (or a monotonic `LocalTime::mono_nanos()` if one is added in Phase 0). Wall-clock timestamps, if ever needed for audit-log correlation, are captured separately and never subtracted to form a duration.

### Creation point

`FuseReqCtx` is created exactly once, in the receiver, **after**:

1. `receive()` returns `Ok(buf)`,
2. `FuseRequest::from_bytes()` succeeds, and
3. the opcode is known well enough to produce a static label.

It is stored on the `FuseResponse` returned by `new_reply()` (the renamed `new_replay`, `fuse_receiver.rs:123-125`). Because `FuseResponse` already travels everywhere a reply can be produced — directly for metadata, and *inside* `ReadTask`/`WriteTask` for stream (`fuse_reader.rs:31`, `fuse_writer.rs:33`) — no new plumbing through the `ReadTask`/`WriteTask` enums is required: the context rides along with the `FuseResponse` they already carry.

**Context reuse on stream enqueue failure.** `send_stream` has an error path that creates a *second* reply on failure: `self.new_replay(req.unique()).send_rep(res)` (`fuse_receiver.rs:158-160`). With a context-less `new_replay`, this would lose the original start time and labels and look like a brand-new, near-zero-latency request. The design therefore requires: error/retry replies created after stream dispatch failure **must reuse the original `FuseReqLabels`**. To make accidental loss hard, `new_replay(unique)` is renamed to `new_reply(unique, Option<FuseReqLabels>)`; the failure path threads the original labels through.

### FuseResponse API strategy (move-only guard) — decided

A move-only `ActiveGuard` cannot be moved out through the current `send_rep(&self)` / `send_buf(&self)` / `send_data(&self)` / `send_none(&self)` signatures (`fuse_response.rs:107,144,162,181`) — a `&self` method cannot give away an owned, non-`Copy` field. **Decision (fixed for Phase 1a, not a menu):**

- **Keep the public reply methods `&self`.** Hold the guard and the finish bookkeeping in an interior-mutable slot on `FuseResponse`:

  ```rust
  struct FuseRespMetrics {
      labels: FuseReqLabels,
      active: Option<ActiveGuard>,          // the E2E guard; take()n exactly once into RequestReply
      op_status: Option<FuseReqStatus>,     // FS-operation result; set by finish_status, read by operation_duration
      request_status: Option<FuseReqStatus>,// final delivery/result status; read by request_duration in the sender
      errno: i32,                           // errno label source for errors_total
      unsupported_reason: Option<&'static str>, // set ONLY at wildcard/Notimplemented/trait-default sites
      finished: bool,                       // state-machine guard (see below)
  }
  // FuseResponse gains: metrics: Option<Arc<Mutex<FuseRespMetrics>>>   // None when metrics_enabled=false
  ```

  **Field invariant:** `op_status`/`errno`/`unsupported_reason` are stored **independently of `active`**, so taking the guard into `RequestReply` does not clear the status that the `operation_duration` timer reads afterwards. `op_status` and `request_status` are separate fields precisely because they can differ — see "operation vs request status" below.

  Use a low-overhead `parking_lot::Mutex` (or equivalent). The lock is **uncontended**: the slot is written once by the reply path and read once by the sender. `&mut self` was rejected because `dispatch_meta(reply: &FuseResponse)` and the `send_rep_then_inval_*` helpers pass `&FuseResponse`; switching them to `&mut self` would reshape the dispatch matrix — the exact churn this proposal avoids.
- **`FuseResponse: Clone` (`fuse_response.rs:81`) semantics.** The `metrics` field is an `Arc<Mutex<…>>`, so a clone shares the *same* single slot — it does **not** duplicate the guard. The first reply `take()`s `active` and sets `finished=true`; any later reply (on the original or a clone) sees `active=None`/`finished=true` and is a **bug**: `debug_assert!(!finished)` in debug, ignored in release, **never** double-counts or double-decrements. (If `Clone` turns out to be unnecessary after Phase 1a it can be removed, but the shared-slot rule is correct either way.)
- The `send_rep_then_inval_inode` / `send_rep_then_inval_entry` helpers (`fuse_response.rs:193,204`) send a **request reply followed by a notification**. The request reply `take()`s the guard exactly once (producing a `RequestReply` task); the subsequent `send_inode_out` / `send_entry_out` produces a `NotifyReply` task and **must not** carry or re-finish the request context — with the shared slot already `finished`, a re-finish is structurally caught. This is the single most error-prone path and is a required test (see Testing).
- When `metrics_enabled=false`, `metrics` is `None`: no guard, no `FuseReqCtx`, no `Mutex` — the methods take the existing `&self` fast path unchanged (see "Disabled-mode object lifecycle").

### Status is computed once, in `finish_status()`

The request `FuseReqStatus` (and errno label) must be computed from the **FS operation result**, not from the value the reply methods return. This is subtle: a `dispatch_meta` arm is `reply.send_rep(fs.<op>().await).await`, and `send_rep` turns an `Err(FuseError)` into a *successful* reply frame and returns `IOResult<()> = Ok(())` (`fuse_response.rs:107-133`). Deriving status from that `Ok(())` would label nearly every request `success`.

Therefore:
- `send_rep` / `send_buf` / `send_data` call the private `finish_status(res, unsupported_reason) -> FuseReqStatus` **before** building the reply frame, and stash `op_status` + `errno` (+ any `unsupported_reason`) into the shared `FuseRespMetrics` slot — stored separately from `active`, so they survive the guard being taken. The sender reads them back when finishing `requests_total` / `request_duration_us` / `operation_duration_us`.
- The `operation_duration_us` timer (Phase 2) observes the **stashed `op_status`**, never the `IOResult<()>` of the awaited `send_*`. If wiring the stashed status into the operation timer proves invasive, the fallback is to ship `operation_duration_us` status-less initially (help string: "all metadata ops, status not yet attributed") rather than mislabel everything `success`.

**`unsupported_reason` carrier.** `finish_status` must distinguish a *genuinely* unsupported op from a backend op that merely returned `ENOSYS`. The errno alone cannot tell them apart (`ENOSYS` comes from both the dispatch wildcard and `FsError::Unsupported`, `fuse_error.rs:80,96`). The carrier is **`FuseRespMetrics.unsupported_reason: Option<&'static str>`**, *not* a new field on `FuseError` (which stays `{errno, error}`, `fuse_error.rs:22-26`). It is set explicitly at the three known sites — the `dispatch_meta` wildcard (`unknown_opcode`/`unimplemented_opcode`), the `Notimplemented` arm, and any `FileSystem`-trait default that returns `ENOSYS` (`trait_default`) — and `finish_status` yields `unsupported` **iff** `unsupported_reason.is_some()`. A backend `ENOSYS` reaches `finish_status` with `unsupported_reason=None` and is therefore classified `error` (errno `ENOSYS`), never laundered into `unsupported`. Both the trait-default and backend-`ENOSYS` cases are required tests.

**operation vs request status.** `op_status` and `request_status` are deliberately separate:
- `operation_duration_us{status}` uses `op_status` — the result of the filesystem operation itself.
- `request_duration_us{status}` uses `request_status` — the final delivery/result status observed at the sender.
- They are equal on the common paths, but **differ when the FS op succeeds yet delivery fails**: a successful op whose reply enqueue fails (`reply_enqueue_errors_total`) or whose kernel-fd write fails finishes `request_duration_us{status=error}` while `operation_duration_us` keeps `status=success`. Keeping them separate prevents delivery failures from polluting operation-latency classification. (On enqueue failure the request never reaches the sender, so `request_status` is set to `error` at the early-finish site; `op_status` retains whatever the op produced.)

### FuseResponse finish state machine

To prevent double-finish and gauge drift, each request context moves through an explicit state held in the shared `FuseRespMetrics` slot (`finished` flag); only the marked transitions touch metrics:

```text
Pending ──enqueue ok──▶ Enqueued ──sender splice──▶ SenderFinished   [requests_total, request_duration_us, response_*, drop ActiveGuard]
   │
   ├──enqueue err────▶ FinishedEarly      [reply_enqueue_errors_total, request_duration_us{status=error}, drop ActiveGuard]
   │
   ├──no-reply op done▶ NoReplyFinished    [requests_total{reply_type=no_reply, status=finish_status(result)}, request_duration_us, drop ActiveGuard]
   │
   └──parse fail after ctx▶ FinishedEarly  [decode_errors_total, drop ActiveGuard; NO requests_total]
```

Rules:
- `requests_total` and `request_duration_us` are recorded on exactly one terminal transition.
- `ActiveGuard` is dropped on every terminal transition, exactly once (its `Drop` does the gauge decrement; the state machine just guarantees the move happens once).
- A second reply attempt on an already-finished context (incl. via a `Clone`) is a bug; `debug_assert!(!finished)` in debug, ignored in release (never double-counts).
- `NoReplyFinished` classifies status from the operation result (see "No-reply status" in Status Semantics), not as an unconditional `success`.

### Finish points

There are **five** terminal paths, mutually exclusive per request:

| Path | Finish point | Metrics completed |
|---|---|---|
| Normal replied request (metadata or stream) | after `FuseSender::splice()` succeeds/fails | `requests_total`, `request_duration_us`, `response_write_duration_us`, `response_bytes_total`, `response_write_errors_total`, `errors_total` |
| No-reply request (`Forget`/`BatchForget`) | after `fs.forget()` / `fs.batch_forget()` completes (`finish_no_reply()`) | `requests_total{kind=metadata,reply_type=no_reply,status=finish_status(result)}`, `request_duration_us{kind=metadata}` — **no** `response_*` |
| Structural decode failure (`from_bytes`, **before** ctx) | at the failure site | `decode_errors_total{phase="decode",reason}` only — no ctx exists, nothing to drop |
| Structural parse failure (`parse_operator`, **after** ctx) | inside the spawned task / `send_stream`, where the failure is observed | `decode_errors_total{phase="parse",reason}`, **drop `ActiveGuard`**; **no** `requests_total` |
| Reply-enqueue failure | immediately when `sender.send()` errors (request never reaches the sender) | `reply_enqueue_errors_total{opcode,reason}`, `request_duration_us{status=error}`, drop `ActiveGuard` |

The fourth row matters because `parse_operator()` runs *inside* the spawned metadata task (`dispatch_meta`, `fuse_receiver.rs:258`) and inside `send_stream` (`:142`), so a structural parse failure can occur **after** `FuseReqCtx` was created. That path must drop the active guard (so `active_requests` does not leak) and record a `decode_errors_total{phase="parse"}`, but must **not** emit `requests_total` (the request never dispatched). It is distinct from `Notimplemented`, which is a *successful* parse that produces a real `ENOSYS` reply and is counted under `unsupported_total` (see Status Semantics).

`reply_enqueue` (the `AsyncSender::send().await` itself) is still observed as a `stage_duration_us{stage="reply_enqueue"}` on the success path. On enqueue failure, the `FuseReqLabels` must be copied out **before** the `send()` attempt (the `RequestReply` task — and its guard — may be consumed/returned by the failed send depending on the channel API), so the early finish can record `reply_enqueue_errors_total{opcode,reason}` + `request_duration_us{status=error}` and drop the guard exactly once.

### In-flight guards

In-flight gauges use a RAII guard so early returns and errors cannot leak a count. Because the guard's natural drop point differs from the request's E2E finish, the gauges are named for what they actually measure rather than implying a single "in-flight request" number:

- **`active_requests{kind}`** — the true E2E in-flight count, from `FuseReqCtx` creation to **sender/no-reply finish**. The guard is carried on `FuseTask::RequestReply` (alongside the labels) and dropped after `FuseSender::splice()`, so reply-channel queue wait and response write are included. This is the gauge to alert on for saturation. (The guard *plumbing* lands in Phase 1a-1; the gauge it drives is emitted in Phase 1a-2.)
- **`stream_io_inflight{opcode}`** — reader/writer task only (recv → backend IO → reply produced). Strictly shorter than `active_requests`: it excludes reply-channel wait and the kernel-fd write. Useful for isolating backend-IO concurrency from delivery backpressure. (Ships in **Phase 2** — it requires instrumenting the reader/writer task bodies, the same files Phase 2 already touches.)
- **`meta_task_inflight`** — spawned metadata task only (`rt.spawn` submission → dispatch returns). This is the unbounded-backlog saturation signal for metadata storms (formerly `meta_inflight`). (Ships in Phase 1b.)
- **`setlkw_inflight`** — SETLKW interruptible waits, tied to the `pending_requests` entry. (Phase 2.)

The distinction matters: `stream_io_inflight` and `meta_task_inflight` are task-local scopes that can each be ≈0 even while `active_requests` is high (e.g. replies piling up in the channel behind a slow kernel-fd write). Conflating them into one gauge would hide exactly that failure mode.

## Naming and Label Policy

### Naming

All new FUSE-specific metrics use the `curvine_fuse_` prefix. This avoids collision with generic metrics (`capacity`, `available`, etc.) and the unprefixed `client_*` metrics already on the same `/metrics` endpoint.

The three legacy gauges (`inode_num`, `file_handle_num`, `dir_handle_num`) are kept for backward compatibility for at least one release, and namespaced equivalents are added.

### Label rules

Allowed labels are bounded enums:

- `opcode` — FUSE opcode short name (e.g. `Lookup`, `GetAttr`, `Read`, `Write`, `Release`). Source of truth: a new `FuseOpCode::as_str() -> &'static str` method (Phase 0).
- `kind` — `metadata` | `stream` | `framework`. There is **no** `no_reply` kind; `Forget`/`BatchForget` are `kind=metadata` and, where it matters, carry `reply_type=no_reply` (see below).
- `reply_type` — `replied` | `no_reply`. Used only where the distinction is needed (e.g. on `requests_total`) so no-reply requests do not appear to have a reply pipeline. Defaults to `replied`.
- `status` — `success` | `error` | `interrupted` | `unsupported` (see "Status semantics" below).
- `errno` — short symbolic name from a fixed table (`ENOENT`, `EIO`, `EINTR`, `ENOSYS`, `EAGAIN`, `EACCES`, `EBADF`, `EINVAL`, `EPERM`, `EOPNOTSUPP`, `EEXIST`, `ENOTDIR`, `EISDIR`, `ENOTEMPTY`, `ETIMEDOUT`, `ENOSPC`, `ETXTBSY`, `EPROTO`, `ERANGE`, `ENODATA`, `ENOMEM`, `EBUSY`, `ENAMETOOLONG`, `OTHER`). Only attached to error metrics where a real OS errno exists (see `reason` for channel-level failures). The table is the closed set of errnos actually produced in the FUSE layer (verified by grepping `libc::E*` across `curvine-fuse/src/` plus the `FsError → errno` mapping in `fuse_error.rs`). `EBUSY` originates from `FsError::InProgress` (write-in-progress contention) and `ENAMETOOLONG` from name-length validation; both are explicitly enumerated because collapsing them into `OTHER` would hide two of the most diagnostically useful classes. When the table is extended, `errno_label()` (Phase 0) and this list must be updated together.
- `stage` — `parse_operator` | `meta_spawn` | `stream_enqueue` | `operation` | `stream_io` | `reply_enqueue` | `reply_write`. (Note: `receive` is **not** a stage — see the lifecycle "What this implies" note; splice cost is the standalone `receive_loop_wait_duration_us`. State persist/restore stages are a *separate* metric, `state_stage_duration_us`, not this enum.)
- `io_type` — `read` | `write` | `flush` | `fsync` | `release`.
- `path_type` — `curvine` | `ufs` | `fallback` | `local` | `unknown`.
- `cache` — `status` | `list` | `blocks` (the `MetaCache` userspace metadata caches). The `NodeMap` dcache is a *different* layer and uses its own `node_cache_total{operation,status}` metric, not this label (see Cache metrics).
- `code` — for notifications: a `FuseNotifyCode` short name (`inval_inode` | `inval_entry` | `delete` | `store` | `retrieve` | `poll` | `other`). Source: the `FuseNotifyCode` enum (`fuse_notify_code.rs`).
- `action` — for receive errors: `continue` | `exit` (whether the splice-error `match` continued the loop or broke out).
- `phase` — for decode errors: `decode` (structural `from_bytes` failure) | `parse` (a structural failure *inside* `parse_operator`, e.g. `decoder.get_struct()?` on a truncated buffer). Note: `parse_operator` returning `Ok(FuseOperator::Notimplemented)` for a known-but-unmapped opcode is **not** a decode error — it parsed fine — and is routed to `unsupported_total`, not here.
- `reason` — bounded enums per metric. For `unsupported`: `unknown_opcode` | `unimplemented_opcode`. For `reply_enqueue_errors`: `channel_closed` | `runtime_shutdown` | `other`. For `decode_errors`: `short_read` | `invalid_header` | `length_mismatch` | `other`. For cache invalidations/evictions: see the respective metric sections.

### Forbidden labels

- File path
- Inode number
- File handle / unique
- User name / UID
- Process ID
- Mount path
- Worker address
- Free-form error message
- Anything formatted from runtime data

### Static-string discipline

Every label site must construct labels from `&'static str` values. Allocations are *not* allowed on the hot path. This is enforced by:

- `FuseOpCode::as_str()` (Phase 0).
- `errno_label(i32) -> &'static str` (Phase 0).
- `path_type` captured as `&'static str` in `FuseReader` / `FuseWriter` at construction.
- `reason` literals at the call site of `invalidate_cache`.

> **Important: the existing `MetricTimerVec` is *not* zero-allocation.** `orpc::common::time_spent::MetricTimerVec` stores its labels as `Vec<String>` and, on every `drop`, executes `self.label_values.iter().map(|s| s.as_str()).collect::<Vec<&str>>()` before calling `with_label_values`. That is one heap allocation **per observation**, which directly violates the rule above. The "use drop guards" recommendation below therefore does **not** mean "reuse `MetricTimerVec` as-is". Phase 0 adds a new guard that stores an already-resolved `Histogram` child (see Phase 0 scope) so the drop path is a single `observe()` with no allocation and no per-call label-map probe.

## Status Semantics

`status` has four values. It is classified by `finish_status()` from the **FS operation result plus an explicit source tag**, *not* by errno alone (errno is ambiguous — see the EINTR/ENOSYS notes below):

| Result / source | status |
|---|---|
| `Ok(_)` returned to kernel | `success` |
| `Err` originating from the SETLKW interrupt-notify path (`fuse_receiver.rs:244`) | `interrupted` |
| `Err` whose source is a known unsupported path (wildcard / `Notimplemented` / trait default) | `unsupported` (+ `unsupported_reason`) |
| Any other `Err` (including a backend `FsError::Unsupported → ENOSYS`) | `error` |

`status` is determined at the finish point (sender, for replied requests) — not at receive time — so the timer and the status label always agree. `requests_total` is incremented at finish, not on receipt (except no-reply requests, which finish at their own point below). If a separate "received but not yet finished" count is ever needed, it is `active_requests` / `meta_task_inflight`, not `requests_total`.

**Why not errno alone:**
- **`interrupted` = the SETLKW path only.** In the FUSE layer `EINTR` is produced at exactly one place that becomes a reply: the interrupt-notify branch of `dispatch_meta_interrupt` (`fuse_receiver.rs:244`). The other `EINTR` (`:203`) is a splice-error `continue` that never forms a reply. So `interrupted` is defined as "result from the interrupt-notify path", carried as a source tag — not "any reply with `errno==EINTR`". This keeps the classification unambiguous if some future op surfaces `EINTR` for an unrelated reason.
- **`unsupported` is not "any `ENOSYS`".** `ENOSYS` arises both from the dispatch wildcard / `Notimplemented` (genuinely unsupported) **and** from `FsError::Unsupported` mapping at the backend (`fuse_error.rs:80,96`). Classifying every `ENOSYS` as `unsupported` would hide a real backend failure as a protocol gap. `finish_status` marks `unsupported` only when the source is a known unsupported path (carrying `unsupported_reason`); a backend `ENOSYS` with no such tag is a plain `error` (with `errno=ENOSYS`). The source tag is set at the wildcard/`Notimplemented` site, not inferred from the integer.

Additionally:

- `Forget` and `BatchForget` send no reply (`send_none`, `fuse_response.rs:181`, which currently **discards** its `FuseResult<()>` argument). Their finish point is `finish_no_reply(result)` — invoked after `fs.forget()` / `fs.batch_forget()` completes — which **inspects the result** and classifies status via the same `finish_status()` logic. A failing forget is therefore `status=error`, not a phantom `success`. They contribute to `requests_total{kind=metadata,reply_type=no_reply,status=…}` and `request_duration_us{kind=metadata}`, but **not** to any reply-pipeline metric (`reply_enqueue`, `reply_write`, `response_*`). (They are `kind=metadata`, never a separate `no_reply` kind — the `reply_type` label carries the distinction.) No-reply error classification is a required test.
- Kernel notifications (`send_notify`, `unique == FUSE_NOTIFY_UNIQUE`) share the reply channel but are **not** request replies. They travel as the `FuseTask::NotifyReply { data, code }` variant (carrying their `FuseNotifyCode`, not as a `labels=None` request reply), are counted in a separate `curvine_fuse_notify_total{code,status}` series, and are excluded from `requests_total` and from request-side finish in the sender. The three `status` values are recorded at **three different points**: `send_notify` records `enqueue_failed` when its channel `send()` fails; the sender records `write_failed` when the splice fails; and `success` is recorded **only after the sender write succeeds** — never at enqueue time. This mirrors the request path's "finish in the sender" rule and means an enqueued-but-never-written notification is not prematurely counted as delivered.
- **`unsupported` is split by `reason`** (the `unsupported_reason`, set at the source path — never inferred from errno):
  - `unknown_opcode` — the raw opcode maps to `FuseOpCode::NOT_SUPPORTED = 0` (the `#[num_enum(default)]`), i.e. the kernel sent an opcode this build does not even have an enum value for. This is a kernel/daemon **compatibility** signal.
  - `unimplemented_opcode` — a known, parseable opcode that `parse_operator` resolves to `FuseOperator::Notimplemented` (`operator.rs:23`, the `_ =>` arm at `fuse_request.rs:308`) or that hits the `dispatch_meta` wildcard and returns `ENOSYS` (e.g. `Rename2`, and the parsed-but-unhandled `FUSE_DESTROY`, `FUSE_LSEEK`, `FUSE_BMAP`, `FUSE_IOCTL`, `FUSE_POLL`). This is an **implementation-gap** signal.
  - `trait_default` — a `FileSystem`-trait method whose default implementation returns `ENOSYS` (an op declared but not overridden by `CurvineFileSystem`). Distinct from the wildcard because the opcode *was* dispatched.
  - All keep the `opcode` label and increment `curvine_fuse_unsupported_total{opcode,reason}`. Critically, `Notimplemented` is a successful parse that produces a real `ENOSYS` *reply* — it is **not** counted as a `decode_errors_total` entry. And a backend-originated `ENOSYS` (`FsError::Unsupported`) carries **no** `unsupported_reason`, so it stays `status=error` — it is not laundered into `unsupported`. Distinguishing these lets operators tell "the kernel speaks a protocol version we don't" from "we haven't implemented this call yet" from "the backend rejected this op".

## Latency Metrics and Prometheus Semantics

Latency metrics use Prometheus histograms, not gauges. Prometheus scrapes the cumulative bucket counters plus `sum` and `count`; clients compute averages and percentiles with PromQL.

Average latency:

```promql
rate(curvine_fuse_request_duration_us_sum[5m])
/
rate(curvine_fuse_request_duration_us_count[5m])
```

Percentiles:

```promql
histogram_quantile(
  0.99,
  sum by (le, opcode) (
    rate(curvine_fuse_request_duration_us_bucket[5m])
  )
)
```

Implications:

- Use `HistogramVec` for request, operation, stage, and IO latency.
- Do not export a precomputed `average_latency` metric.
- Do not use a gauge for `last_latency_us` except for ad-hoc debugging.
- Use counters for requests, errors, bytes.
- Use gauges for current state only (in-flight, queue depth, handle count).

All duration metrics use **microseconds**, matching the existing `client_read_time_us` / `client_write_time_us` convention.

## Proposed Metrics

### Request and error metrics

```text
curvine_fuse_requests_total{opcode,kind,reply_type,status}
curvine_fuse_errors_total{opcode,kind,errno}
curvine_fuse_interrupted_total{opcode}
curvine_fuse_unsupported_total{opcode,reason}
curvine_fuse_notify_total{code,status}
curvine_fuse_decode_errors_total{phase,reason}
```

Notes:
- `requests_total` increments **at the finish point** (sender splice for replied requests; `finish_no_reply()` for `Forget`/`BatchForget`), once status is known — not on receipt. `reply_type=no_reply` only for `Forget`/`BatchForget`, else `replied`. See Status Semantics.
- `errors_total` is only incremented on `status in {error, unsupported}`; `interrupted` has its own counter.
- `unsupported_total{opcode,reason}` carries `reason in {unknown_opcode, unimplemented_opcode}` per Status Semantics. `Notimplemented` opcodes land here, not in `decode_errors_total`.
- `notify_total{code,status}` covers kernel notifications sent via `send_notify` (carried as the `FuseTask::NotifyReply` variant); `code` is the `FuseNotifyCode` short name (`inval_inode`, `inval_entry`, …) and `status` is `success | enqueue_failed | write_failed`. Because notifications traverse the sender splice like replies, the `write_failed` state attributes a kernel-fd write error to the notify `code` (it would otherwise be lost behind the generic sender warning). Optionally split as `notify_write_errors_total{code,errno}` if a per-errno breakdown is wanted.
- `decode_errors_total{phase,reason}` counts **structural** failures that happen before any reply context exists: `phase="decode"` for `FuseRequest::from_bytes` failures (`fuse_receiver.rs:171`), `phase="parse"` for a structural error *inside* `parse_operator` such as `decoder.get_struct()?` on a truncated buffer (`fuse_request.rs`). `reason` is a bounded enum: `short_read` | `invalid_header` | `length_mismatch` | `other`. These are not part of `requests_total` (the request never reached dispatch) nor `errors_total` (no opcode/errno is known). **`Notimplemented` is not here** — it is a successful parse routed to `unsupported_total` (see Status Semantics).
  - **Control-flow caveat (do not regress behaviour):** today a `from_bytes` failure propagates via `?` out of the receiver `start()` loop (`fuse_receiver.rs:171`) and **terminates the receiver task** — it is not merely logged. Instrumentation must increment `decode_errors_total` *without* changing whether the receiver continues or exits. Whether decode failures should become non-fatal (continue the loop) is a separate behavioural decision and is explicitly out of scope for the metrics work; the counter is added on the existing path only.

### Request and stage latency metrics

```text
curvine_fuse_request_duration_us{opcode,kind,status}          # E2E, finished in sender; status = request result
curvine_fuse_operation_duration_us{opcode,kind,status}        # metadata: includes reply_enqueue (see help string)
curvine_fuse_stage_duration_us{stage,kind,status}             # opcode-free by default (see cardinality note)
curvine_fuse_response_write_duration_us{opcode,request_status} # FuseSender::splice() only; request_status, NOT delivery status
curvine_fuse_response_bytes_total{opcode,request_status}      # response size at sender finish
curvine_fuse_reply_enqueue_errors_total{opcode,reason}        # AsyncSender::send() failed (channel-level)
curvine_fuse_response_write_errors_total{opcode,errno}        # kernel-fd write failed in sender (delivery failure)
```

`stage` values: `parse_operator` | `meta_spawn` | `stream_enqueue` | `operation` | `stream_io` | `reply_enqueue` | `reply_write`. (`receive` is intentionally excluded — see lifecycle "What this implies".)

Notes:
- `request_duration_us` is the daemon-internal end-to-end latency, finished in the sender after `FuseSender::splice()`. It includes reply-channel queue wait and the kernel-fd write. Because the sender is a shared pool, this also captures cross-request head-of-line queueing on the reply channel — that is real latency the kernel observes and is intentionally included.
- `operation_duration_us` (metadata only) **includes the awaited reply enqueue** by construction (the `dispatch_meta` arm is one expression). Its help string states this. Do not compute `operation - reply_enqueue` and present it as pure operation latency; the subtraction is approximate and invalid on error/no-reply/notify paths.
- `response_write_duration_us` isolates the kernel-fd write in the sender (`fuse_sender.rs:101`). Its label is **`request_status`** (the FS operation result: `success|error|interrupted|unsupported`), **not** delivery success/failure — a request can have `request_status=error` while the write itself succeeds. Delivery failures live in `response_write_errors_total{opcode,errno}`. (Overloading a single `status` to mean both was ambiguous; the two are independent dimensions.) `request_duration_us - response_write_duration_us` is a population-level estimate of "everything before the write".
- `response_bytes_total{opcode,request_status}` is the total reply size, computed at sender finish (the `ResponseData` length is already known there, `as_iovec()` sums it). Low cardinality; correlates large responses with slow `response_write_duration_us`, especially for `ReadDir`/`ReadDirPlus`/`Readlink`/`GetXAttr`. (Read *data* bytes are still `io_bytes_total{io_type=read}`; this is the on-wire FUSE reply size.)
- `reply_enqueue_errors_total{opcode,reason}` uses `reason` (`channel_closed|runtime_shutdown|other`), **not** `errno`: `AsyncSender::send()` failure is a channel-closed/shutdown condition with no OS errno. It means the request never reaches the sender; the request is finished early with `status=error`. `response_write_errors_total{opcode,errno}` keeps `errno` because the kernel-fd `writev`/`splice` does produce a real OS error (replaces the existing `warn!("error send unique …")` in the sender loop, `fuse_sender.rs:72`).
- **Stage cardinality:** `stage_duration_us` is intentionally opcode-free (stage × kind × status only) to bound series count. Per-opcode attribution of metadata operations is available via `operation_duration_us{opcode}`. If finer stage attribution is needed, `metrics_detail_level >= detailed` (Phase 4) may add an `opcode` dimension to `stage_duration_us` for the `operation` and `stream_io` stages only; it is off by default.

Recommended buckets for request and operation latency (µs):
```
10, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
25000, 50000, 100000, 250000, 500000, 1000000,
2500000, 5000000, 10000000
```

Recommended buckets for short framework stages (`reply_enqueue`, `reply_write`, `meta_spawn`, µs):
```
5, 10, 25, 50, 100, 250, 500, 1000, 2500,
5000, 10000, 25000, 50000, 100000
```

### In-flight and queue metrics

```text
curvine_fuse_active_requests{kind}              # E2E in-flight: ctx creation -> sender/no-reply finish
curvine_fuse_stream_io_inflight{opcode}         # reader/writer task only (shorter than E2E)
curvine_fuse_meta_task_inflight                 # spawned metadata task only
curvine_fuse_setlkw_inflight                    # was: pending_interruptible_requests (v1)
curvine_fuse_reply_queue_depth
curvine_fuse_stream_write_queue_depth           # gauge (event-driven); global across all writers
curvine_fuse_receiver_tasks
curvine_fuse_sender_tasks
```

Rationale (gauges are named for the scope they actually measure — see "In-flight guards"):
- `active_requests{kind}` is the **true E2E in-flight** count: incremented at `FuseReqCtx` creation, decremented at the sender finish (or `finish_no_reply`). The guard is carried on `FuseTask::RequestReply`, so the count covers reply-channel queue wait and the kernel-fd write. This is the gauge that reflects "how many requests are the kernel currently waiting on us for".
- `stream_io_inflight{opcode}` is the reader/writer-task scope only (recv → backend IO → reply produced). It is **deliberately shorter** than `active_requests{kind=stream}`: it excludes reply-channel wait and response write. Naming it `stream_io_inflight` rather than reusing `active_requests` prevents the misreading that it is a full-request gauge. It can sit near 0 while `active_requests` is high if replies are backing up behind a slow kernel-fd write.
- `meta_task_inflight` is the count of metadata tasks `rt.spawn`-ed (`fuse_receiver.rs:193`) but not yet returned. This is the **only genuinely unbounded queue on the metadata path**: there is no bounded channel between receive and metadata execution, so when the master slows down the backlog accumulates here, not in `reply_queue_depth`. Event-driven `AtomicI64` (`+1` at spawn submission, `-1` at the end of the spawned future, including interrupt/error paths). The name says "task, not full request" — its scope is the spawned task only, parallel to `stream_io_inflight`.
- `setlkw_inflight` reflects the only operation today that is interruptible (`FUSE_SETLKW`). The previous name `pending_interruptible_requests` was misleading because it implies a much broader set.
- `reply_queue_depth` requires an event-driven `AtomicI64` because `tokio::mpsc::Sender` does not expose `len()`. (It is a gauge; no `_total` suffix.) **Update rules (must be explicit in code):** increment **only after** `sender.send()` succeeds; decrement when the **sender `recv()`s** the task off the channel (i.e. when it leaves the queue), *not* after `splice()` completes — the task is no longer queued once received; on enqueue failure do **not** increment; on sender shutdown with tasks still queued, the gauge may transiently read non-zero until drained — documented as acceptable drift, and reset to 0 on receiver/sender teardown. These rules are testable and prevent the gauge from leaking on the error paths.
- `stream_write_queue_depth` is a single global atomic gauge, not per-handle. Per-handle gauges would require per-`FuseWriter` registration and high cardinality. It has no `_total` suffix because Prometheus reserves `_total` for monotonic counters; this is a gauge. Same inc/dec discipline as `reply_queue_depth` (inc after a successful enqueue into the writer channel, dec when the writer task pulls the task). (Read/flush/release stream queues are not separately surfaced today — write backpressure is the dominant concern; a future `stream_task_queue_depth{io_type}` could generalize it if needed.)
- `receiver_tasks` and `sender_tasks` are fixed-size pools (`mnt_per_task * num_mounts`), each set once at startup. No `state` label is necessary; an exit-counter is added separately as `curvine_fuse_task_exits_total{role}`.

### IO metrics

```text
curvine_fuse_io_bytes_total{io_type,path_type,status}
curvine_fuse_io_requests_total{io_type,path_type,status}
curvine_fuse_io_duration_us{io_type,path_type,status}
curvine_fuse_io_size_bytes{io_type,path_type}
curvine_fuse_io_enqueue_duration_us{io_type}    # stream_enqueue cost only
```

`io_type`: `read` | `write` | `flush` | `fsync` | `release`.
`path_type`: `curvine` | `ufs` | `fallback` | `local` | `unknown`.

Recommended buckets for IO size (bytes):
```
4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864
```

Notes:
- `io_duration_us` is observed **at completion of the real IO inside the reader/writer task**, not at FUSE enqueue time.
- `io_enqueue_duration_us` is the time `FuseWriter::write` / `FuseReader::read` spends putting the task on the internal channel. This separates kernel-side latency from backend latency. (Reader enqueue is typically near-zero; writer enqueue can spike when stream channel is full.)
- `path_type` is captured at handle construction. A long-lived reader that internally falls back from `curvine` to `ufs` mid-stream (`FallbackFsReader`) is reported as `fallback`.

### Cache metrics

These cover **two distinct daemon userspace cache layers**, kept as separate metrics rather than merged under one `cache` label — they are different abstractions:

1. **`MetaCache`** (`curvine-common/src/fs/meta_cache.rs`) — the status/list/blocks metadata cache wrapping `FastSyncCache`.
2. **`NodeMap` dcache** (`node_map.rs`) — the daemon's inode/name lookup state. This is *not* a `MetaCache` instance; conflating them would imply a uniformity that does not exist in the code.

```text
curvine_fuse_user_meta_cache_total{cache,status}          # MetaCache: cache in {status,list,blocks}
curvine_fuse_user_meta_cache_invalidations_total{cache,reason}
curvine_fuse_node_cache_total{operation,status}           # NodeMap dcache: operation in {lookup}
curvine_fuse_negative_entry_returned_total
```

`cache` values (on `user_meta_cache_*`): `status` (`get_cached_status`) | `list` (`get_cached_list`) | `blocks` (`node_state.rs get_cached_blocks`). The name `user_meta_cache` distinguishes these from the kernel's own dentry/attr caches and from `curvine-client`'s `client_mount_cache_*` series on the same endpoint.

`operation` values (on `node_cache_total`): `lookup` (`node_map.rs lookup_node`); more `NodeMap` operations can be added without touching the `MetaCache` metric.

`status` enum on both `*_cache_total` (bounded): `hit` | `miss` | `put`. A single counter with a `status` label is used instead of separate `_hits_total`/`_misses_total` series so hit-rate is `hit / (hit+miss)` over one metric family.

`negative_entry_returned_total` counts **negative dentry results** returned to the FUSE layer (a cached `ENOENT` for a `Lookup` that previously missed, added in commits `aa0b5f6` / `7278305`). It is deliberately a *separate* counter rather than a `hit`: from the daemon's point of view, returning a cached "this name does not exist" is a distinct outcome from a positive lookup, and conflating them would make the positive hit-rate misleading. Pairing this with `requests_total{opcode="Lookup",status="error",errno="ENOENT"}` shows how effectively the negative cache absorbs repeated failed lookups.

`reason` enum for `invalidations` (bounded): `setattr`, `mkdir`, `unlink`, `rmdir`, `rename`, `link`, `release_writer`, `flush_writer`, `kernel_notify`, `other`. Invalidation counters count **requested** invalidations at the call site (the `invalidate_cache(path, reason)` call), not confirmed entry removals — the cache API does not report how many entries were actually evicted.

> **Eviction counters are deferred.** `MetaCache` wraps `FastSyncCache` and exposes **no eviction callback or listener** in its current API (`meta_cache.rs` has only get/put/entry; `FastSyncCache` has no `on_evict` hook). A `user_meta_cache_evictions_total{cache,reason=ttl|capacity}` metric would require adding an eviction hook to `orpc::sync::FastSyncCache`, which is outside the "do not significantly disturb existing implementation" constraint of this proposal. Eviction counters are therefore **deferred** to a follow-up that either adds that hook or samples cache size deltas; they are explicitly not promised in Phase 3.

> Note on misses: `expired` cannot be distinguished from a cold miss in the current `FastSyncCache` API, so a miss is recorded simply as `status="miss"`; an `expired`-vs-cold breakdown is a known limitation deferred until the cache API exposes it.

### Readdir metrics

```text
curvine_fuse_readdir_entries{status}             # histogram of entry counts
curvine_fuse_readdir_duration_us{status}
```

### State persist / restore metrics

```text
curvine_fuse_state_persist_total{status}
curvine_fuse_state_persist_stage_duration_us{stage,status}
curvine_fuse_state_persist_handle_count{kind}    # gauge sampled at persist time
curvine_fuse_state_restore_total{status}
curvine_fuse_state_restore_stage_duration_us{stage,status}
```

`stage` values here: `node_map` | `file_handles` | `dir_handles` | `mount_fds`. **These are dedicated state-recovery metrics, entirely separate from the request-lifecycle `stage_duration_us`.** The `stage` *label name* is shared, but the metric families and the label's value set are different domains (state recovery vs request handling); a query against `stage_duration_us` never sees `node_map`, and a query against `state_*_stage_duration_us` never sees `reply_write`. They are kept as two `state_persist_*`/`state_restore_*` families (rather than one `state_stage_duration_us{phase}`) to match the existing persist/restore code paths, which are separate functions; either shape is acceptable as long as recovery stages never enter the request `stage` enum.

These are event-triggered (SIGUSR1 / restart with env var). PromQL `rate()` is intentionally near-zero in steady state; the metrics are useful for last-event diagnosis and as alarm sources when stage durations deviate.

### Session lifecycle metrics

```text
curvine_fuse_session_init_total{result}
curvine_fuse_session_shutdown_total{reason}      # reasons: completed | term_signal | sigusr1_persist | fd_watcher
curvine_fuse_kernel_fd_health{state}             # gauge: ok=1, hup=0
```

### Existing gauge compatibility

```text
inode_num                       # legacy, kept
file_handle_num                 # legacy, kept
dir_handle_num                  # legacy, kept
curvine_fuse_inode_count        # new, namespaced
curvine_fuse_file_handle_count  # new, namespaced
curvine_fuse_dir_handle_count   # new, namespaced
```

Both pairs are maintained event-driven (incremented on insert, decremented on remove), removing the scrape-time map traversal in `NodeState::set_metrics()`. The legacy gauges are deprecated and will be removed two minor releases after the namespaced gauges ship and dashboards migrate.

### Self-observation metrics

```text
curvine_fuse_metrics_scrape_duration_us
curvine_fuse_metrics_scrape_bytes
curvine_fuse_receive_loop_wait_duration_us       # standalone; NOT a request stage (includes idle wait)
curvine_fuse_receive_errors_total{errno,action}  # splice errno + loop action
curvine_fuse_setlkw_wait_duration_us
```

`receive_loop_wait_duration_us` is the splice + header-parse cost in the receiver loop. The name says "loop wait" deliberately: it is a **standalone saturation/health histogram**, explicitly not a member of `stage_duration_us` and not part of `request_duration_us`. At low load it is dominated by idle wait for the next kernel request and converges to ~1/QPS, so it must **not** be read as request-receive latency or compared against the per-request stages. It answers "is the receiver saturated / is the kernel feeding us", not "how long did receiving a request take" — the explicit `_loop_wait_` name is chosen to prevent that misreading.

`receive_errors_total{errno,action}`: receive errors occur **before** a request is decoded, so request `kind` is unknown — the label is the splice `errno` (`enoent`|`eintr`|`eagain`|`enodev`|`other`) paired with the loop `action` it triggered (`continue` for `ENOENT`/`EINTR`/`EAGAIN`, `exit` for `ENODEV`, `other` errors return). This mirrors the existing splice-error `match` (`fuse_receiver.rs:201-207`) exactly and replaces its `warn!` log. (The reply-side write-error counter lives with the reply-pipeline metrics as `response_write_errors_total{opcode,errno}`.)

## Performance and Overhead Control

### Hot path costs

Metrics can add overhead through:

- `inc()` and `observe()` on every request.
- Label lookup for `*Vec` metrics (one map probe per call).
- Histogram bucket updates.
- Time measurement calls (`Instant::now()` / monotonic clock — see "Time source"; cheap on Linux via `clock_gettime(CLOCK_MONOTONIC)` vDSO, but not free).
- Allocations from formatted labels (forbidden — see "Static-string discipline").
- Lock contention if metrics are updated under business locks.

Design rules:

1. All labels are `&'static str`. No `format!`, no `to_string`, no `Cow::Owned`.
2. For high-frequency `*Vec` metrics, pre-resolve the `WithLabelValues` child where the label set is known at construction time (e.g., per `FuseReader`/`FuseWriter`). This converts label lookup into a stored `Histogram` reference.
3. Drop guards are used for stage timers so the hot path looks like a single `let _t = ...;`. The existing `orpc` `MetricTimer` (counter-backed) is allocation-free and may be reused directly. The existing `MetricTimerVec` is **not** reused, because it re-allocates a `Vec<&str>` on every drop (see "Static-string discipline"). Phase 0 introduces `HistogramTimer { start: u128, hist: Histogram }` — a guard holding an already-resolved `Histogram` child (obtained once via `with_label_values`), so its drop path is a single `observe()` with no allocation and no label-map probe.
4. Counters and gauges are `AtomicI64`; updates do not block.
5. No metric update is performed while holding a `RwLock` write guard on inode/handle maps. If a metric must be updated near a critical section, the value is computed inside and observed outside.
6. `set_metrics()` scrape-time recomputation is **removed**. All gauges that previously came from map traversal are made event-driven.

### Scrape costs

Prometheus scraping reads `default_registry().gather()` and encodes text. Costs grow with:

- Time series count (each histogram bucket is one series; 18-bucket histograms with 35 opcodes × 4 statuses ≈ 2520 series per histogram).
- The size of each metric's `MetricFamily` value.
- Any work done in `metrics_handler` itself.

Design rules:

1. The `metrics_handler` does **only** `Metrics::text_output()`. No state-gauge refresh, no client RPC, no map traversal.
2. Default scrape interval is documented at ≥ 10s; production should default to 15s or 30s.
3. `curvine_fuse_metrics_scrape_duration_us` is observed inside the handler around `text_output()` to detect regression.
4. `curvine_fuse_metrics_scrape_bytes` is set to `output.len()` after each scrape so dashboards can track payload growth.

### Optional detail levels

A future `[fuse]` config block can let operators trade visibility for overhead:

```toml
[fuse]
metrics_enabled = true
metrics_detail_level = "basic"          # off | basic | detailed
metrics_io_size_histogram = true
metrics_stage_histogram = true
metrics_cache_breakdown = true
```

The `metrics_enabled` master switch ships in Phase 1 (see "Kill switch in Phase 1"). The remaining detail-level switches are added in Phase 4 once the cardinality cost is measured; Phase 1–3 ship everything enabled by default. The internal API uses a `FuseMetricsLevel` enum read once at startup, not at each call site.

### Cardinality budget

Each new metric has an explicit time-series budget:

```text
curvine_fuse_request_duration_us:
  opcode ~= 35, kind <= 2, status <= 4, buckets = 18 + sum + count
  series ~= 35 * 2 * 4 * 20 = 5600

curvine_fuse_operation_duration_us:
  same shape ~= 5600

curvine_fuse_stage_duration_us:
  stage = 7, kind <= 3, status <= 4, buckets = 14 + sum + count
  series ~= 7 * 3 * 4 * 16 = 1344 (sparse: most stages occur for only one kind)

curvine_fuse_io_duration_us:
  io_type = 5, path_type <= 5, status <= 4, buckets = 18 + sum + count
  series ~= 5 * 5 * 4 * 20 = 2000

curvine_fuse_errors_total:
  opcode ~= 35, kind <= 2, errno <= 22
  series ~= 35 * 2 * 22 = 1540 (sparse in practice)
```

The smaller series in this design are negligible against this budget: `decode_errors_total{phase,reason}` is sparse, `meta_task_inflight`/`active_requests`/`stream_io_inflight` are a handful of gauges, `negative_entry_returned_total` and `node_cache_total` are tiny, `response_write_duration_us{opcode,request_status}` and `response_bytes_total{opcode,request_status}` each add one opcode×status family, and the `reply_type` label at most doubles `requests_total`. The channel-level `reply_enqueue_errors{opcode,reason}` and the `{code,status}` notify series do not grow the order of magnitude. None change the ~15k upper bound materially.

Total estimated upper bound for Phase 1 + 2: ~15k series. Sparse population in practice will be lower because not every (opcode × status × errno) combination occurs. The budget is reviewed before each phase merges.

### Benchmark and acceptance criteria

Before each phase merges, the following measurements must be taken in fio + bonnie++ workloads on a representative cluster:

| Workload | Metric | Acceptance threshold |
|---|---|---|
| Metadata QPS (1k clients, mixed lookup/getattr) | QPS regression | ≤ 1% |
| Sequential read 1MB | Throughput regression | ≤ 0.5% |
| Sequential write 1MB | Throughput regression | ≤ 0.5% |
| Random read 4k | p99 absolute increase | ≤ 5 µs |
| Random write 4k | p99 absolute increase | ≤ 5 µs |
| Metadata ops | p99 absolute increase | ≤ 50 µs |
| `/metrics` handler | p99 latency | ≤ 50 ms at 10k series |
| `/metrics` payload size | absolute size | < 2 MB at full deployment |

Comparison scenarios:

1. Baseline (current main).
2. Phase 1 enabled (basic request metrics).
3. Phase 1 + 2 enabled (basic + IO).
4. Phase 1 + 2 + 3 enabled (full).
5. Scrape every 1s as stress (must not affect workload p99 by more than 1%).

Failures must be diagnosed and the offending metric reduced or made optional before merge.

## Cross-Layer Correlation

The proposal does **not** introduce distributed tracing. Two lightweight strategies provide enough cross-layer attribution for most diagnosis:

### 1. Aligned histogram buckets

`curvine_fuse_io_duration_us` and `curvine-client`'s `client_read_time_us` / `client_write_time_us` (when promoted from counter to histogram in a separate proposal) use the same bucket set. PromQL can then compute the FUSE↔client latency gap:

```promql
histogram_quantile(0.99, sum by (le) (rate(curvine_fuse_io_duration_us_bucket{io_type="read",path_type="curvine"}[5m])))
-
histogram_quantile(0.99, sum by (le) (rate(client_read_duration_us_bucket[5m])))
```

This gives a population-level estimate of "how much of read p99 is FUSE framework + delivery cost vs client/RPC/backend cost".

### 2. Sampling-based audit log

For per-request attribution, every Nth FUSE request (configurable via `fuse.audit_sample_rate`, default 0 = disabled, recommended 1/1000 in production) writes a structured audit log line at `FuseReqCtx` creation (receiver) and at the **sender finish point** (the same point where `request_duration_us` is observed), sharing the FUSE `unique` ID. The `used_us` therefore matches the metric exactly. The existing `target: "audit"` log channel (already used by `curvine-client::unified_filesystem`) is reused. Output looks like:

```
[ts] INFO audit fuse_unique=12345 phase=receive opcode=Read kind=stream
[ts] INFO audit fuse_unique=12345 phase=finish opcode=Read used_us=1234 status=success
```

Operators correlate this with client-side audit lines (already emitted per metadata operation) using the same `unique` (passed via thread-local or task-local storage). When deeper visibility is needed, the sampling rate can be raised temporarily.

This is intentionally not a span-based tracer; it is a coarse, opt-in correlation aid that adds zero overhead when disabled.

## Implementation Plan

The plan is sequenced so each phase is a small, mergeable PR. Phase boundaries are chosen so that no single PR modifies the existing `dispatch_meta` opcode matrix in bulk.

### Phase 0 — Enabling primitives

Zero behaviour change (registrations and helpers only, no call sites); ships with the criterion bench.

Scope:

- Add `FuseOpCode::as_str(&self) -> &'static str` (`session/fuse_op_code.rs`).
- Add `errno_label(errno: i32) -> &'static str` (`fuse_error.rs` or a new `errno_table.rs`) covering the full closed errno set listed under "Label rules" (including `EBUSY` and `ENAMETOOLONG`), with `OTHER` as the catch-all.
- Add a `FuseNotifyCode::as_str(&self) -> &'static str` for the `code` label on `notify_total`.
- **Monotonic clock.** Ensure a monotonic time source is available for durations. `LocalTime::nanos()` is wall-clock (`SystemTime::now()`) and must not be used for latency. Either use `std::time::Instant` directly in the timer guards, or add a `LocalTime::mono_nanos()` backed by `Instant`/`CLOCK_MONOTONIC`. The `HistogramTimer` and `FuseReqLabels::start` use this source.
- Define `FuseReqLabels` (`#[derive(Clone, Copy)]`: `opcode`, `kind`, `start: Instant`, `request_bytes`) and `FuseReqKind` (`Metadata` | `Stream`). These are the copyable labels carried on replies; the move-only `FuseReqCtx` that owns the in-flight guard is defined where it is used (Phase 1a-1).
- Establish the `FuseMetrics` struct *shape* so that **each phase registers the concrete metric families it first uses** (not all of Phases 1–3 up front). `FuseMetrics` uses `OnceCell`/lazy fields (or a small sub-struct per phase) so adding a later phase's metrics is an **additive** change — a new field/registration, never a refactor of existing ones. Phase 0 registers only what its own helper tests need. (Rationale: registering all of Phases 1–3 in Phase 0 would prematurely lock metric names/label sets that later phases may still adjust once code is touched, and would force reviewers to read many unused definitions. The "later phases stay pure inc/observe" goal is preserved by the additive struct design, not by front-loading every registration.)
- Add an `ActiveGuard` RAII helper (`fuse_metrics.rs`) that increments a gauge on `new()` and decrements on `drop()`. It must be `Send`, movable, and **not `Copy`** (a `Copy` guard would double-decrement). It travels from `FuseReqCtx` onto `FuseTask::RequestReply` so the count is released at sender finish. Separate `StreamIoGuard` / `MetaTaskGuard` (or the same type bound to different gauges) cover the shorter task-local scopes.
- Add a `HistogramTimer` RAII guard (`fuse_metrics.rs`) holding `{ start: Instant, hist: Histogram }`, where `hist` is an already-resolved `with_label_values(...)` child. Its `drop` calls `observe()` directly with **no allocation and no label-map probe**. This is the zero-allocation replacement for `orpc`'s `MetricTimerVec` (which allocates a `Vec<&str>` per drop and must not be used on the hot path). Where the label set is fixed at construction (per-`FuseReader`/`FuseWriter`), the resolved `Histogram` child is stored once on the struct and reused.
- Add a criterion micro-benchmark (`curvine-fuse/benches/`) covering the hot-path helpers: `FuseOpCode::as_str()`, `errno_label()`, one `inc()` on a pre-resolved counter child, and one `HistogramTimer` create+drop. This gives a deterministic, CI-runnable regression guard for the per-call cost, independent of the full fio/bonnie++ cluster benchmarks (which cannot run in CI). A regression in any of these helpers fails the bench before it reaches a cluster run.

No business code changes, and no future-phase metric registrations beyond what the helper tests require. Tests verify that `as_str()` and `errno_label()` return the expected strings for the full enum range (every `FuseOpCode` variant and every errno in the table, plus the `OTHER` fallback for an unmapped value).

### Phase 1 — Framework request metrics (1a-1, 1a-2, 1b)

Phase 1 establishes the **context-propagation spine** and the framework request metrics on top of it. Because the finish point moves to the sender (and that requires an API change to `FuseResponse` and the `FuseTask` enum), it is too large for one reviewable PR once tests are included. It is split into **three** independently mergeable PRs. The split between 1a-1 and 1a-2 is the key risk-reducer: the ownership model and finish state machine are proven (with tests) **before** any real metric depends on them. (Line counts are intentionally omitted — with tests the real diffs vary; do not treat phase size as a budget.)

#### Phase 1a-1 — Structural context spine (no metrics)

The highest-risk refactor, landed as a **behaviour-preserving** change with tests but no real metric emission — so the ownership model is validated in isolation. After 1a-1 the daemon behaves identically; only the internal plumbing has changed.

- **Context types:** `FuseReqLabels` (Copy) + move-only `FuseReqCtx { labels, active: Option<ActiveGuard> }`. Create `FuseReqCtx` in the receiver right after `from_bytes()` succeeds; store it on the `FuseResponse` from `new_reply()` (renamed from `new_replay`, `fuse_receiver.rs:123-125,143,190`).
- **`ActiveGuard` is a no-op/test guard in 1a-1.** The guard *type* and its move-and-drop lifetime are exercised (and asserted by tests), but it is **not yet wired to the real `active_requests` gauge** — that wiring is Phase 1a-2. In 1a-1 the guard either decrements a test-only counter or does nothing on drop; this is what keeps 1a-1 genuinely "no metrics" while still proving single-take / single-drop semantics — prove the structure here, emit the real gauge next.
- **`FuseTask` split:** `RequestReply { data, labels, active: ActiveGuard, status, errno }`, `NotifyReply { data, code }`, and a `Reply(ResponseData)` legacy fast-path variant for disabled mode (see "Disabled-mode task variant"). The `ActiveGuard` is **moved** onto `RequestReply` so the count releases at sender finish.
- **`FuseRespMetrics` slot + finish state machine:** the interior-mutable `Arc<Mutex<FuseRespMetrics>>` on `FuseResponse` (per "FuseResponse API strategy — decided"), the `finished` flag, the single-take guard rule, and the `Clone` shared-slot semantics. Implement `finish_status()`, `finish_no_reply(labels, result)`, the sender finish site, and the parse-after-ctx cleanup **as control flow** — wired to the state machine — but emitting at most test-only/no-op counters.
- **Context reuse on stream enqueue failure:** thread the original `FuseReqLabels` through `send_stream`'s error-path reply (`fuse_receiver.rs:158-160`) so enqueue failures reuse the context instead of creating a zero-latency phantom request.
- **`send_rep_then_inval_*`:** the request reply transfers the guard once (a `RequestReply`); the trailing notification is a `NotifyReply` that does not re-finish the request.
- **Tests (the point of this PR):** the ownership/error-path tests from the Required Tests section — single-drop guard, clone/double-reply caught, no-reply success+error, notify split, stream-enqueue-failure reuse, parse-after-ctx no leak. These prove the state machine before any metric reads it.

Code touch points (1a-1): `fuse_receiver.rs`, `fuse_sender.rs`, `fuse_response.rs`, `session/mod.rs` (the `FuseTask` enum), `fuse_metrics.rs` (struct + guards only).

#### Phase 1a-2 — End-to-end request metrics

Pure metric emission on top of the proven 1a-1 state machine. After 1a-2, both metadata and stream `request_duration_us` are correct (the context rides on the `FuseResponse` that `ReadTask`/`WriteTask` already carry).

- `active_requests{kind}` (the context guard now drives a real gauge).
- `request_duration_us{opcode,kind,status}` — finished in the sender; `requests_total{opcode,kind,reply_type,status}`.
- `response_write_duration_us{opcode,request_status}` + `response_bytes_total{opcode,request_status}`, observed around `FuseSender::splice()`.
- `stage_duration_us{stage="reply_enqueue"|"reply_write"}`.
- `reply_enqueue_errors_total{opcode,reason}` (early-finish path) + `response_write_errors_total{opcode,errno}` (sender write failure, replacing the `warn!`, `fuse_sender.rs:72`).
- `notify_total{code,status}` recorded at its three points (enqueue / write / success — see Notify metrics).
- `decode_errors_total{phase="parse"}` — the parse-after-ctx cleanup proven in 1a-1 now emits this counter. **The `curvine_fuse_decode_errors_total{phase,reason}` metric family is registered here in 1a-2** (when the `phase="parse"` series is first emitted); Phase 1b does not register it — it only adds the `phase="decode"` observation site. (Only the `phase="parse"` series ships here, because that cleanup lives with the 1a state machine; the `phase="decode"` series — `from_bytes` failures with no context — ships in 1b with the receive-loop health work.)
- `errors_total{opcode,kind,errno}` / `interrupted_total{opcode}` / `unsupported_total{opcode,reason}` — once the `unsupported_reason` source-tag handling (see Status Semantics) is in place.

Code touch points (1a-2): `fuse_receiver.rs`, `fuse_sender.rs`, `fuse_response.rs`, `fuse_metrics.rs`.

#### Phase 1b — Framework health + gauge migration

Pure inc/observe additions plus the gauge migration; no API shape changes.

- **Receiver:** observe standalone `receive_loop_wait_duration_us` around the splice; increment `receive_errors_total{errno,action}` on the splice-error `match` (`fuse_receiver.rs:201-207`); increment `decode_errors_total{phase="decode",reason}` at `from_bytes` failures **without altering control flow** (`from_bytes` failure currently terminates the receiver via `?`; see Proposed Metrics caveat). The `decode_errors_total` family was **already registered in 1a-2**; 1b only adds this `phase="decode"` observation site (the `phase="parse"` series shipped in 1a-2 with the parse-after-ctx cleanup, which has context to clean up — `from_bytes` failures do not).
- **In-flight scopes:** `meta_task_inflight` via a guard moved into the spawned metadata future; observe `stage_duration_us{stage="meta_spawn"}`. (`active_requests{kind}` already shipped in 1a as part of the context guard; `stream_io_inflight` is **Phase 2**, since it needs the reader/writer task bodies.)
- **Legacy gauges → event-driven** and remove the `set_metrics()` scrape-time traversal (see Transition cost note). **Restore semantics:** after `NodeState::restore()` bulk-loads, set the three gauges once from the restored counts; on cold start initialize from the root-inode baseline; on partial restore failure, define the gauges as reflecting whatever was actually loaded (see "Restore/startup" below).
- **Scrape hygiene:** `metrics_scrape_duration_us` / `metrics_scrape_bytes` in `metrics_handler`.

Code touch points (1b): `fuse_receiver.rs`, `fuse_metrics.rs`, `fs/state/node_state.rs`, `fs/state/node_map.rs`, `fs/state/file_handle.rs`, `fs/state/dir_handle.rs`, `web_server.rs`.

> **Restore/startup gauge semantics.** Event-driven gauges only stay correct if bulk state loads update them too. `NodeState::restore()` (`node_state.rs:603`) bulk-loads the node map and handle tables without going through the per-insert call sites, so 1b must explicitly set `inode_num` / `file_handle_num` / `dir_handle_num` from the restored counts exactly once after restore completes. On cold start (no restore) they initialize from the baseline (root inode). On a failed/partial restore, the gauges reflect the counts actually loaded, not a reset — documented so operators reading the gauge after a recovery know it matches live state.

Out of scope for Phase 1: metadata `operation_duration` (Phase 2), per-IO metrics and `stream_io`-stage *latency* breakdown (Phase 2), cache metrics (Phase 3).

> **Transition cost — why `set_metrics()` removal is pulled into Phase 1b.** The legacy `inode_num` / `file_handle_num` / `dir_handle_num` gauges are refreshed at scrape time by `NodeState::set_metrics()` (`node_state.rs:546-549`), called from `metrics_handler` (`web_server.rs:30`), which traverses the inode/handle maps under read locks. If the new event-driven gauges are added but `set_metrics()` removal waits until Phase 3, then for several phases **both** mechanisms run: the new atomics *and* the old scrape-time lock traversal — exactly the overhead this proposal exists to eliminate. Therefore Phase 1b makes the three legacy gauges event-driven (inc/dec at the insert/remove sites, plus the restore/cold-start initialization) and deletes the `set_metrics()` call in the same PR. The namespaced `curvine_fuse_*_count` gauges still arrive in Phase 3; only the removal of the scrape-time traversal is brought forward.

> **Kill switch in Phase 1a-1.** A single `metrics_enabled: bool` (read once at startup into `FuseMetrics`) gates all instrumentation added from Phase 1a-1 onward. The full `[fuse]` detail-level config still lands in Phase 4, but the master on/off switch ships with the first structural PR so a production regression is revertible by config flip rather than only by binary rollback.

> **Disabled-mode task variant (`metrics_enabled=false`).** When disabled, the reply path emits the legacy **`FuseTask::Reply(ResponseData)`** variant — *not* a `RequestReply` with empty metrics. This is an explicit choice: keeping a dedicated context-less variant means disabled mode stays byte-for-byte close to current behaviour and constructs **no** `FuseReqLabels`/`FuseReqCtx`/`ActiveGuard`/`Arc<Mutex<FuseRespMetrics>>`/`HistogramTimer`/label-vector, and runs **no** queue-depth updates. The cost is one extra `match` arm in the sender (which already matches on `FuseTask`), in exchange for zero per-request metric machinery when off. `new_reply()` produces a `FuseResponse` whose `metrics` field is `None`, and a `None` metrics field is what selects the `Reply(ResponseData)` variant at send time. The legacy event-driven gauges (`inode_num` etc.) remain updated unconditionally because they are cheap and needed for compatibility.

Expected outcome: operators can view FUSE QPS, error rate, errno distribution, and stage latency. Dashboards for "FUSE Overview" become possible.

### Phase 2 — Operation and IO metrics (2a + 2b)

Split into two independent PRs by concern: metadata-side (operation duration, the reply-channel queue gauge, SETLKW) vs stream-IO-side (reader/writer task instrumentation, including the writer's own queue gauge). They touch disjoint files and have no ordering dependency between them — `reply_queue_depth` lives in the shared reply pipeline (2a), while `stream_write_queue_depth` is the `FuseWriter` internal channel and is therefore in 2b with the rest of the writer-task work.

#### Phase 2a — Metadata operation, reply-queue depth, SETLKW

- Observe `operation_duration_us{opcode,kind="metadata",status}` at a **single measurement point around the whole `dispatch_meta` `match`**, not per arm. `dispatch_meta` lives in `fuse_receiver.rs:252` (it is **not** in `curvine_file_system.rs`). The opcode is available from `req.opcode()` before the `match` (`fuse_receiver.rs:258`), and the metric is labelled by `opcode` anyway, so wrapping the entire `match` expression in one `HistogramTimer` is *semantically identical* to wrapping each of the ~30 arms individually — but it is a single edit instead of ~30, and it does not touch the dispatch matrix arm-by-arm (the explicit anti-goal this proposal repeats throughout). **Status comes from the stashed `FuseReqStatus`** (computed by `finish_status()` and stored in the shared metrics slot), **not** from the `match`'s `IOResult<()>` — that result only reflects whether the reply *enqueued*, and is `Ok(())` even when the FS op failed (see "Status is computed once"). If wiring the stashed status into this timer proves invasive, ship `operation_duration_us` status-less first (per the same section) rather than mislabel everything `success`. (A per-arm `observe_meta(op_name, fut)` wrapper is deliberately *not* used — it would maximize churn in the matrix the design is trying not to disturb.) Stream opcodes are excluded from `operation_duration_us` per Open Question 4.
- Maintain `reply_queue_depth` (gauge, no `_total`) as an event-driven `AtomicI64` per the queue-depth update rules: **inc** in `FuseResponse` after `sender.send(FuseTask::…)` succeeds (`fuse_response.rs:132,141,159,178`); **dec** in `FuseSender` when it `recv()`s the task off the reply channel (`fuse_sender.rs:66`) — *not* in the receiver. (`stream_write_queue_depth` is the writer's internal channel and ships in 2b.)
- Add `setlkw_wait_duration_us` observation in `SetLkW` (`curvine_file_system.rs:1542`) and the `setlkw_inflight` event-driven gauge in `dispatch_meta_interrupt` `pending_requests` insert/remove. (SETLKW is logically independent of the operation timer; it is folded into 2a only because it is small and metadata-side. It can be carved out as its own PR if 2a grows.)

Code touch points (2a): `fuse_receiver.rs` (metadata operation timer only), `fuse_response.rs` (reply-queue inc after enqueue succeeds), `fuse_sender.rs` (reply-queue dec when the sender `recv()`s a task), `curvine_file_system.rs` (`SetLkW` timer only — note `dispatch_meta` is **not** here).

#### Phase 2b — Stream IO metrics

- `FuseReader::new`: capture the `UnifiedReader` variant as `path_type: &'static str` (`curvine` | `ufs` | `fallback` | `local`). Propagate to the read task. `FuseWriter::new`: use existing `is_ufs` plus the variant to set `path_type`.
- Inside `read_future` (`fuse_reader.rs`) and `writer_future` (`fuse_writer.rs`):
  - Observe `io_duration_us{io_type,path_type,status}` around the actual backend call; `stage_duration_us{stage="stream_io"}` around the same call.
  - Increment `io_bytes_total{...}` / `io_requests_total{...}`; observe `io_size_bytes{io_type,path_type}` for read and write.
  - Add `stream_io_inflight{opcode}` via a guard in the reader/writer task body (recv → backend IO → reply produced); see "In-flight guards".
- Maintain `stream_write_queue_depth` (gauge, no `_total`) on the `FuseWriter` internal channel: **inc** after `self.sender.send(WriteTask::…)` succeeds in `FuseWriter::write/flush/complete/resize`, **dec** when `writer_future`'s `req_receiver.recv()` pulls a task (`fuse_writer.rs`). It lives here, not in 2a, because its update points are in `fuse_writer.rs` — moving it keeps 2a free of reader/writer files and the 2a/2b split genuinely disjoint. (A future generalized `stream_task_queue_depth{io_type}` would also belong here.)
- At the stream entry point (`send_stream` in `fuse_receiver.rs`): observe `io_enqueue_duration_us{io_type}` around `fs.read/write/...`.

Code touch points (2b): `fuse_reader.rs`, `fuse_writer.rs` (incl. `stream_write_queue_depth`), `fuse_receiver.rs` (stream enqueue timer), `curvine-client/src/unified/mod.rs` (small accessor for the variant name on `UnifiedReader`).

Note: stream **`request_duration_us`** and `active_requests{kind=stream}` are already correct from Phase 1a-2 (the context and `ActiveGuard` ride on the `FuseResponse` and are finished/dropped in the sender). Phase 2b only adds the *internal* IO breakdown — `io_duration_us`, the `stream_io` stage latency, `stream_io_inflight`, and byte/size counters.

Expected outcome: operators can compare FUSE-layer IO performance against client-layer counters; slow read/write paths are visible without high-cardinality labels; backpressure is observable.

### Phase 3 — Cache, readdir, state, lifecycle (3a + 3b)

Split into two independent PRs: cache/readdir behaviour vs state-recovery/session observability. They touch mostly different files and have no dependency between them.

#### Phase 3a — Cache and readdir metrics

- Add `user_meta_cache_total{cache,status}` (`cache in {status,list,blocks}`, `status in {hit,miss,put}`) at the `MetaCache` sites (`curvine_file_system.rs` `get_cached_status`/`get_cached_list`; `node_state.rs` `get_cached_blocks`).
- Add `node_cache_total{operation="lookup",status}` at `node_map.rs lookup_node` — a **separate** metric from `user_meta_cache_total` because `NodeMap` is a different cache layer (see Cache metrics).
- Add `negative_entry_returned_total` at the negative-dentry return site, counted separately from positive lookups (see Cache metrics rationale).
- Refactor `invalidate_cache(path)` → `invalidate_cache(path, reason: &'static str)`. Update ~10 callers to pass a static reason string; emit `user_meta_cache_invalidations_total{cache,reason}` counting **requested** invalidations at the call site (the cache API cannot report confirmed removals).
- **Eviction counters are deferred, not in Phase 3.** `MetaCache`/`FastSyncCache` expose no eviction hook (see Cache metrics); a reliable `*_evictions_total` requires adding one to `orpc::sync::FastSyncCache`, which is out of scope. (`NodeMap::clean_cache` removals may optionally be counted as an invalidation `reason="ttl"` if that path is a deterministic call site, but not as a cache-internal eviction.)
- Add `readdir_entries` and `readdir_duration_us` observations at the `ReadDir` and `ReadDirPlus` handlers.

Code touch points (3a): `curvine_file_system.rs`, `fs/state/node_state.rs`, `fs/state/node_map.rs`.

#### Phase 3b — State recovery and session lifecycle

- Add the namespaced `curvine_fuse_inode_count` / `curvine_fuse_file_handle_count` / `curvine_fuse_dir_handle_count` gauges, event-driven at the same insert/remove sites in `node_map.rs`, `node_state.rs`, `file_handle.rs`, `dir_handle.rs`. (The scrape-time `NodeState::set_metrics()` traversal and its `metrics_handler` call were already removed in Phase 1b, the legacy gauges were already made event-driven there, and restore/startup gauge initialization was handled there; 3b only adds the namespaced names so dashboards can migrate. See the Phase 1b "Transition cost" and "Restore/startup" notes.) These three gauges are low-risk aliases of the already-event-driven legacy gauges and depend on nothing else in 3b; if dashboard migration is urgent they can be carved out as a tiny `3b-1` ahead of the persist/restore and lifecycle work.
- Wrap `persist`/`restore` in `fuse_session.rs` with stage-level timers and emit them via the **dedicated** `state_persist_stage_duration_us` / `state_restore_stage_duration_us` families (`stage=node_map|file_handles|dir_handles|mount_fds`) — **not** the request-lifecycle `stage_duration_us`. The two are different domains (state recovery vs request handling); a recovery stage must never appear in the request `stage` enum. See the State persist/restore metrics section.
- Add session lifecycle counters at `init`, `unmount`, and in the fd watcher.
  (Scrape-hygiene metrics `metrics_scrape_duration_us` / `metrics_scrape_bytes` already shipped in Phase 1b.)

Code touch points (3b): `fs/state/node_map.rs`, `fs/state/node_state.rs`, `fs/state/file_handle.rs`, `fs/state/dir_handle.rs`, `session/fuse_session.rs`.

Expected outcome: cache effectiveness and readdir cost (3a), plus state-recovery health and session lifecycle (3b), are observable; the namespaced gauges let dashboards migrate off the legacy names.

### Phase 4 — Configuration and cross-layer correlation (optional)

Scope:

- **Extend** the existing `metrics_enabled` switch (shipped in Phase 1a-1) into a full `[fuse]` config block by adding `metrics_detail_level`, `metrics_io_size_histogram`, `metrics_stage_histogram`, `metrics_cache_breakdown`, and `audit_sample_rate`. `metrics_enabled` is **not** introduced here — it already exists; Phase 4 only adds the detail-level knobs around it.
- Implement `FuseMetrics::level()` and gate detailed metrics behind it.
- Implement audit-log sampling at `FuseReqCtx` creation and the sender finish point. Audit logging has different needs from metrics — it wants a **wall-clock** timestamp, the FUSE `unique` ID, a per-request sample decision, and possibly task-local propagation to client audit lines — none of which belong in `FuseReqLabels` (which is deliberately metrics-only: `&'static str` labels + a *monotonic* `start`). Phase 4 therefore introduces a **separate `FuseAuditCtx`** (or audit-only fields carried alongside, not inside, `FuseReqLabels`) for these. **Phase 1 must not overfit `FuseReqLabels` to anticipate audit needs** — keeping them separate prevents Phase 4 from forcing a rewrite of the Phase 1 context shape.
- Document Grafana dashboard JSON in `etc/grafana/`.

Optional / deferred work:

- Promote `client_read_time_us` and `client_write_time_us` to histograms with aligned buckets (separate proposal, owned by client team).
- Per-master FUSE metrics push (explicitly out of scope; see Non-Goals).

### Implementation order (maps onto the 1a-1 / 1a-2 split)

Phase 1a is the riskiest work — the `FuseTask` split, the `FuseRespMetrics` state machine, sender finish, the notification split, no-reply finish, stream-enqueue-failure reuse, clone/double-reply behaviour, and active-guard lifetime. The 1a-1 / 1a-2 PR boundary exists to localize a failure (prove the structure before any counter depends on it). Concretely:

**Phase 1a-1 (structure, prove with tests, no counters):**
1. Phase 0 static label helpers and guard/struct shapes (no call sites).
2. Introduce `RequestReply` / `NotifyReply` / legacy `Reply` as a **behaviour-preserving** refactor (no metrics yet) — the daemon still works identically.
3. Add the `Arc<Mutex<FuseRespMetrics>>` slot and the finish **state-machine tests** (single-take guard, double-reply caught, no-reply, parse-after-ctx no leak) — still no counters.

**Phase 1a-2 (counters on the proven state machine):**
4. Add `active_requests` + `request_duration_us`; verify the gauge returns to baseline.
5. Add `finish_no_reply` (success + error) emission and stream-enqueue-failure label reuse.
6. Add notification success / enqueue-failed / write-failed at their three record points.
7. Add `errors_total` / `unsupported_total` / `interrupted_total` once the `unsupported_reason` source-tag handling is in place; add `decode_errors_total{phase="parse"}` on the cleanup proven in 1a-1.

**Later:** `operation_duration` (Phase 2a) waits until the stashed-`op_status` read path is proven; `decode_errors_total{phase="decode"}` and receive-loop health land in 1b.

The boundary between steps 1–3 (structure + tests) and 4–7 (counters) is the key risk-reducer: the ownership model is validated before any metric depends on it.

## Required Tests

Most metric bugs in this design come from the ownership/error paths, not the happy path. The following tests are required **before** the instrumentation is considered done; they target the `FuseResponse` finish state machine and guard lifetime. The structural/ownership tests (guard single-drop, clone/double-reply, no-reply, notify split, stream-enqueue-failure reuse, parse-after-ctx no-leak) belong with **Phase 1a-1** and validate the state machine before any counter exists; the counter-value assertions belong with **Phase 1a-2**. (`active_requests` correctness is verified by asserting the gauge returns to its baseline after each scenario.)

1. **Normal metadata request** — `active_requests{kind=metadata}` increments at ctx creation and returns to baseline only **after** the sender splice; `requests_total`/`request_duration_us` recorded exactly once.
2. **Normal stream request** — the start instant survives the hop into the `FuseReader`/`FuseWriter` task; `request_duration_us{kind=stream}` is finished in the sender, not at `send_data`; gauge returns to baseline after sender write.
3. **Stream enqueue failure** — `send_stream` error path reuses the original `FuseReqLabels` (not a fresh zero-latency reply); exactly one finish; guard dropped once.
4. **Reply-enqueue failure** — `sender.send()` error path records `reply_enqueue_errors_total` + `request_duration_us{status=error}` once and drops the active guard once (no leak, no double-decrement).
5. **Response-write failure** — `request_duration_us` is still recorded, `response_write_errors_total{opcode,errno}` increments, `request_status` on the duration metric is independent of the write failure.
6. **`Forget`/`BatchForget`, success and error** — `finish_no_reply` records `request_duration_us` and `requests_total{reply_type=no_reply}`, emits **no** `response_*`, drops the guard once. Run it **twice**: once with `fs.forget()` returning `Ok` (→ `status=success`) and once returning `Err` (→ `status=error`, **not** a phantom success — verifies `finish_no_reply` inspects the result rather than discarding it like the current `send_none`).
7. **`send_rep_then_inval_inode` / `send_rep_then_inval_entry`** — the request reply finishes exactly once and drops its guard once; the trailing notification is a `NotifyReply` counted in `notify_total{code}` and attributed to the notify code, **not** the request opcode; no double-finish of the request.
8. **Structural parse failure after ctx creation** — records `decode_errors_total{phase="parse"}`, drops the active guard, emits **no** `requests_total`; `active_requests` returns to baseline (no leak).
9. **`Notimplemented` opcode** — counted under `unsupported_total{reason="unimplemented_opcode"}`, **not** `decode_errors_total`; a real `ENOSYS` reply is sent and finished normally.
10. **Debug-mode double parse** — with `debug=true`, the debug-logging `parse_operator()` call (`fuse_receiver.rs:174`) does **not** produce a second parse-metric observation or a duplicate `decode_errors_total`; only the dispatch-path parse is counted.
11. **`metrics_enabled=false`** — `FuseResponse.metrics` is `None`; no `FuseReqCtx`/`ActiveGuard`/`Mutex`/timer constructed; replies finish with no metric writes; legacy gauges still update.
12. **Notify write failure** — a failed sender splice on a `NotifyReply` records `notify_total{code,status="write_failed"}` (or `notify_write_errors_total{code,errno}`), not a request metric.
13. **`FuseResponse` clone / double-reply** — cloning a `FuseResponse` shares the single `Arc<Mutex<FuseRespMetrics>>` slot; the first reply `take()`s the guard and sets `finished`, a second reply attempt (on the clone or the original) does **not** double-count or double-decrement (`debug_assert!` fires in debug, no-op in release). Verifies the `Clone` semantics decision.
14. **Backend `ENOSYS` is `error`, not `unsupported`** — an FS op returning `FsError::Unsupported` (→ `errno=ENOSYS`) with no `unsupported_reason` is classified `status=error`, never reclassified into `unsupported_total`. Verifies status comes from the source tag, not the errno integer.
15. **Trait-default unsupported** — an op whose `FileSystem`-trait default returns `ENOSYS` (declared, not overridden) is classified `unsupported_total{reason="trait_default"}` because the source path sets `unsupported_reason`. Run alongside test 14 to prove the two `ENOSYS` cases are distinguished by source tag, not errno.
16. **Op fails but reply enqueues** — an FS op returning `Err` whose reply nonetheless enqueues and writes successfully yields `operation_duration_us{status="error"}` **and** `request_duration_us{status="error"}` (both error, consistent). The complementary case — op `Ok` but reply enqueue fails — yields `operation_duration_us{status="success"}` while `request_duration_us{status="error"}` (op and request status diverge). Verifies `op_status`/`request_status` are stored separately and survive the guard being taken.
17. **Queue-depth shutdown** — after enqueue-success, closing/draining the sender resets `reply_queue_depth` (and `stream_write_queue_depth`) to 0 on teardown; the gauge does not leak a stale non-zero value post-exit. (Best-effort during shutdown, per the queue rules.)

## Grafana Dashboard Suggestions

### FUSE Overview

- Request QPS by opcode (stacked).
- Error rate by opcode and errno (top-K).
- Interrupted and unsupported rate.
- p50, p95, p99 request and operation duration by opcode.
- Active requests (E2E in-flight) by kind.
- Reply queue depth.
- Reply-enqueue and reply-write stage latency (receive cost is a separate saturation panel, not a request stage).

### FUSE IO

- Read and write throughput.
- Read and write request size distribution.
- Read and write p95/p99 latency by `path_type`.
- Flush, fsync, release latency.
- Stream write queue depth.
- Curvine vs UFS vs fallback latency comparison.

### Metadata and Cache

- Lookup, getattr, setattr, create, unlink, rename, readdir latency.
- Metadata error rate by opcode; unsupported rate split by `reason` (unknown vs unimplemented).
- Userspace cache hit rate by `cache` (`user_meta_cache_total{status="hit"}` / total).
- Negative-entry-returned rate vs `Lookup` ENOENT rate (negative dentry cache effectiveness).
- Invalidation rate by `reason`.
- Readdir entries-per-request distribution.
- Inode, file handle, directory handle counts.

### Runtime Health

- Process CPU, RSS, threads, FDs (default Prometheus collectors).
- Receiver / sender task counts and exit counter.
- Active requests (E2E in-flight); reply queue depth; metadata spawn backlog (`meta_task_inflight`); stream IO in-flight.
- SETLKW in-flight.
- Splice receive-error rate by errno class; `receive_loop_wait_duration_us` (saturation).
- Reply-enqueue vs response-write error rate (channel vs kernel-fd failures).
- Decode/parse error rate by `phase`,`reason`.
- `/metrics` scrape duration and payload size.
- Session shutdown counter.

## Open Questions

1. Should we accept a small `Cow<&'static str>`-shaped abstraction for unusual labels, or reject any non-`&'static str` label outright? (Current proposal: reject.)
2. Should we expose `errno_label` reversibility (e.g. `EOTHER:42`) so we don't lose detail on rare errnos? (Current proposal: no — `OTHER` only. Operators can grep logs for the rare cases.)
3. Should `interrupted` count toward `errors_total` (aggregating into "non-success" on dashboards) or stay separate? (Current proposal: separate counter; not in `errors_total`.)
4. For Phase 2's `operation_duration` on stream ops, should the metric exist at all (it would only measure the enqueue) or be omitted to avoid confusion? (Current proposal: omit for stream; only metadata gets `operation_duration`. Stream gets `io_enqueue_duration_us` and `io_duration_us`.)
5. How long should the legacy unprefixed gauges (`inode_num` etc.) live before removal? (Current proposal: two minor releases.)
6. Should FUSE metrics ever be reported to Master? (Out of scope here. Decision deferred to a later proposal once the FUSE-only dashboards stabilise.)
7. `meta_task_inflight` (spawned-task scope) and `active_requests{kind=metadata}` (E2E scope) are **not** equivalent — the former excludes reply-channel wait and response write — so both are kept. (Current proposal: keep both; the only open part is whether dashboards need both surfaced by default.)
8. Is the `{phase,reason}` label set on `decode_errors_total` sufficient, or is an `errno`-class also wanted? (Current proposal: `{phase,reason}` only — structural decode/parse failures have no stable errno, and splice errnos are already covered by `receive_errors_total{errno,action}`.)
9. Should decode failures remain fatal to the receiver task (current behaviour: `from_bytes`'s `?` exits `start()`), or become non-fatal once they are counted? (Current proposal: keep current behaviour; the metric is added on the existing path and any change to fatality is a separate behavioural PR — see the Proposed Metrics control-flow caveat.)
10. `request_duration_us` finished in the sender includes reply-channel head-of-line queueing because the sender pool is shared. Is that the right definition, or should queue wait be a separate stage subtracted out? (Current proposal: include it — it is latency the kernel actually observes; `reply_enqueue` and `reply_write` stages already let dashboards see the components separately.)

## Minimal First Pull Request

The metric-by-metric contents of the first PRs are defined in the Implementation Plan and are not duplicated here. This section only marks **where the first shippable milestone ends and why**.

On acceptance, implement the first PRs in order — their full scope, touch points, and rationale are in the Implementation Plan:

1. [Phase 0 — Enabling primitives](#phase-0--enabling-primitives): static label helpers, the monotonic clock, the metric structs/guards, and the criterion bench. Zero behaviour change.
2. [Phase 1a-1 — Structural context spine](#phase-1a-1--structural-context-spine-no-metrics): the `FuseTask` split, `FuseRespMetrics` slot, and finish state machine — a behaviour-preserving refactor with the ownership/error-path tests, **no counters**. This is the highest-risk PR; isolating it is the whole point of the 1a-1 / 1a-2 split.
3. [Phase 1a-2 — End-to-end request metrics](#phase-1a-2--end-to-end-request-metrics): `active_requests`, `request_duration_us`, `requests_total`, the reply/response error counters, `notify_total`, and the error/unsupported/interrupted series — emitted on the proven state machine.
4. [Phase 1b — Framework health + gauge migration](#phase-1b--framework-health--gauge-migration): receive-loop/decode health metrics, `meta_task_inflight`, scrape hygiene, and the legacy-gauge migration.

See also [Implementation order](#implementation-order-maps-onto-the-1a-1--1a-2-split) and the [Required Tests](#required-tests).

**Why stop at Phase 1b.** After 1b the daemon already exposes FUSE QPS, error/errno distribution, end-to-end and stage latency, in-flight saturation, and the framework-health series — enough to build the "FUSE Overview" dashboard. Everything after that (Phase 2 IO breakdown, Phase 3 cache/readdir/state, Phase 4 config + correlation) is an **incremental enhancement** that does not block a usable first release. Phase 0–1b is therefore the minimum that establishes the context-propagation, finish-point, naming, label, overhead-control, and revertibility patterns every later phase reuses; it is the right boundary for the first set of mergeable PRs.
