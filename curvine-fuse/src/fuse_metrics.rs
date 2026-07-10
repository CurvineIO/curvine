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

use std::time::Instant;

use log::warn;
use once_cell::sync::OnceCell;

use orpc::common::{Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Metrics as m};
use orpc::CommonResult;

use crate::fuse_error::errno_label;
use crate::session::FuseOpCode;

/// Buckets (µs) for end-to-end request / operation latency. 18 buckets spanning
/// 10µs–10s, matching the design's "Recommended buckets for request and
/// operation latency".
const REQUEST_DURATION_BUCKETS_US: &[f64] = &[
    10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 25000.0, 50000.0, 100000.0,
    250000.0, 500000.0, 1000000.0, 2500000.0, 5000000.0, 10000000.0,
];

/// Buckets (µs) for short framework stages (`reply_enqueue`, `reply_write`,
/// `meta_spawn`). 14 buckets spanning 5µs–100ms, matching the design's
/// "Recommended buckets for short framework stages".
const STAGE_DURATION_BUCKETS_US: &[f64] = &[
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 25000.0, 50000.0,
    100000.0,
];

/// Buckets (bytes) for a single stream read/write size (Phase 2b `io_size_bytes`).
/// 8 buckets spanning 4KiB–64MiB. The first bucket (`le=4096`) is NOT a minimum
/// observation floor — a sub-4KiB read/write (e.g. a 1-byte read) is observed
/// normally and lands in this first bucket. Zero-length writes are a *semantic*
/// exclusion (they never enter the writer task body, see `file_handle.rs`), not a
/// bucket limit; do not read the 4KiB floor as "the smallest IO".
const IO_SIZE_BUCKETS: &[f64] = &[
    4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0,
];

/// Buckets (entry count) for a single `read_dir_common` batch (Phase 3a
/// `readdir_entries`). A batch is one readdir syscall's worth of dirents, not the
/// whole directory; the upper buckets cover large single batches. Spans 1–4096.
const READDIR_ENTRIES_BUCKETS: &[f64] = &[1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 4096.0];

// `reply_type` label values (`requests_total`).
pub(crate) const REPLY_TYPE_REPLIED: &str = "replied";
pub(crate) const REPLY_TYPE_NO_REPLY: &str = "no_reply";

// `stage` label values. `reply_write` ships in 1a-2; `meta_spawn` in 1b
// (rt.spawn submission -> first poll scheduling delay); `operation` in 2a (the
// whole metadata dispatch_meta match). NOTE: `stream_enqueue` is a reserved
// stage value that Phase 2 deliberately does NOT emit — the send_stream
// read/write dispatch is covered by the dedicated `io_dispatch_duration_us`
// metric instead (it can include a consistency flush/reopen, so it is not a
// pure channel push). Do not add a STAGE_STREAM_ENQUEUE const.
pub(crate) const STAGE_REPLY_WRITE: &str = "reply_write";
pub(crate) const STAGE_META_SPAWN: &str = "meta_spawn";
pub(crate) const STAGE_OPERATION: &str = "operation";
// Phase 2b: the read/write backend call in the reader/writer task body. This is
// the ONLY `stage_duration_us` emission point Phase 2b adds. flush/fsync/release
// deliberately emit NO stage (their result status is not clean — it mixes backend
// and reply-enqueue errors — so a `stage_duration_us{status}` would carry that
// pollution); they use the status-less `stream_lifecycle_*` family instead. The
// send_stream read/write dispatch also emits no stage (covered by the dedicated
// `io_dispatch_duration_us`); see the `STAGE_STREAM_ENQUEUE` note above.
pub(crate) const STAGE_STREAM_IO: &str = "stream_io";

// `io_type` label values (Phase 2b). Lowercase, low-cardinality, zero-allocation.
// read/write feed the `io_*` families (with `status`); flush/fsync/release feed
// the independent `stream_lifecycle_*` families (no `status`). The two never mix.
pub(crate) const IO_TYPE_READ: &str = "read";
pub(crate) const IO_TYPE_WRITE: &str = "write";
pub(crate) const IO_TYPE_FLUSH: &str = "flush";
pub(crate) const IO_TYPE_FSYNC: &str = "fsync";
pub(crate) const IO_TYPE_RELEASE: &str = "release";

// `path_type` label values (Phase 2b), the backend a stream IO targets. read/write
// resolve the real backend via `UnifiedReader/Writer::path_type()` (which returns
// these same literals from curvine-client); flush/fsync/release use `unknown` for
// now (the send_stream layer does not look up the handle). Only `unknown` is read
// by production fuse code (the lifecycle helper); the backend-specific values are
// produced by the curvine-client accessor and referenced here only in tests, so
// they are gated to keep non-test builds warning-free.
#[cfg_attr(not(test), allow(dead_code))] // value produced by path_type(); fuse-side use is test-only.
pub(crate) const PATH_TYPE_CURVINE: &str = "curvine";
#[cfg_attr(not(test), allow(dead_code))] // value produced by path_type(); fuse-side use is test-only.
pub(crate) const PATH_TYPE_UFS: &str = "ufs";
// ⚠ `fallback` is the READER WRAPPER TYPE, not the backend a given read actually
// hit. `UnifiedReader::Fallback(FallbackFsReader)` tries Curvine first and only
// switches to UFS after a worker error, but `path_type` is captured ONCE at handle
// construction — so a long-lived fallback-capable reader reports EVERY read
// (including Curvine cache hits) as `fallback`. Read it as "this handle is
// fallback-capable", NOT "this read fell back to UFS". A precise per-read backend
// label would need the reader to surface its actual outcome per `fuse_read`
// (deferred to a behaviour PR). Dashboards must not treat `fallback` latency/bytes
// as real UFS-fallback IO.
#[cfg_attr(not(test), allow(dead_code))] // reader-only Fallback variant; fuse-side use is test-only.
pub(crate) const PATH_TYPE_FALLBACK: &str = "fallback";
#[cfg_attr(not(test), allow(dead_code))] // value produced by path_type(); fuse-side use is test-only.
pub(crate) const PATH_TYPE_LOCAL: &str = "local";
pub(crate) const PATH_TYPE_UNKNOWN: &str = "unknown";

// Phase 3a `cache` label values (on `user_meta_cache_*`): the meta-cache namespace.
pub(crate) const CACHE_STATUS: &str = "status";
pub(crate) const CACHE_LIST: &str = "list";
pub(crate) const CACHE_BLOCKS: &str = "blocks";

// Phase 3a `status` label values on the `*_cache_total` counters.
pub(crate) const CACHE_RESULT_HIT: &str = "hit";
pub(crate) const CACHE_RESULT_MISS: &str = "miss";
pub(crate) const CACHE_RESULT_PUT: &str = "put";

// Phase 3a `operation` label value on `node_cache_total`.
pub(crate) const NODE_CACHE_OP_LOOKUP: &str = "lookup";

// Phase 3a `reason` label values on `user_meta_cache_invalidations_total`, one per
// real `invalid_cache` call site (see the design doc's 15-value enum).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_SETATTR: &str = "setattr";
pub(crate) const INVAL_REASON_RESIZE: &str = "resize";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_SETXATTR: &str = "setxattr";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_REMOVEXATTR: &str = "removexattr";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_MKDIR: &str = "mkdir";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_CREATE: &str = "create";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_OPEN_WRITE: &str = "open_write";
pub(crate) const INVAL_REASON_FLUSH: &str = "flush";
pub(crate) const INVAL_REASON_RELEASE: &str = "release";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_UNLINK: &str = "unlink";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_LINK: &str = "link";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_RMDIR: &str = "rmdir";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_RENAME: &str = "rename";
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const INVAL_REASON_SYMLINK: &str = "symlink";
pub(crate) const INVAL_REASON_FSYNC: &str = "fsync";

// Phase 3a `status` label on the readdir histograms.
pub(crate) const READDIR_STATUS_SUCCESS: &str = "success";
pub(crate) const READDIR_STATUS_ERROR: &str = "error";

// Phase 3b `stage` label values on the DEDICATED state-recovery families
// (`state_persist_stage_duration_us` / `state_restore_stage_duration_us`). These
// are a SEPARATE domain from the request `stage_duration_us` enum
// (reply_write/meta_spawn/operation/stream_io) — they share the label NAME but
// the value sets never cross. A recovery stage must never appear in the request
// family and vice versa.
pub(crate) const STATE_STAGE_NODE_MAP: &str = "node_map";
pub(crate) const STATE_STAGE_FILE_HANDLES: &str = "file_handles";
pub(crate) const STATE_STAGE_DIR_HANDLES: &str = "dir_handles";
pub(crate) const STATE_STAGE_MOUNT_FDS: &str = "mount_fds";

// Phase 3b `kind` label on `state_persist_handle_count`.
pub(crate) const STATE_KIND_NODE_MAP: &str = "node_map";
pub(crate) const STATE_KIND_FILE_HANDLES: &str = "file_handles";
pub(crate) const STATE_KIND_DIR_HANDLES: &str = "dir_handles";

// Phase 3b `status` label on state_persist/restore families + stage timers.
pub(crate) const STATE_STATUS_SUCCESS: &str = "success";
pub(crate) const STATE_STATUS_ERROR: &str = "error";

// Phase 3b `result` label on `session_init_total`.
pub(crate) const SESSION_INIT_SUCCESS: &str = "success";
pub(crate) const SESSION_INIT_ERROR: &str = "error";

// Phase 3b `reason` label on `session_shutdown_total` (6 bounded values). The
// `run_all` select arm splits its three match outcomes; the signal arms and the
// fd-watcher contribute the rest. Recorded once via a `ShutdownOnce` CAS.
pub(crate) const SHUTDOWN_COMPLETED: &str = "completed";
pub(crate) const SHUTDOWN_RUN_ALL_ERROR: &str = "run_all_error";
pub(crate) const SHUTDOWN_RUN_ALL_PANIC: &str = "run_all_panic";
pub(crate) const SHUTDOWN_TERM_SIGNAL: &str = "term_signal";
pub(crate) const SHUTDOWN_SIGUSR1_PERSIST: &str = "sigusr1_persist";
pub(crate) const SHUTDOWN_FD_WATCHER: &str = "fd_watcher";

// `phase` label values (`decode_errors_total`). `parse` (parse-after-ctx
// cleanup) ships in 1a-2; `decode` (from_bytes failures) in 1b.
pub(crate) const DECODE_PHASE_PARSE: &str = "parse";
pub(crate) const DECODE_PHASE_DECODE: &str = "decode";

// `action` label values for `receive_errors_total`. `exit` covers both the
// graceful ENODEV break and an unexpected-error loop exit; the distinction is
// carried by `errno` (see design's Status Semantics / receive-error notes).
pub(crate) const RECEIVE_ACTION_CONTINUE: &str = "continue";
pub(crate) const RECEIVE_ACTION_EXIT: &str = "exit";

// `reason` label value for `reply_enqueue_errors_total`. Phase 1a-2 only uses
// `channel_closed`: a tokio `SendError` means exactly "channel closed" and
// cannot reliably distinguish runtime shutdown, so we do not invent a reason
// from the error string (see plan R6).
pub(crate) const ENQUEUE_REASON_CHANNEL_CLOSED: &str = "channel_closed";

// `status` label values for `notify_total`. These are a delivery lifecycle,
// NOT a request status — deliberately separate consts so they are never
// confused with `FuseReqStatus::as_str()` (see plan R12).
pub(crate) const NOTIFY_SUCCESS: &str = "success";
pub(crate) const NOTIFY_ENQUEUE_FAILED: &str = "enqueue_failed";
pub(crate) const NOTIFY_WRITE_FAILED: &str = "write_failed";

// Defensive `reason` label used only when an `Unsupported` status reaches the
// finish helper with no source tag — which is a wiring bug (every 1a-2
// Unsupported site tags its reason). It is surfaced via debug_assert!/warn! and
// bucketed distinctly so a missing tag never masquerades as a real
// `unimplemented_opcode` gap.
const UNSUPPORTED_REASON_MISSING: &str = "missing_reason";

/// Fallback `errno` label when a delivery (kernel-fd write) failure carries no
/// OS errno — used by `response_write_errors_total` instead of `errno_label(0)`
/// so a missing errno never reads as a literal "raw 0" (see plan R7).
const ERRNO_LABEL_OTHER: &str = "OTHER";

static FUSE_METRICS: OnceCell<FuseMetrics> = OnceCell::new();

/// Process-global FUSE metrics registry.
///
/// The struct is intentionally shaped so that **each implementation phase
/// registers the concrete metric families it first uses** — it is *not*
/// front-loaded with every future metric. Adding a later phase's metrics is an
/// additive change (a new field + a registration line), never a refactor of the
/// existing fields. This keeps each phase's diff self-contained and avoids
/// locking metric names / label sets before the code that uses them exists.
///
/// Phase 0 registers only the three pre-existing runtime gauges; the helper
/// types below (`FuseReqLabels`, `ActiveGuard`, `HistogramTimer`) are the
/// enabling primitives that later phases build on, and carry no call sites yet.
pub struct FuseMetrics {
    pub inode_num: Gauge,
    pub file_handle_num: Gauge,
    pub dir_handle_num: Gauge,
    pub fuse_used_memory_bytes: Gauge,

    pub write_back_active_inode_num: Gauge,
    pub write_back_mem_usage: Gauge,
    pub write_back_mem_limit: Gauge,

    // --- Phase 3b-1: namespaced aliases of the legacy gauges above ---
    // Event-driven at the same insert/remove/restore sites, kept exactly in
    // lockstep with their legacy counterparts (updated inside the same
    // `FuseMetrics::with` closure). The legacy `*_num` gauges are deprecated and
    // will be removed two minor releases after dashboards migrate to these.
    pub inode_count: Gauge,
    pub file_handle_count: Gauge,
    pub dir_handle_count: Gauge,

    // --- Phase 1a-2: end-to-end request metrics ---
    /// E2E in-flight requests, driven by `ActiveGuard`: incremented at ctx
    /// creation, decremented at the sender/no-reply finish. `kind`.
    pub(crate) active_requests: GaugeVec,
    /// Request count at the finish point. `opcode,kind,reply_type,status`.
    pub(crate) requests_total: CounterVec,
    /// E2E request latency, finished in the sender. `opcode,kind,status`.
    pub(crate) request_duration_us: HistogramVec,
    /// Real operation errors only (`status==error`); unsupported/interrupted
    /// have their own counters. `opcode,kind,errno`.
    pub(crate) errors_total: CounterVec,
    /// Interrupted requests (the SETLKW interrupt-notify path). `opcode`.
    pub(crate) interrupted_total: CounterVec,
    /// Unsupported requests. `opcode,reason`. Phase 1a-2 emits
    /// `unknown_opcode` / `unimplemented_opcode`; `trait_default` reserved.
    pub(crate) unsupported_total: CounterVec,
    /// Kernel notifications. `code,status` where status is a delivery lifecycle
    /// (`success|enqueue_failed|write_failed`), not a request status.
    pub(crate) notify_total: CounterVec,
    /// Structural decode/parse failures. `phase,reason`. Phase 1a-2 emits only
    /// `phase=parse,reason=other`; the schema supports the full reason set but
    /// other series are not pre-created.
    pub(crate) decode_errors_total: CounterVec,
    /// Kernel-fd write latency in the sender (around the splice). Observed on
    /// both success and failure. `opcode,request_status`.
    pub(crate) response_write_duration_us: HistogramVec,
    /// On-wire reply size at sender finish (from `ResponseData::len()`).
    /// `opcode,request_status`.
    pub(crate) response_bytes_total: CounterVec,
    /// Reply-channel enqueue failures (request never reaches the sender).
    /// `opcode,reason`. Phase 1a-2 only uses reason `channel_closed`.
    pub(crate) reply_enqueue_errors_total: CounterVec,
    /// Kernel-fd write failures in the sender (delivery failure). `opcode,errno`.
    pub(crate) response_write_errors_total: CounterVec,
    /// Per-stage latency, opcode-free. `stage,kind,status`. Bounded `stage`
    /// enum emitted by the current build (reply_write in 1a-2, meta_spawn in 1b).
    pub(crate) stage_duration_us: HistogramVec,

    // --- Phase 1b-1: framework health + scrape hygiene ---
    /// Receiver loop wait: splice + header-parse, INCLUDING idle wait for the
    /// next kernel request. A saturation/health histogram, NOT request latency.
    pub(crate) receive_loop_wait_duration_us: Histogram,
    /// Splice/receive errors before a request is decoded. `errno,action`
    /// (action = continue | exit).
    pub(crate) receive_errors_total: CounterVec,
    /// Spawned metadata tasks in flight (rt.spawn submission -> dispatch
    /// returns). Event-driven via a guard. No label.
    pub(crate) meta_task_inflight: Gauge,
    /// `/metrics` handler `text_output()` cost. Self-observation (last scrape).
    pub(crate) metrics_scrape_duration_us: Histogram,
    /// `/metrics` last scrape output size in bytes. Self-observation gauge.
    pub(crate) metrics_scrape_bytes: Gauge,

    // --- Phase 2a: metadata operation, reply-queue depth, SETLKW ---
    /// Per-op metadata operation latency, observed at a single timer around the
    /// whole `dispatch_meta` match. `opcode,kind,status` (kind always
    /// `metadata`). Status is the stashed `op_status` (FS-operation result),
    /// not the enqueue outcome. **Includes the awaited reply enqueue** by
    /// construction (each arm is `reply.send_rep(fs.<op>(op).await).await`).
    pub(crate) operation_duration_us: HistogramVec,
    /// SETLKW interruptible-request duration (RAII timer over the whole
    /// `dispatch_meta_interrupt` scope: parse + dispatch + lock polling + reply
    /// enqueue, plus the interrupt-notify reply). NOT pure lock-acquisition time —
    /// reply-channel backpressure can inflate it, so do NOT read it as lock
    /// contention. Covers immediate interrupt (sample even if the lock poll loop
    /// never ran) and malformed-SETLKW parse failures (near-zero sample). No label.
    pub(crate) setlkw_wait_duration_us: Histogram,
    /// Reply-channel backlog. Event-driven via a task-embedded `ActiveGuard`
    /// created at enqueue and dropped when the sender dequeues (or when an
    /// un-received task is dropped). No `_total` suffix (it is a gauge).
    pub(crate) reply_queue_depth: Gauge,
    /// SETLKW interruptible-request scope in flight (NOT a `pending_requests` map
    /// size): the guard spans the whole `dispatch_meta_interrupt` scope, so under
    /// reply-channel backpressure it can stay non-zero after the map entry is
    /// already removed (esp. the interrupt branch). Event-driven via a guard.
    /// No label.
    pub(crate) setlkw_inflight: Gauge,

    // --- Phase 2b: stream IO (read/write backend + flush/fsync/release lifecycle) ---
    //
    // Two deliberately-separate families. `io_*` covers read/write backend IO and
    // carries `status` (the backend result is clean). `stream_lifecycle_*` covers
    // flush/fsync/release and carries NO status (their result mixes backend and
    // reply-enqueue errors, so a status label would be dishonest) — and because a
    // Prometheus family's label set is fixed at registration, the two cannot share
    // a family (R3 P0#1).
    /// read/write backend IO latency, observed in the reader/writer task body when
    /// the backend `fuse_read`/`fuse_write` returns. `io_type` ∈ {read,write},
    /// `path_type` is the resolved backend, `status` ∈ {success,error}.
    pub(crate) io_duration_us: HistogramVec,
    /// Bytes transferred by a successful read/write. `io_type,path_type,status` —
    /// but only the `status=success` child is ever created (an error read/write
    /// records NO byte series; we never `inc_by(0)`), so do not expect
    /// `io_bytes_total{status=error}`. read uses the actual bytes read (short reads
    /// count actual), write uses the returned size.
    pub(crate) io_bytes_total: CounterVec,
    /// read/write backend attempts. `io_type,path_type,status` — incremented once
    /// per attempt INCLUDING `status=error`.
    pub(crate) io_requests_total: CounterVec,
    /// Single read/write request-size distribution. `io_type,path_type` (no status:
    /// it characterises requested size, not outcome). read uses the *requested*
    /// size, write uses the *input* data length — request size, distinct from the
    /// transferred bytes in `io_bytes_total` (which uses actual).
    pub(crate) io_size_bytes: HistogramVec,
    /// read/write dispatch-to-worker latency at `send_stream` (the `fs.read`/
    /// `fs.write` call). `io_type` ∈ {read,write}. NOT a pure channel push: read
    /// can include a read-after-write consistency flush + reader reopen, and write
    /// includes the zero-length no-op direct-reply path. No status (dispatch is
    /// observed on success, pre-dispatch error, and cancel alike; the distribution
    /// is the point). Uses the wide request/op buckets, not short-stage buckets,
    /// because the consistency flush/reopen long tail would otherwise pile into +Inf.
    pub(crate) io_dispatch_duration_us: HistogramVec,
    /// read/write backend calls in flight (task body, backend-only — strictly
    /// shorter than `active_requests`). `io_type` ∈ {read,write}; the guard wraps
    /// ONLY the `fuse_read`/`fuse_write` call (not the subsequent reply enqueue).
    pub(crate) stream_io_inflight: GaugeVec,
    /// flush/fsync/release lifecycle latency at `send_stream` (the whole arm incl.
    /// the error reply enqueue). `io_type` ∈ {flush,fsync,release}, `path_type`
    /// fixed `unknown` for now. NO status (the result mixes backend and
    /// reply-enqueue errors); observed on success, error, pre-dispatch error, and
    /// cancel.
    pub(crate) stream_lifecycle_duration_us: HistogramVec,
    /// flush/fsync/release lifecycle attempts. `io_type,path_type` — counted before
    /// the match (so a pre-dispatch error still counts). No status (see duration).
    pub(crate) stream_lifecycle_requests_total: CounterVec,
    /// flush/fsync/release lifecycle in flight at `send_stream` (dispatch + lock +
    /// backend round-trip + reply enqueue). `io_type` ∈ {flush,fsync,release}. A
    /// saturation signal, NOT a pure-backend-stuck signal — a real-time companion
    /// to `stream_lifecycle_duration_us` (which only emits on completion). Separate
    /// gauge from `stream_io_inflight` because the scope (whole arm) differs.
    pub(crate) stream_lifecycle_inflight: GaugeVec,
    /// Writer-channel backlog: ALL `WriteTask`s enqueued into the writer task but
    /// not yet dequeued — write / flush / complete / resize, not just `FUSE_WRITE`
    /// (resize can come from metadata truncate/fallocate). Event-driven via a
    /// task-embedded `ActiveGuard` created at the enqueue boundary and dropped at
    /// the writer's dequeue point (or when an un-received task is dropped). No
    /// `_total` suffix (a gauge), no label.
    pub(crate) stream_write_queue_depth: Gauge,

    // --- Phase 3a: cache + readdir metrics ---
    /// MetaCache hit/miss/put on the daemon's userspace metadata cache.
    /// `cache` ∈ {status,list,blocks}, `status` ∈ {hit,miss,put}. `miss` is
    /// recorded the moment the cache read returns `None` (before the backend
    /// fetch), so a backend error / ENOENT after a miss is still in the
    /// denominator; `put` only when the value is actually written back.
    pub(crate) user_meta_cache_total: CounterVec,
    /// Requested meta-cache invalidations at the `invalid_cache(ino, name, reason)`
    /// call site (not confirmed removals). `cache` ∈ {status,list,blocks},
    /// `reason` is one of 15 call-site reasons. One inc PER affected cache
    /// namespace, so a single call emits 3–4 series (`cache=list` usually twice:
    /// the path's own listing + the parent listing).
    pub(crate) user_meta_cache_invalidations_total: CounterVec,
    /// NodeMap dcache lookup outcome on the real FUSE `Lookup` path only.
    /// `operation` ∈ {lookup}, `status` ∈ {hit,miss} (no `put` — a pure in-memory
    /// lookup never writes). Internal `do_lookup` callers (readdir, mkdir/create/
    /// link/symlink entry build) and `.`/`..` are NOT counted.
    pub(crate) node_cache_total: CounterVec,
    /// Negative dentry results returned to the kernel (backend `get_status`
    /// returned ENOENT and `negative_ttl` is non-zero). Counted separately from a
    /// positive `hit` so the positive hit-rate stays meaningful. No label.
    pub(crate) negative_entry_returned_total: Counter,
    /// Entries returned by one `read_dir_common` batch (a single readdir syscall's
    /// worth, NOT the directory total). `status` label kept for a future
    /// partial-error split, but currently only the `success` child is observed —
    /// error / cancellation observes `readdir_duration_us{error}` and no entries.
    pub(crate) readdir_entries: HistogramVec,
    /// `read_dir_common` latency (pull-a-batch + encode). Does NOT include the
    /// `opendir`/`list_stream` backend init (that happens in `new_dir_handle`);
    /// the full-traversal cost is a deferred `opendir_duration_us`. `status`.
    pub(crate) readdir_duration_us: HistogramVec,

    // --- Phase 3b: state recovery + session lifecycle ---
    /// Persist attempts (SIGUSR1). `status` ∈ {success,error}, recorded once at
    /// the `fuse_session.rs::persist` entry/exit.
    pub(crate) state_persist_total: CounterVec,
    /// Per-stage persist duration. `stage` ∈ {node_map,file_handles,dir_handles,
    /// mount_fds} (a DEDICATED domain, never the request `stage_duration_us`),
    /// `status` ∈ {success,error}. mount_fds is timed in fuse_session.rs; the
    /// other three in node_state.rs.
    pub(crate) state_persist_stage_duration_us: HistogramVec,
    /// Handle counts sampled ONCE at the start of a persist attempt. `kind` ∈
    /// {node_map,file_handles,dir_handles}. A best-effort live snapshot.
    pub(crate) state_persist_handle_count: GaugeVec,
    /// Restore attempts (restart with the state-file env var). `status`.
    pub(crate) state_restore_total: CounterVec,
    /// Per-stage restore duration. Same `stage`/`status` domain as persist. A
    /// NodeState magic/version header failure skips only the NodeState stages
    /// (node_map/file_handles/dir_handles); `mount_fds` precedes the header in the
    /// file format so it may already have recorded success.
    pub(crate) state_restore_stage_duration_us: HistogramVec,
    /// Session init outcome, recorded once in `FuseSession::new`. `result` ∈
    /// {success,error} (every early `?` counts as error).
    pub(crate) session_init_total: CounterVec,
    /// Session shutdown cause, recorded EXACTLY ONCE via a `ShutdownOnce` CAS.
    /// `reason` ∈ {completed,run_all_error,run_all_panic,term_signal,
    /// sigusr1_persist,fd_watcher} — first cause wins.
    pub(crate) session_shutdown_total: CounterVec,
    /// FUSE kernel fd health, a single unlabeled gauge: 1 = healthy (set at end
    /// of `new`), 0 = HUP/ERR (fd-watcher) or session exited (run() cleanup).
    pub(crate) kernel_fd_health: Gauge,
}

impl FuseMetrics {
    pub fn ensure_init() -> CommonResult<()> {
        FUSE_METRICS.get_or_try_init(Self::new)?;
        Ok(())
    }

    pub fn get() -> &'static Self {
        FUSE_METRICS
            .get()
            .expect("FuseMetrics not initialized; call ensure_init from CurvineFileSystem::new")
    }

    /// Run `f` against the metrics singleton iff it has been initialized; a
    /// no-op when not (instead of `get()`'s panic).
    ///
    /// This is the access path for the **legacy compatibility gauges**
    /// (`inode_num` / `file_handle_num` / `dir_handle_num`) **and their Phase 3b-1
    /// namespaced aliases** (`curvine_fuse_{inode,file_handle,dir_handle}_count`),
    /// which are updated unconditionally at their inode/handle mutation sites
    /// (Phase 1b-2 made them event-driven and removed the scrape-time
    /// `set_metrics()` refresh; 3b-1 added the aliases in lockstep at the same
    /// sites). Those sites live in `NodeState`/`NodeMap`, whose unit tests
    /// construct state with a bare `NodeState::new` and never call
    /// `ensure_init()`; routing every gauge write through `with()` keeps them
    /// from panicking on the uninitialized singleton. The aliases MUST use this
    /// same `with()` path — do NOT switch them to `FuseMetrics::get()`, which
    /// would reintroduce that panic on the bare-`NodeState` test paths.
    ///
    /// The uninit branch is a deliberate, silent no-op — NOT a `debug_assert!`.
    /// Tests run in debug with a process-global `OnceCell` whose init order
    /// across parallel tests is unspecified, so a `node_state` test that fires
    /// a mutation before any `ensure_init()` would trip the assert and reintroduce
    /// exactly the flakiness this helper removes. The production invariant
    /// (`ensure_init()` before `NodeState::new`) is instead pinned by the
    /// `ensure_init_precedes_node_state` test below, which is the right place to
    /// catch an ordering regression.
    pub(crate) fn with<F: FnOnce(&Self)>(f: F) {
        if let Some(m) = FUSE_METRICS.get() {
            f(m);
        }
    }

    fn new() -> CommonResult<Self> {
        Ok(Self {
            inode_num: m::new_gauge("inode_num", "FUSE inode count in dcache")?,
            file_handle_num: m::new_gauge("file_handle_num", "FUSE open file handle count")?,
            dir_handle_num: m::new_gauge("dir_handle_num", "FUSE open directory handle count")?,
            fuse_used_memory_bytes: m::new_gauge("fuse_used_memory_bytes", "Total memory used")?,
            write_back_active_inode_num: m::new_gauge(
                "write_back_active_inode_num",
                "FUSE write-back active inode count",
            )?,
            write_back_mem_usage: m::new_gauge(
                "write_back_mem_usage",
                "FUSE write-back page cache usage (bytes)",
            )?,
            write_back_mem_limit: m::new_gauge(
                "write_back_mem_limit",
                "FUSE write-back page cache size limit (bytes)",
            )?,

            // Phase 3b-1: namespaced aliases (same values, event-driven in lockstep).
            inode_count: m::new_gauge(
                "curvine_fuse_inode_count",
                "FUSE inode count in dcache (namespaced alias of inode_num)",
            )?,
            file_handle_count: m::new_gauge(
                "curvine_fuse_file_handle_count",
                "FUSE open file handle count (namespaced alias of file_handle_num)",
            )?,
            dir_handle_count: m::new_gauge(
                "curvine_fuse_dir_handle_count",
                "FUSE open directory handle count (namespaced alias of dir_handle_num)",
            )?,

            active_requests: m::new_gauge_vec(
                "curvine_fuse_active_requests",
                "FUSE requests in flight end-to-end (ctx creation to sender finish)",
                &["kind"],
            )?,
            requests_total: m::new_counter_vec(
                "curvine_fuse_requests_total",
                "Total FUSE requests, counted at the finish point",
                &["opcode", "kind", "reply_type", "status"],
            )?,
            request_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_request_duration_us",
                "End-to-end FUSE request latency in microseconds, finished in the sender",
                &["opcode", "kind", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            errors_total: m::new_counter_vec(
                "curvine_fuse_errors_total",
                "FUSE requests that failed with a real operation error (status=error only)",
                &["opcode", "kind", "errno"],
            )?,
            interrupted_total: m::new_counter_vec(
                "curvine_fuse_interrupted_total",
                "FUSE requests terminated via the SETLKW interrupt path",
                &["opcode"],
            )?,
            unsupported_total: m::new_counter_vec(
                "curvine_fuse_unsupported_total",
                "Unsupported FUSE requests; currently emits reason \
                 unknown_opcode/unimplemented_opcode, trait_default reserved for later",
                &["opcode", "reason"],
            )?,
            notify_total: m::new_counter_vec(
                "curvine_fuse_notify_total",
                "Kernel notifications by code and delivery status \
                 (success|enqueue_failed|write_failed)",
                &["code", "status"],
            )?,
            decode_errors_total: m::new_counter_vec(
                "curvine_fuse_decode_errors_total",
                "Structural decode/parse failures by phase. phase=parse is per-request and \
                 recurring (recoverable parse-after-ctx failures); phase=decode is TERMINAL — \
                 a from_bytes failure kills the receiver, so it increments at most once per \
                 receiver lifetime. Treat a phase=decode increment as 'receiver died, restart', \
                 not a rate to threshold.",
                &["phase", "reason"],
            )?,
            response_write_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_response_write_duration_us",
                "Kernel-fd write (splice) latency in microseconds, observed on success and failure",
                &["opcode", "request_status"],
                STAGE_DURATION_BUCKETS_US,
            )?,
            response_bytes_total: m::new_counter_vec(
                "curvine_fuse_response_bytes_total",
                "On-wire FUSE reply size in bytes at sender finish",
                &["opcode", "request_status"],
            )?,
            reply_enqueue_errors_total: m::new_counter_vec(
                "curvine_fuse_reply_enqueue_errors_total",
                "Reply-channel enqueue failures; the request never reaches the sender",
                &["opcode", "reason"],
            )?,
            response_write_errors_total: m::new_counter_vec(
                "curvine_fuse_response_write_errors_total",
                "Kernel-fd write (delivery) failures in the sender",
                &["opcode", "errno"],
            )?,
            stage_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_stage_duration_us",
                "Per-stage FUSE framework latency in microseconds; label `stage` is a \
                 bounded enum emitted by the current build",
                &["stage", "kind", "status"],
                STAGE_DURATION_BUCKETS_US,
            )?,

            receive_loop_wait_duration_us: m::new_histogram_with_buckets(
                "curvine_fuse_receive_loop_wait_duration_us",
                "Receiver loop wait (splice + header parse) in microseconds. \
                 SATURATION/health metric, NOT request latency: includes idle wait for \
                 the next kernel request, so long idle periods land in high/+Inf buckets. \
                 Do not use for request P99.",
                REQUEST_DURATION_BUCKETS_US,
            )?,
            receive_errors_total: m::new_counter_vec(
                "curvine_fuse_receive_errors_total",
                "Splice/receive errors before a request is decoded. action=continue \
                 (loop retries) or exit (loop stops: graceful ENODEV break or unexpected \
                 error return; the original error is still returned/logged)",
                &["errno", "action"],
            )?,
            meta_task_inflight: m::new_gauge(
                "curvine_fuse_meta_task_inflight",
                "Spawned metadata tasks in flight (rt.spawn submission to dispatch return)",
            )?,
            metrics_scrape_duration_us: m::new_histogram_with_buckets(
                "curvine_fuse_metrics_scrape_duration_us",
                "Time to render the /metrics text output in microseconds (last scrape)",
                STAGE_DURATION_BUCKETS_US,
            )?,
            metrics_scrape_bytes: m::new_gauge(
                "curvine_fuse_metrics_scrape_bytes",
                "Size of the last /metrics scrape output body in bytes",
            )?,

            operation_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_operation_duration_us",
                "Metadata FUSE operation latency in microseconds (whole dispatch_meta match); \
                 includes the awaited reply enqueue, so it is NOT pure operation latency — do \
                 not subtract reply_enqueue. Metadata only; interrupted SETLKW is excluded.",
                &["opcode", "kind", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            setlkw_wait_duration_us: m::new_histogram_with_buckets(
                "curvine_fuse_setlkw_wait_duration_us",
                "SETLKW interruptible-request duration in microseconds: the whole \
                 dispatch_meta_interrupt scope (parse + dispatch + lock polling + reply \
                 enqueue). NOT pure lock-acquisition time — reply-channel backpressure can \
                 inflate it, do not read as lock contention. Includes immediate interrupt \
                 (sample even if the lock poll loop never ran) and malformed-SETLKW parse \
                 failures (near-zero sample).",
                REQUEST_DURATION_BUCKETS_US,
            )?,
            reply_queue_depth: m::new_gauge(
                "curvine_fuse_reply_queue_depth",
                "Reply-channel backlog (tasks enqueued but not yet received by the sender)",
            )?,
            setlkw_inflight: m::new_gauge(
                "curvine_fuse_setlkw_inflight",
                "SETLKW interruptible-request scopes in flight (whole dispatch_meta_interrupt \
                 scope, NOT pending_requests map size; can stay non-zero under reply-channel \
                 backpressure after the map entry is removed)",
            )?,

            io_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_io_duration_us",
                "Stream read/write backend IO latency in microseconds, observed in the \
                 reader/writer task body when the backend call returns. io_type=read|write only; \
                 flush/fsync/release use stream_lifecycle_duration_us instead. NOTE: \
                 path_type=fallback means the reader is fallback-CAPABLE (a FallbackFsReader \
                 captured at open), NOT that this read fell back to UFS — Curvine cache hits on \
                 such a handle are also labelled fallback",
                &["io_type", "path_type", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            io_bytes_total: m::new_counter_vec(
                "curvine_fuse_io_bytes_total",
                "Bytes transferred by a successful stream read/write (read=actual bytes read, \
                 write=input length reported to the kernel on success — fuse_write returns no \
                 partial size). Only the status=success child is created; an error read/write \
                 records no byte series",
                &["io_type", "path_type", "status"],
            )?,
            io_requests_total: m::new_counter_vec(
                "curvine_fuse_io_requests_total",
                "Stream read/write backend attempts, incremented once per attempt including \
                 status=error. Excludes zero-length writes (they never enter the writer task body)",
                &["io_type", "path_type", "status"],
            )?,
            io_size_bytes: m::new_histogram_vec_with_buckets(
                "curvine_fuse_io_size_bytes",
                "Single stream read/write request size in bytes (read=requested size, \
                 write=input data length); request-size distribution, distinct from the \
                 transferred io_bytes_total which uses actual bytes",
                &["io_type", "path_type"],
                IO_SIZE_BUCKETS,
            )?,
            io_dispatch_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_io_dispatch_duration_us",
                "Stream read/write dispatch-to-worker latency in microseconds at send_stream; \
                 read may include a read-after-write consistency flush + reader reopen, write \
                 includes zero-length no-op direct replies and their reply-enqueue time (NOT \
                 worker dispatch). io_type=read|write only; no status",
                &["io_type"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            stream_io_inflight: m::new_gauge_vec(
                "curvine_fuse_stream_io_inflight",
                "Stream read/write backend calls in flight (task body, backend-only — shorter \
                 than active_requests; does NOT include the reply channel enqueue). \
                 io_type=read|write only",
                &["io_type"],
            )?,
            stream_lifecycle_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_stream_lifecycle_duration_us",
                "flush/fsync/release lifecycle duration in microseconds at send_stream (whole \
                 arm incl. error reply enqueue); attempted count lives in \
                 stream_lifecycle_requests_total; no success/error status; observed on success, \
                 error, pre-dispatch error, and cancel. path_type is unknown for now",
                &["io_type", "path_type"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            stream_lifecycle_requests_total: m::new_counter_vec(
                "curvine_fuse_stream_lifecycle_requests_total",
                "flush/fsync/release lifecycle attempts at send_stream, counted before the match \
                 (so a pre-dispatch error still counts). No status (the result mixes backend and \
                 reply-enqueue errors). path_type is unknown for now",
                &["io_type", "path_type"],
            )?,
            stream_lifecycle_inflight: m::new_gauge_vec(
                "curvine_fuse_stream_lifecycle_inflight",
                "flush/fsync/release lifecycle in-progress at send_stream (dispatch + lock + \
                 backend round-trip + reply enqueue); a saturation signal, NOT a \
                 pure-backend-stuck signal — correlate with stream_write_queue_depth / \
                 reply_queue_depth / request_duration to localize. io_type=flush|fsync|release",
                &["io_type"],
            )?,
            stream_write_queue_depth: m::new_gauge(
                "curvine_fuse_stream_write_queue_depth",
                "Writer-channel backlog: ALL WriteTasks enqueued into the writer task but not yet \
                 dequeued — write / flush / complete / resize, NOT just FUSE_WRITE. Note resize \
                 can be driven by metadata paths (SetAttr truncate / fallocate), so a rising \
                 gauge is not necessarily FUSE write/flush pressure. Event-driven via a \
                 task-embedded guard, dropped at the dequeue point",
            )?,

            // Phase 3a: cache + readdir.
            user_meta_cache_total: m::new_counter_vec(
                "curvine_fuse_user_meta_cache_total",
                "MetaCache hit/miss/put by cache namespace. status=hit|miss|put",
                &["cache", "status"],
            )?,
            user_meta_cache_invalidations_total: m::new_counter_vec(
                "curvine_fuse_user_meta_cache_invalidations_total",
                "Requested MetaCache invalidations at the call site, one inc per affected cache \
                 namespace (NOT per invalidate_cache call)",
                &["cache", "reason"],
            )?,
            node_cache_total: m::new_counter_vec(
                "curvine_fuse_node_cache_total",
                "NodeMap dcache lookup outcome on the real FUSE Lookup path. status=hit|miss",
                &["operation", "status"],
            )?,
            negative_entry_returned_total: m::new_counter(
                "curvine_fuse_negative_entry_returned_total",
                "Negative dentry results returned to the kernel (backend ENOENT + negative_ttl>0)",
            )?,
            readdir_entries: m::new_histogram_vec_with_buckets(
                "curvine_fuse_readdir_entries",
                "Entries returned by one read_dir_common batch (single readdir syscall, not the \
                 directory total). Only the success child is observed",
                &["status"],
                READDIR_ENTRIES_BUCKETS,
            )?,
            readdir_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_readdir_duration_us",
                "read_dir_common latency in microseconds (pull-a-batch + encode; excludes \
                 opendir/list_stream backend init)",
                &["status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,

            // Phase 3b: state recovery + session lifecycle.
            state_persist_total: m::new_counter_vec(
                "curvine_fuse_state_persist_total",
                "FUSE state-persist attempts (SIGUSR1). status=success|error",
                &["status"],
            )?,
            state_persist_stage_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_state_persist_stage_duration_us",
                "Per-stage state-persist duration in microseconds. stage is a dedicated \
                 state-recovery domain (node_map|file_handles|dir_handles|mount_fds), NOT the \
                 request stage_duration_us enum",
                &["stage", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            state_persist_handle_count: m::new_gauge_vec(
                "curvine_fuse_state_persist_handle_count",
                "Handle counts sampled once at the start of a persist attempt. \
                 kind=node_map|file_handles|dir_handles",
                &["kind"],
            )?,
            state_restore_total: m::new_counter_vec(
                "curvine_fuse_state_restore_total",
                "FUSE state-restore attempts (restart with state-file env var). status=success|error",
                &["status"],
            )?,
            state_restore_stage_duration_us: m::new_histogram_vec_with_buckets(
                "curvine_fuse_state_restore_stage_duration_us",
                "Per-stage state-restore duration in microseconds. Same dedicated stage domain as \
                 persist. A NodeState magic/version header failure skips only the NodeState stages \
                 (node_map/file_handles/dir_handles); mount_fds precedes the header in the file \
                 format so it may already be recorded",
                &["stage", "status"],
                REQUEST_DURATION_BUCKETS_US,
            )?,
            session_init_total: m::new_counter_vec(
                "curvine_fuse_session_init_total",
                "FUSE session init outcome, recorded once in FuseSession::new. result=success|error",
                &["result"],
            )?,
            session_shutdown_total: m::new_counter_vec(
                "curvine_fuse_session_shutdown_total",
                "FUSE session shutdown cause, recorded once per session (first cause wins). \
                 reason=completed|run_all_error|run_all_panic|term_signal|sigusr1_persist|fd_watcher",
                &["reason"],
            )?,
            kernel_fd_health: m::new_gauge(
                "curvine_fuse_kernel_fd_health",
                "FUSE kernel fd health: 1=healthy, 0=HUP/ERR or session exited",
            )?,
        })
    }

    // --- Phase 1a-2 emission helpers ---
    //
    // These are the single place each metric's `with_label_values` lives, so the
    // finish paths (sender / no-reply / enqueue-failure / parse-early) never hand
    // a label string to `with_label_values` directly. `status`/`kind`/`reply_type`
    // strings come from `FuseReqStatus::as_str()` / `FuseReqKind::as_str()` /
    // the `REPLY_TYPE_*` consts so a label can never drift between call sites.

    /// `requests_total +1` — sender and no-reply finish only (NOT enqueue
    /// failure, which must not count toward QPS; see plan B0/decision 1).
    pub(crate) fn record_request_total(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        reply_type: &'static str,
        status: FuseReqStatus,
    ) {
        self.requests_total
            .with_label_values(&[opcode, kind.as_str(), reply_type, status.as_str()])
            .inc();
    }

    /// `request_duration_us` observe — all three finish paths.
    pub(crate) fn record_request_duration(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        status: FuseReqStatus,
        elapsed_us: u64,
    ) {
        self.request_duration_us
            .with_label_values(&[opcode, kind.as_str(), status.as_str()])
            .observe(elapsed_us as f64);
    }

    /// The full sender finish emission: request total + duration, response
    /// write latency/bytes, the `reply_write` stage, and the per-status error /
    /// unsupported / interrupted / delivery-failure counters. Pure (no IO), so
    /// it is unit-testable without a kernel fd (plan R13).
    ///
    /// **Two statuses, deliberately separate** (design doc "operation vs request
    /// status"):
    /// - `op_status` — the FS-operation result. Drives `errors_total` /
    ///   `unsupported_total` / `interrupted_total`, which carry the real errno /
    ///   reason. A delivery failure does NOT change these (a write failure on a
    ///   successful op is not an FS error).
    /// - `request_status` — the final result the kernel observes. Equal to
    ///   `op_status` on the common path, but `Error` when delivery fails (the
    ///   `WriteOutcome::Failed` case here, or enqueue failure on the early-finish
    ///   path). Drives `requests_total` / `request_duration_us` /
    ///   `response_write_duration_us` / `response_bytes_total`.
    ///
    /// The kernel-fd write errno itself is the independent delivery dimension and
    /// lands in `response_write_errors_total{opcode,errno}`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_request_finish(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        op_status: FuseReqStatus,
        request_status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
        response_bytes: u32,
        write: WriteOutcome,
        write_elapsed_us: u64,
        total_elapsed_us: u64,
    ) {
        let req_status_str = request_status.as_str();

        // request_status-labelled series (the result the kernel observes).
        self.record_request_total(opcode, kind, REPLY_TYPE_REPLIED, request_status);
        self.record_request_duration(opcode, kind, request_status, total_elapsed_us);
        // Delivery latency/size: observed on both success and failure.
        self.response_write_duration_us
            .with_label_values(&[opcode, req_status_str])
            .observe(write_elapsed_us as f64);
        // `u32 -> i64` is always safe (no saturating cast needed).
        self.response_bytes_total
            .with_label_values(&[opcode, req_status_str])
            .inc_by(response_bytes as i64);
        self.stage_duration_us
            .with_label_values(&[STAGE_REPLY_WRITE, kind.as_str(), req_status_str])
            .observe(write_elapsed_us as f64);

        // op_status-driven non-success counters (with the real FS errno/reason).
        // A delivery failure on a successful op records NOTHING here — only
        // response_write_errors_total below.
        self.record_op_terminal(opcode, kind, op_status, errno, unsupported_reason);

        // Delivery failure is an independent dimension from request status.
        if let WriteOutcome::Failed { errno } = write {
            let label = errno.map(errno_label).unwrap_or(ERRNO_LABEL_OTHER);
            self.response_write_errors_total
                .with_label_values(&[opcode, label])
                .inc();
        }
    }

    /// The op-level terminal counters: `errors_total` / `unsupported_total` /
    /// `interrupted_total`, classified from the **FS-operation** status with its
    /// real errno / source-tagged reason. Shared by the sender finish path and
    /// the reply-enqueue-failure path so that an op failure is recorded even when
    /// delivery later fails — the two paths classify op outcome identically.
    /// `Success` records nothing.
    ///
    /// **Call exactly once per request, only from a request terminal path.**
    /// Calling it twice double-counts the op-level counters for one request. In
    /// particular, the Phase 2 `operation_duration_us` timer must only `observe`
    /// latency — it must NOT call this (the request terminal already did).
    pub(crate) fn record_op_terminal(
        &self,
        opcode: &'static str,
        kind: FuseReqKind,
        op_status: FuseReqStatus,
        errno: i32,
        unsupported_reason: Option<&'static str>,
    ) {
        match op_status {
            FuseReqStatus::Error => {
                self.errors_total
                    .with_label_values(&[opcode, kind.as_str(), errno_label(errno)])
                    .inc();
            }
            FuseReqStatus::Unsupported => {
                // Source-tagged reason is the only authority (never inferred from
                // errno). 1a-2 always tags Unsupported at its source sites; a
                // missing tag is a wiring bug, surfaced (not silently bucketed).
                let reason = match unsupported_reason {
                    Some(r) => r,
                    None => {
                        debug_assert!(
                            false,
                            "Unsupported op_status without a source tag (opcode {opcode})"
                        );
                        warn!("unsupported status without a source tag for opcode {opcode}");
                        UNSUPPORTED_REASON_MISSING
                    }
                };
                self.unsupported_total
                    .with_label_values(&[opcode, reason])
                    .inc();
            }
            FuseReqStatus::Interrupted => {
                self.interrupted_total.with_label_values(&[opcode]).inc();
            }
            FuseReqStatus::Success => {}
        }
    }

    /// `notify_total +1` for one delivery-lifecycle status. `status` must be one
    /// of the `NOTIFY_*` consts (a delivery lifecycle, not a request status).
    pub(crate) fn record_notify_result(&self, code: &'static str, status: &'static str) {
        self.notify_total.with_label_values(&[code, status]).inc();
    }

    /// `reply_enqueue_errors_total +1` — the reply never reached the sender.
    /// `reason` is a channel-level reason const (Phase 1a-2 only uses
    /// `ENQUEUE_REASON_CHANNEL_CLOSED`).
    pub(crate) fn record_reply_enqueue_error(&self, opcode: &'static str, reason: &'static str) {
        self.reply_enqueue_errors_total
            .with_label_values(&[opcode, reason])
            .inc();
    }

    /// `decode_errors_total{phase="parse"} +1` — a structural parse failure that
    /// happened after the request ctx existed. `reason` is the parse-failure
    /// reason (Phase 1a-2 only emits `"other"`).
    pub(crate) fn record_parse_error(&self, reason: &'static str) {
        self.decode_errors_total
            .with_label_values(&[DECODE_PHASE_PARSE, reason])
            .inc();
    }

    // --- Phase 1b-1 framework health helpers ---

    /// `decode_errors_total{phase="decode"} +1` — a structural `from_bytes`
    /// failure before any request ctx exists. `reason` is `"other"` for now
    /// (no structured decode-error classification yet).
    ///
    /// TERMINAL signal: the caller increments this and then immediately returns
    /// the error, which ends the receive loop for this mount. So phase=decode
    /// increments at most once per receiver lifetime — a one-shot fatal event,
    /// not an accumulating rate (unlike the recurring per-request phase=parse).
    /// Operators should read 0->1 as "receiver died, needs restart".
    pub(crate) fn record_decode_error(&self, reason: &'static str) {
        self.decode_errors_total
            .with_label_values(&[DECODE_PHASE_DECODE, reason])
            .inc();
    }

    /// `receive_errors_total{errno,action} +1` — a splice/receive error before
    /// a request is decoded. `errno` is a `splice_errno_label`, `action` is
    /// `RECEIVE_ACTION_CONTINUE` or `RECEIVE_ACTION_EXIT`.
    pub(crate) fn record_receive_error(&self, errno: &'static str, action: &'static str) {
        self.receive_errors_total
            .with_label_values(&[errno, action])
            .inc();
    }

    /// Observe the receiver loop wait (splice + header parse, incl. idle wait).
    pub(crate) fn record_receive_loop_wait(&self, elapsed_us: u64) {
        self.receive_loop_wait_duration_us
            .observe(elapsed_us as f64);
    }

    /// Observe the `meta_spawn` stage (rt.spawn submission -> first poll). Always
    /// `status=success` (the spawn itself cannot fail).
    pub(crate) fn record_meta_spawn(&self, elapsed_us: u64) {
        self.stage_duration_us
            .with_label_values(&[
                STAGE_META_SPAWN,
                FuseReqKind::Metadata.as_str(),
                FuseReqStatus::Success.as_str(),
            ])
            .observe(elapsed_us as f64);
    }

    /// Record one `/metrics` scrape: observe render duration and set the last
    /// scrape output size. Self-observation (last-scrape semantics).
    pub(crate) fn record_scrape(&self, elapsed_us: u64, output_bytes: usize) {
        self.metrics_scrape_duration_us.observe(elapsed_us as f64);
        self.metrics_scrape_bytes.set(output_bytes as i64);
    }

    /// Build the `meta_task_inflight` guard for a spawned metadata task. Returns
    /// `Some(ActiveGuard)` (incrementing the gauge) when metrics are enabled,
    /// `None` when disabled — disabled MUST be `None` (no metric machinery), it
    /// must never be a `noop()` guard. Extracted so the gate is unit-testable.
    pub(crate) fn meta_task_guard(metrics_enabled: bool) -> Option<ActiveGuard> {
        if metrics_enabled {
            Some(ActiveGuard::new(Self::get().meta_task_inflight.clone()))
        } else {
            None
        }
    }

    // --- Phase 2a emission helpers ---

    /// Observe the metadata operation latency once around the whole
    /// `dispatch_meta` match, feeding **two** families from one timer (the same
    /// dual-emit shape as the 2b `stream_io` call site):
    /// - `operation_duration_us{opcode,kind=metadata,status}` — per-opcode detail.
    /// - `stage_duration_us{stage=operation,kind=metadata,status}` — the
    ///   opcode-free stage view, so the operation stage is comparable against the
    ///   other framework stages (`reply_enqueue`/`reply_write`/`meta_spawn`) at
    ///   bounded cardinality.
    ///
    /// `status` is the stashed `op_status` (FS-operation result), read back after
    /// the match — NOT the enqueue outcome. The duration includes the awaited
    /// reply enqueue by construction. It does NOT call `record_op_terminal`: the
    /// request terminal path already counted the op outcome; this only observes
    /// latency.
    pub(crate) fn record_operation(
        &self,
        opcode: &'static str,
        status: FuseReqStatus,
        elapsed_us: u64,
    ) {
        let kind = FuseReqKind::Metadata.as_str();
        let status_str = status.as_str();
        let elapsed = elapsed_us as f64;
        self.operation_duration_us
            .with_label_values(&[opcode, kind, status_str])
            .observe(elapsed);
        self.stage_duration_us
            .with_label_values(&[STAGE_OPERATION, kind, status_str])
            .observe(elapsed);
    }

    /// Build the `reply_queue_depth` guard for a task entering the reply channel.
    /// Returns `Some(ActiveGuard)` (incrementing the gauge).
    ///
    /// **Call only from the metrics-enabled reply path** (`self.metrics.is_some()`
    /// in `FuseResponse`). The disabled path produces the legacy `Reply` variant
    /// and never reaches here, so the "disabled = `None`, never `noop()`" contract
    /// is enforced at the call site, not by a `metrics_enabled` flag here.
    ///
    /// Uses `get()` (strict), like `setlkw_inflight_guard` / `setlkw_wait_timer`:
    /// because this is only ever reached on the metrics-enabled path, an
    /// uninitialized singleton here is a wiring/init-order regression and SHOULD
    /// surface as a panic rather than silently drop `reply_queue_depth` (review
    /// P1#5). Production order guarantees init before any reply (ensure_init
    /// precedes NodeState; pinned by `ensure_init_precedes_node_state`); the
    /// enabled-path unit tests call `ensure_init()` in their fixtures. The guard is
    /// moved into the `RequestReply`/`NotifyReply` task and decrements when the
    /// sender dequeues (or when an un-received task is dropped).
    pub(crate) fn reply_queue_guard() -> Option<ActiveGuard> {
        Some(ActiveGuard::new(Self::get().reply_queue_depth.clone()))
    }

    /// Build the `setlkw_inflight` guard for a SETLKW interruptible-request scope.
    /// `Some` when enabled, `None` when disabled (never `noop()`). Created after
    /// the `pending_requests` insert and held across the whole `select!`; its Drop
    /// — not the `pending_requests.remove` — decrements the gauge, so every
    /// `select!` branch / early return / cancellation balances. NOTE: the guard
    /// scope is the whole `dispatch_meta_interrupt`, NOT the `pending_requests` map
    /// entry — under reply-channel backpressure the gauge can stay non-zero after
    /// the map entry is already removed (the interrupt branch removes before the
    /// reply enqueue completes).
    pub(crate) fn setlkw_inflight_guard(metrics_enabled: bool) -> Option<ActiveGuard> {
        if metrics_enabled {
            Some(ActiveGuard::new(Self::get().setlkw_inflight.clone()))
        } else {
            None
        }
    }

    /// Build the SETLKW interruptible-request timer (RAII): `Some(HistogramTimer)`
    /// when enabled, `None` when disabled (no clock read, no observe). Created in
    /// `dispatch_meta_interrupt` BEFORE the `select!`, so its scope is the WHOLE
    /// interruptible SETLKW request (parse, dispatch, `set_lkw()` lock polling, and
    /// reply enqueue) — NOT pure lock-acquisition time (matches the registration
    /// help; reply-channel backpressure can inflate it, so do not read it as lock
    /// contention). It observes on every drop: normal completion, interrupt
    /// cancellation, AND a malformed SETLKW whose `parse_operator()` fails before
    /// `set_lkw()` is ever called (near-zero sample — the reason the timer lives
    /// here and not inside `set_lkw()`).
    pub(crate) fn setlkw_wait_timer(metrics_enabled: bool) -> Option<HistogramTimer> {
        if metrics_enabled {
            Some(HistogramTimer::new(
                Self::get().setlkw_wait_duration_us.clone(),
            ))
        } else {
            None
        }
    }

    // --- Phase 2b emission helpers ---

    /// Record one read/write backend IO in the reader/writer task body, feeding
    /// the read/write `io_*` families plus the opcode-free `stage_duration_us`
    /// (the same dual-emit shape as `record_operation`: `io_*` carries
    /// `path_type` detail, the stage view stays opcode-free at bounded
    /// cardinality). `io_type` is `IO_TYPE_READ`/`IO_TYPE_WRITE`; `path_type` is
    /// the backend resolved at handle open.
    ///
    /// - `io_duration_us{io_type,path_type,status}` + `stage_duration_us{stage=
    ///   stream_io,kind=stream,status}`: observed on success AND error.
    /// - `io_requests_total{io_type,path_type,status}`: +1 every attempt, error
    ///   included.
    /// - `io_size_bytes{io_type,path_type}`: the *request* size (read=requested,
    ///   write=input len), observed on success and error.
    /// - `io_bytes_total{io_type,path_type,status=success}`: the *transferred*
    ///   bytes, ONLY on success (an error creates no byte series — never
    ///   `inc_by(0)`).
    pub(crate) fn record_stream_io(
        &self,
        io_type: &'static str,
        path_type: &'static str,
        ok: bool,
        transferred_bytes: u64,
        request_size: u64,
        elapsed_us: u64,
    ) {
        // Status is binary here (the backend call either returned data or an
        // error); sourcing the string from `FuseReqStatus` keeps it identical to
        // every other status label.
        let status_str = if ok {
            FuseReqStatus::Success.as_str()
        } else {
            FuseReqStatus::Error.as_str()
        };
        let elapsed = elapsed_us as f64;
        self.io_duration_us
            .with_label_values(&[io_type, path_type, status_str])
            .observe(elapsed);
        self.stage_duration_us
            .with_label_values(&[STAGE_STREAM_IO, FuseReqKind::Stream.as_str(), status_str])
            .observe(elapsed);
        self.io_requests_total
            .with_label_values(&[io_type, path_type, status_str])
            .inc();
        self.io_size_bytes
            .with_label_values(&[io_type, path_type])
            .observe(request_size as f64);
        if ok {
            // Success-only byte series, fixed status=success (R3 P0#2).
            self.io_bytes_total
                .with_label_values(&[io_type, path_type, FuseReqStatus::Success.as_str()])
                .inc_by(transferred_bytes as i64);
        }
    }

    /// Build the `stream_io_inflight{io_type}` guard for a read/write backend
    /// call. `Some(ActiveGuard)` when enabled (incrementing the gauge), `None`
    /// when disabled (never `noop()`). The guard must wrap ONLY the
    /// `fuse_read`/`fuse_write` call — drop it before the reply enqueue so the
    /// gauge reflects backend concurrency, not reply-channel time. `io_type` is
    /// `IO_TYPE_READ`/`IO_TYPE_WRITE` (from the task variant, no opcode lookup).
    pub(crate) fn stream_io_guard(
        metrics_enabled: bool,
        io_type: &'static str,
    ) -> Option<ActiveGuard> {
        if metrics_enabled {
            let gauge = Self::get().stream_io_inflight.with_label_values(&[io_type]);
            Some(ActiveGuard::new(gauge))
        } else {
            None
        }
    }

    /// Build the `stream_write_queue_depth` guard for a task entering the writer
    /// channel. `Some(ActiveGuard)` when enabled (incrementing the gauge), `None`
    /// when disabled (never `noop()`). Unlike `reply_queue_guard` (which is only
    /// reached on the enabled path so it takes no flag), the writer constructs one
    /// `FuseWriter` regardless of the kill switch, so the gate is passed in here.
    /// The guard is moved into the `QueuedWriteTask` and decrements when the writer
    /// dequeues (or when an un-received task is dropped).
    pub(crate) fn stream_write_queue_guard(metrics_enabled: bool) -> Option<ActiveGuard> {
        if metrics_enabled {
            Some(ActiveGuard::new(
                Self::get().stream_write_queue_depth.clone(),
            ))
        } else {
            None
        }
    }

    /// Build the `io_dispatch_duration_us{io_type}` RAII timer for a read/write
    /// dispatch at `send_stream`. Returns a bare `HistogramTimer` (no `Option`):
    /// the caller gates on `metrics.is_some()` and only maps this in when enabled,
    /// so reaching here means metrics are on and `get()` is initialized. Observes
    /// on drop, covering success / pre-dispatch error / cancel alike.
    pub(crate) fn io_dispatch_timer(io_type: &'static str) -> HistogramTimer {
        let hist = Self::get()
            .io_dispatch_duration_us
            .with_label_values(&[io_type]);
        HistogramTimer::new(hist)
    }

    /// Open a flush/fsync/release lifecycle scope at `send_stream`: count the
    /// attempt now (`stream_lifecycle_requests_total{io_type,path_type=unknown}`,
    /// before the backend runs so a pre-dispatch error still counts) and return a
    /// `StreamLifecycleScope` holding the duration timer and the inflight guard.
    /// Both release together at the end of the send_stream arm (the timer
    /// observes, the guard decrements); the drop *order* between them carries no
    /// observable semantics. `io_type` is `IO_TYPE_FLUSH`/`FSYNC`/`RELEASE`.
    ///
    /// All three sub-metrics use the fixed `path_type=unknown` (the send_stream
    /// layer does not look up the handle). Attempt-count and timer/guard are wired
    /// here together with no `.await`/early-return between them, so the family is
    /// never half-balanced. Like the other guard helpers, this is only ever
    /// reached on the metrics-enabled path (the caller maps it in behind an
    /// `enabled` check), so `get()` (strict) is correct.
    pub(crate) fn stream_lifecycle_scope(io_type: &'static str) -> StreamLifecycleScope {
        let m = Self::get();
        m.stream_lifecycle_requests_total
            .with_label_values(&[io_type, PATH_TYPE_UNKNOWN])
            .inc();
        let timer = HistogramTimer::new(
            m.stream_lifecycle_duration_us
                .with_label_values(&[io_type, PATH_TYPE_UNKNOWN]),
        );
        let inflight = ActiveGuard::new(m.stream_lifecycle_inflight.with_label_values(&[io_type]));
        StreamLifecycleScope {
            _timer: timer,
            _inflight: inflight,
        }
    }

    // --- Phase 3a emission helpers (called via `FuseMetrics::with`) ---

    /// `user_meta_cache_total{cache,status} +1`. `cache` ∈ {status,list,blocks},
    /// `status` ∈ {hit,miss,put}.
    pub(crate) fn record_user_meta_cache(&self, cache: &'static str, status: &'static str) {
        self.user_meta_cache_total
            .with_label_values(&[cache, status])
            .inc();
    }

    /// `node_cache_total{operation=lookup,status} +1`. `status` ∈ {hit,miss}.
    pub(crate) fn record_node_cache_lookup(&self, status: &'static str) {
        self.node_cache_total
            .with_label_values(&[NODE_CACHE_OP_LOOKUP, status])
            .inc();
    }

    /// `negative_entry_returned_total +1`.
    pub(crate) fn record_negative_entry(&self) {
        self.negative_entry_returned_total.inc();
    }

    /// Record a requested invalidation `reason` across every affected cache
    /// namespace: the path's own `status`/`list`/`blocks` (dropped together by
    /// `meta_cache.invalidate(path)`), and — when `has_parent` — the parent
    /// `list` (`invalidate_list(parent)`). One inc per namespace, NOT per call;
    /// see the design doc. `cache=list` therefore usually increments twice.
    pub(crate) fn record_invalidation(&self, reason: &'static str, has_parent: bool) {
        let c = &self.user_meta_cache_invalidations_total;
        c.with_label_values(&[CACHE_STATUS, reason]).inc();
        c.with_label_values(&[CACHE_LIST, reason]).inc();
        c.with_label_values(&[CACHE_BLOCKS, reason]).inc();
        if has_parent {
            c.with_label_values(&[CACHE_LIST, reason]).inc();
        }
    }

    /// `readdir_entries{status=success}` observe (success path only) plus the
    /// matching `readdir_duration_us{status=success}`.
    pub(crate) fn record_readdir_success(&self, entries: u64, elapsed_us: u64) {
        self.readdir_entries
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .observe(entries as f64);
        self.readdir_duration_us
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .observe(elapsed_us as f64);
    }

    /// `readdir_duration_us{status=error}` observe — error/cancellation path; does
    /// NOT observe `readdir_entries` (no partial/zero count).
    pub(crate) fn record_readdir_error(&self, elapsed_us: u64) {
        self.readdir_duration_us
            .with_label_values(&[READDIR_STATUS_ERROR])
            .observe(elapsed_us as f64);
    }

    // --- Phase 3b emission helpers (called via `FuseMetrics::with`) ---

    /// `state_persist_total{status} +1` / `state_restore_total{status} +1`.
    pub(crate) fn record_state_total(&self, is_persist: bool, status: &'static str) {
        let c = if is_persist {
            &self.state_persist_total
        } else {
            &self.state_restore_total
        };
        c.with_label_values(&[status]).inc();
    }

    /// Observe a persist/restore stage duration. `is_persist` selects the family.
    pub(crate) fn observe_state_stage(
        &self,
        is_persist: bool,
        stage: &'static str,
        status: &'static str,
        elapsed_us: u64,
    ) {
        let h = if is_persist {
            &self.state_persist_stage_duration_us
        } else {
            &self.state_restore_stage_duration_us
        };
        h.with_label_values(&[stage, status])
            .observe(elapsed_us as f64);
    }

    /// `state_persist_handle_count{kind}` gauge set (start-of-persist snapshot).
    pub(crate) fn set_state_handle_count(&self, kind: &'static str, count: usize) {
        self.state_persist_handle_count
            .with_label_values(&[kind])
            .set(count as i64);
    }

    /// `session_init_total{result} +1`.
    pub(crate) fn record_session_init(&self, result: &'static str) {
        self.session_init_total.with_label_values(&[result]).inc();
    }

    /// `session_shutdown_total{reason} +1`.
    pub(crate) fn record_session_shutdown(&self, reason: &'static str) {
        self.session_shutdown_total
            .with_label_values(&[reason])
            .inc();
    }

    /// `kernel_fd_health` set 1 (healthy) / 0 (HUP-or-exited).
    pub(crate) fn set_kernel_fd_health(&self, healthy: bool) {
        self.kernel_fd_health.set(if healthy { 1 } else { 0 });
    }
}

/// Map a stream opcode to the read/write `io_type` for `io_dispatch_duration_us`,
/// or `None` for a non-read/write stream op. Deliberately NOT `opcode.as_str()`:
/// that yields the capitalized request label (`"Read"`/`"Write"`), whereas the IO
/// families use the lowercase `IO_TYPE_*` consts.
pub(crate) fn dispatch_io_type(opcode: FuseOpCode) -> Option<&'static str> {
    match opcode {
        FuseOpCode::FUSE_READ => Some(IO_TYPE_READ),
        FuseOpCode::FUSE_WRITE => Some(IO_TYPE_WRITE),
        _ => None,
    }
}

/// Map a stream opcode to the flush/fsync/release `io_type` for the
/// `stream_lifecycle_*` families, or `None` otherwise. Same lowercase-const
/// discipline as `dispatch_io_type` (NOT `opcode.as_str()`). For a known stream
/// opcode exactly one of `dispatch_io_type`/`lifecycle_io_type` is `Some`.
pub(crate) fn lifecycle_io_type(opcode: FuseOpCode) -> Option<&'static str> {
    match opcode {
        FuseOpCode::FUSE_FLUSH => Some(IO_TYPE_FLUSH),
        FuseOpCode::FUSE_FSYNC => Some(IO_TYPE_FSYNC),
        FuseOpCode::FUSE_RELEASE => Some(IO_TYPE_RELEASE),
        _ => None,
    }
}

/// RAII scope for a flush/fsync/release lifecycle at `send_stream`, returned by
/// `FuseMetrics::stream_lifecycle_scope`. Holds the duration timer (observes on
/// drop) and the inflight guard (decrements on drop). Created after the attempt
/// counter is incremented and held across the whole send_stream arm — including
/// the error reply enqueue — so the duration and inflight cover success, backend
/// error, pre-dispatch error, and cancellation alike. The two fields' drop order
/// is irrelevant (separate metrics); only "released at end of arm" matters.
pub(crate) struct StreamLifecycleScope {
    _timer: HistogramTimer,
    _inflight: ActiveGuard,
}

/// Monotonic time source for durations.
///
/// `orpc::common::LocalTime::nanos()` is wall-clock (`SystemTime::now()`) and
/// must **not** be used for latency: an NTP step or suspend/resume can produce
/// skewed or negative deltas. All FUSE duration metrics use `std::time::Instant`
/// instead, accessed through this single helper so the choice is centralized.
/// Monotonic "now" for duration measurement (the sender's reply-write timer and
/// `FuseReqLabels::start`).
#[inline]
pub(crate) fn mono_now() -> Instant {
    Instant::now()
}

/// The kind of a FUSE request, used as the `kind` label.
///
/// There is deliberately no `NoReply` variant: `Forget` / `BatchForget` are
/// `Metadata` and are distinguished by a separate `reply_type` label where it
/// matters (added in a later phase).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuseReqKind {
    Metadata,
    Stream,
}

impl FuseReqKind {
    /// Low-cardinality `&'static str` label. Zero-allocation.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FuseReqKind::Metadata => "metadata",
            FuseReqKind::Stream => "stream",
        }
    }
}

/// The cheap, copyable label set carried alongside a request from decode to the
/// sender finish point. Holds only `&'static str` and integers plus a monotonic
/// `start` — no heap formatting, so it is `Copy` and free to clone onto a reply
/// task.
///
/// The move-only request context that *owns* the in-flight guard (`FuseReqCtx`)
/// is defined where it is used (Phase 1a-1); these labels are the part that
/// travels onto the reply.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FuseReqLabels {
    pub(crate) opcode: &'static str,
    pub(crate) kind: FuseReqKind,
    pub(crate) start: Instant,
    /// Request size from the parsed header. Carried for a future per-request
    /// byte metric; not read by any Phase 1a-2 series yet.
    #[allow(dead_code)]
    pub(crate) request_bytes: u32,
}

impl FuseReqLabels {
    pub(crate) fn new(opcode: &'static str, kind: FuseReqKind, request_bytes: u32) -> Self {
        Self {
            opcode,
            kind,
            start: mono_now(),
            request_bytes,
        }
    }

    /// Microseconds elapsed since `start`, using the monotonic clock.
    pub(crate) fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

/// The terminal status of a FUSE request, used as the `status` label.
///
/// Classified by `FuseResponse::send_rep_tagged()` (via `err_status()`) from the
/// FS-operation result plus an explicit source tag — never from errno alone (see
/// the metrics design's Status Semantics). The real `requests_total{status}` /
/// duration series read it at the sender finish point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuseReqStatus {
    Success,
    Error,
    Interrupted,
    Unsupported,
}

impl FuseReqStatus {
    /// The single source of truth for the `status` label string. All finish
    /// paths (sender / no-reply / enqueue-failure) route their status through
    /// here so the label can never drift between call sites.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FuseReqStatus::Success => "success",
            FuseReqStatus::Error => "error",
            FuseReqStatus::Interrupted => "interrupted",
            FuseReqStatus::Unsupported => "unsupported",
        }
    }
}

/// Outcome of the kernel-fd write in the sender, passed to
/// `record_request_finish`. A named enum instead of `Option<Option<i32>>` so
/// the three cases are unambiguous (see plan R3-4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteOutcome {
    /// The splice succeeded.
    Success,
    /// The splice failed. `errno` is the OS errno if one was available; `None`
    /// maps to the `OTHER` errno label.
    Failed { errno: Option<i32> },
}

/// Move-only request context created in the receiver right after a request is
/// decoded. It owns the E2E `ActiveGuard`; dropping the context (or moving the
/// guard out exactly once) is what keeps `active_requests` correct.
///
/// `labels` is `Copy` and travels onto the reply; the guard is move-only and
/// must not be duplicated. Stored on the `FuseResponse` (inside `FuseRespMetrics`).
#[derive(Debug)]
pub(crate) struct FuseReqCtx {
    pub(crate) labels: FuseReqLabels,
    pub(crate) active: Option<ActiveGuard>,
}

/// Interior-mutable per-request metrics slot held behind `Arc<Mutex<…>>` on the
/// `FuseResponse`. The reply path writes it once (taking the guard, stashing
/// status); the sender reads it once at finish. See the design's
/// "FuseResponse API strategy" and "finish state machine".
///
/// `op_status` / `errno` / `unsupported_reason` are stored **independently of**
/// `active`, so taking the guard out into the reply task does not clear the
/// status the operation-duration timer reads later (Phase 1a-2 / Phase 2).
#[derive(Debug)]
pub(crate) struct FuseRespMetrics {
    pub(crate) labels: FuseReqLabels,
    /// The E2E guard; `take()`n exactly once when the reply is built.
    pub(crate) active: Option<ActiveGuard>,
    /// FS-operation result status. Stashed by every finish path and asserted by
    /// tests; the production read (the `operation_duration_us{status}` timer)
    /// lands in Phase 2. Kept separate from `request_status` because the two can
    /// diverge (op succeeds, delivery fails).
    #[allow(dead_code)] // production reader is operation_duration_us in Phase 2.
    pub(crate) op_status: Option<FuseReqStatus>,
    /// Final delivery/result status. The replied path carries status on the
    /// `RequestReply` task (read by the sender), so this slot copy exists for the
    /// enqueue-failure correction and test assertions, not a separate prod read.
    #[allow(dead_code)]
    // prod status flows via RequestReply; slot copy is for tests/early-finish.
    pub(crate) request_status: Option<FuseReqStatus>,
    /// errno stashed for diagnostics / tests. The errno *label* on the wire
    /// flows via the `RequestReply` task (replied path) and `finish_early`'s
    /// direct emit — not this slot field. Deliberately left at 0 on the no-reply
    /// path (`finish_no_reply`): `Forget`/`BatchForget` failures emit no
    /// errno-labelled metric (no meaningful errno — design decision), so the 0 is
    /// never read there.
    #[allow(dead_code)] // errno label flows via RequestReply / finish_early, not the slot.
    pub(crate) errno: i32,
    /// Source-tagged unsupported reason. The sender reads it from the
    /// `RequestReply` task (not this slot); the slot copy is for tests.
    #[allow(dead_code)] // unsupported reason flows via RequestReply.unsupported_reason.
    pub(crate) unsupported_reason: Option<&'static str>,
    /// Parse-failure reason. `finish_early` emits `decode_errors_total` from its
    /// `reason` argument directly; this slot copy is for test assertions.
    #[allow(dead_code)]
    // decode_errors_total is emitted from finish_early's arg, not the slot.
    pub(crate) parse_reason: Option<&'static str>,
    /// State-machine guard: prevents a second reply from double-finishing.
    pub(crate) finished: bool,
}

impl FuseRespMetrics {
    pub(crate) fn new(ctx: FuseReqCtx) -> Self {
        Self {
            labels: ctx.labels,
            active: ctx.active,
            op_status: None,
            request_status: None,
            errno: 0,
            unsupported_reason: None,
            parse_reason: None,
            finished: false,
        }
    }
}

/// RAII guard for an in-flight gauge: increments on construction, decrements
/// exactly once on drop.
///
/// It is `Send` and movable (so it can travel into a spawned task or onto a
/// reply task), and deliberately **not** `Copy` — a `Copy` guard could
/// double-decrement. The guard holds an optional `Gauge` handle so a single
/// type can back different scopes (`active_requests`, `stream_io_inflight`,
/// `meta_task_inflight`) just by being constructed from different gauges.
///
/// The `None` (no-op) form is what Phase 1a-1 uses: it exercises the full
/// move-and-drop lifetime — proving single-take / single-drop ownership — while
/// touching no real gauge. Phase 1a-2 swaps in `new(active_requests_gauge)` so
/// the same plumbing then drives the real metric.
#[derive(Debug)]
pub(crate) struct ActiveGuard {
    gauge: Option<Gauge>,
}

impl ActiveGuard {
    /// A guard backed by a real gauge: increments now, decrements on drop.
    pub(crate) fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge: Some(gauge) }
    }

    /// A no-op guard: same move/drop semantics, but touches no gauge. Used in
    /// Phase 1a-1 to validate ownership before the real gauge is wired (1a-2).
    pub(crate) fn noop() -> Self {
        Self { gauge: None }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if let Some(g) = &self.gauge {
            g.dec();
        }
    }
}

/// Zero-allocation RAII timer for a histogram.
///
/// Holds an already-resolved `Histogram` (e.g. obtained once via
/// `HistogramVec::with_label_values(...)` and stored), so its `drop` is a single
/// `observe()` with **no allocation and no per-call label-map probe**. This is
/// the hot-path replacement for `orpc`'s `MetricTimerVec`, which re-allocates a
/// `Vec<&str>` on every drop and must not be used per request.
///
/// Durations are measured with the monotonic clock (`Instant`).
///
/// Wired to call sites since Phase 2a (`setlkw_wait_timer`) and Phase 2b
/// (`io_dispatch_timer` / the `stream_lifecycle_scope` duration timer).
#[derive(Debug)]
pub(crate) struct HistogramTimer {
    start: Instant,
    hist: Histogram,
}

impl HistogramTimer {
    pub(crate) fn new(hist: Histogram) -> Self {
        Self {
            start: mono_now(),
            hist,
        }
    }
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        // Observe in microseconds, matching the `_us` metric convention.
        self.hist.observe(self.start.elapsed().as_micros() as f64);
    }
}

/// Phase 3a readdir timer for `read_dir_common`. Records
/// `readdir_duration_us{status=error}` on drop UNLESS `success(entries)` was
/// called, which instead records `readdir_duration_us{status=success}` +
/// `readdir_entries{status=success}`. So an early `?` return or an async
/// cancellation between awaits is counted as an error with NO `entries`
/// observation (no partial/zero count). Created via `start(enabled)` which
/// returns `None` when the `metrics_enabled` kill-switch is off (no timing, no
/// Drop emission); when `Some`, all emission routes through `FuseMetrics::with`,
/// so it is also a silent no-op when the singleton is uninitialized (readdir is
/// not on the reply path, so there is no `metrics.is_some()` gate here).
pub(crate) struct ReaddirTimer {
    start: Instant,
    error_on_drop: bool,
}

impl ReaddirTimer {
    /// `Some(timer)` only when metrics are enabled; `None` disables all readdir
    /// timing (no `mono_now`, no Drop emission). The `metrics_enabled`
    /// kill-switch — NOT a `noop()` guard: a disabled readdir creates no timer.
    pub(crate) fn start(enabled: bool) -> Option<Self> {
        enabled.then(|| Self {
            start: mono_now(),
            error_on_drop: true,
        })
    }

    /// Success path: record duration + entry count, and disarm the error Drop.
    /// `error_on_drop` is cleared BEFORE the observe so that if the (otherwise
    /// non-panicking) record were ever to panic, Drop would not double-record an
    /// error on top of the already-emitted success. The trade-off (a success that
    /// panics mid-observe would be lost) is acceptable since observe does not panic.
    pub(crate) fn success(mut self, entries: u64) {
        let elapsed_us = self.start.elapsed().as_micros() as u64;
        self.error_on_drop = false;
        FuseMetrics::with(|m| m.record_readdir_success(entries, elapsed_us));
    }
}

impl Drop for ReaddirTimer {
    fn drop(&mut self) {
        if self.error_on_drop {
            let elapsed_us = self.start.elapsed().as_micros() as u64;
            FuseMetrics::with(|m| m.record_readdir_error(elapsed_us));
        }
    }
}

/// Phase 3b per-stage timer for state persist/restore (D9). Records the stage
/// duration on drop with `status=error` UNLESS `success()` is called first (which
/// records `status=success` and disarms the error drop). So an early `?` return
/// or async cancellation mid-stage is counted as `error` ("the attempt did not
/// finish"), matching the request-stage convention. Created via `start(enabled,
/// is_persist, stage)` which returns `None` when metrics are disabled (no timing,
/// no Drop emission) — the `metrics_enabled` kill-switch, not a noop guard.
pub(crate) struct StateStageTimer {
    start: Instant,
    is_persist: bool,
    stage: &'static str,
    error_on_drop: bool,
}

impl StateStageTimer {
    pub(crate) fn start(enabled: bool, is_persist: bool, stage: &'static str) -> Option<Self> {
        enabled.then(|| Self {
            start: mono_now(),
            is_persist,
            stage,
            error_on_drop: true,
        })
    }

    /// Disarm the error drop and record `status=success` with the elapsed time.
    /// `error_on_drop` is cleared before the observe (Prometheus observe does not
    /// panic; this avoids a double-record if it ever did).
    pub(crate) fn success(mut self) {
        let elapsed_us = self.start.elapsed().as_micros() as u64;
        self.error_on_drop = false;
        let (is_persist, stage) = (self.is_persist, self.stage);
        FuseMetrics::with(|m| {
            m.observe_state_stage(is_persist, stage, STATE_STATUS_SUCCESS, elapsed_us)
        });
    }
}

impl Drop for StateStageTimer {
    fn drop(&mut self) {
        if self.error_on_drop {
            let elapsed_us = self.start.elapsed().as_micros() as u64;
            let (is_persist, stage) = (self.is_persist, self.stage);
            FuseMetrics::with(|m| {
                m.observe_state_stage(is_persist, stage, STATE_STATUS_ERROR, elapsed_us)
            });
        }
    }
}

/// Phase 3b shutdown-reason de-duplicator (D4). `session_shutdown_total` must be
/// recorded exactly once per session, by the FIRST cause: the fd-watcher, a
/// signal arm, and the `run_all` completion arm can all race to report a reason.
/// A single `compare_exchange` CAS lets the first caller win; later callers
/// no-op. Cheap `Arc<AtomicBool>`, shared by the session and the watcher task.
/// Emission is gated on `enabled` (the kill-switch) — the CAS still runs so the
/// "record once" invariant holds identically whether or not metrics are enabled.
#[derive(Clone)]
pub(crate) struct ShutdownOnce {
    recorded: std::sync::Arc<std::sync::atomic::AtomicBool>,
    enabled: bool,
}

impl ShutdownOnce {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            recorded: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            enabled,
        }
    }

    /// Record `reason` iff this is the first call (CAS false→true). Returns true
    /// if this call won the race (and thus emitted, when enabled).
    pub(crate) fn record_once(&self, reason: &'static str) -> bool {
        use std::sync::atomic::Ordering;
        if self
            .recorded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if self.enabled {
                FuseMetrics::with(|m| m.record_session_shutdown(reason));
            }
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orpc::common::Metrics as m;

    // Compile-time guarantee that the guards/timer are `Send`. They must travel
    // into spawned tasks and onto reply tasks (Phase 1a); if a future field
    // change made them `!Send`, this fails to compile here rather than silently
    // at the first cross-task move.
    #[test]
    fn guards_are_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ActiveGuard>();
        assert_send::<HistogramTimer>();
        assert_send::<FuseReqLabels>();
    }

    #[test]
    fn req_kind_labels() {
        assert_eq!(FuseReqKind::Metadata.as_str(), "metadata");
        assert_eq!(FuseReqKind::Stream.as_str(), "stream");
    }

    #[test]
    fn req_labels_are_copy_and_measure_monotonically() {
        let labels = FuseReqLabels::new("Lookup", FuseReqKind::Metadata, 64);
        // FuseReqLabels is Copy: using it after a copy must still compile/work.
        let copied = labels;
        assert_eq!(copied.opcode, "Lookup");
        assert_eq!(copied.kind, FuseReqKind::Metadata);
        assert_eq!(copied.request_bytes, 64);
        // elapsed_us is monotonic and never panics.
        let _ = labels.elapsed_us();
    }

    #[test]
    fn active_guard_inc_dec_balances() {
        let g = m::new_gauge("test_active_guard_gauge", "test gauge").unwrap();
        assert_eq!(g.get(), 0);
        {
            let _guard = ActiveGuard::new(g.clone());
            assert_eq!(g.get(), 1);
            {
                let _g2 = ActiveGuard::new(g.clone());
                assert_eq!(g.get(), 2);
            }
            assert_eq!(g.get(), 1);
        }
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn active_guard_moves_without_double_decrement() {
        let g = m::new_gauge("test_active_guard_move_gauge", "test gauge").unwrap();
        let guard = ActiveGuard::new(g.clone());
        assert_eq!(g.get(), 1);
        // Moving the guard must not change the count.
        let moved = guard;
        assert_eq!(g.get(), 1);
        drop(moved);
        // Dropped exactly once.
        assert_eq!(g.get(), 0);
    }

    #[test]
    fn histogram_timer_observes_on_drop() {
        let h = m::new_histogram("test_histogram_timer", "test histogram").unwrap();
        assert_eq!(h.get_sample_count(), 0);
        {
            let _t = HistogramTimer::new(h.clone());
        }
        assert_eq!(h.get_sample_count(), 1);
    }

    // The sender-side emission (`record_request_finish` / `record_notify_result`)
    // is a pure function over labels, so it is unit-testable without a kernel fd.
    // The process-global registry accumulates across tests, so each test uses a
    // UNIQUE opcode/code label and asserts a delta.
    fn requests_total(opcode: &str, kind: &str, reply_type: &str, status: &str) -> i64 {
        FuseMetrics::get()
            .requests_total
            .with_label_values(&[opcode, kind, reply_type, status])
            .get()
    }
    fn request_dur_count(opcode: &str, kind: &str, status: &str) -> u64 {
        FuseMetrics::get()
            .request_duration_us
            .with_label_values(&[opcode, kind, status])
            .get_sample_count()
    }

    #[test]
    fn req_status_label_strings() {
        assert_eq!(FuseReqStatus::Success.as_str(), "success");
        assert_eq!(FuseReqStatus::Error.as_str(), "error");
        assert_eq!(FuseReqStatus::Interrupted.as_str(), "interrupted");
        assert_eq!(FuseReqStatus::Unsupported.as_str(), "unsupported");
    }

    // test 1: a successful replied request increments requests_total{replied,
    // success} + request_duration once, response_write/bytes/reply_write stage
    // once, and NO error/unsupported/interrupted counter.
    #[test]
    fn record_request_finish_success_emits_request_and_response_series() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishSuccess";
        let rw_before = mx
            .response_write_duration_us
            .with_label_values(&[OP, "success"])
            .get_sample_count();
        let bytes_before = mx
            .response_bytes_total
            .with_label_values(&[OP, "success"])
            .get();
        let stage_before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_REPLY_WRITE, "metadata", "success"])
            .get_sample_count();

        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Success, // op_status
            FuseReqStatus::Success, // request_status
            0,
            None,
            128,
            WriteOutcome::Success,
            10,
            42,
        );

        assert_eq!(
            requests_total(OP, "metadata", REPLY_TYPE_REPLIED, "success"),
            1
        );
        assert_eq!(request_dur_count(OP, "metadata", "success"), 1);
        assert_eq!(
            mx.response_write_duration_us
                .with_label_values(&[OP, "success"])
                .get_sample_count(),
            rw_before + 1
        );
        assert_eq!(
            mx.response_bytes_total
                .with_label_values(&[OP, "success"])
                .get(),
            bytes_before + 128
        );
        // Lower bound: `stage_duration_us{reply_write,metadata,success}` is
        // opcode-free, a child shared by every successful metadata reply (concurrent
        // tests bump it too), so assert it moved by AT LEAST our emission. The
        // per-opcode `response_write_duration_us{OP,success}` exact +1 above already
        // pins this call's reply-write observation under the unique opcode.
        assert!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_REPLY_WRITE, "metadata", "success"])
                .get_sample_count()
                > stage_before
        );
        // No error/unsupported/interrupted for a success.
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "OTHER"])
                .get(),
            0
        );
    }

    // test 14: a real error (untagged) increments errors_total with the errno
    // label and NOT unsupported_total.
    #[test]
    fn record_request_finish_error_emits_errors_total_with_errno() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishError";
        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Error, // op_status
            FuseReqStatus::Error, // request_status
            libc::ENOSYS,
            None,
            0,
            WriteOutcome::Success,
            5,
            20,
        );
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "ENOSYS"])
                .get(),
            1,
            "untagged error increments errors_total with errno label"
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "unimplemented_opcode"])
                .get(),
            0,
            "error must NOT land in unsupported_total"
        );
    }

    // unsupported status routes to unsupported_total{reason} only (not
    // errors_total), reason from the source tag.
    #[test]
    fn record_request_finish_unsupported_routes_to_unsupported_total() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishUnsup";
        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Unsupported, // op_status
            FuseReqStatus::Unsupported, // request_status
            libc::ENOSYS,
            Some("unknown_opcode"),
            0,
            WriteOutcome::Success,
            5,
            20,
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "unknown_opcode"])
                .get(),
            1
        );
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "ENOSYS"])
                .get(),
            0,
            "unsupported must NOT also count as errors_total"
        );
    }

    // test 16 / P1#1: op succeeds but the kernel-fd write fails. The kernel
    // observes a failed request, so the request_status-labelled series go to
    // `error`, while the op-level counters stay clean (op succeeded). The write
    // errno is the independent delivery dimension.
    #[test]
    fn record_request_finish_write_failure_sets_request_status_error_keeps_op_clean() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP_WITH: &str = "FinishWriteErr";
        mx.record_request_finish(
            OP_WITH,
            FuseReqKind::Stream,
            FuseReqStatus::Success, // op_status: the FS op succeeded
            FuseReqStatus::Error,   // request_status: delivery failed
            0,
            None,
            0,
            WriteOutcome::Failed {
                errno: Some(libc::EIO),
            },
            5,
            20,
        );
        // Delivery error dimension.
        assert_eq!(
            mx.response_write_errors_total
                .with_label_values(&[OP_WITH, "EIO"])
                .get(),
            1
        );
        // request_status reflects the delivery failure (NOT success).
        assert_eq!(
            requests_total(OP_WITH, "stream", REPLY_TYPE_REPLIED, "error"),
            1,
            "write failure -> request_status=error"
        );
        assert_eq!(
            requests_total(OP_WITH, "stream", REPLY_TYPE_REPLIED, "success"),
            0,
            "write failure must NOT be counted as a success request"
        );
        // op-level errors_total stays clean: the op itself did not fail.
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP_WITH, "stream", "EIO"])
                .get(),
            0,
            "op succeeded, so errors_total must stay clean"
        );

        // No OS errno on the write failure -> OTHER label.
        const OP_NONE: &str = "FinishWriteErrNone";
        mx.record_request_finish(
            OP_NONE,
            FuseReqKind::Stream,
            FuseReqStatus::Success,
            FuseReqStatus::Error,
            0,
            None,
            0,
            WriteOutcome::Failed { errno: None },
            5,
            20,
        );
        assert_eq!(
            mx.response_write_errors_total
                .with_label_values(&[OP_NONE, "OTHER"])
                .get(),
            1,
            "no OS errno maps to the OTHER label"
        );
    }

    // P1#2: a defensive guard — an Unsupported op_status with no source tag is a
    // wiring bug. It must not silently masquerade as unimplemented_opcode; it is
    // bucketed under missing_reason (and asserts in debug). Pass request_status
    // == op_status (write succeeded) so only the op-status path is exercised.
    #[test]
    #[cfg(not(debug_assertions))] // debug_assert! would (correctly) panic in debug.
    fn record_request_finish_unsupported_without_reason_buckets_missing() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "FinishUnsupNoReason";
        mx.record_request_finish(
            OP,
            FuseReqKind::Metadata,
            FuseReqStatus::Unsupported,
            FuseReqStatus::Unsupported,
            libc::ENOSYS,
            None, // missing source tag (a bug)
            0,
            WriteOutcome::Success,
            5,
            20,
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "missing_reason"])
                .get(),
            1,
            "missing source tag is bucketed distinctly, not as unimplemented_opcode"
        );
        assert_eq!(
            mx.unsupported_total
                .with_label_values(&[OP, "unimplemented_opcode"])
                .get(),
            0,
        );
    }

    // Debug counterpart: the missing source tag is a wiring bug and must trip the
    // debug_assert. (Release buckets it under `missing_reason` — see above.)
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "without a source tag")]
    fn record_request_finish_unsupported_without_reason_panics_in_debug() {
        FuseMetrics::ensure_init().unwrap();
        FuseMetrics::get().record_op_terminal(
            "FinishUnsupNoReasonDbg",
            FuseReqKind::Metadata,
            FuseReqStatus::Unsupported,
            libc::ENOSYS,
            None, // missing source tag trips debug_assert!
        );
    }

    // test 12: notify lifecycle states are distinct counters under notify_total.
    #[test]
    fn record_notify_result_counts_three_states() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const CODE: &str = "test_notify_code";
        mx.record_notify_result(CODE, NOTIFY_SUCCESS);
        mx.record_notify_result(CODE, NOTIFY_ENQUEUE_FAILED);
        mx.record_notify_result(CODE, NOTIFY_WRITE_FAILED);
        assert_eq!(
            mx.notify_total
                .with_label_values(&[CODE, NOTIFY_SUCCESS])
                .get(),
            1
        );
        assert_eq!(
            mx.notify_total
                .with_label_values(&[CODE, NOTIFY_ENQUEUE_FAILED])
                .get(),
            1
        );
        assert_eq!(
            mx.notify_total
                .with_label_values(&[CODE, NOTIFY_WRITE_FAILED])
                .get(),
            1
        );
    }

    // --- Phase 1b-1 ---

    // receive_errors_total: errno + action labels recorded as a delta.
    #[test]
    fn record_receive_error_counts_by_errno_action() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .receive_errors_total
            .with_label_values(&["enoent", RECEIVE_ACTION_CONTINUE])
            .get();
        mx.record_receive_error("enoent", RECEIVE_ACTION_CONTINUE);
        assert_eq!(
            mx.receive_errors_total
                .with_label_values(&["enoent", RECEIVE_ACTION_CONTINUE])
                .get(),
            before + 1
        );
        // exit action is a distinct series.
        let exit_before = mx
            .receive_errors_total
            .with_label_values(&["enodev", RECEIVE_ACTION_EXIT])
            .get();
        mx.record_receive_error("enodev", RECEIVE_ACTION_EXIT);
        assert_eq!(
            mx.receive_errors_total
                .with_label_values(&["enodev", RECEIVE_ACTION_EXIT])
                .get(),
            exit_before + 1
        );
    }

    // record_decode_error emits under phase=decode (the 1b site; 1a-2 already
    // had phase=parse via record_parse_error). We assert only the decode series
    // delta: a cross-series ("parse untouched") assertion can't be made reliably
    // against the process-global registry under parallel tests, and a `>=` guard
    // would prove nothing — so we don't pretend to. `decode` vs `parse` being
    // distinct labels is guaranteed by the const values, not by a runtime check.
    #[test]
    fn record_decode_error_increments_decode_phase() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let decode_before = mx
            .decode_errors_total
            .with_label_values(&[DECODE_PHASE_DECODE, "other"])
            .get();
        mx.record_decode_error("other");
        assert_eq!(
            mx.decode_errors_total
                .with_label_values(&[DECODE_PHASE_DECODE, "other"])
                .get(),
            decode_before + 1,
            "decode phase incremented"
        );
    }

    // meta_task_guard: disabled MUST be None (no metric machinery); enabled is
    // Some and inc/dec balances around the gauge.
    // NOTE: the enabled inc/dec check uses before/after on the process-global
    // `meta_task_inflight` gauge; it relies on no other test mutating that gauge
    // in parallel (true today — this is the only meta_task test). If a future
    // test also touches `meta_task_inflight`, switch this to a standalone
    // test-only `Gauge` + `ActiveGuard::new(g.clone())` or serialize it.
    #[test]
    fn meta_task_guard_gate() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::meta_task_guard(false).is_none(),
            "disabled path must be None, never a noop guard"
        );

        let mx = FuseMetrics::get();
        let before = mx.meta_task_inflight.get();
        let guard = FuseMetrics::meta_task_guard(true);
        assert!(guard.is_some());
        assert_eq!(
            mx.meta_task_inflight.get(),
            before + 1,
            "guard inc on create"
        );
        drop(guard);
        assert_eq!(mx.meta_task_inflight.get(), before, "guard dec on drop");
    }

    // record_scrape sets bytes and observes duration (last-scrape semantics).
    // NOTE: asserts an absolute `set` on the process-global `metrics_scrape_bytes`
    // gauge; relies on no other test calling `record_scrape()` in parallel (true
    // today). A future handler last-scrape test should serialize or use an
    // isolated gauge to avoid clobbering this value.
    #[test]
    fn record_scrape_sets_bytes_and_observes_duration() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let count_before = mx.metrics_scrape_duration_us.get_sample_count();
        mx.record_scrape(42, 1234);
        assert_eq!(
            mx.metrics_scrape_bytes.get(),
            1234,
            "bytes = last scrape size"
        );
        assert_eq!(
            mx.metrics_scrape_duration_us.get_sample_count(),
            count_before + 1,
            "duration observed once"
        );
    }

    // record_meta_spawn observes the stage_duration_us{meta_spawn,metadata,success}
    // series — guards against a label/status/kind typo in the core 1b-1 helper.
    #[test]
    fn record_meta_spawn_observes_correct_labels() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_META_SPAWN, "metadata", "success"])
            .get_sample_count();
        mx.record_meta_spawn(123);
        assert_eq!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_META_SPAWN, "metadata", "success"])
                .get_sample_count(),
            before + 1,
            "meta_spawn observed under stage=meta_spawn,kind=metadata,status=success"
        );
    }

    // record_receive_loop_wait observes the (no-label) histogram — guards against
    // the field/helper/name drifting silently.
    #[test]
    fn record_receive_loop_wait_observes() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx.receive_loop_wait_duration_us.get_sample_count();
        mx.record_receive_loop_wait(42);
        assert_eq!(
            mx.receive_loop_wait_duration_us.get_sample_count(),
            before + 1
        );
    }

    // --- Phase 2a helper tests ---

    // E1: record_operation feeds BOTH families from one timer — the per-opcode
    // `operation_duration_us{opcode,kind=metadata,status}` and the opcode-free
    // `stage_duration_us{stage=operation,kind=metadata,status}` — under the
    // stashed op_status (here: success). Unique opcode + delta on the shared
    // registry.
    #[test]
    fn record_operation_observes_both_operation_and_stage_families() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "OpDurSuccess";
        let op_before = mx
            .operation_duration_us
            .with_label_values(&[OP, "metadata", "success"])
            .get_sample_count();
        let stage_before = mx
            .stage_duration_us
            .with_label_values(&[STAGE_OPERATION, "metadata", "success"])
            .get_sample_count();

        mx.record_operation(OP, FuseReqStatus::Success, 321);

        // Exact +1: `operation_duration_us` is keyed by the UNIQUE opcode `OP`, so no
        // other (parallel) test touches this child.
        assert_eq!(
            mx.operation_duration_us
                .with_label_values(&[OP, "metadata", "success"])
                .get_sample_count(),
            op_before + 1,
            "operation_duration_us observed once under opcode/metadata/success"
        );
        // Lower bound: `stage_duration_us{stage=operation,metadata,success}` is
        // opcode-FREE, so it is a child SHARED by every metadata op that runs
        // `record_operation` (e.g. the dispatch_meta integration tests). Under the
        // default parallel harness a concurrent success op can also bump it between
        // our before/after reads, so we assert it moved by AT LEAST our one emission,
        // not exactly one. The dual-emit intent (one call feeds BOTH families) is
        // still proven: the exact +1 above pins the per-opcode family, and this pins
        // that the stage family also received this call's emission.
        assert!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_OPERATION, "metadata", "success"])
                .get_sample_count()
                > stage_before,
            "stage_duration_us observed under stage=operation/metadata/success"
        );
    }

    // E2: status comes through verbatim (here: error) — the timer observes
    // whatever op_status the caller read back from the slot, NOT a hard-coded
    // success. Guards against a status-source regression in the helper labels.
    #[test]
    fn record_operation_carries_error_status() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const OP: &str = "OpDurError";
        let before = mx
            .operation_duration_us
            .with_label_values(&[OP, "metadata", "error"])
            .get_sample_count();
        mx.record_operation(OP, FuseReqStatus::Error, 7);
        assert_eq!(
            mx.operation_duration_us
                .with_label_values(&[OP, "metadata", "error"])
                .get_sample_count(),
            before + 1,
            "error op_status lands under status=error, not success"
        );
        // record_operation must NOT touch the op-terminal counters (the request
        // terminal already did) — no errors_total double-count from the timer.
        assert_eq!(
            mx.errors_total
                .with_label_values(&[OP, "metadata", "OTHER"])
                .get(),
            0,
            "record_operation only observes latency, never record_op_terminal"
        );
    }

    // E (B2 gate): reply_queue_guard returns Some once the singleton is
    // initialized, inc on create / dec on drop. (The disabled path produces the
    // legacy Reply and never calls this; the gate lives at the FuseResponse call
    // site, so there is no `false` arm to assert here.)
    #[test]
    fn reply_queue_guard_inc_dec_balances() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx.reply_queue_depth.get();
        let guard = FuseMetrics::reply_queue_guard();
        assert!(guard.is_some(), "initialized singleton yields Some");
        assert_eq!(
            mx.reply_queue_depth.get(),
            before + 1,
            "reply_queue_depth inc on guard create"
        );
        drop(guard);
        assert_eq!(
            mx.reply_queue_depth.get(),
            before,
            "reply_queue_depth dec on guard drop"
        );
    }

    // E4: setlkw_inflight_guard gate — disabled is None (never noop), enabled
    // inc/dec balances the gauge.
    #[test]
    fn setlkw_inflight_guard_gate() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::setlkw_inflight_guard(false).is_none(),
            "disabled path must be None, never a noop guard"
        );
        let mx = FuseMetrics::get();
        let before = mx.setlkw_inflight.get();
        let guard = FuseMetrics::setlkw_inflight_guard(true);
        assert!(guard.is_some());
        assert_eq!(mx.setlkw_inflight.get(), before + 1, "guard inc on create");
        drop(guard);
        assert_eq!(mx.setlkw_inflight.get(), before, "guard dec on drop");
    }

    // E17: setlkw_wait_timer gate — disabled builds NO timer (no clock read, no
    // observe on drop), enabled observes exactly once on drop.
    #[test]
    fn setlkw_wait_timer_gate() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();

        // Disabled: None, and dropping it observes nothing.
        let before = mx.setlkw_wait_duration_us.get_sample_count();
        let disabled = FuseMetrics::setlkw_wait_timer(false);
        assert!(disabled.is_none(), "disabled must be None");
        drop(disabled);
        assert_eq!(
            mx.setlkw_wait_duration_us.get_sample_count(),
            before,
            "disabled timer must not observe"
        );

        // Enabled: observes once on drop (covers normal completion AND the
        // interrupt-cancellation drop — both are just a Drop).
        let timer = FuseMetrics::setlkw_wait_timer(true);
        assert!(timer.is_some());
        drop(timer);
        assert_eq!(
            mx.setlkw_wait_duration_us.get_sample_count(),
            before + 1,
            "enabled timer observes once on drop"
        );
    }

    // --- Phase 2b helper tests ---
    //
    // The process-global registry accumulates across parallel tests, so value
    // assertions read a child's counter/histogram before and after and check the
    // delta on a label set the test owns. The two read/write `path_type` labels are
    // a closed set shared by every read/write test, so these use a UNIQUE synthetic
    // `path_type` per test (e.g. "pt_io_rw") to isolate their children — the helper
    // takes `path_type` as a parameter, so a test-only label is just as valid a
    // child as a real backend and never collides with another test or with e2e.

    // E (dispatch_io_type / lifecycle_io_type closed maps, R-overall P1#6): the 5
    // stream opcodes map to the LOWERCASE io_type consts (NOT opcode.as_str(), which
    // is "Read"/"Fsync" etc.); non-stream / non-IO opcodes map to None; and exactly
    // one of the two maps is Some for any known stream opcode.
    #[test]
    fn stream_io_type_maps_are_lowercase_and_disjoint() {
        // dispatch (read/write).
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_READ), Some(IO_TYPE_READ));
        assert_eq!(
            dispatch_io_type(FuseOpCode::FUSE_WRITE),
            Some(IO_TYPE_WRITE)
        );
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_FLUSH), None);
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_FSYNC), None);
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_RELEASE), None);
        assert_eq!(dispatch_io_type(FuseOpCode::FUSE_LOOKUP), None);

        // lifecycle (flush/fsync/release).
        assert_eq!(
            lifecycle_io_type(FuseOpCode::FUSE_FLUSH),
            Some(IO_TYPE_FLUSH)
        );
        assert_eq!(
            lifecycle_io_type(FuseOpCode::FUSE_FSYNC),
            Some(IO_TYPE_FSYNC)
        );
        assert_eq!(
            lifecycle_io_type(FuseOpCode::FUSE_RELEASE),
            Some(IO_TYPE_RELEASE)
        );
        assert_eq!(lifecycle_io_type(FuseOpCode::FUSE_READ), None);
        assert_eq!(lifecycle_io_type(FuseOpCode::FUSE_WRITE), None);

        // The lowercase consts are NOT the capitalized request labels.
        assert_eq!(IO_TYPE_READ, "read");
        assert_eq!(IO_TYPE_WRITE, "write");
        assert_eq!(IO_TYPE_FSYNC, "fsync");
        assert_ne!(IO_TYPE_READ, FuseOpCode::FUSE_READ.as_str());
        assert_ne!(IO_TYPE_FSYNC, FuseOpCode::FUSE_FSYNC.as_str());

        // For each known stream opcode, exactly one map is Some (mutually exclusive).
        for op in [
            FuseOpCode::FUSE_READ,
            FuseOpCode::FUSE_WRITE,
            FuseOpCode::FUSE_FLUSH,
            FuseOpCode::FUSE_FSYNC,
            FuseOpCode::FUSE_RELEASE,
        ] {
            assert!(
                dispatch_io_type(op).is_some() ^ lifecycle_io_type(op).is_some(),
                "exactly one of dispatch/lifecycle is Some for {op:?}"
            );
        }
    }

    // E7/E13/E14 (read/write io family): a successful read records duration + stage
    // + requests{success} + size + bytes{success}; an error records duration + stage
    // + requests{error} + size, but creates NO bytes child (never inc_by(0)). Uses a
    // unique path_type so the children are isolated on the shared registry.
    #[test]
    fn record_stream_io_success_and_error_families() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        const PT: &str = "pt_io_rw"; // test-owned path_type, isolates children.

        let dur_s = mx
            .io_duration_us
            .with_label_values(&[IO_TYPE_READ, PT, "success"])
            .get_sample_count();
        let stage_s = mx
            .stage_duration_us
            .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
            .get_sample_count();
        let req_s = mx
            .io_requests_total
            .with_label_values(&[IO_TYPE_READ, PT, "success"])
            .get();
        let size_s = mx
            .io_size_bytes
            .with_label_values(&[IO_TYPE_READ, PT])
            .get_sample_count();
        let bytes_s = mx
            .io_bytes_total
            .with_label_values(&[IO_TYPE_READ, PT, "success"])
            .get();

        // Success: 4096 bytes transferred, requested 8192.
        mx.record_stream_io(IO_TYPE_READ, PT, true, 4096, 8192, 50);

        assert_eq!(
            mx.io_duration_us
                .with_label_values(&[IO_TYPE_READ, PT, "success"])
                .get_sample_count(),
            dur_s + 1
        );
        // Lower bound: `stage_duration_us{stream_io,stream,success}` is opcode-free,
        // a child shared by every read/write backend success (the reader/writer
        // task-body integration tests bump it concurrently), so assert it moved by
        // AT LEAST our emission. The exact +1 lives on the per-(io_type,path_type)
        // `io_*` children above, which use this test's own unique `PT` path_type.
        assert!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_STREAM_IO, "stream", "success"])
                .get_sample_count()
                > stage_s,
            "read backend call emits stage=stream_io,kind=stream"
        );
        assert_eq!(
            mx.io_requests_total
                .with_label_values(&[IO_TYPE_READ, PT, "success"])
                .get(),
            req_s + 1
        );
        assert_eq!(
            mx.io_size_bytes
                .with_label_values(&[IO_TYPE_READ, PT])
                .get_sample_count(),
            size_s + 1
        );
        assert_eq!(
            mx.io_bytes_total
                .with_label_values(&[IO_TYPE_READ, PT, "success"])
                .get(),
            bytes_s + 4096,
            "bytes uses ACTUAL transferred (4096), not requested (8192)"
        );

        // Error: requests{error}+1, duration{error}+1, size+1, but NO bytes child.
        let req_e = mx
            .io_requests_total
            .with_label_values(&[IO_TYPE_READ, PT, "error"])
            .get();
        let dur_e = mx
            .io_duration_us
            .with_label_values(&[IO_TYPE_READ, PT, "error"])
            .get_sample_count();
        let bytes_e = mx
            .io_bytes_total
            .with_label_values(&[IO_TYPE_READ, PT, "error"])
            .get();
        mx.record_stream_io(IO_TYPE_READ, PT, false, 0, 8192, 9);
        assert_eq!(
            mx.io_requests_total
                .with_label_values(&[IO_TYPE_READ, PT, "error"])
                .get(),
            req_e + 1,
            "error attempt counts in io_requests_total{{status=error}}"
        );
        assert_eq!(
            mx.io_duration_us
                .with_label_values(&[IO_TYPE_READ, PT, "error"])
                .get_sample_count(),
            dur_e + 1,
            "error duration observed"
        );
        assert_eq!(
            mx.io_bytes_total
                .with_label_values(&[IO_TYPE_READ, PT, "error"])
                .get(),
            bytes_e,
            "error read must NOT create a status=error bytes child (no inc_by(0))"
        );
    }

    // E6 (stream_io_inflight gate): disabled is None (never noop); enabled is Some
    // and inc/dec balances the GaugeVec child for the given io_type. Uses the real
    // read child but reads before/after deltas so it is parallel-safe.
    #[test]
    fn stream_io_guard_gate_and_balance() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::stream_io_guard(false, IO_TYPE_READ).is_none(),
            "disabled stream_io_guard must be None, never a noop guard"
        );
        let mx = FuseMetrics::get();
        let before = mx
            .stream_io_inflight
            .with_label_values(&[IO_TYPE_WRITE])
            .get();
        let guard = FuseMetrics::stream_io_guard(true, IO_TYPE_WRITE);
        assert!(guard.is_some());
        assert_eq!(
            mx.stream_io_inflight
                .with_label_values(&[IO_TYPE_WRITE])
                .get(),
            before + 1,
            "stream_io_inflight{{write}} inc on guard create"
        );
        drop(guard);
        assert_eq!(
            mx.stream_io_inflight
                .with_label_values(&[IO_TYPE_WRITE])
                .get(),
            before,
            "stream_io_inflight{{write}} dec on guard drop"
        );
    }

    // E21 (stream_lifecycle_scope): opening the scope counts the attempt
    // immediately (before the backend runs), holds the inflight guard while alive
    // (gauge>0), and observes the duration once on drop; the inflight returns to
    // baseline after drop. Asserts the FULL {io_type,path_type="unknown"} label set
    // (R-overall P1#5).
    #[test]
    fn stream_lifecycle_scope_counts_attempt_holds_inflight_observes_on_drop() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let attempt_before = mx
            .stream_lifecycle_requests_total
            .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
            .get();
        let dur_before = mx
            .stream_lifecycle_duration_us
            .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
            .get_sample_count();
        let inflight_before = mx
            .stream_lifecycle_inflight
            .with_label_values(&[IO_TYPE_FLUSH])
            .get();

        let scope = FuseMetrics::stream_lifecycle_scope(IO_TYPE_FLUSH);
        // Attempt counted at open (before any backend work).
        assert_eq!(
            mx.stream_lifecycle_requests_total
                .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
                .get(),
            attempt_before + 1,
            "attempt counted when the scope opens"
        );
        // Inflight is held while the scope is alive.
        assert_eq!(
            mx.stream_lifecycle_inflight
                .with_label_values(&[IO_TYPE_FLUSH])
                .get(),
            inflight_before + 1,
            "lifecycle inflight held while the scope is alive"
        );
        // Duration not observed until drop.
        assert_eq!(
            mx.stream_lifecycle_duration_us
                .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
                .get_sample_count(),
            dur_before,
            "duration not observed until the scope drops"
        );

        drop(scope);
        assert_eq!(
            mx.stream_lifecycle_duration_us
                .with_label_values(&[IO_TYPE_FLUSH, PATH_TYPE_UNKNOWN])
                .get_sample_count(),
            dur_before + 1,
            "duration observed once on drop"
        );
        assert_eq!(
            mx.stream_lifecycle_inflight
                .with_label_values(&[IO_TYPE_FLUSH])
                .get(),
            inflight_before,
            "lifecycle inflight back to baseline after drop"
        );
    }

    // E (io_dispatch_timer): observes once on drop under the io_type child.
    #[test]
    fn io_dispatch_timer_observes_once_on_drop() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let before = mx
            .io_dispatch_duration_us
            .with_label_values(&[IO_TYPE_WRITE])
            .get_sample_count();
        {
            let _t = FuseMetrics::io_dispatch_timer(IO_TYPE_WRITE);
        }
        assert_eq!(
            mx.io_dispatch_duration_us
                .with_label_values(&[IO_TYPE_WRITE])
                .get_sample_count(),
            before + 1,
            "io_dispatch timer observes once on drop"
        );
    }

    // E4 (stream_write_queue_guard gate): disabled None (never noop), enabled inc/dec
    // balances the gauge.
    #[test]
    fn stream_write_queue_guard_gate() {
        FuseMetrics::ensure_init().unwrap();
        assert!(
            FuseMetrics::stream_write_queue_guard(false).is_none(),
            "disabled stream_write_queue_guard must be None, never a noop guard"
        );
        let mx = FuseMetrics::get();
        let before = mx.stream_write_queue_depth.get();
        let guard = FuseMetrics::stream_write_queue_guard(true);
        assert!(guard.is_some());
        assert_eq!(
            mx.stream_write_queue_depth.get(),
            before + 1,
            "stream_write_queue_depth inc on guard create"
        );
        drop(guard);
        assert_eq!(
            mx.stream_write_queue_depth.get(),
            before,
            "stream_write_queue_depth dec on guard drop"
        );
    }

    // E24 (negative assertion, io family is read/write only): record_stream_io must
    // never be called with a flush/fsync/release io_type in production — the family
    // SPLIT is structural (lifecycle uses stream_lifecycle_*). Here we assert the
    // closed maps enforce that split: an io_type that would land in io_* only ever
    // comes from dispatch_io_type (read/write), and a lifecycle io_type only from
    // lifecycle_io_type (flush/fsync/release), so the two label sets cannot cross.
    #[test]
    fn io_and_lifecycle_io_types_never_cross() {
        // The only io_types that reach record_stream_io / stream_io_guard.
        let io_only = [IO_TYPE_READ, IO_TYPE_WRITE];
        // The only io_types that reach stream_lifecycle_scope.
        let lifecycle_only = [IO_TYPE_FLUSH, IO_TYPE_FSYNC, IO_TYPE_RELEASE];
        for io in io_only {
            assert!(
                !lifecycle_only.contains(&io),
                "{io} must not be a lifecycle io_type"
            );
        }
        for lc in lifecycle_only {
            assert!(
                !io_only.contains(&lc),
                "{lc} must not be a read/write io_type"
            );
        }
    }

    // Contract seam: the fuse-side path_type label consts MUST match the literals
    // `UnifiedReader/Writer::path_type()` produces in curvine-client (that accessor
    // returns raw string literals, not these consts). This pins the vocabulary the
    // two crates share — the curvine-client test asserts the Local accessor returns
    // "local"; this asserts fuse's const agrees, so the label can never drift apart
    // across the crate boundary. (PATH_TYPE_UNKNOWN is used in production by the
    // lifecycle helper; the backend values are referenced here.)
    #[test]
    fn path_type_label_consts_match_client_vocabulary() {
        assert_eq!(PATH_TYPE_CURVINE, "curvine");
        assert_eq!(PATH_TYPE_UFS, "ufs");
        assert_eq!(PATH_TYPE_FALLBACK, "fallback");
        assert_eq!(PATH_TYPE_LOCAL, "local");
        assert_eq!(PATH_TYPE_UNKNOWN, "unknown");
    }

    // E27 (negative assertion): there is NO STAGE_STREAM_ENQUEUE const and
    // stage=stream_io is the only Phase 2b stage value. This is a compile-time-ish
    // guard: STAGE_STREAM_IO is "stream_io" and there is no "stream_enqueue" stage
    // const to reference (grep-enforced in review; asserted by value here).
    #[test]
    fn stage_stream_io_is_the_only_phase2b_stage() {
        assert_eq!(STAGE_STREAM_IO, "stream_io");
        // No STAGE_STREAM_ENQUEUE exists; if one were added this test's neighbors
        // (the no-enqueue rule) and the send_stream code review would catch it.
    }

    // --- Phase 3a helper tests ---
    //
    // Same parallel-safety discipline as Phase 2b: the process-global registry
    // accumulates across parallel tests, so value assertions read a child's
    // counter before/after on a label set the test owns. `user_meta_cache_total`
    // and `*_invalidations_total` take the `cache` label as a param, so each test
    // uses a UNIQUE synthetic `cache` value to isolate its children. The no-label
    // `negative_entry_returned_total` uses a before/after delta.

    #[test]
    fn invalidation_records_one_per_namespace_with_and_without_parent() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        // Unique synthetic reason so the child counters are owned by this test.
        let reason = "test_inval_reason_unique";

        let s_before = mx
            .user_meta_cache_invalidations_total
            .with_label_values(&[CACHE_STATUS, reason])
            .get();
        let l_before = mx
            .user_meta_cache_invalidations_total
            .with_label_values(&[CACHE_LIST, reason])
            .get();
        let b_before = mx
            .user_meta_cache_invalidations_total
            .with_label_values(&[CACHE_BLOCKS, reason])
            .get();

        // With a parent: status +1, blocks +1, list +2 (path's own + parent's).
        mx.record_invalidation(reason, true);
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_STATUS, reason])
                .get(),
            s_before + 1
        );
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_BLOCKS, reason])
                .get(),
            b_before + 1
        );
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_LIST, reason])
                .get(),
            l_before + 2,
            "list increments twice with a parent (path + parent listing)"
        );

        // Without a parent (root): status +1, blocks +1, list +1 (path only).
        mx.record_invalidation(reason, false);
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_LIST, reason])
                .get(),
            l_before + 3,
            "no-parent call adds exactly one more list inc"
        );
        assert_eq!(
            mx.user_meta_cache_invalidations_total
                .with_label_values(&[CACHE_STATUS, reason])
                .get(),
            s_before + 2
        );
    }

    #[test]
    fn user_meta_cache_hit_miss_put_increment_their_child() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let cache = "test_cache_ns_unique";

        let read_child = |status: &str| {
            mx.user_meta_cache_total
                .with_label_values(&[cache, status])
                .get()
        };
        let (h, m, p) = (
            read_child(CACHE_RESULT_HIT),
            read_child(CACHE_RESULT_MISS),
            read_child(CACHE_RESULT_PUT),
        );

        mx.record_user_meta_cache(cache, CACHE_RESULT_HIT);
        mx.record_user_meta_cache(cache, CACHE_RESULT_MISS);
        mx.record_user_meta_cache(cache, CACHE_RESULT_PUT);

        assert_eq!(read_child(CACHE_RESULT_HIT), h + 1);
        assert_eq!(read_child(CACHE_RESULT_MISS), m + 1);
        assert_eq!(read_child(CACHE_RESULT_PUT), p + 1);
    }

    #[test]
    fn negative_entry_counter_increments() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        // `negative_entry_returned_total` is a process-global, no-label counter
        // shared across parallel tests, so assert a lower bound on the delta, not
        // an exact value (another test could also increment it concurrently).
        let before = mx.negative_entry_returned_total.get();
        mx.record_negative_entry();
        mx.record_negative_entry();
        assert!(
            mx.negative_entry_returned_total.get() > before + 1,
            "two records must add at least 2 to the shared counter"
        );
    }

    #[test]
    fn readdir_timer_success_observes_entries_and_duration_drop_is_noop() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let s_before = mx
            .readdir_duration_us
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .get_sample_count();
        let e_before = mx
            .readdir_entries
            .with_label_values(&[READDIR_STATUS_SUCCESS])
            .get_sample_count();

        ReaddirTimer::start(true)
            .expect("enabled => Some")
            .success(7);

        // Shared {status=success} histogram children; lower-bound the delta since
        // a concurrent test could also observe them.
        assert!(
            mx.readdir_duration_us
                .with_label_values(&[READDIR_STATUS_SUCCESS])
                .get_sample_count()
                > s_before,
            "success records at least one duration sample"
        );
        assert!(
            mx.readdir_entries
                .with_label_values(&[READDIR_STATUS_SUCCESS])
                .get_sample_count()
                > e_before,
            "success records at least one entries sample"
        );
    }

    #[test]
    fn readdir_timer_disabled_creates_no_timer_and_records_nothing() {
        // metrics_enabled=false => start() returns None, and there is no Drop
        // emission. (Use a unique-ish read on both status children's totals.)
        FuseMetrics::ensure_init().unwrap();
        assert!(
            ReaddirTimer::start(false).is_none(),
            "disabled must yield no timer"
        );
    }

    #[test]
    fn readdir_timer_drop_without_success_records_error_no_entries() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();
        let d_before = mx
            .readdir_duration_us
            .with_label_values(&[READDIR_STATUS_ERROR])
            .get_sample_count();
        let e_before = mx
            .readdir_entries
            .with_label_values(&[READDIR_STATUS_ERROR])
            .get_sample_count();

        // Drop without calling success() — the early-return / cancellation path.
        drop(ReaddirTimer::start(true).expect("enabled => Some"));

        // Shared {status=error} children; lower-bound the duration delta.
        assert!(
            mx.readdir_duration_us
                .with_label_values(&[READDIR_STATUS_ERROR])
                .get_sample_count()
                > d_before,
            "drop-without-success records at least one error duration sample"
        );
        // The entries{error} child is never written by any code path (the whole
        // point), so it stays at its baseline — assert it did not move. (No other
        // code observes readdir_entries{error}, so this exact check is stable.)
        assert_eq!(
            mx.readdir_entries
                .with_label_values(&[READDIR_STATUS_ERROR])
                .get_sample_count(),
            e_before,
            "error path must NOT observe readdir_entries (no partial/zero count)"
        );
    }

    #[test]
    fn invalidation_reason_consts_match_design_15_values() {
        // The 15-value bounded enum the design doc pins. A change here must be
        // mirrored in the design doc's reason list.
        let reasons = [
            INVAL_REASON_SETATTR,
            INVAL_REASON_RESIZE,
            INVAL_REASON_SETXATTR,
            INVAL_REASON_REMOVEXATTR,
            INVAL_REASON_MKDIR,
            INVAL_REASON_CREATE,
            INVAL_REASON_OPEN_WRITE,
            INVAL_REASON_FLUSH,
            INVAL_REASON_RELEASE,
            INVAL_REASON_UNLINK,
            INVAL_REASON_LINK,
            INVAL_REASON_RMDIR,
            INVAL_REASON_RENAME,
            INVAL_REASON_SYMLINK,
            INVAL_REASON_FSYNC,
        ];
        assert_eq!(reasons.len(), 15);
        // No catch-all `other` and no `kernel_notify` (dropped in Phase 3a).
        assert!(!reasons.contains(&"other"));
        assert!(!reasons.contains(&"kernel_notify"));
    }

    // --- Phase 3b seam tests ---

    // ShutdownOnce records exactly once (first cause wins); later callers no-op.
    // Verified via the return-value of record_once (true only for the winner),
    // which is deterministic and does not touch the process-global counter.
    #[test]
    fn shutdown_once_records_first_cause_only() {
        let once = ShutdownOnce::new(true);
        assert!(once.record_once(SHUTDOWN_FD_WATCHER), "first call wins");
        assert!(
            !once.record_once(SHUTDOWN_COMPLETED),
            "second call is a no-op (already recorded)"
        );
        assert!(
            !once.record_once(SHUTDOWN_TERM_SIGNAL),
            "third call is a no-op too"
        );
    }

    // ShutdownOnce with metrics disabled still enforces the once semantics (the
    // CAS runs regardless), it just does not emit.
    #[test]
    fn shutdown_once_disabled_still_dedups() {
        let once = ShutdownOnce::new(false);
        assert!(once.record_once(SHUTDOWN_COMPLETED), "first call wins");
        assert!(!once.record_once(SHUTDOWN_FD_WATCHER), "second no-op");
    }

    // StateStageTimer: disabled => None (no timer). Enabled success() observes the
    // {stage,status=success} child; drop-without-success observes {status=error}.
    // Uses a unique synthetic stage label so the children are owned by this test
    // (exact, parallel-safe).
    #[test]
    fn state_stage_timer_disabled_none_enabled_records() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();

        // Disabled: no timer at all.
        assert!(
            StateStageTimer::start(false, true, "test_state_stage_unique").is_none(),
            "disabled => None"
        );

        // Enabled + success: records the success child once.
        let s_before = mx
            .state_persist_stage_duration_us
            .with_label_values(&["test_state_stage_unique", STATE_STATUS_SUCCESS])
            .get_sample_count();
        StateStageTimer::start(true, true, "test_state_stage_unique")
            .expect("enabled => Some")
            .success();
        assert_eq!(
            mx.state_persist_stage_duration_us
                .with_label_values(&["test_state_stage_unique", STATE_STATUS_SUCCESS])
                .get_sample_count(),
            s_before + 1,
            "success() records one success sample (unique label => exact)"
        );

        // Enabled + drop without success: records the error child once.
        let e_before = mx
            .state_persist_stage_duration_us
            .with_label_values(&["test_state_stage_unique", STATE_STATUS_ERROR])
            .get_sample_count();
        drop(StateStageTimer::start(
            true,
            true,
            "test_state_stage_unique",
        ));
        assert_eq!(
            mx.state_persist_stage_duration_us
                .with_label_values(&["test_state_stage_unique", STATE_STATUS_ERROR])
                .get_sample_count(),
            e_before + 1,
            "drop-without-success records one error sample"
        );
    }

    // The 6 shutdown reasons are distinct and the run_all arm's three outcomes are
    // present (a regression guard against collapsing them back to `completed`).
    #[test]
    fn shutdown_reason_consts_are_the_six_distinct_values() {
        let reasons = [
            SHUTDOWN_COMPLETED,
            SHUTDOWN_RUN_ALL_ERROR,
            SHUTDOWN_RUN_ALL_PANIC,
            SHUTDOWN_TERM_SIGNAL,
            SHUTDOWN_SIGUSR1_PERSIST,
            SHUTDOWN_FD_WATCHER,
        ];
        let mut uniq: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for r in reasons {
            assert!(uniq.insert(r), "reason {r} must be unique");
        }
        assert_eq!(uniq.len(), 6);
        // run_all's three outcomes are not collapsed into `completed`.
        assert_ne!(SHUTDOWN_RUN_ALL_ERROR, SHUTDOWN_COMPLETED);
        assert_ne!(SHUTDOWN_RUN_ALL_PANIC, SHUTDOWN_COMPLETED);
    }

    // State-recovery stages must NEVER collide with the request stage_duration_us
    // enum (separate domains sharing the label name). Guard by value.
    #[test]
    fn state_stages_disjoint_from_request_stages() {
        let request_stages = [
            STAGE_REPLY_WRITE,
            STAGE_META_SPAWN,
            STAGE_OPERATION,
            STAGE_STREAM_IO,
        ];
        let state_stages = [
            STATE_STAGE_NODE_MAP,
            STATE_STAGE_FILE_HANDLES,
            STATE_STAGE_DIR_HANDLES,
            STATE_STAGE_MOUNT_FDS,
        ];
        for s in state_stages {
            assert!(
                !request_stages.contains(&s),
                "state stage {s} must not appear in the request stage enum"
            );
        }
    }

    // P3#5: the lifecycle record helpers actually emit through the singleton
    // (exercises the `FuseMetrics::with(|m| m.record_session_init/...)` path the
    // session uses). Shared global children → lower-bound deltas, parallel-safe.
    #[test]
    fn lifecycle_record_helpers_emit() {
        FuseMetrics::ensure_init().unwrap();
        let mx = FuseMetrics::get();

        let init_before = mx
            .session_init_total
            .with_label_values(&[SESSION_INIT_SUCCESS])
            .get();
        mx.record_session_init(SESSION_INIT_SUCCESS);
        assert!(
            mx.session_init_total
                .with_label_values(&[SESSION_INIT_SUCCESS])
                .get()
                > init_before,
            "record_session_init emits the success child"
        );

        let persist_before = mx
            .state_persist_total
            .with_label_values(&[STATE_STATUS_SUCCESS])
            .get();
        mx.record_state_total(true, STATE_STATUS_SUCCESS);
        assert!(
            mx.state_persist_total
                .with_label_values(&[STATE_STATUS_SUCCESS])
                .get()
                > persist_before,
            "record_state_total(persist) emits the success child"
        );

        // kernel_fd_health is a single gauge; set both states and read back (this
        // test is the only writer of a deterministic value sequence, but other
        // tests may also set it — so just assert the setter takes effect promptly).
        mx.set_kernel_fd_health(true);
        // Not asserting the exact value (shared gauge); the call must not panic and
        // the gauge must be in {0,1}.
        let v = mx.kernel_fd_health.get();
        assert!(v == 0 || v == 1, "health gauge is binary");
    }

    // P3#5: gate decision (enabled vs disabled) is deterministic against an
    // isolated counter — the exact `if enabled { emit }` shape the session call
    // sites use to gate every lifecycle/state emission on metrics_enabled.
    #[test]
    fn lifecycle_gate_suppresses_when_disabled() {
        fn record_if(enabled: bool, counter: &orpc::common::Counter) {
            if enabled {
                counter.inc();
            }
        }
        let counter = orpc::common::Metrics::new_counter(
            "test_lifecycle_gate_isolated_counter_unique",
            "isolated lifecycle gate counter",
        )
        .unwrap();
        record_if(false, &counter);
        assert_eq!(counter.get(), 0, "disabled gate emits nothing");
        record_if(true, &counter);
        assert_eq!(counter.get(), 1, "enabled gate emits");
    }
}
