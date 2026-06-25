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

use orpc::common::{CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Metrics as m};
use orpc::CommonResult;

use crate::fuse_error::errno_label;

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

// `reply_type` label values (`requests_total`).
pub(crate) const REPLY_TYPE_REPLIED: &str = "replied";
pub(crate) const REPLY_TYPE_NO_REPLY: &str = "no_reply";

// `stage` label values. `reply_write` ships in 1a-2; `meta_spawn` in 1b
// (rt.spawn submission -> first poll scheduling delay).
pub(crate) const STAGE_REPLY_WRITE: &str = "reply_write";
pub(crate) const STAGE_META_SPAWN: &str = "meta_spawn";

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

    fn new() -> CommonResult<Self> {
        Ok(Self {
            inode_num: m::new_gauge("inode_num", "FUSE inode count in dcache")?,
            file_handle_num: m::new_gauge("file_handle_num", "FUSE open file handle count")?,
            dir_handle_num: m::new_gauge("dir_handle_num", "FUSE open directory handle count")?,

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
// Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct HistogramTimer {
    start: Instant,
    hist: Histogram,
}

#[allow(dead_code)] // Phase 0 primitive; call sites land in Phase 1.
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
        assert_eq!(
            mx.stage_duration_us
                .with_label_values(&[STAGE_REPLY_WRITE, "metadata", "success"])
                .get_sample_count(),
            stage_before + 1
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
}
