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

use once_cell::sync::OnceCell;

use orpc::common::{Gauge, Histogram, Metrics as m};
use orpc::CommonResult;

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
        })
    }
}

/// Monotonic time source for durations.
///
/// `orpc::common::LocalTime::nanos()` is wall-clock (`SystemTime::now()`) and
/// must **not** be used for latency: an NTP step or suspend/resume can produce
/// skewed or negative deltas. All FUSE duration metrics use `std::time::Instant`
/// instead, accessed through this single helper so the choice is centralized.
// Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
#[allow(dead_code)]
#[inline]
pub(crate) fn mono_now() -> Instant {
    Instant::now()
}

/// The kind of a FUSE request, used as the `kind` label.
///
/// There is deliberately no `NoReply` variant: `Forget` / `BatchForget` are
/// `Metadata` and are distinguished by a separate `reply_type` label where it
/// matters (added in a later phase).
// Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FuseReqKind {
    Metadata,
    Stream,
}

#[allow(dead_code)] // Phase 0 primitive; call sites land in Phase 1.
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
// Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct FuseReqLabels {
    pub(crate) opcode: &'static str,
    pub(crate) kind: FuseReqKind,
    pub(crate) start: Instant,
    pub(crate) request_bytes: u32,
}

#[allow(dead_code)] // Phase 0 primitive; call sites land in Phase 1.
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

/// RAII guard for an in-flight gauge: increments on construction, decrements
/// exactly once on drop.
///
/// It is `Send` and movable (so it can travel into a spawned task or onto a
/// reply task), and deliberately **not** `Copy` — a `Copy` guard could
/// double-decrement. The guard holds its own `Gauge` handle so a single type
/// can back different scopes (`active_requests`, `stream_io_inflight`,
/// `meta_task_inflight`) just by being constructed from different gauges.
// Phase 0 enabling primitive: defined here, wired to call sites in Phase 1.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ActiveGuard {
    gauge: Gauge,
}

#[allow(dead_code)] // Phase 0 primitive; call sites land in Phase 1.
impl ActiveGuard {
    pub(crate) fn new(gauge: Gauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.gauge.dec();
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
}
