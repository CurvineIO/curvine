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

//! Benchmark runner: fixed operation count, per-operation timing, merged
//! percentiles. Decoupled from the operations themselves so that both the
//! baseline (`bff4e7dc5`) and the candidate branch use byte-identical
//! constants and algorithms; every printed column is therefore directly
//! comparable.
//!
//! Timing accuracy: one `Instant::now()` + `elapsed()` pair costs roughly
//! 25-50 ns on x86-64, so it is below 1% of the measured operations (all of
//! which take microseconds). The timed region contains one `MasterFilesystem`
//! call and nothing else: every per-call argument, including its path strings,
//! is built beforehand (see `ops::Case::args`). The only allocations that can
//! still occur inside it are the ones the API itself performs.

use crate::ops::Arg;
use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;
use std::sync::{Arc, Barrier};
use std::time::Instant;

/// Operations per thread. Fixed count (not fixed duration) so both builds do
/// exactly the same work and the sample count is known in advance.
///
/// `BENCH_OPS` exists only for small-scale smoke runs; a real A/B run must not
/// set it, otherwise the two builds would be compared on a reduced workload.
pub fn ops_per_thread() -> u64 {
    match std::env::var("BENCH_OPS") {
        Ok(v) => v.parse().expect("BENCH_OPS must be a positive integer"),
        Err(_) => 10_000_000,
    }
}

/// Per-cell time budget in milliseconds; a thread runs until this budget is
/// exhausted or `BENCH_OPS` (now only an upper bound) is reached.
///
/// NOTE (semantics change): `BENCH_OPS` used to be the exact iteration count.
/// Data produced before this change must not be compared with data produced
/// after it, because the sample count is no longer fixed.
/// Fixture size, deliberately **separate** from the timed iteration budget.
///
/// `ops_per_thread()` now defaults to 10_000_000 because `BENCH_OPS` became an
/// upper bound rather than an iteration count. Fixtures that pre-create one
/// file per `ops()` therefore tried to build ten million files before the timed
/// region even started, which is what made `delete` / `rename_same` look like
/// they were blocking in the kernel. Fixtures must scale on their own knob.
pub fn fixture_ops() -> u64 {
    match std::env::var("BENCH_FIXTURE_OPS") {
        Ok(v) => v.parse().expect("BENCH_FIXTURE_OPS must be a positive integer"),
        Err(_) => 1000,
    }
}

pub fn budget_millis() -> u64 {
    match std::env::var("BENCH_MILLIS") {
        Ok(v) => v.parse().expect("BENCH_MILLIS must be a positive integer"),
        Err(_) => 1000,
    }
}

/// The cap, printed on every RESULT line so a run is self-describing.
pub fn ops_cap() -> u64 {
    ops_per_thread()
}

/// Namespace layout under test. `Same` puts every thread's objects in one
/// parent directory (contended parent); `Multi` gives each thread its own
/// parent (disjoint parents).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    Same,
    Multi,
}

impl Layout {
    pub const ALL: [Layout; 2] = [Layout::Same, Layout::Multi];

    pub fn tag(self) -> &'static str {
        match self {
            Layout::Same => "same",
            Layout::Multi => "multi",
        }
    }
}

/// Fixed per-scenario layout: root directory, thread count, operation count.
///
/// Paths are derived from `(thread, seq)` only, so the preparation phase and
/// the timed phase agree without sharing mutable state.
pub struct Env {
    root: String,
    layout: Layout,
    threads: usize,
    ops: u64,
    /// Fixture scale; independent of the timed budget (see `fixture_ops()`).
    fixture_ops: u64,
}

impl Env {
    pub fn new(op: &str, layout: Layout, threads: usize, ops: u64) -> Self {
        Self {
            root: format!("/bench/{op}-{}-{threads}", layout.tag()),
            layout,
            threads,
            ops,
            fixture_ops: fixture_ops(),
        }
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    /// Fixture scale: how many objects `prepare` may create. Never `ops()`.
    pub fn fixture_ops(&self) -> u64 {
        self.fixture_ops
    }

    pub fn layout(&self) -> Layout {
        self.layout
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn ops(&self) -> u64 {
        self.ops
    }

    /// Working directory of one thread: shared under `Same`, private under
    /// `Multi`.
    pub fn dir(&self, thread: usize) -> String {
        match self.layout {
            Layout::Same => self.root.clone(),
            Layout::Multi => format!("{}/t{thread}", self.root),
        }
    }

    /// Per-operation object path, unique within the scenario. The thread id
    /// is part of the name because `Same` shares one directory between all
    /// threads.
    pub fn path(&self, thread: usize, seq: u64) -> String {
        format!("{}/f{thread}-{seq}", self.dir(thread))
    }
}

/// Aggregated result of one scenario run.
pub struct Report {
    pub ok: u64,
    pub err: u64,
    /// Successful-operation latencies in nanoseconds, sorted ascending.
    pub lat: Vec<u64>,
    pub elapsed_s: f64,
    pub first_err: Option<String>,
}

impl Report {
    pub fn qps(&self) -> f64 {
        if self.elapsed_s <= 0.0 {
            return 0.0;
        }
        self.ok as f64 / self.elapsed_s
    }

    /// Nearest-rank percentile over every recorded sample. Threads are merged
    /// as raw samples, so a high-throughput thread contributes proportionally
    /// more samples and cannot be outvoted by a slow one.
    pub fn percentile(&self, p: usize) -> u64 {
        if self.lat.is_empty() {
            return 0;
        }
        let idx = (self.lat.len() * p).div_ceil(100).saturating_sub(1);
        self.lat[idx.min(self.lat.len() - 1)]
    }

    /// One machine-parsable line. `ver` identifies the build.
    /// Concurrent wall span (max(end) - min(start) across threads) in ns.
    /// `elapsed_s` is already that span, so this only rescales it; it is the QPS
    /// denominator and the value a test asserts is at least one second.
    pub fn span_ns(&self) -> u64 {
        (self.elapsed_s * 1e9) as u64
    }

    /// Mean latency of the successful calls.
    pub fn mean_ns(&self) -> u64 {
        if self.lat.is_empty() {
            0
        } else {
            self.lat.iter().sum::<u64>() / self.lat.len() as u64
        }
    }

    pub fn line(&self, ver: &str, op: &str, layout: Layout, threads: usize) -> String {
        format!(
            "RESULT ver={ver} op={op} layout={} threads={} ops={} ok={} err={} qps={:.0} \
             samples={} p50_ns={} p99_ns={} max_ns={} elapsed_ns={} mean_ns={} bench_millis={} ops_cap={}{}{}",
            layout.tag(),
            threads,
            self.ok + self.err,
            self.ok,
            self.err,
            self.qps(),
            self.lat.len(),
            self.percentile(50),
            self.percentile(99),
            self.lat.last().copied().unwrap_or(0),
            self.span_ns(),
            self.mean_ns(),
            budget_millis(),
            ops_cap(),
            // Sampling is hard-capped (see background::LAT_CAPACITY): past the
            // cap the cell keeps counting but stops recording, so p50/p99 come
            // from a leading window. Flag it rather than let a truncated p99
            // read as exact.
            if self.ok > self.lat.len() as u64 { " lat_capped=1" } else { "" },
            match &self.first_err {
                Some(e) => format!(" first_err=\"{e}\""),
                None => String::new(),
            }
        )
    }
}

pub(crate) struct ThreadResult {
    pub(crate) ok: u64,
    pub(crate) err: u64,
    pub(crate) lat: Vec<u64>,
    pub(crate) first_err: Option<String>,
    /// Wall time this thread spent from barrier release to its last operation.
    /// Groups with different work volumes must not share one elapsed value.
    pub(crate) elapsed_ns: u64,
    /// This thread's first/last timed-call boundary, relative to a shared epoch
    /// taken right before the barrier. Used to derive the true concurrent span
    /// (`max(end) - min(start)`) instead of the slowest thread's own elapsed.
    pub(crate) start_ns: Option<u64>,
    pub(crate) end_ns: u64,
}

/// Runs `body` `env.ops()` times on each thread, timing every call.
///
/// Failed calls are counted but their latency is excluded from the
/// percentiles: mixing failure paths into the success distribution would
/// misreport the operation being measured. The first error text is reported
/// so a broken fixture cannot pass silently.
/// A consumptive operation (delete, rename) leaves the fixture object in the
/// state the next iteration no longer expects. `recycle` restores it **outside
/// the timed call**, so the measured latency stays that of the single body
/// call and QPS keeps meaning "pure delete / pure rename throughput".
///
/// Restoring inside `body` would fold a recreate into every sample, which is
/// exactly what the A/B comparison must not do.
pub type Recycle = fn(&Arc<MasterFilesystem>, &Env, usize, u64) -> FsResult<()>;

pub fn run(
    fs: &Arc<MasterFilesystem>,
    env: &Arc<Env>,
    args: fn(&Arc<MasterFilesystem>, &Env, usize, u64) -> Arg,
    body: fn(&MasterFilesystem, &mut Arg) -> FsResult<()>,
    recycle: Option<Recycle>,
) -> Report {
    let barrier = Arc::new(Barrier::new(env.threads + 1));
    let epoch = Instant::now();
    let mut handles = Vec::with_capacity(env.threads);
    for thread in 0..env.threads {
        let fs = fs.clone();
        let env = env.clone();
        let barrier = barrier.clone();
        // `Instant` is `Copy`, so each spawned closure captures its own copy of
        // the shared epoch; no rebinding is needed.
        handles.push(std::thread::spawn(move || {
            let mut out = ThreadResult {
                ok: 0,
                err: 0,
                lat: Vec::with_capacity(1024.min(env.ops) as usize),
                first_err: None,
                elapsed_ns: 0,
                start_ns: None,
                end_ns: 0,
            };
            barrier.wait();
            let started = Instant::now();
            let budget = std::time::Duration::from_millis(budget_millis());
            out.start_ns = Some(started.duration_since(epoch).as_nanos() as u64);
            // Arguments are built in chunks so that construction sits outside
            // the timed span of a single call, while memory stays bounded by the
            // chunk size.
            //
            // Be precise about what this does and does not buy: chunking moves
            // construction out of the per-call latency, but every argument is
            // still constructed exactly once, so the construction cost remains
            // inside the cell span. For `close_file_by_id` that means the span
            // contains one id resolution per call (`close_id_arg` performs a
            // real file_status), so its QPS is the end-to-end throughput of
            // "resolve id + complete", not of complete alone, and must not be
            // compared with earlier data produced by the pre-constructed build.
            // p50/p99/mean are unaffected: they time the body only.
            //
            // To restore complete-only measurement later, resolve the id during
            // fixture preparation instead of here.
            const CHUNK: u64 = 1024;
            let mut prepared: Vec<Arg> = Vec::with_capacity(CHUNK as usize);
            let mut seq = 0u64;
            'outer: while seq < env.ops {
                let to = (seq + CHUNK).min(env.ops);
                prepared.clear();
                prepared.extend((seq..to).map(|s| args(&fs, &env, thread, s)));
                for (idx, arg) in prepared.iter_mut().enumerate() {
                    let this_seq = seq + idx as u64;
                    let t0 = Instant::now();
                    let result = body(&fs, arg);
                    let ns = t0.elapsed().as_nanos() as u64;
                    // Outside the timed call on purpose: restoring the fixture
                    // must not be charged to the measured operation.
                    if let Some(recycle) = recycle {
                        let _ = recycle(&fs, &env, thread, this_seq);
                    }
                    match result {
                        Ok(()) => {
                            out.ok += 1;
                            out.lat.push(ns);
                        }
                        Err(e) => {
                            out.err += 1;
                            if out.first_err.is_none() {
                                out.first_err = Some(e.to_string());
                            }
                        }
                    }
                    // Checked every iteration: one Instant::now() is negligible
                    // next to a metadata op, and a coarser check overshoots
                    // badly on slow cases (list_status is ~200us per call).
                    if started.elapsed() >= budget {
                        break 'outer;
                    }
                }
                seq = to;
            }

            out.elapsed_ns = started.elapsed().as_nanos() as u64;
            out.end_ns = Instant::now().duration_since(epoch).as_nanos() as u64;
            out
        }));
    }
    barrier.wait();
    let mut merged = ThreadResult {
        ok: 0,
        err: 0,
        lat: Vec::new(),
        first_err: None,
        elapsed_ns: 0,
        start_ns: None,
        end_ns: 0,
    };
    for handle in handles {
        let part = handle.join().expect("benchmark worker panicked");
        merge_into(&mut merged, part);
    }
    // Slowest thread's own span, not the wall clock including join overhead.
    // Prefer the true concurrent span when it is available; it covers every
    // thread's first-to-last call instead of only the slowest thread's window.
    let span_ns = merged
        .start_ns
        .map(|s| merged.end_ns.saturating_sub(s))
        .unwrap_or(0);
    let denom_ns = if span_ns > 0 {
        span_ns
    } else {
        merged.elapsed_ns
    };
    let elapsed_s = denom_ns as f64 / 1e9;
    if std::env::var("DIAG_DENOM").is_ok() {
        println!(
            "DIAGDENOM threads={} ok={} max_thread_elapsed_ns={} span_ns={} start_min_ns={} end_max_ns={} used={}",
            env.threads,
            merged.ok,
            merged.elapsed_ns,
            span_ns,
            merged.start_ns.unwrap_or(0),
            merged.end_ns,
            if span_ns > 0 { "span" } else { "max_elapsed" }
        );
    }
    merged.lat.sort_unstable();
    Report {
        ok: merged.ok,
        err: merged.err,
        lat: merged.lat,
        elapsed_s,
        first_err: merged.first_err,
    }
}

/// Folds one thread's samples into the accumulator. Elapsed is the maximum
/// thread span, so groups with different work volumes do not share a clock.
pub(crate) fn merge_into(acc: &mut ThreadResult, part: ThreadResult) {
    acc.ok += part.ok;
    acc.err += part.err;
    acc.lat.extend(part.lat);
    acc.elapsed_ns = acc.elapsed_ns.max(part.elapsed_ns);
    // Concurrent span across threads: the earliest first call to the latest
    // last call. With per-thread `Instant::now()` after the barrier, thread
    // start times differ by up to a few ms; dividing the summed `ok` by the
    // slowest thread's own elapsed in that case understates throughput.
    acc.start_ns = match (acc.start_ns, part.start_ns) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (None, x) | (x, None) => x,
    };
    acc.end_ns = acc.end_ns.max(part.end_ns);
    if acc.first_err.is_none() {
        acc.first_err = part.first_err;
    }
}
