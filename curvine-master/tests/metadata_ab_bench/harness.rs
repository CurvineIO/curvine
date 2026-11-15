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
        Err(_) => 1000,
    }
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
}

impl Env {
    pub fn new(op: &str, layout: Layout, threads: usize, ops: u64) -> Self {
        Self {
            root: format!("/bench/{op}-{}-{threads}", layout.tag()),
            layout,
            threads,
            ops,
        }
    }

    pub fn root(&self) -> &str {
        &self.root
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
    pub fn line(&self, ver: &str, op: &str, layout: Layout, threads: usize) -> String {
        format!(
            "RESULT ver={ver} op={op} layout={} threads={} ops={} ok={} err={} qps={:.0} \
             samples={} p50_ns={} p99_ns={} max_ns={}{}",
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
}

/// Runs `body` `env.ops()` times on each thread, timing every call.
///
/// Failed calls are counted but their latency is excluded from the
/// percentiles: mixing failure paths into the success distribution would
/// misreport the operation being measured. The first error text is reported
/// so a broken fixture cannot pass silently.
pub fn run(
    fs: &Arc<MasterFilesystem>,
    env: &Arc<Env>,
    args: fn(&Arc<MasterFilesystem>, &Env, usize, u64) -> Arg,
    body: fn(&MasterFilesystem, &mut Arg) -> FsResult<()>,
) -> Report {
    let barrier = Arc::new(Barrier::new(env.threads + 1));
    let mut handles = Vec::with_capacity(env.threads);
    for thread in 0..env.threads {
        let fs = fs.clone();
        let env = env.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut out = ThreadResult {
                ok: 0,
                err: 0,
                lat: Vec::with_capacity(env.ops as usize),
                first_err: None,
                elapsed_ns: 0,
            };
            // Build every per-call argument (path formatting included) before
            // the barrier, so the timed region holds one API call only.
            let mut prepared: Vec<Arg> = (0..env.ops)
                .map(|seq| args(&fs, &env, thread, seq))
                .collect();
            barrier.wait();
            let started = Instant::now();
            for seq in 0..env.ops {
                let arg = &mut prepared[seq as usize];
                let t0 = Instant::now();
                let result = body(&fs, arg);
                let ns = t0.elapsed().as_nanos() as u64;
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
            }
            out.elapsed_ns = started.elapsed().as_nanos() as u64;
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
    };
    for handle in handles {
        let part = handle.join().expect("benchmark worker panicked");
        merge_into(&mut merged, part);
    }
    // Slowest thread's own span, not the wall clock including join overhead.
    let elapsed_s = merged.elapsed_ns as f64 / 1e9;
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
    if acc.first_err.is_none() {
        acc.first_err = part.first_err;
    }
}
