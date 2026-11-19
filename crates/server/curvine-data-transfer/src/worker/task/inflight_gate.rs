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

//! In-flight-byte admission control for worker load tasks.
//!
//! The legacy worker limit is a task count (a `Semaphore` over
//! `worker_max_concurrent_tasks`). That is a poor proxy for what actually
//! saturates a worker: every running load task can hold
//! `load_task.parallel_streams * opendal.read_concurrent * opendal.read_chunk_size`
//! bytes of UFS read buffers, and the memory cost of 100 concurrent small files
//! is very different from 100 concurrent large ones.
//!
//! [`InflightBytesGate`] replaces the count with a byte budget: a task is
//! admitted once its declared in-flight cost fits, so the worker holds at most
//! `target_bytes` of read buffers regardless of file fan-out. The budget is
//! either a fixed configured number or produced by [`ProbePolicy`], which ramps
//! the budget up while goodput improves and settles one step below the level
//! where it stopped improving.

use curvine_config::{JobConf, LoadTransferDefaults};
use curvine_model::LoadTaskInfo;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

static INFLIGHT_BYTES: LazyLock<Option<curvine_metrics::Gauge>> = LazyLock::new(|| {
    curvine_metrics::Metrics::new_gauge(
        "worker_load_inflight_bytes",
        "Bytes of in-flight UFS read buffers reserved by running load tasks",
    )
    .ok()
});

static INFLIGHT_TARGET: LazyLock<Option<curvine_metrics::Gauge>> = LazyLock::new(|| {
    curvine_metrics::Metrics::new_gauge(
        "worker_load_inflight_bytes_target",
        "Current in-flight byte budget of the worker load gate",
    )
    .ok()
});

static INFLIGHT_WAITS: LazyLock<Option<curvine_metrics::Counter>> = LazyLock::new(|| {
    curvine_metrics::Metrics::new_counter(
        "worker_load_inflight_waits_total",
        "Admissions that had to wait for the in-flight byte budget",
    )
    .ok()
});

/// Policy for probing the in-flight byte budget.
///
/// Every `window` the controller measures goodput (completed bytes per second,
/// fed by [`InflightBytesGate::record_completed`]) and only accepts the sample
/// when the budget was actually the binding constraint (the window's in-flight
/// peak reached 90% of the target and at least one whole task fit in the
/// completed bytes). A worker that is app-limited must not read as "the budget
/// is too large".
///
/// Startup: while goodput improves by at least `min_gain_percent`, grow by
/// `growth_percent` (floored at one task's footprint); after `settle_windows`
/// flat windows return to the last budget that produced a gain.
///
/// Steady state: every `probe_interval_rounds` windows (or immediately when
/// goodput falls `drop_percent` below the recent baseline, rate limited by
/// `min_probe_interval_rounds`) probe down 25%, then up 25%, and keep the
/// direction that wins by `probe_gain_percent`. Down-probing is what pulls a
/// budget that is too large back during a plateau.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbePolicy {
    pub min_bytes: i64,
    pub max_bytes: i64,
    pub window: Duration,
    pub growth_percent: u32,
    pub min_gain_percent: u32,
    pub settle_windows: u32,
    pub warmup_windows: u32,
    pub probe_interval_rounds: u32,
    pub min_probe_interval_rounds: u32,
    pub baseline_rounds: u32,
    pub drop_percent: u32,
    pub probe_gain_percent: u32,
}

impl ProbePolicy {
    /// Grow by 50%, but never by less than the largest permit a task has
    /// reserved. A pure 50% step can land inside the same task slot (16 MiB ->
    /// 24 MiB still admits one 16 MiB task) and look flat; growing by a whole
    /// task slot always changes the admitted task count.
    pub const DEFAULT_GROWTH_PERCENT: u32 = 50;
    /// Require a real gain before growing again. 32 -> 64 MiB at the knee of the
    /// dummy-server sweep bought +11% for 2x the memory; 15% rejects that.
    pub const DEFAULT_MIN_GAIN_PERCENT: u32 = 15;
    pub const DEFAULT_SETTLE_WINDOWS: u32 = 2;
    pub const DEFAULT_WARMUP_WINDOWS: u32 = 1;
    pub const DEFAULT_PROBE_INTERVAL_ROUNDS: u32 = 10;
    pub const DEFAULT_MIN_PROBE_INTERVAL_ROUNDS: u32 = 5;
    pub const DEFAULT_BASELINE_ROUNDS: u32 = 10;
    pub const DEFAULT_DROP_PERCENT: u32 = 20;
    pub const DEFAULT_PROBE_GAIN_PERCENT: u32 = 5;

    pub fn new(min_bytes: i64, max_bytes: i64, window: Duration) -> Self {
        let min_bytes = min_bytes.max(1);
        Self {
            min_bytes,
            max_bytes: max_bytes.max(min_bytes),
            window,
            growth_percent: Self::DEFAULT_GROWTH_PERCENT,
            min_gain_percent: Self::DEFAULT_MIN_GAIN_PERCENT,
            settle_windows: Self::DEFAULT_SETTLE_WINDOWS,
            warmup_windows: Self::DEFAULT_WARMUP_WINDOWS,
            probe_interval_rounds: Self::DEFAULT_PROBE_INTERVAL_ROUNDS,
            min_probe_interval_rounds: Self::DEFAULT_MIN_PROBE_INTERVAL_ROUNDS,
            baseline_rounds: Self::DEFAULT_BASELINE_ROUNDS,
            drop_percent: Self::DEFAULT_DROP_PERCENT,
            probe_gain_percent: Self::DEFAULT_PROBE_GAIN_PERCENT,
        }
    }
}

impl Default for ProbePolicy {
    fn default() -> Self {
        Self::new(
            256 * 1024 * 1024,
            8 * 1024 * 1024 * 1024,
            Duration::from_secs(3),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateSnapshot {
    pub target_bytes: i64,
    pub used_bytes: i64,
    pub peak_used_bytes: i64,
    pub active_permits: usize,
    pub waiters: usize,
    pub waits_total: u64,
    pub wait_ms_total: u64,
    pub completed_bytes: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Startup,
    Steady,
    ProbeDown,
    ProbeUp,
}

struct ProbeState {
    last_completed: i64,
    warmup_left: u32,
    phase: Phase,
    /// Startup: goodput of the previous accepted window, and the budget that
    /// produced the last gain (the fallback when the ramp flattens).
    prev_goodput: u64,
    last_good_target: i64,
    flat_rounds: u32,
    /// Steady state: rounds since the last probe cycle, and the goodput of the
    /// last `baseline_rounds` valid windows.
    rounds_since_probe: u32,
    baseline: VecDeque<u64>,
    /// Probe cycle: the budget and goodput the current down/up test is judged
    /// against.
    probe_base_w: i64,
    probe_base_g: u64,
}

impl ProbeState {
    fn new(policy: &ProbePolicy) -> Self {
        Self {
            last_completed: 0,
            warmup_left: policy.warmup_windows,
            phase: Phase::Startup,
            prev_goodput: 0,
            last_good_target: policy.min_bytes,
            flat_rounds: 0,
            rounds_since_probe: 0,
            baseline: VecDeque::with_capacity(policy.baseline_rounds.max(1) as usize),
            probe_base_w: policy.min_bytes,
            probe_base_g: 0,
        }
    }
}

struct GateInner {
    target: AtomicI64,
    used: AtomicI64,
    peak_used: AtomicI64,
    /// Peak in-flight bytes since the last probe tick: proves the budget was
    /// the binding constraint during the window.
    window_peak: AtomicI64,
    /// Largest reservation seen, used as the minimum probe step so a growth
    /// step always fits one more task.
    max_permit: AtomicI64,
    active: AtomicUsize,
    waiters: AtomicUsize,
    waits_total: AtomicU64,
    wait_ns_total: AtomicU64,
    completed: AtomicI64,
    notify: Notify,
    probe: Option<ProbePolicy>,
    probe_state: parking_lot::Mutex<ProbeState>,
}

/// A byte budget that admits work only while the reserved in-flight bytes fit.
#[derive(Clone)]
pub struct InflightBytesGate {
    inner: Arc<GateInner>,
}

impl InflightBytesGate {
    pub fn new(target_bytes: i64) -> Self {
        let target = target_bytes.max(1);
        if let Some(gauge) = INFLIGHT_TARGET.as_ref() {
            gauge.set(target);
        }
        Self {
            inner: Arc::new(GateInner {
                target: AtomicI64::new(target),
                used: AtomicI64::new(0),
                peak_used: AtomicI64::new(0),
                window_peak: AtomicI64::new(0),
                max_permit: AtomicI64::new(0),
                active: AtomicUsize::new(0),
                waiters: AtomicUsize::new(0),
                waits_total: AtomicU64::new(0),
                wait_ns_total: AtomicU64::new(0),
                completed: AtomicI64::new(0),
                notify: Notify::new(),
                probe: None,
                probe_state: parking_lot::Mutex::new(ProbeState::new(&ProbePolicy::default())),
            }),
        }
    }

    /// A gate whose budget starts at `policy.min_bytes` and is ramped by
    /// [`Self::probe_tick`].
    pub fn with_probe(policy: ProbePolicy) -> Self {
        let gate = Self::new(policy.min_bytes);
        let mut gate = gate;
        let raw = Arc::get_mut(&mut gate.inner).expect("gate was just constructed");
        raw.probe = Some(policy);
        *raw.probe_state.get_mut() = ProbeState::new(&policy);
        gate
    }

    pub fn target_bytes(&self) -> i64 {
        self.inner.target.load(Ordering::Acquire)
    }

    pub fn used_bytes(&self) -> i64 {
        self.inner.used.load(Ordering::Acquire)
    }

    pub fn set_target(&self, bytes: i64) {
        let target = bytes.max(1);
        self.inner.target.store(target, Ordering::Release);
        if let Some(gauge) = INFLIGHT_TARGET.as_ref() {
            gauge.set(target);
        }
        // A raised budget may unblock waiters; a lowered one never adds work.
        self.inner.notify.notify_waiters();
    }

    /// Reserve `bytes` of the in-flight budget, waiting until they fit.
    ///
    /// A request larger than the whole budget is clamped to the budget: a single
    /// task must always be able to run, otherwise lowering the budget below one
    /// task's cost deadlocks every load.
    pub async fn acquire(&self, bytes: i64) -> InflightPermit {
        let requested = bytes.max(1);
        let started = Instant::now();
        let mut waited = false;
        loop {
            let target = self.inner.target.load(Ordering::Acquire).max(1);
            let size = requested.min(target);
            if self.inner.try_reserve(size) {
                if waited {
                    let wait_ns = started.elapsed().as_nanos() as u64;
                    self.inner
                        .wait_ns_total
                        .fetch_add(wait_ns, Ordering::Relaxed);
                }
                return InflightPermit {
                    inner: self.inner.clone(),
                    size,
                };
            }

            if !waited {
                waited = true;
                self.inner.waits_total.fetch_add(1, Ordering::Relaxed);
                if let Some(counter) = INFLIGHT_WAITS.as_ref() {
                    counter.inc();
                }
            }
            self.inner.waiters.fetch_add(1, Ordering::AcqRel);
            // The timeout is a safety net: `notify_waiters` only wakes futures
            // that already registered, and a missed wakeup must not hang a task.
            tokio::select! {
                _ = self.inner.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            self.inner.waiters.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Feed goodput into the probe. `bytes` is the number of bytes a task
    /// finished copying; callers must report each byte once.
    pub fn record_completed(&self, bytes: i64) {
        if bytes > 0 {
            self.inner.completed.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Advance the probe by one window of `elapsed`.
    ///
    /// Returns the new budget when it changed. Idle windows and app-limited
    /// windows (the budget was not what limited them) are dropped: they carry
    /// no information about the right budget.
    pub fn probe_tick(&self, elapsed: Duration) -> Option<i64> {
        let policy = self.inner.probe?;
        let completed = self.inner.completed.load(Ordering::Relaxed);
        let window_peak = self.inner.window_peak.swap(0, Ordering::Relaxed);
        let mut state = self.inner.probe_state.lock();

        let delta = (completed - state.last_completed).max(0);
        state.last_completed = completed;

        if state.warmup_left > 0 {
            state.warmup_left -= 1;
            return None;
        }
        if delta == 0 {
            return None;
        }
        let target = self.target_bytes();
        let max_permit = self.inner.max_permit.load(Ordering::Relaxed).max(1);
        // App-limited sample: the queue was empty (upstream slow, disk slow, no
        // tasks) so the budget was not the binding constraint. A budget smaller
        // than the next task still counts as binding: nothing else fits.
        let reached_budget = window_peak * 10 >= target * 9;
        let next_task_cannot_fit = window_peak.saturating_add(max_permit) > target;
        if !reached_budget && !next_task_cannot_fit {
            return None;
        }
        // Fewer bytes than one task: too short to resolve a throughput change.
        if delta < max_permit {
            return None;
        }

        let goodput = (delta as f64 / elapsed.as_secs_f64().max(0.001)) as u64;
        let baseline_rounds = policy.baseline_rounds.max(1) as usize;
        if state.baseline.len() >= baseline_rounds {
            state.baseline.pop_front();
        }
        state.baseline.push_back(goodput);

        match state.phase {
            Phase::Startup => self.probe_startup(&policy, &mut state, target, goodput, max_permit),
            Phase::Steady => self.probe_steady(&policy, &mut state, target, goodput),
            Phase::ProbeDown => self.probe_down(&policy, &mut state, target, goodput),
            Phase::ProbeUp => self.probe_up(&policy, &mut state, target, goodput),
        }
    }

    fn grow_target(&self, policy: &ProbePolicy, target: i64, max_permit: i64) -> i64 {
        let ratio_step = target as i128 * policy.growth_percent as i128 / 100;
        let step = ratio_step.max(max_permit as i128).max(1);
        (target as i128 + step).min(policy.max_bytes as i128) as i64
    }

    fn probe_startup(
        &self,
        policy: &ProbePolicy,
        state: &mut ProbeState,
        target: i64,
        goodput: u64,
        max_permit: i64,
    ) -> Option<i64> {
        let improved = state.prev_goodput == 0
            || goodput * 100 >= state.prev_goodput * (100 + policy.min_gain_percent as u64);
        if improved {
            state.flat_rounds = 0;
            state.prev_goodput = goodput;
            state.last_good_target = target;
            let grown = self.grow_target(policy, target, max_permit);
            if grown <= target {
                state.phase = Phase::Steady;
                state.rounds_since_probe = 0;
                return Some(target);
            }
            self.set_target(grown);
            log::info!(
                "inflight byte probe: startup {} -> {} bytes ({} B/s)",
                target,
                grown,
                goodput
            );
            return Some(grown);
        }

        state.flat_rounds += 1;
        if state.flat_rounds < policy.settle_windows.max(1) {
            return None;
        }
        state.phase = Phase::Steady;
        state.rounds_since_probe = 0;
        let settled = state
            .last_good_target
            .clamp(policy.min_bytes, policy.max_bytes);
        self.set_target(settled);
        log::info!(
            "inflight byte probe: startup settled at {} bytes ({} B/s)",
            settled,
            goodput
        );
        Some(settled)
    }

    fn probe_steady(
        &self,
        policy: &ProbePolicy,
        state: &mut ProbeState,
        target: i64,
        goodput: u64,
    ) -> Option<i64> {
        state.rounds_since_probe += 1;
        let baseline = *state.baseline.iter().max().unwrap_or(&goodput);
        let dropped = baseline > 0 && goodput * 100 < baseline * (100 - policy.drop_percent as u64);
        let due = state.rounds_since_probe >= policy.probe_interval_rounds.max(1);
        let allowed = state.rounds_since_probe >= policy.min_probe_interval_rounds.max(1);
        if !(due || (dropped && allowed)) {
            return None;
        }
        let down = (target as i128 * 3 / 4).max(policy.min_bytes as i128) as i64;
        if down >= target {
            return None;
        }
        state.probe_base_w = target;
        state.probe_base_g = goodput;
        state.phase = Phase::ProbeDown;
        self.set_target(down);
        log::info!(
            "inflight byte probe: down test {} -> {} bytes ({} B/s, baseline {} B/s)",
            target,
            down,
            goodput,
            baseline
        );
        Some(down)
    }

    fn probe_down(
        &self,
        policy: &ProbePolicy,
        state: &mut ProbeState,
        target: i64,
        goodput: u64,
    ) -> Option<i64> {
        // Keep the smaller budget unless it lost more than the dead band.
        let dead_band = 100u64.saturating_sub(policy.probe_gain_percent as u64);
        if goodput * 100 >= state.probe_base_g * dead_band {
            state.probe_base_w = target;
            state.probe_base_g = goodput;
        } else {
            self.set_target(state.probe_base_w);
        }
        // Steady-state probes are a fixed 25% step (the startup ramp uses the
        // task-footprint floor, which is meant for finding the order of
        // magnitude, not for fine steady-state tests).
        let up = (state.probe_base_w as i128 * 5 / 4).min(policy.max_bytes as i128) as i64;
        if up <= state.probe_base_w {
            state.phase = Phase::Steady;
            state.rounds_since_probe = 0;
            return Some(state.probe_base_w);
        }
        state.phase = Phase::ProbeUp;
        self.set_target(up);
        log::info!(
            "inflight byte probe: up test {} -> {} bytes ({} B/s)",
            state.probe_base_w,
            up,
            goodput
        );
        Some(up)
    }

    fn probe_up(
        &self,
        policy: &ProbePolicy,
        state: &mut ProbeState,
        target: i64,
        goodput: u64,
    ) -> Option<i64> {
        let accept = goodput * 100 >= state.probe_base_g * (100 + policy.probe_gain_percent as u64);
        let final_target = if accept { target } else { state.probe_base_w };
        self.set_target(final_target);
        state.phase = Phase::Steady;
        state.rounds_since_probe = 0;
        log::info!(
            "inflight byte probe: steady at {} bytes after up test ({} B/s)",
            final_target,
            goodput
        );
        Some(final_target)
    }

    pub fn is_probing(&self) -> bool {
        self.inner.probe.is_some()
    }

    pub fn snapshot(&self) -> GateSnapshot {
        GateSnapshot {
            target_bytes: self.target_bytes(),
            used_bytes: self.used_bytes(),
            peak_used_bytes: self.inner.peak_used.load(Ordering::Relaxed),
            active_permits: self.inner.active.load(Ordering::Relaxed),
            waiters: self.inner.waiters.load(Ordering::Relaxed),
            waits_total: self.inner.waits_total.load(Ordering::Relaxed),
            wait_ms_total: self.inner.wait_ns_total.load(Ordering::Relaxed) / 1_000_000,
            completed_bytes: self.inner.completed.load(Ordering::Relaxed),
        }
    }
}

impl GateInner {
    fn try_reserve(&self, size: i64) -> bool {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let target = self.target.load(Ordering::Acquire);
            if current.saturating_add(size) > target {
                return false;
            }
            let next = current + size;
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.active.fetch_add(1, Ordering::Relaxed);
                    self.peak_used.fetch_max(next, Ordering::Relaxed);
                    self.window_peak.fetch_max(next, Ordering::Relaxed);
                    self.max_permit.fetch_max(size, Ordering::Relaxed);
                    if let Some(gauge) = INFLIGHT_BYTES.as_ref() {
                        gauge.set(next);
                    }
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }
}

/// Reservation of in-flight bytes; releasing is `Drop`.
pub struct InflightPermit {
    inner: Arc<GateInner>,
    size: i64,
}

impl InflightPermit {
    pub fn size(&self) -> i64 {
        self.size
    }
}

impl Drop for InflightPermit {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::Relaxed);
        let used = self.inner.used.fetch_sub(self.size, Ordering::AcqRel) - self.size;
        if let Some(gauge) = INFLIGHT_BYTES.as_ref() {
            gauge.set(used.max(0));
        }
        self.inner.notify.notify_waiters();
    }
}

/// Bytes one load task is allowed to hold in flight.
///
/// Mirrors the accounting documented on `[job] ufs_read_*`: one reader keeps
/// `read_concurrent * read_chunk_size` bytes buffered, and a load-job task fans
/// out into `parallel_streams` readers. Transfer-service tasks keep the single
/// stream path, so they only pay for one reader.
pub fn task_inflight_bytes(task: &LoadTaskInfo) -> i64 {
    let props = &task.job.mount_info.properties;
    let parse = |key: &str, default: i64| -> i64 {
        props
            .get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default)
    };
    let chunk = parse(
        LoadTransferDefaults::READ_CHUNK_SIZE,
        JobConf::DEFAULT_UFS_READ_CHUNK_SIZE as i64,
    );
    let concurrent = parse(
        LoadTransferDefaults::READ_CONCURRENT,
        JobConf::DEFAULT_UFS_READ_CONCURRENT as i64,
    );
    let streams = if task.transfer_report.is_some() {
        1
    } else {
        parse(
            LoadTransferDefaults::PARALLEL_STREAMS,
            JobConf::DEFAULT_LOAD_PARALLEL_STREAMS as i64,
        )
    };
    chunk
        .saturating_mul(concurrent)
        .saturating_mul(streams)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ProbePolicy {
        ProbePolicy {
            growth_percent: 100,
            min_gain_percent: 10,
            settle_windows: 1,
            warmup_windows: 0,
            ..ProbePolicy::new(100, 10_000, Duration::from_secs(1))
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    #[test]
    fn oversized_request_is_clamped_to_the_budget() {
        runtime().block_on(async {
            let gate = InflightBytesGate::new(100);
            let permit = gate.acquire(10_000).await;
            assert_eq!(permit.size(), 100);
            assert_eq!(gate.used_bytes(), 100);
            assert_eq!(gate.snapshot().active_permits, 1);
            drop(permit);
            assert_eq!(gate.used_bytes(), 0);
            assert_eq!(gate.snapshot().active_permits, 0);
        });
    }

    #[test]
    fn waiter_is_released_when_a_permit_drops() {
        runtime().block_on(async {
            let gate = InflightBytesGate::new(100);
            let first = gate.acquire(80).await;
            assert_eq!(gate.used_bytes(), 80);

            let waiter_gate = gate.clone();
            let waiter = tokio::spawn(async move { waiter_gate.acquire(50).await });
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(gate.snapshot().waiters, 1);
            assert_eq!(gate.snapshot().waits_total, 1);

            drop(first);
            let second = waiter.await.unwrap();
            assert_eq!(second.size(), 50);
            assert_eq!(gate.used_bytes(), 50);
            drop(second);
            assert_eq!(gate.used_bytes(), 0);
        });
    }

    #[test]
    fn raising_the_budget_wakes_waiters() {
        runtime().block_on(async {
            let gate = InflightBytesGate::new(100);
            let first = gate.acquire(100).await;
            let waiter_gate = gate.clone();
            let waiter = tokio::spawn(async move { waiter_gate.acquire(100).await });
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(gate.snapshot().waiters, 1);

            gate.set_target(200);
            let second = waiter.await.unwrap();
            assert_eq!(second.size(), 100);
            assert_eq!(gate.used_bytes(), 200);
            drop(first);
            assert_eq!(gate.used_bytes(), 100);
        });
    }

    /// Mark the window as budget-limited (90% of the budget held by 90-byte
    /// tasks) and report `bytes` completed. Keeps the largest permit at 90 so
    /// the growth floor stays small while the budget grows.
    fn window(gate: &InflightBytesGate, bytes: i64) {
        let target = gate.target_bytes();
        let mut permits = Vec::new();
        while gate.used_bytes() < target * 9 / 10 {
            let size = 90.min(target - gate.used_bytes()).max(1);
            permits.push(runtime().block_on(gate.acquire(size)));
        }
        gate.record_completed(bytes);
        drop(permits);
    }

    #[test]
    fn probe_ignores_idle_and_app_limited_windows() {
        let gate = InflightBytesGate::with_probe(policy());
        assert_eq!(gate.probe_tick(Duration::from_secs(1)), None); // idle
        gate.record_completed(1000); // bytes, but the budget was never binding
        assert_eq!(gate.probe_tick(Duration::from_secs(1)), None);
        assert_eq!(gate.target_bytes(), 100);
        assert!(gate.is_probing());
    }

    #[test]
    fn probe_startup_ramps_and_settles_at_last_good() {
        let gate = InflightBytesGate::with_probe(ProbePolicy {
            min_gain_percent: 25,
            ..policy()
        });
        let window_len = Duration::from_secs(1);

        window(&gate, 100); // 100 B/s at 100 bytes
        assert_eq!(gate.probe_tick(window_len), Some(200));
        window(&gate, 300); // 300 B/s at 200: improving
        assert_eq!(gate.probe_tick(window_len), Some(400));
        window(&gate, 360); // 360 B/s at 400: flat
        assert_eq!(gate.probe_tick(window_len), Some(200));
        assert_eq!(gate.target_bytes(), 200);
    }

    #[test]
    fn probe_step_is_at_least_one_task() {
        runtime().block_on(async {
            let gate = InflightBytesGate::with_probe(ProbePolicy {
                growth_percent: 50,
                ..policy()
            });
            // Two tasks (60 + 30) mark the 100 byte budget as binding; the
            // largest is 60, more than the 50% step, so the step becomes 60.
            let first = gate.acquire(60).await;
            let second = gate.acquire(30).await;
            gate.record_completed(100);
            assert_eq!(gate.probe_tick(Duration::from_secs(1)), Some(160));
            drop(first);
            drop(second);
        });
    }

    #[test]
    fn probe_accepts_a_budget_a_single_task_cannot_fill() {
        runtime().block_on(async {
            let gate = InflightBytesGate::with_probe(ProbePolicy {
                min_bytes: 24,
                max_bytes: 10_000,
                growth_percent: 50,
                ..policy()
            });
            let permit = gate.acquire(16).await;
            gate.record_completed(16);
            // 16 is only 67% of 24, but a second 16-byte task cannot fit, so
            // the budget is binding and the sample is valid.
            assert_eq!(gate.probe_tick(Duration::from_secs(1)), Some(40));
            drop(permit);
        });
    }

    #[test]
    fn probe_steady_probes_on_interval() {
        let gate = InflightBytesGate::with_probe(ProbePolicy {
            min_gain_percent: 25,
            probe_interval_rounds: 1,
            min_probe_interval_rounds: 1,
            ..policy()
        });
        let window_len = Duration::from_secs(1);
        window(&gate, 100);
        assert_eq!(gate.probe_tick(window_len), Some(200));
        window(&gate, 300);
        assert_eq!(gate.probe_tick(window_len), Some(400));
        window(&gate, 360);
        assert_eq!(gate.probe_tick(window_len), Some(200)); // settle
                                                            // Steady state, one round: probe down 25%.
        window(&gate, 360);
        assert_eq!(gate.probe_tick(window_len), Some(150));
    }

    #[test]
    fn probe_reprobes_after_a_drop_and_keeps_the_winner() {
        let gate = InflightBytesGate::with_probe(ProbePolicy {
            min_gain_percent: 25,
            probe_interval_rounds: 100,
            min_probe_interval_rounds: 1,
            ..policy()
        });
        let window_len = Duration::from_secs(1);

        window(&gate, 100);
        assert_eq!(gate.probe_tick(window_len), Some(200));
        window(&gate, 300);
        assert_eq!(gate.probe_tick(window_len), Some(400));
        window(&gate, 360);
        assert_eq!(gate.probe_tick(window_len), Some(200)); // settled to last good

        // 200 B/s against a 360 B/s baseline: probe down to 150.
        window(&gate, 200);
        assert_eq!(gate.probe_tick(window_len), Some(150));

        // The smaller budget keeps throughput: accept it and probe up 25%.
        window(&gate, 300);
        assert_eq!(gate.probe_tick(window_len), Some(187));

        // The larger budget adds nothing: go back to 150.
        window(&gate, 300);
        assert_eq!(gate.probe_tick(window_len), Some(150));
    }

    #[test]
    fn task_cost_accounts_for_streams_and_transfer_tasks() {
        use curvine_model::{
            LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TtlAction, WorkerAddress,
        };
        use std::collections::HashMap;

        let mut properties = HashMap::new();
        properties.insert(
            LoadTransferDefaults::PARALLEL_STREAMS.to_string(),
            "4".to_string(),
        );
        properties.insert(
            LoadTransferDefaults::READ_CHUNK_SIZE.to_string(),
            "1048576".to_string(),
        );
        properties.insert(
            LoadTransferDefaults::READ_CONCURRENT.to_string(),
            "2".to_string(),
        );
        let job = LoadJobInfo {
            job_id: "j".to_string(),
            source_path: "s3://bucket/".to_string(),
            target_path: "/cv/".to_string(),
            block_size: 0,
            replicas: 1,
            storage_type: StorageType::Disk,
            ttl_ms: 0,
            ttl_action: TtlAction::None,
            mount_info: MountInfo {
                properties,
                ..Default::default()
            },
            create_time: 0,
            overwrite: None,
        };
        let task = LoadTaskInfo {
            job,
            task_id: "t".to_string(),
            worker: WorkerAddress::default(),
            source_path: "s3://bucket/key".to_string(),
            target_path: "/cv/key".to_string(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: None,
        };
        assert_eq!(task_inflight_bytes(&task), 4 * 1024 * 1024 * 2);
    }
}
