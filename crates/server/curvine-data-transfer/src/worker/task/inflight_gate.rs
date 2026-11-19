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

/// Ramp-and-settle policy for probing the in-flight byte budget.
///
/// Every `window` the controller measures goodput (completed bytes per second,
/// fed by [`InflightBytesGate::record_completed`]). While goodput improves by at
/// least `min_gain_percent`, the budget grows by `growth_percent`; after
/// `settle_windows` windows without that gain it returns to the last budget
/// that produced an improvement and stops. Windows with no completed bytes are
/// ignored: an idle worker must not starve its own budget down to the floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbePolicy {
    pub min_bytes: i64,
    pub max_bytes: i64,
    pub window: Duration,
    pub growth_percent: u32,
    pub min_gain_percent: u32,
    pub settle_windows: u32,
    pub warmup_windows: u32,
}

impl ProbePolicy {
    /// Double the budget each step: a smaller step can land inside the same
    /// task slot (16 MiB -> 24 MiB still admits one 16 MiB task) and look flat.
    pub const DEFAULT_GROWTH_PERCENT: u32 = 100;
    /// Require a real gain before growing again. 32 -> 64 MiB at the knee of the
    /// dummy-server sweep bought +11% for 2x the memory; 15% rejects that.
    pub const DEFAULT_MIN_GAIN_PERCENT: u32 = 15;
    pub const DEFAULT_SETTLE_WINDOWS: u32 = 2;
    pub const DEFAULT_WARMUP_WINDOWS: u32 = 1;

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

struct ProbeState {
    last_completed: i64,
    best_goodput: u64,
    previous_target: i64,
    low_gain_windows: u32,
    warmup_left: u32,
    settled: bool,
    last_goodput: u64,
}

impl ProbeState {
    fn new(policy: &ProbePolicy) -> Self {
        Self {
            last_completed: 0,
            best_goodput: 0,
            previous_target: policy.min_bytes,
            low_gain_windows: 0,
            warmup_left: policy.warmup_windows,
            settled: false,
            last_goodput: 0,
        }
    }
}

struct GateInner {
    target: AtomicI64,
    used: AtomicI64,
    peak_used: AtomicI64,
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
    /// Returns the new budget when it changed. No-op when the gate was built
    /// without a [`ProbePolicy`] or after the probe settled.
    pub fn probe_tick(&self, elapsed: Duration) -> Option<i64> {
        let policy = self.inner.probe?;
        let completed = self.inner.completed.load(Ordering::Relaxed);
        let mut state = self.inner.probe_state.lock();
        if state.settled {
            return None;
        }

        let delta = (completed - state.last_completed).max(0);
        state.last_completed = completed;

        if state.warmup_left > 0 {
            state.warmup_left -= 1;
            return None;
        }
        // An idle worker produces no signal: do not treat "no tasks" as "the
        // budget is too large" and shrink it.
        if delta == 0 {
            return None;
        }

        let goodput = (delta as f64 / elapsed.as_secs_f64().max(0.001)) as u64;
        state.last_goodput = goodput;
        let target = self.target_bytes();
        let improved = state.best_goodput == 0
            || goodput * 100 >= state.best_goodput * (100 + policy.min_gain_percent as u64);
        if improved {
            state.best_goodput = state.best_goodput.max(goodput);
            state.low_gain_windows = 0;
            if target >= policy.max_bytes {
                state.settled = true;
                log::info!(
                    "inflight byte probe reached max target {} bytes at {} B/s",
                    target,
                    goodput
                );
                return Some(target);
            }
            let grown = (target as i128 * (100 + policy.growth_percent as i128) / 100)
                .min(policy.max_bytes as i128) as i64;
            state.previous_target = target;
            self.set_target(grown);
            log::info!(
                "inflight byte probe: goodput {} B/s improved, target {} -> {} bytes",
                goodput,
                target,
                grown
            );
            return Some(grown);
        }

        state.low_gain_windows += 1;
        if state.low_gain_windows < policy.settle_windows.max(1) {
            return None;
        }
        state.settled = true;
        let settled = state
            .previous_target
            .max(policy.min_bytes)
            .min(policy.max_bytes);
        self.set_target(settled);
        log::info!(
            "inflight byte probe settled at {} bytes ({} B/s, best {} B/s)",
            settled,
            goodput,
            state.best_goodput
        );
        Some(settled)
    }

    pub fn is_probing(&self) -> bool {
        self.inner.probe.is_some() && !self.inner.probe_state.lock().settled
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

    #[test]
    fn probe_ramps_on_gain_then_settles_one_level_back() {
        let gate = InflightBytesGate::with_probe(policy());
        let window = Duration::from_secs(1);

        gate.record_completed(100); // 100 B/s at a 100 byte budget
        assert_eq!(gate.probe_tick(window), Some(200));

        gate.record_completed(300); // 300 B/s at 200: +200%, keep ramping
        assert_eq!(gate.probe_tick(window), Some(400));

        gate.record_completed(300); // 300 B/s at 400: no gain, settle
        assert_eq!(gate.probe_tick(window), Some(200));
        assert_eq!(gate.target_bytes(), 200);
        assert!(!gate.is_probing());
    }

    #[test]
    fn probe_ignores_idle_windows() {
        let gate = InflightBytesGate::with_probe(policy());
        assert_eq!(gate.probe_tick(Duration::from_secs(1)), None);
        assert_eq!(gate.target_bytes(), 100);
        assert!(gate.is_probing());
    }

    #[test]
    fn probe_stops_at_max() {
        let gate = InflightBytesGate::with_probe(ProbePolicy {
            max_bytes: 200,
            ..policy()
        });
        gate.record_completed(100);
        assert_eq!(gate.probe_tick(Duration::from_secs(1)), Some(200));
        gate.record_completed(300);
        assert_eq!(gate.probe_tick(Duration::from_secs(1)), Some(200));
        assert!(!gate.is_probing());
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
