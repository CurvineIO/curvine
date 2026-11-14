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

//! Client-wide budget for bytes queued on the buffered write path.
//!
//! Every writer of one client charges the bytes it enqueues and releases them
//! once the chunk has been written (or dropped, including on cancel). Without
//! this, the only bound on queued write memory is the per-file chunk count, so
//! a fast writer or many concurrent writers can grow memory without limit.
//!
//! A budget of `0` disables the accounting, which keeps the historic behavior.

use curvine_core_error::CommonResult;
use curvine_metrics::{Counter, Metrics as m};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use tokio::sync::Notify;

const WAITS_METRIC: &str = "client_write_budget_waits_total";
const WAITS_HELP: &str = "writes that had to wait for the queued-byte budget";

// One counter for the whole process: `Metrics::register` ignores a second
// metric with the same name, so a per-budget counter would stop being exported
// as soon as the first client (the registered one) is dropped.
static WAITS: LazyLock<Result<Counter, String>> =
    LazyLock::new(|| m::new_counter(WAITS_METRIC, WAITS_HELP).map_err(|e| e.to_string()));

fn shared_waits_counter() -> CommonResult<Counter> {
    WAITS.clone().map_err(Into::into)
}

struct BudgetState {
    used: i64,
}

/// Client-wide queued-byte budget. Cloning is cheap; all clones share one
/// budget.
pub struct WriteBudget {
    capacity: AtomicI64,
    state: Mutex<BudgetState>,
    /// Wakes writers blocked on a full budget.
    notify: Notify,
    waits: Counter,
}

/// Released when the chunk it guards has been written or dropped.
pub struct BudgetPermit {
    budget: Arc<WriteBudget>,
    bytes: i64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

impl WriteBudget {
    /// `capacity == 0` disables the budget.
    pub fn new(capacity: usize) -> CommonResult<Arc<Self>> {
        Ok(Arc::new(Self {
            capacity: AtomicI64::new(capacity as i64),
            state: Mutex::new(BudgetState { used: 0 }),
            notify: Notify::new(),
            waits: shared_waits_counter()?,
        }))
    }

    pub fn capacity(&self) -> i64 {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Bytes currently reserved by queued chunks.
    pub fn used(&self) -> i64 {
        self.lock().used
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BudgetState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reserve room for `bytes` and return the permit that releases it.
    ///
    /// The reservation is clamped to the whole budget, so a chunk larger than
    /// the budget is admitted alone instead of waiting forever for room that can
    /// never exist. Returns `None` when the budget is disabled or `bytes == 0`.
    pub async fn acquire(self: &Arc<Self>, bytes: usize) -> Option<BudgetPermit> {
        let capacity = self.capacity();
        if capacity <= 0 || bytes == 0 {
            return None;
        }
        let want = (bytes as i64).min(capacity);
        let mut waited = false;
        loop {
            // Register before checking the state: `notify_waiters` wakes only
            // already-registered waiters and stores no permit, so a release
            // between the check and the await would otherwise be lost.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.lock();
                if state.used + want <= capacity {
                    state.used += want;
                    if waited {
                        self.waits.inc();
                    }
                    return Some(BudgetPermit {
                        budget: self.clone(),
                        bytes: want,
                    });
                }
            }
            waited = true;
            notified.await;
        }
    }

    fn release(&self, bytes: i64) {
        if bytes <= 0 {
            return;
        }
        {
            let mut state = self.lock();
            state.used = (state.used - bytes).max(0);
        }
        // One release can make room for several small requests, so notify every
        // waiter; waking only one would leave fitting writers asleep with free
        // capacity.
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn disabled_budget_never_blocks() {
        let budget = WriteBudget::new(0).unwrap();
        assert!(budget.acquire(1 << 30).await.is_none());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn empty_write_is_not_reserved() {
        let budget = WriteBudget::new(1024).unwrap();
        assert!(budget.acquire(0).await.is_none());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn permits_are_released_on_drop() {
        let budget = WriteBudget::new(1024).unwrap();
        let permit = budget.acquire(600).await.expect("room is available");
        assert_eq!(budget.used(), 600);
        drop(permit);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn a_full_budget_blocks_until_released() {
        let budget = WriteBudget::new(1024).unwrap();
        let first = budget.acquire(1024).await.expect("room is available");

        let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
        let waiting = {
            let budget = budget.clone();
            tokio::spawn(async move {
                let permit = budget.acquire(128).await;
                let _ = done_tx.send(permit.is_some());
                permit
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            done_rx.try_recv().is_err(),
            "a full budget must block the second writer"
        );
        assert_eq!(budget.used(), 1024);

        drop(first);
        let permit = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("release must wake the blocked writer")
            .expect("waiter task panicked")
            .expect("room is available after release");
        assert_eq!(budget.used(), 128);
        drop(permit);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn oversized_chunks_are_clamped_to_the_budget() {
        let budget = WriteBudget::new(100).unwrap();
        let permit = budget
            .acquire(5000)
            .await
            .expect("an oversized chunk takes the whole budget");
        assert_eq!(budget.used(), 100);
        drop(permit);
        assert_eq!(budget.used(), 0);
    }

    // One release can free room for several requests: every waiter that fits
    // must wake, otherwise the remaining writers sleep with free capacity.
    #[tokio::test]
    async fn one_release_wakes_every_fitting_waiter() {
        let budget = WriteBudget::new(1024).unwrap();
        let held = budget.acquire(1024).await.expect("the whole budget");

        let mut waiters = Vec::new();
        for _ in 0..3 {
            let budget = budget.clone();
            waiters.push(tokio::spawn(async move { budget.acquire(128).await }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(held);

        let mut permits = Vec::new();
        for waiter in waiters {
            let permit = tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("one release must wake every fitting waiter")
                .expect("waiter task panicked")
                .expect("room is available after release");
            permits.push(permit);
        }
        assert_eq!(budget.used(), 3 * 128);
        drop(permits);
        assert_eq!(budget.used(), 0);
    }

    // The metric must survive the client that registered it: a later budget has
    // to reuse the exported counter instead of registering a new one that the
    // registry ignores.
    #[tokio::test]
    async fn later_budgets_export_the_shared_wait_metric() {
        drop(WriteBudget::new(64).unwrap());

        let registered = match m::get(WAITS_METRIC) {
            Some(m::Counter(counter)) => counter,
            other => panic!(
                "wait counter must be registered, got: {:?}",
                other.is_some()
            ),
        };
        let before = registered.get();

        let budget = WriteBudget::new(64).unwrap();
        let held = budget.acquire(64).await.expect("room is available");
        let waiting = {
            let budget = budget.clone();
            tokio::spawn(async move { budget.acquire(64).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(held);
        let permit = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("release must wake the waiter")
            .expect("waiter task panicked")
            .expect("room is available after release");

        assert!(
            registered.get() > before,
            "waits from a later budget must be exported"
        );
        drop(permit);
    }
}
