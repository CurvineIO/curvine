// Copyright 2026 OPPO.
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

use curvine_metrics::Counter;
use parking_lot::Mutex;
use std::time::Duration;
use tokio::time::{sleep, Instant};

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// A Worker-wide bucket for outgoing replica payload. Like the client write
/// limiter it starts full with one second of credit. Unlike that limiter, it
/// splits oversized reads before obtaining credit, so a cancelled waiter never
/// owns part of a chunk and a write never exceeds the burst size.
pub(super) struct ReplicationBandwidth {
    rate: u64,
    bucket: Mutex<Bucket>,
}

struct Bucket {
    // Fixed point, in byte-nanoseconds: preserve fractional tokens on retry.
    credit: u128,
    last: Instant,
}

impl ReplicationBandwidth {
    pub(super) fn new(rate: u64) -> Self {
        Self {
            rate,
            bucket: Mutex::new(Bucket {
                credit: u128::from(rate) * NANOS_PER_SEC,
                last: Instant::now(),
            }),
        }
    }

    pub(super) fn chunk_size(&self, configured: usize) -> usize {
        if self.rate == 0 {
            configured
        } else {
            configured.min(usize::try_from(self.rate).unwrap_or(usize::MAX))
        }
    }

    /// `bytes` must fit in the burst; callers cap the read size before reading.
    pub(super) async fn acquire(&self, bytes: usize, wait_ms: Option<&Counter>) {
        if bytes == 0 || self.rate == 0 {
            return;
        }
        debug_assert!(bytes as u128 <= u128::from(self.rate));
        let mut waited = None;
        while let Some(delay) = self.try_take(bytes) {
            waited.get_or_insert_with(|| WaitTime {
                started: Instant::now(),
                counter: wait_ms,
            });
            sleep(delay).await;
        }
    }

    fn try_take(&self, bytes: usize) -> Option<Duration> {
        let mut bucket = self.bucket.lock();
        let now = Instant::now();
        let rate = u128::from(self.rate);
        let elapsed = now.duration_since(bucket.last).as_nanos();
        bucket.last = now;
        // A one-second burst means older elapsed time cannot add more credit.
        bucket.credit =
            (bucket.credit + elapsed.min(NANOS_PER_SEC) * rate).min(rate * NANOS_PER_SEC);
        let needed = bytes as u128 * NANOS_PER_SEC;
        if bucket.credit >= needed {
            bucket.credit -= needed;
            None
        } else {
            Some(Duration::from_nanos(
                (needed - bucket.credit).div_ceil(rate) as u64,
            ))
        }
    }
}

struct WaitTime<'a> {
    started: Instant,
    counter: Option<&'a Counter>,
}

impl Drop for WaitTime<'_> {
    fn drop(&mut self) {
        if let Some(counter) = self.counter {
            counter.inc_by(i64::try_from(self.started.elapsed().as_millis()).unwrap_or(i64::MAX));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_metrics::Metrics;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::time::{advance, timeout};

    #[tokio::test(start_paused = true)]
    async fn burst_refill_keeps_fractional_credit_and_caps_idle_credit() {
        let limiter = ReplicationBandwidth::new(10);
        assert_eq!(limiter.try_take(10), None);
        assert_eq!(limiter.try_take(10), Some(Duration::from_secs(1)));
        advance(Duration::from_millis(150)).await;
        assert_eq!(limiter.try_take(1), None);
        assert_eq!(limiter.try_take(1), Some(Duration::from_millis(50)));
        advance(Duration::from_millis(50)).await;
        assert_eq!(limiter.try_take(1), None);
        advance(Duration::from_secs(30)).await;
        assert_eq!(limiter.try_take(10), None);
        assert_eq!(limiter.try_take(1), Some(Duration::from_millis(100)));
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_and_oversized_configured_chunks() {
        let now = Instant::now();
        let disabled = ReplicationBandwidth::new(0);
        assert_eq!(disabled.chunk_size(4096), 4096);
        disabled.acquire(4096, None).await;
        assert_eq!(Instant::now(), now);
        let limiter = ReplicationBandwidth::new(7);
        assert_eq!(limiter.chunk_size(4096), 7);
        assert_eq!(limiter.chunk_size(3), 3);
        limiter.acquire(0, None).await;
        assert_eq!(limiter.try_take(7), None);
        assert_eq!(limiter.try_take(7), Some(Duration::from_secs(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_waiter_releases_copy_permit_without_spending_credit() {
        let limiter = ReplicationBandwidth::new(100);
        let permits = Arc::new(Semaphore::new(1));
        let waited = Metrics::new_counter("test_replication_cancel_wait_ms", "test").unwrap();
        limiter.acquire(100, None).await;
        assert!(timeout(Duration::from_millis(250), async {
            let _permit = permits.clone().acquire_owned().await.unwrap();
            limiter.acquire(100, Some(&waited)).await;
        })
        .await
        .is_err());
        assert_eq!(permits.available_permits(), 1);
        assert_eq!(waited.get(), 250);
        assert_eq!(limiter.try_take(25), None);
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_chunk_streams_share_burst_and_rate() {
        let start = Instant::now();
        let limiter = ReplicationBandwidth::new(1024);
        let copy = || async {
            for _ in 0..8 {
                limiter.acquire(limiter.chunk_size(256), None).await;
                tokio::task::yield_now().await;
            }
        };
        tokio::join!(copy(), copy());
        // 4096 bytes total, less 1024 initial credit, at 1024 bytes/s.
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }
}
