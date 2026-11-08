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

//! Client-wide write bandwidth limiter.
//!
//! One limiter is shared by every block writer of a client, so
//! `client.write_limit` bounds the aggregate write bandwidth instead of each
//! file separately. Bytes are charged once per logical chunk, before the chunk
//! is fanned out to replicas.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Longest single sleep inside [`WriteLimiter::acquire`]. It bounds how long a
/// cancelled or timed-out writer waits for tokens and keeps concurrent writers
/// interleaving instead of queueing behind one large chunk.
const MAX_SLEEP: Duration = Duration::from_millis(200);

struct LimiterState {
    /// Tokens available now. One token is one byte.
    tokens: f64,
    last: Instant,
}

/// Token bucket in bytes. `rate == 0` disables it and `acquire` returns
/// immediately.
pub struct WriteLimiter {
    rate: AtomicU64,
    state: Mutex<LimiterState>,
}

impl WriteLimiter {
    /// `bytes_per_sec == 0` builds a disabled limiter.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            rate: AtomicU64::new(bytes_per_sec),
            state: Mutex::new(LimiterState {
                // Start with one second of credit so the first write does not
                // stall, then refill up to the same burst cap.
                tokens: bytes_per_sec as f64,
                last: Instant::now(),
            }),
        }
    }

    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// Wait until `bytes` may be sent.
    pub async fn acquire(&self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 || self.rate() == 0 {
            return;
        }
        while let Some(wait) = self.take(bytes, Instant::now()) {
            tokio::time::sleep(wait).await;
        }
    }

    /// Charge `bytes` against the bucket. `None` means granted; `Some(wait)`
    /// means the caller must wait before trying again.
    ///
    /// Tokens are reset when a request cannot be granted so a chunk larger than
    /// the burst capacity cannot be starved by smaller writers.
    fn take(&self, bytes: u64, now: Instant) -> Option<Duration> {
        let rate = self.rate();
        if rate == 0 || bytes == 0 {
            return None;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cap = rate as f64;
        let elapsed = now.saturating_duration_since(state.last).as_secs_f64();
        state.last = now;
        state.tokens = (state.tokens + elapsed * cap).min(cap);

        let needed = bytes as f64;
        if state.tokens >= needed {
            state.tokens -= needed;
            return None;
        }

        let missing = needed - state.tokens;
        state.tokens = 0.0;
        Some(Duration::from_secs_f64(missing / cap).min(MAX_SLEEP))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_rate_is_unlimited() {
        let limiter = WriteLimiter::new(0);
        assert_eq!(limiter.take(1 << 30, Instant::now()), None);
    }

    #[test]
    fn empty_write_is_never_charged() {
        let limiter = WriteLimiter::new(1000);
        assert_eq!(limiter.take(0, Instant::now()), None);
        // The burst is still intact.
        assert_eq!(limiter.take(1000, Instant::now()), None);
    }

    #[test]
    fn burst_then_refill() {
        let limiter = WriteLimiter::new(1000);
        let start = Instant::now();
        assert_eq!(limiter.take(600, start), None);
        let wait = limiter
            .take(600, start)
            .expect("second burst chunk needs tokens");
        assert!(
            wait >= Duration::from_millis(100) && wait <= MAX_SLEEP,
            "unexpected wait {:?}",
            wait
        );

        // One second later the bucket holds a full burst again.
        let later = start + Duration::from_secs(1);
        assert_eq!(limiter.take(1000, later), None);
    }

    #[test]
    fn wait_is_capped_for_oversized_requests() {
        let limiter = WriteLimiter::new(100);
        let wait = limiter
            .take(1_000_000, Instant::now())
            .expect("a request above the burst capacity must wait");
        assert_eq!(wait, MAX_SLEEP);
    }

    #[test]
    fn refill_never_exceeds_one_second_of_credit() {
        let limiter = WriteLimiter::new(1000);
        let start = Instant::now();
        assert_eq!(limiter.take(1000, start), None);
        // Ten idle seconds still only grant a one second burst.
        let later = start + Duration::from_secs(10);
        assert_eq!(limiter.take(1000, later), None);
        assert!(limiter
            .take(1, later)
            .expect("burst must be exhausted again")
            .le(&MAX_SLEEP));
    }
}
