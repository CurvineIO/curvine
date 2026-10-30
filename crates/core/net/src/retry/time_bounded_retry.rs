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

use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct TimeBondedRetry {
    max_sleep: Duration,
    next_sleep: Duration,

    end_time: Instant,
    count: u32,
}

impl TimeBondedRetry {
    pub fn new(max_duration: Duration, min_sleep: Duration, max_sleep: Duration) -> Self {
        let start_time = Instant::now();
        let end_time = start_time + max_duration;

        Self {
            max_sleep,
            next_sleep: min_sleep,
            end_time,
            count: 0,
        }
    }

    pub async fn attempt(&mut self) -> bool {
        if self.count == 0 {
            self.count += 1;
            return true;
        }

        let now = Instant::now();
        if now >= self.end_time {
            return false;
        }

        let mut wait_time = if self.count == 1 {
            self.next_sleep
        } else {
            (self.next_sleep * 2).min(self.max_sleep)
        };

        if now + wait_time > self.end_time {
            wait_time = self.end_time - now;
        }

        if wait_time.as_nanos() > 0 {
            tokio::time::sleep(wait_time).await;
        }

        self.next_sleep = wait_time;
        self.count += 1;
        true
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    /// Time left in the policy budget, or zero once it is exhausted.
    ///
    /// Callers use this to clamp a single attempt so one attempt cannot run past
    /// the whole policy budget: the budget is a deadline, not just a bound on
    /// the number of attempts.
    pub fn remaining(&self) -> Duration {
        self.end_time.saturating_duration_since(Instant::now())
    }
}

#[derive(Clone)]
pub struct TimeBondedRetryBuilder {
    max_duration: Duration,
    min_sleep: Duration,
    max_sleep: Duration,
}

impl TimeBondedRetryBuilder {
    pub fn new(max_duration: Duration, min_sleep: Duration, max_sleep: Duration) -> Self {
        Self {
            max_duration,
            min_sleep,
            max_sleep,
        }
    }

    pub fn build(&self) -> TimeBondedRetry {
        TimeBondedRetry::new(self.max_duration, self.min_sleep, self.max_sleep)
    }

    /// Total budget a built policy may spend, including sleeps and attempts.
    pub fn max_duration(&self) -> Duration {
        self.max_duration
    }
}

impl Default for TimeBondedRetryBuilder {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            Duration::from_secs(60),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::retry::{TimeBondedRetry, TimeBondedRetryBuilder};
    use curvine_runtime::common::LocalTime;
    use std::time::Duration;

    #[tokio::test]
    async fn backoff() {
        let mut retry = TimeBondedRetry::new(
            Duration::from_millis(500),
            Duration::from_millis(10),
            Duration::from_millis(100),
        );

        let items: Vec<u64> = vec![10, 20, 40, 80, 100];
        let iter = items.into_iter();
        assert!(retry.attempt().await);

        for wait in iter {
            let now = LocalTime::mills();
            assert!(retry.attempt().await);

            let diff = LocalTime::mills() - now;
            println!("{} {}", wait, diff);

            assert!(diff >= wait);
            assert!(diff - wait < 10);
        }

        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(!(retry.attempt().await));
        assert_eq!(retry.remaining(), Duration::ZERO);
    }

    #[tokio::test]
    async fn remaining_tracks_the_budget() {
        let builder = TimeBondedRetryBuilder::new(
            Duration::from_millis(200),
            Duration::from_millis(5),
            Duration::from_millis(10),
        );
        assert_eq!(builder.max_duration(), Duration::from_millis(200));

        let policy = builder.build();
        assert!(policy.remaining() <= Duration::from_millis(200));
        assert!(policy.remaining() > Duration::from_millis(100));

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(policy.remaining(), Duration::ZERO);
    }
}
