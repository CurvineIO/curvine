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

use crate::client::ClientConf;
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

    pub fn from_conf(conf: &ClientConf) -> Self {
        Self::new(
            Duration::from_millis(conf.io_retry_max_duration_ms),
            Duration::from_millis(conf.io_retry_min_sleep_ms),
            Duration::from_millis(conf.io_retry_max_sleep_ms),
        )
    }

    pub fn build(&self) -> TimeBondedRetry {
        TimeBondedRetry::new(self.max_duration, self.min_sleep, self.max_sleep)
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
    use crate::common::LocalTime;
    use crate::io::retry::TimeBondedRetry;
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
    }
}
