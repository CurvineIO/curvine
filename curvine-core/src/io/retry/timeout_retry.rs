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

use crate::common::{LocalTime, Utils};
use std::time::Duration;

// Timeout-based retry policy.
#[derive(Debug, Clone)]
pub struct TimeoutRetry {
    timeout_ms: u64,
    sleep_ms: u64,

    start_ms: u64,
}

impl TimeoutRetry {
    pub fn new(timeout_ms: u64, sleep_ms: u64) -> Self {
        Self {
            timeout_ms,
            sleep_ms,
            start_ms: 0,
        }
    }

    pub async fn attempt(&mut self) -> bool {
        if self.start_ms == 0 {
            self.start_ms = LocalTime::mills();
            return true;
        }

        if self.sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        }

        LocalTime::mills() <= self.start_ms + self.timeout_ms
    }

    pub fn attempt_blocking(&mut self) -> bool {
        if self.start_ms == 0 {
            self.start_ms = LocalTime::mills();
            return true;
        }

        if self.sleep_ms > 0 {
            Utils::sleep(self.sleep_ms);
        }

        LocalTime::mills() <= self.start_ms + self.timeout_ms
    }
}

#[cfg(test)]
mod test {
    use crate::io::retry::TimeoutRetry;
    use std::time::Duration;

    #[tokio::test]
    async fn timeout() {
        let mut retry = TimeoutRetry::new(1000, 100);

        assert!(retry.attempt().await);

        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(retry.attempt().await);

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!(retry.attempt().await));
    }
}
