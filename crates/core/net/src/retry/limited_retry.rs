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

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LimitedRetry {
    max_retry: u32,
    retry_wait: Duration,
    count: u32,
}

impl LimitedRetry {
    pub fn new(max_retry: u32, retry_wait_ms: u64) -> Self {
        Self {
            max_retry,
            retry_wait: Duration::from_millis(retry_wait_ms),
            count: 0,
        }
    }

    pub async fn attempt(&mut self) -> bool {
        if self.count == 0 {
            self.count += 1;
            return true;
        } else if self.count > self.max_retry {
            return false;
        }

        if !self.retry_wait.is_zero() {
            tokio::time::sleep(self.retry_wait).await;
        }
        self.count += 1;
        true
    }

    pub fn count(&self) -> u32 {
        self.count
    }
}
