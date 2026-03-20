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

use log::warn;
use orpc::common::LocalTime;
use orpc::runtime::LoopTask;
use orpc::CommonResult;
use std::thread;
use std::time::Duration;

// Schedules execution threads
pub struct ScheduledExecutor {
    interval_ms: u64,
    thread_name: String,
}

impl ScheduledExecutor {
    pub fn new(thread_name: impl Into<String>, interval_ms: u64) -> Self {
        Self {
            thread_name: thread_name.into(),
            interval_ms,
        }
    }

    pub fn start<T>(self, task: T) -> CommonResult<()>
    where
        T: LoopTask + Send + 'static,
    {
        let name = self.thread_name.to_string();
        let builder = thread::Builder::new().name(name.clone());
        let interval_ms = self.interval_ms;
        builder.spawn(move || {
            Self::loop0(interval_ms, name, task);
        })?;

        Ok(())
    }

    pub fn loop0<T>(interval_ms: u64, name: String, task: T)
    where
        T: LoopTask + Send + 'static,
    {
        let mut next_ms = LocalTime::mills() + interval_ms;
        while !task.terminate() {
            if LocalTime::mills() >= next_ms {
                if let Err(e) = task.run() {
                    // An error occurs only logging.
                    warn!("Scheduler thread {}, run fail: {}", name, e);
                }
                next_ms = LocalTime::mills() + interval_ms;
            }

            let wait_ms = next_ms.saturating_sub(LocalTime::mills());
            if wait_ms >= 1 {
                thread::sleep(Duration::from_millis(wait_ms))
            }
        }
    }
}
