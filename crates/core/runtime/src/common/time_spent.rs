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

use crate::common::LocalTime;

pub struct TimeSpent(u128);

impl TimeSpent {
    pub fn new() -> Self {
        Self(LocalTime::nanos())
    }

    pub fn log(&self, mark: &str) {
        log::info!("{} use {} ms", mark, self.used_ms())
    }

    pub fn log_us(&self, mark: &str) {
        log::info!("{} use {} us", mark, self.used_us())
    }

    // milliseconds
    pub fn used_ms(&self) -> u64 {
        ((LocalTime::nanos() - self.0) / LocalTime::NANOSECONDS_PER_MILLISECOND) as u64
    }

    // microseconds
    pub fn used_us(&self) -> u64 {
        ((LocalTime::nanos() - self.0) / 1000) as u64
    }

    pub fn reset(&mut self) {
        self.0 = LocalTime::nanos();
    }
}

impl Default for TimeSpent {
    fn default() -> Self {
        Self::new()
    }
}
