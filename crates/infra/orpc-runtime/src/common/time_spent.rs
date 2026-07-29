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

use std::sync::Arc;

use crate::common::LocalTime;
use crate::common::{Counter, HistogramVec};

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

impl TimeSpent {
    pub fn timer_counter(metric: Arc<Counter>) -> MetricTimer {
        MetricTimer::new(metric)
    }

    pub fn timer_counter_vec(
        metric: Arc<HistogramVec>,
        label_values: Vec<String>,
    ) -> MetricTimerVec {
        MetricTimerVec::new(metric, label_values)
    }
}

// RAII guard for Counter metrics
pub struct MetricTimer {
    start: u128,
    metric: Arc<Counter>,
}

impl MetricTimer {
    pub fn new(metric: Arc<Counter>) -> Self {
        Self {
            start: LocalTime::nanos(),
            metric,
        }
    }
}

impl Drop for MetricTimer {
    fn drop(&mut self) {
        let used_us = (LocalTime::nanos() - self.start) / 1000;
        self.metric.inc_by(used_us as i64);
    }
}

pub struct MetricTimerVec {
    start: u128,
    metric: Arc<HistogramVec>,
    label_values: Vec<String>,
}

impl MetricTimerVec {
    pub fn new(metric: Arc<HistogramVec>, label_values: Vec<String>) -> Self {
        Self {
            start: LocalTime::nanos(),
            metric,
            label_values,
        }
    }
}

impl Drop for MetricTimerVec {
    fn drop(&mut self) {
        let used_us = (LocalTime::nanos() - self.start) / 1000;
        let values: Vec<&str> = self.label_values.iter().map(|s| s.as_str()).collect();
        self.metric
            .with_label_values(&values)
            .observe(used_us as f64);
    }
}
