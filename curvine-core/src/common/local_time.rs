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

use chrono::{Datelike, Local, Timelike};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

pub struct LocalTime;

impl LocalTime {
    pub const NANOSECONDS_PER_MILLISECOND: u128 = 1000000;

    pub fn new() -> Self {
        Self
    }

    pub fn nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    pub fn mills() -> u64 {
        (Self::nanos() / Self::NANOSECONDS_PER_MILLISECOND) as u64
    }

    pub fn now_datetime() -> String {
        Local::now().format(DATETIME_FORMAT).to_string()
    }
}

impl Default for LocalTime {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatTime for LocalTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let t = Local::now();
        // year() returns i32; clamp to 0-9999 so the {:04} field is always
        // exactly 4 characters and never produces a sign or extra digits.
        let year = t.year().clamp(0, 9999) as u32;
        write!(
            w,
            "{:04}/{:02}/{:02} {:02}:{:02}:{:02}.{:03}",
            year,
            t.month(),
            t.day(),
            t.hour(),
            t.minute(),
            t.second(),
            t.nanosecond() / 1_000_000,
        )
    }
}
