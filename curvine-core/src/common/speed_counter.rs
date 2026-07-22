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

use crate::common::{ByteUnit, TimeSpent};

pub struct SpeedCounter(TimeSpent);

impl SpeedCounter {
    pub fn new() -> Self {
        SpeedCounter(TimeSpent::new())
    }

    pub fn to_string(&self, mark: &str, bytes: u64) -> String {
        // Use microseconds for sub-millisecond precision when the elapsed time
        // is short; fall back to a minimum of 1us to avoid division by zero.
        let cost_secs = (self.0.used_us().max(1) as f64) / 1_000_000.0;

        let size_string = ByteUnit::byte_to_string(bytes);
        let bytes_per_sec = bytes as f64 / cost_secs;
        let bits_per_sec = bytes_per_sec * 8.0;

        let speed = ByteUnit::byte_to_string(bytes_per_sec as u64);
        let band_width = Self::bits_per_sec_to_string(bits_per_sec);

        format!(
            "{} size: {}, cost: {:.2} s, speed: {}/s, bandwidth: {}",
            mark, size_string, cost_secs, speed, band_width
        )
    }

    /// Format a bit-rate (bits per second) using the network-bandwidth
    /// convention (decimal SI prefixes, lower-case `b` for bit).
    fn bits_per_sec_to_string(bits_per_sec: f64) -> String {
        const UNITS: &[&str] = &["bps", "Kbps", "Mbps", "Gbps", "Tbps", "Pbps"];
        if !bits_per_sec.is_finite() || bits_per_sec <= 0.0 {
            return "0bps".to_string();
        }

        let group = (bits_per_sec.log10() / 1000f64.log10()).floor() as i32;
        let group = group.clamp(0, (UNITS.len() - 1) as i32) as usize;

        let scaled = bits_per_sec / 1000f64.powi(group as i32);
        format!("{:.1}{}", scaled, UNITS[group])
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }

    pub fn print(&self, mark: &str, bytes: u64) {
        println!("{}", self.to_string(mark, bytes))
    }

    pub fn log(&self, mark: &str, bytes: u64) {
        log::info!("{}", self.to_string(mark, bytes))
    }
}

impl Default for SpeedCounter {
    fn default() -> Self {
        Self::new()
    }
}
