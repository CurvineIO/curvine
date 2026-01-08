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

#![allow(clippy::should_implement_trait)]

use crate::{err_box, CommonResult};
use std::fmt;
use std::time::Duration;

// The latest units supported are milliseconds, so DurationUnit saves the number of milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Default)]
pub struct DurationUnit(u64);

impl DurationUnit {
    pub const MILLISECONDS: u64 = 1;
    pub const SECONDS: u64 = 1000 * Self::MILLISECONDS;
    pub const MINUTE: u64 = 60 * Self::SECONDS;
    pub const HOUR: u64 = 60 * Self::MINUTE;
    pub const DAY: u64 = 24 * Self::HOUR;

    pub fn new(ms: u64) -> Self {
        DurationUnit(ms)
    }
    pub fn duration_to_ms(d: Duration) -> u64 {
        let nanos = u64::from(d.subsec_nanos());
        d.as_secs() * 1_000 + (nanos / 1_000_000)
    }

    pub fn as_millis(&self) -> u64 {
        self.0
    }

    pub fn as_duration(&self) -> Duration {
        Duration::from_millis(self.0)
    }

    pub fn as_seconds(&self) -> u64 {
        self.0 / Self::SECONDS
    }

    pub fn as_minutes(&self) -> u64 {
        self.0 / Self::MINUTE
    }

    pub fn as_hours(&self) -> u64 {
        self.0 / Self::HOUR
    }

    pub fn as_days(&self) -> u64 {
        self.0 / Self::DAY
    }

    pub fn from_str(dur_str: &str) -> CommonResult<Self> {
        let dur_str = dur_str.trim().to_lowercase();
        if !dur_str.is_ascii() {
            return err_box!("unexpect ascii string: {}", dur_str);
        }

        let size_len = dur_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || ['.', 'e', 'E', '-', '+'].contains(c))
            .count();

        let (size, unit) = dur_str.split_at(size_len);

        let unit = match unit.trim() {
            "s" | "second" => Self::SECONDS,
            "m" | "minute" => Self::MINUTE,
            "h" | "hour" => Self::HOUR,
            "d" | "day" => Self::DAY,
            "ms" | "" => {
                if size.chars().all(|c| char::is_ascii_digit(&c)) {
                    return match size.parse::<u64>() {
                        Ok(n) => Ok(DurationUnit::new(n)),
                        Err(_) => err_box!("invalid duration string: {}", dur_str),
                    };
                }
                Self::MILLISECONDS
            }
            _ => {
                return err_box!(
                    "valid duration {}, only d, h, m, s, ms are supported",
                    dur_str
                );
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(DurationUnit::new((n * unit as f64) as u64)),
            Err(_) => err_box!("invalid duration string: {}", dur_str),
        }
    }
}

impl fmt::Display for DurationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = self.0;
        if ms == 0 {
            write!(f, "{}ms", ms)
        } else if ms.is_multiple_of(Self::DAY) {
            write!(f, "{}d", ms / Self::DAY)
        } else if ms.is_multiple_of(Self::HOUR) {
            write!(f, "{}h", ms / Self::HOUR)
        } else if ms.is_multiple_of(Self::MINUTE) {
            write!(f, "{}m", ms / Self::MINUTE)
        } else if ms.is_multiple_of(Self::SECONDS) {
            write!(f, "{}s", ms / Self::SECONDS)
        } else {
            write!(f, "{}ms", ms)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::common::DurationUnit;

    #[test]
    fn duration_unit() {
        let legal_cases = vec![
            (1, "1ms"),
            (2 * DurationUnit::SECONDS, "2s"),
            (4 * DurationUnit::MINUTE, "4m"),
            (5 * DurationUnit::HOUR, "5h"),
            (DurationUnit::DAY, "1d"),
        ];

        for (ms, exp) in legal_cases {
            let d = DurationUnit::new(ms);
            let res_str = d.to_string();
            println!("res_str = {}, mills = {}", res_str, d.as_millis());
            assert_eq!(res_str, exp);

            let res_dur: DurationUnit = DurationUnit::from_str(exp).unwrap();
            assert_eq!(res_dur.as_millis(), d.as_millis());
        }
    }
}
