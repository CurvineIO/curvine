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
use once_cell::sync::Lazy;
use std::fmt;

#[derive(Clone, Debug, Copy, PartialEq, PartialOrd, Default)]
pub struct ByteUnit(u64);

static UNITS: Lazy<Vec<&str>> = Lazy::new(|| vec!["B", "KB", "MB", "GB", "TB", "PB", "EB"]);

impl ByteUnit {
    pub const B: u64 = 1;
    pub const KB: u64 = 1u64 << 10;
    pub const MB: u64 = 1u64 << 20;
    pub const GB: u64 = 1u64 << 30;
    pub const TB: u64 = 1u64 << 40;
    pub const PB: u64 = 1u64 << 50;

    pub fn new(value: u64) -> Self {
        ByteUnit(value)
    }

    pub fn as_byte(&self) -> u64 {
        self.0
    }

    pub fn as_kb(&self) -> u64 {
        self.0 / Self::KB
    }

    pub fn as_mb(&self) -> u64 {
        self.0 / Self::MB
    }

    pub fn as_gb(&self) -> u64 {
        self.0 / Self::GB
    }

    pub fn as_tb(&self) -> u64 {
        self.0 / Self::TB
    }

    pub fn as_pb(&self) -> u64 {
        self.0 / Self::PB
    }

    pub fn from_str(size_str: impl AsRef<str>) -> CommonResult<Self> {
        let size_str = size_str.as_ref().trim().to_uppercase();
        if size_str.is_empty() {
            return err_box!("{} is not a valid size.", size_str);
        }

        if !size_str.is_ascii() {
            return err_box!("ASCII string is expected, but got {}", size_str);
        }

        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || ['.', 'e', 'E', '-', '+'].contains(c))
            .count();

        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" => Self::KB,
            "M" | "MB" => Self::MB,
            "G" | "GB" => Self::GB,
            "T" | "TB" => Self::TB,
            "P" | "PB" => Self::PB,
            "B" | "" => {
                if size.chars().all(|c| char::is_ascii_digit(&c)) {
                    return match size.parse::<u64>() {
                        Ok(n) => Ok(ByteUnit(n)),
                        Err(_) => err_box!("invalid size string: {}", size_str),
                    };
                }
                Self::B
            }
            _ => {
                return err_box!("only B, KB,, MB, GB, TB, PB are supported: {}", size_str);
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(ByteUnit((n * unit as f64) as u64)),
            Err(_) => err_box!("invalid size string: {}", size_str),
        }
    }

    pub fn byte_to_string(size: u64) -> String {
        if size == 0 {
            return String::from("0");
        }

        let digit_groups = (size as f64).log10() / (1024f64).log10();
        let digit_groups = digit_groups.floor() as usize;

        if digit_groups > UNITS.len() - 1 {
            return format!("{}B", size);
        }

        let v = size as f64 / (1024f64).powi(digit_groups as i32);
        format!("{:.1}{}", v, UNITS[digit_groups])
    }

    pub fn string_to_byte<T: AsRef<str>>(str: T) -> u64 {
        match Self::from_str(str.as_ref()) {
            Ok(v) => v.0,
            Err(_) => 0,
        }
    }
}

impl fmt::Display for ByteUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size = self.0;
        if size == 0 {
            write!(f, "{}B", size)
        } else if size.is_multiple_of(Self::PB) {
            write!(f, "{}PB", size / Self::PB)
        } else if size.is_multiple_of(Self::TB) {
            write!(f, "{}TB", size / Self::TB)
        } else if size.is_multiple_of(Self::GB) {
            write!(f, "{}GB", size / Self::GB)
        } else if size.is_multiple_of(Self::MB) {
            write!(f, "{}MB", size / Self::MB)
        } else if size.is_multiple_of(Self::KB) {
            write!(f, "{}KB", size / Self::KB)
        } else {
            write!(f, "{}B", size)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::ByteUnit;

    #[test]
    fn bytes_unit() {
        let legal_cases = vec![
            (0, "0B"),
            (2 * ByteUnit::KB, "2KB"),
            (4 * ByteUnit::MB, "4MB"),
            (5 * ByteUnit::GB, "5GB"),
            (7 * ByteUnit::TB, "7TB"),
            (11 * ByteUnit::PB, "11PB"),
        ];
        for (size, exp) in legal_cases {
            let c = ByteUnit::new(size);
            let res_str = c.to_string();
            println!("res_str = {}", res_str);
            assert_eq!(res_str, exp);

            let res_size = ByteUnit::from_str(exp).unwrap();
            assert_eq!(res_size.as_byte(), size);
        }
    }
}
