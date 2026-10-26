//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! Process-level profiling (flame graph) configuration.
//!
//! Like `[fault_injection]`, this section is a two-gate control:
//!
//! 1. The `profiling` Cargo feature decides whether the sampling code is
//!    compiled into the binary at all.
//! 2. [`ProfilingConf::cpu_enabled`] decides whether the HTTP routes are
//!    exposed at runtime.
//!
//! Both must be on for the feature to be usable. Enabling `cpu_enabled`
//! on a build without the feature fails startup instead of silently
//! degrading, so an operator never believes samples are being served when
//! they are not.
//!
//! Everything here defaults to *off*: profiling costs CPU (SIGPROF-based
//! sampling) and must never be enabled implicitly.

use serde::{Deserialize, Serialize};

/// Default sampling frequency in Hz.
///
/// 1000 Hz is the conventional value for `pprof`-based sampling: fine
/// enough to resolve hot leaf frames without dominating the profilee.
pub const DEFAULT_CPU_FREQUENCY: u32 = 1000;

/// Upper bound for a single sampling window.
///
/// A request may ask for less, never for more. The cap exists so that a
/// mistyped duration cannot pin a request handler and a signal timer for
/// an unbounded time.
pub const DEFAULT_MAX_DURATION_SECS: u64 = 120;

/// Default frame modules excluded from sampling.
///
/// `backtrace-rs` unwinds through libgcc's libunwind, which is not
/// signal-safe; excluding these modules removes the deadlock risk
/// documented by `pprof-rs` (see its README "Backtrace" section). `vdso`
/// is listed because several distributions ship incorrect DWARF for it.
pub const DEFAULT_BLOCKLIST: [&str; 4] = ["libc", "libgcc", "pthread", "vdso"];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProfilingConf {
    /// Master switch for CPU flame-graph sampling.
    ///
    /// When false, the profiling routes are not registered at all — they
    /// are absent, not present-and-forbidden.
    pub cpu_enabled: bool,

    /// Sampling frequency in Hz, clamped to `1..=1000`.
    pub cpu_frequency: u32,

    /// Longest accepted sampling window in seconds.
    ///
    /// Requests asking for more are rejected with the effective cap
    /// instead of being silently truncated.
    pub max_duration_secs: u64,

    /// Frame modules excluded from the sampled backtrace.
    pub blocklist: Vec<String>,
}

impl Default for ProfilingConf {
    fn default() -> Self {
        Self {
            cpu_enabled: false,
            cpu_frequency: DEFAULT_CPU_FREQUENCY,
            max_duration_secs: DEFAULT_MAX_DURATION_SECS,
            blocklist: DEFAULT_BLOCKLIST.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl ProfilingConf {
    /// Validates the section and rejects configurations that cannot be
    /// honoured.
    ///
    /// Only value ranges are checked here. Whether this binary actually has
    /// a sampler is a separate question, answered by
    /// `curvine_profiling::check_startup` at process startup — this crate
    /// cannot see that feature without inverting the dependency.
    pub fn validate(&mut self) -> Result<(), String> {
        if self.cpu_frequency == 0 || self.cpu_frequency > DEFAULT_CPU_FREQUENCY {
            return Err(format!(
                "profiling.cpu_frequency must be in 1..={}, got {}",
                DEFAULT_CPU_FREQUENCY, self.cpu_frequency
            ));
        }
        if self.max_duration_secs == 0 {
            return Err("profiling.max_duration_secs must be greater than zero".to_string());
        }
        // The blocklist is passed straight to the sampler; a blank entry
        // would silently match every module and empty the profile.
        if self.blocklist.iter().any(|entry| entry.trim().is_empty()) {
            return Err("profiling.blocklist must not contain blank entries".to_string());
        }
        Ok(())
    }

    /// Clamps a requested window to the configured maximum.
    ///
    /// Returns the effective duration so the caller can echo it back
    /// rather than implying the request was honoured verbatim.
    pub fn clamp_duration(&self, requested_secs: Option<u64>) -> u64 {
        requested_secs
            .unwrap_or(self.max_duration_secs)
            .min(self.max_duration_secs)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn defaults_are_disabled() {
        let conf = ProfilingConf::default();
        assert!(!conf.cpu_enabled);
        assert_eq!(conf.cpu_frequency, DEFAULT_CPU_FREQUENCY);
        assert_eq!(conf.max_duration_secs, DEFAULT_MAX_DURATION_SECS);
        assert_eq!(conf.blocklist, DEFAULT_BLOCKLIST);
    }

    #[test]
    fn enabled_config_still_validates() {
        // The feature gate is the sampler crate's concern, so a well-formed
        // enabled config must pass here in every build.
        let mut conf = ProfilingConf {
            cpu_enabled: true,
            ..Default::default()
        };
        assert!(conf.validate().is_ok());
        assert!(conf.cpu_enabled);
    }

    #[test]
    fn rejects_out_of_range_frequency() {
        for frequency in [0, DEFAULT_CPU_FREQUENCY + 1] {
            let mut conf = ProfilingConf {
                cpu_frequency: frequency,
                ..Default::default()
            };
            assert!(conf.validate().is_err(), "frequency {frequency}");
        }
    }

    #[test]
    fn rejects_zero_duration_and_blank_blocklist_entry() {
        let mut zero = ProfilingConf {
            max_duration_secs: 0,
            ..Default::default()
        };
        assert!(zero.validate().is_err());

        let mut blank = ProfilingConf {
            blocklist: vec!["libc".to_string(), "  ".to_string()],
            ..Default::default()
        };
        assert!(blank.validate().is_err());
    }

    #[test]
    fn clamps_requested_duration() {
        let conf = ProfilingConf {
            max_duration_secs: 30,
            ..Default::default()
        };
        assert_eq!(conf.clamp_duration(None), 30);
        assert_eq!(conf.clamp_duration(Some(5)), 5);
        assert_eq!(conf.clamp_duration(Some(300)), 30);
    }
}
