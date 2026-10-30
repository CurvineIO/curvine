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

//! Health primitives shared by the worker (facts), master (routing),
//! client/transfer (retry) and `cv` commands (display).
//!
//! Design contract: the worker only *declares* facts. Whether a worker may
//! receive new allocations is a master decision that consumes these facts.

use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU8, Ordering};

/// Health state of one target (a data dir today, a worker node later).
///
/// `Suspect` also covers the recovery window: the target failed but has not
/// reached the failure threshold, or it is unusable and collecting the
/// consecutive successes required to come back.
#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, IntoPrimitive, FromPrimitive,
)]
pub enum HealthState {
    #[default]
    Healthy = 0,

    Suspect = 1,

    /// Failed: must not receive new allocations. Reads keep working.
    Unusable = 2,
}

impl HealthState {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            HealthState::Healthy => "healthy",
            HealthState::Suspect => "suspect",
            HealthState::Unusable => "unusable",
        }
    }

    /// Whether the target may take new writes.
    pub fn is_usable(&self) -> bool {
        !matches!(self, HealthState::Unusable)
    }
}

impl std::fmt::Display for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str_name())
    }
}

/// Why a target is unhealthy.
///
/// Deliberately small: only faults that the current checkers can actually
/// produce are listed. Unknown errno values collapse into [`FaultKind::Unknown`]
/// and are treated conservatively downstream.
#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, IntoPrimitive, FromPrimitive,
)]
pub enum FaultKind {
    #[default]
    Unknown = 0,

    IoError = 1,

    /// Network-storage session gone (ESTALE).
    Stale = 2,

    /// The mount is read-only.
    ReadOnly = 3,

    /// Quota or capacity exhausted. This is a capacity signal, not a device
    /// fault: it must not quarantine the directory.
    SpaceOut = 4,

    DeviceChanged = 5,

    DeviceGone = 6,

    PathMissing = 7,

    NotDir = 8,

    Permission = 9,
}

impl FaultKind {
    pub fn as_str_name(&self) -> &'static str {
        match self {
            FaultKind::Unknown => "unknown",
            FaultKind::IoError => "io_error",
            FaultKind::Stale => "stale",
            FaultKind::ReadOnly => "read_only",
            FaultKind::SpaceOut => "space_out",
            FaultKind::DeviceChanged => "device_changed",
            FaultKind::DeviceGone => "device_gone",
            FaultKind::PathMissing => "path_missing",
            FaultKind::NotDir => "not_dir",
            FaultKind::Permission => "permission",
        }
    }

    /// Classify a filesystem error without string matching.
    pub fn from_io_error(error: &std::io::Error) -> Self {
        match error.raw_os_error() {
            Some(5) => FaultKind::IoError,               // EIO
            Some(116) => FaultKind::Stale,               // ESTALE
            Some(30) => FaultKind::ReadOnly,             // EROFS
            Some(28) | Some(122) => FaultKind::SpaceOut, // ENOSPC / EDQUOT
            Some(13) | Some(1) => FaultKind::Permission, // EACCES / EPERM
            _ => match error.kind() {
                std::io::ErrorKind::NotFound => FaultKind::PathMissing,
                std::io::ErrorKind::PermissionDenied => FaultKind::Permission,
                _ => FaultKind::Unknown,
            },
        }
    }
}

impl std::fmt::Display for FaultKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str_name())
    }
}

/// Result of one health probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeOutcome {
    Ok,

    Failed { kind: FaultKind, detail: String },
}

impl ProbeOutcome {
    pub fn failed(kind: FaultKind, detail: impl Into<String>) -> Self {
        Self::Failed {
            kind,
            detail: detail.into(),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, ProbeOutcome::Ok)
    }

    pub fn fault(&self) -> Option<(FaultKind, &str)> {
        match self {
            ProbeOutcome::Ok => None,
            ProbeOutcome::Failed { kind, detail } => Some((*kind, detail.as_str())),
        }
    }
}

/// Thresholds shared by every checker so that new checks do not reimplement
/// debounce/backoff logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthPolicy {
    /// Consecutive failures required to move a target to `Unusable`.
    pub failures_to_unusable: u32,

    /// Consecutive successes required to bring an `Unusable` target back.
    pub successes_to_recover: u32,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            failures_to_unusable: 3,
            successes_to_recover: 5,
        }
    }
}

impl HealthPolicy {
    pub fn new(failures_to_unusable: u32, successes_to_recover: u32) -> Self {
        Self {
            failures_to_unusable: failures_to_unusable.max(1),
            successes_to_recover: successes_to_recover.max(1),
        }
    }
}

/// A state change worth logging exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthTransition {
    pub from: HealthState,
    pub to: HealthState,
    pub fault: FaultKind,
    pub detail: String,
}

impl HealthTransition {
    pub fn is_recovery(&self) -> bool {
        self.to == HealthState::Healthy
    }
}

/// Interior-mutable state machine for a single target.
///
/// One producer writes (the probe loop) and any thread may read the state, so
/// the counters are plain atomics: a rare interleaved observation at worst
/// delays a transition by one probe.
#[derive(Debug, Default)]
pub struct HealthTracker {
    state: AtomicU8,

    failures: AtomicU32,

    successes: AtomicU32,

    /// Fault that made this target unhealthy, and when it started.
    fault: AtomicU8,

    since_ms: AtomicI64,
}

impl HealthTracker {
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(HealthState::Healthy as u8),
            failures: AtomicU32::new(0),
            successes: AtomicU32::new(0),
            fault: AtomicU8::new(FaultKind::Unknown as u8),
            since_ms: AtomicI64::new(0),
        }
    }

    pub fn state(&self) -> HealthState {
        HealthState::from(self.state.load(Ordering::Relaxed))
    }

    pub fn failures(&self) -> u32 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn successes(&self) -> u32 {
        self.successes.load(Ordering::Relaxed)
    }

    pub fn fault(&self) -> FaultKind {
        FaultKind::from(self.fault.load(Ordering::Relaxed))
    }

    /// When the target became unhealthy (0 while healthy).
    pub fn since_ms(&self) -> i64 {
        self.since_ms.load(Ordering::Relaxed)
    }

    pub fn reset(&self, state: HealthState) {
        self.state.store(state as u8, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
        self.successes.store(0, Ordering::Relaxed);
        self.fault
            .store(FaultKind::Unknown as u8, Ordering::Relaxed);
        self.since_ms.store(0, Ordering::Relaxed);
    }

    /// Feed one probe result; returns a transition when the state changes.
    pub fn observe(
        &self,
        outcome: &ProbeOutcome,
        policy: &HealthPolicy,
        now_ms: i64,
    ) -> Option<HealthTransition> {
        let from = self.state();
        match outcome {
            ProbeOutcome::Ok => {
                self.failures.store(0, Ordering::Relaxed);
                if from == HealthState::Unusable {
                    let successes = self.successes.fetch_add(1, Ordering::Relaxed) + 1;
                    if successes < policy.successes_to_recover {
                        return None;
                    }
                    self.successes.store(0, Ordering::Relaxed);
                } else {
                    self.successes.store(0, Ordering::Relaxed);
                }

                if from == HealthState::Healthy {
                    return None;
                }
                self.state
                    .store(HealthState::Healthy as u8, Ordering::Relaxed);
                self.fault
                    .store(FaultKind::Unknown as u8, Ordering::Relaxed);
                self.since_ms.store(0, Ordering::Relaxed);
                Some(HealthTransition {
                    from,
                    to: HealthState::Healthy,
                    fault: FaultKind::Unknown,
                    detail: String::new(),
                })
            }

            ProbeOutcome::Failed { kind, detail } => {
                self.successes.store(0, Ordering::Relaxed);
                let failures = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
                if from == HealthState::Unusable {
                    return None;
                }

                let to = if failures >= policy.failures_to_unusable {
                    HealthState::Unusable
                } else {
                    HealthState::Suspect
                };
                self.state.store(to as u8, Ordering::Relaxed);
                self.fault.store(*kind as u8, Ordering::Relaxed);
                if self.since_ms.load(Ordering::Relaxed) == 0 {
                    self.since_ms.store(now_ms, Ordering::Relaxed);
                }
                if to == from {
                    return None;
                }
                Some(HealthTransition {
                    from,
                    to,
                    fault: *kind,
                    detail: detail.clone(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failed() -> ProbeOutcome {
        ProbeOutcome::failed(FaultKind::IoError, "Input/output error (os error 5)")
    }

    fn ok() -> ProbeOutcome {
        ProbeOutcome::Ok
    }

    #[test]
    fn failures_reach_unusable_after_threshold() {
        let policy = HealthPolicy::new(3, 2);
        let tracker = HealthTracker::new();
        assert_eq!(tracker.state(), HealthState::Healthy);

        let first = tracker.observe(&failed(), &policy, 1).unwrap();
        assert_eq!(
            (first.from, first.to),
            (HealthState::Healthy, HealthState::Suspect)
        );
        assert_eq!(first.fault, FaultKind::IoError);

        // Already suspect: same state, no duplicate transition.
        assert!(tracker.observe(&failed(), &policy, 1).is_none());

        let third = tracker.observe(&failed(), &policy, 1).unwrap();
        assert_eq!(
            (third.from, third.to),
            (HealthState::Suspect, HealthState::Unusable)
        );
        assert_eq!(tracker.state(), HealthState::Unusable);

        // No repeated transition while unusable.
        assert!(tracker.observe(&failed(), &policy, 1).is_none());
    }

    #[test]
    fn success_clears_suspect_immediately() {
        let policy = HealthPolicy::new(3, 2);
        let tracker = HealthTracker::new();
        tracker.observe(&failed(), &policy, 1);
        assert_eq!(tracker.state(), HealthState::Suspect);

        let back = tracker.observe(&ok(), &policy, 2).unwrap();
        assert_eq!(
            (back.from, back.to),
            (HealthState::Suspect, HealthState::Healthy)
        );
        assert_eq!(tracker.failures(), 0);
    }

    #[test]
    fn unusable_requires_consecutive_successes_to_recover() {
        let policy = HealthPolicy::new(1, 3);
        let tracker = HealthTracker::new();
        tracker.observe(&failed(), &policy, 1);
        assert_eq!(tracker.state(), HealthState::Unusable);

        assert!(tracker.observe(&ok(), &policy, 2).is_none());
        assert!(tracker.observe(&ok(), &policy, 2).is_none());

        // A failure in the middle restarts the recovery window.
        assert!(tracker.observe(&failed(), &policy, 1).is_none());
        assert_eq!(tracker.successes(), 0);

        assert!(tracker.observe(&ok(), &policy, 2).is_none());
        assert!(tracker.observe(&ok(), &policy, 2).is_none());
        let recovered = tracker.observe(&ok(), &policy, 2).unwrap();
        assert!(recovered.is_recovery());
        assert_eq!(tracker.state(), HealthState::Healthy);
    }

    #[test]
    fn io_errors_are_classified_without_string_matching() {
        let eio = std::io::Error::from_raw_os_error(5);
        assert_eq!(FaultKind::from_io_error(&eio), FaultKind::IoError);
        let estale = std::io::Error::from_raw_os_error(116);
        assert_eq!(FaultKind::from_io_error(&estale), FaultKind::Stale);
        let missing = std::io::Error::from_raw_os_error(2);
        assert_eq!(FaultKind::from_io_error(&missing), FaultKind::PathMissing);
        let io = std::io::Error::other("boom");
        assert_eq!(FaultKind::from_io_error(&io), FaultKind::Unknown);
    }

    #[test]
    fn space_out_does_not_look_like_a_device_fault() {
        let quota = std::io::Error::from_raw_os_error(122);
        assert_eq!(FaultKind::from_io_error(&quota), FaultKind::SpaceOut);
        assert_eq!(FaultKind::SpaceOut.as_str_name(), "space_out");
    }
}
