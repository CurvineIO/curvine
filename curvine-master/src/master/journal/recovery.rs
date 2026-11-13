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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// What the apply path does with an error. Transient faults retry in the
/// background. Only damage that would serve a wrong namespace enters safe mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyDisposition {
    Retry,
    EnterSafeMode,
}

pub(crate) fn classify_apply_error(message: &str) -> ApplyDisposition {
    let lower = message.to_ascii_lowercase();
    if is_metadata_damage(&lower) {
        ApplyDisposition::EnterSafeMode
    } else {
        ApplyDisposition::Retry
    }
}

fn is_metadata_damage(lower: &str) -> bool {
    lower.contains("corrupt")
        || lower.contains("checksum")
        || lower.contains("deserialize")
        || lower.contains("failed to decode journal batch")
        || lower.contains("invalid directory attributes")
        || lower.contains("inode record id mismatch")
        || lower.contains("invariant")
        || lower.contains("poisoned")
}

/// Permission and credential failures will not succeed by repeating the same
/// UFS call. Transport and "no worker" errors stay on the in-log retry path.
pub(crate) fn is_permanent_ufs_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("permission")
        || lower.contains("access denied")
        || lower.contains("credential")
        || lower.contains("unauthorized")
        || lower.contains("not authorized")
        || lower.contains("forbidden")
}

fn keeps_ufs_in_log(lower: &str) -> bool {
    lower.contains("no workers")
        || lower.contains("notleader")
        || lower.contains("leader not ready")
        || lower.contains("leadernotready")
        || lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("deadline")
        || lower.contains("connection")
        || lower.contains("refused")
        || lower.contains("reset by peer")
        || lower.contains("broken pipe")
}

/// Move a failed UFS op out of the raft log only when retrying it there cannot
/// succeed. Transient errors, including an empty worker map, stay put.
pub(crate) fn should_defer_ufs(message: &str, failures: u64, max_retry_num: u64) -> bool {
    let lower = message.to_ascii_lowercase();
    if is_metadata_damage(&lower) || keeps_ufs_in_log(&lower) {
        return false;
    }
    if is_permanent_ufs_error(message) {
        return true;
    }
    max_retry_num > 0 && failures >= max_retry_num
}

/// Shared recovery flags for the journal apply loop, UFS coordinator, writer,
/// and HTTP probes. Clone shares the same atomics.
#[derive(Clone)]
pub struct RecoveryGate {
    inner: Arc<RecoveryInner>,
}

struct RecoveryInner {
    safe_mode: AtomicBool,
    writer_failed: AtomicBool,
    ufs_caught_up: AtomicBool,
    generation: AtomicU64,
    pending: AtomicU64,
    last_error: Mutex<String>,
    failure_seq: AtomicU64,
}

impl RecoveryGate {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RecoveryInner {
                safe_mode: AtomicBool::new(false),
                writer_failed: AtomicBool::new(false),
                ufs_caught_up: AtomicBool::new(false),
                generation: AtomicU64::new(1),
                pending: AtomicU64::new(0),
                last_error: Mutex::new(String::new()),
                failure_seq: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn is_safe_mode(&self) -> bool {
        self.inner.safe_mode.load(Ordering::Acquire)
    }

    pub(crate) fn failure_seq(&self) -> u64 {
        self.inner.failure_seq.load(Ordering::Acquire)
    }

    pub(crate) fn enter_safe_mode(&self, reason: &str) {
        self.note_error(reason);
        self.inner.safe_mode.store(true, Ordering::Release);
        self.inner.ufs_caught_up.store(false, Ordering::Release);
        log::error!("master entered safe mode: {reason}");
    }

    pub(crate) fn record_failure(&self) {
        self.inner.failure_seq.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn mark_writer_failed(&self) {
        self.inner.writer_failed.store(true, Ordering::Release);
    }

    pub(crate) fn rejects_writes(&self) -> bool {
        self.is_safe_mode() || self.inner.writer_failed.load(Ordering::Acquire)
    }

    pub(crate) fn bump_generation(&self) -> u64 {
        self.inner.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(crate) fn generation(&self) -> u64 {
        self.inner.generation.load(Ordering::Acquire)
    }

    pub(crate) fn set_ufs_caught_up(&self, caught_up: bool) {
        self.inner.ufs_caught_up.store(caught_up, Ordering::Release);
    }

    pub(crate) fn ufs_caught_up(&self) -> bool {
        self.inner.ufs_caught_up.load(Ordering::Acquire)
    }

    pub(crate) fn set_pending(&self, pending: u64) {
        self.inner.pending.store(pending, Ordering::Release);
    }

    pub(crate) fn pending(&self) -> u64 {
        self.inner.pending.load(Ordering::Acquire)
    }

    /// Returns the most recently recorded source error, if any. Used to
    /// distinguish a holder retirement that already has a recorded source
    /// error from a panic isolation that has none.
    pub(crate) fn last_error(&self) -> Option<String> {
        self.inner
            .last_error
            .lock()
            .ok()
            .filter(|slot| !slot.is_empty())
            .map(|slot| slot.clone())
    }

    pub(crate) fn note_error(&self, error: &str) {
        if let Ok(mut slot) = self.inner.last_error.lock() {
            slot.clear();
            slot.push_str(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_worker_and_raft_errors_retry() {
        for message in [
            "No workers available",
            "No eligible workers for weighted selection",
            "NotLeader",
            "LeaderNotReady",
            "Connection refused",
            "deadline has elapsed",
            "timed out",
        ] {
            assert_eq!(
                classify_apply_error(message),
                ApplyDisposition::Retry,
                "{message}"
            );
        }
    }

    #[test]
    fn metadata_damage_enters_safe_mode() {
        for message in [
            "failed to deserialize journal batch",
            "journal entry corrupt",
            "checksum mismatch",
            "metadata invariant broken",
        ] {
            assert_eq!(
                classify_apply_error(message),
                ApplyDisposition::EnterSafeMode,
                "{message}"
            );
        }
    }

    #[test]
    fn disk_and_frame_errors_stay_retryable() {
        for message in [
            "rocksdb: IO error: No space left on device",
            "failed to decode rpc frame",
        ] {
            assert_eq!(
                classify_apply_error(message),
                ApplyDisposition::Retry,
                "{message}"
            );
        }
    }

    #[test]
    fn permanent_ufs_failure_defers_and_worker_gaps_do_not() {
        assert!(should_defer_ufs("UFS permission denied", 1, 1000));
        assert!(should_defer_ufs(
            "export job failed: credential rejected",
            1,
            1000
        ));
        assert!(!should_defer_ufs("No workers available", 1000, 1000));
        assert!(!should_defer_ufs("Connection refused", 1000, 1000));
        assert!(!should_defer_ufs("NotLeader", 5, 1000));
        assert!(!should_defer_ufs(
            "failed to deserialize journal batch",
            1000,
            1000
        ));
        assert!(!should_defer_ufs("export job failed: worker busy", 3, 1000));
        assert!(should_defer_ufs(
            "export job failed: worker busy",
            1000,
            1000
        ));
    }

    #[test]
    fn safe_mode_rejects_writes_and_clears_caught_up() {
        let gate = RecoveryGate::new();
        gate.set_ufs_caught_up(true);
        assert!(!gate.rejects_writes());
        gate.enter_safe_mode("journal entry checksum mismatch");
        assert!(gate.is_safe_mode());
        assert!(gate.rejects_writes());
        assert!(!gate.ufs_caught_up());
    }

    #[test]
    fn generation_bumps_cancel_stale_replay() {
        let gate = RecoveryGate::new();
        let first = gate.generation();
        let second = gate.bump_generation();
        assert_ne!(first, second);
        assert_eq!(gate.generation(), second);
    }
}
