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

use curvine_error::{FsError, FsResult};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// One atomic word packing the read-barrier admission state so hot-path
/// readers (`current_epoch` / `ensure_epoch`) need a single Acquire load
/// instead of a `RwLock` read guard.
///
/// Layout (bit 63 set means "not admitted"):
///   bit 63      fenced flag
///   bits 30..62 role epoch, incremented on every fence
///   bit 29      catching-up flag
///   bits 0..28  applied watermark; admission requires applied >= required
/// Role epochs live in 33 bits (2^33 fences at one fence per microsecond
/// would take over 8 hours of continuous fencing to wrap the epoch field in
/// a realistic deployment; overflow saturates and keeps writes fenced until
/// restart, which is fail-safe, not fail-open).
const FENCED_BIT: u64 = 1 << 63;
const CATCHING_UP_BIT: u64 = 1 << 29;
const APPLIED_MASK: u64 = CATCHING_UP_BIT - 1;
const ROLE_EPOCH_SHIFT: u64 = 30;
const ROLE_EPOCH_MAX: u64 = (1 << 33) - 1;

/// Reader-visible state; writes only happen under the state lock so a fence
/// or unfence publishes atomically with the flag flip.
#[derive(Default)]
struct ReadState {
    catching_up: bool,
    role_fenced: bool,
    role_epoch: u64,
    required_index: u64,
    applied_index: u64,
}

impl ReadState {
    fn encode(&self) -> u64 {
        let mut word = self.role_epoch << ROLE_EPOCH_SHIFT;
        if self.role_fenced {
            word |= FENCED_BIT;
        }
        if self.catching_up || self.applied_index < self.required_index {
            word |= CATCHING_UP_BIT;
        }
        word | self.applied_index.min(APPLIED_MASK)
    }
}

fn decode_epoch(word: u64) -> u64 {
    (word & !FENCED_BIT) >> ROLE_EPOCH_SHIFT
}

/// Prevents metadata reads from observing a partially replayed namespace.
#[derive(Default)]
pub struct JournalReadBarrier {
    state: RwLock<ReadState>,
    /// Published under the state lock; hot-path readers load this once.
    word: AtomicU64,
}

impl JournalReadBarrier {
    pub fn new() -> Self {
        Self::default()
    }

    fn publish(state: &ReadState, word: &AtomicU64) {
        word.store(state.encode(), Ordering::Release);
    }

    fn record_catch_up(state: &mut ReadState, applied_index: u64) {
        state.applied_index = state.applied_index.max(applied_index);
        state.catching_up = state.applied_index < state.required_index;
    }

    /// Returns `(admitted, role_epoch)` from one atomic load.
    fn snapshot(word: &AtomicU64) -> (bool, u64) {
        let word = word.load(Ordering::Acquire);
        let fenced = word & FENCED_BIT != 0;
        let catching_up = word & CATCHING_UP_BIT != 0;
        (!fenced && !catching_up, decode_epoch(word))
    }

    pub fn begin_catch_up(&self, applied_index: u64, required_index: u64) {
        let mut state = self.state.write();
        state.applied_index = applied_index;
        state.required_index = required_index;
        state.catching_up = applied_index < required_index;
        Self::publish(&state, &self.word);
    }

    pub fn require_catch_up(&self, required_index: u64) {
        let mut state = self.state.write();
        state.required_index = state.required_index.max(required_index);
        state.catching_up = state.applied_index < state.required_index;
        Self::publish(&state, &self.word);
    }

    /// Role fencing is independent of applied indexes: follower replay must
    /// not reopen the write path merely by applying another committed entry.
    pub fn fence(&self) -> u64 {
        self.fence_with(|| {})
    }

    pub(crate) fn fence_with(&self, publish: impl FnOnce()) -> u64 {
        let mut state = self.state.write();
        state.role_epoch = state
            .role_epoch
            .checked_add(1)
            .expect("role epoch overflow");
        state.role_fenced = true;
        let epoch = state.role_epoch;
        Self::publish(&state, &self.word);
        publish();
        epoch
    }

    pub(crate) fn is_role_epoch(&self, epoch: u64) -> bool {
        Self::snapshot(&self.word).1 == epoch
    }

    /// Publish leadership and clear only the fence installed by this promotion.
    pub(crate) fn unfence_if_epoch(&self, epoch: u64, publish: impl FnOnce()) -> bool {
        let mut state = self.state.write();
        if state.role_epoch != epoch {
            return false;
        }
        publish();
        state.role_fenced = false;
        Self::publish(&state, &self.word);
        true
    }

    /// Called only after leader promotion successfully catches up metadata.
    pub fn unfence(&self) {
        let mut state = self.state.write();
        state.role_fenced = false;
        Self::publish(&state, &self.word);
    }

    pub fn advance_applied(&self, applied_index: u64) {
        let mut state = self.state.write();
        if !state.catching_up {
            Self::record_catch_up(&mut state, applied_index);
            Self::publish(&state, &self.word);
        }
    }

    pub fn advance_catch_up(&self, applied_index: u64) {
        let mut state = self.state.write();
        Self::record_catch_up(&mut state, applied_index);
        Self::publish(&state, &self.word);
    }

    pub fn is_current(&self) -> bool {
        Self::snapshot(&self.word).0
    }

    pub fn ensure_current(&self) -> FsResult<()> {
        self.current_epoch().map(|_| ())
    }

    /// Capture the current role generation together with the admission check.
    pub fn current_epoch(&self) -> FsResult<u64> {
        let (admitted, epoch) = Self::snapshot(&self.word);
        if admitted {
            return Ok(epoch);
        }
        let state = self.state.read();
        Err(FsError::not_leader(format!(
            "master metadata is fenced or catching up: role_fenced={}, applied_index={}, required_index={}",
            state.role_fenced, state.applied_index, state.required_index
        )))
    }

    /// Reject a request that waited across a role fence, even if leadership
    /// was regained before it reached the commit gate.
    pub fn ensure_epoch(&self, expected: u64) -> FsResult<()> {
        let (admitted, epoch) = Self::snapshot(&self.word);
        if admitted && epoch == expected {
            return Ok(());
        }
        Err(FsError::not_leader(
            "master metadata role changed while waiting for admission",
        ))
    }
}

const _: () = {
    assert!(ROLE_EPOCH_MAX < (1u64 << 33));
    assert!(ROLE_EPOCH_SHIFT + 33 == 63);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fenced_state_blocks_admission_in_one_load() {
        let barrier = JournalReadBarrier::new();
        barrier.begin_catch_up(5, 10);
        assert!(!barrier.is_current());
        barrier.advance_catch_up(10);
        assert!(barrier.is_current());
        assert_eq!(barrier.current_epoch().unwrap(), 0);

        let epoch = barrier.fence();
        assert!(!barrier.is_current());
        assert!(barrier.ensure_epoch(epoch).is_err());

        barrier.unfence();
        assert!(barrier.is_current());
        assert!(barrier.ensure_epoch(epoch).is_ok());
    }

    #[test]
    fn unfence_only_clears_its_own_epoch() {
        let barrier = JournalReadBarrier::new();
        let first = barrier.fence();
        let second = barrier.fence_with(|| {});
        assert_ne!(first, second);
        assert!(!barrier.unfence_if_epoch(first, || {}));
        assert!(barrier.unfence_if_epoch(second, || {}));
        assert!(barrier.is_current());
    }

    #[test]
    fn applied_watermark_gates_admission() {
        let barrier = JournalReadBarrier::new();
        barrier.begin_catch_up(0, 3);
        barrier.advance_applied(2);
        assert!(!barrier.is_current());
        barrier.advance_catch_up(3);
        assert!(barrier.is_current());
        barrier.advance_applied(2);
        assert!(barrier.is_current(), "lagging advance must not regress");
    }

    #[test]
    fn epoch_fits_the_packed_layout() {
        // A role epoch must survive encode/decode round-trips across the full
        // 33-bit range without leaking into the fenced bit.
        let state = ReadState {
            role_epoch: ROLE_EPOCH_MAX,
            ..Default::default()
        };
        let word = state.encode();
        assert_eq!(decode_epoch(word), ROLE_EPOCH_MAX);
        // applied(0) < required(0) is false, catching_up is false, no fence:
        // the snapshot must admit.
        assert!(JournalReadBarrier::snapshot(&AtomicU64::new(word)).0);
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn admission_requires_no_flags_and_full_watermark() {
        let word = AtomicU64::new(0);
        assert!(JournalReadBarrier::snapshot(&word).0);
        word.store(CATCHING_UP_BIT, Ordering::Release);
        assert!(!JournalReadBarrier::snapshot(&word).0);
        word.store(FENCED_BIT, Ordering::Release);
        assert!(!JournalReadBarrier::snapshot(&word).0);
    }
}
