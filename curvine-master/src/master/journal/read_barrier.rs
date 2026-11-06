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

#[derive(Default)]
struct ReadState {
    catching_up: bool,
    role_fenced: bool,
    role_epoch: u64,
    required_index: u64,
    applied_index: u64,
}

/// Prevents metadata reads from observing a partially replayed namespace.
#[derive(Default)]
pub struct JournalReadBarrier {
    state: RwLock<ReadState>,
}

impl JournalReadBarrier {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin_catch_up(&self, applied_index: u64, required_index: u64) {
        let mut state = self.state.write();
        state.applied_index = applied_index;
        state.required_index = required_index;
        state.catching_up = applied_index < required_index;
    }

    pub fn require_catch_up(&self, required_index: u64) {
        let mut state = self.state.write();
        state.required_index = state.required_index.max(required_index);
        state.catching_up = state.applied_index < state.required_index;
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
        publish();
        state.role_epoch
    }

    pub(crate) fn is_role_epoch(&self, epoch: u64) -> bool {
        self.state.read().role_epoch == epoch
    }

    /// Publish leadership and clear only the fence installed by this promotion.
    pub(crate) fn unfence_if_epoch(&self, epoch: u64, publish: impl FnOnce()) -> bool {
        let mut state = self.state.write();
        if state.role_epoch != epoch {
            return false;
        }
        publish();
        state.role_fenced = false;
        true
    }

    /// Called only after leader promotion successfully catches up metadata.
    pub fn unfence(&self) {
        self.state.write().role_fenced = false;
    }

    pub fn advance_applied(&self, applied_index: u64) {
        let mut state = self.state.write();
        if !state.catching_up {
            state.applied_index = state.applied_index.max(applied_index);
        }
    }

    pub fn advance_catch_up(&self, applied_index: u64) {
        let mut state = self.state.write();
        state.applied_index = state.applied_index.max(applied_index);
        state.catching_up = state.applied_index < state.required_index;
    }

    pub fn is_current(&self) -> bool {
        let state = self.state.read();
        !state.role_fenced && !state.catching_up && state.applied_index >= state.required_index
    }

    pub fn ensure_current(&self) -> FsResult<()> {
        self.current_epoch().map(|_| ())
    }

    /// Capture the current role generation together with the admission check.
    pub fn current_epoch(&self) -> FsResult<u64> {
        let state = self.state.read();
        if !state.role_fenced && !state.catching_up && state.applied_index >= state.required_index {
            return Ok(state.role_epoch);
        }
        Err(FsError::not_leader(format!(
            "master metadata is fenced or catching up: role_fenced={}, applied_index={}, required_index={}",
            state.role_fenced, state.applied_index, state.required_index
        )))
    }

    /// Reject a request that waited across a role fence, even if leadership
    /// was regained before it reached the commit gate.
    pub fn ensure_epoch(&self, expected: u64) -> FsResult<()> {
        let state = self.state.read();
        if !state.role_fenced
            && !state.catching_up
            && state.applied_index >= state.required_index
            && state.role_epoch == expected
        {
            return Ok(());
        }
        Err(FsError::not_leader(
            "master metadata role changed while waiting for admission",
        ))
    }
}
