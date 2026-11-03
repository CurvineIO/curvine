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

    pub fn fence(&self) {
        let mut state = self.state.write();
        state.required_index = state.applied_index.saturating_add(1);
        state.catching_up = true;
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
        !state.catching_up && state.applied_index >= state.required_index
    }

    pub fn ensure_current(&self) -> FsResult<()> {
        let state = self.state.read();
        if !state.catching_up && state.applied_index >= state.required_index {
            return Ok(());
        }
        Err(FsError::not_leader(format!(
            "master metadata is catching up with committed raft logs: applied_index={}, required_index={}",
            state.applied_index, state.required_index
        )))
    }
}
