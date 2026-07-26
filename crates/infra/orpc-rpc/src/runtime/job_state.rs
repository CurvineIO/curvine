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

use crate::sync::StateCtl;
use num_enum::{FromPrimitive, IntoPrimitive};

#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum JobState {
    #[num_enum(default)]
    Pending,
    Running,
    Canceling,
    Cancelled,
    Finished,
    Failed,
}

// job status controller.
#[derive(Clone)]
pub struct JobCtl(StateCtl);

impl JobCtl {
    pub fn new() -> Self {
        Self(StateCtl::new(JobState::Pending.into()))
    }

    pub fn advance_state(&self, target_state: JobState) {
        self.0.advance_state(target_state)
    }

    pub fn state(&self) -> JobState {
        self.0.state()
    }
}

impl Default for JobCtl {
    fn default() -> Self {
        Self::new()
    }
}
