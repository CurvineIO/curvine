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

use crate::fs::state::NodeState;
use crate::{FuseError, FuseResult};
use curvine_common::executor::ScheduledExecutor;
use orpc::runtime::LoopTask;
use std::sync::{Arc, Weak};

pub struct CleanerTask {
    state: Weak<NodeState>,
}

impl CleanerTask {
    pub fn start(interval_ms: u64, state: Arc<NodeState>) -> FuseResult<()> {
        if interval_ms == 0 {
            return Ok(());
        }

        let task = CleanerTask {
            state: Arc::downgrade(&state),
        };

        let interval_ms = (interval_ms / 2).max(1);
        let executor = ScheduledExecutor::new("dcache-cleaner", interval_ms);
        executor.start(task)?;
        Ok(())
    }
}

impl LoopTask for CleanerTask {
    type Error = FuseError;

    fn run(&self) -> Result<(), Self::Error> {
        match self.state.upgrade() {
            Some(state) => state.clear(),
            None => Ok(()),
        }
    }

    fn terminate(&self) -> bool {
        self.state.strong_count() == 0
    }
}
