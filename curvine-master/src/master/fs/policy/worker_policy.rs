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

use crate::master::fs::policy::ChooseContext;
use curvine_common::state::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;
use orpc::CommonResult;

/// Worker selects a policy
pub trait WorkerPolicy: Send + Sync {
    /// Select multiple workers based on block information
    fn choose(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        ctx: ChooseContext,
    ) -> CommonResult<Vec<WorkerAddress>>;

    /// Select a specified number of workers without relying on block information
    ///
    /// # Arguments
    /// * `count` - The number of workers to select, default is 1, minimum is 1
    fn choose_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: Option<usize>,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let ctx = ChooseContext::with_num(count.unwrap_or(1) as u16, 0, exclude_workers);
        self.choose(workers, ctx)
    }
}
