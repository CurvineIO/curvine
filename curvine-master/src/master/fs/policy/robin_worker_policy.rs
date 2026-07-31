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

use crate::master::fs::policy::{ChooseContext, WorkerPolicy};
use curvine_common::state::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;
use orpc::sync::AtomicLen;
use orpc::{err_box, CommonResult};

// Poll selector.
pub struct RobinWorkerPolicy {
    index: AtomicLen,
}

impl Default for RobinWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinWorkerPolicy {
    pub fn new() -> Self {
        Self {
            index: AtomicLen::new(0),
        }
    }
}

impl WorkerPolicy for RobinWorkerPolicy {
    fn choose(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        mut ctx: ChooseContext,
    ) -> CommonResult<Vec<WorkerAddress>> {
        if workers.is_empty() {
            return err_box!("No workers available");
        }
        if ctx.replicas < 1 {
            return err_box!("The number of replicas cannot be 0");
        }

        let start_index = self.index.get();
        let mut index = start_index;
        let mut res = vec![];

        while res.len() < ctx.replicas as usize {
            let Some((id, worker)) = workers.get_index(index) else {
                return err_box!(
                    "worker selection index {} out of range, workers={}",
                    index,
                    workers.len()
                );
            };

            if !ctx.exclude_workers.contains(id)
                && worker.available > ctx.block_size
                && worker.is_live()
            {
                ctx.exclude_workers.insert(*id);
                res.push(worker.address.clone())
            }

            index = (index + 1) % workers.len();
            if index == start_index {
                break;
            }
        }

        self.index.set(index);
        Ok(res)
    }
}
