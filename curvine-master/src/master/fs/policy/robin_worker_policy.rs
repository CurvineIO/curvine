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
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{WorkerAddress, WorkerInfo};
use curvine_runtime::sync::AtomicLen;
use indexmap::IndexMap;

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

        // Worker membership can shrink between selections. Normalize the
        // remembered cursor before indexing into the current worker set.
        let start_index = self.index.get() % workers.len();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: u32) -> WorkerInfo {
        let mut worker = WorkerInfo::default();
        worker.address.worker_id = id;
        worker
    }

    #[test]
    fn choose_wraps_cursor_after_worker_removal() {
        let policy = RobinWorkerPolicy::new();
        let mut workers = IndexMap::new();
        workers.insert(1, worker(1));
        workers.insert(2, worker(2));

        assert_eq!(
            policy
                .choose(&workers, ChooseContext::with_num(1, 0, vec![]))
                .unwrap()[0]
                .worker_id,
            1
        );

        workers.shift_remove(&1);
        assert_eq!(
            policy
                .choose(&workers, ChooseContext::with_num(1, 0, vec![]))
                .unwrap()[0]
                .worker_id,
            2
        );
    }
}
