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
use orpc::{err_box, CommonResult};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::collections::HashSet;

/// Worker policies are randomly selected
/// This policy randomly selects a specified number of nodes from available Worker nodes
pub struct RandomWorkerPolicy {}

impl Default for RandomWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RandomWorkerPolicy {
    pub fn new() -> Self {
        Self {}
    }

    /// Randomly select the specified number of nodes from the available Worker nodes
    fn select_random_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: usize,
        exclude_workers: &HashSet<u32>,
        min_available: i64,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let mut rng = thread_rng();
        let mut res = Vec::new();

        // Filter out available Worker nodes
        let available_workers: Vec<(&u32, &WorkerInfo)> = workers
            .iter()
            .filter(|(id, worker)| {
                worker.is_live()
                    && !exclude_workers.contains(id)
                    && worker.available > min_available
            })
            .collect();

        if available_workers.is_empty() {
            return err_box!("The cluster does not have enough space");
        }

        // Randomly select the specified number of nodes
        let selected_workers = available_workers.iter().choose_multiple(&mut rng, count);

        for (_, worker) in selected_workers {
            res.push(worker.address.clone());
        }

        if res.is_empty() {
            return err_box!("The cluster does not have enough space");
        }

        Ok(res)
    }
}

impl WorkerPolicy for RandomWorkerPolicy {
    fn choose(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        ctx: ChooseContext,
    ) -> CommonResult<Vec<WorkerAddress>> {
        if workers.is_empty() {
            return err_box!("No workers available");
        }
        if ctx.replicas < 1 {
            return err_box!("The number of replicas cannot be 0");
        }

        self.select_random_workers(
            workers,
            ctx.replicas as usize,
            &ctx.exclude_workers,
            ctx.block_size,
        )
    }
}
