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
use std::collections::HashSet;

/// Load-based Worker Selection Policy
/// This policy selects the most suitable node based on the load of the Worker node
/// Load calculation is based on the available space ratio (available space/total capacity)
pub struct LoadBasedWorkerPolicy {}

impl Default for LoadBasedWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBasedWorkerPolicy {
    pub fn new() -> Self {
        Self {}
    }

    /// Calculate the load score of the Worker node
    /// The higher the score, the lower the load, the more suitable it is to be selected
    fn calculate_score(&self, worker: &WorkerInfo) -> f64 {
        if worker.capacity == 0 {
            return 0.0;
        }
        // Calculate the available space ratio as the base fraction
        // Other load factors can be added here, such as CPU usage, memory usage, etc.
        // Currently only the available space ratio is used as the load metric
        worker.available as f64 / worker.capacity as f64
    }

    /// Select the specified number of workers according to the load
    fn select_workers_by_load(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: usize,
        exclude_workers: Option<&HashSet<u32>>,
        min_available: Option<u64>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let mut available_workers: Vec<(&u32, &WorkerInfo, f64)> = workers
            .iter()
            .filter(|(id, worker)| {
                // Basic state verification: the worker must be active and the effective capacity is greater than 0
                worker.is_live() && worker.capacity > 0 && worker.available >= 0 &&
                // Check if it is in the exclusion list
                match exclude_workers {
                    Some(excluded) => !excluded.contains(id),
                    None => true
                }&&
                // Check whether the available space meets the minimum requirements
                match min_available {
                    Some(min) => worker.available >= min as i64,
                    None => worker.available > 0 // Even if there are no minimum requirements, make sure there is space available
                }
            })
            .map(|(id, worker)| (id, worker, self.calculate_score(worker)))
            .collect();

        if available_workers.is_empty() {
            return err_box!("No available workers found");
        }

        // Sort by load fraction descending order
        available_workers
            .sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        // Select the first count workers
        let selected = available_workers
            .into_iter()
            .take(count)
            .map(|(_, worker, _)| worker.address.clone())
            .collect();

        Ok(selected)
    }
}

impl WorkerPolicy for LoadBasedWorkerPolicy {
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

        self.select_workers_by_load(
            workers,
            ctx.replicas as usize,
            Some(&ctx.exclude_workers),
            Some(ctx.block_size as u64),
        )
    }
}
