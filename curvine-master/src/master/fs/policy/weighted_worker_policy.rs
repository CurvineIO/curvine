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
use rand::{thread_rng, Rng};
use std::collections::HashSet;

/// Selects workers randomly in proportion to their configured weights.
///
/// Selection is without replacement, so a worker can host at most one replica
/// of a block. Workers with weight 0 remain registered but receive no new data.
pub struct WeightedWorkerPolicy {}

impl Default for WeightedWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl WeightedWorkerPolicy {
    pub fn new() -> Self {
        Self {}
    }

    fn is_eligible(
        id: &u32,
        worker: &WorkerInfo,
        exclude_workers: &HashSet<u32>,
        selected_ids: &HashSet<u32>,
        min_available: i64,
    ) -> bool {
        worker.is_live()
            && !exclude_workers.contains(id)
            && !selected_ids.contains(id)
            && worker.available >= min_available
            && worker.weight > 0
    }

    fn select_weighted_workers<R: Rng + ?Sized>(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: usize,
        exclude_workers: &HashSet<u32>,
        min_available: i64,
        rng: &mut R,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let mut selected = Vec::with_capacity(count.min(workers.len()));
        let mut selected_ids = HashSet::with_capacity(count);

        // Treat each worker as a virtual range whose length is its weight.
        // Calculate the full range once, then shrink it as workers are selected.
        let mut total_weight = workers
            .iter()
            .filter(|(id, worker)| {
                Self::is_eligible(id, worker, exclude_workers, &selected_ids, min_available)
            })
            .map(|(_, worker)| worker.weight as u64)
            .sum::<u64>();

        if total_weight == 0 {
            return err_box!("No eligible workers for weighted selection");
        }

        for _ in 0..count {
            if total_weight == 0 {
                break;
            }

            // The second pass locates the worker containing the random offset.
            let mut target = rng.gen_range(0..total_weight);
            let mut found = false;
            for (id, worker) in workers {
                if !Self::is_eligible(id, worker, exclude_workers, &selected_ids, min_available) {
                    continue;
                }

                let weight = worker.weight as u64;
                if target < weight {
                    selected_ids.insert(*id);
                    selected.push(worker.address.clone());
                    total_weight -= weight;
                    found = true;
                    break;
                }
                target -= weight;
            }

            if !found {
                return err_box!("Worker weight state is inconsistent");
            }
        }

        Ok(selected)
    }
}

impl WorkerPolicy for WeightedWorkerPolicy {
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

        self.select_weighted_workers(
            workers,
            ctx.replicas as usize,
            &ctx.exclude_workers,
            ctx.block_size,
            &mut thread_rng(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn worker(id: u32, weight: u32) -> WorkerInfo {
        WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            weight,
            capacity: 1024,
            available: 1024,
            ..Default::default()
        }
    }

    #[test]
    fn weighted_selection_respects_weights() {
        let policy = WeightedWorkerPolicy::new();
        let workers = IndexMap::from([(1, worker(1, 1)), (2, worker(2, 3)), (3, worker(3, 6))]);
        let mut rng = StdRng::seed_from_u64(42);
        let mut counts = [0_u32; 3];

        for _ in 0..10_000 {
            let selected = policy
                .select_weighted_workers(&workers, 1, &HashSet::new(), 0, &mut rng)
                .unwrap();
            counts[selected[0].worker_id as usize - 1] += 1;
        }

        let actual = counts.map(|count| count as f64 / 10_000.0);
        let expected = [0.1, 0.3, 0.6];
        for (actual, expected) in actual.into_iter().zip(expected) {
            assert!(
                (actual - expected).abs() < 0.02,
                "actual ratio {actual:.3} differs from expected {expected:.3}"
            );
        }
    }

    #[test]
    fn weighted_selection_does_not_duplicate_replicas() {
        let policy = WeightedWorkerPolicy::new();
        let workers = IndexMap::from([(1, worker(1, 1)), (2, worker(2, 10)), (3, worker(3, 100))]);
        let mut rng = StdRng::seed_from_u64(7);

        let selected = policy
            .select_weighted_workers(&workers, 3, &HashSet::new(), 0, &mut rng)
            .unwrap();
        let selected_ids: HashSet<u32> = selected.iter().map(|worker| worker.worker_id).collect();

        assert_eq!(selected.len(), 3);
        assert_eq!(selected_ids.len(), 3);
    }

    #[test]
    fn weighted_selection_filters_ineligible_workers() {
        let policy = WeightedWorkerPolicy::new();
        let mut no_space = worker(4, 100);
        no_space.available = 10;
        let workers = IndexMap::from([
            (1, worker(1, 1)),
            (2, worker(2, 10)),
            (3, worker(3, 0)),
            (4, no_space),
        ]);
        let mut rng = StdRng::seed_from_u64(7);

        let selected = policy
            .select_weighted_workers(&workers, 3, &HashSet::from([2]), 20, &mut rng)
            .unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].worker_id, 1);
    }

    #[test]
    fn weighted_selection_accepts_exact_available_space() {
        let policy = WeightedWorkerPolicy::new();
        let mut exact_space = worker(1, 1);
        exact_space.available = 20;
        let workers = IndexMap::from([(1, exact_space)]);
        let mut rng = StdRng::seed_from_u64(9);

        let selected = policy
            .select_weighted_workers(&workers, 1, &HashSet::new(), 20, &mut rng)
            .unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].worker_id, 1);
    }
}
