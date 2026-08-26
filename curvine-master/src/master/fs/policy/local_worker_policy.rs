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

use crate::master::fs::policy::{ChooseContext, WeightedWorkerPolicy, WorkerPolicy};
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{WorkerAddress, WorkerInfo};
use indexmap::IndexMap;

/// A local worker is preferred for the first replica, and the remaining replicas are assigned
/// by the weighted policy. If no eligible local worker exists, all replicas are assigned by
/// the weighted policy. Local workers with weight 0 are skipped so that drained nodes
/// receive no new data.
pub struct LocalWorkerPolicy {
    inner: WeightedWorkerPolicy,
}

impl Default for LocalWorkerPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalWorkerPolicy {
    pub fn new() -> Self {
        Self {
            inner: WeightedWorkerPolicy::new(),
        }
    }
}

impl WorkerPolicy for LocalWorkerPolicy {
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

        let mut res = vec![];

        // step1: Detect whether the local worker exists
        for (id, worker) in workers {
            if !ctx.exclude_workers.contains(id)
                && worker.address.is_local(&ctx.client_host)
                && worker.available > ctx.block_size
                && worker.weight > 0
                && worker.is_live()
            {
                res.push(worker.address.clone());
                ctx.exclude_workers.insert(*id);
                break;
            }
        }

        let replicas = ctx.replicas;
        if res.len() < replicas as usize {
            match self.inner.choose(workers, ctx) {
                Ok(remote_res) => {
                    for item in remote_res {
                        res.push(item);
                        if res.len() == replicas as usize {
                            break;
                        }
                    }
                }
                // The weighted policy errors when no remote worker is eligible. A local
                // replica is still valid, so under-replicate instead of failing; keep
                // the error when nothing was selected at all.
                Err(e) => {
                    if res.is_empty() {
                        return Err(e);
                    }
                }
            }
        }

        Ok(res)
    }

    fn choose_workers(
        &self,
        workers: &IndexMap<u32, WorkerInfo>,
        count: Option<usize>,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        // Without block or client context the local worker cannot be determined,
        // so delegate directly to the weighted policy.
        self.inner.choose_workers(workers, count, exclude_workers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::StoragePolicy;
    use std::collections::HashSet;

    fn worker_on(id: u32, weight: u32, hostname: &str) -> WorkerInfo {
        WorkerInfo {
            address: WorkerAddress {
                worker_id: id,
                hostname: hostname.to_string(),
                ..Default::default()
            },
            weight,
            capacity: 1024,
            available: 1024,
            ..Default::default()
        }
    }

    fn ctx(client_host: &str, replicas: u16, block_size: i64) -> ChooseContext {
        ChooseContext {
            replicas,
            block_size,
            storage_policy: StoragePolicy::default(),
            client_host: client_host.to_string(),
            exclude_workers: HashSet::new(),
        }
    }

    #[test]
    fn local_first_then_weighted_remaining() {
        let policy = LocalWorkerPolicy::new();
        // Worker 1 has weight 0, so the weighted fallback can only pick worker 3.
        let workers = IndexMap::from([
            (1, worker_on(1, 0, "host-a")),
            (2, worker_on(2, 1, "host-b")),
            (3, worker_on(3, 1, "host-c")),
        ]);

        let selected = policy.choose(&workers, ctx("host-b", 2, 10)).unwrap();

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].worker_id, 2);
        assert_eq!(selected[1].worker_id, 3);
    }

    #[test]
    fn under_replicates_when_no_eligible_remote_worker() {
        let policy = LocalWorkerPolicy::new();
        // Worker 1 is local; worker 2 has weight 0, so the weighted fallback
        // finds no eligible remote worker and the local replica is kept alone.
        let workers = IndexMap::from([
            (1, worker_on(1, 1, "host-a")),
            (2, worker_on(2, 0, "host-b")),
        ]);

        let selected = policy.choose(&workers, ctx("host-a", 2, 10)).unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].worker_id, 1);
    }

    #[test]
    fn no_local_worker_falls_back_to_weighted() {
        let policy = LocalWorkerPolicy::new();
        // Worker 2 has weight 0 and must never be selected by the weighted fallback.
        let workers = IndexMap::from([
            (1, worker_on(1, 1, "host-a")),
            (2, worker_on(2, 0, "host-b")),
            (3, worker_on(3, 1, "host-c")),
        ]);

        let selected = policy.choose(&workers, ctx("host-d", 2, 10)).unwrap();

        assert_eq!(selected.len(), 2);
        let ids: HashSet<u32> = selected.iter().map(|addr| addr.worker_id).collect();
        assert_eq!(ids, HashSet::from([1, 3]));
    }

    #[test]
    fn errors_when_no_worker_is_eligible() {
        let policy = LocalWorkerPolicy::new();
        // No local worker and every worker has weight 0: the weighted error propagates.
        let workers = IndexMap::from([
            (1, worker_on(1, 0, "host-a")),
            (2, worker_on(2, 0, "host-b")),
        ]);

        let selected = policy.choose(&workers, ctx("host-c", 2, 10));

        assert!(selected.is_err());
    }

    #[test]
    fn skips_drained_local_worker() {
        let policy = LocalWorkerPolicy::new();
        // Worker 1 is local but drained (weight 0), so it must not receive any replica.
        let workers = IndexMap::from([
            (1, worker_on(1, 0, "host-a")),
            (2, worker_on(2, 1, "host-b")),
        ]);

        let selected = policy.choose(&workers, ctx("host-a", 2, 10)).unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].worker_id, 2);
    }
}
