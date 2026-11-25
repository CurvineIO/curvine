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

//! Pure byte-budget planning for capacity balancing. See `curvine-master/docs/capacity-balance-model.md`.

use curvine_model::WorkerInfo;
use ethnum::U256;

const BPS_PER_UNIT: u16 = 10_000;
/// 10 percentage points of effective utilization, not 10% of the baseline.
pub const DEFAULT_THRESHOLD_BPS: u16 = 1_000;

/// A single snapshot of a Live Worker. Use `from_live_worker` when capturing
/// registrations; do not include stale or unreachable Workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerCapacity {
    pub worker_id: u32,
    pub weight: u32,
    pub capacity_bytes: i64,
    /// Heartbeat availability, already net of whatever space the Worker withholds.
    pub available_bytes: i64,
    /// Curvine-owned usage; an upper bound on what a Move can free, not proof that
    /// those bytes belong to eligible blocks.
    pub cache_used_bytes: i64,
    /// Aggregate receive headroom after Master-side reservations.
    pub allocatable_bytes: i64,
}

impl WorkerCapacity {
    pub fn from_live_worker(worker: &WorkerInfo) -> Option<Self> {
        worker.is_live().then(|| Self {
            worker_id: worker.worker_id(),
            weight: worker.weight,
            capacity_bytes: worker.capacity,
            available_bytes: worker.available,
            cache_used_bytes: worker.fs_used,
            allocatable_bytes: worker
                .allocatable_available()
                .clamp(0, worker.available.max(0)),
        })
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlanningError {
    #[error("threshold must be in 0..=10000 basis points, got {0}")]
    InvalidThreshold(u16),
    #[error("duplicate Worker {0} in balancing snapshot")]
    DuplicateWorker(u32),
    #[error("invalid capacity, availability, cache usage or headroom for Worker {0}")]
    InvalidBytes(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanOutcome {
    NoEligibleWorkers,
    Balanced,
    NoFeasibleMove,
    /// Both sides have positive headroom, but the round budget is zero.
    BudgetExhausted,
    /// A positive byte budget exists; no block-level feasibility is implied.
    Planned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterUtilization {
    pub used_bytes: u128,
    pub capacity_bytes: u128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerBudget {
    pub worker_id: u32,
    /// Per-Worker limit before the global round and opposite-side caps.
    pub limit_bytes: u64,
    /// Share of `total_bytes` for this round; may be zero.
    pub planned_bytes: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BalancePlan {
    pub baseline: Option<ClusterUtilization>,
    pub outcome: PlanOutcome,
    /// Overloaded Workers, in Worker ID order, including zero byte limits.
    pub sources: Vec<WorkerBudget>,
    /// Underused Workers, in Worker ID order, including zero byte limits.
    pub targets: Vec<WorkerBudget>,
    /// Sum of planned source bytes and, separately, of planned target bytes.
    pub total_bytes: u64,
}

/// Compute one deterministic, aggregate byte plan. This does not select blocks,
/// reserve capacity, or submit copies. A caller must revalidate at admission.
pub fn plan_capacity_balance(
    workers: &[WorkerCapacity],
    threshold_bps: u16,
    round_limit_bytes: u64,
) -> Result<BalancePlan, PlanningError> {
    if threshold_bps > BPS_PER_UNIT {
        return Err(PlanningError::InvalidThreshold(threshold_bps));
    }
    let mut workers: Vec<_> = workers.iter().collect();
    workers.sort_unstable_by_key(|worker| worker.worker_id);
    for pair in workers.windows(2) {
        if pair[0].worker_id == pair[1].worker_id {
            return Err(PlanningError::DuplicateWorker(pair[0].worker_id));
        }
    }
    for worker in &workers {
        if !(0..=worker.capacity_bytes).contains(&worker.available_bytes)
            || !(0..=worker.available_bytes).contains(&worker.allocatable_bytes)
            || worker.cache_used_bytes < 0
        {
            return Err(PlanningError::InvalidBytes(worker.worker_id));
        }
    }
    workers.retain(|worker| worker.capacity_bytes > 0 && worker.weight > 0);

    let mut plan = BalancePlan {
        baseline: None,
        outcome: PlanOutcome::NoEligibleWorkers,
        sources: Vec::new(),
        targets: Vec::new(),
        total_bytes: 0,
    };
    if workers.is_empty() {
        return Ok(plan);
    }

    let mut capacity = 0_u128;
    let mut used = 0_u128;
    let mut weighted_capacity = 0_u128;
    for worker in &workers {
        capacity += worker.capacity_bytes as u128;
        used += (worker.capacity_bytes - worker.available_bytes) as u128;
        weighted_capacity += worker.capacity_bytes as u128 * u128::from(worker.weight);
    }
    plan.baseline = Some(ClusterUtilization {
        used_bytes: used,
        capacity_bytes: capacity,
    });

    for worker in workers {
        let used_here = (worker.capacity_bytes - worker.available_bytes) as u64;
        let share = worker.capacity_bytes as u128 * u128::from(worker.weight);
        // Work in exact integers at the classification boundary. Unique u32
        // IDs bound capacity < 2^95 and weighted capacity < 2^127. Products
        // with a local byte count or share and basis points fit in 256 bits.
        let actual = U256::from(used_here) * weighted_capacity;
        let ideal = U256::from(share) * used;
        let (delta, side, local_limit) = if actual > ideal {
            (
                actual - ideal,
                &mut plan.sources,
                worker.cache_used_bytes as u64,
            )
        } else {
            (
                ideal - actual,
                &mut plan.targets,
                worker.allocatable_bytes as u64,
            )
        };
        // Compare |effective utilization - U| with the threshold. Divide by
        // weighted capacity only when converting the gap to *physical* bytes.
        if delta * u128::from(BPS_PER_UNIT)
            <= U256::from(share) * capacity * u128::from(threshold_bps)
        {
            continue;
        }
        let limit_bytes = (delta / weighted_capacity)
            .min(U256::from(local_limit))
            .as_u64();
        side.push(WorkerBudget {
            worker_id: worker.worker_id,
            limit_bytes,
            planned_bytes: 0,
        });
    }

    let feasible_bytes = sum_limits(&plan.sources).min(sum_limits(&plan.targets));
    plan.total_bytes = feasible_bytes.min(u128::from(round_limit_bytes)) as u64;
    plan.outcome = if plan.sources.is_empty() && plan.targets.is_empty() {
        PlanOutcome::Balanced
    } else if feasible_bytes == 0 {
        PlanOutcome::NoFeasibleMove
    } else if plan.total_bytes == 0 {
        PlanOutcome::BudgetExhausted
    } else {
        allocate_bytes(&mut plan.sources, plan.total_bytes);
        allocate_bytes(&mut plan.targets, plan.total_bytes);
        PlanOutcome::Planned
    };
    Ok(plan)
}

fn sum_limits(budgets: &[WorkerBudget]) -> u128 {
    budgets
        .iter()
        .map(|worker| u128::from(worker.limit_bytes))
        .sum()
}

fn allocate_bytes(budgets: &mut [WorkerBudget], mut remaining: u64) {
    for worker in budgets {
        worker.planned_bytes = remaining.min(worker.limit_bytes);
        remaining -= worker.planned_bytes;
    }
}

#[cfg(test)]
mod tests;
