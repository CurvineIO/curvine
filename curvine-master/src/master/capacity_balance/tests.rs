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

use super::*;
use curvine_model::{WorkerAddress, WorkerStatus};

fn worker(worker_id: u32, capacity_bytes: i64, used: i64, weight: u32) -> WorkerCapacity {
    let available = capacity_bytes - used;
    WorkerCapacity {
        worker_id,
        weight,
        capacity_bytes,
        available_bytes: available,
        cache_used_bytes: used,
        allocatable_bytes: available,
    }
}

fn budget(worker_id: u32, limit_bytes: u64, planned_bytes: u64) -> WorkerBudget {
    WorkerBudget {
        worker_id,
        limit_bytes,
        planned_bytes,
    }
}

#[test]
fn equal_utilization_at_unequal_capacities_does_not_move_bytes() {
    for weight in [1, 10, u32::MAX] {
        let result = plan_capacity_balance(
            &[worker(1, 100, 25, weight), worker(2, 900, 225, weight)],
            0,
            u64::MAX,
        )
        .unwrap();
        assert_eq!(
            result.baseline,
            Some(ClusterUtilization {
                used_bytes: 250,
                capacity_bytes: 1_000
            })
        );
        assert_eq!(result.outcome, PlanOutcome::Balanced);
        assert_eq!(result.total_bytes, 0);
        assert!(result.sources.is_empty() && result.targets.is_empty());
    }
}

#[test]
fn uniform_weights_match_capacity_weighted_utilization() {
    let result = plan_capacity_balance(
        &[worker(1, 100, 100, 7), worker(2, 300, 0, 7)],
        DEFAULT_THRESHOLD_BPS,
        200,
    )
    .unwrap();
    // Cluster U = 25%; each Worker moves to its own 25% used-byte level.
    assert_eq!(result.outcome, PlanOutcome::Planned);
    assert_eq!(result.sources, [budget(1, 75, 75)]);
    assert_eq!(result.targets, [budget(2, 75, 75)]);
    assert_eq!(result.total_bytes, 75);
}

#[test]
fn unequal_weights_use_capacity_weighted_normalization_and_physical_bytes() {
    let workers = [worker(7, 300, 150, 3), worker(2, 100, 100, 1)];
    let result = plan_capacity_balance(&workers, DEFAULT_THRESHOLD_BPS, 60).unwrap();
    // U = .625, weighted mean = 2.5. Ideal used bytes = (25, 225).
    assert_eq!(
        result.baseline,
        Some(ClusterUtilization {
            used_bytes: 250,
            capacity_bytes: 400
        })
    );
    assert_eq!(result.outcome, PlanOutcome::Planned);
    assert_eq!(result.sources, [budget(2, 75, 60)]);
    assert_eq!(result.targets, [budget(7, 75, 60)]);
    assert_eq!(result.total_bytes, 60);
    let restored = [worker(7, 300, 225, 3), worker(2, 100, 25, 1)];
    assert_eq!(
        plan_capacity_balance(&restored, 0, 100).unwrap().outcome,
        PlanOutcome::Balanced
    );
    assert_eq!(
        plan_capacity_balance(
            &workers.map(|w| WorkerCapacity {
                weight: w.weight * 10,
                ..w
            }),
            DEFAULT_THRESHOLD_BPS,
            60
        )
        .unwrap(),
        result
    );
}

#[test]
fn exact_threshold_is_inclusive_on_both_sides() {
    let workers = [worker(1, 100, 60, 5), worker(2, 100, 40, 5)];
    for (threshold, expected) in [
        (999, PlanOutcome::Planned),
        (1000, PlanOutcome::Balanced),
        (1001, PlanOutcome::Balanced),
    ] {
        assert_eq!(
            plan_capacity_balance(&workers, threshold, 100)
                .unwrap()
                .outcome,
            expected
        );
    }
}

#[test]
fn empty_zero_capacity_and_zero_weight_do_not_contribute() {
    let excluded = [
        worker(1, 0, 0, 4),
        worker(2, 500, 500, 0),
        worker(3, 500, 0, 0),
    ];
    for workers in [&[][..], &excluded[..]] {
        let result = plan_capacity_balance(workers, 0, 100).unwrap();
        assert_eq!(result.outcome, PlanOutcome::NoEligibleWorkers);
        assert_eq!(result.baseline, None);
        assert_eq!(result.total_bytes, 0);
        assert!(result.sources.is_empty() && result.targets.is_empty());
    }
    let mut mixed = excluded.to_vec();
    mixed.push(worker(4, 10, 6, 1));
    let result = plan_capacity_balance(&mixed, 0, 100).unwrap();
    assert_eq!(
        result.baseline,
        Some(ClusterUtilization {
            used_bytes: 6,
            capacity_bytes: 10
        })
    );
    assert_eq!(result.outcome, PlanOutcome::Balanced);
}

#[test]
fn distinguishes_infeasible_sides_from_balance_or_zero_round_budget() {
    let source = worker(1, 100, 80, 1);
    let target = worker(2, 100, 20, 1);
    for (workers, threshold, round_limit, expected) in [
        // The source differs from the weighted baseline by .3, the target by .1.
        (
            [worker(1, 100, 90, 1), worker(2, 300, 150, 1)],
            1500,
            100,
            PlanOutcome::NoFeasibleMove,
        ),
        (
            [
                source,
                WorkerCapacity {
                    allocatable_bytes: 0,
                    ..target
                },
            ],
            0,
            100,
            PlanOutcome::NoFeasibleMove,
        ),
        (
            [
                WorkerCapacity {
                    cache_used_bytes: 0,
                    ..source
                },
                target,
            ],
            0,
            100,
            PlanOutcome::NoFeasibleMove,
        ),
        // Nonzero utilization differences round down to zero whole bytes.
        (
            [worker(1, 2, 1, 1), worker(2, 2, 0, 1)],
            0,
            100,
            PlanOutcome::NoFeasibleMove,
        ),
        ([source, target], 0, 0, PlanOutcome::BudgetExhausted),
    ] {
        let result = plan_capacity_balance(&workers, threshold, round_limit).unwrap();
        assert_eq!(result.outcome, expected, "{workers:?}");
        assert_eq!(result.total_bytes, 0);
        assert!(result
            .sources
            .iter()
            .chain(&result.targets)
            .all(|budget| budget.planned_bytes == 0));
    }
}

#[test]
fn stable_budgets_never_exceed_reclaimable_bytes_or_headroom() {
    let a = WorkerCapacity {
        cache_used_bytes: 10,
        ..worker(9, 100, 100, 1)
    };
    let b = worker(6, 100, 100, 1);
    let c = WorkerCapacity {
        allocatable_bytes: 15,
        ..worker(4, 100, 0, 1)
    };
    let d = WorkerCapacity {
        allocatable_bytes: 20,
        ..worker(1, 100, 0, 1)
    };
    let result = plan_capacity_balance(&[a, b, c, d], 0, 120).unwrap();
    assert_eq!(result.total_bytes, 35);
    assert_eq!(result.sources, [budget(6, 50, 35), budget(9, 10, 0)]);
    assert_eq!(result.targets, [budget(1, 20, 20), budget(4, 15, 15)]);
    assert_eq!(
        plan_capacity_balance(&[d, a, c, b], 0, 120).unwrap(),
        result
    );
}

#[test]
fn rejects_invalid_data_even_for_disabled_workers() {
    let base = worker(1, 100, 60, 0);
    for invalid in [
        WorkerCapacity {
            capacity_bytes: -1,
            ..base
        },
        WorkerCapacity {
            available_bytes: -1,
            ..base
        },
        WorkerCapacity {
            available_bytes: 101,
            ..base
        },
        WorkerCapacity {
            allocatable_bytes: -1,
            ..base
        },
        WorkerCapacity {
            allocatable_bytes: 41,
            ..base
        },
        WorkerCapacity {
            cache_used_bytes: -1,
            ..base
        },
    ] {
        assert_eq!(
            plan_capacity_balance(&[invalid], 0, 100),
            Err(PlanningError::InvalidBytes(1))
        );
    }
    assert_eq!(
        plan_capacity_balance(&[base, base], 0, 100),
        Err(PlanningError::DuplicateWorker(1))
    );
    assert_eq!(
        plan_capacity_balance(&[], 10001, 100),
        Err(PlanningError::InvalidThreshold(10001))
    );
}

#[test]
fn large_values_preserve_an_exact_one_byte_move() {
    let capacity = i64::MAX;
    let result = plan_capacity_balance(
        &[
            worker(1, capacity, capacity, u32::MAX),
            worker(2, capacity, capacity - 2, u32::MAX),
            worker(3, capacity, capacity - 1, u32::MAX),
        ],
        0,
        u64::MAX,
    )
    .unwrap();
    assert_eq!(
        result.baseline.unwrap().capacity_bytes,
        3 * capacity as u128
    );
    assert_eq!(result.sources, [budget(1, 1, 1)]);
    assert_eq!(result.targets, [budget(2, 1, 1)]);
    assert_eq!(result.total_bytes, 1);
}

#[test]
fn worker_snapshot_uses_reported_usage_and_reserved_receive_headroom() {
    let mut registration = WorkerInfo::new(
        WorkerAddress {
            worker_id: 5,
            ..Default::default()
        },
        3,
    );
    registration.capacity = 100;
    registration.available = 40;
    registration.fs_used = 27;
    registration.scheduled_bytes = 9;
    assert_eq!(
        WorkerCapacity::from_live_worker(&registration),
        Some(WorkerCapacity {
            worker_id: 5,
            weight: 3,
            capacity_bytes: 100,
            available_bytes: 40,
            cache_used_bytes: 27,
            allocatable_bytes: 31,
        })
    );
    registration.scheduled_bytes = 100;
    assert_eq!(
        WorkerCapacity::from_live_worker(&registration)
            .unwrap()
            .allocatable_bytes,
        0
    );
    for status in [
        WorkerStatus::Decommission,
        WorkerStatus::Blacklist,
        WorkerStatus::Lost,
        WorkerStatus::Unknown,
    ] {
        registration.status = status;
        assert_eq!(WorkerCapacity::from_live_worker(&registration), None);
    }
}
