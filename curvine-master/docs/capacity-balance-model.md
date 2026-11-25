# Capacity balancer utilization and byte budgets

`plan_capacity_balance` is a pure Master-side model for [Elastic Cache T13](https://github.com/CurvineIO/curvine/issues/2443). It neither schedules copies nor reserves space. A later caller must recheck current Worker registrations, block eligibility, per-device capacity and replication admission before submitting a Move.

## Snapshot and units

The model accepts one sample per active Live Worker. `WorkerCapacity::from_live_worker` maps the fields as follows:

| Input | Source and meaning |
| --- | --- |
| Capacity `C` | `WorkerInfo::capacity`: aggregate reported capacity of usable storage directories. |
| Availability `A` | `WorkerInfo::available`: reported bytes available to Curvine, already accounting for Worker-side limits such as reservations or a free-space floor. |
| Used `B = C - A` | Unavailable bytes for utilization purposes. This is not necessarily movable cache: it can include space withheld from Curvine. |
| Source cap | `WorkerInfo::fs_used`: Curvine-owned bytes, an upper bound, not a count of eligible blocks. |
| Target cap | `WorkerInfo::allocatable_available()` clamped to `[0, A]`: aggregate receive headroom after Master-side reservations. This is not a guarantee that any single directory fits a block. |

Master-side reservations affect target headroom, **not** the utilization baseline. Zero-capacity and zero-weight Workers are excluded from the baseline and both sides; zero weight does not request evacuation. Non-Live Workers are not sampled. Duplicate Worker IDs, negative capacity or cache usage, availability outside `[0, C]`, target headroom outside `[0, A]`, and thresholds above 10,000 basis points reject the plan, even for otherwise excluded samples. Cache usage may exceed `B` if supplied from independently sampled accounting; the source limit can never exceed `B`.

## Classification and budgets

For eligible Workers, let `U = ΣB / ΣC` be the capacity-weighted baseline and `W = Σ(C × weight) / ΣC` the capacity-weighted mean weight. A Worker's effective utilization is `E = (B / C) / (weight / W)`. A common weight cancels, recovering ordinary capacity-based utilization. Scaling all positive weights by the same factor has no effect.

The threshold is a difference in effective utilization, in basis points: 1,000 means **10 percentage points** (`0.10`), not 10% of `U`. Classify a Worker as a source only when `E > U + threshold`, and a target only when `E < U - threshold`. Equality is inside the band. Comparisons use exact integer arithmetic before rounding byte budgets.

The ideal used bytes are `I = C × (weight / W) × U`. Convert the effective gap to physical bytes using `|E - U| × C × (weight / W) = |B - I|`. Multiplying by `C` alone would give a normalized byte count and could overshoot or underfill for unequal weights. Round down to whole bytes, cap source limits by Curvine-owned bytes and target limits by allocatable bytes. This cap also handles ideal usage above physical capacity. The round budget is the minimum of the sum of source limits, the sum of target limits, and the caller's byte cap. Each side distributes that same total in ascending Worker ID order; the model does not pair Workers or select blocks.

Example: `(C, B, weight)` of `(100, 100, 1)` and `(300, 150, 3)` gives `U = 0.625`, `W = 2.5`, and ideal usage of 25 and 225 bytes. With a 1,000-bp threshold, the first Worker can send 75 bytes and the second can receive 75. A 60-byte round cap budgets 60 bytes on each side.

## Outcomes

| Outcome | Meaning |
| --- | --- |
| `NoEligibleWorkers` | There is no baseline. |
| `Balanced` | All eligible Workers are within the threshold band. |
| `NoFeasibleMove` | Some Workers are outside the band, but one side has no positive whole-byte limit. |
| `BudgetExhausted` | Both sides have positive limits but the caller's round cap is zero. |
| `Planned` | Both sides have positive budget. Block-level feasibility is not known yet. |
