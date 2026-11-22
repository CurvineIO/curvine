# Manual capacity balancer

The capacity balancer is a leader-local, operator-started controller. It is disabled by
`master.balancer_enabled = false` by default and does not schedule work on its own.
Enable it only on a deployment that has upgraded all Master peers and has the
session-fenced replica-release path available.

## Configuration

```toml
[master]
balancer_enabled = true
balancer_bytes_per_iteration = 10737418240 # 10 GiB
```

The byte budget must be positive. A balancing run uses the T13 weighted-utilization
model and a bounded Worker reverse-index page on each tick. It admits at most one Move
at a time, waits for the fenced replacement and source deletion acknowledgement, and
then starts the next Move or round. Blocks that are incomplete, being written, already
busy, or already present on a candidate target are skipped.

The controller never bypasses T10/T12 replication admission or source-release fences.
A stop prevents new Move admission but lets an already admitted Move finish its normal
copy-before-delete protocol. A serving-term change abandons the local run; the new
leader must be explicitly started again. Completed Moves are not rolled back by stop.

## HTTP API

All mutating endpoints require the serving Leader. Both `GET` and `POST` are accepted
for start and stop for compatibility with existing administrative tooling:

```text
GET|POST /balancer/start?threshold=0.1
GET|POST /balancer/stop
GET      /api/balancer
```

`threshold` is a decimal fraction in `[0, 1]` with at most four fractional digits.
The default is `0.1` (10 percentage points). Status reports `running`, `stopping`,
`balanced`, `blocked`, `stopped`, or `leadership_changed`, together with the current
round, byte budget, admitted bytes, and last detail.

## CLI

```text
cv node --balance start [--threshold 0.1]
cv node --balance stop
cv node --balance status
```

The CLI first discovers the active Master through the filesystem RPC and then sends the
request to that Master's web endpoint. It does not silently fall back to a follower.

## Prometheus metrics

Metrics are registered by the Master and use only fixed, bounded label values. Block
IDs, Worker IDs, hostnames, and request details are never metric labels.

| Metric | Type | Meaning |
| --- | --- | --- |
| `master_balancer_state{state}` | gauge | One-hot controller state: `disabled`, `idle`, `running`, `stopping`, `stopped`, `balanced`, `blocked`, or `leadership_changed`. |
| `master_balancer_rounds_total` | counter | Rounds created from the T13 model. |
| `master_balancer_round_bytes_planned_total` | counter | Bytes planned by the model, before block-level feasibility is known. |
| `master_balancer_bytes_admitted_total` | counter | Bytes admitted into fenced Move requests. |
| `master_balancer_bytes_moved_total` | counter | Bytes whose Move completed with the designated source release. |
| `master_balancer_inflight_moves` | gauge | Number of balancer Moves waiting for settlement; the controller admits at most one. |
| `master_balancer_errors_total{reason}` | counter | Errors by bounded reason, such as `internal_error`, `no_feasible_move`, or `move_not_settled`. |
| `master_balancer_skipped_blocks_total{reason}` | counter | Bounded skip reasons such as `busy`, `incomplete`, `oversized_or_invalid`, `no_target`, and `leadership_changed`. |

`round_bytes_planned_total` is intentionally different from
`bytes_admitted_total`: capacity-model budgets do not prove that a concrete block and
fenced target exist. A rising `rounds_total` with no corresponding admitted bytes is a
signal to inspect `master_balancer_state`, `master_balancer_errors_total`, and the skip
counters. An increasing in-flight gauge means the controller is waiting for replication
and deletion acknowledgements, not repeatedly copying the same block.

Example recording rules and alerts:

```promql
rate(master_balancer_bytes_admitted_total[10m])
  / clamp_min(rate(master_balancer_round_bytes_planned_total[10m]), 1) < 0.01

master_balancer_state{state="blocked"} == 1

master_balancer_inflight_moves > 0
  and rate(master_balancer_bytes_moved_total[15m]) == 0
```

Tune these windows to the block size, Worker replication bandwidth, and
`master.block_replication_job_timeout`. T10's replication admission and T9's Worker
bandwidth limits affect how quickly a Move settles; the balancer does not override either
limit.

## Scale-out walkthrough

1. Add the new Worker and wait until it appears as Live with a current heartbeat and a
   non-zero weight. Confirm its allocatable headroom is sufficient for the expected block
   size.
2. Upgrade every Master peer, enable the guarded Move/deletion path and
   `master.balancer_enabled`, then restart/reload according to the deployment procedure.
   The balancer remains idle until explicitly started.
3. Inspect `/metrics` and `/api/balancer`, then start with a conservative threshold:

   ```sh
   cv node --balance status
   cv node --balance start --threshold 0.1
   ```

4. Watch `master_balancer_state`, planned versus admitted/moved bytes, skip reasons,
   and the in-flight gauge. `blocked` or `no_progress` means the model found a positive
   aggregate budget but the current block inventory, capacity, sessions, or admission
   limits did not yield a safe Move; it is not a claim that the cluster is balanced.
5. Stop admission when the maintenance window ends:

   ```sh
   cv node --balance stop
   cv node --balance status
   ```

   A Move already admitted before stop is allowed to complete its fenced protocol. Stop
   does not delete or roll back a completed replacement. After a leader change, start a
   new run from the serving Leader.

This is deliberately not automatic rebalancing, consistent hashing, hotspot management,
or topology-aware placement. A positive model budget is not a promise that a block-level
Move is feasible; a blocked run reports no progress rather than repeatedly copying the
same block.
