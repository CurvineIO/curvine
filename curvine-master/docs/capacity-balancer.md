# Manual capacity balancer

The capacity balancer is a leader-local, operator-started controller. It is disabled by
`master.balancer_enabled = false` by default and does not schedule work on its own.
Enable it only on a deployment that has upgraded all Master peers and has the
session-fenced replica-release path available.

## Configuration

```toml
[master]
# Guarded source release is a prerequisite for the balancer.
decommission_drain_enabled = true
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
leader must be explicitly started again.

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

This is deliberately not automatic rebalancing, consistent hashing, hotspot
management, or topology-aware placement. A positive model budget is not a promise that
a block-level Move is feasible; a blocked run reports no progress rather than repeatedly
copying the same block.
