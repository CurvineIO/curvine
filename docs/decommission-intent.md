# Durable Worker decommission intent

The Master stores the decommission list by Worker hostname, not by Worker ID.
Once a hostname is added, a Worker that registers under that hostname is still
marked decommissioned even if its ID has changed. Removing a hostname clears
that intent. The existing list, add, and remove endpoints are unchanged.
Marking a host does not by itself make it safe to stop a Worker that still holds
needed replicas. Background drain is separately enabled with
`master.decommission_drain_enabled` (off by default).

## Enable journaling of decommission changes

In Raft deployments, `journal.decommission_intent_enabled` defaults to `false`.
While disabled, Masters reject non-empty add and remove requests but still
replay decommission entries they already receive or have on disk.

1. Before upgrading, record the current Leader's `/get-dcm` list and suspend
   decommission edits. Old Masters kept this list in memory; upgrading does not
   migrate it automatically.
2. Upgrade every Master with the flag disabled, including peers that could
   rejoin. Leave `format_master = false` when reopening existing directories.
3. Set `decommission_intent_enabled = true` under `[journal]` on every Master
   and restart them in a quorum-preserving order. Re-add any hostnames recorded
   in step 1 and verify the list on the serving Leader.

Only the serving Leader accepts changes. Success means the Raft entry was
committed and applied on that Master. After a timeout or connection loss, the
outcome may be unknown; check the list on the current Leader and retry the
intended add or remove if needed. Follower lists may lag the Leader.

On a standalone Master with `journal.enable = false`, the Leader persists
changes directly to RocksDB and waits for the flush before returning success.
The Raft write gate does not control that mode. Do not disable journaling to
bypass the write gate on a replicated cluster.

## Inspect and pause a drain

```sh
cv node --decommission-status
cv node --decommission-status=worker-a
cv node --pause-decommission worker-a,worker-b
cv node --resume-decommission worker-a,worker-b
```

The new commands print JSON and target the serving Master. In a Raft cluster,
pause and resume require `journal.decommission_intent_enabled = true`; this
flag does not block reading status or replaying a pause already committed.
Pause stops new drain work for those hosts. An already-started copy can finish;
a deletion already sent to a Worker cannot be recalled. Pause is not
recommissioning and does not disable ordinary repair for the cluster.

On the Master admin HTTP listener, `GET /api/decommission` lists marked hosts;
`?host=worker-a` selects one. `GET /pause-dcm?workers=worker-a,worker-b` and
`GET /resume-dcm?workers=worker-a,worker-b` change existing intents. Use exact
registration hostnames, without Worker RPC ports. Repeated requests are safe.
An invalid input returns 400, an unmarked host returns 404 (no partial change),
and a Master that is not serving returns 503. These are admin endpoints; use
only a trusted admin listener or network. The existing `/api/workers`,
`/get-dcm`, `/add-dcm` and `/remove-dcm` contracts are unchanged.

Status is per host even when several Workers register under it. `Disabled`
means drain is off; `PendingScan` means current progress is not available yet.
`Paused` is durable. `Blocked` includes unavailable Workers and insufficient
target capacity; neither is evidence of a successful drain. Remaining blocks,
bytes, and blockers are scan estimates, not a durable percentage. Unknown
estimates are `null`, not zero. The new Leader reconstructs progress, while
preserving pause intent. `Drained` requires all registrations on the host to
pass the current Leader's drain checks.

`/metrics` exposes `decommission_remaining_blocks{host}` and
`decommission_remaining_bytes{host}` for available estimates and
`decommission_inflight` for the last observed queued and active copies. Host
series disappear when their estimates are no longer available.
`decommission_admitted_total` counts accepted drain requests, and
`decommission_failed_total` counts submission and result-processing errors
(not unique failed blocks). Counters are per Master process, not journaled.

## Rollback boundary

Before writing any new decommission entries, this change does not alter the
existing Raft entry formats. Once a new entry has been written, do not run an
older Master against the resulting journal or checkpoints. An older binary
cannot replay the new variant and does not enforce the stored intent. Turning
`decommission_intent_enabled` off stops new writes; it does not remove history
or make downgrading safe. To roll back to an older binary, use a consistent
pre-activation backup and account for metadata writes made since that backup.
