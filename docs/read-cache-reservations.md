# Optional read-cache reservation confirmation

The default capacity accounting remains unchanged. A dedicated read-cache
cluster can opt into earlier settlement of reservations for newly loaded files:

```toml
[master]
read_cache_reservation_enabled = true
```

Use matching upgraded Masters and Workers. Leave this option disabled for
write-cache or mixed-use clusters. It is an accounting optimization, not an
enforcement mechanism that makes a cluster read-only.

## Problem and scope

Heartbeats normally reclaim scheduled capacity from the decrease in available
space. Loading a 4 MiB file while evicting 4 MiB can leave available space
unchanged. The new file's remaining 4 MiB reservation then waits for another
reclamation or the existing reservation timeout.

This option targets that case. Eligibility is limited to files created during
the current Master leadership, with delete-on-TTL cache policy, closed by
CompleteFile and subsequently marked as present in both Curvine and UFS. The
loader can publish the UFS timestamp in CompleteFile or in SetAttr after rename.
Metadata imported from UFS, existing files, open/flush-only files, files with
multiple links, and SPDK replicas are excluded. Reopen, overwrite, resize,
worker reassignment, repeated block handoff, or completion/flush after close
revokes eligibility.

## Confirmation flow

1. Master retains block-specific credits for an eligible completed file.
2. A heartbeat response sends a token for a frozen, bounded batch of credits.
3. Worker receives the token, collects a new capacity sample, and echoes the
   token with that sample's process-local sequence number.
4. Master first rejects replayed capacity samples, applies normal accounting
   for a fresh sample, then releases only still-valid credits in the batch.

For a 16 MiB reservation and a 4 MiB file, normal commit settlement first returns
12 MiB. If the write is offset by a 4 MiB eviction, confirmation can return the
remaining 4 MiB without reducing a concurrent file's reservation. Normally this
takes two successful Running heartbeats after publication; it is not a deadline.

## Conservative fallback

An aggregate decrease in available space does not identify which block consumed
it. Therefore any such reclamation invalidates proof for all allocations that
already existed, including anonymous reservations not yet bound to blocks. Those
credits continue using legacy accounting. This avoids counting the same disk
consumption once through a heartbeat and again through confirmation.

The option deliberately does not resolve every reservation discrepancy. Missing
snapshots, worker restart, Start/End, leadership changes, lease revocation,
and exhausted tracking limits may retain reservations until legacy reclamation
or timeout. Abandoning proof itself does not request re-registration or change
inventory reconciliation, replica deletion, replication repair, or the existing
lost-worker policy. Session-mismatch recovery is described below.

Within a known Worker session, older or equal sequence numbers cannot replace
accepted storage capacity or trigger net-delta reclamation. A missing sequence
after sequenced samples is likewise ignored for capacity. Ordinary liveness,
commands, and reservation expiry still run. A same-session Start revokes proof
but retains the capacity sequence high-water mark. Followers apply the same
capacity ordering and revoke confirmation proof, preserving sample ordering
across promotion without changing inventory recovery. Legacy sessions that have
never supplied a sequence, and feature-disabled Masters, keep existing behavior.

An active heartbeat rechecks leadership and epoch after acquiring the worker
manager lock and before confirming credits. If demotion or an epoch change
crosses processing, it returns `NotLeader` without confirming credits or sending
commands. Any capacity sample already applied retains its sequence high-water
mark, while its confirmation proof is revoked.

For a known sequenced session, mismatched `Running` and `End` heartbeats are
rejected before changing capacity, liveness, commands, or proof. After a
`Start`, Master remembers the expected session until its first `Running` or
matching `End`, protecting the registration gap from an old process. Duplicate
`Start`/`Running` requests remain retryable. This pending table is bounded at
65,536 entries; at the limit a new registration returns an error and must retry
after an existing pending registration completes or ends. Entries are not timed
out into an unfenced registration gap.

An upgraded Worker treats a typed `InProgress` / `NotRegistered:` response to
its active heartbeat as a request to repeat `Start` and the existing batched full
report. This handles A → B → A leadership failback when a Worker restarted on B
and A retained the old session (including when follower reporting is disabled).
Recovery clears the old confirmation token, preserves the process's snapshot
sequence, and collects a fresh sample before resuming `Running`. A failed Start
or report keeps recovery pending for the next heartbeat tick; a lost reply cannot
make the Worker skip the report. Generic errors do not trigger registration.
During recovery the Worker may be unavailable for new allocations; report cost
and retry limits are the same as startup. Enable this feature only after Workers
have this recovery support. No new wire fields or inventory semantics are added.
Recovery assumes one live process owns a worker identity. Delayed RPCs from a
dead process cannot re-register it, but a still-running old process can respond
with another `Start`; two live processes sharing an identity can therefore
compete for registration. This handshake is not split-brain ownership fencing.

This is not a general registration-generation protocol: `Start` remains the
existing authority for session replacement. Delayed, conflicting `Start`
requests, or unknown sessions after Master state loss, retain existing recovery
behavior and can reset the capacity baseline. No persistent retired-session
history or full-report recovery redesign is introduced. Deployments requiring
that broader fencing need a separate registration fix; this optimization alone
does not guarantee capacity accuracy across those cases.

Lost responses cause the same token and frozen batch to be redelivered, including
RPC retries of the identical sequence. Such retries never acknowledge a batch,
freeze a new batch, or extend its deadline; settlement requires a newer sample. A batch
expires after the greater of 60 seconds and five configured heartbeat intervals;
expiry drops its proof without releasing its credits and allows later batches
to proceed. Late and duplicate acknowledgements cannot settle the expired batch.

## Bounds and compatibility

- Per worker: at most 65,536 pending entries plus one batch of up to 4,096.
- Master file eligibility tracker: at most 65,536 files, with bounded pruning
  on creation and a 10-minute age threshold. Dropping an entry revokes its proof.
- New heartbeat fields are optional. Legacy peers retain legacy accounting.
- Proof is ephemeral and is neither journaled nor restored from checkpoints.

Publication still holds the namespace **read** lock during the location lookup,
with one RocksDB seek per block lacking inline locations. A 1 TiB file with
128 MiB blocks can require about 8,192 seeks, delaying a queued metadata writer.
The namespace lock is released before acquiring the worker-manager lock and
enrolling credits. Moving the database scan outside the read guard is a separate
performance change that must preserve inline-location precedence and eligibility
revalidation.

## Observability

Master exports these process-local counters, including zero-valued series when
the feature is disabled. They do not add labels for worker IDs, file paths,
block IDs, or tokens, and are not persisted through Master restart.

| Metric | Meaning |
| --- | --- |
| `master_read_cache_reservation_batches_issued_total` | Newly frozen batches; retransmissions do not count again. |
| `master_read_cache_reservation_batches_confirmed_total` | Batches matched by an ACK on a fresh snapshot; a batch can release zero bytes. |
| `master_read_cache_reservation_released_bytes_total` | Reservation bytes actually released by confirmation, summed across replicas. Excludes commit padding refunds and heartbeat net-delta reclamation. |
| `master_read_cache_reservation_batches_expired_total` | Batches dropped at their ACK deadline; expiry releases no reservation bytes. |
| `master_read_cache_reservation_skipped_total{reason}` | Diagnostic skips, with the fixed reasons below. These are not unique-file counts. |

Skip reasons and units:

- `allocation_floor`: replica enrollment/drain attempts rejected by the reclamation watermark.
- `anonymous_reclaim`: block bindings whose proof is invalidated by earlier anonymous reclamation.
- `pending_limit`: new replica enrollment attempts rejected at the pending-entry cap.
- `lease_revoked`: queued replica entries rejected after file eligibility was revoked.
- `no_credit`: replica enrollment/drain attempts without the matching reservation.
- `unsequenced_worker`: replica enrollment attempts before the worker supplies sequenced capacity.
- `tracker_ttl`: tracked files aged out before their tracked work finished.
- `tracker_limit`: new files not tracked because the file-entry cap was reached.
- `publication_invalidated`: deferred file publications rejected during revalidation.

Repeated skipped attempts may count again; one file may involve multiple replicas
and reasons. Normal cleanup of a fully drained file tracker is not a skip.
These counters add no pending-map scan at scrape time. Some skip events are reported
when queued entries are next examined, so they are diagnostic rather than a
durable audit of all abandoned proof. Lifecycle/epoch invalidation can discard
batches before their deadline: issued minus confirmed minus expired is **not**
a backlog gauge. Released reservation bytes are **not** physical disk bytes freed.

Example PromQL (add the deployment's cluster/instance selectors as appropriate):

```promql
sum(rate(master_read_cache_reservation_released_bytes_total[5m])) / 1024 / 1024
sum by (reason) (rate(master_read_cache_reservation_skipped_total[5m]))
```

Unit tests cover both allocation orders, net-zero eviction/write, heartbeat
before/after commit, fully reclaimed credit, anonymous reservations, retries,
expiry, lifecycle revocation, independent replicas, default-off behavior, and
legacy wire compatibility. Real multi-node failure injection and a sustained
OSS cache-churn benchmark are separate deployment validation steps.
