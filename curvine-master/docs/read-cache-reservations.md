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
4. Master first applies normal heartbeat accounting, then releases only the
   still-owned eligible credits whose confirmation remains valid.

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

The option deliberately does not resolve every reservation discrepancy. Lost or
stale snapshots, worker restart, Start/End, leadership changes, lease revocation,
and exhausted tracking limits may retain reservations until legacy reclamation
or timeout. None requests re-registration or changes inventory reconciliation,
replica deletion, replication repair, or the existing lost-worker policy.

Lost responses cause the same token and frozen batch to be redelivered. A batch
expires after the greater of 60 seconds and five configured heartbeat intervals;
expiry drops its proof without releasing its credits and allows later batches
to proceed. Late and duplicate acknowledgements cannot settle the expired batch.

## Bounds and compatibility

- Per worker: at most 65,536 pending entries plus one batch of up to 4,096.
- Master file eligibility tracker: at most 65,536 files, with bounded pruning
  on creation and a 10-minute age threshold. Dropping an entry revokes its proof.
- New heartbeat fields are optional. Legacy peers retain legacy accounting.
- Proof is ephemeral and is neither journaled nor restored from checkpoints.

Unit tests cover both allocation orders, net-zero eviction/write, heartbeat
before/after commit, fully reclaimed credit, anonymous reservations, retries,
expiry, lifecycle revocation, independent replicas, default-off behavior, and
legacy wire compatibility. Real multi-node failure injection and a sustained
OSS cache-churn benchmark are separate deployment validation steps.
