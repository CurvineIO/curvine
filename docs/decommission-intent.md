# Durable Worker decommission intent

The Master stores the decommission list by Worker hostname, not by Worker ID.
Once a hostname is added, a Worker that registers under that hostname is still
marked decommissioned even if its ID has changed. Removing a hostname clears
that intent. The existing list, add, and remove endpoints are unchanged. This
feature does not drain or migrate blocks and does not make it safe to stop a
Worker that still holds needed replicas.

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

## Rollback boundary

Before writing any new decommission entries, this change does not alter the
existing Raft entry formats. Once a new entry has been written, do not run an
older Master against the resulting journal or checkpoints. An older binary
cannot replay the new variant and does not enforce the stored intent. Turning
`decommission_intent_enabled` off stops new writes; it does not remove history
or make downgrading safe. To roll back to an older binary, use a consistent
pre-activation backup and account for metadata writes made since that backup.
