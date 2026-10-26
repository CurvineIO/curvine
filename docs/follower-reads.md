# Follower metadata reads

Follower reads are opt-in at the master, worker, and client. Roll them out in
that order so clients do not send block-location requests to followers before
those followers have worker state.

## Configuration

Set these options in the corresponding TOML sections on every master, worker,
and client:

```toml
[master]
follower_reads_enabled = true
follower_worker_state_enabled = true
follower_read_max_commit_lag = 0
follower_read_apply_stall_timeout = "30s"

[worker]
follower_reports_enabled = true

[client]
follower_reads_enabled = true
follower_write_sticky_ms = 5000
```

All switches default to `false`. `follower_read_max_commit_lag = 0` requires a
follower to have applied its locally observed committed entries. The allowed
range is 0–10,000 entries. The apply-stall timeout defaults to 30 seconds and
must be between 1 ms and 10 minutes; it only runs while committed entries are
waiting to be applied, so an idle follower does not become ineligible merely
because it has no new work.

The client sticky window must be positive when client follower reads are
enabled; its default is 5 seconds. Each client context remembers at most 1,024
mutation paths, each up to 4 KiB. Oversized paths or a full path table pin all
reads to the leader for the sticky window. Large encoded mutation batches also
fall back to pinning the full namespace when they exceed 1,024 paths or contain
a path over 4 KiB.

Worker report mirroring is bounded. Defaults are 256 queued operations per
follower, 64 MiB across worker queues, 32 peer queues, and 16 concurrent peer
RPCs. The configured caps are 4,096 operations, 1 GiB, 32 queues, and 32
concurrent RPCs. A queue overflow or an omitted peer requires a full report
resync; followers whose worker report state is incomplete must refuse
location-dependent reads. The shared resync inventory also consumes this byte
budget. If a full inventory cannot fit, the peer stays unready and reads fall
back to the leader; increase the budget for large worker inventories. Budget
failures are retried with a delay rather than rebuilding in a tight loop. Retry defaults are 30 seconds total, starting at
100 ms and capped at 5 seconds between attempts. The maximum retry timeout is
5 minutes and the maximum retry delay is 60 seconds.

## Rollout order

1. Enable follower admission and passive worker state on the masters. Keep the
   commit-lag limit at zero unless the application explicitly accepts staler
   metadata.
2. Enable worker report mirroring and restart workers. Keep `worker.follower_report_max_peer_queues`
   at least as large as the number of follower masters that need worker state.
3. Wait for workers to send their Start heartbeats and complete full block
   reports to every candidate follower. Do this before enabling clients; older
   workers that do not mirror reports cannot make follower block locations
   complete.
4. Enable follower reads on clients and monitor follower refusals and fallback
   traffic. A refusal or peer/network error causes one leader fallback.

The client tries at most one follower for an eligible read. It bounds that
attempt to half of the remaining `client.rpc_timeout_ms` deadline, then uses
the remaining time for the leader route. It does not cycle through every
follower on a slow or unavailable peer.

## Read consistency

The client allowlist is `Exists`, `FileStatus`, `ListStatus`, `ListOptions`,
`GetMountTable`, `GetMountInfo`, `GetBlockLocations`, and `OpenFile` with a
valid read-only flag set. Writes, trash operations, unknown RPCs, malformed
requests, and write-capable `OpenFile` flags stay on the leader path.
Only the direct follower attempt carries the follower-read marker. Sticky
reads and all leader fallbacks remain unmarked so followers refuse them.

Recent mutations pin related reads in that client context to the leader while
the sticky window is live. In-flight writes stay pinned until their RPC ends,
then the configured window starts, including when the RPC fails or is
cancelled. Matching includes the mutated path, descendants, and ancestor
listings. This is a finite, per-client hint: it does not make reads linearizable
or provide cross-client read-after-write guarantees. After the window expires,
eligible reads may use a follower. A nonzero commit-lag setting also
intentionally permits followers to serve a view behind the current leader.

Use follower reads for workloads that can tolerate those limits. Keep the
feature disabled when every read must observe the latest committed write.
