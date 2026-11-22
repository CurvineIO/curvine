# Worker replication bandwidth

Limit the aggregate outgoing replica payload of a source Worker by setting:

```toml
[worker]
block_replication_bandwidth = "200MB"
```

Rates use Curvine byte units per second (`200MB` means 200 MiB/s). The default
`"0"` leaves replication unlimited. Apply the setting on each source Worker and
restart it to change the rate. Older Workers do not enforce the setting; the
replication protocol is unchanged during a rolling upgrade.

Each Worker starts with one second of byte credit and refills up to that same
burst size. All local replication copies share the budget. A copy charges the
actual payload length before sending a chunk; when the configured chunk size
exceeds the burst, the Worker uses smaller chunks. Over an interval of `t`
seconds, the bucket can grant up to `rate × (1 + t)` bytes, including burst
credit. This is not a cluster-wide or destination-side limit and does not
throttle foreground I/O, Load, or Export.

Time spent waiting for credit counts toward the existing replication job
deadline (`master.block_replication_job_timeout`). Set the rate and deadline so
that the expected block sizes can finish, including other replication work and
I/O overhead. Cancelling a wait releases the copy's concurrency permit without
spending credit for the waiting chunk; credits already granted are not refunded.

Source Worker `/metrics` counters:

- `replication_bytes_total`: payload bytes acknowledged by the destination,
  including bytes from retries or copies that later fail. Not a count of
  successfully committed replica bytes.
- `replication_throttle_wait_ms_total`: aggregate milliseconds spent waiting
  for byte credit, including cancelled waits. Concurrent waits are additive.
