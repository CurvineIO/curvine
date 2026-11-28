# Worker write benchmark: staging versus io_uring (#791)

This is the historical checkpoint-on-`Complete` result. The current harness
checks the buffered fast path; see the [follow-up](worker-io-uring-follow-up.md)
for a reproduction command and results on the new implementation.

**The staged write path accounts for most of the throughput gap on this workload.**
At 16 TCP clients, the median was 2,984 MiB/s for `LocalFile`, 516 MiB/s for
staged blocking `pwrite`, and 519 MiB/s for staged `io_uring`. The last two have
the same checkpoint and recovery behavior. Switching from ring I/O to `pwrite`
did not restore throughput. Keep `enable_io_uring` default-off.

## Original benchmark command

Use a Linux host that can execute `io_uring` and has free space for a trial on
ext4. The [ignored worker benchmark](../../curvine-worker/src/worker/write_benchmark.rs)
starts a fresh worker process per trial on loopback. It uses the real TCP block
handlers, but neither a master nor the `cv bench` client is in this measurement.
The `pwrite` variant is test-only; the default worker has no new setting.

```sh
CURVINE_WRITE_BENCH_BLOCKS=512 CURVINE_WRITE_BENCH_REPS=3 \
  cargo test --locked --release -p curvine-worker --lib -j 6 \
  worker::worker_server::write_benchmark::worker_write_ablation \
  -- --ignored --exact --nocapture > target/worker-write-ablation-791.log 2>&1
```

The default data root is the repository's `target/` on disk. To choose another
ext4 mount, set `CURVINE_WRITE_BENCH_DIR` to an existing writable directory.
Each trial deletes its private data directory after verification. Keep the
`WORKER_WRITE_BENCH` JSON lines for per-trial throughput, per-block Complete RPC
latencies, phase timers, CPU, and backend counters.

- **Build:** Rust 1.98.1, Cargo release profile; working tree on `bac29dd0`.
  The build reused a static RocksDB library from the pinned dependency version.
  The worker test executable SHA-256 was
  `1fc7f714bd4a03853e00ddd1c2a8ae1ac9ab7e518bfaaf8174fd07d6501f3396`.
- **Host:** Linux `7.0.0-38-generic`, 16 AMD EPYC-Rome vCPUs (KVM), 15.6 GiB RAM,
  ext4 on `/dev/vda1` (`rw,relatime,discard,errors=remount-ro,commit=30`).
- **Trial:** 512 fresh 1 MiB blocks, one buffered `WriteBlock` payload per block,
  then `Complete`. No explicit flush, checksum, short-circuit, splice, or
  sendfile. Worker: four I/O threads and 16 blocking threads. Each client uses
  one reused connection. Three repetitions at 1 and 16 clients with rotated
  variant order. No forced cache eviction or writeback between trials.
- **Verification:** All 18 trials completed every block. Every ring trial recorded
  exactly 512 MiB of completed ring writes; the other trials recorded zero.
  Each staged trial checkpointed all 512 blocks; the `LocalFile` trials did not
  use the staged checkpoint. Three finalized blocks were checked byte-for-byte
  outside each timing window.

## Results

Throughput is median with the observed min–max. Payload and checkpoint times are
the median of the per-trial worker means. Payload includes the write and staged
view update, not the completion checkpoint. Complete RPC latency is the median
of the per-trial percentiles, not a pooled percentile. Worker CPU is median user
plus system process seconds for 512 MiB, excluding client CPU and any work
charged to other kernel tasks. Concurrent per-block timings can overlap.

| Clients | Backend           |           MiB/s [min–max] | Payload µs/block | Checkpoint ms/block | Complete RPC p50/p95 ms | Worker CPU s |
| ------: | ----------------- | ------------------------: | ---------------: | ------------------: | ----------------------: | -----------: |
|       1 | `LocalFile`       |       814.1 [415.7–865.9] |              366 |                   — |             0.18 / 0.26 |         0.53 |
|       1 | Staged `pwrite`   |       164.1 [160.9–164.1] |              370 |                4.82 |             4.81 / 6.42 |         0.71 |
|       1 | Staged `io_uring` |       156.2 [142.4–160.3] |              663 |                4.80 |             4.78 / 6.59 |         0.85 |
|      16 | `LocalFile`       | 2,984.3 [2,842.5–3,496.8] |              560 |                   — |             1.18 / 2.85 |         0.87 |
|      16 | Staged `pwrite`   |       516.0 [512.3–571.9] |            1,062 |               24.17 |           25.14 / 32.18 |         1.32 |
|      16 | Staged `io_uring` |       518.9 [507.9–528.5] |            2,223 |               23.36 |           24.55 / 31.73 |         1.55 |

The staged `pwrite` arm falls about 80–83% below `LocalFile` at the median. These
arms do different persistence work at `Complete`: the staged arms sync and
install a recoverable data/watermark checkpoint. That is a feature cost, not a
syscall-only comparison. The `pwrite` and `io_uring` arms hold that work constant.
The ring increased worker CPU by about 20% at one client and 17% at 16. It was
about 5% slower at one and within run-to-run variation at 16. The payload timer
also rose, but does not isolate copies, handoff, or queue wait individually.

Prioritize investigating the checkpoint cost while preserving recovery behavior.
This short loopback VM benchmark does not estimate production latency or establish
how the ring would behave on an I/O-bound workload. It complements, rather than
replaces, the [end-to-end `cv bench` comparison](worker-io-uring-results.md).
