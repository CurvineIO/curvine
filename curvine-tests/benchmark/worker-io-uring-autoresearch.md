# Worker io_uring experiments on the Linux VM

The worker now uses up to two Compio rings when `worker.enable_io_uring` is enabled. It sends each admitted operation to the ring with fewer outstanding requests. A tie goes to the first ring, so a serial stream reuses the same issuer. The rings share the existing limit of 64 admitted operations. Each issuer owns its runtime and drains accepted work on shutdown. The worker uses one ring on a single-CPU host or when the second ring cannot start.

The experiments ran on one 16-vCPU KVM guest with Linux `7.0.0-38-generic`, Rust 1.98.1, and ext4 on `/dev/vda1`. The checkout included the fresh-staging and profiling work from the previous experiment. The NVMe host was unavailable. Throughput here characterizes the virtual guest and its page cache and storage stack.

## What the kernel trace showed

Two baseline trials each wrote 512 fresh 1 MiB blocks through one ring at 16 clients. Linux queued all 1,024 writes to `io-wq`. A single kernel worker completed the 512 writes in each trial. On this kernel, ext4 supports asynchronous buffered reads. Its buffered writes follow the `io-wq` path when the initial nonblocking issue cannot proceed. `IOSQE_ASYNC` marks work concurrent, which can cause the kernel to start another worker. [Linux write path](https://github.com/torvalds/linux/blob/v7.0/io_uring/rw.c#L1122), [ext4 file operations](https://github.com/torvalds/linux/blob/v7.0/fs/ext4/file.c#L962), [io-wq scheduling](https://github.com/torvalds/linux/blob/v7.0/io_uring/io-wq.c#L1023), [SQE flags](https://man7.org/linux/man-pages/man3/io_uring_sqe_set_flags.3.html)

| Worker prototype | 1 MiB writes traced | Kernel completion workers per ring | Submit to CQE p50 / p95 |
|---|---:|---:|---:|
| Original one ring | 1,024 | 1 | 4.89 / 7.10 ms |
| Two rings, round robin | 1,024 | 1 per issuer | 1.10 / 2.29 ms |
| One ring, forced async writes, cap 8 | 1,024 | 7 observed | 0.62 / 1.52 ms |

The trace matched each payload write by ring, request and user data. These intervals include kernel queueing and buffered-I/O work. Separate, untraced runs supply the throughput and latency results below.

## Designs screened

3FS starts multiple read workers with ring resources and stable buffer registrations. Ceph's kernel block backend batches submissions and uses fixed descriptors. Those designs motivated the ring-count and registration comparisons. Gluster's configurable POSIX brick backend motivated the matched `pwrite` arm and an explicit flush workload. SeaweedVFS's reusable command slots motivated comparing the standard Linux FUSE commit-and-fetch protocol with different userspace ring layouts. [3FS AIO workers](https://github.com/deepseek-ai/3FS/blob/main/src/storage/aio/AioReadWorker.cc), [Ceph io_uring backend](https://github.com/ceph/ceph/blob/main/src/blk/kernel/io_uring.cc), [Gluster POSIX backend](https://github.com/gluster/glusterfs/commit/aafb69a), [SeaweedVFS ring design](https://github.com/seaweedfs/seaweedvfs-kmod/blob/main/IOURING_RING.md)

The first write screen used 512 fresh blocks and 16 loopback TCP clients per trial. Each arm got a warmup, then six measured groups ran in rotated order. The rate is the median of the six trials. Paired wins compare each arm against the single ring within the same group.

| Worker ring prototype | Median MiB/s | Paired wins |
|---|---:|---:|
| One ring | 2,188 | — |
| Drain up to 16 ready requests at once | 2,030 | 1/6 |
| Single issuer with deferred task work | 2,154 | 2/6 |
| Forced async writes with a cap of 2 / 4 / 8 / 16 | 2,860 / 2,817 / 2,882 / 2,847 | 6/6 each |
| Round-robin dispatch across 2 / 4 / 8 rings | 3,114 / 2,886 / 2,613 | 6/6, 6/6, 5/6 |

The forced-write arm set the bound on regular-file I/O workers via `IORING_REGISTER_IOWQ_MAX_WORKERS`. The deferred arm used `SINGLE_ISSUER`, `DEFER_TASKRUN`, and `TASKRUN_FLAG`. The queue-drain arm only grouped requests that had already arrived. [Worker limits](https://man7.org/linux/man-pages/man3/io_uring_register_iowq_max_workers.3.html), [Ring setup flags](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html), [Submit and wait](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html)

## Dispatch and workload checks

The next write comparison ran the original ring, round robin, and least-busy dispatch in the same release benchmark binary. Each trial verified finalized lengths and contents, completed ring bytes, shared payload bytes, and checkpoint or buffered publication counts. Block latency covers `Open`, every payload RPC, the optional `Flush`, and `Complete`. Percentiles are medians of per-trial percentiles.

| Buffered workload | Clients | Original ring MiB/s | Round robin MiB/s | Least busy MiB/s | Least busy block p95 vs original |
|---|---:|---:|---:|---:|---:|
| 1 MiB payload, 512 blocks | 1 | 752 | 770 | 791 | 1.50 vs 1.60 ms |
| 1 MiB payload, 512 blocks | 16 | 2,175 | 3,174 | 3,233 | 7.23 vs 9.33 ms |
| 4 KiB payloads, 48 blocks | 1 | 34.7 | 26.5 | 34.6 | 35.5 vs 33.2 ms |
| 4 KiB payloads, 48 blocks | 16 | 231 | 232 | 243 | 79.1 vs 74.2 ms |

Least busy won all six 16-client 1 MiB trials. Its median paired gain was 50.5%. The median block p99 moved from 12.60 to 13.46 ms and worker CPU from 0.825 to 0.905 s per 512 MiB. The 16-client 4 KiB result was noisy. Least busy won two of five pairs and staged `pwrite` delivered 360 MiB/s at its median.

In the earlier screen, 128 KiB writes at 16 clients reached 2,433 MiB/s with two round-robin rings versus 1,997 MiB/s with one ring. At one client, staged `pwrite` led at 571 MiB/s versus 415 MiB/s with one ring. An explicit flush after every 1 MiB payload brought the 16-client ring medians within 1% of one another. Each flush benchmark verified one staged checkpoint per block. [io_uring fsync semantics](https://man7.org/linux/man-pages/man3/io_uring_prep_fsync.3.html)

On reads, the first two-ring prototype raised median warm sequential throughput at 16 clients from 2,317 to 3,023 MiB/s and lowered read RPC p95 from 1.14 to 0.89 ms. Median cold-advised random throughput at 16 clients moved from 51.7 to 58.4 MiB/s. The follow-up preopened benchmark issued 16,384 verified 4 KiB reads per trial and excluded session setup from the timer. It synced each file and requested eviction before reading, so filesystem readahead and guest caching can affect the measurements.

| Preopened 4 KiB reads | Original ring MiB/s | Least busy MiB/s | Least busy RPC p95 / p99 vs original |
|---|---:|---:|---:|
| 1 client | 53.4 | 52.9 | 91 / 115 vs 93 / 117 µs |
| 16 clients | 383 | 420 | 229 / 470 vs 250 / 694 µs |
| 64 clients | 499 | 593 | 692 / 1,383 vs 780 / 1,874 µs |

Least busy won three of four groups at both 16 and 64 clients. Pinning every read to the first ring yielded 335 and 515 MiB/s at those concurrencies. Skipping read-buffer initialization moved the results to 459 and 607 MiB/s, with overlapping ranges. The earlier warm sequential screen favored initialized buffers when using two rings.

## Full Curvine cluster

The loopback `cv bench` matrix used a release master and worker, the same existing CLI executable for every arm, 4 MiB files, and 1 MiB worker chunks. The cluster ran with short circuit, sendfile, splice, and client read-ahead disabled so the measured buffers exercised the selected path. All 48 arms passed the harness checks for successful operations, effective backend, and exact byte counters. The table shows medians across three trials per cell.

| Workload | Clients | LocalFile read / write MiB/s | Original ring read / write MiB/s | Least busy read / write MiB/s |
|---|---:|---:|---:|---:|
| Sequential throughput | 1 | 662 / 346 | 647 / 307 | 670 / 317 |
| Sequential throughput | 16 | 2,334 / 1,844 | 2,018 / 1,746 | 2,306 / 1,875 |
| Mixed throughput | 1 | 230 / 237 | 202 / 208 | 215 / 222 |
| Mixed throughput | 16 | 1,075 / 1,153 | 1,060 / 1,137 | 1,018 / 1,092 |

At 16 clients, least busy won all three sequential read and write comparisons against the original ring. Median paired gains were 14.3% for reads and 9.0% for writes. Median whole-file p95 fell from 49.3 to 34.6 ms for reads and from 59.9 to 51.4 ms for writes. Round robin reached 1,583 MiB/s median write throughput in this case.

At 16 clients, the median paired change in mixed throughput was +0.5%. Its two directions share the same interval. The median rates in the table are computed per arm, so the difference between arm medians can diverge from the paired change. Whole-file write p95 rose from 53.6 to 57.9 ms in this mixed workload.

## Mounted FUSE transport

Two memory-backed daemons exercised the real Linux mount interface. One used libfuse 3.18.2 with either legacy `/dev/fuse` or one ring per CPU. The other implemented the kernel ABI directly with one shared ring serving all 16 kernel queues. Both used zero attribute TTL and direct file I/O. Reads checked the returned size and data sentinels. Writes read back and checked every touched offset. The stock libfuse package fell back when asked for the ring, so the comparison used a local libfuse build with liburing support. A traced smoke run recorded 1,568 `URING_CMD` submissions from the libfuse ring threads. Each measured mount checked its effective ring count. [Linux FUSE-over-io_uring](https://docs.kernel.org/7.0/filesystems/fuse/fuse-io-uring.html), [libfuse ring implementation](https://github.com/libfuse/libfuse/blob/fuse-3.18.2/lib/fuse_uring.c)

Depths 1, 4, and 8 worked in the first screen. Depth 4 led the 16-client `getattr` and 128 KiB mixed tests. The follow-up ran 13 workloads on three transport configurations across five groups. The table shows medians across the five runs per cell.

| Workload | Clients | Legacy calls/s | Shared ring calls/s | Per-CPU rings calls/s | Legacy → per-CPU p95 |
|---|---:|---:|---:|---:|---:|
| Uncached attributes | 1 | 38,000 | 58,300 | 139,000 | 41.7 → 8.6 µs |
| Uncached attributes | 16 | 206,000 | 188,000 | 895,000 | 167 → 23 µs |
| Uncached attributes | 64 | 374,000 | 224,000 | 1,242,000 | 266 → 65 µs |
| Negative lookup | 16 | 23,300 | 185,000 | 61,300 | 1,458 → 674 µs |
| 4 KiB reads | 16 | 224,000 | 157,000 | 886,000 | 153 → 23 µs |
| 4 KiB writes | 16 | 201,000 | 166,000 | 666,000 | 164 → 34 µs |
| 128 KiB reads | 16 | 139,000 | 28,400 | 238,000 | 170 → 72 µs |
| 1 MiB reads | 16 | 19,200 | 1,760 | 25,800 | 1,286 → 827 µs |
| 128 KiB mixed | 16 | 79,900 | 24,800 | 205,000 | 676 → 153 µs |

At 16 clients, median daemon CPU per `getattr` went from 40.3 to 4.6 µs in libfuse and to 5.4 µs in the shared-ring daemon. The shared daemon executes handlers on a single issuer, and both its handlers and negotiation differ from libfuse. The cross-daemon numbers describe the measured implementations. The libfuse modes keep the callbacks identical.

A follow-up varied the legacy thread limit and enabled fd cloning. At 16 clients the best legacy `getattr` rate in this comparison came from four threads, at 446,000 calls/s and 48.7 µs p95. The same three groups measured 922,000 calls/s and 21.7 µs p95 with per-CPU rings.

## Registered files, buffers, and SQPOLL

A native liburing experiment replayed cached reads at depths 1 and 16, with 4 KiB and 128 KiB buffers. It compared ordinary descriptors, preregistered files, fixed buffers, both registrations, and updates to the fixed-file table once per batch. Each group checked CQE length and returned bytes. The test circulated among 64 opened descriptors over a prewarmed file. [Registered files](https://man7.org/linux/man-pages/man7/io_uring_registered_files.7.html), [Registered buffers](https://man7.org/linux/man-pages/man7/io_uring_registered_buffers.7.html)

| Cached reads, median paired change | 4 KiB, depth 1 | 4 KiB, depth 16 | 128 KiB, depth 1 | 128 KiB, depth 16 |
|---|---:|---:|---:|---:|
| Fixed files plus fixed buffers | −9.4% | +23.1% | +9.7% | −8.6% |
| Update file registrations every batch | −21.1% | +3.7% | −8.1% | +0.1% |
| SQPOLL | −89.9% | −42.1% | −37.0% | −10.2% |

Fixed buffers alone gained 20.7% in the cached 4 KiB depth-16 test, which reused the same buffers for every batch. Curvine's staged worker writes directly from RPC-owned buffers and opens a scratch file per fresh block. SQPOLL rates apply to this batched client and VM. The process CPU samples exclude the polling thread.

## Final implementation verification

The final and exact single-ring baseline builds used the same compiler, dependencies, and updated benchmark harness. The two builds differed in the worker ring implementation. Six measured groups alternated build order after a warmup. Each invocation wrote 512 fresh 1 MiB blocks with 16 TCP clients and verified all bytes and publication counts.

| 16-client buffered writes | Original ring | Final worker | Median paired change |
|---|---:|---:|---:|
| Rate, MiB/s | 2,248 [2,047–2,414] | 3,363 [3,172–3,605] | +45.4%, 6/6 wins |
| Block latency p95, ms | 8.94 | 6.86 | |
| Block latency p99, ms | 14.20 | 13.00 | |
| Worker process CPU, s per 512 MiB | 0.81 | 0.89 | |

The matched staged `pwrite` control had medians of 3,071 and 3,047 MiB/s. The explicit-flush rerun checked all checkpoints and produced overlapping ring throughput ranges. The final worker won all three 16-client warm sequential read trials, with medians of 2,433 → 2,788 MiB/s and read RPC p95 of 1.32 → 0.99 ms. The cold-advised random read ranges overlapped at both 1 and 16 clients.

Release verification passed 161 storage-adapter tests and 175 worker tests. Six targeted real-kernel test invocations passed, including round trips on one CPU and on the full guest, sparse reads, sendfile selection, and staged write visibility and checkpoint recovery. Clippy passed with the profiling feature enabled. The final release server passed four `cv bench` smoke trials covering sequential and mixed I/O with verified ring byte counters.

The benchmark accepts `CURVINE_WRITE_BENCH_CHUNK_BYTES` for chunk sizes that divide the 1 MiB block. It records full-block and `Complete` latency samples. Use an existing writable ext4 directory for its per-trial data, with room for the requested block count. [Write harness](../../curvine-worker/src/worker/write_benchmark.rs)

```bash
CURVINE_WRITE_BENCH_DIR="$PWD/target" CURVINE_WRITE_BENCH_STAGE_PAIR=1 \
  CURVINE_WRITE_BENCH_BLOCKS=512 CURVINE_WRITE_BENCH_REPS=6 \
  cargo test --locked --release -p curvine-worker worker_write_ablation -- --ignored --nocapture
```

The VM artifacts include the experiment sources, paired benchmark runners, kernel traces, release verification logs, and raw rows. [Published end-to-end measurements](results/io-uring-791/README.md)
