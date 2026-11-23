# Where worker io_uring helps (#791)

**Keep `enable_io_uring = false` by default.** In this VM, owned async reads beat `LocalFile` on 4 KiB reads across 64 already-open TCP sessions: 132,432 vs 110,477 requests/s at lower worker CPU cost. But p99 went **up**, from 1.22 to 1.98 ms. The full 16-client `cv bench` workloads also favored `LocalFile`. The small-read result is a reason to qualify a particular workload, not a reason to switch all file I/O to a ring or introduce a fixed concurrency threshold.

## Placement in the worker

- The [stream handler](../../crates/core/rpc/src/handler/stream_handler.rs) awaits one response per connection. Independent **connections** can overlap; a single client doing one request at a time cannot hide a handoff behind other requests on that connection.
- The [read handler](../../curvine-worker/src/worker/handler/read_handler.rs) considers the ring at `Open` only if the worker has one, sendfile and read-ahead are off, and the requested chunk fits one submission. [File layout](../../crates/adapters/curvine-storage-local/src/layout/file_layout.rs) checks the actual pinned generation, physical coverage and checksum sidecar before installing the owned reader. On eligible headerless `Running` frames, the RPC task awaits completion and passes the returned buffer to the response. The blocking pool is not occupied while waiting. This is not zero-copy from disk to socket.
- A synchronous read no longer queues to the ring, waits and copies the result into a second buffer. On an owned reader, a synchronous seek/read uses `pread` at the same logical cursor. Sendfile, staged snapshots, checksum verification, sparse tails and read-ahead keep their existing data paths. Stale request IDs are rejected before either read path can seek or advance the cursor.
- **Writes are different.** [Staged writes](../../crates/adapters/curvine-storage-local/src/staged_write.rs) still have to isolate uncommitted bytes, preserve acknowledged checkpoints and publish in order. The [executor](../../crates/adapters/curvine-storage-local/src/uring.rs) still copies a borrowed write payload into an owned chunk; the blocking caller waits for the completion. The [matched write ablation](worker-io-uring-follow-up.md) found that removing a redundant checkpoint mattered more than the syscall choice; staged `pwrite` still beat staged ring writes. This review does not change write eligibility, `Flush`, `Complete`, or recovery. Copy and handoff costs are candidates for profiling, not a measured attribution of the remaining difference.

## Isolated data-read comparison

The [pre-opened benchmark](../../curvine-worker/src/worker/read_benchmark.rs) transfers 64 MiB in 16,384 headerless 4 KiB `Running` RPCs per trial. Each connection walks a distinct finalized block sequentially, with one outstanding request per connection. The worker syncs and advises eviction of each trial file before opening the sessions. Linux filesystem readahead is still possible. `/proc/<worker>/io` reported 64 MiB of backing reads in every trial; that does not establish a cold physical device. All opens and completes are outside the timing window. Client verification (block ID and offset) is included in rate but not in the per-RPC latency sample.

Medians of three rotated trials. Brackets are the observed rate range; p95 and p99 are medians of per-trial percentiles. CPU is worker process user + system seconds per 64 MiB, not host CPU. The blocking-ring arm is a test-only control with the old handoff and copy.

| Clients | Read path | Requests/s [min–max] | p95 / p99 µs | Worker CPU s |
|---:|---|---:|---:|---:|
| 1 | LocalFile | 14,392 [13,526–14,627] | 89 / 134 | 0.89 |
| 1 | Blocking ring | 9,068 [8,882–10,942] | 142 / 202 | 1.45 |
| 1 | Async ring | 11,308 [11,028–14,103] | 114 / 144 | 1.18 |
| 16 | LocalFile | 97,866 [80,810–106,884] | 256 / 396 | 0.78 |
| 16 | Blocking ring | 70,673 [68,522–80,011] | 385 / 732 | 1.09 |
| 16 | Async ring | 102,114 [96,861–102,636] | 255 / 570 | 0.58 |
| 64 | LocalFile | 110,477 [98,100–123,414] | 933 / 1,224 | 0.81 |
| 64 | Blocking ring | 114,380 [106,393–115,632] | 827 / 2,260 | 0.81 |
| 64 | Async ring | 132,432 [127,158–148,282] | 821 / 1,980 | 0.46 |

The async arm dispatched all 16,384 data frames asynchronously; the other arms dispatched none. Both ring arms completed exactly 64 MiB through the ring per trial; the local arm completed zero. At 64 clients the async arm gained about 20% in rate and used 43% less worker CPU than `LocalFile`, but its p99 was about 62% higher. At one client it lost on both rate and CPU. At 16 clients the throughput ranges overlapped. The benchmark measures the entire worker RPC, not device service time or actual kernel queue depth.

## End-to-end: `cv bench`

One loopback master and worker, 1 MiB blocks and chunks, 64 × 4 MiB files. `throughput` reads back what it just wrote; `mixed_throughput` reads from a prefilled working set and writes new files (512 MiB combined limit, up to 30 s). No forced cache eviction. Both arms disabled short-circuit, worker sendfile/splice, read-ahead and checksum verification to exercise the buffered path. Two pairs per case, order reversed for pair two. Numbers below are means of the two CLI-reported rates and per-run whole-file percentiles, **not** pooled percentiles. CPU includes the measured command but excludes cluster startup and mixed prefill.

| Workload | Clients | Op | Off → on MiB/s | Off p95 / p99 ms | On p95 / p99 ms |
|---|---:|---|---:|---:|---:|
| Throughput | 1 | Read | 674 → 668 | 10.2 / 10.2 | 8.2 / 8.2 |
| Throughput | 1 | Write | 332 → 320 | 17.3 / 17.3 | 20.1 / 20.1 |
| Throughput | 16 | Read | 2,662 → 2,123 | 32.0 / 32.0 | 35.9 / 35.9 |
| Throughput | 16 | Write | 1,903 → 1,647 | 51.3 / 51.3 | 62.7 / 63.1 |
| Mixed | 1 | Read | 191 → 196 | 8.2 / 8.2 | 8.2 / 8.2 |
| Mixed | 1 | Write | 197 → 203 | 18.7 / 19.0 | 24.6 / 24.6 |
| Mixed | 16 | Read | 1,207 → 1,048 | 32.8 / 32.8 | 32.1 / 32.1 |
| Mixed | 16 | Write | 1,295 → 1,124 | 43.0 / 43.0 | 46.4 / 46.4 |

Mean worker CPU seconds (off → on): throughput 1 client 0.56 → 0.59, 16 clients 0.77 → 0.96; mixed 1 client 0.73 → 0.67, 16 clients 0.75 → 1.01. Mixed reads and writes share a timer. The small 1-client mixed difference changed direction between the two pairs. All 16 arms had zero operation errors. Measured ring read/write bytes matched CLI read/write bytes exactly in enabled arms and were zero in disabled arms, excluding mixed prefill. Unlike the [historical debug comparison](worker-io-uring-results.md), this uses a release worker with buffered `Complete`; don't treat the difference between reports as a controlled before/after experiment.

## Would the research proposal work?

| Proposal | Decision for #791 |
|---|---|
| Applications submit io_uring operations against a Curvine mount | Useful as a POSIX compatibility test. This doesn't change how the worker handles a block RPC. A cache hit is governed by the [FUSE I/O mode](https://docs.kernel.org/filesystems/fuse/fuse-io.html), not a special bypass granted by io_uring. Don't assume `IOPOLL` works on arbitrary buffered FUSE files. |
| Replace the daemon's `/dev/fuse` request loop | A different transport, not on the `cv bench --mode client` path. The [kernel FUSE-over-io_uring documentation](https://docs.kernel.org/filesystems/fuse/fuse-io-uring.html) describes negotiated setup, per-queue entries and continued `/dev/fuse` handling for unsupported messages. A custom kernel-module transport isn't proof of a speedup on upstream FUSE. Benchmark mount workloads separately before implementing. |
| Worker storage with owned buffers and a capability probe | Yes, selectively. Preserve offset, ordering, bounds, buffer lifetime, cancellation, explicit durability and fallback before submission. Don't implement the example's `buf.to_vec()` plus copy-back for reads. Registered buffers can reduce registration overhead; they don't make ordinary buffered file I/O zero-copy. |
| One ring per core, same-shard TCP/storage, fixed files, direct I/O or polling | Experiments, not prerequisites. More rings without connection/file ownership on the same shards can retain the handoff while increasing resource use. Direct I/O also changes the cache regime. Profile queue wait, handoff, copying and device wait on representative storage before changing the transport or enabling polling. |

## Reproduction

Linux `7.0.0-38-generic`, ext4 `/dev/vda1`, 16 EPYC-Rome vCPUs on KVM, ~15.6 GiB RAM, Rust 1.98.1. Worker: 4 I/O / 16 blocking threads. Same binaries within each comparison. Worker release test SHA-256: `27dd5fd53b337cebd936c1af6c75a70a2939428592459f74dad6640adeaca8f0`; release server: `a03c9c17348f7d4d142b7122112ea88dfef2d90b7df2c64762105101af36bfcc`; unchanged debug CLI: `31d36c5e7825faefb8fb7940121fbdeb5954b19d05e6f934fc02a3a160a74b2e`. The client, worker and master share the VM; none of these short trials establishes production NVMe performance or a production latency SLO.

```sh
CURVINE_READ_BENCH_REPS=3 cargo test --locked --release -j 4 -p curvine-worker --lib \
  worker::worker_server::write_benchmark::read_benchmark::worker_read_preopened \
  -- --ignored --exact --nocapture

cargo build --locked --release -j 4 -p curvine-server --bin curvine-server
cargo build --locked -j 4 -p curvine-cli
python3 curvine-tests/benchmark/worker-io-uring-local.py \
  --server target/release/curvine-server --cli target/debug/curvine-cli \
  --repetitions 2 --files 64 --file-mib 4 --mixed-mib 512 --mixed-seconds 30
```

Require a writable ext4 test directory and working Linux ring operations. The [worker harness](../../curvine-worker/src/worker/read_benchmark.rs) emits `WORKER_READ_BENCH` JSON lines. The [end-to-end runner](worker-io-uring-local.py) creates a fresh results directory under `target/`. This checkout's final raw runs are `target/791-placement-final-reads.log` and `target/791-placement-final-e2e/` (including per-trial config, latencies, counters and CPU). No host policy or global cache settings were changed.
