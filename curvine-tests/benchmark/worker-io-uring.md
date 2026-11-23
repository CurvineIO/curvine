# Worker io_uring: rollout and benchmarking

This guide describes the opt-in worker file backend for Curvine #791. Use a
worker build that includes the feature. See the [initial Linux loopback results](worker-io-uring-results.md)
and the [follow-up for buffered Complete and async reads](worker-io-uring-follow-up.md).
The [placement review](worker-io-uring-placement.md) measures the current read routing.

## Enable, check, and roll back

The worker defaults to the existing positional file I/O backend. In the cluster
TOML used by the worker (see the
[repository example](../../etc/curvine-cluster.toml)), add the setting to the
existing `[worker]` table:

```toml
[worker]
enable_io_uring = false
```

`false` is the default. To try io_uring, set it to `true` and restart a canary
worker. Expect one backend-selection log entry per worker startup identifying
what was **requested**, what is **effective**, and the **reason for fallback**
if applicable. Check the worker log (the example config uses `logs/worker.log`)
or the logs collected by your service manager. If io_uring was requested but
the existing backend is effective, record the reason; this is not an io_uring
run. Roll back by setting `enable_io_uring = false`, restarting the affected
workers, and confirming the existing backend is effective. No client restart or
configuration change is required for rollout or rollback.

Support on Linux is determined by probing the actual required ring operations,
including successful buffered positional reads and writes, not just by checking
the kernel version or creating a ring. The probe must work in the worker's real
runtime context. Kernel policy (for example, `kernel.io_uring_disabled` where
available), container seccomp filters, or service sandbox restrictions can deny
setup or operation. An unsupported platform or failed capability probe falls
back to the existing backend. A successful probe on the host as an administrator
does not establish support inside a restricted worker. Leave the fallback in
place if deployment policy prohibits io_uring.

To reproduce the startup fallback on Linux without changing host policy, run
this test with a version of `strace` that supports syscall injection and is
allowed to trace a child process:

```sh
cargo test --locked -p curvine-server --test worker_test \
  test_worker_io_uring_setup_denied_falls_back_over_rpc -- --ignored
```

The test injects `EPERM` into `io_uring_setup`, checks the worker reports
`requested=io_uring, effective=local_file` with the setup error, and writes,
flushes, and reads back a block over TCP with the bypass paths disabled.
On a ring-capable Linux host, run the real-ring TCP tests separately:

```sh
cargo test --locked -p curvine-server --test worker_test 'real_io_uring::' -- --ignored
```

These exercise single and batch writes and restart recovery. The workload
completion counters must increase, so a fallback run cannot pass.

The ring handles positional, page-cache-backed I/O. Buffered remote reads and
eligible single and batch writes can use it. Ring writers start on **fresh
file blocks** only. Existing blocks, allocation/resize requests, short-circuit
writes, splice, and writes on workers configured with SPDK stay on the existing
backend. `sendfile` still bypasses buffered reads. Headerless remote reads of
at most 1 MiB from fully physical, finalized file blocks can await a ring
completion directly and return its buffer if sendfile, checksum verification,
and read-ahead aren't in use. Synchronous read frames (including seeks) use
ordinary file I/O, not a blocking wait on the ring. Staging snapshots, sparse
tails, checksum-verified reads, and read-ahead keep their existing paths.
Eligible staged writes still wait for ring completion from the blocking pool.
Single-block TCP payloads and decoded batch members of at most 1 MiB can share
an owned buffer with the ring; larger and borrowed payloads copy one admitted
chunk at a time. See the [matched write benchmark](worker-io-uring-owned-writes.md).
The RPC transport is unchanged.

A ring writer uses a private staging inode and exposes only completed bytes to
live readers. For a nonempty fresh block with no prior successful checkpoint,
`Complete` can publish that inode without an intermediate durable checkpoint.
This preserves buffered `Complete`: it acknowledges logical publication, not
persistence across power loss. Explicit `Flush` still persists a recoverable
checkpoint; a writer with a prior checkpoint keeps the existing completion
path. Post-flush writes and overwrites of an exposed prefix may copy the block.
Allow space for scratch files and inodes still pinned by readers. Successful
ring completions alone are not proof of publication or durability.
On an I/O error, the worker does not replay submitted writes through `LocalFile`.
The block and watermark encodings, client configuration, and RPC transport are
unchanged. Disabling the flag and restarting the same build still runs recovery
for any interrupted staging checkpoint.

## Benchmark the eligible path

Use an isolated test cluster, ideally with one eligible worker, and run `cv`
from a separate client host. Use a native `cv://` test path (not a FUSE mount or
a UFS-backed mount) and confirm both reads and writes hit the target worker's
file-backed data directory. For this experiment only, use `[client]`
`short_circuit = false` and `enable_read_ahead = false` in the benchmark client's
config. Use `[worker]` `enable_send_file = false`, `enable_splice = false`, and
`block_checksum_verify = false` on the isolated test worker, in
**both** arms. These are existing controls to keep bypass paths out of the
comparison, not requirements for deployment.

Use the same binaries, client config, and worker config except for
`worker.enable_io_uring`. Keep worker placement and replica count, disk and
filesystem (including mount options), hardware, network, CPU/memory limits, and
worker thread and buffer settings fixed. Record them along with the kernel and
sandbox profile. Restart before each arm, check the effective backend, and wait
for the worker to be ready. Restarting a worker does not clear the kernel page
cache. Use the same cache preparation, warm-up, and writeback settling policy
before each paired trial. The throughput suite reads immediately after writing,
and the mixed preset reads from pre-seeded files; neither is a cold-read test
by default. Record the dataset size relative to RAM and label the cache regime.

Run both workloads at low (`1`) and high (`16`) client concurrency, repeating
with the flag off and on. `--threads` controls benchmark concurrency, not the
worker thread count or ring depth. This example holds the single-op file count
and mixed read seed count at 128 for both concurrency levels because
`--big-file-count` is **per thread**. The `throughput` preset writes the dataset
then reads it back; it ignores `--duration`, `--total-size`, and `--total-ops`.
The `mixed_throughput` preset is a 50:50 mix of big-file read/write operations
by weight, seeded before timing. The mixed example stops starting new work at
the duration or combined byte limit, whichever is reached first; in-flight
operations can overshoot. Size the files, count, and limits for available disk
space and enough samples for tail latency, and use the same settings in each
off/on pair.

Run the following on the client after configuring and restarting the worker for
the desired arm. Replace the example `CLIENT_CONF` with the client TOML for the
test cluster and choose a fresh `RUN_ID` for each arm and repetition. Run the
commands individually if your cache preparation requires a reset or warm-up
between workloads or concurrency levels. `cv` is the packaged CLI wrapper
([source](../../build/bin/cv)); flags are defined in
[`curvine-cli/src/cmds/bench.rs`](../../curvine-cli/src/cmds/bench.rs).

```sh
(
  set -eu
  CLIENT_CONF=/etc/curvine/bench-client.toml
  RUN_ID=off-01  # e.g. on-01 after enabling and restarting the worker
  RESULTS="./io-uring-results/$RUN_ID"
  mkdir -p ./io-uring-results
  mkdir "$RESULTS"

  for threads in 1 16; do
    files_per_thread=$((128 / threads))
    base="cv://default/__io_uring_bench__/$RUN_ID"

    cv --conf "$CLIENT_CONF" bench --mode client --path "$base/throughput-p$threads" \
      --workload throughput --threads "$threads" --block-size 256KB \
      --big-file-size 64MB --big-file-count "$files_per_thread" --json \
      > "$RESULTS/throughput-p$threads.json"

    cv --conf "$CLIENT_CONF" bench --mode client --path "$base/mixed-p$threads" \
      --workload mixed_throughput --threads "$threads" --block-size 256KB \
      --big-file-size 64MB --big-file-count "$files_per_thread" \
      --duration 60s --total-size 16GB --json \
      > "$RESULTS/mixed-throughput-p$threads.json"
  done
)
```

Repeat the pairs and alternate which backend runs first to reduce order effects.
Use fresh test paths and check for leftover data or delayed reclamation before
the next trial. Check stderr as well as exit status and reported errors: a
client config load failure can fall back to defaults, and a completed command
can still report operation errors. Don't treat either as a valid comparison.

## Verify completed ring I/O separately

Startup logs and probe success show which backend was selected, not that the
workload used it. Use a separate verification run with the same worker and
client configuration, routing, and workloads so the tracing overhead does not
enter the performance numbers. Require successful **data-bearing read and write
completions** before attributing workload results to io_uring. On the worker's
`/metrics` endpoint, compare `worker_io_uring_read_bytes` and
`worker_io_uring_write_bytes` before and after the measured phase. These
counters exclude the startup probe and count bytes in successfully completed
ring chunks; they do not count a partially completed chunk that later errors,
or establish that a write was flushed or committed. Expect zero deltas in the
disabled arm. For a separate kernel-level audit:

1. On the worker host, use an approved kernel tracing tool such as `perf` with
   the running kernel's io_uring tracepoints. Start with `perf list 'io_uring:*'`
   and inspect the tracepoint formats exposed by that kernel for submit opcode,
   ring/request identity, and completion result. Event names, fields, and
   access vary. Validate the capture and decoding first against a known-good
   ring program that reads and writes a regular test file and checks the byte
   counts. Use the kernel's matching opcode definitions.
2. Start capture after worker startup. Isolate benchmark traffic and record the
   worker's host PID or cgroup, the ring identity, and the data-phase time
   window. Attribute submissions to the worker's ring; match them to
   completions by ring and request identity (or `user_data` if unique among
   outstanding requests). Completions can run in another kernel task, so don't
   filter completion events by the submitting PID alone. Count successful
   nonzero-byte read and write completions. Exclude probe, prefill or seed I/O, and
   unrelated activity. If the trace cannot separate seeding
   from measured I/O, it is not evidence for the measured mixed workload.
3. Repeat with the switch off as a negative control: the same workload should
   transfer data without corresponding ring completions for this path. Save
   the procedure, trace or event counts, time window, and any trace loss. A
   ring FD, setup or submit syscall, submission alone, EOF, or failed completion
   is not evidence of completed data transfer. If the tool is unavailable or
   the events cannot be reliably attributed, report **ring use unverified**;
   do not label the comparison as verified io_uring performance.

## Record and interpret

- Keep the raw JSON and record read and write throughput separately in `MiB/s`,
  bytes and operations completed, and errors. Use `latency_p95_ms` and
  `latency_p99_ms` from JSON for each operation, with `latency_samples`: even
  though the single-op throughput text table is average-only, JSON includes
  the bucketed percentiles. These are client-observed whole-file latencies,
  not worker block or ring latencies. Flag sparse samples instead of drawing
  conclusions from unstable tails.
- Record worker CPU for each trial using the worker process's user and system
  CPU time (for example, use `pidstat -u -p <worker-pid> 1` and align samples
  with the data phases). State the units and window. Exclude startup, seeding,
  and cleanup where possible; if only whole-command CPU is available, label it
  as such. Process CPU may omit work charged to separate kernel tasks; report
  host or worker-cgroup CPU separately if collected.
- Keep requested/effective backend and fallback reason alongside each report,
  with the separate completion evidence. Report fallbacks, failures, and
  unverified runs explicitly. Compare only matched trials and report the
  measured variation across repetitions. Do not infer a speedup from the flag
  being enabled or fill in missing measurements.
