# Incremental gRPC read-path experiments

Each priority was implemented as a separate checkpoint and benchmarked before moving on. The final retained version removes P4 batching and defaults to a read window of one because a window of four caused a substantial P99 regression on the bandwidth-limited profile. The read pipeline remains available through `client.read_pipeline_window` (1–32).

## Method

- Same host and optimized LLVM release toolchain for all checkpoints, with mold, sccache and line-table debug information. These are independent of the earlier Cranelift ORPC comparison.
- Linux namespaces and `tc netem`: 1 Gbit/s or 10 Gbit/s in each direction, 1 ms one-way delay, four readers, 64 MiB file, 1 MiB chunks, 256 reads per reader (1 GiB total), 16 warmup reads per reader. Remote reads; high-level read-ahead disabled; warm cache. Readers rewind every 64 chunks. The next timed read after a seek includes any speculative-reply drain, although the seek call itself is excluded from its latency sample.
- Three isolated repetitions per checkpoint and profile. Checkpoints alternate in reverse order on the second round to reduce monotonic drift. Plaintext gRPC without compression. No compilation ran during measurements. Workers are terminated before the next trial.
- Tables show medians. CPU is combined client and server-namespace CPU seconds per GiB; timed reads include seek and discarded-prefetch overhead. RSS is the median sampled peak aggregate process RSS across the whole run, including startup/warmup/teardown, sampled every 100 ms; it excludes kernel page cache and may count shared process pages more than once.
- These are virtual-network measurements and do not establish the representative 25/100 GbE release gate. Three repetitions do not establish statistical significance; small changes should be treated as inconclusive.

## 1gbit, 1 ms one-way delay

| Checkpoint | Read MB/s | P99 ms | CPU s/GiB | Peak RSS MiB | CPU change from previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 118.8 | 71.85 | 3.66 | 157.9 | — |
| 1. Payload buffers | 118.8 | 79.44 | 3.26 | 145.7 | -10.9% |
| 2. HTTP/2 windows | 118.9 | 78.13 | 3.21 | 145.4 | -1.5% |
| 3. Read window = 4 | 113.7 | 218.65 | 2.71 | 198.0 | -15.6% |
| 4. Blocking batches | 113.9 | 178.56 | 2.60 | 197.1 | -4.1% |
| 5. Typed payload dispatch | 113.8 | 141.22 | 2.62 | 200.8 | +0.8% |
| Final retained defaults | 118.6 | 74.63 | 3.20 | 148.5 | +22.1% |
| Final, opt-in read window = 4 | 113.9 | 184.89 | 2.82 | 197.6 | -11.9% |

## 10gbit, 1 ms one-way delay

| Checkpoint | Read MB/s | P99 ms | CPU s/GiB | Peak RSS MiB | CPU change from previous |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 614.8 | 10.47 | 4.54 | 160.4 | — |
| 1. Payload buffers | 669.5 | 8.66 | 4.20 | 151.9 | -7.5% |
| 2. HTTP/2 windows | 651.5 | 8.83 | 4.07 | 151.2 | -3.1% |
| 3. Read window = 4 | 935.1 | 11.88 | 3.89 | 209.3 | -4.4% |
| 4. Blocking batches | 933.3 | 12.84 | 4.08 | 215.2 | +4.9% |
| 5. Typed payload dispatch | 1064.2 | 13.21 | 3.79 | 215.7 | -7.1% |
| Final retained defaults | 652.4 | 9.48 | 4.09 | 152.5 | +7.9% |
| Final, opt-in read window = 4 | 1070.1 | 12.68 | 3.68 | 213.0 | -10.0% |

The final two rows are configuration/selection results, not additional isolated priorities. The retained-default row removes batching and disables speculative reads; the last row enables window four on those same final binaries. P3–P5 were measured with read window four. P4 is preserved as an experimental commit but is absent from the final source. Full per-run values, ranges, binary hashes and source revisions are in [results.json](results.json).

## Decisions

- P1 retained: payload adapters use reference-counted buffers. Protobuf/HTTP2 encoding still copies data; this does not restore sendfile.
- P2 retained with bounded receive credit. The preliminary six-run alternating check showed the apparent throughput regression was not stable; do not infer a small gain from three runs.
- P3 retained as opt-in: high-bandwidth throughput and CPU can improve, but the observed tail-latency cost makes window four unsuitable as a universal default. Use `RPC_TC_READ_WINDOW=4` with `bench.sh` to exercise the opt-in final configuration.
- P4 not retained: the small/variable benefit did not justify extra buffering and batching complexity.
- P5 retained for typed payload dispatch, including conversion of read buffers on the blocking I/O path rather than the async response task. Opens, commits, batches and fenced replication still use shared lifecycle adapters; their safety semantics were preserved. Read measurements do not quantify write-performance changes.

## Validation

- Ten focused block-client tests, including ownership, sliding-window refill, stale-prefetch exclusion on seek, early completion, acknowledgement identity, write backpressure and errors before commit.
- Real read integration at windows 1/4/8/32, backward seeks and partial final chunks; real write/flush/seek/commit integration at write windows 8/16/32.
- Nine worker regressions; strict all-target Clippy; API dependency boundaries; formatting and whitespace checks.
- Benchmark-accounting and checkpoint-comparison Python tests; namespace cleanup tested with a process ignoring SIGTERM.

## Reproduction

Build each checkpoint with the compiler version recorded in `results.json` and the same flags:

```sh
export RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0
export CC=gcc CXX=g++
export CXXFLAGS='-include cstdint -g1' # GCC 15 RocksDB header workaround
export RUSTFLAGS='-Cdebuginfo=line-tables-only -Clink-arg=-fuse-ld=mold'
export CARGO_PROFILE_RELEASE_DEBUG=line-tables-only CARGO_PROFILE_RELEASE_STRIP=none
export RPC_TC_SOURCE_SHA256=$(sha256sum crates/client/curvine-client-core/examples/rpc_tc_read.rs | cut -d " " -f1)
cargo build --locked --release -p curvine-server --bin curvine-server \
  -p curvine-client-core --example rpc_tc_read
```

Retain both server and workload binaries for each source revision listed in `results.json`. Use `bench.sh grpc SERVER CLIENT CHECKPOINT RESULT_DIR` with the profile/workload settings above. Compare matched repetitions with:

```sh
scripts/perf/rpc-tc/compare.py --baseline before-1/result.json before-2/result.json before-3/result.json \
  --candidate after-1/result.json after-2/result.json after-3/result.json
```

The retained `tc` queue limit is 1000 packets. Queue-drop counts for the whole run are recorded per trial in `results.json`; some pipelined trials overflow this queue. These counts include setup/warmup and are not time-aligned to individual timed reads, so they do not establish which P99 samples were caused by drops.

The initial two-run fixed-burst experiment was stopped after observing a tail-latency regression and replaced by the sliding window before the isolated comparison. Earlier exploratory runs lacked forced cleanup and are not used in these tables. The complete local logs, binaries and raw JSON samples remain under `/home/ubuntu/curvine-grpc-priorities`.
