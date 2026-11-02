# Curvine ORPC vs gRPC remote-read benchmark

This harness builds the same Curvine client workload against a specified ORPC
baseline commit and the current working tree, then runs a real master, worker,
and client in separate Linux network namespaces. A veth pair and `tc netem`
apply the same one-way delay and rate in each direction. Runs create isolated
metadata and worker data under their result directory; the generated config
formats only those newly created directories.

It requires Linux, root or equivalent network administration privileges,
`ip`, `tc`, Python 3, Rust build dependencies, `getconf`, and substantial disk
space for two release build trees. The server and client binaries run as root;
use only trusted binaries and source. The baseline must expose the same
Curvine client API used by `crates/client/curvine-client-core/examples/rpc_tc_read.rs`.

```sh
# Choose the commit before the transport change, not the candidate commit.
BASE_REV=<orpc-baseline-commit>
OUT=/absolute/path/to/new-benchmark-build-directory
scripts/perf/rpc-tc/build.sh "$BASE_REV" "$OUT"

# Use a fresh result directory for every run. Pass the same settings to both.
sudo env RATE=1gbit DELAY=1ms RPC_TC_CONCURRENCY=4 \
  scripts/perf/rpc-tc/bench.sh orpc \
  "$OUT/baseline-target/release/curvine-server" \
  "$OUT/baseline-target/release/examples/rpc_tc_read" \
  "$BASE_REV" /absolute/path/to/new-orpc-run
sudo env RATE=1gbit DELAY=1ms RPC_TC_CONCURRENCY=4 \
  scripts/perf/rpc-tc/bench.sh grpc \
  "$OUT/candidate-target/release/curvine-server" \
  "$OUT/candidate-target/release/examples/rpc_tc_read" \
  "$(git rev-parse HEAD)" /absolute/path/to/new-grpc-run

# Run each transport the same number of times, preferably interleaved.
scripts/perf/rpc-tc/compare.py --orpc /absolute/path/to/new-orpc-run/result.json \
  --grpc /absolute/path/to/new-grpc-run/result.json
```

`build.sh` makes a detached baseline worktree under `OUT`, copies the exact
workload source into it, builds both revisions independently, and records the
baseline commit, candidate HEAD, candidate status, tracked patch, source digest,
and binary digests. A dirty candidate's untracked files are not copied into the
patch: commit or separately retain the working tree to reproduce it. The
revision passed to `bench.sh` is a user-supplied label; retain the build metadata
and check that the labels identify the binaries used. When finished, remove the
baseline worktree using `git worktree remove` and dispose of the output directory
only when its data is no longer needed.

Defaults are 64 MiB of file data, 1 MiB per measured read, 256 reads and 16
warmup reads per reader, and one reader. Override with `RPC_TC_FILE_BYTES`,
`RPC_TC_CHUNK_BYTES`, `RPC_TC_READS`, `RPC_TC_WARMUP_READS`, and
`RPC_TC_CONCURRENCY`. File size must be a positive multiple of chunk size.
Each reader validates warmup data, then repeatedly reads the same file; the
sampled read latency excludes seek-to-start, while aggregate elapsed time and
CPU include it. Short-circuit reads and client read-ahead are disabled. Data is
written and warmed before timing, so runs primarily measure a warm cache; keep
machine load and storage/cache conditions comparable.

The client records each completed read latency, total read bytes, wall time, and
process CPU from `/proc/self/stat`. The runner snapshots CPU for every process
in the server namespace immediately before and after timed reads, including the
master, worker, and their supervisor. It rejects a changed server PID set or a
restarted process; short-lived server subprocesses between snapshots are not
observable. CPU uses Linux clock-tick resolution: increase duration if the
reported total is zero or too coarse. Each result records workload settings,
rate/delay, a build-time workload digest, binary digests, and a revision label.
The comparator requires matching settings and at least 100 latency samples per
run, computes nearest-rank per-run P99 and median per-run throughput/P99/CPU per
GiB, and evaluates the 85% throughput, 110% P99, and 115% CPU thresholds. Exit
status 0 means the **provisional** thresholds pass, 1 means they fail, and 2
means the inputs are invalid or unmatched.

These results do not establish the hardware release gate: a veth pair and
software shaping cannot reproduce representative 25/100 GbE NICs, production
CPU costs, or all sendfile and cache behavior. Repeat matched measurements on
representative hardware before drawing release conclusions.

For custom commands, `run.sh 'SERVER_COMMAND' 'CLIENT_COMMAND'` still provides
only network namespaces and shaping. Set `RPC_TC_READY_PORT` to wait for a
server TCP port. For coordinated result collection, set `RPC_TC_RESULT_DIR`,
`RPC_TC_TRANSPORT`, and `RPC_TC_REVISION`; the client must use the ready/go and
`client.json` protocol implemented by the supplied workload. `bench.sh` sets
these and the binary hashes automatically.
