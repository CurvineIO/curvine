# Metadata A/B benchmark

One harness that compiles unchanged on the global-lock baseline `bff4e7dc5` and
on the sharded-lock candidate branch, times every metadata operation
independently, and prints machine-parsable results for a strict per-cell
comparison.

## Why this exists

`curvine-master/tests/metadata_edge_bench.rs` cannot carry the acceptance
comparison: it does not exist on the baseline, it times combined
create+delete / create+rename+delete loops, it samples every 16th operation and
keeps only the last 4096 samples per thread, and it covers about six
operations with no 32-thread or cross-parent cases. This harness fixes those
gaps. It does not replace the acceptance decision, which is made by the
architect from alternating A/B runs.

## Three builds for the real comparison

WAL is a separate switch from the journal: `RocksInodeStore::new` hardcodes
`set_disable_wal(true)` on the baseline and `false` on the candidate. Changing
a config file does nothing. Both sides must run with WAL on, so the comparison
needs three builds:

| Build | How | Purpose |
|---|---|---|
| Baseline native | `git checkout bff4e7dc5` | Reference only: the old WAL-off number |
| Baseline WAL-on | baseline + `git apply WAL-ON-CONTROL.patch` | Main comparison (same WAL state as the candidate) |
| Candidate | current branch | The thing being accepted |

`WAL-ON-CONTROL.patch` changes exactly one line. Do not turn off the
candidate's WAL or journal confirmation to chase a score.

Journal stays disabled on both sides (`fs_only_for_test`), so these numbers
measure the metadata layer: path resolution, locks, planning and the store
write path. They are not Raft end-to-end numbers. An end-to-end run with the
journal enabled is feasible later (same harness, real `JournalSystem`), but it
is deliberately out of scope here: it would mix Raft commit latency into the
lock comparison.

## File layout

| File | Responsibility |
|---|---|
| `main.rs` | Entry test, per-cell isolation, output |
| `harness.rs` | Layout/env, main matrix runner, latency statistics |
| `background.rs` | Hot-parent background scenario (separate thread topology) |
| `ops.rs` | Case table: one `MasterFilesystem` call per case |
| `fixtures.rs` | Fixture preparation and per-call argument builders |
| `hot.rs` | Hot-parent scenario fixtures |

Each file stays well under 500 lines and has one job.

## Isolation between cells

Every cell gets its own `MasterFilesystem` on its own meta directory, which is
deleted afterwards; the worker is re-registered per cell. Sharing one store
across cells would let earlier cells accumulate delete tombstones and
compaction debt, and the two builds write-amplify differently, so that debt
would bias them asymmetrically. Store setup and teardown are outside the timed
region, so they do not enter any number.

## What each layout measures

* `same` — all threads work in one parent directory. Writes contend on the same
  parent, reads share one ancestor chain.
* `multi` — one parent per thread. No parent contention; this is the parallel
  ceiling.

For the list cases both layouts list directories of exactly `LIST_SIZE` (256)
entries: `same` has one such directory shared by all threads, `multi` has one
per thread. Directory size is deliberately held constant so a `same` vs `multi`
difference comes from contention, not from a differently sized listing.

## Running

Release is mandatory; debug numbers are meaningless.

```bash
# Main matrix + background scenario
BENCH_VER=baseline-wal-on cargo test --release -p curvine-master \
    --test metadata_ab_bench -- --ignored --nocapture

# Smoke only: shrinks the workload, never use for a reported comparison
BENCH_OPS=20 BENCH_VER=smoke cargo test -p curvine-master \
    --test metadata_ab_bench -- --ignored --nocapture
```

`BENCH_OPS` overrides operations per thread (default 1000). A real A/B run must
not set it: both builds have to do the identical default workload.

`BENCH_VER` is echoed into every line so results from different builds cannot
be mixed up.

## Output

One line per cell:

```
RESULT ver=<build> op=<operation> layout=<same|multi> threads=<n> ops=<n> ok=<n> err=<n> \
       qps=<n> samples=<n> p50_ns=<n> p99_ns=<n> max_ns=<n> [first_err="..."]
```

Background scenario lines carry `bg=hot_parent`, plus `target_rate_per_thread`,
`rate_ratio` (achieved/target) and `ok_rate` on the background line. A
background that fell short of its target rate is visible next to the
foreground numbers, so a foreground tail-latency win cannot be bought by
silently doing less background work.

## Coverage matrix

Every operation runs at 1 / 8 / 32 threads in both layouts (`same` = all
threads share one parent directory, `multi` = one parent per thread):
19 operations x 2 layouts x 3 thread counts = 114 cells, plus 3 thread counts x
3 lines for the background scenario = 123 lines.

| Operation | Measured call | Fixture (untimed) |
|---|---|---|
| create | `create` | parent dirs only |
| mkdir | `mkdir` | parent dirs only |
| delete | `delete` | one empty file per call |
| rename_same | `rename` within one parent | one file per call |
| rename_cross | `rename` into a sibling parent | one file + destination parent per thread |
| set_attr | `set_attr` mode `0o640` | one file per call |
| link | `link` | one source file per call |
| symlink | `symlink` | parent dirs only |
| overwrite | `create` on an existing path | one existing file per call |
| open | `open_file` with `O_WRONLY` (no `O_TRUNC`/`O_CREAT`) | one file per call |
| add_block | `add_block` | one empty file per call |
| complete_file | `complete_file(only_flush = false)` | one empty file per call |
| free | `free` | one empty file per call |
| resize | `resize(with_truncate(0))` | one empty file per call |
| file_status | `file_status` | one read target per thread |
| exists | `exists` | one read target per thread |
| list_status | `list_status` | 256 entries per listed directory |
| list_page | `list_options(limit = 64, start_after = None)` | 256 entries per listed directory |
| get_block_locations | `get_block_locations` | one file with one allocated block per thread |

Background scenario (`bg=hot_parent`): background threads run create+delete on
the hot parent `/bench/hot/parent`, throttled to
`HOT_PARENT_TARGET_OPS_PER_SEC` (20 000 per thread) outside the timed region.
Foreground threads concurrently call `file_status` and `create` on a shared
deep child path (`/bench/hot/parent/deep/d0/../d3`), so foreground ancestor
read locks contend with the background parent write lock.

## Not comparable, and why

* **close** — the baseline has no `MasterFilesystem::close_file`. The real
  baseline close path is the `CompleteFile` RPC, which calls
  `complete_file(only_flush = false)`; that is exactly the `complete_file`
  case. The candidate's separate `close_file` entry point is deliberately not
  in the matrix because the baseline has no counterpart to compare it against.
  Measuring it would require inventing a baseline path.
* **open** — read-only `open_file` delegates straight to
  `get_block_locations`, so timing it would measure an operation already
  covered. The case uses `O_WRONLY` without `O_TRUNC`/`O_CREAT`, which takes
  `FsDir::reopen_file`, the real client open-for-write path.
* **overwrite** — `create` already sets `O_TRUNC`, so an existing path takes
  `FsDir::overwrite_file` rather than the create branch. Same public API, two
  real branches; the case distinguishes them by fixture, not by API.
* **complete_file** — the case closes a freshly created empty file with empty
  `commit_blocks` and `len = 0`. `complete_len == len == 0`, so this is the
  ordinary close path, not a retry shortcut.
* **resize** — `with_truncate(0)` on an empty file is idempotent, so the case
  does not depend on prior state. `resize` internally re-reads block locations
  after resizing; that read is part of the real client-visible operation.
* **add_block / get_block_locations** — require a registered worker; the
  harness registers one `Live` worker with `capacity = available = i64::MAX/4`.
  Without it `choose_worker_for_file` fails.
* **persisted children count** — the format experiment in
  `metadata_edge_bench.rs` is intentionally not carried over. It changes a
  process-wide switch and measures a non-default format, so it cannot serve as
  a normal read-path acceptance number.

Nothing else is blocked: every operation above can be constructed with public
API that exists on both builds, so this file needs no `cfg` split.

## Statistics

* **Fixed operation count** (default 1000 per thread), not fixed duration, so
  both builds do exactly the same work and the sample count is known in
  advance.
* **Every operation is timed**, no sampling and no per-thread truncation. One
  `Instant::now()` + `elapsed()` pair costs roughly 25-50 ns on x86-64, which
  is below 1% of the measured calls (all in the microsecond range).
* **The timed region holds one `MasterFilesystem` call and nothing else.**
  Every per-call argument, including path strings and the `ClientAddress`
  `add_block` takes by value, is built before the barrier. Path formatting is a
  heap allocation; leaving it inside the timed region would add allocator noise
  to every sample and compress the difference between the two builds. The only
  allocations that can still occur inside the timed region are the ones the
  API itself performs (`CreateFileOpts::with_create` and
  `SetAttrOptsBuilder::new` use zero-capacity strings, so they allocate
  nothing).
* **Percentiles** are nearest-rank over the merged raw samples of all threads,
  sorted with `sort_unstable`. Merging raw samples means a high-throughput
  thread contributes proportionally more samples, so threads are not equally
  weighted by construction.
  With 1000 samples, `p99` is the 990th value; the percentile position is exact
  and no interpolation is applied.
* **Failures are counted, never folded into successes.** Failed calls are
  excluded from the latency distribution (mixing error paths into the success
  distribution would misreport the operation) and the first error text is
  printed, so a broken fixture cannot pass silently.
* **Background target rate is an offered rate.** The background throttle never
  pushes past `HOT_PARENT_TARGET_OPS_PER_SEC` per thread, but it cannot make a
  saturated store keep up, so `rate_ratio` below 1 means the store could not
  absorb the offered load - not that the load was reduced. Two builds are only
  comparable when their achieved background rates are close; if one build
  reports a much lower `rate_ratio`, the foreground numbers are taken under
  lighter pressure and must not be compared as if identical.
* **The background runs until the last foreground thread finishes.** Both sides
  doing a fixed count would let whichever finished first leave the other
  running without contention, and that tail would drag its p99 down -
  differently on the two builds. Background counters therefore cover only the
  foreground's lifetime, so its sample count is much larger than the
  foreground's.
* **Memory** per thread is `ops` `u64`s for the matrix cases: 8 KB at the
  default 1000. In the background scenario the background accumulates one
  sample per offered cycle, so its vectors are sized by the foreground's
  duration, not by `ops`.
* Single-round numbers carry system noise. The acceptance decision must come
  from alternating same-round A/B runs (at least two rounds) with identical
  workloads, not from one run per build.

## Verification record

Compile, same source file on both builds:

| Build | Command | Result |
|---|---|---|
| Baseline `bff4e7dc5` | `cargo check -p curvine-master --test metadata_ab_bench` | exit 0, 0 errors |
| Candidate `0b7eff494` | same, in a temporary detached worktree | exit 0, 0 errors |

Smoke runs (`BENCH_OPS=20`, debug build, correctness only, no performance
conclusion):

| Run | Log | Result |
|---|---|---|
| Baseline `bff4e7dc5` | `/tmp/bench-smoke6.log` | exit 0, 123 lines, all `err=0` |
| Candidate `0b7eff494` | `/tmp/bench-smoke6-candidate.log` | exit 0, 123 lines, all `err=0` |

Candidate compile log: `/tmp/bench-check-candidate.log` (exit 0).

Smoke logs are local to the machine that produced them and are not committed.
The exit codes above are the real `cargo test` status (`pipefail` enabled), not
a `grep`/`tail` status.

## Deliberate non-goals

* No large-scale benchmark run and no performance conclusion in this
  deliverable; the formal A/B is scheduled by the architect after all
  development and the full regression pass.
* No new dependency: latency is stored as `Vec<u64>` nanoseconds and sorted.
* No journal-enabled end-to-end measurement (see above).

## Measurement semantics (current)

* `BENCH_MILLIS` (default 1000) bounds each cell in time. `BENCH_OPS` is only an
  upper cap on iterations, not the iteration count it used to be, so results
  produced before this change are not comparable with results produced after it.
* A cell runs on its own thread under a watchdog: `CASE_TIMEOUT_SECS` (default
  120) bounds it. On expiry the cell prints `flag=TIMEOUT`, is invalid, and the
  stuck thread is abandoned (it cannot be cancelled). The remaining cells still
  run. A timeout is never a pass.
* `RESULT` carries `elapsed_ns` (concurrent span, the QPS denominator),
  `mean_ns`, `bench_millis`, `ops_cap`. A test asserts `elapsed_ns >= 1s` for a
  formal cell, `samples == ok` and `err == 0`.
* Arguments are built in chunks of 1024 outside the per-call span. Construction
  still counts inside the cell span, so `close_file_by_id` measures
  "resolve id + complete" end to end; its p50/p99/mean are unaffected.
* `THREADS = [1, 8, 16, 32]`: 1/8/16 are the formal cells, 32 is a reference
  oversubscription row and is excluded from the judgement.
* The hot-parent background rate is `HOT_PARENT_TARGET_OPS_PER_SEC = 500`. It was
  chosen from a baseline trial and frozen with the harness, because both sides
  must carry a load they can actually reach; at 20000 both sides ran at a
  fraction of the target and the rows could not be judged.
