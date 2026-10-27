# Process-Scoped Runtime Config View (P1)

- Status: implemented — see §7 for the commit map and deviations
- Date: 2026-10-26 (supersedes the 2026-10-16 draft)
- Scope: P1 read-only view. master / worker / transfer each expose their own
  effective full configuration plus a per-item source annotation; the admin
  console reads the active master's view by default. This is **not** a cluster-wide
  configuration center.

## 1. Problem

The console config page cannot show the cluster's real configuration: it renders
the **curvine-web process' own startup snapshot**, and runtime overrides applied
through the admin path are never visible there.

Two configuration tracks currently exist and do not intersect:

| Track | Where it lives | What it shows |
| --- | --- | --- |
| Startup snapshot | `ClusterConf::from` (`cluster_conf.rs`) | full config tree + `sources` limited to `default/file/env/cli`; frozen at boot |
| Runtime registry | `runtime_registry.rs` | only **registered** keys, with `file_value / override_value / effective_value`; volatile (`persistent=false`) |

`GET /api/v1/config` (console, port 9000) serves the first;
`GET /api/v1/{master,worker,transfer}/runtime/config` — served on each process's own
`*.web_port` (`master.web_port` defaults to 9010) — serves the second. The key sets
are disjoint, so neither endpoint answers "what is this process actually running
with, and where did each value come from".

## 2. Root causes (verified on `origin/main-next` afae0817)

### 2.1 Paired `_str` duration fields hide the real configuration path

Duration and byte-size knobs are stored as a pair:

```rust
#[serde(skip)]               pub block_size: i64,
#[serde(alias = "block_size")] pub block_size_str: String,
```

The serialized tree therefore contains `client.block_size_str`, while the source
map records `client.block_size`. Any source annotation for an operator-written
`block_size = "64MB"` resolves against a path that does not exist in the tree and
silently falls back to `default`.

Measured on the current tree: **25** `*_str` fields, **22** of them reach the
serialized tree, **20** carry an `alias`, **0** carry a `rename`.

Five of the 25 have no alias at all, and three of those five are real bugs (the
base name documented in configuration files is silently dropped):

| Knob | Symptom |
| --- | --- |
| `transfer.cluster_snapshot_max_staleness` | `#[serde(skip, default = ...)]` on the `_str` twin makes both the base name and the `_str` name unconfigurable |
| `transfer.task_retention` | base name has no alias: writing `task_retention = "99h"` logs `unrecognized config key` and keeps 86400s |
| `transfer.worker_terminal_retention` | same |

(`transfer.task_stale_timeout` and `transfer.task_report_interval` are derived
inside `TransferConf::init()` and are intentionally not configurable.)

### 2.2 Registry keys are not configuration paths

`master.dir_space_enabled` / `master.dir_space_tracked_paths` are registry keys,
but the configuration path is `master.dir_space.enabled` /
`master.dir_space.tracked_paths` (`DirSpaceConf` is nested under
`MasterConf.dir_space`). Pushing such a key through `pipeline::set_dotted` creates
a **sibling key** that serde ignores: the override is a silent no-op and the view
cannot resolve it. Those two key names are documented as an operator interface in
`etc/curvine-cluster.toml`, so renaming them is a breaking change.

### 2.3 Overlay values are JSON, configuration values are TOML

Runtime values arrive as JSON strings (`"true"`, `"64"`, `"[\"/a\"]"`,
`"1800000ms"`). Feeding a JSON string straight into a TOML array field fails hard
at deserialization time (`invalid type: string, expected a sequence`). A
translation layer is mandatory.

### 2.4 Post-deserialization mutations leave no trace

`ClusterConf::from` mutates the value after deserialization:

- `normalize_split_web_ports` rewrites `master.web_port` 9000 → 9010
- `apply_hostname_overrides` rewrites five hostnames from `net_interface` / env
- `resolve_master_addrs` derives `client.master_addrs`

None of these appear in the source map, so a naive "sources from the pipeline"
implementation reports `file` for a value the process is not using.

### 2.5 Duration fields are zero until `validate()` runs

`ClusterConf` duration fields are `#[serde(skip)]` and are populated by each
section's `init()`, invoked from `ConfValidate::validate()`. Serializing a
deserialized-but-not-validated `ClusterConf` yields `0ns` for every duration. Any
view builder must run validation for the matching profile first.

### 2.6 Credentials are serialized verbatim

`S3GatewayConf.access_key` / `secret_key` have no serde gate. The current
`GET /api/v1/config` payload already contains them in plaintext. Widening the view
to master / worker / transfer would spread the leak to three ports that carry no
authentication at all (`metrics_router` installs only `Extension`), i.e. it would
lower the authentication boundary from 9000 (session-gated) to 9010 (bare).

Note: these two fields currently have **no consumer anywhere in the repository** —
the S3 gateway reads `credentials_path`, environment variables and its own
credential store. They are write-only configuration today.

## 3. Decisions

1. **Registry keys are stable; declare the configuration path separately.** A
   registry entry carries an optional `config_path`. Defaults to the key itself.
   `None` means "no configuration-file entry at all" and the entry is writable but
   not shown in the view. This keeps `master.dir_space_enabled` working while
   giving the view a resolvable path.
2. **Knobs with no configuration-file entry are not displayed.** Example:
   worker `transfer_inflight_budget_bytes` is a constant
   (`TransferConf::DEFAULT_WORKER_INFLIGHT_BUDGET_BYTES`) and also seeds
   `SEGMENT_GROUP_TARGET_BYTES`, so exposing it would change segment sizing.
3. **Web fails loudly when the master view is unavailable.** `502` with
   `MASTER_CONFIG_UNAVAILABLE`; no fallback to the local snapshot.
4. **`persistent: false` semantics are unchanged.** Overrides stay volatile.
5. **A sixth source label `derived` marks post-deserialization mutations.** Do not
   misreport them as `file`.
6. **Do not rename shipped runtime keys.** Operator scripts and
   `etc/curvine-cluster.toml` already reference them.
7. **Credentials never leave the process.** `access_key` / `secret_key` gain
   `#[serde(skip_serializing)]`.
8. **A live value must be the value the component holds, not a value that merely
   looks right.** The view pins each runtime entry to the effective value the
   registry reports; rebuilding the configuration from the overlay and reading the
   field back is a separate, fallible computation whose result can differ from what
   is actually throttling traffic (commit `87089df4`).
9. **A hot-tunable limit must reach connections that already exist.** Neither limit
   may be snapshotted per connection or per accept. `conn_limit` is an atomic read on
   every receive iteration and `global_limit` moves capacity in place, so an edit
   takes effect on the next request of an established connection and never splits
   into an old pool and a new pool (commits `aa7ec644`, `28bf503d`).

## 4. Contract

```
GET  /api/v1/{role}/config          role ∈ {master, worker, transfer}
GET  /api/v1/master/runtime/config  frozen alias of the master view, one release
POST /api/update_config?<key>=<value>
```

`/api/v1/master/runtime` is **not** part of this contract: it is the HA probe
consumed by `curvine-web` (`addr.rs::master_runtime_scrape_url`) and its shape is
frozen.

```jsonc
{
  "role": "master",
  "node": "host-01:9010",
  "values": { "master": { "hostname": "host-01", "dir_space": { "enabled": true } } },
  "sources": {
    "master.hostname": "file",
    "master.web_port": "derived",
    "master.dir_space.enabled": "runtime"
  },
  "runtime": [
    {
      "key": "master.dir_space_enabled",
      "path": "master.dir_space.enabled",
      "file_value": false,
      "override_value": true,
      "effective_value": true
    }
  ],
  "writable": ["master.dir_space_enabled", "master.dir_space_tracked_paths"],
  "unresolved": [],
  "persistent": false
}
```

Field semantics, stated explicitly because the distinction between a registry key
and a configuration path is the source of the bug in §2.2:

| Field | Meaning |
| --- | --- |
| `sources` key | dotted **configuration path**, identical to a path inside `values` |
| `sources` value | `default` \| `file` \| `env` \| `cli` \| `derived` \| `runtime` |
| `runtime[].key` | registry key, i.e. the argument accepted by `POST /api/update_config` |
| `runtime[].path` | configuration path for that key (`config_path`, defaults to the key) |
| `writable` | list of registry **keys**, not paths |
| `unresolved` | registered keys whose `path` does not exist in the tree; see below |
| `persistent` | always `false` |

A key whose `path` cannot be resolved inside the deserialized tree is excluded from
`values`, `sources`, `writable` and `runtime`, and listed in `unresolved` so a
misnamed key is visible to an operator instead of silently swallowing their writes.
Serde ignores unknown fields, so this check cannot be delegated to deserialization;
it reuses the probe-based `validation::audit_unknown_keys_in`.

## 5. Out of scope for P1

- RPC / CLI full-view alignment
- worker / standby node selector in the console
- editing configuration from the console (P2)
- persisting overrides (P3)
- MDS process
- fuse / client CLI source tracking
- per-role section pruning: `ClusterConf` is one struct, so a transfer view also
  carries `master` / `journal` / `mds` / `fuse` / `web`. Pruning risks hiding a
  configuration item that is actually read; measured consumer matrix for a later
  pass: master reads `master/journal/job/transfer`, worker reads
  `worker/job/master`, transfer reads `transfer/job/client`.
- token authentication for the 9010 endpoints (documented as internal-only)

## 6. Reuse

| Existing piece | Use |
| --- | --- |
| `pipeline::build_document_with_sources` | already returns `(toml::Value, ConfigSourceMap)`; wrap it |
| `pipeline::set_dotted` | only writer for dotted paths; there is no reader |
| `validation::audit_unknown_keys_in` | probe-based unknown-key detection; reuse for phantom-key filtering instead of writing a new validator |
| `validation::ConfValidate` | must be invoked before serializing, see §2.5 |
| `reload.rs` | already defines the same dotted-path overlay model (`RuntimeOverlay` + `ConfigHandle`); P1 deliberately keeps the per-knob closure registry, this module stays unwired |
| `curvine-web` shared handler module | master already does `use curvine_web::router::{get_runtime_config, ...}` |

## 7. What was actually built

Behaviour commits on `feature/runtime-config-view`, branched from `origin/main-next`
(`afae0817`), in the order they landed. The plan had ten tasks; Task 8
(documentation) and Task 9 (CI guard) share one commit because both are non-code
changes of the same review unit. Three follow-on requests arrived after the plan was
executed and added commits 9–17: land the documents of record in the repository, make
the console able to *write* rather than only read, and turn the RPC concurrency limits
into genuinely live values instead of a per-connection snapshot.

Dependency order:

| # | Commit | Subject |
| --- | --- | --- |
| 0 | `d2f079f9` | `fix(config): stop serializing s3 gateway credentials` |
| 1 | `8e36ee9c` | `refactor(config): return a source map from the cluster conf loader` |
| 2 | `a6f1c72d` | `fix(config): serialize duration twins under their documented names` |
| 3 | `6a53aa0e` | `feat(config): let a runtime key declare its configuration path` |
| 4 | `900e0722` | `feat(config): build a process-scoped effective config view` |
| 5 | `7c031267` | `docs(web): document the process config endpoints and gate the config crate` |
| 6 | `07b86f1d` | `feat(config): serve the same config view from every process` |
| 7 | `37ff943b` | `feat(web): proxy the console config page to the active master` |
| 8 | `49857a99` | `feat(web): show where each value came from in the config page` |
| 9 | `3baee156` | `docs(config): commit the design and plan of record` |
| 10 | `f5f7013f` | `fix(web): report an unreachable master as 502 on the config endpoint` |
| 11 | `7c060f0c` | `docs(config): record the single-node run and the 502 defect it found` |
| 12 | `f2c43ceb` | `feat(web): accept runtime config writes on the console endpoint` |
| 13 | `1a11903f` | `feat(webui): edit hot-tunable settings inline in the config table` |
| 14 | `87089df4` | `fix(config): report the live value instead of rebuilding it` |
| 15 | `aa7ec644` | `feat(rpc): let the concurrency limits be resized at runtime` |
| 16 | `c762035b` | `feat(transfer): read the dispatch ceiling from the live runtime value` |
| 17 | `28bf503d` | `feat(master): expose the RPC concurrency limits as runtime keys` |

Commit 14 is a defect that only a live cluster exposed: the view reported the
*rebuilt* value (the file layer with the override re-applied) instead of the value
the running component holds, so an override could read back as something the process
was not actually using. Each runtime entry is now pinned to its `effective_value`.

Commits 15–17 close the "per-connection snapshot" hole. `conn_limit` was a `usize`
copied into every accepted connection, and `global_limit` was a `Semaphore` captured
at startup, so the pilot's approach of building a second `LimitConf` produced two
pools. `LimitConf` now shares one `GlobalLimit` and one `AtomicUsize`: capacity moves
in place (`capacity`/`target` atomics, `Mutex` only for the resize itself), so growth
reaches connections that are already established, while a shrink withdraws idle
permits at once and a target below the in-flight count converges as permits are
released. `LimitPermit::drop` drives that convergence; the fast path is a single
atomic check, so the common case never takes the mutex.

Commits 18–20 (this document's revision, `DEPLOY.md`, and the plan) are docs-only and
change no behaviour; they are the same review unit as row 9 and are not given separate
rows.

Deviations from the plan, all deliberate:

1. **The plan's Task 6 named the new route `/api/v1/master/config` and dropped
   `/api/v1/master/runtime/config`.** Both are mounted; the old path is kept as an
   alias for one release because dashboards and curl recipes reference it. A test
   (`proxy_targets_the_view_endpoint_not_the_ha_probe_route`) pins that the proxy
   uses the new path and never `/api/v1/master/runtime`, whose shape is frozen.
2. **`curvine-web` lost its startup snapshot entirely.** The plan said to keep
   `config_sources` and just change the read; T6 actually removed the field, the
   `with_sources` constructor and the CLI-source inserts in `curvine-web.rs`,
   because after the proxy there is no reader left. `from_with_sources`' only
   production caller is now `curvine-server`.
3. **`ConfigSource` gained two variants, not one.** `Derived` was added in Task 1
   (§2.4 — the loader mutates values after deserialization) and `Runtime` in Task
   4. Had only `Runtime` been added, every migrated `master.web_port` would still
   have been reported as `file`.
4. **Task 3 `config_path` needed a tri-state.** `config_path` (defaults to the key)
   cannot distinguish "not registered" from "registered with no configuration
   entry", which is exactly the worker `transfer_inflight_budget_bytes` case.
   `declared_config_path` returns `Option<Option<&str>>` and the view builder uses
   that one.
5. **The console's source whitelist was the bug, not just a missing label.** The
   plan listed two edits for Task 7; five files changed, and `configSourceFor` was
   rewritten around a single `KNOWN_SOURCES` list so an unrecognised source cannot
   be silently relabelled `default`.
6. **`unresolved` is a first-class response field** (plan §4 only promised a
   `warn!`). A key filtered out of every other field with only a server-side log
   line is invisible from the console, which is the same class of silent failure
   this work exists to remove.

Two claims in the plan were verified wrong while implementing, and the code follows
the evidence rather than the plan:

- Task 2's "20 places" missed three real bugs: `transfer.cluster_snapshot_max_staleness`,
  `transfer.task_retention` and `transfer.worker_terminal_retention` were
  unconfigurable under their documented names. Twenty aliases gained a `rename`;
  those three needed their serde attributes fixed instead, for 23 touched fields in
  the tree this was written against. A fourth instance landed upstream while the
  branch was in flight — `4fc6e8ff` added `client.seek_absorb_window` as a bare
  `alias` twin — so the count is now 24, and the regression test caught the new one
  rather than letting it back in.
- Task 2's safety argument for the CLI ("the macro ignores `rename`") is wrong.
  `curvine-macros` reads `alias` from the same `#[serde(...)]` attribute. The long
  names survive **because `rename` and `alias` share one attribute**; splitting
  them across two attributes silently renames `--client.block-size` to
  `--client.block-size-str`. A regression test in `client_conf_cli_test.rs` pins
  the long name.

## 8. Verification actually performed

Everything below was run on this host. The earlier version of this section claimed the
console and a live cluster were unverified; both gaps were closed afterwards.

**Rust**

| Gate | Result |
| --- | --- |
| `cargo test -p curvine-config -p curvine-rpc -p curvine-common-macros` | all targets ok, `EXIT=0` |
| `cargo test -p curvine-web` | 224 passed, `EXIT=0` |
| `cargo test -p curvine-worker --lib` | 70 passed, 1 ignored, `EXIT=0` |
| `cargo test -p curvine-data-transfer --lib --features transfer-web` | 150 passed, `EXIT=0` |
| `cargo test -p curvine-master --lib` | 348 passed, **2 failed** — pre-existing, see below |
| `cargo clippy` on all six touched crates, `--all-targets -- -D warnings` | `EXIT=0` (`curvine-config`, `curvine-rpc`, `curvine-common-macros`, `curvine-web`, `curvine-worker`, `curvine-master`, and `curvine-data-transfer --features transfer-web`) |
| `cargo fmt --all -- --check` | clean |

The two `curvine-master` failures are `master::fs::master_filesystem::tests::
set_attr_cannot_override_internal_ttl_type_marker` and `ttl_skips_in_use_files_and_
complete_reopens_the_window`. They are **not** caused by this work:

- Neither `curvine-master/src/master/fs/master_filesystem.rs` nor
  `curvine-master/src/meta/fs_dir.rs` is touched by any commit here; both are
  byte-identical to `afae0817` (`git diff afae0817 HEAD -- <paths>` is empty).
- Re-running the same suite in the separate worktree that sits on pristine
  `afae0817` (`/data/CodeSpace/curvine-next`) reproduces exactly these two and
  nothing else: `57 passed; 2 failed; 291 filtered out`.

The reservation check landed in `b60f8f61` (trash) after the test was written in
`ceedfd79` (atime TTL); upstream owns the fix.

**Console**

The Node toolchain was found under `~/.nvm/versions/node/v22.21.0` rather than on
`PATH`, so the frontend is not CI-only after all:

| Gate | Result |
| --- | --- |
| `npm run type-check` | `tsc --noEmit`, `EXIT=0` |
| `npm run test` | 29 files, **316 tests passed** |
| config-page cases | `flattens the readonly config table…`, `filters config entries by source`, `filters config entries by the runtime source` — 3 passed |

**Live three-process cluster** (`testing/conf/curvine-single.toml`, one host)

| Probe | Observed |
| --- | --- |
| `GET :9010/api/v1/master/config` | `role=master`, `node=127.0.0.1:9010`, `writable=[master.conn_limit, master.dir_space_enabled, master.dir_space_tracked_paths, master.global_limit]` |
| `GET :9010/api/v1/master/runtime/config` | byte-identical body (alias confirmed by md5) |
| `GET :9010/api/v1/master/runtime` | 200, HA probe shape intact |
| `POST :9010/api/update_config?master.conn_limit=17` | `previous_value=8`, `value=17`; re-read `effective=17 override=17 file=8`, `sources= runtime` |
| `POST :9010/api/update_config?master.global_limit=512` | `previous_value=4096`; `healthz` still 200 afterwards |
| `POST` with empty value | 422 `master.conn_limit expects an integer: ` |
| `POST {key}` with no value on a process port | 422 `master.conn_limit expects an integer` — `post_update_config` always sets a value, so the process ports have **no** reset route |
| `GET :9002/api/v1/worker/config` | 4 writable, 3 in `runtime[]` with `path` under `job.*`, `transfer_inflight_budget_bytes` writable but absent (no file entry), `unresolved=[]` |
| `GET :9004/api/v1/transfer/config` | `writable=[worker_max_concurrent_tasks]`, live value 100 |
| `POST :9004/api/update_config?worker_max_concurrent_tasks=7` | `previous_value=100`; view then shows `effective=7 override=7 file=100`, source `runtime`; `readyz` 200 |
| `GET :9000/api/v1/config` without a cookie | 401 `UNAUTHORIZED` — the proxy inherits session auth |
| `POST :9000/api/v1/auth/login` then `GET :9000/api/v1/config` | `role=master`, `values.master.conn_limit=17`, `sources[master.conn_limit]=runtime`, `sources[master.hostname]=file` |
| `POST :9000/api/v1/config {key,value}` | `previous_value=17`, `value=31`; master direct **and** console both re-read 31 with source `runtime` |
| `POST :9000/api/update_config` | 405 — the console writes only through `POST /api/v1/config`, which is a JSON body; the `key=value` query form exists on the process ports |

S3 credentials were absent from every payload, confirming `skip_serializing` at the
only place that now serializes the whole tree.

Still not run here: a full workspace `cargo build` (it compiles `librocksdb-sys`,
>20 min). Every gate used `-p <crate>`.
