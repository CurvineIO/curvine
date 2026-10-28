# Curvine Web Multi-Cluster Console Design

Related work: standalone `curvine-web` admin console, the observability platform design
(`curvine-web/docs/curvine-web-observability-platform-design.md`), and the React console in
`curvine-web/webui-next`.

## Summary

A `curvine-web` deployment used to serve exactly one Curvine cluster: the one described by its
`--conf` file. Every handler captured a single `UnifiedFileSystem`, a single `ObservabilityService`
and one set of in-memory job/resync maps at construction time, and the browser had exactly one API
base URL.

This document describes the multi-cluster retrofit: the console keeps its own cluster
(`cluster_id` from `--conf`) as the **default cluster**, and can additionally *import* more cluster
configurations at runtime. Administrators manage that set from a new **Clusters** page, and every
page of the console follows a **cluster switcher** in the topbar.

```text
                     +----------------------------------------------+
  browser            |            curvine-web (one process)         |
  -------            |                                              |
  X-Curvine-Cluster  |  resolve_cluster middleware                  |
  ---------------- > |    header absent -> default ClusterCtx       |      +---------------+
  (omitted when the  |    header = id    -> imported ClusterCtx     | ---> | Curvine       |
   default cluster   |    unknown id     -> 404 unknown_cluster     |      | cluster A     |
   is selected)      |                                              |      +---------------+
                     |  ClusterRegistry                             |      +---------------+
  /api/v1/clusters   |    default entry  (--conf, not removable)    | ---> | Curvine       |
  (console level)    |    file entries   (<clusters_dir>/*.toml)    |      | cluster B     |
                     |                                              |      +---------------+
                     +----------------------------------------------+
```

The retrofit is deliberately **narrow**: the console stays one process, one auth session store, one
HTTP port and one SPA. Only the pieces that are *about a cluster* became per-cluster; the pieces that
are about the console stayed console-scoped. This is why a single-cluster deployment behaves exactly
as before with no configuration change at all.

## Goals

- Import, validate, list, edit, probe and delete additional cluster configurations at runtime,
  without restarting the console and without hand-editing the console's own config file.
- Switch the whole console between clusters from the UI; every page follows the selection.
- Keep zero-config compatibility: a console started the old way still serves its cluster as default.
- Survive a restart: imported clusters come back from disk.
- Bound memory: an imported cluster is not free (see [Resource model](#resource-model)).
- Never route a request to the wrong cluster silently.

## Non-goals

- No cross-cluster queries or aggregated dashboards (one request, one cluster).
- No cross-cluster data movement (copy/migrate) — that belongs to the Transfer/CLI layer.
- No per-cluster authorization: one console session can reach every registered cluster.
- No remote config editing: the console does not write another cluster's `curvine-cluster.toml`, it
  stores its own copy of the import.
- No HA of the console itself; imported cluster definitions are local files, not replicated state.

## Current state

Single-cluster assumptions that had to be broken (backend):

| Location | Assumption |
| --- | --- |
| `curvine-web/src/server/admin_web_service.rs` | One `AdminRouterHandler` built from one `ClusterConf`; `UnifiedFileSystem` / `ObservabilityService` created once |
| `curvine-web/src/router/admin_handler.rs` | Handlers are closures over `Arc<UnifiedFileSystem>`; `resync_tasks` / `load_jobs` / `load_job_cache` are fields of the handler |
| `curvine-web/src/observability/*` | `router(service)` carried its own `Extension(service)`, one collector per process |
| `curvine-web/src/util/addr.rs` | Mostly already `&ClusterConf`-parameterised (only the display helpers needed a cluster label) |

Single-cluster assumptions in the SPA (`curvine-web/webui-next`):

| Location | Assumption |
| --- | --- |
| `src/api/http.ts` (`API_BASE`, `createDownloadClient`) | One API base URL per build |
| `src/auth/auth-context.tsx` | The API client is built once from the session |
| `vite.config.ts` | Dev proxy to one backend |

## Design

### Cluster identity

A cluster is identified by `ClusterConf::cluster_id` (already present in the config crate), so the
identity is the operator's own label, not an invented console-side UUID:

- the default cluster's id comes from the console `--conf`;
- an imported cluster's id comes from the imported TOML and must be **explicit**;
- ids match `^[a-z0-9_][a-z0-9_-]{0,62}$` (lowercase only, no leading `-`) and are the file name
  (`<id>.toml`) and the header value. Lowercase is not cosmetic: the id is written to the filesystem,
  so on a case-insensitive volume (macOS) `beta` and `BETA` would overwrite each other, and a leading
  `-` would read as a flag wherever the id is passed on a command line. The rule is enforced in
  `configuration_errors()` — the same gate used by `validate`, `create`/`PUT` and the directory
  loader — so the dry run can never say `ok` for text that the write path rejects;
- an import whose id equals the default cluster's id is rejected (`cluster_exists`).

### Registry

`curvine-web/src/cluster/registry.rs`:

```text
ClusterRegistry
  default_id : String                      # from --conf, always present, never removable
  entries    : RwLock<BTreeMap<String, Entry>>
  dir        : PathBuf                     # loader's clusters directory
  max        : usize                       # WebConf::max_clusters, default 8

Entry = Ready(Arc<ClusterCtx>) | Invalid(reason)

ClusterCtx { conf, unified_fs, observability, resync_tasks, load_jobs, load_job_cache }
```

- **Sources**: `default` (the process `--conf`) and `file` (one TOML per imported cluster). A
  summary reports which one a cluster came from, plus `editable` / `removable`.
- **Directory**: `--clusters-dir`, else `<dir of --conf>/clusters`, else `web.clusters_dir`; created
  on demand, and left alone if nothing was ever imported (zero side effects).
- **Persistence**: `PUT`/`POST` write `<id>.toml` with the operator's own text, via
  tmp file -> `fsync` -> `rename` at mode `0o600`.
- **Degradation**: a broken file is loaded as `Entry::Invalid` with its parse error, shows up in the
  list with `status: "invalid"`, and requests for it answer `404 unknown_cluster`. One bad import
  never blocks startup or the other clusters.
- **Limits**: `max_clusters` counts the default cluster (`entries.len() >= max` -> `cluster_limit`),
  checked again under the write lock so concurrent imports cannot overshoot.
- **Release**: removing or replacing a cluster builds the new context first, then calls
  `ObservabilityService::stop()` on the old one (the health loop holds an `Arc<Self>`, so without an
  explicit shutdown flag the collector, TSDB and socket tasks would never be released).

### Request routing

One middleware, `resolve_cluster` in `curvine-web/src/router/admin_handler.rs`, sits between auth and
the handlers:

```text
Router::layer(Extension<Arc<AdminRouterHandler>>)      # console-level state
      -> layer(require_auth)                            # /api/v1/auth and /metrics excluded
            -> layer(resolve_cluster)                   # /api/v1/clusters* and /api/v1/auth excluded
                  -> handlers read Extension<Arc<ClusterSnapshot>>
```

- `X-Curvine-Cluster: <id>` selects a cluster. Absent (or blank) means the default cluster, which is
  what keeps single-cluster deployments and a not-yet-switched browser working.
- The header is resolved to an `Arc<ClusterCtx>`; the middleware injects a per-request snapshot
  (cluster-scoped handler) plus that cluster's `Arc<ObservabilityService>` into request extensions.
  Handlers are therefore *not* closures over a single cluster anymore, and they stay free of
  per-cluster bookkeeping.
- Because auth runs **outside** this layer, an anonymous caller cannot use the error codes to
  enumerate cluster ids.
- The cluster list/import API is console-scoped (`/api/v1/clusters*` is skipped by the middleware), as
  are auth endpoints. `/metrics` (the console's own Prometheus endpoint) stays console-wide.
- `overview`, `workers`, `browse`, `blocks`, `config`, both job APIs, both resync APIs and the
  observability endpoints all resolve per request, so a switched browser sees a different cluster on
  every page, including the legacy non-versioned `/api/*` routes.

### Cluster management API

Authenticated by the existing session cookie (the whole `/api/` prefix is protected), same
`{success, data, error}` envelope as the rest of the console:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/clusters` (also with trailing slash) | list: id, display name, master addrs, source, status, editable, removable, error |
| POST | `/api/v1/clusters/validate` | parse + structurally check a TOML payload without persisting |
| POST | `/api/v1/clusters` | import (persist + build context) |
| GET | `/api/v1/clusters/{id}/config` | raw TOML of an imported cluster (`text/plain`, `no-store`) — used to prefill the editor; the default cluster answers 409 `not_removable`, and a file that stopped parsing is still readable so it can be repaired in place |
| PUT | `/api/v1/clusters/{id}` | replace an imported cluster's config (hot swap, old context released) |
| DELETE | `/api/v1/clusters/{id}` | remove an imported cluster (204; default cluster -> 409 `not_removable`) |
| POST | `/api/v1/clusters/{id}/probe` | master reachability/latency probe of one cluster |
| POST | `/api/v1/clusters/reload` | re-read the clusters directory |

- Bodies are TOML with `Content-Type: text/plain`, capped at 1 MiB (`413` beyond).
- Error codes: `invalid_config` (400), `cluster_exists` / `cluster_limit` / `not_removable` (409),
  `unknown_cluster` (404), `internal_error` (500).
- `validate` never touches the network (that is `/probe`) and never returns credentials; it returns
  structural `errors` (unparseable TOML, empty or unparsable master address, zero/out-of-range port)
  plus advisory `warnings`. `data.cluster.status` mirrors the verdict (`errors` non-empty ->
  `invalid`, otherwise `ready`) so a client can render the badge straight from the response.
- `validate` answers "is this configuration usable?", not "can I create it now": an id that is
  already registered is a **warning** (`POST` will answer `cluster_exists`, `PUT` will replace it),
  never an error. Making it an error would deadlock editing, because the console only enables
  *Save* after a passing validation.
- `POST` and `PUT` run the same structural check as `validate`, so text that fails validation is
  refused before it reaches the disk (`400 invalid_config`); `PUT` additionally rejects a payload
  whose `cluster_id` differs from the target path (`400 invalid_config`).
- `/probe` failures are reported as short human messages (`master info request timed out after 3s`,
  transport error text with the `err_box!` source location stripped); internals are never echoed.

### Import semantics

`ClusterConf::from_text_import(&str)` (`crates/common/curvine-config/src/cluster_conf.rs`) is the
import entry point. It is deliberately **not** the normal `ClusterConf::from()` path:

- no environment layer: `CURVINE_*_HOSTNAME` overrides would rewrite another cluster's hostnames, and
  the pipeline's `net_interface` handling would resolve *this host's* NIC address and paste it into
  every role hostname of the imported cluster (importing a config with an unknown interface fails
  outright, and with a known one silently points the console at this machine);
- no `apply_hostname_overrides`;
- an explicit `cluster_id` is required;
- the rest of the pipeline is unchanged (`normalize_whitespace`, `normalize_split_web_ports`,
  `validate`, `discovery.init`, `resolve_master_addrs`), so an imported file behaves like a normal
  config file afterwards.

Derived master addresses are the sharp edge here: a file that names only `[master] hostname` and
neither `[client] master_addrs` nor `[journal] journal_addrs` inherits the built-in
`localhost:8995`/`localhost:8996` defaults, which in a multi-cluster console means "talk to whatever
answers on this box". That case is reported as a `warnings` entry from `validate`.

### Frontend

`curvine-web/webui-next` keeps its page structure; three seams changed:

```text
ClusterSelectionProvider (localStorage: curvine-cluster)
   └── AuthProvider  (rebuilds the ApiClient when the selection changes)
         └── ApiProvider
               ├── ClusterListBridge   (fetches /v1/clusters, falls back if the id vanished)
               └── ConsoleLayout
                     ├── ClusterSwitcher  (topbar, beside the locale selector)
                     └── <Outlet/>
```

- **Header injection**: `createHttpClient({ clusterId })` and `createDownloadClient(clusterId)` add
  `X-Curvine-Cluster` in a request interceptor; an explicitly set header wins. Uploads and downloads
  use the same client. The header is present on every request as soon as the registry answers, because
  the selection is always a concrete cluster id; it is absent only before the first list arrives and on
  a deployment whose backend serves no list at all, which is the single-cluster case where the backend
  default applies anyway.
- **The switcher lists clusters, not "default"**: options are labeled by cluster id, and the `--conf`
  cluster is just one of them (it reports `source: 'default'`). An empty selection is therefore not a
  user-visible state: as soon as the list arrives the console picks a concrete cluster — the stored id
  when it is still there, else the declared default cluster when it is ready, else any ready cluster.
  The clusters page keeps showing the `default` / `file` source badge, so "which one is the built-in
  cluster" is answered there rather than in the switcher.
- **Refetch on switch, for free**: the `ApiClient` identity changes with the selection, so every page
  built on `useAsync`/`useApi` re-runs its loader. The switcher additionally fires the existing
  refresh signal, which covers loaders that are not keyed on the api client.
- **Stale selection**: the stored id is validated against the live list; if the backend no longer
  knows it, the console selects the default cluster and toasts instead of leaving every page on
  `404 unknown_cluster`. An empty list is treated as "could not read", not as "cluster removed", so a
  degraded backend cannot silently reset the operator's choice (and the switcher stays disabled).
- **`/clusters` page**: table of registered clusters (status dot, source, master addresses, editable /
  removable), import sheet with validate-before-save, delete with typed confirmation, probe button,
  refresh and rescan (reload).
- **Editing = read, change, validate, replace**: opening the editor fetches
  `GET /v1/clusters/{id}/config` and prefills the textarea with the file's own text, so *Save* means
  "replace this file" and not "retype the whole config". Load failure degrades to an empty textarea
  with an explicit "paste the full TOML" hint (the sheet stays open), and the default cluster's editor
  is disabled outright. Save stays gated on a passing validation; an unchanged text does not trip the
  unsaved-changes guard.
- **Error surfacing**: cluster API error codes travel through `ApiError.code`, so a 409
  `not_removable` renders as "this cluster cannot be edited" instead of a generic failure banner.
- **SPA routing**: `/clusters` is registered in `src/navigation.ts`, `src/App.tsx` and the backend's
  SPA allow-list, so a deep link and a hard reload both work.

## Resource model

Per registered cluster the console holds:

- one `UnifiedFileSystem` (lazy: connectors and caches only, no connection is opened at registration
  time, which is why an unreachable cluster can still be imported and then probed);
- one `ObservabilityService` with its collector loop, discovery loop, health loop and in-memory TSDB
  (up to 15M samples);
- one set of job/resync maps.

`web.max_clusters` (default 8) is therefore a **memory guard-rail**, not a scaling target: the
per-cluster TSDB dominates. Removing or replacing a cluster stops its service and drops the context.
Client-side metrics (`CLIENT_METRICS`) are process-global and shared by all clusters; that is
pre-existing behavior of the client crate and does not affect the console's own per-cluster
observability data.

## Security

- Cluster management endpoints sit behind the normal session auth; the middleware that resolves the
  header runs *after* auth, so cluster ids are not enumerable anonymously.
- The list projection is allow-listed (`id`, `display_name`, `master_addrs`, `source`, `status`,
  `error`, `editable`, `removable`) — an imported config's body, credentials or UFS secrets are never
  echoed back by the list.
- Raw TOML is served by `GET /api/v1/clusters/{id}/config`, which an authenticated session can call
  for any registered cluster. That is a deliberate extension of the pre-existing trust model: the
  console already has `/api/config`, which returns its own configuration verbatim to any logged-in
  session, and the editor cannot work without the original text. Everything written to disk is
  `0o600`, and the file only ever holds what an authenticated operator submitted. If per-cluster RBAC
  arrives (see follow-ups), this endpoint needs a `config:read` capability too.
- Error text returned to the browser is sanitised: the `err_box!` source location
  (`(crates/.../cluster_conf.rs:161)`), the `[<logger>] ERROR:` prefix and multi-line TOML diagnostic
  snippets are stripped by `sanitize_error_text()` before a message reaches `validate.errors[]`, an API
  error envelope, or the `error` field of an `invalid` list entry. Internals stay in the log.
- Import bodies are capped at 1 MiB and must parse; the file on disk is `0o600`.
- The header is a routing hint, not a privilege: any authenticated console user can address any
  registered cluster (documented non-goal, see follow-ups).

## Compatibility

- Old deployments: no `--clusters-dir`, no extra files -> registry has exactly the default cluster ->
  header-less requests behave exactly as before, `/api/v1/clusters` returns a one-entry list.
- The observability routers keep their paths and payloads; only the `Extension` they read changed.
- `/api/config` became cluster-aware (it used to answer the default cluster's config for every
  cluster, which would have been a footgun once a switcher exists).

## Testing

- `cargo nextest run -p curvine-config -p curvine-web`: 497 tests, including
  `crates/common/curvine-config/tests/from_text_import_test.rs` (6), `cluster::registry` (29),
  `router::cluster_api` (24) and `cluster_routing_tests` (10, three of which drive the real router
  through the header).
- `cargo clippy -p curvine-config -p curvine-web --all-targets -- -D warnings`, `cargo fmt --check`.
- SPA (this crate uses **npm**, `package-lock.json` is tracked): `npx tsc -b --noEmit`,
  `npx vitest run` (32 files / 336 tests) and `npx vite build`.
- Manual end-to-end procedure (two live clusters, one console):

```bash
# default cluster: a running single-node cluster on 127.0.0.1:8995
# second cluster: same shape, ports shifted, cluster_id = "beta"
curvine-web start --conf alpha-console.toml --port 9100
curl -c jar -X POST localhost:9100/api/v1/auth/login -d '{"username":"admin",...}'
curl -b jar -X POST localhost:9100/api/v1/clusters/validate --data-binary @beta.toml
curl -b jar -X POST localhost:9100/api/v1/clusters        --data-binary @beta.toml
curl -b jar localhost:9100/api/v1/overview                        # -> cluster_id alpha
curl -b jar -H 'X-Curvine-Cluster: beta' localhost:9100/api/v1/overview  # -> cluster_id beta
curl -b jar -X POST localhost:9100/api/v1/clusters/beta/probe      # -> reachable true
curl -b jar -X POST localhost:9100/api/v1/clusters/ghost/probe     # -> reachable false
curl -b jar localhost:9100/api/v1/clusters/beta/config             # -> raw TOML (editor prefill)
curl -b jar -X PUT  localhost:9100/api/v1/clusters/beta --data-binary @beta-edited.toml  # -> 200, hot swap
# restart the console -> /api/v1/clusters still lists alpha + beta
```

Four verification rounds were run against two real clusters (alpha = 127.0.0.1:8995, beta =
127.0.0.1:18995 with its own journal/worker/transfer). They confirmed: header routing per cluster
(`overview`/`workers` answer with the addressed cluster's master and worker ports), persistence
across a console restart, `invalid` degradation for a hand-broken file on disk, the structural
checks above, hot replacement through `PUT`, raw-config prefill round-tripping byte for byte, 0600
file mode on everything written, and the security surface (`401` without a session, `413` above
1 MiB, `404` for path-traversal ids, no source locations in any returned error text).

## Known limitations and follow-ups

- `probe.version` is always `null`: `FilesystemInfo` carries no version field and a second RPC per
  probe was not worth it yet.
- `/report` (the human-readable HTML report) is not cluster-aware; it always renders the default
  cluster. It is a navigation URL, so it cannot carry the header.
- The cluster switcher labels every entry by cluster id; `display_name` is only a tooltip, because the
  operator is reasoning about the ids that appear in configs and logs. A deployment that wants pretty
  names in the topbar needs a small follow-up (show `display_name` with the id as subtitle).
- No RBAC across clusters: any console session can operate every registered cluster.
- No HA for imported cluster definitions; they are local files under the console's clusters directory.
- An import whose master addresses derive to the built-in `localhost` defaults is only a warning today;
  a future revision may refuse it outright.
