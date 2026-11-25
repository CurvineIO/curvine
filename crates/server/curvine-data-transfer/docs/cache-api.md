# Cache load and free job API

The master web server exposes cache operations as `load` and `free` jobs. The
API is independent of whether transfer runs inside the master (embedded) or as a
standalone `curvine-transfer` Deployment; that choice is an implementation
detail and never appears in responses.

Both APIs are unauthenticated JSON/text APIs. Put the master behind the
cluster's normal network boundary or an authenticating gateway before exposing
it to other systems.

## Endpoint inventory

| Method | Path | Purpose | Success |
| --- | --- | --- | --- |
| `GET` | `/api/load/jobs` | List load jobs, newest first | `200` |
| `POST` | `/api/load/jobs` | Submit a load job (JSON or text file list) | `202` |
| `GET` | `/api/load/jobs/{job_id}` | Get one load job | `200` |
| `POST` | `/api/load/jobs/{job_id}/cancel` | Cancel a load job | `200` |
| `GET` | `/api/export/jobs` | List export jobs, newest first | `200` |
| `POST` | `/api/export/jobs` | Submit an export job | `202` |
| `GET` | `/api/export/jobs/{job_id}` | Get one export job | `200` |
| `POST` | `/api/export/jobs/{job_id}/cancel` | Cancel an export job | `200` |
| `GET` | `/api/free/jobs` | List recent cache-release jobs | `200` |
| `POST` | `/api/free/jobs` | Start a cache-release job | `202` |
| `GET` | `/api/free/jobs/{job_id}` | Get one cache-release job | `200` |

A load job is one `cv load` request; per-file tasks are internal and are not
exposed here.

## Load jobs

### List: `GET /api/load/jobs`

| Parameter | Type | Required | Default | Meaning / constraints |
| --- | --- | --- | --- | --- |
| `state` | string | No | all | Job state. Accepts `pending`, `planning`, `dispatching`, `running`, `canceling`, `completed`, `failed`, `canceled`, `partial-success` (or `partial_success`) |
| `submitter` | string | No | all | Exact submitter match |
| `tenant` | string | No | all | Exact tenant match |
| `limit` | integer | No | `20` | Page size, `1..1000` |
| `page_token` | string | No | first page | Value from the previous response's `next_page_token` |

Response:

| Field | Type | Meaning |
| --- | --- | --- |
| `jobs` | array | Load jobs, newest first |
| `next_page_token` | string \| null | Pass back as `page_token`; null on the last page |

Each job has the fields in [Job fields](#job-fields).

Errors: `400` for an unknown `state` or invalid `limit`; `502` when the
standalone transfer service cannot be reached.

### Submit: `POST /api/load/jobs`

JSON body:

| Parameter | Type | Required | Default | Meaning / constraints |
| --- | --- | --- | --- | --- |
| `source_path` | string | Yes | — | UFS source directory; the root for mapping `files` |
| `target_path` | string | Yes | — | Curvine target directory |
| `files` | string[] | No | `[]` | Exact files to load. Each entry must be under `source_path`; mapped to `target_path` by its relative path. Empty means a normal directory load |
| `overwrite` | boolean | No | `true` | Overwrite an existing target file |
| `submitter` | string | No | `http-api` | Free-form caller identity |
| `tenant` | string | No | `""` | Tenant label |
| `force` | boolean | No | `false` | Retry a job that already exists in a terminal state |

A `text/plain` body is treated as the file list: each line is one source path,
blank lines and lines starting with `#` are ignored. `source_path` and
`target_path` are then required as query parameters; the same query parameters
also accept `overwrite`, `submitter`, `tenant`, and `force`. The body limit is
64 MiB.

Response (`202`):

| Field | Type | Meaning |
| --- | --- | --- |
| `job_id` | string | Job identifier |
| `run_id` | integer | Run number (increments after a retry) |
| `state` | string | Initial state, usually `pending` |
| `files` | integer | Number of entries submitted (0 for a directory load) |

Errors:

| Status | Meaning |
| --- | --- |
| `400` | Invalid JSON, empty `source_path`/`target_path`, invalid limit/state, or planner/service validation failure |
| `413` | Body exceeds the 64 MiB limit |
| `502` | Standalone transfer service unreachable |

```bash
# Directory load
curl -X POST http://master:9000/api/load/jobs \
  -H 'Content-Type: application/json' \
  -d '{"source_path":"s3://bucket/dir","target_path":"/data/dir"}'

# File-list load
curl -X POST \
  'http://master:9000/api/load/jobs?source_path=s3://bucket/dir&target_path=/data/dir' \
  -H 'Content-Type: text/plain' \
  --data-binary @files.txt
```

### Detail: `GET /api/load/jobs/{job_id}`

Returns one job with the same [Job fields](#job-fields). A job id that belongs
to an export returns `404`; an unknown id returns `404`.

```bash
curl http://master:9000/api/load/jobs/9c2f... 
```

### Cancel: `POST /api/load/jobs/{job_id}/cancel`

Requests cancellation of the current run. No body is required.

| Field | Type | Meaning |
| --- | --- | --- |
| `job_id` | string | Job identifier |
| `state` | string | State after the request, normally `canceling` |

| Status | Meaning |
| --- | --- |
| `200` | Cancellation requested (or the job was already canceling/terminal) |
| `404` | Unknown id, or the id is not a load job |
| `400` | The job cannot be canceled in its current state |
| `502` | Standalone transfer service unreachable |

```bash
curl -X POST http://master:9000/api/load/jobs/9c2f.../cancel
```

### Job fields

| Field | Type | Meaning |
| --- | --- | --- |
| `job_id` | string | Job identifier |
| `run_id` | integer | Run number |
| `kind` | string | `load` |
| `state` | string | Current state |
| `source_path` | string | UFS source root |
| `target_path` | string | Curvine target root |
| `submitter` | string | Submitter |
| `tenant` | string | Tenant |
| `owner` | string | Master that currently owns the job |
| `progress.loaded_size` | integer | Bytes transferred for planned tasks |
| `progress.total_size` | integer | Bytes of planned tasks (excluding skipped files) |
| `progress.total_files` | integer | Planned files (excluding skipped files) |
| `progress.loaded_files` | integer | Planned files that completed |
| `progress.skipped_files` | integer | Files skipped by incremental planning |
| `progress.skipped_size` | integer | Bytes skipped by incremental planning |
| `progress.update_time` | integer | Last progress update, milliseconds since epoch |
| `progress.message` | string | Human-readable status |
| `created_at` | integer | Creation time, milliseconds since epoch |
| `updated_at` | integer | Last update, milliseconds since epoch |
| `next_page_token` | string \| null | Only in list responses |

Progress semantics:

- `total_files` / `total_size` are persisted when planning finishes, so byte
  progress cannot reach 100% before every planned file has reported.
- `skipped_files` / `skipped_size` are incremental skips (for example a file
  already cached and matching its UFS fingerprint); they are not included in
  `total_files` / `total_size`.
- A job whose files are all skipped completes with `0/0` plus the skipped
  counters.
- Skipped files now receive the mount/job TTL during planning, so an
  incremental load also refreshes their lifetime.

Finished load jobs are retained by the transfer service's
`terminal_retention` (10 minutes for embedded, 168 hours for standalone by
default).

## Export jobs

An export moves a Curvine file or directory back to UFS. The HTTP endpoint
requires an explicit UFS target; the CLI derives it from the mount, but the
master API does not guess mount peers.

### List: `GET /api/export/jobs`

Same query parameters, pagination and `Job fields` as load jobs. Only export
jobs are returned.

### Submit: `POST /api/export/jobs`

| Parameter | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `source_path` | string | Yes | — | Curvine source path |
| `target_path` | string | Yes | — | UFS destination path |
| `overwrite` | boolean | No | `true` | Overwrite an existing UFS target |
| `submitter` | string | No | `http-api` | Caller identity |
| `tenant` | string | No | `""` | Tenant label |
| `force` | boolean | No | `false` | Retry a terminal job |

`files` is rejected for export jobs. A `text/plain` body is also rejected.

```bash
curl -X POST http://master:9000/api/export/jobs \
  -H 'Content-Type: application/json' \
  -d '{"source_path":"/data/dir","target_path":"s3://bucket/dir"}'
```

Response (`202`): `{"job_id","run_id","state","files":0}`.

| Status | Meaning |
| --- | --- |
| `202` | Accepted |
| `400` | Invalid body, missing paths, or a `files` list |
| `502` | Standalone transfer service unreachable |

### Detail: `GET /api/export/jobs/{job_id}`

Returns one export job with the `Job fields`; a non-export id returns `404`.

### Cancel: `POST /api/export/jobs/{job_id}/cancel`

Same response and status table as load cancellation, scoped to export jobs.

## Free jobs

A free job releases the Curvine cache copy of files that are also present in
UFS. The free RPC is synchronous internally; the HTTP API runs it on a blocking
task and exposes job state.

### List: `GET /api/free/jobs`

| Parameter | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `limit` | integer | No | `20` | Number of newest jobs, `1..1000` |

Response: `{"jobs": [FreeJob]}`. Errors: `400` for an invalid limit.

### Submit: `POST /api/free/jobs`

| Parameter | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `path` | string | Yes | — | Curvine path to release |
| `recursive` | boolean | No | `false` | Release the whole subtree |

Response (`202`): a `FreeJob` with `state=running`.

Errors: `400` for an empty path or invalid JSON; `502` is not used because free
runs on the master itself.

```bash
curl -X POST http://master:9000/api/free/jobs \
  -H 'Content-Type: application/json' \
  -d '{"path":"/data/dir","recursive":true}'
```

### Detail: `GET /api/free/jobs/{job_id}`

Returns the `FreeJob`. Poll until `state` is `completed` or `failed`.

| Field | Type | Meaning |
| --- | --- | --- |
| `job_id` | string | Job identifier |
| `path` | string | Requested path |
| `recursive` | boolean | Whether the subtree was included |
| `state` | string | `running`, `completed`, or `failed` |
| `freed_inodes` | integer | Number of inodes whose cache copy was released |
| `freed_bytes` | integer | Released cache bytes |
| `message` | string | Failure detail, empty on success |
| `created_at` | integer | Creation time, milliseconds since epoch |
| `updated_at` | integer | Last update, milliseconds since epoch |

The free-job registry is in master memory and bounded to the 200 newest jobs;
it is not durable across a master restart. Only one free request is executed at
a time per master because the underlying free operation takes the metadata
write lock.
