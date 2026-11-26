# Transfer HTTP API

The transfer service owns load/export jobs. The master no longer exposes a
separate load-job API. All paths below are served by the transfer process
(`transfer.web_port`, default `9011`).

## Endpoint inventory

| Method | Path | Purpose | Success |
| --- | --- | --- | --- |
| POST | `/api/transfer/jobs` | Submit a load or export job | `202` new, `200` idempotent replay |
| GET | `/api/transfer/jobs` | List jobs | `200` |
| GET | `/api/transfer/jobs/{job_id}` | Job detail and task page | `200` |
| GET | `/api/transfer/jobs/{job_id}/tasks` | Task page | `200` |
| POST | `/api/transfer/jobs/{job_id}/cancel` | Request cancellation | `200` |
| POST | `/api/transfer/jobs/{job_id}/retry` | Retry a terminal job | `200` |
| GET | `/api/transfer/tenants` | Tenant summary | `200` |

## Submit a load

`target_path` is optional. When omitted, the transfer service derives it from
the matching mount: the source UFS prefix is replaced by the mount's Curvine
path. For example, a load of `s3://bucket/dir/sub/a.parquet` under a
`s3://bucket/dir` -> `/data/dir` mount targets
`/data/dir/sub/a.parquet`.

```json
{
  "kind": "load",
  "source_path": "s3://bucket/dir/sub",
  "files": [],
  "overwrite": true,
  "submitter": "scheduler",
  "tenant": "team-a",
  "force": false
}
```

- `kind`: `load` (default) or `export`.
- `source_path`: required UFS file/directory for load.
- `target_path`: optional; required only when no mount can be inferred.
- `files`: load-only explicit file list. Relative entries are resolved against
  `source_path`; empty entries and `#` comment lines are ignored.
- `overwrite`: default `true`.
- `force`: rerun a terminal job with the same identity.

A `text/plain` body is accepted as one file per line; `source_path`,
optional `target_path`, `overwrite`, `submitter`, `tenant`, and `force` are
query parameters.

The request identity is `(kind, source_path, target_path, files, overwrite,
submitter)`; a replay returns `200` with `created=false` instead of starting a
second job.

```shell
curl -i -X POST "http://transfer:9011/api/transfer/jobs" \
  -H 'Content-Type: application/json' \
  --data '{
    "source_path": "s3://bucket/dir/sub",
    "submitter": "scheduler"
  }'
```

Response:

```json
{
  "job_id": "…",
  "run_id": 1,
  "state": "pending",
  "files": 0,
  "created": true
}
```

## Progress and files

`GET /api/transfer/jobs/{job_id}` returns the job and a task page. The
`progress` object contains:

| Field | Meaning |
| --- | --- |
| `loaded_size` / `total_size` | Bytes completed / planned for non-skipped tasks |
| `loaded_files` / `total_files` | Files completed / planned |
| `skipped_files` / `skipped_size` | Incremental-load files and bytes skipped |
| `update_time` / `message` | Last update timestamp and scheduler message |

`planning` may report zero totals while a large directory is still being
listed; do not treat that as a percentage.

## Errors

Handler errors use `{"message": "…"}`. Invalid bodies/query values return
`400`; an unknown job returns `404`; active-job conflicts return `409`;
transfer-store or upstream failures return `500`/`502`.

The API has no authentication of its own; do not expose the transfer port
directly to the public internet.
