# Cache load and free job API

The master web server exposes cache operations as `load` and `free` jobs. The
API is independent of whether transfer runs inside the master (embedded) or as a
standalone `curvine-transfer` Deployment; that choice is an implementation
detail and never appears in responses.

Both operations are unauthenticated JSON APIs. Put the master behind the
cluster's normal network boundary or an authenticating gateway before exposing
it to other systems.

## Load jobs

A load job copies UFS files into Curvine. One job is one `cv load` request; the
per-file tasks are internal and are not exposed here.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/load/jobs` | List load jobs, newest first |
| `POST` | `/api/load/jobs` | Submit a load job |
| `GET` | `/api/load/jobs/{job_id}` | Get one load job (404 for a non-load id) |

### List

Query parameters: `state`, `submitter`, `tenant`, `limit` (default 20, max
1000), `page_token`. The response is `{"jobs": [...], "next_page_token": ...}`.

### Submit

JSON body:

```json
{
  "source_path": "s3://bucket/dir",
  "target_path": "/data/dir",
  "files": [
    "s3://bucket/dir/a",
    "s3://bucket/dir/b"
  ],
  "overwrite": true,
  "submitter": "my-service",
  "tenant": "default",
  "force": false
}
```

- `source_path` and `target_path` are required: a UFS root and a Curvine root.
- `files` is optional. When present, the planner builds tasks from this exact
  list instead of listing `source_path`; every entry must be under
  `source_path` and maps to `target_path` by its relative path. Omit it for a
  normal directory load.
- `force` retries a job that already exists in a terminal state.
- `files` participates in the job identity, so the same roots with a different
  list create a different job.

A `text/plain` body is also accepted: each line is one source file (`#` and
blank lines are ignored), and `source_path`/`target_path` go in the query
string. The body limit is 64 MiB.

```bash
curl -X POST \
  'http://master:9000/api/load/jobs?source_path=s3://bucket/dir&target_path=/data/dir' \
  -H 'Content-Type: text/plain' \
  --data-binary @files.txt
```

Successful submission returns `202`:

```json
{"job_id": "…", "run_id": 1, "state": "pending", "files": 2}
```

### Progress

```json
{
  "job_id": "…",
  "kind": "load",
  "state": "running",
  "source_path": "s3://bucket/dir",
  "target_path": "/data/dir",
  "submitter": "my-service",
  "tenant": "default",
  "progress": {
    "loaded_size": 1048576,
    "total_size": 2097152,
    "total_files": 2,
    "loaded_files": 1,
    "skipped_files": 0,
    "skipped_size": 0,
    "update_time": 1790000000000,
    "message": "task completed successfully"
  },
  "created_at": 1790000000000,
  "updated_at": 1790000000000
}
```

- `total_files` / `total_size` are the files and bytes that need transferring.
  They are persisted when planning finishes, so byte progress cannot reach
  100% before every planned file has reported.
- `loaded_files` counts planned tasks that completed.
- `skipped_files` / `skipped_size` come from incremental planning (for example
  `--refresh skip-cached`) and are not included in `total_files`/`total_size`.
  A job whose files are all skipped completes with `0/0` plus the skipped
  counters.

Finished jobs are retained by the transfer service's `terminal_retention`
(10 minutes for embedded, 168 hours for standalone by default).

## Free jobs

A free job releases the Curvine cache copy of files that are also present in
UFS. The free RPC is synchronous internally; the HTTP API runs it on a blocking
task and exposes job state.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/free/jobs` | List recent free jobs, newest first (`limit`, default 20) |
| `POST` | `/api/free/jobs` | Start a free job |
| `GET` | `/api/free/jobs/{job_id}` | Get one free job |

Submit:

```json
{"path": "/data/dir", "recursive": true}
```

The response is `202` with a `running` job. Poll the detail endpoint until
`state` is `completed` or `failed`:

```json
{
  "job_id": "…",
  "path": "/data/dir",
  "recursive": true,
  "state": "completed",
  "freed_inodes": 120,
  "freed_bytes": 104857600,
  "message": "",
  "created_at": 1790000000000,
  "updated_at": 1790000000000
}
```

The free-job registry is in master memory and bounded to the 200 newest jobs;
it is not durable across a master restart. Only one free request is executed at
a time per master because the underlying free operation takes the metadata
write lock.
