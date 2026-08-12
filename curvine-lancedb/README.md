# curvine-lancedb

`curvine-lancedb` lets applications use LanceDB with Curvine storage. It is a
facade over upstream `lancedb` v0.27.2: the Cargo package is
`curvine-lancedb`, while the Rust library name remains `lancedb`.

The crate provides the Curvine `ObjectStoreProvider`, `ObjectStoreRegistry`,
`Session`, and safe commit wiring required by LanceDB. Normal LanceDB APIs stay
upstream-compatible. It also includes the Python SDK bindings under the
`python-sdk` feature.

| Path | Role |
|------|------|
| `src/` | Rust facade, Curvine object store, connection and safe commit wiring |
| `src/python/` | PyO3 bindings, enabled by the `python-sdk` feature |
| `python/curvine_lancedb/` | Python package re-exporting the native module |
| `python/tests/` | pytest suite |
| `pyproject.toml` | maturin wheel configuration |

## Status

The Rust crate is not published to crates.io yet. Rust applications should use a
Git or path dependency.

For production-like usage, pin a commit:

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", rev = "<commit-sha>" }
```

For local development:

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", path = "/path/to/curvine/curvine-lancedb" }
```

## URI Semantics

Use `curvine://` URIs directly. FUSE is not required.

```text
curvine:///data/lancedb/demo
```

This URI maps to the Curvine filesystem path:

```text
/data/lancedb/demo
```

Authority is treated as the first path segment:

```text
curvine://tenant/data/lancedb/demo
```

maps to:

```text
/tenant/data/lancedb/demo
```

`.curvine` is reserved for internal metadata. Do not use it as an application
workspace.

## Curvine Configuration

The canonical storage option for direct master addresses is:

```text
curvine.master_addrs
```

The crate exposes it as `CURVINE_MASTER_ADDRS_KEY`.

```rust,no_run
use lancedb::connect;
use lancedb::object_store::CURVINE_MASTER_ADDRS_KEY;

# async fn example() -> lancedb::Result<()> {
let db = connect("curvine:///data/lancedb/demo")
    .storage_option(
        CURVINE_MASTER_ADDRS_KEY,
        "10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995",
    )
    .execute()
    .await?;
# Ok(())
# }
```

The unprefixed key `master_addrs` is intentionally not supported. LanceDB
passes storage options through a flat map shared by all backends, so Curvine
only consumes namespaced keys.

If the application needs a full Curvine client configuration file, use:

```text
curvine.conf.path
```

The crate exposes it as `CURVINE_CONF_FILE_KEY`.

```rust,no_run
use lancedb::connect;
use lancedb::object_store::CURVINE_CONF_FILE_KEY;

# async fn example() -> lancedb::Result<()> {
let db = connect("curvine:///data/lancedb/demo")
    .storage_option(CURVINE_CONF_FILE_KEY, "/path/to/curvine-cluster.toml")
    .execute()
    .await?;
# Ok(())
# }
```

The environment fallback is:

```bash
export CURVINE_CONF_FILE=/path/to/curvine-cluster.toml
```

Configuration precedence is fixed:

```text
curvine.conf.path > curvine.master_addrs > CURVINE_CONF_FILE
```

Example configuration file:

```toml
[client]
master_addrs = [
    { hostname = "10.209.148.124", port = 8995 },
    { hostname = "10.209.148.125", port = 8995 },
    { hostname = "10.209.148.127", port = 8995 },
]
```

`curvine.master_addrs` initializes only the Curvine master/client configuration
needed by `CurvineFileSystem`. It does not initialize FUSE or job subsystems.
Use `curvine.conf.path` if the application needs non-default Curvine client
settings.

## Rust Example

```rust,no_run
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::connect;
use lancedb::object_store::CURVINE_MASTER_ADDRS_KEY;
use lancedb::query::ExecutableQuery;

# async fn example() -> lancedb::Result<()> {
let master_addrs = "10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995";

let db = connect("curvine:///data/lancedb/demo")
    .storage_option(CURVINE_MASTER_ADDRS_KEY, master_addrs)
    .execute()
    .await?;

let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
let batch = RecordBatch::try_new(
    schema,
    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
)?;

let table = db
    .create_table("items", batch)
    .storage_option(CURVINE_MASTER_ADDRS_KEY, master_addrs)
    .execute()
    .await?;

let batches = table.query().execute().await?.try_collect::<Vec<_>>().await?;
let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
assert_eq!(row_count, 3);
# Ok(())
# }
```

## Session Usage

`connect("curvine://...")` injects a Curvine-aware `Session` by default.

If the application constructs sessions explicitly, use:

```rust,no_run
use lancedb::connect;
use lancedb::object_store::curvine_session;

# async fn example() -> lancedb::Result<()> {
let db = connect("curvine:///data/lancedb/demo")
    .session(curvine_session())
    .storage_option(
        lancedb::object_store::CURVINE_MASTER_ADDRS_KEY,
        "10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995",
    )
    .execute()
    .await?;
# Ok(())
# }
```

If a custom `Session` is passed, the facade does not replace it. The custom
session must register the `curvine` scheme, or LanceDB will report the same
missing-provider error as upstream.

## Python SDK

The Python package is `curvine-lancedb` and is imported as `curvine_lancedb`.
Python bindings are built with `maturin` and the `python-sdk` Cargo feature.

```bash
cd curvine-lancedb
python3 -m venv .venv
source .venv/bin/activate
pip3 install maturin pytest pytest-asyncio

python3 -m maturin develop --features python-sdk
python3 -m pytest python/tests/ -v
```

Build a wheel:

```bash
python3 -m maturin build --features python-sdk --release
pip3 install ../target/wheels/curvine_lancedb-*.whl
```

Minimal Python usage:

```python
import asyncio
import curvine_lancedb
import pyarrow as pa


async def main():
    conn = await curvine_lancedb.connect("/tmp/my-db").execute()
    data = pa.record_batch({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    table = await conn.create_table("users", data)
    result = await table.search().where("id > 1").limit(10).to_arrow()
    print(result.to_pandas())


asyncio.run(main())
```

Arrow data crosses the Python/Rust boundary through the Arrow C Data Interface,
zero-copy where possible.

## Supported Scope

The current implementation covers the LanceDB object-store path needed for:

- `connect("curvine://...")`
- `connect_namespace(...).storage_option(...)`
- create, open, drop, and list table
- add, query, count rows
- scalar filters, projection, and limit
- vector search and index smoke tests
- conditional commit via `PutMode::Update`
- multipart upload completion
- shallow clone object-store path semantics

The Curvine object store implements `put`, `head`, `get`, range reads,
`delete`, `copy`, `list`, `list_with_delimiter`, conditional put, and multipart
upload.

## Known Boundaries

- `curvine.master_addrs` only carries master addresses. Other Curvine client
  settings use defaults unless `curvine.conf.path` is provided.
- eTags are weak values derived from Curvine file metadata. They are not content
  hashes.
- Python SDK storage behavior must reuse the Rust core. Do not implement a
  separate Curvine storage layer in Python.
- The crate is developed inside the Curvine repository. Pin `rev` for stable
  application builds.

## Validation

Default Rust tests do not require a live Curvine cluster:

```bash
cargo test -p curvine-lancedb
```

Run the live crate smoke test with direct master addresses:

```bash
CURVINE_MASTER_ADDRS=10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995 \
  cargo test -p curvine-lancedb --test lancedb_smoke -- --ignored --nocapture
```

Run Curvine minicluster e2e:

```bash
cargo test -p curvine-tests --features lancedb --test lancedb_object_store_e2e -- --nocapture
```

Run e2e against an external cluster:

```bash
cat >/tmp/curvine-lancedb-e2e.toml <<'EOF'
[client]
master_addrs = [
    { hostname = "10.209.148.124", port = 8995 },
    { hostname = "10.209.148.125", port = 8995 },
    { hostname = "10.209.148.127", port = 8995 },
]
EOF

CURVINE_E2E_CONF_FILE=/tmp/curvine-lancedb-e2e.toml \
  cargo test -p curvine-tests --features lancedb --test lancedb_object_store_e2e -- --nocapture
```

Before submitting changes, run at least:

```bash
cargo fmt --all
cargo clippy -p curvine-lancedb --all-targets --all-features -- -D warnings
cargo test -p curvine-lancedb
```

## 中文简述

`curvine-lancedb` 是 LanceDB on Curvine 的 facade crate。业务侧 Rust 项目
通过 Git 或 path 依赖引入，Cargo package 名是 `curvine-lancedb`，Rust crate
名仍是 `lancedb`。生产使用建议固定 Git `rev`。连接 Curvine 推荐使用
`curvine.master_addrs`；如果需要完整 Curvine client 配置，则使用
`curvine.conf.path`。
