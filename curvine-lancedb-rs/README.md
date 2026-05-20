# Curvine LanceDB

Facade crate over upstream [LanceDB](https://github.com/lancedb/lancedb) (v0.27.2) that injects Curvine object store for `curvine://` URIs. Provides both Rust and Python APIs.

| Path | Role |
|------|------|
| `src/` | Rust crate: connection, table, query, object store integration |
| `src/python/` | PyO3 bindings (feature-gated via `python-sdk`) |
| `python/curvine_lancedb/` | Python package re-exporting `_native` module |
| `python/tests/` | pytest test suite |
| `pyproject.toml` | maturin wheel config |

---

## Rust

```bash
# Check compilation
cargo check -p curvine-lancedb-rs

# Run Rust tests
cargo test -p curvine-lancedb-rs
```

---

## Python SDK

**Package**: `curvine-lancedb` on PyPI, imported as `curvine_lancedb`.

**Prerequisites**: Python >= 3.8.

### Development

```bash
cd curvine-lancedb-rs

python3 -m venv .venv && source .venv/bin/activate
pip3 install maturin pytest pytest-asyncio

python3 -m maturin develop --features python-sdk
python3 -m pytest python/tests/ -v
```

`maturin develop` compiles and installs directly into the current Python environment. Re-run after Rust code changes.

### Build wheel (for distribution)

```bash
python3 -m maturin build --features python-sdk --release
pip3 install ../target/wheels/curvine_lancedb-*.whl
```

### Usage

```python
import asyncio
import curvine_lancedb
import pyarrow as pa


async def main():
    # Connect (local path or curvine://...)
    conn = await curvine_lancedb.connect("/tmp/my-db").execute()

    # Create table from pyarrow RecordBatch
    data = pa.record_batch({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    table = await conn.create_table("users", data)

    # Insert more rows
    more = pa.record_batch({"id": [4, 5], "name": ["David", "Eve"]})
    await table.add(more)

    # Query
    result = await table.search().where("id > 2").limit(10).to_arrow()
    print(result.to_pandas())

    # Count / delete
    print(await table.count_rows())
    await table.delete("id = 1")


asyncio.run(main())
```

### API overview

| Method | Returns | Notes |
|--------|---------|-------|
| `connect(uri)` | `ConnectBuilder` | `.storage_option(k, v)` then `.execute()` |
| `conn.table_names()` | `list[str]` | async |
| `conn.create_table(name, data)` | `Table` | `data`: pyarrow RecordBatch or Table |
| `conn.open_table(name)` | `Table` | async |
| `conn.drop_table(name)` | `None` | async |
| `table.count_rows(filter=None)` | `int` | async |
| `table.add(data)` | `None` | async, appends rows |
| `table.delete(predicate)` | `None` | async, SQL-like filter |
| `table.search()` | `Query` | sync, returns builder |
| `query.limit(n)` | `Query` | chainable |
| `query.offset(n)` | `Query` | chainable |
| `query.where(filter)` | `Query` | chainable, SQL-like filter |
| `query.select(columns)` | `Query` | chainable, `list[str]` |
| `query.vector(v)` | `VectorQuery` | chainable, `list[float]` |
| `query.to_arrow()` | `pyarrow.Table` | async, executes query |

### Architecture

```
Python                  Rust (PyO3)                Upstream lancedb
curvine_lancedb   →     src/python/py_*.rs    →    lancedb v0.27.2
.connect()              ConnectBuilder              + CurvineObjectStore
.create_table()         PyConnection / PyTable      (injected for curvine://)
.search()               PyQuery / PyVectorQuery
```

Arrow data is exchanged via the C Data Interface (PyCapsule protocol), zero-copy where possible.
