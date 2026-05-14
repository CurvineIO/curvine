# curvine-lancedb

`curvine-lancedb` lets Rust applications use LanceDB with Curvine storage. It is
a small facade over upstream `lancedb`: the Cargo package is
`curvine-lancedb`, but the Rust library name is still `lancedb`.

The crate adds the Curvine `ObjectStoreProvider`, `ObjectStoreRegistry`,
`Session`, and commit wiring needed by LanceDB. Normal LanceDB APIs remain
upstream-compatible.

## 当前状态

当前不发布到 crates.io。业务项目使用 Git 或 path 方式引入。

推荐生产业务固定 commit：

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", rev = "<commit-sha>" }
```

开发阶段也可以跟随分支：

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", branch = "feat/lancedb-on-curvine" }
```

本地联调可以使用 path：

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", path = "/path/to/curvine/curvine-lancedb" }
```

## URI 与 workspace

使用 `curvine://` URI，不需要 FUSE。

```text
curvine:///data/lancedb/demo
```

这个 URI 表示 Curvine 文件系统里的 workspace root。LanceDB 表、索引和提交文件都写在这个目录下面。

也支持带 authority 的 URI：

```text
curvine://tenant/data/lancedb/demo
```

它会映射到 Curvine 绝对路径：

```text
/tenant/data/lancedb/demo
```

`.curvine` 是内部保留命名空间，不要作为业务 workspace 使用。

## Curvine 连接配置

推荐业务直接传 master 地址，不需要生成临时配置文件。

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

`CURVINE_MASTER_ADDRS_KEY` 的值是：

```text
curvine.master_addrs
```

也兼容短 key：

```text
master_addrs
```

如果业务已有 Curvine client 配置文件，也可以传配置文件路径：

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

`CURVINE_CONF_FILE_KEY` 的值是：

```text
curvine.conf.path
```

还可以使用环境变量：

```bash
export CURVINE_CONF_FILE=/path/to/curvine-cluster.toml
```

配置优先级固定为：

```text
curvine.conf.path > curvine.master_addrs/master_addrs > CURVINE_CONF_FILE
```

三 master 配置文件示例：

```toml
[client]
master_addrs = [
    { hostname = "10.209.148.124", port = 8995 },
    { hostname = "10.209.148.125", port = 8995 },
    { hostname = "10.209.148.127", port = 8995 },
]
```

## 最小示例

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

## Session 用法

默认 `connect("curvine://...")` 会注入支持 Curvine 的 `Session`。如果业务需要显式构造 session，可以使用：

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

如果业务传入自定义 `Session`，facade 不会覆盖它。自定义 session 必须注册 `curvine` scheme，否则 LanceDB 会按上游语义报找不到 object store provider。

## 支持范围

已覆盖主线能力：

- `connect("curvine://...")`
- `connect_namespace(...).storage_option(...)`
- create/open/drop/list table
- add/query/count rows
- scalar filter、projection、limit
- vector search 和 index smoke
- conditional commit 所需的 `PutMode::Update`
- multipart upload complete
- shallow clone 所需的 object store path 语义

对象存储语义通过 Curvine 文件系统实现，包括 `put`、`head`、`get`、range read、delete、copy、list、list_with_delimiter、conditional put、multipart upload。

## 已知边界

- `curvine.master_addrs` 只包含 master 地址。其它 Curvine client 参数使用默认值；需要完整调参时请传 `curvine.conf.path`。
- eTag 是基于 Curvine 文件状态生成的 weak eTag，不是内容哈希。
- 当前 crate 面向 Rust 业务使用。Python SDK 仍应通过 Rust 核心复用存储逻辑，不应另写一套 Curvine 存储实现。
- 该 crate 目前随 Curvine 仓库开发，不保证分支 head 对业务构建稳定。生产使用请固定 `rev`。

## 本地验证

默认测试不需要真实 Curvine 集群：

```bash
cargo test -p curvine-lancedb
```

运行 live smoke 时传 master 地址：

```bash
CURVINE_MASTER_ADDRS=10.209.148.124:8995,10.209.148.125:8995,10.209.148.127:8995 \
  cargo test -p curvine-lancedb --test lancedb_smoke -- --ignored --nocapture
```

运行 Curvine minicluster e2e：

```bash
cargo test -p curvine-tests --test lancedb_object_store_e2e -- --nocapture
```

使用外部集群跑 e2e：

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
  cargo test -p curvine-tests --test lancedb_object_store_e2e -- --nocapture
```

提交前至少运行：

```bash
cargo fmt --all
cargo clippy -p curvine-lancedb --all-targets --all-features -- -D warnings
cargo test -p curvine-lancedb
```

## English Quick Start

Use this crate through Git or path dependencies. The package name is
`curvine-lancedb`; the library name is `lancedb`.

```toml
[dependencies]
lancedb = { package = "curvine-lancedb", git = "https://github.com/CurvineIO/curvine", rev = "<commit-sha>" }
```

Pass Curvine master addresses directly:

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

Use `curvine.conf.path` when the application needs a full Curvine client
configuration file. The priority order is:

```text
curvine.conf.path > curvine.master_addrs/master_addrs > CURVINE_CONF_FILE
```
