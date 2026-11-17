# Crate map

`crates/` holds the library crates of the Curvine Cargo workspace, grouped by layer. The service binaries and facade packages live at the repository root, for example `curvine-server`, `curvine-cli` and `curvine-fuse`.

When you add, rename or remove a crate, update the `members` and `default-members` lists in the root [`Cargo.toml`](../Cargo.toml) and this page in the same pull request.

## Layers

| Directory | What lives there |
| --- | --- |
| `core/` | Error primitives, async runtime, networking, RPC, I/O, OS helpers, global allocator, metrics, fault injection |
| `common/` | Configuration, shared model types, protobuf messages, filesystem/storage/UFS APIs, filesystem errors, config macros |
| `adapters/` | Concrete backends: local and SPDK worker storage, RocksDB, service discovery, UFS adapters (OpenDAL, OSS-HDFS, COSN), HDFS JNI bindings |
| `metadata/` | Raft-based replication of master metadata |
| `client/` | Client block and file I/O, the unified filesystem over Curvine and UFS, job clients, benchmarks |
| `server/` | Data transfer jobs and workers |
| `sdk/` | Binding-neutral SDK core and the Java (JNI) and Python (PyO3) bindings |

A crate depends only on crates in its own row or in rows above it: `core` uses only `core`, `common` uses `core`, and so on down the table. A few crates also use packages at the repository root, for example `curvine-sdk-core` uses `curvine-client` and `curvine-data-transfer` uses `curvine-web`.

## Crates

"Default member" says whether the crate is in `default-members`. See [Default members and opt-in packages](#default-members-and-opt-in-packages) for what "No" means.

| Path | Package | Purpose | Default member |
| --- | --- | --- | --- |
| `core/error` | `curvine-core-error` | Base error primitives used across the workspace | Yes |
| `core/runtime` | `curvine-runtime` | Async runtime primitives | Yes |
| `core/net` | `curvine-net` | Networking primitives | Yes |
| `core/rpc` | `curvine-rpc` | RPC primitives | Yes |
| `core/curvine-io` | `curvine-io` | Block I/O, local files, data slices, cache manager | Yes |
| `core/curvine-sys` | `curvine-sys` | OS and libc helpers: build version (`curvine_sys::version`), pipes, signals, raw buffers, FFI utilities | Yes |
| `core/curvine-alloc` | `curvine-alloc` | Global allocator for binaries: jemalloc, mimalloc or system, chosen by feature | Yes |
| `core/curvine-metrics` | `curvine-metrics` | Prometheus metrics primitives | Yes |
| `core/curvine-fault` | `curvine-fault` | Fault-injection framework, enabled by the `fault-injection` feature | Yes |
| `common/curvine-error` | `curvine-error` | Filesystem error types | Yes |
| `common/curvine-proto` | `curvine-proto` | Protobuf messages for the common, master, worker, job, mount, replication and transfer RPCs (`proto/`) | Yes |
| `common/curvine-model` | `curvine-model` | Shared model types | Yes |
| `common/curvine-fs-api` | `curvine-fs-api` | Filesystem API | Yes |
| `common/curvine-storage-api` | `curvine-storage-api` | Worker storage abstractions | Yes |
| `common/curvine-ufs-api` | `curvine-ufs-api` | API that UFS adapters implement | Yes |
| `common/curvine-config` | `curvine-config` | Cluster, client and service configuration | Yes |
| `common/curvine-macros` | `curvine-common-macros` | Proc macros for configuration types such as `ClientConf` and `ClusterConf` | Yes |
| `adapters/curvine-rocksdb` | `curvine-rocksdb` | RocksDB metadata store | No |
| `adapters/curvine-service-discovery` | `curvine-service-discovery` | Service discovery abstractions and providers; etcd behind the `etcd` feature | Yes |
| `adapters/curvine-storage-local` | `curvine-storage-local` | Local worker storage; SPDK behind the `spdk` feature | Yes |
| `adapters/curvine-storage-spdk` | `curvine-storage-spdk` | SPDK worker storage | No |
| `adapters/curvine-ufs-opendal` | `curvine-ufs-opendal` | UFS adapter on OpenDAL, one feature per backend (S3, OSS, COS, Azure Blob, GCS, WebHDFS, HDFS) | No |
| `adapters/curvine-ufs-oss-hdfs` | `curvine-ufs-oss-hdfs` | UFS adapter for Alibaba Cloud OSS-HDFS | No |
| `adapters/curvine-ufs-cosn` | `curvine-ufs-cosn` | UFS adapter for Tencent Cloud COS through Hadoop COSN | No |
| `adapters/curvine-hdfs-jni` | `curvine-hdfs-jni` | JNI bindings to the HDFS Java client, used by the `opendal-hdfs` backend | No |
| `metadata/curvine-raft` | `curvine-raft` | Raft replication of master metadata, stored in RocksDB | Yes |
| `client/curvine-client-core` | `curvine-client-core` | Client block and file I/O | Yes |
| `client/curvine-job-client` | `curvine-job-client` | Job and transfer RPC clients | Yes |
| `client/curvine-unified-fs` | `curvine-unified-fs` | One filesystem over Curvine and UFS; UFS adapters behind features | Yes |
| `client/curvine-bench` | `curvine-bench` | Client benchmarks | Yes |
| `server/curvine-data-transfer` | `curvine-data-transfer` | Data transfer jobs and workers; web UI and job stores (SQLite, MySQL, PostgreSQL) behind features | Yes |
| `sdk/curvine-sdk-core` | `curvine-sdk-core` | SDK core shared by the language bindings | Yes |
| `sdk/curvine-libsdk-java` | `curvine-libsdk-java` | Java SDK JNI bindings | No |
| `sdk/curvine-libsdk-python` | `curvine-libsdk-python` | Python SDK bindings and the maturin project for the wheel | No |

### Packages at the repository root

| Package | Purpose | Default member |
| --- | --- | --- |
| `curvine-server` | Master and worker server binary | Yes |
| `curvine-master` | Master metadata service | Yes |
| `curvine-worker` | Worker block storage service | Yes |
| `curvine-mds` | KV metadata service | Yes |
| `curvine-client` | Rust client | Yes |
| `curvine-cli` | Command-line tool | Yes |
| `curvine-fuse` | FUSE filesystem | Yes |
| `curvine-s3-gateway` | S3-compatible object gateway | Yes |
| `curvine-web` | Web server for the master UI and HTTP API | Yes |
| `curvine-ufs` | UFS compatibility facade | Yes |
| `curvine-libsdk` | Native SDK library; the Java and Python bindings are features | Yes |
| `curvine-lancedb` | LanceDB on Curvine storage | No, opt-in |
| `curvine-tests` | Integration and regression test harness | No, opt-in |

## Default members and opt-in packages

`cargo build` and `cargo test` at the workspace root only cover `default-members`. Use `cargo test -p <package>` for anything else.

- **Opt-in packages.** `curvine-tests` and `curvine-lancedb` are left out on purpose. `curvine-tests` pulls in whole-cluster integration, FUSE and optional external storage dependencies, see [curvine-tests/README.md](../curvine-tests/README.md). `curvine-lancedb` pulls in heavy Lance and Arrow dependencies, see [curvine-lancedb/README.md](../curvine-lancedb/README.md).
- **Feature-gated crates.** The crates marked "No" in the table above are still compiled whenever a default member uses them. `curvine-rocksdb` comes in through `curvine-raft` and `curvine-storage-local`. The UFS adapters come in through `curvine-unified-fs` features, SPDK through the `spdk` features, and the SDK bindings through the `java-sdk` and `python-sdk` features of `curvine-libsdk`. Only `python-sdk` is opt-in: `java-sdk` is one of `curvine-libsdk`'s default features, so the default build already includes the JNI binding. To build or test one of them directly, pass the features it needs, for example `cargo test -p curvine-ufs-opendal --features opendal-s3`.

## Dependency boundaries

[`scripts/check-deps.sh`](../scripts/check-deps.sh) checks the boundaries, and [scripts/README.md](../scripts/README.md#dependency-boundaries) explains its modes. In short:

- Minimal client artifacts stay light. `curvine-client-core`, `curvine-cli` without default features, and the minimal Java and Python SDK builds must not depend on the UFS adapters, `curvine-hdfs-jni`, `curvine-storage-spdk`, `curvine-rocksdb` or `curvine-raft`.
- `curvine-fuse` without default features must not pull in SPDK, OSS-HDFS, COSN or HDFS JNI.
- The Java SDK build must not pull in `pyo3`, and the Python SDK build must not pull in `jni`.
- Production crates must not depend on `curvine-tests`.

Heavy backends therefore belong behind features of the crate that needs them, so the minimal client keeps the same dependency tree. Run `scripts/check-deps.sh --mode report` before you add a dependency between workspace crates.

## Where to start

| Change | Start here |
| --- | --- |
| Add or change an RPC message | `common/curvine-proto`, then the client side in `client/curvine-client-core` or `client/curvine-job-client`, and the handler in `curvine-master` or `curvine-worker` |
| Add a configuration option | `common/curvine-config` |
| Add a UFS backend | An `adapters/curvine-ufs-*` crate that implements `common/curvine-ufs-api`, enabled through a `client/curvine-unified-fs` feature |
| Change worker storage | `common/curvine-storage-api` and `adapters/curvine-storage-local` |
| Add a metric | `core/curvine-metrics` |
| Change CLI behavior | `curvine-cli` at the repository root |
| Change the Java or Python SDK | `sdk/curvine-sdk-core` for shared logic, then `sdk/curvine-libsdk-java` or `sdk/curvine-libsdk-python` |
