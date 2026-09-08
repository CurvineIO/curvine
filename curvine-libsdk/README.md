# Curvine libsdk

Rust SDK facade for Java (JNI), Python (PyO3), and Rust consumers. The public compatibility package remains **`curvine-libsdk/`**, while implementation is split into FFI-neutral core and language binding crates.

| Path | Role |
|------|------|
| `Cargo.toml` | Compatibility facade and release-profile features |
| `../crates/sdk/curvine-sdk-core/` | FFI-neutral session, filesystem, reader/writer, master, and job helpers |
| `../crates/sdk/curvine-libsdk-java/` | Java JNI ABI and Java-specific conversion helpers |
| `../crates/sdk/curvine-libsdk-python/` | Python PyO3 ABI and wheel metadata |
| `java/` | Hadoop `FileSystem`, JUnit |
| `python/` | `curvinefs`, package `curvine_libsdk` (re-exports PyO3 module `curvine_libsdk._native`), tests |

---

## UFS Cargo features

`curvine-libsdk` forwards optional UFS backends through `curvine-sdk-core` and the selected language binding crate. Default features remain **`java-sdk` only** and build a minimal Java SDK; Python is minimal with `--no-default-features --features python-sdk`. No UFS backend is enabled unless a backend feature is requested explicitly.

| Feature | Forwards to |
|---------|-------------|
| `java-sdk-minimal` | `java-sdk` without UFS backends |
| `python-sdk-minimal` | `python-sdk` without UFS backends |
| `opendal-s3` | `curvine-sdk-core/opendal-s3` and selected binding crate |
| `opendal-oss` | `curvine-sdk-core/opendal-oss` and selected binding crate |
| `opendal-gcs` | `curvine-sdk-core/opendal-gcs` and selected binding crate |
| `opendal-azblob` | `curvine-sdk-core/opendal-azblob` and selected binding crate |
| `opendal-cos` | `curvine-sdk-core/opendal-cos` and selected binding crate |
| `opendal-webhdfs` | `curvine-sdk-core/opendal-webhdfs` and selected binding crate |
| `opendal-hdfs` | `curvine-sdk-core/opendal-hdfs` and selected binding crate |
| `opendal-hdfs-native` | `curvine-sdk-core/opendal-hdfs-native` and selected binding crate |
| `internal-oss-hdfs-jindo`, `oss-hdfs` | Explicit internal OSS-HDFS/Jindo profile; needs `JINDOSDK_HOME` |

**Rust consumers**

```toml
curvine-libsdk = { path = "...", features = ["java-sdk", "oss-hdfs"] }
# or: features = ["rust-sdk", "opendal-oss"]
```

```bash
cargo build -p curvine-libsdk --no-default-features --features "java-sdk,oss-hdfs"
```

### Client context (audit correlation)

Client metadata RPCs can carry a thread-local business context that Master appends
to its audit log. Set it once before a job's FS calls and clear it when the job ends:

```rust
use curvine_libsdk::client_context::ClientContext;

ClientContext::set_current([
    ("app_id", "application_1720000000000_0001"),
    ("scenario", "spark"),
    ("user", "etl"),
]);
// ... file operations ...
ClientContext::clear_current();
```

From Java:

```java
Map<String, String> context = new HashMap<>();
context.put("app_id", "application_1720000000000_0001");
context.put("scenario", "spark");
CurvineFileSystem.setClientContext(context);
// ... file operations ...
CurvineFileSystem.clearClientContext();
```

The context is thread-local. For async Rust callers, set it on the thread that
polls the `FsClient` future; on a multi-threaded Tokio runtime this usually
means setting it inside the task, or using `block_on` on the calling thread.

Recommended keys are `app_id`, `scenario`, `user`, and `job_id`. Master logs the
context as `context=app_id=... scenario=...` while keeping the existing
`cmd/ok/ip/src/dst/usedUs/err` fields unchanged. Context is bounded to 16 keys,
keys to 64 bytes, and values to 256 bytes; sensitive key names such as
`password`, `token`, `secret`, and `authorization` are dropped before logging.

**Java / Python packaging (`make` / `build/build.sh`)**

`--ufs` flags are passed through into SDK artifacts only when present. The broader distribution still defaults server/client UFS to `opendal-s3`, but Java/Python SDK artifacts stay minimal unless `--ufs` is provided:

```bash
# Minimal Java JNI .so
make build ARGS='--package java'

# Java JNI .so with OSS-HDFS/Jindo only
make build ARGS='--package java --ufs oss-hdfs'

# Python wheel with an extra OpenDAL backend
make build ARGS='--package python --ufs opendal-oss'
```

`oss-hdfs` requires JindoSDK at build/link time (`JINDOSDK_HOME`, default `/opt/jindosdk`). For a Java SDK build with `--ufs oss-hdfs`, the build packages the architecture-matched Jindo runtime in the JAR as `libjindosdk_c_linux_{x86,aarch}_64.so.6`. The Java loader extracts it as `libjindosdk_c.so.6` and calls `System.load` before loading Curvine JNI. Default Java artifacts do not package Jindo; missing Jindo resources are intentionally ignored at runtime.

---

## Python SDK (recommended)

**1. Build** — from workspace root:

```bash
make build
```

Runs `build/build.sh`: creates **`build/.venv-python-sdk`** (gitignored), installs **`build/requirements-python-sdk.txt`** (e.g. maturin), runs **`protoc`** into **`python/curvine_libsdk/_proto/`** (namespaced protobuf stubs), produces the wheel.  
Needs **`python3`** with **`venv`**, and **`protoc`** (`check-env` expects Python **≥ 3.6**). Override venv dir: **`CURVINE_PYTHON_SDK_VENV`**. **maturin** is installed via **cargo** when missing from PATH.  
Skip Python SDK: **`make build ARGS='--skip-python-sdk'`**.

**2. Artifact** — `build/dist/lib/curvine_libsdk-*-cp38-abi3-*.whl` (same dir may contain legacy `libcurvine_libsdk_python_*`).

**3. Install & use**

Wheel tag defaults to **`linux_<arch>`** (not **`manylinux_2_34`**+) so **`uv pip install`** / **pip** accept it on typical internal Linux hosts.

```bash
# If python has pip:
python3 -m pip install build/dist/lib/curvine_libsdk-*.whl

# Often easier with uv (works when the venv has no bundled pip):
uv pip install build/dist/lib/curvine_libsdk-*.whl
```

No pip in the venv: **`python3 -m ensurepip --upgrade`** once, or keep using **`uv pip`**.

PyPI-style strict manylinux: rebuild with **`CURVINE_MATURIN_COMPATIBILITY=pypi CURVINE_MATURIN_AUDITWHEEL=repair`** (see **`build/build.sh`**).

Then (no `PYTHONPATH` needed):

```python
from curvinefs.curvineFileSystem import CurvineFileSystem
```

Runtime deps (**`protobuf`**, **`fsspec`**) come from **`crates/sdk/curvine-libsdk-python/pyproject.toml`**.

**4. Smoke / integration tests** — cluster + default `etc/curvine-cluster.toml`; optional **`CURVINE_CONF_FILE`**, **`CURVINE_TEST_CV_PATH`**. Example after install:

```bash
python3 curvine-libsdk/python/test/curvineFileSystemTest.py
```

---

## Java SDK

JDK **8**, Maven **≥ 3.8.1**. From workspace root, **`make build`** (with **`java`** in the package set) builds the JNI native copy and **`curvine-hadoop-*.jar`** under **`build/dist/lib/`**. On Linux, `CurvineNative` resolves native libraries using an ordered candidate list: the detected `<distribution><major>` name, then `linux`, `centos7`, `amzn2`, `alinux3`, and `rocky9` for the detected architecture, followed by the generic `x86` library name where applicable. Package the names produced by the native build under `java/native/` and ensure **`java.library.path`** includes the directory containing them (`bin/dfs` wrappers often set this). This fallback order lets one jar run on distributions without a dedicated native filename, provided the fallback `.so` is present.

The canonical HDFS backend selector is `hdfs.provider=native` (or `jvm` for the JVM-backed OpenDAL HDFS provider). The legacy boolean `hdfs.native=true` remains supported when `hdfs.provider` is absent.

**`cannot allocate memory in static TLS block`** (large JNI `.so` + glibc): load from a real **`build/dist/lib`** path first (`CurvineNative` scans `java.library.path` entries); if it still fails, try preloading **`LD_PRELOAD`** with the same **`libcurvine_libsdk_<os>_<arch>_64.so`**, or use a host/OS image validated for Curvine JNI.

### Transfer load routing

When the cluster has Transfer enabled, configure the Java client with the same switch and the
reachable Transfer service addresses. `CurvineTransferClient` provides load/export submission,
status, cancellation, retry, and wait operations while routing requests to Transfer instead of
the legacy Master Job API:

```java
Configuration conf = new Configuration();
conf.set("fs.cv.master_addrs", "master-0:8995,master-1:8995");
conf.set("fs.cv.transfer.enabled", "true");
conf.set("fs.cv.transfer.endpoints", "transfer-0:9010,transfer-1:9010");

try (CurvineTransferClient client = CurvineTransferClient.from(conf)) {
    LoadJobResult job = client.submitLoad(LoadJobRequest.builder()
            .sourcePath("s3://bucket/model/v1")
            .targetPath("/bucket/model/v1")
            .build());
    LoadJobStatus status = client.getJobStatus(job.getJobId());
    LoadJobResult export = client.submitExport(ExportJobRequest.builder()
            .sourcePath("/bucket/model/v1")
            .build());
}
```

`submitLoad(request, true)` implements the CLI `cv load --force` behavior. Export targets are
derived from the existing mount mapping; export source paths must be Curvine paths. The existing
`CurvineLoadClient` remains a compatibility facade for load-only callers.

`fs.cv.transfer.endpoints` is a comma-separated list and is independent from Master addresses.
It must point to the externally reachable Transfer service endpoint; the client does not infer it
from Master nodes. With `fs.cv.transfer.enabled=false` (the default), the same API remains
compatible with the legacy Master LoadJob service.

---

## Local dev (without `make`)

From **`crates/sdk/curvine-libsdk-python/`**: own venv, **`pip install maturin`**, **`maturin develop --release --no-default-features --features extension-module`**, then run **`protoc`** into **`../../../curvine-libsdk/python/curvine_libsdk/_proto/`** and apply the same **`sed`** relative-import fix as **`build/build.sh`**, **`export PYTHONPATH=../../../curvine-libsdk/python`**.
