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

`oss-hdfs` requires JindoSDK at build/link time (`JINDOSDK_HOME`, default `/opt/jindosdk`). Do not enable it for default public artifacts unless that native SDK is available on the build host.

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

JDK **8**, Maven **≥ 3.8.1**. From workspace root, **`make build`** (with **`java`** in the package set) builds the JNI native copy and **`curvine-hadoop-*.jar`** under **`build/dist/lib/`**. JNI library name must match **`CurvineNative.getLibraryName()`** (see `java/native/`). Put the matching **`.so`** in **`build/dist/lib/`** next to the JAR and ensure **`java.library.path`** includes that directory (`bin/dfs` wrappers often set this).

**`cannot allocate memory in static TLS block`** (large JNI `.so` + glibc): load from a real **`build/dist/lib`** path first (`CurvineNative` scans `java.library.path` entries); if it still fails, try preloading **`LD_PRELOAD`** with the same **`libcurvine_libsdk_<os>_<arch>_64.so`**, or use a host/OS image validated for Curvine JNI.

### Transfer load routing

When the cluster has Transfer enabled, configure the Java client with the same switch and the
reachable Transfer service addresses. `CurvineLoadClient` then keeps its existing submit/status/
cancel API while routing those requests to Transfer instead of the legacy Master Job API:

```java
Configuration conf = new Configuration();
conf.set("fs.cv.master_addrs", "master-0:8995,master-1:8995");
conf.set("fs.cv.transfer.enabled", "true");
conf.set("fs.cv.transfer.endpoints", "transfer-0:9010,transfer-1:9010");

try (CurvineLoadClient client = CurvineLoadClient.from(conf)) {
    LoadJobResult job = client.submitLoad(LoadJobRequest.builder()
            .sourcePath("s3://bucket/model/v1")
            .targetPath("/bucket/model/v1")
            .build());
    LoadJobStatus status = client.getJobStatus(job.getJobId());
}
```

`fs.cv.transfer.endpoints` is a comma-separated list and is independent from Master addresses.
It must point to the externally reachable Transfer service endpoint; the client does not infer it
from Master nodes. With `fs.cv.transfer.enabled=false` (the default), the same API remains
compatible with the legacy Master LoadJob service.

---

## Local dev (without `make`)

From **`crates/sdk/curvine-libsdk-python/`**: own venv, **`pip install maturin`**, **`maturin develop --release --no-default-features --features extension-module`**, then run **`protoc`** into **`../../../curvine-libsdk/python/curvine_libsdk/_proto/`** and apply the same **`sed`** relative-import fix as **`build/build.sh`**, **`export PYTHONPATH=../../../curvine-libsdk/python`**.
