# Curvine libsdk

Rust **`cdylib`** with **Java (JNI)** and **Python (PyO3)**. Crate path: **`curvine-libsdk/`** under the workspace root (`curvine-vllm`).

| Path | Role |
|------|------|
| `Cargo.toml`, `pyproject.toml` | Rust + wheel metadata |
| `java/` | Hadoop `FileSystem`, JUnit |
| `python/` | `curvinefs`, native module `curvine_libsdk`, tests |

---

## Python SDK (recommended)

**1. Build** — from workspace root:

```bash
make build
```

Runs `build/build.sh`: creates **`build/.venv-python-sdk`** (gitignored), installs **`build/requirements-python-sdk.txt`** (e.g. maturin), regenerates `*_pb2.py`, produces the wheel.  
Needs **`python3`** with **`venv`**, and **`protoc`**. Override venv dir: **`CURVINE_PYTHON_SDK_VENV`**.  
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

Runtime deps (**`protobuf`**, **`fsspec`**) come from **`pyproject.toml`**.

**4. Smoke / integration tests** — cluster + default `etc/curvine-cluster.toml`; optional **`CURVINE_CONF_FILE`**, **`CURVINE_TEST_CV_PATH`**. Example after install:

```bash
python3 curvine-libsdk/python/test/curvineFileSystemTest.py
```

---

## Java SDK

JDK **8**, Maven **≥ 3.8.1**. From workspace root, **`make build`** (with **`java`** in the package set) builds the JNI native copy and **`curvine-hadoop-*.jar`** under **`build/dist/lib/`**. JNI library name must match **`CurvineNative.getLibraryName()`** (see `java/native/`).

---

## Local dev (without `make`)

From **`curvine-libsdk/`**: own venv, **`pip install maturin`**, **`maturin develop --release`**, **`protoc -I ../curvine-common/proto --python_out=python ../curvine-common/proto/*.proto`**, **`export PYTHONPATH=python`**.
