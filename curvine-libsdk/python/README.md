# Curvine Python SDK

Python SDK for the Curvine distributed file system, providing a simple and efficient interface for file operations.

## Installation

`curvine_libsdk` is not published to a package index yet. Build the wheel from this repository and install it locally.

### Build and install the wheel (recommended)

Run from the repository root:

```bash
make build ARGS='-p python'    # Python SDK only; plain `make build` builds every package
python3 -m pip install build/dist/lib/curvine_libsdk-*.whl
```

`make build` runs `build/build.sh`, which:

- creates an isolated build venv at `build/.venv-python-sdk` (override with `CURVINE_PYTHON_SDK_VENV`),
- generates the protobuf stubs into `curvine_libsdk/_proto/` with `protoc`,
- builds the wheel with maturin into `build/dist/lib/curvine_libsdk-*-cp38-abi3-*.whl`.

You need `python3` with `venv`, `protoc`, and the Rust toolchain. maturin is installed through `cargo` if it is not on `PATH`.

To build all packages except the Python SDK, run `make build ARGS='--skip-python-sdk'`.

For `uv pip install`, wheel platform tags, and PyPI-style (`manylinux`) builds, see [Python SDK (recommended)](../README.md#python-sdk-recommended).

### Local development with maturin (SDK contributors)

The maturin project is `crates/sdk/curvine-libsdk-python/`, not this directory. Its `pyproject.toml` points back here as the Python source.

```bash
# from the repository root; build/.venv-python-sdk is the same git-ignored venv that build/build.sh uses
python3 -m venv build/.venv-python-sdk && . build/.venv-python-sdk/bin/activate
pip install maturin
cd crates/sdk/curvine-libsdk-python
maturin develop --release --no-default-features --features extension-module

# Generate the protobuf stubs and switch them to relative imports (same steps as build/build.sh)
PROTO_DIR=../../common/curvine-proto/proto
PROTO_PKG=../../../curvine-libsdk/python/curvine_libsdk/_proto
protoc -I"$PROTO_DIR" --python_out="$PROTO_PKG" "$PROTO_DIR"/*.proto
for f in "$PROTO_PKG"/*_pb2.py; do
  sed -i.bak -E 's/^import ([A-Za-z0-9_]+_pb2)( as .*)$/from . import \1\2/' "$f" && rm -f "$f.bak"
done

export PYTHONPATH=../../../curvine-libsdk/python
```

This mirrors [Local dev (without make)](../README.md#local-dev-without-make).

## Quick Start

### Using CurvineClient

```python
from curvinefs import CurvineClient

# Initialize the client
client = CurvineClient(
    config_path="/path/to/curvine.toml",
    write_chunk_num=4,
    write_chunk_size=1024 * 1024  # 1MB
)

# Create a file and write data
writer = client.create("/path/to/file.txt", overwrite=True)
writer.write(b"Hello, Curvine!")
writer.close()

# Read a file
reader = client.open("/path/to/file.txt")
data = reader.read(offset=0, length=100)
print(data)
reader.close()

# List directory contents
files = client.ls("/path/to/directory")
for file in files:
    print(file["name"], file["size"])

# Check if path exists
if client.get_file_status("/path/to/file.txt"):
    print("File exists!")

# Close the client when done
client.close()
```

### Using CurvineFileSystem (fsspec compatible)

```python
from curvinefs import CurvineFileSystem

# Initialize the file system
fs = CurvineFileSystem(
    config_path="/path/to/curvine.toml",
    write_chunk_size=1024 * 1024,
    write_chunk_num=4
)

# Write a file
with fs.open("/path/to/file.txt", "wb") as f:
    f.write(b"Hello, Curvine!")

# Read a file
data = fs.cat("/path/to/file.txt")
print(data)

# List files
files = fs.ls("/path/to/directory", detail=True)
for file in files:
    print(f"{file['name']}: {file['size']} bytes")

# Check if path exists
if fs.exists("/path/to/file.txt"):
    print("File exists!")

# Close the file system
fs.close()
```

## API Reference

### CurvineClient

Main client for interacting with the Curvine file system.

#### Methods

| Method | Description |
|--------|-------------|
| `get_file_status(path)` | Get status information for a file or directory |
| `get_master_info()` | Get cluster information |
| `mkdir(path, create_parents)` | Create a directory |
| `rm(path, recursive=False)` | Delete a file or directory |
| `rename(src_path, dest_path)` | Rename a file or directory |
| `mv(src_path, dest_path)` | Move a file or directory (alias for rename) |
| `list_status(path)` | List status of entries in a directory |
| `ls(path, detail=True)` | List files in a directory |
| `open(path)` | Open a file for reading |
| `read_range(path, offset, length)` | Read a range of bytes |
| `head(path, size)` | Read first N bytes |
| `tail(path, size)` | Read last N bytes |
| `create(path, overwrite)` | Create a file for writing |
| `write_string(path, data)` | Write a string to a file |
| `append(path)` | Open a file for appending |
| `touch(path, truncate=True)` | Create an empty file |
| `copy(src_path, dest_path)` | Copy a file or directory |
| `download(remote_path, local_path)` | Download a file |
| `upload(local_path, remote_path)` | Upload a file |
| `close()` | Close the connection |

### CurvineFileSystem

fsspec-compatible file system interface.

#### Methods

Implements all standard fsspec `AbstractFileSystem` methods including:

- `exists(path)`, `isdir(path)`, `isfile(path)`
- `cat(path)`, `cat_file(path, start, end)`
- `ls(path, detail)`
- `mkdir(path, create_parents)`, `rm(path, recursive)`
- `copy(src, dst)`, `move(src, dst)`
- `head(path, size)`, `tail(path, size)`

### CurvineReader

File reader for reading from Curvine.

#### Methods

| Method | Description |
|--------|-------------|
| `read(offset, length)` | Read data from the file |
| `seek(pos)` | Seek to a position in the file |
| `close()` | Close the reader |

### CurvineWriter

File writer for writing to Curvine with buffering.

#### Methods

| Method | Description |
|--------|-------------|
| `write(bytes_data)` | Write bytes to the file |
| `flush()` | Flush buffered data |
| `close()` | Close the writer |

## Requirements

- Python 3.6+
- protobuf>=3.19,<6
- fsspec

## Testing

### Unit tests (no cluster needed)

`test/test_curvine_sdk.py` mocks the native bindings, so it does not need a running cluster. It puts `curvine-libsdk/python` first on `sys.path` and imports the in-tree `curvine_libsdk` package. Set up the [maturin development flow](#local-development-with-maturin-sdk-contributors) first, so the native module and the protobuf stubs exist in this directory. Installing the wheel alone is not enough for these tests.

```bash
# from the repository root, in the venv used for `maturin develop`
cd curvine-libsdk/python
python3 -m unittest test.test_curvine_sdk
# or
pytest test/test_curvine_sdk.py -v
```

### Integration test (needs a running cluster)

`test/curvineFileSystemTest.py` runs against a live Curvine cluster. Start one first, for example with the [Quick Start](../../README.md#-quick-start), and install the SDK with either flow above.

```bash
# from the repository root
python3 curvine-libsdk/python/test/curvineFileSystemTest.py
```

The test reads the client config from `CURVINE_CONF_FILE` (default: `etc/curvine-cluster.toml`) and uses `CURVINE_TEST_CV_PATH` as its working path (default: `file:///fs_test`).

## Contributing

Please read our contributing guidelines before submitting pull requests.

## License

See LICENSE file for details.