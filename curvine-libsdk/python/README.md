# Curvine Python SDK

Python SDK for the Curvine distributed file system, providing a simple and efficient interface for file operations.

## Installation

```bash
pip install curvine_libsdk
```

Or build from source:

```bash
cd curvine-libsdk/python
maturin develop
```

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

Run the unit tests:

```bash
python -m unittest test.test_curvine_sdk
```

Or with pytest:

```bash
pytest test/test_curvine_sdk.py -v
```

## Contributing

Please read our contributing guidelines before submitting pull requests.

## License

See LICENSE file for details.