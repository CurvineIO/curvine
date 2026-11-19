# Curvine S3 Object Gateway

Curvine S3 Object Gateway is an S3-compatible object storage gateway that provides AWS S3 API compatibility for the Curvine distributed file caching system.

## Features

- **S3 API**: buckets, objects, server-side copy, batch delete and the whole multipart upload flow. See [Supported S3 operations](#supported-s3-operations)
- **Range Request Support**: HTTP Range requests with 206 Partial Content responses
- **Multipart Upload**: Support for large file chunked uploads
- **AWS Signature Authentication**: Signature V4 in the `Authorization` header or in a presigned URL, and Signature V2
- **Flexible Deployment**: Runs as its own process next to any Curvine cluster, alone or as several instances behind a load balancer
- **High Performance**: Built with Rust and Axum for async high-throughput processing
- **Enterprise Security**: Comprehensive authentication and authorization
- **Streaming Operations**: Memory-efficient handling of large objects

## Quick Start

The gateway always runs as its own `curvine-s3-gateway` process and connects to a Curvine cluster as a client. There is no master-embedded mode: the master, worker and server binaries do not start it.

Run the commands below from the Curvine installation directory, which is `build/dist` after `make build`. The scripts in the source tree's `build/bin` need the `conf/` directory that the build puts next to them.

### 1. Point the gateway at your cluster

The gateway reads the same cluster configuration file as the other services and finds the masters through `client.master_addrs`. When that list is empty, it is derived from `journal.journal_addrs`, or from `master.hostname` if that list is empty too, using `master.rpc_port`. That default works for a single-node setup. For an existing multi-node cluster, list the masters:

```toml
[client]
master_addrs = [
    { hostname = "master-1.example.com", port = 8995 },
    { hostname = "master-2.example.com", port = 8995 },
]
```

Gateway settings live in the `[s3_gateway]` section, see [Configuration File](#configuration-file). The S3 API listens on `s3_gateway.listen` (default `0.0.0.0:9900`) and the metrics server on `s3_gateway.web_port` (default `9003`). The FUSE client also uses port `9003` by default, so change one of them if both run on the same host.

### 2. Create a credential

```bash
cd build/dist

# Prints the new secret key once; store it securely
./bin/curvine-s3-gateway.sh --conf conf/curvine-cluster.toml credential generate --description local
```

Put `--conf` before `credential` so the command uses the same credential store as the gateway. `credential list`, `credential add` and `credential stats` manage existing keys.

### 3. Start and stop the gateway

```bash
# Start in the background with conf/curvine-cluster.toml; output goes to logs/s3-gateway.out
./bin/curvine-s3-gateway.sh start

# Start in the background with another configuration file and a different listen address
./bin/curvine-s3-gateway.sh start --conf /etc/curvine/curvine-cluster.toml --listen 0.0.0.0:9000

# Stop or restart the background process
./bin/curvine-s3-gateway.sh stop
./bin/curvine-s3-gateway.sh restart

# Run in the foreground, for example under a process supervisor
./bin/curvine-s3-gateway.sh --conf conf/curvine-cluster.toml --listen 0.0.0.0:9000 --region us-west-2
```

`--listen` and `--region` override the values from `[s3_gateway]`, except when they equal the built-in defaults (`0.0.0.0:9900`, `us-east-1`). The binary treats those values as "not set", so `--listen 0.0.0.0:9900` falls back to `s3_gateway.listen`.

### 4. Run several gateways

`worker.enable_s3_gateway` (default `false`) is read by the gateway, not by the worker. It controls where multipart upload parts are staged:

- `false`: parts go to the local `s3_gateway.put_temp_dir`, so every part and the final `CompleteMultipartUpload` of one upload must reach the same gateway instance.
- `true`: parts go to Curvine under `/.s3temp`, so any gateway instance can finish the upload.

For both backends, signed `aws-chunked` records over 16 MiB are buffered locally until their signature is verified. Use a private directory in a trusted location on a filesystem sized for `signed_chunk_spool_max_bytes` per gateway process, plus room for any multipart staging on the same volume. When a record cannot reserve its full size, the gateway returns `503 SlowDown` without buffering it. Keep the budget at least 5 GiB to admit any legal single record.

Credentials are kept either in a local file (`~/.curvine/credentials.jsonl` by default) or in Curvine at `/system/auth/credentials.jsonl`, and the two switches differ:

- The running gateway uses the Curvine store only when `s3_gateway.enable_distributed_auth = true`.
- The `credential` command uses the Curvine store when either `s3_gateway.enable_distributed_auth` or `worker.enable_s3_gateway` is true.

Set both flags or neither, so the command writes keys where the gateway reads them. The two stores are not migrated into each other: after you switch, run `credential generate` again, because keys created in the local file stay there.

When several gateways sit behind a load balancer, set both:

```toml
[worker]
enable_s3_gateway = true

[s3_gateway]
enable_distributed_auth = true
```

### 5. Building from Source

```bash
# Build the project
cargo build --release --bin curvine-s3-gateway

# Run tests
cargo test -p curvine-s3-gateway

# Generate documentation
cargo doc --open
```

## Configuration

### Environment Variables

- `AWS_ACCESS_KEY_ID` or `CURVINE_ACCESS_KEY`: S3 access key, used as a fallback
  when the credential store is empty.
- `AWS_SECRET_ACCESS_KEY` or `CURVINE_SECRET_KEY`: matching S3 secret key.
- `RUST_LOG` is not read: the log level is set in the configuration file, see
  [Logging Configuration](#logging-configuration).

There are no built-in default credentials: create one with
`curvine-s3-gateway credential generate` or set the
`s3_gateway.access_key`/`s3_gateway.secret_key` configuration pair.

### Configuration File

S3 gateway supports configuration via Curvine cluster configuration file:

```toml
[s3_gateway]
# Listen address and port
listen = "0.0.0.0:9900"
# S3 region identifier
region = "us-east-1"
# Staging directory for multipart upload parts
put_temp_dir = "/tmp/curvine-temp"
# Absolute, gateway-owned directory (created with mode 0700 if absent)
signed_chunk_spool_dir = "/tmp/curvine-signed-spool"
# Shared signed-record buffer budget per gateway process, in bytes (10 GiB)
signed_chunk_spool_max_bytes = 10737418240
# Multipart part sizes kept in memory instead of written to the staging file
put_memory_buffer_threshold = 1048576
# Maximum buffer size used for multipart part staging
put_max_memory_buffer = 16777216
# Optional fixed credential pair (overrides the credential store)
# access_key = "AKIA..."
# secret_key = "..."
# Credential store file (default: ~/.curvine/credentials.jsonl)
# credentials_path = "/etc/curvine/s3-credentials.jsonl"
# Share credentials through Curvine instead of the local file
enable_distributed_auth = false
# Credential cache refresh interval in seconds
cache_refresh_interval_secs = 30
# GET read chunk size in MiB
get_chunk_size_mb = 1.0
# Gateway metrics/health web port
web_port = 9003
```

### Command Line Arguments

- `--conf`: Curvine cluster configuration file path (optional, default: etc/curvine-cluster.toml)
- `--listen`: Listen address in host:port format (optional, overrides config file)
- `--region`: S3 region identifier (optional, overrides config file)

### Configuration Priority

1. Command line arguments (highest priority)
2. Configuration file values
3. Default values (lowest priority)

## Usage Examples

### AWS CLI

```bash
# Configure AWS CLI to use Curvine S3 gateway
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_DEFAULT_REGION=us-east-1

# Create a bucket
aws s3 mb s3://mybucket --endpoint-url http://localhost:9900

# Upload a file
aws s3 cp /path/to/file.txt s3://mybucket/ --endpoint-url http://localhost:9900

# Download a file
aws s3 cp s3://mybucket/file.txt /tmp/ --endpoint-url http://localhost:9900

# List bucket contents
aws s3 ls s3://mybucket/ --endpoint-url http://localhost:9900

# Delete a file
aws s3 rm s3://mybucket/file.txt --endpoint-url http://localhost:9900

# Delete a bucket
aws s3 rb s3://mybucket --endpoint-url http://localhost:9900

# Sync directories
aws s3 sync /local/directory s3://mybucket/prefix/ --endpoint-url http://localhost:9900
```

### MinIO Client (mc)

```bash
# Configure MinIO client
mc alias set curvine http://localhost:9900 <your-access-key> <your-secret-key>

# Create a bucket
mc mb curvine/mybucket

# Upload a file
mc cp /path/to/file.txt curvine/mybucket/

# Download a file
mc cp curvine/mybucket/file.txt /tmp/

# List contents
mc ls curvine/mybucket/

# Delete a file
mc rm curvine/mybucket/file.txt

# Delete a bucket
mc rb curvine/mybucket

# Mirror directories
mc mirror /local/directory curvine/mybucket/prefix/
```

### Range Requests (Partial Downloads)

```bash
# Get first 1000 bytes of a file
curl -H "Range: bytes=0-999" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     http://localhost:9900/mybucket/file.txt

# Get bytes 1000-1999 of a file
curl -H "Range: bytes=1000-1999" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     http://localhost:9900/mybucket/file.txt

# Get last 1000 bytes of a file
curl -H "Range: bytes=-1000" \
     -H "Authorization: AWS4-HMAC-SHA256 ..." \
     http://localhost:9900/mybucket/file.txt
```

### Multipart Upload

```bash
# Create multipart upload session
aws s3api create-multipart-upload \
    --bucket mybucket \
    --key large-file.zip \
    --endpoint-url http://localhost:9900

# Upload a part
aws s3api upload-part \
    --bucket mybucket \
    --key large-file.zip \
    --part-number 1 \
    --upload-id <upload-id> \
    --body part1.bin \
    --endpoint-url http://localhost:9900

# Complete multipart upload
aws s3api complete-multipart-upload \
    --bucket mybucket \
    --key large-file.zip \
    --upload-id <upload-id> \
    --multipart-upload file://parts.json \
    --endpoint-url http://localhost:9900

# Abort multipart upload
aws s3api abort-multipart-upload \
    --bucket mybucket \
    --key large-file.zip \
    --upload-id <upload-id> \
    --endpoint-url http://localhost:9900
```

### Python boto3 Example

```python
import boto3
from botocore.config import Config

# Configure S3 client
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9900',
    aws_access_key_id='<your-access-key>',
    aws_secret_access_key='<your-secret-key>',
    region_name='us-east-1',
    config=Config(retries={'max_attempts': 3})
)

# Create bucket
s3_client.create_bucket(Bucket='my-python-bucket')

# Upload file
with open('local-file.txt', 'rb') as f:
    s3_client.put_object(Bucket='my-python-bucket', Key='remote-file.txt', Body=f)

# Download file
response = s3_client.get_object(Bucket='my-python-bucket', Key='remote-file.txt')
data = response['Body'].read()

# List objects
response = s3_client.list_objects_v2(Bucket='my-python-bucket')
for obj in response.get('Contents', []):
    print(f"Object: {obj['Key']}, Size: {obj['Size']}")
```

## Supported S3 operations

Requests are routed in [`src/http/router.rs`](src/http/router.rs). Operations that are not listed are not implemented. A request that names an S3 sub-resource the gateway does not serve, such as `?tagging`, `?acl`, `?versioning`, `?cors`, `?policy` or `?object-lock`, gets `501` with the error code `NotImplemented` and a message such as `tagging is not supported`, whatever its method. The full list is `UNSUPPORTED_SUB_RESOURCES` in the router. The check runs after authentication, so a request without valid credentials still gets `403`. With Signature V2 the gateway signs only the sub-resources in `INCLUDED_QUERY_PARAMS` ([`src/auth/sig_v2.rs`](src/auth/sig_v2.rs)), so a V2 client that also signs another name, as botocore does for `?object-lock` and `?select`, gets `403 SignatureDoesNotMatch` instead of `501`.

| Operation | Request |
| --- | --- |
| ListBuckets | `GET /` |
| CreateBucket | `PUT /{bucket}` |
| HeadBucket | `HEAD /{bucket}` |
| DeleteBucket | `DELETE /{bucket}` |
| GetBucketLocation | `GET /{bucket}?location` |
| ListObjectsV2 | `GET /{bucket}?list-type=2` |
| ListObjects (V1) | `GET /{bucket}` without `list-type`: with V1 listing parameters (`prefix`, `delimiter`, `marker`, `max-keys`, `encoding-type`), with no query, or with a query the router does not recognise |
| ListObjectVersions | `GET /{bucket}?versions` |
| PutObject | `PUT /{bucket}/{key}` |
| CopyObject | `PUT /{bucket}/{key}` with an `x-amz-copy-source` header |
| GetObject | `GET /{bucket}/{key}`, with `Range` and `response-*` overrides |
| HeadObject | `HEAD /{bucket}/{key}`, with `response-*` overrides |
| DeleteObject | `DELETE /{bucket}/{key}` |
| DeleteObjects | `POST /{bucket}?delete` |
| CreateMultipartUpload | `POST /{bucket}/{key}?uploads` |
| UploadPart | `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` |
| UploadPartCopy | `PUT /{bucket}/{key}?partNumber={n}&uploadId={id}` with an `x-amz-copy-source` header |
| CompleteMultipartUpload | `POST /{bucket}/{key}?uploadId={id}` |
| AbortMultipartUpload | `DELETE /{bucket}/{key}?uploadId={id}` |
| ListParts | `GET /{bucket}/{key}?uploadId={id}` |
| ListMultipartUploads | `GET /{bucket}?uploads` |

Besides the S3 API, the gateway serves `GET /healthz`, see [Health Check](#health-check).

## Path Mapping

Curvine S3 gateway maps S3 paths to Curvine filesystem paths:

- **Buckets**: `/buckets/{bucket-name}` → `/{bucket-name}`
- **Objects**: `/buckets/{bucket-name}/{object-path}` → `/{bucket-name}/{object-path}`

Examples:
- S3 bucket `my-bucket` → Curvine path `/my-bucket`
- S3 object `my-bucket/folder/file.txt` → Curvine path `/my-bucket/folder/file.txt`


## Performance and Scalability

### Performance Characteristics

- **High Throughput**: Async processing with Tokio runtime
- **Memory Efficient**: Streaming operations for large objects
- **Low Latency**: Average response time < 50ms
- **High Concurrency**: Supports 10,000+ concurrent connections
- **Zero-Copy Operations**: Direct streaming from storage to network

### Scaling Guidelines

- **Horizontal Scaling**: Deploy multiple gateway instances behind a load balancer
- **Resource Allocation**: 4GB RAM and 2 CPU cores for moderate workloads
- **Network**: 1Gbps network interface recommended for high throughput
- **Storage**: SSD storage for optimal I/O performance

## Monitoring and Logging

### Health Check

```bash
# Check gateway health
curl http://localhost:9900/healthz
```

The gateway refuses to start when it finds no credential: none in `[s3_gateway]`, none in the credential store and none in the environment. Create one first, see [Create a credential](#2-create-a-credential), or set `CURVINE_S3_ALLOW_EMPTY_CREDENTIALS=1` to start it anyway.

On a running gateway, `/healthz` on the S3 port needs no signature. It answers `200 ok`, or `503 no credentials configured` while the credential store is empty, for example after a start with `CURVINE_S3_ALLOW_EMPTY_CREDENTIALS=1` or after every credential was removed. A credential added while the gateway runs is picked up within `s3_gateway.cache_refresh_interval_secs` (default `30`). The web port (`s3_gateway.web_port`, default `9003`) also serves `/healthz`, which always answers `ok` without checking credentials, and Prometheus metrics at `/metrics`.

### Logging Configuration

The gateway logs with the `log` entry of the `[master]` section in the file passed with `--conf`. `RUST_LOG` has no effect, because this logger is installed before the gateway starts. For debug logs on the console:

```toml
[master]
log = { level = "debug", log_dir = "stdout" }
```

`level` is one of `trace`, `debug`, `info` (the default), `warn` and `error`. `log_dir` is a directory for the log files (default `logs`), or `stdout` or `stderr`. A master that reads the same file gets the same settings, so give the gateway its own copy of the file if that matters.

### Metrics

The gateway provides detailed logging for monitoring:
- Request/response times
- Error rates and types
- Upload/download throughput
- Authentication failures
- File system operations

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Change listen port
   ./curvine-s3-gateway --listen 0.0.0.0:9901
   ```

2. **Authentication Failures**
   ```bash
   # Check environment variables
   echo $AWS_ACCESS_KEY_ID
   echo $AWS_SECRET_ACCESS_KEY
   
   # More detail in the gateway log: set level = "debug" in the
   # [master] log entry (see Logging Configuration)
   ```

3. **Configuration File Errors**
   ```bash
   # Validate configuration file
   ./curvine-s3-gateway --conf /path/to/config.toml
   
   # Use default configuration
   ./curvine-s3-gateway --conf ""
   ```

4. **File System Errors**
   ```bash
   # Check file system permissions
   ls -la /path/to/storage
   
   # Verify disk space
   df -h /path/to/storage
   ```

5. **Network Connectivity**
   ```bash
   # Test connectivity
   curl -v http://localhost:9900/healthz
   
   # Check if port is listening
   netstat -tlnp | grep 9900
   ```

### Debug Mode

Set the level in the file passed with `--conf` (see [Logging Configuration](#logging-configuration)), then start the gateway with that file:

```toml
[master]
log = { level = "debug", log_dir = "stdout" }  # "trace" is more verbose
```

```bash
./curvine-s3-gateway --conf /path/to/curvine-cluster.toml
```

### Performance Tuning

```bash
# Increase file descriptor limits
ulimit -n 65536

# Enable TCP keep-alive
echo 'net.ipv4.tcp_keepalive_time = 600' >> /etc/sysctl.conf

# Optimize network buffer sizes
echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 16777216' >> /etc/sysctl.conf
```

## Development

### Building from Source

```bash
# Clone repository
git clone https://github.com/CurvineIO/curvine.git
cd curvine

# Build the gateway
cargo build --release --bin curvine-s3-gateway

# Run tests
cargo test -p curvine-s3-gateway

# Run against the cluster in etc/curvine-cluster.toml, the default --conf
# (add -- --conf <file> for another one); create a credential first, see
# "Create a credential" above
cargo run --bin curvine-s3-gateway
```

### Running Tests

```bash
# Unit tests
cargo test -p curvine-s3-gateway --lib

# Unit and integration tests (every file in tests/)
cargo test -p curvine-s3-gateway --tests

# End-to-end suites against a running gateway on http://localhost:9900, with a
# gateway credential in AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
bash curvine-s3-gateway/src/test/s3_test.sh     # AWS CLI; ENDPOINT overrides the URL
python3 curvine-s3-gateway/src/test/s3_test.py  # boto3
```

### Documentation

- [S3 gateway user guide](https://curvineio.github.io/docs/User-Manuals/Access/s3-gateway/) on the Curvine documentation site
- Request routing: [`src/http/router.rs`](src/http/router.rs)
- Signature V2 and V4 authentication and the credential stores: [`src/auth/`](src/auth/)
- S3 handlers and XML responses: [`src/s3/`](src/s3/)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for the development setup, commit conventions and pull request checklist.

## Support

- **Questions and ideas**: [GitHub Discussions](https://github.com/CurvineIO/curvine/discussions)
- **Bugs and feature requests**: [GitHub Issues](https://github.com/CurvineIO/curvine/issues)
- **Chat**: Slack or Discord, linked from the [Community section](../README.md#-community) of the main README