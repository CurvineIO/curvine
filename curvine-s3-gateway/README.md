# Curvine S3 Object Gateway

Curvine S3 Object Gateway is an S3-compatible object storage gateway that provides AWS S3 API compatibility for the Curvine distributed file caching system.

## Features

- **Complete S3 API Support**: PutObject, GetObject, HeadObject, DeleteObject, CreateBucket, HeadBucket, ListBucket, DeleteBucket, GetBucketLocation
- **Range Request Support**: HTTP Range requests with 206 Partial Content responses
- **Multipart Upload**: Support for large file chunked uploads
- **AWS Signature V4 Authentication**: Complete AWS S3 V4 signature verification
- **Flexible Deployment**: Standalone startup or integrated with Master
- **High Performance**: Built with Rust and Axum for async high-throughput processing
- **Enterprise Security**: Comprehensive authentication and authorization
- **Streaming Operations**: Memory-efficient handling of large objects

## Quick Start

### 1. Standalone Mode

```bash
# 1. Register an access key (printed once by the tool, store it securely)
./build/bin/curvine-s3-gateway.sh credential generate --description local

# 2. Start with the default configuration file (etc/curvine-cluster.toml)
./build/bin/curvine-s3-gateway.sh serve

# Start with a custom configuration file
./build/bin/curvine-s3-gateway.sh serve --conf /path/to/curvine.conf

# Override the listen address and region from the configuration
./build/bin/curvine-s3-gateway.sh serve \
    --conf /path/to/curvine.conf \
    --listen 0.0.0.0:9000 \
    --region us-west-2

# Override only listen address, use region from config file
./build/bin/curvine-s3-gateway.sh \
    --conf /path/to/curvine.conf \
    --listen 0.0.0.0:9000
```

### 2. Integrated with Master

### 3. Building from Source

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
- `RUST_LOG`: Logging level (e.g., `debug`, `info`, `warn`, `error`).

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

## API Endpoints

- `GET /healthz`: Health check endpoint
- All S3 operations are available through standard S3 REST API endpoints:
  - `PUT /{bucket}`: Create bucket
  - `DELETE /{bucket}`: Delete bucket
  - `GET /{bucket}`: List objects in bucket
  - `PUT /{bucket}/{object}`: Upload object
  - `GET /{bucket}/{object}`: Download object
  - `HEAD /{bucket}/{object}`: Get object metadata
  - `DELETE /{bucket}/{object}`: Delete object
  - `GET /{bucket}?location`: Get bucket location
  - `POST /{bucket}/{object}?uploads`: Initiate multipart upload

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

### Logging Configuration

```bash
# Enable debug logging
export RUST_LOG=curvine_gateway=debug

# Enable info level logging for specific modules
export RUST_LOG=curvine_gateway::s3::handlers=info

# Enable trace logging (very verbose)
export RUST_LOG=trace
```

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
   
   # Verify credentials in application logs
   export RUST_LOG=curvine_gateway::auth=debug
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

```bash
# Enable verbose debugging
export RUST_LOG=debug
./curvine-s3-gateway --conf /path/to/curvine.conf

# Enable trace level for all modules
export RUST_LOG=trace
./curvine-s3-gateway --conf /path/to/curvine.conf
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
git clone https://github.com/your-org/curvine.git
cd curvine

# Build the gateway
cargo build --release --bin curvine-s3-gateway

# Run tests
cargo test -p curvine-s3-gateway

# Run with development logging
RUST_LOG=debug cargo run --bin curvine-s3-gateway
```

### Running Tests

```bash
# Unit tests
cargo test -p curvine-s3-gateway --lib

# Integration tests
cargo test -p curvine-s3-gateway --test integration

# Complete test suite
./curvine-s3-gateway/src/example/test_s3_complete.sh
```

### Documentation

For detailed technical documentation, see:
- [Technical Implementation Guide](TECHNICAL_IMPLEMENTATION.md)
- [API Reference](docs/api-reference.md)
- [Development Guide](docs/development.md)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## Support

- **Documentation**: See technical implementation guide
- **Issues**: Report bugs on GitHub
- **Discussions**: Join our community forums
- **Enterprise Support**: Contact our enterprise team