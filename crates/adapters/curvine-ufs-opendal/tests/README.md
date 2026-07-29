# Curvine UFS Testing

This directory contains test code for Curvine Unified File System (UFS).

## S3 Test

The `s3/` directory contains tests for the S3 file system. These tests verify the integration of Curvine UFS with S3-compatible storage.

### Test environment settings

S3 testing requires an S3-compatible storage service. It is recommended to use MinIO as a local test environment:

```bash
# Start the MinIO server
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

The test uses the following configuration parameters (defined in `s3/test_utils.rs`):
-Endpoint: http://127.0.0.1:9000
-Area: us-east-1
-Access key: miniauadmin
-Key: miniauadmin
-Use path style: true

### Test content

The S3 test includes the following aspects:

1. **Basic S3 Operation**(`s3_tests.rs`):
   -List directories (recursive and non-recursive)
   -Create and delete directories
   -Rename the file
   -Check if the file exists
   -Get file size

2. **Zero Copy File Transfer**(`zero_copy_transfer_tests.rs`):
   -S3 to local file transfer
   -Performance tests for different buffer sizes
   -Concurrent transmission test
   -Transmission Cancel Test

3. **Concurrent S3 operation**(`concurrent_s3_tests.rs`):
   -Concurrent upload test
   -Concurrent read test

### Run the test

Run all S3 tests with the following command:

```bash
cargo test -p curvine-ufs --test s3
```

Or run a specific test:

```bash
# Run only basic operation tests
cargo test -p curvine-ufs --test s3_tests

# Run only zero copy tests
cargo test -p curvine-ufs --test zero_copy_transfer_tests

# Run concurrent tests only
cargo test -p curvine-ufs --test concurrent_s3_tests
```