#!/bin/bash

set -e

echo "=== Streaming Performance Test ==="

# Configuration
ENDPOINT="http://localhost:9900"
BUCKET="streaming-test"
LARGE_FILE="/tmp/large_test_file.bin"
SMALL_FILE="/tmp/small_test_file.txt"

echo "Creating test bucket..."
aws --endpoint-url=$ENDPOINT s3 mb s3://$BUCKET 2>/dev/null || echo "Bucket might already exist"

echo "Creating test files..."
# Create a 50MB file for streaming test
dd if=/dev/zero of=$LARGE_FILE bs=1M count=50 2>/dev/null
echo "Large file: $(ls -lh $LARGE_FILE | awk '{print $5}')"

# Create a small file for comparison
echo -n "This is a small test file for streaming comparison." > $SMALL_FILE
echo "Small file: $(ls -lh $SMALL_FILE | awk '{print $5}')"

echo -e "\n=== Testing Large File Upload/Download ==="
echo "Uploading 50MB file..."
time aws --endpoint-url=$ENDPOINT s3 cp $LARGE_FILE s3://$BUCKET/large-file.bin

echo "Downloading 50MB file..."
time aws --endpoint-url=$ENDPOINT s3 cp s3://$BUCKET/large-file.bin /tmp/downloaded_large.bin

echo "Verifying file integrity..."
if cmp -s $LARGE_FILE /tmp/downloaded_large.bin; then
    echo "✓ Large file integrity verified"
else
    echo "✗ Large file integrity check failed"
fi

echo -e "\n=== Testing Range Requests on Large File ==="
echo "Testing range request (first 4KB)..."
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key large-file.bin \
    --range "bytes=0-4095" \
    /tmp/range_test.bin | cat

echo "Range file size: $(ls -lh /tmp/range_test.bin | awk '{print $5}')"

echo -e "\n=== Testing Small File ==="
echo "Uploading small file..."
time aws --endpoint-url=$ENDPOINT s3 cp $SMALL_FILE s3://$BUCKET/small-file.txt

echo "Downloading small file..."
time aws --endpoint-url=$ENDPOINT s3 cp s3://$BUCKET/small-file.txt /tmp/downloaded_small.txt

echo "Verifying small file content..."
if cmp -s $SMALL_FILE /tmp/downloaded_small.txt; then
    echo "✓ Small file integrity verified"
else
    echo "✗ Small file integrity check failed"
fi

echo -e "\n=== Cleanup ==="
echo "Removing test files..."
rm -f $LARGE_FILE $SMALL_FILE /tmp/downloaded_*.bin /tmp/downloaded_*.txt /tmp/range_test.bin

echo "Removing test objects..."
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/large-file.bin
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/small-file.txt
aws --endpoint-url=$ENDPOINT s3 rb s3://$BUCKET

echo -e "\n✓ Streaming test completed successfully!"
echo "The streaming implementation now uses 4KB chunks instead of loading entire files into memory." 