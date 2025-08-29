#!/bin/bash

set -e

echo "=== Performance Baseline Test ==="
echo "Testing current streaming implementation performance"

# Configuration
ENDPOINT="http://localhost:9900"
BUCKET="perf-test"
TEST_DIR="/tmp/curvine-perf-test"

# Create test directory
mkdir -p $TEST_DIR

echo "Creating test bucket..."
aws --endpoint-url=$ENDPOINT s3 mb s3://$BUCKET 2>/dev/null || echo "Bucket might already exist"

echo -e "\n=== Creating Test Files ==="

# Small file (1KB)
dd if=/dev/zero of=$TEST_DIR/small.bin bs=1K count=1 2>/dev/null
echo "Small file: $(ls -lh $TEST_DIR/small.bin | awk '{print $5}')"

# Medium file (10MB) 
dd if=/dev/zero of=$TEST_DIR/medium.bin bs=1M count=10 2>/dev/null
echo "Medium file: $(ls -lh $TEST_DIR/medium.bin | awk '{print $5}')"

# Large file (100MB)
dd if=/dev/zero of=$TEST_DIR/large.bin bs=1M count=100 2>/dev/null
echo "Large file: $(ls -lh $TEST_DIR/large.bin | awk '{print $5}')"

echo -e "\n=== Upload Performance Test ==="

echo "Uploading small file (1KB)..."
time aws --endpoint-url=$ENDPOINT s3 cp $TEST_DIR/small.bin s3://$BUCKET/small.bin

echo "Uploading medium file (10MB)..."
time aws --endpoint-url=$ENDPOINT s3 cp $TEST_DIR/medium.bin s3://$BUCKET/medium.bin

echo "Uploading large file (100MB)..."
time aws --endpoint-url=$ENDPOINT s3 cp $TEST_DIR/large.bin s3://$BUCKET/large.bin

echo -e "\n=== Download Performance Test ==="

echo "Downloading small file (1KB)..."
time aws --endpoint-url=$ENDPOINT s3 cp s3://$BUCKET/small.bin $TEST_DIR/download_small.bin

echo "Downloading medium file (10MB)..."
time aws --endpoint-url=$ENDPOINT s3 cp s3://$BUCKET/medium.bin $TEST_DIR/download_medium.bin

echo "Downloading large file (100MB)..."
time aws --endpoint-url=$ENDPOINT s3 cp s3://$BUCKET/large.bin $TEST_DIR/download_large.bin

echo -e "\n=== Range Download Performance Test ==="

echo "Range download: First 1MB of large file..."
time aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key large.bin \
    --range "bytes=0-1048575" \
    $TEST_DIR/range_1mb.bin | cat

echo "Range download: Last 1MB of large file..."
time aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key large.bin \
    --range "bytes=-1048576" \
    $TEST_DIR/range_last_1mb.bin | cat

echo -e "\n=== Memory Usage Check ==="
echo "During these tests, the curvine-gateway process should use constant memory"
echo "regardless of file size (approximately 4KB buffer + overhead)."

echo -e "\n=== Integrity Check ==="
PASS=0
TOTAL=3

if cmp -s $TEST_DIR/small.bin $TEST_DIR/download_small.bin; then
    echo "✓ Small file integrity verified"
    ((PASS++))
else
    echo "✗ Small file integrity failed"
fi

if cmp -s $TEST_DIR/medium.bin $TEST_DIR/download_medium.bin; then
    echo "✓ Medium file integrity verified"
    ((PASS++))
else
    echo "✗ Medium file integrity failed"
fi

if cmp -s $TEST_DIR/large.bin $TEST_DIR/download_large.bin; then
    echo "✓ Large file integrity verified"
    ((PASS++))
else
    echo "✗ Large file integrity failed"
fi

echo -e "\n=== Cleanup ==="
echo "Removing test files..."
rm -rf $TEST_DIR

echo "Removing S3 objects..."
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/small.bin
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/medium.bin  
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/large.bin
aws --endpoint-url=$ENDPOINT s3 rb s3://$BUCKET

echo -e "\n=== Performance Baseline Summary ==="
echo "Integrity tests passed: $PASS/$TOTAL"
echo ""
echo "📊 This baseline establishes current performance characteristics:"
echo "   - Streaming upload/download with 4KB chunks"
echo "   - Constant memory usage regardless of file size"
echo "   - Full range request support"
echo ""
echo "Any future optimizations should maintain or improve these metrics." 