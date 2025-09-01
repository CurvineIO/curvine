#!/bin/bash

set -e

echo "=== Range Request Streaming Test ==="

# Configuration
ENDPOINT="http://localhost:9900"
BUCKET="range-streaming-test" 
TEST_FILE="/tmp/range_test_file.txt"

echo "Creating test bucket..."
aws --endpoint-url=$ENDPOINT s3 mb s3://$BUCKET 2>/dev/null || echo "Bucket might already exist"

echo "Creating test file with known content..."
# Create a 26-byte file: ABCDEFGHIJKLMNOPQRSTUVWXYZ
printf "ABCDEFGHIJKLMNOPQRSTUVWXYZ" > $TEST_FILE
echo "Test file size: $(wc -c < $TEST_FILE) bytes"
echo "Test file content: $(cat $TEST_FILE)"

echo -e "\n=== Uploading test file ==="
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE s3://$BUCKET/alphabet.txt

echo -e "\n=== Testing Range Requests with Streaming ==="

echo "1. Normal range: bytes=0-4 (first 5 bytes)"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key alphabet.txt \
    --range "bytes=0-4" \
    /tmp/range1.txt | cat
echo "Downloaded: '$(cat /tmp/range1.txt)' (expected: 'ABCDE')"

echo -e "\n2. Normal range: bytes=10-15 (middle 6 bytes)"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key alphabet.txt \
    --range "bytes=10-15" \
    /tmp/range2.txt | cat
echo "Downloaded: '$(cat /tmp/range2.txt)' (expected: 'KLMNOP')"

echo -e "\n3. Open range: bytes=20- (last 6 bytes)"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key alphabet.txt \
    --range "bytes=20-" \
    /tmp/range3.txt | cat
echo "Downloaded: '$(cat /tmp/range3.txt)' (expected: 'UVWXYZ')"

echo -e "\n4. Suffix range: bytes=-5 (last 5 bytes)"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key alphabet.txt \
    --range "bytes=-5" \
    /tmp/range4.txt | cat
echo "Downloaded: '$(cat /tmp/range4.txt)' (expected: 'VWXYZ')"

echo -e "\n5. Single byte: bytes=12-12"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key alphabet.txt \
    --range "bytes=12-12" \
    /tmp/range5.txt | cat
echo "Downloaded: '$(cat /tmp/range5.txt)' (expected: 'M')"

echo -e "\n=== Verifying Results ==="
PASS=0
TOTAL=5

if [ "$(cat /tmp/range1.txt)" = "ABCDE" ]; then
    echo "✓ Test 1 passed"
    ((PASS++))
else
    echo "✗ Test 1 failed"
fi

if [ "$(cat /tmp/range2.txt)" = "KLMNOP" ]; then
    echo "✓ Test 2 passed"
    ((PASS++))
else
    echo "✗ Test 2 failed"
fi

if [ "$(cat /tmp/range3.txt)" = "UVWXYZ" ]; then
    echo "✓ Test 3 passed"
    ((PASS++))
else
    echo "✗ Test 3 failed"
fi

if [ "$(cat /tmp/range4.txt)" = "VWXYZ" ]; then
    echo "✓ Test 4 passed"
    ((PASS++))
else
    echo "✗ Test 4 failed"
fi

if [ "$(cat /tmp/range5.txt)" = "M" ]; then
    echo "✓ Test 5 passed"
    ((PASS++))
else
    echo "✗ Test 5 failed"
fi

echo -e "\n=== Large File Range Test ==="
echo "Creating 1MB test file..."
dd if=/dev/zero of=/tmp/large_range_test.bin bs=1M count=1 2>/dev/null

echo "Uploading large file..."
aws --endpoint-url=$ENDPOINT s3 cp /tmp/large_range_test.bin s3://$BUCKET/large.bin

echo "Testing range on large file: bytes=500000-500099 (100 bytes from middle)"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key large.bin \
    --range "bytes=500000-500099" \
    /tmp/large_range.bin | cat

LARGE_SIZE=$(wc -c < /tmp/large_range.bin)
if [ "$LARGE_SIZE" -eq 100 ]; then
    echo "✓ Large file range test passed (got $LARGE_SIZE bytes)"
    ((PASS++))
    ((TOTAL++))
else
    echo "✗ Large file range test failed (got $LARGE_SIZE bytes, expected 100)"
    ((TOTAL++))
fi

echo -e "\n=== Cleanup ==="
rm -f /tmp/range*.txt /tmp/large*.bin $TEST_FILE
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/alphabet.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/large.bin
aws --endpoint-url=$ENDPOINT s3 rb s3://$BUCKET

echo -e "\n=== Summary ==="
echo "Range streaming tests passed: $PASS/$TOTAL"
if [ $PASS -eq $TOTAL ]; then
    echo "🎉 All range requests work perfectly with streaming!"
    echo "✅ Streaming GET with 4KB chunks supports all range types"
else
    echo "⚠️  Some range tests failed"
fi 