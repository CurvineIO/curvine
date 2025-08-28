#!/bin/bash

# Range Request Suffix Syntax Fix Test
# Tests the bytes=-N suffix range functionality

set -e

ENDPOINT="http://localhost:9900"
TEST_BUCKET="range-test-$(date +%s)"
TEST_FILE="/tmp/range_test_file.txt"

echo "Testing Range Request Suffix Syntax Fix"
echo "Endpoint: $ENDPOINT"
echo "Test Bucket: $TEST_BUCKET"
echo

# Create test file with known content (no newline)
echo "Creating test file with known content..."
printf "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" > $TEST_FILE
echo "File content: $(cat $TEST_FILE)"
echo "File size: $(wc -c < $TEST_FILE) bytes"
echo

# Create bucket
echo "Creating test bucket..."
aws --endpoint-url=$ENDPOINT s3 mb s3://$TEST_BUCKET
echo

# Upload test file
echo "Uploading test file..."
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE s3://$TEST_BUCKET/test.txt
echo

# Test 1: bytes=-10 (last 10 bytes)
echo "=== Test 1: bytes=-10 (last 10 bytes) ==="
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key test.txt --range "bytes=-10" /tmp/test1.txt | cat
echo "Expected: QRSTUVWXYZ (last 10 characters)"
echo "Actual:   $(cat /tmp/test1.txt)"
echo "Size:     $(wc -c < /tmp/test1.txt) bytes"
if [ "$(cat /tmp/test1.txt)" = "QRSTUVWXYZ" ]; then
    echo "✅ Test 1 PASSED"
else
    echo "❌ Test 1 FAILED"
fi
echo

# Test 2: bytes=-5 (last 5 bytes)
echo "=== Test 2: bytes=-5 (last 5 bytes) ==="
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key test.txt --range "bytes=-5" /tmp/test2.txt | cat
echo "Expected: VWXYZ (last 5 characters)"
echo "Actual:   $(cat /tmp/test2.txt)"
echo "Size:     $(wc -c < /tmp/test2.txt) bytes"
if [ "$(cat /tmp/test2.txt)" = "VWXYZ" ]; then
    echo "✅ Test 2 PASSED"
else
    echo "❌ Test 2 FAILED"
fi
echo

# Test 3: bytes=-50 (more than file size)
echo "=== Test 3: bytes=-50 (more than file size) ==="
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key test.txt --range "bytes=-50" /tmp/test3.txt | cat
echo "Expected: $(cat $TEST_FILE) (entire file)"
echo "Actual:   $(cat /tmp/test3.txt)"
echo "Size:     $(wc -c < /tmp/test3.txt) bytes"
if [ "$(cat /tmp/test3.txt)" = "$(cat $TEST_FILE)" ]; then
    echo "✅ Test 3 PASSED"
else
    echo "❌ Test 3 FAILED"
fi
echo

# Test 4: bytes=-1 (last 1 byte)
echo "=== Test 4: bytes=-1 (last 1 byte) ==="
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key test.txt --range "bytes=-1" /tmp/test4.txt | cat
echo "Expected: Z (last character)"
echo "Actual:   $(cat /tmp/test4.txt)"
echo "Size:     $(wc -c < /tmp/test4.txt) bytes"
if [ "$(cat /tmp/test4.txt)" = "Z" ]; then
    echo "✅ Test 4 PASSED"
else
    echo "❌ Test 4 FAILED"
fi
echo

# Test 5: Verify other range formats still work
echo "=== Test 5: Verify other range formats still work ==="

# Test 5.1: bytes=0-9 (first 10 bytes)
echo "Test 5.1: bytes=0-9 (first 10 bytes)"
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key test.txt --range "bytes=0-9" /tmp/test5_1.txt | cat
echo "Expected: 0123456789"
echo "Actual:   $(cat /tmp/test5_1.txt)"
if [ "$(cat /tmp/test5_1.txt)" = "0123456789" ]; then
    echo "✅ Test 5.1 PASSED"
else
    echo "❌ Test 5.1 FAILED"
fi
echo

# Test 5.2: bytes=10- (from byte 10 to end)
echo "Test 5.2: bytes=10- (from byte 10 to end)"
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key test.txt --range "bytes=10-" /tmp/test5_2.txt | cat
echo "Expected: ABCDEFGHIJKLMNOPQRSTUVWXYZ"
echo "Actual:   $(cat /tmp/test5_2.txt)"
if [ "$(cat /tmp/test5_2.txt)" = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" ]; then
    echo "✅ Test 5.2 PASSED"
else
    echo "❌ Test 5.2 FAILED"
fi
echo

# Cleanup
echo "Cleaning up..."
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/test.txt
aws --endpoint-url=$ENDPOINT s3 rb s3://$TEST_BUCKET
rm -f $TEST_FILE /tmp/test*.txt
echo "Cleanup completed"
echo

echo "=== Range Request Suffix Syntax Test Summary ==="
echo "All tests completed. Check results above for PASS/FAIL status." 