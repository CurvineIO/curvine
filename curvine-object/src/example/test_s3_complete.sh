#!/bin/bash

# S3 Complete Test Script
# Tests all S3 operations against curvine-object gateway

set -e  # Exit on any error

ENDPOINT="http://localhost:9900"
TEST_BUCKET="test-bucket-$(date +%s)"
TEST_FILE_SMALL="/tmp/test_small.txt"
TEST_FILE_LARGE="/tmp/test_large.bin"
TEST_FILE_DOWNLOAD="/tmp/test_download.txt"

echo "🚀 Starting Complete S3 Test Suite"
echo "Endpoint: $ENDPOINT"
echo "Test Bucket: $TEST_BUCKET"
echo

# Prepare test files
echo "📁 Preparing test files..."
echo "Hello, this is a small test file for S3 operations!" > $TEST_FILE_SMALL
dd if=/dev/zero of=$TEST_FILE_LARGE bs=1M count=10 2>/dev/null
echo "✅ Test files created"
echo

# Test 1: Create Bucket
echo "🪣 Test 1: Create Bucket"
aws --endpoint-url=$ENDPOINT s3 mb s3://$TEST_BUCKET
echo "✅ Bucket created successfully"
echo

# Test 2: Head Bucket
echo "🔍 Test 2: Head Bucket"
aws --endpoint-url=$ENDPOINT s3api head-bucket --bucket $TEST_BUCKET
echo "✅ Head bucket successful"
echo

# Test 3: Put Object (Small File)
echo "📤 Test 3: Put Object (Small File)"
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE_SMALL s3://$TEST_BUCKET/small-test.txt
echo "✅ Small file uploaded successfully"
echo

# Test 4: Head Object
echo "🔍 Test 4: Head Object"
aws --endpoint-url=$ENDPOINT s3api head-object --bucket $TEST_BUCKET --key small-test.txt
echo "✅ Head object successful"
echo

# Test 5: Get Object
echo "📥 Test 5: Get Object"
aws --endpoint-url=$ENDPOINT s3 cp s3://$TEST_BUCKET/small-test.txt $TEST_FILE_DOWNLOAD
echo "Downloaded file content:"
cat $TEST_FILE_DOWNLOAD
echo
echo "✅ Get object successful"
echo

# Test 6: Range Get (partial download)
echo "📥 Test 6: Range Get"
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key small-test.txt --range "bytes=0-10" /tmp/range_test.txt
echo "Range download content (first 11 bytes):"
cat /tmp/range_test.txt
echo
echo "✅ Range get successful"
echo

# Test 7: List Bucket
echo "📋 Test 7: List Bucket"
aws --endpoint-url=$ENDPOINT s3 ls s3://$TEST_BUCKET/
echo "✅ List bucket successful"
echo

# Test 8: Get Bucket Location
echo "🌍 Test 8: Get Bucket Location"
aws --endpoint-url=$ENDPOINT s3api get-bucket-location --bucket $TEST_BUCKET || echo "⚠️  Get bucket location not implemented (optional)"
echo

# Test 9: Multipart Upload (Large File)
echo "📤 Test 9: Multipart Upload (Large File)"
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE_LARGE s3://$TEST_BUCKET/large-test.bin
echo "✅ Large file multipart upload successful"
echo

# Test 10: Get Large Object
echo "📥 Test 10: Get Large Object"
aws --endpoint-url=$ENDPOINT s3 cp s3://$TEST_BUCKET/large-test.bin /tmp/large_download.bin
echo "Comparing file sizes:"
echo "Original: $(wc -c < $TEST_FILE_LARGE) bytes"
echo "Downloaded: $(wc -c < /tmp/large_download.bin) bytes"
if cmp -s $TEST_FILE_LARGE /tmp/large_download.bin; then
    echo "✅ Files are identical - large file download successful"
else
    echo "❌ Files differ - large file download failed"
    exit 1
fi
echo

# Test 11: Delete Objects
echo "🗑️  Test 11: Delete Objects"
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/small-test.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/large-test.bin
echo "✅ Objects deleted successfully"
echo

# Test 12: Delete Bucket
echo "🗑️  Test 12: Delete Bucket"
aws --endpoint-url=$ENDPOINT s3 rb s3://$TEST_BUCKET
echo "✅ Bucket deleted successfully"
echo

# Cleanup
echo "🧹 Cleaning up test files..."
rm -f $TEST_FILE_SMALL $TEST_FILE_LARGE $TEST_FILE_DOWNLOAD /tmp/range_test.txt /tmp/large_download.bin
echo "✅ Cleanup completed"
echo

echo "🎉 All S3 tests completed successfully!"
echo "✅ PUT Object: Working"
echo "✅ GET Object: Working" 
echo "✅ HEAD Object: Working"
echo "✅ DELETE Object: Working"
echo "✅ CREATE Bucket: Working"
echo "✅ HEAD Bucket: Working"
echo "✅ LIST Bucket: Working"
echo "✅ DELETE Bucket: Working"
echo "✅ Range GET: Working"
echo "✅ Multipart Upload: Working"
echo
echo "🚀 curvine-object S3 gateway is fully functional!" 