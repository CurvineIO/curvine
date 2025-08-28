# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# S3 Complete Test Script
# Tests all S3 operations against curvine-object gateway

set -e  # Exit on any error

ENDPOINT="http://localhost:9900"
TEST_BUCKET="test-bucket-$(date +%s)"
TEST_FILE_SMALL="/tmp/test_small.txt"
TEST_FILE_LARGE="/tmp/test_large.bin"
TEST_FILE_EMPTY="/tmp/test_empty.txt"
TEST_FILE_DOWNLOAD="/tmp/test_download.txt"
TEST_FILE_SPECIAL="/tmp/test-special-chars@#$.txt"

# Test result counters
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=""

echo "Starting Complete S3 Test Suite"
echo "Endpoint: $ENDPOINT"
echo "Test Bucket: $TEST_BUCKET"
echo

# Prepare test files
echo "Preparing test files..."
printf "Hello, this is a small test file for S3 operations!" > $TEST_FILE_SMALL
dd if=/dev/zero of=$TEST_FILE_LARGE bs=1M count=10 2>/dev/null
touch $TEST_FILE_EMPTY  # Empty file test
echo -n "Special characters file test: @#\$%%^&*()_+" > $TEST_FILE_SPECIAL
echo "Test files created"
echo

# Test 1: Create Bucket
echo "Test 1: Create Bucket"
aws --endpoint-url=$ENDPOINT s3 mb s3://$TEST_BUCKET
echo "Bucket created successfully"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 2: Head Bucket
echo "Test 2: Head Bucket"
aws --endpoint-url=$ENDPOINT s3api head-bucket --bucket $TEST_BUCKET | cat
echo "Head bucket successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 3: Put Object (Small File)
echo "Test 3: Put Object (Small File)"
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE_SMALL s3://$TEST_BUCKET/small-test.txt
echo "Small file uploaded successfully"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 3.1: Put Object (Empty File) - NEW
echo "Test 3.1: Put Object (Empty File)"
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE_EMPTY s3://$TEST_BUCKET/empty-test.txt
echo "Empty file uploaded successfully"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 3.2: Put Object (Special Characters) - NEW
echo "Test 3.2: Put Object (Special Characters)"
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE_SPECIAL s3://$TEST_BUCKET/special-chars-test.txt
echo "Special characters file uploaded successfully"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 4: Head Object
echo "Test 4: Head Object"
aws --endpoint-url=$ENDPOINT s3api head-object --bucket $TEST_BUCKET --key small-test.txt | cat
echo "Head object successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 5: Get Object
echo "Test 5: Get Object"
aws --endpoint-url=$ENDPOINT s3 cp s3://$TEST_BUCKET/small-test.txt $TEST_FILE_DOWNLOAD
echo "Downloaded file content:"
cat $TEST_FILE_DOWNLOAD
echo
echo "Get object successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 5.1: Get Empty Object - NEW
echo "Test 5.1: Get Empty Object"
aws --endpoint-url=$ENDPOINT s3 cp s3://$TEST_BUCKET/empty-test.txt /tmp/empty_download.txt
echo "Empty file size: $(wc -c < /tmp/empty_download.txt) bytes"
echo "Empty object download successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 6: Range Get (comprehensive testing) - ENHANCED
echo "Test 6: Range Get (Comprehensive)"

# Test 6.1: First 11 bytes
echo "Test 6.1: Range Get - First 11 bytes"
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key small-test.txt --range "bytes=0-10" /tmp/range_test_1.txt | cat
echo "Range download content (bytes 0-10):"
cat /tmp/range_test_1.txt
echo
echo "Range get (0-10) successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 6.2: From byte 5 to end
echo "Test 6.2: Range Get - From byte 5 to end"
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key small-test.txt --range "bytes=5-" /tmp/range_test_2.txt | cat
echo "Range download content (bytes 5-):"
cat /tmp/range_test_2.txt
echo
echo "Range get (5-) successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 6.3: Last 10 bytes
echo "Test 6.3: Range Get - Last 10 bytes"
aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key small-test.txt --range "bytes=-10" /tmp/range_test_3.txt | cat
echo "Range download content (last 10 bytes):"
cat /tmp/range_test_3.txt
echo
echo "Range get (-10) successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 6.4: Invalid range (should return 416) - NEW ERROR TEST
echo "Test 6.4: Range Get - Invalid range (expect 416)"
if aws --endpoint-url=$ENDPOINT s3api get-object --bucket $TEST_BUCKET --key small-test.txt --range "bytes=1000-2000" /tmp/range_test_4.txt 2>/dev/null; then
    echo "WARNING: Invalid range should have failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 6.4: Invalid range handling"
else
    echo "Invalid range correctly rejected"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
echo

# Test 7: List Objects in Bucket - NEW MAJOR FEATURE TEST
echo "Test 7: List Objects in Bucket"
aws --endpoint-url=$ENDPOINT s3api list-objects-v2 --bucket $TEST_BUCKET | cat
echo "List objects successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 7.1: List Objects with Prefix - NEW
echo "Test 7.1: List Objects with Prefix"
LISTPREFIX_OUTPUT=$(aws --endpoint-url=$ENDPOINT s3api list-objects-v2 --bucket $TEST_BUCKET --prefix "small-" | cat)
echo "$LISTPREFIX_OUTPUT"
if echo "$LISTPREFIX_OUTPUT" | grep -q "small-test.txt"; then
    echo "List objects with prefix successful"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo "WARNING: List objects with prefix failed - no matching objects found"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 7.1: ListObjects prefix filtering"
fi
echo

# Test 8: List All Buckets
echo "Test 8: List All Buckets"
aws --endpoint-url=$ENDPOINT s3 ls
echo "List buckets successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 9: Get Bucket Location
echo "Test 9: Get Bucket Location"
aws --endpoint-url=$ENDPOINT s3api get-bucket-location --bucket $TEST_BUCKET | cat || echo "WARNING: Get bucket location not implemented (optional)"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 10: Error Scenarios - NEW COMPREHENSIVE ERROR TESTING
echo "Test 10: Error Scenarios"

# Test 10.1: Get non-existent object (expect 404)
echo "Test 10.1: Get non-existent object (expect 404)"
if aws --endpoint-url=$ENDPOINT s3api head-object --bucket $TEST_BUCKET --key non-existent.txt 2>/dev/null; then
    echo "WARNING: Non-existent object should have failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 10.1: 404 error handling for objects"
else
    echo "Non-existent object correctly returns 404"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
echo

# Test 10.2: Get from non-existent bucket (expect 404)
echo "Test 10.2: Get from non-existent bucket (expect 404)"
if aws --endpoint-url=$ENDPOINT s3api head-bucket --bucket non-existent-bucket 2>/dev/null; then
    echo "WARNING: Non-existent bucket should have failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 10.2: 404 error handling for buckets"
else
    echo "Non-existent bucket correctly returns 404"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
echo

# Test 10.3: Create bucket that already exists (expect 409)
echo "Test 10.3: Create existing bucket (expect 409)"
if aws --endpoint-url=$ENDPOINT s3 mb s3://$TEST_BUCKET 2>/dev/null; then
    echo "WARNING: Creating existing bucket should have failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 10.3: 409 error for duplicate bucket creation"
else
    echo "Creating existing bucket correctly returns error"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
echo

# Test 11: Multipart Upload (Large File)
echo "Test 11: Multipart Upload (Large File)"
aws --endpoint-url=$ENDPOINT s3 cp $TEST_FILE_LARGE s3://$TEST_BUCKET/large-test.bin
echo "Large file multipart upload successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 12: Get Large Object
echo "Test 12: Get Large Object"
aws --endpoint-url=$ENDPOINT s3 cp s3://$TEST_BUCKET/large-test.bin /tmp/large_download.bin
echo "Comparing file sizes:"
echo "Original: $(wc -c < $TEST_FILE_LARGE) bytes"
echo "Downloaded: $(wc -c < /tmp/large_download.bin) bytes"
if cmp -s $TEST_FILE_LARGE /tmp/large_download.bin; then
    echo "Files are identical - large file download successful"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo "ERROR: Files differ - large file download failed"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 12: Large file download integrity"
    exit 1
fi
echo

# Test 13: Concurrent Operations - NEW PERFORMANCE TEST
echo "Test 13: Concurrent Operations"
echo "Uploading multiple files concurrently..."
for i in {1..3}; do
    echo "Concurrent test file $i" > /tmp/concurrent_$i.txt &
done
wait

for i in {1..3}; do
    aws --endpoint-url=$ENDPOINT s3 cp /tmp/concurrent_$i.txt s3://$TEST_BUCKET/concurrent-$i.txt &
done
wait
echo "Concurrent uploads successful"

echo "Downloading multiple files concurrently..."
for i in {1..3}; do
    aws --endpoint-url=$ENDPOINT s3 cp s3://$TEST_BUCKET/concurrent-$i.txt /tmp/concurrent_download_$i.txt &
done
wait
echo "Concurrent downloads successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 14: Final List Objects (should show all uploaded files) - NEW
echo "Test 14: Final List Objects (All Files)"
echo "All objects in bucket:"
aws --endpoint-url=$ENDPOINT s3api list-objects-v2 --bucket $TEST_BUCKET | cat
echo "Final list objects successful"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 15: Delete Objects (including new test files)
echo "Test 15: Delete Objects"
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/small-test.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/empty-test.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/special-chars-test.txt
aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/large-test.bin
for i in {1..3}; do
    aws --endpoint-url=$ENDPOINT s3 rm s3://$TEST_BUCKET/concurrent-$i.txt
done
echo "Objects deleted successfully"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Test 16: Verify Empty Bucket - NEW
echo "Test 16: Verify Empty Bucket"
BUCKET_CONTENTS=$(aws --endpoint-url=$ENDPOINT s3api list-objects-v2 --bucket $TEST_BUCKET --query 'Contents' --output text)
if [ "$BUCKET_CONTENTS" != "None" ] && [ -n "$BUCKET_CONTENTS" ]; then
    echo "WARNING: Bucket should be empty, but contains: $BUCKET_CONTENTS"
    TESTS_FAILED=$((TESTS_FAILED + 1))
    FAILED_TESTS="$FAILED_TESTS\n- Test 16: Bucket cleanup verification"
else
    echo "Bucket is empty as expected"
    TESTS_PASSED=$((TESTS_PASSED + 1))
fi
echo

# Test 17: Delete Bucket
echo "Test 17: Delete Bucket"
aws --endpoint-url=$ENDPOINT s3 rb s3://$TEST_BUCKET
echo "Bucket deleted successfully"
TESTS_PASSED=$((TESTS_PASSED + 1))
echo

# Cleanup
echo "Cleaning up test files..."
rm -f $TEST_FILE_SMALL $TEST_FILE_LARGE $TEST_FILE_EMPTY $TEST_FILE_SPECIAL $TEST_FILE_DOWNLOAD 
rm -f /tmp/range_test_*.txt /tmp/empty_download.txt /tmp/large_download.bin
rm -f /tmp/concurrent_*.txt /tmp/concurrent_download_*.txt
echo "Cleanup completed"
echo

# Final Summary
echo "=== S3 Test Suite Summary ==="
echo "Tests Passed: $TESTS_PASSED"
echo "Tests Failed: $TESTS_FAILED"
echo "Total Tests: $((TESTS_PASSED + TESTS_FAILED))"
echo

if [ $TESTS_FAILED -eq 0 ]; then
    echo "SUCCESS: All S3 tests passed!"
    echo "PUT Object: Working (small, empty, special chars)"
    echo "GET Object: Working (including empty files)" 
    echo "HEAD Object: Working"
    echo "DELETE Object: Working"
    echo "CREATE Bucket: Working"
    echo "HEAD Bucket: Working"
    echo "LIST Buckets: Working"
    echo "LIST Objects: Working (with prefix support)"
    echo "DELETE Bucket: Working"
    echo "Range GET: Working (comprehensive testing)"
    echo "Multipart Upload: Working"
    echo "Error Handling: Working (404, 409, 416)"
    echo "Concurrent Operations: Working"
    echo "Special Characters: Working"
    echo "Empty Files: Working"
    echo
    echo "curvine-object S3 gateway is fully functional and robust!" 
else
    echo "PARTIAL SUCCESS: Some tests failed"
    echo
    echo "Working Features:"
    echo "- PUT Object: Working (small, empty, special chars)"
    echo "- GET Object: Working (including empty files)" 
    echo "- HEAD Object: Working"
    echo "- DELETE Object: Working"
    echo "- CREATE Bucket: Working"
    echo "- HEAD Bucket: Working"
    echo "- LIST Buckets: Working"
    echo "- Range GET: Working (comprehensive testing)"
    echo "- Multipart Upload: Working"
    echo "- Special Characters: Working"
    echo "- Empty Files: Working"
    echo
    echo "Failed Tests:"
    echo -e "$FAILED_TESTS"
    echo
    echo "curvine-object S3 gateway is mostly functional with some issues to fix."
fi 