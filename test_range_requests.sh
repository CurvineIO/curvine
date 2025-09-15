#!/bin/bash

# Configuration
S3_ENDPOINT="http://127.0.0.1:9900"
AWS_ACCESS_KEY_ID="AqU4axe4feDyIielarPI"
AWS_SECRET_ACCESS_KEY="0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt"
BUCKET_NAME="test-bucket"
OBJECT_NAME="test-range-object"
FILE_SIZE_MB=10

echo "Testing Range Requests for curvine-s3-gateway..."
echo "Endpoint: $S3_ENDPOINT"
echo "Bucket: $BUCKET_NAME"
echo "Object: $OBJECT_NAME"
echo "File Size: ${FILE_SIZE_MB}MB"

# 1. Create bucket
echo -e "\n--- Creating bucket: $BUCKET_NAME ---"
aws --endpoint-url $S3_ENDPOINT \
    s3 mb s3://$BUCKET_NAME \
    --aws-access-key-id $AWS_ACCESS_KEY_ID \
    --aws-secret-access-key $AWS_SECRET_ACCESS_KEY

# 2. Create a test file with identifiable content
echo -e "\n--- Creating test file with identifiable content ---"
# Create a file with repeating pattern for easy verification
for i in {1..1024}; do
    printf "Line %04d: This is test content for range request validation. ABCDEFGHIJKLMNOPQRSTUVWXYZ\n" $i
done > test_content.txt

# Repeat to make it ~10MB
for i in {1..100}; do
    cat test_content.txt >> test_file_10mb.txt
done

# 3. Upload the test file
echo -e "\n--- Uploading object: $OBJECT_NAME ---"
aws --endpoint-url $S3_ENDPOINT \
    s3 cp test_file_10mb.txt s3://$BUCKET_NAME/$OBJECT_NAME \
    --aws-access-key-id $AWS_ACCESS_KEY_ID \
    --aws-secret-access-key $AWS_SECRET_ACCESS_KEY

# Get file size for range calculations
FILE_SIZE=$(stat -c%s test_file_10mb.txt)
echo "Uploaded file size: $FILE_SIZE bytes"

# 4. Test various range requests using curl
echo -e "\n--- Testing Range Requests ---"

# Test 1: First 1KB (bytes=0-1023)
echo "Test 1: First 1KB (bytes=0-1023)"
curl -s -H "Range: bytes=0-1023" \
     -H "Authorization: AWS4-HMAC-SHA256 Credential=$AWS_ACCESS_KEY_ID/$(date +%Y%m%d)/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=dummy" \
     "$S3_ENDPOINT/$BUCKET_NAME/$OBJECT_NAME" \
     -o range_test_1.bin
echo "Downloaded $(stat -c%s range_test_1.bin) bytes"
echo "First 100 chars: $(head -c 100 range_test_1.bin)"

# Test 2: Last 1KB (bytes=-1024)
echo -e "\nTest 2: Last 1KB (bytes=-1024)"
curl -s -H "Range: bytes=-1024" \
     -H "Authorization: AWS4-HMAC-SHA256 Credential=$AWS_ACCESS_KEY_ID/$(date +%Y%m%d)/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=dummy" \
     "$S3_ENDPOINT/$BUCKET_NAME/$OBJECT_NAME" \
     -o range_test_2.bin
echo "Downloaded $(stat -c%s range_test_2.bin) bytes"
echo "Last 100 chars: $(tail -c 100 range_test_2.bin)"

# Test 3: Middle range (bytes=5000-10000)
echo -e "\nTest 3: Middle range (bytes=5000-10000)"
curl -s -H "Range: bytes=5000-10000" \
     -H "Authorization: AWS4-HMAC-SHA256 Credential=$AWS_ACCESS_KEY_ID/$(date +%Y%m%d)/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=dummy" \
     "$S3_ENDPOINT/$BUCKET_NAME/$OBJECT_NAME" \
     -o range_test_3.bin
echo "Downloaded $(stat -c%s range_test_3.bin) bytes (expected: 5001 bytes)"

# Test 4: Open-ended range (bytes=1000000-)
echo -e "\nTest 4: Open-ended range from 1MB (bytes=1000000-)"
curl -s -H "Range: bytes=1000000-" \
     -H "Authorization: AWS4-HMAC-SHA256 Credential=$AWS_ACCESS_KEY_ID/$(date +%Y%m%d)/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=dummy" \
     "$S3_ENDPOINT/$BUCKET_NAME/$OBJECT_NAME" \
     -o range_test_4.bin
EXPECTED_SIZE=$((FILE_SIZE - 1000000))
echo "Downloaded $(stat -c%s range_test_4.bin) bytes (expected: $EXPECTED_SIZE bytes)"

# Test 5: Full file download (no range header)
echo -e "\nTest 5: Full file download (no range header)"
curl -s "$S3_ENDPOINT/$BUCKET_NAME/$OBJECT_NAME" \
     -o range_test_full.bin
echo "Downloaded $(stat -c%s range_test_full.bin) bytes (expected: $FILE_SIZE bytes)"

# Test 6: Use AWS CLI with range (if supported)
echo -e "\nTest 6: AWS CLI range download"
aws --endpoint-url $S3_ENDPOINT \
    s3api get-object \
    --bucket $BUCKET_NAME \
    --key $OBJECT_NAME \
    --range "bytes=0-2047" \
    --aws-access-key-id $AWS_ACCESS_KEY_ID \
    --aws-secret-access-key $AWS_SECRET_ACCESS_KEY \
    range_test_aws.bin
echo "AWS CLI downloaded $(stat -c%s range_test_aws.bin) bytes (expected: 2048 bytes)"

# 7. Verify content integrity
echo -e "\n--- Verifying Content Integrity ---"
# Extract same ranges from original file for comparison
head -c 1024 test_file_10mb.txt > original_first_1kb.bin
tail -c 1024 test_file_10mb.txt > original_last_1kb.bin
dd if=test_file_10mb.txt bs=1 skip=5000 count=5001 of=original_middle.bin 2>/dev/null
dd if=test_file_10mb.txt bs=1 skip=1000000 of=original_tail.bin 2>/dev/null
head -c 2048 test_file_10mb.txt > original_first_2kb.bin

echo "Comparing first 1KB..."
if cmp -s range_test_1.bin original_first_1kb.bin; then
    echo "✓ First 1KB range request: PASS"
else
    echo "✗ First 1KB range request: FAIL"
fi

echo "Comparing last 1KB..."
if cmp -s range_test_2.bin original_last_1kb.bin; then
    echo "✓ Last 1KB range request: PASS"
else
    echo "✗ Last 1KB range request: FAIL"
fi

echo "Comparing middle range..."
if cmp -s range_test_3.bin original_middle.bin; then
    echo "✓ Middle range request: PASS"
else
    echo "✗ Middle range request: FAIL"
fi

echo "Comparing open-ended range..."
if cmp -s range_test_4.bin original_tail.bin; then
    echo "✓ Open-ended range request: PASS"
else
    echo "✗ Open-ended range request: FAIL"
fi

echo "Comparing full file..."
if cmp -s range_test_full.bin test_file_10mb.txt; then
    echo "✓ Full file download: PASS"
else
    echo "✗ Full file download: FAIL"
fi

echo "Comparing AWS CLI range..."
if cmp -s range_test_aws.bin original_first_2kb.bin; then
    echo "✓ AWS CLI range request: PASS"
else
    echo "✗ AWS CLI range request: FAIL"
fi

# 8. Clean up
echo -e "\n--- Cleaning up ---"
aws --endpoint-url $S3_ENDPOINT \
    s3 rm s3://$BUCKET_NAME/$OBJECT_NAME \
    --aws-access-key-id $AWS_ACCESS_KEY_ID \
    --aws-secret-access-key $AWS_SECRET_ACCESS_KEY

aws --endpoint-url $S3_ENDPOINT \
    s3 rb s3://$BUCKET_NAME \
    --force \
    --aws-access-key-id $AWS_ACCESS_KEY_ID \
    --aws-secret-access-key $AWS_SECRET_ACCESS_KEY

# Clean up test files
rm -f test_content.txt test_file_10mb.txt
rm -f range_test_*.bin original_*.bin

echo -e "\nRange request testing completed."
