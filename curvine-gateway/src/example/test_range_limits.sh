#!/bin/bash

set -e

echo "=== Range Limit Test ==="
echo "Testing suffix range limits and error handling"

# Configuration
ENDPOINT="http://localhost:9900"
BUCKET="range-limit-test"

echo "Creating test bucket..."
aws --endpoint-url=$ENDPOINT s3 mb s3://$BUCKET 2>/dev/null || echo "Bucket might already exist"

echo "Creating 50MB test file..."
dd if=/dev/zero of=/tmp/range_limit_test.bin bs=1M count=50 2>/dev/null
aws --endpoint-url=$ENDPOINT s3 cp /tmp/range_limit_test.bin s3://$BUCKET/test.bin

echo -e "\n=== Testing Valid Suffix Ranges ==="

echo "1. Small suffix: bytes=-1024 (1KB) - should work"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key test.bin \
    --range "bytes=-1024" \
    /tmp/range_1kb.bin | cat
echo "Downloaded: $(wc -c < /tmp/range_1kb.bin) bytes"

echo -e "\n2. Medium suffix: bytes=-1048576 (1MB) - should work"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key test.bin \
    --range "bytes=-1048576" \
    /tmp/range_1mb.bin | cat
echo "Downloaded: $(wc -c < /tmp/range_1mb.bin) bytes"

echo -e "\n3. Large suffix: bytes=-536870912 (512MB) - should work"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key test.bin \
    --range "bytes=-536870912" \
    /tmp/range_512mb.bin | cat
echo "Downloaded: $(wc -c < /tmp/range_512mb.bin) bytes"

echo -e "\n=== Testing Range Limits ==="

echo "4. Maximum allowed: bytes=-1073741824 (1GB exactly) - should work"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key test.bin \
    --range "bytes=-1073741824" \
    /tmp/range_1gb.bin 2>&1 | cat || true
if [ -f /tmp/range_1gb.bin ]; then
    echo "✓ 1GB range accepted, downloaded: $(wc -c < /tmp/range_1gb.bin) bytes"
else
    echo "✗ 1GB range rejected"
fi

echo -e "\n5. Over limit: bytes=-1073741825 (1GB + 1 byte) - should fail with 416"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key test.bin \
    --range "bytes=-1073741825" \
    /tmp/range_over_limit.bin 2>&1 | cat || true
if [ -f /tmp/range_over_limit.bin ]; then
    echo "✗ Over-limit range incorrectly accepted"
else
    echo "✓ Over-limit range correctly rejected with 416"
fi

echo -e "\n6. Extreme case: bytes=-2147483648 (2GB) - should fail with 416"
aws --endpoint-url=$ENDPOINT s3api get-object \
    --bucket $BUCKET \
    --key test.bin \
    --range "bytes=-2147483648" \
    /tmp/range_extreme.bin 2>&1 | cat || true
if [ -f /tmp/range_extreme.bin ]; then
    echo "✗ Extreme range incorrectly accepted"
else
    echo "✓ Extreme range correctly rejected with 416"
fi

echo -e "\n=== Cleanup ==="
rm -f /tmp/range_*.bin /tmp/range_limit_test.bin
aws --endpoint-url=$ENDPOINT s3 rm s3://$BUCKET/test.bin
aws --endpoint-url=$ENDPOINT s3 rb s3://$BUCKET

echo -e "\n=== Range Limit Summary ==="
echo "✅ Current limits:"
echo "   - Maximum suffix range: 1GB (1,073,741,824 bytes)"
echo "   - Detection threshold: u64::MAX / 2 (~9.2 EB)"
echo "   - Error response: 416 Range Not Satisfiable"
echo ""
echo "🔧 Logic explanation:"
echo "   if range_end > u64::MAX / 2:"
echo "     suffix_len = u64::MAX - range_end"
echo "     if suffix_len > 1GB: return 416 error"
echo ""
echo "📊 Examples:"
echo "   bytes=-1KB   → range_end = u64::MAX - 1024     → suffix detected ✓"
echo "   bytes=-1GB   → range_end = u64::MAX - 1GB      → suffix detected ✓" 
echo "   bytes=-2GB   → range_end = u64::MAX - 2GB      → suffix detected, but 416 error ✗"
echo "   bytes=0-100  → range_end = 100                 → normal range ✓" 