#!/bin/bash

# Curvine S3 Gateway Quick Benchmark Test Script
# Simple version - for detailed analysis use: ./benchmark/s3_benchmark_test.sh

set -e

# Configuration
S3_ENDPOINT="http://localhost:9900"
ACCESS_KEY="AqU4axe4feDyIielarPI"
SECRET_KEY="0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt"
S3_BENCHMARK_PATH="/home/oppo/Documents/s3-benchmark/s3-benchmark"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Curvine S3 Gateway Quick Benchmark Test ===${NC}"
echo -e "${CYAN}For detailed performance analysis, use: ./benchmark/s3_benchmark_test.sh${NC}"
echo "Endpoint: $S3_ENDPOINT"
echo "Access Key: $ACCESS_KEY"
echo

# Check if s3-benchmark exists
if [ ! -f "$S3_BENCHMARK_PATH" ]; then
    echo -e "${RED}Error: s3-benchmark not found at $S3_BENCHMARK_PATH${NC}"
    echo "Please ensure s3-benchmark is built and available"
    exit 1
fi

# Check if S3 Gateway is running
echo -e "${YELLOW}🔍 Checking if S3 Gateway is running...${NC}"
if ! curl -s "$S3_ENDPOINT/healthz" > /dev/null; then
    echo -e "${RED}Error: S3 Gateway is not running at $S3_ENDPOINT${NC}"
    echo "Please start the gateway first: ./build/dist/bin/curvine-s3-gateway.sh start"
    exit 1
fi
echo -e "${GREEN}✓ S3 Gateway is running${NC}"
echo

# Quick validation test
echo -e "${YELLOW}=== Quick Validation Test ===${NC}"
echo "Object size: 1KB, Threads: 1, Duration: 5s"
$S3_BENCHMARK_PATH \
    -a "$ACCESS_KEY" \
    -s "$SECRET_KEY" \
    -u "$S3_ENDPOINT" \
    -b "validation-test" \
    -z 1K \
    -t 1 \
    -d 5
echo

echo -e "${GREEN}=== Quick Test Completed ===${NC}"
echo -e "${CYAN}✓ Basic functionality validated${NC}"
echo
echo -e "${YELLOW}💡 For comprehensive performance testing:${NC}"
echo "  cd benchmark && ./s3_benchmark_test.sh"
echo
echo -e "${YELLOW}📋 This validates:${NC}"
echo "  ✓ AWS Signature V2 authentication works"
echo "  ✓ PUT/GET/DELETE operations function"
echo "  ✓ ListObjectVersions API is implemented"
echo "  ✓ Basic performance characteristics"
