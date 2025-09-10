# Curvine S3 Gateway Benchmark Usage Guide

This guide provides detailed instructions for using the s3-benchmark performance testing suite.

## Table of Contents
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Running Tests](#running-tests)
- [Understanding Results](#understanding-results)
- [Customization](#customization)
- [Troubleshooting](#troubleshooting)

## Quick Start

### 1-Minute Setup
```bash
# 1. Ensure s3-benchmark is available
ls /home/oppo/Documents/s3-benchmark/s3-benchmark

# 2. Start Curvine S3 Gateway
cd /home/oppo/Documents/curvine
./build/dist/bin/curvine-s3-gateway.sh start

# 3. Run benchmarks
cd benchmark
./s3_benchmark_test.sh
```

## Detailed Setup

### Prerequisites Checklist

- [ ] **Go Environment**: Required for building s3-benchmark
- [ ] **s3-benchmark Tool**: Built and accessible
- [ ] **Curvine S3 Gateway**: Running on port 9900
- [ ] **Network Access**: Gateway accessible from benchmark host
- [ ] **System Tools**: `curl`, `bc` calculator installed

### Step-by-Step Setup

#### 1. Install System Dependencies
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install curl bc golang-go

# CentOS/RHEL
sudo yum install curl bc golang
```

#### 2. Build s3-benchmark Tool
```bash
cd /home/oppo/Documents
git clone https://github.com/wasabi-tech/s3-benchmark.git
cd s3-benchmark
go build
chmod +x s3-benchmark
```

#### 3. Verify s3-benchmark Installation
```bash
./s3-benchmark -h
# Should show help output
```

#### 4. Start Curvine S3 Gateway
```bash
cd /home/oppo/Documents/curvine

# Check if already running
curl -s http://localhost:9900/healthz

# Start if not running
./build/dist/bin/curvine-s3-gateway.sh start

# Verify startup
curl http://localhost:9900/healthz
# Should return "ok"
```

## Running Tests

### Basic Execution
```bash
cd /home/oppo/Documents/curvine/benchmark
./s3_benchmark_test.sh
```

### What Happens During Execution

1. **Pre-flight Checks**
   - Verifies s3-benchmark tool exists
   - Checks S3 Gateway connectivity
   - Creates results directory

2. **Test Execution** (5 tests total)
   - Quick Performance Test (1KB objects)
   - Medium Performance Test (10KB objects)
   - Concurrency Test (5KB objects, 5 threads)
   - Large Object Test (100KB objects)
   - Stress Test (50KB objects, 8 threads)

3. **Performance Analysis**
   - Real-time performance evaluation
   - Comparative analysis across tests
   - Overall performance scoring

4. **Report Generation**
   - Individual test result files
   - Comprehensive summary report
   - Performance recommendations

### Expected Output Format
```
=== Curvine S3 Gateway Performance Benchmark Test ===
Endpoint: http://localhost:9900
Access Key: AqU4axe4feDyIielarPI
Results Directory: ./results
Timestamp: 20250910_120000

🔍 Checking if S3 Gateway is running...
✓ S3 Gateway is running

=== Quick_Performance_Test ===
Fast validation test with small objects and single thread
Object size: 1K, Threads: 1, Duration: 5s

Wasabi benchmark program v2.0
Parameters: url=http://localhost:9900, bucket=quick-test-20250910_120000, region=us-east-1, duration=5, threads=1, loops=1, size=1K
Loop 1: PUT time 5.0 secs, objects = 641, speed = 641KB/sec, 641.0 operations/sec. Slowdowns = 0
Loop 1: GET time 5.0 secs, objects = 1112, speed = 1.1MB/sec, 1111.8 operations/sec. Slowdowns = 0
Loop 1: DELETE time 0.6 secs, 5524.3 deletes/sec. Slowdowns = 0

📊 Performance Analysis for Quick_Performance_Test:
  PUT: ○ Good - 641.0 ops/sec, 641KB/sec
  GET: ✓ Excellent - 1111.8 ops/sec, 1.1MB/sec
  DELETE: ✓ Excellent - 5524.3 deletes/sec

📁 Results saved to: ./results/Quick_Performance_Test_20250910_120000.txt
```

## Understanding Results

### Performance Metrics

#### Operations Per Second (ops/sec)
- **PUT**: Write operations per second
- **GET**: Read operations per second  
- **DELETE**: Delete operations per second

#### Throughput (MB/sec, KB/sec)
- **Speed**: Data transfer rate
- **Calculated**: ops/sec × object_size

#### Performance Ratings
- **✓ Excellent**: Exceeds performance expectations
- **○ Good**: Meets acceptable performance standards
- **△ Needs optimization**: Below optimal performance

### Performance Thresholds

#### PUT Operations
- **Excellent**: > 1000 ops/sec
- **Good**: 500-1000 ops/sec
- **Needs optimization**: < 500 ops/sec

#### GET Operations
- **Excellent**: > 2000 ops/sec
- **Good**: 1000-2000 ops/sec
- **Needs optimization**: < 1000 ops/sec

#### DELETE Operations
- **Excellent**: > 10000 ops/sec
- **Good**: 5000-10000 ops/sec
- **Needs optimization**: < 5000 ops/sec

### Result Files

#### Individual Test Results
Location: `./results/{TestName}_{Timestamp}.txt`

Content includes:
- Test configuration
- Raw s3-benchmark output
- Performance metrics
- Timestamp information

#### Summary Report
Location: `./results/performance_summary_{Timestamp}.md`

Content includes:
- Complete test suite results
- Configuration details
- Formatted markdown output
- Historical comparison data

### Overall Performance Score
Calculated as weighted average:
- PUT performance: 40% weight
- GET performance: 40% weight
- DELETE performance: 20% weight

Score interpretation:
- **8-10**: Excellent performance
- **5-7**: Good performance
- **0-4**: Needs optimization

## Customization

### Modifying Test Parameters

#### Edit Test Configuration
```bash
# Open the script for editing
vim s3_benchmark_test.sh

# Key variables to modify:
S3_ENDPOINT="http://localhost:9900"    # Gateway URL
ACCESS_KEY="your_access_key"           # S3 access key
SECRET_KEY="your_secret_key"           # S3 secret key
S3_BENCHMARK_PATH="/path/to/s3-benchmark"  # Tool location
```

#### Customize Test Cases
```bash
# Example: Add new test case
run_benchmark_test "Custom_Test" "custom-bucket" "1M" 4 60 \
    "Custom test with 1MB objects, 4 threads, 60 seconds"
```

#### Modify Performance Thresholds
```bash
# Edit analyze_performance function
# Change threshold values:
if (( $(echo "$put_ops > 2000" | bc -l) )); then  # Raise PUT threshold
    echo -e "  PUT: ${GREEN}✓ Excellent${NC}"
```

### Advanced Configuration

#### Custom Endpoint Testing
```bash
# Test against different endpoints
S3_ENDPOINT="https://your-gateway.example.com:9443"
```

#### Credential Management
```bash
# Use environment variables
export S3_ACCESS_KEY="your_key"
export S3_SECRET_KEY="your_secret"

# Reference in script
ACCESS_KEY="${S3_ACCESS_KEY:-AqU4axe4feDyIielarPI}"
SECRET_KEY="${S3_SECRET_KEY:-0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt}"
```

#### Result Directory Customization
```bash
# Change result location
RESULTS_DIR="/path/to/custom/results"
```

## Troubleshooting

### Common Issues and Solutions

#### 1. s3-benchmark Not Found
```
Error: s3-benchmark not found at /path/to/s3-benchmark
```

**Solution:**
```bash
# Verify path
ls -la /home/oppo/Documents/s3-benchmark/s3-benchmark

# Rebuild if necessary
cd /home/oppo/Documents/s3-benchmark
go build

# Update path in script if different
```

#### 2. Gateway Connection Failed
```
Error: S3 Gateway is not running at http://localhost:9900
```

**Solution:**
```bash
# Check gateway status
ps aux | grep curvine-s3-gateway

# Check port binding
netstat -tlnp | grep 9900

# Start gateway
cd /home/oppo/Documents/curvine
./build/dist/bin/curvine-s3-gateway.sh start

# Check logs
tail -f ./build/dist/logs/curvine-s3-gateway.out
```

#### 3. Authentication Errors
```
Upload status 403 Forbidden
```

**Solution:**
```bash
# Verify credentials in script match gateway configuration
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY

# Check gateway authentication logs
tail -f ./build/dist/logs/curvine-s3-gateway.out | grep -i auth
```

#### 4. Performance Analysis Issues
```
Warning: 'bc' calculator not found. Performance analysis will be limited.
```

**Solution:**
```bash
# Install bc calculator
sudo apt-get install bc

# Verify installation
bc --version
```

#### 5. Permission Errors
```
Permission denied: ./s3_benchmark_test.sh
```

**Solution:**
```bash
# Make script executable
chmod +x s3_benchmark_test.sh

# Verify permissions
ls -la s3_benchmark_test.sh
```

### Debug Mode

#### Enable Verbose Output
```bash
# Add debug flag to script
set -x  # Add at top of script for verbose execution

# Or run with bash debug
bash -x s3_benchmark_test.sh
```

#### Check Individual Components
```bash
# Test s3-benchmark directly
/home/oppo/Documents/s3-benchmark/s3-benchmark \
  -a AqU4axe4feDyIielarPI \
  -s 0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt \
  -u http://localhost:9900 \
  -b debug-test \
  -z 1K -t 1 -d 5

# Test gateway health
curl -v http://localhost:9900/healthz

# Test basic S3 operations with AWS CLI
aws --endpoint-url=http://localhost:9900 s3 ls
```

### Performance Troubleshooting

#### Low Performance Issues
1. **Check System Resources**
   ```bash
   # Monitor during test execution
   htop
   iotop
   ```

2. **Review Gateway Logs**
   ```bash
   tail -f ./build/dist/logs/curvine-s3-gateway.out
   ```

3. **Network Latency**
   ```bash
   # Test network latency
   ping localhost
   ```

4. **Concurrent Connection Limits**
   ```bash
   # Check connection limits
   ulimit -n
   
   # Increase if needed
   ulimit -n 65536
   ```

### Getting Help

#### Log Collection
```bash
# Collect comprehensive logs
mkdir debug-info
cp -r ./results debug-info/
cp ./build/dist/logs/curvine-s3-gateway.out debug-info/
dmesg > debug-info/system.log
ps aux > debug-info/processes.log
netstat -tlnp > debug-info/network.log
```

#### Support Information
When reporting issues, include:
- Script output and error messages
- Gateway logs
- System information (OS, memory, CPU)
- Network configuration
- s3-benchmark version
- Test configuration used