# Curvine S3 Gateway Performance Benchmark

This directory contains performance testing tools for the Curvine S3 Gateway using the industry-standard `s3-benchmark` tool.

## Overview

The Curvine S3 Gateway implements AWS S3-compatible API with both Signature V4 and V2 authentication support. This benchmark suite validates performance characteristics and ensures compatibility with standard S3 tools.

## Quick Start

### Prerequisites

1. **s3-benchmark tool**: Download and build from [Wasabi s3-benchmark](https://github.com/wasabi-tech/s3-benchmark)
   ```bash
   cd /home/oppo/Documents
   git clone https://github.com/wasabi-tech/s3-benchmark.git
   cd s3-benchmark
   go build
   ```

2. **Curvine S3 Gateway**: Ensure the gateway is running
   ```bash
   cd /home/oppo/Documents/curvine
   ./build/dist/bin/curvine-s3-gateway.sh start
   ```

3. **System dependencies**: Install bc calculator for performance analysis
   ```bash
   sudo apt-get install bc
   ```

### Running Benchmarks

Execute the comprehensive benchmark test suite:

```bash
cd /home/oppo/Documents/curvine/benchmark
./s3_benchmark_test.sh
```

## Test Suite Description

The benchmark suite includes 5 comprehensive tests:

### 1. Quick Performance Test
- **Object Size**: 1KB
- **Threads**: 1
- **Duration**: 5 seconds
- **Purpose**: Fast validation and basic performance check

### 2. Medium Performance Test  
- **Object Size**: 10KB
- **Threads**: 3
- **Duration**: 15 seconds
- **Purpose**: Moderate workload with some concurrency

### 3. Concurrency Test
- **Object Size**: 5KB
- **Threads**: 5
- **Duration**: 20 seconds
- **Purpose**: High concurrency evaluation

### 4. Large Object Test
- **Object Size**: 100KB
- **Threads**: 2
- **Duration**: 30 seconds
- **Purpose**: Throughput testing with large objects

### 5. Stress Test
- **Object Size**: 50KB
- **Threads**: 8
- **Duration**: 25 seconds
- **Purpose**: Maximum load testing

## Performance Analysis

The script provides real-time performance analysis:

### Metrics Analyzed
- **PUT Operations**: Upload performance and throughput
- **GET Operations**: Download performance and throughput  
- **DELETE Operations**: Deletion performance
- **Overall Score**: Weighted performance score

### Performance Ratings
- **✓ Excellent**: High performance, optimal for production
- **○ Good**: Acceptable performance, minor optimizations possible
- **△ Needs optimization**: Performance issues identified

## Results and Reporting

### Output Files
All results are saved to the `./results/` directory:

- **Individual Test Results**: `{TestName}_{Timestamp}.txt`
- **Summary Report**: `performance_summary_{Timestamp}.md`

### Result Analysis
The script automatically:
1. Analyzes individual test performance
2. Calculates overall performance scores
3. Generates comprehensive markdown reports
4. Provides optimization recommendations

## What This Validates

### ✅ Functional Validation
- AWS Signature V2 authentication compatibility
- PUT/GET/DELETE operations functionality
- ListObjectVersions API implementation
- Concurrent request handling
- Error handling and recovery

### ✅ Performance Validation
- Throughput characteristics
- Latency measurements
- Concurrency scalability
- Resource utilization efficiency

## Configuration

### Default Settings
```bash
S3_ENDPOINT="http://localhost:9900"
ACCESS_KEY="AqU4axe4feDyIielarPI"
SECRET_KEY="0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt"
```

### Customization
Modify the script variables to:
- Change endpoint URL
- Update credentials
- Adjust test parameters
- Modify result directories

## Troubleshooting

### Common Issues

1. **s3-benchmark not found**
   ```bash
   # Ensure s3-benchmark is built and path is correct
   ls -la /home/oppo/Documents/s3-benchmark/s3-benchmark
   ```

2. **Gateway not running**
   ```bash
   # Check gateway status
   curl http://localhost:9900/healthz
   
   # Start if needed
   ./build/dist/bin/curvine-s3-gateway.sh start
   ```

3. **Permission errors**
   ```bash
   # Ensure script is executable
   chmod +x s3_benchmark_test.sh
   ```

4. **bc calculator missing**
   ```bash
   # Install bc for performance calculations
   sudo apt-get install bc
   ```

## Performance Baselines

### Expected Performance Ranges

| Test Type | PUT (ops/sec) | GET (ops/sec) | DELETE (ops/sec) |
|-----------|---------------|---------------|------------------|
| Small Objects (1KB) | 500-1000+ | 1000-2000+ | 5000-10000+ |
| Medium Objects (10KB) | 200-500 | 500-1500 | 5000-20000+ |
| Large Objects (100KB) | 50-200 | 1000-5000+ | 5000-15000+ |

### Performance Factors
- **Hardware**: CPU, memory, and storage performance
- **Network**: Latency and bandwidth characteristics
- **Concurrency**: Thread count and connection pooling
- **Object Size**: Larger objects typically have lower ops/sec but higher throughput

## Integration with CI/CD

The benchmark script is designed for automation:

```bash
# Run benchmarks and check exit code
./s3_benchmark_test.sh
if [ $? -eq 0 ]; then
    echo "Benchmarks passed"
else
    echo "Benchmarks failed"
    exit 1
fi
```

## Contributing

When adding new benchmark tests:
1. Follow the existing test structure
2. Add performance analysis logic
3. Update documentation
4. Ensure results are saved properly
5. Add appropriate validation criteria

## Support

For issues with:
- **Curvine S3 Gateway**: Check gateway logs and configuration
- **s3-benchmark tool**: Refer to upstream documentation
- **Performance issues**: Review system resources and configuration
- **Test failures**: Check network connectivity and credentials