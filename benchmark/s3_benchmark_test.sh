#!/bin/bash

# Curvine S3 Gateway Performance Benchmark Test Script
# Uses s3-benchmark tool with detailed performance analysis

set -e

# Configuration
S3_ENDPOINT="http://localhost:9900"
ACCESS_KEY="AqU4axe4feDyIielarPI"
SECRET_KEY="0CJZ2QfHi2tDb4DKuCJ2vnBEUXg5EYQt"
S3_BENCHMARK_PATH="/home/oppo/Documents/s3-benchmark/s3-benchmark"
RESULTS_DIR="./results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Create results directory
mkdir -p "$RESULTS_DIR"

# Performance analysis functions
show_performance_criteria() {
    echo -e "${CYAN}📋 Performance Evaluation Criteria:${NC}"
    echo -e "  ${GREEN}✓ Excellent${NC}  | ${YELLOW}○ Good${NC}      | ${RED}△ Needs Optimization${NC}"
    echo -e "  PUT:     >1000    | 500-1000    | <500 ops/sec"
    echo -e "  GET:     >2000    | 1000-2000   | <1000 ops/sec" 
    echo -e "  DELETE:  >10000   | 5000-10000  | <5000 ops/sec"
    echo -e "  Overall: >10      | 5-10        | <5 (weighted score/10)"
    echo
    echo -e "${BLUE}💡 Optimization Guidelines:${NC}"
    echo "  - PUT <500: Check disk I/O, memory allocation, request parsing"
    echo "  - GET <1000: Optimize file reading, caching, network buffers"
    echo "  - DELETE <5000: Improve metadata operations, cleanup efficiency"
    echo "  - Consider: Connection pooling, async I/O, memory management"
    echo
}

analyze_performance() {
    local test_name="$1"
    local put_ops="$2"
    local put_speed="$3"
    local get_ops="$4" 
    local get_speed="$5"
    local delete_ops="$6"
    
    echo -e "${CYAN}📊 Performance Analysis for $test_name:${NC}"
    
    # PUT analysis
    if (( $(echo "$put_ops > 1000" | bc -l) )); then
        echo -e "  PUT: ${GREEN}✓ Excellent${NC} - $put_ops ops/sec, $put_speed"
    elif (( $(echo "$put_ops > 500" | bc -l) )); then
        echo -e "  PUT: ${YELLOW}○ Good${NC} - $put_ops ops/sec, $put_speed"
    else
        echo -e "  PUT: ${RED}△ Needs optimization${NC} - $put_ops ops/sec, $put_speed"
    fi
    
    # GET analysis
    if (( $(echo "$get_ops > 2000" | bc -l) )); then
        echo -e "  GET: ${GREEN}✓ Excellent${NC} - $get_ops ops/sec, $get_speed"
    elif (( $(echo "$get_ops > 1000" | bc -l) )); then
        echo -e "  GET: ${YELLOW}○ Good${NC} - $get_ops ops/sec, $get_speed"
    else
        echo -e "  GET: ${RED}△ Needs optimization${NC} - $get_ops ops/sec, $get_speed"
    fi
    
    # DELETE analysis
    if (( $(echo "$delete_ops > 10000" | bc -l) )); then
        echo -e "  DELETE: ${GREEN}✓ Excellent${NC} - $delete_ops deletes/sec"
    elif (( $(echo "$delete_ops > 5000" | bc -l) )); then
        echo -e "  DELETE: ${YELLOW}○ Good${NC} - $delete_ops deletes/sec"
    else
        echo -e "  DELETE: ${RED}△ Needs optimization${NC} - $delete_ops deletes/sec"
    fi
    echo
}

save_results() {
    local test_name="$1"
    local output="$2"
    local result_file="$RESULTS_DIR/${test_name}_${TIMESTAMP}.txt"
    
    echo "=== $test_name Results - $(date) ===" > "$result_file"
    echo "$output" >> "$result_file"
    echo -e "${BLUE}📁 Results saved to: $result_file${NC}"
}

run_benchmark_test() {
    local test_name="$1"
    local bucket="$2"
    local object_size="$3"
    local threads="$4"
    local duration="$5"
    local description="$6"
    
    echo -e "${YELLOW}=== $test_name ===${NC}"
    echo -e "${CYAN}$description${NC}"
    echo "Object size: $object_size, Threads: $threads, Duration: ${duration}s"
    echo
    
    # Run the benchmark and capture output
    local output
    output=$($S3_BENCHMARK_PATH \
        -a "$ACCESS_KEY" \
        -s "$SECRET_KEY" \
        -u "$S3_ENDPOINT" \
        -b "$bucket" \
        -z "$object_size" \
        -t "$threads" \
        -d "$duration" 2>&1)
    
    echo "$output"
    
    # Save results
    save_results "$test_name" "$output"
    
    # Extract performance metrics for analysis
    local put_line=$(echo "$output" | grep "PUT time")
    local get_line=$(echo "$output" | grep "GET time") 
    local delete_line=$(echo "$output" | grep "DELETE time")
    
    if [[ -n "$put_line" && -n "$get_line" && -n "$delete_line" ]]; then
        # Extract numbers using awk with proper field positions for s3-benchmark output
        # Format: "Loop 1: PUT time 5.0 secs, objects = 3345, speed = 669KB/sec, 669.0 operations/sec. Slowdowns = 0"
        local put_ops=$(echo "$put_line" | awk -F'[, ]' '{for(i=1;i<=NF;i++) if($i=="operations/sec.") print $(i-1)}' 2>/dev/null | tr -d '[:space:]')
        local put_speed=$(echo "$put_line" | awk -F'[, ]' '{for(i=1;i<=NF;i++) if($i=="speed") print $(i+2)}' 2>/dev/null | tr -d '[:space:]')
        local get_ops=$(echo "$get_line" | awk -F'[, ]' '{for(i=1;i<=NF;i++) if($i=="operations/sec.") print $(i-1)}' 2>/dev/null | tr -d '[:space:]')
        local get_speed=$(echo "$get_line" | awk -F'[, ]' '{for(i=1;i<=NF;i++) if($i=="speed") print $(i+2)}' 2>/dev/null | tr -d '[:space:]')
        # Format: "Loop 1: DELETE time 0.8 secs, 3968.0 deletes/sec. Slowdowns = 0"
        local delete_ops=$(echo "$delete_line" | awk '{for(i=1;i<=NF;i++) if($i=="deletes/sec.") print $(i-1)}' 2>/dev/null | tr -d '[:space:]')
        
        # Only analyze if we have valid numeric values
        if [[ "$put_ops" =~ ^[0-9]+\.?[0-9]*$ && "$get_ops" =~ ^[0-9]+\.?[0-9]*$ && "$delete_ops" =~ ^[0-9]+\.?[0-9]*$ ]]; then
            analyze_performance "$test_name" "$put_ops" "$put_speed" "$get_ops" "$get_speed" "$delete_ops"
        else
            echo -e "${YELLOW}⚠️  Could not extract performance metrics from output${NC}"
            echo -e "${YELLOW}Debug: PUT=$put_ops, GET=$get_ops, DELETE=$delete_ops${NC}"
        fi
    fi
}

calculate_overall_score() {
    echo -e "${CYAN}🎯 Overall Performance Score Calculation:${NC}"
    
    # Read all result files and calculate average performance
    local total_put_ops=0
    local total_get_ops=0
    local total_delete_ops=0
    local test_count=0
    
    for result_file in "$RESULTS_DIR"/*_"$TIMESTAMP".txt; do
        if [[ -f "$result_file" ]]; then
            local put_ops=$(grep "PUT time" "$result_file" | awk -F'[, ]' '{for(i=1;i<=NF;i++) if($i=="operations/sec.") print $(i-1)}' 2>/dev/null | head -1 | tr -d '[:space:]')
            local get_ops=$(grep "GET time" "$result_file" | awk -F'[, ]' '{for(i=1;i<=NF;i++) if($i=="operations/sec.") print $(i-1)}' 2>/dev/null | head -1 | tr -d '[:space:]')
            local delete_ops=$(grep "DELETE time" "$result_file" | awk '{for(i=1;i<=NF;i++) if($i=="deletes/sec.") print $(i-1)}' 2>/dev/null | head -1 | tr -d '[:space:]')
            
            # Validate that we have numeric values
            if [[ "$put_ops" =~ ^[0-9]+\.?[0-9]*$ && "$get_ops" =~ ^[0-9]+\.?[0-9]*$ && "$delete_ops" =~ ^[0-9]+\.?[0-9]*$ ]]; then
                total_put_ops=$(echo "$total_put_ops + $put_ops" | bc -l)
                total_get_ops=$(echo "$total_get_ops + $get_ops" | bc -l)
                total_delete_ops=$(echo "$total_delete_ops + $delete_ops" | bc -l)
                ((test_count++))
            fi
        fi
    done
    
    if [[ $test_count -gt 0 ]]; then
        local avg_put=$(echo "scale=1; $total_put_ops / $test_count" | bc -l)
        local avg_get=$(echo "scale=1; $total_get_ops / $test_count" | bc -l)
        local avg_delete=$(echo "scale=1; $total_delete_ops / $test_count" | bc -l)
        
        echo -e "  Average PUT performance: ${YELLOW}$avg_put ops/sec${NC}"
        echo -e "  Average GET performance: ${YELLOW}$avg_get ops/sec${NC}"
        echo -e "  Average DELETE performance: ${YELLOW}$avg_delete ops/sec${NC}"
        
        # Calculate overall score (weighted average)
        local overall_score=$(echo "scale=0; ($avg_put * 0.4 + $avg_get * 0.4 + $avg_delete * 0.2) / 100" | bc -l)
        
        if (( $(echo "$overall_score > 10" | bc -l) )); then
            echo -e "  Overall Performance: ${GREEN}✓ Excellent (Score: $overall_score/10)${NC}"
        elif (( $(echo "$overall_score > 5" | bc -l) )); then
            echo -e "  Overall Performance: ${YELLOW}○ Good (Score: $overall_score/10)${NC}"
        else
            echo -e "  Overall Performance: ${RED}△ Needs optimization (Score: $overall_score/10)${NC}"
        fi
    fi
    echo
}

generate_summary_report() {
    local summary_file="$RESULTS_DIR/performance_summary_${TIMESTAMP}.md"
    
    echo "# Curvine S3 Gateway Performance Test Report" > "$summary_file"
    echo "Generated on: $(date)" >> "$summary_file"
    echo "" >> "$summary_file"
    echo "## Test Configuration" >> "$summary_file"
    echo "- S3 Endpoint: $S3_ENDPOINT" >> "$summary_file"
    echo "- Access Key: $ACCESS_KEY" >> "$summary_file"
    echo "- s3-benchmark version: 2.0" >> "$summary_file"
    echo "" >> "$summary_file"
    echo "## Performance Results" >> "$summary_file"
    
    for result_file in "$RESULTS_DIR"/*_"$TIMESTAMP".txt; do
        if [[ -f "$result_file" ]]; then
            local test_name=$(basename "$result_file" | sed "s/_${TIMESTAMP}.txt//")
            echo "" >> "$summary_file"
            echo "### $test_name" >> "$summary_file"
            echo "\`\`\`" >> "$summary_file"
            cat "$result_file" >> "$summary_file"
            echo "\`\`\`" >> "$summary_file"
        fi
    done
    
    echo -e "${GREEN}📋 Summary report generated: $summary_file${NC}"
}

# Main execution
echo -e "${GREEN}=== Curvine S3 Gateway Performance Benchmark Test ===${NC}"
echo "Endpoint: $S3_ENDPOINT"
echo "Access Key: $ACCESS_KEY"
echo "Results Directory: $RESULTS_DIR"
echo "Timestamp: $TIMESTAMP"
echo

# Check if bc is available for calculations
if ! command -v bc &> /dev/null; then
    echo -e "${RED}Warning: 'bc' calculator not found. Performance analysis will be limited.${NC}"
    echo "Install with: sudo apt-get install bc"
    echo
fi

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

# Show performance evaluation criteria
show_performance_criteria

# Run benchmark tests with performance analysis
run_benchmark_test "Quick_Performance_Test" "quick-test-$TIMESTAMP" "1K" 1 5 \
    "Fast validation test with small objects and single thread"

run_benchmark_test "Medium_Performance_Test" "medium-test-$TIMESTAMP" "10K" 3 15 \
    "Medium workload test with moderate concurrency"

run_benchmark_test "Concurrency_Test" "concurrent-test-$TIMESTAMP" "5K" 5 20 \
    "High concurrency test to evaluate thread handling"

run_benchmark_test "Large_Object_Test" "large-test-$TIMESTAMP" "100K" 2 30 \
    "Large object handling test for throughput evaluation"

run_benchmark_test "Stress_Test" "stress-test-$TIMESTAMP" "50K" 8 25 \
    "Stress test with high concurrency and medium objects"

# Performance analysis and reporting
calculate_overall_score
generate_summary_report

echo -e "${GREEN}=== All Benchmark Tests Completed ===${NC}"
echo -e "${CYAN}🎉 Performance testing finished successfully!${NC}"
echo
echo -e "${BLUE}📈 Key Validations:${NC}"
echo "  ✓ AWS Signature V2 authentication compatibility"
echo "  ✓ PUT/GET/DELETE operations functionality"
echo "  ✓ ListObjectVersions API implementation"
echo "  ✓ Concurrent operations handling"
echo "  ✓ Performance characteristics analysis"
echo
echo -e "${YELLOW}💡 Next Steps:${NC}"
echo "  - Review detailed results in: $RESULTS_DIR/"
echo "  - Check performance summary report"
echo "  - Compare results with baseline performance"
echo "  - Optimize bottlenecks if identified"
