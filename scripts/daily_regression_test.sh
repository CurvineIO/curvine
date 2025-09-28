#!/bin/bash

# Curvine daily regression test script
# Drives daily automated tests and generates an HTML report
# Supports standalone execution by passing project root and result dir

set -e

# Load shared colors and logging helpers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/colors.sh"

# Parse arguments (require exactly two)
if [ $# -lt 2 ]; then
    echo "用法: $0 <project_root> <result_dir>"
    echo "示例: $0 /root/codespace/curvine /root/codespace/result"
    exit 1
fi

PROJECT_ROOT="$1"
RESULTS_DIR="$2"

# Validate project root
if [ ! -d "$PROJECT_ROOT" ]; then
    echo "错误: 指定的项目路径不存在: $PROJECT_ROOT"
    exit 1
fi

# Optional project fingerprint check
if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
    echo "警告: 指定路径可能不是curvine项目（未找到Cargo.toml）"
fi

# Switch into project root so relative paths resolve from there
cd "$PROJECT_ROOT"

# Create result directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TEST_DIR="$RESULTS_DIR/$TIMESTAMP"
mkdir -p "$TEST_DIR"

# Main log file
MAIN_LOG="$TEST_DIR/daily_test.log"

# Append to main log file
log_to_file() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$MAIN_LOG"
}

# Print test start banner
show_test_start() {
    echo ""
    echo "=========================================="
    echo "Curvine Daily Regression Tests"
    echo "=========================================="
    echo "Start time: $(date)"
    echo "Test directory: $TEST_DIR"
    echo "=========================================="
    echo ""
}

# Environment checks
check_environment() {
    log_step "Checking test environment..."
    
    # 检查 Rust
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo is not installed"
        exit 1
    fi
    
    # 检查 jq
    if ! command -v jq &> /dev/null; then
        log_error "jq is not installed"
        exit 1
    fi
    
    # Check required config file
    if [ ! -f "etc/curvine-cluster.toml" ]; then
        log_error "Config file etc/curvine-cluster.toml not found"
        exit 1
    fi
    
    log_success "Environment checks passed"
}

# Start test cluster
start_cluster() {
    log_step "Starting test cluster..."
    
    # Clean up lingering processes and files
    log_info "Cleaning up lingering Curvine processes and files..."
    
    # 1. Kill all known Curvine-related processes (avoid killing this script)
    pkill -f curvine-master 2>/dev/null || true
    pkill -f curvine-worker 2>/dev/null || true
    pkill -f curvine-fuse 2>/dev/null || true
    pkill -f test_cluster 2>/dev/null || true
    
    # 2. Clean testing dir
    if [ -d "testing" ]; then
        log_info "Cleaning testing directory..."
        rm -rf testing/
    fi
    
    # 3. Clean logs and PID files
    if [ -d "logs" ]; then
        log_info "Cleaning logs directory..."
        rm -rf logs/
    fi
    
    # 4. Delete any PID files
    find . -name "*.pid" -delete 2>/dev/null || true
    
    # 5. Wait for processes to exit
    sleep 2
    
    # 6. Verify cleanup
    local remaining_processes=$(ps aux | grep -E "(curvine|test_cluster)" | grep -v grep | wc -l)
    if [ "$remaining_processes" -gt 0 ]; then
        log_warning "$remaining_processes Curvine-related processes still running, forcing kill..."
        pkill -9 -f curvine-master 2>/dev/null || true
        pkill -9 -f curvine-worker 2>/dev/null || true
        pkill -9 -f curvine-fuse 2>/dev/null || true
        pkill -9 -f test_cluster 2>/dev/null || true
        sleep 1
    fi
    
    log_success "Cleanup finished"
    
    # Create logs dir
    mkdir -p logs
    
    # Launch mini cluster
    log_info "Launching test cluster..."
    # Set environment variable to ensure consistent config file path
    export CURVINE_CONF_FILE="$PROJECT_ROOT/testing/curvine-cluster.toml"
    nohup cargo run --example test_cluster --package curvine-tests > "$TEST_DIR/cluster.log" 2>&1 &
    CLUSTER_PID=$!
    echo "$CLUSTER_PID" > "logs/test_cluster.pid"
    
    # Wait for cluster readiness
    log_info "Waiting for cluster to start..."
    
    # Use cluster readiness checker
    if bash "$SCRIPT_DIR/check_cluster_status.sh" "$TEST_DIR/cluster.log" 1800 "$CLUSTER_PID"; then
        log_success "Cluster started (PID: $CLUSTER_PID)"
    elif [ $? -eq 2 ]; then
        log_warning "Cluster partially ready, waiting more..."
        sleep 10
        if bash "$SCRIPT_DIR/check_cluster_status.sh" "$TEST_DIR/cluster.log" 30 "$CLUSTER_PID"; then
            log_success "Cluster started (PID: $CLUSTER_PID)"
        else
            log_error "Cluster start failed or timed out"
            exit 1
        fi
    else
        log_error "Cluster failed to start"
        exit 1
    fi
}

# Run tests
run_tests() {
    log_step "Running test suites..."
    
    # Temporarily disable set -e to continue on failed tests
    set +e
    
    # Test groups mapped to test files
    declare -A test_categories
    test_categories["fs_test"]="fs_test"
    test_categories["block_test"]="block_test"
    test_categories["cluster_test"]="cluster_test"
    test_categories["mount_test"]="mount_test"
    test_categories["replication_test"]="replication_test"
    test_categories["ttl_test"]="ttl_test"
    test_categories["timeout_test"]="timeout_test"
    test_categories["load_client_test"]="load_client_test"
    test_categories["unified_test"]="unified_test"
    # 注意: bench_test 中的测试函数被注释，暂不包含
    
    local total_tests=0
    local passed_tests=0
    local failed_tests=0
    local test_results=()
    
    for category in "${!test_categories[@]}"; do
        test_pattern="${test_categories[$category]}"
        log_info "Running $category ($test_pattern)..."
        
        if CURVINE_CONF_FILE="$PROJECT_ROOT/testing/curvine-cluster.toml" cargo test --package curvine-tests --test "$test_pattern" -- --nocapture > "$TEST_DIR/${test_pattern}.log" 2>&1; then
            log_success "$category passed"
            test_results+=("$category:PASSED")
            ((passed_tests++))
        else
            log_error "$category failed"
            test_results+=("$category:FAILED")
            ((failed_tests++))
        fi
        ((total_tests++))
    done
    
    # Persist test summary JSON
    cat > "$TEST_DIR/test_summary.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "total_tests": $total_tests,
    "passed_tests": $passed_tests,
    "failed_tests": $failed_tests,
    "success_rate": $(( passed_tests * 100 / (total_tests > 0 ? total_tests : 1) )),
    "results": [
EOF
    
    local first=true
    for result in "${test_results[@]}"; do
        IFS=':' read -r category status <<< "$result"
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$TEST_DIR/test_summary.json"
        fi
        echo "        {\"category\": \"$category\", \"status\": \"$status\"}" >> "$TEST_DIR/test_summary.json"
    done
    
    echo "    ]" >> "$TEST_DIR/test_summary.json"
    echo "}" >> "$TEST_DIR/test_summary.json"
    
    log_success "Tests finished - total: $total_tests, passed: $passed_tests, failed: $failed_tests"
    
    # Re-enable set -e
    set -e
}

# Generate HTML report
generate_html_report() {
    log_step "Generating HTML report..."
    
    # Read test summary
    local total=$(jq -r '.total_tests' "$TEST_DIR/test_summary.json")
    local passed=$(jq -r '.passed_tests' "$TEST_DIR/test_summary.json")
    local failed=$(jq -r '.failed_tests' "$TEST_DIR/test_summary.json")
    local success_rate=$(jq -r '.success_rate' "$TEST_DIR/test_summary.json")
    
    # Build HTML report
    cat > "$TEST_DIR/daily_test_report.html" << EOF
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Curvine Daily Regression Test Report - $TIMESTAMP</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 30px; text-align: center; }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .summary-card { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }
        .summary-card h3 { color: #666; margin-bottom: 10px; }
        .summary-card .number { font-size: 2.5em; font-weight: bold; }
        .total { color: #3498db; }
        .passed { color: #27ae60; }
        .failed { color: #e74c3c; }
        .success-rate { color: #f39c12; }
        .results { background: white; border-radius: 10px; padding: 30px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .results h2 { color: #2c3e50; margin-bottom: 20px; }
        .result-item { display: flex; justify-content: space-between; align-items: center; padding: 15px; border-bottom: 1px solid #eee; }
        .result-item:last-child { border-bottom: none; }
        .status-badge { padding: 5px 15px; border-radius: 20px; font-weight: bold; }
        .status-passed { background: #d4edda; color: #155724; }
        .status-failed { background: #f8d7da; color: #721c24; }
        .footer { text-align: center; margin-top: 40px; padding: 20px; color: #666; border-top: 1px solid #e0e0e0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Curvine Daily Regression Test Report</h1>
            <div>Generated at: $(date '+%Y-%m-%d %H:%M:%S')</div>
        </div>
        
        <div class="summary">
            <div class="summary-card">
                <h3>Total</h3>
                <div class="number total">$total</div>
            </div>
            <div class="summary-card">
                <h3>Passed</h3>
                <div class="number passed">$passed</div>
            </div>
            <div class="summary-card">
                <h3>Failed</h3>
                <div class="number failed">$failed</div>
            </div>
            <div class="summary-card">
                <h3>Success Rate</h3>
                <div class="number success-rate">${success_rate}%</div>
            </div>
        </div>
        
        <div class="results">
            <h2>Test Results</h2>
EOF
    
    # 添加测试结果
    jq -r '.results[] | "\(.category)|\(.status)"' "$TEST_DIR/test_summary.json" | while IFS='|' read -r category status; do
        if [ "$status" = "PASSED" ]; then
            status_class="status-passed"
            status_text="通过"
        else
            status_class="status-failed"
            status_text="失败"
        fi
        
        cat >> "$TEST_DIR/daily_test_report.html" << EOF
            <div class="result-item">
                <span><strong>$category</strong></span>
                <span class="status-badge $status_class">$status_text</span>
            </div>
EOF
    done
    
    cat >> "$TEST_DIR/daily_test_report.html" << EOF
        </div>
        
        <div class="footer">
            <p>报告生成时间: $(date '+%Y-%m-%d %H:%M:%S')</p>
            <p>Curvine 每日回归测试系统</p>
        </div>
    </div>
</body>
</html>
EOF
    
    log_success "HTML报告已生成: $TEST_DIR/daily_test_report.html"
}

# 清理资源
cleanup() {
    log_step "Cleaning test resources..."
    
    # 停止测试集群
    if [ -f "logs/test_cluster.pid" ]; then
        CLUSTER_PID=$(cat "logs/test_cluster.pid")
        if ps -p "$CLUSTER_PID" > /dev/null 2>&1; then
            log_info "Stopping test cluster (PID: $CLUSTER_PID)"
            kill "$CLUSTER_PID" 2>/dev/null || true
            sleep 2
        fi
        rm -f "logs/test_cluster.pid"
    fi
    
    log_success "Resource cleanup finished"
}

# 显示测试结果
show_results() {
    log_step "Showing test results..."
    
    echo ""
    echo "=========================================="
    echo "📊 Test results summary"
    echo "=========================================="
    
    if [ -f "$TEST_DIR/test_summary.json" ]; then
        local total=$(jq -r '.total_tests' "$TEST_DIR/test_summary.json")
        local passed=$(jq -r '.passed_tests' "$TEST_DIR/test_summary.json")
        local failed=$(jq -r '.failed_tests' "$TEST_DIR/test_summary.json")
        local success_rate=$(jq -r '.success_rate' "$TEST_DIR/test_summary.json")
        
        echo "Total tests: $total"
        echo "Passed: $passed"
        echo "Failed: $failed"
        echo "Success rate: ${success_rate}%"
        echo ""
        echo "HTML report: $TEST_DIR/daily_test_report.html"
        echo "Detailed logs: $TEST_DIR/"
    fi
    
    echo "=========================================="
}

main() {
    trap 'log_warning "Received interrupt signal, cleaning up..."; cleanup; exit 1' INT TERM
    
    show_test_start
    
    log_to_file "Starting daily regression test"
    
    check_environment
    log_to_file "Environment check completed"

    start_cluster
    log_to_file "Test cluster started"
    
    run_tests
    log_to_file "Test execution completed"
    
    generate_html_report
    log_to_file "HTML report generated"
    
    show_results
    
    cleanup
    log_to_file "Resource cleanup completed"
    
    log_to_file "Daily regression test completed"
    
    echo ""
    log_success "🎉 Daily regression test completed!"
    echo "Report location: $TEST_DIR/daily_test_report.html"
}

main "$@"
