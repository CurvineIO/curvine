#!/bin/bash

# Curvine daily regression test script
# Drives daily automated tests and generates an HTML report
# Supports standalone execution by passing project root and result dir

set -e

# Load shared colors and logging helpers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/colors.sh"

# Parse arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <project_root> <result_dir> [package] [test_file] [test_case]"
    echo "Example: $0 /root/codespace/curvine /root/codespace/result"
    echo "Example: $0 /root/codespace/curvine /root/codespace/result curvine-tests ttl_test test_ttl_cleanup"
    exit 1
fi

PROJECT_ROOT="$1"
RESULTS_DIR="$2"
SPECIFIC_PACKAGE="$3"
SPECIFIC_TEST_FILE="$4"
SPECIFIC_TEST_CASE="$5"

# Validate project root
if [ ! -d "$PROJECT_ROOT" ]; then
    echo "Error: Specified project path does not exist: $PROJECT_ROOT"
    exit 1
fi

# Optional project fingerprint check
if [ ! -f "$PROJECT_ROOT/Cargo.toml" ]; then
    echo "Warning: Specified path may not be a curvine project (Cargo.toml not found)"
fi

# Switch into project root so relative paths resolve from there
cd "$PROJECT_ROOT"

# Create result directory.
# If CURVINE_TEST_DIR is set (passed by build-server.py), write directly to that dir.
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
if [ -n "${CURVINE_TEST_DIR}" ]; then
    TEST_DIR="${CURVINE_TEST_DIR}"
else
    TEST_DIR="$RESULTS_DIR/$TIMESTAMP"
fi
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
    
    # Check Rust
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo is not installed"
        exit 1
    fi
    
    # Check jq
    if ! command -v jq &> /dev/null; then
        log_error "jq is not installed"
        exit 1
    fi
    
    log_success "Environment checks passed"
}

# Run single test and write test_summary.json for build-server (no nextest required)
run_specific_test_and_summary() {
    local package="$1"
    local test_file="$2"
    local test_case="$3"
    log_step "Running specific test: $package/$test_file::$test_case..."
    set +e
    mkdir -p "$TEST_DIR/logs/$package"
    local safe_tf="${test_file//::/_}"
    local safe_tc="${test_case//::/_}"
    local log_file="$TEST_DIR/logs/$package/${safe_tf}_${safe_tc}.log"
    cd "$PROJECT_ROOT"
    if [ "$test_file" = "lib" ]; then
        cargo test --package "$package" --lib -- "$test_case" --exact --nocapture > "$log_file" 2>&1
    else
        cargo test --package "$package" --test "$test_file" -- "$test_case" --exact --nocapture > "$log_file" 2>&1
    fi
    local exit_code=$?
    set -e
    local status="FAILED"
    [ $exit_code -eq 0 ] && status="PASSED"
    local rel_log="logs/$package/${safe_tf}_${safe_tc}.log"
    local success_rate=0
    [ "$status" = "PASSED" ] && success_rate=100
    cat > "$TEST_DIR/test_summary.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "total_tests": 1,
    "passed_tests": $([ "$status" = "PASSED" ] && echo 1 || echo 0),
    "failed_tests": $([ "$status" = "FAILED" ] && echo 1 || echo 0),
    "success_rate": $success_rate,
    "packages": [{"name": "$package", "total": 1, "passed": $([ "$status" = "PASSED" ] && echo 1 || echo 0), "failed": $([ "$status" = "FAILED" ] && echo 1 || echo 0), "success_rate": $success_rate}],
    "test_cases": [{"package": "$package", "test_file": "$test_file", "test_case": "$test_case", "status": "$status", "log": "$rel_log"}]
}
EOF
    [ "$status" = "PASSED" ] && log_success "Test passed" || log_error "Test failed"
    return $exit_code
}

# Cleanup resources

cleanup() {
    log_step "Cleaning test resources..."
    log_success "Resource cleanup finished"
}

# Run tests with nextest and produce test_summary.json + logs for build-server.
# Optional: pass package name to run only that package (e.g. run_tests_nextest "curvine-tests").
run_tests_nextest() {
    local pkg_filter="${1:-}"
    log_step "Running tests with cargo nextest..."
    set +e
    mkdir -p "$TEST_DIR/logs"
    local nextest_log="$TEST_DIR/nextest_run.log"
    local profile_arg=""
    local nextest_profile_dir="default"
    if [ -n "${NEXTEST_CI_NO_UFS}" ]; then
        profile_arg="--profile ci-no-ufs"
        nextest_profile_dir="ci-no-ufs"
    fi
    local filter_arg=""
    if [ -n "$pkg_filter" ]; then
        filter_arg="-E 'package($pkg_filter)'"
        log_info "Filtering to package: $pkg_filter"
    fi
    log_info "Using nextest config: $SCRIPT_DIR/nextest.toml ${profile_arg}"
    cd "$PROJECT_ROOT"
    eval cargo nextest run \
        --config-file "$SCRIPT_DIR/nextest.toml" \
        $profile_arg \
        --no-fail-fast \
        --workspace \
        $filter_arg \
        2>&1 | tee "$nextest_log"
    local nextest_exit=$?
    set -e

    # Copy JUnit from nextest store (target/nextest/<profile>/junit.xml)
    local junit_src="$PROJECT_ROOT/target/nextest/$nextest_profile_dir/junit.xml"
    if [ -f "$junit_src" ]; then
        cp "$junit_src" "$TEST_DIR/junit.xml"
    else
        log_error "JUnit output not found at $junit_src"
        return 1
    fi

    # Parse skipped count from nextest summary line (e.g. "12 passed, 3 skipped")
    local skipped_count=0
    if grep -qE "[0-9]+ skipped" "$nextest_log"; then
        skipped_count=$(grep -oE "[0-9]+ skipped" "$nextest_log" | tail -1 | grep -oE "[0-9]+" || echo 0)
    fi

    # Convert JUnit to test_summary.json and per-test logs
    local converter="$SCRIPT_DIR/nextest_junit_to_summary.py"
    if [ ! -f "$converter" ]; then
        log_error "Converter script not found: $converter"
        return 1
    fi
    python3 "$converter" "$TEST_DIR" --skipped-count "$skipped_count"
    local conv_exit=$?
    if [ $conv_exit -ne 0 ]; then
        log_error "Failed to convert JUnit to test_summary.json"
        return 1
    fi

    # Exit with failure if nextest had failures
    if [ $nextest_exit -ne 0 ]; then
        log_error "Nextest reported failures (exit $nextest_exit)"
        return 1
    fi
    log_success "Nextest run completed; test_summary.json and logs written"
    return 0
}

# Show test results
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
        local skipped=$(jq -r '.skipped_tests // empty' "$TEST_DIR/test_summary.json")
        if [ -n "$skipped" ] && [ "$skipped" != "null" ]; then
            echo "Skipped (e.g. UFS not available): $skipped"
        fi
        echo "Success rate: ${success_rate}%"
        echo ""
        
        # Show package statistics (new JSON structure)
        echo "Package statistics:"
        jq -r '.packages[] | "  - \(.name): \(.passed)/\(.total) passed (\(.success_rate)%)"' "$TEST_DIR/test_summary.json"
        
        echo ""
        echo "JSON summary: $TEST_DIR/test_summary.json"
        echo "Detailed logs: $TEST_DIR/logs/"
    fi
    
    echo "=========================================="
}

main() {
    trap 'log_warning "Received interrupt signal, cleaning up..."; cleanup; exit 1' INT TERM
    
    show_test_start
    
    log_to_file "Starting daily regression test"
    
    check_environment
    log_to_file "Environment check completed"
    
    # Check whether to run a single specific test (uses cargo test, no nextest required)
    if [ -n "$SPECIFIC_PACKAGE" ] && [ -n "$SPECIFIC_TEST_FILE" ] && [ -n "$SPECIFIC_TEST_CASE" ]; then
        log_info "Running specific test: $SPECIFIC_PACKAGE/$SPECIFIC_TEST_FILE::$SPECIFIC_TEST_CASE"
        if run_specific_test_and_summary "$SPECIFIC_PACKAGE" "$SPECIFIC_TEST_FILE" "$SPECIFIC_TEST_CASE"; then
            log_to_file "Specific test passed"
        else
            log_to_file "Specific test failed"
            exit 1
        fi
    else
        # All tests or single package: require nextest (per-package discovery mode removed)
        if ! cargo nextest --version &>/dev/null 2>&1; then
            log_error "cargo nextest is required. Install with: cargo install cargo-nextest --locked"
            exit 1
        fi
        if [ -n "$SPECIFIC_PACKAGE" ]; then
            log_info "Run tests only for specified package: $SPECIFIC_PACKAGE"
            if run_tests_nextest "$SPECIFIC_PACKAGE"; then
                log_to_file "Nextest run (package $SPECIFIC_PACKAGE) completed"
            else
                log_to_file "Nextest run finished with failures"
                exit 1
            fi
        else
            log_info "Running all tests with cargo nextest"
            if run_tests_nextest; then
                log_to_file "Nextest run completed"
            else
                log_to_file "Nextest run finished with failures"
                exit 1
            fi
        fi
    fi

    # Only output JSON, HTML by service rendered
    show_results
    
    cleanup
    log_to_file "Resource cleanup completed"
    
    log_to_file "Daily regression test completed"
    
    echo ""
    log_success "🎉 Daily regression test completed!"
    echo "JSON summary: $TEST_DIR/test_summary.json"
}

main "$@"

