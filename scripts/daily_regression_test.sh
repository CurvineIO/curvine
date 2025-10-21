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

# Run all tests (including integration and unit tests)
run_all_tests() {
    log_step "Running all tests in workspace..."
    
    # Temporarily disable set -e to continue on test failures
    set +e
    
    # Create test result directory
    mkdir -p "$TEST_DIR/logs"
    local all_tests_log="$TEST_DIR/logs/all_tests.log"
    
    # Run all tests with --all-targets to include all types of tests
    log_info "Running 'cargo test' for all packages with all targets..."
    cd "$PROJECT_ROOT" && cargo test --all-targets -- --nocapture > "$all_tests_log" 2>&1
    
    log_success "All tests completed. See $all_tests_log for details"
    return 0
}

# Run package tests
run_package_tests() {
    local package="$1"
    
    log_info "Running all tests for package $package..."
    
    # Temporarily disable set -e to continue on test failures
    set +e
    
    # Create test result directory
    mkdir -p "$TEST_DIR/logs/$package"
    local log_file="$TEST_DIR/logs/$package/all_tests.log"
    
    # Run all tests in the package
    log_info "Running all tests in package $package"
    cd "$PROJECT_ROOT" && cargo test --package "$package" --all-targets -- --nocapture > "$log_file" 2>&1
    local exit_code=$?
    
    # Parse test results
    if [ $exit_code -eq 0 ]; then
        local test_count=$(grep -c "test result:" "$log_file" || echo 0)
        if [ "$test_count" -gt 0 ]; then
            # Extract test summary
            local test_summary=$(grep "test result:" "$log_file" | head -1)
            local tests_run=$(echo "$test_summary" | grep -oP '\d+(?= tests)' || echo 0)
            local tests_passed=$(echo "$test_summary" | grep -oP '\d+(?= passed)' || echo 0)
            local tests_failed=$(echo "$test_summary" | grep -oP '\d+(?= failed)' || echo 0)
            
            # If no tests ran but there are passed tests, set run to passed
            tests_run=${tests_run:-0}
            tests_passed=${tests_passed:-0}
            tests_failed=${tests_failed:-0}
            
            # Compute success rate
            local success_rate=0
            if [ "$tests_run" -gt 0 ]; then
                success_rate=$(( tests_passed * 100 / tests_run ))
            fi
            
            if [ "$tests_failed" -eq 0 ]; then
                log_success "$package: All $tests_run tests passed"
                module_results+=("$package:$tests_run:$tests_passed:$tests_failed:$success_rate")
            else
                log_error "$package: $tests_failed of $tests_run tests failed"
                module_results+=("$package:$tests_run:$tests_passed:$tests_failed:$success_rate")
            fi
            
            # Parse detailed test results
            local all_tests=$(grep -E "^test .* ... (ok|FAILED)" "$log_file" || echo "")
            
            # Create module test count associative arrays
            declare -A module_test_counts
            declare -A module_fail_counts
            
            if [ ! -z "$all_tests" ]; then
                while IFS= read -r line; do
                    # Parse test name and result
                    local test_name=$(echo "$line" | grep -oP 'test \K[^:]+(?= ... )')
                    local test_result=$(echo "$line" | grep -oP ' ... \K(ok|FAILED)')
                    
                    if [ ! -z "$test_name" ] && [ ! -z "$test_result" ]; then
                        # Parse module path
                        local module_path=$(echo "$test_name" | grep -oP '^[^:]+(?=::)' || echo "default")
                        
                        # Create test log file
                        local test_log_file="$TEST_DIR/logs/$package/${test_name//\//_}.log"
                        grep -A 50 "$test_name" "$log_file" > "$test_log_file" 2>/dev/null
                        
                        # Increment module test count
                        module_test_counts["$module_path"]=$((${module_test_counts["$module_path"]:-0} + 1))
                        
                        if [ "$test_result" = "ok" ]; then
                            test_results+=("$package::$test_name:PASSED:$test_log_file")
                        else
                            test_results+=("$package::$test_name:FAILED:$test_log_file")
                            # Increment module fail count
                            module_fail_counts["$module_path"]=$((${module_fail_counts["$module_path"]:-0} + 1))
                        fi
                    fi
                done <<< "$all_tests"
                
                # Output each module's test stats
                for module in "${!module_test_counts[@]}"; do
                    local total_tests=${module_test_counts["$module"]}
                    local failed_tests=${module_fail_counts["$module"]:-0}
                    local passed_tests=$((total_tests - failed_tests))
                    local success_rate=0
                    
                    if [ "$total_tests" -gt 0 ]; then
                        success_rate=$(( passed_tests * 100 / total_tests ))
                    fi
                    
                    log_info "Module $module: total $total_tests, passed $passed_tests, failed $failed_tests, success rate $success_rate%"
                done
            fi
            
            # If no tests found but exit_code is 0, add a default passed test
            if [ -z "$all_tests" ] && [ "$tests_run" -eq 0 ]; then
                tests_run=1
                tests_passed=1
                success_rate=100
                test_results+=("$package::default:PASSED:$log_file")
                module_results=("$package:1:1:0:100")
            fi
            
            return 0
        else
            # If no test results found but exit_code is 0, add a default passed test
            log_warning "No tests were found in package $package, but exit code is 0"
            module_results+=("$package:1:1:0:100")
            test_results+=("$package::default:PASSED:$log_file")
            return 0
        fi
    fi
    
    log_error "Error running tests for package $package"
    module_results+=("$package:0:0:0:0")
    return 1
}

# Discover all tests
discover_all_tests() {
    log_step "Discovering all test cases by package..."

    # Temporarily disable set -e to avoid exiting on single package failure
    set +e

    # Use cargo metadata to get all package names (limit to curvine-*)
    cd "$PROJECT_ROOT"
    local packages=$(cargo metadata --format-version=1 | jq -r '.packages[] | select(.name | startswith("curvine-")) | .name')
    if [ -z "$packages" ]; then
        log_warning "No packages found via cargo metadata, trying directory listing..."
        packages=$(find "$PROJECT_ROOT" -name "Cargo.toml" -not -path "*/target/*" -not -path "*/\.*" | xargs dirname | xargs basename | grep "^curvine-")
    fi
    if [ -z "$packages" ]; then
        log_error "No packages discovered; cannot list tests."
        set -e
        return 1
    fi

    # Clean up old discovered_tests.txt
    : > "$TEST_DIR/discovered_tests.txt"

    declare -A discovered_tests

    for package in $packages; do
        log_info "Listing tests for package: $package"
        local test_list_file="$TEST_DIR/${package}_tests_list.txt"
        cargo test --package "$package" -- --list > "$test_list_file" 2>&1
        local exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log_warning "Failed to list tests for package $package (exit $exit_code). Skipping."
            continue
        fi

        # Current test file (for integration tests tests/<file>.rs)
        local current_test_file="lib"

        while IFS= read -r line; do
            # Record the current test file name being listed (integration tests)
            if [[ "$line" =~ Running[[:space:]]+tests/([^[:space:]]+)\ \(target/debug/deps/ ]]; then
                current_test_file="${BASH_REMATCH[1]}"
                # Remove .rs suffix
                current_test_file="${current_test_file%.rs}"
                continue
            fi

            # Match test case lines (tolerate leading/trailing spaces)
            if [[ "$line" =~ ^[[:space:]]*([^:]+):[[:space:]]*test[[:space:]]*$ ]]; then
                local test_name="${BASH_REMATCH[1]}"
                if [ -n "$test_name" ]; then
                    local test_file="$current_test_file"
                    local test_case="$test_name"
                    # If unit test form (module::case), keep module path
                    if [[ "$test_name" =~ ^(.+)::([^:]+)$ ]]; then
                        test_file="${BASH_REMATCH[1]}"
                        test_case="${BASH_REMATCH[2]}"
                    fi
                    discovered_tests["$package::$test_file::$test_case"]=1
                fi
            fi
        done < "$test_list_file"
    done

    # Output discovered test cases
    log_info "Discovered ${#discovered_tests[@]} test cases:"
    for test_key in "${!discovered_tests[@]}"; do
        echo "$test_key" >> "$TEST_DIR/discovered_tests.txt"
        log_info "  $test_key"
    done

    # Restore set -e
    set -e

    return 0
}

# Run single test case
run_single_test_case() {
    local package="$1"
    local test_file="$2"
    local test_case="$3"
    
    log_info "Running test case: $package::$test_file::$test_case"
    
    # Temporarily disable set -e to continue on test failures
    set +e
    
    # Create test result directory
    mkdir -p "$TEST_DIR/logs/$package"
    local log_file="$TEST_DIR/logs/$package/${test_file//::/_}_${test_case}.log"
    
    # Build full test name (for unit tests)
    local full_test_name="$test_case"
    if [ "$test_file" != "lib" ]; then
        full_test_name="$test_file::$test_case"
    fi
    
    # For integration tests (tests/<test_file>.rs) use --test; unit tests use package-level filter
    if [ "$test_file" != "lib" ]; then
        # Integration tests
        cd "$PROJECT_ROOT" && cargo test --package "$package" --test "$test_file" -- "$test_case" --exact --nocapture > "$log_file" 2>&1
    else
        # Unit/library tests
        cd "$PROJECT_ROOT" && cargo test --package "$package" -- "$full_test_name" --exact --nocapture > "$log_file" 2>&1
    fi
    local exit_code=$?
    
    # Re-enable set -e
    set -e
    
    # Return result
    if [ $exit_code -eq 0 ]; then
        echo "PASSED:$log_file"
        return 0
    else
        echo "FAILED:$log_file"
        return 1
    fi
}

# Run tests individually
run_tests_individually() {
    log_step "Running all tests individually..."
    
    # If specific test list exists, skip auto discovery
    if [ -s "$TEST_DIR/discovered_tests.txt" ]; then
        log_info "Using existing discovered tests list: $TEST_DIR/discovered_tests.txt"
    else
        # First, discover all test cases
        discover_all_tests
    fi
    
    # Check if discovered test cases file exists
    if [ ! -f "$TEST_DIR/discovered_tests.txt" ]; then
        log_error "No tests discovered"
        return 1
    fi
    
    # Initialize counters and result arrays
    local total_tests=0
    local passed_tests=0
    local failed_tests=0
    declare -A package_stats
    declare -a detailed_results
    
    # Disable set -e during individual runs to avoid exit on single failure
    set +e
    
    # Run test cases one by one
    while IFS= read -r test_key; do
        if [ -z "$test_key" ]; then
            continue
        fi
        
        # Safely parse package, test_file, test_case (split by "::")
        local package="${test_key%%::*}"
        local rest="${test_key#*::}"
        local test_file="${rest%%::*}"
        local test_case="${rest#*::}"
        
        if [ -z "$package" ] || [ -z "$test_case" ]; then
            log_warning "Skipping invalid test: $test_key"
            continue
        fi
        
        # Run single test case and capture function output last line (PASSED/FAILED)
        local result=$(run_single_test_case "$package" "$test_file" "$test_case" | tail -n 1)
        local status="${result%%:*}"
        local log_file="${result##*:}"
        
        # Update counters
        ((total_tests++))
        
        # Initialize package statistics
        if [ -z "${package_stats[$package]}" ]; then
            package_stats["$package"]="0:0:0"
        fi
        
        # Parse current package statistics
        IFS=':' read -r pkg_total pkg_passed pkg_failed <<< "${package_stats[$package]}"
        ((pkg_total++))
        
        if [ "$status" = "PASSED" ]; then
            ((passed_tests++))
            ((pkg_passed++))
            log_success "✓ $package::$test_file::$test_case"
        else
            ((failed_tests++))
            ((pkg_failed++))
            log_error "✗ $package::$test_file::$test_case"
        fi
        
        # Update package statistics
        package_stats["$package"]="$pkg_total:$pkg_passed:$pkg_failed"
        
        # Add to detailed results using safe separator '|'
        local rel_log_path="${log_file#$TEST_DIR/}"
        detailed_results+=("$package|$test_file|$test_case|$status|$rel_log_path")
        
    done < "$TEST_DIR/discovered_tests.txt"
    
    # Restore set -e
    set -e
    
    # Compute overall success rate
    local success_rate=0
    if [ "$total_tests" -gt 0 ]; then
        success_rate=$(( passed_tests * 100 / total_tests ))
    fi
    
    # Generate test summary JSON
    cat > "$TEST_DIR/test_summary.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "total_tests": $total_tests,
    "passed_tests": $passed_tests,
    "failed_tests": $failed_tests,
    "success_rate": $success_rate,
    "packages": [
EOF
    
    # Add package statistics
    local first=true
    for package in "${!package_stats[@]}"; do
        IFS=':' read -r pkg_total pkg_passed pkg_failed <<< "${package_stats[$package]}"
        local pkg_success_rate=0
        if [ "$pkg_total" -gt 0 ]; then
            pkg_success_rate=$(( pkg_passed * 100 / pkg_total ))
        fi
        
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$TEST_DIR/test_summary.json"
        fi
        echo "        {\"name\": \"$package\", \"total\": $pkg_total, \"passed\": $pkg_passed, \"failed\": $pkg_failed, \"success_rate\": $pkg_success_rate}" >> "$TEST_DIR/test_summary.json"
    done
    
    echo "    ]," >> "$TEST_DIR/test_summary.json"
    echo "    \"test_cases\": [" >> "$TEST_DIR/test_summary.json"
    
    # Add detailed test results (parsed by '|')
    first=true
    for result in "${detailed_results[@]}"; do
        IFS='|' read -r package test_file test_case status log_path <<< "$result"
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$TEST_DIR/test_summary.json"
        fi
        echo "        {\"package\": \"$package\", \"test_file\": \"$test_file\", \"test_case\": \"$test_case\", \"status\": \"$status\", \"log\": \"$log_path\"}" >> "$TEST_DIR/test_summary.json"
    done
    
    echo "    ]" >> "$TEST_DIR/test_summary.json"
    echo "}" >> "$TEST_DIR/test_summary.json"
    
    log_success "Individual test execution completed - total: $total_tests, passed: $passed_tests, failed: $failed_tests, success rate: ${success_rate}%"
    
    return 0
}

# Run specific test
run_specific_test() {
    local package="$1"
    local test_file="$2"
    local test_case="$3"
    
    log_step "Running specific test: $package/$test_file::$test_case..."
    
    # Temporarily disable set -e to continue on test failures
    set +e
    
    # Create test result directory
    mkdir -p "$TEST_DIR/logs/$package"
    local log_file="$TEST_DIR/logs/$package/${test_file}_${test_case}.log"
    
    # Run specific test
    log_info "Running test: $package/$test_file::$test_case"
    cd "$PROJECT_ROOT" && cargo test --package "$package" --test "$test_file" -- "$test_case" --exact --show-output > "$log_file" 2>&1
    local exit_code=$?
    
    # Parse test result
    if [ $exit_code -eq 0 ]; then
        if grep -q "test result: ok" "$log_file"; then
            log_success "Test $package/$test_file::$test_case passed"
            return 0
        fi
    fi
    
    log_error "Test $package/$test_file::$test_case failed"
    return 1
}

# Auto-discover and run all tests (by package)
run_tests() {
    log_step "Running test suites by package..."
    
    # Temporarily disable set -e to continue on test failures
    set +e
    
    # Create test result directory
    mkdir -p "$TEST_DIR/logs"
    
    # Initialize test counters
    local total_tests=0
    local passed_tests=0
    local failed_tests=0
    local test_results=()
    local module_results=()
    
    # Get all package names
    log_info "Discovering Rust packages..."
    cd "$PROJECT_ROOT"
    local packages=$(cargo metadata --format-version=1 | jq -r '.packages[] | select(.name | startswith("curvine-")) | .name')
    
    # If no packages found, try direct directory listing
    if [ -z "$packages" ]; then
        log_warning "No packages found via cargo metadata, trying directory listing..."
        packages=$(find "$PROJECT_ROOT" -name "Cargo.toml" -not -path "*/target/*" -not -path "*/\.*" | xargs dirname | xargs basename | grep "^curvine-")
    fi
    
    # If still no packages, add a default package for reporting
    if [ -z "$packages" ]; then
        log_warning "No packages found, adding a default package for reporting..."
        packages="curvine-default"
    fi
    
    # For each package create a module result array
    declare -A module_test_counts
    declare -A module_pass_counts
    declare -A module_fail_counts
    
    for package in $packages; do
        log_info "Processing package: $package"
        module_test_counts["$package"]=0
        module_pass_counts["$package"]=0
        module_fail_counts["$package"]=0
        
        # Create package log directory
        local package_dir="$TEST_DIR/logs/$package"
        mkdir -p "$package_dir"
        
        # Directly run all tests in the package
        log_info "Running all tests in package $package..."
        local log_file="$package_dir/all_tests.log"
        
        # Use cargo test to run all tests in the package, adding --all-targets to ensure running all types of tests
        if cargo test --package "$package" --all-targets -- --nocapture > "$log_file" 2>&1; then
            local test_count=$(grep -c "test result:" "$log_file" || echo 0)
            if [ "$test_count" -gt 0 ]; then
                # Extract test results
                local test_summary=$(grep "test result:" "$log_file" | head -1)
                local tests_run=$(echo "$test_summary" | grep -oP '\d+(?= tests)' || echo 0)
                local tests_passed=$(echo "$test_summary" | grep -oP '\d+(?= passed)' || echo 0)
                local tests_failed=$(echo "$test_summary" | grep -oP '\d+(?= failed)' || echo 0)
                
                # If no tests ran but there are passed tests, set run to passed
                tests_run=${tests_run:-0}
                tests_passed=${tests_passed:-0}
                tests_failed=${tests_failed:-0}
                
                # Compute success rate
                local success_rate=0
                if [ "$tests_run" -gt 0 ]; then
                    success_rate=$(( tests_passed * 100 / tests_run ))
                fi
                
                if [ "$tests_failed" -eq 0 ]; then
                    log_success "$package: All $tests_run tests passed"
                    test_results+=("$package:PASSED:$log_file")
                    ((passed_tests += tests_passed))
                    ((module_pass_counts["$package"] += tests_passed))
                else
                    log_error "$package: $tests_failed of $tests_run tests failed"
                    test_results+=("$package:FAILED:$log_file")
                    ((passed_tests += tests_passed))
                    ((failed_tests += tests_failed))
                    ((module_pass_counts["$package"] += tests_passed))
                    ((module_fail_counts["$package"] += tests_failed))
                fi
                ((total_tests += tests_run))
                ((module_test_counts["$package"] += tests_run))
                
                # Create module-level result map
                declare -A module_cases
                
                # Extract all test results (including passed and failed)
                local all_tests=$(grep -E "^test .* ... (ok|FAILED)" "$log_file" || echo "")
                if [ ! -z "$all_tests" ]; then
                    while IFS= read -r line; do
                        # Parse test name and result
                        local test_name=$(echo "$line" | grep -oP 'test \K[^:]+(?= ... )')
                        local test_result=$(echo "$line" | grep -oP ' ... \K(ok|FAILED)')
                        
                        if [ ! -z "$test_name" ] && [ ! -z "$test_result" ]; then
                            # Parse module path
                            local module_path=$(echo "$test_name" | grep -oP '^[^:]+(?=::)')
                            local test_case=$(echo "$test_name" | grep -oP '(?<=::)[^:]+$')
                            
                            # If no module path, use default
                            if [ -z "$module_path" ]; then
                                module_path="default"
                                test_case="$test_name"
                            fi
                            
                            # Add to module case map
                            if [ "$test_result" = "ok" ]; then
                                module_cases["$module_path:$test_case"]="PASSED"
                            else
                                module_cases["$module_path:$test_case"]="FAILED"
                                log_error "  Failed test: $test_name"
                            fi
                        fi
                    done <<< "$all_tests"
                    
                    # Add module cases to test results
                    for key in "${!module_cases[@]}"; do
                        IFS=':' read -r module_path test_case <<< "$key"
                        local status="${module_cases[$key]}"
                        test_results+=("$package::$module_path::$test_case:$status:$log_file")
                    done
                fi
                
                # Extract individual failed test results (fallback)
                if [ -z "$all_tests" ]; then
                    local failed_tests_list=$(grep -A 1 "failures:" "$log_file" | grep -v "failures:" | grep -v "\-\-" | tr -d ' ' | tr ',' '\n')
                    for failed_test in $failed_tests_list; do
                        if [ ! -z "$failed_test" ]; then
                            log_error "  Failed test: $failed_test"
                            test_results+=("$package::$failed_test:FAILED:$log_file")
                        fi
                    done
                fi
            else
                log_info "No tests were run in package $package"
            fi
        else
            log_error "Error running tests for package $package"
            test_results+=("$package:ERROR:$log_file")
            ((failed_tests++))
            ((module_fail_counts["$package"]++))
            ((total_tests++))
            ((module_test_counts["$package"]++))
        fi
        
        # Add module result
        local module_pass="${module_pass_counts["$package"]}"
        local module_fail="${module_fail_counts["$package"]}"
        local module_total="${module_test_counts["$package"]}"
        local module_success_rate=0
        
        if [ "$module_total" -gt 0 ]; then
            module_success_rate=$(( module_pass * 100 / module_total ))
            module_results+=("$package:$module_total:$module_pass:$module_fail:$module_success_rate")
        else
            # If no tests, add a default 0-value result
            module_results+=("$package:0:0:0:0")
        fi
    done
    
    # Compute overall success rate
    local success_rate=0
    if [ "$total_tests" -gt 0 ]; then
        success_rate=$(( passed_tests * 100 / total_tests ))
    fi
    
    # If passed tests but total_tests == 0, success_rate should be 100%
    if [ "$total_tests" -eq 0 ] && [ "$passed_tests" -gt 0 ]; then
        success_rate=100
        total_tests=$passed_tests
    fi
    
    # Save test summary JSON
    cat > "$TEST_DIR/test_summary.json" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "total_tests": $total_tests,
    "passed_tests": $passed_tests,
    "failed_tests": $failed_tests,
    "success_rate": $success_rate,
    "modules": [
EOF
    
    # Add module results
    local first=true
    for module_result in "${module_results[@]}"; do
        IFS=':' read -r module total pass fail rate <<< "$module_result"
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$TEST_DIR/test_summary.json"
        fi
        echo "        {\"name\": \"$module\", \"total\": $total, \"passed\": $pass, \"failed\": $fail, \"success_rate\": $rate}" >> "$TEST_DIR/test_summary.json"
    done
    
    echo "    ]," >> "$TEST_DIR/test_summary.json"
    echo "    \"results\": [" >> "$TEST_DIR/test_summary.json"
    
    # Add detailed test results
    first=true
    for result in "${test_results[@]}"; do
        IFS=':' read -r test_name status log_file <<< "$result"
        if [ "$first" = true ]; then
            first=false
        else
            echo "," >> "$TEST_DIR/test_summary.json"
        fi
        # Compute relative path for HTML links
        local rel_log_path="${log_file#$TEST_DIR/}"
        echo "        {\"test\": \"$test_name\", \"status\": \"$status\", \"log\": \"$rel_log_path\"}" >> "$TEST_DIR/test_summary.json"
    done
    
    echo "    ]" >> "$TEST_DIR/test_summary.json"
    echo "}" >> "$TEST_DIR/test_summary.json"
    
    log_success "Tests finished - total: $total_tests, passed: $passed_tests, failed: $failed_tests, success rate: ${success_rate}%"
    
    # Re-enable set -e
    set -e
}

# Cleanup resources

cleanup() {
    log_step "Cleaning test resources..."
    log_success "Resource cleanup finished"
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
    
    # Check whether to run a specific test
    if [ ! -z "$SPECIFIC_PACKAGE" ] && [ ! -z "$SPECIFIC_TEST_FILE" ] && [ ! -z "$SPECIFIC_TEST_CASE" ]; then
        log_info "Running specific test: $SPECIFIC_PACKAGE/$SPECIFIC_TEST_FILE::$SPECIFIC_TEST_CASE"
        
        # Construct discovery list for single test
        mkdir -p "$TEST_DIR"
        echo "$SPECIFIC_PACKAGE::$SPECIFIC_TEST_FILE::$SPECIFIC_TEST_CASE" > "$TEST_DIR/discovered_tests.txt"
        
        # Run individually by case and generate JSON
        run_tests_individually
        log_to_file "Specific test execution (individual) completed"
    elif [ ! -z "$SPECIFIC_PACKAGE" ]; then
        log_info "Run tests only for specified package: $SPECIFIC_PACKAGE"
        
        # Use new per-case execution, but limited to specified package
        # First discover all test cases
        discover_all_tests
        
        # Filter out test cases for the specified package
        if [ -f "$TEST_DIR/discovered_tests.txt" ]; then
            grep "^$SPECIFIC_PACKAGE::" "$TEST_DIR/discovered_tests.txt" > "$TEST_DIR/filtered_tests.txt" || true
            mv "$TEST_DIR/filtered_tests.txt" "$TEST_DIR/discovered_tests.txt"
        fi
        
        # Run filtered test cases
        run_tests_individually
        log_to_file "Package $SPECIFIC_PACKAGE individual test execution completed"
    else
        # Use new per-case execution to run all tests
        run_tests_individually
        log_to_file "Individual test execution completed"
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
