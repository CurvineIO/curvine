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
    cd "$PROJECT_ROOT" && cargo test --package "$package" --all-targets --all-features -- --nocapture > "$log_file" 2>&1
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

    # Clean up old discovered_tests.txt
    : > "$TEST_DIR/discovered_tests.txt"

    # Use workspace-wide listing to avoid per-package feature/compile skew
    cd "$PROJECT_ROOT"
    local test_list_file="$TEST_DIR/workspace_tests_list.txt"
    cargo test --workspace -- --list --format=terse > "$test_list_file" 2>&1
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_warning "Failed to list tests for workspace (exit $exit_code)."
    fi

    # Parse unique test names
    # Format: test_name: test (test_name can contain "::")
    declare -A discovered_set
    while IFS= read -r line; do
        if [ -z "$line" ]; then
            continue
        fi
        # Match pattern: "anything: test" where "anything" can contain "::"
        # Remove leading/trailing whitespace first
        line=$(echo "$line" | xargs)
        if [[ "$line" =~ ^(.+):[[:space:]]*test[[:space:]]*$ ]]; then
            local test_name="${BASH_REMATCH[1]}"
            test_name=$(echo "$test_name" | xargs)
            if [ -n "$test_name" ]; then
                discovered_set["$test_name"]=1
            fi
        fi
    done < "$test_list_file"

    # Build mapping: prefer precise mapping by target
    local mapping_file="$TEST_DIR/test_name_to_package.txt"
    : > "$mapping_file"
    # Build auxiliary mapping: test_name -> package (from per-package aggregate listing)
    local mapping_pkg_all="$TEST_DIR/test_name_in_package.txt"
    : > "$mapping_pkg_all"
    local packages=$(cargo metadata --format-version=1 --no-deps | jq -r '.packages[].name' | sort -u)
    if [ -z "$packages" ]; then
        packages=$(cargo metadata --format-version=1 2>/dev/null | jq -r '.workspace_members as $m | .packages[] | select(.id as $id | $m | index($id)) | .name' | sort -u)
    fi
    for package in $packages; do
        log_info "Mapping tests for package: $package"
        # Aggregate list for package -> used for fallback mapping
        local pkg_all_list="$TEST_DIR/${package}_all_tests_list.txt"
        cargo test --package "$package" -- --list --format=terse > "$pkg_all_list" 2>&1 || true
        while IFS= read -r line; do
            line=$(echo "$line" | xargs)
            if [[ "$line" =~ ^(.+):[[:space:]]*test[[:space:]]*$ ]]; then
                local tn="${BASH_REMATCH[1]}"
                tn=$(echo "$tn" | xargs)
                if [ -n "$tn" ]; then
                    echo "$tn|$package" >> "$mapping_pkg_all"
                fi
            fi
        done < "$pkg_all_list"
        # Get test targets for integration tests
        local test_targets=$(cargo metadata --format-version=1 | jq -r --arg pkg "$package" '.packages[] | select(.name==$pkg) | .targets[] | select(.kind | index("test")) | .name')
        for tgt in $test_targets; do
            local tgt_list_file="$TEST_DIR/${package}_${tgt}_tests_list.txt"
            cargo test --package "$package" --test "$tgt" -- --list --format=terse > "$tgt_list_file" 2>&1 || true
            while IFS= read -r line; do
                line=$(echo "$line" | xargs)
                if [[ "$line" =~ ^(.+):[[:space:]]*test[[:space:]]*$ ]]; then
                    local tc="${BASH_REMATCH[1]}"
                    tc=$(echo "$tc" | xargs)
                    if [ -n "$tc" ]; then
                        echo "$package|$tgt|$tc" >> "$mapping_file"
                    fi
                fi
            done < "$tgt_list_file"
        done
        # Map library unit tests if present
        local has_lib=$(cargo metadata --format-version=1 | jq -r --arg pkg "$package" '.packages[] | select(.name==$pkg) | .targets[] | select(.kind | index("lib")) | .name' | head -n 1)
        if [ -n "$has_lib" ]; then
            local lib_list_file="$TEST_DIR/${package}_lib_tests_list.txt"
            cargo test --package "$package" --lib -- --list --format=terse > "$lib_list_file" 2>&1 || true
            while IFS= read -r line; do
                line=$(echo "$line" | xargs)
                if [[ "$line" =~ ^(.+):[[:space:]]*test[[:space:]]*$ ]]; then
                    local tn="${BASH_REMATCH[1]}"
                    tn=$(echo "$tn" | xargs)
                    if [ -n "$tn" ]; then
                        local tf="lib"
                        local tc="$tn"
                        if [[ "$tn" == *"::"* ]]; then
                            tf="${tn%::*}"
                            tc="${tn##*::}"
                        fi
                        echo "$package|$tf|$tc" >> "$mapping_file"
                    fi
                fi
            done < "$lib_list_file"
        fi
    done

    # Output discovered test cases with package prefix if mapping available
    local out_count=0
    local skipped_no_pkg=0
    # Always iterate workspace-discovered names to ensure full coverage
    declare -A emitted
    for name in "${!discovered_set[@]}"; do
        local pkg=""
        local tf=""
        local tc=""
        
        # First, try to find package from per-package aggregate mapping (most reliable)
        pkg=$(grep "^$name|" "$mapping_pkg_all" 2>/dev/null | head -n 1 | awk -F'|' '{print $2}' || true)
        
        # If package found, try to find precise mapping from target-based mapping_file
        if [ -n "$pkg" ]; then
            # Try exact match first (for integration tests where testcase is just the function name)
            # Format: package|testfile|testcase
            local mapped_line=$(grep -E "^${pkg}\|[^|]+\|${name}$" "$mapping_file" 2>/dev/null | head -n 1 || true)
            
            # If no exact match, try to match by testcase (last part of name)
            if [ -z "$mapped_line" ] && [[ "$name" == *"::"* ]]; then
                local testcase="${name##*::}"
                mapped_line=$(grep -E "^${pkg}\|[^|]+\|${testcase}$" "$mapping_file" 2>/dev/null | head -n 1 || true)
            fi
            
            # If still no match, try to match by full name as testcase (for unit tests in lib)
            if [ -z "$mapped_line" ]; then
                mapped_line=$(grep -E "^${pkg}\|lib\|${name}$" "$mapping_file" 2>/dev/null | head -n 1 || true)
            fi
            
            if [ -n "$mapped_line" ]; then
                pkg="${mapped_line%%|*}"
                local rest="${mapped_line#*|}"
                tf="${rest%%|*}"
                tc="${rest##*|}"
            fi
        fi
        
        # If still no package found, try to infer from test name's module path
        if [ -z "$pkg" ] && [[ "$name" == *"::"* ]]; then
            # Extract first module segment and try to match with package names
            local first_module="${name%%::*}"
            # Try exact match first
            pkg=$(echo "$packages" | grep -x "$first_module" | head -n 1 || true)
            # If no exact match, try case-insensitive match
            if [ -z "$pkg" ]; then
                pkg=$(echo "$packages" | grep -i "^$first_module" | head -n 1 || true)
            fi
        fi
        
        # If package still not found, search all package test lists to find which package contains this test
        if [ -z "$pkg" ]; then
            for pkg_candidate in $packages; do
                local pkg_all_list="$TEST_DIR/${pkg_candidate}_all_tests_list.txt"
                if [ -f "$pkg_all_list" ] && grep -qE "^${name}:[[:space:]]*test[[:space:]]*$" "$pkg_all_list" 2>/dev/null; then
                    pkg="$pkg_candidate"
                    break
                fi
            done
        fi
        
        # Derive tf/tc from name if not already set
        if [ -z "$tf" ] || [ -z "$tc" ]; then
            if [[ "$name" == *"::"* ]]; then
                # For unit tests: module::path::test_function
                # Extract test_file (all but last segment) and test_case (last segment)
                # Find the last occurrence of "::"
                local reversed=$(echo "$name" | rev)
                local reversed_after="${reversed#*::}"
                if [ "$reversed" != "$reversed_after" ]; then
                    # Found "::", extract test_case (last part after "::")
                    tc=$(echo "$reversed_after" | rev)
                    # Extract test_file (everything before last "::")
                    local reversed_before="${reversed%"$reversed_after"}"
                    tf=$(echo "$reversed_before" | rev | sed 's/::$//')
                else
                    tf="lib"
                    tc="$name"
                fi
            else
                # For integration tests: just test_function
                tf="lib"
                tc="$name"
            fi
            
            # If name has no module path and package is known, try to find which test target contains it
            if [ "$tf" = "lib" ] && [ -n "$pkg" ]; then
                local test_targets=$(cargo metadata --format-version=1 | jq -r --arg p "$pkg" '.packages[] | select(.name==$p) | .targets[] | select(.kind | index("test")) | .name' 2>/dev/null || true)
                local matching_targets=0
                local first_matching_target=""
                for tgt in $test_targets; do
                    local tgt_list_file="$TEST_DIR/${pkg}_${tgt}_tests_list.txt"
                    if [ -f "$tgt_list_file" ] && grep -qE "^${name}:[[:space:]]*test[[:space:]]*$" "$tgt_list_file" 2>/dev/null; then
                        ((matching_targets++))
                        if [ -z "$first_matching_target" ]; then
                            first_matching_target="$tgt"
                        fi
                    fi
                done
                # If exactly one target contains this test, use it as TestFile
                if [ "$matching_targets" = "1" ] && [ -n "$first_matching_target" ]; then
                    tf="$first_matching_target"
                elif [ "$matching_targets" -gt 1 ] && [ -n "$first_matching_target" ]; then
                    # If multiple targets, prefer the one that matches the test name pattern
                    # For integration tests, the test file name usually matches
                    tf="$first_matching_target"
                fi
            fi
        fi
        
        # Guard: require package; if missing, skip
        if [ -z "$pkg" ]; then
            ((skipped_no_pkg++))
            log_warning "Could not map test to package: $name"
            continue
        fi
        
        local key="$pkg::$tf::$tc"
        if [ -z "${emitted[$key]}" ]; then
            echo "$key" >> "$TEST_DIR/discovered_tests.txt"
            emitted["$key"]=1
            ((out_count++))
        fi
    done
    log_info "Discovered $out_count test cases (workspace, package-qualified)"
    if [ "$skipped_no_pkg" -gt 0 ]; then
        log_warning "Skipped $skipped_no_pkg test cases due to missing package mapping"
    fi

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
    local safe_test_file="${test_file//::/_}"
    local safe_test_case="${test_case//::/_}"
    local log_file="$TEST_DIR/logs/$package/${safe_test_file}_${safe_test_case}.log"
    
    # Build full test name and determine test type
    local full_test_name=""
    local is_integration_test=false
    local is_unit_test=false
    
    # Check if test_file is an integration test file (no "::" in name)
    if [ "$test_file" != "lib" ] && [[ "$test_file" != *"::"* ]]; then
        # Integration test: test_file is the test target name (e.g., "block_test")
        is_integration_test=true
        full_test_name="$test_case"
    elif [ "$test_file" = "lib" ]; then
        # Unit test in lib: test_case might be full path or just function name
        is_unit_test=true
        full_test_name="$test_case"
    else
        # Unit test with module path: test_file is module path, test_case is function name
        is_unit_test=true
        full_test_name="$test_file::$test_case"
    fi
    
    # Run the test
    cd "$PROJECT_ROOT"
    if [ "$is_integration_test" = true ]; then
        # For integration tests, use --test flag with exact match
        # Use --exact to ensure only the specified test case runs
        log_info "Running integration test: cargo test --package $package --test $test_file -- $test_case --exact"
        cargo test --package "$package" --test "$test_file" -- "$test_case" --exact --nocapture > "$log_file" 2>&1
    elif [ "$is_unit_test" = true ]; then
        # For unit tests, use --lib flag to only run lib tests, not integration tests
        log_info "Running unit test: cargo test --package $package --lib -- $full_test_name --exact"
        cargo test --package "$package" --lib -- "$full_test_name" --exact --nocapture > "$log_file" 2>&1
    else
        # Fallback: use package-level filter (should not reach here)
        log_info "Running test: cargo test --package $package --all-features -- $full_test_name --exact"
        cargo test --package "$package" --all-features -- "$full_test_name" --exact --nocapture > "$log_file" 2>&1
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

# Run single test by full name in workspace scope
run_single_test_name() {
    local package="$1"
    local full_test_name="$2"
    local safe_name="${full_test_name//::/_}"
    local log_dir="$TEST_DIR/logs/${package:-workspace}"
    mkdir -p "$log_dir"
    local log_file="$log_dir/${safe_name}.log"
    log_info "Running test by name (workspace): $full_test_name"
    set +e
    cd "$PROJECT_ROOT" && cargo test --workspace -- "$full_test_name" --exact --nocapture > "$log_file" 2>&1
    local exit_code=$?
    set -e
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
    # Use single workspace bucket for stats
    declare -A package_stats
    declare -a detailed_results
    
    # Disable set -e during individual runs to avoid exit on single failure
    set +e
    
    # Prepare package name set to detect legacy lines like package::mod::case
    cd "$PROJECT_ROOT"
    local packages=$(cargo metadata --format-version=1 --no-deps | jq -r '.packages[].name' | tr '\n' ' ')
    
    # Run test cases one by one
    # Format: package::testfile::testcase
    while IFS= read -r test_key; do
        if [ -z "$test_key" ]; then
            continue
        fi
        
        # Parse package::testfile::testcase format
        local package=""
        local test_file=""
        local test_case=""
        
        # Count number of "::" separators
        local sep_count=$(echo "$test_key" | grep -o "::" | wc -l)
        
        if [ "$sep_count" -ge 2 ]; then
            # Format: package::testfile::testcase
            package="${test_key%%::*}"
            local rest="${test_key#*::}"
            # Find the last "::" to split test_file and test_case
            # Use ## to get the longest match (last occurrence)
            test_case="${rest##*::}"
            # Extract test_file (everything before last "::")
            local test_file_with_sep="${rest%"::$test_case"}"
            if [ "$test_file_with_sep" != "$rest" ]; then
                test_file="$test_file_with_sep"
            else
                # Fallback: no "::" found in rest (shouldn't happen with sep_count >= 2)
                test_file="lib"
                test_case="$rest"
            fi
        elif [ "$sep_count" -eq 1 ]; then
            # Legacy format: testfile::testcase (try to infer package)
            local first_seg="${test_key%%::*}"
            local second_seg="${test_key##*::}"
            # Try to find package by checking if first_seg matches a package name
            if [[ " $packages " == *" $first_seg "* ]]; then
                package="$first_seg"
                test_file="lib"
                test_case="$second_seg"
                full_test_name="$test_case"
            else
                # Assume it's testfile::testcase, need to find package
                test_file="$first_seg"
                test_case="$second_seg"
                full_test_name="$test_key"
                # Try to find package by searching test files
                for pkg_candidate in $packages; do
                    local test_targets=$(cargo metadata --format-version=1 | jq -r --arg p "$pkg_candidate" '.packages[] | select(.name==$p) | .targets[] | select(.kind | index("test")) | .name' 2>/dev/null || true)
                    if echo "$test_targets" | grep -q "^${test_file}$"; then
                        package="$pkg_candidate"
                        break
                    fi
                done
                if [ -z "$package" ]; then
                    package="workspace"
                fi
            fi
        else
            # No separators: just test case name
            test_case="$test_key"
            test_file="lib"
            full_test_name="$test_key"
            package="workspace"
        fi
        
        # Use run_single_test_case for proper package-scoped execution
        local result=$(run_single_test_case "$package" "$test_file" "$test_case" | tail -n 1)
        local status="${result%%:*}"
        local log_file="${result##*:}"
        
        # Update counters
        ((total_tests++))
        
        # Initialize package statistics
        local pkg_key="$package"
        if [ -z "${package_stats[$pkg_key]}" ]; then
            package_stats["$pkg_key"]="0:0:0"
        fi
        
        # Parse current package statistics
        IFS=':' read -r pkg_total pkg_passed pkg_failed <<< "${package_stats[$pkg_key]}"
        ((pkg_total++))
        
        if [ "$status" = "PASSED" ]; then
            ((passed_tests++))
            ((pkg_passed++))
            log_success "✓ $full_test_name"
        else
            ((failed_tests++))
            ((pkg_failed++))
            log_error "✗ $full_test_name"
        fi
        
        # Update package statistics
        package_stats["$pkg_key"]="$pkg_total:$pkg_passed:$pkg_failed"
        
        # Add to detailed results using safe separator '|'
        local rel_log_path="${log_file#$TEST_DIR/}"
        detailed_results+=("$pkg_key|$test_file|$test_case|$status|$rel_log_path")
        
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
    
    # Add package statistics (workspace bucket)
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
        
        # Use cargo test to run all tests in the package, include all targets and all features
        if cargo test --package "$package" --all-targets --all-features -- --nocapture > "$log_file" 2>&1; then
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
