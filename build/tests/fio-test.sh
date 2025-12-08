#!/usr/bin/env bash

#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Curvine FIO Performance Test Script
#
# This script performs FIO (Flexible I/O Tester) performance tests
# on the Curvine FUSE mount point.
#

set -e

# Load shared colors and logging helpers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/colors.sh"

# Default configuration
TEST_DIR="/curvine-fuse/fio-test"
FIO_SIZE="500m"
FIO_RUNTIME="30s"
FIO_NUMJOBS="1"
FIO_DIRECT="1"  # Disable direct I/O by default (use page cache)
FIO_VERIFY="1"  # Enable data verification by default
FIO_CLEANUP="1"  # Cleanup test files by default
JSON_OUTPUT=""  # JSON output file path (empty = disabled)

# Test results tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
FAILED_TEST_LIST=()
FAILED_CMD_LIST=()

# JSON test results tracking
JSON_TEST_RESULTS=()
CURRENT_TEST_GROUP=""

# Print functions
print_help() {
    cat << EOF
Usage: $0 [OPTIONS]

Curvine FIO Performance Test Suite

OPTIONS:
    -t, --test-dir PATH       Test directory path (default: /curvine-fuse/fio-test)
        --size SIZE           FIO test file size (default: 500m)
                              Examples: 1G, 512M, 2G
        --runtime TIME        FIO test runtime (default: 30s)
                              Examples: 30s, 1m, 2m
        --numjobs NUM         Number of FIO parallel jobs (default: 1)
        --direct <0|1>        Enable direct I/O (default: 1)
                              0=disable (use page cache), 1=enable (bypasses page cache)
        --verify <0|1>        Enable data verification (default: 1)
                              0=disable, 1=enable CRC32C verification
        --cleanup <0|1>       Cleanup test files after completion (default: 1)
                              0=keep files, 1=cleanup files
        --json-output PATH    Output test results to JSON file (for regression testing)
    -h, --help                Show this help message

EXAMPLES:
    # Test with default settings (verification enabled)
    $0

    # Test with custom parameters
    $0 --size 2G --runtime 60s --numjobs 4

    # Quick test with custom directory
    $0 -t /mnt/curvine/fio-test --size 512M --runtime 30s
    
    # Disable data verification for faster performance testing
    $0 --verify 0 --size 1G --runtime 30s
    
    # Disable direct I/O to test with page cache
    $0 --direct 0 --size 1G --runtime 30s

    # Keep test files for inspection (do not cleanup)
    $0 --cleanup 0 --size 500M
    
    # Output results to JSON file for regression testing
    $0 --json-output /tmp/fio-test-results.json --size 1G

EOF
}

# Error handling
handle_error() {
    print_fail "$1"
    # Record failed test
    FAILED_TEST_LIST+=("$1")
    # Record failed command if provided (remove newlines for display)
    local cleaned_cmd=""
    if [ -n "$2" ] && [ "$2" != "fatal" ]; then
        cleaned_cmd=$(echo "$2" | tr '\n' ' ' | tr -s ' ')
        FAILED_CMD_LIST+=("$cleaned_cmd")
    elif [ -n "$3" ]; then
        cleaned_cmd=$(echo "$3" | tr '\n' ' ' | tr -s ' ')
        FAILED_CMD_LIST+=("$cleaned_cmd")
    else
        FAILED_CMD_LIST+=("")
    fi
    
    # Record test result for JSON output
    if [ -n "$JSON_OUTPUT" ]; then
        local test_name="${LAST_TEST_NAME:-$1}"
        local test_cmd="${cleaned_cmd:-${LAST_TEST_CMD:-}}"
        JSON_TEST_RESULTS+=("FAIL|$CURRENT_TEST_GROUP|$test_name|$test_cmd|$1")
    fi
    
    if [ "$2" == "fatal" ] || [ "$3" == "fatal" ]; then
        cleanup
        exit 1
    fi
}

# Cleanup function
cleanup() {
    print_info "Cleaning up test directory..."
    if [ -d "$TEST_DIR" ]; then
        rm -rf "$TEST_DIR" 2>/dev/null || true
    fi
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Extract parent directory to check if it's accessible
    local parent_dir
    parent_dir=$(dirname "$TEST_DIR")
    
    # Check if parent directory exists
    if [ ! -d "$parent_dir" ]; then
        print_fail "Parent directory $parent_dir does not exist"
        exit 1
    fi
    
    # Check if parent directory is a mount point (curvine-fuse should be mounted)
    if command -v mountpoint >/dev/null 2>&1; then
        if ! mountpoint -q "$parent_dir" 2>/dev/null; then
            print_fail "Parent directory $parent_dir is not a mount point. Curvine cluster may not be started."
            echo "  Please ensure Curvine cluster is running and $parent_dir is mounted"
            echo "  If running via build-server.py, cluster should be prepared automatically"
            exit 1
        fi
        print_info "Mount point $parent_dir is properly mounted"
    else
        print_info "mountpoint command not found, skipping mount point check"
        print_info "Test directory parent is accessible: $parent_dir"
    fi
    
    # Check for fio
    if ! command -v fio &> /dev/null; then
        print_fail "fio command not found. Please install fio:"
        echo "  Ubuntu/Debian: sudo apt-get install fio"
        echo "  RHEL/CentOS:   sudo yum install fio"
        echo "  macOS:         brew install fio"
        exit 1
    fi
    
    print_info "fio is installed"
}

# Initialize test environment
init_test_env() {
    print_header "Initializing Test Environment"
    
    # Create test directory
    if [ -d "$TEST_DIR" ]; then
        print_info "Test directory exists, cleaning up..."
        rm -rf "$TEST_DIR"
    fi
    
    mkdir -p "$TEST_DIR"
    print_info "Test directory created: $TEST_DIR"
}

# Get verification options for write operations
get_write_verify_opt() {
    if [ "$FIO_VERIFY" = "1" ]; then
        echo "--verify=crc32c"
    else
        echo ""
    fi
}

# Get verification options for read operations
get_read_verify_opt() {
    if [ "$FIO_VERIFY" = "1" ]; then
        echo "--verify=crc32c --verify_only=1"
    else
        echo ""
    fi
}

# Get verification message
get_verify_msg() {
    if [ "$FIO_VERIFY" = "1" ]; then
        echo " (data verified)"
    else
        echo ""
    fi
}

# Test 1: FIO Sequential Read/Write
test_fio_sequential() {
    CURRENT_TEST_GROUP="Test 1: FIO Sequential Read/Write Performance"
    print_header "$CURRENT_TEST_GROUP"
    
    local fio_dir="$TEST_DIR/fio_seq"
    mkdir -p "$fio_dir"
    
    print_test "FIO Sequential Write Test (256KB blocks)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local verify_opt=$(get_write_verify_opt)
    local cmd="fio \
        --name=seq_write \
        --directory=$fio_dir \
        --ioengine=libaio \
        --direct=$FIO_DIRECT \
        --bs=256k \
        --size=$FIO_SIZE \
        --numjobs=$FIO_NUMJOBS \
        --rw=write \
        $verify_opt \
        --group_reporting \
        --runtime=$FIO_RUNTIME \
        --time_based=0"
    print_command "$cmd"
    echo ""  # Add newline for better readability
    if eval "$cmd"; then
        print_success "Sequential Write completed"
    else
        handle_error "FIO Sequential Write test failed" "$cmd"
    fi
    
    print_test "FIO Sequential Read Test (256KB blocks)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    verify_opt=$(get_read_verify_opt)
    local verify_msg=$(get_verify_msg)
    cmd="fio \
        --name=seq_write \
        --directory=$fio_dir \
        --ioengine=libaio \
        --direct=$FIO_DIRECT \
        --bs=256k \
        --size=$FIO_SIZE \
        --numjobs=$FIO_NUMJOBS \
        --rw=read \
        $verify_opt \
        --group_reporting \
        --runtime=$FIO_RUNTIME \
        --time_based=0"
    print_command "$cmd"
    echo ""  # Add newline for better readability
    if eval "$cmd"; then
        print_success "Sequential Read completed${verify_msg}"
    else
        handle_error "FIO Sequential Read test failed" "$cmd"
    fi
}

# Test 2: FIO Random Read/Write
test_fio_random() {
    CURRENT_TEST_GROUP="Test 2: FIO Random Read/Write Performance"
    print_header "$CURRENT_TEST_GROUP"
    
    local fio_dir="$TEST_DIR/fio_rand"
    mkdir -p "$fio_dir"
    
    # Test 1: Random Write
    print_test "FIO Random Write Test (256KB blocks)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local verify_opt=$(get_write_verify_opt)
    local cmd="fio \
        --name=rand_write \
        --directory=$fio_dir \
        --ioengine=libaio \
        --direct=$FIO_DIRECT \
        --bs=256k \
        --size=$FIO_SIZE \
        --numjobs=$FIO_NUMJOBS \
        --rw=randwrite \
        $verify_opt \
        --group_reporting \
        --runtime=$FIO_RUNTIME \
        --time_based=0"
    print_command "$cmd"
    echo ""  # Add newline for better readability
    if eval "$cmd"; then
        print_success "Random Write completed"
    else
        handle_error "FIO Random Write test failed" "$cmd"
    fi

    # Test 2: Random Read
    print_test "FIO Random Read Test (256KB blocks)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    verify_opt=$(get_read_verify_opt)
    local verify_msg=$(get_verify_msg)
    cmd="fio \
        --name=rand_write \
        --directory=$fio_dir \
        --ioengine=libaio \
        --direct=$FIO_DIRECT \
        --bs=256k \
        --size=$FIO_SIZE \
        --numjobs=$FIO_NUMJOBS \
        --rw=randread \
        $verify_opt \
        --group_reporting \
        --runtime=$FIO_RUNTIME \
        --time_based"
    print_command "$cmd"
    echo ""  # Add newline for better readability
    if eval "$cmd"; then
        print_success "Random Read completed${verify_msg}"
    else
        handle_error "FIO Random Read test failed" "$cmd"
    fi
    
    # Test 3: Mixed Random Read/Write
    print_test "FIO Mixed Random Read/Write Test (256KB blocks, 70% read, 30% write)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    verify_opt=$(get_write_verify_opt)
    local verify_msg=$(get_verify_msg)
    cmd="fio \
        --name=rand_rw \
        --directory=$fio_dir \
        --ioengine=libaio \
        --direct=$FIO_DIRECT \
        --bs=256k \
        --size=$FIO_SIZE \
        --numjobs=$FIO_NUMJOBS \
        --rw=randrw \
        --rwmixread=70 \
        $verify_opt \
        --group_reporting \
        --runtime=$FIO_RUNTIME \
        --time_based=0"
    print_command "$cmd"
    echo ""  # Add newline for better readability
    if eval "$cmd"; then
        print_success "Mixed Random Read/Write completed${verify_msg}"
    else
        handle_error "FIO Mixed Random Read/Write test failed" "$cmd"
    fi
}

# Escape JSON string
json_escape() {
    local str="$1"
    # Escape special JSON characters
    # Order matters: escape backslash first, then other characters
    str=$(printf '%s' "$str" | sed 's/\\/\\\\/g')
    str=$(printf '%s' "$str" | sed 's/"/\\"/g')
    str=$(printf '%s' "$str" | sed 's/\t/\\t/g')
    str=$(printf '%s' "$str" | sed 's/\r/\\r/g')
    # Replace newlines with \n
    str=$(printf '%s' "$str" | sed ':a;N;$!ba;s/\n/\\n/g')
    printf '%s' "$str"
}

# Generate JSON report
generate_json_report() {
    if [ -z "$JSON_OUTPUT" ]; then
        return
    fi
    
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%S")
    local test_suite="fio-test"
    
    # Create JSON file
    {
        echo "{"
        echo "  \"test_suite\": \"$test_suite\","
        echo "  \"timestamp\": \"$timestamp\","
        echo "  \"test_config\": {"
        echo "    \"test_dir\": \"$(json_escape "$TEST_DIR")\","
        echo "    \"fio_size\": \"$FIO_SIZE\","
        echo "    \"fio_runtime\": \"$FIO_RUNTIME\","
        echo "    \"fio_numjobs\": \"$FIO_NUMJOBS\","
        echo "    \"fio_verify\": \"$FIO_VERIFY\","
        echo "    \"cleanup\": \"$FIO_CLEANUP\""
        echo "  },"
        echo "  \"summary\": {"
        echo "    \"total_tests\": $TOTAL_TESTS,"
        echo "    \"passed\": $PASSED_TESTS,"
        echo "    \"failed\": $FAILED_TESTS"
        echo "  },"
        echo "  \"tests\": ["
        
        # Output test results
        local first=true
        for result in "${JSON_TEST_RESULTS[@]}"; do
            IFS='|' read -r status test_group test_name test_cmd error_msg <<< "$result"
            
            if [ "$first" = true ]; then
                first=false
            else
                echo ","
            fi
            
            echo -n "    {"
            echo -n "\"name\": \"$(json_escape "$test_name")\","
            echo -n "\"status\": \"$status\","
            echo -n "\"test_group\": \"$(json_escape "$test_group")\""
            
            if [ -n "$test_cmd" ]; then
                echo -n ",\"command\": \"$(json_escape "$test_cmd")\""
            fi
            
            if [ "$status" = "FAIL" ] && [ -n "$error_msg" ]; then
                echo -n ",\"error\": \"$(json_escape "$error_msg")\""
            fi
            
            echo -n "}"
        done
        
        echo ""
        echo "  ]"
        echo "}"
    } > "$JSON_OUTPUT"
    
    print_info "JSON report saved to: $JSON_OUTPUT"
}

# Print final report
print_report() {
    print_header "Test Summary"
    
    echo -e "Total Tests:  ${BLUE}$TOTAL_TESTS${NC}"
    echo -e "Passed:       ${GREEN}$PASSED_TESTS${NC}"
    echo -e "Failed:       ${RED}$FAILED_TESTS${NC}"
    
    if [ $FAILED_TESTS -eq 0 ]; then
        echo -e "\n${GREEN}✓ All tests passed!${NC}\n"
    else
        echo -e "\n${RED}✗ Some tests failed!${NC}\n"
        
        # Print failed test details
        if [ ${#FAILED_TEST_LIST[@]} -gt 0 ]; then
            echo -e "${RED}Failed Tests:${NC}"
            for i in "${!FAILED_TEST_LIST[@]}"; do
                echo -e "  ${RED}✗${NC} ${FAILED_TEST_LIST[$i]}"
                if [ -n "${FAILED_CMD_LIST[$i]}" ]; then
                    echo -e "    ${BLUE}Command: ${FAILED_CMD_LIST[$i]}${NC}"
                fi
            done
            echo ""
        fi
    fi
    
    # Generate JSON report if requested
    if [ -n "$JSON_OUTPUT" ]; then
        generate_json_report
    fi
    
    if [ $FAILED_TESTS -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

# Main execution
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -t|--test-dir)
                TEST_DIR="$2"
                shift 2
                ;;
            --size)
                FIO_SIZE="$2"
                shift 2
                ;;
            --runtime)
                FIO_RUNTIME="$2"
                shift 2
                ;;
            --numjobs)
                FIO_NUMJOBS="$2"
                shift 2
                ;;
            --direct)
                FIO_DIRECT="$2"
                shift 2
                ;;
            --verify)
                FIO_VERIFY="$2"
                shift 2
                ;;
            --cleanup)
                FIO_CLEANUP="$2"
                shift 2
                ;;
            --json-output)
                JSON_OUTPUT="$2"
                shift 2
                ;;
            -h|--help)
                print_help
                exit 0
                ;;
            *)
                echo "Error: Unknown option: $1" >&2
                print_help
                exit 1
                ;;
        esac
    done
    
    print_header "Curvine FIO Performance Test Suite"
    
    echo "Test Directory: $TEST_DIR"
    echo "FIO Size:       $FIO_SIZE"
    echo "FIO Runtime:    $FIO_RUNTIME"
    echo "FIO Jobs:       $FIO_NUMJOBS"
    echo "FIO Direct I/O: $([ "$FIO_DIRECT" = "1" ] && echo "Enabled" || echo "Disabled")"
    echo "Data Verify:    $([ "$FIO_VERIFY" = "1" ] && echo "Enabled (CRC32C)" || echo "Disabled")"
    echo "Cleanup Files:  $([ "$FIO_CLEANUP" = "1" ] && echo "Enabled" || echo "Disabled")"
    if [ -n "$JSON_OUTPUT" ]; then
        echo "JSON Output:    $JSON_OUTPUT"
    fi
    
    # Run tests
    check_prerequisites
    init_test_env
    
    test_fio_sequential
    test_fio_random
    
    # Cleanup and report
    if [ "$FIO_CLEANUP" = "1" ]; then
        cleanup
    else
        print_info "Skipping cleanup, test files preserved in: $TEST_DIR"
    fi
    print_report
    
    return $?
}

# Run main function
main "$@"

