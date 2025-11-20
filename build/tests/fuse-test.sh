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
# Curvine FUSE Filesystem Test Script
#
# This script tests basic filesystem operations and performance
# of the Curvine FUSE mount point.
#

set -e

# Load shared colors and logging helpers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/colors.sh"

# Default configuration
TEST_DIR="/curvine-fuse/fuse-test"
CLEANUP="1"  # Cleanup test files by default
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

Curvine FUSE Filesystem Test Suite

This script tests basic filesystem operations including:
  - File and directory operations (create, read, write, delete)
  - Copy and move operations
  - Symbolic and hard links
  - File permissions (chmod, chown, chgrp)
  - Sed in-place editing

For FIO performance tests, use fio-test.sh instead.

OPTIONS:
    -t, --test-dir PATH       Test directory path (default: /curvine-fuse/fuse-test)
        --cleanup <0|1>       Cleanup test files after completion (default: 1)
                              0=keep files, 1=cleanup files
        --json-output PATH    Output test results to JSON file (for regression testing)
    -h, --help                Show this help message

EXAMPLES:
    # Test with default directory
    $0

    # Test with custom directory
    $0 -t /curvine-fuse/fuse-test
    
    # Keep test files for inspection (do not cleanup)
    $0 --cleanup 0
    
    # Output results to JSON file for regression testing
    $0 --json-output /tmp/fuse-test-results.json

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

    # Extract parent directory to check if it's a mount point
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
    
    # Check for required commands
    local missing_commands=()
    for cmd in touch mkdir cp mv rm cat grep sed; do
        if ! command -v $cmd &> /dev/null; then
            missing_commands+=("$cmd")
        fi
    done
    
    if [ ${#missing_commands[@]} -gt 0 ]; then
        print_fail "Missing required commands: ${missing_commands[*]}"
        exit 1
    fi
    
    print_info "All required commands are available"
}

# Initialize test environment
init_test_env() {
    print_header "Initializing Test Environment"

    cleanup

    print_info "Creating test directory: $TEST_DIR"
    if mkdir -p "$TEST_DIR"; then
        print_info "Created test directory: $TEST_DIR"
    else
        handle_error "Failed to create test directory" "fatal"
    fi
    
    print_info "Test environment initialized successfully"
}

# Test 1: Basic file operations
test_basic_operations() {
    CURRENT_TEST_GROUP="Test 1: Basic File Operations"
    print_header "$CURRENT_TEST_GROUP"

    # Test touch
    print_test "Creating empty file with touch"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="touch $TEST_DIR/test_file.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Created file: test_file.txt"
    else
        handle_error "Failed to create file with touch" "$cmd"
        return
    fi

    # Test write
    print_test "Writing data to file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="echo \"Hello, Curvine FUSE!\" > $TEST_DIR/test_file.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Wrote data to file"
    else
        handle_error "Failed to write to file" "$cmd"
        return
    fi

    # Test read
    print_test "Reading data from file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="grep -q \"Hello, Curvine FUSE!\" $TEST_DIR/test_file.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Read data from file successfully"
    else
        handle_error "Failed to read from file" "$cmd"
        return
    fi

    # Test append
    print_test "Appending data to file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="echo \"Second line\" >> $TEST_DIR/test_file.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        line_count=$(wc -l < "$TEST_DIR/test_file.txt")
        if [ "$line_count" -eq 2 ]; then
            print_success "Appended data successfully"
        else
            handle_error "Append operation failed (expected 2 lines, got $line_count)" "$cmd"
        fi
    else
        handle_error "Failed to append to file" "$cmd"
    fi
}

# Test 2: Directory operations
test_directory_operations() {
    CURRENT_TEST_GROUP="Test 2: Directory Operations"
    print_header "$CURRENT_TEST_GROUP"

    # Test mkdir
    print_test "Creating directory"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="mkdir $TEST_DIR/subdir"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Created directory: subdir"
    else
        handle_error "Failed to create directory" "$cmd"
        return
    fi

    # Test nested directories
    print_test "Creating nested directories"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="mkdir -p $TEST_DIR/dir1/dir2/dir3"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Created nested directories"
    else
        handle_error "Failed to create nested directories" "$cmd"
    fi

    # Test listing directories
    print_test "Listing directory contents"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="ls -la $TEST_DIR > /dev/null"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Listed directory contents"
    else
        handle_error "Failed to list directory" "$cmd"
    fi
}

# Test 3: Copy and move operations
test_copy_move() {
    CURRENT_TEST_GROUP="Test 3: Copy and Move Operations"
    print_header "$CURRENT_TEST_GROUP"

    # Test cp
    print_test "Copying file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="cp $TEST_DIR/test_file.txt $TEST_DIR/test_file_copy.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        if diff "$TEST_DIR/test_file.txt" "$TEST_DIR/test_file_copy.txt" > /dev/null; then
            print_success "Copied file successfully (content verified)"
        else
            handle_error "File copied but content differs" "$cmd"
        fi
    else
        handle_error "Failed to copy file" "$cmd"
    fi

    # Test mv
    print_test "Moving/renaming file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="mv $TEST_DIR/test_file_copy.txt $TEST_DIR/test_file_renamed.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        if [ -f "$TEST_DIR/test_file_renamed.txt" ] && [ ! -f "$TEST_DIR/test_file_copy.txt" ]; then
            print_success "Moved/renamed file successfully"
        else
            handle_error "Move operation incomplete" "$cmd"
        fi
    else
        handle_error "Failed to move file" "$cmd"
    fi
}

# Test 4: Large file operations
test_large_files() {
    CURRENT_TEST_GROUP="Test 4: Large File Operations"
    print_header "$CURRENT_TEST_GROUP"

    print_test "Creating 100MB file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="dd if=/dev/zero of=$TEST_DIR/large_file.dat bs=1M count=100 2>/dev/null"
    print_command "$cmd"
    if eval "$cmd"; then
        file_size=$(stat -f%z "$TEST_DIR/large_file.dat" 2>/dev/null || stat -c%s "$TEST_DIR/large_file.dat")
        expected_size=$((100 * 1024 * 1024))
        if [ "$file_size" -eq "$expected_size" ]; then
            print_success "Created 100MB file (size verified)"
        else
            handle_error "File created but size mismatch (expected: $expected_size, got: $file_size)" "$cmd"
        fi
    else
        handle_error "Failed to create large file" "$cmd"
    fi
}

# Test 5: Vi editor test
test_vi_editor() {
    CURRENT_TEST_GROUP="Test 5: Vi Editor Test"
    print_header "$CURRENT_TEST_GROUP"

    local test_file="$TEST_DIR/vi_test.txt"
    local test_content="Line 1: Original content"
    local new_content="Line 1: Modified by sed"

    # Create initial file
    echo "$test_content" > "$test_file"

    print_test "Testing sed editor operations - in-place edit"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="sed -i 's/Original content/Modified by sed/' $test_file 2>/dev/null"
    print_command "$cmd"
    if eval "$cmd"; then
        if grep -q "Modified by sed" "$test_file"; then
            print_success "Sed editor operations successful"
        else
            handle_error "Sed edit did not produce expected result" "$cmd"
        fi
    else
        handle_error "Sed editor operations failed" "$cmd"
    fi

    # Test sed append
    print_test "Testing sed append mode"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="sed -i '\$a Line 2: Appended content' $test_file 2>/dev/null"
    print_command "$cmd"
    if eval "$cmd"; then
        line_count=$(wc -l < "$test_file")
        if [ "$line_count" -ge 2 ]; then
            if grep -q "Appended content" "$test_file"; then
                print_success "Sed append mode successful"
            else
                handle_error "Sed append mode: content not found" "$cmd"
            fi
        else
            handle_error "Sed append mode: line count incorrect (expected >= 2, got $line_count)" "$cmd"
        fi
    else
        handle_error "Sed append mode failed" "$cmd"
    fi
}

# Test 6: Symbolic links
test_symlinks() {
    CURRENT_TEST_GROUP="Test 6: Symbolic Link Operations"
    print_header "$CURRENT_TEST_GROUP"

    print_test "Creating symbolic link"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="ln -s $TEST_DIR/test_file.txt $TEST_DIR/test_link"
    print_command "$cmd"
    if eval "$cmd"; then
        if [ -L "$TEST_DIR/test_link" ]; then
            print_success "Created symbolic link"
        else
            handle_error "Link created but not recognized as symlink" "$cmd"
        fi
    else
        handle_error "Failed to create symbolic link" "$cmd"
    fi

    print_test "Reading through symbolic link"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    cmd="cat $TEST_DIR/test_link > /dev/null"
    print_command "$cmd"
    if eval "$cmd"; then
        print_success "Read through symbolic link successfully"
    else
        handle_error "Failed to read through symbolic link" "$cmd"
    fi
}

# Test 7: Hard links
test_hardlinks() {
    CURRENT_TEST_GROUP="Test 7: Hard Link Operations"
    print_header "$CURRENT_TEST_GROUP"

    local original_file="$TEST_DIR/hardlink_original.txt"
    local hard_link="$TEST_DIR/hardlink_copy"
    local test_content="Original content for hard link test"

    # Create original file
    echo "$test_content" > "$original_file"

    print_test "Creating hard link"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local cmd="ln $original_file $hard_link"
    print_command "$cmd"
    if eval "$cmd"; then
        if [ -f "$hard_link" ]; then
            print_success "Created hard link"
        else
            handle_error "Hard link created but file doesn't exist" "$cmd"
            return
        fi
    else
        handle_error "Failed to create hard link" "$cmd"
        return
    fi

    print_test "Verifying hard link points to same inode"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local orig_inode=$(ls -i "$original_file" | awk '{print $1}')
    local link_inode=$(ls -i "$hard_link" | awk '{print $1}')
    if [ "$orig_inode" = "$link_inode" ]; then
        print_success "Hard link and original file share same inode ($orig_inode)"
    else
        handle_error "Hard link has different inode (orig: $orig_inode, link: $link_inode)"
    fi

    print_test "Verifying link count is 2"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    # Use stat to check link count (different format on Linux vs macOS)
    local link_count=$(stat -c '%h' "$original_file" 2>/dev/null || stat -f '%l' "$original_file")
    if [ "$link_count" -eq 2 ]; then
        print_success "Link count is correct: $link_count"
    else
        handle_error "Link count is incorrect (expected: 2, got: $link_count)"
    fi

    print_test "Reading content through hard link"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if grep -q "$test_content" "$hard_link"; then
        print_success "Read content through hard link successfully"
    else
        handle_error "Failed to read correct content through hard link"
    fi

    print_test "Writing to hard link affects original file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local new_content="Modified via hard link"
    echo "$new_content" > "$hard_link"
    if grep -q "$new_content" "$original_file"; then
        print_success "Writing to hard link updated original file"
    else
        handle_error "Writing to hard link did not update original file"
    fi

    print_test "Writing to original file affects hard link"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local another_content="Modified via original"
    echo "$another_content" > "$original_file"
    if grep -q "$another_content" "$hard_link"; then
        print_success "Writing to original file updated hard link"
    else
        handle_error "Writing to original file did not update hard link"
    fi

    print_test "Hard link survives after deleting original file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    rm "$original_file"
    if [ ! -f "$original_file" ] && [ -f "$hard_link" ]; then
        if grep -q "$another_content" "$hard_link"; then
            print_success "Hard link survives and retains content after original deletion"
        else
            handle_error "Hard link survives but content is incorrect"
        fi
    else
        handle_error "Hard link behavior incorrect after original deletion"
    fi

    print_test "Verifying link count decreased to 1 after deletion"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    local final_link_count=$(stat -c '%h' "$hard_link" 2>/dev/null || stat -f '%l' "$hard_link")
    if [ "$final_link_count" -eq 1 ]; then
        print_success "Link count decreased to 1 after deleting original"
    else
        handle_error "Link count incorrect after deletion (expected: 1, got: $final_link_count)"
    fi

    # Cleanup
    rm -f "$hard_link"
}

# Test 8: File permissions
test_permissions() {
    CURRENT_TEST_GROUP="Test 8: File Permission Operations"
    print_header "$CURRENT_TEST_GROUP"

    local perm_file="$TEST_DIR/perm_test.txt"
    echo "test" > "$perm_file"

    print_test "Changing file permissions with chmod"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if chmod 644 "$perm_file"; then
        print_success "Changed file permissions to 644"
    else
        handle_error "Failed to change file permissions"
    fi

    print_test "Verifying file permissions"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if ls -l "$perm_file" | grep -q "rw-r--r--"; then
        print_success "File permissions verified (644)"
    else
        handle_error "File permissions not as expected"
    fi

    print_test "Changing file permissions to executable"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if chmod 755 "$perm_file"; then
        if ls -l "$perm_file" | grep -q "rwxr-xr-x"; then
            print_success "Changed file permissions to 755 (executable)"
        else
            handle_error "File permissions not as expected after chmod 755"
        fi
    else
        handle_error "Failed to change file permissions to 755"
    fi

    # Create test user and group
    local test_user="fuse-test"
    local test_group="fuse-test"

    sudo groupadd "$test_group" 2>/dev/null || true
    sudo useradd -g "$test_group" -M -s /bin/false "$test_user" 2>/dev/null || true

    # Test chown
    print_test "Changing file owner to $test_user with chown"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if sudo chown "$test_user" "$perm_file" 2>/dev/null; then
        local file_owner=$(stat -c '%U' "$perm_file" 2>/dev/null || stat -f '%Su' "$perm_file")
        if [ "$file_owner" = "$test_user" ]; then
            print_success "Changed file owner to $test_user"
        else
            handle_error "File owner not as expected (expected: $test_user, got: $file_owner)"
        fi
    else
        handle_error "Failed to change owner to $test_user"
    fi

    # Test chgrp
    print_test "Changing file group to $test_group with chgrp"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if sudo chgrp "$test_group" "$perm_file" 2>/dev/null; then
        local file_group=$(stat -c '%G' "$perm_file" 2>/dev/null || stat -f '%Sg' "$perm_file")
        if [ "$file_group" = "$test_group" ]; then
            print_success "Changed file group to $test_group"
        else
            handle_error "File group not as expected (expected: $test_group, got: $file_group)"
        fi
    else
        handle_error "Failed to change group to $test_group"
    fi

    # Test chown with both user and group
    local another_file="$TEST_DIR/perm_test2.txt"
    echo "test2" > "$another_file"

    print_test "Changing file owner and group to $test_user:$test_group with chown"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if sudo chown "$test_user:$test_group" "$another_file" 2>/dev/null; then
        local file_owner=$(stat -c '%U' "$another_file" 2>/dev/null || stat -f '%Su' "$another_file")
        local file_group=$(stat -c '%G' "$another_file" 2>/dev/null || stat -f '%Sg' "$another_file")
        if [ "$file_owner" = "$test_user" ] && [ "$file_group" = "$test_group" ]; then
            print_success "Changed owner:group to $test_user:$test_group"
        else
            handle_error "Owner/group not as expected (expected: $test_user:$test_group, got: $file_owner:$file_group)"
        fi
    else
        handle_error "Failed to change owner:group to $test_user:$test_group"
    fi

    # Cleanup test user and group
    sudo userdel "$test_user" 2>/dev/null || true
    sudo groupdel "$test_group" 2>/dev/null || true

    # Test numeric chmod
    print_test "Changing permissions with numeric mode (chmod 600)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if chmod 600 "$perm_file"; then
        if ls -l "$perm_file" | grep -q "rw-------"; then
            print_success "Changed to 600 (rw-------)"
        else
            handle_error "Numeric chmod failed"
        fi
    else
        handle_error "Failed to chmod 600"
    fi

    # Test symbolic chmod
    print_test "Changing permissions with symbolic mode (chmod u+x)"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    if chmod u+x "$perm_file"; then
        if ls -l "$perm_file" | grep -q "rwx------"; then
            print_success "Added execute permission for user (rwx------)"
        else
            handle_error "Symbolic chmod u+x failed"
        fi
    else
        handle_error "Failed to chmod u+x"
    fi
}

# Test 9: Delete operations
test_delete_operations() {
    CURRENT_TEST_GROUP="Test 9: Delete Operations"
    print_header "$CURRENT_TEST_GROUP"

    print_test "Deleting file"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    touch "$TEST_DIR/to_delete.txt"
    local cmd="rm $TEST_DIR/to_delete.txt"
    print_command "$cmd"
    if eval "$cmd"; then
        if [ ! -f "$TEST_DIR/to_delete.txt" ]; then
            print_success "Deleted file successfully"
        else
            handle_error "File still exists after deletion" "$cmd"
        fi
    else
        handle_error "Failed to delete file" "$cmd"
    fi

    print_test "Deleting directory"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    mkdir "$TEST_DIR/to_delete_dir"
    cmd="rmdir $TEST_DIR/to_delete_dir"
    print_command "$cmd"
    if eval "$cmd"; then
        if [ ! -d "$TEST_DIR/to_delete_dir" ]; then
            print_success "Deleted directory successfully"
        else
            handle_error "Directory still exists after deletion" "$cmd"
        fi
    else
        handle_error "Failed to delete directory" "$cmd"
    fi

    print_test "Recursively deleting directory tree"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    mkdir -p "$TEST_DIR/recursive/sub1/sub2"
    touch "$TEST_DIR/recursive/file.txt"
    cmd="rm -rf $TEST_DIR/recursive"
    print_command "$cmd"
    if eval "$cmd"; then
        if [ ! -d "$TEST_DIR/recursive" ]; then
            print_success "Recursively deleted directory tree"
        else
            handle_error "Directory tree still exists after recursive deletion" "$cmd"
        fi
    else
        handle_error "Failed to recursively delete directory tree" "$cmd"
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
    local test_suite="fuse-test"
    
    # Create JSON file
    {
        echo "{"
        echo "  \"test_suite\": \"$test_suite\","
        echo "  \"timestamp\": \"$timestamp\","
        echo "  \"test_config\": {"
        echo "    \"test_dir\": \"$(json_escape "$TEST_DIR")\","
        echo "    \"cleanup\": \"$CLEANUP\""
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
            --cleanup)
                CLEANUP="$2"
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

    print_header "Curvine FUSE Filesystem Test Suite"
    
    echo "Test Directory: $TEST_DIR"
    echo "Cleanup Files:  $([ "$CLEANUP" = "1" ] && echo "Enabled" || echo "Disabled")"
    if [ -n "$JSON_OUTPUT" ]; then
        echo "JSON Output:    $JSON_OUTPUT"
    fi

    # Run tests
    check_prerequisites
    init_test_env
    
    print_info "Starting test suite..."
    
    test_basic_operations
    test_directory_operations
    test_copy_move
    test_large_files
    test_vi_editor
    test_symlinks
    test_hardlinks
    test_permissions
    test_delete_operations
    
    print_info "All test functions completed"

    # Cleanup and report
    if [ "$CLEANUP" = "1" ]; then
        cleanup
    else
        print_info "Skipping cleanup, test files preserved in: $TEST_DIR"
    fi
    print_report

    return $?
}

# Run main function
main "$@"