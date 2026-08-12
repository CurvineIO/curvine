#!/bin/bash

# Color definitions shared by all scripts

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Logging helpers
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

log_header() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Test-specific print functions (used by fio-test.sh and fuse-test.sh)
# Note: These functions depend on variables defined in the test scripts:
#   - PASSED_TESTS, FAILED_TESTS (for counting)
#   - JSON_OUTPUT, CURRENT_TEST_GROUP, LAST_TEST_CMD, LAST_TEST_NAME (for JSON output)
print_header() {
    echo -e "\n${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}\n"
}

print_test() {
    echo -e "${YELLOW}► Testing: $1${NC}"
    LAST_TEST_NAME="$1"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
    # Record test result for JSON output (if JSON_OUTPUT is set)
    if [ -n "${JSON_OUTPUT:-}" ]; then
        JSON_TEST_RESULTS+=("PASS|${CURRENT_TEST_GROUP:-}|$1|${LAST_TEST_CMD:-}")
    fi
}

print_fail() {
    echo -e "${RED}✗ $1${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

print_command() {
    echo -e "${BLUE}$ $1${NC}"
    LAST_TEST_CMD="$1"
}
