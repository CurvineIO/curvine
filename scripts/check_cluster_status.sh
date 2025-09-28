#!/bin/bash

# Cluster status check script
# Verifies Curvine test cluster readiness

set -e

# Load shared colors
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/colors.sh"

# Check cluster readiness
check_cluster_status() {
    local log_file="$1"
    local max_wait="${2:-120}"
    local pid="${3:-}"
    
    log_info "Checking cluster status..."
    
    local wait_time=0
    
    while [ $wait_time -lt $max_wait ]; do
        # Check if cluster is fully ready first
        if grep -q "Cluster is ready, active Worker count: 3" "$log_file" 2>/dev/null; then
            log_success "Cluster is ready! Active worker count: 3"
            return 0
        fi
        
        # Otherwise, check generic readiness
        if grep -q "Cluster is ready" "$log_file" 2>/dev/null; then
            log_success "Cluster is ready"
            return 0
        fi
        
        # If PID provided, ensure process is still running
        if [ -n "$pid" ] && ! ps -p "$pid" > /dev/null 2>&1; then
            log_error "Cluster process exited unexpectedly (PID: $pid)"
            return 1
        fi
        
        # Show waiting progress
        if [ $((wait_time % 10)) -eq 0 ] && [ $wait_time -gt 0 ]; then
            log_info "Waiting for cluster... (${wait_time}s/${max_wait}s)"
        fi
        
        sleep 2
        wait_time=$((wait_time + 2))
    done
    
    # Timeout handling
    log_warning "Timed out (${max_wait}s), cluster may still be starting"
    log_info "Current log tail:"
    tail -10 "$log_file" 2>/dev/null || true
    
    # Look for partial readiness hints
    if grep -q "Worker count" "$log_file" 2>/dev/null; then
        log_warning "Detected worker info; cluster may be partially ready"
        return 2  # partial ready
    fi
    
    return 1  # not ready
}

# Print cluster info
get_cluster_info() {
    local log_file="$1"
    
    if [ ! -f "$log_file" ]; then
        echo "Log file not found: $log_file"
        return 1
    fi
    
    # Extract cluster info
    local worker_count=$(grep -o "active Worker count: [0-9]*" "$log_file" 2>/dev/null | tail -1 | grep -o "[0-9]*" || echo "unknown")
    local ready_time=$(grep "Cluster is ready" "$log_file" 2>/dev/null | tail -1 | cut -d' ' -f1-2 || echo "unknown")
    
    echo "Cluster info:"
    echo "  Ready time: $ready_time"
    echo "  Active worker count: $worker_count"
    echo "  Log file: $log_file"
}

# Main entrypoint
main() {
    local log_file="$1"
    local max_wait="${2:-120}"
    local pid="${3:-}"
    
    if [ -z "$log_file" ]; then
        log_error "Please provide log file path"
        echo "Usage: $0 <log_file> [max_wait] [pid]"
        exit 1
    fi
    
    # Ensure log exists
    if [ ! -f "$log_file" ]; then
        log_error "Log file not found: $log_file"
        exit 1
    fi
    
    # Check status
    check_cluster_status "$log_file" "$max_wait" "$pid"
    local status=$?
    
    # Print info
    get_cluster_info "$log_file"
    
    # Exit with status
    exit $status
}

# If executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
