#!/bin/bash

# Shared cluster preparation functions for test scripts
# This script provides functions to check and prepare Curvine cluster

# Check if a mount point is properly mounted
check_mount_point() {
    local mount_path="$1"
    local max_wait_seconds="${2:-10}"
    
    if [ -z "$mount_path" ]; then
        echo "Error: mount_path is required" >&2
        return 1
    fi
    
    if [ ! -d "$mount_path" ]; then
        echo "Error: Mount path does not exist: $mount_path" >&2
        return 1
    fi
    
    # Use mountpoint command to check if it's a mount point
    local start_time=$(date +%s)
    local check_interval=1
    local first_check=true
    
    while true; do
        if command -v mountpoint >/dev/null 2>&1; then
            if mountpoint -q "$mount_path" 2>/dev/null; then
                if [ "$first_check" = false ]; then
                    local elapsed=$(($(date +%s) - start_time))
                    echo "Mount point $mount_path is now mounted (waited ${elapsed}s)"
                fi
                return 0
            fi
        else
            echo "Error: mountpoint command not found" >&2
            return 1
        fi
        
        local elapsed=$(($(date +%s) - start_time))
        if [ $elapsed -ge $max_wait_seconds ]; then
            echo "Error: Path $mount_path is not a mount point after waiting ${max_wait_seconds}s" >&2
            return 1
        fi
        
        if [ "$first_check" = true ]; then
            echo "Mount point $mount_path is not mounted yet, waiting up to ${max_wait_seconds}s..."
            first_check=false
        fi
        
        sleep $check_interval
    done
}

# Prepare Curvine cluster
# This function should be called from Python build-server.py, not directly from shell scripts
# Shell scripts should use ensure_cluster_ready instead
prepare_cluster() {
    local project_path="$1"
    local test_path="${2:-/curvine-fuse}"
    
    if [ -z "$project_path" ]; then
        echo "Error: project_path is required" >&2
        return 1
    fi
    
    echo "Preparing Curvine cluster..."
    echo "Note: This function should be called via Python build-server.py"
    echo "For shell scripts, use ensure_cluster_ready function"
    return 1
}

# Ensure cluster is ready (check mount point, prepare if needed)
# This function checks if mount point is ready, and if not, it will
# attempt to prepare the cluster by calling Python build-server.py API
ensure_cluster_ready() {
    local project_path="$1"
    local test_path="${2:-/curvine-fuse}"
    local max_wait_seconds="${3:-10}"
    
    if [ -z "$project_path" ]; then
        echo "Error: project_path is required" >&2
        return 1
    fi
    
    # First check if mount point is already ready
    if check_mount_point "$test_path" "$max_wait_seconds"; then
        echo "Mount point $test_path is already mounted and ready"
        return 0
    fi
    
    # If not mounted, we need to prepare the cluster
    # Since we're in a shell script, we'll call the Python function via a helper
    # For now, we'll just check and report error - actual preparation should be done
    # by the Python build-server.py before calling the test scripts
    echo "Warning: Mount point $test_path is not ready"
    echo "Please ensure cluster is prepared before running tests"
    echo "Cluster preparation should be done by build-server.py"
    return 1
}

