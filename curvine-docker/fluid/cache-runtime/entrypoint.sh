#!/bin/bash

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

# Unified Entrypoint for Curvine (supports both standalone and Fluid CacheRuntime)

set -e

# Fluid environment variables (if running in Fluid CacheRuntime mode)
FLUID_DATASET_NAME="${FLUID_DATASET_NAME:-}"
FLUID_DATASET_NAMESPACE="${FLUID_DATASET_NAMESPACE:-}"
FLUID_RUNTIME_CONFIG_PATH="${FLUID_RUNTIME_CONFIG_PATH:-/etc/fluid/config/config.json}"
FLUID_RUNTIME_MOUNT_PATH="${FLUID_RUNTIME_MOUNT_PATH:-/runtime-mnt}"
FLUID_RUNTIME_COMPONENT_TYPE="${FLUID_RUNTIME_COMPONENT_TYPE:-}"

# Curvine configuration paths
CURVINE_HOME="${CURVINE_HOME:-/opt/curvine}"
CURVINE_CONF_DIR="${CURVINE_HOME}/conf"
CURVINE_DATA_DIR="${CURVINE_HOME}/data"
CURVINE_LOG_DIR="${CURVINE_HOME}/logs"

# Determine if we're running in Fluid mode
FLUID_MODE=false
if [ -n "$FLUID_RUNTIME_COMPONENT_TYPE" ] || [ -f "$FLUID_RUNTIME_CONFIG_PATH" ]; then
    FLUID_MODE=true
fi

# Service type: master, worker, client, all
SERVER_TYPE="${1:-${FLUID_RUNTIME_COMPONENT_TYPE:-all}}"

# Operation type: start, stop, restart, health-check
ACTION_TYPE="${2:-start}"

echo "Curvine Entrypoint - Fluid Mode: $FLUID_MODE"
echo "SERVER_TYPE: $SERVER_TYPE, ACTION_TYPE: $ACTION_TYPE"

# Parse Fluid runtime configuration (only in Fluid mode)
parse_fluid_config() {
    if [ "$FLUID_MODE" != "true" ] || [ ! -f "$FLUID_RUNTIME_CONFIG_PATH" ]; then
        echo "Skipping Fluid config parsing (not in Fluid mode or config not found)"
        return 0
    fi
    
    echo "Parsing Fluid runtime configuration..."
    echo "Config file: $FLUID_RUNTIME_CONFIG_PATH"
    echo "Component type: $SERVER_TYPE"
    
    # Use Python script to parse JSON configuration and generate curvine-cluster.toml
    python3 /opt/curvine/generate_config.py > /tmp/fluid_env.sh || exit 1
    
    # Source the generated environment if it exists
    if [ -f /tmp/fluid_env.sh ]; then
        echo "Generated environment file content:"
        cat /tmp/fluid_env.sh
        source /tmp/fluid_env.sh
        rm -f /tmp/fluid_env.sh
        echo "Fluid configuration parsed successfully"
        echo "CURVINE_MASTER_HOSTNAME: ${CURVINE_MASTER_HOSTNAME:-not set}"
        echo "CURVINE_MASTER_SERVICE: ${CURVINE_MASTER_SERVICE:-not set}"
    else
        echo "Warning: /tmp/fluid_env.sh not found"
    fi
}

# Generate Curvine configuration files
generate_curvine_config() {
    mkdir -p "$CURVINE_CONF_DIR"
    mkdir -p "$CURVINE_DATA_DIR"
    mkdir -p "$CURVINE_LOG_DIR"
    
    local config_file="$CURVINE_CONF_DIR/curvine-cluster.toml"
    
    # Always generate basic configuration if it doesn't exist
    # In Fluid mode, this will be used as default config for Python script to merge with
    if [ ! -f "$config_file" ]; then
        if [ "$FLUID_MODE" = "true" ]; then
            echo "Generating default Curvine configuration for Fluid mode (to be merged with options)"
        else
            echo "Generating basic Curvine configuration for standalone mode"
        fi
        # Basic standalone configuration
        cat > "$config_file" << EOF
format_master = false
format_worker = false
testing = false
cluster_id = "curvine"

[master]
hostname = "localhost"
rpc_port = 8995
web_port = 9000
meta_dir = "$CURVINE_DATA_DIR/meta"
audit_logging_enabled = true
log = { level = "info", log_dir = "$CURVINE_LOG_DIR", file_name = "master.log" }

[journal]
rpc_port = 8996
journal_addrs = [
    {id = 1, hostname = "localhost", port = 8996}
]
journal_dir = "$CURVINE_DATA_DIR/journal"

[worker]
rpc_port = 8997
web_port = 9001
dir_reserved = "10GB"
data_dir = ["[SSD]/cache-data"]
log = { level = "info", log_dir = "$CURVINE_LOG_DIR", file_name = "worker.log" }

[client]
master_addrs = [
    { hostname = "localhost", port = 8995 }
]

[fuse]
debug = false

[log]
level = "info"
log_dir = "$CURVINE_LOG_DIR"
file_name = "client.log"
EOF
        echo "Basic Curvine configuration generated successfully"
    fi
}

# Setup environment
setup_environment() {
    echo "Setting up Curvine environment..."
    
    # Create necessary directories
    mkdir -p "$CURVINE_DATA_DIR"/{meta,journal,data1}
    mkdir -p "$CURVINE_LOG_DIR"
    mkdir -p "${CURVINE_CACHE_PATH:-/cache-data}"
    
    # Set permissions
    chmod -R 755 "$CURVINE_HOME"
    
    # For client component, ensure FUSE device is available
    if [ "$SERVER_TYPE" = "client" ]; then
        if [ ! -c /dev/fuse ]; then
            echo "Error: /dev/fuse device not found"
            exit 1
        fi
        
        # Load FUSE module if needed
        modprobe fuse 2>/dev/null || true
        
        # Create mount point
        mkdir -p "$FLUID_RUNTIME_MOUNT_PATH"
    fi
    
    echo "Environment setup completed"
}

# Set hosts (original function)
set_hosts() {
    if [[ $(grep -c $HOSTNAME /etc/hosts) = '0' ]]; then
        echo "$POD_IP $HOSTNAME" >> /etc/hosts
    fi

    echo "POD_IP: ${POD_IP}"
    echo "POD_NAMESPACE: ${POD_NAMESPACE}"
    echo "POD_CLUSTER_DOMAIN: ${POD_CLUSTER_DOMAIN}"
    if [[ -z "$POD_IP" || -z "$POD_NAMESPACE" || -z "$POD_CLUSTER_DOMAIN" ]]; then
        echo "missing env, POD_IP: $POD_IP, POD_NAMESPACE: $POD_NAMESPACE, POD_CLUSTER_DOMAIN: $POD_CLUSTER_DOMAIN"
        return 0
    fi
    name="${POD_IP//./-}.${POD_NAMESPACE//_/-}.pod.${POD_CLUSTER_DOMAIN}"
    echo "$(cat /etc/hosts | sed s/"$POD_IP"/"$POD_IP $name"/g)" >/etc/hosts
    echo 'export PS1="[\u@\H \W]\\$ "' >>/etc/bashrc
}

# Start Curvine component
start_curvine_component() {
    local component="$1"
    local action="$2"
    local config_file="$CURVINE_CONF_DIR/curvine-cluster.toml"
    
    echo "Starting Curvine $component component (action: $action)..."
    
    case "$component" in
        master)
            if [ "$FLUID_MODE" = "true" ]; then
                echo "Starting Curvine Master in Fluid mode..."
                # Source curvine environment variables and override for Kubernetes
                if [ -f "$CURVINE_HOME/conf/curvine-env.sh" ]; then
                    echo "Loading curvine environment variables..."
                    # Temporarily save current CURVINE_HOME before sourcing
                    local original_home="$CURVINE_HOME"
                    source "$CURVINE_HOME/conf/curvine-env.sh"
                    # Restore CURVINE_HOME to correct Docker path
                    export CURVINE_HOME="$original_home"
                fi
                
                # Override hostname settings for Kubernetes environment
                local pod_ip="${POD_IP:-$(hostname -I | awk '{print $1}')}"
                local pod_name="${HOSTNAME:-$(hostname)}"
                local namespace="${FLUID_DATASET_NAMESPACE:-default}"
                local master_service="${CURVINE_MASTER_SERVICE:-curvine-master}"
                local master_fqdn="${pod_name}.${master_service}.${namespace}.svc.cluster.local"
                export CURVINE_MASTER_HOSTNAME="$master_fqdn"
                echo "Set CURVINE_MASTER_HOSTNAME to: $CURVINE_MASTER_HOSTNAME"
                
                # Start master directly in foreground for Kubernetes
                exec "$CURVINE_HOME/lib/curvine-server" \
                    --service=master \
                    --conf="$config_file"
            else
                echo "Starting Curvine Master in standalone mode..."
                RUN_CMD=${CURVINE_HOME}/bin/launch-process.sh
                exec $RUN_CMD master $action
            fi
            ;;
        
        worker)
            if [ "$FLUID_MODE" = "true" ]; then
                echo "Starting Curvine Worker in Fluid mode..."
                # Source curvine environment variables and override for Kubernetes
                if [ -f "$CURVINE_HOME/conf/curvine-env.sh" ]; then
                    echo "Loading curvine environment variables..."
                    # Temporarily save current CURVINE_HOME before sourcing
                    local original_home="$CURVINE_HOME"
                    source "$CURVINE_HOME/conf/curvine-env.sh"
                    # Restore CURVINE_HOME to correct Docker path
                    export CURVINE_HOME="$original_home"
                fi
                
                # If legacy env leaked in, warn and ignore
                if [ -n "$CURVINE_MASTER_HOSTNAME" ]; then
                    echo "WARN: Ignoring CURVINE_MASTER_HOSTNAME in worker: $CURVINE_MASTER_HOSTNAME"
                fi
                
                # Override hostname settings for Kubernetes environment (after sourcing env)
                local pod_ip="${POD_IP:-$(hostname -I | awk '{print $1}')}"
                local pod_name="${HOSTNAME:-$(hostname)}"
                local namespace="${FLUID_DATASET_NAMESPACE:-default}"
                local worker_service="${CURVINE_WORKER_SERVICE:-$(echo $CURVINE_MASTER_SERVICE | sed 's/master/worker/')}"
                local worker_fqdn="${pod_name}.${worker_service}.${namespace}.svc.cluster.local"
                # Force override CURVINE_WORKER_HOSTNAME to use FQDN in Kubernetes
                export CURVINE_WORKER_HOSTNAME="$worker_fqdn"
                echo "Set CURVINE_WORKER_HOSTNAME to: $CURVINE_WORKER_HOSTNAME"
                
                # Debug: Print master hostname configuration
                echo "DEBUG: Config file master.hostname:"
                grep -A 1 -B 1 "hostname.*=" "$config_file" || echo "No hostname found"
                
                # Debug: Print all CURVINE environment variables
                echo "DEBUG: All CURVINE environment variables:"
                env | grep CURVINE || echo "No CURVINE env vars found"
                
                # Start worker directly in foreground for Kubernetes (drop possible leaked env)
                exec env -u CURVINE_MASTER_HOSTNAME "$CURVINE_HOME/lib/curvine-server" \
                    --service=worker \
                    --conf="$config_file"
            else
                echo "Starting Curvine Worker in standalone mode..."
                RUN_CMD=${CURVINE_HOME}/bin/launch-process.sh
                exec $RUN_CMD worker $action
            fi
            ;;
        
        client)
            echo "Starting Curvine FUSE Client..."
            local mount_point="${CURVINE_MOUNT_POINT:-curvine:///}"
            local target_path="${CURVINE_TARGET_PATH:-$FLUID_RUNTIME_MOUNT_PATH}"
            
            echo "FUSE mount configuration:"
            echo "  Mount point: $mount_point"
            echo "  Target path: $target_path"
            echo "  Config file: $config_file"
            
            # Ensure target directory exists and is empty
            mkdir -p "$target_path"
            # Clean the directory to ensure FUSE can mount
            rm -rf "$target_path"/*
            rm -rf "$target_path"/.*  2>/dev/null || true
            
            # Start FUSE client
            exec "$CURVINE_HOME/lib/curvine-fuse" \
                --conf="$config_file" \
                --mnt-path="$target_path"
            ;;
        
        all)
            if [ "$FLUID_MODE" = "true" ]; then
                echo "Error: 'all' mode not supported in Fluid CacheRuntime"
                exit 1
            fi
            RUN_CMD=${CURVINE_HOME}/bin/launch-process.sh
            $RUN_CMD master $action
            $RUN_CMD worker $action
            ;;
        
        *)
            echo "Unknown component: $component"
            exit 1
            ;;
    esac
}

# Health check
health_check() {
    local component="$1"
    
    case "$component" in
        master)
            # Check if master RPC port is listening
            netstat -ln 2>/dev/null | grep ":8995" > /dev/null
            ;;
        
        worker)
            # Check if worker RPC port is listening
            netstat -ln 2>/dev/null | grep ":8997" > /dev/null
            ;;
        
        client)
            # Check if mount point is available
            local target_path="${CURVINE_TARGET_PATH:-$FLUID_RUNTIME_MOUNT_PATH}"
            mountpoint -q "$target_path" 2>/dev/null
            ;;
        
        *)
            return 1
            ;;
    esac
}

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    
    if [ "$SERVER_TYPE" = "client" ]; then
        local target_path="${CURVINE_TARGET_PATH:-$FLUID_RUNTIME_MOUNT_PATH}"
        if mountpoint -q "$target_path" 2>/dev/null; then
            echo "Unmounting $target_path"
            fusermount -u "$target_path" || umount "$target_path" || true
        fi
    fi
}

# Signal handlers
trap cleanup EXIT INT TERM

# Main execution
main() {
    echo "Curvine Unified Entrypoint"
    echo "Arguments: $*"
    
    # Setup environment
    set_hosts
    
    # Generate basic Curvine configuration first (for Python script to read)
    generate_curvine_config
    
    # Parse Fluid configuration if in Fluid mode (this will merge with existing config)
    if [ "$FLUID_MODE" = "true" ]; then
        parse_fluid_config
    fi
    
    # Setup environment
    setup_environment
    
    # Handle different actions
    case "$ACTION_TYPE" in
        start)
            start_curvine_component "$SERVER_TYPE" "$ACTION_TYPE"
            ;;
        
        health-check)
            health_check "$SERVER_TYPE"
            ;;
        
        stop|restart)
            if [ "$FLUID_MODE" = "true" ]; then
                echo "Stop/restart not supported in Fluid mode"
                exit 1
            fi
            RUN_CMD=${CURVINE_HOME}/bin/launch-process.sh
            case "$SERVER_TYPE" in
                all)
                    $RUN_CMD master $ACTION_TYPE
                    $RUN_CMD worker $ACTION_TYPE
                    ;;
                master|worker)
                    $RUN_CMD $SERVER_TYPE $ACTION_TYPE
                    ;;
                *)
                    exec "$@"
                    ;;
            esac
            ;;
        
        *)
            if [ "$FLUID_MODE" = "true" ]; then
                echo "Unknown action: $ACTION_TYPE"
                exit 1
            else
                # Fallback to original behavior
                exec "$@"
            fi
            ;;
    esac
    
    # Keep container running (for non-exec cases)
    if [ "$ACTION_TYPE" != "health-check" ]; then
        tail -f /dev/null &
        wait || :
    fi
}

# Execute main function
main "$@"