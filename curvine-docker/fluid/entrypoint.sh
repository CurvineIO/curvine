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


set -euo pipefail

# Fluid environment variables
FLUID_DATASET_NAME="${FLUID_DATASET_NAME:-}"
FLUID_DATASET_NAMESPACE="${FLUID_DATASET_NAMESPACE:-}"
FLUID_RUNTIME_CONFIG_PATH="${FLUID_RUNTIME_CONFIG_PATH:-/etc/fluid/config/config.json}"
FLUID_RUNTIME_MOUNT_PATH="${FLUID_RUNTIME_MOUNT_PATH:-/runtime-mnt}"
FLUID_RUNTIME_COMPONENT_TYPE="${FLUID_RUNTIME_COMPONENT_TYPE:-}"

CURVINE_HOME="${CURVINE_HOME:-/app/curvine}"
CURVINE_CONF_DIR="${CURVINE_HOME}/conf"
CURVINE_DATA_DIR="${CURVINE_HOME}/data"
CURVINE_LOG_DIR="${CURVINE_HOME}/logs"

FLUID_MODE=false
FLUID_RUNTIME_TYPE_ENV="${FLUID_RUNTIME_TYPE:-}"
FLUID_RUNTIME_TYPE=""
ENTRY_ARG="${1:-}"

if [ -n "$FLUID_RUNTIME_TYPE_ENV" ]; then
    FLUID_MODE=true
    if [ "$FLUID_RUNTIME_TYPE_ENV" = "thin" ]; then
        FLUID_RUNTIME_TYPE="thin-runtime"
        echo "Detected Fluid thin-runtime mode (from FLUID_RUNTIME_TYPE=thin)"
    else
        FLUID_RUNTIME_TYPE="cache-runtime"
        echo "Detected Fluid cache-runtime mode (from FLUID_RUNTIME_TYPE=$FLUID_RUNTIME_TYPE_ENV)"
    fi
elif [ "$ENTRY_ARG" = "fluid-thin-runtime" ]; then
    FLUID_MODE=true
    FLUID_RUNTIME_TYPE="thin-runtime"
    echo "Detected Fluid thin-runtime mode (from parameter)"
elif [ -n "$FLUID_RUNTIME_COMPONENT_TYPE" ] || [ -f "$FLUID_RUNTIME_CONFIG_PATH" ]; then
    FLUID_MODE=true
    FLUID_RUNTIME_TYPE="cache-runtime"
    echo "Detected Fluid cache-runtime mode"
fi

SERVER_TYPE="${1:-${FLUID_RUNTIME_COMPONENT_TYPE:-master}}"
ACTION_TYPE="${2:-start}"

echo "Curvine Fluid Entrypoint - Mode: $FLUID_RUNTIME_TYPE"
echo "SERVER_TYPE: $SERVER_TYPE, ACTION_TYPE: $ACTION_TYPE"

set_hosts() {
    :
}

load_curvine_env() {
    if [ -f "$CURVINE_HOME/conf/curvine-env.sh" ]; then
        echo "Loading curvine environment variables..."
        local original_home="$CURVINE_HOME"
        source "$CURVINE_HOME/conf/curvine-env.sh"
        export CURVINE_HOME="$original_home"
        export CURVINE_CONF_FILE="${CURVINE_CONF_DIR}/curvine-cluster.toml"
    fi
}

parse_fluid_config() {
    if [ "$FLUID_RUNTIME_TYPE" != "cache-runtime" ] || [ ! -f "$FLUID_RUNTIME_CONFIG_PATH" ]; then
        echo "Skipping Fluid config parsing (not in cache-runtime mode or config not found)"
        return 0
    fi
    
    echo "Parsing Fluid runtime configuration for cache-runtime..."
    echo "Config file: $FLUID_RUNTIME_CONFIG_PATH"
    echo "Component type: $SERVER_TYPE"
    
    python3 "$CURVINE_HOME/generate_config.py" > /tmp/fluid_env.sh || exit 1
    
    if [ -f /tmp/fluid_env.sh ]; then
        source /tmp/fluid_env.sh
        rm -f /tmp/fluid_env.sh
        echo "Fluid configuration parsed successfully"
    else
        echo "Warning: /tmp/fluid_env.sh not found"
    fi
}

generate_curvine_config() {
    mkdir -p "$CURVINE_CONF_DIR"
    mkdir -p "$CURVINE_DATA_DIR"
    mkdir -p "$CURVINE_LOG_DIR"
    
    local config_file="$CURVINE_CONF_DIR/curvine-cluster.toml"
    
    if [ ! -f "$config_file" ]; then
        if [ "$FLUID_MODE" = "true" ]; then
            echo "Generating default Curvine configuration for Fluid mode (to be merged with options)"
        else
            echo "Generating basic Curvine configuration for standalone mode"
        fi
        cat > "$config_file" << EOF
format_master = false
format_worker = false
testing = false
cluster_id = "curvine"

[master]
hostname = "localhost"
rpc_port = 8995
web_port =  9000
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
file_name = "curvine.log"
EOF
        echo "Basic Curvine configuration generated successfully"
    fi
}

setup_environment() {
    echo "Setting up Curvine environment..."
    
    mkdir -p "$CURVINE_DATA_DIR"/{meta,journal,data1}
    mkdir -p "$CURVINE_LOG_DIR"
    mkdir -p "${CURVINE_CACHE_PATH:-/cache-data}"
    
    chmod -R 755 "$CURVINE_HOME"
    
    if [ "$SERVER_TYPE" = "client" ]; then
        if [ ! -c /dev/fuse ]; then
            echo "Error: /dev/fuse device not found"
            exit 1
        fi
        
        modprobe fuse 2>/dev/null || true
        mkdir -p "$FLUID_RUNTIME_MOUNT_PATH"
    fi
    
    echo "Environment setup completed"
}

start_service() {
  local SERVICE_NAME=$1

  if [ -z "${CURVINE_CONF_FILE:-}" ]; then
    export CURVINE_CONF_FILE=${CURVINE_HOME}/conf/curvine-cluster.toml
  fi

  mkdir -p "${CURVINE_HOME}/logs"

  cd "${CURVINE_HOME}"

  echo "Starting ${SERVICE_NAME} service..."

  exec "${CURVINE_HOME}/lib/curvine-server" \
    --service "${SERVICE_NAME}" \
    --conf "${CURVINE_CONF_FILE}"
}

start_curvine_component() {
    local component="$1"
    local action="$2"
    
    echo "Starting Curvine $component component (action: $action)..."
    
    case "$component" in
        master)
            echo "Starting Curvine Master in Fluid cache-runtime mode..."
            load_curvine_env
            
            local pod_name="${HOSTNAME:-$(hostname)}"
            local namespace="${FLUID_DATASET_NAMESPACE:-default}"
            local master_service="${CURVINE_MASTER_SERVICE:-curvine-master}"
            local master_fqdn="${pod_name}.${master_service}.${namespace}.svc.cluster.local"
            export CURVINE_MASTER_HOSTNAME="$master_fqdn"
            echo "Set CURVINE_MASTER_HOSTNAME to: $CURVINE_MASTER_HOSTNAME"
            
            start_service "master"
            ;;
        
        worker)
            echo "Starting Curvine Worker in Fluid cache-runtime mode..."
            load_curvine_env
            
            if [ -n "$CURVINE_MASTER_HOSTNAME" ]; then
                echo "WARN: Ignoring CURVINE_MASTER_HOSTNAME in worker: $CURVINE_MASTER_HOSTNAME"
            fi
            
            local pod_name="${HOSTNAME:-$(hostname)}"
            local namespace="${FLUID_DATASET_NAMESPACE:-default}"
            local master_service="${CURVINE_MASTER_SERVICE:-curvine-master}"
            local worker_service="${CURVINE_WORKER_SERVICE:-$(echo "$master_service" | sed 's/master/worker/')}"
            local worker_fqdn="${pod_name}.${worker_service}.${namespace}.svc.cluster.local"
            export CURVINE_WORKER_HOSTNAME="$worker_fqdn"
            echo "Set CURVINE_WORKER_HOSTNAME to: $CURVINE_WORKER_HOSTNAME"
            
            start_service "worker"
            ;;
        
        client)
            echo "Starting Curvine FUSE Client..."
            local config_file="$CURVINE_CONF_DIR/curvine-cluster.toml"
            local mount_point="${CURVINE_MOUNT_POINT:-curvine:///}"
            local target_path="${CURVINE_TARGET_PATH:-$FLUID_RUNTIME_MOUNT_PATH}"
            
            echo "FUSE mount configuration:"
            echo "  Mount point: $mount_point"
            echo "  Target path: $target_path"
            echo "  Config file: $config_file"
            
            mkdir -p "$target_path"
            rm -rf "$target_path"/*
            rm -rf "$target_path"/.* 2>/dev/null || true
            
            exec "$CURVINE_HOME/lib/curvine-fuse" \
                --conf "$config_file" \
                --mnt-path "$target_path"
            ;;
        
        *)
            echo "Unknown component: $component"
            exit 1
            ;;
    esac
}

handle_thin_runtime() {
    echo "Handling Fluid thin-runtime mode..."
    
    python3 "$CURVINE_HOME/config-parse.py" || exit 1
    
    if [ -f "$CURVINE_HOME/mount-curvine.sh" ]; then
        echo "Executing mount script: $CURVINE_HOME/mount-curvine.sh"
        exec "$CURVINE_HOME/mount-curvine.sh"
    else
        echo "Error: mount script not found: $CURVINE_HOME/mount-curvine.sh"
        exit 1
    fi
}

main() {
    echo "Curvine Fluid Unified Entrypoint"
    echo "Arguments: $*"
    
    set_hosts
    
    if [ "$FLUID_RUNTIME_TYPE" = "thin-runtime" ]; then
        handle_thin_runtime
        return
    fi
    
    generate_curvine_config
    
    if [ "$FLUID_RUNTIME_TYPE" = "cache-runtime" ]; then
        parse_fluid_config
    fi
    
    setup_environment
    
    case "$ACTION_TYPE" in
        start)
            start_curvine_component "$SERVER_TYPE" "$ACTION_TYPE"
            ;;
        
        health-check)
            echo "Health check not implemented yet"
            exit 0
            ;;
        
        stop|restart)
            echo "Stop/restart not supported in Fluid mode"
            exit 1
            ;;
        
        *)
            echo "Unknown action: $ACTION_TYPE"
            exit 1
            ;;
    esac
}

main "$@"
