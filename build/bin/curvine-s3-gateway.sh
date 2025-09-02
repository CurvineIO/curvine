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

# Source environment variables
. "$(cd "`dirname "$0"`"; pwd)"/../conf/curvine-env.sh

# Service configuration
SERVICE_NAME="curvine-s3-gateway"
PID_FILE=${CURVINE_HOME}/${SERVICE_NAME}.pid
LOG_DIR=${CURVINE_HOME}/logs
OUT_FILE=${LOG_DIR}/${SERVICE_NAME}.out
GRACEFULLY_TIMEOUT=15

# Default values
DEFAULT_CONF="${CURVINE_HOME}/conf/curvine-cluster.toml"
DEFAULT_LISTEN="0.0.0.0:9900"
DEFAULT_REGION="us-east-1"

# Parse command line arguments
parse_args() {
    # First, check for actions
    if [[ $# -gt 0 ]] && [[ "$1" =~ ^(start|stop|status|restart)$ ]]; then
        ACTION="$1"
        shift
    fi
    
    # Then parse options
    while [[ $# -gt 0 ]]; do
        case $1 in
            --conf)
                CONF="$2"
                shift 2
                ;;
            --listen)
                LISTEN="$2"
                shift 2
                ;;
            --region)
                REGION="$2"
                shift 2
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Show usage information
show_usage() {
    cat << EOF
Usage: $0 [ACTION] [OPTIONS]

ACTIONS:
    start       Start the curvine-s3-gateway S3 gateway service
stop        Stop the curvine-s3-gateway S3 gateway service
status      Show the status of the curvine-s3-gateway service
restart     Restart the curvine-s3-gateway S3 gateway service

OPTIONS:
    --conf <config>     Path to curvine cluster configuration file
                        (default: ${CURVINE_HOME}/conf/curvine-cluster.toml)
    --listen <host:port> Listen address (default: 0.0.0.0:9900)
    --region <region>   S3 region to report (default: us-east-1)
    --help, -h          Show this help message

EXAMPLES:
    $0 start                                    # Start with default settings
    $0 start --conf /path/to/config.toml       # Start with custom config
    $0 start --listen 127.0.0.1:9900          # Start on specific address
    $0 stop                                     # Stop the service
    $0 status                                   # Check service status
    $0 restart                                  # Restart the service

EOF
}

# Check if service is already running
check_running() {
    if [ -f "${PID_FILE}" ]; then
        local PID=$(cat ${PID_FILE})
        if kill -0 ${PID} > /dev/null 2>&1; then
            return 0  # Running
        else
            # PID file exists but process is dead, clean up
            rm -f "${PID_FILE}"
        fi
    fi
    return 1  # Not running
}

# Get service status
get_status() {
    if check_running; then
        local PID=$(cat ${PID_FILE})
        echo "Status: RUNNING (PID: ${PID})"
        echo "Process: $(ps -p ${PID} -o pid,ppid,cmd --no-headers 2>/dev/null || echo 'Process info not available')"
        echo "Log file: ${OUT_FILE}"
        return 0
    else
        echo "Status: STOPPED"
        return 1
    fi
}

# Wait for process to stop gracefully
wait_for_stop() {
    local PID=$1
    local n=$(expr ${GRACEFULLY_TIMEOUT} / 3)
    local i=0
    
    while [ $i -le $n ]; do
        if kill -0 ${PID} > /dev/null 2>&1; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') Waiting for ${SERVICE_NAME} to stop gracefully..."
            sleep 3
        else
            break
        fi
        let i++
    done
}

# Start the service
start_service() {
    echo "Starting ${SERVICE_NAME} S3 Gateway..."

    # Check if already running
    if check_running; then
        local PID=$(cat ${PID_FILE})
        echo "Error: ${SERVICE_NAME} is already running with PID ${PID}"
        echo "Please stop the service first or use 'restart'"
        exit 1
    fi

    # Validate configuration
    if [ -n "$CONF" ] && [ ! -f "$CONF" ]; then
        echo "Error: Configuration file '$CONF' not found"
        exit 1
    fi

    # Set configuration values
    local CONFIG_FILE=${CONF:-${DEFAULT_CONF}}
    local LISTEN_ADDR=${LISTEN:-${DEFAULT_LISTEN}}
    local REGION_VALUE=${REGION:-${DEFAULT_REGION}}

    # Try to read from config file if not specified
    if [ -f "$CONFIG_FILE" ]; then
        if [ -z "$LISTEN" ]; then
            local LISTEN_FROM_CONFIG=$(grep -E '^\s*listen\s*=' "$CONFIG_FILE" | head -1 | sed 's/.*=\s*"\([^"]*\)".*/\1/' 2>/dev/null || echo "")
            if [ -n "$LISTEN_FROM_CONFIG" ]; then
                LISTEN_ADDR="$LISTEN_FROM_CONFIG"
                echo "Using listen address from config: $LISTEN_ADDR"
            fi
        fi

        if [ -z "$REGION" ]; then
            local REGION_FROM_CONFIG=$(grep -E '^\s*region\s*=' "$CONFIG_FILE" | head -1 | sed 's/.*=\s*"\([^"]*\)".*/\1/' 2>/dev/null || echo "")
            if [ -n "$REGION_FROM_CONFIG" ]; then
                REGION_VALUE="$REGION_FROM_CONFIG"
                echo "Using region from config: $REGION_FROM_CONFIG"
            fi
        fi
    else
        echo "Warning: Configuration file '$CONFIG_FILE' not found, using defaults"
    fi

    local PORT=""
    if [[ "$LISTEN_ADDR" =~ :([0-9]+)$ ]]; then
        PORT="${BASH_REMATCH[1]}"
    fi
    if [ -n "$PORT" ]; then
        local OCCUPIED_PIDS=$(lsof -t -i :"$PORT" 2>/dev/null)
        if [ -n "$OCCUPIED_PIDS" ]; then
            echo "Warning: Port $PORT is already in use by process(es): $OCCUPIED_PIDS"
            echo "Killing process(es) occupying port $PORT..."
            kill -9 $OCCUPIED_PIDS
            sleep 1
        fi
    fi

    # Check if binary exists
    local BINARY_PATH="${CURVINE_HOME}/lib/${SERVICE_NAME}"
    if [ ! -f "$BINARY_PATH" ]; then
        echo "Error: ${SERVICE_NAME} binary not found at $BINARY_PATH"
        echo "Please ensure the project has been built with 'make all'"
        exit 1
    fi
    echo "Starting ${SERVICE_NAME} S3 Gateway..."
    
    # Check if already running
    if check_running; then
        local PID=$(cat ${PID_FILE})
        echo "Error: ${SERVICE_NAME} is already running with PID ${PID}"
        echo "Please stop the service first or use 'restart'"
        exit 1
    fi
    
    # Validate configuration
    if [ -n "$CONF" ] && [ ! -f "$CONF" ]; then
        echo "Error: Configuration file '$CONF' not found"
        exit 1
    fi
    
    # Set configuration values
    local CONFIG_FILE=${CONF:-${DEFAULT_CONF}}
    local LISTEN_ADDR=${LISTEN:-${DEFAULT_LISTEN}}
    local REGION_VALUE=${REGION:-${DEFAULT_REGION}}
    
    # Try to read from config file if not specified
    if [ -f "$CONFIG_FILE" ]; then
        if [ -z "$LISTEN" ]; then
            local LISTEN_FROM_CONFIG=$(grep -E '^\s*listen\s*=' "$CONFIG_FILE" | head -1 | sed 's/.*=\s*"\([^"]*\)".*/\1/' 2>/dev/null || echo "")
            if [ -n "$LISTEN_FROM_CONFIG" ]; then
                LISTEN_ADDR="$LISTEN_FROM_CONFIG"
                echo "Using listen address from config: $LISTEN_ADDR"
            fi
        fi
        
        if [ -z "$REGION" ]; then
            local REGION_FROM_CONFIG=$(grep -E '^\s*region\s*=' "$CONFIG_FILE" | head -1 | sed 's/.*=\s*"\([^"]*\)".*/\1/' 2>/dev/null || echo "")
            if [ -n "$REGION_FROM_CONFIG" ]; then
                REGION_VALUE="$REGION_FROM_CONFIG"
                echo "Using region from config: $REGION_FROM_CONFIG"
            fi
        fi
    else
        echo "Warning: Configuration file '$CONFIG_FILE' not found, using defaults"
    fi
    
    # Check if binary exists
    local BINARY_PATH="${CURVINE_HOME}/lib/${SERVICE_NAME}"
    if [ ! -f "$BINARY_PATH" ]; then
        echo "Error: ${SERVICE_NAME} binary not found at $BINARY_PATH"
        echo "Please ensure the project has been built with 'make all'"
        exit 1
    fi
    
    # Create log directory
    mkdir -p "${LOG_DIR}"
    
    echo "Configuration: $CONFIG_FILE"
    echo "Listen address: $LISTEN_ADDR"
    echo "Region: $REGION_VALUE"
    
    # Start the service in background
    cd "${CURVINE_HOME}"
    nohup env S3_ACCESS_KEY="$S3_ACCESS_KEY" S3_SECRET_KEY="$S3_SECRET_KEY" "${BINARY_PATH}" \
        --conf "$CONFIG_FILE" \
        --listen "$LISTEN_ADDR" \
        --region "$REGION_VALUE" \
        > "${OUT_FILE}" 2>&1 < /dev/null &
    
    local NEW_PID=$!
    sleep 3
    
    # Verify the process started successfully
    if kill -0 ${NEW_PID} > /dev/null 2>&1; then
        echo ${NEW_PID} > "${PID_FILE}"
        echo "${SERVICE_NAME} started successfully with PID ${NEW_PID}"
        echo "Gateway available at: http://${LISTEN_ADDR}"
        echo "Log file: ${OUT_FILE}"
        
        # Show recent log output
        if [ -f "${OUT_FILE}" ]; then
            echo "Recent log output:"
            tail -10 "${OUT_FILE}" 2>/dev/null || echo "No log output yet"
        fi
    else
        echo "Error: ${SERVICE_NAME} failed to start"
        if [ -f "${OUT_FILE}" ]; then
            echo "Error log:"
            cat "${OUT_FILE}"
        fi
        exit 1
    fi
}

# Stop the service
stop_service() {
    echo "Stopping ${SERVICE_NAME}..."
    
    if [ -f "${PID_FILE}" ]; then
        local PID=$(cat ${PID_FILE})
        if kill -0 ${PID} > /dev/null 2>&1; then
            echo "Sending SIGTERM to PID ${PID}..."
            kill ${PID}
            
            # Wait for graceful shutdown
            wait_for_stop ${PID}
            
            # Force kill if still running
            if kill -0 ${PID} > /dev/null 2>&1; then
                echo "Warning: ${SERVICE_NAME} did not stop gracefully after ${GRACEFULLY_TIMEOUT} seconds"
                echo "Force killing with SIGKILL..."
                kill -9 ${PID}
                sleep 1
            fi
            
            # Clean up PID file
            if [ -f "${PID_FILE}" ]; then
                rm -f "${PID_FILE}"
            fi
            
            echo "${SERVICE_NAME} stopped successfully"
        else
            echo "Process ${PID} is not running, cleaning up PID file"
            rm -f "${PID_FILE}"
        fi
    else
        echo "No PID file found, ${SERVICE_NAME} may not be running"
    fi
}

# Main execution logic
main() {
    # Parse arguments
    parse_args "$@"
    
    # Set default action if none specified
    if [ -z "$ACTION" ]; then
        ACTION="start"
    fi
    
    # Execute action
    case "$ACTION" in
        "start")
            start_service
            ;;
        "stop")
            stop_service
            ;;
        "status")
            get_status
            ;;
        "restart")
            echo "Restarting ${SERVICE_NAME}..."
            stop_service
            sleep 2
            start_service
            ;;
        *)
            echo "Unknown action: $ACTION"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 