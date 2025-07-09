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

# local-cluster.sh -Used to start a stand-alone Curvine cluster locally
# Support start, stop, restart, status operations

# Get the absolute path to the directory where the script is located
BIN_DIR="$(cd "`dirname "$0"`"; pwd)"
CURVINE_HOME="$(cd "$BIN_DIR/.."; pwd)"

# Loading environment variables
. "$CURVINE_HOME/conf/curvine-env.sh"

# Define the service list
SERVICES=("master" "worker")

# Define log directory
LOG_DIR="$CURVINE_HOME/logs"
mkdir -p "$LOG_DIR"

# Define color
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

# Print a message with color
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check service status
check_service_status() {
    local service=$1
    local pid_file="$CURVINE_HOME/${service}.pid"
    
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" > /dev/null 2>&1; then
            echo "$service is running (PID: $pid)"
            return 0
        else
            echo "$service is not running (stale PID file exists)"
            return 1
        fi
    else
        echo "$service is not running"
        return 1
    fi
}

# Start all services
start_all() {
    print_info "Starting Curvine local cluster..."
    
    # Check if there is already a service running
    local running=false
    for service in "${SERVICES[@]}"; do
        if [ -f "$CURVINE_HOME/${service}.pid" ]; then
            local pid=$(cat "$CURVINE_HOME/${service}.pid")
            if kill -0 "$pid" > /dev/null 2>&1; then
                print_warn "$service is already running (PID: $pid)"
                running=true
            fi
        fi
    done
    
    if [ "$running" = true ] && [ "$1" != "force" ]; then
        print_error "Some services are already running. Use 'restart' or 'start force' to restart them."
        return 1
    fi
    
    # Start each service
    for service in "${SERVICES[@]}"; do
        print_info "Starting $service..."
        "$BIN_DIR/curvine-${service}.sh" start
        sleep 2
    done
    
    print_info "All services started. Use 'status' to check cluster status."
}

# Stop all services
stop_all() {
    print_info "Stopping Curvine local cluster..."
    
    # Stop service in reverse order
    for (( idx=${#SERVICES[@]}-1 ; idx>=0 ; idx--)) ; do
        service="${SERVICES[idx]}"
        print_info "Stopping $service..."
        "$BIN_DIR/curvine-${service}.sh" stop
        sleep 1
    done
    
    # Check if any service is still running
    local still_running=false
    for service in "${SERVICES[@]}"; do
        if [ -f "$CURVINE_HOME/${service}.pid" ]; then
            local pid=$(cat "$CURVINE_HOME/${service}.pid")
            if kill -0 "$pid" > /dev/null 2>&1; then
                print_warn "$service is still running (PID: $pid)"
                Still_running=true
            fi
        fi
    done
    
    if [ "$still_running" = true ] && [ "$1" = "force" ]; then
        print_warn "Force killing remaining processes..."
        pkill -9 -f "curvine"
        for service in "${SERVICES[@]}"; do
            if [ -f "$CURVINE_HOME/${service}.pid" ]; then
                rm -f "$CURVINE_HOME/${service}.pid"
            fi
        done
    fi
    
    print_info "All services stopped."
}

# Restart all services
restart_all() {
    print_info "Restarting Curvine local cluster..."
    stop_all "$1"
    sleep 3
    start_all "force"
}

# Show all service status
show_status() {
    print_info "Curvine local cluster status:"
    echo "-----------------------------------"
    
    local all_running=true
    for service in "${SERVICES[@]}"; do
        echo -n "$service: "
        if ! check_service_status "$service"; then
            all_running=false
        fi
    done
    
    echo "-----------------------------------"
    if [ "$all_running" = true ]; then
        print_info "Cluster is fully operational."
    else
        print_warn "Some services are not running."
    fi
}

# Show Help
show_usage() {
echo "Usage: $0 [start|stop|restart|status] [force]"
    echo ""
    echo "Commands:"
    echo "  start   - Start the Curvine local cluster"
    echo "  stop    - Stop the Curvine local cluster"
    echo "  restart - Restart the Curvine local cluster"
    echo "  status  - Show status of all services"
    echo ""
    echo "Options:"
    echo "  force   - Force operation (kill processes if needed)"
}

# Main function
main() {
    local command=$1
    local option=$2
    
    case "$command" in
        "start")
            start_all "$option"
            ;;
        "stop")
            stop_all "$option"
            ;;
        "restart")
            restart_all "$option"
            ;;
        "status")
            show_status
            ;;
        *)
            show_usage
            ;;
    esac
}

# Execute the main function
main "$@"