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

# Get the absolute path to the directory where the script is located
BIN_DIR="$(cd "`dirname "$0"`"; pwd)"

# Close all services and restart.

read_rpc_port() {
    local section=$1
    local default_port=$2
    local conf_file="${CURVINE_CONF_FILE:-${BIN_DIR}/../conf/curvine-cluster.toml}"

    if [ ! -f "$conf_file" ]; then
        echo "$default_port"
        return
    fi

    awk -v section="[$section]" -v default_port="$default_port" '
        /^[[:space:]]*\[/ {
            line = $0
            sub(/[[:space:]]*#.*/, "", line)
            gsub(/[[:space:]]/, "", line)
            in_section = (line == section)
        }
        in_section && /^[[:space:]]*rpc_port[[:space:]]*=/ {
            line = $0
            sub(/[[:space:]]*#.*/, "", line)
            sub(/^[^=]*=/, "", line)
            gsub(/[[:space:]"]/, "", line)
            if (line != "") {
                print line
                found = 1
                exit
            }
        }
        END {
            if (!found) {
                print default_port
            }
        }
    ' "$conf_file"
}

# Function to wait for a process to start
wait_for_process() {
    local service_name=$1
    local timeout=30
    local count=0
    
    echo "Waiting for $service_name to start..."
    while [ $count -lt $timeout ]; do
        if ps -ef | grep "curvine" | grep "$service_name" | grep -v grep > /dev/null; then
            echo "$service_name started successfully"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done
    
    echo "Warning: $service_name did not start within $timeout seconds"
    return 1
}

wait_for_port() {
    local service_name=$1
    local port=$2
    local timeout=${3:-60}
    local count=0

    echo "Waiting for $service_name RPC port $port..."
    while [ $count -lt $timeout ]; do
        if command -v nc >/dev/null 2>&1; then
            if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
                echo "$service_name RPC port $port is ready"
                return 0
            fi
        elif command -v ss >/dev/null 2>&1; then
            if ss -ltn "( sport = :$port )" | grep -q ":$port"; then
                echo "$service_name RPC port $port is ready"
                return 0
            fi
        else
            echo "Warning: neither nc nor ss is available; only process start was verified"
            return 0
        fi

        if ! ps -ef | grep "curvine" | grep "$service_name" | grep -v grep > /dev/null; then
            echo "Error: $service_name exited before RPC port $port became ready"
            tail -80 "${BIN_DIR}/../logs/${service_name}.out" 2>/dev/null || true
            return 1
        fi

        sleep 1
        count=$((count + 1))
    done

    echo "Error: $service_name RPC port $port did not become ready within $timeout seconds"
    tail -80 "${BIN_DIR}/../logs/${service_name}.out" 2>/dev/null || true
    return 1
}

umount -l /curvine-fuse
pkill -9 -f "curvine"

# Wait a moment for processes to be killed
sleep 3

MASTER_PORT=$(read_rpc_port "master" 8995)
WORKER_PORT=$(read_rpc_port "worker" 8997)

# Start master and worker services
${BIN_DIR}/curvine-master.sh start
wait_for_process "master" || exit 1
wait_for_port "master" "$MASTER_PORT" 180 || exit 1

${BIN_DIR}/curvine-worker.sh start

# Wait for master and worker to start
wait_for_process "worker" || exit 1
wait_for_port "worker" "$WORKER_PORT" 60 || exit 1

# Start fuse service
${BIN_DIR}/curvine-fuse.sh start
