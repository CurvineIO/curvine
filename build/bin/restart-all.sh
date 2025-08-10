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

# Close all services and restart.

curr_dir="$(pwd)"

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

pkill -9 -f "curvine"

# Wait a moment for processes to be killed
sleep 3

# Start master and worker services
"${curr_dir}/curvine-master.sh" start
"${curr_dir}/curvine-worker.sh" start

# Wait for master and worker to start
wait_for_process "master"
wait_for_process "worker"

# Start fuse service
"${curr_dir}/curvine-fuse.sh" start
