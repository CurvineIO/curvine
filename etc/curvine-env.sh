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

export CURVINE_HOME="$(cd "$(dirname "$0")"/..; pwd)"

OS_NAME=$(uname)
# Get the IP address from hostname, taking the last network interface address
LOCAL_HOSTNAME="localhost"

if [ "$OS_NAME" == "Linux" ]; then
    LOCAL_IP=$(hostname -I | awk '{print $NF}')
elif [ "$OS_NAME" == "Darwin" ]; then
    LOCAL_IP=$(ifconfig | grep "inet " | awk '{print $2}' | grep -v 127.0.0.1 | tail -n 1)
else
    echo "Unsupported OS: $OS_NAME"
    exit 1
fi


# master bound host name
export CURVINE_MASTER_HOSTNAME=$LOCAL_HOSTNAME

# worker bound host name
export CURVINE_WORKER_HOSTNAME=$LOCAL_IP

# The client server hostname is used to determine whether the worker and client are on the same machine.
export CURVINE_CLIENT_HOSTNAME=$LOCAL_HOSTNAME

export ORPC_BIND_HOSTNAME=0.0.0.0

export CURVINE_CONF_FILE=$CURVINE_HOME/conf/curvine-cluster.toml
