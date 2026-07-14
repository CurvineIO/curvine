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

OS_NAME=$(uname 2>/dev/null || echo unknown)
LOCAL_HOSTNAME=$(hostname 2>/dev/null || true)
LOCAL_HOSTNAME=${LOCAL_HOSTNAME:-localhost}

# Get the last IP address from local network interfaces, falling back to loopback.
LOCAL_IP=127.0.0.1
case "$OS_NAME" in
    Linux)
        DETECTED_IP=$(hostname -I 2>/dev/null | awk '{print $NF}')
        ;;
    Darwin)
        DETECTED_IP=$(ifconfig 2>/dev/null | awk '$1 == "inet" && $2 != "127.0.0.1" { ip = $2 } END { print ip }')
        ;;
    *)
        DETECTED_IP=
        ;;
esac
LOCAL_IP=${DETECTED_IP:-$LOCAL_IP}

# master bound host name
export CURVINE_MASTER_HOSTNAME=${CURVINE_MASTER_HOSTNAME:-$LOCAL_HOSTNAME}

# worker bound host name
export CURVINE_WORKER_HOSTNAME=${CURVINE_WORKER_HOSTNAME:-$LOCAL_IP}

# The client server hostname is used to determine whether the worker and client are on the same machine.
export CURVINE_CLIENT_HOSTNAME=${CURVINE_CLIENT_HOSTNAME:-$LOCAL_IP}

export ORPC_BIND_HOSTNAME=0.0.0.0

export CURVINE_CONF_FILE=${CURVINE_CONF_FILE:-$CURVINE_HOME/conf/curvine-cluster.toml}
