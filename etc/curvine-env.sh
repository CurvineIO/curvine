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

LOCAL_HOSTNAME=localhost
LOCAL_IP=localhost

# master bound host name
export CURVINE_MASTER_HOSTNAME=${CURVINE_MASTER_HOSTNAME:-$LOCAL_HOSTNAME}

# worker bound host name
export CURVINE_WORKER_HOSTNAME=${CURVINE_WORKER_HOSTNAME:-$LOCAL_IP}

# The client server hostname is used to determine whether the worker and client are on the same machine.
export CURVINE_CLIENT_HOSTNAME=${CURVINE_CLIENT_HOSTNAME:-$LOCAL_IP}

export ORPC_BIND_HOSTNAME=0.0.0.0

# Configuration directory for curvine-cluster.toml. Can be overridden to
# place the cluster configuration outside CURVINE_HOME, e.g. when the
# installation directory is replaced on upgrade. This file itself always
# comes from the installation so it stays in sync with the launchers.
export CURVINE_CONF_DIR=${CURVINE_CONF_DIR:-$CURVINE_HOME/conf}

export CURVINE_CONF_FILE=${CURVINE_CONF_FILE:-$CURVINE_CONF_DIR/curvine-cluster.toml}

# Directory for service stdout files (*.out) and pid files. The internal log
# files of each service are controlled by log_dir in curvine-cluster.toml.
export CURVINE_LOG_DIR=${CURVINE_LOG_DIR:-$CURVINE_HOME/logs}
