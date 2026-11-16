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

# S3 object gateway launcher.
#
#   curvine-s3-gateway.sh [serve]            start/stop the gateway
#   curvine-s3-gateway.sh --version          print the version
#   curvine-s3-gateway.sh credential ...     manage access keys
#
# Extra gateway flags (--conf/--listen/--region) are forwarded to the binary
# when the first argument is not a launch-process action.
BIN_DIR="$(cd "`dirname "$0"`"; pwd)"
source "${BIN_DIR}/../conf/curvine-env.sh"

GATEWAY="${CURVINE_HOME}/lib/curvine-s3-gateway"

case "$1" in
  --version|--version-json|-V)
    exec "${GATEWAY}" "$@"
    ;;
  serve|start|stop|restart|status)
    exec "${BIN_DIR}"/launch-process.sh s3-gateway "$@"
    ;;
  credential)
    shift
    exec "${GATEWAY}" credential "$@"
    ;;
  *)
    exec "${GATEWAY}" "$@"
    ;;
esac
