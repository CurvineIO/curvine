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

# Run performance testing tools
# client mode write: bin/curvine-bench.sh fs.write /fs-bench
# client mode reading: bin/curvine-bench.sh fs.read /fs-bench
#
# # fuse mode, need to add mount point path
# client mode write: bin/curvine-bench.sh fuse.write /curvine-fuse/fs-bench
# client mode reading: bin/curvine-bench.sh fuse.read /curvine-fuse/fs-bench


. "$(cd "`dirname "$0"`"; pwd)"/../conf/curvine-env.sh

ACTION=$1
DIR=$2
if [ -z "$DIR" ]; then
  if [[ "$ACTION" == fuse* ]]; then
    DIR="/curvine-fuse/fs-bench"
  else
    DIR="/fs-bench"
  fi
fi

${CURVINE_HOME}/lib/curvine-bench \
--action ${ACTION} \
--dir $DIR \
--conf $CURVINE_HOME/conf/curvine-cluster.toml \
--checksum true \
--client-threads 10 \
--buf-size 128KB \
--file-size 100MB \
--file-num 10

# Performance Testing
# ${CURVINE_HOME}/lib/curvine-bench \
# --action ${ACTION} \
# --dir $DIR \
# --conf $CURVINE_HOME/conf/curvine-cluster.toml \
# --checksum true \
# --client-threads 40 \
# --buf-size 128kb \
# --file-size 100MB \
# --file-num 10000