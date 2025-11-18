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

# Java customer service performance test script
# client mode write: bin/java-bench.sh fs.write /fs-bench
# client mode reading: bin/java-bench.sh fs.read /fs-bench
#
# fuse mode, need to add mount point path
# client mode write: bin/java-bench.sh fuse.write /curvine-fuse/fs-bench
# client mode reading: bin/java-bench.sh fuse.read /curvine-fuse/fs-bench


. "$(cd "`dirname "$0"`"; pwd)"/../conf/curvine-env.sh
export CLASSPATH=$(echo $CURVINE_HOME/lib/curvine-hadoop-*shade.jar | tr ' ' ':')

ACTION=$1
DIR=$2
if [ -z "$DIR" ]; then
  if [[ "$ACTION" == fuse* ]]; then
      DIR="/curvine-fuse/fs-bench"
  else
     DIR="/fs-bench"
  fi
fi

java -Xms256m -Xmx1g \
-Dcurvine.conf.dir=${CURVINE_HOME}/conf \
io.curvine.bench.CurvineBenchV2 \
-action $ACTION \
-dataDir $DIR \
-threads 10 \
-bufferSize 128kb \
-fileSize 100mb \
-fileNum 10 \
-checksum true \
-clearDir fasle


# Performance test code.
#java -Xms10g -Xmx10g \
#-Dcurvine.conf.dir=${CURVINE_HOME}/conf \
#io.curvine.bench.CurvineBench \
#-action $ACTION \
#-dataDir $DIR \
#-threads 40 \
#-bufferSize 128kb \
#-fileSize 100mb \
#-fileNum 1000 \
# -checksum true \
#-clearDir fasle