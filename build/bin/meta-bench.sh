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

# Java client side metadata performance test script

. "$(cd "`dirname "$0"`"; pwd)"/../conf/curvine-env.sh
export CLASSPATH=$(echo $CURVINE_HOME/lib/curvine-hadoop-*shade.jar | tr ' ' ':')

# createWrite
# openRead
# rename
# delete
# rmdir
ACTION=$1

java -Xms256m -Xmx256m \
io.curvine.bench.NNBenchWithoutMR \
-operation $ACTION \
-bytesToWrite 0 \
-confDir ${CURVINE_HOME}/conf \
-threads 10 \
-baseDir cv:///fs-meta \
-numFiles 1000