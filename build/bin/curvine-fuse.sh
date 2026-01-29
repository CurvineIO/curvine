#!/bin/bash

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

# Execute fuse mount with parameter passthrough

# Default mount point
MNT_PATH=/curvine-fuse

# Process arguments in a single loop
for arg in "$@"; do
    if [[ "$arg" == "--help" ]] || [[ "$arg" == "-h" ]]; then
        . "$(cd "`dirname "$0"`"; pwd)"/../conf/curvine-env.sh
        ${CURVINE_HOME}/lib/curvine-fuse --help
        exit 0
    fi
    
    # Extract mount path from arguments if specified
    if [[ "$arg" == --mnt-path=* ]]; then
        MNT_PATH="${arg#*=}"
    elif [[ "$arg" == "--mnt-path" ]]; then
        next_is_path=true
        continue
    elif [[ "$next_is_path" == true ]]; then
        MNT_PATH="$arg"
        next_is_path=false
    fi
done

mkdir -p "$MNT_PATH"

echo "Starting curvine-fuse with arguments: $*"

# Pass all arguments to launch-process.sh
"$(cd "$(dirname "$0")"; pwd)"/launch-process.sh fuse "$@"