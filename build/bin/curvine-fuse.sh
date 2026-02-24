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
NEXT_IS_PATH=false
HAS_ENTRY_TIMEOUT=false
HAS_ATTR_TIMEOUT=false

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
    elif [[ "$arg" == --entry-timeout=* ]]; then
        HAS_ENTRY_TIMEOUT=true
    elif [[ "$arg" == --attr-timeout=* ]]; then
        HAS_ATTR_TIMEOUT=true
    elif [[ "$arg" == "--mnt-path" ]]; then
        NEXT_IS_PATH=true
        continue
    elif [[ "$arg" == "--entry-timeout" ]]; then
        HAS_ENTRY_TIMEOUT=true
        continue
    elif [[ "$arg" == "--attr-timeout" ]]; then
        HAS_ATTR_TIMEOUT=true
        continue
    elif [[ "$NEXT_IS_PATH" == true ]]; then
        MNT_PATH="$arg"
        NEXT_IS_PATH=false
    fi
done

mkdir -p "$MNT_PATH"

EXTRA_ARGS=()
STRICT_VISIBILITY="${CURVINE_FUSE_STRICT_VISIBILITY:-0}"
if [[ "$STRICT_VISIBILITY" == "1" ]] || [[ "$STRICT_VISIBILITY" == "true" ]]; then
    # Optional strict mode for read-after-write visibility tests.
    # Keep default timeout behavior unchanged unless explicitly enabled.
    if [[ "$HAS_ENTRY_TIMEOUT" == "false" ]]; then
        EXTRA_ARGS+=(--entry-timeout 0)
    fi
    if [[ "$HAS_ATTR_TIMEOUT" == "false" ]]; then
        EXTRA_ARGS+=(--attr-timeout 0)
    fi
fi

echo "Starting curvine-fuse with arguments: $* ${EXTRA_ARGS[*]}"

# Pass all arguments to launch-process.sh
"$(cd "$(dirname "$0")"; pwd)"/launch-process.sh fuse "$@" "${EXTRA_ARGS[@]}"
