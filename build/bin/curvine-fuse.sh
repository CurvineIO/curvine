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

# Execute fuse mount.
set -x
set -euo pipefail

# Detect OS (robust macOS detection)
OS=$(uname -s)
IS_DARWIN=0
if [[ "${OSTYPE:-}" == darwin* ]] || echo "$OS" | grep -qi "darwin"; then
  IS_DARWIN=1
fi

# Mount Point (override with CURVINE_MNT or --mnt-path in launch-process)
if [ "${CURVINE_MNT:-}" != "" ]; then
  MNT_PATH="${CURVINE_MNT}"
else
  if [ "$IS_DARWIN" = "1" ]; then
    # On macOS default to user directory to avoid root requirement
    MNT_PATH="$HOME/curvine-fuse"
  else
    MNT_PATH="/curvine-fuse"
  fi
fi

# Number of mount points.
MNT_NUMBER=1

# fuse2 default options: -o allow_other -o async
# fuse3 default options: -o allow_other -o async -o direct_io -o big_write -o max_write=131072
FUSE_OPTS=""

# Cancel the last mount.
unmount_path() {
  local path="$1"
  if [ -z "$path" ]; then return 0; fi
  if [ "$IS_DARWIN" = "1" ]; then
    umount -f "$path" >/dev/null 2>&1 || diskutil unmount force "$path" >/dev/null 2>&1 || true
  else
    if command -v fusermount3 >/dev/null 2>&1; then
      fusermount3 -u "$path" >/dev/null 2>&1 || true
    elif command -v fusermount >/dev/null 2>&1; then
      fusermount -u "$path" >/dev/null 2>&1 || true
    else
      umount -f "$path" >/dev/null 2>&1 || true
    fi
  fi
}

unmount_path "$MNT_PATH"
if [ -d "$MNT_PATH" ]; then
  for file in "$MNT_PATH"/*; do
    [ -e "$file" ] || continue
    unmount_path "$file"
  done
fi

# Informational note for macOS users
if [ "$IS_DARWIN" = "1" ]; then
  if [ -f "/usr/local/lib/libfuse-t.dylib" ] || [ -f "/opt/homebrew/lib/libfuse-t.dylib" ]; then
    echo "[info] Detected FUSE-T on macOS"
  else
    echo "[warn] FUSE-T not found. Install via 'brew install fuse-t'." >&2
  fi
  echo "[warn] curvine-fuse currently only supports Linux; macOS mount will likely fail at runtime." >&2
fi

mkdir -p "$MNT_PATH"


"$(cd "`dirname "$0"`"; pwd)"/launch-process.sh fuse ${1:-} \
--mnt-path "$MNT_PATH" \
--mnt-number "${MNT_NUMBER}" \
${FUSE_OPTS}