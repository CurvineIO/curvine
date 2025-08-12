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

# Mount Point
# Allow override via environment variable
if [ -n "$MNT_PATH" ]; then
    # User specified mount path
    echo "Using user-specified mount path: $MNT_PATH"
elif [ "$EUID" -eq 0 ]; then
    # Root user default
    MNT_PATH=/curvine-fuse
else
    # Non-root user default
    MNT_PATH=$HOME/curvine-fuse
fi

# Number of mount points.
MNT_NUMBER=1

# fuse2 default options: -o allow_other -o async
# fuse3 default options: -o allow_other -o async -o direct_io -o big_write -o max_write=131072
# For non-root users, add user_allow_other option (requires /etc/fuse.conf configuration), this need restart fuse service
FUSE_OPTS=""

# Cancel the last mount.
# Only use -f flag for root user
if [ "$EUID" -eq 0 ]; then
    umount -f $MNT_PATH 2>/dev/null || true
else
    fusermount -u $MNT_PATH 2>/dev/null || true
fi

if [ -d "$MNT_PATH" ]; then
  for file in `ls $MNT_PATH 2>/dev/null`; do
      if [ "$EUID" -eq 0 ]; then
          umount -f "$MNT_PATH/$file" 2>/dev/null || true
      else
          fusermount -u "$MNT_PATH/$file" 2>/dev/null || true
      fi
  done
fi

mkdir -p $MNT_PATH


"$(cd "`dirname "$0"`"; pwd)"/launch-process.sh fuse $1 \
--mnt-path $MNT_PATH \
--mnt-number ${MNT_NUMBER} \
${FUSE_OPTS}