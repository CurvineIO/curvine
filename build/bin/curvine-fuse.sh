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
MNT_PATH=/curvine-fuse

# Number of mount points.
MNT_NUMBER=1

# fuse2 default options: -o allow_other -o async
# fuse3 default options: -o allow_other -o async -o direct_io -o big_write -o max_write=131072
FUSE_OPTS=""

# Cancel the last mount.
umount -f $MNT_PATH;
if [ -d "$fuse_mnt" ]; then
  for file in `ls $MNT_PATH`; do
      umount -f "$MNT_PATH/$file"
  done
fi

mkdir -p $MNT_PATH


"$(cd "`dirname "$0"`"; pwd)"/launch-process.sh fuse $1 \
--mnt-path $MNT_PATH \
--mnt-number ${MNT_NUMBER} \
${FUSE_OPTS}