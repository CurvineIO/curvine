#!/usr/bin/env bash

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

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE="$(mktemp)"
trap 'rm -f "$FIXTURE"' EXIT

cat > "$FIXTURE" <<'EOF'
1 2 0:1 / /mnt/fuse\040data rw - fuse.curvinefs curvinefs rw
2 3 0:1 / /mnt/fuse\011tab rw - fuse.curvinefs curvinefs rw
3 4 0:1 / /mnt/fuse\012newline rw - fuse.curvinefs curvinefs rw
4 5 0:1 / /mnt/fuse\134backslash rw - fuse.curvinefs curvinefs rw
EOF

# Source the production script without executing its action dispatcher.
source "$ROOT/build/bin/curvine-fuse.sh"

is_mountpoint "/mnt/fuse data" "$FIXTURE"
is_mountpoint $'/mnt/fuse\ttab' "$FIXTURE"
is_mountpoint $'/mnt/fuse\nnewline' "$FIXTURE"
is_mountpoint '/mnt/fuse\backslash' "$FIXTURE"

if is_mountpoint "/mnt/not-mounted" "$FIXTURE"; then
    echo "unexpectedly matched an absent mountpoint" >&2
    exit 1
fi

echo "curvine fuse mountinfo escaping: ok"
