#!/usr/bin/env bash

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

# `launch-process.sh <service> start` must recognise a service whose binary
# name is longer than the 15 characters Linux keeps as a process name
# (TASK_COMM_LEN). `ps -o comm` reports curvine-s3-gateway as
# curvine-s3-gate, so a check against the full name reported a gateway that
# had started as "start failed" and wrote no pid file for `stop`.
#
# The harness runs the real launch-process.sh from a sandbox whose gateway
# binary is a stub that stays up, with a `ps` stub that prints the name the
# way the kernel truncates it, so the check can be asserted on any host.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
SANDBOX="$TMP_DIR/sandbox"
PID_FILE="$SANDBOX/logs/s3-gateway.pid"

cleanup() {
  if [ -f "$PID_FILE" ]; then
    kill "$(cat "$PID_FILE")" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

mkdir -p "$SANDBOX/bin" "$SANDBOX/conf" "$SANDBOX/lib" "$SANDBOX/stubs"
cp "$ROOT/bin/launch-process.sh" "$SANDBOX/bin/"
cp "$ROOT/../etc/curvine-env.sh" "$SANDBOX/conf/"
: > "$SANDBOX/conf/curvine-cluster.toml"
unset CURVINE_CONF_DIR CURVINE_CONF_FILE CURVINE_LOG_DIR

# The gateway binary: stays up like a gateway that started.
cat > "$SANDBOX/lib/curvine-s3-gateway" <<'STUB'
#!/usr/bin/env bash
exec sleep 30
STUB
chmod +x "$SANDBOX/lib/curvine-s3-gateway"

# What `ps -o comm=` prints for that process on Linux.
cat > "$SANDBOX/stubs/ps" <<'STUB'
#!/usr/bin/env bash
echo "curvine-s3-gate"
STUB
chmod +x "$SANDBOX/stubs/ps"

output="$(PATH="$SANDBOX/stubs:$PATH" bash "$SANDBOX/bin/launch-process.sh" s3-gateway start 2>&1)" \
  || fail "start reported a failure: $output"
[[ "$output" == *"s3-gateway start success"* ]] || fail "no success message: $output"
[ -s "$PID_FILE" ] || fail "no pid file was written"

echo "PASS: launch-process.sh start recognises a truncated process name"
