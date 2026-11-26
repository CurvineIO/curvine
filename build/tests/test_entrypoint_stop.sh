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

# The container entrypoint's `stop` must signal every curvine-server process
# of the service that pgrep finds, not just the first one: pgrep prints one
# pid per line.
#
# The harness runs curvine-docker/deploy/entrypoint.sh with a pgrep stub that
# reports two sleep processes (only while they are alive), so `stop` can be
# asserted without a container or a curvine-server binary.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
PIDS=()
cleanup() {
  kill -9 "${PIDS[@]}" 2>/dev/null || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for _ in 1 2; do
  sleep 300 &
  PIDS+=("$!")
  disown "$!"
done
printf '%s\n' "${PIDS[@]}" > "$TMP_DIR/pids"

mkdir -p "$TMP_DIR/stubs"
cat > "$TMP_DIR/stubs/pgrep" <<STUB
#!/usr/bin/env bash
while read -r pid; do
  kill -0 "\$pid" 2>/dev/null && echo "\$pid"
done < "$TMP_DIR/pids"
exit 0
STUB
chmod +x "$TMP_DIR/stubs/pgrep"

output="$(CURVINE_HOME="$TMP_DIR/curvine" FLUID_RUNTIME_CONFIG_PATH="$TMP_DIR/no-fluid.json" \
  PATH="$TMP_DIR/stubs:$PATH" bash "$ROOT/../curvine-docker/deploy/entrypoint.sh" master stop 2>&1)" \
  || fail "entrypoint stop failed: $output"

for pid in "${PIDS[@]}"; do
  if kill -0 "$pid" 2>/dev/null; then
    fail "process $pid was not stopped: $output"
  fi
done
[[ "$output" == *"master stopped gracefully"* ]] || fail "unexpected output: $output"

echo "PASS: entrypoint.sh stop signals every matching process"
