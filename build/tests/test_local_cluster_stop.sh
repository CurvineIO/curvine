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

# `local-cluster.sh stop` must notice a service that its stop script left
# running: without `force` it says so and fails, and `stop force` kills that
# service's recorded pid. It must not kill other processes whose command line
# merely contains "curvine" (an editor, a log tail, a second deployment).
#
# The harness runs the real local-cluster.sh from a sandbox whose service
# scripts are stubs that ignore `stop`, so a "stuck" service is a sleep
# process whose pid is in master.pid.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
SANDBOX="$TMP_DIR/sandbox"
STUCK_PID=""
DECOY_PID=""

cleanup() {
  kill -9 ${STUCK_PID:-} ${DECOY_PID:-} 2>/dev/null || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

alive() {
  kill -0 "$1" 2>/dev/null
}

mkdir -p "$SANDBOX/bin" "$SANDBOX/conf" "$SANDBOX/logs"
cp "$ROOT/bin/local-cluster.sh" "$SANDBOX/bin/"
cp "$ROOT/../etc/curvine-env.sh" "$SANDBOX/conf/"
: > "$SANDBOX/conf/curvine-cluster.toml"
unset CURVINE_CONF_DIR CURVINE_CONF_FILE CURVINE_LOG_DIR

# Service scripts whose `stop` leaves the process running.
for svc in master worker; do
  cat > "$SANDBOX/bin/curvine-$svc.sh" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
  chmod +x "$SANDBOX/bin/curvine-$svc.sh"
done

# The stuck master, and an unrelated process with "curvine" in its command line.
sleep 300 &
STUCK_PID=$!
disown "$STUCK_PID"
echo "$STUCK_PID" > "$SANDBOX/logs/master.pid"
cat > "$TMP_DIR/curvine-decoy.sh" <<'DECOY'
#!/usr/bin/env bash
exec -a curvine-decoy sleep 300
DECOY
bash "$TMP_DIR/curvine-decoy.sh" &
DECOY_PID=$!
disown "$DECOY_PID"

# Without force: the stuck service is reported and stop fails.
if output="$(bash "$SANDBOX/bin/local-cluster.sh" stop 2>&1)"; then
  fail "stop reported success with master still running: $output"
fi
[[ "$output" == *"master is still running"* ]] || fail "no still-running warning: $output"
[[ "$output" != *"All services stopped"* ]] || fail "claimed all services stopped: $output"
alive "$STUCK_PID" || fail "stop without force killed the master"

# With force: the recorded pid is killed and its pid file removed.
output="$(bash "$SANDBOX/bin/local-cluster.sh" stop force 2>&1)" \
  || fail "stop force failed: $output"
sleep 1
alive "$STUCK_PID" && fail "stop force left the stuck master running: $output"
[ ! -e "$SANDBOX/logs/master.pid" ] || fail "stop force left master.pid behind"
alive "$DECOY_PID" || fail "stop force killed an unrelated process with curvine in its command line"

echo "PASS: local-cluster.sh stop reports and force-kills only the stuck services"
