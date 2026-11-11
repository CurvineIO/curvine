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

# restart-all.sh must stop services through their own scripts (pid-file based),
# never via a command-line-wide `pkill -f curvine` that also kills unrelated
# processes: an editor holding a file under a curvine checkout, a `tail -f` on
# a log, or a second deployment on the same host.
#
# The harness runs restart-all.sh from a sandbox whose service scripts are
# stubs that record every invocation, so the stop orchestration can be
# asserted without a real cluster. Probe processes keep `wait_for_process`
# happy; a throwaway python listener keeps `wait_for_port` happy on hosts
# that have nc/ss.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_UNDER_TEST="$ROOT/bin/restart-all.sh"

TMP_DIR="$(mktemp -d)"
trap 'kill "${PROBE_PIDS[@]:-}" "${LISTENER_PID:-}" 2>/dev/null || true; rm -rf "$TMP_DIR"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

PYTHON="$(command -v python3 || command -v python)" || fail "python is required to fake RPC listeners"

# --- fixtures ---------------------------------------------------------------

setup_sandbox() {
  SANDBOX="$TMP_DIR/sandbox"
  mkdir -p "$SANDBOX/bin" "$SANDBOX/logs" "$SANDBOX/conf"
  CALLS_LOG="$SANDBOX/calls.log"
  : > "$CALLS_LOG"
  cp "$SCRIPT_UNDER_TEST" "$SANDBOX/bin/restart-all.sh"
  for svc in master worker transfer fuse; do
    cat > "$SANDBOX/bin/curvine-$svc.sh" <<EOF
#!/usr/bin/env bash
echo "curvine-$svc.sh \$*" >> "$CALLS_LOG"
EOF
    chmod +x "$SANDBOX/bin/curvine-$svc.sh"
  done
}

write_conf() {
  # %b turns the \n escapes into real newlines; the awk parsers inside
  # restart-all.sh are line based.
  printf '%b\n' "$1" > "$SANDBOX/conf/curvine-cluster.toml"
}

# A background process whose command line mentions curvine, like an editor or
# tail on a log would. The old `pkill -9 -f "curvine"` killed it (and the
# probes); the pid-file based stops must leave it alone.
spawn_unrelated_process() {
  # exec -a puts the marker in argv[0]: that is what `ps -ef` reports and
  # what `pkill -f curvine` used to match.
  bash -c 'exec -a curvine-innocent-editor sleep 60' &
  UNRELATED_PID=$!
  PROBE_PIDS+=("$UNRELATED_PID")
}

spawn_service_probes() {
  local svc
  for svc in "$@"; do
    bash -c "exec -a curvine-$svc-probe sleep 60" &
    PROBE_PIDS+=("$!")
  done
}

# accept() loop on the RPC ports so nc -z / ss finds them listening.
spawn_listeners() {
  "$PYTHON" - "$@" <<'PYEOF' &
import socket, sys, time
socks = []
for p in (int(a) for a in sys.argv[1:]):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", p))
    s.listen(16)
    socks.append(s)
end = time.time() + 120
while time.time() < end:
    for s in socks:
        s.settimeout(0.5)
        try:
            s.accept()
        except OSError:
            pass
PYEOF
  LISTENER_PID=$!
  PROBE_PIDS+=("$LISTENER_PID")
  sleep 1
}

# The stop section must come before the starts, in reverse start order
# (fuse, [transfer], worker, master) and with no transfer stop when the
# transfer section is disabled.
assert_stop_sequence() {
  local expected=("$@")
  local actual=()
  # while-read instead of mapfile: mapfile needs bash 4+, macOS still ships 3.2
  local line
  while IFS= read -r line; do
    actual+=("$line")
  done < <(grep -F ' stop' "$CALLS_LOG" || true)
  [ "${#actual[@]}" = "${#expected[@]}" ] ||
    fail "expected ${#expected[@]} stop calls, got ${#actual[@]}: ${actual[*]:-none}"
  local i
  for i in "${!expected[@]}"; do
    [ "${actual[$i]}" = "${expected[$i]}" ] ||
      fail "stop #$((i + 1)) was '${actual[$i]}', want '${expected[$i]}'"
  done
}

# --- the old script must not pass these checks either -----------------------

if grep -qE '^[[:space:]]*pkill' "$SCRIPT_UNDER_TEST"; then
  fail "restart-all.sh still contains a pkill"
fi

# --- scenario 1: transfer disabled -> fuse, worker, master ------------------

setup_sandbox
write_conf '[master]
rpc_port = 18995
[worker]
rpc_port = 18997'
spawn_listeners 18995 18997
spawn_service_probes master worker
spawn_unrelated_process

CURVINE_CONF_FILE="$SANDBOX/conf/curvine-cluster.toml" bash "$SANDBOX/bin/restart-all.sh" \
  > "$SANDBOX/run.log" 2>&1 || fail "restart-all.sh exited non-zero, log: $(tail -5 "$SANDBOX/run.log")"

assert_stop_sequence "curvine-fuse.sh stop" "curvine-worker.sh stop" "curvine-master.sh stop"
grep -qF 'curvine-transfer.sh stop' "$CALLS_LOG" &&
  fail "transfer is disabled but was stopped"
kill -0 "$UNRELATED_PID" 2>/dev/null || fail "unrelated process mentioning curvine was killed"
grep -qF 'curvine-master.sh start' "$CALLS_LOG" || fail "master was not restarted"
grep -qF 'curvine-fuse.sh start' "$CALLS_LOG" || fail "fuse was not restarted"

# --- scenario 2: transfer enabled -> fuse, transfer, worker, master ---------
# Distinct ports from scenario 1: scenario 1's listener is left running and
# SO_REUSEADDR cannot take a port away from an active listener, so reusing
# 18995/18997 would kill the scenario 2 listener on bind (18999 never opens).

setup_sandbox
write_conf '[master]
rpc_port = 18985
[worker]
rpc_port = 18987
[transfer]
enabled = true
rpc_port = 18989'
spawn_listeners 18985 18987 18989
spawn_service_probes master worker transfer

CURVINE_CONF_FILE="$SANDBOX/conf/curvine-cluster.toml" bash "$SANDBOX/bin/restart-all.sh" \
  > "$SANDBOX/run.log" 2>&1 || fail "restart-all.sh exited non-zero, log: $(tail -5 "$SANDBOX/run.log")"

assert_stop_sequence "curvine-fuse.sh stop" "curvine-transfer.sh stop" "curvine-worker.sh stop" "curvine-master.sh stop"

echo "PASS: restart-all.sh stops only the managed services, in reverse start order"
