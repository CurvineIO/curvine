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

# restart-all.sh stops only the services it manages.
#
# It used to run `pkill -9 -f "curvine"`, which killed every process whose
# command line mentioned curvine: another Curvine deployment on the same
# host, an editor with a Curvine file open, a shell in a curvine/ directory,
# even a `tail -f curvine.log`. Each service's own stop signals only the
# PID recorded in that service's pid file, so the stop phase calls those,
# in the reverse order the services were started in.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TMP_DIR="$(mktemp -d)"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

# The calls the fake services recorded, and the processes they spawned.
CALLS="$TMP_DIR/calls.txt"
PIDS="$TMP_DIR/pids.txt"
: > "$CALLS"
: > "$PIDS"

cleanup() {
    while read -r pid; do
        kill "$pid" 2>/dev/null || true
    done < "$PIDS"
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# A copy of the script, with fake services beside it. A fake records the
# call it was given, and on a start it leaves behind a process whose
# command line names the service, which is what the script waits for.
cp "$ROOT/bin/restart-all.sh" "$TMP_DIR/restart-all.sh"

for service in fuse transfer worker master; do
    cat > "$TMP_DIR/curvine-${service}.sh" <<EOF
#!/usr/bin/env bash
echo "${service} \$1" >> "${CALLS}"
if [ "\$1" = "start" ]; then
    bash -c 'exec -a curvine-${service} sleep 300' &
    echo \$! >> "${PIDS}"
fi
EOF
    chmod +x "$TMP_DIR/curvine-${service}.sh"
done

# The ports the services are expected on, with a listener on each, so the
# script's wait for a port passes. Where python3 is missing, the script
# falls back to the process it started.
python3 - "$TMP_DIR" <<'EOF' &
import socket
import sys
import time

socks = []
for port in (8995, 8997, 9010):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
        sock.listen(5)
        socks.append(sock)
    except OSError:
        pass
time.sleep(300)
EOF
echo $! >> "$PIDS"

# A process that mentions curvine in its command line and is not a
# service: the script must leave it alone.
bash -c 'exec -a curvine-decoy sleep 300' &
DECOY_PID=$!
echo "$DECOY_PID" >> "$PIDS"

cat > "$TMP_DIR/curvine-cluster.toml" <<'EOF'
[transfer]
enabled = true
EOF

export CURVINE_CONF_FILE="$TMP_DIR/curvine-cluster.toml"

# The script, against the fake services. The start phase waits for the
# services it starts, so the run is bounded.
if ! timeout 120 bash "$TMP_DIR/restart-all.sh" > "$TMP_DIR/out.txt" 2>&1; then
    cat "$TMP_DIR/out.txt" >&2
    fail "restart-all.sh did not finish"
fi

# The decoy is still alive: the script did not kill processes by name.
if ! kill -0 "$DECOY_PID" 2>/dev/null; then
    fail "restart-all.sh killed a process that only mentions curvine"
fi

# The services were stopped in the reverse order they were started in, and
# then started again.
expected="fuse stop
transfer stop
worker stop
master stop
master start
worker start
transfer start
fuse start"
actual="$(cat "$CALLS")"
if [ "$actual" != "$expected" ]; then
    fail "the services were not stopped in the reverse order they were started in:
expected:
$expected
actual:
$actual"
fi

# Nothing in the script may kill a process by name again.
if grep -qE '^[[:space:]]*pkill' "$TMP_DIR/restart-all.sh"; then
    fail "restart-all.sh still kills processes by name"
fi

echo "PASS: restart-all.sh stops only the services it manages"
