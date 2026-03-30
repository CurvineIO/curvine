#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
cleanup() {
  set +e
  [[ -n "${FWD_PID:-}" ]] && kill "$FWD_PID" >/dev/null 2>&1 || true
  [[ -n "${BACKEND_PID:-}" ]] && kill "$BACKEND_PID" >/dev/null 2>&1 || true
  wait "${FWD_PID:-}" >/dev/null 2>&1 || true
  wait "${BACKEND_PID:-}" >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

read -r FORWARD_PORT BACKEND_PORT < <(python3 - <<'PY'
import socket
ports = []
for _ in range(2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    ports.append(s.getsockname()[1])
    s.close()
print(*ports)
PY
)

python3 -u - <<'PY' "$BACKEND_PORT" >"$TMP_DIR/backend.log" 2>&1 &
import socket
import sys
import time

port = int(sys.argv[1])
ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ls.bind(("127.0.0.1", port))
ls.listen(1)
while True:
    conn, _ = ls.accept()
    data = conn.recv(4)
    if data != b"ping":
        conn.close()
        continue
    time.sleep(6.2)
    conn.sendall(b"pong")
    conn.close()
    break
ls.close()
PY
BACKEND_PID=$!

python3 -u "$ROOT/scripts/tests/master-forwarder.py" \
  --listen-host 127.0.0.1 \
  --listen-port "$FORWARD_PORT" \
  --target-host 127.0.0.1 \
  --target-port "$BACKEND_PORT" \
  >"$TMP_DIR/forwarder.log" 2>&1 &
FWD_PID=$!

python3 - <<'PY' "$FORWARD_PORT"
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 5
while time.time() < deadline:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        if sock.connect_ex(("127.0.0.1", port)) == 0:
            raise SystemExit(0)
    time.sleep(0.1)
raise SystemExit("forwarder did not start")
PY

python3 - <<'PY' "$FORWARD_PORT"
import socket
import sys

port = int(sys.argv[1])
with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
    sock.settimeout(8)
    sock.sendall(b"ping")
    data = sock.recv(4)
if data != b"pong":
    raise SystemExit(f"unexpected response: {data!r}")
PY
