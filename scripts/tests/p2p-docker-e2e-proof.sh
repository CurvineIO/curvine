#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER_BIN="$ROOT/target/debug/p2p_proof_driver"
EVID_ROOT="${P2P_EVID_ROOT:-$ROOT/evidence/p2p-proof}"
TS="$(date +%Y%m%d-%H%M%S)"
EVID="$EVID_ROOT/$TS"
DATA_MB="${P2P_DATA_MB:-8}"
CONSUMER_READS="${P2P_CONSUMER_READS:-1}"
CONSUMER_READ_INTERVAL_MS="${P2P_CONSUMER_READ_INTERVAL_MS:-0}"
METRICS_WAIT_SECS="${P2P_METRICS_WAIT_SECS:-12}"
NETEM_RULE="${P2P_NETEM_RULE:-}"
RUNTIME_IMAGE="${P2P_RUNTIME_IMAGE:-curvine/p2p-proof-runner:latest}"
mkdir -p "$EVID"

log() {
  echo "[p2p-docker-e2e] $*"
}

sum_metric() {
  local file="$1"
  local name="$2"
  awk -v n="$name" '$1 ~ ("^" n "($|\\{)") {sum+=$2} END {print sum+0}' "$file"
}

cleanup() {
  set +e
  if [[ -n "${TCPDUMP_PID:-}" ]]; then
    kill "$TCPDUMP_PID" >/dev/null 2>&1 || true
    wait "$TCPDUMP_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${FWD_PID:-}" ]]; then
    kill "$FWD_PID" >/dev/null 2>&1 || true
    wait "$FWD_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f client-a >/dev/null 2>&1 || true
  docker rm -f client-b >/dev/null 2>&1 || true
  docker network rm cv-p2p-net >/dev/null 2>&1 || true
}

can_manage_cluster() {
  [[ -x "$ROOT/build/bin/local-cluster.sh" ]] \
    && [[ -x "$ROOT/build/bin/curvine-master.sh" ]] \
    && [[ -x "$ROOT/build/bin/curvine-worker.sh" ]] \
    && [[ -f "$ROOT/build/conf/curvine-env.sh" ]]
}

ensure_runtime_image() {
  if docker image inspect "$RUNTIME_IMAGE" >/dev/null 2>&1; then
    return
  fi
  log "building runtime image: $RUNTIME_IMAGE"
  docker build -t "$RUNTIME_IMAGE" - <<'DOCKERFILE'
FROM ubuntu:24.04
RUN apt-get update \
  && apt-get install -y --no-install-recommends iproute2 ca-certificates \
  && rm -rf /var/lib/apt/lists/*
DOCKERFILE
}

trap cleanup EXIT

if [[ ! -x "$DRIVER_BIN" ]]; then
  log "building p2p_proof_driver"
  (cd "$ROOT" && cargo build -p curvine-cli --bin p2p_proof_driver)
fi
ensure_runtime_image

if can_manage_cluster; then
  if timeout 1 bash -lc '</dev/tcp/127.0.0.1/8995' >/dev/null 2>&1; then
    log "restarting local cluster for clean metrics baseline"
    bash "$ROOT/build/bin/local-cluster.sh" stop || true
    sleep 2
  fi
  log "starting local cluster"
  bash "$ROOT/build/bin/local-cluster.sh" start
  sleep 6
else
  log "skip cluster restart/start: local-cluster scripts are unavailable or not executable"
fi

if ! timeout 1 bash -lc '</dev/tcp/127.0.0.1/8995' >/dev/null 2>&1; then
  echo "master 127.0.0.1:8995 is not reachable" >&2
  exit 1
fi

log "using evidence dir: $EVID"
dd if=/dev/urandom of="$EVID/source.bin" bs=1M count="$DATA_MB" status=none

cat > "$EVID/provider.toml" <<'TOML'
[client]
read_chunk_size = "1MB"
read_chunk_num = 1

[client.p2p]
enable = true
trace_enable = true
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-a"
identity_key_path = "/evidence/id-a.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/31001"]
bootstrap_peers = []
enable_mdns = false
enable_dht = true
fallback_worker_on_fail = true
cache_capacity = "8GB"
cache_ttl = "24h"
provider_ttl = "10m"
provider_publish_interval = "1s"

[log]
level = "debug"
log_dir = "/evidence"
file_name = "provider.log"
TOML

python3 -u - <<'PY' >"$EVID/master-forward.log" 2>&1 &
import socket
import threading

ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ls.bind(("0.0.0.0", 18995))
ls.listen(512)
print("forwarder listening", flush=True)


def pipe(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except Exception:
            pass


def handle(c):
    try:
        s = socket.create_connection(("127.0.0.1", 8995), timeout=5)
    except Exception:
        c.close()
        return
    threading.Thread(target=pipe, args=(c, s), daemon=True).start()
    threading.Thread(target=pipe, args=(s, c), daemon=True).start()


while True:
    conn, _ = ls.accept()
    threading.Thread(target=handle, args=(conn,), daemon=True).start()
PY
FWD_PID=$!
sleep 1

if ! timeout 1 bash -lc '</dev/tcp/127.0.0.1/18995' >/dev/null 2>&1; then
  echo "master forwarder 127.0.0.1:18995 is not reachable" >&2
  exit 1
fi

log "prepare docker network"
docker rm -f client-a >/dev/null 2>&1 || true
docker rm -f client-b >/dev/null 2>&1 || true
docker network rm cv-p2p-net >/dev/null 2>&1 || true
docker network create --subnet 172.28.0.0/16 cv-p2p-net >/dev/null

log "start provider container"
docker run -d --name client-a \
  --network cv-p2p-net --ip 172.28.0.11 \
  --cap-add NET_ADMIN \
  --add-host host.docker.internal:host-gateway \
  -e P2P_NETEM_RULE="$NETEM_RULE" \
  -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
  -v "$EVID:/evidence" \
  "$RUNTIME_IMAGE" \
  bash -lc '
set -euo pipefail
if [[ -n "${P2P_NETEM_RULE:-}" ]]; then
  tc qdisc replace dev eth0 root netem ${P2P_NETEM_RULE}
fi
/usr/local/bin/p2p_proof_driver \
  --role provider \
  --conf /evidence/provider.toml \
  --master-addrs host.docker.internal:18995 \
  --bootstrap-host 172.28.0.11 \
  --remote-path /p2p-proof/e2e-proof.bin \
  --input-file /evidence/source.bin \
  --output-file /evidence/provider.out \
  --bootstrap-file /evidence/provider_bootstrap_addr.raw.txt \
  --peer-file /evidence/provider_peer_id.txt \
  --warm-reads 3 \
  --hold-secs 300 \
  --bootstrap-wait-secs 120 \
  --report-interval-secs 2 \
  > /evidence/provider-driver.log 2>&1
' >/dev/null

for _ in $(seq 1 240); do
  if [[ -s "$EVID/provider_bootstrap_addr.raw.txt" && -s "$EVID/provider_peer_id.txt" ]]; then
    break
  fi
  sleep 0.5
done
if [[ ! -s "$EVID/provider_bootstrap_addr.raw.txt" || ! -s "$EVID/provider_peer_id.txt" ]]; then
  docker logs client-a > "$EVID/provider.container.log" 2>&1 || true
  echo "provider bootstrap/peer not ready" >&2
  exit 1
fi

BOOTSTRAP_ADDR="$(cat "$EVID/provider_bootstrap_addr.raw.txt")"
PROVIDER_PEER_ID="$(cat "$EVID/provider_peer_id.txt")"
log "provider bootstrap: $BOOTSTRAP_ADDR"

cat > "$EVID/consumer.toml" <<TOML
[client]
read_chunk_size = "1MB"
read_chunk_num = 1

[client.p2p]
enable = true
trace_enable = true
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-b"
identity_key_path = "/evidence/id-b.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/31002"]
bootstrap_peers = ["$BOOTSTRAP_ADDR"]
enable_mdns = false
enable_dht = true
fallback_worker_on_fail = true
cache_capacity = "8GB"
cache_ttl = "24h"
provider_ttl = "10m"
provider_publish_interval = "1s"

[log]
level = "debug"
log_dir = "/evidence"
file_name = "consumer.log"
TOML

cat > "$EVID/consumer_fallback.toml" <<TOML
[client]
read_chunk_size = "1MB"
read_chunk_num = 1

[client.p2p]
enable = true
trace_enable = true
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-c"
identity_key_path = "/evidence/id-c.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/31003"]
bootstrap_peers = ["$BOOTSTRAP_ADDR"]
enable_mdns = false
enable_dht = true
fallback_worker_on_fail = true
cache_capacity = "8GB"
cache_ttl = "24h"
provider_ttl = "10m"
provider_publish_interval = "1s"

[log]
level = "debug"
log_dir = "/evidence"
file_name = "consumer_fallback.log"
TOML

cat > "$EVID/consumer_no_fallback.toml" <<TOML
[client]
read_chunk_size = "1MB"
read_chunk_num = 1

[client.p2p]
enable = true
trace_enable = true
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-d"
identity_key_path = "/evidence/id-d.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/31004"]
bootstrap_peers = ["$BOOTSTRAP_ADDR"]
enable_mdns = false
enable_dht = true
fallback_worker_on_fail = false
cache_capacity = "8GB"
cache_ttl = "24h"
provider_ttl = "10m"
provider_publish_interval = "1s"

[log]
level = "debug"
log_dir = "/evidence"
file_name = "consumer_no_fallback.log"
TOML

log "wait provider publish warmup"
sleep 6

log "capture metrics.before"
curl -sS http://127.0.0.1:9000/metrics > "$EVID/metrics.before.prom"

log "start tcpdump"
tcpdump -i any host 172.28.0.11 and host 172.28.0.12 -w "$EVID/p2p.pcap" > "$EVID/tcpdump.log" 2>&1 &
TCPDUMP_PID=$!
sleep 1

log "run positive consumer read"
docker run --rm --name client-b \
  --network cv-p2p-net --ip 172.28.0.12 \
  --cap-add NET_ADMIN \
  --add-host host.docker.internal:host-gateway \
  -e P2P_NETEM_RULE="$NETEM_RULE" \
  -e P2P_CONSUMER_READS="$CONSUMER_READS" \
  -e P2P_CONSUMER_READ_INTERVAL_MS="$CONSUMER_READ_INTERVAL_MS" \
  -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
  -v "$EVID:/evidence" \
  "$RUNTIME_IMAGE" \
  bash -lc '
set -euo pipefail
if [[ -n "${P2P_NETEM_RULE:-}" ]]; then
  tc qdisc replace dev eth0 root netem ${P2P_NETEM_RULE}
fi
/usr/local/bin/p2p_proof_driver \
  --role consumer \
  --conf /evidence/consumer.toml \
  --master-addrs host.docker.internal:18995 \
  --remote-path /p2p-proof/e2e-proof.bin \
  --consumer-reads "${P2P_CONSUMER_READS}" \
  --consumer-read-interval-ms "${P2P_CONSUMER_READ_INTERVAL_MS}" \
  --output-file /evidence/consumer.out \
  > /evidence/consumer-driver.log 2>&1
' >/dev/null

sleep "$METRICS_WAIT_SECS"

log "capture metrics.after"
curl -sS http://127.0.0.1:9000/metrics > "$EVID/metrics.after.prom"

kill "$TCPDUMP_PID" >/dev/null 2>&1 || true
wait "$TCPDUMP_PID" >/dev/null 2>&1 || true
unset TCPDUMP_PID

if [[ -s "$EVID/p2p.pcap" ]]; then
  tcpdump -nn -r "$EVID/p2p.pcap" > "$EVID/p2p.decoded.txt" 2>/dev/null || true
else
  : > "$EVID/p2p.decoded.txt"
fi

log "run reverse scenario: provider down + fallback on"
docker rm -f client-a >/dev/null

docker run --rm --name client-b \
  --network cv-p2p-net --ip 172.28.0.12 \
  --cap-add NET_ADMIN \
  --add-host host.docker.internal:host-gateway \
  -e P2P_NETEM_RULE="$NETEM_RULE" \
  -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
  -v "$EVID:/evidence" \
  "$RUNTIME_IMAGE" \
  bash -lc '
set -euo pipefail
if [[ -n "${P2P_NETEM_RULE:-}" ]]; then
  tc qdisc replace dev eth0 root netem ${P2P_NETEM_RULE}
fi
/usr/local/bin/p2p_proof_driver \
  --role consumer \
  --conf /evidence/consumer_fallback.toml \
  --master-addrs host.docker.internal:18995 \
  --remote-path /p2p-proof/e2e-proof.bin \
  --output-file /evidence/consumer-fallback.out \
  > /evidence/consumer-fallback-driver.log 2>&1
' >/dev/null

echo 0 > "$EVID/consumer_fallback.exit"

log "run reverse scenario: provider down + fallback off (expect fail)"
set +e
docker run --rm --name client-b \
  --network cv-p2p-net --ip 172.28.0.12 \
  --cap-add NET_ADMIN \
  --add-host host.docker.internal:host-gateway \
  -e P2P_NETEM_RULE="$NETEM_RULE" \
  -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
  -v "$EVID:/evidence" \
  "$RUNTIME_IMAGE" \
  bash -lc '
if [[ -n "${P2P_NETEM_RULE:-}" ]]; then
  tc qdisc replace dev eth0 root netem ${P2P_NETEM_RULE}
fi
/usr/local/bin/p2p_proof_driver \
  --role consumer \
  --conf /evidence/consumer_no_fallback.toml \
  --master-addrs host.docker.internal:18995 \
  --remote-path /p2p-proof/e2e-proof.bin \
  --output-file /evidence/consumer-no-fallback.out \
  > /evidence/consumer-no-fallback-driver.log 2>&1
'
NO_FB_EXIT=$?
set -e
echo "$NO_FB_EXIT" > "$EVID/consumer_no_fallback.exit"

PROVIDER_SHA="$(sha256sum "$EVID/provider.out" | awk '{print $1}')"
CONSUMER_SHA="$(sha256sum "$EVID/consumer.out" | awk '{print $1}')"
echo "PROVIDER_SHA256=$PROVIDER_SHA" > "$EVID/provider.sha256"
echo "CONSUMER_SHA256=$CONSUMER_SHA" > "$EVID/consumer.sha256"

CONSUMER_LOG_FILE="$(ls -1t "$EVID"/consumer.log* 2>/dev/null | head -n1 || true)"
CONSUMER_FETCH_LINE=""
if [[ -n "$CONSUMER_LOG_FILE" ]]; then
  CONSUMER_FETCH_LINE="$(grep 'stage="transfer" event="response_ok"' "$CONSUMER_LOG_FILE" | tail -n1 || true)"
fi
printf '%s\n' "$CONSUMER_FETCH_LINE" > "$EVID/consumer_fetch.log"
CONSUMER_PEER_ID="$(sed -n 's/.*peer_id=Some(PeerId("\([^"]*\)")).*/\1/p' "$EVID/consumer_fetch.log" | tail -n1)"
CONSUMER_P2P_RECV_DELTA="$(sed -n 's/^CONSUMER_P2P_RECV_DELTA=\([0-9][0-9]*\)$/\1/p' "$EVID/consumer-driver.log" | tail -n1)"
CONSUMER_P2P_RECV_DELTA="${CONSUMER_P2P_RECV_DELTA:-0}"

B_SENT_BEFORE="$(sum_metric "$EVID/metrics.before.prom" client_p2p_bytes_sent)"
B_SENT_AFTER="$(sum_metric "$EVID/metrics.after.prom" client_p2p_bytes_sent)"
B_RECV_BEFORE="$(sum_metric "$EVID/metrics.before.prom" client_p2p_bytes_recv)"
B_RECV_AFTER="$(sum_metric "$EVID/metrics.after.prom" client_p2p_bytes_recv)"
HIT_BEFORE="$(sum_metric "$EVID/metrics.before.prom" client_p2p_hit_rate)"
HIT_AFTER="$(sum_metric "$EVID/metrics.after.prom" client_p2p_hit_rate)"

DELTA_SENT="$(awk -v a="$B_SENT_AFTER" -v b="$B_SENT_BEFORE" 'BEGIN{print a-b}')"
DELTA_RECV="$(awk -v a="$B_RECV_AFTER" -v b="$B_RECV_BEFORE" 'BEGIN{print a-b}')"
DELTA_HIT="$(awk -v a="$HIT_AFTER" -v b="$HIT_BEFORE" 'BEGIN{print a-b}')"

PCAP_LINES="$(wc -l < "$EVID/p2p.decoded.txt" | tr -d '[:space:]')"
PCAP_A2B=false
PCAP_B2A=false
if grep -Eq '172\.28\.0\.11\.31001 > 172\.28\.0\.12\.' "$EVID/p2p.decoded.txt"; then
  PCAP_A2B=true
fi
if grep -Eq '172\.28\.0\.12\.[0-9]+ > 172\.28\.0\.11\.31001' "$EVID/p2p.decoded.txt"; then
  PCAP_B2A=true
fi

FB_EXIT="$(cat "$EVID/consumer_fallback.exit" 2>/dev/null || echo 1)"
NO_FB_EXIT="$(cat "$EVID/consumer_no_fallback.exit" 2>/dev/null || echo 1)"

EVID="$EVID" \
NETEM_RULE="$NETEM_RULE" \
CONSUMER_READS="$CONSUMER_READS" \
DATA_MB="$DATA_MB" \
PROVIDER_SHA="$PROVIDER_SHA" \
CONSUMER_SHA="$CONSUMER_SHA" \
PROVIDER_PEER_ID="$PROVIDER_PEER_ID" \
CONSUMER_PEER_ID="$CONSUMER_PEER_ID" \
CONSUMER_P2P_RECV_DELTA="$CONSUMER_P2P_RECV_DELTA" \
DELTA_SENT="$DELTA_SENT" \
DELTA_RECV="$DELTA_RECV" \
DELTA_HIT="$DELTA_HIT" \
PCAP_LINES="$PCAP_LINES" \
PCAP_A2B="$PCAP_A2B" \
PCAP_B2A="$PCAP_B2A" \
FB_EXIT="$FB_EXIT" \
NO_FB_EXIT="$NO_FB_EXIT" \
python3 - <<'PY'
import json
import os
from pathlib import Path

def to_bool(v: str) -> bool:
    return str(v).lower() == "true"

evid = Path(os.environ["EVID"])
provider_sha = os.environ.get("PROVIDER_SHA", "")
consumer_sha = os.environ.get("CONSUMER_SHA", "")
provider_peer_id = os.environ.get("PROVIDER_PEER_ID", "")
consumer_peer_id = os.environ.get("CONSUMER_PEER_ID", "")
consumer_fetch = (evid / "consumer_fetch.log").read_text() if (evid / "consumer_fetch.log").exists() else ""

summary = {
    "netem_rule": os.environ.get("NETEM_RULE", ""),
    "consumer_reads": int(float(os.environ.get("CONSUMER_READS", "1"))),
    "data_mb": int(float(os.environ.get("DATA_MB", "8"))),
    "provider_sha256": provider_sha,
    "consumer_sha256": consumer_sha,
    "sha_match": bool(provider_sha) and provider_sha == consumer_sha,
    "provider_libp2p_peer_id": provider_peer_id,
    "consumer_response_ok_found": bool(consumer_fetch.strip()),
    "consumer_response_ok_peer_id": consumer_peer_id,
    "response_peer_matches_provider": bool(provider_peer_id) and provider_peer_id == consumer_peer_id,
    "consumer_p2p_recv_delta": int(float(os.environ.get("CONSUMER_P2P_RECV_DELTA", "0"))),
    "metrics_delta": {
        "client_p2p_hit_rate": float(os.environ.get("DELTA_HIT", "0")),
        "client_p2p_bytes_sent": float(os.environ.get("DELTA_SENT", "0")),
        "client_p2p_bytes_recv": float(os.environ.get("DELTA_RECV", "0")),
    },
    "pcap_lines": int(float(os.environ.get("PCAP_LINES", "0"))),
    "pcap_a_to_b": to_bool(os.environ.get("PCAP_A2B", "false")),
    "pcap_b_to_a": to_bool(os.environ.get("PCAP_B2A", "false")),
    "consumer_fallback_exit": int(float(os.environ.get("FB_EXIT", "1"))),
    "consumer_no_fallback_exit": int(float(os.environ.get("NO_FB_EXIT", "1"))),
    "consumer_no_fallback_failed_as_expected": int(float(os.environ.get("NO_FB_EXIT", "1"))) != 0,
}
(evid / "proof-summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
PY

cat "$EVID/proof-summary.json"

if [[ "$PROVIDER_SHA" != "$CONSUMER_SHA" ]]; then
  echo "sha mismatch" >&2
  exit 1
fi
if [[ -z "$CONSUMER_FETCH_LINE" || "$CONSUMER_PEER_ID" != "$PROVIDER_PEER_ID" ]]; then
  echo "consumer response_ok peer evidence missing or mismatch" >&2
  exit 1
fi
if ! awk -v v="$CONSUMER_P2P_RECV_DELTA" 'BEGIN{exit !(v>0)}'; then
  echo "consumer p2p recv delta <= 0" >&2
  exit 1
fi
if ! awk -v v="$DELTA_RECV" 'BEGIN{exit !(v>0)}'; then
  echo "client_p2p_bytes_recv delta <= 0" >&2
  exit 1
fi
if ! awk -v v="$DELTA_SENT" 'BEGIN{exit !(v>0)}'; then
  echo "client_p2p_bytes_sent delta <= 0" >&2
  exit 1
fi
if [[ "$PCAP_A2B" != "true" || "$PCAP_B2A" != "true" ]]; then
  echo "pcap direction evidence missing" >&2
  exit 1
fi
if [[ "$FB_EXIT" != "0" || "$NO_FB_EXIT" == "0" ]]; then
  echo "fallback behavior does not match expectation" >&2
  exit 1
fi

log "PASS"
log "evidence_dir=$EVID"
