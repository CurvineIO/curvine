#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER_BIN="$ROOT/target/debug/p2p_proof_driver"
EVID_ROOT="${P2P_PERF_EVID_ROOT:-$ROOT/evidence/p2p-perf}"
TS="$(date +%Y%m%d-%H%M%S)"
EVID="$EVID_ROOT/$TS"
RUNS="${P2P_PERF_RUNS:-8}"
DATA_MB="${P2P_PERF_DATA_MB:-128}"
GW_DELAY_MS="${P2P_PERF_GW_DELAY_MS:-40}"
WORKER_RATE_MBIT="${P2P_PERF_WORKER_RATE_MBIT:-0}"
SHORT_CIRCUIT="${P2P_PERF_SHORT_CIRCUIT:-false}"
REMOTE_PATH="${P2P_PERF_REMOTE_PATH:-/p2p-proof/perf-$TS.bin}"
RUNTIME_IMAGE="${P2P_RUNTIME_IMAGE:-curvine/p2p-proof-runner:latest}"
READ_CHUNK_SIZE="${P2P_PERF_READ_CHUNK_SIZE:-1MB}"
READ_CHUNK_NUM="${P2P_PERF_READ_CHUNK_NUM:-1}"
CONSUMER_READS="${P2P_PERF_CONSUMER_READS:-2}"
CONSUMER_READ_INTERVAL_MS="${P2P_PERF_CONSUMER_READ_INTERVAL_MS:-0}"
LOG_LEVEL="${P2P_PERF_LOG_LEVEL:-info}"
TRACE_ENABLE="${P2P_PERF_TRACE_ENABLE:-false}"
mkdir -p "$EVID"

log() {
  echo "[p2p-perf-ab] $*"
}

sum_metric() {
  local file="$1"
  local name="$2"
  awk -v n="$name" '$1 ~ ("^" n "($|\\{)") {sum+=$2} END {print sum+0}' "$file"
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

cleanup() {
  set +e
  if [[ -n "${FWD_PID:-}" ]]; then
    kill "$FWD_PID" >/dev/null 2>&1 || true
    wait "$FWD_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f perf-provider >/dev/null 2>&1 || true
  docker network rm cv-p2p-perf-net >/dev/null 2>&1 || true
}

write_consumer_conf() {
  local mode="$1"
  local round="$2"
  local conf_path="$3"
  local p2p_enable="false"
  local bootstrap_line='bootstrap_peers = []'
  if [[ "$mode" == "on" ]]; then
    p2p_enable="true"
    bootstrap_line="bootstrap_peers = [\"$BOOTSTRAP_ADDR\"]"
  fi

  cat > "$conf_path" <<TOML
[client]
read_chunk_size = "${READ_CHUNK_SIZE}"
read_chunk_num = ${READ_CHUNK_NUM}
short_circuit = ${SHORT_CIRCUIT}

[client.p2p]
enable = ${p2p_enable}
trace_enable = ${TRACE_ENABLE}
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-${mode}-${round}"
identity_key_path = "/evidence/id-${mode}-${round}.key"
listen_addrs = ["/ip4/0.0.0.0/tcp/0"]
${bootstrap_line}
enable_mdns = false
enable_dht = true
fallback_worker_on_fail = true
cache_capacity = "8GB"
cache_ttl = "24h"
provider_ttl = "10m"
provider_publish_interval = "1s"

[log]
level = "${LOG_LEVEL}"
log_dir = "/evidence"
file_name = "consumer-${mode}-${round}.log"
TOML
}

run_case() {
  local mode="$1"
  local round="$2"
  local conf_file="$EVID/consumer-${mode}-${round}.toml"
  local output_file="/evidence/output-${mode}-${round}.bin"
  local driver_log_file="$EVID/consumer-${mode}-${round}.driver.log"
  local driver_log="/evidence/consumer-${mode}-${round}.driver.log"

  write_consumer_conf "$mode" "$round" "$conf_file"

  docker run --rm --name "perf-${mode}-${round}" \
    --network cv-p2p-perf-net \
    --cap-add NET_ADMIN \
    --add-host host.docker.internal:host-gateway \
    -e P2P_GW_DELAY_MS="$GW_DELAY_MS" \
    -e P2P_WORKER_RATE_MBIT="$WORKER_RATE_MBIT" \
    -e P2P_PROVIDER_IP="172.29.0.11" \
    -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
    -v "$EVID:/evidence" \
    "$RUNTIME_IMAGE" \
    bash -lc '
set -euo pipefail
if [[ "${P2P_GW_DELAY_MS:-0}" -gt 0 || "${P2P_WORKER_RATE_MBIT:-0}" -gt 0 ]]; then
  PROVIDER_IP="${P2P_PROVIDER_IP:-}"
  if [[ -z "$PROVIDER_IP" ]]; then
    PROVIDER_IP="172.29.0.11"
  fi
  tc qdisc add dev eth0 root handle 1: prio
  tc qdisc add dev eth0 parent 1:3 handle 30: netem delay "${P2P_GW_DELAY_MS}"ms
  if [[ "${P2P_WORKER_RATE_MBIT:-0}" -gt 0 ]]; then
    tc qdisc add dev eth0 parent 30:1 handle 31: tbf rate "${P2P_WORKER_RATE_MBIT}"mbit burst 64kb latency 400ms
  fi
  tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst "$PROVIDER_IP"/32 flowid 1:1
  tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 match ip dst 0.0.0.0/0 flowid 1:3
fi
/usr/local/bin/p2p_proof_driver \
  --role consumer \
  --conf /evidence/'"consumer-${mode}-${round}.toml"' \
  --master-addrs host.docker.internal:18995 \
  --remote-path '"$REMOTE_PATH"' \
  --consumer-reads '"$CONSUMER_READS"' \
  --consumer-read-interval-ms '"$CONSUMER_READ_INTERVAL_MS"' \
  --output-file '"$output_file"' \
  > '"$driver_log"' 2>&1
' >/dev/null

  local elapsed_ms
  elapsed_ms="$(sed -n 's/^CONSUMER_READ_ELAPSED_MS=\([0-9.][0-9.]*\)$/\1/p' "$driver_log_file" | tail -n1)"
  if [[ -z "$elapsed_ms" ]]; then
    echo "missing CONSUMER_READ_ELAPSED_MS in $driver_log_file" >&2
    return 1
  fi
  local elapsed
  elapsed="$(awk -v ms="$elapsed_ms" 'BEGIN{printf "%.6f", ms/1000.0}')"

  local p2p_hit=0
  if [[ "$mode" == "on" ]]; then
    local recv_delta
    recv_delta="$(sed -n 's/^CONSUMER_P2P_RECV_DELTA=\([0-9][0-9]*\)$/\1/p' "$driver_log_file" | tail -n1)"
    recv_delta="${recv_delta:-0}"
    if awk -v v="$recv_delta" 'BEGIN{exit !(v>0)}'; then
      p2p_hit=1
    fi
  fi

  local output_sha
  output_sha="$(sha256sum "$EVID/output-${mode}-${round}.bin" | awk '{print $1}')"
  printf '%s,%s,%s,%s,%s\n' "$round" "$mode" "$elapsed" "$p2p_hit" "$output_sha" >> "$EVID/timings.csv"
}

trap cleanup EXIT

if [[ ! -x "$DRIVER_BIN" ]]; then
  log "building p2p_proof_driver"
  (cd "$ROOT" && cargo build -p curvine-cli --bin p2p_proof_driver)
fi
ensure_runtime_image

if can_manage_cluster; then
  if timeout 1 bash -lc '</dev/tcp/127.0.0.1/8995' >/dev/null 2>&1; then
    log "restarting local cluster for clean baseline"
    bash "$ROOT/build/bin/local-cluster.sh" stop || true
    sleep 2
  fi
  log "starting local cluster"
  bash "$ROOT/build/bin/local-cluster.sh" start
  sleep 6
else
  log "skip cluster restart/start: local-cluster scripts unavailable or not executable"
fi

if ! timeout 1 bash -lc '</dev/tcp/127.0.0.1/8995' >/dev/null 2>&1; then
  echo "master 127.0.0.1:8995 is not reachable" >&2
  exit 1
fi

python3 -u "$ROOT/scripts/tests/master-forwarder.py" \
  --listen-port 18995 \
  --target-port 8995 \
  >"$EVID/master-forward.log" 2>&1 &
FWD_PID=$!
sleep 1

if ! timeout 1 bash -lc '</dev/tcp/127.0.0.1/18995' >/dev/null 2>&1; then
  echo "master forwarder 127.0.0.1:18995 is not reachable" >&2
  exit 1
fi

log "prepare docker network"
docker rm -f perf-provider >/dev/null 2>&1 || true
docker network rm cv-p2p-perf-net >/dev/null 2>&1 || true
docker network create --subnet 172.29.0.0/16 cv-p2p-perf-net >/dev/null

log "prepare source data ${DATA_MB}MB"
dd if=/dev/urandom of="$EVID/source.bin" bs=1M count="$DATA_MB" status=none
SOURCE_SHA="$(sha256sum "$EVID/source.bin" | awk '{print $1}')"
printf 'round,mode,elapsed_sec,p2p_hit,output_sha\n' > "$EVID/timings.csv"

touch "$EVID/provider-driver.log"
cat > "$EVID/provider.toml" <<TOML
[client]
read_chunk_size = "${READ_CHUNK_SIZE}"
read_chunk_num = ${READ_CHUNK_NUM}
short_circuit = ${SHORT_CIRCUIT}

[client.p2p]
enable = true
trace_enable = ${TRACE_ENABLE}
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-provider"
identity_key_path = "/evidence/id-provider.key"
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
level = "${LOG_LEVEL}"
log_dir = "/evidence"
file_name = "provider.log"
TOML

log "start provider"
docker run -d --name perf-provider \
  --network cv-p2p-perf-net --ip 172.29.0.11 \
  --add-host host.docker.internal:host-gateway \
  -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
  -v "$EVID:/evidence" \
  "$RUNTIME_IMAGE" \
  bash -lc '
set -euo pipefail
/usr/local/bin/p2p_proof_driver \
  --role provider \
  --conf /evidence/provider.toml \
  --master-addrs host.docker.internal:18995 \
  --bootstrap-host 172.29.0.11 \
  --remote-path '"$REMOTE_PATH"' \
  --input-file /evidence/source.bin \
  --output-file /evidence/provider.out \
  --bootstrap-file /evidence/provider_bootstrap_addr.raw.txt \
  --peer-file /evidence/provider_peer_id.txt \
  --warm-reads 3 \
  --hold-secs 1800 \
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
  docker logs perf-provider > "$EVID/provider.container.log" 2>&1 || true
  echo "provider bootstrap/peer not ready" >&2
  exit 1
fi

BOOTSTRAP_ADDR="$(cat "$EVID/provider_bootstrap_addr.raw.txt")"
log "provider bootstrap: $BOOTSTRAP_ADDR"

if [[ ! -s "$EVID/provider.out" ]]; then
  echo "provider output not ready" >&2
  exit 1
fi
PROVIDER_SHA="$(sha256sum "$EVID/provider.out" | awk '{print $1}')"
if [[ "$PROVIDER_SHA" != "$SOURCE_SHA" ]]; then
  echo "provider output sha mismatch: source=$SOURCE_SHA provider=$PROVIDER_SHA" >&2
  exit 1
fi

sleep 6
curl -sS http://127.0.0.1:9000/metrics > "$EVID/metrics.before.prom"

for round in $(seq 1 "$RUNS"); do
  log "round=$round mode=off"
  run_case off "$round"
  log "round=$round mode=on"
  run_case on "$round"
done

curl -sS http://127.0.0.1:9000/metrics > "$EVID/metrics.after.prom"

B_SENT_BEFORE="$(sum_metric "$EVID/metrics.before.prom" client_p2p_bytes_sent)"
B_SENT_AFTER="$(sum_metric "$EVID/metrics.after.prom" client_p2p_bytes_sent)"
B_RECV_BEFORE="$(sum_metric "$EVID/metrics.before.prom" client_p2p_bytes_recv)"
B_RECV_AFTER="$(sum_metric "$EVID/metrics.after.prom" client_p2p_bytes_recv)"
DELTA_SENT="$(awk -v a="$B_SENT_AFTER" -v b="$B_SENT_BEFORE" 'BEGIN{print a-b}')"
DELTA_RECV="$(awk -v a="$B_RECV_AFTER" -v b="$B_RECV_BEFORE" 'BEGIN{print a-b}')"

EVID="$EVID" \
RUNS="$RUNS" \
GW_DELAY_MS="$GW_DELAY_MS" \
WORKER_RATE_MBIT="$WORKER_RATE_MBIT" \
DATA_MB="$DATA_MB" \
READ_CHUNK_SIZE="$READ_CHUNK_SIZE" \
READ_CHUNK_NUM="$READ_CHUNK_NUM" \
CONSUMER_READS="$CONSUMER_READS" \
CONSUMER_READ_INTERVAL_MS="$CONSUMER_READ_INTERVAL_MS" \
LOG_LEVEL="$LOG_LEVEL" \
TRACE_ENABLE="$TRACE_ENABLE" \
SOURCE_SHA="$SOURCE_SHA" \
PROVIDER_SHA="$PROVIDER_SHA" \
REMOTE_PATH="$REMOTE_PATH" \
DELTA_SENT="$DELTA_SENT" \
DELTA_RECV="$DELTA_RECV" \
python3 - <<'PY'
import csv
import json
import os
from pathlib import Path


def percentile(data, p):
    if not data:
        return 0.0
    data = sorted(data)
    if len(data) == 1:
        return float(data[0])
    rank = (len(data) - 1) * p
    lo = int(rank)
    hi = min(lo + 1, len(data) - 1)
    frac = rank - lo
    return float(data[lo] + (data[hi] - data[lo]) * frac)


def stats(values):
    values = [float(v) for v in values]
    if not values:
        return {
            "count": 0,
            "avg_sec": 0.0,
            "p50_sec": 0.0,
            "p95_sec": 0.0,
            "min_sec": 0.0,
            "max_sec": 0.0,
        }
    return {
        "count": len(values),
        "avg_sec": sum(values) / len(values),
        "p50_sec": percentile(values, 0.5),
        "p95_sec": percentile(values, 0.95),
        "min_sec": min(values),
        "max_sec": max(values),
    }


evid = Path(os.environ["EVID"])
source_sha = os.environ["SOURCE_SHA"]
rows = list(csv.DictReader((evid / "timings.csv").open("r", encoding="utf-8")))

off_times = [float(r["elapsed_sec"]) for r in rows if r["mode"] == "off"]
on_times = [float(r["elapsed_sec"]) for r in rows if r["mode"] == "on"]
on_hits = sum(1 for r in rows if r["mode"] == "on" and r["p2p_hit"] == "1")
sha_mismatches = [r for r in rows if r["output_sha"] != source_sha]

off_stats = stats(off_times)
on_stats = stats(on_times)

speedup = (off_stats["avg_sec"] / on_stats["avg_sec"]) if on_stats["avg_sec"] > 0 else 0.0
latency_reduction_pct = (1.0 - (on_stats["avg_sec"] / off_stats["avg_sec"])) * 100 if off_stats["avg_sec"] > 0 else 0.0

all_on_hit = on_hits == len(on_times) and len(on_times) > 0
all_sha_match = len(sha_mismatches) == 0

report = {
    "runs_per_mode": int(os.environ["RUNS"]),
    "data_mb": int(os.environ["DATA_MB"]),
    "remote_path": os.environ["REMOTE_PATH"],
    "read_chunk_size": os.environ["READ_CHUNK_SIZE"],
    "read_chunk_num": int(os.environ["READ_CHUNK_NUM"]),
    "consumer_reads_per_run": int(os.environ["CONSUMER_READS"]),
    "consumer_read_interval_ms": int(os.environ["CONSUMER_READ_INTERVAL_MS"]),
    "log_level": os.environ["LOG_LEVEL"],
    "trace_enable": os.environ["TRACE_ENABLE"].lower() == "true",
    "worker_path_gateway_delay_ms": int(os.environ["GW_DELAY_MS"]),
    "worker_path_rate_limit_mbit": int(os.environ["WORKER_RATE_MBIT"]),
    "mode_off": off_stats,
    "mode_on": on_stats,
    "speedup_x": speedup,
    "avg_latency_reduction_pct": latency_reduction_pct,
    "all_on_runs_p2p_hit": all_on_hit,
    "on_hit_count": on_hits,
    "on_total": len(on_times),
    "all_output_sha_match_source": all_sha_match,
    "sha_mismatch_count": len(sha_mismatches),
    "provider_output_sha_match_source": os.environ["PROVIDER_SHA"] == source_sha,
    "metrics_delta": {
        "client_p2p_bytes_sent": float(os.environ["DELTA_SENT"]),
        "client_p2p_bytes_recv": float(os.environ["DELTA_RECV"]),
    },
}

(evid / "perf-report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
print(json.dumps(report, indent=2, ensure_ascii=False))
PY

cat "$EVID/perf-report.json"

SPEEDUP="$(python3 - <<'PY' "$EVID/perf-report.json"
import json,sys
r=json.load(open(sys.argv[1]))
print(r['speedup_x'])
PY
)"
ON_HIT_OK="$(python3 - <<'PY' "$EVID/perf-report.json"
import json,sys
r=json.load(open(sys.argv[1]))
print('1' if r['all_on_runs_p2p_hit'] else '0')
PY
)"
SHA_OK="$(python3 - <<'PY' "$EVID/perf-report.json"
import json,sys
r=json.load(open(sys.argv[1]))
print('1' if r['all_output_sha_match_source'] else '0')
PY
)"
if [[ "$ON_HIT_OK" != "1" || "$SHA_OK" != "1" ]]; then
  echo "p2p hit or sha validation failed" >&2
  exit 1
fi
if ! awk -v v="$SPEEDUP" 'BEGIN{exit !(v>1.0)}'; then
  echo "no performance gain detected" >&2
  exit 1
fi

log "PASS"
log "evidence_dir=$EVID"
