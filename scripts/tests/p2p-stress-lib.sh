#!/usr/bin/env bash

p2p_stress_init() {
  local script_path="$1"
  ROOT="$(cd "$(dirname "$script_path")/../.." && pwd)"
  DRIVER_BIN="$ROOT/target/debug/p2p_proof_driver"
  EVID_ROOT="${P2P_STRESS_EVID_ROOT:-$ROOT/evidence/p2p-stress}"
  TS="$(date +%Y%m%d-%H%M%S)"
  RUN_ROOT="$EVID_ROOT/$TS"
  RUNTIME_IMAGE="${P2P_RUNTIME_IMAGE:-curvine/p2p-proof-runner:latest}"
  CLIENTS="${P2P_STRESS_CLIENTS:-4}"
  DURATION_SECS="${P2P_STRESS_DURATION_SECS:-20}"
  OP_INTERVAL_MS="${P2P_STRESS_OP_INTERVAL_MS:-10}"
  BOOTSTRAP_WAIT_SECS="${P2P_STRESS_BOOTSTRAP_WAIT_SECS:-120}"
  MASTER_PORT="${P2P_STRESS_MASTER_PORT:-8995}"
  FORWARD_PORT="${P2P_STRESS_FORWARD_PORT:-18995}"
  READ_CHUNK_SIZE="${P2P_STRESS_READ_CHUNK_SIZE:-1MB}"
  READ_CHUNK_NUM="${P2P_STRESS_READ_CHUNK_NUM:-1}"
  TRACE_ENABLE="${P2P_STRESS_TRACE_ENABLE:-false}"
  LOG_LEVEL="${P2P_STRESS_LOG_LEVEL:-info}"
  NETWORK_NAME="${P2P_STRESS_NETWORK_NAME:-cv-p2p-stress-net}"
  NETWORK_CIDR="${P2P_STRESS_NETWORK_CIDR:-}"
  SEED_IP=""
  mkdir -p "$RUN_ROOT"
}

log() {
  echo "[p2p-stress] $*"
}

sum_metric() {
  local file="$1"
  local name="$2"
  awk -v n="$name" '$1 ~ ("^" n "($|\\{)") {sum+=$2} END {print sum+0}' "$file"
}

can_manage_cluster() {
  [[ -f "$ROOT/build/bin/local-cluster.sh" ]] \
    && [[ -f "$ROOT/build/bin/curvine-master.sh" ]] \
    && [[ -f "$ROOT/build/bin/curvine-worker.sh" ]] \
    && [[ -f "$ROOT/build/conf/curvine-env.sh" ]]
}

network_prefix() {
  local cidr="$1"
  local addr="${cidr%%/*}"
  IFS='.' read -r a b c _ <<<"$addr"
  printf '%s.%s.%s' "$a" "$b" "$c"
}

auto_network_cidr() {
  local used
  used="$(
    docker network inspect $(docker network ls -q) \
      --format '{{range .IPAM.Config}}{{.Subnet}} {{end}}' 2>/dev/null || true
  )"
  local octet
  for octet in $(seq 60 250); do
    local candidate="172.31.${octet}.0/24"
    if ! grep -qw "$candidate" <<<"$used"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

ensure_network_plan() {
  if [[ -z "$NETWORK_CIDR" ]]; then
    NETWORK_CIDR="$(auto_network_cidr)" || {
      echo "unable to find free docker subnet for stress network" >&2
      exit 1
    }
  fi
  SEED_IP="$(network_prefix "$NETWORK_CIDR").11"
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
  docker ps -a --format '{{.Names}}' | rg '^stress-' | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
}

write_client_conf() {
  local conf_path="$1"
  local client_name="$2"
  local bootstrap_addr="${3:-}"
  local listen_addr="${4:-/ip4/0.0.0.0/tcp/0}"
  local log_file="${5:-${client_name}.log}"
  local bootstrap_peers='[]'
  if [[ -n "$bootstrap_addr" ]]; then
    bootstrap_peers="[\"${bootstrap_addr}\"]"
  fi
  cat > "$conf_path" <<TOML
[client]
read_chunk_size = "${READ_CHUNK_SIZE}"
read_chunk_num = ${READ_CHUNK_NUM}
short_circuit = false

[client.p2p]
enable = true
trace_enable = ${TRACE_ENABLE}
trace_sample_rate = 1.0
cache_dir = "/evidence/cache-${client_name}"
identity_key_path = "/evidence/id-${client_name}.key"
listen_addrs = ["${listen_addr}"]
bootstrap_peers = ${bootstrap_peers}
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
file_name = "${log_file}"
TOML
}

start_forwarder() {
  python3 -u "$ROOT/scripts/tests/master-forwarder.py" \
    --listen-port "$FORWARD_PORT" \
    --target-port "$MASTER_PORT" \
    >"$RUN_ROOT/master-forward.log" 2>&1 &
  FWD_PID=$!
  sleep 1
}

prepare_env() {
  if [[ ! -x "$DRIVER_BIN" ]]; then
    log "building p2p_proof_driver"
    (cd "$ROOT" && cargo build -p curvine-cli --bin p2p_proof_driver)
  fi
  ensure_runtime_image

  if can_manage_cluster; then
    if timeout 1 bash -lc "</dev/tcp/127.0.0.1/${MASTER_PORT}" >/dev/null 2>&1; then
      log "restarting local cluster for clean stress baseline"
      bash "$ROOT/build/bin/local-cluster.sh" stop || true
      sleep 2
    fi
    log "starting local cluster"
    bash "$ROOT/build/bin/local-cluster.sh" start
    sleep 6
  else
    log "skip cluster restart/start: local-cluster scripts unavailable or not executable"
  fi

  if ! timeout 1 bash -lc "</dev/tcp/127.0.0.1/${MASTER_PORT}" >/dev/null 2>&1; then
    echo "master 127.0.0.1:${MASTER_PORT} is not reachable" >&2
    exit 1
  fi

  export FORWARD_PORT MASTER_PORT
  start_forwarder
  if ! timeout 1 bash -lc "</dev/tcp/127.0.0.1/${FORWARD_PORT}" >/dev/null 2>&1; then
    echo "master forwarder 127.0.0.1:${FORWARD_PORT} is not reachable" >&2
    exit 1
  fi

  ensure_network_plan
  docker network rm "$NETWORK_NAME" >/dev/null 2>&1 || true
  docker network create --subnet "$NETWORK_CIDR" "$NETWORK_NAME" >/dev/null
}

wait_for_file() {
  local path="$1"
  local label="$2"
  local attempts="${3:-240}"
  local sleep_secs="${4:-0.5}"
  for _ in $(seq 1 "$attempts"); do
    if [[ -s "$path" ]]; then
      return 0
    fi
    sleep "$sleep_secs"
  done
  echo "${label} not ready: $path" >&2
  return 1
}

count_manifest_entries() {
  local manifest_dir="$1"
  python3 - <<'PY' "$manifest_dir"
from pathlib import Path
import sys

manifest_dir = Path(sys.argv[1])
total = 0
if manifest_dir.exists():
    for path in manifest_dir.glob("*.jsonl"):
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            total += sum(1 for line in handle if line.strip())
print(total)
PY
}

wait_for_manifest_entries() {
  local manifest_dir="$1"
  local min_entries="$2"
  local label="$3"
  local attempts="${4:-240}"
  local sleep_secs="${5:-0.5}"
  local total=0
  for _ in $(seq 1 "$attempts"); do
    total="$(count_manifest_entries "$manifest_dir")"
    if [[ "$total" -ge "$min_entries" ]]; then
      return 0
    fi
    sleep "$sleep_secs"
  done
  echo "${label} not ready: $manifest_dir expected>=$min_entries actual=$total" >&2
  return 1
}

snapshot_manifest_dir() {
  local src_dir="$1"
  local dst_dir="$2"
  mkdir -p "$dst_dir"
  find "$dst_dir" -type f -name '*.jsonl' -delete
  if find "$src_dir" -maxdepth 1 -type f -name '*.jsonl' | grep -q .; then
    find "$src_dir" -maxdepth 1 -type f -name '*.jsonl' -exec cp {} "$dst_dir"/ \;
  fi
}
