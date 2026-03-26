#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/p2p-stress-lib.sh"
p2p_stress_init "${BASH_SOURCE[0]}"

run_case() {
  local name="$1"
  local size_spec="$2"
  local read_ratio="$3"
  local hotset_ratio="$4"
  local file_count="$5"
  local duration_secs="$6"
  local case_root="$RUN_ROOT/$name"
  local manifest_dir="$case_root/manifest"
  local remote_root="/p2p-stress/$name"
  mkdir -p "$case_root" "$manifest_dir"

  log "case=$name sizes=$size_spec read_ratio=$read_ratio hotset_ratio=$hotset_ratio"
  curl -sS http://127.0.0.1:9000/metrics > "$case_root/metrics.before.prom"

  write_client_conf "$case_root/seed.toml" seed "" "/ip4/0.0.0.0/tcp/31001" "seed.log"

  docker run -d --name "stress-${name}-seed" \
    --network "$NETWORK_NAME" --ip "$SEED_IP" \
    --cap-add NET_ADMIN \
    --add-host host.docker.internal:host-gateway \
    -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
    -v "$case_root:/evidence" \
    "$RUNTIME_IMAGE" \
    bash -lc '
set -euo pipefail
/usr/local/bin/p2p_proof_driver \
  --role mixed \
  --conf /evidence/seed.toml \
  --master-addrs host.docker.internal:'"$FORWARD_PORT"' \
  --bootstrap-host '"$SEED_IP"' \
  --remote-path '"$remote_root"' \
  --manifest-dir /evidence/manifest \
  --client-id seed \
  --file-size-spec '"$size_spec"' \
  --file-count '"$file_count"' \
  --duration-secs '"$duration_secs"' \
  --read-ratio 0 \
  --hotset-ratio '"$hotset_ratio"' \
  --op-interval-ms '"$OP_INTERVAL_MS"' \
  --warm-written \
  --bootstrap-file /evidence/bootstrap.txt \
  --peer-file /evidence/peer.txt \
  --bootstrap-wait-secs '"$BOOTSTRAP_WAIT_SECS"' \
  --output-file /evidence/seed-summary.json \
  > /evidence/seed-driver.log 2>&1
' >/dev/null

  wait_for_file "$case_root/bootstrap.txt" "seed bootstrap for case $name"
  local bootstrap_addr
  bootstrap_addr="$(cat "$case_root/bootstrap.txt")"

  for client_idx in $(seq 1 "$CLIENTS"); do
    local client_name="client-${client_idx}"
    write_client_conf "$case_root/${client_name}.toml" "$client_name" "$bootstrap_addr" "/ip4/0.0.0.0/tcp/0" "${client_name}.log"
    docker run -d --name "stress-${name}-${client_idx}" \
      --network "$NETWORK_NAME" \
      --cap-add NET_ADMIN \
      --add-host host.docker.internal:host-gateway \
      -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
      -v "$case_root:/evidence" \
      "$RUNTIME_IMAGE" \
      bash -lc '
set -euo pipefail
/usr/local/bin/p2p_proof_driver \
  --role mixed \
  --conf /evidence/'"${client_name}.toml"' \
  --master-addrs host.docker.internal:'"$FORWARD_PORT"' \
  --remote-path '"$remote_root"' \
  --manifest-dir /evidence/manifest \
  --client-id '"$client_name"' \
  --file-size-spec '"$size_spec"' \
  --file-count '"$file_count"' \
  --duration-secs '"$duration_secs"' \
  --read-ratio '"$read_ratio"' \
  --hotset-ratio '"$hotset_ratio"' \
  --op-interval-ms '"$OP_INTERVAL_MS"' \
  --bootstrap-wait-secs '"$BOOTSTRAP_WAIT_SECS"' \
  --output-file /evidence/'"${client_name}-summary.json"' \
  > /evidence/'"${client_name}-driver.log"' 2>&1
' >/dev/null
  done

  local rc=0
  docker wait "stress-${name}-seed" >/dev/null || rc=1
  for client_idx in $(seq 1 "$CLIENTS"); do
    docker wait "stress-${name}-${client_idx}" >/dev/null || rc=1
  done

  curl -sS http://127.0.0.1:9000/metrics > "$case_root/metrics.after.prom"
  python3 - <<'PY' "$case_root" "$name" "$rc" "$READ_CHUNK_SIZE" "$READ_CHUNK_NUM"
import json
import sys
from pathlib import Path

case_root = Path(sys.argv[1])
name = sys.argv[2]
rc = int(sys.argv[3])
read_chunk_size = sys.argv[4]
read_chunk_num = int(sys.argv[5])
summaries = []
for path in sorted(case_root.glob('*-summary.json')):
    summaries.append(json.loads(path.read_text()))
report = {
    'case': name,
    'exit_code': rc,
    'summary_count': len(summaries),
    'read_chunk_size': read_chunk_size,
    'read_chunk_num': read_chunk_num,
    'clients': summaries,
    'total_ops': sum(item.get('total_ops', 0) for item in summaries),
    'read_ok': sum(item.get('read_ok', 0) for item in summaries),
    'write_ok': sum(item.get('write_ok', 0) for item in summaries),
    'read_errors': sum(item.get('read_errors', 0) for item in summaries),
    'write_errors': sum(item.get('write_errors', 0) for item in summaries),
    'sha_mismatches': sum(item.get('sha_mismatches', 0) for item in summaries),
    'p2p_recv_delta': sum(item.get('p2p_recv_delta', 0) for item in summaries),
    'p2p_sent_delta': sum(item.get('p2p_sent_delta', 0) for item in summaries),
    'any_p2p_observed': any(item.get('p2p_recv_delta', 0) > 0 or item.get('p2p_sent_delta', 0) > 0 for item in summaries),
}
(case_root / 'scenario-summary.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
print(case_root / 'scenario-summary.json')
PY

  local before_sent after_sent before_recv after_recv any_p2p_observed
  before_sent="$(sum_metric "$case_root/metrics.before.prom" client_p2p_bytes_sent)"
  after_sent="$(sum_metric "$case_root/metrics.after.prom" client_p2p_bytes_sent)"
  before_recv="$(sum_metric "$case_root/metrics.before.prom" client_p2p_bytes_recv)"
  after_recv="$(sum_metric "$case_root/metrics.after.prom" client_p2p_bytes_recv)"
  any_p2p_observed="$(python3 - <<'PY' "$case_root/scenario-summary.json"
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    report = json.load(f)
print(1 if report.get('any_p2p_observed') else 0)
PY
)"
  printf '%s,%s,%s,%s,%s,%s\n' "$name" "$((after_sent-before_sent))" "$((after_recv-before_recv))" "$CLIENTS" "$rc" "$any_p2p_observed" >> "$RUN_ROOT/report.csv"

  if [[ "$rc" -ne 0 ]]; then
    return 1
  fi
  python3 - <<'PY' "$case_root/scenario-summary.json"
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    report = json.load(f)
raise SystemExit(0 if report['sha_mismatches'] == 0 and report['read_errors'] == 0 and report['write_errors'] == 0 and report['summary_count'] > 0 else 1)
PY
}

main() {
  trap cleanup EXIT
  prepare_env
  printf 'case,p2p_bytes_sent_delta,p2p_bytes_recv_delta,clients,exit_code,any_p2p_observed\n' > "$RUN_ROOT/report.csv"

  run_case small_fixed_cost "64KB,256KB,1MB" 90 80 12 "$DURATION_SECS"
  run_case mixed_balanced "256KB,1MB,16MB" 70 70 16 "$DURATION_SECS"
  run_case large_hotset "16MB,64MB,128MB" 80 90 8 "$DURATION_SECS"

  python3 - <<'PY' "$RUN_ROOT" "$READ_CHUNK_SIZE" "$READ_CHUNK_NUM"
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
read_chunk_size = sys.argv[2]
read_chunk_num = int(sys.argv[3])
reports = []
for path in sorted(run_root.glob('*/scenario-summary.json')):
    reports.append(json.loads(path.read_text()))
final = {
    'run_root': str(run_root),
    'case_count': len(reports),
    'all_passed': all(item.get('exit_code') == 0 and item.get('sha_mismatches') == 0 and item.get('read_errors') == 0 and item.get('write_errors') == 0 for item in reports),
    'p2p_observed_cases': sum(1 for item in reports if item.get('any_p2p_observed')),
    'any_p2p_observed': any(item.get('any_p2p_observed') for item in reports),
    'read_chunk_size': read_chunk_size,
    'read_chunk_num': read_chunk_num,
    'cases': reports,
}
(run_root / 'final-report.json').write_text(json.dumps(final, indent=2, ensure_ascii=False) + '\n')
print(run_root / 'final-report.json')
PY
  cat "$RUN_ROOT/final-report.json"
  python3 - <<'PY' "$RUN_ROOT/final-report.json"
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    report = json.load(f)
raise SystemExit(0 if report.get('all_passed') and report.get('any_p2p_observed') else 1)
PY
}

main "$@"
