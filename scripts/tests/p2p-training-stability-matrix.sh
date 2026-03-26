#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/p2p-stress-lib.sh"
p2p_stress_init "${BASH_SOURCE[0]}"

TRAIN_DURATION_SECS="${P2P_TRAIN_DURATION_SECS:-$DURATION_SECS}"
TRAINERS="${P2P_TRAIN_TRAINERS:-4}"
CHECKPOINTERS="${P2P_TRAIN_CHECKPOINTERS:-1}"
EVALUATORS="${P2P_TRAIN_EVALUATORS:-1}"
DATASET_FILE_COUNT="${P2P_TRAIN_DATASET_FILE_COUNT:-24}"
CHECKPOINT_FILE_COUNT="${P2P_TRAIN_CHECKPOINT_FILE_COUNT:-4}"
REQUIRE_P2P="${P2P_TRAIN_REQUIRE_P2P:-false}"
DATASET_WARMUP_ENTRIES="${P2P_TRAIN_DATASET_WARMUP_ENTRIES:-4}"
CHECKPOINT_WARMUP_ENTRIES="${P2P_TRAIN_CHECKPOINT_WARMUP_ENTRIES:-1}"
DATASET_SETTLE_SECS="${P2P_TRAIN_DATASET_SETTLE_SECS:-3}"
CHECKPOINT_SETTLE_SECS="${P2P_TRAIN_CHECKPOINT_SETTLE_SECS:-2}"
SEED_OP_INTERVAL_MS="${P2P_TRAIN_SEED_OP_INTERVAL_MS:-25}"
TRAINER_OP_INTERVAL_MS="${P2P_TRAIN_TRAINER_OP_INTERVAL_MS:-5}"
CHECKPOINT_OP_INTERVAL_MS="${P2P_TRAIN_CHECKPOINT_OP_INTERVAL_MS:-200}"
EVALUATOR_OP_INTERVAL_MS="${P2P_TRAIN_EVALUATOR_OP_INTERVAL_MS:-20}"
PROVIDER_HOLD_BUFFER_SECS="${P2P_TRAIN_PROVIDER_HOLD_BUFFER_SECS:-15}"
SEED_POST_RUN_HOLD_SECS="${P2P_TRAIN_SEED_POST_RUN_HOLD_SECS:-$((TRAIN_DURATION_SECS * 2 + DATASET_SETTLE_SECS + CHECKPOINT_SETTLE_SECS + PROVIDER_HOLD_BUFFER_SECS))}"
CHECKPOINT_POST_RUN_HOLD_SECS="${P2P_TRAIN_CHECKPOINT_POST_RUN_HOLD_SECS:-$((TRAIN_DURATION_SECS + CHECKPOINT_SETTLE_SECS + PROVIDER_HOLD_BUFFER_SECS))}"

run_mixed_container() {
  local container_name="$1"
  local case_root="$2"
  local conf_name="$3"
  local remote_root="$4"
  local client_id="$5"
  local size_spec="$6"
  local file_count="$7"
  local duration_secs="$8"
  local read_ratio="$9"
  local hotset_ratio="${10}"
  local op_interval_ms="${11}"
  local output_name="${12}"
  local driver_log="${13}"
  local read_manifest_rel="${14:-}"
  local write_manifest_rel="${15:-}"
  local warm_written="${16:-false}"
  local bootstrap_host="${17:-}"
  local bootstrap_file="${18:-}"
  local peer_file="${19:-}"
  local post_run_hold_secs="${20:-0}"

  local manifest_flags=""
  local warm_flag=""
  local bootstrap_host_flag=""
  local bootstrap_file_flag=""
  local peer_file_flag=""
  local post_run_hold_flag=""
  local docker_ip_args=()
  [[ -n "$read_manifest_rel" ]] && manifest_flags+=" --read-manifest-dir /evidence/${read_manifest_rel}"
  [[ -n "$write_manifest_rel" ]] && manifest_flags+=" --write-manifest-dir /evidence/${write_manifest_rel}"
  [[ "$warm_written" == "true" ]] && warm_flag=" --warm-written"
  [[ -n "$bootstrap_host" ]] && bootstrap_host_flag=" --bootstrap-host ${bootstrap_host}"
  [[ -n "$bootstrap_file" ]] && bootstrap_file_flag=" --bootstrap-file /evidence/${bootstrap_file}"
  [[ -n "$peer_file" ]] && peer_file_flag=" --peer-file /evidence/${peer_file}"
  [[ "$post_run_hold_secs" -gt 0 ]] && post_run_hold_flag=" --post-run-hold-secs ${post_run_hold_secs}"
  [[ -n "$bootstrap_host" ]] && docker_ip_args=(--ip "$bootstrap_host")

  docker run -d --name "$container_name" \
    --network "$NETWORK_NAME" \
    "${docker_ip_args[@]}" \
    --cap-add NET_ADMIN \
    --add-host host.docker.internal:host-gateway \
    -v "$DRIVER_BIN:/usr/local/bin/p2p_proof_driver:ro" \
    -v "$case_root:/evidence" \
    "$RUNTIME_IMAGE" \
    bash -lc '
set -euo pipefail
/usr/local/bin/p2p_proof_driver \
  --role mixed \
  --conf /evidence/'"$conf_name"' \
  --master-addrs host.docker.internal:'"$FORWARD_PORT"' \
  --remote-path '"$remote_root"' \
  --client-id '"$client_id"' \
  --file-size-spec '"$size_spec"' \
  --file-count '"$file_count"' \
  --duration-secs '"$duration_secs"' \
  --read-ratio '"$read_ratio"' \
  --hotset-ratio '"$hotset_ratio"' \
  --op-interval-ms '"$op_interval_ms"' \
  --bootstrap-wait-secs '"$BOOTSTRAP_WAIT_SECS"' \
  --output-file /evidence/'"$output_name"''"$manifest_flags"''"$warm_flag"''"$bootstrap_host_flag"''"$bootstrap_file_flag"''"$peer_file_flag"''"$post_run_hold_flag"' \
  > /evidence/'"$driver_log"' 2>&1
' >/dev/null
}

summarize_training_case() {
  local case_root="$1"
  local case_name="$2"
  local rc="$3"
  local expected_clients="$4"
  python3 - <<'PY' "$case_root" "$case_name" "$rc" "$expected_clients" "$READ_CHUNK_SIZE" "$READ_CHUNK_NUM" "$REQUIRE_P2P"
import json
import sys
from pathlib import Path

case_root = Path(sys.argv[1])
case_name = sys.argv[2]
rc = int(sys.argv[3])
expected_clients = int(sys.argv[4])
read_chunk_size = sys.argv[5]
read_chunk_num = int(sys.argv[6])
require_p2p = sys.argv[7].lower() == 'true'

summaries = []
for path in sorted(case_root.glob('*-summary.json')):
    data = json.loads(path.read_text())
    client_id = data.get('client_id', '')
    if client_id == 'seed':
        group = 'seed'
    elif client_id.startswith('trainer-'):
        group = 'trainer'
    elif client_id.startswith('checkpoint-'):
        group = 'checkpoint'
    elif client_id.startswith('eval-'):
        group = 'evaluator'
    else:
        group = 'unknown'
    data['group'] = group
    summaries.append(data)

log_counts = {
    'response_retry': 0,
    'data_plane_failure': 0,
    'miss': 0,
    'response_ok': 0,
    'response_redirect': 0,
    'dht_query': 0,
    'dht_result': 0,
    'fatal_lines': 0,
}
for path in sorted(case_root.glob('*.log*')):
    if path.name.endswith('driver.log') or '.driver.' in path.name:
        continue
    text = path.read_text(errors='ignore')
    log_counts['response_retry'] += text.count('event="response_retry"')
    log_counts['data_plane_failure'] += text.count('event="data_plane_failure"')
    log_counts['miss'] += text.count('event="miss"')
    log_counts['response_ok'] += text.count('event="response_ok"')
    log_counts['response_redirect'] += text.count('event="response_redirect"')
    log_counts['dht_query'] += text.count('event="dht_query"')
    log_counts['dht_result'] += text.count('event="dht_result"')
    for line in text.splitlines():
        lower = line.lower()
        if 'panicked at' in lower or ' fatal ' in lower or ('thread \'' in lower and 'panicked' in lower):
            log_counts['fatal_lines'] += 1

trainers = [item for item in summaries if item['group'] == 'trainer']
checkpoints = [item for item in summaries if item['group'] == 'checkpoint']
evaluators = [item for item in summaries if item['group'] == 'evaluator']
read_clients = [item for item in summaries if item['group'] in {'trainer', 'evaluator'}]
read_p99 = [item.get('read_latency', {}).get('p99_ms', 0.0) for item in read_clients if item.get('read_latency', {}).get('count', 0) > 0]
report = {
    'case': case_name,
    'exit_code': rc,
    'expected_clients': expected_clients,
    'summary_count': len(summaries),
    'p2p_required': require_p2p,
    'read_chunk_size': read_chunk_size,
    'read_chunk_num': read_chunk_num,
    'clients': summaries,
    'trainer_count': len(trainers),
    'checkpoint_count': len(checkpoints),
    'evaluator_count': len(evaluators),
    'total_ops': sum(item.get('total_ops', 0) for item in summaries),
    'read_ok': sum(item.get('read_ok', 0) for item in summaries),
    'write_ok': sum(item.get('write_ok', 0) for item in summaries),
    'read_errors': sum(item.get('read_errors', 0) for item in summaries),
    'write_errors': sum(item.get('write_errors', 0) for item in summaries),
    'sha_mismatches': sum(item.get('sha_mismatches', 0) for item in summaries),
    'p2p_recv_delta': sum(item.get('p2p_recv_delta', 0) for item in summaries),
    'p2p_sent_delta': sum(item.get('p2p_sent_delta', 0) for item in summaries),
    'any_p2p_observed': any(item.get('p2p_recv_delta', 0) > 0 or item.get('p2p_sent_delta', 0) > 0 for item in summaries),
    'max_reader_p99_ms': max(read_p99) if read_p99 else 0.0,
    'log_counts': log_counts,
}
report['all_stable'] = (
    report['exit_code'] == 0
    and report['summary_count'] == report['expected_clients']
    and report['sha_mismatches'] == 0
    and report['read_errors'] == 0
    and report['write_errors'] == 0
    and report['log_counts']['fatal_lines'] == 0
    and report['log_counts']['data_plane_failure'] == 0
)
report['has_p2p_gap'] = report['all_stable'] and not report['any_p2p_observed']
report['status'] = (
    'stable_accelerated' if report['all_stable'] and report['any_p2p_observed']
    else 'stable_worker_fallback' if report['all_stable']
    else 'unstable'
)
(case_root / 'scenario-summary.json').write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n')
print(case_root / 'scenario-summary.json')
PY
}

run_training_case() {
  local name="$1"
  local dataset_sizes="$2"
  local dataset_hotset="$3"
  local checkpoint_sizes="$4"
  local checkpoint_hotset="$5"
  local evaluator_hotset="$6"
  local evaluator_manifest_kind="${7:-dataset}"
  local case_root="$RUN_ROOT/$name"
  local dataset_manifest_live_rel="manifest-dataset-live"
  local dataset_manifest_read_rel="manifest-dataset-read"
  local checkpoint_manifest_live_rel="manifest-checkpoint-live"
  local checkpoint_manifest_read_rel="manifest-checkpoint-read"
  local remote_root="/p2p-training/$name"
  mkdir -p \
    "$case_root/$dataset_manifest_live_rel" \
    "$case_root/$dataset_manifest_read_rel" \
    "$case_root/$checkpoint_manifest_live_rel" \
    "$case_root/$checkpoint_manifest_read_rel"

  log "case=$name dataset_sizes=$dataset_sizes checkpoint_sizes=$checkpoint_sizes"
  curl -sS http://127.0.0.1:9000/metrics > "$case_root/metrics.before.prom"

  write_client_conf "$case_root/seed.toml" seed "" "/ip4/0.0.0.0/tcp/31001" "seed.log"
  run_mixed_container \
    "stress-${name}-seed" \
    "$case_root" \
    "seed.toml" \
    "$remote_root" \
    seed \
    "$dataset_sizes" \
    "$DATASET_FILE_COUNT" \
    "$TRAIN_DURATION_SECS" \
    0 \
    "$dataset_hotset" \
    "$SEED_OP_INTERVAL_MS" \
    "seed-summary.json" \
    "seed-driver.log" \
    "$dataset_manifest_live_rel" \
    "$dataset_manifest_live_rel" \
    true \
    "$SEED_IP" \
    "bootstrap.txt" \
    "peer.txt" \
    "$SEED_POST_RUN_HOLD_SECS"

  wait_for_file "$case_root/bootstrap.txt" "seed bootstrap for case $name"
  wait_for_manifest_entries \
    "$case_root/$dataset_manifest_live_rel" \
    "$DATASET_WARMUP_ENTRIES" \
    "dataset manifest for case $name"
  sleep "$DATASET_SETTLE_SECS"
  snapshot_manifest_dir \
    "$case_root/$dataset_manifest_live_rel" \
    "$case_root/$dataset_manifest_read_rel"
  local bootstrap_addr
  bootstrap_addr="$(cat "$case_root/bootstrap.txt")"

  if [[ "$evaluator_manifest_kind" == "checkpoint" ]]; then
    for idx in $(seq 1 "$CHECKPOINTERS"); do
      local client_name="checkpoint-$(printf '%02d' "$idx")"
      write_client_conf "$case_root/${client_name}.toml" "$client_name" "$bootstrap_addr" "/ip4/0.0.0.0/tcp/0" "${client_name}.log"
      run_mixed_container \
        "stress-${name}-${client_name}" \
        "$case_root" \
        "${client_name}.toml" \
        "$remote_root" \
        "$client_name" \
        "$checkpoint_sizes" \
        "$CHECKPOINT_FILE_COUNT" \
        "$TRAIN_DURATION_SECS" \
        0 \
        "$checkpoint_hotset" \
        "$CHECKPOINT_OP_INTERVAL_MS" \
        "${client_name}-summary.json" \
        "${client_name}-driver.log" \
        "$dataset_manifest_read_rel" \
        "$checkpoint_manifest_live_rel" \
        true \
        "" \
        "" \
        "" \
        "$CHECKPOINT_POST_RUN_HOLD_SECS"
    done
    wait_for_manifest_entries \
      "$case_root/$checkpoint_manifest_live_rel" \
      "$CHECKPOINT_WARMUP_ENTRIES" \
      "checkpoint manifest for case $name"
    sleep "$CHECKPOINT_SETTLE_SECS"
    snapshot_manifest_dir \
      "$case_root/$checkpoint_manifest_live_rel" \
      "$case_root/$checkpoint_manifest_read_rel"
  fi

  local idx rc expected_clients
  expected_clients=$((1 + TRAINERS + CHECKPOINTERS + EVALUATORS))
  for idx in $(seq 1 "$TRAINERS"); do
    local client_name="trainer-$(printf '%02d' "$idx")"
    write_client_conf "$case_root/${client_name}.toml" "$client_name" "$bootstrap_addr" "/ip4/0.0.0.0/tcp/0" "${client_name}.log"
    run_mixed_container \
      "stress-${name}-${client_name}" \
      "$case_root" \
      "${client_name}.toml" \
      "$remote_root" \
      "$client_name" \
      "$dataset_sizes" \
      "$DATASET_FILE_COUNT" \
      "$TRAIN_DURATION_SECS" \
      100 \
      "$dataset_hotset" \
      "$TRAINER_OP_INTERVAL_MS" \
      "${client_name}-summary.json" \
      "${client_name}-driver.log" \
      "$dataset_manifest_read_rel" \
      "$dataset_manifest_read_rel"
  done

  if [[ "$evaluator_manifest_kind" != "checkpoint" ]]; then
    for idx in $(seq 1 "$CHECKPOINTERS"); do
      local client_name="checkpoint-$(printf '%02d' "$idx")"
      write_client_conf "$case_root/${client_name}.toml" "$client_name" "$bootstrap_addr" "/ip4/0.0.0.0/tcp/0" "${client_name}.log"
      run_mixed_container \
        "stress-${name}-${client_name}" \
        "$case_root" \
        "${client_name}.toml" \
        "$remote_root" \
        "$client_name" \
        "$checkpoint_sizes" \
        "$CHECKPOINT_FILE_COUNT" \
        "$TRAIN_DURATION_SECS" \
        0 \
        "$checkpoint_hotset" \
        "$CHECKPOINT_OP_INTERVAL_MS" \
        "${client_name}-summary.json" \
        "${client_name}-driver.log" \
        "$dataset_manifest_read_rel" \
        "$checkpoint_manifest_live_rel" \
        true \
        "" \
        "" \
        "" \
        "$CHECKPOINT_POST_RUN_HOLD_SECS"
    done
  fi

  for idx in $(seq 1 "$EVALUATORS"); do
    local client_name="eval-$(printf '%02d' "$idx")"
    local eval_read_manifest_rel="$dataset_manifest_read_rel"
    [[ "$evaluator_manifest_kind" == "checkpoint" ]] && eval_read_manifest_rel="$checkpoint_manifest_read_rel"
    write_client_conf "$case_root/${client_name}.toml" "$client_name" "$bootstrap_addr" "/ip4/0.0.0.0/tcp/0" "${client_name}.log"
    run_mixed_container \
      "stress-${name}-${client_name}" \
      "$case_root" \
      "${client_name}.toml" \
      "$remote_root" \
      "$client_name" \
      "$dataset_sizes" \
      "$DATASET_FILE_COUNT" \
      "$TRAIN_DURATION_SECS" \
      100 \
      "$evaluator_hotset" \
      "$EVALUATOR_OP_INTERVAL_MS" \
      "${client_name}-summary.json" \
      "${client_name}-driver.log" \
      "$eval_read_manifest_rel" \
      "$eval_read_manifest_rel"
  done

  rc=0
  docker wait "stress-${name}-seed" >/dev/null || rc=1
  for idx in $(seq 1 "$TRAINERS"); do
    docker wait "stress-${name}-trainer-$(printf '%02d' "$idx")" >/dev/null || rc=1
  done
  for idx in $(seq 1 "$CHECKPOINTERS"); do
    docker wait "stress-${name}-checkpoint-$(printf '%02d' "$idx")" >/dev/null || rc=1
  done
  for idx in $(seq 1 "$EVALUATORS"); do
    docker wait "stress-${name}-eval-$(printf '%02d' "$idx")" >/dev/null || rc=1
  done

  curl -sS http://127.0.0.1:9000/metrics > "$case_root/metrics.after.prom"
  summarize_training_case "$case_root" "$name" "$rc" "$expected_clients"

  local before_sent after_sent before_recv after_recv any_p2p_observed max_p99 all_stable data_plane_failures response_retry
  before_sent="$(sum_metric "$case_root/metrics.before.prom" client_p2p_bytes_sent)"
  after_sent="$(sum_metric "$case_root/metrics.after.prom" client_p2p_bytes_sent)"
  before_recv="$(sum_metric "$case_root/metrics.before.prom" client_p2p_bytes_recv)"
  after_recv="$(sum_metric "$case_root/metrics.after.prom" client_p2p_bytes_recv)"
  read -r any_p2p_observed max_p99 all_stable data_plane_failures response_retry <<<"$(python3 - <<'PY' "$case_root/scenario-summary.json"
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    report = json.load(f)
print(
    int(report.get('any_p2p_observed', False)),
    report.get('max_reader_p99_ms', 0.0),
    int(report.get('all_stable', False)),
    report.get('log_counts', {}).get('data_plane_failure', 0),
    report.get('log_counts', {}).get('response_retry', 0),
)
PY
)"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$name" \
    "$((after_sent-before_sent))" \
    "$((after_recv-before_recv))" \
    "$TRAINERS" \
    "$CHECKPOINTERS" \
    "$EVALUATORS" \
    "$REQUIRE_P2P" \
    "$any_p2p_observed" \
    "$all_stable" \
    "$data_plane_failures" \
    "$response_retry" >> "$RUN_ROOT/report.csv"

  python3 - <<'PY' "$case_root/scenario-summary.json"
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    report = json.load(f)
raise SystemExit(0 if report.get('all_stable') else 1)
PY
}

main() {
  trap cleanup EXIT
  prepare_env
  printf 'case,p2p_bytes_sent_delta,p2p_bytes_recv_delta,trainers,checkpointers,evaluators,p2p_required,any_p2p_observed,all_stable,data_plane_failures,response_retry\n' > "$RUN_ROOT/report.csv"

  run_training_case epoch_hotset "16MB,64MB" 100 "64MB" 100 100 dataset
  run_training_case checkpoint_overlap "16MB,64MB" 100 "64MB,256MB" 100 100 checkpoint
  run_training_case shuffle_eval_mix "4MB,16MB,64MB" 100 "64MB" 100 100 dataset

  python3 - <<'PY' "$RUN_ROOT" "$READ_CHUNK_SIZE" "$READ_CHUNK_NUM" "$REQUIRE_P2P"
import json
import sys
from pathlib import Path

run_root = Path(sys.argv[1])
read_chunk_size = sys.argv[2]
read_chunk_num = int(sys.argv[3])
require_p2p = sys.argv[4].lower() == 'true'
reports = []
for path in sorted(run_root.glob('*/scenario-summary.json')):
    reports.append(json.loads(path.read_text()))
final = {
    'run_root': str(run_root),
    'case_count': len(reports),
    'all_stable': all(item.get('all_stable') for item in reports),
    'p2p_required': require_p2p,
    'p2p_observed_cases': sum(1 for item in reports if item.get('any_p2p_observed')),
    'any_p2p_observed': any(item.get('any_p2p_observed') for item in reports),
    'coverage_gap_cases': sum(1 for item in reports if item.get('has_p2p_gap')),
    'read_chunk_size': read_chunk_size,
    'read_chunk_num': read_chunk_num,
    'cases': reports,
}
(run_root / 'final-report.json').write_text(json.dumps(final, indent=2, ensure_ascii=False) + '\n')
print(run_root / 'final-report.json')
PY
  cat "$RUN_ROOT/final-report.json"
  python3 - <<'PY' "$RUN_ROOT/final-report.json" "$REQUIRE_P2P"
import json
import sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    report = json.load(f)
require_p2p = sys.argv[2].lower() == 'true'
raise SystemExit(0 if report.get('all_stable') and (report.get('any_p2p_observed') or not require_p2p) else 1)
PY
}

main "$@"
