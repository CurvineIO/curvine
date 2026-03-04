#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_SCRIPT="$ROOT/scripts/tests/p2p-docker-e2e-proof.sh"
OUT_ROOT="${P2P_MATRIX_EVID_ROOT:-$ROOT/evidence/p2p-matrix}"
TS="$(date +%Y%m%d-%H%M%S)"
RUN_ROOT="$OUT_ROOT/$TS"
mkdir -p "$RUN_ROOT"

log() {
  echo "[p2p-matrix] $*"
}

if [[ ! -x "$BASE_SCRIPT" ]]; then
  echo "base script not found: $BASE_SCRIPT" >&2
  exit 1
fi

CASES=(
  "baseline||4|0|12|8"
  "cross_az|delay 20ms loss 0.1%|4|0|14|8"
  "unstable|delay 35ms 10ms loss 0.3%|4|50|16|8"
  "soak|delay 20ms loss 0.1%|64|20|20|4"
)

for spec in "${CASES[@]}"; do
  IFS='|' read -r CASE_NAME NETEM_RULE READS READ_INTERVAL METRICS_WAIT DATA_MB <<<"$spec"
  CASE_ROOT="$RUN_ROOT/$CASE_NAME"
  mkdir -p "$CASE_ROOT"
  log "run case=$CASE_NAME netem='${NETEM_RULE:-none}' reads=$READS"

  set +e
  P2P_EVID_ROOT="$CASE_ROOT/evidence" \
  P2P_NETEM_RULE="$NETEM_RULE" \
  P2P_CONSUMER_READS="$READS" \
  P2P_CONSUMER_READ_INTERVAL_MS="$READ_INTERVAL" \
  P2P_METRICS_WAIT_SECS="$METRICS_WAIT" \
  P2P_DATA_MB="$DATA_MB" \
  bash "$BASE_SCRIPT" > "$CASE_ROOT/run.log" 2>&1
  RC=$?
  set -e

  echo "$RC" > "$CASE_ROOT/exit_code.txt"
  SUMMARY_PATH="$(find "$CASE_ROOT/evidence" -name proof-summary.json -type f 2>/dev/null | sort | tail -n1 || true)"
  echo "$SUMMARY_PATH" > "$CASE_ROOT/summary_path.txt"

  if [[ "$RC" -eq 0 ]]; then
    log "case=$CASE_NAME PASS"
  else
    log "case=$CASE_NAME FAIL (rc=$RC)"
  fi
done

RUN_ROOT="$RUN_ROOT" python3 - <<'PY'
import json
import os
from pathlib import Path

run_root = Path(os.environ["RUN_ROOT"])
results = []
pass_count = 0

for case_dir in sorted([p for p in run_root.iterdir() if p.is_dir()]):
    rc_path = case_dir / "exit_code.txt"
    rc = int(rc_path.read_text().strip()) if rc_path.exists() else 1
    summary_path = (case_dir / "summary_path.txt").read_text().strip() if (case_dir / "summary_path.txt").exists() else ""
    summary = {}
    if summary_path and Path(summary_path).exists():
        summary = json.loads(Path(summary_path).read_text())

    passed = rc == 0
    if passed:
        pass_count += 1

    results.append(
        {
            "case": case_dir.name,
            "passed": passed,
            "exit_code": rc,
            "summary_path": summary_path,
            "netem_rule": summary.get("netem_rule", ""),
            "consumer_reads": summary.get("consumer_reads", 0),
            "data_mb": summary.get("data_mb", 0),
            "consumer_p2p_recv_delta": summary.get("consumer_p2p_recv_delta", 0),
            "metrics_delta": summary.get("metrics_delta", {}),
            "response_peer_matches_provider": summary.get("response_peer_matches_provider", False),
            "consumer_no_fallback_failed_as_expected": summary.get("consumer_no_fallback_failed_as_expected", False),
        }
    )

report = {
    "run_root": str(run_root),
    "total_cases": len(results),
    "passed_cases": pass_count,
    "all_passed": pass_count == len(results),
    "cases": results,
}

out = run_root / "final-report.json"
out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
print(out)
PY

cat "$RUN_ROOT/final-report.json"

if ! python3 - <<'PY' "$RUN_ROOT/final-report.json"
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    report = json.load(f)
raise SystemExit(0 if report.get("all_passed") else 1)
PY
then
  echo "[p2p-matrix] FAIL" >&2
  exit 1
fi

log "PASS"
log "report=$RUN_ROOT/final-report.json"
