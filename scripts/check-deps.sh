#!/usr/bin/env bash
#
# Dependency boundary checks for the Curvine crate reorganization.
#
# report mode: show current violations without failing; useful during migration.
# final mode: enforce the target architecture; intended for the P7 gate.

set -euo pipefail

MODE="report"

usage() {
  cat <<'EOF'
Usage: scripts/check-deps.sh [--mode report|final]

Modes:
  report  Print current dependency boundary violations and exit 0.
  final   Fail if final forbidden dependencies remain.

Examples:
  scripts/check-deps.sh --mode report
  scripts/check-deps.sh --mode final
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      if [[ $# -lt 2 ]]; then
        echo "--mode requires one of: report, final" >&2
        usage >&2
        exit 2
      fi
      MODE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$MODE" in
  report|final) ;;
  *)
    echo "Invalid mode: $MODE" >&2
    usage >&2
    exit 2
    ;;
esac

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

for bin in cargo jq awk sort grep sed; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required command: $bin" >&2
    exit 2
  fi
done

failures=0

record_violation() {
  local label="$1"
  local detail="$2"

  if [[ "$MODE" == "final" ]]; then
    echo "FAIL [$label] $detail"
    failures=$((failures + 1))
  else
    echo "WARN [$label] $detail"
  fi
}

record_ok() {
  local label="$1"
  echo "OK   [$label]"
}

metadata="$(cargo metadata --format-version 1 --no-deps)"

direct_internal_deps="$(
  jq -r '
    .packages[]
    | select(.source == null) as $pkg
    | $pkg.dependencies[]
    | select(.path != null)
    | [$pkg.name, .name]
    | @tsv
  ' <<<"$metadata" | sort
)"

check_facade_direct_deps() {
  local facade_deps
  facade_deps="$(
    awk -F '\t' '
      ($2 == "curvine-common" || $2 == "orpc") && $1 != $2 {
        print $1 " -> " $2
      }
    ' <<<"$direct_internal_deps"
  )"

  if [[ -n "$facade_deps" ]]; then
    record_violation "facade-direct-deps" "internal crates still depend on curvine-common/orpc:
$facade_deps"
  else
    record_ok "facade-direct-deps"
  fi
}

tree_packages() {
  cargo tree "$@" -e normal --prefix none -f '{p}' \
    | awk '{print $1}' \
    | sed '/^$/d' \
    | sort -u
}

check_tree_forbidden() {
  local label="$1"
  local forbidden_csv="$2"
  shift 2

  local packages
  packages="$(tree_packages "$@")"

  local found=()
  local name
  IFS=',' read -r -a forbidden <<<"$forbidden_csv"
  for name in "${forbidden[@]}"; do
    if grep -qx "$name" <<<"$packages"; then
      found+=("$name")
    fi
  done

  if ((${#found[@]} > 0)); then
    record_violation "$label" "forbidden packages found: ${found[*]}"
  else
    record_ok "$label"
  fi
}

check_mixed_spdk_feature_risk() {
  local output
  local err
  err="$(mktemp)"

  if output="$(
    cargo tree \
      --no-default-features \
      --features curvine-server/spdk-rdma,curvine-common/system \
      -p curvine-cli \
      -p curvine-server \
      -e features \
      --prefix none \
      -f '{p} {f}' 2>"$err"
  )"; then
    if awk '
      $1 == "orpc" {
        features = $0
        sub(/^orpc v[^ ]+ /, "", features)
        sub(/^\([^)]*\) /, "", features)
        count = split(features, values, ",")
        for (i = 1; i <= count; i++) {
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", values[i])
          if (values[i] == "spdk" || values[i] == "spdk-rdma") {
            found = 1
          }
        }
      }
      END { exit found ? 0 : 1 }
    ' <<<"$output"; then
      record_violation "mixed-spdk-feature-risk" "server spdk-rdma feature still enables SPDK features on shared orpc during a mixed CLI/server cargo tree"
    else
      record_ok "mixed-spdk-feature-risk"
    fi
  else
    echo "SKIP [mixed-spdk-feature-risk] cargo tree command failed, likely because legacy features were removed:"
    sed 's/^/  /' "$err"
  fi

  rm -f "$err"
}

common_client_forbidden="curvine-common,orpc,curvine-ufs,opendal,rocksdb,raft,curvine-storage-spdk,curvine-rocksdb,curvine-raft,curvine-ufs-oss-hdfs,curvine-hdfs-jni"
libsdk_java_forbidden="curvine-common,orpc,curvine-ufs,opendal,rocksdb,raft,pyo3,curvine-storage-spdk,curvine-rocksdb,curvine-raft,curvine-ufs-oss-hdfs,curvine-hdfs-jni"
libsdk_python_forbidden="curvine-common,orpc,curvine-ufs,opendal,rocksdb,raft,jni,curvine-storage-spdk,curvine-rocksdb,curvine-raft,curvine-ufs-oss-hdfs,curvine-hdfs-jni"

echo "Dependency boundary check mode: $MODE"
echo

check_facade_direct_deps
check_tree_forbidden "curvine-cli-final-tree" "$common_client_forbidden" -p curvine-cli
check_tree_forbidden "curvine-fuse-final-tree" "$common_client_forbidden" -p curvine-fuse
check_tree_forbidden "curvine-libsdk-java-min-final-tree" "$libsdk_java_forbidden" -p curvine-libsdk --no-default-features --features java-sdk
check_tree_forbidden "curvine-libsdk-python-min-final-tree" "$libsdk_python_forbidden" -p curvine-libsdk --no-default-features --features python-sdk
check_mixed_spdk_feature_risk

echo
if [[ "$MODE" == "final" && "$failures" -gt 0 ]]; then
  echo "Dependency boundary check failed with $failures violation(s)."
  exit 1
fi

if [[ "$MODE" == "report" ]]; then
  echo "Report mode completed. Warnings are expected until the migration reaches the relevant DAG layer."
else
  echo "Final dependency boundary check passed."
fi
