#!/usr/bin/env bash
#
# Stage-2 dependency boundary checks for lightweight API/domain crates.

set -euo pipefail

for bin in git cargo awk grep sort; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required command: $bin" >&2
    exit 2
  fi
done

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

crates=(
  curvine-error
  curvine-proto
  curvine-model
  curvine-fs-api
  curvine-storage-api
  curvine-ufs-api
  curvine-config
)

forbidden=(
  rocksdb
  raft
  axum
  opendal
  jni
  pyo3
  curvine-ufs
  curvine-client
  curvine-server
  curvine-web
  curvine-libsdk
  curvine-storage-spdk
  curvine-rocksdb
  curvine-raft
  curvine-ufs-oss-hdfs
  curvine-hdfs-jni
)

# tonic 0.14 separates its HTTP/2 transport from its axum router. Checking
# for axum alone no longer detects a transport-enabled tonic dependency.
forbidden_tonic_features=(
  channel
  server
  transport
  router
  h2
  tls-ring
  tls-aws-lc
  tls-native-roots
  tls-webpki-roots
  tls-connect-info
  _tls-any
)

failures=0

dependency_tree() {
  cargo tree \
    --no-default-features \
    -p "$1" \
    -e normal \
    --prefix none \
    -f '{p}|{f}'
}

for crate in "${crates[@]}"; do
  tree="$(dependency_tree "$crate")"
  packages="$(awk -F'|' '{print $1}' <<<"$tree" | awk '{print $1}' | sort -u)"
  tonic_features="$(awk -F'|' '$1 ~ /^tonic v/ {print $2}' <<<"$tree" | sort -u)"
  found=()

  for dep in "${forbidden[@]}"; do
    if grep -qx "$dep" <<<"$packages"; then
      found+=("$dep")
    fi
  done

  for feature in "${forbidden_tonic_features[@]}"; do
    if grep -Eq "(^|,)${feature}(,|$)" <<<"$tonic_features"; then
      found+=("tonic feature $feature")
    fi
  done

  if ((${#found[@]} > 0)); then
    echo "FAIL [$crate] forbidden dependencies found: ${found[*]}"
    failures=$((failures + 1))
  else
    echo "OK   [$crate]"
  fi
done

if ((failures > 0)); then
  echo "API crate dependency boundary check failed with $failures violation(s)." >&2
  exit 1
fi

echo "API crate dependency boundary check passed."
