#!/usr/bin/env bash
#
# Stage-2 dependency boundary checks for lightweight API/domain crates.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

for bin in cargo awk grep sort; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required command: $bin" >&2
    exit 2
  fi
done

crates=(
  curvine-error
  curvine-proto
  curvine-model
  curvine-fs-api
  curvine-storage-api
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

failures=0

tree_packages() {
  cargo tree \
    --no-default-features \
    -p "$1" \
    -e normal \
    --prefix none \
    -f '{p}' \
    | awk '{print $1}' \
    | sort -u
}

for crate in "${crates[@]}"; do
  packages="$(tree_packages "$crate")"
  found=()

  for dep in "${forbidden[@]}"; do
    if grep -qx "$dep" <<<"$packages"; then
      found+=("$dep")
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
