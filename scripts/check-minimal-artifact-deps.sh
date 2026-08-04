#!/usr/bin/env bash
#
# Runtime dependency checks for minimal client-side artifacts.

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/check-minimal-artifact-deps.sh [--allow-missing] [--allow-rdma-spdk] [--artifact LABEL=PATH] [PATH ...]

By default this enforces minimal/non-RDMA client artifacts. With no artifacts,
the script checks known Curvine outputs in target/{debug,release} and
build/dist/lib. Explicit artifacts must exist unless --allow-missing is set.

Use --allow-rdma-spdk only for explicitly RDMA/SPDK-enabled client or FUSE
artifacts; native UFS and storage-library checks remain enforced.
EOF
}

allow_missing=0
allow_rdma_spdk=0
declare -a artifact_specs=()
using_defaults=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-missing)
      allow_missing=1
      shift
      ;;
    --allow-rdma-spdk)
      allow_rdma_spdk=1
      shift
      ;;
    --artifact)
      if [[ $# -lt 2 ]]; then
        echo "--artifact requires LABEL=PATH" >&2
        usage >&2
        exit 2
      fi
      artifact_specs+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      artifact_specs+=("$1")
      shift
      ;;
  esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

for bin in grep sed; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required command: $bin" >&2
    exit 2
  fi
done

if [[ ${#artifact_specs[@]} -eq 0 ]]; then
  using_defaults=1
  artifact_specs=(
    "curvine-cli=target/debug/curvine-cli"
    "curvine-fuse=target/debug/curvine-fuse"
    "curvine-libsdk=target/debug/libcurvine_libsdk.so"
    "curvine-libsdk-python=target/debug/libcurvine_libsdk_python.so"
    "curvine-cli=target/release/curvine-cli"
    "curvine-fuse=target/release/curvine-fuse"
    "curvine-libsdk=target/release/libcurvine_libsdk.so"
    "curvine-libsdk-python=target/release/libcurvine_libsdk_python.so"
    "curvine-cli=build/dist/lib/curvine-cli"
    "curvine-fuse=build/dist/lib/curvine-fuse"
    "curvine-libsdk=build/dist/lib/libcurvine_libsdk.so"
    "curvine-libsdk-python=build/dist/lib/libcurvine_libsdk_python.so"
  )
fi

artifact_dynamic_entries() {
  local inspector="$1"
  local artifact="$2"

  case "$inspector" in
    readelf)
      readelf -d "$artifact" 2>/dev/null | grep -E 'NEEDED|RPATH|RUNPATH' || true
      ;;
    llvm-readelf)
      llvm-readelf -d "$artifact" 2>/dev/null | grep -E 'NEEDED|RPATH|RUNPATH' || true
      ;;
    otool)
      otool -L "$artifact" 2>/dev/null || true
      ;;
  esac
}

artifact_inspector() {
  local artifact="$1"

  if command -v readelf >/dev/null 2>&1 && readelf -h "$artifact" >/dev/null 2>&1; then
    echo "readelf"
  elif command -v llvm-readelf >/dev/null 2>&1 && llvm-readelf -h "$artifact" >/dev/null 2>&1; then
    echo "llvm-readelf"
  elif command -v otool >/dev/null 2>&1 && otool -hv "$artifact" >/dev/null 2>&1; then
    echo "otool"
  fi
}

rdma_spdk_pattern='libibverbs\.so|librdmacm\.so|libspdk|libdpdk|librte_'
native_ufs_pattern='libjindosdk|libhdfs|libjvm|libjli'
native_storage_pattern='librocksdb'
checked=0
failures=0

for spec in "${artifact_specs[@]}"; do
  label="$spec"
  artifact="$spec"
  if [[ "$spec" == *=* ]]; then
    label="${spec%%=*}"
    artifact="${spec#*=}"
  else
    label="${artifact##*/}"
  fi

  if [[ ! -e "$artifact" ]]; then
    if [[ "$allow_missing" -eq 1 || "$using_defaults" -eq 1 ]]; then
      echo "SKIP [$label] missing artifact: $artifact"
      continue
    fi
    echo "FAIL [$label] missing artifact: $artifact" >&2
    failures=$((failures + 1))
    continue
  fi

  inspector="$(artifact_inspector "$artifact")"
  if [[ -z "$inspector" ]]; then
    echo "SKIP [$label] artifact is not inspectable by readelf/llvm-readelf/otool: $artifact"
    continue
  fi

  checked=$((checked + 1))
  needed="$(artifact_dynamic_entries "$inspector" "$artifact")"
  if [[ "$allow_rdma_spdk" -eq 0 ]] && grep -E "$rdma_spdk_pattern" <<<"$needed" >/dev/null; then
    echo "FAIL [$label] RDMA/SPDK runtime dependency found in minimal/non-RDMA artifact: $artifact" >&2
    echo "$needed" | sed 's/^/  /' >&2
    failures=$((failures + 1))
    continue
  fi

  if grep -E "$native_storage_pattern" <<<"$needed" >/dev/null; then
    echo "FAIL [$label] native storage runtime dependency found in minimal client artifact: $artifact" >&2
    echo "$needed" | sed 's/^/  /' >&2
    failures=$((failures + 1))
    continue
  fi

  if grep -E "$native_ufs_pattern" <<<"$needed" >/dev/null; then
    echo "FAIL [$label] native UFS runtime dependency found in minimal client artifact: $artifact" >&2
    echo "$needed" | sed 's/^/  /' >&2
    failures=$((failures + 1))
    continue
  fi

  echo "OK   [$label] runtime dependencies"
done

if ((failures > 0)); then
  echo "Minimal artifact runtime dependency check failed with $failures violation(s)." >&2
  exit 1
fi

if ((checked == 0)); then
  echo "No inspectable artifacts found. Build minimal client artifacts before running this gate." >&2
  exit 2
fi

if [[ "$allow_rdma_spdk" -eq 1 ]]; then
  echo "Runtime dependency check passed with explicit RDMA/SPDK dependencies allowed."
else
  echo "Minimal/non-RDMA artifact runtime dependency check passed."
fi
