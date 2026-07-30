#!/usr/bin/env bash
#
# Runtime dependency gates for minimal client-side artifacts.

set -euo pipefail

PROFILE_DIR="${1:-target/debug}"

for bin in grep; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required command: $bin" >&2
    exit 2
  fi
done

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

select_inspector() {
  local artifact="$1"

  if command -v readelf >/dev/null 2>&1 && readelf -h "$artifact" >/dev/null 2>&1; then
    echo "readelf"
  elif command -v llvm-readelf >/dev/null 2>&1 && llvm-readelf -h "$artifact" >/dev/null 2>&1; then
    echo "llvm-readelf"
  elif command -v otool >/dev/null 2>&1 && otool -hv "$artifact" >/dev/null 2>&1; then
    echo "otool"
  fi
}

check_artifact() {
  local label="$1"
  local artifact="$2"

  if [ ! -e "$artifact" ]; then
    echo "FAIL [$label] missing artifact: $artifact"
    return 1
  fi

  local inspector
  inspector="$(select_inspector "$artifact")"
  if [ -z "$inspector" ]; then
    echo "FAIL [$label] no readelf/llvm-readelf/otool inspector for artifact: $artifact"
    return 1
  fi

  local needed
  needed="$(artifact_dynamic_entries "$inspector" "$artifact")"
  if grep -E 'libibverbs\.so|librdmacm\.so|libspdk|librte_|libjindosdk|libhdfs|libjvm|libjli' <<<"$needed" >/dev/null; then
    echo "FAIL [$label] forbidden native runtime dependency found:"
    echo "$needed"
    return 1
  fi

  echo "OK   [$label] $inspector"
}

failures=0

check_artifact "curvine-cli" "$PROFILE_DIR/curvine-cli" || failures=$((failures + 1))
check_artifact "curvine-fuse" "$PROFILE_DIR/curvine-fuse" || failures=$((failures + 1))

sdk_found=0
for sdk_artifact in \
  "$PROFILE_DIR"/libcurvine_libsdk.so \
  "$PROFILE_DIR"/libcurvine_libsdk.dylib \
  "$PROFILE_DIR"/curvine_libsdk.dll \
  "$PROFILE_DIR"/libcurvine_libsdk_python.so \
  "$PROFILE_DIR"/libcurvine_libsdk_python.dylib \
  "$PROFILE_DIR"/curvine_libsdk_python.dll
do
  if [ -e "$sdk_artifact" ]; then
    sdk_found=1
    check_artifact "$(basename "$sdk_artifact")" "$sdk_artifact" || failures=$((failures + 1))
  fi
done

if [ "$sdk_found" -eq 0 ]; then
  echo "FAIL [libsdk] no SDK dynamic library found under $PROFILE_DIR"
  failures=$((failures + 1))
fi

if [ "$failures" -gt 0 ]; then
  echo "Minimal artifact runtime dependency gate failed with $failures violation(s)."
  exit 1
fi

echo "Minimal artifact runtime dependency gate passed."
