#!/usr/bin/env bash

set -euo pipefail

# E2E test for: `cv mount resync`
CV_BIN="${CV_BIN:-/data/CodeSpace/curvine/build/dist/bin/cv}"
CV_CONF="${CV_CONF:-/data/CodeSpace/curvine/build/dist/conf/curvine-cluster.toml}"
MC_ALIAS="${MC_ALIAS:-local}"
BUCKET="${BUCKET:-miniocluster}"
UFS_PREFIX="${UFS_PREFIX:-curvine-test}"
CV_PATH="${CV_PATH:-/miniocluster/curvine-test}"
S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://127.0.0.1:9009}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minio}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minio123}"
S3_FORCE_PATH_STYLE="${S3_FORCE_PATH_STYLE:-true}"

run_cv() {
  "$CV_BIN" --conf "$CV_CONF" "$@"
}

log() { echo "[resync-e2e] $*"; }
pass() { echo "[PASS] $*"; }
fail() { echo "[FAIL] $*"; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "command not found: $1"
}

assert_contains() {
  local text="$1"
  local pattern="$2"
  local msg="$3"
  echo "$text" | grep -qE "$pattern" || fail "$msg"
}

assert_not_contains() {
  local text="$1"
  local pattern="$2"
  local msg="$3"
  if echo "$text" | grep -qE "$pattern"; then
    fail "$msg"
  fi
}

need_cmd mc
need_cmd "$CV_BIN"

log "using CV_BIN=$CV_BIN"
log "using CV_CONF=$CV_CONF"
log "using MC_ALIAS=$MC_ALIAS BUCKET=$BUCKET UFS_PREFIX=$UFS_PREFIX"
log "using S3_ENDPOINT_URL=$S3_ENDPOINT_URL S3_FORCE_PATH_STYLE=$S3_FORCE_PATH_STYLE"

if ! run_cv mount | grep -q "$CV_PATH"; then
  log "mount not found, creating mount for $CV_PATH"
  run_cv mount "s3://$BUCKET/$UFS_PREFIX" "$CV_PATH" \
    --config s3.endpoint_url="$S3_ENDPOINT_URL" \
    --config s3.credentials.access="$S3_ACCESS_KEY" \
    --config s3.credentials.secret="$S3_SECRET_KEY" \
    --config s3.force.path.style="$S3_FORCE_PATH_STYLE"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "a1" > "$TMP_DIR/a.txt"
echo "b1" > "$TMP_DIR/b.txt"
echo "c1" > "$TMP_DIR/c.txt"

a_mc_path="$MC_ALIAS/$BUCKET/$UFS_PREFIX/a.txt"
b_mc_path="$MC_ALIAS/$BUCKET/$UFS_PREFIX/dir1/b.txt"
c_mc_path="$MC_ALIAS/$BUCKET/$UFS_PREFIX/dir1/dir2/c.txt"

log "uploading layered test files to UFS"
mc cp "$TMP_DIR/a.txt" "$a_mc_path" >/dev/null
mc cp "$TMP_DIR/b.txt" "$b_mc_path" >/dev/null
mc cp "$TMP_DIR/c.txt" "$c_mc_path" >/dev/null

log "scenario A: initial resync"
out_a="$(run_cv mount resync "$CV_PATH" --verbose 2>&1)"
echo "$out_a"
assert_not_contains "$out_a" "sender dropped|buffer writer error| ERROR " "resync output contains unexpected ERROR"

cache_ls="$(run_cv fs ls --cache-only "$CV_PATH/" 2>&1)"
assert_contains "$cache_ls" "a.txt" "cache-only should include a.txt"
assert_contains "$cache_ls" "b.txt|dir1" "cache-only should include nested entries"

log "scenario B: same mtime -> skip"
out_b="$(run_cv mount resync "$CV_PATH" --verbose 2>&1)"
echo "$out_b"
assert_contains "$out_b" "skip \(same mtime\)" "expected same-mtime skips"

log "scenario C: cv ufs_mtime=0 -> skip"
run_cv fs rm --cache-only "$CV_PATH/a.txt" >/dev/null || true
run_cv fs touch --cache-only "$CV_PATH/a.txt" >/dev/null
out_c="$(run_cv mount resync "$CV_PATH" --verbose 2>&1)"
echo "$out_c"
assert_contains "$out_c" "skip \(ufs_time=0\): $CV_PATH/a.txt" "expected ufs_time=0 skip for a.txt"

log "scenario D: mtime mismatch -> recreate"
echo "b2-mod" > "$TMP_DIR/b.txt"
mc cp "$TMP_DIR/b.txt" "$b_mc_path" >/dev/null
out_d="$(run_cv mount resync "$CV_PATH" --verbose 2>&1)"
echo "$out_d"
assert_contains "$out_d" "recreate $CV_PATH/dir1/b.txt" "expected recreate for dir1/b.txt"

stat_b="$(run_cv fs stat --cache-only "$CV_PATH/dir1/b.txt" 2>&1)"
assert_contains "$stat_b" "ufs_mtime: [1-9][0-9]+" "expected non-zero ufs_mtime in cache-only stat"

pass "all resync scenarios passed"
