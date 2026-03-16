#!/usr/bin/env bash

set -euo pipefail

# E2E test for: `cv mount resync`
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)"
CV_BIN="${CV_BIN:-$REPO_ROOT/build/dist/bin/cv}"
CV_CONF="${CV_CONF:-$REPO_ROOT/build/dist/conf/curvine-cluster.toml}"
MC_ALIAS="${MC_ALIAS:-local}"
BUCKET="${BUCKET:-miniocluster}"
UFS_PREFIX="${UFS_PREFIX:-curvine-test}"
CV_PATH="${CV_PATH:-/miniocluster/curvine-test}"
S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://127.0.0.1:9009}"
S3_REGION="${S3_REGION:-cn-beijing}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minio}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minio123}"
S3_FORCE_PATH_STYLE="${S3_FORCE_PATH_STYLE:-true}"
STRESS_FILE_COUNT="${STRESS_FILE_COUNT:-10000}"
STRESS_MIN_FILES=1000
STRESS_MIN_DIRS=100
STRESS_UPLOAD_JOBS="${STRESS_UPLOAD_JOBS:-16}"

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

get_last_resync_metric() {
  local text="$1"
  local key="$2"
  echo "$text" | awk -v k="$key" '
    /resync summary:/ {
      for (i = 1; i <= NF; i++) {
        gsub(",", "", $i)
        split($i, kv, "=")
        if (kv[1] == k) {
          val = kv[2]
        }
      }
    }
    END {
      if (val != "") {
        print val
      }
    }
  '
}

cleanup_ufs_prefix() {
  local prefix="$1"
  mc rm --recursive --force "$MC_ALIAS/$BUCKET/$prefix" >/dev/null 2>&1 || true
}

cleanup_mount_and_ufs() {
  local cv_path="$1"
  local ufs_prefix="$2"
  run_cv umount "$cv_path" >/dev/null 2>&1 || true
  cleanup_ufs_prefix "$ufs_prefix"
}

cleanup_cv_test_path() {
  local cv_path="$1"
  local entry

  while read -r entry; do
    [[ -z "$entry" ]] && continue
    run_cv fs rm -r "$entry" >/dev/null 2>&1 || true
  done < <(run_cv fs ls --cache-only -C "$cv_path/" 2>/dev/null || true)
}

cleanup_base_test_data() {
  cleanup_ufs_prefix "$UFS_PREFIX"
  cleanup_cv_test_path "$CV_PATH"
}

cleanup_previous_test_prefixes() {
  local raw_name prefix
  while read -r _ _ _ raw_name; do
    [[ -z "$raw_name" ]] && continue
    prefix="${raw_name%/}"
    case "$prefix" in
      "$UFS_PREFIX"-auto-*|"$UFS_PREFIX"-*-stress|"$UFS_PREFIX"-cache-mode)
        cleanup_ufs_prefix "$prefix"
        ;;
    esac
  done < <(mc ls "$MC_ALIAS/$BUCKET" 2>/dev/null || true)
}

upload_stress_dir_files() {
  local prefix="$1"
  local path="$2"
  local start_idx="$3"
  local files_in_dir="$4"
  local seed_file="$5"
  local idx dst

  for idx in $(seq "$start_idx" $((start_idx + files_in_dir - 1))); do
    dst="$MC_ALIAS/$BUCKET/$prefix/$path/f-$idx.txt"
    mc cp "$seed_file" "$dst" >/dev/null
  done
}

create_nested_stress_files() {
  local prefix="$1"
  local count="$2"
  local seed_file="$3"
  local target_dirs base_files remainder
  local dir_idx depth file_idx created_files
  local files_in_dir part p path
  local -a dir_paths
  local -a batch_pids

  if ((count < STRESS_MIN_FILES)); then
    fail "STRESS_FILE_COUNT must be >= $STRESS_MIN_FILES, got $count"
  fi

  # Compute directory distribution first:
  # - keep at least 100 subdirectories
  # - spread files across directories to avoid single-dir hot spot
  target_dirs=$((count / 50))
  if ((target_dirs < STRESS_MIN_DIRS)); then
    target_dirs=$STRESS_MIN_DIRS
  fi
  if ((target_dirs > count)); then
    target_dirs=$count
  fi

  base_files=$((count / target_dirs))
  remainder=$((count % target_dirs))

  dir_paths=()
  for dir_idx in $(seq 1 "$target_dirs"); do
    depth=$((3 + (dir_idx % 3)))
    path="d1-$dir_idx"
    for part in $(seq 2 "$depth"); do
      p=$(((dir_idx * 17 + part * 13) % 127))
      path="${path}/d${part}-${p}"
    done
    dir_paths+=("$path")
  done

  log "stress distribution: files=$count dirs=$target_dirs depth=3~5 base_files_per_dir=$base_files remainder=$remainder"
  log "stress upload jobs: $STRESS_UPLOAD_JOBS"

  batch_pids=()
  created_files=0
  file_idx=1
  for dir_idx in $(seq 1 "$target_dirs"); do
    files_in_dir=$base_files
    if ((dir_idx <= remainder)); then
      files_in_dir=$((files_in_dir + 1))
    fi

    path="${dir_paths[$((dir_idx - 1))]}"
    upload_stress_dir_files "$prefix" "$path" "$file_idx" "$files_in_dir" "$seed_file" &
    batch_pids+=("$!")
    file_idx=$((file_idx + files_in_dir))
    created_files=$((created_files + files_in_dir))

    if ((${#batch_pids[@]} >= STRESS_UPLOAD_JOBS)); then
      for pid in "${batch_pids[@]}"; do
        wait "$pid" || fail "parallel stress upload failed for prefix $prefix"
      done
      batch_pids=()
      log "created $created_files/$count stress files under $prefix"
    fi
  done

  for pid in "${batch_pids[@]}"; do
    wait "$pid" || fail "parallel stress upload failed for prefix $prefix"
  done
  log "created $created_files/$count stress files under $prefix"

  if ((created_files != count)); then
    fail "stress file creation mismatch: created $created_files expected $count"
  fi
}

need_cmd mc
need_cmd "$CV_BIN"

log "using CV_BIN=$CV_BIN"
log "using CV_CONF=$CV_CONF"
log "using MC_ALIAS=$MC_ALIAS BUCKET=$BUCKET UFS_PREFIX=$UFS_PREFIX"
log "using S3_ENDPOINT_URL=$S3_ENDPOINT_URL S3_FORCE_PATH_STYLE=$S3_FORCE_PATH_STYLE"
log "using STRESS_FILE_COUNT=$STRESS_FILE_COUNT"
log "using STRESS_UPLOAD_JOBS=$STRESS_UPLOAD_JOBS"
log "cleaning previous test prefixes in s3"
cleanup_previous_test_prefixes
log "cleaning previous base test data in ufs and curvine"
cleanup_base_test_data

if ! run_cv mount | grep -q "$CV_PATH"; then
  log "mount not found, creating mount for $CV_PATH"
  run_cv mount "s3://$BUCKET/$UFS_PREFIX" "$CV_PATH" \
    --config s3.region_name="$S3_REGION" \
    --config s3.endpoint_url="$S3_ENDPOINT_URL" \
    --config s3.credentials.access="$S3_ACCESS_KEY" \
    --config s3.credentials.secret="$S3_SECRET_KEY" \
    --config s3.force.path.style="$S3_FORCE_PATH_STYLE"
fi

TMP_DIR="$(mktemp -d)"
trap 'cleanup_previous_test_prefixes; cleanup_base_test_data; rm -rf "$TMP_DIR"' EXIT

echo "a1" > "$TMP_DIR/a.txt"
echo "b1" > "$TMP_DIR/b.txt"
echo "c1" > "$TMP_DIR/c.txt"

a_mc_path="$MC_ALIAS/$BUCKET/$UFS_PREFIX/a.txt"
b_mc_path="$MC_ALIAS/$BUCKET/$UFS_PREFIX/dir1/b.txt"
c_mc_path="$MC_ALIAS/$BUCKET/$UFS_PREFIX/dir1/dir2/c.txt"

log "uploading layered test files to UFS"
cleanup_ufs_prefix "$UFS_PREFIX"
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
# Ensure source object exists in UFS so ufs_time=0 skip assertion is stable.
mc cp "$TMP_DIR/a.txt" "$a_mc_path" >/dev/null
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

# Scenario E: resync on cache_mode mount must fail with clear error
CACHE_MODE_CV_PATH="${CV_PATH}-cache-mode"
CACHE_MODE_UFS_PREFIX="${UFS_PREFIX}-cache-mode"
cleanup_mount_and_ufs "$CACHE_MODE_CV_PATH" "$CACHE_MODE_UFS_PREFIX"
if run_cv mount | grep -q "$CACHE_MODE_CV_PATH"; then
  run_cv umount "$CACHE_MODE_CV_PATH" >/dev/null 2>&1 || true
fi
log "creating cache_mode mount at $CACHE_MODE_CV_PATH for resync rejection test"
run_cv mount "s3://$BUCKET/$CACHE_MODE_UFS_PREFIX" "$CACHE_MODE_CV_PATH" \
  --write-type cache_mode \
  --config s3.region_name="$S3_REGION" \
  --config s3.endpoint_url="$S3_ENDPOINT_URL" \
  --config s3.credentials.access="$S3_ACCESS_KEY" \
  --config s3.credentials.secret="$S3_SECRET_KEY" \
  --config s3.force.path.style="$S3_FORCE_PATH_STYLE" \
  >/dev/null 2>&1
out_resync_reject="$(run_cv mount resync "$CACHE_MODE_CV_PATH" 2>&1)" || true
assert_contains "$out_resync_reject" "resync is only allowed for fs_mode" "resync on cache_mode mount must report fs_mode-only error"
assert_contains "$out_resync_reject" "cache_mode" "error message must mention current write_type cache_mode"
cleanup_mount_and_ufs "$CACHE_MODE_CV_PATH" "$CACHE_MODE_UFS_PREFIX"
log "scenario E: resync correctly rejected for cache_mode mount"

# Scenario F: first mount auto-resync should create nested CV dirs and avoid list-status errors.
AUTO_TAG="$(date +%s)-$$"
AUTO_UFS_PREFIX="${UFS_PREFIX}-auto-${AUTO_TAG}"
AUTO_CV_PATH="${CV_PATH}-auto-${AUTO_TAG}"
cleanup_mount_and_ufs "$AUTO_CV_PATH" "$AUTO_UFS_PREFIX"
auto_a_mc_path="$MC_ALIAS/$BUCKET/$AUTO_UFS_PREFIX/dir_auto/a.txt"
auto_b_mc_path="$MC_ALIAS/$BUCKET/$AUTO_UFS_PREFIX/dir_auto/dir2/b.txt"
echo "auto-a" > "$TMP_DIR/auto-a.txt"
echo "auto-b" > "$TMP_DIR/auto-b.txt"
mc cp "$TMP_DIR/auto-a.txt" "$auto_a_mc_path" >/dev/null
mc cp "$TMP_DIR/auto-b.txt" "$auto_b_mc_path" >/dev/null
log "scenario F: first mount auto-resync should not fail on missing cv dirs"
out_f="$(run_cv mount "s3://$BUCKET/$AUTO_UFS_PREFIX" "$AUTO_CV_PATH" \
  --config s3.region_name="$S3_REGION" \
  --config s3.endpoint_url="$S3_ENDPOINT_URL" \
  --config s3.credentials.access="$S3_ACCESS_KEY" \
  --config s3.credentials.secret="$S3_SECRET_KEY" \
  --config s3.force.path.style="$S3_FORCE_PATH_STYLE" 2>&1)"
echo "$out_f"
assert_contains "$out_f" "first mount detected, start resync" "expected auto resync on first mount"
assert_not_contains "$out_f" "failed to list cv dir" "auto-resync should create missing cv dirs before listing"
assert_not_contains "$out_f" "resync summary:.*failed=[1-9][0-9]*" "auto-resync should not report failures for missing cv dirs"
cleanup_mount_and_ufs "$AUTO_CV_PATH" "$AUTO_UFS_PREFIX"

# Scenario G: stress test with 10000 nested files and verify metadata sync.
STRESS_TAG="$(date +%s)-$$-stress"
STRESS_UFS_PREFIX="${UFS_PREFIX}-${STRESS_TAG}"
STRESS_CV_PATH="${CV_PATH}-${STRESS_TAG}"
cleanup_mount_and_ufs "$STRESS_CV_PATH" "$STRESS_UFS_PREFIX"
echo "stress-seed" > "$TMP_DIR/stress-seed.txt"
log "scenario G: creating $STRESS_FILE_COUNT nested files in UFS for stress test"
create_nested_stress_files "$STRESS_UFS_PREFIX" "$STRESS_FILE_COUNT" "$TMP_DIR/stress-seed.txt"
log "scenario G: mount and trigger auto resync for stress path"
out_g_mount="$(run_cv mount "s3://$BUCKET/$STRESS_UFS_PREFIX" "$STRESS_CV_PATH" \
  --config s3.region_name="$S3_REGION" \
  --config s3.endpoint_url="$S3_ENDPOINT_URL" \
  --config s3.credentials.access="$S3_ACCESS_KEY" \
  --config s3.credentials.secret="$S3_SECRET_KEY" \
  --config s3.force.path.style="$S3_FORCE_PATH_STYLE" 2>&1)"
echo "$out_g_mount"
assert_contains "$out_g_mount" "first mount detected, start resync" "stress mount should trigger auto resync"
assert_not_contains "$out_g_mount" "resync summary:.*failed=[1-9][0-9]*" "stress auto-resync should finish without failures"
stress_scanned="$(get_last_resync_metric "$out_g_mount" "scanned")"
stress_recreated="$(get_last_resync_metric "$out_g_mount" "recreated")"
stress_skip_same="$(get_last_resync_metric "$out_g_mount" "skip_same_mtime")"
stress_skip_zero="$(get_last_resync_metric "$out_g_mount" "skip_ufs_time_zero")"
if [[ -z "$stress_scanned" || -z "$stress_recreated" || -z "$stress_skip_same" || -z "$stress_skip_zero" ]]; then
  fail "cannot parse stress resync summary from mount output"
fi
if ((stress_scanned != STRESS_FILE_COUNT)); then
  fail "stress scanned mismatch: got $stress_scanned expected $STRESS_FILE_COUNT"
fi
if ((stress_recreated != STRESS_FILE_COUNT)); then
  fail "stress recreated mismatch: got $stress_recreated expected $STRESS_FILE_COUNT"
fi
if ((stress_skip_same != 0 || stress_skip_zero != 0)); then
  fail "stress skip mismatch: skip_same_mtime=$stress_skip_same skip_ufs_time_zero=$stress_skip_zero expected 0"
fi

log "scenario G: verify synced metadata count == $STRESS_FILE_COUNT"
stress_listing="$(run_cv fs ls --cache-only -C -R "$STRESS_CV_PATH" 2>&1)"
stress_file_count="$(echo "$stress_listing" | grep -cE "/f-[0-9]+\\.txt$")"
if ((stress_file_count != STRESS_FILE_COUNT)); then
  fail "stress sync file count mismatch: got $stress_file_count expected $STRESS_FILE_COUNT"
fi
cleanup_mount_and_ufs "$STRESS_CV_PATH" "$STRESS_UFS_PREFIX"
cleanup_previous_test_prefixes
cleanup_base_test_data

pass "all resync scenarios passed"
