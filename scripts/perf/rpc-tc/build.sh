#!/usr/bin/env bash
# Build identical workloads against a detached baseline and this working tree.
set -euo pipefail
if [[ $# != 2 ]]; then echo 'Usage: build.sh BASE_REVISION /absolute/path/output-directory' >&2; exit 2; fi
repo=$(git rev-parse --show-toplevel)
base=$(git -C "$repo" rev-parse --verify "$1^{commit}")
mkdir -p "$2"
out=$(cd "$2" && pwd -P)
[[ ! -e $out/baseline-src && ! -e $out/build-metadata.txt ]] || { echo 'Output already contains benchmark build files' >&2; exit 2; }
example=crates/client/curvine-client-core/examples/rpc_tc_read.rs
[[ -f $repo/$example ]] || { echo "Missing $example" >&2; exit 1; }
source_hash=$(sha256sum "$repo/$example" | cut -d' ' -f1)
git -C "$repo" worktree add --detach "$out/baseline-src" "$base"
mkdir -p "$out/baseline-src/$(dirname "$example")"
cp "$repo/$example" "$out/baseline-src/$example"
{
    printf 'baseline_revision=%s\n' "$base"
    printf 'candidate_head=%s\n' "$(git -C "$repo" rev-parse HEAD)"
    printf 'workload_sha256=%s\n' "$source_hash"
    printf 'candidate_status_begin\n'
    git -C "$repo" status --short
    printf 'candidate_status_end\n'
} > "$out/build-metadata.txt"
git -C "$repo" diff --binary HEAD > "$out/candidate-tracked.patch"
# Untracked files are intentionally not copied: keep the working tree (or commit
# it) to reproduce a dirty candidate, and retain its status above.
for variant in baseline candidate; do
    if [[ $variant == baseline ]]; then tree=$out/baseline-src; else tree=$repo; fi
    target=$out/$variant-target
    (cd "$tree" && CARGO_TARGET_DIR="$target" cargo build --locked -p curvine-server --bin curvine-server --release)
    (cd "$tree" && RPC_TC_SOURCE_SHA256="$source_hash" CARGO_TARGET_DIR="$target" cargo build --locked -p curvine-client-core --example rpc_tc_read --release)
done
sha256sum "$out"/{baseline,candidate}-target/release/curvine-server \
    "$out"/{baseline,candidate}-target/release/examples/rpc_tc_read > "$out/binary-sha256.txt"
printf 'baseline_server=%s\nbaseline_client=%s\ncandidate_server=%s\ncandidate_client=%s\n' \
    "$out/baseline-target/release/curvine-server" "$out/baseline-target/release/examples/rpc_tc_read" \
    "$out/candidate-target/release/curvine-server" "$out/candidate-target/release/examples/rpc_tc_read"
