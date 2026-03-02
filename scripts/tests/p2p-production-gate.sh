#!/usr/bin/env bash
set -euo pipefail

run() {
  echo "[p2p-gate] $*"
  "$@"
}

run cargo test -p curvine-client --test p2p_e2e_test -- --nocapture
run cargo test -p curvine-tests --test p2p_read_acceleration_test -- --nocapture

echo "[p2p-gate] all required checks passed"
