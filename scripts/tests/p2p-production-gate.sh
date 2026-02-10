#!/usr/bin/env bash
set -euo pipefail

run() {
  echo "[p2p-gate] $*"
  "$@"
}

run cargo test -p curvine-client p2p::service::tests::master_policy_accepts_any_signature_in_rotation_window -- --nocapture
run cargo test -p curvine-client p2p::service::tests::persisted_policy_version_rejects_stale_master_version_after_restart -- --nocapture
run cargo test -p curvine-server master::master_metrics::tests::histogram_metric_should_reuse_existing_counter_vec_as_count_delta -- --nocapture
run cargo test -p curvine-tests --test p2p_read_acceleration_test test_minicluster_p2p_runtime_policy_sync_from_master -- --exact --nocapture
run cargo test -p curvine-tests --test p2p_read_acceleration_test test_minicluster_p2p_runtime_policy_dual_key_rotation_window -- --exact --nocapture
run cargo test -p curvine-tests --test p2p_read_acceleration_test test_minicluster_p2p_runtime_policy_signature_mismatch_rejected -- --exact --nocapture
run cargo test -p curvine-tests --test p2p_read_acceleration_test test_minicluster_p2p_miss_fallbacks_to_worker -- --exact --nocapture
run cargo test -p curvine-tests --test p2p_read_acceleration_test test_minicluster_p2p_miss_fails_when_fallback_disabled -- --exact --nocapture

echo "[p2p-gate] all required checks passed"
