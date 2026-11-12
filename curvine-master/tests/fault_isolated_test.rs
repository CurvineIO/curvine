// Copyright 2026 Curvine Authors
// Licensed under the Apache License, Version 2.0 (see LICENSE).
//! Fault-injection isolation harness.
//!
//! The FaultRuntime keeps a process-global rule table, so fault-injection
//! tests interfere with each other when the lib suite runs with
//! `--test-threads 8` (a record() rule counts another test's hook hits and a
//! return_error rule fires on another test's operation). This harness runs
//! each fault test in its own subprocess via `current_exe --exact`, reusing
//! the established isolation pattern from meta/tests/block_lock_metrics_test.
//! The in-process tests keep their real multi-threaded interleavings and
//! assertions; only the process boundary differs.

use std::env;

const ENV_GUARD: &str = "CURVINE_FAULT_ISOLATED";

#[cfg(feature = "fault-injection")]
const FAULT_TESTS: &[&str] = &[
    "master::fs::master_filesystem::tests::complete_file_sharding_test::add_block_by_id_survives_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::add_block_commit_failure_keeps_file_state_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::add_block_does_not_wait_for_a_shared_namespace_reader",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::close_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::free_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::resize_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::set_attr_lock_test::set_lock_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::create_failure_test::link_replans_after_source_parent_replacement",
    "master::fs::master_filesystem::tests::rename_failure_test::rename_replans_after_source_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::block_locations_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::close_file_does_not_wait_for_a_shared_namespace_reader",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::close_file_commit_failure_keeps_writer_state_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::complete_by_id_survives_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::complete_file_commit_failure_keeps_memory_and_store_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::concurrent_overwrites_of_the_same_target_keep_one_consistent_file_version",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::delete_commit_failure_keeps_file_state_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::delete_file_waits_for_conflicting_block_location_lock",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::delete_is_rejected_during_metadata_restore_generation",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::delete_reloads_inode_after_a_hard_link_is_added",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::delete_retries_when_replication_changes_sampled_worker_set",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::delete_serializes_with_worker_location_cleanup",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::expired_delete_commit_failure_keeps_memory_and_store_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::expired_delete_does_not_delete_replaced_inode",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::expired_delete_does_not_wait_for_a_shared_namespace_reader",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::expired_trash_root_keeps_reserved_delete_protection",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::free_file_commit_failure_keeps_memory_and_store_unchanged",
    "master::fs::master_filesystem::tests::multiblock_open_preserves_order_and_filters_current_worker_state",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::overwrite_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::reopen_file_does_not_wait_for_a_shared_namespace_reader",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::reopen_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::truncate_open_commit_failure_keeps_file_state_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::truncate_open_queues_old_worker_blocks_after_commit",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::truncate_open_replans_after_parent_replacement",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::verified_release_does_not_touch_replaced_inode",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::verified_release_commit_failure_keeps_memory_and_store_unchanged",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::verified_release_waits_for_conflicting_worker_location_lock",
    "master::fs::master_filesystem::tests::set_attr_lock_test::recursive_set_attr_preserves_concurrent_hardlink_update",
    "master::fs::master_filesystem::tests::set_attr_lock_test::recursive_set_attr_waits_for_shared_namespace_readers",
    "master::fs::master_filesystem::tests::set_attr_lock_test::single_inode_set_attr_does_not_wait_for_fs_dir_readers",
    "master::fs::master_filesystem::tests::rename_failure_test::file_status_lock_window_excludes_rename_writers",
    "master::fs::master_filesystem::tests::rename_failure_test::same_parent_rename_commit_failure_preserves_tree_and_reloads",
    "master::fs::master_filesystem::tests::rename_failure_test::ttl_free_does_not_release_replacement_of_hard_link_path",
    "master::fs::master_filesystem::tests::cache_invalidation_lock_test::cache_invalidation_waits_for_the_file_inode_read_lock",
    "master::fs::master_filesystem::tests::shared_handles_test::open_missing_path_fallback_excludes_namespace_readers",
    "master::fs::master_filesystem::tests::create_failure_test::create_commit_failure_does_not_publish_to_the_namespace_tree",
    "master::fs::master_filesystem::tests::lost_worker_gate_test::lost_worker_cleanup_does_not_cross_a_closed_commit_gate",
    "master::fs::master_filesystem::tests::fs_mode_release_gate_test::a_verified_release_refuses_a_renamed_file",
    "master::fs::master_filesystem::tests::fs_mode_release_gate_test::a_verified_release_refuses_a_changed_file",
    "master::fs::master_filesystem::tests::complete_file_sharding_test::atime_refresh_does_not_touch_replaced_inode",
];

#[cfg(feature = "fault-injection")]
#[test]
fn fault_tests_run_in_isolated_processes() {
    if env::var(ENV_GUARD).is_ok() {
        // Re-entered inside the isolated child: the lib harness invoked with
        // --exact runs its own tests; nothing to do here.
        return;
    }
    // The isolated children run the lib suite (where the fault tests live).
    // The fault-gated lib binary must be named explicitly: guessing it by
    // mtime can silently pick the default-feature binary, whose run then
    // matches zero tests.
    let lib_exe = std::path::PathBuf::from(
        env::var("CURVINE_LIB_TEST_BIN")
            .expect("CURVINE_LIB_TEST_BIN must point at the fault-injection lib test binary"),
    );
    assert!(
        lib_exe.is_file(),
        "CURVINE_LIB_TEST_BIN is not a file: {}",
        lib_exe.display()
    );
    let log_dir = std::env::temp_dir().join("curvine-fault-isolated");
    std::fs::create_dir_all(&log_dir).expect("create isolated fault log dir");
    let mut failures = Vec::new();
    let mut zero_test_runs = Vec::new();
    let mut wrong_shape = Vec::new();
    for test in FAULT_TESTS {
        let output = std::process::Command::new(&lib_exe)
            .args(["--exact", test, "--nocapture", "--test-threads", "1"])
            .env(ENV_GUARD, "1")
            .output()
            .unwrap_or_else(|e| panic!("failed to spawn isolated run for {test}: {e}"));
        // Persist the child's raw streams so a failure is diagnosable.
        let safe_name = test.replace("::", "_");
        let _ = std::fs::write(log_dir.join(format!("{safe_name}.stdout")), &output.stdout);
        let _ = std::fs::write(log_dir.join(format!("{safe_name}.stderr")), &output.stderr);
        if !output.status.success() {
            failures.push((*test, output.status.code()));
            continue;
        }
        // Exactly one non-ignored test must have executed: the child's own
        // summary line is authoritative.
        // Parse only the libtest summary line so `--nocapture` output from
        // the test itself cannot confuse the counters.
        let stdout_text = String::from_utf8_lossy(&output.stdout);
        let summary = stdout_text
            .lines()
            .rev()
            .find(|line| line.starts_with("test result:"))
            .unwrap_or("");
        let parse_count = |label: &str| -> Option<usize> {
            summary
                .strip_prefix("test result:")
                .and_then(|rest| rest.split_once('.').map(|(_, tail)| tail))
                .and_then(|tail| {
                    tail.split(';').map(str::trim).find_map(|part| {
                        let num = part.strip_suffix(label)?.trim();
                        num.parse::<usize>().ok()
                    })
                })
        };
        let passed = parse_count("passed").unwrap_or(0);
        let failed = parse_count("failed").unwrap_or(usize::MAX);
        let ignored = parse_count("ignored").unwrap_or(0);
        if passed != 1 {
            zero_test_runs.push(*test);
        } else if ignored != 0 {
            wrong_shape.push(*test);
        } else if failed != 0 {
            failures.push((*test, output.status.code()));
        }
    }
    eprintln!(
        "isolated fault harness: {} spawned, {} failures, {} zero-test, {} with-ignored, logs in {}",
        FAULT_TESTS.len(),
        failures.len(),
        zero_test_runs.len(),
        wrong_shape.len(),
        log_dir.display()
    );
    assert!(
        failures.is_empty(),
        "isolated fault tests failed: {failures:?}"
    );
    assert!(
        zero_test_runs.is_empty(),
        "isolated fault runs executed zero tests: {zero_test_runs:?}"
    );
    assert!(
        wrong_shape.is_empty(),
        "isolated fault runs ignored tests: {wrong_shape:?}"
    );
    eprintln!(
        "isolated fault harness: {} tests spawned, {} failures",
        FAULT_TESTS.len(),
        failures.len()
    );
}
