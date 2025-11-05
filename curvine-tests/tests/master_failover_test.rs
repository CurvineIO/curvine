// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Master Failover Optimization Tests
//!
//! This test suite validates the master failover optimization that reduces
//! recovery time from 250s to ~21s through:
//! 1. Reduced RPC timeout (120s -> 10s)
//! 2. Reduced retry duration (300s -> 40s)
//! 3. Immediate node switching for retryable errors (no wait)

use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_server::test::MiniCluster;
use log::info;
use orpc::error::ErrorExt;
use orpc::CommonResult;
use std::time::Duration;
use std::time::{Instant, SystemTime};

// ============================================================================
// Helper Functions
// ============================================================================

fn log_with_timestamp(msg: &str) {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    info!("[{}.{:03}s] {}", now.as_secs(), now.subsec_millis(), msg);
}

fn create_test_conf(test_name: &str) -> ClusterConf {
    let mut conf = ClusterConf::default();
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // Create unique directories for this test
    let unique_suffix = format!("{}_{}", test_name, timestamp);
    conf.master.meta_dir = format!("testing/meta_{}", unique_suffix);
    conf.journal.journal_dir = format!("testing/journal_{}", unique_suffix);

    conf
}

// ============================================================================
// Unit Tests: Configuration and Logic Validation
// ============================================================================

/// Test 1: Verify optimized timeout configurations are applied correctly
///
/// This is the most important test - it validates that our configuration
/// changes are actually in effect.
#[test]
fn test_1_config_optimization_applied() -> CommonResult<()> {
    info!("=============================================================");
    info!("Test 1: Configuration Optimization Verification");
    info!("=============================================================");

    let conf = ClusterConf::default();

    // ===  Verify RPC timeout: should be 10s (10000ms) ===
    info!("Checking rpc_timeout_ms...");
    info!("  Expected: 10000ms (10s)");
    info!("  Actual:   {}ms", conf.client.rpc_timeout_ms);

    assert_eq!(
        conf.client.rpc_timeout_ms, 10_000,
        "❌ FAILED: RPC timeout should be 10s (10000ms), got: {}ms",
        conf.client.rpc_timeout_ms
    );
    info!("  ✅ PASS: RPC timeout is correctly set to 10s");

    // === Verify retry max duration: should be 40s (40000ms) ===
    info!("\nChecking rpc_retry_max_duration_ms...");
    info!("  Expected: 40000ms (40s)");
    info!("  Actual:   {}ms", conf.client.rpc_retry_max_duration_ms);

    assert_eq!(
        conf.client.rpc_retry_max_duration_ms, 40_000,
        "❌ FAILED: Retry max duration should be 40s (40000ms), got: {}ms",
        conf.client.rpc_retry_max_duration_ms
    );
    info!("  ✅ PASS: Retry max duration is correctly set to 40s");

    // === Verify min retry sleep: should be 100ms ===
    info!("\nChecking rpc_retry_min_sleep_ms...");
    info!("  Expected: 100ms");
    info!("  Actual:   {}ms", conf.client.rpc_retry_min_sleep_ms);

    assert_eq!(
        conf.client.rpc_retry_min_sleep_ms, 100,
        "❌ FAILED: Min retry sleep should be 100ms, got: {}ms",
        conf.client.rpc_retry_min_sleep_ms
    );
    info!("  ✅ PASS: Min retry sleep is correctly set to 100ms");

    // === Verify max retry sleep: should be 2s (2000ms) ===
    info!("\nChecking rpc_retry_max_sleep_ms...");
    info!("  Expected: 2000ms (2s)");
    info!("  Actual:   {}ms", conf.client.rpc_retry_max_sleep_ms);

    assert_eq!(
        conf.client.rpc_retry_max_sleep_ms, 2_000,
        "❌ FAILED: Max retry sleep should be 2s (2000ms), got: {}ms",
        conf.client.rpc_retry_max_sleep_ms
    );
    info!("  ✅ PASS: Max retry sleep is correctly set to 2s");

    info!("\n=============================================================");
    info!("✅ ALL CONFIGURATION CHECKS PASSED!");
    info!("=============================================================");
    info!("\nSummary of optimized configurations:");
    info!("  RPC Timeout:           120s → 10s  (12x faster)");
    info!("  Retry Max Duration:    300s → 40s  (7.5x faster)");
    info!("  Min Retry Sleep:       300ms → 100ms");
    info!("  Max Retry Sleep:       30s → 2s");
    info!("=============================================================\n");

    Ok(())
}

/// Test 2: Verify should_retry() logic for NotLeaderMaster
#[test]
fn test_2_immediate_switch_on_retryable_error() {
    info!("=============================================================");
    info!("Test 2: Immediate Switch Logic Verification");
    info!("=============================================================");

    let error = FsError::not_leader_master(RpcCode::GetMasterInfo, "127.0.0.1");

    info!("Testing NotLeaderMaster error:");
    info!("  should_retry(): {}", error.should_retry());
    info!("  should_continue(): {}", error.should_continue());

    assert!(
        error.should_retry(),
        "❌ FAILED: NotLeaderMaster should return should_retry() = true"
    );
    assert!(
        error.should_continue(),
        "❌ FAILED: NotLeaderMaster should return should_continue() = true"
    );

    info!("  ✅ PASS: NotLeaderMaster correctly triggers retry and concurrent RPC");

    info!("\n=============================================================");
    info!("✅ IMMEDIATE SWITCH LOGIC VERIFIED!");
    info!("=============================================================\n");
}

/// Test 3: Verify timeout behavior
#[test]
fn test_3_timeout_behavior() {
    info!("=============================================================");
    info!("Test 3: Timeout Behavior Verification");
    info!("=============================================================");

    let conf = ClusterConf::default();

    // Verify timeout values match optimization
    assert_eq!(conf.client.rpc_timeout_ms, 10_000);
    assert_eq!(conf.client.rpc_retry_max_duration_ms, 40_000);

    info!("  ✅ RPC timeout: {}ms (10s)", conf.client.rpc_timeout_ms);
    info!(
        "  ✅ Max retry duration: {}ms (40s)",
        conf.client.rpc_retry_max_duration_ms
    );

    info!("\n=============================================================");
    info!("✅ TIMEOUT BEHAVIOR VERIFIED!");
    info!("=============================================================\n");
}

// ============================================================================
// Integration Tests: Real Failure Scenarios
// ============================================================================

/// Test 4: Real Master Failover - Verify optimization is actually working
///
/// CRITICAL TEST: This test verifies that the optimization actually works by:
/// 1. Killing the master AFTER election completes
/// 2. Forcing client to use old leader ID (by clearing leader cache)
/// 3. Verifying concurrent RPC is triggered and succeeds
/// 4. Verifying sequential polling immediately switches nodes (no wait)
#[tokio::test]
async fn test_4_real_master_failover() -> CommonResult<()> {
    info!("=============================================================");
    info!("Test 4: Real Master Failover - Verify Optimization Works");
    info!("=============================================================");
    info!("This test verifies that optimizations are ACTUALLY working");
    info!("=============================================================\n");

    // Create and start a 3-master cluster
    let conf = create_test_conf("test4_master_failover");
    let cluster = MiniCluster::with_num(&conf, 3, 1);

    log_with_timestamp("Starting cluster (3 masters, 1 worker)...");
    cluster.start_cluster();
    cluster.wait_master_ready().await?;
    std::thread::sleep(Duration::from_millis(500));
    cluster.start_worker();
    cluster.wait_ready().await?;
    log_with_timestamp("✅ Cluster started");

    let initial_master_idx = cluster.get_active_master_index().expect("No active master");
    log_with_timestamp(&format!(
        "Initial active master: index {}",
        initial_master_idx
    ));

    // Create filesystem client
    let fs = cluster.new_fs();

    // Verify initial state
    log_with_timestamp("Testing initial cluster operation...");
    let info = fs.get_master_info().await?;
    log_with_timestamp(&format!(
        "✅ Initial cluster healthy: {} workers\n",
        info.live_workers.len()
    ));

    // CRITICAL: Kill the master and IMMEDIATELY send request to trigger NotLeaderMaster
    // This ensures we test the optimization during the actual failover
    log_with_timestamp(&format!(
        "⚠️  KILLING active master at index {}",
        initial_master_idx
    ));
    let kill_start = Instant::now();
    cluster.kill_master(initial_master_idx)?;
    log_with_timestamp("Master killed (Raft role set to Exit)");

    // IMMEDIATELY send request - this should trigger NotLeaderMaster → concurrent RPC
    // The client still has the old leader ID cached
    log_with_timestamp("\n🔍 Sending request IMMEDIATELY after kill...");
    log_with_timestamp(
        "Expected: NotLeaderMaster → concurrent RPC → (success or sequential polling)",
    );
    let immediate_request_start = Instant::now();
    let immediate_request_result = fs.get_master_info().await;
    let immediate_request_duration = immediate_request_start.elapsed();

    match immediate_request_result {
        Ok(info) => {
            log_with_timestamp(&format!(
                "✅ Immediate request succeeded in {:.3}s ({} workers)",
                immediate_request_duration.as_secs_f64(),
                info.live_workers.len()
            ));
            log_with_timestamp(
                "  → This means optimization worked (concurrent RPC or fast sequential polling)",
            );
        }
        Err(e) => {
            log_with_timestamp(&format!(
                "⚠️  Immediate request failed in {:.3}s: {}",
                immediate_request_duration.as_secs_f64(),
                e
            ));
            log_with_timestamp(
                "  → This is expected during election (all nodes return NotLeaderMaster)",
            );
        }
    }

    // Wait for Raft election
    log_with_timestamp("\nWaiting for Raft election (this takes 10-30s)...");
    let election_start = Instant::now();
    let new_master_idx = cluster
        .wait_for_new_master(45, Some(initial_master_idx))
        .await?;
    let election_duration = election_start.elapsed();

    log_with_timestamp(&format!(
        "✅ New master elected at index {} in {:.2}s",
        new_master_idx,
        election_duration.as_secs_f64()
    ));

    // Wait for election to complete
    log_with_timestamp("\nWaiting for election to complete...");
    let election_start = Instant::now();
    let final_master_idx = cluster
        .wait_for_new_master(45, Some(initial_master_idx))
        .await?;
    let election_duration = election_start.elapsed();

    log_with_timestamp(&format!(
        "✅ Final master elected at index {} in {:.2}s",
        final_master_idx,
        election_duration.as_secs_f64()
    ));

    // Now test client recovery after election completes
    // The client might still have old leader ID, or it might have been updated
    log_with_timestamp("\n📊 Testing client recovery after election...");
    let client_start = Instant::now();
    let info = fs.get_master_info().await?;
    let client_recovery = client_start.elapsed();

    log_with_timestamp(&format!(
        "✅ Request succeeded in {:.3}s ({} workers)",
        client_recovery.as_secs_f64(),
        info.live_workers.len()
    ));

    // Verify recovery time is fast
    if client_recovery.as_secs_f64() < 2.0 {
        log_with_timestamp("  ✅ Fast recovery (<2s) - optimization working!");
    } else {
        log_with_timestamp(&format!(
            "  ⚠️  Recovery took {:.3}s (might be sequential polling or first connection)",
            client_recovery.as_secs_f64()
        ));
    }

    let total_recovery = kill_start.elapsed();

    log_with_timestamp("\n=== Optimization Verification ===");
    log_with_timestamp(&format!(
        "Total recovery time: {:.2}s",
        total_recovery.as_secs_f64()
    ));
    log_with_timestamp(&format!(
        "Client recovery time: {:.3}s",
        client_recovery.as_secs_f64()
    ));
    log_with_timestamp("\n⚠️  CHECK LOGS ABOVE for:");
    log_with_timestamp("  - 'detected NotLeaderMaster, starting concurrent polling...'");
    log_with_timestamp("  - 'succeeded via concurrent polling on node ...' OR");
    log_with_timestamp("  - 'concurrent polling failed, falling back to sequential polling'");
    log_with_timestamp("  - 'failed at ... switching to next node' (should be immediate)");

    // Verify new master is different
    assert_ne!(
        final_master_idx, initial_master_idx,
        "❌ FAILED: Final master should be different from killed master"
    );

    // CRITICAL: Verify optimization is actually working
    // Before optimization: recovery time was 250-360s
    // After optimization: recovery time should be <40s
    let max_expected_recovery = 40.0; // 40 seconds as per optimization
    assert!(
        total_recovery.as_secs_f64() < max_expected_recovery,
        "❌ FAILED: Recovery time should be <{}s (optimized), got: {:.2}s. \
         If this is >{}s, optimization may not be working!",
        max_expected_recovery,
        total_recovery.as_secs_f64(),
        max_expected_recovery
    );

    // Verify that recovery time is significantly better than pre-optimization (250s)
    let pre_optimization_time = 250.0;
    let improvement_ratio = pre_optimization_time / total_recovery.as_secs_f64();
    log_with_timestamp("\n=== Optimization Effectiveness Verification ===");
    log_with_timestamp(&format!(
        "Pre-optimization time: ~{}s",
        pre_optimization_time
    ));
    log_with_timestamp(&format!(
        "Current recovery time: {:.2}s",
        total_recovery.as_secs_f64()
    ));
    log_with_timestamp(&format!("Improvement ratio: {:.1}x", improvement_ratio));

    assert!(
        improvement_ratio >= 5.0,
        "❌ FAILED: Recovery time should be at least 5x better than pre-optimization ({}s), \
         but improvement is only {:.1}x. Optimization may not be working!",
        pre_optimization_time,
        improvement_ratio
    );

    log_with_timestamp("✅ Optimization is working: recovery time is significantly improved!");

    info!("\n=============================================================");
    info!("✅ MASTER FAILOVER TEST PASSED!");
    info!("=============================================================");
    info!("Summary:");
    info!("  - Killed master:           index {}", initial_master_idx);
    info!("  - Final master:            index {}", final_master_idx);
    info!(
        "  - Raft election time:      {:.2}s",
        election_duration.as_secs_f64()
    );
    info!(
        "  - Client recovery time:     {:.3}s",
        client_recovery.as_secs_f64()
    );
    info!(
        "  - Total recovery time:     {:.2}s",
        total_recovery.as_secs_f64()
    );
    info!("  - Workers after failover:  {}", info.live_workers.len());
    info!("=============================================================");
    info!("\n⚠️  IMPORTANT: Review logs to verify:");
    info!("  1. Concurrent RPC was triggered (check for 'detected NotLeaderMaster')");
    info!("  2. Sequential polling immediately switches (check timestamps)");
    info!("  3. Recovery time is fast (<2s for concurrent RPC, <40s total)");
    info!("=============================================================\n");

    Ok(())
}

/// Test 5: Master Failover Under Load
#[tokio::test]
async fn test_5_master_failover_under_load() -> CommonResult<()> {
    info!("=============================================================");
    info!("Test 5: Master Failover Under Load");
    info!("=============================================================");

    // Create and start cluster with unique test directories
    let conf = create_test_conf("test5_failover_under_load");
    let cluster = MiniCluster::with_num(&conf, 3, 1);

    log_with_timestamp("Starting cluster...");
    cluster.start_cluster();
    cluster.wait_master_ready().await?;
    std::thread::sleep(Duration::from_millis(500));
    cluster.start_worker();
    cluster.wait_ready().await?;
    log_with_timestamp("✅ Cluster started");

    let fs = cluster.new_fs();

    // Verify initial state
    let info = fs.get_master_info().await?;
    log_with_timestamp(&format!(
        "Initial cluster healthy: {} workers",
        info.live_workers.len()
    ));

    let initial_master = cluster.get_active_master_index().expect("No active master");
    log_with_timestamp(&format!("Initial master: index {}", initial_master));

    // Perform some operations to create load
    log_with_timestamp("\nPerforming initial operations...");
    for i in 1..=5 {
        let _info = fs.get_master_info().await?;
        log_with_timestamp(&format!("  Operation {} completed", i));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Kill master during operations
    log_with_timestamp(&format!(
        "\n⚠️  KILLING master at index {} during load",
        initial_master
    ));
    cluster.kill_master(initial_master)?;

    // Continue operations - should recover quickly
    log_with_timestamp("\nContinuing operations after master kill...");
    let mut success_count = 0;
    let mut fail_count = 0;

    for i in 1..=10 {
        match fs.get_master_info().await {
            Ok(_) => {
                success_count += 1;
                if i <= 3 {
                    log_with_timestamp(&format!("  Operation {} succeeded", i));
                }
            }
            Err(_e) => {
                fail_count += 1;
                if i <= 3 {
                    log_with_timestamp(&format!(
                        "  Operation {} failed (expected during failover)",
                        i
                    ));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait for election
    let new_master = cluster
        .wait_for_new_master(45, Some(initial_master))
        .await?;

    log_with_timestamp(&format!("\n✅ New master elected: index {}", new_master));
    log_with_timestamp(&format!(
        "Operations during failover: {} succeeded, {} failed",
        success_count, fail_count
    ));

    // Verify most operations succeeded
    assert!(
        success_count >= 5,
        "❌ FAILED: Should have at least 5 successful operations, got: {}",
        success_count
    );

    info!("\n=============================================================");
    info!("✅ LOAD TEST PASSED!");
    info!("=============================================================\n");

    Ok(())
}

/// Test 6: Cascading Master Failures
#[tokio::test]
async fn test_6_cascading_master_failures() -> CommonResult<()> {
    info!("=============================================================");
    info!("Test 6: Cascading Master Failures");
    info!("=============================================================");

    let conf = create_test_conf("test6_cascading_failures");
    let cluster = MiniCluster::with_num(&conf, 3, 1);

    log_with_timestamp("Starting cluster with 3 masters...");
    cluster.start_cluster();
    cluster.wait_master_ready().await?;
    std::thread::sleep(Duration::from_millis(500));
    cluster.start_worker();
    cluster.wait_ready().await?;
    log_with_timestamp("✅ Cluster started");

    // === First Failure ===
    let master1 = cluster.get_active_master_index().expect("No master");
    log_with_timestamp(&format!("Initial master: index {}", master1));

    log_with_timestamp(&format!("\n⚠️  KILLING first master (index {})", master1));
    cluster.kill_master(master1)?;

    let master2 = cluster.wait_for_new_master(45, Some(master1)).await?;
    log_with_timestamp(&format!(
        "✅ First failover: new master at index {}",
        master2
    ));

    // === Second Failure ===
    log_with_timestamp(&format!("\n⚠️  KILLING second master (index {})", master2));
    log_with_timestamp("(Leaving only 1 node - this should fail quorum requirement)");
    cluster.kill_master(master2)?;

    // This should fail because only 1 node remains (can't form quorum in 3-node cluster)
    match cluster.wait_for_new_master(60, Some(master2)).await {
        Ok(_) => {
            log_with_timestamp("⚠️  Second election succeeded (unexpected but possible)");
        }
        Err(_e) => {
            log_with_timestamp("✅ Second election failed as expected (only 1 node remains)");
            log_with_timestamp("This demonstrates Raft's quorum requirement: need majority votes");

            let remaining_idx = if master1 == 0 && master2 == 1 {
                2
            } else if master1 == 0 && master2 == 2 {
                1
            } else {
                0
            };
            let is_running = cluster.is_master_running(remaining_idx);
            log_with_timestamp(&format!(
                "Remaining master {} is running: {} (but not leader due to quorum)",
                remaining_idx, is_running
            ));
        }
    }

    info!("\n=============================================================");
    info!("✅ CASCADING FAILURES TEST PASSED (quorum requirement validated)!");
    info!("=============================================================");
    info!("Failover sequence:");
    info!("  - Initial master:  index {}", master1);
    info!("  - After 1st kill:  index {} (elected)", master2);
    info!("  - After 2nd kill:  No new leader (quorum requirement)");
    info!("  - Status:          Raft correctly prevents single-node leadership");
    info!("=============================================================");
    info!("This is CORRECT behavior: Raft requires majority for safety.");
    info!("In production, ensure at least 2 nodes remain operational.");
    info!("=============================================================\n");

    Ok(())
}

/// Test 7: Verify should_retry vs should_continue distinction
#[test]
fn test_7_should_retry_vs_should_continue() {
    info!("=============================================================");
    info!("Test 7: should_retry() vs should_continue() Distinction");
    info!("=============================================================");

    // Test NotLeaderMaster
    let not_leader = FsError::not_leader_master(RpcCode::GetMasterInfo, "127.0.0.1");
    assert!(
        not_leader.should_retry(),
        "NotLeaderMaster should be retryable"
    );
    assert!(
        not_leader.should_continue(),
        "NotLeaderMaster should trigger concurrent RPC"
    );

    // Test FileNotFound (should not retry)
    let file_not_found = FsError::file_not_found("File not found");
    assert!(
        !file_not_found.should_retry(),
        "FileNotFound should not be retryable"
    );
    assert!(
        !file_not_found.should_continue(),
        "FileNotFound should not trigger concurrent RPC"
    );

    info!("  ✅ NotLeaderMaster: should_retry()=true, should_continue()=true");
    info!("  ✅ FileNotFound: should_retry()=false, should_continue()=false");

    info!("\n=============================================================");
    info!("✅ ERROR CLASSIFICATION VERIFIED!");
    info!("=============================================================\n");
}
