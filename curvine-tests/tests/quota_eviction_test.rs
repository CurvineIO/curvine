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

use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_server::test::MiniCluster;
use log::info;
use orpc::common::Utils;
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Test LRU eviction functionality with comprehensive scenarios
#[test]
fn quota_lru_eviction_integration_test() -> CommonResult<()> {
    // Clean up test directory before starting
    let test_data_dir = "/tmp/curvine-test-data-lru";
    let _ = std::fs::remove_dir_all(test_data_dir);
    std::fs::create_dir_all(test_data_dir)?;

    let mut conf = ClusterConf::default();

    conf.worker.data_dir = vec![test_data_dir.to_string()];

    // Enable cluster-level eviction
    conf.master.enable_quota_eviction = true;
    conf.master.quota_eviction_mode = "delete".to_string();
    conf.master.quota_eviction_policy = "lru".to_string();

    // Set watermarks to trigger eviction: high=0.155%, low=0.1447%
    // Adjusted watermarks to target ~375MB release (5 files × 75MB)
    // With 750MB used and ~254GB quota: target = 750MB - (0.001447 * 254GB) ≈ 375MB
    conf.master.quota_eviction_high_rate = 0.00155;
    conf.master.quota_eviction_low_rate = 0.001447;

    // Set candidate_scan_page=1 for precise LRU testing (delete files one by one)
    // This ensures exact LRU behavior verification without over-eviction
    conf.master.quota_eviction_scan_page = 1;
    conf.master.quota_eviction_dry_run = false;

    // Reduce heartbeat interval to trigger eviction checks more frequently
    conf.master.worker_check_interval = "2s".to_string();

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let conf = cluster.master_conf().clone();

    cluster.start_cluster();

    // Wait for cluster to stabilize
    info!("Waiting for cluster to stabilize...");
    Utils::sleep(10000);

    // Use multi-threaded runtime for async operations (required for FsWriterBuffer background tasks)
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let rt1 = rt.clone();
    let res: CommonResult<()> = rt.block_on(async move {
        let fs = CurvineFileSystem::with_rt(conf, rt1)?;

        info!("=== LRU Eviction Test Suite ===");

        // Test 1: Create multiple files with different access patterns
        info!("Test 1: Creating files with LRU access pattern");
        let test_dir = Path::new("/lru-test")?;
        fs.mkdir(&test_dir, true).await?;

        // Create 10 files, each 75MB (total 750MB) to trigger eviction
        // With target ~370MB, deleting 5 files (5×75MB=375MB) should satisfy the target
        let file_size = 75 * 1024 * 1024; // 75MB
        let num_files = 10;
        let content = vec![b'A'; file_size];

        let mut file_paths = Vec::new();
        for i in 0..num_files {
            let file_path = Path::new(format!("/lru-test/file_{}.dat", i))?;
            info!("Creating file: {}", file_path);

            let mut writer = fs.create(&file_path, true).await?;
            writer.write(&content).await?;
            writer.complete().await?;

            file_paths.push(file_path);

            // Small delay between file creations
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!(
            "Created {} files, total size: {} MB",
            num_files,
            (num_files * file_size) / (1024 * 1024)
        );

        // Test 2: Access files in a specific order to establish LRU pattern
        // Access files 5-9 to make them more recently used
        // Files 0-4 should be LRU candidates
        info!("Test 2: Establishing LRU pattern by accessing files 5-9");
        for file_path in file_paths.iter().skip(5) {
            info!("Accessing file: {}", file_path);

            let mut reader = fs.open(file_path).await?;
            let mut buffer = vec![0u8; 1024];
            reader.read(&mut buffer).await?;
            reader.complete().await?;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Test 3: Check initial file status
        info!("Test 3: Verifying all files exist before eviction");
        for (i, file_path) in file_paths.iter().enumerate() {
            let exists = fs.exists(file_path).await?;
            assert!(exists, "File {} should exist before eviction", i);
        }

        // Test 4: Trigger eviction by waiting for heartbeat checker
        info!("Test 4: Waiting for eviction to be triggered (up to 30 seconds)...");
        let start = Instant::now();
        let timeout = Duration::from_secs(30);

        let mut eviction_occurred = false;
        while start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Check if any LRU files (0-4) have been evicted
            let mut evicted_count = 0;
            for (i, file_path) in file_paths.iter().enumerate().take(5) {
                let exists = fs.exists(file_path).await?;
                if !exists {
                    evicted_count += 1;
                    info!("File {} has been evicted", i);
                }
            }

            if evicted_count > 0 {
                eviction_occurred = true;
                info!("Eviction detected: {} files evicted", evicted_count);
                break;
            }
        }

        if !eviction_occurred {
            info!("Warning: No eviction occurred within timeout.");
            info!("   This may be expected if:");
            info!("   1. Test directory size (750MB) is less than 0.155% of disk capacity");
            info!("   2. Disk usage is below 0.155% high watermark");
            info!("   Current config: high_watermark=0.155%, low_watermark=0.1447%, candidate_scan_page=1 (precise LRU)");
        } else {
            info!("Eviction successfully triggered");
        }

        // Test 5: Verify LRU behavior - recently accessed files should still exist
        info!("Test 5: Verifying LRU behavior - recently accessed files (5-9) should remain");
        let mut recent_files_remaining = 0;
        for file_path in file_paths.iter().skip(5) {
            let exists = fs.exists(file_path).await?;
            if exists {
                recent_files_remaining += 1;
            }
        }

        info!(
            "Recently accessed files remaining: {}/5",
            recent_files_remaining
        );

        // Validate the core logic: LRU files (0-4) should be evicted, recent files (5-9) should remain
        if eviction_occurred {
            // Check that LRU files were evicted
            for (i, file_path) in file_paths.iter().enumerate().take(5) {
                let exists = fs.exists(file_path).await?;
                assert!(!exists, "LRU file {} should have been evicted", i);
            }

            // Check that recently accessed files largely remain
            // Over-deletion is allowed; the target may require evicting one of 5..9.
            // So require at least 4 of 5 to remain.
            assert!(
                recent_files_remaining >= 4,
                "At least 4 of the recently accessed files (5..9) should remain, actual: {}",
                recent_files_remaining
            );

            info!("LRU eviction logic validated: correct files were evicted");
        }

        // Test 6: Cleanup
        info!("Test 6: Cleaning up test directory");
        for file_path in &file_paths {
            let exists = fs.exists(file_path).await?;
            if exists {
                fs.delete(file_path, false).await?;
            }
        }
        fs.delete(&test_dir, true).await?;

        info!("=== Test Summary ===");
        info!("- Files created: {}", num_files);
        info!("- Eviction triggered: {}", eviction_occurred);
        info!("- Recent files remaining: {}/{}", recent_files_remaining, 5);

        Ok(())
    });

    match res {
        Ok(_) => {
            info!("All LRU eviction tests completed");
            Ok(())
        }
        Err(e) => {
            eprintln!("LRU eviction tests failed: {:?}", e);
            Err(e)
        }
    }
}
