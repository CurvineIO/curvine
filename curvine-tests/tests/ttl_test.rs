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

use curvine_client::file::{FsClient, FsContext};
use curvine_common::conf::ClientConf;
use curvine_common::fs::Path;
use curvine_common::state::{CreateFileOptsBuilder, MkdirOptsBuilder, StoragePolicy, TtlAction};
use curvine_tests::Testing;
use log::info;
use orpc::common::{DurationUnit, LogConf, Logger, Utils};
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// TTL functionality test
///
/// Test scenarios:
/// 1. Create files with TTL and verify automatic deletion
/// 2. Create directories with TTL and verify automatic deletion
/// 3. Test TTL cleanup timing and effectiveness
#[test]
fn test_ttl_cleanup() -> CommonResult<()> {
    Logger::init(LogConf::default());

    info!("Starting TTL cleanup test");
    // Build and start test cluster via Testing builder
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            // Update both string and unit fields to ensure configuration is applied correctly
            conf.master.ttl_checker_interval = "1s".to_string();
            conf.master.ttl_checker_interval_unit = DurationUnit::from_str("1s").unwrap();
            conf.master.ttl_bucket_interval = "1s".to_string();
            conf.master.ttl_bucket_interval_unit = DurationUnit::from_str("1s").unwrap();
            conf.master.ttl_checker_retry_attempts = 1;
        })
        .build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    println!("conf: {:?}", conf);
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs_context = Arc::new(FsContext::with_rt(conf.clone(), rt.clone())?);
    let client = FsClient::new(fs_context);

    // Execute async code in runtime
    rt.block_on(async move {
        // Test 1: Verify file exists before TTL expiration (negative test)
        test_ttl_file_not_expired_before_time(&client, &conf.client).await?;

        // Test 2: Create files with TTL
        test_ttl_file_cleanup(&client, &conf.client).await?;

        // Test 3: Create directories with TTL
        test_ttl_directory_cleanup(&client, &conf.client).await?;

        // Test 4: Test TTL cleanup timing
        test_ttl_cleanup_timing(&client, &conf.client).await?;

        // Test 5: Test multiple files cleanupFix TTL test failures and improve test coverage
        test_ttl_multiple_files_cleanup(&client, &conf.client).await?;

        info!("Inode ttl cleanup test completed successfully");
        Ok(())
    })
}

/// Test that file exists before TTL expiration (negative test)
async fn test_ttl_file_not_expired_before_time(
    fs: &FsClient,
    conf: &ClientConf,
) -> CommonResult<()> {
    info!("Testing TTL file not expired before time");

    let file_path = Path::from("/test_ttl_file_not_expired.txt");
    let ttl_ms = 5000; // 5 seconds TTL

    let opts = CreateFileOptsBuilder::with_conf(conf)
        .create_parent(true)
        .ttl_ms(ttl_ms)
        .ttl_action(TtlAction::Delete)
        .build();

    // Create file
    let _file = fs.create_with_opts(&file_path, opts, true).await?;
    info!("Created file with TTL: {}", file_path);

    // Verify file exists immediately
    assert!(
        fs.exists(&file_path).await?,
        "File should exist immediately after creation"
    );

    // Wait for half of TTL duration
    tokio::time::sleep(Duration::from_millis((ttl_ms / 2) as u64)).await;

    // Verify file still exists (should not be deleted before TTL expiration)
    assert!(
        fs.exists(&file_path).await?,
        "File should still exist before TTL expiration"
    );
    info!("File correctly exists before TTL expiration");

    // Clean up manually for this test
    let _ = fs.delete(&file_path, true, Utils::req_id()).await;

    Ok(())
}

/// Test TTL file cleanup
async fn test_ttl_file_cleanup(fs: &FsClient, conf: &ClientConf) -> CommonResult<()> {
    info!("Testing TTL file cleanup");

    let file_path = Path::from("/test_ttl_file.txt");
    let ttl_ms = 3000; // 3 seconds TTL

    let opts = CreateFileOptsBuilder::with_conf(conf)
        .create_parent(true)
        .ttl_ms(ttl_ms)
        .ttl_action(TtlAction::Delete)
        .build();
    // Create file
    let _file = fs.create_with_opts(&file_path, opts, true).await?;
    info!("Created file with TTL: {}", file_path);

    // Verify file exists
    assert!(fs.exists(&file_path).await?);
    info!("File exists before TTL expiration");

    // Wait for TTL expiration using awaitility
    // Note: Due to bucket-based cleanup, file may be cleaned up later than TTL expiration
    // - TTL = 3000ms, bucket_interval = 1000ms
    // - File expires at T+3000ms, but bucket may expire at T+4000ms (worst case)
    // - TTL checker runs every 1000ms, so cleanup may happen at T+4000ms to T+6000ms
    info!("Waiting for TTL expiration...");
    awaitility::at_most(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(100))
        .until_async(|| async {
            let exists = fs.exists(&file_path).await.unwrap_or(true);
            !exists
        })
        .await;

    // Verify file has been deleted
    let exists = fs.exists(&file_path).await?;
    assert!(!exists, "File should be deleted after TTL expiration");
    info!("File successfully deleted after TTL expiration");

    Ok(())
}

/// Test TTL directory cleanup
async fn test_ttl_directory_cleanup(fs: &FsClient, conf: &ClientConf) -> CommonResult<()> {
    info!("Testing TTL directory cleanup");

    let dir_path = Path::from("/test_ttl_dir");
    let ttl_ms = 3000; // 3 seconds TTL

    let opts = MkdirOptsBuilder::with_conf(conf)
        .create_parent(true)
        .ttl_ms(ttl_ms)
        .ttl_action(TtlAction::Delete)
        .build();

    // Create directory
    fs.mkdir(&dir_path, opts).await?;
    info!("Created directory with TTL: {}", dir_path);

    // Verify directory exists
    assert!(fs.exists(&dir_path).await?);
    info!("Directory exists before TTL expiration");

    // Wait for TTL expiration using awaitility
    // Note: Due to bucket-based cleanup, directory may be cleaned up later than TTL expiration
    info!("Waiting for TTL expiration...");
    awaitility::at_most(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(100))
        .until_async(|| async {
            let exists = fs.exists(&dir_path).await.unwrap_or(true);
            !exists
        })
        .await;

    // Verify directory has been deleted
    let exists = fs.exists(&dir_path).await?;
    assert!(!exists, "Directory should be deleted after TTL expiration");
    info!("Directory successfully deleted after TTL expiration");

    Ok(())
}

/// Test TTL cleanup timing
async fn test_ttl_cleanup_timing(fs: &FsClient, conf: &ClientConf) -> CommonResult<()> {
    info!("Testing TTL cleanup timing");

    let file_path = Path::from("/test_ttl_timing.txt");
    let ttl_ms = 2000; // 2 seconds TTL

    let opts = CreateFileOptsBuilder::with_conf(conf)
        .create_parent(true)
        .ttl_ms(ttl_ms)
        .ttl_action(TtlAction::Delete)
        .build();

    // Record creation time
    let start_time = SystemTime::now();

    // Create file
    let _file = fs.create_with_opts(&file_path, opts, true).await?;
    info!("Created file with TTL: {}", file_path);

    // Verify file exists
    assert!(fs.exists(&file_path).await?);

    // Wait for TTL expiration using awaitility
    // Note: Due to bucket-based cleanup, file may be cleaned up later than TTL expiration
    info!("Waiting for TTL expiration...");
    awaitility::at_most(Duration::from_secs(10))
        .poll_interval(Duration::from_millis(100))
        .until_async(|| async {
            let exists = fs.exists(&file_path).await.unwrap_or(true);
            !exists
        })
        .await;

    // Verify file has been deleted
    let exists = fs.exists(&file_path).await?;
    assert!(!exists, "File should be deleted after TTL expiration");
    info!("File successfully deleted after TTL expiration");

    // Calculate actual cleanup time
    let cleanup_time = SystemTime::now().duration_since(start_time).unwrap();
    info!("Inode ttl cleanup took: {:?}", cleanup_time);

    // Verify cleanup time is within reasonable range
    // - TTL = 2000ms, bucket_interval = 1000ms
    // - File expires at T+2000ms, but bucket may expire at T+3000ms (worst case)
    // - TTL checker runs every 1000ms, so cleanup may happen at T+3000ms to T+5000ms
    // - Adding buffer for system delays, allow up to 10 seconds
    assert!(
        cleanup_time >= Duration::from_secs(2),
        "Cleanup should take at least 2 seconds"
    );
    assert!(
        cleanup_time <= Duration::from_secs(10),
        "Cleanup should not take more than 10 seconds"
    );

    Ok(())
}

/// Test multiple files cleanup with TTL
async fn test_ttl_multiple_files_cleanup(fs: &FsClient, conf: &ClientConf) -> CommonResult<()> {
    info!("Testing TTL multiple files cleanup");

    let ttl_ms = 3000; // 3 seconds TTL
    let file_count = 5;
    let mut file_paths = Vec::new();

    // Create multiple files with same TTL
    for i in 0..file_count {
        let file_path = Path::from(format!("/test_ttl_multi_{}.txt", i).as_str());
        file_paths.push(file_path.clone());

        let opts = CreateFileOptsBuilder::with_conf(conf)
            .create_parent(true)
            .ttl_ms(ttl_ms)
            .ttl_action(TtlAction::Delete)
            .build();

        let _file = fs.create_with_opts(&file_path, opts, true).await?;
        info!("Created file {} with TTL: {}", i, file_path);
    }

    // Verify all files exist
    for (i, file_path) in file_paths.iter().enumerate() {
        assert!(
            fs.exists(file_path).await?,
            "File {} should exist before TTL expiration",
            i
        );
    }
    info!("All {} files exist before TTL expiration", file_count);

    // Wait for TTL expiration
    info!("Waiting for TTL expiration...");
    for (i, file_path) in file_paths.iter().enumerate() {
        awaitility::at_most(Duration::from_secs(10))
            .poll_interval(Duration::from_millis(100))
            .until_async(|| async {
                let exists = fs.exists(file_path).await.unwrap_or(true);
                !exists
            })
            .await;
        info!("File {} deleted after TTL expiration", i);
    }

    // Verify all files have been deleted
    for (i, file_path) in file_paths.iter().enumerate() {
        let exists = fs.exists(file_path).await?;
        assert!(!exists, "File {} should be deleted after TTL expiration", i);
    }
    info!(
        "All {} files successfully deleted after TTL expiration",
        file_count
    );

    Ok(())
}

/// Test TTL configuration validation
#[test]
fn test_ttl_config_validation() -> CommonResult<()> {
    info!("Testing TTL configuration validation");

    // Test valid TTL configuration
    let policy = StoragePolicy {
        ttl_ms: 3600000, // 1 hour
        ttl_action: TtlAction::Delete,
        ..Default::default()
    };

    assert!(policy.ttl_ms > 0, "TTL should be positive");
    assert_ne!(
        policy.ttl_action,
        TtlAction::None,
        "TTL action should be set"
    );

    // Test invalid TTL configuration
    let invalid_policy = StoragePolicy {
        ttl_ms: 0,
        ttl_action: TtlAction::None,
        ..Default::default()
    };

    assert_eq!(invalid_policy.ttl_ms, 0, "Invalid TTL should be 0");
    assert_eq!(
        invalid_policy.ttl_action,
        TtlAction::None,
        "Invalid TTL action should be None"
    );

    info!("Inode ttl configuration validation test passed");
    Ok(())
}

/// Test TTL bucket interval impact
#[test]
fn test_ttl_bucket_interval_impact() -> CommonResult<()> {
    info!("Testing TTL bucket interval impact");

    // Test impact of 1 second bucket interval
    let bucket_interval_ms = 1000; // 1 second

    // Simulate files with different expiration times
    // Test cases: expiration_offset_ms
    // The bucket_start_ms is calculated by rounding down expiration_time_ms to bucket boundary
    let test_cases = vec![1000, 1500, 2000, 2500];

    for expiration_offset_ms in test_cases {
        let current_time_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let expiration_time_ms = current_time_ms + expiration_offset_ms;
        let bucket_start_ms = (expiration_time_ms / bucket_interval_ms) * bucket_interval_ms;

        println!("bucket_start_ms: {:?}, expiration_time_ms: {:?}, current_time_ms: {:?}, expiration_offset_ms: {:?}", bucket_start_ms, expiration_time_ms, current_time_ms, expiration_offset_ms);

        // Verify bucket_start_ms is aligned to bucket boundary
        assert_eq!(
            bucket_start_ms % bucket_interval_ms,
            0,
            "Bucket start time {}ms is not aligned to bucket interval {}ms",
            bucket_start_ms,
            bucket_interval_ms
        );

        // Verify bucket_start_ms <= expiration_time_ms (rounding down)
        assert!(
            bucket_start_ms <= expiration_time_ms,
            "Bucket start time {}ms should be <= expiration time {}ms",
            bucket_start_ms,
            expiration_time_ms
        );

        // Verify bucket_start_ms is within one bucket interval of expiration_time_ms
        assert!(
            expiration_time_ms - bucket_start_ms < bucket_interval_ms,
            "Bucket start time {}ms should be within {}ms of expiration time {}ms",
            bucket_start_ms,
            bucket_interval_ms,
            expiration_time_ms
        );

        info!(
            "File expiring in {}ms assigned to bucket starting at {}ms",
            expiration_offset_ms, bucket_start_ms
        );
    }

    info!("Inode ttl bucket interval impact test passed");
    Ok(())
}
