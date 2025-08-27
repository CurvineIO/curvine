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

use bytes::BytesMut;
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::state::{CreateFileOptsBuilder, FileBlocks, WorkerAddress};
use curvine_server::test::MiniCluster;
use log::info;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::{CommonError, CommonResult};
use std::sync::Arc;

/// Create a test configuration for replication testing
/// 2 masters, 3 workers, no S3 mounting
fn create_test_config() -> ClusterConf {
    let mut conf = ClusterConf::default();

    // Block configuration
    conf.client.block_size = 64 * 1024; // 64KB
    conf.master.min_block_size = 64 * 1024;
    conf.master.min_replication = 1;
    conf.master.max_replication = 3;

    // Test directories (will be overridden by MiniCluster)
    conf.master.meta_dir = "/tmp/curvine-test/meta".to_string();
    conf.journal.journal_dir = "/tmp/curvine-test/journal".to_string();
    conf.worker.data_dir = vec!["/tmp/curvine-test/data".to_string()];

    // Network configuration (will be overridden by MiniCluster)
    conf.master.hostname = "127.0.0.1".to_string();
    conf.worker.hostname = "127.0.0.1".to_string();
    conf.journal.hostname = "127.0.0.1".to_string();

    // Timeouts and intervals
    // conf.master.heartbeat_interval_ms = 1000;
    // conf.worker.heartbeat_interval_ms = 1000;

    conf
}

/// Test end-to-end replication functionality
/// This test verifies that when blocks become under-replicated,
/// the replication manager automatically replicates them to ensure data availability
#[test]
fn test_block_replication_e2e() -> CommonResult<()> {
    let conf = create_test_config();

    // Use 2 masters and 3 workers as requested
    let cluster = MiniCluster::with_num(&conf, 2, 3);

    info!("Starting cluster with 2 masters and 3 workers");
    cluster.start_cluster();

    // Wait for cluster to be fully ready
    Utils::sleep(3000);

    let rt = Arc::new(AsyncRuntime::single());
    let fs = cluster.new_fs();

    // Step 1: Write a file to create blocks
    let path = Path::from_str("/replication_test.dat")?;
    let test_data = generate_test_data(200 * 1024); // 200KB data (will create ~3-4 blocks)

    info!("Writing test file: {} bytes", test_data.len());
    let file_blocks = rt.block_on(async { write_test_file(&fs, &path, &test_data).await })?;

    info!("File written with {} blocks", file_blocks.block_locs.len());

    // Step 2: Get initial block locations
    let initial_locations = rt.block_on(async { get_block_locations(&fs, &path).await })?;

    info!(
        "Initial block locations: {} total locations",
        initial_locations.len()
    );
    for (block_id, locations) in &initial_locations {
        info!("Block {} has {} replicas", block_id, locations.len());
    }

    // Step 3: Simulate under-replication by reporting blocks as missing
    // In a real scenario, this would happen when a worker fails
    let master_replication_manager = cluster.get_active_master_replication_manager();

    // Pick the first block and simulate it becoming under-replicated
    let first_block = file_blocks.block_locs.first().unwrap();
    let block_id = first_block.block.id;

    info!("Simulating under-replication for block {}", block_id);

    // Report that this block is under-replicated
    // We'll use worker_id 1 (assuming it exists)
    master_replication_manager.report_under_replicated_blocks(1, vec![block_id])?;

    // Step 4: Wait for replication to complete
    info!("Waiting for replication to complete...");
    Utils::sleep(5000); // Give time for replication process

    // Step 5: Verify that the block has been replicated
    let final_locations = rt.block_on(async { get_block_locations(&fs, &path).await })?;

    info!("Final block locations after replication:");
    for (block_id, locations) in &final_locations {
        info!("Block {} has {} replicas", block_id, locations.len());
    }

    // Step 6: Verify data integrity
    info!("Verifying data integrity after replication");
    let read_data = rt.block_on(async { read_test_file(&fs, &path).await })?;
    info!("Expected: {}. Real: {}", test_data.len(), read_data.len());

    if read_data == test_data {
        info!("✓ Data integrity verified - all data matches original");
    } else {
        return Err(CommonError::from("Data integrity check failed"));
    }

    // Step 7: Verify increased replication
    let target_block_locations = final_locations.get(&block_id);
    if let Some(locations) = target_block_locations {
        if locations.len()
            > initial_locations
                .get(&block_id)
                .map(|l| l.len())
                .unwrap_or(0)
        {
            info!(
                "✓ Block replication succeeded - block {} now has {} replicas",
                block_id,
                locations.len()
            );
        } else {
            return Err(CommonError::from(
                "Block replication did not increase replica count",
            ));
        }
    } else {
        return Err(CommonError::from(
            "Target block not found in final locations",
        ));
    }

    info!("✅ End-to-end replication test completed successfully");
    Ok(())
}

/// Test replication with worker failure simulation  
#[test]
fn test_replication_with_simulated_worker_failure() -> CommonResult<()> {
    let mut conf = create_test_config();
    conf.client.block_size = 32 * 1024; // 32KB blocks
    conf.master.min_block_size = 32 * 1024;

    // Use 2 masters and 4 workers for more comprehensive testing
    let cluster = MiniCluster::with_num(&conf, 2, 4);

    info!("Starting cluster with 2 masters and 4 workers");
    cluster.start_cluster();
    Utils::sleep(3000);

    let fs = cluster.new_fs();
    let rt = cluster.clone_client_rt();

    // Write multiple files to distribute blocks across workers
    let files = vec![
        ("/repl_test_1.dat", 80 * 1024),  // ~2-3 blocks
        ("/repl_test_2.dat", 120 * 1024), // ~3-4 blocks
        ("/repl_test_3.dat", 60 * 1024),  // ~2 blocks
    ];

    let mut all_blocks = Vec::new();
    let mut test_data_map = std::collections::HashMap::new();

    for (file_path, size) in files {
        let path = Path::from_str(file_path)?;
        let test_data = generate_test_data(size);

        info!("Writing file {}: {} bytes", file_path, size);
        let file_blocks = rt.block_on(async { write_test_file(&fs, &path, &test_data).await })?;

        all_blocks.extend(file_blocks.block_locs.iter().map(|b| b.block.id));
        test_data_map.insert(file_path.to_string(), test_data);
    }

    info!(
        "Created {} blocks across {} files",
        all_blocks.len(),
        test_data_map.len()
    );

    // Wait for initial replication to complete
    Utils::sleep(2000);

    // Get master filesystem and replication manager
    let replication_manager = cluster.get_active_master_replication_manager();

    // Simulate multiple blocks becoming under-replicated
    let blocks_to_replicate = all_blocks[0..std::cmp::min(3, all_blocks.len())].to_vec();

    info!(
        "Simulating under-replication for {} blocks",
        blocks_to_replicate.len()
    );
    replication_manager.report_under_replicated_blocks(1, blocks_to_replicate)?;

    // Wait for replication
    Utils::sleep(8000);

    // Verify all files can still be read correctly
    for (file_path, expected_data) in test_data_map {
        let path = Path::from_str(&file_path)?;
        info!("Verifying file: {}", file_path);

        let read_data = rt.block_on(async { read_test_file(&fs, &path).await })?;

        if read_data != expected_data {
            return Err(CommonError::from(format!(
                "Data integrity failed for {}",
                file_path
            )));
        }
    }

    info!("✅ Replication with simulated worker failure test completed successfully");
    Ok(())
}

async fn write_test_file(
    fs: &CurvineFileSystem,
    path: &Path,
    data: &[u8],
) -> CommonResult<FileBlocks> {
    let opts = CreateFileOptsBuilder::with_conf(&fs.fs_context().cluster_conf().client)
        .client_name(fs.fs_context().clone_client_name())
        .create(true)
        .overwrite(true)
        .append(false)
        .replicas(2)
        .create_parent(true)
        .build();
    let mut writer = fs.create_with_opts(path, opts).await?;
    writer.write(data).await?;
    writer.complete().await?;

    let blocks = fs.get_block_locations(path).await?;
    Ok(blocks)
}

async fn read_test_file(fs: &CurvineFileSystem, path: &Path) -> CommonResult<Vec<u8>> {
    let file_status = fs.get_status(path).await?;
    let mut reader = fs.open(path).await?;

    // Create buffer with the exact file size, filled with zeros
    let mut buffer = BytesMut::zeroed(file_status.len as usize);

    // Read the entire file in one call
    let bytes_read = reader.read_full(&mut buffer).await?;
    reader.complete().await?;

    // Truncate buffer to actual bytes read
    buffer.truncate(bytes_read);
    Ok(buffer.to_vec())
}

async fn get_block_locations(
    fs: &CurvineFileSystem,
    path: &Path,
) -> CommonResult<std::collections::HashMap<i64, Vec<WorkerAddress>>> {
    let block_locations = fs.get_block_locations(path).await?;

    let mut location_map = std::collections::HashMap::new();
    for block_location in block_locations.block_locs.iter() {
        location_map.insert(block_location.block.id, block_location.locs.clone());
    }

    Ok(location_map)
}

fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let pattern = b"REPLICATION_TEST_DATA_";

    for i in 0..size {
        data.push(pattern[i % pattern.len()]);
    }

    // Add some unique markers at specific positions to help with debugging
    if size > 100 {
        data[50] = b'S'; // Start marker
        data[size - 50] = b'E'; // End marker
    }

    data
}
