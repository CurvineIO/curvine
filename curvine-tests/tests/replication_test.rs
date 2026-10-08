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
use curvine_config::ClusterConf;
use curvine_core_error::{CommonError, CommonResult};
use curvine_fs_api::{Path, Reader, Writer};
use curvine_model::{BlockLocation, CreateFileOptsBuilder, FileBlocks, StorageType, WorkerAddress};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Create a test configuration for replication testing
/// 2 masters, 3 workers, no S3 mounting
#[allow(dead_code)]
fn create_test_config(tmp_dir: &TempDir) -> ClusterConf {
    let root = tmp_dir.path().to_str().unwrap();
    let mut conf = ClusterConf::default();

    // Block configuration
    conf.client.block_size = 64 * 1024; // 64KB
    conf.master.min_block_size = 64 * 1024;
    conf.master.min_replication = 1;
    conf.master.max_replication = 3;
    conf.master.block_replication_enabled = true;

    // Test directories (will be overridden by MiniCluster)
    conf.master.meta_dir = format!("{}/meta", root);
    conf.journal.journal_dir = format!("{}/journal", root);
    conf.worker.data_dir = vec![format!("{}/data", root)];

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
    // Build base config via Testing builder (loads default conf), then tweak replication params
    let testing = Testing::builder()
        .default()
        .masters(2)
        .workers(3)
        .mutate_conf(|conf| {
            conf.client.block_size_str = "64KB".to_string();
            conf.master.min_block_size = 64 * 1024;
            conf.master.min_replication = 1;
            conf.master.max_replication = 3;
            conf.master.block_replication_enabled = true;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

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

    // Step 3: Remove one real metadata location so the block is genuinely
    // under-replicated. Merely submitting an already healthy block would ask the
    // scheduler to exceed the file's configured replica count.
    let master_replication_manager = cluster.get_active_master_replication_manager();
    let master_filesystem = cluster.get_active_master_fs();
    let first_block = file_blocks.block_locs.first().unwrap();
    let block_id = first_block.block.id;
    let target_replica_count = file_blocks.status.replicas as usize;
    let removed_location = first_block
        .locs
        .last()
        .expect("replication test file must have at least one location")
        .clone();

    info!("Simulating under-replication for block {}", block_id);
    let fs_dir = master_filesystem.fs_dir();
    fs_dir.write().block_report(vec![(
        false,
        block_id,
        BlockLocation {
            worker_id: removed_location.worker_id,
            storage_type: Default::default(),
        },
    )])?;
    let under_replicated_locations =
        rt.block_on(async { get_block_locations(&fs, &path).await })?;
    assert_eq!(
        target_replica_count - 1,
        replica_count(&under_replicated_locations, block_id)
    );

    master_replication_manager
        .report_under_replicated_blocks(removed_location.worker_id, vec![block_id])?;

    // Step 4: Wait until replication restores the configured replica count.
    info!("Waiting for replication to complete...");
    let deadline = Instant::now() + Duration::from_secs(8);
    let final_locations = loop {
        let current_locations = rt.block_on(async { get_block_locations(&fs, &path).await })?;
        let current_replica_count = replica_count(&current_locations, block_id);
        if current_replica_count == target_replica_count {
            break current_locations;
        }
        if Instant::now() >= deadline {
            return Err(CommonError::from(format!(
                "block {} did not recover its configured replica count before timeout (expected={}, actual={})",
                block_id, target_replica_count, current_replica_count
            )));
        }
        std::thread::sleep(Duration::from_millis(100));
    };

    info!("Final block locations after replication:");
    for (block_id, locations) in &final_locations {
        info!("Block {} has {} replicas", block_id, locations.len());
    }

    let target_block_locations = final_locations
        .get(&block_id)
        .ok_or_else(|| CommonError::from("Target block not found in final locations"))?;
    let replicated_location = target_block_locations
        .iter()
        .find(|candidate| {
            !under_replicated_locations
                .get(&block_id)
                .is_some_and(|locations| {
                    locations
                        .iter()
                        .any(|location| location.worker_id == candidate.worker_id)
                })
        })
        .ok_or_else(|| CommonError::from("No new replication target was recorded"))?;

    // Step 5: Keep only the location produced by replication and verify it can
    // serve the complete file.
    let reports = target_block_locations
        .iter()
        .filter(|location| location.worker_id != replicated_location.worker_id)
        .map(|location| {
            (
                false,
                block_id,
                BlockLocation {
                    worker_id: location.worker_id,
                    storage_type: Default::default(),
                },
            )
        })
        .collect();
    fs_dir.write().block_report(reports)?;
    let latest_locations = rt.block_on(async { get_block_locations(&fs, &path).await })?;
    let first_block_locations = latest_locations.get(&block_id).unwrap();
    assert_eq!(1, first_block_locations.len());
    assert_eq!(
        replicated_location.worker_id,
        first_block_locations[0].worker_id
    );

    // Step 7: Verify data integrity
    info!("Verifying data integrity after replication");
    let read_data = rt.block_on(async { read_test_file(&fs, &path).await })?;
    info!("Expected: {}. Real: {}", test_data.len(), read_data.len());

    if read_data == test_data {
        info!("✓ Data integrity verified - all data matches original");
    } else {
        return Err(CommonError::from("Data integrity check failed"));
    }

    info!("✅ End-to-end replication test completed successfully");
    Ok(())
}

/// Replication reports the destination writer's actual storage tier after fallback.
#[test]
fn test_replication_persists_destination_actual_storage_type() -> CommonResult<()> {
    const REPLICATION_TIMEOUT: Duration = Duration::from_secs(8);
    const REPLICATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

    let testing = Testing::builder()
        .default()
        .masters(1)
        .workers(3)
        .mutate_conf(|conf| {
            conf.master.worker_policy = "robin".to_string();
            conf.master.min_replication = 1;
            conf.master.max_replication = 3;
            conf.master.block_replication_enabled = true;
        })
        .mutate_worker_conf(|index, conf| {
            let storage_type = if index < 2 { "MEM" } else { "DISK" };
            conf.worker.data_dir = conf
                .worker
                .data_dir
                .iter()
                .map(|path| format!("[{storage_type}:512MB]{path}"))
                .collect();
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let mem_worker_ports = [
        cluster.worker_conf[0].worker.rpc_port as u32,
        cluster.worker_conf[1].worker.rpc_port as u32,
    ];
    let disk_worker_port = cluster.worker_conf[2].worker.rpc_port as u32;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.replicas = 2;
    conf.client.short_circuit = false;
    conf.client.storage_type = StorageType::Mem;
    conf.client.storage_type_str = "mem".to_string();
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    let path = Path::from_str("/replication_actual_storage_type.data")?;
    let test_data = generate_test_data(16 * 1024);
    let file_blocks =
        rt.block_on(async { write_test_file_with_replicas(&fs, &path, &test_data, 2).await })?;
    let block = file_blocks
        .block_locs
        .first()
        .ok_or_else(|| CommonError::from("replication fallback test created no blocks"))?;
    assert_eq!(block.locs.len(), 2);
    assert!(block
        .locs
        .iter()
        .all(|location| mem_worker_ports.contains(&location.rpc_port)));
    let block_id = block.block.id;
    let retained_source = block.locs[0].clone();
    let removed_source = block.locs[1].clone();

    let master_fs = cluster.get_active_master_fs();
    master_fs.fs_dir.write().block_report(vec![(
        false,
        block_id,
        BlockLocation::new(removed_source.worker_id, StorageType::Mem),
    )])?;
    cluster
        .get_active_master_replication_manager()
        .report_under_replicated_blocks(removed_source.worker_id, vec![block_id])?;

    let deadline = Instant::now() + REPLICATION_TIMEOUT;
    let replicated_location = loop {
        let blocks = rt.block_on(async { fs.get_block_locations(&path).await })?;
        let locations = &blocks.block_locs[0].locs;
        if let Some(location) = locations
            .iter()
            .find(|location| location.worker_id != retained_source.worker_id)
        {
            break location.clone();
        }
        if Instant::now() >= deadline {
            return Err(CommonError::from(format!(
                "block {block_id} did not replicate to the Disk-only worker before timeout"
            )));
        }
        std::thread::sleep(REPLICATION_POLL_INTERVAL);
    };

    assert_eq!(replicated_location.rpc_port, disk_worker_port);
    let locations = master_fs.fs_dir.read().get_block_locations(block_id)?;
    let target_location = locations
        .iter()
        .find(|location| location.worker_id == replicated_location.worker_id)
        .ok_or_else(|| CommonError::from("replicated target missing from Master metadata"))?;
    assert_eq!(target_location.storage_type, StorageType::Disk);

    let read_data = rt.block_on(async { read_test_file(&fs, &path).await })?;
    assert_eq!(read_data, test_data);
    Ok(())
}

/// Test replication with worker failure simulation  
#[test]
fn test_replication_with_simulated_worker_failure() -> CommonResult<()> {
    let testing = Testing::builder()
        .default()
        .masters(2)
        .workers(4)
        .mutate_conf(|conf| {
            conf.client.block_size_str = "32KB".to_string();
            conf.master.min_block_size = 32 * 1024;
            conf.master.min_replication = 1;
            conf.master.max_replication = 3;
            conf.master.block_replication_enabled = true;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

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

/// Replication must size the target from source block metadata rather than the
/// worker client's unrelated default block size.
#[test]
fn test_replication_honors_source_block_capacity() -> CommonResult<()> {
    const CLIENT_DEFAULT_BLOCK_SIZE: i64 = 32 * 1024;
    const FILE_BLOCK_SIZE: i64 = 64 * 1024;
    const FILE_SIZE: usize = 96 * 1024;
    const REPLICATION_TIMEOUT: Duration = Duration::from_secs(8);
    const REPLICATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

    let testing = Testing::builder()
        .default()
        .masters(2)
        .workers(3)
        .mutate_conf(|conf| {
            conf.client.block_size_str = format!("{CLIENT_DEFAULT_BLOCK_SIZE}B");
            conf.master.min_block_size = CLIENT_DEFAULT_BLOCK_SIZE;
            conf.master.min_replication = 1;
            conf.master.max_replication = 3;
            conf.master.block_replication_enabled = true;
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.block_size_str = format!("{FILE_BLOCK_SIZE}B");
    conf.client.init()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    let path = Path::from_str("/replication_large_block.dat")?;
    let test_data = generate_test_data(FILE_SIZE);
    let file_blocks = rt.block_on(async { write_test_file(&fs, &path, &test_data).await })?;
    let oversized_block = file_blocks
        .block_locs
        .iter()
        .find(|block| block.block.len > CLIENT_DEFAULT_BLOCK_SIZE)
        .expect("missing block larger than the worker client default");
    let block_id = oversized_block.block.id;
    let target_replica_count = file_blocks.status.replicas as usize;
    let removed_location = oversized_block
        .locs
        .last()
        .expect("replication test file must have at least one location")
        .clone();

    let fs_dir = cluster.get_active_master_fs().fs_dir();
    fs_dir.write().block_report(vec![(
        false,
        block_id,
        BlockLocation {
            worker_id: removed_location.worker_id,
            storage_type: Default::default(),
        },
    )])?;
    let under_replicated_locations =
        rt.block_on(async { get_block_locations(&fs, &path).await })?;
    assert_eq!(
        target_replica_count - 1,
        replica_count(&under_replicated_locations, block_id)
    );

    cluster
        .get_active_master_replication_manager()
        .report_under_replicated_blocks(removed_location.worker_id, vec![block_id])?;

    let deadline = Instant::now() + REPLICATION_TIMEOUT;
    let final_locations = loop {
        let current_locations = rt.block_on(async { get_block_locations(&fs, &path).await })?;
        let current_replica_count = replica_count(&current_locations, block_id);
        if current_replica_count == target_replica_count {
            break current_locations;
        }
        if Instant::now() >= deadline {
            return Err(CommonError::from(format!(
                "oversized block {} did not recover its configured replica count before timeout (expected={}, actual={})",
                block_id, target_replica_count, current_replica_count
            )));
        }
        std::thread::sleep(REPLICATION_POLL_INTERVAL);
    };

    let final_block_locations = final_locations
        .get(&block_id)
        .ok_or_else(|| CommonError::from("oversized block missing after replication"))?;
    let replicated_location = final_block_locations
        .iter()
        .find(|candidate| {
            !under_replicated_locations
                .get(&block_id)
                .is_some_and(|locations| {
                    locations
                        .iter()
                        .any(|location| location.worker_id == candidate.worker_id)
                })
        })
        .ok_or_else(|| CommonError::from("No new oversized-block replication target found"))?;
    let reports = final_block_locations
        .iter()
        .filter(|location| location.worker_id != replicated_location.worker_id)
        .map(|location| {
            (
                false,
                block_id,
                BlockLocation {
                    worker_id: location.worker_id,
                    storage_type: Default::default(),
                },
            )
        })
        .collect();
    fs_dir.write().block_report(reports)?;
    let latest_locations = rt.block_on(async { get_block_locations(&fs, &path).await })?;
    assert_eq!(1, replica_count(&latest_locations, block_id));

    let read_data = rt.block_on(async { read_test_file(&fs, &path).await })?;
    if read_data != test_data {
        return Err(CommonError::from(
            "Data integrity check failed after oversized-block replication",
        ));
    }

    Ok(())
}

async fn write_test_file(
    fs: &CurvineFileSystem,
    path: &Path,
    data: &[u8],
) -> CommonResult<FileBlocks> {
    write_test_file_with_replicas(fs, path, data, 2).await
}

async fn write_test_file_with_replicas(
    fs: &CurvineFileSystem,
    path: &Path,
    data: &[u8],
    replicas: i32,
) -> CommonResult<FileBlocks> {
    let opts = CreateFileOptsBuilder::with_conf(&fs.fs_context().cluster_conf().client)
        .client_name(fs.fs_context().clone_client_name())
        .replicas(replicas)
        .create_parent(true)
        .build();
    let mut writer = fs.create_with_opts(path, opts, true).await?;
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
) -> CommonResult<HashMap<i64, Vec<WorkerAddress>>> {
    let block_locations = fs.get_block_locations(path).await?;

    let mut location_map = HashMap::new();
    for block_location in block_locations.block_locs.iter() {
        location_map.insert(block_location.block.id, block_location.locs.clone());
    }

    Ok(location_map)
}

fn replica_count(locations: &HashMap<i64, Vec<WorkerAddress>>, block_id: i64) -> usize {
    locations
        .get(&block_id)
        .map(|locations| locations.len())
        .unwrap_or(0)
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

/// Exercise the actual submit/ACK/deadline/reaper/metadata chain, not the tracker in isolation.
#[cfg(feature = "fault-injection")]
#[test]
fn test_lost_replication_result_releases_scheduler_and_repairs_block() -> CommonResult<()> {
    replication_recovery_scenario(false, false)
}

#[cfg(feature = "fault-injection")]
#[test]
fn test_failed_copy_and_lost_result_are_retried_without_heartbeat() -> CommonResult<()> {
    replication_recovery_scenario(true, false)
}

#[cfg(feature = "fault-injection")]
#[test]
fn test_metadata_failure_retries_result_without_recopying() -> CommonResult<()> {
    replication_recovery_scenario(false, true)
}

#[cfg(feature = "fault-injection")]
fn replication_recovery_scenario(fail_copy: bool, fail_metadata: bool) -> CommonResult<()> {
    use curvine_fault::{FaultRuleBuilder, FaultRuntime};
    struct Rules(Vec<String>);
    impl Drop for Rules {
        fn drop(&mut self) {
            for id in &self.0 {
                let _ = FaultRuntime::process().remove(id);
            }
        }
    }
    let testing = Testing::builder()
        .default()
        .masters(1)
        .workers(3)
        .mutate_conf(|conf| {
            conf.client.block_size_str = "64KB".into();
            conf.master.min_block_size = 64 * 1024;
            conf.master.min_replication = 1;
            conf.master.max_replication = 3;
            conf.master.block_replication_enabled = true;
            conf.master.block_replication_concurrency_limit = 1;
            conf.master.block_replication_job_timeout = "2s".into();
            conf.master.block_replication_retry_interval = "100ms".into();
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    // MiniClusters share one process-wide fault runtime. Their inode allocators start
    // at the same ID, so reserve distinct IDs for each recovery scenario.
    for index in 0..(if fail_copy {
        2
    } else if fail_metadata {
        4
    } else {
        0
    }) {
        let path = Path::from_str(format!("/recovery-padding-{index}"))?;
        rt.block_on(write_test_file_with_replicas(&fs, &path, b"padding", 1))?;
    }
    let a = Path::from_str("/lost-result-a")?;
    let b = Path::from_str("/lost-result-b")?;
    let data = generate_test_data(4096);
    let fa = rt.block_on(write_test_file_with_replicas(&fs, &a, &data, 2))?;
    let fb = rt.block_on(write_test_file_with_replicas(&fs, &b, &data, 2))?;
    let aid = fa.block_locs[0].block.id;
    let bid = fb.block_locs[0].block.id;
    let runtime = FaultRuntime::process();
    let drop_id = format!("drop-result-{aid}");
    let ack_id = format!("record-ack-{aid}");
    let copy_id = format!("a-record-copy-{aid}");
    let fail_copy_id = format!("b-fail-copy-{aid}");
    let metadata_id = format!("fail-metadata-{aid}");
    let _rules = Rules(vec![
        drop_id.clone(),
        ack_id.clone(),
        copy_id.clone(),
        fail_copy_id.clone(),
        metadata_id.clone(),
    ]);
    runtime.configure(
        &copy_id,
        FaultRuleBuilder::named("worker.replication.before_copy")
            .matches("block_id", aid)?
            .record()?,
    )?;
    if fail_copy {
        runtime.configure(
            &fail_copy_id,
            FaultRuleBuilder::named("worker.replication.before_copy")
                .matches("block_id", aid)?
                .times(1)?
                .return_error("source lost before copy")?,
        )?;
    }
    if fail_metadata {
        runtime.configure(
            &metadata_id,
            FaultRuleBuilder::named("master.replication.before_metadata_commit")
                .matches("block_id", aid)?
                .times(2)?
                .return_error("metadata unavailable")?,
        )?;
    }
    runtime.configure(
        &drop_id,
        FaultRuleBuilder::named("worker.replication.before_report")
            .matches("block_id", aid)?
            .times(1)?
            .return_error("drop A result")?,
    )?;
    runtime.configure(
        &ack_id,
        FaultRuleBuilder::named("master.replication.submit_acked")
            .matches("block_id", aid)?
            .record()?,
    )?;
    let master_fs = cluster.get_active_master_fs();
    let manager = cluster.get_active_master_replication_manager();
    for blocks in [&fa, &fb] {
        let blk = &blocks.block_locs[0];
        let loc = &blk.locs[1];
        master_fs.fs_dir().write().block_report(vec![(
            false,
            blk.block.id,
            BlockLocation::new(loc.worker_id, StorageType::Disk),
        )])?;
    }
    manager.report_under_replicated_blocks(fa.block_locs[0].locs[1].worker_id, vec![aid])?;
    let start = Instant::now();
    loop {
        let executed = |id: &str| runtime.rule(id).unwrap().is_some_and(|r| r.executions > 0);
        if executed(&drop_id) && executed(&ack_id) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "A must ACK and lose its report"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        replica_count(&rt.block_on(get_block_locations(&fs, &a))?, aid),
        1,
        "A must still be deficient after its result was dropped"
    );
    manager.report_under_replicated_blocks(fb.block_locs[0].locs[1].worker_id, vec![bid])?;
    let wait = |path: &Path, id| -> CommonResult<()> {
        let start = Instant::now();
        loop {
            if replica_count(&rt.block_on(get_block_locations(&fs, path))?, id) == 2 {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "block {id} did not regain its configured replica count"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    };
    wait(&b, bid)?;
    wait(&a, aid)?;
    let copies = runtime.rule(&copy_id)?.unwrap().executions;
    assert_eq!(
        copies,
        if fail_copy { 2 } else { 1 },
        "successful copy must not be repeated for metadata failure"
    );
    if fail_metadata {
        assert_eq!(runtime.rule(&metadata_id)?.unwrap().executions, 2);
    }
    // Force reads from the repaired destination rather than accidentally reading the source.
    for blocks in [&fa, &fb] {
        let blk = &blocks.block_locs[0];
        let source = &blk.locs[0];
        master_fs.fs_dir().write().block_report(vec![(
            false,
            blk.block.id,
            BlockLocation::new(source.worker_id, StorageType::Disk),
        )])?;
    }
    assert_eq!(rt.block_on(read_test_file(&fs, &a))?, data);
    assert_eq!(rt.block_on(read_test_file(&fs, &b))?, data);
    for blocks in [&fa, &fb] {
        let blk = &blocks.block_locs[0];
        let source = &blk.locs[0];
        master_fs.fs_dir().write().block_report(vec![(
            true,
            blk.block.id,
            BlockLocation::new(source.worker_id, StorageType::Disk),
        )])?;
    }
    Ok(())
}
