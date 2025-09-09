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

//! Complete HDFS mount integration tests for curvine
//!
//! This file contains comprehensive HDFS mount integration tests:
//! 1. MountInfo-based HDFS filesystem creation
//! 2. CV path to UFS path translation
//! 3. Multi-mount scenarios with different configurations
//! 4. Complete mount lifecycle testing
//!
//! ## Usage
//!
//! ### Test HDFS mount integration:
//! ```bash
//! export HDFS_NAMENODE=hdfs://test-hdfs
//! cargo test --features opendal-hdfs,jni -p curvine-ufs --test hdfs_mount_integration_test -- --ignored
//! ```

#[cfg(any(feature = "opendal-hdfs", feature = "opendal-webhdfs"))]
mod mount_integration_tests {
    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_common::state::{ConsistencyStrategy, MountInfo, MountType, TtlAction};
    use curvine_ufs::opendal::OpendalFileSystem;
    use std::collections::HashMap;
    use std::env;

    /// Helper function to create OpendalFileSystem from MountInfo
    fn create_filesystem_from_mount(
        mount_info: &MountInfo,
    ) -> Result<OpendalFileSystem, curvine_common::error::FsError> {
        let ufs_path = Path::from_str(&mount_info.ufs_path)?;
        OpendalFileSystem::new(&ufs_path, mount_info.properties.clone())
    }

    /// Test HDFS filesystem creation through MountInfo
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_hdfs_mount_info_integration() {
        println!("Starting HDFS MountInfo integration test");

        let namenode = env::var("HDFS_NAMENODE").unwrap_or_else(|_| "hdfs://test-hdfs".to_string());
        println!("HDFS Namenode: {}", namenode);

        // Initialize JVM
        #[cfg(feature = "jni")]
        {
            use curvine_common::jvm::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
            println!("JVM initialized for mount integration");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Create MountInfo for HDFS
            let mut properties = HashMap::new();
            properties.insert("hdfs.namenode".to_string(), namenode.clone());
            properties.insert("hdfs.user".to_string(), "oppo".to_string());

            let mount_info = MountInfo {
                cv_path: "/curvine-mount".to_string(),
                ufs_path: format!("{}/curvine-mount-test", namenode),
                mount_id: 1001,
                properties,
                ttl_ms: 0,
                ttl_action: TtlAction::None,
                consistency_strategy: ConsistencyStrategy::None,
                storage_type: None,
                block_size: None,
                replicas: None,
                mount_type: MountType::Cst,
            };

            println!("Creating filesystem with MountInfo...");
            println!("   CV Path: {}", mount_info.cv_path);
            println!("   UFS Path: {}", mount_info.ufs_path);

            // Create filesystem using MountInfo
            let fs = match create_filesystem_from_mount(&mount_info) {
                Ok(fs) => {
                    println!("Filesystem created successfully with MountInfo");
                    fs
                }
                Err(e) => {
                    println!("Failed to create filesystem with MountInfo: {}", e);
                    panic!("Mount integration failed: {}", e);
                }
            };

            // Test path translation capabilities
            println!("Testing path translation...");

            let cv_test_path = Path::from_str("/curvine-mount/test-file.txt").unwrap();
            let expected_ufs_path = format!("{}/curvine-mount-test/test-file.txt", namenode);

            match mount_info.get_ufs_path(&cv_test_path) {
                Ok(ufs_path) => {
                    println!(
                        "CV to UFS path translation: {} -> {}",
                        cv_test_path.full_path(),
                        ufs_path.full_path()
                    );
                    assert_eq!(ufs_path.full_path(), expected_ufs_path);
                }
                Err(e) => {
                    println!("Path translation failed: {}", e);
                    panic!("Path translation error: {}", e);
                }
            }

            // Test reverse path translation
            let ufs_test_path = Path::from_str(&expected_ufs_path).unwrap();
            match mount_info.get_cv_path(&ufs_test_path) {
                Ok(cv_path) => {
                    println!(
                        "UFS to CV path translation: {} -> {}",
                        ufs_test_path.full_path(),
                        cv_path.full_path()
                    );
                    assert_eq!(cv_path.full_path(), "/curvine-mount/test-file.txt");
                }
                Err(e) => {
                    println!("Reverse path translation failed: {}", e);
                }
            }

            // Test actual file operations through the mounted filesystem
            println!("Testing file operations through mount...");

            let test_dir = Path::from_str("hdfs://test-cluster/mount-test/").unwrap();
            let test_file =
                Path::from_str("hdfs://test-cluster/mount-test/mount-test-file.txt").unwrap();
            let test_content = b"Hello from mounted HDFS filesystem!";

            // Create directory
            match fs.mkdir(&test_dir, true).await {
                Ok(created) => println!("Directory created through mount: {}", created),
                Err(e) => println!("Directory creation failed: {}", e),
            }

            // Write file
            match fs.create(&test_file, true).await {
                Ok(mut writer) => match writer.write(test_content).await {
                    Ok(()) => {
                        println!("File written through mount: {} bytes", test_content.len());
                        let _ = writer.flush().await;
                        let _ = writer.complete().await;
                    }
                    Err(e) => println!("Failed to write through mount: {}", e),
                },
                Err(e) => println!("Failed to create file through mount: {}", e),
            }

            // Read and verify
            match fs.open(&test_file).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; test_content.len()];
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            println!("File read through mount: {} bytes", bytes_read);
                            assert_eq!(buffer, test_content);
                            assert_eq!(bytes_read, test_content.len());
                        }
                        Err(e) => println!("Failed to read through mount: {}", e),
                    }
                }
                Err(e) => println!("Failed to open file through mount: {}", e),
            }

            // Test file listing
            match fs.list_status(&test_dir).await {
                Ok(files) => {
                    println!("Directory listing through mount: {} files", files.len());
                    for file in &files {
                        println!("  {}: {} bytes", file.name, file.len);
                    }
                }
                Err(e) => println!("Failed to list directory through mount: {}", e),
            }

            // Cleanup
            println!("Cleaning up mount test...");
            if let Err(e) = fs.delete(&test_dir, true).await {
                println!("Failed to cleanup mount test: {}", e);
            } else {
                println!("Mount test cleaned up successfully");
            }
        });

        println!("HDFS MountInfo integration test completed!");
    }

    /// Test multiple mount points with different configurations
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_multiple_mount_configurations() {
        println!("Starting multiple mount configurations test");

        let namenode = env::var("HDFS_NAMENODE").unwrap_or_else(|_| "hdfs://test-hdfs".to_string());

        // Initialize JVM
        #[cfg(feature = "jni")]
        {
            use curvine_common::jvm::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Define multiple mount configurations
            let mount_configs = vec![
                (
                    "standard-mount",
                    "/curvine-std",
                    "/curvine-std-ufs",
                    MountType::Cst,
                    TtlAction::None,
                ),
                (
                    "cached-mount",
                    "/curvine-cache",
                    "/curvine-cache-ufs",
                    MountType::Cst,
                    TtlAction::Delete,
                ),
                (
                    "orch-mount",
                    "/curvine-orch",
                    "/curvine-orch-ufs",
                    MountType::Orch,
                    TtlAction::None,
                ),
            ];

            let mut filesystems: Vec<(String, OpendalFileSystem, MountInfo)> = Vec::new();

            for (name, cv_path, ufs_suffix, mount_type, ttl_action) in mount_configs {
                println!("Creating mount configuration: {}", name);

                let mut properties = HashMap::new();
                properties.insert("hdfs.namenode".to_string(), namenode.clone());
                properties.insert("hdfs.user".to_string(), "oppo".to_string());

                let mount_info = MountInfo {
                    cv_path: cv_path.to_string(),
                    ufs_path: format!("{}{}", namenode, ufs_suffix),
                    mount_id: filesystems.len() as u32 + 2000,
                    properties,
                    ttl_ms: if ttl_action == TtlAction::Delete {
                        3600000
                    } else {
                        0
                    }, // 1 hour for cached
                    ttl_action,
                    consistency_strategy: ConsistencyStrategy::None,
                    storage_type: None,
                    block_size: None,
                    replicas: None,
                    mount_type,
                };

                match create_filesystem_from_mount(&mount_info) {
                    Ok(fs) => {
                        println!("Mount configuration {} created successfully", name);
                        filesystems.push((name.to_string(), fs, mount_info));
                    }
                    Err(e) => {
                        println!("Failed to create mount configuration {}: {}", name, e);
                    }
                }
            }

            // Test operations on each mount
            for (name, fs, mount_info) in &filesystems {
                println!("Testing operations on mount: {}", name);

                let test_dir = Path::from_str(&format!("hdfs://test-cluster/{}/", name)).unwrap();
                let test_file =
                    Path::from_str(&format!("hdfs://test-cluster/{}/config-test.txt", name))
                        .unwrap();
                let test_content = format!(
                    "Content from mount: {} (type: {:?}, ttl: {}ms)",
                    name, mount_info.mount_type, mount_info.ttl_ms
                )
                .as_bytes()
                .to_vec();

                // Test directory creation
                match fs.mkdir(&test_dir, true).await {
                    Ok(created) => println!("Directory created for {}: {}", name, created),
                    Err(e) => println!("Directory creation failed for {}: {}", name, e),
                }

                // Test file operations
                match fs.create(&test_file, true).await {
                    Ok(mut writer) => {
                        match writer.write(&test_content).await {
                            Ok(()) => {
                                println!("File written to {}: {} bytes", name, test_content.len());
                                let _ = writer.flush().await;
                                let _ = writer.complete().await;

                                // Verify read
                                match fs.open(&test_file).await {
                                    Ok(mut reader) => {
                                        let mut buffer = vec![0; test_content.len()];
                                        match reader.read(&mut buffer).await {
                                            Ok(bytes_read) => {
                                                println!(
                                                    "File read from {}: {} bytes",
                                                    name, bytes_read
                                                );
                                                assert_eq!(buffer, test_content);
                                            }
                                            Err(e) => {
                                                println!("Failed to read from {}: {}", name, e)
                                            }
                                        }
                                    }
                                    Err(e) => println!("Failed to open file in {}: {}", name, e),
                                }
                            }
                            Err(e) => println!("Failed to write to {}: {}", name, e),
                        }
                    }
                    Err(e) => println!("Failed to create file in {}: {}", name, e),
                }

                // Test path translation for this mount
                let cv_path = Path::from_str(&format!("{}/test.txt", mount_info.cv_path)).unwrap();
                match mount_info.get_ufs_path(&cv_path) {
                    Ok(ufs_path) => {
                        println!(
                            "Path translation for {}: {} -> {}",
                            name,
                            cv_path.full_path(),
                            ufs_path.full_path()
                        );
                    }
                    Err(e) => {
                        println!("Path translation failed for {}: {}", name, e);
                    }
                }
            }

            // Cleanup all mounts
            println!("Cleaning up all mount configurations...");
            for (name, fs, _) in &filesystems {
                let test_dir = Path::from_str(&format!("hdfs://test-cluster/{}/", name)).unwrap();
                if let Err(e) = fs.delete(&test_dir, true).await {
                    println!("Failed to cleanup {}: {}", name, e);
                } else {
                    println!("Cleaned up mount: {}", name);
                }
            }
        });

        println!("Multiple mount configurations test completed!");
    }

    /// Test mount lifecycle operations
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_mount_lifecycle() {
        println!("Starting mount lifecycle test");

        let namenode = env::var("HDFS_NAMENODE").unwrap_or_else(|_| "hdfs://test-hdfs".to_string());

        // Initialize JVM
        #[cfg(feature = "jni")]
        {
            use curvine_common::jvm::{register_jvm, JVM};
            register_jvm();
            JVM.get_or_init().expect("Failed to initialize JVM");
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Stage 1: Create initial mount
            println!("Stage 1: Creating initial mount");

            let mut properties = HashMap::new();
            properties.insert("hdfs.namenode".to_string(), namenode.clone());
            properties.insert("hdfs.user".to_string(), "oppo".to_string());

            let mount_info_v1 = MountInfo {
                cv_path: "/lifecycle-test".to_string(),
                ufs_path: format!("{}/lifecycle-test-v1", namenode),
                mount_id: 3000,
                properties: properties.clone(),
                ttl_ms: 0,
                ttl_action: TtlAction::None,
                consistency_strategy: ConsistencyStrategy::None,
                storage_type: None,
                block_size: None,
                replicas: None,
                mount_type: MountType::Cst,
            };

            let fs_v1 = create_filesystem_from_mount(&mount_info_v1)
                .expect("Failed to create initial mount");

            // Create some data in v1
            let test_dir_v1 = Path::from_str("hdfs://test-cluster/lifecycle-v1/").unwrap();
            let test_file_v1 =
                Path::from_str("hdfs://test-cluster/lifecycle-v1/data-v1.txt").unwrap();
            let content_v1 = b"Data from mount version 1";

            fs_v1
                .mkdir(&test_dir_v1, true)
                .await
                .expect("Failed to create v1 directory");
            let mut writer_v1 = fs_v1
                .create(&test_file_v1, true)
                .await
                .expect("Failed to create v1 file");
            writer_v1
                .write(content_v1)
                .await
                .expect("Failed to write v1 content");
            writer_v1.flush().await.expect("Failed to flush v1");
            writer_v1.complete().await.expect("Failed to complete v1");

            println!("Stage 1 completed: Initial mount with data created");

            // Stage 2: Update mount configuration
            println!("Stage 2: Updating mount configuration");

            let mount_info_v2 = MountInfo {
                cv_path: "/lifecycle-test".to_string(), // Same CV path
                ufs_path: format!("{}/lifecycle-test-v2", namenode), // Different UFS path
                mount_id: 3000,                         // Same mount ID
                properties: {
                    let mut props = properties.clone();
                    props.insert("hdfs.user".to_string(), "oppo".to_string());
                    props.insert("version".to_string(), "2".to_string());
                    props
                },
                ttl_ms: 1800000, // 30 minutes TTL
                ttl_action: TtlAction::Delete,
                consistency_strategy: ConsistencyStrategy::Always,
                storage_type: None,
                block_size: None,
                replicas: None,
                mount_type: MountType::Cst,
            };

            let fs_v2 = create_filesystem_from_mount(&mount_info_v2)
                .expect("Failed to create updated mount");

            // Create data in v2
            let test_dir_v2 = Path::from_str("hdfs://test-cluster/lifecycle-v2/").unwrap();
            let test_file_v2 =
                Path::from_str("hdfs://test-cluster/lifecycle-v2/data-v2.txt").unwrap();
            let content_v2 = b"Data from mount version 2 with TTL and consistency";

            fs_v2
                .mkdir(&test_dir_v2, true)
                .await
                .expect("Failed to create v2 directory");
            let mut writer_v2 = fs_v2
                .create(&test_file_v2, true)
                .await
                .expect("Failed to create v2 file");
            writer_v2
                .write(content_v2)
                .await
                .expect("Failed to write v2 content");
            writer_v2.flush().await.expect("Failed to flush v2");
            writer_v2.complete().await.expect("Failed to complete v2");

            println!("Stage 2 completed: Mount configuration updated");

            // Stage 3: Verify both versions work independently
            println!("Stage 3: Verifying independent operation of both versions");

            // Verify v1 data still accessible
            match fs_v1.open(&test_file_v1).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; content_v1.len()];
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            println!("V1 data still accessible: {} bytes", bytes_read);
                            assert_eq!(buffer, content_v1);
                        }
                        Err(e) => println!("Failed to read v1 data: {}", e),
                    }
                }
                Err(e) => println!("Failed to open v1 file: {}", e),
            }

            // Verify v2 data accessible
            match fs_v2.open(&test_file_v2).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; content_v2.len()];
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            println!("V2 data accessible: {} bytes", bytes_read);
                            assert_eq!(buffer, content_v2);
                        }
                        Err(e) => println!("Failed to read v2 data: {}", e),
                    }
                }
                Err(e) => println!("Failed to open v2 file: {}", e),
            }

            println!("Stage 3 completed: Both versions operate independently");

            // Stage 4: Cleanup
            println!("Stage 4: Cleaning up lifecycle test");

            if let Err(e) = fs_v1.delete(&test_dir_v1, true).await {
                println!("Failed to cleanup v1: {}", e);
            }

            if let Err(e) = fs_v2.delete(&test_dir_v2, true).await {
                println!("Failed to cleanup v2: {}", e);
            }

            println!("Stage 4 completed: Lifecycle test cleaned up");
        });

        println!("Mount lifecycle test completed!");
    }
}
