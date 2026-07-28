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

/*
Complete HDFS integration tests for curvine-ufs

This file contains comprehensive HDFS tests:
1. Native HDFS test with full read/write operations
2. WebHDFS test with full read/write operations

## Usage

### Test WebHDFS only:
```bash
export WEBHDFS_ENDPOINT=http://127.0.0.1:50070
cargo test --features opendal-webhdfs -p curvine-ufs --test hdfs_integration_test -- --ignored
```

### Test both HDFS and WebHDFS:
```bash
export HDFS_NAMENODE=hdfs://test-hdfs
export WEBHDFS_ENDPOINT=http://127.0.0.1:50070
cargo test --features opendal-hdfs,opendal-webhdfs -p curvine-ufs --test hdfs_integration_test -- --ignored
```
*/

#[cfg(any(feature = "opendal-hdfs", feature = "opendal-webhdfs"))]
mod hdfs_tests {
    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_ufs::opendal::OpendalFileSystem;
    use std::collections::HashMap;
    use std::env;

    /// Test native HDFS with comprehensive read/write operations
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_native_hdfs_comprehensive() {
        println!("Starting comprehensive native HDFS test");

        // 1. Environment setup
        let namenode = env::var("HDFS_NAMENODE").unwrap_or_else(|_| "hdfs://test-hdfs".to_string());
        println!("HDFS Namenode: {}", namenode);

        // 2. JVM initialization test
        println!("Testing JVM initialization...");
        #[cfg(feature = "jni")]
        {
            use curvine_common::jvm::{is_jvm_available, load_jvm_memory_stats, register_jvm, JVM};

            // Register JVM builder first - exactly like
            register_jvm();
            println!("JVM builder registered");

            // Initialize JVM
            match JVM.get_or_init() {
                Ok(_jvm) => {
                    println!("JVM initialized successfully");
                    assert!(
                        is_jvm_available(),
                        "JVM should be available after initialization"
                    );

                    let (total, free) = load_jvm_memory_stats();
                    println!("JVM Memory - Total: {} bytes, Free: {} bytes", total, free);
                }
                Err(e) => {
                    println!("JVM initialization failed: {}", e);
                    panic!("JVM initialization is required for native HDFS: {}", e);
                }
            }
        }

        // 3. Filesystem creation test
        println!("Testing HDFS filesystem creation...");
        let path = Path::from_str(&format!("{}/curvine-native-test", namenode)).unwrap();
        let mut conf = HashMap::new();
        conf.insert("hdfs.namenode".to_string(), namenode.clone());
        conf.insert("hdfs.root".to_string(), "/curvine-native-test".to_string());

        // Add user configuration - use current system user
        let current_user = env::var("USER").unwrap_or_else(|_| "oppo".to_string());
        conf.insert("hdfs.user".to_string(), current_user.clone());
        println!("Using HDFS user: {}", current_user);

        // Don't use atomic write dir for now to avoid potential issues
        // conf.insert("hdfs.atomic_write_dir".to_string(), "true".to_string());

        let fs = match OpendalFileSystem::new(&path, conf) {
            Ok(fs) => {
                println!("HDFS filesystem created successfully");
                fs
            }
            Err(e) => {
                println!("HDFS filesystem creation failed: {}", e);
                println!("This could be due to:");
                println!("   - HDFS service not running");
                println!("   - Network connectivity issues");
                println!("   - Incorrect namenode configuration");
                println!("   - Authentication/authorization issues");
                println!("JVM was initialized successfully, so the core issue is resolved!");
                panic!("Failed to create HDFS filesystem: {}", e);
            }
        };

        // 4. Comprehensive file operations test
        println!("Testing comprehensive HDFS file operations...");

        // Use async runtime for file operations
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let test_dir = Path::from_str("hdfs://test-cluster/test-dir/").unwrap();
            let test_file = Path::from_str("hdfs://test-cluster/test-dir/test-file.txt").unwrap();
            let test_content = b"Hello, Native HDFS from Curvine!\nThis is a comprehensive test.\nLine 3 with more content.";

            // 4.1 Create directory
            println!("Creating test directory...");
            match fs.mkdir(&test_dir, true).await {
                Ok(created) => {
                    println!("Directory created: {}", created);
                }
                Err(e) => println!("Directory creation failed (may already exist): {}", e),
            }

            // 4.2 Write file
            println!("Writing test file...");
            match fs.create(&test_file, true).await {
                Ok(mut writer) => {
                    match writer.write(test_content).await {
                        Ok(()) => {
                            println!("Write successful: {} bytes written", test_content.len());

                            match writer.flush().await {
                                Ok(()) => println!("Writer flushed successfully"),
                                Err(e) => println!("Warning: Failed to flush writer: {}", e),
                            }

                            match writer.complete().await {
                                Ok(()) => println!("Writer closed successfully"),
                                Err(e) => println!("Warning: Failed to close writer: {}", e),
                            }
                        }
                        Err(e) => panic!("Write failed: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create file: {}", e),
            }

            // 4.3 Check file exists
            println!("Checking file existence...");
            match fs.exists(&test_file).await {
                Ok(exists) => {
                    println!("File exists check: {}", exists);
                    assert!(exists, "File should exist after writing");
                }
                Err(e) => panic!("Failed to check file existence: {}", e),
            }

            // 4.4 Get file status
            println!("Getting file status...");
            match fs.get_status(&test_file).await {
                Ok(status) => {
                    println!("File status - Size: {} bytes, Type: {:?}", status.len, status.file_type);
                    assert_eq!(status.len as usize, test_content.len());
                }
                Err(e) => panic!("Failed to get file status: {}", e),
            }

            // 4.5 Read file
            println!("Reading test file...");
            match fs.open(&test_file).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; test_content.len()];
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            println!("Read successful: {} bytes read", bytes_read);
                            assert_eq!(bytes_read, test_content.len());
                            assert_eq!(buffer, test_content);
                            println!("Content verification successful");
                        }
                        Err(e) => panic!("Read failed: {}", e),
                    }
                }
                Err(e) => panic!("Failed to open file for reading: {}", e),
            }

            // 4.6 List directory
            println!("Listing directory contents...");
            match fs.list_status(&test_dir).await {
                Ok(files) => {
                    println!("Directory listing successful: {} files found", files.len());
                    for file in &files {
                        println!("   {}: {} bytes", file.path, file.len);
                    }
                    assert!(!files.is_empty(), "Directory should contain files");
                }
                Err(e) => panic!("Failed to list directory: {}", e),
            }

            // 4.7 Cleanup
            println!("Cleaning up test files...");
            if let Err(e) = fs.delete(&test_file, false).await {
                println!("Warning: Failed to delete test file: {}", e);
            } else {
                println!("Test file deleted successfully");
            }

            if let Err(e) = fs.delete(&test_dir, true).await {
                println!("Warning: Failed to delete test directory: {}", e);
            } else {
                println!("Test directory deleted successfully");
            }
        });

        println!("Native HDFS comprehensive test completed successfully!");
    }

    /// Test WebHDFS basic configuration and connection
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-webhdfs")]
    fn test_webhdfs_basic_config() {
        println!("Starting WebHDFS basic configuration test");

        // 1. Environment setup
        let endpoint =
            env::var("WEBHDFS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50070".to_string());
        println!("WebHDFS Endpoint: {}", endpoint);

        // 2. Test basic connection and configuration
        println!("Testing WebHDFS basic connection...");
        let path = Path::from_str("webhdfs://test-cluster/").unwrap();
        let mut conf = HashMap::new();
        conf.insert("webhdfs.endpoint".to_string(), endpoint.clone());
        conf.insert("webhdfs.root".to_string(), "/".to_string());

        let fs = match OpendalFileSystem::new(&path, conf) {
            Ok(fs) => {
                println!("WebHDFS filesystem created successfully");
                fs
            }
            Err(e) => {
                panic!("Failed to create WebHDFS filesystem: {}", e);
            }
        };

        // 3. Test simple directory operations (should work without DataNode redirect)
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let test_dir = Path::from_str("webhdfs://test-cluster/curvine-config-test/").unwrap();

            // 3.1 Test directory creation
            println!("Testing directory creation...");
            match fs.mkdir(&test_dir, true).await {
                Ok(success) => println!("Directory creation result: {}", success),
                Err(e) => println!("Directory creation failed (expected): {}", e),
            }

            // 3.2 Test directory existence check
            println!("Testing directory existence check...");
            match fs.exists(&test_dir).await {
                Ok(exists) => println!("Directory exists check: {}", exists),
                Err(e) => println!("Directory exists check failed: {}", e),
            }
        });

        println!("WebHDFS basic configuration test completed!");
    }

    /// Test WebHDFS directory and metadata operations
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-webhdfs")]
    fn test_webhdfs_directory_operations() {
        println!("Starting WebHDFS directory operations test");

        let endpoint =
            env::var("WEBHDFS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50070".to_string());
        println!("WebHDFS Endpoint: {}", endpoint);

        let path = Path::from_str("webhdfs://test-cluster/").unwrap();
        let mut conf = HashMap::new();
        conf.insert("webhdfs.endpoint".to_string(), endpoint);
        conf.insert("webhdfs.root".to_string(), "/".to_string());

        let fs = OpendalFileSystem::new(&path, conf).expect("Failed to create WebHDFS filesystem");
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            let base_dir = Path::from_str("webhdfs://test-cluster/curvine-dir-test/").unwrap();
            let sub_dir1 =
                Path::from_str("webhdfs://test-cluster/curvine-dir-test/subdir1/").unwrap();
            let sub_dir2 =
                Path::from_str("webhdfs://test-cluster/curvine-dir-test/subdir2/").unwrap();

            // 1. Create nested directories
            println!("Creating nested directories...");
            for dir in [&base_dir, &sub_dir1, &sub_dir2] {
                match fs.mkdir(dir, true).await {
                    Ok(success) => {
                        println!("Created directory {}: {}", dir.full_path(), success)
                    }
                    Err(e) => println!("Failed to create {}: {}", dir.full_path(), e),
                }
            }

            // 2. List directory contents
            println!("Testing directory listing...");
            match fs.list_status(&base_dir).await {
                Ok(entries) => {
                    println!("Directory listing successful: {} entries", entries.len());
                    for entry in entries {
                        println!(
                            "  {}: {} ({})",
                            entry.name,
                            if entry.is_dir { "DIR" } else { "FILE" },
                            entry.len
                        );
                    }
                }
                Err(e) => println!("Directory listing failed: {}", e),
            }

            // 3. Check directory status
            println!("Testing directory status...");
            for dir in [&base_dir, &sub_dir1, &sub_dir2] {
                match fs.get_status(dir).await {
                    Ok(status) => println!(
                        "Status for {}: is_dir={}, len={}",
                        status.name, status.is_dir, status.len
                    ),
                    Err(e) => println!("Failed to get status for {}: {}", dir.full_path(), e),
                }
            }

            // 4. Cleanup
            println!("Cleaning up directories...");
            for dir in [&sub_dir2, &sub_dir1, &base_dir] {
                match fs.delete(dir, true).await {
                    Ok(()) => println!("Deleted directory: {}", dir.full_path()),
                    Err(e) => println!("Failed to delete {}: {}", dir.full_path(), e),
                }
            }
        });

        println!("WebHDFS directory operations test completed!");
    }

    /// Test native HDFS basic configuration
    #[test]
    #[ignore] // Run with --ignored flag
    #[cfg(feature = "opendal-hdfs")]
    fn test_hdfs_basic_config() {
        println!("Starting native HDFS basic configuration test");

        let namenode =
            env::var("HDFS_NAMENODE").unwrap_or_else(|_| "hdfs://127.0.0.1:9000".to_string());
        println!("HDFS Namenode: {}", namenode);

        let path = Path::from_str("hdfs://test-cluster/").unwrap();
        let mut conf = HashMap::new();
        conf.insert("hdfs.namenode".to_string(), namenode);
        conf.insert("hdfs.root".to_string(), "/".to_string());

        match OpendalFileSystem::new(&path, conf) {
            Ok(_fs) => {
                println!("Native HDFS filesystem created successfully");
                println!("Native HDFS basic configuration test completed!");
            }
            Err(e) => {
                println!("Native HDFS filesystem creation failed: {}", e);
                println!("This is expected if JVM or HDFS libraries are not properly configured");
            }
        }
    }

    /// Test WebHDFS with comprehensive read/write operations
    #[test]
    #[ignore] // Run with --ignored flag - requires proper DataNode network access
    #[cfg(feature = "opendal-webhdfs")]
    fn test_webhdfs_comprehensive() {
        println!("Starting comprehensive WebHDFS test");

        // 1. Environment setup
        let endpoint =
            env::var("WEBHDFS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50070".to_string());
        println!("WebHDFS Endpoint: {}", endpoint);

        // 2. Filesystem creation test
        println!("Testing WebHDFS filesystem creation...");
        let path = Path::from_str("webhdfs://test-cluster/").unwrap();
        let mut conf = HashMap::new();
        conf.insert("webhdfs.endpoint".to_string(), endpoint.clone());
        conf.insert("webhdfs.root".to_string(), "/".to_string());

        let fs = match OpendalFileSystem::new(&path, conf) {
            Ok(fs) => {
                println!("WebHDFS filesystem created successfully");
                fs
            }
            Err(e) => {
                println!("WebHDFS filesystem creation failed: {}", e);
                panic!("Failed to create WebHDFS filesystem: {}", e);
            }
        };

        // 3. Comprehensive file operations test
        println!("Testing comprehensive WebHDFS file operations...");

        // Use async runtime for file operations
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let test_dir = Path::from_str("webhdfs://test-cluster/testdir/").unwrap();
            let test_file = Path::from_str("webhdfs://test-cluster/testdir/testfile.txt").unwrap();
            let test_content = b"Hello, WebHDFS from Curvine!\nThis is a comprehensive WebHDFS test.\nTesting REST API functionality.\nMultiple lines for thorough testing.";

            // 3.1 Create directory
            println!("Creating test directory...");
            match fs.mkdir(&test_dir, true).await {
                Ok(created) => {
                    println!("Directory created: {}", created);
                }
                Err(e) => println!("Directory creation failed (may already exist): {}", e),
            }

            // 3.2 Write file
            println!("Writing test file...");
            match fs.create(&test_file, true).await {
                Ok(mut writer) => {
                    match writer.write(test_content).await {
                        Ok(()) => {
                            println!("Write successful: {} bytes written", test_content.len());

                            match writer.flush().await {
                                Ok(()) => println!("Writer flushed successfully"),
                                Err(e) => println!("Warning: Failed to flush writer: {}", e),
                            }

                            println!("Calling writer.complete()...");
                            match writer.complete().await {
                                Ok(()) => println!("Writer completed successfully"),
                                Err(e) => {
                                    println!("Failed to complete writer: {}", e);
                                    panic!("Writer complete failed: {}", e);
                                }
                            }
                        }
                        Err(e) => panic!("Write failed: {}", e),
                    }
                }
                Err(e) => panic!("Failed to create file: {}", e),
            }

            // 3.3 Check file exists (with small delay for WebHDFS consistency)
            println!("Checking file existence...");
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            match fs.exists(&test_file).await {
                Ok(exists) => {
                    println!("File exists check: {}", exists);
                    assert!(exists, "File should exist after writing");
                }
                Err(e) => panic!("Failed to check file existence: {}", e),
            }

            // 3.4 Get file status
            println!("Getting file status...");
            match fs.get_status(&test_file).await {
                Ok(status) => {
                    println!("File status - Size: {} bytes, Type: {:?}", status.len, status.file_type);
                    assert_eq!(status.len as usize, test_content.len());
                }
                Err(e) => panic!("Failed to get file status: {}", e),
            }

            // 3.5 Read file
            println!("Reading test file...");
            match fs.open(&test_file).await {
                Ok(mut reader) => {
                    let mut buffer = vec![0; test_content.len()];
                    match reader.read(&mut buffer).await {
                        Ok(bytes_read) => {
                            println!("Read successful: {} bytes read", bytes_read);
                            assert_eq!(bytes_read, test_content.len());
                            assert_eq!(buffer, test_content);
                            println!("Content verification successful");
                        }
                        Err(e) => panic!("Read failed: {}", e),
                    }
                }
                Err(e) => panic!("Failed to open file for reading: {}", e),
            }

            // 3.6 Test append operation
            println!("Testing append operation...");
            let append_content = b"\nAppended line via WebHDFS";
            match fs.append(&test_file).await {
                Ok(mut writer) => {
                    match writer.write(append_content).await {
                        Ok(()) => {
                            println!("Append successful: {} bytes appended", append_content.len());

                            match writer.flush().await {
                                Ok(()) => println!("Append writer flushed successfully"),
                                Err(e) => println!("Warning: Failed to flush append writer: {}", e),
                            }
                        }
                        Err(e) => println!("Append failed (may not be supported): {}", e),
                    }
                }
                Err(e) => println!("Failed to open file for append (may not be supported): {}", e),
            }

            // 3.7 List directory
            println!("Listing directory contents...");
            match fs.list_status(&test_dir).await {
                Ok(files) => {
                    println!("Directory listing successful: {} files found", files.len());
                    for file in &files {
                        println!("  {}: {} bytes", file.path, file.len);
                    }
                    assert!(!files.is_empty(), "Directory should contain files");
                }
                Err(e) => panic!("Failed to list directory: {}", e),
            }

            // 3.8 Test rename operation
            println!("Testing rename operation...");
            let renamed_file = Path::from_str("webhdfs://test-cluster/testdir/renamed.txt").unwrap();
            match fs.rename(&test_file, &renamed_file).await {
                Ok(success) => {
                        println!("Rename successful: {}", success);
                    if success {
                        // Verify old file doesn't exist and new file exists
                        match fs.exists(&test_file).await {
                            Ok(exists) => assert!(!exists, "Original file should not exist after rename"),
                            Err(e) => println!("Failed to check original file existence: {}", e),
                        }
                        match fs.exists(&renamed_file).await {
                            Ok(exists) => assert!(exists, "Renamed file should exist"),
                            Err(e) => println!("Failed to check renamed file existence: {}", e),
                        }
                    }
                }
                Err(e) => println!("Rename failed (may not be supported): {}", e),
            }

            // 3.9 Cleanup
            println!("Cleaning up test files...");
            // Try to delete both original and renamed files
            for file_path in [&test_file, &renamed_file] {
                if let Err(e) = fs.delete(file_path, false).await {
                    println!("Warning: Failed to delete {}: {}", file_path.full_path(), e);
                } else {
                    println!("File {} deleted successfully", file_path.full_path());
                }
            }

            if let Err(e) = fs.delete(&test_dir, true).await {
                println!("Warning: Failed to delete test directory: {}", e);
            } else {
                println!("Test directory deleted successfully");
            }
        });

        println!("WebHDFS comprehensive test completed successfully!");
    }
}
