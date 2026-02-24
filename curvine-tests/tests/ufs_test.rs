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

//! Unified File System Integration Tests
//!
//! Test all UnifiedFileSystem interfaces on various storage backends
//!
//! ## Configuration via Environment Variables
//!
//! You can configure the test storage backend using environment variables:
//!
//! ### Basic Configuration:
//! - `UFS_TEST_PATH`: Full path of the storage endpoint (e.g., hdfs://namenode:9000/test)
//! - `UFS_TEST_MOUNT_PATH`: Mount path in Curvine (default: /ufs-test)
//!
//! ### Storage-specific Configuration (key=value, comma-separated):
//! - `UFS_TEST_PROPERTIES`: Additional properties for the storage backend
//!
//! ### Examples:
//!
//! #### HDFS Native:
//! ```bash
//! export UFS_TEST_PATH=hdfs://namenode:9000/test
//! export UFS_TEST_PROPERTIES="hdfs.user=root,hdfs.namenode=hdfs://namenode:9000/"
//! ```
//!
//! #### HDFS (JNI):
//! ```bash
//! export UFS_TEST_PATH=hdfs://namenode:9000/test
//! export UFS_TEST_PROPERTIES="hdfs.user=hdfs,hdfs.namenode=hdfs://namenode:9000/"
//! ```
//!
//! #### S3:
//! ```bash
//! export UFS_TEST_PATH=s3://cv-test/ufs-test
//! export UFS_TEST_PROPERTIES="s3.credentials.access=access,s3.credentials.secret=secret,s3.region_name=cn,s3.endpoint_url=http://192.168.108.129:9000"
//! ```
//!
//! #### OSS (Aliyun):
//! ```bash
//! export UFS_TEST_PATH=oss://my-bucket/test
//! export UFS_TEST_PROPERTIES="oss.access_key_id=xxx,oss.access_key_secret=yyy,oss.endpoint=https://oss-cn-hangzhou.aliyuncs.com"
//! ```
//!
//! #### WebHDFS:
//! ```bash
//! export UFS_TEST_PATH=hdfs://namenode:9870/test
//! export UFS_TEST_PROPERTIES="webhdfs.endpoint=http://namenode:9870,webhdfs.root=/,webhdfs.delegation=token"
//! ```

use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{ListOptions, MountOptions, MountType, WriteType};
use curvine_tests::Testing;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct TestConfig {
    ufs_path: String,
    mount_path: String,
    properties: HashMap<String, String>,
}

impl TestConfig {
    fn from_env() -> Self {
        let ufs_path = std::env::var("UFS_TEST_PATH").unwrap();
        let mount_path =
            std::env::var("UFS_TEST_MOUNT_PATH").unwrap_or_else(|_| "/ufs-test".to_string());

        let mut properties = HashMap::new();

        if let Ok(props_str) = std::env::var("UFS_TEST_PROPERTIES") {
            for pair in props_str.split(',') {
                if let Some((key, value)) = pair.split_once('=') {
                    properties.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }
        Self {
            ufs_path,
            mount_path,
            properties,
        }
    }

    /// Get the test base directory path
    fn test_base_dir(&self) -> &str {
        &self.mount_path
    }
}

/// Main test entry
#[test]
fn ufs_test() -> CommonResult<()> {
    // Check if UFS_TEST_PATH is set, if not, skip the test
    if std::env::var("UFS_TEST_PATH").is_err() {
        println!("⚠️  UFS_TEST_PATH is not set, skipping test");
        println!("   Set UFS_TEST_PATH environment variable to run this test");
        println!("   Example: export UFS_TEST_PATH=hdfs://192.168.108.129:9000/hdfs-test");
        return Ok(());
    }

    // Load test configuration from environment variables
    let config = TestConfig::from_env();

    println!("=== Test Configuration ===");
    println!("Ufs path: {}", config.ufs_path);
    println!("Mount Path: {}", config.mount_path);
    println!("Properties: {:?}", config.properties);
    println!();

    let rt = Arc::new(AsyncRuntime::single());
    let rt_clone = rt.clone();

    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;

    let cluster_conf = testing.get_active_cluster_conf()?;
    let fs = UnifiedFileSystem::with_rt(cluster_conf, rt)?;

    let config_clone = config.clone();
    rt_clone.block_on(async move {
        // 1. Mount storage backend
        println!("=== Testing Storage Mount ===");
        mount_storage(&fs, &config_clone).await?;

        // 2. Cleanup test directory (if exists)
        // leanup_test_dir(&fs, &config_clone).await?;

        // 3. Test directory operations
        println!("\n=== Testing Directory Operations ===");
        test_list_options(&fs, &config_clone).await?;

        //test_delete_file(&fs, &config_clone).await?;

        Ok(())
    })
}

/// Mount storage backend to test directory
async fn mount_storage(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let opts = MountOptions::builder()
        .set_properties(config.properties.clone())
        .mount_type(MountType::Orch)
        .write_type(WriteType::Through)
        .build();

    let ufs_path: Path = Path::from_str(&config.ufs_path).unwrap();
    let cv_path: Path = Path::from_str(&config.mount_path).unwrap();

    fs.mount(&ufs_path, &cv_path, opts).await?;
    println!(
        "✓ Storage mounted successfully: {} -> {}",
        cv_path, ufs_path
    );
    Ok(())
}

/// Cleanup test directory
async fn cleanup_test_dir(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let test_path: Path = config.test_base_dir().into();
    let test_opts: Path = format!("{}/list_opts", config.test_base_dir()).into();

    if fs.exists(&test_opts).await? {
        fs.delete(&test_opts, true).await?;
    }

    if fs.exists(&test_path).await? {
        fs.delete(&test_path, true).await?;
        println!("✓ Test directory cleaned: {}", config.test_base_dir());
    }
    Ok(())
}

/// Test directory creation
async fn test_mkdir(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();

    // Test creating single-level directory
    let dir1: Path = format!("{}/dir1", base_dir).into();
    // Only enable create_parent for OSS when the `oss` feature is enabled.
    // When the feature is disabled, force it to false to avoid backend-specific behavior.
    let create_parent = {
        #[cfg(feature = "oss-hdfs")]
        {
            config.ufs_path.starts_with("oss://")
        }
        #[cfg(not(feature = "oss-hdfs"))]
        {
            false
        }
    };
    log::info!("create_parent: {}", create_parent);
    let result = fs.mkdir(&dir1, create_parent).await?;
    assert!(result, "Directory creation should return true");
    println!("✓ Directory created: {}", dir1);

    // Test creating multi-level directories
    let dir2: Path = format!("{}/dir2/subdir1/subdir2", base_dir).into();
    let result = fs.mkdir(&dir2, true).await?;
    assert!(result, "Multi-level directory creation should return true");
    println!("✓ Multi-level directory created: {}", dir2);

    // Test duplicate creation (should return false or not error)
    let result = fs.mkdir(&dir1, false).await?;
    println!("✓ Duplicate directory creation returned: {}", result);

    Ok(())
}

/// Test file/directory existence check
async fn test_exists(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();

    // Test existing directory
    let existing_dir: Path = format!("{}/dir1", base_dir).into();
    let exists = fs.exists(&existing_dir).await?;
    assert!(exists, "Created directory should exist");
    println!("✓ Directory existence check: {} = true", existing_dir);

    // Test non-existing path
    let non_existing: Path = format!("{}/non-existing", base_dir).into();
    let exists = fs.exists(&non_existing).await?;
    assert!(!exists, "Non-created path should not exist");
    println!("✓ Non-existing path check: {} = false", non_existing);

    Ok(())
}

/// Test getting file/directory status
async fn test_get_status(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let dir_path: Path = format!("{}/dir1", base_dir).into();
    let status = fs.get_status(&dir_path).await?;

    assert!(status.is_dir, "Should be a directory");
    assert_eq!(status.name, "dir1", "Directory name should be correct");
    println!(
        "✓ Directory status retrieved: name={}, is_dir={}",
        status.name, status.is_dir
    );

    Ok(())
}

/// Test listing directory contents
async fn test_list_status(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_path: Path = config.test_base_dir().into();
    let statuses = fs.list_status(&base_path).await?;

    assert!(
        !statuses.is_empty(),
        "Test directory should contain entries"
    );

    // Check the number of returned entries matches expected count
    // Expected: 2 directories (dir1 and dir2) created in test_mkdir
    let expected_count = 2;
    assert_eq!(
        statuses.len(),
        expected_count,
        "Directory should contain {} entries, but got {}",
        expected_count,
        statuses.len()
    );
    println!(
        "✓ Directory contents listed: {} entries (expected: {})",
        statuses.len(),
        expected_count
    );

    for status in &statuses {
        println!(
            "  - {}: {} (is_dir={})",
            status.name, status.path, status.is_dir
        );
    }

    // Verify created directories are in the list
    let dir_names: Vec<&str> = statuses.iter().map(|s| s.name.as_str()).collect();
    assert!(dir_names.contains(&"dir1"), "dir1 should be in the list");
    assert!(dir_names.contains(&"dir2"), "dir2 should be in the list");

    Ok(())
}

/// Test listing directory contents with ListOptions
async fn test_list_options(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let test_dir: Path = format!("{}/list_opts", base_dir).into();

    println!("\n=== Testing ListOptions ===");
    let subdir1: Path = format!("{}/1", test_dir).into();
    let file1: Path = format!("{}/2", test_dir).into();
    let subdir2: Path = format!("{}/3", test_dir).into();
    let file2: Path = format!("{}/4", test_dir).into();
    let file3: Path = format!("{}/5", test_dir).into();

    // Setup: Create test directory structure (交叉创建目录和文件)
    println!("Setting up test data...");

    // Verify setup: should have 5 entries total (2 dirs + 3 files)
    let all_statuses = fs.list_status(&test_dir).await?;
    assert_eq!(
        all_statuses.len(),
        5,
        "Test directory should contain 5 entries (2 dirs + 3 files), got {}",
        all_statuses.len()
    );
    println!("✓ Verified test data: {} entries total", all_statuses.len());

    // Test 1: List with limit=1
    println!("\nTest 1: List with limit=1");
    let opts = ListOptions {
        limit: Some(2),
        start_after: None,
    };
    let limited_statuses = fs.list_options(&test_dir, opts).await?;
    // println!("{:?}", limited_statuses);
    assert_eq!(
        limited_statuses.len(),
        1,
        "Limited list should return exactly 1 entry, got {}",
        limited_statuses.len()
    );
    println!("✓ Limited list returned {} entry", limited_statuses.len());

    // Test 2: List with limit=3
    println!("\nTest 2: List with limit=3");
    let opts = ListOptions {
        limit: Some(3),
        start_after: None,
    };
    let limited_statuses = fs.list_options(&test_dir, opts).await?;
    assert_eq!(
        limited_statuses.len(),
        3,
        "Limited list should return exactly 3 entries, got {}",
        limited_statuses.len()
    );
    println!("✓ Limited list returned {} entries", limited_statuses.len());

    // Test 3: List with start_after (starting after first entry)
    println!("\nTest 3: List with start_after");
    let first_status = &all_statuses[0];
    let opts = ListOptions {
        limit: None,
        start_after: Some(first_status.name.clone()),
    };
    let after_statuses = fs.list_options(&test_dir, opts).await?;
    println!("{:?}", after_statuses);
    assert_eq!(
        after_statuses.len(),
        4,
        "List after '{}' should return 4 entries, got {}",
        first_status.name,
        after_statuses.len()
    );
    println!(
        "✓ List after '{}' returned {} entries",
        first_status.name,
        after_statuses.len()
    );

    // Verify that the first entry is not in the result
    let after_names: Vec<&str> = after_statuses.iter().map(|s| s.name.as_str()).collect();
    assert!(
        !after_names.contains(&first_status.name.as_str()),
        "First entry '{}' should not be in the result",
        first_status.name
    );

    // Test 4: List with limit and start_after
    println!("\nTest 4: List with limit=2 and start_after");
    let first_status = &all_statuses[0];
    let opts = ListOptions {
        limit: Some(2),
        start_after: Some(first_status.name.clone()),
    };
    let limited_after_statuses = fs.list_options(&test_dir, opts).await?;
    assert_eq!(
        limited_after_statuses.len(),
        2,
        "Limited list with start_after should return exactly 2 entries, got {}",
        limited_after_statuses.len()
    );
    println!(
        "✓ Limited list after '{}' returned {} entries",
        first_status.name,
        limited_after_statuses.len()
    );

    // Test 5: Using from_status helper method
    println!("\nTest 5: Using from_status helper method");
    let first_status = &all_statuses[0];
    let opts = ListOptions::from_status(2, first_status);
    let from_status_result = fs.list_options(&test_dir, opts).await?;
    assert_eq!(
        from_status_result.len(),
        2,
        "List using from_status should return 2 entries, got {}",
        from_status_result.len()
    );
    println!(
        "✓ List using from_status returned {} entries",
        from_status_result.len()
    );

    // Test 6: List with start_after pointing to a directory
    println!("\nTest 6: List with start_after pointing to a directory");
    let subdir_status = fs.get_status(&subdir1).await?;
    let opts = ListOptions {
        limit: None,
        start_after: Some(subdir_status.name.clone()),
    };
    let after_dir_statuses = fs.list_options(&test_dir, opts).await?;
    println!(
        "✓ List after directory '{}' returned {} entries",
        subdir_status.name,
        after_dir_statuses.len()
    );

    // Verify that subdir1 is not in the result
    let after_dir_names: Vec<&str> = after_dir_statuses.iter().map(|s| s.name.as_str()).collect();
    assert!(
        !after_dir_names.contains(&subdir_status.name.as_str()),
        "Directory '{}' should not be in the result",
        subdir_status.name
    );

    // Test 7: Default options (should return all entries)
    println!("\nTest 7: Default options (should return all entries)");
    let opts = ListOptions::default();
    let default_statuses = fs.list_options(&test_dir, opts).await?;
    assert_eq!(
        default_statuses.len(),
        5,
        "Default list should return all 5 entries, got {}",
        default_statuses.len()
    );
    println!("✓ Default list returned {} entries", default_statuses.len());

    Ok(())
}

/// Test creating file and writing data
async fn test_create_and_write(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let file_path: Path = format!("{}/test_file.txt", base_dir).into();
    let test_data = b"Hello, Unified File System!\nThis is a test file.\n";

    // Create file and write data
    let mut writer = fs.create(&file_path, false).await?;
    writer.write(test_data).await?;
    writer.complete().await?;

    println!(
        "✓ File created and written: {} ({} bytes)",
        file_path,
        test_data.len()
    );

    // Verify file status
    let status = fs.get_status(&file_path).await?;
    assert!(!status.is_dir, "Should be a file, not a directory");
    assert_eq!(status.len, test_data.len() as i64, "File size should match");
    println!("✓ File status verified: size={} bytes", status.len);

    Ok(())
}

/// Test opening file and reading complete content
async fn test_open_and_read(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let file_path: Path = format!("{}/test_file.txt", base_dir).into();
    let expected_data = b"Hello, Unified File System!\nThis is a test file.\n";

    // Open and read file
    let mut reader = fs.open(&file_path).await?;
    let mut read_data = Vec::new();
    let mut buf = vec![0u8; 1024];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        read_data.extend_from_slice(&buf[..n]);
    }

    assert_eq!(
        read_data.as_slice(),
        expected_data,
        "Read data should match written data"
    );
    println!("✓ File read: {} ({} bytes)", file_path, read_data.len());
    println!("  Content: {:?}", String::from_utf8_lossy(&read_data));

    Ok(())
}

/// Test partial read and positioning
async fn test_read_partial(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let file_path: Path = format!("{}/test_file.txt", base_dir).into();

    // Open file
    let mut reader = fs.open(&file_path).await?;
    let file_size = reader.len();
    println!(
        "✓ File opened for partial read: {} (size={} bytes)",
        file_path, file_size
    );

    // Read first 10 bytes
    let mut buf = vec![0u8; 10];
    let n = reader.read(&mut buf).await?;
    let partial_data = &buf[..n];
    println!(
        "✓ First 10 bytes read: {:?}",
        String::from_utf8_lossy(partial_data)
    );

    // Continue reading to verify continuity
    let mut buf2 = vec![0u8; 1024];
    let n2 = reader.read(&mut buf2).await?;
    println!("✓ Continue reading: {} bytes", n2);

    Ok(())
}

/// Test random read with seek operations
async fn test_random_read(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();

    // Create a test file with known content for random access
    let file_path: Path = format!("{}/random_read_test.txt", base_dir).into();
    let test_data = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    // Write test data
    let mut writer = fs.create(&file_path, false).await?;
    writer.write(test_data).await?;
    writer.complete().await?;
    println!(
        "✓ Test file created for random read: {} ({} bytes)",
        file_path,
        test_data.len()
    );

    // Open file for random read
    let mut reader = fs.open(&file_path).await?;
    let file_size = reader.len();
    println!(
        "✓ File opened for random read testing (size={} bytes)",
        file_size
    );

    // Test 1: Seek to position 10 and read 5 bytes
    reader.seek(10).await?;
    let mut buf = vec![0u8; 5];
    let n = reader.read(&mut buf).await?;
    assert_eq!(n, 5, "Should read 5 bytes");
    assert_eq!(&buf[..n], b"ABCDE", "Data at position 10 should be 'ABCDE'");
    println!(
        "✓ Random read at position 10: {:?}",
        String::from_utf8_lossy(&buf[..n])
    );

    // Test 2: Seek backward to position 5 and read 5 bytes
    reader.seek(5).await?;
    let mut buf = vec![0u8; 5];
    let n = reader.read(&mut buf).await?;
    assert_eq!(n, 5, "Should read 5 bytes");
    assert_eq!(&buf[..n], b"56789", "Data at position 5 should be '56789'");
    println!(
        "✓ Random read at position 5: {:?}",
        String::from_utf8_lossy(&buf[..n])
    );

    // Test 3: Seek to near end and read
    // test_data has 62 bytes total, last 10 bytes are at position 52-61: "qrstuvwxyz"
    let seek_pos = file_size - 10;
    reader.seek(seek_pos).await?;
    let mut buf = vec![0u8; 10];
    let n = reader.read(&mut buf).await?;
    assert_eq!(n, 10, "Should read 10 bytes from near end");
    assert_eq!(&buf[..n], b"qrstuvwxyz", "Data at near end should match");
    println!(
        "✓ Random read at position {} (near end): {:?}",
        seek_pos,
        String::from_utf8_lossy(&buf[..n])
    );

    // Test 4: Seek to beginning and read first bytes
    reader.seek(0).await?;
    let mut buf = vec![0u8; 10];
    let n = reader.read(&mut buf).await?;
    assert_eq!(n, 10, "Should read 10 bytes from beginning");
    assert_eq!(
        &buf[..n],
        b"0123456789",
        "Data at beginning should be '0123456789'"
    );
    println!(
        "✓ Random read at position 0 (beginning): {:?}",
        String::from_utf8_lossy(&buf[..n])
    );

    // Test 5: Multiple random reads to verify seek consistency
    let test_positions: Vec<(usize, usize, &[u8])> = vec![
        (20, 5, b"KLMNO"),
        (36, 4, b"abcd"),
        (15, 3, b"FGH"),
        (0, 3, b"012"),
    ];

    for (pos, len, expected) in test_positions {
        reader.seek(pos as i64).await?;
        let mut buf = vec![0u8; len];
        let n = reader.read(&mut buf).await?;
        assert_eq!(n, len, "Should read {} bytes at position {}", len, pos);
        assert_eq!(&buf[..n], expected, "Data at position {} should match", pos);
        println!(
            "✓ Random read at position {}: {:?}",
            pos,
            String::from_utf8_lossy(&buf[..n])
        );
    }

    println!("✓ All random read tests passed");

    // Cleanup test file
    fs.delete(&file_path, false).await?;
    println!("✓ Random read test file cleaned up");

    Ok(())
}

/// Test file rename
async fn test_rename(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let old_path: Path = format!("{}/test_file.txt", base_dir).into();
    let new_path: Path = format!("{}/renamed_file.txt", base_dir).into();

    // Rename file
    let result = fs.rename(&old_path, &new_path).await?;
    assert!(result, "Rename should succeed");
    println!("✓ File renamed: {} -> {}", old_path, new_path);

    // Verify old path doesn't exist
    let old_exists = fs.exists(&old_path).await?;
    assert!(!old_exists, "Old path should not exist");
    println!("✓ Old path no longer exists: {}", old_path);

    // Verify new path exists
    let new_exists = fs.exists(&new_path).await?;
    assert!(new_exists, "New path should exist");
    println!("✓ New path exists: {}", new_path);

    Ok(())
}

/// Test file deletion
async fn test_delete_file(fs: &UnifiedFileSystem, config: &TestConfig) -> CommonResult<()> {
    let base_dir = config.test_base_dir();
    let file_path: Path = format!("{}/renamed_file.txt", base_dir).into();

    // Delete file
    fs.delete(&file_path, false).await?;
    println!("✓ File deleted: {}", file_path);

    // Verify file doesn't exist
    let exists = fs.exists(&file_path).await?;
    assert!(!exists, "File should not exist after deletion");
    println!("✓ File deletion verified");

    Ok(())
}
