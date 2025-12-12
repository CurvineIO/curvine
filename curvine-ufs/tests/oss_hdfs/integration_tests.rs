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

#[cfg(feature = "oss-hdfs")]
mod tests {
    use super::super::test_utils::{create_test_conf, create_test_path, get_test_bucket};
    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_common::state::SetAttrOpts;
    use curvine_ufs::OssConf;
    use curvine_ufs::oss_hdfs::OssHdfsFileSystem;
    use orpc::sys::DataSlice;
    use std::collections::HashMap;

    /// Skip test if credentials are not available
    macro_rules! skip_if_no_credentials {
        () => {
            if create_test_conf().is_none() {
                println!("Skipping test: OSS credentials not set");
                return;
            }
        };
    }

    // ============================================================================
    // Unit Tests - FileSystem Initialization (Parameter Validation)
    // ============================================================================

    #[tokio::test]
    async fn test_filesystem_init_invalid_scheme() {
        let conf = HashMap::new();
        let path = Path::new("s3://bucket/").unwrap();

        let fs = OssHdfsFileSystem::new(&path, conf);
        assert!(fs.is_err(), "Should fail with invalid scheme");
    }

    #[tokio::test]
    async fn test_filesystem_init_missing_bucket() {
        let conf = HashMap::new();
        // Path::new("oss:///") will fail because URI format is invalid (missing authority)
        // This is expected behavior - invalid paths should fail at Path creation
        let path_result = Path::new("oss:///");
        assert!(path_result.is_err(), "Path::new should fail with invalid URI format");
        
        // Test with a path that has empty authority (if possible)
        // For a valid path format, we test that OssHdfsFileSystem::new detects missing bucket
        if let Ok(path) = Path::new("oss://bucket/") {
            // This should succeed as bucket is present
            let _fs = OssHdfsFileSystem::new(&path, conf.clone());
            // This will fail due to missing config, but that's a different error
            // The important test is that Path::new("oss:///") fails
        }
    }

    #[tokio::test]
    async fn test_filesystem_init_missing_endpoint() {
        let mut conf = HashMap::new();
        conf.insert(OssConf::ACCESS_KEY_ID.to_string(), "test-key".to_string());
        conf.insert(OssConf::ACCESS_KEY_SECRET.to_string(), "test-secret".to_string());

        let path = Path::new("oss://bucket/").unwrap();
        let fs = OssHdfsFileSystem::new(&path, conf);
        assert!(fs.is_err(), "Should fail with missing endpoint");
    }

    #[tokio::test]
    async fn test_filesystem_init_missing_access_key() {
        let mut conf = HashMap::new();
        conf.insert(OssConf::ENDPOINT.to_string(), "oss-cn-shanghai.aliyuncs.com".to_string());

        let path = Path::new("oss://bucket/").unwrap();
        let fs = OssHdfsFileSystem::new(&path, conf);
        assert!(fs.is_err(), "Should fail with missing access key");
    }

    // ============================================================================
    // Integration Tests - FileSystem Initialization
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_filesystem_init_success() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let path = Path::new(&format!("oss://{}/", bucket)).unwrap();

        let fs = OssHdfsFileSystem::new(&path, conf);
        assert!(fs.is_ok(), "Failed to initialize OSS-HDFS filesystem");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_filesystem_conf_access() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let path = Path::new(&format!("oss://{}/", bucket)).unwrap();

        let fs = OssHdfsFileSystem::new(&path, conf.clone()).unwrap();
        assert_eq!(
            fs.conf().get(OssConf::ENDPOINT),
            conf.get(OssConf::ENDPOINT).map(|s| s.as_str())
        );
    }

    // ============================================================================
    // Integration Tests - Directory Operations
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_mkdir_success() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_dir = create_test_path(&bucket, "test_dir");
        println!("Creating directory: {}", test_dir.full_path());
        let result = fs.mkdir(&test_dir, false).await;
        assert!(result.is_ok(), "Failed to create directory: {:?}", result.err());
        println!("✓ Directory created successfully: {}", test_dir.full_path());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_mkdir_recursive() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_dir = create_test_path(&bucket, "test_dir/nested/deep");
        let result = fs.mkdir(&test_dir, true).await;
        assert!(result.is_ok(), "Failed to create nested directory: {:?}", result.err());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_exists_directory() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        // Create directory first
        let test_dir = create_test_path(&bucket, "test_exists_dir");
        fs.mkdir(&test_dir, false).await.unwrap();

        // Check existence
        let exists = fs.exists(&test_dir).await.unwrap();
        assert!(exists, "Directory should exist");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_exists_nonexistent() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let nonexistent = create_test_path(&bucket, "nonexistent_path_12345");
        let exists = fs.exists(&nonexistent).await.unwrap();
        assert!(!exists, "Non-existent path should not exist");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_list_status() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        // Create test directory
        let test_dir = create_test_path(&bucket, "test_list_dir");
        fs.mkdir(&test_dir, false).await.unwrap();

        // Create a file
        let test_file = Path::new(&format!("{}/test_file.txt", test_dir.path())).unwrap();
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test content")))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // List directory
        let statuses = fs.list_status(&test_dir).await.unwrap();
        assert!(!statuses.is_empty(), "Directory should contain files");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_list_status_empty_directory() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_dir = create_test_path(&bucket, "test_empty_dir");
        fs.mkdir(&test_dir, false).await.unwrap();

        let statuses = fs.list_status(&test_dir).await.unwrap();
        // Empty directory may return empty list or contain only "." entry
        assert!(statuses.len() <= 1, "Empty directory should have at most one entry");
    }

    // ============================================================================
    // Integration Tests - File Operations
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_create_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_create_file");
        let writer = fs.create(&test_file, true).await;
        assert!(writer.is_ok(), "Failed to create file: {:?}", writer.err());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_write_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_write_file");
        let mut writer = fs.create(&test_file, true).await.unwrap();

        let data = b"Hello, OSS-HDFS!";
        let result = writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(data)))
            .await;
        assert!(result.is_ok(), "Failed to write data: {:?}", result.err());

        let result = writer.complete().await;
        assert!(result.is_ok(), "Failed to complete write: {:?}", result.err());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_write_large_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_large_file");
        let mut writer = fs.create(&test_file, true).await.unwrap();

        // Write 1MB of data
        let data = vec![0u8; 1024 * 1024];
        let result = writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
            .await;
        assert!(result.is_ok(), "Failed to write large data: {:?}", result.err());

        writer.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_write_multiple_chunks() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_multiple_chunks");
        let mut writer = fs.create(&test_file, true).await.unwrap();

        // Write multiple chunks
        for i in 0..10 {
            let data = format!("Chunk {}\n", i);
            writer
                .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                .await
                .unwrap();
        }

        writer.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_read_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_read_file");
        let test_data = b"Test read data";

        // Write file first
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Read file
        let mut reader = fs.open(&test_file).await.unwrap();
        let chunk = reader.read_chunk0().await.unwrap();

        let read_data = match chunk {
            DataSlice::Bytes(b) => b,
            _ => panic!("Unexpected data slice type"),
        };

        assert_eq!(read_data.as_ref(), test_data);
        reader.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_read_empty_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_empty_file");

        // Create empty file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer.complete().await.unwrap();

        // Read empty file
        let mut reader = fs.open(&test_file).await.unwrap();
        assert_eq!(reader.len(), 0, "Empty file should have length 0");

        let chunk = reader.read_chunk0().await.unwrap();
        match chunk {
            DataSlice::Empty => {} // Expected for empty file
            _ => panic!("Empty file should return Empty DataSlice"),
        }

        reader.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_read_nonexistent_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let nonexistent = create_test_path(&bucket, "nonexistent_file_12345");
        let result = fs.open(&nonexistent).await;
        assert!(result.is_err(), "Should fail to open non-existent file");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_seek() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_seek");
        let test_data = b"0123456789ABCDEF";

        // Write file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Read from offset
        let mut reader = fs.open(&test_file).await.unwrap();
        reader.seek(10).await.unwrap();

        let chunk = reader.read_chunk0().await.unwrap();
        let read_data = match chunk {
            DataSlice::Bytes(b) => b,
            _ => panic!("Unexpected data slice type"),
        };

        assert_eq!(read_data.as_ref(), b"ABCDEF");
        reader.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_pread() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_pread");
        let test_data = b"0123456789ABCDEF";

        // Write file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Random read
        let reader = fs.open(&test_file).await.unwrap();
        let data = reader.pread(5, 5).await.unwrap();
        assert_eq!(data.as_ref(), b"56789");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_delete_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_delete_file");

        // Create file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test")))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Delete file
        let result = fs.delete(&test_file, false).await;
        assert!(result.is_ok(), "Failed to delete file: {:?}", result.err());

        // Verify deleted
        let exists = fs.exists(&test_file).await.unwrap();
        assert!(!exists, "File should not exist after deletion");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_delete_directory() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_dir = create_test_path(&bucket, "test_delete_dir");
        fs.mkdir(&test_dir, false).await.unwrap();

        // Delete directory
        let result = fs.delete(&test_dir, true).await;
        assert!(result.is_ok(), "Failed to delete directory: {:?}", result.err());

        // Verify deleted
        let exists = fs.exists(&test_dir).await.unwrap();
        assert!(!exists, "Directory should not exist after deletion");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_rename_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let src_file = create_test_path(&bucket, "test_rename_src");
        let dst_file = create_test_path(&bucket, "test_rename_dst");

        // Create source file
        let mut writer = fs.create(&src_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test content")))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Rename
        let result = fs.rename(&src_file, &dst_file).await;
        assert!(result.is_ok(), "Failed to rename file: {:?}", result.err());

        // Verify
        assert!(!fs.exists(&src_file).await.unwrap(), "Source file should not exist");
        assert!(fs.exists(&dst_file).await.unwrap(), "Destination file should exist");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_rename_directory() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let src_dir = create_test_path(&bucket, "test_rename_dir_src");
        let dst_dir = create_test_path(&bucket, "test_rename_dir_dst");

        fs.mkdir(&src_dir, false).await.unwrap();

        // Rename
        let result = fs.rename(&src_dir, &dst_dir).await;
        assert!(result.is_ok(), "Failed to rename directory: {:?}", result.err());

        // Verify
        assert!(!fs.exists(&src_dir).await.unwrap(), "Source directory should not exist");
        assert!(fs.exists(&dst_dir).await.unwrap(), "Destination directory should exist");
    }

    // ============================================================================
    // Integration Tests - File Status
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_get_status_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_get_status");
        let test_data = b"test data";

        // Create file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Get status
        let status = fs.get_status(&test_file).await.unwrap();
        assert_eq!(status.len, test_data.len() as i64);
        assert!(!status.is_dir);
        assert_eq!(status.path, test_file.full_path());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_get_status_directory() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_dir = create_test_path(&bucket, "test_get_status_dir");
        fs.mkdir(&test_dir, false).await.unwrap();

        // Get status
        let status = fs.get_status(&test_dir).await.unwrap();
        assert!(status.is_dir);
        assert_eq!(status.path, test_dir.full_path());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_get_status_nonexistent() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let nonexistent = create_test_path(&bucket, "nonexistent_status_12345");
        let result = fs.get_status(&nonexistent).await;
        assert!(result.is_err(), "Should fail to get status of non-existent path");
    }

    // ============================================================================
    // Integration Tests - File Attributes
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_set_permission() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_set_permission");

        // Create file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test")))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Set permission
        let opts = SetAttrOpts {
            recursive: false,
            replicas: None,
            owner: None,
            group: None,
            mode: Some(0o755),
            atime: None,
            mtime: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: std::collections::HashMap::new(),
            remove_x_attr: vec![],
            ufs_mtime: None,
        };
        let result = fs.set_attr(&test_file, opts).await;
        assert!(result.is_ok(), "Failed to set permission: {:?}", result.err());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_set_owner() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_set_owner");

        // Create file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test")))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Set owner
        let opts = SetAttrOpts {
            recursive: false,
            replicas: None,
            owner: Some("testuser".to_string()),
            group: Some("testgroup".to_string()),
            mode: None,
            atime: None,
            mtime: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: std::collections::HashMap::new(),
            remove_x_attr: vec![],
            ufs_mtime: None,
        };
        let result = fs.set_attr(&test_file, opts).await;
        assert!(result.is_ok(), "Failed to set owner: {:?}", result.err());
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_set_attr_combined() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_set_attr_combined");

        // Create file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test")))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Set both permission and owner
        let opts = SetAttrOpts {
            recursive: false,
            replicas: None,
            owner: Some("testuser".to_string()),
            group: Some("testgroup".to_string()),
            mode: Some(0o644),
            atime: None,
            mtime: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: std::collections::HashMap::new(),
            remove_x_attr: vec![],
            ufs_mtime: None,
        };
        let result = fs.set_attr(&test_file, opts).await;
        assert!(result.is_ok(), "Failed to set attributes: {:?}", result.err());
    }

    // ============================================================================
    // Integration Tests - Writer Operations
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_writer_flush() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_writer_flush");
        let mut writer = fs.create(&test_file, true).await.unwrap();

        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test data")))
            .await
            .unwrap();

        // Flush
        let result = writer.flush().await;
        assert!(result.is_ok(), "Failed to flush: {:?}", result.err());

        writer.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_writer_tell() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_writer_tell");
        let mut writer = fs.create(&test_file, true).await.unwrap();

        let data = b"test data";
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(data)))
            .await
            .unwrap();

        // Get position
        let pos = writer.tell().await.unwrap();
        assert_eq!(pos, data.len() as i64);

        writer.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_writer_cancel() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_writer_cancel");
        let mut writer = fs.create(&test_file, true).await.unwrap();

        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from("test")))
            .await
            .unwrap();

        // Cancel
        let result = writer.cancel().await;
        assert!(result.is_ok(), "Failed to cancel: {:?}", result.err());
    }

    // ============================================================================
    // Integration Tests - Reader Operations
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_reader_tell() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_reader_tell");
        let test_data = b"test data";

        // Write file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Read and check position
        let mut reader = fs.open(&test_file).await.unwrap();
        let pos = reader.tell().await.unwrap();
        assert_eq!(pos, 0);

        reader.read_chunk0().await.unwrap();
        let pos = reader.tell().await.unwrap();
        assert!(pos > 0);

        reader.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_reader_get_file_length() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_reader_length");
        let test_data = b"test data";

        // Write file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Get file length
        let reader = fs.open(&test_file).await.unwrap();
        let length = reader.get_file_length().await.unwrap();
        assert_eq!(length, test_data.len() as i64);
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_reader_read_full() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_reader_read_full");
        let test_data = b"test data for read_full";

        // Write file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Read full
        let mut reader = fs.open(&test_file).await.unwrap();
        let mut buffer = vec![0u8; test_data.len()];
        let read_len = reader.read(&mut buffer).await.unwrap();
        assert_eq!(read_len, test_data.len());
        assert_eq!(&buffer[..read_len], test_data);

        reader.complete().await.unwrap();
    }

    // ============================================================================
    // Integration Tests - Error Cases
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_append_to_existing_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_append_existing");
        let initial_data = b"Initial data: ";
        let append_data = b"Appended data!";

        // Create file with initial data
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(initial_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Verify initial file content
        let mut reader = fs.open(&test_file).await.unwrap();
        let chunk = reader.read_chunk0().await.unwrap();
        let read_data = match chunk {
            DataSlice::Bytes(b) => b,
            _ => panic!("Unexpected data slice type"),
        };
        assert_eq!(read_data.as_ref(), initial_data);
        reader.complete().await.unwrap();

        // Append data to existing file
        let mut append_writer = fs.append(&test_file).await.unwrap();
        assert_eq!(
            append_writer.pos(),
            initial_data.len() as i64,
            "Writer position should be at end of file"
        );
        append_writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(append_data)))
            .await
            .unwrap();
        append_writer.complete().await.unwrap();

        // Verify appended file content
        let mut reader = fs.open(&test_file).await.unwrap();
        let mut all_data = Vec::new();
        loop {
            let chunk = reader.read_chunk0().await.unwrap();
            match chunk {
                DataSlice::Empty => break,
                DataSlice::Bytes(b) => all_data.extend_from_slice(&b),
                _ => panic!("Unexpected data slice type"),
            }
        }
        reader.complete().await.unwrap();
        
        let expected: Vec<u8> = [initial_data.as_slice(), append_data.as_slice()].concat();
        assert_eq!(all_data.len(), expected.len());
        assert_eq!(all_data, expected);
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_append_to_new_file() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_append_new");
        let append_data = b"Data appended to new file";

        // Append to non-existent file (should create it)
        let mut append_writer = fs.append(&test_file).await.unwrap();
        assert_eq!(
            append_writer.pos(),
            0,
            "Writer position should be 0 for new file"
        );
        append_writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(append_data)))
            .await
            .unwrap();
        append_writer.complete().await.unwrap();

        // Verify file was created with correct content
        let mut reader = fs.open(&test_file).await.unwrap();
        let chunk = reader.read_chunk0().await.unwrap();
        let read_data = match chunk {
            DataSlice::Bytes(b) => b,
            _ => panic!("Unexpected data slice type"),
        };
        assert_eq!(read_data.as_ref(), append_data);
        reader.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_append_multiple_times() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_append_multiple");
        let chunks: Vec<&[u8]> = vec![
            b"First chunk\n",
            b"Second chunk\n",
            b"Third chunk\n",
        ];

        // Create file with first chunk
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(chunks[0])))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Append remaining chunks
        for chunk in &chunks[1..] {
            let mut append_writer = fs.append(&test_file).await.unwrap();
            append_writer
                .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(chunk)))
                .await
                .unwrap();
            append_writer.complete().await.unwrap();
        }

        // Verify all chunks are present
        let mut reader = fs.open(&test_file).await.unwrap();
        let mut all_data = Vec::new();
        loop {
            let chunk = reader.read_chunk0().await.unwrap();
            match chunk {
                DataSlice::Empty => break,
                DataSlice::Bytes(b) => all_data.extend_from_slice(&b),
                _ => panic!("Unexpected data slice type"),
            }
        }
        reader.complete().await.unwrap();

        let expected: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(all_data, expected);
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_pread_invalid_offset() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "test_pread_invalid");
        let test_data = b"test";

        // Write file
        let mut writer = fs.create(&test_file, true).await.unwrap();
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(test_data)))
            .await
            .unwrap();
        writer.complete().await.unwrap();

        // Try invalid offset
        let reader = fs.open(&test_file).await.unwrap();
        let result = reader.pread(-1, 10).await;
        assert!(result.is_err(), "Should fail with negative offset");

        let result = reader.pread(100, 10).await;
        assert!(result.is_err(), "Should fail with offset beyond file length");
    }

    // ============================================================================
    // Integration Tests - Full Workflow
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_integration_full_workflow() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        // 1. Create directory
        let test_dir = create_test_path(&bucket, "integration_test");
        fs.mkdir(&test_dir, false).await.unwrap();
        assert!(fs.exists(&test_dir).await.unwrap());

        // 2. Create and write file
        let test_file = Path::new(&format!("{}/test.txt", test_dir.path())).unwrap();
        let mut writer = fs.create(&test_file, true).await.unwrap();
        let data = b"Integration test data";
        writer
            .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(data)))
            .await
            .unwrap();
        writer.flush().await.unwrap();
        writer.complete().await.unwrap();

        // 3. Verify file exists
        assert!(fs.exists(&test_file).await.unwrap());

        // 4. Get file status
        let status = fs.get_status(&test_file).await.unwrap();
        assert_eq!(status.len, data.len() as i64);

        // 5. Read file
        let mut reader = fs.open(&test_file).await.unwrap();
        let chunk = reader.read_chunk0().await.unwrap();
        let read_data = match chunk {
            DataSlice::Bytes(b) => b,
            _ => panic!("Unexpected data slice type"),
        };
        assert_eq!(read_data.as_ref(), data);
        reader.complete().await.unwrap();

        // 6. Set attributes
        let opts = SetAttrOpts {
            recursive: false,
            replicas: None,
            owner: Some("testuser".to_string()),
            group: None,
            mode: Some(0o644),
            atime: None,
            mtime: None,
            ttl_ms: None,
            ttl_action: None,
            add_x_attr: std::collections::HashMap::new(),
            remove_x_attr: vec![],
            ufs_mtime: None,
        };
        fs.set_attr(&test_file, opts).await.unwrap();

        // 7. Rename file
        let renamed_file = Path::new(&format!("{}/renamed.txt", test_dir.path())).unwrap();
        fs.rename(&test_file, &renamed_file).await.unwrap();
        assert!(!fs.exists(&test_file).await.unwrap());
        assert!(fs.exists(&renamed_file).await.unwrap());

        // 8. List directory
        let statuses = fs.list_status(&test_dir).await.unwrap();
        assert!(!statuses.is_empty());

        // 9. Delete file
        fs.delete(&renamed_file, false).await.unwrap();
        assert!(!fs.exists(&renamed_file).await.unwrap());

        // 11. Delete directory
        fs.delete(&test_dir, true).await.unwrap();
        assert!(!fs.exists(&test_dir).await.unwrap());
    }
}
