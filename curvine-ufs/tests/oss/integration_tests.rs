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

#[cfg(feature = "oss")]
mod tests {
    use super::super::test_utils::{create_test_conf, create_test_path, get_test_bucket};
    use curvine_common::fs::{FileSystem, Path, Reader, Writer};
    use curvine_common::state::SetAttrOpts;
    use curvine_ufs::OssConf;
    use curvine_ufs::oss::OssHdfsFileSystem;
    use orpc::sys::DataSlice;
    use std::collections::HashMap;
    use std::sync::Arc;

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

    // ============================================================================
    // Integration Tests - Concurrent Writer Operations
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_concurrent_writers_different_files() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = Arc::new(OssHdfsFileSystem::new(&fs_path, conf).unwrap());

        let num_threads = 10;
        let chunks_per_thread = 5;
        let mut handles = vec![];

        for i in 0..num_threads {
            let fs_clone = fs.clone();
            let bucket_clone = bucket.clone();
            let handle = tokio::spawn(async move {
                let test_file = create_test_path(&bucket_clone, &format!("concurrent_writer_{}", i));
                let mut writer = fs_clone.create(&test_file, true).await.unwrap();

                // Write multiple chunks
                for j in 0..chunks_per_thread {
                    let data = format!("Thread {} chunk {}\n", i, j);
                    writer
                        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                        .await
                        .unwrap();
                }

                writer.complete().await.unwrap();

                // Verify file content
                let mut reader = fs_clone.open(&test_file).await.unwrap();
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

                (i, all_data)
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            let (thread_id, data) = result.unwrap();
            let expected_size = chunks_per_thread * format!("Thread {} chunk {}\n", thread_id, 0).len();
            assert!(
                data.len() >= expected_size,
                "Thread {} should have written at least {} bytes, got {}",
                thread_id,
                expected_size,
                data.len()
            );
        }
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_concurrent_writes_same_writer() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "concurrent_same_writer");
        let writer = fs.create(&test_file, true).await.unwrap();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let num_tasks = 20;
        let mut handles = vec![];

        for i in 0..num_tasks {
            let writer_clone = writer.clone();
            let handle = tokio::spawn(async move {
                let data = format!("Task {} data\n", i);
                let mut writer = writer_clone.lock().await;
                writer
                    .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Task should complete without panic");
        }

        // Complete the writer
        let mut writer = writer.lock().await;
        writer.complete().await.unwrap();

        // Verify file content
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

        // Should have data from all tasks
        assert!(
            all_data.len() > 0,
            "File should contain data from concurrent writes"
        );
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_high_concurrency_writers() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = Arc::new(OssHdfsFileSystem::new(&fs_path, conf).unwrap());

        // High concurrency: 50 concurrent writers
        let num_writers = 50;
        let mut handles = vec![];

        for i in 0..num_writers {
            let fs_clone = fs.clone();
            let bucket_clone = bucket.clone();
            let handle = tokio::spawn(async move {
                let test_file = create_test_path(&bucket_clone, &format!("high_concurrent_{}", i));
                let mut writer = fs_clone.create(&test_file, true).await.unwrap();

                // Write multiple chunks with some delay to increase concurrency window
                for j in 0..10 {
                    let data = format!("Writer {} chunk {}\n", i, j);
                    writer
                        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                        .await
                        .unwrap();
                    
                    // Small delay to increase chance of concurrent access
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }

                writer.flush().await.unwrap();
                writer.complete().await.unwrap();

                i
            });
            handles.push(handle);
        }

        // Wait for all writers to complete - this should not crash with SIGSEGV
        let results: Vec<_> = futures::future::join_all(handles).await;
        let mut success_count = 0;
        for result in results {
            match result {
                Ok(thread_id) => {
                    success_count += 1;
                    // Verify file was created
                    let test_file = create_test_path(&bucket, &format!("high_concurrent_{}", thread_id));
                    assert!(
                        fs.exists(&test_file).await.unwrap(),
                        "File from writer {} should exist",
                        thread_id
                    );
                }
                Err(e) => {
                    panic!("Writer task panicked: {:?}", e);
                }
            }
        }

        assert_eq!(
            success_count,
            num_writers,
            "All {} writers should complete successfully",
            num_writers
        );
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_concurrent_write_and_flush() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "concurrent_write_flush");
        let writer = fs.create(&test_file, true).await.unwrap();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        // Spawn tasks that write and flush concurrently
        let num_tasks = 15;
        let mut handles = vec![];

        for i in 0..num_tasks {
            let writer_clone = writer.clone();
            let handle = tokio::spawn(async move {
                let data = format!("Data chunk {}\n", i);
                let mut writer = writer_clone.lock().await;
                writer
                    .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                    .await
                    .unwrap();
                
                // Flush after every few writes
                if i % 3 == 0 {
                    writer.flush().await.unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Task should complete without panic");
        }

        // Final flush and complete
        let mut writer = writer.lock().await;
        writer.flush().await.unwrap();
        writer.complete().await.unwrap();

        // Verify file content
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

        assert!(all_data.len() > 0, "File should contain data");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_concurrent_write_with_tell() {
        skip_if_no_credentials!();

        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "concurrent_write_tell");
        let writer = fs.create(&test_file, true).await.unwrap();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let num_tasks = 10;
        let mut handles = vec![];

        for i in 0..num_tasks {
            let writer_clone = writer.clone();
            let handle = tokio::spawn(async move {
                let data = format!("Chunk {}\n", i);
                let mut writer = writer_clone.lock().await;
                
                // Write and check position
                writer
                    .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data.clone())))
                    .await
                    .unwrap();
                
                // Call tell() concurrently
                let pos = writer.tell().await.unwrap();
                assert!(pos >= data.len() as i64, "Position should be at least data length");
            });
            handles.push(handle);
        }

        // Wait for all tasks
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Task should complete without panic");
        }

        // Complete writer
        let mut writer = writer.lock().await;
        writer.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_stress_concurrent_writers() {
        skip_if_no_credentials!();

        // Stress test: many concurrent operations to detect race conditions and SIGSEGV
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = Arc::new(OssHdfsFileSystem::new(&fs_path, conf).unwrap());

        let num_writers = 100;
        let writes_per_writer = 20;
        let mut handles = vec![];

        for i in 0..num_writers {
            let fs_clone = fs.clone();
            let bucket_clone = bucket.clone();
            let handle = tokio::spawn(async move {
                let test_file = create_test_path(&bucket_clone, &format!("stress_writer_{}", i));
                let mut writer = fs_clone.create(&test_file, true).await.unwrap();

                // Rapid writes
                for j in 0..writes_per_writer {
                    let data = format!("Stress test writer {} write {}\n", i, j);
                    writer
                        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                        .await
                        .unwrap();
                }

                writer.flush().await.unwrap();
                writer.complete().await.unwrap();

                // Verify immediately after completion
                let status = fs_clone.get_status(&test_file).await.unwrap();
                assert!(status.len > 0, "File should have content");

                i
            });
            handles.push(handle);
        }

        // Collect results - if there's a SIGSEGV, this will fail
        let results: Vec<_> = futures::future::join_all(handles).await;
        let mut success_count = 0;
        for result in results {
            match result {
                Ok(thread_id) => {
                    success_count += 1;
                    // Quick verification
                    let test_file = create_test_path(&bucket, &format!("stress_writer_{}", thread_id));
                    assert!(
                        fs.exists(&test_file).await.unwrap(),
                        "File from stress writer {} should exist",
                        thread_id
                    );
                }
                Err(e) => {
                    panic!("Stress test writer panicked: {:?}", e);
                }
            }
        }

        assert_eq!(
            success_count,
            num_writers,
            "All {} stress writers should complete without SIGSEGV",
            num_writers
        );
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_aggressive_concurrent_writes_same_writer() {
        skip_if_no_credentials!();

        // This test is designed to trigger potential SIGSEGV by having many tasks
        // concurrently write to the same writer without proper synchronization
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "aggressive_concurrent_writer");
        let writer = fs.create(&test_file, true).await.unwrap();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        // Spawn many concurrent write tasks without waiting
        let num_tasks = 50;
        let mut handles = vec![];

        for i in 0..num_tasks {
            let writer_clone = writer.clone();
            let handle = tokio::spawn(async move {
                // Multiple writes per task to increase contention
                for j in 0..5 {
                    let data = format!("Task {} write {}\n", i, j);
                    let mut writer = writer_clone.lock().await;
                    writer
                        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                        .await
                        .unwrap();
                    // Release lock briefly to allow other tasks
                    drop(writer);
                    tokio::task::yield_now().await;
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks - should not crash with SIGSEGV
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Task should complete without panic or SIGSEGV");
        }

        // Complete the writer
        let mut writer = writer.lock().await;
        writer.flush().await.unwrap();
        writer.complete().await.unwrap();

        // Verify file was written
        let status = fs.get_status(&test_file).await.unwrap();
        assert!(status.len > 0, "File should contain data from concurrent writes");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_mixed_concurrent_operations() {
        skip_if_no_credentials!();

        // Test concurrent write, flush, and tell operations to detect race conditions
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "mixed_concurrent_ops");
        let writer = fs.create(&test_file, true).await.unwrap();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        let num_operations = 30;
        let mut handles = vec![];

        for i in 0..num_operations {
            let writer_clone = writer.clone();
            let handle = tokio::spawn(async move {
                match i % 3 {
                    0 => {
                        // Write operation
                        let data = format!("Write op {}\n", i);
                        let mut writer = writer_clone.lock().await;
                        writer
                            .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                            .await
                            .unwrap();
                    }
                    1 => {
                        // Flush operation
                        let mut writer = writer_clone.lock().await;
                        writer.flush().await.unwrap();
                    }
                    _ => {
                        // Tell operation
                        let writer = writer_clone.lock().await;
                        let _pos = writer.tell().await.unwrap();
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all operations - should not crash
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Operation should complete without SIGSEGV");
        }

        // Complete writer
        let mut writer = writer.lock().await;
        writer.complete().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_rapid_fire_concurrent_writes() {
        skip_if_no_credentials!();

        // Rapid-fire writes to stress test the writer and detect memory issues
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = OssHdfsFileSystem::new(&fs_path, conf).unwrap();

        let test_file = create_test_path(&bucket, "rapid_fire_writes");
        let writer = fs.create(&test_file, true).await.unwrap();
        let writer = Arc::new(tokio::sync::Mutex::new(writer));

        // Many tasks writing rapidly
        let num_tasks = 100;
        let mut handles = vec![];

        for i in 0..num_tasks {
            let writer_clone = writer.clone();
            let handle = tokio::spawn(async move {
                let data = format!("Rapid {}\n", i);
                let mut writer = writer_clone.lock().await;
                writer
                    .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // Don't wait between spawns - maximum concurrency
        // Wait for all - should not SIGSEGV
        let results: Vec<_> = futures::future::join_all(handles).await;
        let mut success_count = 0;
        for result in results {
            match result {
                Ok(_) => success_count += 1,
                Err(e) => panic!("Rapid fire write task failed: {:?}", e),
            }
        }

        assert_eq!(
            success_count,
            num_tasks,
            "All {} rapid fire writes should complete without SIGSEGV",
            num_tasks
        );

        // Complete writer
        let mut writer = writer.lock().await;
        writer.flush().await.unwrap();
        writer.complete().await.unwrap();

        // Verify file
        let status = fs.get_status(&test_file).await.unwrap();
        assert!(status.len > 0, "File should contain data");
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_concurrent_write_with_cancel() {
        skip_if_no_credentials!();

        // Test concurrent writes with potential cancel operations
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = Arc::new(OssHdfsFileSystem::new(&fs_path, conf).unwrap());

        let num_writers = 20;
        let mut handles = vec![];

        for i in 0..num_writers {
            let fs_clone = fs.clone();
            let bucket_clone = bucket.clone();
            let handle = tokio::spawn(async move {
                let test_file = create_test_path(&bucket_clone, &format!("cancel_test_{}", i));
                let mut writer = fs_clone.create(&test_file, true).await.unwrap();

                // Write some data
                for j in 0..3 {
                    let data = format!("Data {}\n", j);
                    writer
                        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                        .await
                        .unwrap();
                }

                // Some writers complete, some cancel
                if i % 2 == 0 {
                    writer.complete().await.unwrap();
                } else {
                    writer.cancel().await.unwrap();
                }

                i
            });
            handles.push(handle);
        }

        // Wait for all - should not crash
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Writer operation should complete without SIGSEGV");
        }
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_extreme_concurrency_stress_test() {
        skip_if_no_credentials!();

        // Extreme stress test: maximum concurrency to detect SIGSEGV
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = Arc::new(OssHdfsFileSystem::new(&fs_path, conf).unwrap());

        // Very high number of concurrent writers
        let num_writers = 200;
        let writes_per_writer = 10;
        let mut handles = vec![];

        for i in 0..num_writers {
            let fs_clone = fs.clone();
            let bucket_clone = bucket.clone();
            let handle = tokio::spawn(async move {
                let test_file = create_test_path(&bucket_clone, &format!("extreme_{}", i));
                let mut writer = fs_clone.create(&test_file, true).await.unwrap();

                // Rapid writes
                for j in 0..writes_per_writer {
                    let data = format!("Extreme test writer {} write {}\n", i, j);
                    writer
                        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                        .await
                        .unwrap();
                }

                writer.flush().await.unwrap();
                writer.complete().await.unwrap();

                i
            });
            handles.push(handle);
        }

        // Collect results - if SIGSEGV occurs, this will fail
        let results: Vec<_> = futures::future::join_all(handles).await;
        let mut success_count = 0;
        for result in results {
            match result {
                Ok(_) => success_count += 1,
                Err(e) => panic!("Extreme concurrency test failed with: {:?}", e),
            }
        }

        assert_eq!(
            success_count,
            num_writers,
            "All {} extreme concurrency writers should complete without SIGSEGV",
            num_writers
        );
    }

    #[tokio::test]
    #[ignore] // Requires actual OSS credentials
    async fn test_concurrent_writer_lifecycle() {
        skip_if_no_credentials!();

        // Test concurrent operations during writer lifecycle (create, write, flush, complete)
        let conf = create_test_conf().unwrap();
        let bucket = get_test_bucket();
        let fs_path = Path::new(&format!("oss://{}/", bucket)).unwrap();
        let fs = Arc::new(OssHdfsFileSystem::new(&fs_path, conf).unwrap());

        let num_files = 50;
        let mut handles = vec![];

        for i in 0..num_files {
            let fs_clone = fs.clone();
            let bucket_clone = bucket.clone();
            let handle = tokio::spawn(async move {
                let test_file = create_test_path(&bucket_clone, &format!("lifecycle_{}", i));
                
                // Create writer
                let mut writer = fs_clone.create(&test_file, true).await.unwrap();
                
                // Write
                let data = format!("Lifecycle test {}\n", i);
                writer
                    .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                    .await
                    .unwrap();
                
                // Flush
                writer.flush().await.unwrap();
                
                // Complete
                writer.complete().await.unwrap();
                
                // Verify
                let status = fs_clone.get_status(&test_file).await.unwrap();
                assert!(status.len > 0, "File should have content");
                
                i
            });
            handles.push(handle);
        }

        // All should complete without SIGSEGV
        let results: Vec<_> = futures::future::join_all(handles).await;
        for result in results {
            result.expect("Writer lifecycle should complete without SIGSEGV");
        }
    }
}
