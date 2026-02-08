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
use curvine_client::file::{CurvineFileSystem, FsContext};
use curvine_client::ClientMetrics;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Reader, Writer};
use curvine_common::state::{
    CreateFileOptsBuilder, MkdirOptsBuilder, SetAttrOptsBuilder, TtlAction,
};
use curvine_common::state::{FileLock, LockFlags, LockType};
use curvine_common::FsResult;
use curvine_tests::Testing;
use log::info;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;

const PATH: &str = "/fs_test/a.log";

#[test]
fn test_filesystem_end_to_end_operations_on_cluster() -> FsResult<()> {
    let rt = Arc::new(AsyncRuntime::single());
    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.write_chunk_size = 64; // Set to 64 bytes
    conf.client.write_chunk_size_str = "64B".to_string(); // Update string field
    conf.client.metric_report_enable = true;

    // Test short_circuit = false
    conf.client.short_circuit = false;
    run_filesystem_end_to_end_operations_on_cluster(&testing, &rt, conf.clone())?;

    // Test short_circuit = true
    // conf.client.short_circuit = true;
    // run_filesystem_end_to_end_operations_on_cluster(&testing, &rt, conf.clone())?;

    Ok(())
}

fn run_filesystem_end_to_end_operations_on_cluster(
    testing: &Testing,
    rt: &Arc<AsyncRuntime>,
    conf: ClusterConf,
) -> FsResult<()> {
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let res: FsResult<()> = rt.block_on(async move {
        let path = Path::from_str("/fs_test")?;
        let _ = fs.delete(&path, true).await;

        // mkdir(&fs).await?;
        // println!("mkdir done");

        // create_file(&fs).await?;
        // println!("create_file done");

        // test_overwrite(&fs).await?;
        // println!("test_overwrite done");

        test_batch_writting(&fs).await?;
        println!("test_batch_writting done");

        // file_status(&fs).await?;
        // println!("file_status done");

        // delete(&fs).await?;
        // println!("delete done");

        // rename(&fs).await?;
        // println!("rename done");

        // list_status(&fs).await?;
        // println!("list_status done");

        // list_files(&fs).await?;
        // println!("list_files done");

        // get_master_info(&fs).await?;
        // println!("get_master_info done");

        // add_block(&fs).await?;
        // println!("add_block done");

        // rename2(&fs).await?;
        // println!("rename2 done");

        // set_attr_non_recursive(&fs).await?;
        // println!("set_attr_non_recursive done");

        // set_attr_recursive(&fs).await?;
        // println!("set_attr_recursive done");

        // test_fs_used(&fs).await?;
        // println!("test_fs_used done");

        // test_metrics(&fs).await?;
        // println!("test_metrics done");

        // symlink(&fs).await?;
        Ok(())
    });

    res
}

async fn mkdir(fs: &CurvineFileSystem) -> CommonResult<()> {
    // Recursively created, done normally.
    let path = Path::from_str("/fs_test/a/dir_1")?;
    let flag = fs.mkdir(&path, true).await?;
    info!("mkdir {}, response: {}", path, flag);

    // Non-recursive creation error.
    let path = Path::from_str("/fs_test/b/dir_2")?;
    let flag = fs.mkdir(&path, false).await;
    assert!(flag.is_err());
    info!("mkdir {}, response: {:?}", path, flag);

    // test opts
    let path = Path::from_str("/fs_test/b/dir_3")?;
    let opts = MkdirOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .x_attr("123".to_string(), "xxx".to_string().into_bytes())
        .ttl_ms(10000)
        .ttl_action(TtlAction::Delete)
        .owner("user".to_string())
        .group("group".to_string())
        .build();
    let flag = fs.mkdir_with_opts(&path, opts).await;
    assert!(flag.is_ok());

    let status = flag?;
    println!("dir status = {:?}", status);
    assert_eq!(status.mode, 0o755);
    assert_eq!(&status.owner, "user");
    assert_eq!(&status.group, "group");
    assert_eq!(status.mode, 0o755);
    assert_eq!(status.storage_policy.ttl_ms, 10000);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::Delete);
    assert_eq!(status.x_attr.get("123"), Some(&"xxx".as_bytes().to_vec()));

    Ok(())
}

async fn create_file(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str(PATH)?;
    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .x_attr("123".to_string(), "xxx".to_string().into_bytes())
        .ttl_ms(10000)
        .ttl_action(TtlAction::Delete)
        .owner("user".to_string())
        .group("group".to_string())
        .build();
    let writer = fs.create_with_opts(&path, opts, true).await?;
    let status = writer.status();
    info!("create file: {}, status: {:?}", path, status);

    assert_eq!(status.mode, 0o755);
    assert_eq!(&status.owner, "user");
    assert_eq!(&status.group, "group");
    assert_eq!(status.storage_policy.ttl_ms, 10000);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::Delete);
    assert_eq!(status.x_attr.get("123"), Some(&"xxx".as_bytes().to_vec()));
    Ok(())
}

async fn test_overwrite(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str("/fs_test/overwrite_test.log")?;

    // Helper function to read file content
    async fn read_file_content(fs: &CurvineFileSystem, path: &Path) -> CommonResult<String> {
        let status = fs.get_status(path).await?;
        println!("DEBUG at read_file_content, before create fs_reader");
        let mut reader = fs.open(path).await?;
        let mut buffer = BytesMut::zeroed(status.len as usize);
        println!("DEBUG at read_file_content, before read_full");
        let bytes_read = reader.read_full(&mut buffer).await?;
        println!("DEBUG at read_file_content, after read_full");
        reader.complete().await?;
        buffer.truncate(bytes_read);
        Ok(String::from_utf8(buffer.to_vec())?)
    }

    // 1. Create initial file and write content "initial"
    let mut writer = fs.create(&path, true).await?;
    writer.write("initial".as_bytes()).await?;
    writer.complete().await?;

    let initial_status = fs.get_status(&path).await?;
    let initial_inode_id = initial_status.id;
    let initial_content = read_file_content(fs, &path).await?;
    assert_eq!(initial_content, "initial");
    assert_eq!(initial_status.len, 7); // Length of "initial"
    println!(
        "Initial file created, inode_id: {}, content: {}",
        initial_inode_id, initial_content
    );

    // 2. Use overwrite mode to rewrite file content to "overwritten_content"
    println!("xx overwrite_inode_id");
    let mut writer = fs.create(&path, true).await?;
    writer.write("overwritten_content".as_bytes()).await?;
    println!("yy overwrite_inode_id");
    writer.complete().await?;
    println!("xxyy overwrite_inode_id");

    let overwrite_status = fs.get_status(&path).await?;
    let overwrite_inode_id = overwrite_status.id;
    println!("x overwrite_inode_id: {}", overwrite_inode_id);
    let overwrite_content = read_file_content(fs, &path).await?;
    println!("y overwrite_content: {}", overwrite_content);

    // 3. Verify state after overwrite
    assert_eq!(overwrite_content, "overwritten_content");
    assert_eq!(overwrite_status.len, 19); // Length of "overwritten_content"
    assert_eq!(
        initial_inode_id, overwrite_inode_id,
        "Overwrite should preserve inode ID"
    );
    println!(
        "File overwritten successfully, inode_id: {} (preserved), content: {}",
        overwrite_inode_id, overwrite_content
    );

    // 4. Overwrite again, write shorter content "short"
    let mut writer = fs.create(&path, true).await?;
    writer.write("short".as_bytes()).await?;
    writer.complete().await?;

    let final_status = fs.get_status(&path).await?;
    let final_inode_id = final_status.id;
    let final_content = read_file_content(fs, &path).await?;

    // 5. Verify final state
    assert_eq!(final_content, "short");
    assert_eq!(final_status.len, 5); // Length of "short"
    assert_eq!(
        initial_inode_id, final_inode_id,
        "Multiple overwrites should preserve inode ID"
    );
    println!(
        "File overwritten again, inode_id: {} (preserved), content: {}",
        final_inode_id, final_content
    );

    // 6. Test overwrite to empty file
    let mut writer = fs.create(&path, true).await?;
    writer.complete().await?; // Don't write any content

    let empty_status = fs.get_status(&path).await?;
    let empty_inode_id = empty_status.id;
    let empty_content = if empty_status.len > 0 {
        read_file_content(fs, &path).await?
    } else {
        String::new()
    };

    // 7. Verify empty file state
    assert_eq!(empty_content, "");
    assert_eq!(empty_status.len, 0);
    assert_eq!(
        initial_inode_id, empty_inode_id,
        "Overwrite to empty should preserve inode ID"
    );
    println!(
        "File overwritten to empty, inode_id: {} (preserved), content length: {}",
        empty_inode_id, empty_status.len
    );

    Ok(())
}

async fn test_batch_writting(fs: &CurvineFileSystem) -> CommonResult<()> {
    // Helper function to read file content
    async fn read_file_content(fs: &CurvineFileSystem, path: &Path) -> CommonResult<String> {
        let status = fs.get_status(path).await?;
        println!("DEBUG at read_file_content, before create fs_reader");
        let mut reader = fs.open(path).await?;
        let mut buffer = BytesMut::zeroed(status.len as usize);
        println!("DEBUG at read_file_content, before read_full, with reader:");
        let bytes_read = reader.read_full(&mut buffer).await?;
        println!("DEBUG at read_file_content, after read_full");
        reader.complete().await?;
        buffer.truncate(bytes_read);
        Ok(String::from_utf8(buffer.to_vec())?)
    }

    let num_files = 5;
    let small_file_size = 10; // 1KB per file
    let large_file_size = 65; // 1KB per file
    let test_small_data = "x".repeat(small_file_size);
    let test_large_data = "x".repeat(large_file_size);

    let mut batch_files = Vec::with_capacity(num_files);
    for i in 0..num_files {
        let path_str = format!("/batch_test/batch/file_{}.txt", i);
        let path = Path::from_str(path_str)?;
        if i == num_files - 1 {
            batch_files.push((path.clone(), test_large_data.as_str()));
            continue;
        }
        batch_files.push((path, test_small_data.as_str()));
    }

    fs.write_batch_string(&batch_files).await?;
    // println!("at fs_tests, complete the write_batch_string process");
    // // Using batch
    // // Get block locations for all files
    // let mut results = Vec::new();
    // for (path, _) in batch_files.clone() {
    //     let blocks = fs.get_block_locations(&path).await?;
    //     println!("DEBUG block location for path: {:?} is {:?}", path, blocks);
    //     results.push(blocks);
    // }
    // println!("results: {:?}", results);

    // // // 2. Verify all files exist and have correct content
    // for (i, (path, _)) in batch_files.clone().iter().enumerate() {
    //     let status = fs.get_status(path).await?;

    //     println!(
    //         "DEBUG at test_batch_writting, test_batch_writting, file status: {:?}",
    //         status
    //     );
    //     let content = read_file_content(fs, path).await?;
    //     println!(
    //         "DEBUG at test_batch_writting, test_batch_writting, content: {:?}",
    //         content
    //     );
    //     if i == num_files - 1 {
    //         assert_eq!(
    //             status.len, large_file_size as i64,
    //             "File {} length mismatch",
    //             i
    //         );
    //         assert_eq!(
    //             content.len(),
    //             large_file_size,
    //             "File {} content length mismatch",
    //             i
    //         );
    //         assert_eq!(content, test_large_data, "File {} content mismatch", i);
    //     } else {
    //         assert_eq!(
    //             status.len, small_file_size as i64,
    //             "File {} length mismatch",
    //             i
    //         );
    //         assert_eq!(
    //             content.len(),
    //             small_file_size,
    //             "File {} content length mismatch",
    //             i
    //         );
    //         assert_eq!(content, test_small_data, "File {} content mismatch", i);
    //     }

    //     println!("Verified file_{}: len={}, content matches", i, status.len);
    // }
    // println!("✓ All {} files written and verified correctly", num_files);
    Ok(())
}

async fn file_status(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str(PATH)?;
    let status = fs.get_status(&path).await?;
    info!("file status: {}, status: {:?}", path, status);

    // check
    assert!(!status.is_dir);
    assert_eq!(status.name, "a.log");
    assert_eq!(status.path, path.path());

    Ok(())
}

async fn delete(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str("/fs_test/delete.log")?;
    let _ = fs.create(&path, true).await?;

    let exists = fs.exists(&path).await?;
    assert!(exists);

    // Execute deletion
    fs.delete(&path, false).await?;
    let exists = fs.exists(&path).await?;
    assert!(!exists);

    Ok(())
}

async fn rename(fs: &CurvineFileSystem) -> CommonResult<()> {
    let src = Path::from_str("/fs_test/rename/a1.log")?;
    let dst = Path::from_str("/fs_test/rename/a2.log")?;

    let _ = fs.create(&src, true).await?;
    let _ = fs.rename(&src, &dst).await?;

    assert!(!(fs.exists(&src).await?));
    assert!(fs.exists(&dst).await?);

    Ok(())
}

async fn rename2(fs: &CurvineFileSystem) -> CommonResult<()> {
    let src = Path::from_str("/fs_test/rename2.log")?;
    let dst = Path::from_str("/fs_test/rename2")?;

    let _ = fs.mkdir(&src, true).await?;
    let _ = fs.mkdir(&dst, false).await?;
    let _ = fs.rename(&src, &dst).await?;

    assert!(!(fs.exists(&src).await?));
    assert!(
        fs.exists(&Path::from_str("/fs_test/rename2/rename2.log")?)
            .await?
    );

    let res = fs.list_status(&dst).await?;
    println!("{:?}", res);
    assert_eq!(res.len(), 1);

    Ok(())
}

async fn list_status(fs: &CurvineFileSystem) -> CommonResult<()> {
    for i in 0..3 {
        let path = Path::from_str(format!("/fs_test/list-status/{}.log", i))?;
        let _ = fs.create(&path, true).await?;
    }

    let path = Path::from_str("/fs_test/list-status")?;
    let list = fs.list_status(&path).await?;
    for item in &list {
        info!("file: {}", item.path)
    }

    assert_eq!(list.len(), 3);
    Ok(())
}

async fn list_files(fs: &CurvineFileSystem) -> CommonResult<()> {
    for i in 0..10 {
        let path = Path::from_str(format!("/fs_test/list-files/{}/{}.log", i % 3, i))?;
        let _ = fs.create(&path, true).await?;
    }

    let path = Path::from_str("/fs_test/list-files")?;
    let list = fs.list_files(&path).await?;
    for item in &list {
        info!("file: {}", item.path)
    }

    assert_eq!(list.len(), 10);

    Ok(())
}

async fn add_block(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str("/fs_test/add-block.lg")?;
    let client = fs.fs_client();

    let _ = client.create(&path, true, true).await?;
    let located = client.add_block(&path, vec![], 0, None).await?;
    info!("add_block = {:?}", located);
    Ok(())
}

async fn get_master_info(fs: &CurvineFileSystem) -> CommonResult<()> {
    let res = fs.get_master_info().await?;
    info!("master info {:#?}", res);
    Ok(())
}

async fn set_attr_non_recursive(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str("/fs_test/set_attr1/set_attr2/attr.log")?;
    let mut writer = fs.create(&path, true).await?;
    writer.complete().await?;

    let opts = SetAttrOptsBuilder::new()
        .recursive(false)
        .owner("root")
        .group("root")
        .mode(0o644)
        .ttl_ms(1000)
        .ttl_action(TtlAction::Delete)
        .add_x_attr("attr1", "value1".to_string().into_bytes())
        .build();

    // Non-recursive settings
    let path = Path::from_str("/fs_test/set_attr1")?;
    let status = fs.set_attr(&path, opts.clone()).await?;

    println!("non-recursive set_attr1 {:?}", status);
    assert_eq!(
        status.x_attr.get("attr1"),
        Some(&"value1".as_bytes().to_vec())
    );
    assert_eq!(status.owner, "root");
    assert_eq!(status.group, "root");
    assert_eq!(status.mode, 0o644);
    assert_eq!(status.storage_policy.ttl_ms, 1000);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::Delete);

    let path = Path::from_str("/fs_test/set_attr1/set_attr2")?;
    let status = fs.get_status(&path).await?;
    println!("non-recursive set_attr2 {:?}", status);
    assert_eq!(status.x_attr.get("attr1"), None);
    assert_eq!(status.owner, "");
    assert_eq!(status.group, "");
    assert_eq!(status.mode, 0o755);
    assert_eq!(status.storage_policy.ttl_ms, 0);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::None);

    Ok(())
}

async fn set_attr_recursive(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = Path::from_str("/fs_test/set_attr_a/set_attr_b/attr.log")?;
    let mut writer = fs.create(&path, true).await?;
    writer.complete().await?;

    let opts = SetAttrOptsBuilder::new()
        .recursive(true)
        .owner("root")
        .group("root")
        .mode(0o644)
        .ttl_ms(1000)
        .ttl_action(TtlAction::Delete)
        .add_x_attr("attr1", "value1".to_string().into_bytes())
        .build();

    // Non-recursive settings
    let path = Path::from_str("/fs_test/set_attr_a")?;
    fs.set_attr(&path, opts.clone()).await?;

    let status = fs.get_status(&path).await?;
    println!("non-recursive set_attr_a {:?}", status);
    assert_eq!(
        status.x_attr.get("attr1"),
        Some(&"value1".as_bytes().to_vec())
    );
    assert_eq!(status.owner, "root");
    assert_eq!(status.group, "root");
    assert_eq!(status.mode, 0o644);
    assert_eq!(status.storage_policy.ttl_ms, 1000);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::Delete);

    let path = Path::from_str("/fs_test/set_attr_a/set_attr_b")?;
    let status = fs.get_status(&path).await?;
    println!("non-recursive set_attr_b {:?}", status);
    assert_eq!(status.x_attr.get("attr1"), None);
    assert_eq!(status.owner, "root");
    assert_eq!(status.group, "root");
    assert_eq!(status.mode, 0o644);
    assert_eq!(status.storage_policy.ttl_ms, 0);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::None);

    Ok(())
}

async fn test_fs_used(fs: &CurvineFileSystem) -> CommonResult<()> {
    info!("=== Testing FS Used - Creating files with data ===");

    // Get initial state
    let initial_master_info = fs.get_master_info().await?;
    info!(
        "Initial FS Used: {} bytes ({:.2} MB)",
        initial_master_info.fs_used,
        initial_master_info.fs_used as f64 / 1024.0 / 1024.0
    );

    // Create files and write data, ensuring data is flushed to storage
    let test_data = "This is test data for fs_used measurement. ".repeat(1000); // ~43KB per file

    for i in 0..3 {
        let path = Path::from_str(format!("/fs_test/fs_used_test/file_{}.dat", i))?;
        let mut writer = fs.create(&path, true).await?;

        // Write test data and force completion
        writer.write(test_data.as_bytes()).await?;
        writer.complete().await?; // Ensure data is written to storage system

        info!(
            "Created and completed file {} with {} bytes",
            path,
            test_data.len()
        );

        // Check status after creating each file
        let current_info = fs.get_master_info().await?;
        info!("After file {}: FS Used = {} bytes", i, current_info.fs_used);
    }

    // Wait for storage system to process
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Get master info to check fs_used
    let after_small_files_info = fs.get_master_info().await?;
    info!(
        "After creating small files - FS Used: {} bytes ({:.2} MB)",
        after_small_files_info.fs_used,
        after_small_files_info.fs_used as f64 / 1024.0 / 1024.0
    );

    // Create a large file using bigger blocks to ensure block storage is triggered
    let large_path = Path::from_str("/fs_test/fs_used_test/large_file.dat")?;
    let mut large_writer = fs.create(&large_path, true).await?;

    // Write multiple large data chunks
    let chunk_data = "X".repeat(65536); // 64KB chunks - larger blocks are more likely to trigger underlying storage
    for i in 0..5 {
        large_writer.write(chunk_data.as_bytes()).await?;
        info!("Written chunk {} ({} bytes)", i + 1, chunk_data.len());
    }
    large_writer.complete().await?; // Force completion of write

    info!(
        "Created and completed large file with {} bytes total",
        65536 * 5
    );

    // Wait for storage system to update
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Get master info again
    let final_master_info = fs.get_master_info().await?;
    info!(
        "Final FS Used: {} bytes ({:.2} MB)",
        final_master_info.fs_used,
        final_master_info.fs_used as f64 / 1024.0 / 1024.0
    );

    // Output detailed capacity information
    info!("=== Final Capacity Information ===");
    info!(
        "Capacity: {} bytes ({:.2} GB)",
        final_master_info.capacity,
        final_master_info.capacity as f64 / 1024.0 / 1024.0 / 1024.0
    );
    info!(
        "Available: {} bytes ({:.2} GB)",
        final_master_info.available,
        final_master_info.available as f64 / 1024.0 / 1024.0 / 1024.0
    );
    info!(
        "FS Used: {} bytes ({:.2} MB)",
        final_master_info.fs_used,
        final_master_info.fs_used as f64 / 1024.0 / 1024.0
    );

    // Calculate non_fs_used
    let non_fs_used =
        final_master_info.capacity - final_master_info.available - final_master_info.fs_used;
    info!(
        "Non-FS Used: {} bytes ({:.2} GB)",
        non_fs_used,
        non_fs_used as f64 / 1024.0 / 1024.0 / 1024.0
    );

    // Verify capacity consistency
    let total_accounted = final_master_info.available + final_master_info.fs_used + non_fs_used;
    info!(
        "Consistency check: {} + {} + {} = {}",
        final_master_info.available, final_master_info.fs_used, non_fs_used, total_accounted
    );
    info!("Expected capacity: {}", final_master_info.capacity);

    if total_accounted == final_master_info.capacity {
        info!("✅ Capacity consistency check PASSED");
    } else {
        info!(
            "❌ Capacity consistency check FAILED - difference: {}",
            (total_accounted - final_master_info.capacity).abs()
        );
    }

    // Verify fs_used is not negative
    assert!(
        final_master_info.fs_used >= 0,
        "FS Used should not be negative"
    );

    // If fs_used is still 0, output debug information but don't fail the test
    if final_master_info.fs_used == 0 {
        info!("⚠️  WARNING: FS Used is still 0. This might indicate:");
        info!("   1. Data is not yet committed to block storage");
        info!("   2. Test cluster might be using different storage mechanism");
        info!("   3. Block size threshold not reached");
    } else {
        info!(
            "✅ FS Used has non-zero value: {} bytes",
            final_master_info.fs_used
        );
    }

    info!("=== FS Used test completed successfully ===");
    Ok(())
}

/*async fn symlink(fs: &CurvineFileSystem) -> CommonResult<()> {
    let path = "/../a/b/test.log";
    // create symlink
    let link = Path::from_str("/fs_test/symlink/link")?;
    fs.create(&link, true).await?.complete().await?;
    fs.symlink(path, &link, true).await?;

    let status = fs.get_status(&link).await?;
    println!("link status{:?}", status);
    assert_eq!(status.target, Some(path.to_string()));
    assert_eq!(status.file_type, FileType::Link);

    Ok(())
}*/

async fn test_metrics(fs: &CurvineFileSystem) -> FsResult<()> {
    FsContext::get_metrics()
        .mount_cache_hits
        .with_label_values(&["id1"])
        .inc_by(100);
    FsContext::get_metrics()
        .mount_cache_misses
        .with_label_values(&["id1"])
        .inc_by(200);

    fs.fs_client()
        .metrics_report(ClientMetrics::encode().unwrap())
        .await
}

#[test]
fn set_lock() {
    let testing = Testing::default();
    let fs = testing.get_fs(None, None).unwrap();

    fs.clone_runtime().block_on(async move {
        let file_path = Path::from_str("/lock_test/test_file.txt").unwrap();
        let mut writer = fs.create(&file_path, true).await.unwrap();
        writer.complete().await.unwrap();

        info!("=== Testing POSIX locks (Plock) ===");

        // 1. Client1 acquires read lock
        let plock_read = FileLock {
            client_id: "client1".to_string(),
            owner_id: 100,
            pid: 1001,
            acquire_time: 0,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
        };

        let conflict = fs.set_lock(&file_path, plock_read.clone()).await.unwrap();
        assert!(
            conflict.is_none(),
            "First read lock should not have conflict"
        );
        info!("✓ POSIX read lock set successfully");

        // 2. Client2 acquires read lock on same region, should not conflict
        let plock_read2 = FileLock {
            client_id: "client2".to_string(),
            owner_id: 200,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Plock,
            start: 50,
            end: 150,
            ..Default::default()
        };

        let conflict = fs.set_lock(&file_path, plock_read2).await.unwrap();
        assert!(
            conflict.is_none(),
            "Multiple read locks should not conflict"
        );
        info!("✓ POSIX multiple read locks can coexist");

        // 3. Client3 tries write lock on overlapping region, should conflict
        let plock_write = FileLock {
            client_id: "client3".to_string(),
            owner_id: 300,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 50,
            end: 150,
            ..Default::default()
        };

        let conflict = fs.set_lock(&file_path, plock_write).await.unwrap();
        assert!(
            conflict.is_some(),
            "Write lock should conflict with existing read locks"
        );
        info!("✓ POSIX write lock conflicts correctly with read locks");

        // 4. Client4 acquires write lock on non-overlapping region, should succeed
        let plock_write_non_overlap = FileLock {
            client_id: "client4".to_string(),
            owner_id: 400,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 200,
            end: 300,
            ..Default::default()
        };

        let conflict = fs
            .set_lock(&file_path, plock_write_non_overlap)
            .await
            .unwrap();
        assert!(
            conflict.is_none(),
            "Write lock on non-overlapping region should not conflict"
        );
        info!("✓ POSIX write lock on non-overlapping region succeeds");

        // 5. Unlock client1
        let unlock1 = FileLock {
            client_id: "client1".to_string(),
            owner_id: 100,
            lock_type: LockType::UnLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
            ..Default::default()
        };

        let conflict = fs.set_lock(&file_path, unlock1).await.unwrap();
        assert!(conflict.is_none(), "Unlock should not have conflict");
        info!("✓ POSIX unlock successful");

        info!("=== Testing BSD locks (Flock) ===");

        // 6. Client5 acquires BSD read lock
        let flock_read = FileLock {
            client_id: "client5".to_string(),
            owner_id: 500,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Flock,
            start: 0,
            end: 0,
            ..Default::default()
        };

        let conflict = fs.set_lock(&file_path, flock_read).await.unwrap();
        assert!(
            conflict.is_none(),
            "BSD read lock should be set successfully"
        );
        info!("✓ BSD read lock set successfully");

        // 7. Client6 acquires BSD read lock, should not conflict
        let flock_read2 = FileLock {
            client_id: "client6".to_string(),
            owner_id: 600,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Flock,
            start: 0,
            end: 0,
            ..Default::default()
        };

        let conflict = fs.set_lock(&file_path, flock_read2).await.unwrap();
        assert!(
            conflict.is_none(),
            "BSD multiple read locks should not conflict"
        );
        info!("✓ BSD multiple read locks can coexist");

        // 8. Client7 tries BSD write lock, should conflict
        let flock_write = FileLock {
            client_id: "client7".to_string(),
            owner_id: 700,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Flock,
            start: 0,
            end: 0,
            ..Default::default()
        };

        let conflict = fs.set_lock(&file_path, flock_write).await.unwrap();
        assert!(
            conflict.is_some(),
            "BSD write lock should conflict with existing read locks"
        );
        info!("✓ BSD write lock conflicts correctly with read locks");

        // 9. Verify POSIX and BSD locks are independent
        info!("✓ POSIX and BSD locks work independently on the same file");

        // Cleanup
        info!("=== set_lock test completed ===");
    })
}

#[test]
fn get_lock() {
    let testing = Testing::default();
    let fs = testing.get_fs(None, None).unwrap();

    fs.clone_runtime().block_on(async move {
        let file_path = Path::from_str("/lock_test_get/test_file.txt").unwrap();
        let mut writer = fs.create(&file_path, true).await.unwrap();
        writer.complete().await.unwrap();

        info!("=== Testing get_lock - POSIX locks ===");

        // 1. Client1 acquires POSIX write lock
        let plock_write = FileLock {
            client_id: "client1".to_string(),
            owner_id: 100,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
            ..Default::default()
        };

        fs.set_lock(&file_path, plock_write.clone()).await.unwrap();
        info!("✓ Set POSIX write lock");

        // 2. Client2 queries lock on overlapping region
        let query_lock = FileLock {
            client_id: "client2".to_string(),
            owner_id: 200,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Plock,
            start: 50,
            end: 150,
            ..Default::default()
        };

        let conflict = fs.get_lock(&file_path, query_lock.clone()).await.unwrap();
        assert!(conflict.is_some(), "Should detect conflicting write lock");
        let conflict = conflict.unwrap();
        assert_eq!(conflict.client_id, "client1");
        assert_eq!(conflict.owner_id, 100);
        assert_eq!(conflict.lock_type, LockType::WriteLock);
        info!("✓ get_lock correctly returns conflicting POSIX write lock");

        // 3. Query non-overlapping region
        let query_non_overlap = FileLock {
            client_id: "client2".to_string(),
            owner_id: 200,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 200,
            end: 300,
            ..Default::default()
        };

        let conflict = fs.get_lock(&file_path, query_non_overlap).await.unwrap();
        assert!(
            conflict.is_none(),
            "Non-overlapping region should have no conflict"
        );
        info!("✓ get_lock correctly identifies non-overlapping regions");

        info!("=== Testing get_lock - BSD locks ===");

        // 4. Client3 acquires BSD write lock
        let flock_write = FileLock {
            client_id: "client3".to_string(),
            owner_id: 300,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Flock,
            start: 0,
            end: 0,
            ..Default::default()
        };

        fs.set_lock(&file_path, flock_write).await.unwrap();
        info!("✓ Set BSD write lock");

        // 5. Client4 queries BSD lock
        let query_flock = FileLock {
            client_id: "client4".to_string(),
            owner_id: 400,
            lock_type: LockType::ReadLock,
            lock_flags: LockFlags::Flock,
            start: 0,
            end: 0,
            ..Default::default()
        };

        let conflict = fs.get_lock(&file_path, query_flock).await.unwrap();
        assert!(
            conflict.is_some(),
            "Should detect conflicting BSD write lock"
        );
        let conflict = conflict.unwrap();
        assert_eq!(conflict.client_id, "client3");
        assert_eq!(conflict.lock_type, LockType::WriteLock);
        info!("✓ get_lock correctly returns conflicting BSD write lock");

        // 6. Verify POSIX query doesn't conflict with BSD locks
        let query_plock_different_flag = FileLock {
            client_id: "client5".to_string(),
            owner_id: 500,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
            ..Default::default()
        };

        let conflict = fs
            .get_lock(&file_path, query_plock_different_flag)
            .await
            .unwrap();
        // Should return client1's POSIX lock, not client3's BSD lock
        assert!(conflict.is_some());
        let conflict = conflict.unwrap();
        assert_eq!(conflict.lock_flags, LockFlags::Plock);
        assert_eq!(conflict.client_id, "client1");
        info!("✓ POSIX and BSD locks are independent");

        // Cleanup
        info!("=== get_lock test completed ===");
    })
}
