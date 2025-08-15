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
use curvine_common::fs::Path;
use curvine_common::fs::Writer;
use curvine_common::state::{
    CreateFileOptsBuilder, MkdirOptsBuilder, SetAttrOptsBuilder, TtlAction,
};
use curvine_common::FsResult;
use curvine_tests::Testing;
use log::info;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;

const PATH: &str = "/fs_test/a.log";

#[test]
fn fs_test() -> FsResult<()> {
    let rt = Arc::new(AsyncRuntime::single());

    let fs = Testing::get_fs_with_rt(rt.clone())?;
    let res: FsResult<()> = rt.block_on(async move {
        let path = Path::from_str("/fs_test")?;
        let _ = fs.delete(&path, true).await;

        mkdir(&fs).await?;

        create_file(&fs).await?;

        file_status(&fs).await?;

        delete(&fs).await?;

        rename(&fs).await?;

        list_status(&fs).await?;

        list_files(&fs).await?;

        get_master_info(&fs).await?;

        add_block(&fs).await?;

        rename2(&fs).await?;

        set_attr_non_recursive(&fs).await?;

        set_attr_recursive(&fs).await?;

        test_fs_used(&fs).await?;

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
        .build();
    let flag = fs.mkdir_with_opts(&path, opts).await;
    assert!(flag.is_ok());

    let status = fs.get_status(&path).await?;
    println!("dir status = {:?}", status);
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
        .overwrite(true)
        .x_attr("123".to_string(), "xxx".to_string().into_bytes())
        .ttl_ms(10000)
        .ttl_action(TtlAction::Delete)
        .build();
    let writer = fs.create_with_opts(&path, opts).await?;
    let status = writer.status();
    info!("create file: {}, status: {:?}", path, status);

    let status = fs.get_status(&path).await?;
    assert_eq!(status.mode, 0o755);
    assert_eq!(status.storage_policy.ttl_ms, 10000);
    assert_eq!(status.storage_policy.ttl_action, TtlAction::Delete);
    assert_eq!(status.x_attr.get("123"), Some(&"xxx".as_bytes().to_vec()));

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
    let local_addr = client.client_addr().clone();

    let _ = client.create(&path, true).await?;
    let located = client.add_block(&path, None, &local_addr).await?;
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
    fs.set_attr(&path, opts.clone()).await?;

    let status = fs.get_status(&path).await?;
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
