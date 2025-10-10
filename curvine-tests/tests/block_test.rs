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
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::fs::Reader;
use curvine_common::fs::Writer;
use curvine_tests::Testing;
use log::info;
use orpc::common::{LocalTime, Utils};
use orpc::runtime::RpcRuntime;
use orpc::{CommonError, CommonResult};
use std::sync::Arc;
use std::time::Duration;

// Test local short-circuit read and write
#[test]
fn local() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = true;
    let path = Path::from_str("/file_local.data")?;
    run(conf, path)
}

#[test]
fn remote() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = false;
    let path = Path::from_str("/file_remote.data")?;
    run(conf, path)
}

#[test]
fn remote_parallel_1() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = false;

    conf.client.write_chunk_num = 1;
    conf.client.write_chunk_size = 65536;

    conf.client.read_chunk_num = 1;
    conf.client.read_parallel = 1;
    conf.client.read_chunk_size = 65536;

    let path = Path::from_str("/file_remote_parallel_1.data")?;
    run(conf, path)
}

#[test]
fn remote_parallel_4() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = false;

    conf.client.write_chunk_num = 4;
    conf.client.write_chunk_size = 65536;

    conf.client.read_chunk_num = 4;
    conf.client.read_parallel = 4;
    conf.client.read_chunk_size = 65536;

    let path = Path::from_str("/file_remote_parallel_4.data")?;
    run(conf, path)
}

#[test]
fn remote_parallel_4_cache() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = false;

    conf.client.write_chunk_num = 4;
    conf.client.write_chunk_size = 65536;

    conf.client.read_chunk_num = 4;
    conf.client.read_parallel = 4;
    conf.client.read_chunk_size = 65536;

    let path = Path::from_str("/file_remote_parallel_4_cache.data")?;
    run(conf, path)
}

#[test]
fn replicas_3() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = true;
    conf.client.replicas = 3;
    let path = Path::from_str("/replicas_3.data")?;

    conf.client.block_size = 1024 * 1024;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = Testing::get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let (write_len, write_ck) = write(&fs, &path).await?;
        let (read_len, read_ck) = read(&fs, &path).await?;
        assert_eq!(write_len, read_len);
        assert_eq!(write_ck, read_ck);

        let locate = fs.get_block_locations(&path).await?;
        println!("locates {:#?}", locate);
        for loc in locate.block_locs {
            assert_eq!(loc.locs.len(), 3);
        }

        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}

#[test]
fn append_local() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = true;
    conf.client.replicas = 2;
    let path = Path::from_str("/append_local.data")?;
    append(conf, path)
}

#[test]
fn append_remote() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.replicas = 2;
    let path = Path::from_str("/append_remote.data")?;
    append(conf, path)
}

fn append(mut conf: ClusterConf, path: Path) -> CommonResult<()> {
    conf.client.block_size = 1024 * 1024;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = Testing::get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        fs.write_string(&path, "123").await.unwrap();
        fs.append_string(&path, "abc").await.unwrap();
        let str = fs.read_string(&path).await?;

        println!("append data {}", str);
        assert_eq!("123abc", str);

        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}

// @todo cannot be completed in parallel tests, follow-up optimization.
fn _abort() -> CommonResult<()> {
    let conf = Testing::get_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = Testing::get_fs(Some(rt.clone()), Some(conf))?;

    let path = Path::from_str("/file-abort.log")?;

    rt.block_on(async move {
        let before = fs.get_master_info().await?.available;
        let mut writer = fs.create(&path, true).await?;
        writer.write("123".as_bytes()).await?;
        writer.flush().await?;
        drop(writer);

        fs.delete(&path, false).await?;

        tokio::time::sleep(Duration::from_secs(10)).await;
        let after = fs.get_master_info().await?.available;

        println!("before {}, after {}", before, after);
        assert_eq!(before, after);
        Ok::<(), CommonError>(())
    })
    .unwrap();

    Ok(())
}

fn run(mut conf: ClusterConf, path: Path) -> CommonResult<()> {
    conf.client.block_size = 1024 * 1024;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = Testing::get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let (write_len, write_ck) = write(&fs, &path).await?;
        let (read_len, read_ck) = read(&fs, &path).await?;
        assert_eq!(write_len, read_len);
        assert_eq!(write_ck, read_ck);

        seek(&fs, &path).await?;

        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}

async fn write(fs: &CurvineFileSystem, path: &Path) -> CommonResult<(u64, u64)> {
    let mut writer = fs.create(path, true).await?;
    let mut checksum: u64 = 0;
    let mut len = 0;

    for _ in 0..10240 {
        let str = Utils::rand_str(1024);
        checksum += Utils::crc32(str.as_bytes()) as u64;
        writer.write(str.as_bytes()).await?;
        len += str.len()
    }

    let time = LocalTime::now_datetime();
    checksum += Utils::crc32(time.as_bytes()) as u64;
    writer.write(time.as_bytes()).await?;
    len += time.len();

    writer.complete().await?;
    Ok((len as u64, checksum))
}

async fn read(fs: &CurvineFileSystem, path: &Path) -> CommonResult<(u64, u64)> {
    let mut reader = fs.open(path).await?;

    let mut checksum: u64 = 0;
    let mut len: usize = 0;
    let mut buf = BytesMut::zeroed(1024);
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        len += n;
        checksum += Utils::crc32(&buf[0..n]) as u64;
    }
    reader.complete().await?;
    Ok((len as u64, checksum))
}

async fn seek(fs: &CurvineFileSystem, path: &Path) -> CommonResult<()> {
    let mut reader = fs.open(path).await?;
    let mut content = BytesMut::new();
    let mut buf = BytesMut::zeroed(1024);
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        content.extend_from_slice(&buf[0..n])
    }
    info!("content size: {}", content.len());

    let mut buf = BytesMut::zeroed(1024);
    //Test the situation across blocks and cross chunk seeks
    // for pos in 0..content.len() {
    for pos in [
        1024 * 1024,
        1024 * 1024,
        1024 * 1024 - 1,
        1024 * 1024 + 64 * 1024 - 1024,
        1024 * 1024 + 64 * 1024,
    ] {
        reader.seek(pos as i64).await?;
        let size = reader.read_full(&mut buf).await?;

        let read_checksum = Utils::crc32(&content[pos..pos + size]) as u64;
        let seek_checksum = Utils::crc32(&buf[0..size]) as u64;

        assert_eq!(read_checksum, seek_checksum);

        assert_eq!((pos + size) as i64, reader.pos());
    }

    Ok(())
}

#[test]
fn random_write_multiple_blocks() -> CommonResult<()> {
    let mut conf = Testing::get_cluster_conf()?;
    conf.client.short_circuit = true;
    conf.client.block_size = 1024 * 1024; // 1MB block size
    let path = Path::from_str("/random_write_multiple_blocks.data")?;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = Testing::get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move { random_write_multiple_blocks_test(&fs, &path).await })
        .unwrap();

    Ok(())
}

async fn random_write_multiple_blocks_test(
    fs: &CurvineFileSystem,
    path: &Path,
) -> CommonResult<()> {
    let block_size = 1024 * 1024; // 1MB
    let total_blocks = 3;
    let total_size = block_size * total_blocks;
    let mut expected_content = vec![0u8; total_size];

    // Create writer
    let mut writer = fs.create(path, true).await?;

    // Phase 1: Initial writes across all blocks
    let initial_writes = vec![
        // Block 0 initial writes
        (0, "INITIAL_START_BLOCK0".as_bytes()),
        (block_size / 4, "INITIAL_QUARTER_BLOCK0".as_bytes()),
        (block_size / 2, "INITIAL_MIDDLE_BLOCK0".as_bytes()),
        (block_size * 3 / 4, "INITIAL_3QUARTER_BLOCK0".as_bytes()),
        // Block 1 initial writes
        (block_size + 100, "INITIAL_START_BLOCK1".as_bytes()),
        (
            block_size + block_size / 4,
            "INITIAL_QUARTER_BLOCK1".as_bytes(),
        ),
        (
            block_size + block_size / 2,
            "INITIAL_MIDDLE_BLOCK1".as_bytes(),
        ),
        (
            block_size + block_size * 3 / 4,
            "INITIAL_3QUARTER_BLOCK1".as_bytes(),
        ),
        // Block 2 initial writes
        (block_size * 2 + 200, "INITIAL_START_BLOCK2".as_bytes()),
        (
            block_size * 2 + block_size / 4,
            "INITIAL_QUARTER_BLOCK2".as_bytes(),
        ),
        (
            block_size * 2 + block_size / 2,
            "INITIAL_MIDDLE_BLOCK2".as_bytes(),
        ),
        (
            block_size * 2 + block_size * 3 / 4,
            "INITIAL_3QUARTER_BLOCK2".as_bytes(),
        ),
        // Cross-block boundary writes
        (block_size - 20, "INITIAL_CROSS_BLOCK01_BOUNDARY".as_bytes()),
        (
            block_size * 2 - 25,
            "INITIAL_CROSS_BLOCK12_BOUNDARY".as_bytes(),
        ),
    ];

    // Apply initial writes
    for (offset, data) in &initial_writes {
        writer.seek(*offset as i64).await?;
        writer.write(data).await?;

        // Update expected content
        let end_pos = (*offset + data.len()).min(expected_content.len());
        if *offset < expected_content.len() {
            let copy_len = end_pos - offset;
            expected_content[*offset..end_pos].copy_from_slice(&data[0..copy_len]);
        }
    }

    // Phase 2: Overwrite operations - later writes overwrite earlier content
    let overwrite_operations = vec![
        // Overwrite in Block 0 - should overwrite initial content
        (0, "OVERWRITE_START_B0".as_bytes()), // Overwrites "INITIAL_START_BLOCK0"
        (block_size / 4 + 5, "OVERWRITE_QUARTER_B0".as_bytes()), // Partial overwrite of quarter position
        (
            block_size / 2 - 10,
            "OVERWRITE_MIDDLE_B0_EXTENDED".as_bytes(),
        ), // Overwrites and extends middle
        // Overwrite in Block 1 - should overwrite initial content
        (block_size + 100, "OVERWRITE_START_B1".as_bytes()), // Overwrites "INITIAL_START_BLOCK1"
        (
            block_size + block_size / 2 + 8,
            "OVERWRITE_MID_B1".as_bytes(),
        ), // Partial overwrite of middle
        (
            block_size + block_size * 3 / 4 - 5,
            "OVERWRITE_3Q_B1_LONG".as_bytes(),
        ), // Overwrites 3/4 position
        // Overwrite in Block 2 - should overwrite initial content
        (block_size * 2 + 200, "OVERWRITE_START_B2".as_bytes()), // Overwrites "INITIAL_START_BLOCK2"
        (
            block_size * 2 + block_size / 4 + 10,
            "OVERWRITE_Q_B2".as_bytes(),
        ), // Partial overwrite
        // Cross-block boundary overwrites - should overwrite initial boundary writes
        (block_size - 20, "OVERWRITE_CROSS_01".as_bytes()), // Overwrites initial cross-block write
        (block_size * 2 - 25, "OVERWRITE_CROSS_12".as_bytes()), // Overwrites initial cross-block write
        // Additional overwrites to test multiple overwrite layers
        (50, "FINAL_OVERWRITE_B0".as_bytes()), // Another overwrite in block 0
        (block_size + 150, "FINAL_OVERWRITE_B1".as_bytes()), // Another overwrite in block 1
        (block_size * 2 + 250, "FINAL_OVERWRITE_B2".as_bytes()), // Another overwrite in block 2
    ];

    // Apply overwrite operations
    for (offset, data) in &overwrite_operations {
        writer.seek(*offset as i64).await?;
        writer.write(data).await?;

        // Update expected content (this simulates the overwrite effect)
        let end_pos = (*offset + data.len()).min(expected_content.len());
        if *offset < expected_content.len() {
            let copy_len = end_pos - offset;
            expected_content[*offset..end_pos].copy_from_slice(&data[0..copy_len]);
        }
    }

    writer.complete().await?;

    // Calculate expected checksum from final content (after all overwrites)
    let expected_checksum = Utils::crc32(&expected_content) as u64;

    // Read back and verify using existing read function
    let (_actual_len, actual_checksum) = read(fs, path).await?;

    // Verify checksums match after all overwrites
    assert_eq!(
        expected_checksum, actual_checksum,
        "Checksums don't match after overwrite operations"
    );

    // Phase 3: Verify specific overwrite results using existing seek function
    let mut reader = fs.open(path).await?;

    // Test positions that should contain the final overwritten data
    let verification_positions = vec![
        // Verify final overwritten content
        (0, "OVERWRITE_START_B0".as_bytes()),
        (50, "FINAL_OVERWRITE_B0".as_bytes()),
        (block_size + 100, "OVERWRITE_START_B1".as_bytes()),
        (block_size + 150, "FINAL_OVERWRITE_B1".as_bytes()),
        (block_size * 2 + 200, "OVERWRITE_START_B2".as_bytes()),
        (block_size * 2 + 250, "FINAL_OVERWRITE_B2".as_bytes()),
        (block_size - 20, "OVERWRITE_CROSS_01".as_bytes()),
        (block_size * 2 - 25, "OVERWRITE_CROSS_12".as_bytes()),
    ];

    for (pos, expected_data) in verification_positions {
        reader.seek(pos as i64).await?;
        let mut verify_buf = vec![0u8; expected_data.len()];
        let n = reader.read_full(&mut verify_buf).await?;
        assert_eq!(n, expected_data.len());
        assert_eq!(
            &verify_buf[..n],
            expected_data,
            "Overwrite verification failed at position {} (block {})",
            pos,
            pos / block_size
        );
    }

    reader.complete().await?;

    Ok(())
}
