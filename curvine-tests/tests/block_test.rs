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

#![allow(clippy::useless_vec)]

use bytes::BytesMut;
use curvine_client::file::{CurvineFileSystem, FsWriter};
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::fs::Reader;
use curvine_common::fs::Writer;
use curvine_common::state::{FileAllocMode, FileAllocOpts};
use curvine_tests::Testing;
use log::info;
use orpc::common::{LocalTime, Utils};
use orpc::runtime::RpcRuntime;
use orpc::{err_box, CommonError, CommonResult};
use std::sync::Arc;
use std::time::Duration;
// Test local short-circuit read and write
#[test]
fn test_local_short_circuit_file_read_write() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = true;
    let path = Path::from_str("/file_local.data")?;
    run(testing, conf, path)
}

#[test]
fn test_remote_network_file_read_write() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    let path = Path::from_str("/file_remote.data")?;
    run(testing, conf, path)
}

#[test]
fn test_remote_file_operations_with_single_chunk_parallelism() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;

    conf.client.write_chunk_num = 1;
    conf.client.write_chunk_size = 65536;

    conf.client.read_chunk_num = 1;
    conf.client.read_parallel = 1;
    conf.client.read_chunk_size = 65536;

    let path = Path::from_str("/file_remote_parallel_1.data")?;
    run(testing, conf, path)
}

#[test]
fn test_remote_file_operations_with_four_chunk_parallelism() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;

    conf.client.write_chunk_num = 4;
    conf.client.write_chunk_size = 65536;

    conf.client.read_chunk_num = 4;
    conf.client.read_parallel = 4;
    conf.client.read_chunk_size = 65536;

    let path = Path::from_str("/file_remote_parallel_4.data")?;
    run(testing, conf, path)
}

#[test]
fn test_remote_file_operations_with_parallelism_and_caching() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;

    conf.client.write_chunk_num = 4;
    conf.client.write_chunk_size = 65536;

    conf.client.read_chunk_num = 4;
    conf.client.read_parallel = 4;
    conf.client.read_chunk_size = 65536;

    let path = Path::from_str("/file_remote_parallel_4_cache.data")?;
    run(testing, conf, path)
}

#[test]
fn test_file_replication_with_three_replicas() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = true;
    conf.client.replicas = 3;
    conf.client.block_size = 1024 * 1024;
    let path = Path::from_str("/replicas_3.data")?;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

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
fn test_local_file_append_operation() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = true;
    conf.client.replicas = 2;
    let path = Path::from_str("/append_local.data")?;
    append(testing, conf, path)
}

#[test]
fn test_remote_file_append_operation() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.short_circuit = false;
    conf.client.replicas = 2;
    let path = Path::from_str("/append_remote.data").unwrap();
    append(testing, conf, path)
}

fn append(testing: Testing, mut conf: ClusterConf, path: Path) -> CommonResult<()> {
    conf.client.block_size = 1024 * 1024;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();

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
    let testing = Testing::default();
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

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

fn run(testing: Testing, mut conf: ClusterConf, path: Path) -> CommonResult<()> {
    conf.client.block_size = 1024 * 1024;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
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
fn test_local_random_position_file_write() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = true;
    random_write(conf, "local")
}

#[test]
fn test_remote_random_position_file_write() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    random_write(conf, "remote")
}

#[test]
fn test_local_random_write_with_replication() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = true;
    conf.client.replicas = 2;
    random_write(conf, "local_replicas")
}

#[test]
fn test_remote_random_write_with_replication() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.replicas = 2;
    random_write(conf, "remote_replicas")
}

fn random_write(mut conf: ClusterConf, mark: &str) -> CommonResult<()> {
    let block_size = 1024;
    conf.client.block_size = block_size; // 1KB block size

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = Testing::default().get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let data = BytesMut::from(Utils::rand_str(2 * 1024).as_bytes());

        // create mode
        let path = Path::from_str(format!("/random_write_create_{}.data", mark))?;
        let mut writer = fs.create(&path, true).await?;
        writer.write(&data).await?;
        test_random_write(&fs, writer, block_size, data.clone()).await?;

        //  open mode
        let path = Path::from_str(format!("/random_write_overwrite_{}.data", mark))?;
        let mut writer = fs.create(&path, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;
        let writer = fs.open_for_write(&path, false).await?;
        test_random_write(&fs, writer, block_size, data).await?;

        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}

/// Test specified write mode
async fn test_random_write(
    fs: &CurvineFileSystem,
    mut writer: FsWriter,
    block_size: i64,
    mut expect_data: BytesMut,
) -> CommonResult<()> {
    let write_positions = vec![
        256,              // block 0 1/4 position
        0,                // block 0 start
        768,              // block 0 3/4 position
        512,              // block 0 middle
        block_size + 128, // block 1 start
        block_size + 384, // block 1 1/4 position
        block_size + 896, // block 1 3/4 position
        block_size + 640, // block 1 middle
        // Random position test
        100,
        500,
        1500,
        1200,
        1900,
        1800,
    ];

    for pos in write_positions.iter() {
        // Random data
        let str = Utils::rand_str(20);

        writer.seek(*pos).await?;
        writer.write(str.as_bytes()).await?;

        let (start, end) = (*pos as usize, writer.pos() as usize);
        println!("pos {} {}", start, end);
        expect_data[start..end].copy_from_slice(str.as_bytes());
    }
    writer.complete().await?;

    // Check if data is equal。
    let data = fs.read_string(writer.path()).await?;
    assert_eq!(&expect_data, data.as_bytes());

    Ok(())
}
#[test]
fn resize_truncate_extend() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = 1024 * 1024; // 1MB
    resize_test(testing, conf, "truncate_extend")
}

#[test]
fn resize_truncate_shrink() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = 1024 * 1024; // 1MB
    resize_test(testing, conf, "truncate_shrink")
}

#[test]
fn resize_allocate() -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = 1024 * 1024; // 1MB
    resize_test(testing, conf, "allocate")
}

fn resize_test(testing: Testing, conf: ClusterConf, test_type: &str) -> CommonResult<()> {
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let block_size = fs.conf().client.block_size;

    rt.block_on(async move {
        let path = Path::from_str(format!("/resize_test_{}.data", test_type))?;

        // Create initial file with some data
        let initial_data = b"Hello, World!";
        let mut writer = fs.create(&path, true).await?;
        writer.write(initial_data).await?;
        writer.complete().await?;

        let initial_status = fs.get_status(&path).await?;
        let initial_len = initial_status.len;
        println!("Initial file len: {}", initial_len);

        match test_type {
            "truncate_extend" => {
                // Test 1: truncate extends file size, creating holes
                // Test multiple resize operations, focusing on block_size boundaries
                let test_cases = vec![
                    // Extend to exactly 1 block boundary
                    (block_size, "exactly 1 block"),
                    // Extend to 1.5 blocks
                    (block_size + block_size / 2, "1.5 blocks"),
                    // Extend to exactly 2 blocks
                    (block_size * 2, "exactly 2 blocks"),
                    // Extend to 2.5 blocks
                    (block_size * 2 + block_size / 2, "2.5 blocks"),
                    // Extend to exactly 3 blocks
                    (block_size * 3, "exactly 3 blocks"),
                ];

                for (target_len, description) in test_cases {
                    println!(
                        "Testing truncate extend to {} ({})",
                        target_len, description
                    );
                    let opts = FileAllocOpts::with_truncate(target_len);
                    let file_blocks = test_resize(&fs, &path, opts).await?;

                    // Verify file size matches target
                    assert_eq!(
                        file_blocks.status.len, target_len,
                        "File size should be {} after truncate extend",
                        target_len
                    );

                    // Verify block count matches expected
                    let expected_blocks = (target_len + block_size - 1) / block_size;
                    assert_eq!(
                        file_blocks.block_locs.len() as i64,
                        expected_blocks,
                        "Block count should be {} for size {}",
                        expected_blocks,
                        target_len
                    );

                    // Verify file content is preserved
                    let mut reader = fs.open(&path).await?;
                    let mut buf = vec![0u8; initial_data.len()];
                    reader.read_full(&mut buf).await?;
                    assert_eq!(
                        &buf[..initial_data.len()],
                        initial_data,
                        "Initial data should be preserved after truncate extend"
                    );
                }
            }
            "truncate_shrink" => {
                // Test 2: truncate shrinks file size
                // First extend file to 3 blocks
                let extend_len = block_size * 3;
                let extend_opts = FileAllocOpts::with_truncate(extend_len);
                let _ = test_resize(&fs, &path, extend_opts).await?;

                // Write data across blocks
                let mut writer = fs.open_for_write(&path, false).await?;
                let test_data = b"X".repeat(extend_len as usize);
                writer.seek(0).await?;
                writer.write(&test_data).await?;
                writer.complete().await?;

                // Test multiple shrink operations, focusing on block_size boundaries
                let test_cases = vec![
                    // Shrink to 2.5 blocks
                    (block_size * 2 + block_size / 2, "2.5 blocks"),
                    // Shrink to exactly 2 blocks
                    (block_size * 2, "exactly 2 blocks"),
                    // Shrink to 1.5 blocks
                    (block_size + block_size / 2, "1.5 blocks"),
                    // Shrink to exactly 1 block
                    (block_size, "exactly 1 block"),
                    // Shrink to half block
                    (block_size / 2, "half block"),
                    // Shrink to small size
                    (100, "small size"),
                ];

                for (target_len, description) in test_cases {
                    println!(
                        "Testing truncate shrink to {} ({})",
                        target_len, description
                    );
                    let opts = FileAllocOpts::with_truncate(target_len);
                    let file_blocks = test_resize(&fs, &path, opts).await?;

                    // Verify file size matches target
                    assert_eq!(
                        file_blocks.status.len, target_len,
                        "File size should be {} after truncate shrink",
                        target_len
                    );

                    // Verify block count matches expected
                    let expected_blocks = if target_len == 0 {
                        0
                    } else {
                        (target_len + block_size - 1) / block_size
                    };
                    assert_eq!(
                        file_blocks.block_locs.len() as i64,
                        expected_blocks,
                        "Block count should be {} for size {}",
                        expected_blocks,
                        target_len
                    );

                    // Verify file content is truncated correctly
                    let mut reader = fs.open(&path).await?;
                    let mut buf = vec![0u8; target_len as usize];
                    let read_len = reader.read_full(&mut buf).await?;
                    assert_eq!(
                        read_len, target_len as usize,
                        "Read length should match file size"
                    );
                }
            }
            "allocate" => {
                // Test 3: allocate pre-allocates space
                // Test multiple allocate operations, focusing on block_size boundaries
                let test_cases = vec![
                    // Allocate exactly 1 block
                    (
                        block_size,
                        FileAllocMode::DEFAULT,
                        "exactly 1 block, DEFAULT mode",
                    ),
                    // Allocate 1.5 blocks
                    (
                        block_size + block_size / 2,
                        FileAllocMode::DEFAULT,
                        "1.5 blocks, DEFAULT mode",
                    ),
                    // Allocate exactly 2 blocks
                    (
                        block_size * 2,
                        FileAllocMode::DEFAULT,
                        "exactly 2 blocks, DEFAULT mode",
                    ),
                    // Allocate with ZERO_RANGE mode
                    (
                        block_size * 2 + block_size / 2,
                        FileAllocMode::ZERO_RANGE,
                        "2.5 blocks, ZERO_RANGE mode",
                    ),
                    // Allocate exactly 3 blocks
                    (
                        block_size * 3,
                        FileAllocMode::DEFAULT,
                        "exactly 3 blocks, DEFAULT mode",
                    ),
                ];

                for (target_len, mode, description) in test_cases {
                    println!("Testing allocate to {} ({})", target_len, description);
                    let opts = FileAllocOpts::with_alloc(target_len, mode);
                    let file_blocks = test_resize(&fs, &path, opts).await?;

                    // Verify file size matches target (fallocate extends file size)
                    assert_eq!(
                        file_blocks.status.len, target_len,
                        "File size should be {} after allocate",
                        target_len
                    );

                    // Verify block count matches expected
                    let expected_blocks = (target_len + block_size - 1) / block_size;
                    assert_eq!(
                        file_blocks.block_locs.len() as i64,
                        expected_blocks,
                        "Block count should be {} for size {}",
                        expected_blocks,
                        target_len
                    );

                    // Verify file content is preserved
                    let mut reader = fs.open(&path).await?;
                    let mut buf = vec![0u8; initial_data.len()];
                    reader.read_full(&mut buf).await?;
                    assert_eq!(
                        &buf[..initial_data.len()],
                        initial_data,
                        "Initial data should be preserved after allocate"
                    );
                }
            }
            _ => {
                return err_box!("Unknown test type: {}", test_type);
            }
        }

        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}

/// Unified method to test resize operation
/// Verifies the returned FileBlocks data matches expectations
async fn test_resize(
    fs: &CurvineFileSystem,
    path: &Path,
    opts: FileAllocOpts,
) -> CommonResult<curvine_common::state::FileBlocks> {
    let block_size = fs.conf().client.block_size;
    fs.resize(path, opts.clone()).await?;
    let file_blocks = fs.get_block_locations(path).await?;
    assert_eq!(
        file_blocks.status.len, opts.len,
        "FileBlocks.status.len should match opts.len"
    );

    if opts.len == 0 {
        assert_eq!(
            file_blocks.block_locs.len(),
            0,
            "Empty file should have no blocks"
        );
    } else {
        // Calculate expected block count
        let expected_blocks = (opts.len + block_size - 1) / block_size;
        assert_eq!(
            file_blocks.block_locs.len() as i64,
            expected_blocks,
            "Block count should match expected for size {}",
            opts.len
        );

        // Verify block lengths sum up correctly
        let mut total_len = 0i64;
        for (idx, block) in file_blocks.block_locs.iter().enumerate() {
            let is_last = idx == file_blocks.block_locs.len() - 1;
            let expected_block_len = if is_last {
                // Last block may be partial
                opts.len - total_len
            } else {
                block_size
            };

            assert_eq!(
                block.block.len, expected_block_len,
                "Block {} length should be {}",
                idx, expected_block_len
            );

            total_len += block.block.len;
        }

        assert_eq!(
            total_len, opts.len,
            "Sum of block lengths should equal file size"
        );
    }

    Ok(file_blocks)
}

#[test]
fn resize_file_read_write() {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf().unwrap();
    conf.client.short_circuit = false;
    conf.client.replicas = 1;
    conf.client.block_size = 1024; // 1KB

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf)).unwrap();
    let block_size = fs.conf().client.block_size;

    rt.block_on(async {
        let path = Path::from_str("/resize_file_read_write.data").unwrap();

        let mut writer = fs.create(&path, true).await.unwrap();

        let write_positions = vec![
            (0, b"A", 100, "start of file"),
            (
                block_size - 100,
                b"C",
                100,
                "just before block_size boundary",
            ),
            (block_size, b"B", 100, "block_size boundary"),
            (block_size * 2, b"E", 100, "2 * block_size boundary"),
            (
                block_size + 100,
                b"D",
                100,
                "just after block_size boundary",
            ),
            (
                block_size * 2 + block_size / 2,
                b"F",
                100,
                "middle of third block",
            ),
            (block_size * 3, b"G", 100, "3 * block_size boundary"),
            (
                block_size * 3 + block_size - 100,
                b"H",
                100,
                "end of last block (must be allocated)",
            ),
        ];

        let max_pos = write_positions
            .iter()
            .map(|(pos, _, len, _)| pos + *len as i64)
            .max()
            .unwrap_or(0);

        for (pos, pattern, len, description) in &write_positions {
            writer.seek(*pos).await.expect(description);
            let data = pattern.repeat(*len);
            writer.write(&data).await.expect(description);
            writer.flush().await.expect(description);
            println!("Wrote {} bytes at position {} ({})", len, pos, description);
        }

        writer.complete().await.unwrap();

        let file_status = fs.get_status(&path).await.unwrap();
        println!(
            "File size: {}, max write position: {}",
            file_status.len, max_pos
        );
        assert!(
            file_status.len >= max_pos,
            "File size should be at least {} (max write position), but got {}",
            max_pos,
            file_status.len
        );

        let mut reader = fs.open(&path).await.unwrap();
        let read_buf_size = 100;

        for (pos, pattern, len, description) in &write_positions {
            reader.seek(*pos).await.unwrap();
            let mut buf = vec![0u8; *len];
            let read_len = reader.read_full(&mut buf).await.unwrap();

            assert_eq!(
                read_len, *len,
                "Should read {} bytes at position {} ({}), but only read {}",
                len, pos, description, read_len
            );

            let pattern_byte = pattern[0];
            assert!(
                buf[..read_len].iter().all(|&b| b == pattern_byte),
                "Data at position {} ({}) should be all '{}' (0x{:02x}), but got: {:?}",
                pos,
                description,
                pattern_byte as char,
                pattern_byte,
                &buf[..read_len.min(20)]
            );
            println!(
                "Verified data at position {} ({}): all {} bytes are '{}'",
                pos, description, len, pattern_byte as char
            );
        }

        let hole_positions = vec![
            (200, "middle of first block (after A, before C)"),
            (block_size / 2, "middle of first block"),
            (block_size - 200, "before C in first block"),
            (block_size + block_size / 2, "middle of second block"),
            (block_size + 300, "after D in second block"),
            (
                block_size * 2 + block_size / 4,
                "quarter of third block (before F)",
            ),
            (
                block_size * 2 + block_size / 2 + 200,
                "after F in third block",
            ),
            (
                block_size * 3 + block_size / 2,
                "middle of fourth block (between G and H)",
            ),
            (block_size * 3 + 200, "after G in fourth block"),
        ];

        for (pos, description) in &hole_positions {
            reader.seek(*pos).await.unwrap();

            let mut buf = vec![0u8; read_buf_size];
            let read_len = reader.read_full(&mut buf).await.unwrap();

            if read_len == 0 {
                println!(
                    "Skipped hole verification at position {} ({}): no data to read",
                    pos, description
                );
                continue;
            }

            assert!(
                buf[..read_len].iter().all(|&b| b == 0),
                "Hole at position {} ({}) should be all zeros, but got: {:?}",
                pos,
                description,
                &buf[..read_len.min(20)]
            );
            println!(
                "Verified hole at position {} ({}): all {} bytes are zero",
                pos, description, read_len
            );
        }
    });
}
