// Copyright 2026 OPPO.
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

// `UnifiedFileSystem` async futures nest deeply enough that the test binary
// exceeds rustc's default query depth limit without this attribute.
#![recursion_limit = "256"]

//! End-to-end coverage for worker block checksums (#1921).
//!
//! With `worker.block_checksum` and `worker.block_checksum_verify` on, a block
//! whose bytes change on disk after it was finalized must never be served. The
//! tests flip one byte of a finalized block file, keeping its length, so only
//! the content checksum can tell the replica changed.
//!
//! | Test | Data | Expected |
//! | ---- | ---- | -------- |
//! | `cache_mode_read_of_corrupted_block_returns_ufs_bytes` | cache_mode mount, block cached by a load job | The read falls back to UFS and returns the original bytes; the worker removes the replica and master invalidates the cache |
//! | `read_of_corrupted_block_without_other_copy_fails` | Curvine file with one replica and no UFS copy | The read fails with an IO error instead of returning the changed bytes; the worker removes the replica and master drops its location |
//!
//! `bench_read_throughput_with_and_without_checksum` is a manual benchmark.

use curvine_client::unified::{UnifiedFileSystem, UnifiedReader};
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{FileSystem, Path, Reader, RpcCode, Writer};
use curvine_model::{MountOptionsBuilder, WriteType};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_server::storage::local::{BlockState, FileLayout};
use curvine_tests::Testing;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Several checksum chunks, so the flipped byte sits inside a chunk that has
/// verified neighbours on both sides.
const DATA_LEN: usize = 4 * 1024 * 1024;

/// Worker heartbeats run every 3s by default; the corrupt replica is removed on
/// the first heartbeat after the failed read and master applies the report.
const INVALIDATION_TIMEOUT: Duration = Duration::from_secs(60);

fn start_cluster(checksum: bool) -> (Testing, UnifiedFileSystem) {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(move |conf| {
            conf.worker.block_checksum = checksum;
            conf.worker.block_checksum_verify = checksum;
            // One replica, so the read cannot be served by another healthy copy.
            conf.client.replicas = 1;
        })
        .build()
        .unwrap();
    testing.start_cluster().unwrap();
    let rt = Arc::new(Runtime::new("block-checksum-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt).unwrap();
    (testing, fs)
}

fn random_data(len: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(len);
    while data.len() < len {
        data.extend_from_slice(Utils::rand_str(1024).as_bytes());
    }
    data.truncate(len);
    data
}

/// Every on-disk replica of `block_id` under the cluster's worker data dirs.
fn block_files(testing: &Testing, block_id: i64) -> Vec<PathBuf> {
    let conf = testing.get_active_cluster_conf().unwrap();
    let name = BlockState::Finalized.get_name(block_id);
    let mut dirs: Vec<PathBuf> = conf.worker.data_dir.iter().map(PathBuf::from).collect();
    let mut files = vec![];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name().is_some_and(|n| n == name.as_str()) {
                files.push(path);
            }
        }
    }
    files
}

/// The single block of `path` and its only on-disk replica, which must carry a
/// checksum sidecar.
async fn single_replica(
    testing: &Testing,
    fs: &UnifiedFileSystem,
    path: &Path,
) -> FsResult<(i64, PathBuf)> {
    let blocks = fs.cv().get_block_locations(path).await?;
    assert_eq!(blocks.block_locs.len(), 1, "{path} must fit in one block");
    let block_id = blocks.block_locs[0].block.id;

    let files = block_files(testing, block_id);
    assert_eq!(files.len(), 1, "block {block_id} must have one replica");
    let file = files.into_iter().next().unwrap();
    assert!(
        FileLayout::checksum_path_of(&file).exists(),
        "block {block_id} must have been finalized with a checksum sidecar"
    );
    Ok((block_id, file))
}

/// Flip one byte in the middle of a block file without changing its length or
/// its sidecar.
fn corrupt(file: &std::path::Path) -> std::io::Result<()> {
    let mut f = OpenOptions::new().read(true).write(true).open(file)?;
    let off = f.metadata()?.len() / 2;
    let mut byte = [0u8; 1];
    f.seek(SeekFrom::Start(off))?;
    f.read_exact(&mut byte)?;
    byte[0] ^= 0xFF;
    f.seek(SeekFrom::Start(off))?;
    f.write_all(&byte)?;
    f.sync_all()
}

async fn read_all(reader: &mut UnifiedReader) -> FsResult<Vec<u8>> {
    let mut buf = vec![0u8; reader.len() as usize];
    let n = reader.read_full(&mut buf).await?;
    buf.truncate(n);
    reader.complete().await?;
    Ok(buf)
}

async fn wait_until<F, Fut>(what: &str, mut cond: F) -> FsResult<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = FsResult<bool>>,
{
    let deadline = Instant::now() + INVALIDATION_TIMEOUT;
    while !cond().await? {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(())
}

#[test]
fn cache_mode_read_of_corrupted_block_returns_ufs_bytes() -> CommonResult<()> {
    let (testing, fs) = start_cluster(true);
    let rt = fs.clone_runtime();
    rt.block_on(async {
        let ufs_root = Path::from_str(format!("{}/checksum-cache", testing.ufs_path))?;
        std::fs::create_dir_all(ufs_root.path())?;
        let mnt = Path::from_str("/checksum-cache")?;
        let opts = MountOptionsBuilder::new()
            .write_type(WriteType::CacheMode)
            .build();
        fs.mount(&ufs_root, &mnt, opts).await?;

        let data = random_data(DATA_LEN);
        let path = Path::from_str("/checksum-cache/data.bin")?;
        let (ufs_path, mount) = fs.get_mount(&path, RpcCode::GetMountInfo).await?.unwrap();
        let mut writer = mount.ufs()?.create(&ufs_path, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;

        // Cache the UFS file in Curvine.
        fs.async_cache(&ufs_path)?;
        fs.wait_job_complete(&path, false).await?;
        let (block_id, file) = single_replica(&testing, &fs, &path).await?;

        corrupt(&file)?;

        // The cache still looks valid, so the read starts on the corrupt
        // replica. The worker rejects the chunk and the reader falls back to
        // UFS, which is authoritative in cache mode.
        let mut reader = fs.open(&path).await?;
        assert!(
            matches!(reader, UnifiedReader::Fallback(_)),
            "the read must start from the Curvine cache, got a {} reader",
            reader.path_type()
        );
        let read = read_all(&mut reader).await?;
        assert_eq!(read.len(), data.len());
        assert!(read == data, "cache_mode read must return the UFS bytes");

        // The worker re-verifies and removes the replica, then master
        // invalidates the cache file that lost its only replica.
        wait_until("the corrupt replica is removed", || async {
            Ok(block_files(&testing, block_id).is_empty())
        })
        .await?;
        assert!(!FileLayout::checksum_path_of(&file).exists());
        wait_until("master invalidates the cache", || async {
            Ok(!fs.cv().get_status(&path).await?.cv_valid(None))
        })
        .await?;

        // Later reads are cache misses served from UFS.
        let mut reader = fs.open(&path).await?;
        assert!(
            !matches!(reader, UnifiedReader::Fallback(_)),
            "an invalidated cache must not be read from Curvine"
        );
        let read = read_all(&mut reader).await?;
        assert!(
            read == data,
            "a read after invalidation must return the UFS bytes"
        );
        Ok(())
    })
}

#[test]
fn read_of_corrupted_block_without_other_copy_fails() -> CommonResult<()> {
    let (testing, fs) = start_cluster(true);
    let rt = fs.clone_runtime();
    rt.block_on(async {
        let data = random_data(DATA_LEN);
        let path = Path::from_str("/checksum-native/data.bin")?;
        let mut writer = fs.create(&path, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;

        // Sanity check: the intact block verifies and reads back.
        let mut reader = fs.open(&path).await?;
        assert!(read_all(&mut reader).await? == data);

        let (block_id, file) = single_replica(&testing, &fs, &path).await?;
        corrupt(&file)?;

        // No other replica and no UFS copy: the read must fail rather than
        // return the changed bytes.
        let mut reader = fs.open(&path).await?;
        let mut buf = vec![0u8; data.len()];
        match reader.read_full(&mut buf).await {
            Err(e @ FsError::IO(_)) => assert!(
                e.to_string().contains("checksum mismatch"),
                "the IO error must come from checksum verification, got {e}"
            ),
            Err(e) => panic!("expected an IO error for the corrupt block, got {e:?}"),
            Ok(n) => panic!("read of a corrupt block returned {n} bytes"),
        }
        drop(reader);

        // The worker removes the replica and master drops the location.
        wait_until("the corrupt replica is removed", || async {
            Ok(block_files(&testing, block_id).is_empty())
        })
        .await?;
        wait_until("master drops the corrupt location", || async {
            let blocks = fs.cv().get_block_locations(&path).await?;
            Ok(blocks.block_locs.iter().all(|b| b.locs.is_empty()))
        })
        .await?;
        Ok(())
    })
}

/// Read throughput of the same data with checksum verification on and off.
/// Run with:
/// `cargo test --release -p curvine-tests --test block_checksum_test bench_ -- --ignored --nocapture`
#[test]
#[ignore = "manual benchmark: measures read throughput with checksum on and off"]
fn bench_read_throughput_with_and_without_checksum() -> CommonResult<()> {
    // Override with BENCH_FILE_MB / BENCH_READS to measure other sizes.
    let env = |name: &str, default: usize| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let file_mb = env("BENCH_FILE_MB", 256);
    let reads = env("BENCH_READS", 5);
    let data = random_data(file_mb * 1024 * 1024);

    for checksum in [false, true] {
        let (_testing, fs) = start_cluster(checksum);
        let rt = fs.clone_runtime();
        let mib_per_sec = rt.block_on(async {
            let path = Path::from_str("/checksum-bench/data.bin")?;
            let mut writer = fs.create(&path, true).await?;
            writer.write(&data).await?;
            writer.complete().await?;

            let mut buf = vec![0u8; data.len()];
            // Warm the worker page cache so both runs measure the read path,
            // not the disk.
            let mut reader = fs.open(&path).await?;
            reader.read_full(&mut buf).await?;
            reader.complete().await?;

            let started = Instant::now();
            for _ in 0..reads {
                let mut reader = fs.open(&path).await?;
                assert_eq!(reader.read_full(&mut buf).await?, data.len());
                reader.complete().await?;
            }
            let secs = started.elapsed().as_secs_f64();
            FsResult::Ok((file_mb * reads) as f64 / secs)
        })?;
        println!(
            "checksum={checksum} file={file_mb}MiB reads={reads} throughput={mib_per_sec:.1}MiB/s"
        );
    }
    Ok(())
}
