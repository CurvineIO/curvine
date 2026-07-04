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

//! Integration tests for `FallbackFsReader`.
//!
//! # Test Plan
//!
//! ## Runnable (requires UFS_TEST_PATH env var)
//!
//! | ID     | Scenario                                                          |
//! |--------|-------------------------------------------------------------------|
//! | TC-10  | FsMode open() returns UnifiedReader::Fallback                     |
//! | TC-11a | FallbackFsReader reads correct data (normal path, no failure)     |
//! | TC-15  | seek() then read via FallbackFsReader returns correct slice       |
//! | TC-16  | read_full() returns all data intact                               |
//!
//! ## Require worker-kill infrastructure (skipped with #[ignore])
//!
//! | ID     | Scenario                                                          |
//! |--------|-------------------------------------------------------------------|
//! | TC-11b | Worker failure during read -> fallback to UFS transparently       |
//! | TC-12  | FsMode worker failure + ufs_mtime mismatch -> returns error        |
//! | TC-13  | FsMode worker failure + ufs_mtime=0 (not flushed) -> returns error |
//! | TC-14  | seek() after fallback to UFS -> continues from new position       |
//! | TC-17  | CacheMode worker failure, pos==0 -> reads current S3 (incl. shrunk)|
//! | TC-18  | CacheMode worker failure, S3 shrunk past pos -> seek error         |
//! | TC-19  | CacheMode worker failure, S3 unchanged -> fallback reads value    |
//! | TC-20  | CacheMode worker failure, S3 grown -> reads current (longer) S3   |
//! | TC-21  | CacheMode fallback -> len() reflects current (grown) S3 length    |

use bytes::BytesMut;
use curvine_client::file::FsReader;
use curvine_client::unified::{FallbackFsReader, UfsFileSystem, UnifiedFileSystem, UnifiedReader};
use curvine_common::fs::{FileSystem, Path, Reader, RpcCode, Writer};
use curvine_common::state::{MountOptionsBuilder, WriteType};
use curvine_tests::Testing;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use std::env;
use std::sync::Arc;
use std::sync::OnceLock;

// ---- helpers ---------------------------------------------------------------

fn setup() -> Option<UnifiedFileSystem> {
    let ufs_path = env::var("UFS_TEST_PATH").unwrap_or_default();
    if ufs_path.is_empty() {
        println!("WARNING: UFS_TEST_PATH not set or empty, skipping fallback_read tests");
        return None;
    }
    let testing = shared_testing();
    let rt = Arc::new(AsyncRuntime::single());
    Some(testing.get_unified_fs_with_rt(rt).unwrap())
}

fn shared_testing() -> &'static Testing {
    static TESTING: OnceLock<Testing> = OnceLock::new();
    TESTING.get_or_init(|| {
        let testing = Testing::builder().workers(3).build().unwrap();
        testing.start_cluster().unwrap();
        testing
    })
}

async fn mount_fs_mode(fs: &UnifiedFileSystem, mount_dir: &str) {
    let ufs_base = env::var("UFS_TEST_PATH").unwrap();
    let ufs_path = Path::from_str(format!("{}/{}", ufs_base, mount_dir)).unwrap();
    let cv_path = Path::from_str(format!("/{}", mount_dir)).unwrap();

    if fs
        .get_mount(&cv_path, RpcCode::GetMountInfo)
        .await
        .unwrap()
        .is_some()
    {
        return;
    }

    let mut opts_builder = MountOptionsBuilder::new().write_type(WriteType::FsMode);
    if let Ok(props_str) = env::var("UFS_TEST_PROPERTIES") {
        for pair in props_str.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                opts_builder = opts_builder.add_property(k.trim(), v.trim());
            }
        }
    }
    let opts = opts_builder.build();

    let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties.clone(), None).unwrap();
    if ufs.exists(&ufs_path).await.unwrap() {
        ufs.delete(&ufs_path, true).await.unwrap();
    }
    ufs.mkdir(&ufs_path, true).await.unwrap();
    fs.mount(&ufs_path, &cv_path, opts).await.unwrap();
}

/// Mount `mount_dir` in CacheMode (UFS/S3 is authoritative, Curvine is a read cache).
async fn mount_cache_mode(fs: &UnifiedFileSystem, mount_dir: &str) {
    let ufs_base = env::var("UFS_TEST_PATH").unwrap();
    let ufs_path = Path::from_str(format!("{}/{}", ufs_base, mount_dir)).unwrap();
    let cv_path = Path::from_str(format!("/{}", mount_dir)).unwrap();

    if fs
        .get_mount(&cv_path, RpcCode::GetMountInfo)
        .await
        .unwrap()
        .is_some()
    {
        return;
    }

    let mut opts_builder = MountOptionsBuilder::new().write_type(WriteType::CacheMode);
    if let Ok(props_str) = env::var("UFS_TEST_PROPERTIES") {
        for pair in props_str.split(',') {
            if let Some((k, v)) = pair.split_once('=') {
                opts_builder = opts_builder.add_property(k.trim(), v.trim());
            }
        }
    }
    let opts = opts_builder.build();

    let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties.clone(), None).unwrap();
    if ufs.exists(&ufs_path).await.unwrap() {
        ufs.delete(&ufs_path, true).await.unwrap();
    }
    ufs.mkdir(&ufs_path, true).await.unwrap();
    fs.mount(&ufs_path, &cv_path, opts).await.unwrap();
}

/// Write `data` directly into the CacheMode UFS (S3), then warm the Curvine cache
/// by submitting a load job, so a subsequent read can hit Curvine blocks. Returns
/// the cv path. This mirrors the cache_mode lifecycle: write goes straight to S3,
/// the cache is populated asynchronously by a LoadJob.
async fn write_ufs_and_warm_cache(
    fs: &UnifiedFileSystem,
    mount_dir: &str,
    name: &str,
    data: &str,
) -> Path {
    let cv_path = Path::from_str(format!("/{}/{}", mount_dir, name)).unwrap();
    let (ufs_path, mount) = fs
        .get_mount(&cv_path, RpcCode::GetMountInfo)
        .await
        .unwrap()
        .unwrap();

    let mut w = mount.ufs.create(&ufs_path, true).await.unwrap();
    w.write(data.as_bytes()).await.unwrap();
    w.complete().await.unwrap();

    // Warm the Curvine cache from S3 and wait for the load job to finish.
    // async_cache takes the ufs path (server derives job_id from it in CacheMode),
    // but wait_job_complete requires the cv path (it checks path.is_cv() and derives
    // the ufs path internally). Passing ufs_path here fails with "not a cache file".
    fs.async_cache(&ufs_path).unwrap();
    fs.wait_job_complete(&cv_path, false).await.unwrap();
    cv_path
}

/// Write `data` to Curvine FsMode and wait for it to flush to UFS.
async fn write_and_flush(fs: &UnifiedFileSystem, mount_dir: &str, name: &str, data: &str) -> Path {
    let cv_path = Path::from_str(format!("/{}/{}", mount_dir, name)).unwrap();
    let mut w = fs.create(&cv_path, true).await.unwrap();
    w.write(data.as_bytes()).await.unwrap();
    w.complete().await.unwrap();

    // Submit load/export job explicitly, then wait for completion by cv path job id.
    fs.async_cache(&cv_path).unwrap();
    fs.wait_job_complete(&cv_path, false).await.unwrap();
    cv_path
}

/// Write data but do not flush to UFS, so `ufs_mtime` stays 0.
async fn write_without_flush(
    fs: &UnifiedFileSystem,
    mount_dir: &str,
    name: &str,
    data: &str,
) -> Path {
    let cv_path = Path::from_str(format!("/{}/{}", mount_dir, name)).unwrap();
    let mut w = fs.create(&cv_path, true).await.unwrap();
    w.write(data.as_bytes()).await.unwrap();
    w.complete().await.unwrap();
    cv_path
}

/// Build a `FallbackFsReader` whose internal Curvine block locations point to an
/// unreachable worker endpoint, to deterministically trigger worker read errors.
async fn build_reader_with_unreachable_worker(
    fs: &UnifiedFileSystem,
    cv_path: &Path,
) -> FallbackFsReader {
    let mut blocks = fs.cv().get_block_locations(cv_path).await.unwrap();
    for (i, located) in blocks.block_locs.iter_mut().enumerate() {
        for (j, worker) in located.locs.iter_mut().enumerate() {
            // IMPORTANT:
            // WorkerAddress hash/eq uses worker_id only. We must also mutate worker_id
            // to avoid reusing an existing pooled connection for the original worker.
            worker.worker_id = 4_000_000_000u32.saturating_sub((i * 100 + j) as u32);
            worker.hostname = "127.0.0.1".to_string();
            worker.ip_addr = "127.0.0.1".to_string();
            worker.rpc_port = 1;
        }
    }

    let cv_reader = FsReader::new(cv_path.clone(), fs.cv().fs_context(), blocks).unwrap();
    let (ufs_path, mount) = fs
        .get_mount(cv_path, RpcCode::GetMountInfo)
        .await
        .unwrap()
        .unwrap();
    FallbackFsReader::new(
        cv_reader,
        ufs_path,
        mount.ufs.clone(),
        mount.info.is_fs_mode(),
    )
}

// ---- TC-10: FsMode open returns Fallback -----------------------------------

/// TC-10: `open()` on an FsMode path with cached data must return
/// `UnifiedReader::Fallback`, not `UnifiedReader::Cv`.
#[test]
fn test_tc10_fsmode_open_returns_fallback() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc10";
        mount_fs_mode(&fs, mount_dir).await;
        let cv_path = write_and_flush(&fs, mount_dir, "tc10.log", "hello fallback").await;

        let reader = fs.open(&cv_path).await.unwrap();
        assert!(
            matches!(reader, UnifiedReader::Fallback(_)),
            "FsMode with cached blocks must return UnifiedReader::Fallback"
        );
    });
}

// ---- TC-11a: FallbackFsReader reads correct data (normal path) -------------

/// TC-11a: Data read through `FallbackFsReader` (Curvine path, no failure)
/// must match what was written.
#[test]
fn test_tc11a_fallback_reader_reads_correct_data() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc11a";
        mount_fs_mode(&fs, mount_dir).await;
        let data = Utils::rand_str(4 * 1024);
        let cv_path = write_and_flush(&fs, mount_dir, "tc11a.log", &data).await;

        let mut reader = fs.open(&cv_path).await.unwrap();
        assert!(matches!(reader, UnifiedReader::Fallback(_)));

        let mut buf = BytesMut::zeroed(data.len());
        reader.read_full(&mut buf).await.unwrap();
        reader.complete().await.unwrap();

        assert_eq!(
            data.as_bytes(),
            &buf[..],
            "data read via FallbackFsReader does not match written data"
        );
    });
}

// ---- TC-15: seek + read via FallbackFsReader --------------------------------

/// TC-15: After `seek()` to an offset, reading must return only the bytes from
/// that offset onward.
#[test]
fn test_tc15_fallback_reader_seek_and_read() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc15";
        mount_fs_mode(&fs, mount_dir).await;

        let prefix = "PREFIX_DATA_";
        let suffix = Utils::rand_str(512);
        let data = format!("{}{}", prefix, suffix);
        let cv_path = write_and_flush(&fs, mount_dir, "tc15.log", &data).await;

        let mut reader = fs.open(&cv_path).await.unwrap();
        assert!(matches!(reader, UnifiedReader::Fallback(_)));

        let seek_pos = prefix.len() as i64;
        reader.seek(seek_pos).await.unwrap();

        let remaining = data.len() - prefix.len();
        let mut buf = BytesMut::zeroed(remaining);
        reader.read_full(&mut buf).await.unwrap();
        reader.complete().await.unwrap();

        assert_eq!(
            suffix.as_bytes(),
            &buf[..],
            "seek + read via FallbackFsReader returned wrong bytes"
        );
    });
}

// ---- TC-16: read_full returns all data -------------------------------------

/// TC-16: `read_full()` across a 256 KiB file via `FallbackFsReader` must
/// return all data with a matching checksum.
#[test]
fn test_tc16_fallback_reader_multi_chunk_read() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc16";
        mount_fs_mode(&fs, mount_dir).await;

        let data = Utils::rand_str(256 * 1024);
        let cv_path = write_and_flush(&fs, mount_dir, "tc16.log", &data).await;

        let mut reader = fs.open(&cv_path).await.unwrap();
        assert!(matches!(reader, UnifiedReader::Fallback(_)));

        let mut buf = BytesMut::zeroed(data.len());
        reader.read_full(&mut buf).await.unwrap();
        reader.complete().await.unwrap();

        assert_eq!(
            Utils::crc32(data.as_bytes()),
            Utils::crc32(&buf),
            "CRC32 mismatch on multi-chunk read via FallbackFsReader"
        );
    });
}

// ---- TC-11b / TC-12 / TC-13 / TC-14: worker-failure paths ------------------

/// TC-11b: Simulate worker failure during read and verify transparent fallback
/// to UFS returns the correct data.
#[test]
fn test_tc11b_worker_failure_falls_back_to_ufs() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc11b";
        mount_fs_mode(&fs, mount_dir).await;
        let data = Utils::rand_str(16 * 1024);
        let cv_path = write_and_flush(&fs, mount_dir, "tc11b.log", &data).await;

        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut buf = BytesMut::zeroed(data.len());
        reader.read_full(&mut buf).await.unwrap();
        let _ = reader.complete().await;

        assert_eq!(data.as_bytes(), &buf[..]);
    });
}

/// TC-12: FsMode worker failure when `ufs_mtime` differs from actual UFS mtime
/// -> `read_chunk0` must return an error (no silent fallback with stale data).
/// This refusal is FsMode-specific: Curvine is authoritative and a UFS mismatch
/// means the flushed copy is untrustworthy. CacheMode behaves oppositely (TC-17).
#[test]
fn test_tc12_worker_failure_ufs_mtime_mismatch_returns_error() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc12";
        mount_fs_mode(&fs, mount_dir).await;
        let cv_path = write_and_flush(&fs, mount_dir, "tc12.log", "original-data").await;

        // Overwrite UFS file to change mtime/len and create metadata mismatch.
        let (ufs_path, mount) = fs
            .get_mount(&cv_path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let mut ufs_writer = mount.ufs.create(&ufs_path, true).await.unwrap();
        ufs_writer.write(b"changed-in-ufs").await.unwrap();
        ufs_writer.complete().await.unwrap();

        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut buf = BytesMut::zeroed(32);
        let err = reader.read(&mut buf).await.unwrap_err().to_string();
        assert!(err.contains("UFS data inconsistent"));
    });
}

/// TC-13: Worker failure when `ufs_mtime == 0` (never flushed to UFS)
/// -> `read_chunk0` must return an error.
#[test]
fn test_tc13_worker_failure_ufs_not_flushed_returns_error() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc13";
        mount_fs_mode(&fs, mount_dir).await;
        let cv_path = write_without_flush(&fs, mount_dir, "tc13.log", "not-flushed").await;

        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut buf = BytesMut::zeroed(32);
        let err = reader.read(&mut buf).await.unwrap_err().to_string();
        assert!(err.contains("not been flushed"));
    });
}

/// TC-14: After falling back to UFS, `seek()` to a new position then read
/// must return data from the new offset, not the original fallback position.
#[test]
fn test_tc14_seek_after_ufs_fallback() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc14";
        mount_fs_mode(&fs, mount_dir).await;
        let prefix = "PREFIX-";
        let middle = Utils::rand_str(128);
        let suffix = Utils::rand_str(256);
        let data = format!("{}{}{}", prefix, middle, suffix);
        let cv_path = write_and_flush(&fs, mount_dir, "tc14.log", &data).await;

        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;

        // First read triggers fallback from Curvine to UFS.
        let mut first = BytesMut::zeroed(8);
        let n = reader.read(&mut first).await.unwrap();
        assert!(n > 0);

        // Seek after fallback and verify the returned slice.
        let seek_pos = (prefix.len() + middle.len()) as i64;
        reader.seek(seek_pos).await.unwrap();
        let mut buf = BytesMut::zeroed(suffix.len());
        reader.read_full(&mut buf).await.unwrap();
        let _ = reader.complete().await;

        assert_eq!(suffix.as_bytes(), &buf[..]);
    });
}

/// # How to run this test (requires a real S3/UFS)
///
/// This test is skipped unless `UFS_TEST_PATH` is set. Run it on the dev host
/// (cluster + S3 connectivity + the `opendal-s3` build feature live there):
///
/// ```bash
/// # 1) Point the test at an S3 base path + connection properties
/// #    (fill in your own bucket / endpoint / credentials)
/// export UFS_TEST_PATH="s3://<bucket name>/curvine-fallback-test"
/// export UFS_TEST_PROPERTIES="s3.endpoint_url=<your s3 endpoint>,s3.region_name=<region>,s3.credentials.access=<your access key>,s3.credentials.secret=<your secret key>,s3.force.path.style=false,s3.list_objects_version=v1"
///
/// # 2) Run only this case
/// cargo test -p curvine-tests --test fallback_read_test \
///     --features curvine-client/opendal-s3 \
///     test_tc17_cachemode_worker_failure_pos0_reads_current_s3 \
///     -- --exact --nocapture
/// ```
///
/// Property keys map to `S3Conf` in curvine-ufs/src/conf.rs. `s3.endpoint_url`
/// MUST start with http:// or https://. `s3.region_name` is required by the
/// OpenDAL S3 builder (omitting it fails with "region is missing"). If output
/// shows "WARNING: UFS_TEST_PATH not set", the env vars did not take effect.
/// TC-17: CacheMode worker failure at pos == 0 after S3 was changed (shrunk) out-of-band.
/// CacheMode treats S3 as authoritative and validates nothing; the read starts at pos 0
/// (always in range) and must return the CURRENT S3 content, not refuse.
#[test]
fn test_tc17_cachemode_worker_failure_pos0_reads_current_s3() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc17";
        mount_cache_mode(&fs, mount_dir).await;

        // Warm the cache with the original (longer) content.
        let original = "abcdefghijklmnopqrstuvwxyz";
        let cv_path = write_ufs_and_warm_cache(&fs, mount_dir, "tc17.log", original).await;

        // Modify S3 out-of-band to SHORTER content (changes mtime AND len).
        let changed = "abcdefghij";
        let (ufs_path, mount) = fs
            .get_mount(&cv_path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let mut w = mount.ufs.create(&ufs_path, true).await.unwrap();
        w.write(changed.as_bytes()).await.unwrap();
        w.complete().await.unwrap();

        // Fresh reader at pos == 0; first read triggers fallback. pos 0 is always in
        // range, so it must succeed and return the current (shrunk) S3 content.
        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut buf = BytesMut::zeroed(changed.len());
        let n = reader
            .read_full(&mut buf)
            .await
            .expect("cache_mode fallback at pos==0 must read current S3, not refuse");
        let _ = reader.complete().await;

        assert_eq!(
            &buf[..n],
            changed.as_bytes(),
            "cache_mode fallback at pos==0 must return current S3 content"
        );
    });
}

/// TC-18: CacheMode worker failure when S3 was shrunk past the current read position.
/// CacheMode validates nothing and does NOT clamp the seek: reading from a `pos` that
/// no longer exists in S3 must surface the underlying seek error, not silently return
/// EOF. (The caller then knows the object changed and can re-open.)
#[test]
fn test_tc18_cachemode_worker_failure_shrunk_past_pos_errors() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc18";
        mount_cache_mode(&fs, mount_dir).await;

        let original = "abcdefghijklmnopqrstuvwxyz";
        let cv_path = write_ufs_and_warm_cache(&fs, mount_dir, "tc18.log", original).await;

        // Shrink S3 out-of-band to a few bytes.
        let changed = "abcd";
        let (ufs_path, mount) = fs
            .get_mount(&cv_path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let mut w = mount.ufs.create(&ufs_path, true).await.unwrap();
        w.write(changed.as_bytes()).await.unwrap();
        w.complete().await.unwrap();

        // Position the reader well past the new (shrunk) S3 length before the failing
        // read. pos_mut delegates to cv_reader while no fallback has happened yet, so
        // this needs no worker IO.
        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        *reader.pos_mut() = 20; // > changed.len()

        let mut buf = BytesMut::zeroed(8);
        let err = reader.read_full(&mut buf).await.unwrap_err().to_string();
        assert!(
            err.contains("Invalid seek position"),
            "expected seek-out-of-range error (no clamp), got: {err}"
        );
    });
}

/// TC-19: CacheMode worker failure with S3 unchanged -> fallback reads the value.
/// The happy-path counterpart to TC-17/TC-18: nothing is modified out-of-band, the
/// Curvine worker is simply unreachable, so the read must transparently fall back to
/// S3 and return exactly what was written. Verifies fallback works in the common case,
/// not just under S3 mutation.
#[test]
fn test_tc19_cachemode_worker_failure_unchanged_reads_value() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc19";
        mount_cache_mode(&fs, mount_dir).await;

        // Write to S3 and warm the cache. S3 is NOT modified afterwards.
        let original = "abcdefghijklmnopqrstuvwxyz";
        let cv_path = write_ufs_and_warm_cache(&fs, mount_dir, "tc19.log", original).await;

        // Worker is unreachable; first read triggers fallback to the (unchanged) S3.
        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut buf = BytesMut::zeroed(original.len());
        reader
            .read_full(&mut buf)
            .await
            .expect("cache_mode fallback with unchanged S3 must succeed");
        let _ = reader.complete().await;

        assert_eq!(
            &buf[..],
            original.as_bytes(),
            "cache_mode fallback must return the original S3 content unchanged"
        );
    });
}

/// TC-20: CacheMode worker failure after S3 was GROWN (lengthened) out-of-band.
/// The lengthening counterpart to TC-17 (which shrinks). CacheMode validates nothing
/// and treats S3 as authoritative; reading from pos 0 must fall back and return the
/// CURRENT, longer S3 content -- not the shorter content that was warmed into cache.
#[test]
fn test_tc20_cachemode_worker_failure_grown_reads_current_s3() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc20";
        mount_cache_mode(&fs, mount_dir).await;

        // Warm the cache with the original (shorter) content.
        let original = "abcdefghij";
        let cv_path = write_ufs_and_warm_cache(&fs, mount_dir, "tc20.log", original).await;

        // Modify S3 out-of-band to LONGER content (changes mtime AND len).
        let changed = "abcdefghijklmnopqrstuvwxyz";
        let (ufs_path, mount) = fs
            .get_mount(&cv_path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let mut w = mount.ufs.create(&ufs_path, true).await.unwrap();
        w.write(changed.as_bytes()).await.unwrap();
        w.complete().await.unwrap();

        // Fresh reader at pos == 0; first read triggers fallback. Must succeed and
        // return the current (grown) S3 content in full.
        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut buf = BytesMut::zeroed(changed.len());
        let n = reader
            .read_full(&mut buf)
            .await
            .expect("cache_mode fallback after S3 grew must read current S3, not refuse");
        let _ = reader.complete().await;

        assert_eq!(
            &buf[..n],
            changed.as_bytes(),
            "cache_mode fallback must return the grown S3 content in full"
        );
    });
}

/// TC-21: CacheMode fallback must expose the CURRENT S3 length via len().
/// Regression guard for the reviewer's note on PR #963: after fallback to S3,
/// len() must delegate to the active ufs_reader so len()-based callers (e.g.
/// read_as_string) see the grown object size, not the stale cached length.
#[test]
fn test_tc21_cachemode_fallback_len_reflects_current_s3() {
    let Some(fs) = setup() else {
        return;
    };
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        let mount_dir = "fallback_read_tc21";
        mount_cache_mode(&fs, mount_dir).await;

        // Warm the cache with the original (shorter) content.
        let original = "abcdefghij";
        let cv_path = write_ufs_and_warm_cache(&fs, mount_dir, "tc21.log", original).await;

        // Grow S3 out-of-band to a longer object.
        let changed = "abcdefghijklmnopqrstuvwxyz";
        let (ufs_path, mount) = fs
            .get_mount(&cv_path, RpcCode::GetMountInfo)
            .await
            .unwrap()
            .unwrap();
        let mut w = mount.ufs.create(&ufs_path, true).await.unwrap();
        w.write(changed.as_bytes()).await.unwrap();
        w.complete().await.unwrap();

        // Trigger fallback with a single read, then check len() reflects the grown S3.
        let mut reader = build_reader_with_unreachable_worker(&fs, &cv_path).await;
        let mut probe = BytesMut::zeroed(1);
        let _ = reader.read(&mut probe).await.unwrap();

        assert_eq!(
            reader.len(),
            changed.len() as i64,
            "after CacheMode fallback, len() must report the current (grown) S3 length"
        );
        let _ = reader.complete().await;
    });
}
