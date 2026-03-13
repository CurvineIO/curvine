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
//! | TC-12  | Worker failure when ufs_mtime mismatches -> returns error         |
//! | TC-13  | Worker failure when ufs_mtime=0 (not flushed) -> returns error    |
//! | TC-14  | seek() after fallback to UFS -> continues from new position       |

use bytes::BytesMut;
use curvine_client::file::FsReader;
use curvine_client::unified::{FallbackFsReader, UfsFileSystem, UnifiedFileSystem, UnifiedReader};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
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

    if fs.get_mount(&cv_path).await.unwrap().is_some() {
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
    let (ufs_path, mount) = fs.get_mount(cv_path).await.unwrap().unwrap();
    FallbackFsReader::new(cv_reader, ufs_path, mount.ufs.clone())
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

/// TC-12: Worker failure when `ufs_mtime` differs from actual UFS mtime
/// -> `read_chunk0` must return an error (no silent fallback with stale data).
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
        let (ufs_path, mount) = fs.get_mount(&cv_path).await.unwrap().unwrap();
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
