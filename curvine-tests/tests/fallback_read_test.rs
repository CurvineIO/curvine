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
use curvine_client::unified::{UfsFileSystem, UnifiedFileSystem, UnifiedReader};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{MountOptionsBuilder, WriteType};
use curvine_tests::Testing;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use std::env;
use std::sync::Arc;

// ---- helpers ---------------------------------------------------------------

fn setup() -> Option<UnifiedFileSystem> {
    let ufs_path = env::var("UFS_TEST_PATH").unwrap_or_default();
    if ufs_path.is_empty() {
        println!("WARNING: UFS_TEST_PATH not set or empty, skipping fallback_read tests");
        return None;
    }
    let testing = Testing::default();
    let rt = Arc::new(AsyncRuntime::single());
    Some(testing.get_unified_fs_with_rt(rt).unwrap())
}

async fn mount_fs_mode(fs: &UnifiedFileSystem) {
    let ufs_base = env::var("UFS_TEST_PATH").unwrap();
    let ufs_path = Path::from_str(format!("{}/fallback_read", ufs_base)).unwrap();
    let cv_path: Path = "/fallback_read".into();

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
async fn write_and_flush(fs: &UnifiedFileSystem, name: &str, data: &str) -> Path {
    let cv_path = Path::from_str(format!("/fallback_read/{}", name)).unwrap();
    let mut w = fs.create(&cv_path, true).await.unwrap();
    w.write(data.as_bytes()).await.unwrap();
    w.complete().await.unwrap();

    // Trigger background flush and wait.
    let (ufs_path, _) = fs.get_mount(&cv_path).await.unwrap().unwrap();
    fs.wait_job_complete(&ufs_path, false).await.unwrap();
    cv_path
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
        mount_fs_mode(&fs).await;
        let cv_path = write_and_flush(&fs, "tc10.log", "hello fallback").await;

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
        mount_fs_mode(&fs).await;
        let data = Utils::rand_str(4 * 1024);
        let cv_path = write_and_flush(&fs, "tc11a.log", &data).await;

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
        mount_fs_mode(&fs).await;

        let prefix = "PREFIX_DATA_";
        let suffix = Utils::rand_str(512);
        let data = format!("{}{}", prefix, suffix);
        let cv_path = write_and_flush(&fs, "tc15.log", &data).await;

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
        mount_fs_mode(&fs).await;

        let data = Utils::rand_str(256 * 1024);
        let cv_path = write_and_flush(&fs, "tc16.log", &data).await;

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

// ---- TC-11b / TC-12 / TC-13 / TC-14: require worker-kill ------------------
//
// These tests are marked #[ignore] because they require the ability to stop a
// specific worker mid-read, which the current MiniCluster API does not support.
// Run manually against a real cluster:
//   cargo test -p curvine-tests fallback -- --ignored

/// TC-11b: Killing the worker node while reading causes transparent fallback.
/// Expected: data read via UFS fallback == data written, no error to caller.
#[test]
#[ignore = "requires manual worker kill; run against a real cluster"]
fn test_tc11b_worker_failure_falls_back_to_ufs() {
    // Manual steps:
    // 1. Write a large file to FsMode and flush to UFS.
    // 2. Start reading, kill the worker holding block 2.
    // 3. Assert read_full() succeeds and data matches.
    todo!("requires worker kill infrastructure")
}

/// TC-12: Worker failure when `ufs_mtime` differs from actual UFS mtime
/// -> `read_chunk0` must return an error (no silent fallback with stale data).
#[test]
#[ignore = "requires manual worker kill; run against a real cluster"]
fn test_tc12_worker_failure_ufs_mtime_mismatch_returns_error() {
    // Manual steps:
    // 1. Write file to FsMode, flush to UFS (sets ufs_mtime).
    // 2. Overwrite the same path directly in UFS to change its mtime.
    // 3. Kill the worker.
    // 4. Assert read_chunk0 returns an error containing "UFS data inconsistent".
    todo!("requires worker kill + UFS direct overwrite")
}

/// TC-13: Worker failure when `ufs_mtime == 0` (never flushed to UFS)
/// -> `read_chunk0` must return an error.
#[test]
#[ignore = "requires manual worker kill; run against a real cluster"]
fn test_tc13_worker_failure_ufs_not_flushed_returns_error() {
    // Manual steps:
    // 1. Write file to FsMode but do NOT wait for flush (ufs_mtime stays 0).
    // 2. Kill the worker.
    // 3. Assert read_chunk0 returns an error containing "not been flushed to UFS".
    todo!("requires worker kill without UFS flush")
}

/// TC-14: After falling back to UFS, `seek()` to a new position then read
/// must return data from the new offset, not the original fallback position.
#[test]
#[ignore = "requires manual worker kill; run against a real cluster"]
fn test_tc14_seek_after_ufs_fallback() {
    // Manual steps:
    // 1. Write a file to FsMode and flush.
    // 2. Start reading, kill worker after block 1, triggering fallback.
    // 3. seek() to offset N.
    // 4. Assert subsequent read returns data starting at offset N.
    todo!("requires worker kill infrastructure")
}
