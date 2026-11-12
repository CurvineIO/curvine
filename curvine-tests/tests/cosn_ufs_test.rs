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

//! COSN (Tencent COS via libhdfs + hadoop-cos) UFS integration tests.
//!
//! These tests require a real COSN-enabled bucket and are ignored by default.
//! Enable with:
//!
//! ```bash
//! cargo test -p curvine-tests --features cosn --test cosn_ufs_test -- --ignored
//! ```
//!
//! Environment variables:
//! - `COSN_BUCKET` — bucket authority, e.g. `mybucket-1250000000`
//! - `COSN_SECRET_ID` / `COSN_SECRET_KEY` / `COSN_REGION`
//! - Optional: `COSN_PREFIX` (bucket subdirectory for test objects, e.g. `tmp/curvine-it`)
//! - Optional: `COSN_ENDPOINT_SUFFIX`, `COSN_USER`
//! - For multi-credential mount: `COSN_BUCKET_B`, `COSN_SECRET_ID_B`,
//!   `COSN_SECRET_KEY_B`, `COSN_REGION_B` (region/bucket fall back to primary)

#![cfg(feature = "cosn")]

use curvine_client::unified::UfsFileSystem;
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_io::DataSlice;
use curvine_model::Provider;
use curvine_ufs_api::CosnConf;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn require_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{} must be set to run ignored COSN tests", key))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

fn primary_conf() -> HashMap<String, String> {
    let mut conf = HashMap::new();
    conf.insert(CosnConf::USER_SECRET_ID.to_string(), require_env("COSN_SECRET_ID"));
    conf.insert(
        CosnConf::USER_SECRET_KEY.to_string(),
        require_env("COSN_SECRET_KEY"),
    );
    conf.insert(CosnConf::USER_REGION.to_string(), require_env("COSN_REGION"));
    if let Some(suffix) = optional_env("COSN_ENDPOINT_SUFFIX") {
        conf.insert(CosnConf::USER_ENDPOINT_SUFFIX.to_string(), suffix);
    }
    if let Some(user) = optional_env("COSN_USER") {
        conf.insert(CosnConf::USER_USER.to_string(), user);
    }
    conf
}

fn secondary_conf() -> HashMap<String, String> {
    let mut conf = HashMap::new();
    conf.insert(
        CosnConf::USER_SECRET_ID.to_string(),
        require_env("COSN_SECRET_ID_B"),
    );
    conf.insert(
        CosnConf::USER_SECRET_KEY.to_string(),
        require_env("COSN_SECRET_KEY_B"),
    );
    conf.insert(
        CosnConf::USER_REGION.to_string(),
        optional_env("COSN_REGION_B").unwrap_or_else(|| require_env("COSN_REGION")),
    );
    if let Some(suffix) = optional_env("COSN_ENDPOINT_SUFFIX_B")
        .or_else(|| optional_env("COSN_ENDPOINT_SUFFIX"))
    {
        conf.insert(CosnConf::USER_ENDPOINT_SUFFIX.to_string(), suffix);
    }
    conf
}

fn bucket_uri(bucket: &str) -> Path {
    Path::from_str(format!("cosn://{}/", bucket)).unwrap()
}

fn test_root_prefix() -> String {
    optional_env("COSN_PREFIX")
        .unwrap_or_default()
        .trim_matches('/')
        .to_string()
}

fn unique_path(bucket: &str, name: &str) -> Path {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let root = test_root_prefix();
    let rel = if root.is_empty() {
        format!("{}_{}", name, ts)
    } else {
        format!("{}/{}_{}", root, name, ts)
    };
    Path::from_str(format!("cosn://{}/{}", bucket, rel)).unwrap()
}

fn open_ufs(bucket: &str, conf: HashMap<String, String>) -> UfsFileSystem {
    UfsFileSystem::new(&bucket_uri(bucket), conf, Some(Provider::Cosn))
        .expect("failed to open COSN UfsFileSystem")
}

/// Two mounts with different credentials must not share a Hadoop FS cache
/// instance (`hdfsBuilderSetForceNewInstance`).
#[tokio::test]
#[ignore = "Requires real COSN buckets and two credential sets"]
async fn cosn_multi_mount_different_credentials() -> CommonResult<()> {
    let bucket_a = require_env("COSN_BUCKET");
    let bucket_b = optional_env("COSN_BUCKET_B").unwrap_or_else(|| bucket_a.clone());
    let conf_a = primary_conf();
    let conf_b = secondary_conf();

    let fs_a = open_ufs(&bucket_a, conf_a);
    let fs_b = open_ufs(&bucket_b, conf_b);

    let dir_a = unique_path(&bucket_a, "cv_cosn_mount_a");
    let dir_b = unique_path(&bucket_b, "cv_cosn_mount_b");

    assert!(fs_a.mkdir(&dir_a, true).await?);
    assert!(fs_b.mkdir(&dir_b, true).await?);
    assert!(fs_a.exists(&dir_a).await?);
    assert!(fs_b.exists(&dir_b).await?);

    // Cross-check: each FS still sees its own path after the other mounted.
    assert!(fs_a.exists(&dir_a).await?);
    assert!(fs_b.exists(&dir_b).await?);

    let _ = fs_a.delete(&dir_a, true).await;
    let _ = fs_b.delete(&dir_b, true).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a real COSN bucket"]
async fn cosn_directory_rename() -> CommonResult<()> {
    let bucket = require_env("COSN_BUCKET");
    let fs = open_ufs(&bucket, primary_conf());

    let src = unique_path(&bucket, "cv_cosn_rename_src");
    let dst = unique_path(&bucket, "cv_cosn_rename_dst");
    assert!(fs.mkdir(&src, true).await?);

    let child = Path::from_str(format!("{}/child.txt", src.full_path().trim_end_matches('/'))).unwrap();
    let mut writer = fs.create(&child, true).await?;
    writer
        .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(b"rename-me")))
        .await?;
    writer.complete().await?;

    assert!(fs.rename(&src, &dst).await?);
    assert!(!fs.exists(&src).await?);
    assert!(fs.exists(&dst).await?);
    let renamed_child =
        Path::from_str(format!("{}/child.txt", dst.full_path().trim_end_matches('/'))).unwrap();
    assert!(fs.exists(&renamed_child).await?);

    let _ = fs.delete(&dst, true).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a real COSN bucket"]
async fn cosn_large_file_read_write() -> CommonResult<()> {
    let bucket = require_env("COSN_BUCKET");
    let fs = open_ufs(&bucket, primary_conf());

    // ~16 MiB: exercises CosnWriter chunking and CosnReader short-read loops.
    const SIZE: usize = 16 * 1024 * 1024;
    let mut payload = vec![0u8; SIZE];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let path = unique_path(&bucket, "cv_cosn_large");
    let mut writer = fs.create(&path, true).await?;
    writer
        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(payload.clone())))
        .await?;
    writer.complete().await?;

    let mut reader = fs.open(&path).await?;
    let mut got = Vec::with_capacity(SIZE);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got.len(), SIZE);
    assert_eq!(got, payload);

    // Mid-file pread / seek
    reader.seek((SIZE / 2) as i64).await?;
    let mut mid = vec![0u8; 1024];
    let n = reader.read(&mut mid).await?;
    assert_eq!(n, 1024);
    assert_eq!(&mid[..], &payload[SIZE / 2..SIZE / 2 + 1024]);

    let _ = fs.delete(&path, false).await;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a real COSN bucket"]
async fn cosn_append_unsupported() -> CommonResult<()> {
    let bucket = require_env("COSN_BUCKET");
    let fs = open_ufs(&bucket, primary_conf());

    let path = unique_path(&bucket, "cv_cosn_append");
    let mut writer = fs.create(&path, true).await?;
    writer
        .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(b"seed")))
        .await?;
    writer.complete().await?;

    let Err(err) = fs.append(&path).await else {
        panic!("append must fail");
    };
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("append") && msg.contains("not supported"),
        "unexpected error: {}",
        err
    );

    let _ = fs.delete(&path, false).await;
    Ok(())
}
