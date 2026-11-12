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

//! COSN adapter integration tests against a real bucket.
//!
//! ```bash
//! cargo test -p curvine-ufs-cosn --features cosn --test integration -- --ignored
//! ```
//!
//! Env: `COSN_BUCKET`, `COSN_SECRET_ID`, `COSN_SECRET_KEY`, `COSN_REGION`,
//! optional `COSN_PREFIX` (bucket subdirectory for all test objects, e.g.
//! `tmp/curvine-it`), `COSN_ENDPOINT_SUFFIX` / `COSN_USER`, and `*_B` variants
//! for the multi-credential case.

#![cfg(feature = "cosn")]

use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_io::DataSlice;
use curvine_ufs_api::CosnConf;
use curvine_ufs_cosn::CosnFileSystem;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn require_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{} must be set for ignored COSN tests", key))
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
    if let Some(suffix) =
        optional_env("COSN_ENDPOINT_SUFFIX_B").or_else(|| optional_env("COSN_ENDPOINT_SUFFIX"))
    {
        conf.insert(CosnConf::USER_ENDPOINT_SUFFIX.to_string(), suffix);
    }
    conf
}

fn bucket_uri(bucket: &str) -> Path {
    Path::from_str(format!("cosn://{}/", bucket)).unwrap()
}

/// Optional subdirectory under the bucket for all test objects.
/// Example: `export COSN_PREFIX=tmp/curvine-it` → `cosn://bucket/tmp/curvine-it/...`
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

fn open_fs(bucket: &str, conf: HashMap<String, String>) -> CosnFileSystem {
    CosnFileSystem::new(&bucket_uri(bucket), conf).expect("CosnFileSystem::new")
}

#[tokio::test]
#[ignore = "Requires real COSN buckets and two credential sets"]
async fn cosn_multi_mount_different_credentials() {
    let bucket_a = require_env("COSN_BUCKET");
    let bucket_b = optional_env("COSN_BUCKET_B").unwrap_or_else(|| bucket_a.clone());
    let fs_a = open_fs(&bucket_a, primary_conf());
    let fs_b = open_fs(&bucket_b, secondary_conf());

    let dir_a = unique_path(&bucket_a, "cv_cosn_mount_a");
    let dir_b = unique_path(&bucket_b, "cv_cosn_mount_b");
    assert!(fs_a.mkdir(&dir_a, true).await.unwrap());
    assert!(fs_b.mkdir(&dir_b, true).await.unwrap());
    assert!(fs_a.exists(&dir_a).await.unwrap());
    assert!(fs_b.exists(&dir_b).await.unwrap());

    let _ = fs_a.delete(&dir_a, true).await;
    let _ = fs_b.delete(&dir_b, true).await;
}

#[tokio::test]
#[ignore = "Requires a real COSN bucket"]
async fn cosn_directory_rename() {
    let bucket = require_env("COSN_BUCKET");
    let fs = open_fs(&bucket, primary_conf());

    let src = unique_path(&bucket, "cv_cosn_rename_src");
    let dst = unique_path(&bucket, "cv_cosn_rename_dst");
    assert!(fs.mkdir(&src, true).await.unwrap());

    let child =
        Path::from_str(format!("{}/child.txt", src.full_path().trim_end_matches('/'))).unwrap();
    let mut writer = fs.create(&child, true).await.unwrap();
    writer
        .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(b"rename-me")))
        .await
        .unwrap();
    writer.complete().await.unwrap();

    assert!(fs.rename(&src, &dst).await.unwrap());
    assert!(!fs.exists(&src).await.unwrap());
    assert!(fs.exists(&dst).await.unwrap());

    let _ = fs.delete(&dst, true).await;
}

#[tokio::test]
#[ignore = "Requires a real COSN bucket"]
async fn cosn_large_file_read_write() {
    let bucket = require_env("COSN_BUCKET");
    let fs = open_fs(&bucket, primary_conf());

    const SIZE: usize = 16 * 1024 * 1024;
    let mut payload = vec![0u8; SIZE];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let path = unique_path(&bucket, "cv_cosn_large");
    let mut writer = fs.create(&path, true).await.unwrap();
    writer
        .write_chunk(DataSlice::Bytes(bytes::Bytes::from(payload.clone())))
        .await
        .unwrap();
    writer.complete().await.unwrap();

    let mut reader = fs.open(&path).await.unwrap();
    let mut got = Vec::with_capacity(SIZE);
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(got, payload);

    reader.seek((SIZE / 2) as i64).await.unwrap();
    let mut mid = vec![0u8; 1024];
    let n = reader.read(&mut mid).await.unwrap();
    assert_eq!(n, 1024);
    assert_eq!(&mid[..], &payload[SIZE / 2..SIZE / 2 + 1024]);

    let _ = fs.delete(&path, false).await;
}

#[tokio::test]
#[ignore = "Requires a real COSN bucket"]
async fn cosn_append_unsupported() {
    let bucket = require_env("COSN_BUCKET");
    let fs = open_fs(&bucket, primary_conf());

    let path = unique_path(&bucket, "cv_cosn_append");
    let mut writer = fs.create(&path, true).await.unwrap();
    writer
        .write_chunk(DataSlice::Bytes(bytes::Bytes::from_static(b"seed")))
        .await
        .unwrap();
    writer.complete().await.unwrap();

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
}
