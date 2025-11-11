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
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{MountOptions, MountType, TtlAction};
use curvine_common::FsResult;
use curvine_tests::Testing;
use orpc::common::{DurationUnit, Logger};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn run() -> CommonResult<()> {
    Logger::default();
    let rt = Arc::new(AsyncRuntime::single());

    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;
    let path = Path::from_str("/s3/xuen-test/test.log")?;
    let s3_conf = testing.get_s3_conf();

    // Skip test if S3 configuration is not available
    if s3_conf.is_none() {
        println!("Skipping unified_test as S3 configuration is not available");
        return Ok(());
    }

    rt.block_on(async {
        mount(&fs, s3_conf).await.unwrap();
        get_mount(&fs).await.unwrap();

        let data = "test unified_fs";
        write(&fs, &path, data).await.unwrap();

        let read_data = read(&fs, &path).await.unwrap();
        println!("read {}", read_data);

        assert_eq!(data, read_data)
    });

    Ok(())
}

async fn get_mount(fs: &UnifiedFileSystem) -> FsResult<()> {
    let path = Path::from_str("s3://curvine-test/xuen-test")?;
    let res = fs.cv().get_mount_info(&path).await?;
    println!("res {:?}", res);
    Ok(())
}

async fn mount(fs: &UnifiedFileSystem, s3_conf: Option<HashMap<String, String>>) -> FsResult<()> {
    let exists_mnts = fs.get_mount_table().await?;
    for mnt in exists_mnts {
        println!("Unmounting existing mount point: {}", mnt.cv_path);
        let path = mnt.cv_path.into();
        let umount_resp = fs.umount(&path).await;
        assert!(umount_resp.is_ok(), "{}", umount_resp.unwrap_err());
    }

    let ttl_ms = DurationUnit::from_str("1h")?.as_millis() as i64;

    let mut opts_builder = MountOptions::builder();

    // Set properties only if s3_conf is Some
    if let Some(conf) = s3_conf {
        opts_builder = opts_builder.set_properties(conf);
    }

    let opts = opts_builder
        .ttl_ms(ttl_ms)
        .ttl_action(TtlAction::Delete)
        .mount_type(MountType::Orch)
        .build();

    let ufs_path = "s3://curvine-test/xuen-test".into();
    let cv_path = "/s3/xuen-test".into();
    fs.mount(&ufs_path, &cv_path, opts).await?;
    Ok(())
}

async fn write(fs: &UnifiedFileSystem, path: &Path, data: &str) -> FsResult<()> {
    let mut writer = fs.create(path, true).await?;

    writer.write(data.as_bytes()).await?;
    writer.complete().await?;
    Ok(())
}

async fn read(fs: &UnifiedFileSystem, path: &Path) -> FsResult<String> {
    let mut reader = fs.open(path).await?;

    let mut buf = BytesMut::zeroed(1024);
    let n = reader.read(&mut buf[..]).await?;

    reader.complete().await?;
    Ok(String::from_utf8_lossy(&buf[..n]).to_string())
}
