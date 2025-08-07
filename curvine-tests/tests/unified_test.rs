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
use curvine_common::proto::MountOptions;
use curvine_common::FsResult;
use curvine_tests::Testing;
use orpc::common::Logger;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;

#[test]
fn run() -> CommonResult<()> {
    Logger::default();
    let rt = Arc::new(AsyncRuntime::single());

    let fs = Testing::get_unified_fs_with_rt(rt.clone())?;
    let path = Path::from_str("/s3/xuen-test/test.log")?;

    rt.block_on(async {
        mount(&fs).await.unwrap();
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
    let path = Path::from_str("s3://flink/xuen-test")?;
    let res = fs.cv().get_mount_point(&path).await?;
    println!("res {:?}", res);
    Ok(())
}
async fn mount(fs: &UnifiedFileSystem) -> FsResult<()> {
    let s3_conf = Testing::get_s3_conf().unwrap();
    let opts = MountOptions {
        update: false,
        properties: s3_conf,
        auto_cache: true,
        cache_ttl_secs: Some(3600),
        consistency_config: None,
    };
    let ufs_path = "s3://flink/xuen-test".into();
    let cv_path = "/xuen-test".into();
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
