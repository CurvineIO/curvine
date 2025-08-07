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
