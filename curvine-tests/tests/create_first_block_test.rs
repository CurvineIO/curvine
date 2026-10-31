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

//! Issue #433: CreateFile/OpenFile return block 0, so a small file skips
//! the AddBlock RPC. These tests drive the real client writer against a
//! mini cluster.

use curvine_client::file::CurvineFileSystem;
use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_model::OpenFlags;
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::master::fs::MasterFilesystem;
use curvine_tests::Testing;
use std::sync::Arc;

const BLOCK_SIZE: i64 = 64 * 1024;

fn master_block_ids(master: &MasterFilesystem, path: &str) -> CommonResult<Vec<i64>> {
    Ok(master
        .get_block_locations(path)?
        .block_locs
        .iter()
        .map(|v| v.block.id)
        .collect())
}

async fn read_all(fs: &CurvineFileSystem, path: &Path) -> CommonResult<Vec<u8>> {
    let mut reader = fs.open(path).await?;
    let mut buf = vec![0u8; reader.len() as usize];
    reader.read_full(&mut buf).await?;
    reader.complete().await?;
    Ok(buf)
}

#[test]
fn small_files_use_the_block_returned_by_create() -> CommonResult<()> {
    let testing = Testing::builder().default().build()?;
    let cluster = testing.start_cluster()?;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = BLOCK_SIZE;
    conf.client.alloc_first_block_on_create = true;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let master = cluster.get_active_master_fs();

    rt.block_on(async move {
        // The master holds block 0 as soon as CreateFile returns.
        let path = Path::from_str("/first_block/small.data")?;
        let mut writer = fs.create(&path, true).await?;
        let ids = master_block_ids(&master, path.path())?;
        assert_eq!(ids.len(), 1, "create should pre-allocate block 0");
        writer.write(b"hello first block").await?;
        writer.complete().await?;
        assert_eq!(master_block_ids(&master, path.path())?, ids);
        assert_eq!(read_all(&fs, &path).await?, b"hello first block");

        // Empty file: block 0 is released at close and the file stays readable.
        let path = Path::from_str("/first_block/empty.data")?;
        let mut writer = fs.create(&path, true).await?;
        writer.complete().await?;
        assert!(master_block_ids(&master, path.path())?.is_empty());
        assert!(read_all(&fs, &path).await?.is_empty());

        // Cancel without writing: no block is left behind.
        let path = Path::from_str("/first_block/cancel.data")?;
        let mut writer = fs.create(&path, true).await?;
        writer.cancel().await?;
        assert!(master_block_ids(&master, path.path())?.is_empty());

        // Multi-block file: block 0 from create, the rest from AddBlock.
        let path = Path::from_str("/first_block/multi.data")?;
        let data: Vec<u8> = (0..(BLOCK_SIZE * 2 + 100))
            .map(|v| (v % 251) as u8)
            .collect();
        let mut writer = fs.create(&path, true).await?;
        let first = master_block_ids(&master, path.path())?;
        writer.write(&data).await?;
        writer.complete().await?;
        let ids = master_block_ids(&master, path.path())?;
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], first[0]);
        assert_eq!(read_all(&fs, &path).await?, data);

        // O_TRUNC on a file with data gets a fresh block 0.
        let opts = fs.create_opts_builder().build();
        let flags = OpenFlags::new_write_only().set_truncate(true);
        let mut writer = fs.open_with_opts(&path, opts, flags).await?;
        assert_eq!(master_block_ids(&master, path.path())?.len(), 1);
        writer.write(b"truncated").await?;
        writer.complete().await?;
        assert_eq!(read_all(&fs, &path).await?, b"truncated");

        // Append to a file with data keeps the AddBlock path and the old blocks.
        let before = master_block_ids(&master, path.path())?;
        let mut writer = fs.append(&path).await?;
        assert_eq!(master_block_ids(&master, path.path())?, before);
        writer.write(b"+more").await?;
        writer.complete().await?;
        assert_eq!(read_all(&fs, &path).await?, b"truncated+more");

        // ftruncate right after create: the pending block is released first.
        let path = Path::from_str("/first_block/resize.data")?;
        let mut writer = fs.create(&path, true).await?;
        writer.seek(10).await?;
        writer.write(b"x").await?;
        writer.complete().await?;
        let mut expect = vec![0u8; 10];
        expect.push(b'x');
        assert_eq!(read_all(&fs, &path).await?, expect);
        Ok(())
    })
}

#[test]
fn disabled_flag_keeps_the_add_block_path() -> CommonResult<()> {
    let testing = Testing::builder().default().build()?;
    let cluster = testing.start_cluster()?;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.alloc_first_block_on_create = false;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let master = cluster.get_active_master_fs();

    rt.block_on(async move {
        let path = Path::from_str("/first_block_off/small.data")?;
        let mut writer = fs.create(&path, true).await?;
        assert!(master_block_ids(&master, path.path())?.is_empty());
        writer.write(b"old path").await?;
        writer.complete().await?;
        assert_eq!(master_block_ids(&master, path.path())?.len(), 1);
        assert_eq!(read_all(&fs, &path).await?, b"old path");
        Ok(())
    })
}
