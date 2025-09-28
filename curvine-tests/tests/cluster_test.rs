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

#![allow(unused)]

use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::{Path, Writer};
use curvine_common::state::FileBlocks;
use curvine_server::test::MiniCluster;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::{CommonError, CommonResult};
use std::sync::Arc;

// Cluster functional unit test.

#[test]
fn block_delete_test() -> CommonResult<()> {
    let mut conf = ClusterConf::default();
    conf.client.block_size = 64 * 1024;
    conf.master.min_block_size = 64 * 1024;

    let cluster = MiniCluster::with_num(&conf, 1, 1);
    let conf = cluster.master_conf().clone();

    cluster.start_cluster();

    Utils::sleep(10000);

    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();
    let path = Path::from_str("/block_delete_test.log")?;
    let file_blocks = rt.block_on(async move {
        let fs = CurvineFileSystem::with_rt(conf, rt1)?;
        let file_blocks = write(&fs, &path).await?;

        fs.delete(&path, false).await?;
        Ok::<FileBlocks, CommonError>(file_blocks)
    })?;

    // Wait for the block to be deleted asynchronously.
    Utils::sleep(10000);

    log::info!("file_blocks {:?}", file_blocks);
    let fs = cluster.get_active_master_fs();
    let sync_fs_dir = fs.fs_dir();
    let fs_dir = sync_fs_dir.read();

    for lc in file_blocks.block_locs {
        // block id no longer exists.
        assert!(!(fs_dir.block_exists(lc.block.id)?));

        // locs have been deleted.
        let block_locs = fs_dir.get_rocks_store().get_locations(lc.block.id)?;
        assert!(block_locs.is_empty());
    }

    Ok(())
}

async fn write(fs: &CurvineFileSystem, path: &Path) -> CommonResult<FileBlocks> {
    let mut writer = fs.create(path, false).await?;
    for _ in 0..10 {
        let str = Utils::rand_str(64 * 1024);
        writer.write(str.as_bytes()).await?;
    }
    writer.complete().await?;

    let locs = fs.get_block_locations(path).await?;
    Ok(locs)
}
