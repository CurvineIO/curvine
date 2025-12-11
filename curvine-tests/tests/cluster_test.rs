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
use curvine_tests::Testing;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::{CommonError, CommonResult};
use std::sync::Arc;

// Cluster functional unit test.

#[test]
fn test_block_deletion_and_cleanup_verification() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(3)
        .with_base_conf_path("../etc/curvine-cluster.toml")
        .mutate_conf(|conf| {
            conf.client.block_size = 64 * 1024;
            conf.master.min_block_size = 64 * 1024;
            print!("-----------------------------------------")
        })
        .build()?;
    testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;

    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();
    let fs = testing.get_fs(Some(rt1.clone()), Some(conf))?;
    let path = Path::from_str("/block_delete_test.log")?;
    rt.block_on(async move {
        let file_blocks = write(&fs, &path).await?;
        log::info!("file_blocks {:?}", file_blocks);

        fs.delete(&path, false).await.map_err(CommonError::from)?;
        Utils::sleep(10000);

        let exists = fs.exists(&path).await.map_err(CommonError::from)?;
        assert!(!exists);
        assert!(fs.get_status(&path).await.is_err());
        assert!(fs.get_block_locations(&path).await.is_err());

        // Verify each previously allocated block cannot be opened on any worker
        for lc in file_blocks.block_locs {
            for loc in lc.locs {
                let bc = fs
                    .fs_context()
                    .block_client(&loc)
                    .await
                    .map_err(CommonError::from)?;
                let res = bc
                    .open_block(
                        &fs.conf().client,
                        &lc.block,
                        0,
                        lc.block.len,
                        Utils::req_id(),
                        0,
                        false,
                    )
                    .await;
                assert!(res.is_err());
            }
        }
        Ok::<(), CommonError>(())
    })?;

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
