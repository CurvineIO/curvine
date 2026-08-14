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
use curvine_config::ClusterConf;
use curvine_core_error::{CommonError, CommonResult};
use curvine_fs_api::{Path, Writer};
use curvine_model::FileBlocks;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
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

#[test]
fn test_client_master_handshake_on_cluster() -> CommonResult<()> {
    // New client handshake against a new master: the client reports its own
    // component_info and caches the master's advertised version / protocol /
    // capabilities from the GetFilesystemInfo compatibility contract.
    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;

    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let info = fs.get_filesystem_info().await?;
        assert!(!info.active_master.is_empty());

        let hs = fs.master_handshake();
        assert!(
            !hs.legacy,
            "a new master must advertise a compatibility contract"
        );
        let compat = hs
            .compatibility
            .as_ref()
            .expect("master compatibility must be cached after the handshake");
        assert_eq!(compat.server.component.as_deref(), Some("master"));
        assert_eq!(hs.protocol_version(), Some(1));
        assert_eq!(hs.min_protocol_version(), Some(1));
        assert_eq!(
            hs.compatibility_mode(),
            curvine_model::proto::CompatibilityModeProto::Diagnose
        );

        // A second handshake is idempotent and keeps the cached contract.
        let hs2 = fs.handshake().await?;
        assert_eq!(hs2.compatibility, hs.compatibility);
        Ok::<(), CommonError>(())
    })?;

    Ok(())
}

#[test]
fn test_client_handshake_accepts_legacy_master_response() -> CommonResult<()> {
    // Legacy master + new client: a response without a compatibility contract
    // (a legacy master encodes only business fields and none of the reserved
    // 1000+ handshake fields) must be treated as a legacy peer and never
    // rejected. The client-side parsing the FsClient runs after every
    // GetFilesystemInfo RPC is fed the same response shape a legacy master
    // produces; wire-level decode of such a payload is covered by the proto
    // compat tests in curvine-proto.
    use curvine_client::file::MasterHandshake;
    use curvine_model::proto::GetFilesystemInfoResponse;

    let legacy_response = GetFilesystemInfoResponse {
        active_master: "old-master".to_string(),
        ..Default::default()
    };
    assert!(legacy_response.compatibility.is_none());

    let hs = MasterHandshake::from_response(&legacy_response);
    assert!(hs.legacy);
    assert!(hs.compatibility.is_none());
    assert!(hs.master_version().is_none());
    assert_eq!(
        hs.compatibility_mode(),
        curvine_model::proto::CompatibilityModeProto::Diagnose
    );
    Ok(())
}
