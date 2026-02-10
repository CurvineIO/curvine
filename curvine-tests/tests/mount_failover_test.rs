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

use curvine_client::file::{FsClient, FsContext};
use curvine_common::fs::Path;
use curvine_common::state::MountOptions;
use curvine_tests::Testing;
use orpc::common::Logger;
use orpc::runtime::RpcRuntime;
use orpc::CommonResult;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn test_mount_replicated_to_standby_before_mount_returns() -> CommonResult<()> {
    Logger::default();

    // Expand journal flush window so replication lag is deterministic if mount does not force a flush.
    let testing = Testing::builder()
        .default()
        .masters(2)
        .workers(0)
        .mutate_conf(|conf| {
            conf.journal.writer_flush_batch_size = 1_000;
            conf.journal.writer_flush_batch_ms = 60_000;
            conf.journal.raft_tick_interval_ms = 100;
        })
        .build()?;

    let cluster = testing.start_cluster()?;
    let cluster_conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(cluster_conf.client_rpc_conf().create_runtime());

    rt.block_on(async {
        let fs_context = Arc::new(FsContext::with_rt(cluster_conf.clone(), rt.clone())?);
        let client = FsClient::new(fs_context);

        let ufs_path = Path::from_str("oss://mount-failover-test/path")?;
        let cv_path = Path::from_str("/mount-failover-test/path")?;
        let opts = MountOptions::builder().build();

        client.mount(&ufs_path, &cv_path, opts).await?;
        Ok::<(), orpc::CommonError>(())
    })?;

    let standby_fs = cluster
        .get_standby_master_fs()
        .expect("expected one standby master in 2-master cluster");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut replicated = false;
    while Instant::now() < deadline {
        let standby_mounts = standby_fs.fs_dir.read().get_mount_table()?;
        if standby_mounts.len() == 1 {
            replicated = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(
        replicated,
        "mount returned success, but standby still has no mount entry within 2s; leader cutover can lose mount info"
    );

    Ok(())
}
