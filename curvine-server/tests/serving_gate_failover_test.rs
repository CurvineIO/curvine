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

#![cfg(target_os = "linux")]

#[path = "common/follower_daemon.rs"]
mod daemon;

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_fs_api::{Path, RpcCode};
use curvine_net::net::InetAddr;
use curvine_proto::GetFilesystemInfoRequest;
use curvine_raft::raft::RaftPeer;
use curvine_rpc::client::RpcClient;
use curvine_rpc::message::{MessageBuilder, INIT_SEQ_ID};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use daemon::{Daemon, TestDirectory};
use std::collections::HashSet;
use std::fs;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn cluster_config() -> (TestDirectory, Vec<ClusterConf>) {
    let sockets: Vec<_> = (0..9)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect();
    let ports: Vec<_> = sockets
        .iter()
        .map(|s| s.local_addr().unwrap().port())
        .collect();
    let root = TestDirectory(std::env::temp_dir().join(format!(
        "curvine-serving-gate-{}-{}",
        std::process::id(),
        ports[0]
    )));
    fs::create_dir_all(&root.0).unwrap();

    let mut base = ClusterConf {
        format_master: true,
        ..Default::default()
    };
    base.master.hostname = "127.0.0.1".to_string();
    base.master.io_threads = 1;
    base.master.worker_threads = 2;
    base.master.log.log_dir = "stderr".to_string();
    base.master.heartbeat_interval = "500ms".to_string();
    base.journal.hostname = "127.0.0.1".to_string();
    base.journal.io_threads = 1;
    base.journal.worker_threads = 2;
    base.journal.raft_tick_interval_ms = 100;
    base.journal.raft_heartbeat_tick = 1;
    base.journal.journal_addrs = (0..3)
        .map(|i| RaftPeer::new(i as u64 + 1, "127.0.0.1", ports[3 * i + 2]))
        .collect();

    base.client.master_addrs = (0..3)
        .map(|i| InetAddr::new("127.0.0.1", ports[3 * i]))
        .collect();
    base.client.hostname = "127.0.0.1".to_string();
    base.client.io_threads = 1;
    base.client.worker_threads = 2;
    base.client.conn_timeout_ms = 300;
    base.client.rpc_timeout_ms = 3_000;
    base.client.conn_retry_max_duration_ms = 0;
    base.client.conn_retry_min_sleep_ms = 10;
    base.client.conn_retry_max_sleep_ms = 100;
    base.client.rpc_retry_max_duration_ms = 0;
    base.client.rpc_retry_min_sleep_ms = 10;
    base.client.rpc_retry_max_sleep_ms = 100;
    base.client.init().unwrap();

    let confs = (0..3)
        .map(|i| {
            let mut conf = base.clone();
            conf.master.rpc_port = ports[3 * i];
            conf.master.web_port = ports[3 * i + 1];
            conf.journal.rpc_port = ports[3 * i + 2];
            conf.master.meta_dir = root
                .0
                .join(format!("meta-{i}"))
                .to_str()
                .unwrap()
                .to_string();
            conf.journal.journal_dir = root
                .0
                .join(format!("journal-{i}"))
                .to_str()
                .unwrap()
                .to_string();
            conf
        })
        .collect();
    (root, confs)
}

async fn wait_ready(fs: &CurvineFileSystem, daemons: &mut [Daemon]) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        for daemon in daemons.iter_mut() {
            daemon.assert_running();
        }
        if tokio::time::timeout(Duration::from_secs(2), fs.get_filesystem_info())
            .await
            .is_ok_and(|r| r.is_ok())
        {
            return;
        }
        assert!(Instant::now() < deadline, "no elected master became ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn leader_index(confs: &[ClusterConf]) -> Option<usize> {
    for (index, conf) in confs.iter().enumerate() {
        let Ok(rpc) =
            RpcClient::with_raw(&conf.client.master_addrs[index], &conf.client_rpc_conf()).await
        else {
            continue;
        };
        let request = MessageBuilder::new_rpc(RpcCode::GetFilesystemInfo)
            .seq_id(INIT_SEQ_ID)
            .proto_header(GetFilesystemInfoRequest::default())
            .build();
        if let Ok(response) = rpc.timeout_rpc(Duration::from_secs(2), request).await {
            if response.is_success() {
                return Some(index);
            }
        }
    }
    None
}

async fn status_with_retry(fs: &CurvineFileSystem, path: &Path) -> curvine_model::FileStatus {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(status) = fs.get_status(path).await {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "status never succeeded: {path:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[test]
fn abrupt_failover_during_writes_keeps_inode_ids_unique() {
    let (root, confs) = cluster_config();
    let mut daemons = Vec::new();
    for (i, conf) in confs.iter().enumerate() {
        let path = root.0.join(format!("master-{i}.toml"));
        fs::write(&path, conf.to_pretty_toml().unwrap()).unwrap();
        daemons.push(Daemon::spawn("master", &path, &root));
    }

    let rt = Arc::new(AsyncRuntime::single());
    let rejections = Arc::new(AtomicUsize::new(0));

    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(confs[0].clone(), rt.clone()).unwrap();
        wait_ready(&fs, &mut daemons).await;

        let mut pre = Vec::new();
        for i in 0..16 {
            let path = Path::from_str(format!("/pre/{i:03}")).unwrap();
            fs.mkdir(&path, true).await.unwrap();
            pre.push(path);
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_idx = {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                if let Some(index) = leader_index(&confs).await {
                    break index;
                }
                assert!(Instant::now() < deadline, "no master reported leadership");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        let client = CurvineFileSystem::with_rt(confs[0].clone(), rt.clone()).unwrap();
        let mut set = tokio::task::JoinSet::new();
        for task in 0..4u32 {
            let client = client.clone();
            let rejections = rejections.clone();
            set.spawn(async move {
                let mut paths = Vec::new();
                for i in 0..50u32 {
                    let path = Path::from_str(format!("/load/{task:02}/{i:03}")).unwrap();
                    let deadline = Instant::now() + Duration::from_secs(60);
                    loop {
                        match client.mkdir(&path, true).await {
                            Ok(_) => break,
                            Err(_) => {
                                rejections.fetch_add(1, Ordering::Relaxed);
                                assert!(
                                    Instant::now() < deadline,
                                    "mkdir never succeeded after failover: {path:?}"
                                );
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                    }
                    paths.push(path);
                }
                paths
            });
        }

        // The writers above are running when the leader dies, so it is still
        // committing while the next leader inherits a tail to replay.
        tokio::time::sleep(Duration::from_millis(200)).await;
        // SIGKILL instead of SIGTERM: a graceful stop drains the journal, which
        // removes the replay window this failover has to survive.
        daemons[leader_idx].kill().await;

        let mut load = Vec::new();
        while let Some(result) = set.join_next().await {
            load.extend(result.unwrap());
        }

        let verifier = CurvineFileSystem::with_rt(confs[0].clone(), rt.clone()).unwrap();
        let mut ids = HashSet::new();
        for path in &pre {
            let status = status_with_retry(&verifier, path).await;
            assert!(ids.insert(status.id), "duplicate inode id {}", status.id);
        }
        for path in &load {
            let status = status_with_retry(&verifier, path).await;
            assert!(status.is_dir, "expected a directory at {path:?}");
            assert!(
                ids.insert(status.id),
                "duplicate inode id {} for {path:?}",
                status.id
            );
        }

        // A rejected call only proves the client retried while leadership
        // moved: it cannot tell the serving gate from a dead connection. The
        // gate semantics are covered deterministically by the MasterFilesystem
        // tests in curvine-master/tests/serving_gate.rs.
        assert!(
            rejections.load(Ordering::Relaxed) > 0,
            "the failover never rejected a write"
        );
    });
}
