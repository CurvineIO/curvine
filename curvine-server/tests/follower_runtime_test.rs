// Copyright 2026 OPPO.
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
use curvine_model::{HeartbeatStatus, ProtoUtils};
use curvine_net::net::InetAddr;
use curvine_proto::{
    GetBlockLocationsRequest, GetBlockLocationsResponse, GetFileStatusRequest,
    GetFileStatusResponse, GetFilesystemInfoRequest, GetFilesystemInfoResponse, MkdirRequest,
    WorkerHeartbeatRequest,
};
use curvine_raft::raft::RaftPeer;
use curvine_rpc::client::RpcClient;
use curvine_rpc::message::{
    Message, MessageBuilder, FOLLOWER_READ_SEQ_ID, INIT_SEQ_ID, WORKER_MIRROR_SEQ_ID,
};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use daemon::{Daemon, TestDirectory};
use prost::Message as ProtoMessage;
use std::fs;
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn cluster_config(mirror_reports: bool) -> (TestDirectory, Vec<ClusterConf>) {
    let sockets: Vec<_> = (0..11)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect();
    let ports: Vec<_> = sockets
        .iter()
        .map(|s| s.local_addr().unwrap().port())
        .collect();
    let root = TestDirectory(std::env::temp_dir().join(format!(
        "curvine-follower-runtime-{}-{}",
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
    base.master.follower_reads_enabled = true;
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
    base.client.block_size_str = "1MB".to_string();
    base.client.hostname = "127.0.0.1".to_string();
    base.client.io_threads = 1;
    base.client.worker_threads = 2;
    base.client.init().unwrap();
    base.worker.hostname = "127.0.0.1".to_string();
    base.worker.rpc_port = ports[9];
    base.worker.web_port = ports[10];
    base.worker.io_threads = 1;
    base.worker.worker_threads = 2;
    base.worker.log.log_dir = "stderr".to_string();
    base.worker.follower_reports_enabled = mirror_reports;
    base.worker.data_dir = vec![format!("[SSD:64MB]{}", root.0.join("data").display())];
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

async fn direct<T: ProtoMessage + std::fmt::Debug>(
    conf: &ClusterConf,
    index: usize,
    code: RpcCode,
    header: T,
    follower: bool,
) -> Message {
    direct_with_seq(
        conf,
        index,
        code,
        header,
        if follower {
            FOLLOWER_READ_SEQ_ID
        } else {
            INIT_SEQ_ID
        },
    )
    .await
}

async fn direct_with_seq<T: ProtoMessage + std::fmt::Debug>(
    conf: &ClusterConf,
    index: usize,
    code: RpcCode,
    header: T,
    seq_id: i32,
) -> Message {
    let rpc = RpcClient::with_raw(&conf.client.master_addrs[index], &conf.client_rpc_conf())
        .await
        .unwrap();
    let request = MessageBuilder::new_rpc(code)
        .seq_id(seq_id)
        .proto_header(header)
        .build();
    rpc.timeout_rpc(Duration::from_secs(3), request)
        .await
        .unwrap()
}

async fn follower_reads_served(confs: &[ClusterConf]) -> u64 {
    let mut total = 0;
    for conf in confs {
        let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", conf.master.web_port))
            .await
            .unwrap();
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut body = String::new();
        tokio::time::timeout(Duration::from_secs(3), socket.read_to_string(&mut body))
            .await
            .unwrap()
            .unwrap();
        total += body
            .lines()
            .filter(|line| {
                !line.starts_with('#') && line.contains("master_follower_read_accepted_total{")
            })
            .map(|line| {
                line.split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .sum::<u64>();
    }
    total
}

async fn active_master_index(confs: &[ClusterConf], exclude: Option<usize>) -> Option<usize> {
    for (index, conf) in confs.iter().enumerate() {
        if Some(index) == exclude {
            continue;
        }
        let response = direct(
            conf,
            index,
            RpcCode::GetFilesystemInfo,
            GetFilesystemInfoRequest::default(),
            false,
        )
        .await;
        if response.is_success() {
            return Some(index);
        }
    }
    None
}

fn response_has_live_worker(response: &Message, worker_id: u32) -> bool {
    response.is_success()
        && response
            .parse_header::<GetFilesystemInfoResponse>()
            .is_ok_and(|info| {
                info.live_workers
                    .iter()
                    .any(|worker| worker.address.worker_id == worker_id)
            })
}

fn signal(pid: u32, name: &str) {
    assert!(Command::new("kill")
        .args([name, &pid.to_string()])
        .status()
        .unwrap()
        .success());
}

#[test]
fn static_three_master_follower_reads_reports_and_contact_loss() {
    run_cluster(true);
}

#[test]
fn workers_without_mirroring_fall_back_and_reregister_after_leader_change() {
    run_cluster(false);
}

fn run_cluster(mirror_reports: bool) {
    let (root, confs) = cluster_config(mirror_reports);
    let mut daemons = Vec::new();
    let mut paths = Vec::new();
    for (i, conf) in confs.iter().enumerate() {
        let path = root.0.join(format!("master-{i}.toml"));
        fs::write(&path, conf.to_pretty_toml().unwrap()).unwrap();
        daemons.push(Daemon::spawn("master", &path, &root));
        paths.push(path);
    }
    let rt = Arc::new(AsyncRuntime::single());
    rt.block_on(async {
        let fs = CurvineFileSystem::with_rt(confs[0].clone(), rt.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            for daemon in &mut daemons {
                daemon.assert_running();
            }
            if tokio::time::timeout(Duration::from_secs(2), fs.get_filesystem_info())
                .await
                .is_ok_and(|r| r.is_ok())
            {
                break;
            }
            assert!(Instant::now() < deadline, "no elected master became ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // A quorum can elect a leader before the third RPC listener opens.
        // Wait for every listener before directing reads at a specific follower.
        let deadline = Instant::now() + Duration::from_secs(30);
        for (index, address) in confs[0].client.master_addrs.iter().enumerate() {
            loop {
                daemons[index].assert_running();
                if RpcClient::with_raw(address, &confs[0].client_rpc_conf())
                    .await
                    .is_ok()
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "master {index} RPC never started"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let mut worker = Daemon::spawn("worker", &paths[0], &root);
        let deadline = Instant::now() + Duration::from_secs(30);
        while fs
            .get_filesystem_info()
            .await
            .unwrap()
            .live_workers
            .is_empty()
        {
            worker.assert_running();
            assert!(Instant::now() < deadline, "worker did not register");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let registered_worker = fs
            .get_filesystem_info()
            .await
            .unwrap()
            .live_workers
            .into_iter()
            .next()
            .unwrap();
        let worker_id = registered_worker.address.worker_id;
        let marked_end = WorkerHeartbeatRequest {
            cluster_id: confs[0].cluster_id.clone(),
            worker_id,
            fs_ctime: registered_worker.startup_time_ms as i64,
            address: ProtoUtils::worker_address_to_pb(&registered_worker.address),
            failed_dirs: 0,
            status: HeartbeatStatus::End.into(),
            software_version: registered_worker.software_version.clone(),
            weight: Some(registered_worker.weight),
            worker_session_id: Some(registered_worker.worker_session_id.clone()),
            component_info: registered_worker.component_info.clone(),
            ..Default::default()
        };
        let marked_response = direct_with_seq(
            &confs[0],
            leader_index,
            RpcCode::WorkerHeartbeat,
            marked_end,
            WORKER_MIRROR_SEQ_ID,
        )
        .await;
        assert!(
            !marked_response.is_success(),
            "active master accepted a marked mirror heartbeat as authoritative"
        );
        let after_marked_mirror = direct(
            &confs[0],
            leader_index,
            RpcCode::GetFilesystemInfo,
            GetFilesystemInfoRequest::default(),
            false,
        )
        .await;
        assert!(
            response_has_live_worker(&after_marked_mirror, worker_id),
            "rejected mirror heartbeat changed the active worker registration"
        );

        let path = Path::from_str("/follower-file").unwrap();
        fs.write_string(&path, "passive follower locations")
            .await
            .unwrap();
        let leader_blocks = fs.get_block_locations(&path).await.unwrap();
        assert!(!leader_blocks.block_locs.is_empty());
        let leader_id = fs.fs_client().master_peer_leader_id().unwrap();
        let leader_index = fs
            .fs_client()
            .master_peer_snapshot()
            .iter()
            .position(|p| p.id == leader_id)
            .unwrap();
        let follower_index = (leader_index + 1) % 3;
        let deadline = Instant::now() + Duration::from_secs(30);
        if mirror_reports {
            loop {
                let rep = direct(
                    &confs[0],
                    follower_index,
                    RpcCode::GetBlockLocations,
                    GetBlockLocationsRequest {
                        path: path.encode(),
                    },
                    true,
                )
                .await;
                if rep.is_success() {
                    let header: GetBlockLocationsResponse = rep.parse_header().unwrap();
                    let follower_blocks = ProtoUtils::file_blocks_from_pb(header.blocks);
                    // Replay and passive reports progress independently. A locally
                    // fresh response can precede the leader's latest inode update;
                    // wait for controlled convergence before comparing placements.
                    if follower_blocks.block_locs.len() == leader_blocks.block_locs.len()
                        && follower_blocks
                            .block_locs
                            .iter()
                            .zip(&leader_blocks.block_locs)
                            .all(|(a, b)| a.block.id == b.block.id && a.locs == b.locs)
                    {
                        break;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "follower locations never ready: {}",
                    rep.to_error_msg()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if !mirror_reports {
            // A worker that keeps the old leader-only reporting behavior cannot
            // establish passive location readiness. Metadata may still replay.
            loop {
                let rep = direct(
                    &confs[0],
                    follower_index,
                    RpcCode::FileStatus,
                    GetFileStatusRequest {
                        path: path.encode(),
                        inode_id: None,
                    },
                    true,
                )
                .await;
                if rep.is_success() {
                    let status: GetFileStatusResponse = rep.parse_header().unwrap();
                    if status.status.len == "passive follower locations".len() as i64 {
                        break;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "follower metadata did not catch up"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                !direct(
                    &confs[0],
                    follower_index,
                    RpcCode::GetBlockLocations,
                    GetBlockLocationsRequest {
                        path: path.encode()
                    },
                    true
                )
                .await
                .is_success(),
                "worker without mirroring made follower locations ready"
            );
        }
        // Exercise ordinary opt-in client routing, and observe a real follower
        // counter increase rather than merely proving that direct RPCs work.
        let before = follower_reads_served(&confs).await;
        let mut read_conf = confs[0].clone();
        read_conf.client.follower_reads_enabled = true;
        let reader = CurvineFileSystem::with_rt(read_conf, rt.clone()).unwrap();
        for _ in 0..4 {
            assert!(reader.exists(&path).await.unwrap());
        }
        assert_eq!(
            reader.read_string(&path).await.unwrap(),
            "passive follower locations"
        );
        assert!(
            follower_reads_served(&confs).await > before,
            "opt-in client never offloaded a read to a follower"
        );
        assert_eq!(reader.fs_client().master_peer_leader_id(), Some(leader_id));
        // Normal requests (including sticky reads sent to an old cached leader)
        // remain leader-only even though marked reads can be served here.
        let rep = direct(
            &confs[0],
            follower_index,
            RpcCode::FileStatus,
            GetFileStatusRequest {
                path: path.encode(),
                inode_id: None,
            },
            false,
        )
        .await;
        assert!(!rep.is_success(), "unmarked request accepted on follower");
        let rep = direct(
            &confs[0],
            follower_index,
            RpcCode::Mkdir,
            MkdirRequest {
                path: "/must-not-exist".to_string(),
                opts: Default::default(),
            },
            true,
        )
        .await;
        assert!(!rep.is_success(), "follower accepted a mutation");
        assert!(!fs
            .exists(&Path::from_str("/must-not-exist").unwrap())
            .await
            .unwrap());
        // Healthy idle followers keep accepting marked status reads without any
        // new metadata to apply. Leader-only stats continue to refuse here.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(direct(
            &confs[0],
            follower_index,
            RpcCode::FileStatus,
            GetFileStatusRequest {
                path: path.encode(),
                inode_id: None
            },
            true
        )
        .await
        .is_success());
        assert!(!direct(
            &confs[0],
            follower_index,
            RpcCode::GetFilesystemInfo,
            GetFilesystemInfoRequest::default(),
            true
        )
        .await
        .is_success());
        // Stop both other Raft participants. The target still serves RPCs but
        // must refuse reads once leader contact expires, even when apply=commit.
        for (i, daemon) in daemons.iter().enumerate() {
            if i != follower_index {
                signal(daemon.id(), "-STOP");
            }
        }
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(
            !direct(
                &confs[0],
                follower_index,
                RpcCode::FileStatus,
                GetFileStatusRequest {
                    path: path.encode(),
                    inode_id: None
                },
                true
            )
            .await
            .is_success(),
            "isolated follower served a stale read"
        );
        for (i, daemon) in daemons.iter().enumerate() {
            if i != follower_index {
                signal(daemon.id(), "-CONT");
            }
        }

        if !mirror_reports {
            // Freeze the worker while the cached leader is stopped. Once the
            // other two masters elect a replacement, resume the old leader as
            // a follower before resuming the worker. Its next ordinary,
            // unmarked heartbeat must be refused so the worker's connector
            // discovers the new leader instead of treating the mirror reply
            // as its authoritative heartbeat response.
            let deadline = Instant::now() + Duration::from_secs(30);
            let (worker_info, old_leader_index) = loop {
                worker.assert_running();
                if let Ok(info) = fs.get_filesystem_info().await {
                    if let (Some(worker_info), Some(leader_id)) = (
                        info.live_workers.first().cloned(),
                        fs.fs_client().master_peer_leader_id(),
                    ) {
                        let leader_index = fs
                            .fs_client()
                            .master_peer_snapshot()
                            .iter()
                            .position(|peer| peer.id == leader_id)
                            .unwrap();
                        break (worker_info, leader_index);
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "worker was not live after recovery"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            let worker_id = worker_info.address.worker_id;
            signal(worker.id(), "-STOP");
            signal(daemons[old_leader_index].id(), "-STOP");

            let deadline = Instant::now() + Duration::from_secs(40);
            let new_leader_index = loop {
                for daemon in &daemons {
                    daemon.assert_running();
                }
                if let Some(index) = active_master_index(&confs, Some(old_leader_index)).await {
                    break index;
                }
                assert!(
                    Instant::now() < deadline,
                    "no replacement master was elected"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            assert_ne!(new_leader_index, old_leader_index);
            let before_worker_resume = direct(
                &confs[0],
                new_leader_index,
                RpcCode::GetFilesystemInfo,
                GetFilesystemInfoRequest::default(),
                false,
            )
            .await;
            assert!(before_worker_resume.is_success());
            assert!(
                !response_has_live_worker(&before_worker_resume, worker_id),
                "replacement leader unexpectedly inherited non-mirrored worker state"
            );

            signal(daemons[old_leader_index].id(), "-CONT");
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let old_master = direct(
                    &confs[0],
                    old_leader_index,
                    RpcCode::GetFilesystemInfo,
                    GetFilesystemInfoRequest::default(),
                    false,
                )
                .await;
                if !old_master.is_success() {
                    break;
                }
                assert!(Instant::now() < deadline, "old leader did not step down");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            let unmarked_heartbeat = WorkerHeartbeatRequest {
                cluster_id: confs[0].cluster_id.clone(),
                worker_id,
                fs_ctime: worker_info.startup_time_ms as i64,
                address: ProtoUtils::worker_address_to_pb(&worker_info.address),
                failed_dirs: 0,
                status: HeartbeatStatus::Running.into(),
                software_version: worker_info.software_version.clone(),
                weight: Some(worker_info.weight),
                worker_session_id: Some(worker_info.worker_session_id.clone()),
                component_info: worker_info.component_info.clone(),
                ..Default::default()
            };
            let refused = direct(
                &confs[0],
                old_leader_index,
                RpcCode::WorkerHeartbeat,
                unmarked_heartbeat,
                false,
            )
            .await;
            assert!(
                !refused.is_success(),
                "demoted leader accepted an unmarked heartbeat"
            );
            assert!(
                refused.to_error_msg().contains("Not a leader master"),
                "follower should return NotLeaderMaster, got: {}",
                refused.to_error_msg()
            );

            signal(worker.id(), "-CONT");
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                worker.assert_running();
                let new_master = direct(
                    &confs[0],
                    new_leader_index,
                    RpcCode::GetFilesystemInfo,
                    GetFilesystemInfoRequest::default(),
                    false,
                )
                .await;
                if response_has_live_worker(&new_master, worker_id) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "worker did not re-register with the elected leader"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        worker.terminate().await;
        for daemon in &mut daemons {
            daemon.terminate().await;
        }
    });
}
