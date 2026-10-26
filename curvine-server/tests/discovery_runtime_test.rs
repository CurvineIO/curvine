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

#![cfg(all(feature = "discovery-etcd", target_os = "linux"))]

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::ClusterConf;
use curvine_fs_api::Path;
use curvine_net::net::InetAddr;
use curvine_raft::raft::RaftPeer;
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_service_discovery::{
    EtcdDiscoveryConfig, EtcdServiceResolver, ServiceEndpoint, ServiceKind, ServiceResolver,
    ServiceStatus,
};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
#[path = "common/daemon.rs"]
mod daemon;
use daemon::{Daemon, TestDirectory};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn test_config(etcd: String) -> (TestDirectory, ClusterConf, PathBuf) {
    // Hold all sockets together while selecting distinct ports for this cluster.
    let listeners: Vec<_> = (0..5)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
        .collect();
    let ports: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    let directory = TestDirectory(std::env::temp_dir().join(format!(
        "curvine-discovery-runtime-{}-{}",
        std::process::id(),
        ports[0]
    )));
    fs::create_dir_all(directory.0.join("master-log")).unwrap();
    fs::create_dir_all(directory.0.join("worker-log")).unwrap();
    let mut conf = ClusterConf::default();
    conf.format_master = true;
    conf.cluster_id = format!("discovery-runtime-{}", ports[0]);
    conf.master.hostname = "127.0.0.1".to_string();
    conf.master.rpc_port = ports[0];
    conf.master.web_port = ports[1];
    conf.master.meta_dir = directory.0.join("meta").to_str().unwrap().to_string();
    conf.master.io_threads = 1;
    conf.master.worker_threads = 2;
    conf.master.log.log_dir = directory.0.join("master-log").to_str().unwrap().to_string();
    conf.journal.hostname = "127.0.0.1".to_string();
    conf.journal.rpc_port = ports[2];
    conf.journal.journal_dir = directory.0.join("journal").to_str().unwrap().to_string();
    conf.journal.journal_addrs = vec![RaftPeer::new(1, "127.0.0.1", ports[2])];
    conf.journal.io_threads = 1;
    conf.journal.worker_threads = 2;
    conf.worker.hostname = "127.0.0.1".to_string();
    conf.worker.rpc_port = ports[3];
    conf.worker.web_port = ports[4];
    conf.worker.io_threads = 1;
    conf.worker.worker_threads = 2;
    conf.worker.data_dir = vec![format!(
        "[SSD:64MB]{}",
        directory.0.join("worker-data").display()
    )];
    conf.worker.log.log_dir = directory.0.join("worker-log").to_str().unwrap().to_string();
    conf.client.master_addrs = vec![InetAddr::new("127.0.0.1", ports[0])];
    conf.client.io_threads = 1;
    conf.client.worker_threads = 2;
    conf.discovery.enabled = true;
    conf.discovery.endpoints = vec![etcd];
    conf.discovery.prefix = format!("/curvine-runtime-test/{}", std::process::id());
    let config_path = directory.0.join("curvine-cluster.toml");
    fs::write(&config_path, conf.to_pretty_toml().unwrap()).unwrap();
    drop(listeners);
    (directory, conf, config_path)
}

async fn wait_endpoints(
    resolver: &EtcdServiceResolver,
    kind: &str,
    count: usize,
    daemon: &mut Daemon,
) -> Vec<ServiceEndpoint> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        daemon.assert_running();
        let snapshot = resolver
            .list(ServiceKind::try_new(kind).unwrap())
            .await
            .unwrap();
        if snapshot.endpoints.len() == count
            && snapshot
                .endpoints
                .iter()
                .all(|endpoint| endpoint.status == ServiceStatus::Serving)
        {
            return snapshot.endpoints;
        }
        assert!(
            Instant::now() < deadline,
            "expected {count} {kind} endpoints, found {:?}: {}",
            snapshot.endpoints,
            fs::read_to_string(&daemon.log_path).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_removed(resolver: &EtcdServiceResolver, kind: &str) {
    // This must be immediate lease revocation, not waiting for lease TTL expiry.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if resolver
            .list(ServiceKind::try_new(kind).unwrap())
            .await
            .unwrap()
            .endpoints
            .is_empty()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{kind} registration was not revoked"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
#[ignore = "requires CURVINE_ETCD_ADDR and a running etcd cluster"]
fn daemon_registration_live_resolution_and_graceful_revoke() {
    let etcd = std::env::var("CURVINE_ETCD_ADDR").expect("CURVINE_ETCD_ADDR must be set");
    let (directory, conf, config_path) = test_config(etcd);
    let rt = Arc::new(AsyncRuntime::single());
    rt.block_on(async {
        let resolver = EtcdServiceResolver::connect(
            EtcdDiscoveryConfig::from((&conf.discovery, conf.cluster_id.as_str())),
            rt.clone(),
        )
        .await
        .unwrap();
        let mut master = Daemon::spawn("master", &config_path, &directory);
        let masters = wait_endpoints(&resolver, "master", 1, &mut master).await;
        assert_eq!(masters[0].id, "1");
        assert_eq!(masters[0].host, "127.0.0.1");
        assert_eq!(masters[0].rpc_port, conf.master.rpc_port);
        assert_eq!(masters[0].status, ServiceStatus::Serving);

        let mut worker = Daemon::spawn("worker", &config_path, &directory);
        let workers = wait_endpoints(&resolver, "worker", 1, &mut worker).await;
        assert_eq!(workers[0].host, "127.0.0.1");
        assert_eq!(workers[0].rpc_port, conf.worker.rpc_port);
        assert_eq!(workers[0].status, ServiceStatus::Serving);
        assert!(!workers[0].id.is_empty());

        // A dead static seed proves that the client actually consumed etcd
        // membership instead of succeeding through the old configured address.
        let dead_seed = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let mut client_conf = conf.clone();
        client_conf.client.master_addrs = vec![InetAddr::new(
            "127.0.0.1",
            dead_seed.local_addr().unwrap().port(),
        )];
        let client = CurvineFileSystem::with_rt(client_conf, rt.clone()).unwrap();
        // Raw metadata entry points must also await the first snapshot.
        client.get_filesystem_info().await.unwrap();
        let path = Path::from_str("/runtime-discovery").unwrap();
        client.mkdir(&path, false).await.unwrap();
        assert!(client.exists(&path).await.unwrap());
        drop(client);
        drop(dead_seed);

        worker.terminate().await;
        wait_removed(&resolver, "worker").await;
        master.terminate().await;
        wait_removed(&resolver, "master").await;
    });
}
