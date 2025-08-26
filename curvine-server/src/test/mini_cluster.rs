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

use crate::master::fs::MasterFilesystem;
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use crate::master::Master;
use crate::worker::Worker;
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::raft::{NodeId, RaftPeer};
use curvine_common::FsResult;
use dashmap::DashMap;
use log::info;
use orpc::client::RpcClient;
use orpc::common::LocalTime;
use orpc::io::net::{InetAddr, NetUtils};
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::{err_box, CommonResult};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct MasterEntry(MasterFilesystem, Arc<MasterReplicationManager>);

// A cluster of unit tests.
pub struct MiniCluster {
    // Cluster default configuration file
    pub cluster_conf: ClusterConf,
    pub master_conf: Vec<ClusterConf>,
    pub worker_conf: Vec<ClusterConf>,

    pub master_entries: DashMap<usize, MasterEntry>,
    pub client_rt: Arc<Runtime>,
}

impl MiniCluster {
    pub fn new(master_conf: Vec<ClusterConf>, worker_conf: Vec<ClusterConf>) -> Self {
        let client_rt = Arc::new(master_conf[0].client_rpc_conf().create_runtime());
        Self {
            cluster_conf: master_conf[0].clone(),
            master_conf,
            worker_conf,
            master_entries: Default::default(),
            client_rt,
        }
    }

    pub fn with_num(conf: &ClusterConf, master_num: u16, worker_num: u16) -> Self {
        let master_conf = Self::default_master_conf(conf, master_num);
        let worker_conf = Self::default_worker_conf(&master_conf[0], worker_num);
        Self::new(master_conf, worker_conf)
    }

    pub fn start_master(&self) {
        for (index, conf) in self.master_conf.iter().enumerate() {
            let master = Master::with_conf(conf.clone()).unwrap();
            self.master_entries.insert(
                index,
                MasterEntry(master.get_fs(), master.get_replication_manager()),
            );
            thread::spawn(move || master.block_on_start());
        }
    }

    pub fn start_worker(&self) {
        for conf in &self.worker_conf {
            let worker = Worker::with_conf(conf.clone()).unwrap();
            thread::spawn(move || worker.block_on_start());
        }
    }

    pub fn start_cluster(&self) {
        self.start_master();

        // Wait for Master to be fully ready before starting Workers
        self.client_rt.block_on(self.wait_master_ready()).unwrap();

        // Add a small additional delay to ensure Master is fully stable
        std::thread::sleep(Duration::from_millis(500));

        self.start_worker();

        self.client_rt.block_on(self.wait_ready()).unwrap();
    }

    /// Wait for Master service to be fully ready (Raft Leader + RPC service started)
    async fn wait_master_ready(&self) -> FsResult<()> {
        let wait_time = LocalTime::mills() + 60 * 1000; // 60 second timeout
        let mut retry_count = 0;
        let mut rpc_connection_attempted = false;

        info!("Waiting for Master service to be ready");

        while LocalTime::mills() <= wait_time {
            retry_count += 1;

            // First check if any master is active by checking the master_fs
            let mut raft_leader_ready = false;
            for master_fs in self.master_entries.iter().map(|x| x.0.clone()) {
                if master_fs.master_monitor.is_active() {
                    raft_leader_ready = true;
                    break;
                }
            }

            if raft_leader_ready && !rpc_connection_attempted {
                info!("Raft leader elected, now checking RPC service availability...");
                rpc_connection_attempted = true;

                // Give RPC service a moment to fully initialize after Raft leader election
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }

            if raft_leader_ready {
                // Additional check: try to create a simple RPC connection to verify the service is actually listening
                let conf = self.master_conf().client_rpc_conf();
                let addr = self.master_conf().master_addr();

                match RpcClient::with_raw(&addr, &conf).await {
                    Ok(_client) => {
                        info!("Master service is ready (Raft leader elected and RPC service listening), retry count: {}", retry_count);
                        return Ok(());
                    }
                    Err(_) => {
                        if retry_count % 5 == 0 && rpc_connection_attempted {
                            // Log every 5 retries after first RPC attempt
                            info!("Raft leader ready but RPC service not yet available, continuing to wait... (attempt {})", retry_count);
                        }
                    }
                }
            } else if retry_count % 20 == 0 {
                // Log every 20 retries before Raft leader is ready
                info!(
                    "Waiting for Raft leader election... (attempt {})",
                    retry_count
                );
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        err_box!("Master service failed to become ready within 60 seconds")
    }

    /// Wait for the entire cluster to be ready (all Workers registered)
    async fn wait_ready(&self) -> FsResult<()> {
        let fs = self.new_fs();
        let wait_time = LocalTime::mills() + 20 * 1000; // 20 second timeout
        let mut retry_count = 0;

        info!(
            "Waiting for all Workers to register, expected Worker count: {}",
            self.worker_conf.len()
        );

        while LocalTime::mills() <= wait_time {
            retry_count += 1;
            let info = fs.get_master_info().await?;
            if info.live_workers.len() == self.worker_conf.len() {
                info!(
                    "Cluster is ready, active Worker count: {}",
                    info.live_workers.len()
                );
                return Ok(());
            } else {
                if retry_count % 3 == 0 {
                    // Log every 3 retries
                    info!(
                        "Waiting for Worker registration... current: {}/{}, retry count: {}",
                        info.live_workers.len(),
                        self.worker_conf.len(),
                        retry_count
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        err_box!("Cluster failed to become ready within 20 seconds")
    }

    pub async fn master_client(&self) -> CommonResult<RpcClient> {
        let conf = self.master_conf().client_rpc_conf();
        let addr = self.master_conf().master_addr();
        let client = RpcClient::with_raw(&addr, &conf).await?;
        Ok(client)
    }

    pub fn master_conf(&self) -> &ClusterConf {
        &self.cluster_conf
    }

    pub fn new_fs(&self) -> CurvineFileSystem {
        CurvineFileSystem::with_rt(self.cluster_conf.clone(), self.client_rt.clone()).unwrap()
    }

    pub fn clone_client_rt(&self) -> Arc<Runtime> {
        self.client_rt.clone()
    }

    pub fn get_active_master_fs(&self) -> MasterFilesystem {
        self.master_entries
            .iter()
            .find(|x| x.0.master_monitor.is_active())
            .map(|x| x.0.clone())
            .unwrap()
    }

    pub fn get_active_master_replication_manager(&self) -> Arc<MasterReplicationManager> {
        self.master_entries
            .iter()
            .find(|x| x.0.master_monitor.is_active())
            .map(|x| x.1.clone())
            .unwrap()
    }

    // Create the default master node configuration.
    // Multiple masters can be created
    fn default_master_conf(conf: &ClusterConf, num: u16) -> Vec<ClusterConf> {
        let mut list = vec![];
        let mut journal_addrs = vec![];
        let mut master_addrs = vec![];
        for index in 0..num {
            let mut master = conf.clone();
            master.master.rpc_port = NetUtils::get_available_port();
            master.journal.rpc_port = NetUtils::get_available_port();
            master.master.web_port = NetUtils::get_available_port();
            master.master.meta_dir = format!("{}/{}", master.master.meta_dir, index);
            master.journal.journal_dir = format!("{}/{}", master.journal.journal_dir, index);

            journal_addrs.push(RaftPeer::new(
                (index + 1) as NodeId,
                &master.journal.hostname,
                master.journal.rpc_port,
            ));

            master_addrs.push(InetAddr::new(
                &master.master.hostname,
                master.master.rpc_port,
            ));

            list.push(master);
        }

        for item in list.iter_mut() {
            item.journal.journal_addrs = journal_addrs.clone();
            item.client.master_addrs = master_addrs.clone();
        }

        list
    }

    pub fn default_worker_conf(conf: &ClusterConf, num: u16) -> Vec<ClusterConf> {
        let mut worker_conf = vec![];

        for index in 0..num {
            let mut worker = conf.clone();
            worker.worker.rpc_port = NetUtils::get_available_port();
            worker.worker.web_port = NetUtils::get_available_port();

            let mut dirs = vec![];
            for item in worker.worker.data_dir {
                dirs.push(format!("{}/{}", item, index));
            }
            worker.worker.data_dir = dirs;
            worker_conf.push(worker)
        }

        worker_conf
    }
}

impl Default for MiniCluster {
    fn default() -> Self {
        Self::with_num(&ClusterConf::default(), 1, 1)
    }
}
