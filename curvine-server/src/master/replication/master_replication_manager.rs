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
use crate::master::SyncWorkerManager;
use curvine_common::conf::ClusterConf;
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    ReportBlockReplicationRequest, SubmitBlockReplicationResponse, SumbitBlockReplicationRequest,
};
use curvine_common::state::{BlockLocation, StorageType, WorkerAddress};
use log::{error, warn};
use orpc::client::ClientFactory;
use orpc::io::net::InetAddr;
use orpc::message::{Builder, RequestStatus};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sync::FastDashMap;
use orpc::{err_box, try_log, try_option, CommonResult};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub type BlockId = i64;
type WorkerId = u32;

#[derive(Clone)]
pub struct MasterReplicationManager {
    fs: MasterFilesystem,
    worker_manager: SyncWorkerManager,

    replication_semaphore: Arc<Semaphore>,
    runtime: Arc<AsyncRuntime>,

    staging_queue_sender: Arc<Sender<BlockId>>,
    inflight_blocks: Arc<FastDashMap<BlockId, InflightReplicationJob>>,

    worker_client_factory: Arc<ClientFactory>,
    // todo: add some metrics here.
}

struct InflightReplicationJob {
    block_id: BlockId,
    permit: OwnedSemaphorePermit,
    target_worker: WorkerAddress,
}

impl MasterReplicationManager {
    pub fn new(
        fs: &MasterFilesystem,
        conf: &ClusterConf,
        rt: &Arc<AsyncRuntime>,
        worker_manager: &SyncWorkerManager,
    ) -> Arc<Self> {
        let async_runtime = rt.clone();
        let semaphore = Semaphore::new(10);
        let (send, recv) = tokio::sync::mpsc::channel(10000);

        let manager = Self {
            fs: fs.clone(),
            worker_manager: worker_manager.clone(),
            replication_semaphore: Arc::new(semaphore),
            staging_queue_sender: Arc::new(send),
            runtime: rt.clone(),
            inflight_blocks: Default::default(),
            worker_client_factory: Arc::new(Default::default()),
        };
        let manager = Arc::new(manager);
        Self::handle(async_runtime, manager.clone(), recv);
        manager
    }

    fn handle(async_runtime: Arc<AsyncRuntime>, me: Arc<Self>, mut recv: Receiver<BlockId>) {
        let fork = me.clone();
        async_runtime.spawn(async move {
            let manager = fork;
            while let Some(block_id) = recv.recv().await {
                // todo: graceful handle the acquire error
                let permit = manager
                    .replication_semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap();
                if let Err(e) = manager.replicate_block(block_id, permit).await {
                    error!("Failed to replicate block: {}. err: {}", block_id, e);
                }
            }
        });
    }

    fn get_next_worker(&self, worker_id: WorkerId) -> CommonResult<WorkerAddress> {
        let worker_manager = self.worker_manager.read();
        match worker_manager.get_worker(worker_id) {
            None => {
                err_box!("Worker not found: {}", worker_id)
            }
            Some(worker) => Ok(worker.address.clone()),
        }
    }

    fn assign(&self, exclusive_worker_ids: Vec<WorkerId>) -> CommonResult<WorkerAddress> {
        let worker_manager = self.worker_manager.read();
        let mut assignment = worker_manager.choose_workers(1, exclusive_worker_ids)?;
        let worker_id = try_option!(assignment.pop()).worker_id;
        let worker_addr = try_option!(worker_manager.get_worker(worker_id))
            .address
            .clone();
        Ok(worker_addr)
    }

    async fn replicate_block(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
    ) -> CommonResult<()> {
        // todo: check whether the block_id replicas legal

        let locations = {
            let fs_dir = self.fs.fs_dir.read();
            fs_dir.get_block_locations(block_id)?
        };

        // step1: find out the available worker to replicate blocks
        // todo: use pluggable policy to find out the best worker to do replication
        let source_worker_id =
            try_option!(locations.first(), "missing block: {}", block_id).worker_id;
        let source_worker_addr = self.get_next_worker(source_worker_id)?;

        // step2: choose the target worker
        let target_worker_addr = self.assign(locations.iter().map(|x| x.worker_id).collect())?;

        // step3: call the corresponding worker to do replication
        let source_worker_addr = InetAddr::new(
            &source_worker_addr.ip_addr,
            source_worker_addr.rpc_port as u16,
        );
        let source_worker_client = self
            .worker_client_factory
            .create_raw(&source_worker_addr)
            .await?;

        let request = SumbitBlockReplicationRequest {
            block_id,
            target_worker_info: target_worker_addr.clone().into(),
        };
        let msg = Builder::new_rpc(RpcCode::SubmitBlockReplicationJob)
            .request(RequestStatus::Rpc)
            .proto_header(request)
            .build();
        match source_worker_client.rpc(msg).await {
            Ok(response) => {
                let response: SubmitBlockReplicationResponse = response.parse_header()?;
                if !response.success {
                    return err_box!(
                        "Errors on submit replication job to {}. err: {:?}",
                        &source_worker_addr,
                        response.message
                    );
                }
            }
            Err(e) => {
                return err_box!(
                    "Errors on sending replication job to {}, err: {:?}",
                    &source_worker_addr,
                    e
                );
            }
        }

        // step4: add into the replicating queue
        self.inflight_blocks.insert(
            block_id,
            InflightReplicationJob {
                block_id,
                permit,
                target_worker: target_worker_addr,
            },
        );

        Ok(())
    }

    pub fn report_under_replicated_blocks(
        &self,
        worker_id: WorkerId,
        block_ids: Vec<i64>,
    ) -> CommonResult<()> {
        self.runtime.block_on(async move {
            for block_id in &block_ids {
                let _ = try_log!(self.staging_queue_sender.send(*block_id).await);
            }
        });
        Ok(())
    }

    pub fn finish_replicated_block(&self, req: ReportBlockReplicationRequest) -> CommonResult<()> {
        // todo: retry on failure of block replication

        let block_id = req.block_id;
        let success = req.success;
        let message = req.message;
        let storage_type = req.storage_type;
        match self.inflight_blocks.remove(&block_id) {
            None => {
                warn!("Should not happen that Block {} not found", block_id);
            }
            Some(entry) => {
                if success {
                    let dir = self.fs.fs_dir.write();
                    let location =
                        BlockLocation::new(entry.1.target_worker.worker_id, storage_type.into());
                    dir.add_block_location(block_id, location)?;
                } else {
                    error!(
                        "Errors on block replication for block_id: {} to worker: {}. error: {:?}",
                        block_id, &entry.1.target_worker, message
                    );
                }
                drop(entry.1.permit);
            }
        }
        Ok(())
    }
}
