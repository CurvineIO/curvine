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
use curvine_common::proto::ReportBlockReplicationRequest;
use curvine_common::state::{BlockLocation, StorageType, WorkerAddress};
use log::{error, warn};
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sync::FastDashMap;
use orpc::CommonResult;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

type BlockId = i64;
type WorkerId = u32;

#[derive(Clone)]
pub struct BlockReplicationManager {
    fs: MasterFilesystem,
    worker_manager: SyncWorkerManager,

    replication_semaphore: Arc<Semaphore>,
    runtime: Arc<AsyncRuntime>,

    staging_queue_sender: Arc<Sender<BlockId>>,
    inflight_blocks: Arc<FastDashMap<BlockId, InflightReplicationJob>>,
    // todo: add some metrics here.
}

struct InflightReplicationJob {
    block_id: BlockId,
    permit: OwnedSemaphorePermit,
    target_worker: WorkerAddress,
}

impl BlockReplicationManager {
    pub fn new(
        fs: MasterFilesystem,
        conf: ClusterConf,
        rt: Arc<AsyncRuntime>,
        worker_manager: &SyncWorkerManager,
    ) -> Arc<Self> {
        let async_runtime = rt.clone();
        let semaphore = Semaphore::new(10);
        let (send, recv) = tokio::sync::mpsc::channel(10000);

        let manager = Self {
            fs,
            worker_manager: worker_manager.clone(),
            replication_semaphore: Arc::new(semaphore),
            staging_queue_sender: Arc::new(send),
            runtime: rt.clone(),
            inflight_blocks: Default::default(),
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

    async fn replicate_block(
        &self,
        block_id: BlockId,
        permit: OwnedSemaphorePermit,
    ) -> CommonResult<()> {
        // todo: check whether the block_id replicas legal

        let fs = self.fs.fs_dir.write();
        let locations = fs.get_block_locations(block_id)?;
        drop(fs);

        if locations.is_empty() {
            warn!("Found missing block: {}", block_id);
            return Ok(());
        }

        // step1: find out the available worker to replicate blocks
        // todo: use pluggable policy to find out the best worker to do replication
        let worker_id = locations.first().unwrap().worker_id;
        let worker_manager = self.worker_manager.read();
        let Some(worker) = worker_manager.get_worker(worker_id) else {
            warn!("Invalid worker id: {}", worker_id);
            return Ok(());
        };

        // step2: call the corresponding worker to do replication

        // step3: add into the replicating queue
        self.inflight_blocks.insert(
            block_id,
            InflightReplicationJob {
                block_id,
                permit,
                target_worker: worker.address.clone(),
            },
        );

        Ok(())
    }

    // This is invoked by heartbeat task or worker manager to report under_replicated blocks due to the missing workers
    pub async fn report_under_replicated_blocks(
        &self,
        worker_id: WorkerId,
        block_ids: Vec<BlockId>,
    ) -> CommonResult<()> {
        for block_id in block_ids {
            self.staging_queue_sender.send(block_id).await?;
        }
        Ok(())
    }

    pub fn report_replicated_block(&self, req: ReportBlockReplicationRequest) -> CommonResult<()> {
        // todo: retry on failure of block replication

        let block_id = req.block_id;
        let success = req.success;
        let message = req.message;
        match self.inflight_blocks.remove(&block_id) {
            None => {
                warn!("Should not happen that Block {} not found", block_id);
            }
            Some(entry) => {
                if success {
                    // todo: add location for the block id
                    let dir = self.fs.fs_dir.write();
                    let location =
                        BlockLocation::new(entry.1.target_worker.worker_id, StorageType::Disk);
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
