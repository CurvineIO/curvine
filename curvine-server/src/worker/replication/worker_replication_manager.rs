use crate::worker::block::BlockStore;
use crate::worker::replication::replication_job::ReplicationJob;
use curvine_common::conf::ClusterConf;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::CommonResult;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct WorkerReplicationManager {
    block_store: BlockStore,
    replication_semaphore: Arc<Semaphore>,
    jobs_queue_sender: Arc<Sender<ReplicationJob>>,

    runtime: Arc<AsyncRuntime>,
}

impl WorkerReplicationManager {
    pub fn new(
        block_store: BlockStore,
        async_runtime: Arc<AsyncRuntime>,
        conf: &ClusterConf,
    ) -> Arc<Self> {
        let (send, recv) = tokio::sync::mpsc::channel(10000);
        let handler = Self {
            block_store,
            replication_semaphore: Arc::new(Semaphore::new(10)),
            jobs_queue_sender: Arc::new(send),
            runtime: async_runtime.clone(),
        };
        let handler = Arc::new(handler);
        Self::handle(&handler, async_runtime, recv);
        handler
    }

    fn handle(
        me: &Arc<Self>,
        async_runtime: Arc<AsyncRuntime>,
        mut recv: Receiver<ReplicationJob>,
    ) {
        let handler = me.clone();
        async_runtime.spawn(async move { while let Some(block_id) = recv.recv().await {} });
    }

    pub fn accept_job(&self, job: ReplicationJob) -> CommonResult<()> {
        // step1: check the block_id existence (todo)
        // step2: push into the queue

        self.runtime
            .block_on(async move { self.jobs_queue_sender.send(job).await })?;
        Ok(())
    }
}
